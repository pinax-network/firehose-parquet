//! Compaction that keeps the writer's row order.
//!
//! delta-rs's OPTIMIZE (fireparq-maintenance up to 1.0.5) bins a date's files
//! newest first and reads each bin through a parallel DataFusion scan, writing
//! batches in the order they arrive: its files hold a date's blocks out of
//! order, some blocks in pieces, and now and then a block's rows out of their
//! Firehose order. This compaction writes the rows as `fireparq build` did:
//!
//! - A date's active files are planned by block range (their `block_num`
//!   stats): files whose ranges overlap form one unit, and units are packed, in
//!   block order, into bins of at most the target size. A unit larger than the
//!   target is a bin of its own, and a bin of one file in writer order is left
//!   as it is.
//! - A bin of files in writer order (fireparq's `part-v1-*` parts, and files
//!   this job wrote, tagged [`ROW_ORDER_TAG`]) is concatenated: each file is
//!   read start to finish, one after the other, and its rows are written as
//!   they are. A block lower than the one before it stops the bin.
//! - A bin with a file out of that order (a delta-rs compaction) is repaired:
//!   its rows are sorted by `block_num` and the table's in-block key
//!   ([`row_order_key`]), a window of blocks at a time. Two rows of a block that
//!   tie on the key stop the bin, since their order can't be recovered.
//!
//! Every file written is tagged [`ROW_ORDER_TAG`] in the log and carries
//! [`ROW_ORDER_KEY`] in its footer. A bin's row count must equal its files'
//! `numRecords`. A date's bins commit together as one OPTIMIZE, with
//! `dataChange: false`, as delta-rs's does.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch};
use deltalake_core::arrow::compute::{
    concat, interleave_record_batch, is_not_null, lexsort_to_indices, take, SortColumn, SortOptions,
};
use deltalake_core::arrow::datatypes::{Int64Type, Schema, SchemaRef};
use deltalake_core::arrow::error::ArrowError;
use deltalake_core::arrow::row::{OwnedRow, RowConverter, SortField};
use deltalake_core::datafile::writer::{PartitionWriter, PartitionWriterConfig};
use deltalake_core::kernel::engine::arrow_conversion::TryIntoArrow as _;
use deltalake_core::kernel::schema::cast::cast_record_batch;
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::kernel::{Action, Add, Remove};
use deltalake_core::logstore::ObjectStoreRef;
use deltalake_core::parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderOptions, RowFilter,
};
use deltalake_core::parquet::arrow::async_reader::{
    AsyncFileReader, ParquetRecordBatchStreamBuilder,
};
use deltalake_core::parquet::arrow::ProjectionMask;
use deltalake_core::parquet::errors::ParquetError;
use deltalake_core::parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use deltalake_core::parquet::file::properties::WriterProperties;
use deltalake_core::parquet::file::statistics::Statistics;
use deltalake_core::protocol::DeltaOperation;
use deltalake_core::table::config::TablePropertiesExt as _;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableError};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStoreExt as _;
use serde_json::{json, Value};

use crate::row_order::{row_order_key, KeyPart};

/// The tag on a file's `add` whose rows are in the writer's order: this
/// job's compactions since 1.0.6. Tags survive checkpoints and log replay, so
/// planning needs no footer read.
pub const ROW_ORDER_TAG: &str = "fireparq.rowOrder";

/// The footer key of a compacted file whose rows are in the writer's order.
pub const ROW_ORDER_KEY: &str = "fireparq-maintenance.row_order";

/// The value of [`ROW_ORDER_TAG`] and [`ROW_ORDER_KEY`].
pub const WRITER_ORDER: &str = "writer";

/// A part `fireparq build` wrote, in its rows' stream order.
const WRITER_PART_PREFIX: &str = "part-v1-";

/// Rows per batch read and written.
const BATCH_ROWS: usize = 8192;

const BLOCK_NUM: &str = "block_num";

/// An active file of a date, from the log.
#[derive(Clone, Debug)]
pub struct DateFile {
    /// Relative to the table: `date=<day>/<name>`.
    pub path: String,
    pub size: u64,
    pub rows: Option<u64>,
    pub first_block: i64,
    pub last_block: i64,
    /// A writer part, or a file this job compacted: rows in the writer's order.
    pub ordered: bool,
    store_path: ObjectPath,
    remove: Remove,
}

/// Files rewritten together into one or more files.
#[derive(Clone, Debug)]
pub struct Bin {
    /// In block order.
    pub files: Vec<DateFile>,
    /// A file is out of the writer's order: the bin is sorted, not concatenated.
    pub repair: bool,
}

impl Bin {
    fn size(&self) -> u64 {
        self.files.iter().map(|file| file.size).sum()
    }
}

fn generic(message: String) -> DeltaTableError {
    DeltaTableError::Generic(message)
}

fn stats_block(stats: &Value, kind: &str) -> Option<i64> {
    stats.get(kind)?.get(BLOCK_NUM)?.as_i64()
}

/// The active files of `date`, with their block ranges from the log's stats
/// (`delta.dataSkippingStatsColumns` includes `block_num`).
pub fn date_files(table: &DeltaTable, date: &str) -> DeltaResult<Vec<DateFile>> {
    let snapshot = table.snapshot()?;
    let mut files = Vec::new();
    for file in snapshot.log_data().iter() {
        let partition = file.partition_values_map().get("date").cloned().flatten();
        if partition.as_deref() != Some(date) {
            continue;
        }
        let path = file.path().to_string();
        let stats: Value = file
            .stats()
            .and_then(|stats| serde_json::from_str(&stats).ok())
            .unwrap_or(Value::Null);
        let (Some(first_block), Some(last_block)) = (
            stats_block(&stats, "minValues"),
            stats_block(&stats, "maxValues"),
        ) else {
            return Err(generic(format!("{path}: no block_num stats in the log")));
        };
        // The deprecated conversion is the only one that carries the tags.
        #[allow(deprecated)]
        let add = file.add_action();
        let name = path.rsplit('/').next().unwrap_or(&path);
        let tagged = add
            .tags
            .as_ref()
            .and_then(|tags| tags.get(ROW_ORDER_TAG).cloned().flatten())
            .is_some_and(|value| value == WRITER_ORDER);
        files.push(DateFile {
            ordered: name.starts_with(WRITER_PART_PREFIX) || tagged,
            size: u64::try_from(file.size()).unwrap_or(0),
            rows: stats.get("numRecords").and_then(Value::as_u64),
            first_block,
            last_block,
            store_path: file.object_store_path(),
            remove: file.remove_action(false),
            path,
        });
    }
    Ok(files)
}

/// The bins of a date's files (see the module documentation), in block order.
pub fn plan(mut files: Vec<DateFile>, target: u64) -> Vec<Bin> {
    files.sort_by(|a, b| {
        (a.first_block, a.last_block, &a.path).cmp(&(b.first_block, b.last_block, &b.path))
    });
    // Units: runs of files whose block ranges overlap.
    let mut units: Vec<Vec<DateFile>> = Vec::new();
    let mut unit_last = i64::MIN;
    for file in files {
        match units.last_mut() {
            Some(unit) if file.first_block <= unit_last => {
                unit_last = unit_last.max(file.last_block);
                unit.push(file);
            }
            _ => {
                unit_last = file.last_block;
                units.push(vec![file]);
            }
        }
    }
    let mut bins: Vec<Bin> = Vec::new();
    let mut current = Bin {
        files: Vec::new(),
        repair: false,
    };
    for unit in units {
        let size: u64 = unit.iter().map(|file| file.size).sum();
        let repair = unit.len() > 1 || unit.iter().any(|file| !file.ordered);
        if !current.files.is_empty() && current.size() + size > target {
            bins.push(std::mem::replace(
                &mut current,
                Bin {
                    files: Vec::new(),
                    repair: false,
                },
            ));
        }
        current.files.extend(unit);
        current.repair |= repair;
    }
    if !current.files.is_empty() {
        bins.push(current);
    }
    // A file alone in writer order is already compact.
    bins.retain(|bin| bin.repair || bin.files.len() > 1);
    bins
}

/// One data file of the store as a Parquet reader; the log gives its size.
struct StoreReader {
    store: ObjectStoreRef,
    path: ObjectPath,
    size: u64,
}

fn store_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileReader for StoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        async move {
            let mut ranges = self
                .store
                .get_ranges(&self.path, &[range])
                .await
                .map_err(store_error)?;
            ranges
                .pop()
                .ok_or_else(|| ParquetError::General(format!("{}: empty read", self.path)))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(store_error)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
        let size = self.size;
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// What a compaction of one date did.
#[derive(Debug, Default)]
pub struct DateCompaction {
    pub files_removed: usize,
    pub files_added: usize,
    pub rows: u64,
    /// Bins sorted back into the writer's order.
    pub repaired_bins: usize,
}

/// A date's compaction: its table, writer settings and the target file size.
pub struct Compaction<'a> {
    pub table: &'a DeltaTable,
    /// The table's name, for its row-order key.
    pub name: &'a str,
    pub date: &'a str,
    pub target: NonZeroU64,
    /// With the footer metadata to carry ([`crate::carried_metadata`] and
    /// [`ROW_ORDER_KEY`]).
    pub properties: WriterProperties,
    /// Uncompressed bytes a repair sorts at once.
    pub window_bytes: u64,
    pub commit: CommitProperties,
}

impl Compaction<'_> {
    /// Rewrites `bins` (of [`plan`]) and commits them as one OPTIMIZE: all of
    /// them or none. When a bin fails, the files written for the bins before
    /// it are deleted again, as nothing names them. A failed commit leaves its
    /// files: a commit whose outcome is unknown may have landed, and an
    /// orphan is only space, which the weekly full VACUUM reclaims.
    pub async fn run(&self, bins: &[Bin]) -> DeltaResult<DateCompaction> {
        let mut outcome = DateCompaction::default();
        let mut actions = Vec::new();
        for bin in bins {
            let (adds, rows) = match self.rewrite(bin).await {
                Ok(written) => written,
                Err(error) => {
                    self.delete_written(&actions).await;
                    return Err(error);
                }
            };
            outcome.files_removed += bin.files.len();
            outcome.files_added += adds.len();
            outcome.rows += rows;
            outcome.repaired_bins += usize::from(bin.repair);
            actions.extend(
                bin.files
                    .iter()
                    .map(|file| Action::Remove(file.remove.clone())),
            );
            actions.extend(adds.into_iter().map(Action::Add));
        }
        let operation = DeltaOperation::Optimize {
            predicate: serde_json::to_string(&[format!("date = '{}'", self.date)]).ok(),
            target_size: i64::try_from(self.target.get()).unwrap_or(i64::MAX),
        };
        CommitBuilder::from(self.commit.clone())
            .with_actions(actions)
            .with_app_metadata(HashMap::from([
                (
                    crate::COMPACTED_BY_KEY.to_string(),
                    json!(env!("CARGO_PKG_VERSION")),
                ),
                (ROW_ORDER_KEY.to_string(), json!(WRITER_ORDER)),
            ]))
            .build(
                Some(self.table.snapshot()?),
                self.table.log_store(),
                operation,
            )
            .await?;
        Ok(outcome)
    }

    /// Deletes the files of `actions`' adds, written for a commit never tried.
    async fn delete_written(&self, actions: &[Action]) {
        let store = self.table.object_store();
        for action in actions {
            if let Action::Add(add) = action {
                if let Ok(path) = ObjectPath::parse(&add.path) {
                    let _ = store.delete(&path).await;
                }
            }
        }
    }

    fn file_schema(&self) -> DeltaResult<SchemaRef> {
        let snapshot = self.table.snapshot()?;
        let partitions = snapshot.metadata().partition_columns();
        let schema: Schema = snapshot.schema().as_ref().try_into_arrow()?;
        let fields: Vec<_> = schema
            .fields()
            .iter()
            .filter(|field| !partitions.contains(field.name()))
            .cloned()
            .collect();
        Ok(Arc::new(Schema::new(fields)))
    }

    /// One bin into new files: their `add`s, and the rows written.
    async fn rewrite(&self, bin: &Bin) -> DeltaResult<(Vec<Add>, u64)> {
        let snapshot = self.table.snapshot()?;
        let properties = snapshot.table_config();
        let stats_columns = properties
            .data_skipping_stats_columns
            .as_ref()
            .map(|columns| columns.iter().map(ToString::to_string).collect::<Vec<_>>());
        let file_schema = self.file_schema()?;
        let config = PartitionWriterConfig::try_new(
            file_schema.clone(),
            Default::default(),
            Some(self.properties.clone()),
            Some(self.target),
            None,
            None,
            Some(ObjectPath::parse(format!("date={}", self.date))?),
        )?;
        let mut writer = PartitionWriter::try_with_config(
            self.table.object_store(),
            config,
            properties.num_indexed_cols(),
            stats_columns,
        )?;
        let written = if bin.repair {
            self.repair(bin, &file_schema, &mut writer).await
        } else {
            self.concatenate(bin, &file_schema, &mut writer).await
        };
        let rows = match written {
            Ok(rows) => rows,
            Err(error) => {
                let _ = writer.abort().await;
                return Err(error);
            }
        };
        let expected: Option<u64> = bin.files.iter().map(|file| file.rows).sum();
        if expected.is_some_and(|expected| expected != rows) {
            let _ = writer.abort().await;
            return Err(generic(format!(
                "{} {}: wrote {rows} rows of {expected:?}",
                self.name, self.date
            )));
        }
        let mut adds = writer.close().await?;
        for add in &mut adds {
            add.data_change = false;
            add.partition_values =
                HashMap::from([("date".to_string(), Some(self.date.to_string()))]);
            add.tags
                .get_or_insert_with(HashMap::new)
                .insert(ROW_ORDER_TAG.to_string(), Some(WRITER_ORDER.to_string()));
        }
        Ok((adds, rows))
    }

    fn reader(&self, file: &DateFile) -> StoreReader {
        StoreReader {
            store: self.table.object_store(),
            path: file.store_path.clone(),
            size: file.size,
        }
    }

    /// Files in writer order, one after the other, their rows as they are.
    async fn concatenate(
        &self,
        bin: &Bin,
        file_schema: &SchemaRef,
        writer: &mut PartitionWriter,
    ) -> DeltaResult<u64> {
        let mut last_block = i64::MIN;
        let mut rows = 0;
        for file in &bin.files {
            let mut stream = ParquetRecordBatchStreamBuilder::new(self.reader(file))
                .await?
                .with_batch_size(BATCH_ROWS)
                .build()?;
            while let Some(batch) = stream.next().await {
                let batch = cast_record_batch(&batch?, file_schema.clone(), false, true)?;
                for block in block_nums(&batch)?.values().iter().copied() {
                    if block < last_block {
                        return Err(generic(format!(
                            "{}: block {block} after block {last_block}, not in block order",
                            file.path
                        )));
                    }
                    last_block = block;
                }
                rows += batch.num_rows() as u64;
                writer.write(&batch).await?;
            }
        }
        Ok(rows)
    }

    /// Files out of writer order, sorted back into it a window of blocks at a
    /// time.
    async fn repair(
        &self,
        bin: &Bin,
        file_schema: &SchemaRef,
        writer: &mut PartitionWriter,
    ) -> DeltaResult<u64> {
        let Some(key) = row_order_key(self.name) else {
            return Err(generic(format!(
                "{}: no row order is known for this table, so it is not repaired",
                self.name
            )));
        };
        let first = bin
            .files
            .iter()
            .map(|file| file.first_block)
            .min()
            .unwrap_or(0);
        let last = bin
            .files
            .iter()
            .map(|file| file.last_block)
            .max()
            .unwrap_or(-1);
        // Each file's row groups, with their block ranges and uncompressed bytes.
        let mut groups: Vec<Vec<RowGroup>> = Vec::new();
        let mut uncompressed = 0u64;
        for file in &bin.files {
            let mut reader = self.reader(file);
            let metadata = reader.get_metadata(None).await?;
            let file_groups = row_groups(&metadata);
            uncompressed += file_groups.iter().map(|group| group.bytes).sum::<u64>();
            groups.push(file_groups);
        }
        // Windows of about `window_bytes` decoded. Parquet's uncompressed size
        // undercounts decoded rows (a dictionary-encoded string is an index
        // there, its whole text in Arrow), so the first window takes a quarter
        // of what it suggests, and each next one is sized by the decoded bytes
        // per block of the one before.
        let span = u64::try_from(last - first + 1).unwrap_or(1).max(1);
        let window_bytes = u128::from(self.window_bytes.max(1));
        let blocks_for = |bytes_per_block: u128| -> i64 {
            let blocks = (window_bytes / bytes_per_block.max(1)).clamp(1, u128::from(span));
            i64::try_from(blocks).unwrap_or(i64::MAX)
        };
        let mut step = blocks_for(u128::from(uncompressed) * 4 / u128::from(span));
        let mut previous: Option<OwnedRow> = None;
        let mut rows = 0;
        let mut low = first;
        while low <= last {
            let high = low.saturating_add(step - 1).min(last);
            let mut batches = Vec::new();
            for (file, file_groups) in bin.files.iter().zip(&groups) {
                if file.last_block < low || file.first_block > high {
                    continue;
                }
                let selected: Vec<usize> = file_groups
                    .iter()
                    .enumerate()
                    .filter(|(_, group)| group.overlaps(low, high))
                    .map(|(index, _)| index)
                    .collect();
                if selected.is_empty() {
                    continue;
                }
                let builder = ParquetRecordBatchStreamBuilder::new(self.reader(file)).await?;
                let schema = builder.parquet_schema();
                if !(0..schema.num_columns()).any(|index| schema.column(index).name() == BLOCK_NUM)
                {
                    return Err(generic(format!("{}: no block_num column", file.path)));
                }
                let mask = ProjectionMask::columns(schema, [BLOCK_NUM]);
                let filter = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                    in_blocks(batch.column(0), low, high)
                });
                let mut stream = builder
                    .with_batch_size(BATCH_ROWS)
                    .with_row_groups(selected)
                    .with_row_filter(RowFilter::new(vec![Box::new(filter)]))
                    .build()?;
                while let Some(batch) = stream.next().await {
                    batches.push(cast_record_batch(
                        &batch?,
                        file_schema.clone(),
                        false,
                        true,
                    )?);
                }
            }
            let window_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            if window_rows > 0 {
                let decoded: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
                let blocks = u128::try_from(high - low + 1).unwrap_or(1);
                step = blocks_for(decoded as u128 / blocks);
            }
            low = high.saturating_add(1);
            if window_rows == 0 {
                continue;
            }
            // Sort the key columns alone, then gather each output batch from
            // the decoded ones: the window is held once, never copied whole.
            let mut keys: Vec<Vec<ArrayRef>> = Vec::new();
            let mut sources: Vec<(usize, usize)> = Vec::with_capacity(window_rows);
            for (index, batch) in batches.iter().enumerate() {
                keys.push(order_columns(batch, key)?);
                sources.extend((0..batch.num_rows()).map(|row| (index, row)));
            }
            let columns: Vec<ArrayRef> = (0..=key.len())
                .map(|column| {
                    let parts: Vec<&dyn Array> =
                        keys.iter().map(|batch| batch[column].as_ref()).collect();
                    concat(&parts)
                })
                .collect::<Result<_, _>>()?;
            drop(keys);
            let sort: Vec<SortColumn> = columns
                .iter()
                .map(|values| SortColumn {
                    values: values.clone(),
                    options: Some(SortOptions::default()),
                })
                .collect();
            let order = lexsort_to_indices(&sort, None)?;
            let sorted_keys: Vec<ArrayRef> = columns
                .iter()
                .map(|values| take(values.as_ref(), &order, None))
                .collect::<Result<_, _>>()?;
            check_strictly_increasing(&sorted_keys, &mut previous, self.name)?;
            drop(sorted_keys);
            let refs: Vec<&RecordBatch> = batches.iter().collect();
            for chunk in order.values().chunks(BATCH_ROWS) {
                let picks: Vec<(usize, usize)> =
                    chunk.iter().map(|&row| sources[row as usize]).collect();
                writer
                    .write(&interleave_record_batch(&refs, &picks)?)
                    .await?;
            }
            rows += window_rows as u64;
        }
        Ok(rows)
    }
}

/// A row group's `block_num` range (`None` without statistics) and size.
struct RowGroup {
    blocks: Option<(i64, i64)>,
    bytes: u64,
}

impl RowGroup {
    fn overlaps(&self, low: i64, high: i64) -> bool {
        self.blocks
            .is_none_or(|(first, last)| last >= low && first <= high)
    }
}

fn row_groups(metadata: &ParquetMetaData) -> Vec<RowGroup> {
    let schema = metadata.file_metadata().schema_descr();
    let column = (0..schema.num_columns()).find(|&index| schema.column(index).name() == BLOCK_NUM);
    metadata
        .row_groups()
        .iter()
        .map(|group| {
            let blocks = column
                .and_then(|index| group.column(index).statistics())
                .and_then(|statistics| match statistics {
                    Statistics::Int64(values) => Some((*values.min_opt()?, *values.max_opt()?)),
                    _ => None,
                });
            RowGroup {
                blocks,
                bytes: u64::try_from(group.total_byte_size()).unwrap_or(0),
            }
        })
        .collect()
}

fn block_nums(batch: &RecordBatch) -> DeltaResult<&deltalake_core::arrow::array::Int64Array> {
    batch
        .column_by_name(BLOCK_NUM)
        .and_then(|column| column.as_primitive_opt::<Int64Type>())
        .ok_or_else(|| generic("no int64 block_num column".into()))
}

fn in_blocks(column: &ArrayRef, low: i64, high: i64) -> Result<BooleanArray, ArrowError> {
    let blocks = column
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| ArrowError::SchemaError("block_num is not int64".into()))?;
    Ok(blocks
        .iter()
        .map(|block| Some(block.is_some_and(|block| block >= low && block <= high)))
        .collect())
}

/// `block_num` and the key's columns, the columns the writer's order sorts by.
fn order_columns(batch: &RecordBatch, key: &[KeyPart]) -> DeltaResult<Vec<ArrayRef>> {
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .cloned()
            .ok_or_else(|| generic(format!("no {name} column for the row order")))
    };
    let mut columns = vec![column(BLOCK_NUM)?];
    for part in key {
        columns.push(match part {
            KeyPart::Column(name) => column(name)?,
            KeyPart::IsSet(name) => Arc::new(is_not_null(column(name)?.as_ref())?) as ArrayRef,
        });
    }
    Ok(columns)
}

/// Each row strictly after the one before, and after `previous` (the last row
/// of the window before); a tie means the writer's order can't be recovered.
fn check_strictly_increasing(
    columns: &[ArrayRef],
    previous: &mut Option<OwnedRow>,
    table: &str,
) -> DeltaResult<()> {
    let fields = columns
        .iter()
        .map(|column| SortField::new(column.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields)?;
    let rows = converter.convert_columns(columns)?;
    for index in 0..rows.num_rows() {
        let row = rows.row(index);
        if previous.as_ref().is_some_and(|before| before.row() >= row) {
            return Err(generic(format!(
                "{table}: two rows of a block tie on the row-order key, so their order \
                 can't be recovered"
            )));
        }
        *previous = Some(row.owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, blocks: (i64, i64), size: u64, ordered: bool) -> DateFile {
        DateFile {
            path: format!("date=2026-09-20/{name}"),
            size,
            rows: Some(1),
            first_block: blocks.0,
            last_block: blocks.1,
            ordered,
            store_path: ObjectPath::from(name),
            remove: Remove {
                path: name.to_string(),
                data_change: false,
                deletion_timestamp: None,
                extended_file_metadata: None,
                partition_values: None,
                size: None,
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
            },
        }
    }

    fn names(bins: &[Bin]) -> Vec<(Vec<&str>, bool)> {
        bins.iter()
            .map(|bin| {
                let names = bin
                    .files
                    .iter()
                    .map(|file| file.path.rsplit('/').next().unwrap())
                    .collect();
                (names, bin.repair)
            })
            .collect()
    }

    #[test]
    fn writer_parts_pack_oldest_first_and_compacted_files_stay() {
        let parts = vec![
            file("part-v1-c", (21, 30), 40, true),
            file("part-v1-a", (1, 10), 40, true),
            file("part-v1-b", (11, 20), 40, true),
            file("part-v1-d", (31, 40), 40, true),
        ];
        assert_eq!(
            names(&plan(parts, 100)),
            vec![
                (vec!["part-v1-a", "part-v1-b"], false),
                (vec!["part-v1-c", "part-v1-d"], false),
            ]
        );
        let compacted = vec![
            file("part-v1-first", (1, 5), 30, true),
            file("compacted-1", (6, 50), 90, true),
            file("compacted-2", (51, 90), 90, true),
        ];
        assert!(plan(compacted, 100).is_empty(), "nothing left to compact");
    }

    #[test]
    fn files_out_of_order_are_repaired_alone_or_with_their_overlap() {
        let files = vec![
            file("part-v1-first", (1, 5), 30, true),
            file("legacy-1", (6, 50), 90, false),
            file("legacy-2", (51, 90), 60, false),
            file("legacy-3", (70, 99), 60, false),
            file("part-v1-last", (100, 101), 10, true),
        ];
        // The writer parts on either side fit no bin with their neighbours,
        // so they are left as they are.
        assert_eq!(
            names(&plan(files, 100)),
            vec![
                (vec!["legacy-1"], true),
                (vec!["legacy-2", "legacy-3"], true),
            ]
        );
        // A date of one file out of order is repaired too.
        let single = vec![file("legacy", (1, 9), 20, false)];
        assert_eq!(names(&plan(single, 100)), vec![(vec!["legacy"], true)]);
        // A small writer part beside it is sorted with it: the key keeps its order.
        let small = vec![
            file("part-v1-a", (1, 4), 10, true),
            file("legacy", (5, 9), 20, false),
        ];
        assert_eq!(
            names(&plan(small, 100)),
            vec![(vec!["part-v1-a", "legacy"], true)]
        );
    }

    #[test]
    fn a_tie_on_the_key_stops_a_repair() {
        use deltalake_core::arrow::array::Int64Array;
        let column = |values: &[i64]| Arc::new(Int64Array::from(values.to_vec())) as ArrayRef;
        let mut previous = None;
        let ordered = [column(&[1, 1, 2]), column(&[0, 1, 0])];
        check_strictly_increasing(&ordered, &mut previous, "logs").unwrap();
        // The next window starts after the last row of this one.
        let next = [column(&[2, 3]), column(&[1, 0])];
        check_strictly_increasing(&next, &mut previous, "logs").unwrap();
        let tie = [column(&[4, 4]), column(&[7, 7])];
        let error = check_strictly_increasing(&tie, &mut previous, "logs").unwrap_err();
        assert!(error.to_string().contains("tie"), "{error}");
        let mut previous = None;
        let behind = [column(&[1, 1]), column(&[2, 1])];
        assert!(check_strictly_increasing(&behind, &mut previous, "logs").is_err());
    }
}
