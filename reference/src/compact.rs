//! OPTIMIZE of an unpartitioned append-only table, at the Arrow level:
//! delta-rs's OPTIMIZE needs DataFusion, and `fireparq maintenance` compacts
//! only `date` partitions in block order.
//!
//! The active files smaller than the target, in commit order (modification
//! time, then path), are packed greedily into bins of at most the target
//! size; a bin of one file is left as it is. Each bin's files are read one
//! after the other and their batches written as they are, cast to the table's
//! current schema (a file written before a column was added gets NULLs
//! there), into one new file: no row goes through the model's cells. A bin's
//! file is kept only if it holds as many rows and, per column, as many NULLs
//! as its inputs. All bins commit together: a `remove` of every input and an
//! `add` of every output, both `dataChange: false`, which an append-only
//! table accepts.

use std::collections::{HashMap, HashSet};

use deltalake_core::arrow::datatypes::SchemaRef;
use deltalake_core::datafile::writer::PartitionWriter;
use deltalake_core::kernel::schema::cast::cast_record_batch;
use deltalake_core::kernel::transaction::CommitBuilder;
use deltalake_core::kernel::{Action, Add, LogicalFileView};
use deltalake_core::parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use deltalake_core::protocol::DeltaOperation;
use deltalake_core::{DeltaResult, DeltaTableError};
use fireparq_maintenance::compact::StoreReader;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStoreExt as _;
use serde_json::Value;

use crate::arrow::file_schema;
use crate::store::{stats, Store, Table};

/// What an OPTIMIZE did: no files when no bin had two.
pub(crate) struct Compacted {
    pub(crate) files_removed: usize,
    pub(crate) files_added: usize,
}

/// Rows and NULL counts, of a bin's inputs or of its output.
#[derive(Default)]
struct Counts {
    /// `None` when a file's statistics lack it.
    rows: Option<u64>,
    /// Per top-level column; `None` when a file's statistics lack it.
    nulls: HashMap<String, Option<u64>>,
}

/// A file's statistic as a count.
fn count(stats: Option<&Value>, path: &[&str]) -> Option<u64> {
    path.iter()
        .try_fold(stats?, |value, key| value.get(key))?
        .as_u64()
}

/// Packs the files smaller than `target` into bins of consecutive files up to
/// `target` bytes; only bins of two files or more are rewritten.
fn bins(files: &[LogicalFileView], target: u64) -> Vec<Vec<&LogicalFileView>> {
    let size = |file: &LogicalFileView| u64::try_from(file.size()).unwrap_or(u64::MAX);
    let mut bins = Vec::new();
    let mut bin: Vec<&LogicalFileView> = Vec::new();
    let mut bytes = 0u64;
    for file in files.iter().filter(|file| size(file) < target) {
        if !bin.is_empty() && bytes.saturating_add(size(file)) > target {
            bins.push(std::mem::take(&mut bin));
            bytes = 0;
        }
        bytes = bytes.saturating_add(size(file));
        bin.push(file);
    }
    bins.push(bin);
    bins.retain(|bin| bin.len() > 1);
    bins
}

/// Compacts `files` (the table's active files, in commit order) into files of
/// up to `target` bytes, as one OPTIMIZE commit.
pub(crate) async fn optimize(
    store: &Store,
    t: &mut Table,
    files: &[LogicalFileView],
    target: u64,
) -> DeltaResult<Compacted> {
    let bins = bins(files, target);
    let mut compacted = Compacted {
        files_removed: 0,
        files_added: 0,
    };
    if bins.is_empty() {
        return Ok(compacted);
    }
    let schema = file_schema(&t.delta)?;
    let mut actions = Vec::new();
    for bin in &bins {
        let adds = rewrite(store, t, bin, &schema).await?;
        compacted.files_removed += bin.len();
        compacted.files_added += adds.len();
        actions.extend(
            bin.iter()
                .map(|file| Action::Remove(file.remove_action(false))),
        );
        actions.extend(adds.into_iter().map(|mut add| {
            add.data_change = false;
            Action::Add(add)
        }));
    }
    let before = t.version();
    let operation = DeltaOperation::Optimize {
        predicate: None,
        target_size: i64::try_from(target).unwrap_or(i64::MAX),
    };
    let committed = CommitBuilder::from(store.commit_properties("OPTIMIZE"))
        .with_actions(actions)
        .build(Some(t.delta.snapshot()?), t.delta.log_store(), operation)
        .await?;
    t.advance(&committed, before + 1)?;
    Ok(compacted)
}

/// One bin into one file of `schema`: its `add`s.
async fn rewrite(
    store: &Store,
    t: &Table,
    bin: &[&LogicalFileView],
    schema: &SchemaRef,
) -> DeltaResult<Vec<Add>> {
    let mut writer = store.writer(t, schema.clone())?;
    let (inputs, written) = match concatenate(t, bin, schema, &mut writer).await {
        Ok(counted) => counted,
        Err(error) => {
            let _ = writer.abort().await;
            return Err(error);
        }
    };
    let mismatch = |what: String| {
        DeltaTableError::Generic(format!("{}: the compacted file {what}", t.spec.name))
    };
    if inputs.rows.is_some_and(|rows| rows != written) {
        let _ = writer.abort().await;
        return Err(mismatch(format!(
            "has {written} rows, its inputs {:?}",
            inputs.rows
        )));
    }
    let adds = writer.close().await?;
    if let Err(difference) = compare(&inputs, &output_counts(&adds), written) {
        // Nothing names the file: delete it rather than leave it to VACUUM.
        let data = store.data_store(t);
        for add in &adds {
            if let Ok(path) = ObjectPath::parse(&add.path) {
                let _ = data.delete(&path).await;
            }
        }
        return Err(mismatch(difference));
    }
    Ok(adds)
}

/// The output's rows and NULL counts against the rows written and the
/// inputs' NULL counts (a count either side lacks is not compared).
fn compare(inputs: &Counts, output: &Counts, written: u64) -> Result<(), String> {
    if output.rows != Some(written) {
        return Err(format!(
            "records {:?} rows, {written} were written",
            output.rows
        ));
    }
    for (column, nulls) in &output.nulls {
        let expected = inputs.nulls.get(column).copied().flatten();
        if let (Some(nulls), Some(expected)) = (nulls, expected) {
            if *nulls != expected {
                return Err(format!(
                    "has {nulls} NULLs in {column}, its inputs {expected}"
                ));
            }
        }
    }
    Ok(())
}

/// Writes the bin's files, one after the other, into `writer`: the rows and
/// NULL counts their statistics give (a column a file lacks is NULL in all its
/// rows), and the rows written.
async fn concatenate(
    t: &Table,
    bin: &[&LogicalFileView],
    schema: &SchemaRef,
    writer: &mut PartitionWriter,
) -> DeltaResult<(Counts, u64)> {
    let mut inputs = Counts {
        rows: Some(0),
        nulls: schema
            .fields()
            .iter()
            .map(|field| (field.name().clone(), Some(0)))
            .collect(),
    };
    let mut written = 0u64;
    for file in bin {
        let size = u64::try_from(file.size()).unwrap_or_default();
        let reader = StoreReader::new(t.delta.object_store(), file.object_store_path(), size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let present: HashSet<String> = builder
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let mut batches = builder.build()?;
        let mut rows = 0u64;
        while let Some(batch) = batches.next().await {
            let batch = cast_record_batch(&batch?, schema.clone(), false, true)?;
            rows += batch.num_rows() as u64;
            writer.write(&batch).await?;
        }
        written += rows;
        let stats = stats(file);
        let file_rows = count(stats.as_ref(), &["numRecords"]);
        inputs.rows = inputs.rows.zip(file_rows).map(|(sum, rows)| sum + rows);
        for (column, nulls) in &mut inputs.nulls {
            let file_nulls = if present.contains(column) {
                count(stats.as_ref(), &["nullCount", column.as_str()])
            } else {
                file_rows
            };
            *nulls = nulls.zip(file_nulls).map(|(sum, nulls)| sum + nulls);
        }
    }
    Ok((inputs, written))
}

/// The rows and NULL counts the output files' statistics give.
fn output_counts(adds: &[Add]) -> Counts {
    let mut counts = Counts {
        rows: Some(0),
        nulls: HashMap::new(),
    };
    for add in adds {
        let stats: Option<Value> = add
            .stats
            .as_deref()
            .and_then(|text| serde_json::from_str(text).ok());
        counts.rows = counts
            .rows
            .zip(count(stats.as_ref(), &["numRecords"]))
            .map(|(sum, rows)| sum + rows);
        let columns = stats
            .as_ref()
            .and_then(|stats| stats.get("nullCount")?.as_object())
            .map(|nulls| nulls.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for column in columns {
            let nulls = count(stats.as_ref(), &["nullCount", column.as_str()]);
            let sum = counts.nulls.entry(column).or_insert(Some(0));
            *sum = sum.zip(nulls).map(|(sum, nulls)| sum + nulls);
        }
    }
    counts
}
