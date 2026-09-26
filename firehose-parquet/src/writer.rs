use crate::config::{BlockMetadata, Compression, Config, Partition};
use crate::metrics::PipelineMetrics;
use anyhow::Result;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

mod local;
pub(crate) use local::create_dir_all_durable;
pub mod properties;
pub mod protected;

/// Key-value metadata to embed in every Parquet file's footer.
#[derive(Debug, Clone, Default)]
pub struct ParquetFileMetadata {
    /// Key-value pairs to store in the Parquet file metadata.
    pub entries: Vec<(String, String)>,
}

impl ParquetFileMetadata {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn add(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.entries.push((key.into(), value.into()));
    }
}

/// Writes Arrow RecordBatches to Parquet files, handling partitioning and
/// file naming. Supports local filesystem and S3 output.
pub struct ParquetTableWriter {
    output_dir: PathBuf,
    partition: Partition,
    compression: Compression,
    /// Part counter per logical partition key (table + partition value).
    part_counters: HashMap<String, u32>,
    /// Unique process identifier to prevent concurrent write collisions.
    /// Each writer instance gets a short UUID prefix so two processes
    /// streaming into the same partition produce distinct file names.
    process_id: String,
    /// S3 object store client (set when output starts with `s3://`).
    s3_client: Option<Arc<dyn ObjectStore>>,
    /// S3 key prefix (bucket path after `s3://bucket/`).
    s3_prefix: Option<String>,
    /// Cache-Control header value for S3 uploads (empty = omit).
    cache_control: String,
    /// File-level metadata embedded in every Parquet file footer.
    file_metadata: ParquetFileMetadata,
}

impl ParquetTableWriter {
    pub fn new(
        output_dir: impl Into<PathBuf>,
        partition: Partition,
        compression: Compression,
    ) -> Self {
        Self {
            output_dir: output_dir.into(),
            partition,
            compression,
            part_counters: HashMap::new(),
            process_id: short_uuid(),
            s3_client: None,
            s3_prefix: None,
            cache_control: String::new(),
            file_metadata: ParquetFileMetadata::new(),
        }
    }

    /// Create a writer that uploads Parquet files to S3.
    pub fn new_s3(
        output_path: &str,
        partition: Partition,
        compression: Compression,
        config: &Config,
    ) -> Result<Self> {
        crate::s3::validate_output_bucket(output_path, config.s3_bucket.as_deref())?;
        let (bucket, prefix) = parse_s3_url(output_path)?;
        let client = crate::s3::build_s3_client(config, &bucket)?;

        Ok(Self {
            output_dir: PathBuf::from(output_path),
            partition,
            compression,
            part_counters: HashMap::new(),
            process_id: short_uuid(),
            s3_client: Some(client),
            s3_prefix: Some(prefix),
            cache_control: config.cache_control.clone().unwrap_or_default(),
            file_metadata: ParquetFileMetadata::new(),
        })
    }

    /// Write a single RecordBatch for `table` to a new Parquet part file.
    /// Returns the file path and the compressed (on-disk) byte size.
    pub fn write_batch(
        &mut self,
        table: &str,
        batch: &RecordBatch,
        metadata: &BlockMetadata,
    ) -> Result<(PathBuf, usize)> {
        if batch.num_rows() == 0 {
            let dir = self.output_dir.join(table);
            if self.s3_client.is_none() {
                local::create_dir_all_durable(&dir)?;
            }
            return Ok((dir, 0));
        }

        let dir = self.partition_dir(table, metadata)?;

        let counter_key = dir.to_string_lossy().to_string();
        let counter = self.part_counters.entry(counter_key).or_insert(0);
        *counter += 1;
        let filename = format!("part-{}-{:06}.parquet", self.process_id, counter);
        let path = dir.join(&filename);

        let compressed_bytes;

        if let Some(ref s3) = self.s3_client {
            // Write to in-memory buffer, then upload to S3.
            let props = self.writer_properties(batch)?;
            let mut buf = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
            writer.write(batch)?;
            writer.close()?;
            compressed_bytes = buf.len();

            let s3_key = self.s3_object_key(table, metadata, &filename)?;
            let s3_path = object_store::path::Path::from(s3_key.as_str());
            let s3_client = Arc::clone(s3);
            let payload = object_store::PutPayload::from(bytes::Bytes::from(buf));
            // block_in_place is needed because this sync writer is called from
            // within a tokio multi-threaded runtime (the gRPC stream handler).
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    s3_client
                        .put_opts(&s3_path, payload, s3_put_options(&self.cache_control))
                        .await
                })
            })
            .map_err(|error| {
                with_root_cause_context(format!("uploading to S3: {s3_key}"), error.into())
            })?;

            info!(
                table,
                path = %s3_key,
                rows = batch.num_rows(),
                compressed_bytes,
                "wrote parquet part to S3"
            );
        } else {
            // Only a closed, synced Parquet file may become visible at its final name.
            compressed_bytes = local::write_parquet(&path, batch, self.writer_properties(batch)?)?;

            info!(
                table,
                path = %path.display(),
                rows = batch.num_rows(),
                compressed_bytes,
                "wrote parquet part"
            );
        }

        Ok((path, compressed_bytes))
    }

    /// Compute the S3 object key for a partition + filename.
    fn s3_object_key(
        &self,
        table: &str,
        metadata: &BlockMetadata,
        filename: &str,
    ) -> Result<String> {
        let prefix = self.s3_prefix.as_deref().unwrap_or("");
        let partition_suffix = self.partition_suffix(table, metadata)?;
        Ok(if prefix.is_empty() {
            format!("{partition_suffix}/{filename}")
        } else {
            format!("{prefix}/{partition_suffix}/{filename}")
        })
    }

    /// Return the partition-relative path, rejecting invalid time metadata.
    /// Missing time retains the nullable-chain unpartitioned fallback.
    pub fn partition_suffix(&self, table: &str, metadata: &BlockMetadata) -> Result<String> {
        if !matches!(
            self.partition,
            Partition::None | Partition::BlockRange { .. }
        ) {
            for timestamp in [metadata.min_timestamp, metadata.max_timestamp]
                .into_iter()
                .flatten()
            {
                crate::traits::checked_timestamp(timestamp)?;
            }
        }
        if !matches!(
            self.partition,
            Partition::None | Partition::BlockRange { .. }
        ) && metadata.min_timestamp.is_none()
        {
            return Ok(table.to_string());
        }
        Ok(
            match self.partition.partition_key(
                metadata.min_block_number,
                metadata.min_timestamp.unwrap_or(0),
            )? {
                Some(key) => format!("{table}/{key}"),
                None => table.to_string(),
            },
        )
    }

    fn partition_dir(&self, table: &str, metadata: &BlockMetadata) -> Result<PathBuf> {
        Ok(self
            .output_dir
            .join(self.partition_suffix(table, metadata)?))
    }

    /// Set file-level metadata to embed in every Parquet file footer.
    pub fn set_file_metadata(&mut self, metadata: ParquetFileMetadata) {
        self.file_metadata = metadata;
    }

    /// Check every row against the declared destination, without assuming row
    /// order: reversible NEW/UNDO streams may visit the same partition in either
    /// order. Metadata describes the whole mapper flush, not each table's exact
    /// extrema. Existing partition-key timestamp policy is shared with routing.
    fn validate_partition(
        &self,
        table: &str,
        batch: &RecordBatch,
        metadata: &BlockMetadata,
    ) -> Result<()> {
        // Reject invalid numeric configuration before path formatting can divide
        // by zero or use the legacy pre-anchor fallback.
        if let Partition::BlockRange { size, start_block } = &self.partition {
            anyhow::ensure!(
                *size > 0,
                "block-range partition size must be greater than zero"
            );
            let anchor = start_block.unwrap_or(0);
            anyhow::ensure!(
                metadata.min_block_number >= anchor && metadata.max_block_number >= anchor,
                "table `{table}` metadata precedes block-range start {anchor}"
            );
        }
        let expected = self.partition_suffix(table, metadata)?;
        let check = |row_metadata: &BlockMetadata| -> Result<()> {
            let actual = self.partition_suffix(table, row_metadata)?;
            anyhow::ensure!(actual == expected,
                "table `{table}` spans partitions or disagrees with its metadata: expected `{expected}`, found `{actual}`; flush the mapper at partition boundaries");
            Ok(())
        };
        match &self.partition {
            Partition::None => Ok(()),
            Partition::BlockRange { start_block, .. } => {
                let anchor = start_block.unwrap_or(0);
                check(&BlockMetadata {
                    min_block_number: metadata.max_block_number,
                    ..metadata.clone()
                })?;
                let blocks = batch.column_by_name("block_num")
                    .and_then(|column| column.as_any().downcast_ref::<arrow::array::UInt64Array>())
                    .ok_or_else(|| anyhow::anyhow!("table `{table}` needs canonical UInt64 block_num for block-range partitioning"))?;
                anyhow::ensure!(
                    blocks.null_count() == 0,
                    "table `{table}` has null block_num values"
                );
                let mut previous = None;
                for block in blocks.values() {
                    // Canonical identities repeat across a block's table rows.
                    // Still inspect every row, but format its destination only
                    // when the routing value changes.
                    if previous == Some(*block) {
                        continue;
                    }
                    anyhow::ensure!(
                        *block >= anchor,
                        "table `{table}` block {block} precedes block-range start {anchor}"
                    );
                    check(&BlockMetadata {
                        min_block_number: *block,
                        ..metadata.clone()
                    })?;
                    previous = Some(*block);
                }
                Ok(())
            }
            Partition::Date | Partition::Hour | Partition::Minute | Partition::Second => {
                let column = batch.column_by_name("timestamp").ok_or_else(|| {
                    anyhow::anyhow!(
                        "table `{table}` needs canonical timestamp for time partitioning"
                    )
                })?;
                anyhow::ensure!(
                    column.data_type() == &crate::traits::timestamp_millis_utc_type(),
                    "table `{table}` timestamp must be Timestamp(Millisecond, UTC)"
                );
                let timestamps = column
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .expect("canonical timestamp type checked above");
                match (metadata.min_timestamp, metadata.max_timestamp) {
                    (Some(_), Some(max)) => check(&BlockMetadata { min_timestamp: Some(max), ..metadata.clone() })?,
                    (None, None) => {},
                    _ => anyhow::bail!("table `{table}` needs both minimum and maximum routing timestamps, or neither"),
                }
                let mut previous = None;
                for timestamp in timestamps.iter().flatten() {
                    let seconds = timestamp.div_euclid(1_000);
                    if previous == Some(seconds) {
                        continue;
                    }
                    anyhow::ensure!(metadata.min_timestamp.is_some(),
                        "table `{table}` has non-null timestamps without routing timestamp metadata");
                    check(&BlockMetadata {
                        min_timestamp: Some(seconds),
                        ..metadata.clone()
                    })?;
                    previous = Some(seconds);
                }
                // Null Solana payload times deliberately use the metadata's
                // synthetic anchor. All-null rows without an anchor preserve
                // the existing flat-table destination.
                Ok(())
            }
        }
    }

    fn writer_properties(&self, batch: &RecordBatch) -> Result<WriterProperties> {
        let metadata = (!self.file_metadata.entries.is_empty()).then(|| {
            self.file_metadata
                .entries
                .iter()
                .map(|(key, value)| {
                    parquet::file::metadata::KeyValue::new(key.clone(), Some(value.clone()))
                })
                .collect()
        });
        properties::for_batch(self.compression, batch, metadata)
    }
}

fn with_root_cause_context(context: impl Display, error: anyhow::Error) -> anyhow::Error {
    let context = context.to_string();
    let root_cause = error.root_cause().to_string();

    if root_cause == error.to_string() {
        error.context(context)
    } else {
        error.context(format!("{context} (root cause: {root_cause})"))
    }
}

/// Fixed compression ratio (compressed/uncompressed) used for diagnostic
/// estimates of validated data retained after a failed table write.
pub(crate) fn compression_ratio(compression: &Compression) -> f64 {
    match compression {
        Compression::None => 0.50,   // Parquet encoding alone: ~2×
        Compression::Snappy => 0.25, // Parquet + Snappy: ~4×
        Compression::Gzip => 0.12,   // Parquet + Gzip: ~8×
        Compression::Zstd | Compression::ZstdWithLevel(_) => 0.12, // Parquet + Zstd: ~8×
    }
}

/// One validated table write retained until publication succeeds.
struct TableBuffer {
    batch: RecordBatch,
    metadata: BlockMetadata,
}

/// Snapshot of validated data that still needs to be materialized.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WriterBufferStats {
    pub tables: usize,
    pub batches: usize,
    pub rows: usize,
    pub estimated_arrow_bytes: u64,
    pub estimated_compressed_bytes: u64,
}

/// Materializes one mapper flush at a time, with one partition per table.
///
/// Every nonempty table is validated before any table is published. Successful
/// writes leave no buffered rows; after an I/O failure, only the failed and
/// unattempted tables remain visible for recovery. Retry only after confirming
/// the failed write did not publish: a post-publication error can leave a final
/// file, so retrying that table can duplicate output. Ingestion stops on errors.
///
/// Callers own flush boundaries; this writer never splits, accumulates or
/// coalesces mapper batches. It is an unprotected single-file writer: protected
/// `fireparq build` publishes all tables through the ingestion transaction
/// instead (`writer::protected`).
pub struct OutputWriter {
    pub inner: ParquetTableWriter,
    buffers: HashMap<String, TableBuffer>,
    compression_ratio: f64,
    metrics: Option<PipelineMetrics>,
}

impl OutputWriter {
    pub fn new(
        output_dir: impl Into<PathBuf>,
        partition: Partition,
        compression: Compression,
    ) -> Self {
        Self {
            inner: ParquetTableWriter::new(output_dir, partition, compression),
            buffers: HashMap::new(),
            compression_ratio: compression_ratio(&compression),
            metrics: None,
        }
    }

    /// Create a writer that uploads Parquet files to S3.
    pub fn new_s3(
        output_path: &str,
        partition: Partition,
        compression: Compression,
        config: &Config,
    ) -> Result<Self> {
        Ok(Self {
            inner: ParquetTableWriter::new_s3(output_path, partition, compression, config)?,
            buffers: HashMap::new(),
            compression_ratio: compression_ratio(&compression),
            metrics: None,
        })
    }

    pub fn set_metrics(&mut self, metrics: PipelineMetrics) {
        self.metrics = Some(metrics);
    }

    pub fn buffered_stats(&self) -> WriterBufferStats {
        let mut stats = WriterBufferStats {
            tables: self.buffers.len(),
            batches: self.buffers.len(),
            ..WriterBufferStats::default()
        };
        for buf in self.buffers.values() {
            stats.rows += buf.batch.num_rows();
            stats.estimated_arrow_bytes += buf.batch.get_array_memory_size() as u64;
        }
        stats.estimated_compressed_bytes =
            (stats.estimated_arrow_bytes as f64 * self.compression_ratio) as u64;
        stats
    }

    /// Fixed compression estimate used only for buffered-data diagnostics.
    pub fn compression_ratio(&self) -> f64 {
        self.compression_ratio
    }

    fn update_buffer_metrics(&self) {
        if let Some(m) = &self.metrics {
            for (table, buf) in &self.buffers {
                m.buffer_rows
                    .get_or_create(&crate::metrics::TableLabels {
                        table: table.clone(),
                    })
                    .set(buf.batch.num_rows() as i64);
            }
            m.buffer_estimated_bytes
                .set(self.buffered_stats().estimated_compressed_bytes as i64);
        }
    }

    /// Validate all nonempty tables, then publish each immediately in table-name
    /// order. Returns true if any data was written. A successful call leaves no
    /// pending rows.
    ///
    /// After a confirmed pre-publication I/O failure, retry retained data with
    /// `flush_remaining` before submitting a new mapper flush. Ambiguous
    /// post-publication failures need external reconciliation first. Retrying
    /// `write_all` would duplicate successful tables, so it is rejected while
    /// any data remains.
    pub fn write_all(
        &mut self,
        batches: &HashMap<String, RecordBatch>,
        metadata: &BlockMetadata,
    ) -> Result<bool> {
        anyhow::ensure!(self.buffers.is_empty(),
            "a previous table write is still pending; reconcile any possibly published output, then retry flush_remaining before submitting new batches");
        let mut tables: Vec<_> = batches
            .iter()
            .filter(|(_, batch)| batch.num_rows() > 0)
            .collect();
        tables.sort_by_key(|(table, _)| *table);
        // Complete preflight before changing buffers, counters, metrics or files.
        for (table, batch) in &tables {
            self.inner.validate_partition(table, batch, metadata)?;
        }
        for (table, batch) in tables {
            self.buffers.insert(
                table.clone(),
                TableBuffer {
                    batch: batch.clone(),
                    metadata: metadata.clone(),
                },
            );
        }
        self.update_buffer_metrics();
        self.flush_remaining()
    }

    fn flush_table(&mut self, table: &str) -> Result<bool> {
        let Some(buf) = self.buffers.get(table) else {
            return Ok(false);
        };
        let num_rows = buf.batch.num_rows();
        let (_, compressed_bytes) = self.inner.write_batch(table, &buf.batch, &buf.metadata)?;
        self.buffers.remove(table);
        if let Some(m) = &self.metrics {
            use crate::metrics::TableLabels;
            m.files_written_total
                .get_or_create(&TableLabels {
                    table: table.to_string(),
                })
                .inc();
            m.file_bytes_total
                .get_or_create(&TableLabels {
                    table: table.to_string(),
                })
                .inc_by(compressed_bytes as u64);
            m.rows_written_total
                .get_or_create(&TableLabels {
                    table: table.to_string(),
                })
                .inc_by(num_rows as u64);
            m.buffer_rows
                .get_or_create(&TableLabels {
                    table: table.to_string(),
                })
                .set(0);
        }
        self.update_buffer_metrics();
        Ok(true)
    }

    /// Retry all retained table writes in stable order. Successful tables are
    /// removed individually; failed and unattempted tables remain visible in
    /// `buffered_stats`. No deferred partition data is hidden after a flush.
    /// Callers must establish that the failed write did not already publish;
    /// post-publication errors can otherwise duplicate that table on retry.
    /// This is in-process recovery, not the crash/replay transaction in #468.
    pub fn flush_remaining(&mut self) -> Result<bool> {
        let mut tables: Vec<_> = self.buffers.keys().cloned().collect();
        tables.sort();
        let mut wrote = false;
        for table in tables {
            wrote |= self.flush_table(&table)?;
        }
        Ok(wrote)
    }
}

/// Parse an S3 URL into (bucket, prefix).
///
/// Generate a short 8-character hex process identifier from a UUID v4.
/// Used to make part file names unique across concurrent processes.
fn short_uuid() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

/// Supports: `s3://bucket/prefix/path` → `("bucket", "prefix/path")`
pub fn parse_s3_url(url: &str) -> Result<(String, String)> {
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow::anyhow!("expected s3:// URL, got: {url}"))?;
    let (bucket, prefix) = match rest.find('/') {
        Some(i) => {
            let prefix = rest[i + 1..].trim_end_matches('/');
            (rest[..i].to_string(), prefix.to_string())
        }
        None => (rest.to_string(), String::new()),
    };
    if bucket.is_empty() {
        return Err(anyhow::anyhow!("S3 URL missing bucket name: {url}"));
    }
    Ok((bucket, prefix))
}

/// Returns [`object_store::PutOptions`] with cache and content-type headers.
///
/// When `cache_control` is non-empty, sets the `Cache-Control` header so that
/// Tigris / CloudFront / any CDN caches accordingly.
pub fn s3_put_options(cache_control: &str) -> object_store::PutOptions {
    use object_store::Attribute;

    let mut attrs = object_store::Attributes::new();
    if !cache_control.is_empty() {
        attrs.insert(Attribute::CacheControl, cache_control.to_string().into());
    }
    attrs.insert(
        Attribute::ContentType,
        "application/vnd.apache.parquet".into(),
    );
    object_store::PutOptions {
        attributes: attrs,
        ..Default::default()
    }
}

/// Returns `true` if the output path is an S3 URL.
pub fn is_s3_output(path: &Path) -> bool {
    path.to_string_lossy().starts_with("s3://")
}

/// Read a Parquet file and return all RecordBatches.
pub fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_writer_uses_output_bucket_and_rejects_mismatched_default() {
        let mut config = Config {
            output: "s3://data/mainnet".into(),
            s3_bucket: Some("data".into()),
            aws_access_key_id: Some("test-key".into()),
            aws_secret_access_key: Some("test-secret".into()),
            aws_region: Some("us-east-1".into()),
            ..Config::default()
        };
        let writer = ParquetTableWriter::new_s3(
            "s3://data/mainnet",
            Partition::None,
            Compression::Zstd,
            &config,
        )
        .unwrap();
        assert_eq!(writer.s3_client.unwrap().to_string(), "AmazonS3(data)");
        assert_eq!(writer.s3_prefix.as_deref(), Some("mainnet"));

        config.s3_bucket = Some("wrong-bucket".into());
        let error = ParquetTableWriter::new_s3(
            "s3://data/mainnet",
            Partition::None,
            Compression::Zstd,
            &config,
        )
        .err()
        .expect("bucket mismatch must fail");
        assert!(error
            .to_string()
            .contains("S3 output bucket `data` disagrees"));
    }
    use crate::config::{BlockMetadata, Compression, Partition};
    use anyhow::anyhow;
    use arrow::array::UInt64Builder;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_number",
            DataType::UInt64,
            false,
        )]));
        let mut builder = UInt64Builder::new();
        builder.append_value(42);
        RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap()
    }

    fn default_metadata() -> BlockMetadata {
        BlockMetadata {
            min_block_number: 0,
            max_block_number: 0,
            min_timestamp: None,
            max_timestamp: None,
        }
    }

    #[test]
    fn test_parquet_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer = ParquetTableWriter::new(dir.path(), Partition::None, Compression::Snappy);
        let meta = default_metadata();
        let (path, compressed_bytes) = writer.write_batch("blocks", &batch, &meta).unwrap();

        assert!(compressed_bytes > 0);
        let read_batches = read_parquet(&path).unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 1);
    }

    #[test]
    fn test_block_range_partitioning() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer =
            ParquetTableWriter::new(dir.path(), Partition::block_range(100), Compression::None);
        let meta = BlockMetadata {
            min_block_number: 150,
            max_block_number: 150,
            min_timestamp: None,
            max_timestamp: None,
        };
        let (path, _) = writer.write_batch("blocks", &batch, &meta).unwrap();
        assert!(path.to_string_lossy().contains("block_range=100-200"));
    }

    #[test]
    fn test_block_range_partitioning_uses_start_block_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut partition = Partition::block_range(100);
        partition.set_block_range_start(Some(9_820_210));
        let mut writer = ParquetTableWriter::new(dir.path(), partition, Compression::None);
        let meta = BlockMetadata {
            min_block_number: 9_820_250,
            max_block_number: 9_820_250,
            min_timestamp: None,
            max_timestamp: None,
        };

        let (path, _) = writer.write_batch("blocks", &batch, &meta).unwrap();
        assert!(path
            .to_string_lossy()
            .contains("block_range=9820210-9820310"));
    }

    #[test]
    fn test_date_partitioning() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer = ParquetTableWriter::new(dir.path(), Partition::Date, Compression::None);
        // 2024-01-15 12:00:00 UTC = 1705320000
        let meta = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705320000),
            max_timestamp: Some(1705320000),
        };
        let (path, _) = writer.write_batch("blocks", &batch, &meta).unwrap();
        assert!(
            path.to_string_lossy().contains("year=2024/month=01/day=15"),
            "path: {}",
            path.display()
        );
    }

    #[test]
    fn test_hour_partitioning() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer = ParquetTableWriter::new(dir.path(), Partition::Hour, Compression::None);
        // 2024-01-15 14:30:00 UTC = 1705329000
        let meta = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705329000),
            max_timestamp: Some(1705329000),
        };
        let (path, _) = writer.write_batch("blocks", &batch, &meta).unwrap();
        let path_str = path.to_string_lossy();
        assert!(
            path_str.contains("year=2024/month=01/day=15"),
            "path: {}",
            path_str
        );
        assert!(path_str.contains("hour=14"), "path: {}", path_str);
    }

    #[test]
    fn test_rollover_part_counter() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer = ParquetTableWriter::new(dir.path(), Partition::Date, Compression::None);
        let meta = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705320000),
            max_timestamp: Some(1705320000),
        };
        let (path1, _) = writer.write_batch("blocks", &batch, &meta).unwrap();
        let (path2, _) = writer.write_batch("blocks", &batch, &meta).unwrap();
        // File names now include a process UUID prefix: part-{uuid}-NNNNNN.parquet
        let name1 = path1.file_name().unwrap().to_string_lossy();
        let name2 = path2.file_name().unwrap().to_string_lossy();
        assert!(
            name1.starts_with("part-") && name1.ends_with("-000001.parquet"),
            "unexpected: {name1}"
        );
        assert!(
            name2.starts_with("part-") && name2.ends_with("-000002.parquet"),
            "unexpected: {name2}"
        );
        // Both should share the same process ID prefix.
        let prefix1 = &name1["part-".len()..name1.len() - "-000001.parquet".len()];
        let prefix2 = &name2["part-".len()..name2.len() - "-000002.parquet".len()];
        assert_eq!(
            prefix1, prefix2,
            "same writer should produce same process prefix"
        );
    }

    #[test]
    fn test_output_writer_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), batch);

        // flush_bytes=0 disables size-based rollover; data is written on flush_remaining().
        let mut out = OutputWriter::new(dir.path(), Partition::None, Compression::Zstd);
        let meta = default_metadata();
        out.write_all(&batches, &meta).unwrap();
        out.flush_remaining().unwrap();
        assert!(dir.path().join("blocks").exists());

        let parts: Vec<_> = std::fs::read_dir(dir.path().join("blocks"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
            .collect();
        assert_eq!(parts.len(), 1, "should be a single part file");
        let file_path = parts[0].path();
        let read_batches = read_parquet(&file_path).unwrap();
        let total_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 1,
            "should contain the single row from the batch"
        );
    }

    #[test]
    fn test_parse_s3_url_with_prefix() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket/some/prefix").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "some/prefix");
    }

    #[test]
    fn test_parse_s3_url_bucket_only() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_s3_url_trailing_slash() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket/path/").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path");
    }

    #[test]
    fn test_parse_s3_url_invalid() {
        assert!(parse_s3_url("http://not-s3").is_err());
        assert!(parse_s3_url("s3://").is_err());
    }

    #[test]
    fn test_with_root_cause_context_includes_distinct_root_cause() {
        let error = anyhow!("operation timed out")
            .context("error sending request")
            .context("HTTP error: error sending request");

        let error = with_root_cause_context("uploading to S3: path/file.parquet", error);

        assert_eq!(
            error.to_string(),
            "uploading to S3: path/file.parquet (root cause: operation timed out)"
        );
        let chain: Vec<_> = error.chain().map(ToString::to_string).collect();
        assert_eq!(chain[1], "HTTP error: error sending request");
        assert_eq!(chain.last().unwrap(), "operation timed out");
    }

    #[test]
    fn test_with_root_cause_context_avoids_duplicate_single_cause() {
        let error = with_root_cause_context(
            "uploading to S3: path/file.parquet",
            anyhow!("operation timed out"),
        );

        assert_eq!(error.to_string(), "uploading to S3: path/file.parquet");
        let chain: Vec<_> = error.chain().map(ToString::to_string).collect();
        assert_eq!(chain[1], "operation timed out");
    }

    #[test]
    fn test_is_s3_output() {
        assert!(is_s3_output(Path::new("s3://bucket/prefix")));
        assert!(!is_s3_output(Path::new("output")));
        assert!(!is_s3_output(Path::new("/tmp/local")));
    }

    fn parquet_rows_in(dir: &Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
            .flat_map(|entry| read_parquet(&entry.path()).unwrap())
            .map(|batch| batch.num_rows())
            .sum()
    }

    fn partition_batch(blocks: Vec<Option<u64>>, timestamps: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, true),
            Field::new(
                "timestamp",
                crate::traits::timestamp_millis_utc_type(),
                true,
            ),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::UInt64Array::from(blocks)),
                Arc::new(
                    arrow::array::TimestampMillisecondArray::from(timestamps).with_timezone("UTC"),
                ),
            ],
        )
        .unwrap()
    }

    fn routed_metadata(timestamp: Option<i64>) -> BlockMetadata {
        BlockMetadata {
            min_block_number: 500,
            max_block_number: 599,
            min_timestamp: timestamp,
            max_timestamp: timestamp,
        }
    }

    fn assert_rejected_without_publication(
        partition: Partition,
        batch: RecordBatch,
        metadata: BlockMetadata,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output");
        let mut writer = OutputWriter::new(&output, partition, Compression::None);
        let (_, metrics) = crate::metrics::init();
        writer.set_metrics(metrics.clone());
        assert!(writer
            .write_all(&HashMap::from([("blocks".into(), batch)]), &metadata)
            .is_err());
        assert!(!output.exists());
        assert_eq!(writer.buffered_stats(), WriterBufferStats::default());
        assert!(writer.inner.part_counters.is_empty());
        assert_eq!(metrics.buffer_estimated_bytes.get(), 0);
    }

    #[test]
    fn test_each_mapper_batch_materializes_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = OutputWriter::new(dir.path(), Partition::None, Compression::Zstd);
        let batches = HashMap::from([("blocks".into(), make_test_batch())]);
        for _ in 0..2 {
            assert!(writer.write_all(&batches, &default_metadata()).unwrap());
            assert_eq!(writer.buffered_stats(), WriterBufferStats::default());
            assert!(!writer.flush_remaining().unwrap());
        }
        assert_eq!(
            std::fs::read_dir(dir.path().join("blocks"))
                .unwrap()
                .count(),
            2
        );
        assert_eq!(parquet_rows_in(&dir.path().join("blocks")), 2);
    }

    #[test]
    fn metrics_keep_one_file_series_across_partitions_and_reset_writer_buffers() {
        let dir = tempfile::tempdir().unwrap();
        let (registry, metrics) = crate::metrics::init();
        let mut writer = OutputWriter::new(dir.path(), Partition::Minute, Compression::None);
        writer.set_metrics(metrics.clone());
        for offset in 0..3 {
            let timestamp = 1_705_320_000 + offset * 60;
            let batch = partition_batch(vec![Some(501)], vec![Some(timestamp * 1000)]);
            writer
                .write_all(
                    &HashMap::from([("blocks".into(), batch)]),
                    &routed_metadata(Some(timestamp)),
                )
                .unwrap();
            assert_eq!(metrics.buffer_estimated_bytes.get(), 0);
            assert_eq!(
                metrics
                    .buffer_rows
                    .get_or_create(&crate::metrics::TableLabels {
                        table: "blocks".into()
                    })
                    .get(),
                0
            );
        }
        let mut encoded = String::new();
        prometheus_client::encoding::text::encode(&mut encoded, &registry).unwrap();
        let series = encoded
            .lines()
            .filter(|line| line.starts_with("firehose_parquet_files_written_total{"))
            .collect::<Vec<_>>();
        assert_eq!(
            series,
            ["firehose_parquet_files_written_total{table=\"blocks\"} 3"]
        );
        assert!(!encoded.contains("partition="));
    }

    #[test]
    fn test_all_tables_are_preflighted_before_any_publication() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output");
        let timestamp = 1_705_320_000;
        let valid = partition_batch(vec![Some(501)], vec![Some(timestamp * 1_000)]);
        let invalid = partition_batch(
            vec![Some(501), Some(502), Some(503)],
            vec![
                Some(timestamp * 1_000),
                Some((timestamp + 172_800) * 1_000),
                Some(timestamp * 1_000),
            ],
        );
        let mut writer = OutputWriter::new(&output, Partition::Date, Compression::None);
        let error = writer
            .write_all(
                &HashMap::from([("a_valid".into(), valid), ("z_invalid".into(), invalid)]),
                &routed_metadata(Some(timestamp)),
            )
            .unwrap_err();
        assert!(error.to_string().contains("z_invalid"));
        assert!(
            !output.exists(),
            "an invalid later table must not leave an earlier file"
        );
        assert!(writer.inner.part_counters.is_empty());
        assert_eq!(writer.buffered_stats(), WriterBufferStats::default());
    }

    #[test]
    fn test_time_partition_validation_checks_all_rows_and_metadata_endpoints() {
        let timestamp = 1_705_320_000;
        for (partition, width) in [
            (Partition::Date, 86_400),
            (Partition::Hour, 3_600),
            (Partition::Minute, 60),
            (Partition::Second, 1),
        ] {
            let batch = partition_batch(
                vec![Some(501), Some(502), Some(503), Some(504)],
                vec![
                    Some(timestamp * 1_000),
                    Some((timestamp + width * 2) * 1_000),
                    Some((timestamp + width) * 1_000),
                    Some(timestamp * 1_000),
                ],
            );
            assert_rejected_without_publication(
                partition.clone(),
                batch,
                routed_metadata(Some(timestamp)),
            );
            let batch = partition_batch(vec![Some(501)], vec![Some(timestamp * 1_000)]);
            let mut metadata = routed_metadata(Some(timestamp));
            metadata.max_timestamp = Some(timestamp + width);
            assert_rejected_without_publication(partition, batch, metadata);
        }
    }

    #[test]
    fn test_time_partition_validation_allows_unordered_same_partition_and_null_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let timestamp = 1_705_320_000;
        let batch = partition_batch(
            vec![Some(503), Some(501), Some(502)],
            vec![
                Some((timestamp + 2) * 1_000),
                None,
                Some(timestamp * 1_000 + 999),
            ],
        );
        let mut writer = OutputWriter::new(dir.path(), Partition::Date, Compression::None);
        assert!(writer
            .write_all(
                &HashMap::from([("blocks".into(), batch.clone())]),
                &routed_metadata(Some(timestamp))
            )
            .unwrap());
        let path = std::fs::read_dir(dir.path().join("blocks/year=2024/month=01/day=15"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(read_parquet(&path).unwrap(), vec![batch]);
    }

    #[test]
    fn invalid_times_never_publish_files_or_advance_part_counters() {
        for partition in [
            Partition::Date,
            Partition::Hour,
            Partition::Minute,
            Partition::Second,
        ] {
            for timestamp in [i64::MIN, i64::MAX, 1_700_000_000_000] {
                let batch = partition_batch(vec![Some(501)], vec![Some(0)]);
                assert_rejected_without_publication(
                    partition.clone(),
                    batch.clone(),
                    routed_metadata(Some(timestamp)),
                );
                let mut metadata = routed_metadata(Some(0));
                metadata.max_timestamp = Some(timestamp);
                assert_rejected_without_publication(partition.clone(), batch.clone(), metadata);
                let dir = tempfile::tempdir().unwrap();
                let output = dir.path().join("output");
                let mut writer =
                    ParquetTableWriter::new(&output, partition.clone(), Compression::None);
                assert!(writer
                    .write_batch("blocks", &batch, &routed_metadata(Some(timestamp)))
                    .is_err());
                assert!(!output.exists());
                assert!(writer.part_counters.is_empty());
                assert!(writer
                    .s3_object_key("blocks", &routed_metadata(Some(timestamp)), "part.parquet")
                    .is_err());
            }
            let batch = partition_batch(vec![Some(501)], vec![Some(i64::MAX)]);
            assert_rejected_without_publication(partition, batch, routed_metadata(Some(0)));
        }
    }

    #[test]
    fn test_negative_milliseconds_use_floor_seconds_for_partition_membership() {
        let dir = tempfile::tempdir().unwrap();
        let batch = partition_batch(vec![Some(501), Some(502)], vec![Some(-1), Some(-999)]);
        let mut writer = OutputWriter::new(dir.path(), Partition::Second, Compression::None);
        assert!(writer
            .write_all(
                &HashMap::from([("blocks".into(), batch)]),
                &routed_metadata(Some(-1))
            )
            .unwrap());
        assert!(dir
            .path()
            .join("blocks/year=1969/month=12/day=31/hour=23/minute=59/second=59")
            .is_dir());
    }

    #[test]
    fn test_null_timestamp_routes_preserve_flat_and_anchored_destinations() {
        for timestamp in [None, Some(1_705_320_000)] {
            let dir = tempfile::tempdir().unwrap();
            let batch = partition_batch(vec![Some(501)], vec![None]);
            let metadata = routed_metadata(timestamp);
            let mut writer = OutputWriter::new(dir.path(), Partition::Date, Compression::None);
            let suffix = writer.inner.partition_suffix("blocks", &metadata).unwrap();
            assert!(writer
                .write_all(&HashMap::from([("blocks".into(), batch)]), &metadata)
                .unwrap());
            assert_eq!(parquet_rows_in(&dir.path().join(suffix)), 1);
        }
    }

    #[test]
    fn test_time_partition_rejects_missing_mistyped_or_unanchored_timestamps() {
        let timestamp = 1_705_320_000;
        assert_rejected_without_publication(
            Partition::Date,
            make_test_batch(),
            routed_metadata(Some(timestamp)),
        );
        let wrong = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(arrow::array::Int64Array::from(vec![timestamp]))],
        )
        .unwrap();
        assert_rejected_without_publication(
            Partition::Date,
            wrong,
            routed_metadata(Some(timestamp)),
        );
        let unzoned = arrow::array::TimestampMillisecondArray::from(vec![timestamp * 1_000]);
        let wrong = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                unzoned.data_type().clone(),
                false,
            )])),
            vec![Arc::new(unzoned)],
        )
        .unwrap();
        assert_rejected_without_publication(
            Partition::Date,
            wrong,
            routed_metadata(Some(timestamp)),
        );
        let batch = partition_batch(vec![Some(501)], vec![Some(timestamp * 1_000)]);
        assert_rejected_without_publication(Partition::Date, batch.clone(), routed_metadata(None));
        let mut metadata = routed_metadata(Some(timestamp));
        metadata.max_timestamp = None;
        assert_rejected_without_publication(Partition::Date, batch, metadata);
    }

    #[test]
    fn test_block_partition_checks_all_rows_and_anchor_without_timestamps() {
        let mut partition = Partition::block_range(100);
        partition.set_block_range_start(Some(500));
        for blocks in [
            vec![Some(501), Some(701), Some(601), Some(501)],
            vec![None],
            vec![Some(499)],
        ] {
            let len = blocks.len();
            assert_rejected_without_publication(
                partition.clone(),
                partition_batch(blocks, vec![None; len]),
                routed_metadata(None),
            );
        }
        assert_rejected_without_publication(
            partition.clone(),
            make_test_batch(),
            routed_metadata(None),
        );
        let valid = partition_batch(vec![Some(599), Some(501), Some(599)], vec![None; 3]);
        let mut metadata = routed_metadata(None);
        metadata.max_block_number = 600;
        assert_rejected_without_publication(partition.clone(), valid.clone(), metadata);
        let dir = tempfile::tempdir().unwrap();
        let mut writer = OutputWriter::new(dir.path(), partition, Compression::None);
        assert!(writer
            .write_all(
                &HashMap::from([("blocks".into(), valid)]),
                &routed_metadata(None)
            )
            .unwrap());
        assert_eq!(
            parquet_rows_in(&dir.path().join("blocks/block_range=500-600")),
            3
        );
    }

    #[test]
    fn test_invalid_numeric_configuration_is_an_error_before_path_formatting() {
        let batch = partition_batch(vec![Some(501)], vec![None]);
        assert_rejected_without_publication(
            Partition::block_range(0),
            batch.clone(),
            routed_metadata(None),
        );
        let mut partition = Partition::block_range(100);
        partition.set_block_range_start(Some(600));
        assert_rejected_without_publication(partition, batch, routed_metadata(None));
    }

    #[test]
    fn test_failed_and_unattempted_tables_stay_visible_until_explicit_retry() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("logs");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let mut writer = OutputWriter::new(dir.path(), Partition::None, Compression::None);
        let batches = HashMap::from([
            ("blocks".into(), make_test_batch()),
            ("logs".into(), make_test_batch()),
            ("transactions".into(), make_test_batch()),
        ]);
        assert!(writer.write_all(&batches, &default_metadata()).is_err());
        assert_eq!(parquet_rows_in(&dir.path().join("blocks")), 1);
        assert!(!dir.path().join("transactions").exists());
        let stats = writer.buffered_stats();
        assert_eq!(stats.tables, 2);
        assert_eq!(stats.batches, 2);
        assert_eq!(stats.rows, 2);
        assert!(stats.estimated_arrow_bytes > 0 && stats.estimated_compressed_bytes > 0);
        assert!(writer.flush_remaining().is_err());
        assert!(writer
            .write_all(&batches, &default_metadata())
            .unwrap_err()
            .to_string()
            .contains("retry flush_remaining"));
        assert_eq!(writer.buffered_stats(), stats);
        // Directory creation failed before publication, so this specific retry
        // is unambiguous. Post-publication failures require #468 recovery.
        std::fs::remove_file(blocker).unwrap();
        assert!(writer.flush_remaining().unwrap());
        assert!(!writer.flush_remaining().unwrap());
        assert_eq!(writer.buffered_stats(), WriterBufferStats::default());
        for table in ["blocks", "logs", "transactions"] {
            assert_eq!(parquet_rows_in(&dir.path().join(table)), 1);
        }
    }

    #[test]
    fn test_empty_mapper_flush_does_not_publish_or_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output");
        let mut writer = OutputWriter::new(&output, Partition::Date, Compression::None);
        assert!(!writer
            .write_all(&HashMap::new(), &default_metadata())
            .unwrap());
        let empty = make_test_batch().slice(0, 0);
        assert!(!writer
            .write_all(
                &HashMap::from([("blocks".into(), empty)]),
                &default_metadata()
            )
            .unwrap());
        assert_eq!(writer.buffered_stats(), WriterBufferStats::default());
        assert!(!output.exists());
    }

    #[test]
    fn test_metadata_merge() {
        let mut a = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1000),
            max_timestamp: Some(2000),
        };
        let b = BlockMetadata {
            min_block_number: 50,
            max_block_number: 300,
            min_timestamp: Some(500),
            max_timestamp: Some(2500),
        };
        a.merge(&b);
        assert_eq!(a.min_block_number, 50);
        assert_eq!(a.max_block_number, 300);
        assert_eq!(a.min_timestamp, Some(500));
        assert_eq!(a.max_timestamp, Some(2500));
    }
}
