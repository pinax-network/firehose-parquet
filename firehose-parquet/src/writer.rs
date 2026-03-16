use crate::config::{BlockMetadata, Compression, Config, Partition};
use crate::metrics::PipelineMetrics;
use anyhow::{Context, Result};
use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::info;
use uuid::Uuid;

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
        let (bucket, prefix) = parse_s3_url(output_path)?;

        let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);

        if let Some(ref key) = config.aws_access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(ref secret) = config.aws_secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(ref token) = config.aws_session_token {
            builder = builder.with_token(token);
        }
        if let Some(ref region) = config.aws_region {
            builder = builder.with_region(region);
        }
        if let Some(ref endpoint_url) = config.aws_endpoint_url {
            builder = builder.with_endpoint(endpoint_url);
        }

        let client = builder
            .build()
            .with_context(|| format!("building S3 client for bucket {bucket}"))?;

        Ok(Self {
            output_dir: PathBuf::from(output_path),
            partition,
            compression,
            part_counters: HashMap::new(),
            process_id: short_uuid(),
            s3_client: Some(Arc::new(client)),
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
                fs::create_dir_all(&dir)?;
            }
            return Ok((dir, 0));
        }

        let dir = self.partition_dir(table, metadata);

        let counter_key = dir.to_string_lossy().to_string();
        let counter = self.part_counters.entry(counter_key).or_insert(0);
        *counter += 1;
        let filename = format!("part-{}-{:06}.parquet", self.process_id, counter);
        let path = dir.join(&filename);

        let compressed_bytes;

        if let Some(ref s3) = self.s3_client {
            // Write to in-memory buffer, then upload to S3.
            let props = self.writer_properties();
            let mut buf = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
            writer.write(batch)?;
            writer.close()?;
            compressed_bytes = buf.len();

            let s3_key = self.s3_object_key(table, metadata, &filename);
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
            // Write to local filesystem.
            fs::create_dir_all(&dir).with_context(|| format!("creating dir {}", dir.display()))?;

            let file =
                File::create(&path).with_context(|| format!("creating file {}", path.display()))?;
            let props = self.writer_properties();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
            writer.write(batch)?;
            writer.close()?;
            compressed_bytes = fs::metadata(&path)?.len() as usize;

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
    fn s3_object_key(&self, table: &str, metadata: &BlockMetadata, filename: &str) -> String {
        let prefix = self.s3_prefix.as_deref().unwrap_or("");
        let partition_suffix = self.partition_suffix(table, metadata);
        if prefix.is_empty() {
            format!("{partition_suffix}/{filename}")
        } else {
            format!("{prefix}/{partition_suffix}/{filename}")
        }
    }

    /// Return the partition-relative path component (e.g. `blocks/year=2024/month=01/date=15`).
    pub fn partition_suffix(&self, table: &str, metadata: &BlockMetadata) -> String {
        match &self.partition {
            Partition::None => table.to_string(),
            Partition::BlockRange { .. } => {
                let (start, stop) = self
                    .partition
                    .block_range_bounds(metadata.min_block_number)
                    .expect("block-range partition should resolve bounds");
                format!("{table}/block_range={start}-{stop}")
            }
            Partition::Date => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    format!(
                        "{table}/year={:04}/month={:02}/date={:02}",
                        dt.year(),
                        dt.month() as u8,
                        dt.day()
                    )
                } else {
                    table.to_string()
                }
            }
            Partition::Hour => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    format!(
                        "{table}/year={:04}/month={:02}/date={:02}/hour={:02}",
                        dt.year(),
                        dt.month() as u8,
                        dt.day(),
                        dt.hour()
                    )
                } else {
                    table.to_string()
                }
            }
            Partition::Minute => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    format!(
                        "{table}/year={:04}/month={:02}/date={:02}/hour={:02}/minute={:02}",
                        dt.year(),
                        dt.month() as u8,
                        dt.day(),
                        dt.hour(),
                        dt.minute()
                    )
                } else {
                    table.to_string()
                }
            }
            Partition::Second => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    format!(
                        "{table}/year={:04}/month={:02}/date={:02}/hour={:02}/minute={:02}/second={:02}",
                        dt.year(),
                        dt.month() as u8,
                        dt.day(),
                        dt.hour(),
                        dt.minute(),
                        dt.second()
                    )
                } else {
                    table.to_string()
                }
            }
        }
    }

    fn partition_dir(&self, table: &str, metadata: &BlockMetadata) -> PathBuf {
        let base = self.output_dir.join(table);
        match &self.partition {
            Partition::None => base,
            Partition::BlockRange { .. } => {
                let (start, stop) = self
                    .partition
                    .block_range_bounds(metadata.min_block_number)
                    .expect("block-range partition should resolve bounds");
                base.join(format!("block_range={start}-{stop}"))
            }
            Partition::Date => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    base.join(format!("year={:04}", dt.year()))
                        .join(format!("month={:02}", dt.month() as u8))
                        .join(format!("date={:02}", dt.day()))
                } else {
                    base
                }
            }
            Partition::Hour => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    base.join(format!("year={:04}", dt.year()))
                        .join(format!("month={:02}", dt.month() as u8))
                        .join(format!("date={:02}", dt.day()))
                        .join(format!("hour={:02}", dt.hour()))
                } else {
                    base
                }
            }
            Partition::Minute => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    base.join(format!("year={:04}", dt.year()))
                        .join(format!("month={:02}", dt.month() as u8))
                        .join(format!("date={:02}", dt.day()))
                        .join(format!("hour={:02}", dt.hour()))
                        .join(format!("minute={:02}", dt.minute()))
                } else {
                    base
                }
            }
            Partition::Second => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    base.join(format!("year={:04}", dt.year()))
                        .join(format!("month={:02}", dt.month() as u8))
                        .join(format!("date={:02}", dt.day()))
                        .join(format!("hour={:02}", dt.hour()))
                        .join(format!("minute={:02}", dt.minute()))
                        .join(format!("second={:02}", dt.second()))
                } else {
                    base
                }
            }
        }
    }

    /// Set file-level metadata to embed in every Parquet file footer.
    pub fn set_file_metadata(&mut self, metadata: ParquetFileMetadata) {
        self.file_metadata = metadata;
    }

    fn writer_properties(&self) -> WriterProperties {
        use parquet::file::metadata::KeyValue;

        let compression = match self.compression {
            Compression::None => PqCompression::UNCOMPRESSED,
            Compression::Snappy => PqCompression::SNAPPY,
            Compression::Gzip => PqCompression::GZIP(Default::default()),
            Compression::Zstd => PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        };
        let mut builder = WriterProperties::builder().set_compression(compression);

        if !self.file_metadata.entries.is_empty() {
            let kvs: Vec<KeyValue> = self
                .file_metadata
                .entries
                .iter()
                .map(|(k, v)| KeyValue::new(k.clone(), Some(v.clone())))
                .collect();
            builder = builder.set_key_value_metadata(Some(kvs));
        }

        builder.build()
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

/// Fixed compression ratio (compressed/uncompressed) used for estimating
/// when the buffered data will reach the target file size. These are
/// hard-coded to keep file rollover deterministic: given identical input
/// and configuration, the writer will produce consistent rollover behavior.
fn compression_ratio(compression: &Compression) -> f64 {
    match compression {
        Compression::None => 0.50,   // Parquet encoding alone: ~2×
        Compression::Snappy => 0.25, // Parquet + Snappy: ~4×
        Compression::Gzip => 0.12,   // Parquet + Gzip: ~8×
        Compression::Zstd => 0.12,   // Parquet + Zstd: ~8×
    }
}

/// A batch that needs to be re-buffered after a global flush (partition change).
struct PendingBatch {
    table: String,
    batch: RecordBatch,
    metadata: BlockMetadata,
    partition_key: String,
}

/// Buffered state for a single output table.
struct TableBuffer {
    batches: Vec<RecordBatch>,
    /// Accumulated Arrow memory across all buffered batches.
    total_bytes: usize,
    /// Merged metadata across all contributing flushes.
    metadata: BlockMetadata,
    /// Partition suffix for this buffer (used to detect partition changes).
    partition_key: String,
}

/// High-level writer that buffers RecordBatches per table and writes to disk
/// when the estimated **compressed** size reaches `flush_bytes`. The
/// compression ratio is a fixed hard-coded value per codec to keep file
/// rollover deterministic.
///
/// When `flush_bytes` is `0`, size-based file rollover is disabled and data
/// is only flushed on partition changes or when `flush_remaining()` is called.
///
/// This prevents many tiny Parquet files for tables that have few rows per
/// block (e.g. `blocks`). Small batches are concatenated into a single large
/// RecordBatch before writing.
pub struct OutputWriter {
    pub inner: ParquetTableWriter,
    /// Per-table buffer for accumulating batches before writing.
    buffers: HashMap<String, TableBuffer>,
    /// Target **compressed** output file size in bytes. `0` disables size-based rollover.
    flush_bytes: u64,
    /// Fixed compression ratio (compressed_bytes / arrow_bytes).
    compression_ratio: f64,
    /// Batches that need to be re-buffered after a global flush (from partition changes).
    pending_after_flush: Vec<PendingBatch>,
    /// Optional pipeline metrics for Prometheus instrumentation.
    metrics: Option<PipelineMetrics>,
}

impl OutputWriter {
    pub fn new(
        output_dir: impl Into<PathBuf>,
        partition: Partition,
        compression: Compression,
        flush_bytes: u64,
    ) -> Self {
        let cr = compression_ratio(&compression);
        Self {
            inner: ParquetTableWriter::new(output_dir, partition, compression),
            buffers: HashMap::new(),
            flush_bytes,
            compression_ratio: cr,
            pending_after_flush: Vec::new(),
            metrics: None,
        }
    }

    /// Create a writer that uploads Parquet files to S3.
    pub fn new_s3(
        output_path: &str,
        partition: Partition,
        compression: Compression,
        config: &Config,
        flush_bytes: u64,
    ) -> Result<Self> {
        let cr = compression_ratio(&compression);
        Ok(Self {
            inner: ParquetTableWriter::new_s3(output_path, partition, compression, config)?,
            buffers: HashMap::new(),
            flush_bytes,
            compression_ratio: cr,
            pending_after_flush: Vec::new(),
            metrics: None,
        })
    }

    /// Set the pipeline metrics for Prometheus instrumentation.
    pub fn set_metrics(&mut self, metrics: PipelineMetrics) {
        self.metrics = Some(metrics);
    }

    /// Current observed compression ratio (compressed / uncompressed).
    pub fn compression_ratio(&self) -> f64 {
        self.compression_ratio
    }

    /// Split a RecordBatch into sub-batches when the metadata spans multiple
    /// time-based partitions. Each returned tuple contains the sub-batch and
    /// a BlockMetadata with timestamps scoped to that subset.
    ///
    /// For `Partition::None` and `Partition::BlockRange` no splitting is needed
    /// and the original batch + metadata are returned as-is.
    fn split_batch_by_partition(
        &self,
        table: &str,
        batch: &RecordBatch,
        metadata: &BlockMetadata,
    ) -> Result<Vec<(RecordBatch, BlockMetadata)>> {
        // Only time-based partitions need splitting.
        match &self.inner.partition {
            Partition::None | Partition::BlockRange { .. } => {
                return Ok(vec![(batch.clone(), metadata.clone())]);
            }
            _ => {}
        }

        // If timestamps are missing, no splitting possible.
        let (Some(min_ts), Some(max_ts)) = (metadata.min_timestamp, metadata.max_timestamp) else {
            return Ok(vec![(batch.clone(), metadata.clone())]);
        };

        // Quick check: if min and max land in the same partition, no split needed.
        let min_meta = BlockMetadata {
            min_timestamp: Some(min_ts),
            max_timestamp: Some(min_ts),
            ..*metadata
        };
        let max_meta = BlockMetadata {
            min_timestamp: Some(max_ts),
            max_timestamp: Some(max_ts),
            ..*metadata
        };
        if self.inner.partition_suffix(table, &min_meta)
            == self.inner.partition_suffix(table, &max_meta)
        {
            return Ok(vec![(batch.clone(), metadata.clone())]);
        }

        // Find the timestamp column.
        let ts_col_idx = batch
            .schema()
            .index_of("timestamp")
            .map_err(|_| anyhow::anyhow!("timestamp column not found in batch schema"))?;
        let ts_array = batch
            .column(ts_col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("timestamp column is not Int64"))?;

        // Compute the partition key for each row and group rows by partition.
        // Use BTreeMap so partitions are ordered by time.
        let mut partition_groups: std::collections::BTreeMap<String, Vec<usize>> =
            std::collections::BTreeMap::new();
        for i in 0..ts_array.len() {
            let ts = ts_array.value(i);
            let row_meta = BlockMetadata {
                min_timestamp: Some(ts),
                max_timestamp: Some(ts),
                ..*metadata
            };
            let key = self.inner.partition_suffix(table, &row_meta);
            partition_groups.entry(key).or_default().push(i);
        }

        // Build sub-batches for each partition group.
        let mut results = Vec::with_capacity(partition_groups.len());
        for (_key, indices) in partition_groups {
            // Compute the range of row indices — they should be contiguous since
            // data arrives sorted by time, but use individual indices to be safe.
            let row_min_ts = indices.iter().map(|&i| ts_array.value(i)).min().unwrap();
            let row_max_ts = indices.iter().map(|&i| ts_array.value(i)).max().unwrap();

            // Build sub-batch by slicing. If indices are contiguous we can use
            // RecordBatch::slice for efficiency.
            let first = indices[0];
            let last = *indices.last().unwrap();
            let sub_batch = if last - first + 1 == indices.len() {
                // Contiguous range — use zero-copy slice.
                batch.slice(first, indices.len())
            } else {
                // Non-contiguous — use take (rare, but safe).
                let idx_array = arrow::array::UInt32Array::from(
                    indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
                );
                let columns: Vec<Arc<dyn arrow::array::Array>> = batch
                    .columns()
                    .iter()
                    .map(|col| arrow::compute::take(col.as_ref(), &idx_array, None))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                RecordBatch::try_new(batch.schema(), columns)?
            };

            let sub_meta = BlockMetadata {
                min_block_number: metadata.min_block_number,
                max_block_number: metadata.max_block_number,
                min_timestamp: Some(row_min_ts),
                max_timestamp: Some(row_max_ts),
            };
            results.push((sub_batch, sub_meta));
        }

        Ok(results)
    }

    /// Write all table batches produced by a BlockMapper::flush().
    ///
    /// Small tables are buffered until the estimated compressed size reaches
    /// `flush_bytes`, or the partition changes. This prevents tiny files for
    /// low-row-count tables like `blocks`.
    ///
    /// When any table triggers a rollover (partition change or size threshold),
    /// **all** tables are flushed together so every table starts fresh for the
    /// next block range. This keeps file boundaries deterministic and aligned.
    ///
    /// Returns `true` if data was actually written to disk (any table flushed).
    pub fn write_all(
        &mut self,
        batches: &HashMap<String, RecordBatch>,
        metadata: &BlockMetadata,
    ) -> Result<bool> {
        let mut needs_global_flush = false;

        for (table, batch) in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            // Split batch if it spans multiple time-based partitions.
            let sub_batches = self.split_batch_by_partition(table, batch, metadata)?;
            for (sub_batch, sub_meta) in &sub_batches {
                if self.buffer_batch(table, sub_batch, sub_meta)? {
                    needs_global_flush = true;
                }
            }
        }

        // Update buffer gauge metrics.
        if let Some(ref m) = self.metrics {
            let mut total_estimated = 0u64;
            for (table, buf) in &self.buffers {
                let row_count: usize = buf.batches.iter().map(|b| b.num_rows()).sum();
                m.buffer_rows
                    .get_or_create(&crate::metrics::TableLabels {
                        table: table.clone(),
                    })
                    .set(row_count as i64);
                total_estimated += (buf.total_bytes as f64 * self.compression_ratio) as u64;
            }
            m.buffer_estimated_bytes.set(total_estimated as i64);
        }

        if needs_global_flush {
            self.flush_remaining()?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Buffer a single batch. Returns `true` if a global flush is needed
    /// (partition change or size threshold reached for this table).
    fn buffer_batch(
        &mut self,
        table: &str,
        batch: &RecordBatch,
        metadata: &BlockMetadata,
    ) -> Result<bool> {
        let partition_key = self.inner.partition_suffix(table, metadata);
        let batch_bytes = batch.get_array_memory_size();

        // Check if the partition changed for this table.
        let needs_partition_flush = self
            .buffers
            .get(table)
            .map_or(false, |buf| buf.partition_key != partition_key);

        // Add to buffer.
        {
            // If partition changed, we'll flush everything via the global flush,
            // but we still need to buffer the new batch under the new partition key.
            // First, mark that we need a flush (don't clear the old buffer yet).
            if needs_partition_flush {
                // The old data will be flushed by the caller via flush_remaining().
                // We don't add the new batch yet — it will be added after the flush.
            }

            if !needs_partition_flush {
                let buf = self
                    .buffers
                    .entry(table.to_string())
                    .or_insert_with(|| TableBuffer {
                        batches: Vec::new(),
                        total_bytes: 0,
                        metadata: metadata.clone(),
                        partition_key: partition_key.clone(),
                    });
                buf.batches.push(batch.clone());
                buf.total_bytes += batch_bytes;
                buf.metadata.merge(metadata);
            }
        }

        // Coalesce many small batches into one to keep memory compact.
        if !needs_partition_flush {
            if let Some(buf) = self.buffers.get_mut(table) {
                if buf.batches.len() >= 100 {
                    let schema = buf.batches[0].schema();
                    let merged = arrow::compute::concat_batches(&schema, &buf.batches)?;
                    buf.batches = vec![merged];
                }
            }
        }

        // Check if estimated compressed size reaches the target.
        let needs_size_flush = !needs_partition_flush
            && self.flush_bytes > 0
            && self.buffers.get(table).map_or(false, |buf| {
                (buf.total_bytes as f64 * self.compression_ratio) >= self.flush_bytes as f64
            });

        if needs_partition_flush || needs_size_flush {
            // If partition changed, we need to re-buffer the new batch after flush.
            if needs_partition_flush {
                // Store the pending batch info for re-buffering after flush.
                self.pending_after_flush.push(PendingBatch {
                    table: table.to_string(),
                    batch: batch.clone(),
                    metadata: metadata.clone(),
                    partition_key,
                });
            }
            return Ok(true);
        }

        Ok(false)
    }

    /// Concatenate and write all buffered batches for a single table.
    /// Returns `true` if data was written.
    fn flush_table(&mut self, table: &str) -> Result<bool> {
        let buf = match self.buffers.remove(table) {
            Some(buf) if !buf.batches.is_empty() => buf,
            _ => return Ok(false),
        };
        let schema = buf.batches[0].schema();
        let merged = arrow::compute::concat_batches(&schema, &buf.batches)?;
        let num_rows = merged.num_rows();
        let partition_key = buf.partition_key.clone();
        let (_path, compressed_bytes) = self.inner.write_batch(table, &merged, &buf.metadata)?;

        // Update Prometheus metrics if available.
        if let Some(ref m) = self.metrics {
            use crate::metrics::{TableLabels, TablePartitionLabels};
            m.files_written_total
                .get_or_create(&TablePartitionLabels {
                    table: table.to_string(),
                    partition: partition_key,
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
        }

        Ok(true)
    }

    /// Flush all remaining buffered data to disk, then re-buffer any
    /// pending batches from partition changes.
    ///
    /// Returns `true` if any data was written.
    pub fn flush_remaining(&mut self) -> Result<bool> {
        let tables: Vec<String> = self.buffers.keys().cloned().collect();
        let mut wrote = false;
        for table in tables {
            if self.flush_table(&table)? {
                wrote = true;
            }
        }

        // Re-buffer any batches that arrived during a partition change.
        let pending = std::mem::take(&mut self.pending_after_flush);
        for p in pending {
            let buf = self.buffers.entry(p.table).or_insert_with(|| TableBuffer {
                batches: Vec::new(),
                total_bytes: 0,
                metadata: p.metadata.clone(),
                partition_key: p.partition_key,
            });
            let batch_bytes = p.batch.get_array_memory_size();
            buf.batches.push(p.batch);
            buf.total_bytes += batch_bytes;
            buf.metadata.merge(&p.metadata);
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
            path.to_string_lossy()
                .contains("year=2024/month=01/date=15"),
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
            path_str.contains("year=2024/month=01/date=15"),
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
        let mut out = OutputWriter::new(dir.path(), Partition::None, Compression::Zstd, 0);
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

    #[test]
    fn test_buffered_writer_accumulates_small_batches() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), batch);

        // flush_bytes large enough that a tiny batch won't trigger a write.
        let mut out = OutputWriter::new(dir.path(), Partition::None, Compression::None, 1_000_000);
        let meta = default_metadata();

        // Write twice — both should be buffered, not written to disk yet.
        out.write_all(&batches, &meta).unwrap();
        out.write_all(&batches, &meta).unwrap();
        assert!(
            !dir.path().join("blocks").exists(),
            "should still be buffered"
        );

        // flush_remaining writes the concatenated data.
        out.flush_remaining().unwrap();
        assert!(
            dir.path().join("blocks").exists(),
            "should be written after flush"
        );

        // Verify the file has 2 rows (from the 2 batches).
        let parts: Vec<_> = std::fs::read_dir(dir.path().join("blocks"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
            .collect();
        assert_eq!(parts.len(), 1, "should be a single part file");

        let file_path = parts[0].path();
        let read_batches = read_parquet(&file_path).unwrap();
        let total_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "concatenated batch should have 2 rows");
    }

    #[test]
    fn test_buffered_writer_flushes_on_partition_change() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), batch);

        let mut out = OutputWriter::new(dir.path(), Partition::Date, Compression::None, 1_000_000);

        // First write: date=2024-01-15
        let meta1 = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705320000), // 2024-01-15 12:00:00 UTC
            max_timestamp: Some(1705320000),
        };
        out.write_all(&batches, &meta1).unwrap();

        // Second write: different date — should flush the first.
        let meta2 = BlockMetadata {
            min_block_number: 300,
            max_block_number: 400,
            min_timestamp: Some(1705406400), // 2024-01-16 12:00:00 UTC
            max_timestamp: Some(1705406400),
        };
        out.write_all(&batches, &meta2).unwrap();

        // The 2024-01-15 partition should have been written (partition change).
        let jan15 = dir.path().join("blocks/year=2024/month=01/date=15");
        assert!(
            jan15.exists(),
            "old partition should be flushed on date change"
        );

        // The 2024-01-16 data is still buffered.
        let jan16 = dir.path().join("blocks/year=2024/month=01/date=16");
        assert!(!jan16.exists(), "new partition should still be buffered");

        out.flush_remaining().unwrap();
        assert!(
            jan16.exists(),
            "new partition should be written after flush"
        );
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

    #[test]
    fn test_flush_bytes_zero_disables_size_rollover() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), batch);

        // flush_bytes=0 disables size-based rollover.
        let mut out = OutputWriter::new(dir.path(), Partition::None, Compression::None, 0);
        let meta = default_metadata();

        // Write many times — nothing should be flushed to disk.
        for _ in 0..50 {
            out.write_all(&batches, &meta).unwrap();
        }
        assert!(
            !dir.path().join("blocks").exists(),
            "size rollover should be disabled"
        );

        // Explicit flush writes the accumulated data.
        out.flush_remaining().unwrap();
        assert!(
            dir.path().join("blocks").exists(),
            "flush_remaining should write data"
        );

        // Should produce a single part file with all 50 rows.
        let parts: Vec<_> = std::fs::read_dir(dir.path().join("blocks"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
            .collect();
        assert_eq!(parts.len(), 1, "should be a single part file");
        let file_path = parts[0].path();
        let read_batches = read_parquet(&file_path).unwrap();
        let total_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 50);
    }

    /// Helper: build a batch with block_num and timestamp columns for partition split tests.
    fn make_timestamped_batch(timestamps: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));
        let mut block_builder = UInt64Builder::new();
        let mut ts_builder = arrow::array::Int64Builder::new();
        for (i, &ts) in timestamps.iter().enumerate() {
            block_builder.append_value(i as u64);
            ts_builder.append_value(ts);
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(block_builder.finish()),
                Arc::new(ts_builder.finish()),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_split_batch_across_date_boundary() {
        let dir = tempfile::tempdir().unwrap();
        // 2024-01-15 23:59:58, 23:59:59, 2024-01-16 00:00:00, 00:00:01
        let ts_before = 1705363198_i64; // 2024-01-15 23:59:58
        let ts_boundary = 1705363200_i64; // 2024-01-16 00:00:00
        let batch =
            make_timestamped_batch(&[ts_before, ts_before + 1, ts_boundary, ts_boundary + 1]);

        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), batch);

        let mut out = OutputWriter::new(dir.path(), Partition::Date, Compression::None, 0);
        let meta = BlockMetadata {
            min_block_number: 0,
            max_block_number: 3,
            min_timestamp: Some(ts_before),
            max_timestamp: Some(ts_boundary + 1),
        };
        out.write_all(&batches, &meta).unwrap();
        out.flush_remaining().unwrap();

        // Both date partitions should exist.
        let jan15 = dir.path().join("blocks/year=2024/month=01/date=15");
        let jan16 = dir.path().join("blocks/year=2024/month=01/date=16");
        assert!(jan15.exists(), "2024-01-15 partition should exist");
        assert!(jan16.exists(), "2024-01-16 partition should exist");

        // Check row counts.
        let parts15: Vec<_> = std::fs::read_dir(&jan15)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
            .collect();
        let rows15: usize = parts15
            .iter()
            .flat_map(|p| read_parquet(&p.path()).unwrap())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows15, 2, "jan15 should have 2 rows");

        let parts16: Vec<_> = std::fs::read_dir(&jan16)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
            .collect();
        let rows16: usize = parts16
            .iter()
            .flat_map(|p| read_parquet(&p.path()).unwrap())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows16, 2, "jan16 should have 2 rows");
    }

    #[test]
    fn test_split_batch_across_hour_boundary() {
        let dir = tempfile::tempdir().unwrap();
        // 2024-01-15 13:59:59 and 14:00:00
        let ts1 = 1705327199_i64; // 13:59:59
        let ts2 = 1705327200_i64; // 14:00:00
        let batch = make_timestamped_batch(&[ts1, ts2]);

        let mut batches = HashMap::new();
        batches.insert("events".to_string(), batch);

        let mut out = OutputWriter::new(dir.path(), Partition::Hour, Compression::None, 0);
        let meta = BlockMetadata {
            min_block_number: 0,
            max_block_number: 1,
            min_timestamp: Some(ts1),
            max_timestamp: Some(ts2),
        };
        out.write_all(&batches, &meta).unwrap();
        out.flush_remaining().unwrap();

        let h13 = dir.path().join("events/year=2024/month=01/date=15/hour=13");
        let h14 = dir.path().join("events/year=2024/month=01/date=15/hour=14");
        assert!(h13.exists(), "hour=13 should exist");
        assert!(h14.exists(), "hour=14 should exist");
    }

    #[test]
    fn test_no_split_when_same_partition() {
        let dir = tempfile::tempdir().unwrap();
        // Both timestamps in 2024-01-15
        let ts1 = 1705320000_i64;
        let ts2 = 1705320060_i64;
        let batch = make_timestamped_batch(&[ts1, ts2]);

        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), batch);

        let mut out = OutputWriter::new(dir.path(), Partition::Date, Compression::None, 0);
        let meta = BlockMetadata {
            min_block_number: 0,
            max_block_number: 1,
            min_timestamp: Some(ts1),
            max_timestamp: Some(ts2),
        };
        out.write_all(&batches, &meta).unwrap();
        out.flush_remaining().unwrap();

        let jan15 = dir.path().join("blocks/year=2024/month=01/date=15");
        assert!(jan15.exists());
        // Only one partition should exist (one year directory)
        let dirs: Vec<_> = std::fs::read_dir(dir.path().join("blocks"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map_or(false, |ft| ft.is_dir()))
            .collect();
        assert_eq!(dirs.len(), 1, "should only have one date partition");
    }

    #[test]
    fn test_no_split_for_block_range_partition() {
        let dir = tempfile::tempdir().unwrap();
        let ts1 = 1705363198_i64;
        let ts2 = 1705363200_i64;
        let batch = make_timestamped_batch(&[ts1, ts2]);

        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), batch);

        let mut out = OutputWriter::new(
            dir.path(),
            Partition::block_range(1000),
            Compression::None,
            0,
        );
        let meta = BlockMetadata {
            min_block_number: 100,
            max_block_number: 101,
            min_timestamp: Some(ts1),
            max_timestamp: Some(ts2),
        };
        out.write_all(&batches, &meta).unwrap();
        out.flush_remaining().unwrap();

        // Should write to a single block_range partition
        let br = dir.path().join("blocks/block_range=0-1000");
        assert!(br.exists());
        let parts: Vec<_> = std::fs::read_dir(&br)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
            .collect();
        let rows: usize = parts
            .iter()
            .flat_map(|p| read_parquet(&p.path()).unwrap())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 2, "all rows should be in one partition");
    }

    #[test]
    fn test_compression_ratio_is_fixed() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), batch);

        let mut out = OutputWriter::new(dir.path(), Partition::None, Compression::Zstd, 1_000_000);
        let initial_ratio = out.compression_ratio();
        let meta = default_metadata();

        // Write and flush — the ratio should remain unchanged.
        out.write_all(&batches, &meta).unwrap();
        out.flush_remaining().unwrap();
        assert_eq!(
            out.compression_ratio(),
            initial_ratio,
            "ratio should not change after writes"
        );
    }
}
