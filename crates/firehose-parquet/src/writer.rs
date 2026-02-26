use crate::config::{BlockMetadata, Compression, Config, Partition};
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::info;

/// Writes Arrow RecordBatches to Parquet files, handling partitioning and
/// file naming. Supports local filesystem and S3 output.
pub struct ParquetTableWriter {
    output_dir: PathBuf,
    partition: Partition,
    compression: Compression,
    /// Part counter per logical partition key (table + partition value).
    part_counters: HashMap<String, u32>,
    /// S3 object store client (set when output starts with `s3://`).
    s3_client: Option<Arc<dyn ObjectStore>>,
    /// S3 key prefix (bucket path after `s3://bucket/`).
    s3_prefix: Option<String>,
}

impl ParquetTableWriter {
    pub fn new(output_dir: impl Into<PathBuf>, partition: Partition, compression: Compression) -> Self {
        Self {
            output_dir: output_dir.into(),
            partition,
            compression,
            part_counters: HashMap::new(),
            s3_client: None,
            s3_prefix: None,
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

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&bucket);

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
            s3_client: Some(Arc::new(client)),
            s3_prefix: Some(prefix),
        })
    }

    /// Write a single RecordBatch for `table` to a new Parquet part file.
    pub fn write_batch(
        &mut self,
        table: &str,
        batch: &RecordBatch,
        metadata: &BlockMetadata,
    ) -> Result<PathBuf> {
        if batch.num_rows() == 0 {
            let dir = self.output_dir.join(table);
            if self.s3_client.is_none() {
                fs::create_dir_all(&dir)?;
            }
            return Ok(dir);
        }

        let dir = self.partition_dir(table, metadata);

        let counter_key = dir.to_string_lossy().to_string();
        let counter = self.part_counters.entry(counter_key).or_insert(0);
        *counter += 1;
        let filename = format!("part-{:06}.parquet", counter);
        let path = dir.join(&filename);

        if let Some(ref s3) = self.s3_client {
            // Write to in-memory buffer, then upload to S3.
            let props = self.writer_properties();
            let mut buf = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
            writer.write(batch)?;
            writer.close()?;

            let s3_key = self.s3_object_key(table, metadata, &filename);
            let s3_path = object_store::path::Path::from(s3_key.as_str());
            let s3_client = Arc::clone(s3);
            let payload = object_store::PutPayload::from(bytes::Bytes::from(buf));
            // block_in_place is needed because this sync writer is called from
            // within a tokio multi-threaded runtime (the gRPC stream handler).
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    s3_client.put(&s3_path, payload).await
                })
            })
            .with_context(|| format!("uploading to S3: {s3_key}"))?;

            info!(
                table,
                path = %s3_key,
                rows = batch.num_rows(),
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

            info!(
                table,
                path = %path.display(),
                rows = batch.num_rows(),
                "wrote parquet part"
            );
        }

        Ok(path)
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

    /// Return the partition-relative path component (e.g. `blocks/date=2024-01-15`).
    fn partition_suffix(&self, table: &str, metadata: &BlockMetadata) -> String {
        match &self.partition {
            Partition::None => table.to_string(),
            Partition::BlockRange(size) => {
                let start = (metadata.min_block_number / size) * size;
                let end = start + size - 1;
                format!("{table}/block_range={start}-{end}")
            }
            Partition::Date => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts).unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    format!("{table}/date={:04}-{:02}-{:02}", dt.year(), dt.month() as u8, dt.day())
                } else {
                    table.to_string()
                }
            }
            Partition::Hour => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts).unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    format!(
                        "{table}/date={:04}-{:02}-{:02}/hour={:02}",
                        dt.year(), dt.month() as u8, dt.day(), dt.hour()
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
            Partition::BlockRange(size) => {
                let start = (metadata.min_block_number / size) * size;
                let end = start + size - 1;
                base.join(format!("block_range={start}-{end}"))
            }
            Partition::Date => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts).unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    base.join(format!("date={:04}-{:02}-{:02}", dt.year(), dt.month() as u8, dt.day()))
                } else {
                    base
                }
            }
            Partition::Hour => {
                if let Some(ts) = metadata.min_timestamp {
                    let dt = OffsetDateTime::from_unix_timestamp(ts).unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    base.join(format!("date={:04}-{:02}-{:02}", dt.year(), dt.month() as u8, dt.day()))
                        .join(format!("hour={:02}", dt.hour()))
                } else {
                    base
                }
            }
        }
    }

    fn writer_properties(&self) -> WriterProperties {
        let compression = match self.compression {
            Compression::None => PqCompression::UNCOMPRESSED,
            Compression::Snappy => PqCompression::SNAPPY,
            Compression::Gzip => PqCompression::GZIP(Default::default()),
            Compression::Zstd => PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        };
        WriterProperties::builder()
            .set_compression(compression)
            .build()
    }
}

/// High-level writer that accepts a generic HashMap<String, RecordBatch>
/// produced by any BlockMapper implementation.
pub struct OutputWriter {
    pub inner: ParquetTableWriter,
}

impl OutputWriter {
    pub fn new(output_dir: impl Into<PathBuf>, partition: Partition, compression: Compression) -> Self {
        Self {
            inner: ParquetTableWriter::new(output_dir, partition, compression),
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
        })
    }

    /// Write all table batches produced by a BlockMapper::flush().
    pub fn write_all(
        &mut self,
        batches: &HashMap<String, RecordBatch>,
        metadata: &BlockMetadata,
    ) -> Result<()> {
        for (table, batch) in batches {
            self.inner.write_batch(table, batch, metadata)?;
        }
        Ok(())
    }
}

/// Parse an S3 URL into (bucket, prefix).
///
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
    use arrow::array::UInt64Builder;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
        ]));
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
        let mut writer =
            ParquetTableWriter::new(dir.path(), Partition::None, Compression::Snappy);
        let meta = default_metadata();
        let path = writer.write_batch("blocks", &batch, &meta).unwrap();

        let read_batches = read_parquet(&path).unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 1);
    }

    #[test]
    fn test_block_range_partitioning() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer =
            ParquetTableWriter::new(dir.path(), Partition::BlockRange(100), Compression::None);
        let meta = BlockMetadata {
            min_block_number: 150,
            max_block_number: 150,
            min_timestamp: None,
            max_timestamp: None,
        };
        let path = writer.write_batch("blocks", &batch, &meta).unwrap();
        assert!(path.to_string_lossy().contains("block_range=100-199"));
    }

    #[test]
    fn test_date_partitioning() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer =
            ParquetTableWriter::new(dir.path(), Partition::Date, Compression::None);
        // 2024-01-15 12:00:00 UTC = 1705320000
        let meta = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705320000),
            max_timestamp: Some(1705320000),
        };
        let path = writer.write_batch("blocks", &batch, &meta).unwrap();
        assert!(path.to_string_lossy().contains("date=2024-01-15"), "path: {}", path.display());
    }

    #[test]
    fn test_hour_partitioning() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer =
            ParquetTableWriter::new(dir.path(), Partition::Hour, Compression::None);
        // 2024-01-15 14:30:00 UTC = 1705329000
        let meta = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705329000),
            max_timestamp: Some(1705329000),
        };
        let path = writer.write_batch("blocks", &batch, &meta).unwrap();
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("date=2024-01-15"), "path: {}", path_str);
        assert!(path_str.contains("hour=14"), "path: {}", path_str);
    }

    #[test]
    fn test_rollover_part_counter() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer =
            ParquetTableWriter::new(dir.path(), Partition::Date, Compression::None);
        let meta = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705320000),
            max_timestamp: Some(1705320000),
        };
        let path1 = writer.write_batch("blocks", &batch, &meta).unwrap();
        let path2 = writer.write_batch("blocks", &batch, &meta).unwrap();
        assert!(path1.to_string_lossy().contains("part-000001"));
        assert!(path2.to_string_lossy().contains("part-000002"));
    }

    #[test]
    fn test_output_writer_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), batch);

        let mut out = OutputWriter::new(dir.path(), Partition::None, Compression::Zstd);
        let meta = default_metadata();
        out.write_all(&batches, &meta).unwrap();
        assert!(dir.path().join("blocks").exists());
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
    fn test_is_s3_output() {
        assert!(is_s3_output(Path::new("s3://bucket/prefix")));
        assert!(!is_s3_output(Path::new("output")));
        assert!(!is_s3_output(Path::new("/tmp/local")));
    }
}
