//! Roll up fine-grained partitioned Parquet files into coarser intervals.
//!
//! Reads minute- or hour-partitioned files and merges them into hourly or daily
//! partitions, respecting a compressed file-size limit (`flush_bytes`).

use crate::cli::{format_bytes, AwsConfig};
use crate::config::Compression;
use anyhow::{Context, Result};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::basic::ZstdLevel;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

/// Target partition granularity for rollup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupTarget {
    /// Merge into hourly partitions: `table/year=YYYY/month=MM/date=DD/hour=HH/`
    Hour,
    /// Merge into daily partitions: `table/year=YYYY/month=MM/date=DD/`
    Date,
}

/// Parse a target partition string.
pub fn parse_rollup_target(s: &str) -> Result<RollupTarget> {
    match s.to_lowercase().as_str() {
        "hour" | "hourly" => Ok(RollupTarget::Hour),
        "date" | "daily" | "day" => Ok(RollupTarget::Date),
        other => anyhow::bail!(
            "invalid --target-partition '{other}': expected one of: hour, date"
        ),
    }
}

/// Configuration for a rollup operation.
pub struct RollupConfig {
    pub source: String,
    pub output: String,
    pub target: RollupTarget,
    pub compression: Compression,
    pub flush_bytes: u64,
    pub delete_source: bool,
    pub aws: Option<AwsConfig>,
    pub cache_control: String,
}

/// Run the rollup operation.
pub fn run_rollup(config: &RollupConfig) -> Result<()> {
    if config.source.starts_with("s3://") || config.output.starts_with("s3://") {
        run_rollup_s3(config)
    } else {
        run_rollup_local(config)
    }
}

// ---------------------------------------------------------------------------
// Local filesystem rollup
// ---------------------------------------------------------------------------

fn run_rollup_local(config: &RollupConfig) -> Result<()> {
    let source = PathBuf::from(&config.source);
    if !source.is_dir() {
        anyhow::bail!("source path does not exist or is not a directory: {}", source.display());
    }

    let output = PathBuf::from(&config.output);

    // Discover all .parquet files under source.
    let mut files: Vec<PathBuf> = Vec::new();
    collect_parquet_files_recursive(&source, &mut files)?;
    files.sort();

    if files.is_empty() {
        info!("no parquet files found in {}", source.display());
        return Ok(());
    }

    info!(files = files.len(), source = %source.display(), "discovered parquet files");

    // Group files by (table, target_partition_key).
    // The key is the output directory relative to the output root.
    let groups = group_files_by_target(&source, &files, config.target)?;

    let mut total_input_files = 0usize;
    let mut total_output_files = 0usize;
    let mut total_rows = 0usize;
    let mut source_files_to_delete: Vec<PathBuf> = Vec::new();

    for (group_key, group_files) in &groups {
        info!(
            group = %group_key,
            files = group_files.len(),
            "processing group"
        );

        // Read all batches from all files in this group.
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
        for file_path in group_files {
            let file = std::fs::File::open(file_path)
                .with_context(|| format!("opening {}", file_path.display()))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            if file_kv_metadata.is_none() {
                file_kv_metadata = builder.metadata().file_metadata().key_value_metadata().cloned();
            }
            let reader = builder.build()?;
            for batch_result in reader {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    all_batches.push(batch);
                }
            }
        }

        if all_batches.is_empty() {
            info!(group = %group_key, "no rows, skipping");
            continue;
        }

        let schema = all_batches[0].schema();
        let merged = concat_batches(&schema, &all_batches)?;
        let rows = merged.num_rows();
        total_rows += rows;
        total_input_files += group_files.len();

        // Write merged data, splitting by flush_bytes.
        let out_dir = output.join(group_key);
        std::fs::create_dir_all(&out_dir)
            .with_context(|| format!("creating output dir {}", out_dir.display()))?;

        let written = write_merged_batches_local(
            &out_dir,
            &merged,
            config.compression,
            config.flush_bytes,
            file_kv_metadata.as_deref(),
        )?;
        total_output_files += written;

        if config.delete_source {
            source_files_to_delete.extend(group_files.iter().cloned());
        }
    }

    // Delete source files after all groups are successfully written.
    if config.delete_source && !source_files_to_delete.is_empty() {
        info!(files = source_files_to_delete.len(), "deleting source files");
        for f in &source_files_to_delete {
            std::fs::remove_file(f)
                .with_context(|| format!("deleting source file {}", f.display()))?;
        }
        // Clean up empty directories.
        cleanup_empty_dirs(&PathBuf::from(&config.source))?;
    }

    info!(
        input_files = total_input_files,
        output_files = total_output_files,
        total_rows,
        "rollup complete"
    );

    Ok(())
}

/// Write a merged RecordBatch to one or more part files, splitting when
/// estimated compressed size exceeds `flush_bytes`.
///
/// Returns the number of files written.
fn write_merged_batches_local(
    out_dir: &Path,
    batch: &RecordBatch,
    compression: Compression,
    flush_bytes: u64,
    kv_metadata: Option<&[KeyValue]>,
) -> Result<usize> {
    let props = writer_properties(compression, kv_metadata);

    if flush_bytes == 0 {
        // No size limit — write everything to a single file.
        let path = out_dir.join("part-000001.parquet");
        let file = std::fs::File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;
        let size = std::fs::metadata(&path)?.len();
        info!(path = %path.display(), rows = batch.num_rows(), size = %format_bytes(size), "wrote merged file");
        return Ok(1);
    }

    // Strategy: write rows in slices. After each slice, check the file size.
    // If it exceeds flush_bytes, close the file and start a new one.
    //
    // We use a simple approach: write the full batch to a buffer first to get
    // the actual compressed size, then decide if we need to split.
    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props.clone()))?;
        writer.write(batch)?;
        writer.close()?;
    }

    if (buf.len() as u64) <= flush_bytes {
        // Fits in a single file.
        let path = out_dir.join("part-000001.parquet");
        std::fs::write(&path, &buf)?;
        info!(path = %path.display(), rows = batch.num_rows(), size = %format_bytes(buf.len() as u64), "wrote merged file");
        return Ok(1);
    }

    // Need to split. Estimate how many rows per file.
    let bytes_per_row = buf.len() as f64 / batch.num_rows() as f64;
    let rows_per_file = ((flush_bytes as f64 / bytes_per_row) as usize).max(1);
    let total_rows = batch.num_rows();
    let mut offset = 0usize;
    let mut part = 0u32;

    while offset < total_rows {
        let end = (offset + rows_per_file).min(total_rows);
        let slice = batch.slice(offset, end - offset);
        part += 1;
        let path = out_dir.join(format!("part-{:06}.parquet", part));
        let file = std::fs::File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props.clone()))?;
        writer.write(&slice)?;
        writer.close()?;
        let size = std::fs::metadata(&path)?.len();
        info!(
            path = %path.display(),
            rows = slice.num_rows(),
            size = %format_bytes(size),
            "wrote merged part"
        );
        offset = end;
    }

    Ok(part as usize)
}

/// Group source files by their target (coarser) partition key.
///
/// Returns a map from output relative path (e.g. `blocks/year=2024/month=01/date=15`)
/// to the list of source files that belong to that group.
fn group_files_by_target(
    source_root: &Path,
    files: &[PathBuf],
    target: RollupTarget,
) -> Result<BTreeMap<String, Vec<PathBuf>>> {
    let mut groups: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();

    for file in files {
        let rel = file
            .strip_prefix(source_root)
            .unwrap_or(file)
            .to_string_lossy()
            .to_string();

        let key = compute_group_key(&rel, target);
        groups.entry(key).or_default().push(file.clone());
    }

    Ok(groups)
}

/// Compute the group key for a relative file path given the target partition.
///
/// Examples (target=Date):
///   `blocks/year=2024/month=01/date=15/hour=14/minute=30/part-000001.parquet`
///   → `blocks/year=2024/month=01/date=15`
///
/// Examples (target=Hour):
///   `blocks/year=2024/month=01/date=15/hour=14/minute=30/part-000001.parquet`
///   → `blocks/year=2024/month=01/date=15/hour=14`
///
/// Files without recognized partition components keep everything except the filename.
fn compute_group_key(rel_path: &str, target: RollupTarget) -> String {
    // Split into path components.
    let parts: Vec<&str> = rel_path.split('/').collect();

    // Find the components we want to keep based on target.
    let mut kept: Vec<&str> = Vec::new();

    for part in &parts {
        // Skip the filename (last component with .parquet extension).
        if part.ends_with(".parquet") {
            continue;
        }

        match target {
            RollupTarget::Date => {
                // Keep table name and date= component, drop hour=/minute=/second=.
                if part.starts_with("hour=") || part.starts_with("minute=") || part.starts_with("second=") {
                    continue;
                }
                kept.push(part);
            }
            RollupTarget::Hour => {
                // Keep table name, date=, hour=. Drop minute=/second=.
                if part.starts_with("minute=") || part.starts_with("second=") {
                    continue;
                }
                kept.push(part);
            }
        }
    }

    if kept.is_empty() {
        // Fallback: use directory of the file.
        let p = Path::new(rel_path);
        p.parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default()
    } else {
        kept.join("/")
    }
}

/// Recursively collect `.parquet` files from a directory.
fn collect_parquet_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files_recursive(&path, out)?;
        } else if path.extension().map_or(false, |ext| ext == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}

/// Remove empty directories recursively (bottom-up).
fn cleanup_empty_dirs(dir: &Path) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            cleanup_empty_dirs(&path)?;
        }
    }
    // Try to remove — will fail if not empty, which is fine.
    let _ = std::fs::remove_dir(dir);
    Ok(())
}

fn writer_properties(compression: Compression, kv_metadata: Option<&[KeyValue]>) -> WriterProperties {
    let pq_compression = match compression {
        Compression::None => PqCompression::UNCOMPRESSED,
        Compression::Snappy => PqCompression::SNAPPY,
        Compression::Gzip => PqCompression::GZIP(Default::default()),
        Compression::Zstd => PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap()),
    };
    let mut builder = WriterProperties::builder()
        .set_compression(pq_compression);
    if let Some(kvs) = kv_metadata {
        if !kvs.is_empty() {
            builder = builder.set_key_value_metadata(Some(kvs.to_vec()));
        }
    }
    builder.build()
}

// ---------------------------------------------------------------------------
// S3 rollup
// ---------------------------------------------------------------------------

fn run_rollup_s3(config: &RollupConfig) -> Result<()> {
    use crate::writer::parse_s3_url;

    use crate::cli::block_on_async;

    let aws = config.aws.as_ref()
        .ok_or_else(|| anyhow::anyhow!("AWS config required for S3 rollup"))?;

    // Build S3 client for source.
    let (src_bucket, src_prefix) = parse_s3_url(&config.source)?;
    let src_client = build_s3_client(&src_bucket, aws)?;

    // Build S3 client for output (may be same bucket).
    let (out_bucket, out_prefix) = parse_s3_url(&config.output)?;
    let out_client = if out_bucket == src_bucket {
        Arc::clone(&src_client)
    } else {
        build_s3_client(&out_bucket, aws)?
    };

    // List all .parquet objects under source prefix.
    let objects: Vec<object_store::ObjectMeta> = block_on_async(async {
        use futures::TryStreamExt;
        let prefix = if src_prefix.is_empty() {
            None
        } else {
            Some(object_store::path::Path::from(src_prefix.as_str()))
        };
        src_client.list(prefix.as_ref()).try_collect().await
    }).map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let mut parquet_keys: Vec<String> = objects
        .iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .map(|obj| obj.location.as_ref().to_string())
        .collect();
    parquet_keys.sort();

    if parquet_keys.is_empty() {
        info!("no parquet files found in {}", config.source);
        return Ok(());
    }

    info!(files = parquet_keys.len(), source = %config.source, "discovered parquet files on S3");

    // Group by target partition. Strip source prefix for relative paths.
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for key in &parquet_keys {
        let rel = key.strip_prefix(&src_prefix)
            .map(|s| s.trim_start_matches('/'))
            .unwrap_or(key);
        let group_key = compute_group_key(rel, config.target);
        groups.entry(group_key).or_default().push(key.clone());
    }

    let mut total_input_files = 0usize;
    let mut total_output_files = 0usize;
    let mut total_rows = 0usize;
    let mut source_keys_to_delete: Vec<String> = Vec::new();

    for (group_key, group_keys) in &groups {
        info!(group = %group_key, files = group_keys.len(), "processing group");

        // Read all batches and extract file-level metadata from the first file.
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
        for s3_key in group_keys {
            let data = block_on_async(async {
                let path = object_store::path::Path::from(s3_key.as_str());
                src_client.get(&path).await?.bytes().await
            }).map_err(|e| anyhow::anyhow!("reading s3://{src_bucket}/{s3_key}: {e}"))?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
            if file_kv_metadata.is_none() {
                file_kv_metadata = builder.metadata().file_metadata().key_value_metadata().cloned();
            }
            let reader = builder.build()?;
            for batch_result in reader {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    all_batches.push(batch);
                }
            }
        }

        if all_batches.is_empty() {
            info!(group = %group_key, "no rows, skipping");
            continue;
        }

        let schema = all_batches[0].schema();
        let merged = concat_batches(&schema, &all_batches)?;
        let rows = merged.num_rows();
        total_rows += rows;
        total_input_files += group_keys.len();

        // Write merged data to S3.
        let written = write_merged_batches_s3(
            &out_client,
            &out_bucket,
            &out_prefix,
            group_key,
            &merged,
            config.compression,
            config.flush_bytes,
            &config.cache_control,
            file_kv_metadata.as_deref(),
        )?;
        total_output_files += written;

        if config.delete_source {
            source_keys_to_delete.extend(group_keys.iter().cloned());
        }
    }

    // Delete source objects.
    if config.delete_source && !source_keys_to_delete.is_empty() {
        info!(files = source_keys_to_delete.len(), "deleting source files from S3");
        for key in &source_keys_to_delete {
            block_on_async(async {
                let path = object_store::path::Path::from(key.as_str());
                src_client.delete(&path).await
            }).map_err(|e| anyhow::anyhow!("deleting s3://{src_bucket}/{key}: {e}"))?;
        }
    }

    info!(
        input_files = total_input_files,
        output_files = total_output_files,
        total_rows,
        "rollup complete"
    );

    Ok(())
}

fn write_merged_batches_s3(
    client: &Arc<dyn ObjectStore>,
    bucket: &str,
    prefix: &str,
    group_key: &str,
    batch: &RecordBatch,
    compression: Compression,
    flush_bytes: u64,
    cache_control: &str,
    kv_metadata: Option<&[KeyValue]>,
) -> Result<usize> {
    let props = writer_properties(compression, kv_metadata);

    // Write full batch to buffer to check size.
    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props.clone()))?;
        writer.write(batch)?;
        writer.close()?;
    }

    let make_key = |part: u32| -> String {
        let filename = format!("part-{:06}.parquet", part);
        if prefix.is_empty() {
            format!("{group_key}/{filename}")
        } else {
            format!("{prefix}/{group_key}/{filename}")
        }
    };

    let upload = |key: &str, data: Vec<u8>| -> Result<()> {
        use crate::cli::block_on_async;
        let size = data.len();
        let path = object_store::path::Path::from(key);
        let payload = object_store::PutPayload::from(bytes::Bytes::from(data));
        let client = Arc::clone(client);
        let cc = cache_control.to_string();
        block_on_async(async { client.put_opts(&path, payload, crate::writer::s3_put_options(&cc)).await })
            .map_err(|e| anyhow::anyhow!("uploading s3://{bucket}/{key}: {e}"))?;
        info!(path = %key, size = %format_bytes(size as u64), "wrote merged file to S3");
        Ok(())
    };

    if flush_bytes == 0 || (buf.len() as u64) <= flush_bytes {
        let key = make_key(1);
        info!(rows = batch.num_rows(), "single file fits");
        upload(&key, buf)?;
        return Ok(1);
    }

    // Need to split by estimated rows per file.
    let bytes_per_row = buf.len() as f64 / batch.num_rows() as f64;
    let rows_per_file = ((flush_bytes as f64 / bytes_per_row) as usize).max(1);
    let total_rows = batch.num_rows();
    let mut offset = 0usize;
    let mut part = 0u32;

    while offset < total_rows {
        let end = (offset + rows_per_file).min(total_rows);
        let slice = batch.slice(offset, end - offset);
        part += 1;

        let mut part_buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut part_buf, batch.schema(), Some(props.clone()))?;
        writer.write(&slice)?;
        writer.close()?;

        let key = make_key(part);
        info!(rows = slice.num_rows(), "writing split part");
        upload(&key, part_buf)?;
        offset = end;
    }

    Ok(part as usize)
}

fn build_s3_client(bucket: &str, aws: &AwsConfig) -> Result<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
    if let Some(ref key) = aws.aws_access_key_id {
        builder = builder.with_access_key_id(key);
    }
    if let Some(ref secret) = aws.aws_secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }
    if let Some(ref token) = aws.aws_session_token {
        builder = builder.with_token(token);
    }
    if let Some(ref region) = aws.aws_region {
        builder = builder.with_region(region);
    }
    if let Some(ref endpoint) = aws.aws_endpoint_url {
        builder = builder.with_endpoint(endpoint);
    }
    Ok(Arc::new(builder.build()?))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Builder;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_parse_rollup_target() {
        assert_eq!(parse_rollup_target("hour").unwrap(), RollupTarget::Hour);
        assert_eq!(parse_rollup_target("hourly").unwrap(), RollupTarget::Hour);
        assert_eq!(parse_rollup_target("date").unwrap(), RollupTarget::Date);
        assert_eq!(parse_rollup_target("daily").unwrap(), RollupTarget::Date);
        assert_eq!(parse_rollup_target("day").unwrap(), RollupTarget::Date);
        assert!(parse_rollup_target("minute").is_err());
        assert!(parse_rollup_target("unknown").is_err());
    }

    #[test]
    fn test_compute_group_key_date() {
        assert_eq!(
            compute_group_key("blocks/year=2024/month=01/date=15/hour=14/minute=30/part-000001.parquet", RollupTarget::Date),
            "blocks/year=2024/month=01/date=15"
        );
        assert_eq!(
            compute_group_key("blocks/year=2024/month=01/date=15/hour=14/part-000001.parquet", RollupTarget::Date),
            "blocks/year=2024/month=01/date=15"
        );
        assert_eq!(
            compute_group_key("blocks/year=2024/month=01/date=15/part-000001.parquet", RollupTarget::Date),
            "blocks/year=2024/month=01/date=15"
        );
    }

    #[test]
    fn test_compute_group_key_hour() {
        assert_eq!(
            compute_group_key("blocks/year=2024/month=01/date=15/hour=14/minute=30/part-000001.parquet", RollupTarget::Hour),
            "blocks/year=2024/month=01/date=15/hour=14"
        );
        assert_eq!(
            compute_group_key("blocks/year=2024/month=01/date=15/hour=14/part-000001.parquet", RollupTarget::Hour),
            "blocks/year=2024/month=01/date=15/hour=14"
        );
    }

    #[test]
    fn test_compute_group_key_no_partition() {
        // Files without partition dirs — keep the directory path.
        assert_eq!(
            compute_group_key("blocks/part-000001.parquet", RollupTarget::Date),
            "blocks"
        );
    }

    fn make_test_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
        ]));
        let mut builder = UInt64Builder::new();
        for i in 0..rows {
            builder.append_value(i as u64);
        }
        RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap()
    }

    #[test]
    fn test_rollup_local_basic() {
        let source = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        // Create minute-partitioned source files.
        let dir1 = source.path().join("blocks/date=2024-01-15/hour=14/minute=30");
        let dir2 = source.path().join("blocks/date=2024-01-15/hour=14/minute=31");
        std::fs::create_dir_all(&dir1).unwrap();
        std::fs::create_dir_all(&dir2).unwrap();

        let batch1 = make_test_batch(10);
        let batch2 = make_test_batch(20);

        write_test_parquet(&dir1.join("part-000001.parquet"), &batch1);
        write_test_parquet(&dir2.join("part-000001.parquet"), &batch2);

        let config = RollupConfig {
            source: source.path().to_string_lossy().to_string(),
            output: output.path().to_string_lossy().to_string(),
            target: RollupTarget::Hour,
            compression: Compression::None,
            flush_bytes: 0,
            delete_source: false,
            aws: None,
            cache_control: String::new(),
        };

        run_rollup(&config).unwrap();

        // Should produce a single merged file under hour=14.
        let out_dir = output.path().join("blocks/date=2024-01-15/hour=14");
        assert!(out_dir.exists());

        let mut out_files = Vec::new();
        collect_parquet_files_recursive(&out_dir, &mut out_files).unwrap();
        assert_eq!(out_files.len(), 1);

        // Verify row count.
        let batches = crate::writer::read_parquet(&out_files[0]).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 30);
    }

    #[test]
    fn test_rollup_local_date_target() {
        let source = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        // Create hour-partitioned source files across 2 hours.
        let dir1 = source.path().join("blocks/date=2024-01-15/hour=14");
        let dir2 = source.path().join("blocks/date=2024-01-15/hour=15");
        std::fs::create_dir_all(&dir1).unwrap();
        std::fs::create_dir_all(&dir2).unwrap();

        write_test_parquet(&dir1.join("part-000001.parquet"), &make_test_batch(5));
        write_test_parquet(&dir2.join("part-000001.parquet"), &make_test_batch(7));

        let config = RollupConfig {
            source: source.path().to_string_lossy().to_string(),
            output: output.path().to_string_lossy().to_string(),
            target: RollupTarget::Date,
            compression: Compression::Zstd,
            flush_bytes: 0,
            delete_source: false,
            aws: None,
            cache_control: String::new(),
        };

        run_rollup(&config).unwrap();

        let out_dir = output.path().join("blocks/date=2024-01-15");
        assert!(out_dir.exists());

        let mut out_files = Vec::new();
        collect_parquet_files_recursive(&out_dir, &mut out_files).unwrap();
        assert_eq!(out_files.len(), 1);

        let batches = crate::writer::read_parquet(&out_files[0]).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 12);
    }

    #[test]
    fn test_rollup_local_flush_bytes_split() {
        let source = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        // Create a source file with enough rows that splitting should occur.
        let dir = source.path().join("blocks/date=2024-01-15/hour=14/minute=00");
        std::fs::create_dir_all(&dir).unwrap();
        write_test_parquet(&dir.join("part-000001.parquet"), &make_test_batch(10000));

        let config = RollupConfig {
            source: source.path().to_string_lossy().to_string(),
            output: output.path().to_string_lossy().to_string(),
            target: RollupTarget::Hour,
            compression: Compression::None,
            flush_bytes: 1024, // Very small — should force splitting.
            delete_source: false,
            aws: None,
            cache_control: String::new(),
        };

        run_rollup(&config).unwrap();

        let out_dir = output.path().join("blocks/date=2024-01-15/hour=14");
        let mut out_files = Vec::new();
        collect_parquet_files_recursive(&out_dir, &mut out_files).unwrap();
        assert!(out_files.len() > 1, "should have split into multiple files, got {}", out_files.len());

        // Total rows should still be 10000.
        let mut total = 0;
        for f in &out_files {
            let batches = crate::writer::read_parquet(f).unwrap();
            total += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(total, 10000);
    }

    #[test]
    fn test_rollup_local_delete_source() {
        let source = tempfile::tempdir().unwrap();

        let dir = source.path().join("blocks/date=2024-01-15/hour=14/minute=30");
        std::fs::create_dir_all(&dir).unwrap();
        write_test_parquet(&dir.join("part-000001.parquet"), &make_test_batch(5));

        let config = RollupConfig {
            source: source.path().to_string_lossy().to_string(),
            output: source.path().to_string_lossy().to_string(), // In-place.
            target: RollupTarget::Hour,
            compression: Compression::None,
            flush_bytes: 0,
            delete_source: true,
            aws: None,
            cache_control: String::new(),
        };

        run_rollup(&config).unwrap();

        // Original minute dir should be cleaned up.
        assert!(!dir.join("part-000001.parquet").exists());
    }

    fn write_test_parquet(path: &Path, batch: &RecordBatch) {
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    fn write_test_parquet_with_metadata(path: &Path, batch: &RecordBatch, kvs: Vec<KeyValue>) {
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(kvs))
            .build();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    fn read_parquet_kv_metadata(path: &Path) -> Option<Vec<KeyValue>> {
        let file = std::fs::File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        builder.metadata().file_metadata().key_value_metadata().cloned()
    }

    #[test]
    fn test_rollup_preserves_metadata() {
        let source = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let dir1 = source.path().join("blocks/date=2024-01-15/hour=14/minute=30");
        let dir2 = source.path().join("blocks/date=2024-01-15/hour=14/minute=31");
        std::fs::create_dir_all(&dir1).unwrap();
        std::fs::create_dir_all(&dir2).unwrap();

        let kvs = vec![
            KeyValue::new("firehose-parquet.version".to_string(), Some("0.1.0".to_string())),
            KeyValue::new("firehose-parquet.chain_name".to_string(), Some("eth".to_string())),
        ];

        write_test_parquet_with_metadata(&dir1.join("part-000001.parquet"), &make_test_batch(10), kvs.clone());
        write_test_parquet_with_metadata(&dir2.join("part-000001.parquet"), &make_test_batch(20), kvs.clone());

        let config = RollupConfig {
            source: source.path().to_string_lossy().to_string(),
            output: output.path().to_string_lossy().to_string(),
            target: RollupTarget::Hour,
            compression: Compression::None,
            flush_bytes: 0,
            delete_source: false,
            aws: None,
            cache_control: String::new(),
        };

        run_rollup(&config).unwrap();

        let out_dir = output.path().join("blocks/date=2024-01-15/hour=14");
        let mut out_files = Vec::new();
        collect_parquet_files_recursive(&out_dir, &mut out_files).unwrap();
        assert_eq!(out_files.len(), 1);

        // Verify metadata is preserved.
        let out_kvs = read_parquet_kv_metadata(&out_files[0]).expect("metadata should be present");
        let find = |key: &str| out_kvs.iter().find(|kv| kv.key == key).and_then(|kv| kv.value.clone());
        assert_eq!(find("firehose-parquet.version"), Some("0.1.0".to_string()));
        assert_eq!(find("firehose-parquet.chain_name"), Some("eth".to_string()));
    }

    #[test]
    fn test_rollup_flush_bytes_split_preserves_metadata() {
        let source = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let dir = source.path().join("blocks/date=2024-01-15/hour=14/minute=00");
        std::fs::create_dir_all(&dir).unwrap();

        let kvs = vec![
            KeyValue::new("firehose-parquet.version".to_string(), Some("0.1.0".to_string())),
        ];

        write_test_parquet_with_metadata(&dir.join("part-000001.parquet"), &make_test_batch(10000), kvs);

        let config = RollupConfig {
            source: source.path().to_string_lossy().to_string(),
            output: output.path().to_string_lossy().to_string(),
            target: RollupTarget::Hour,
            compression: Compression::None,
            flush_bytes: 1024,
            delete_source: false,
            aws: None,
            cache_control: String::new(),
        };

        run_rollup(&config).unwrap();

        let out_dir = output.path().join("blocks/date=2024-01-15/hour=14");
        let mut out_files = Vec::new();
        collect_parquet_files_recursive(&out_dir, &mut out_files).unwrap();
        assert!(out_files.len() > 1, "should have split into multiple files");

        // Verify all output files have metadata.
        for f in &out_files {
            let out_kvs = read_parquet_kv_metadata(f).expect("metadata should be present");
            let find = |key: &str| out_kvs.iter().find(|kv| kv.key == key).and_then(|kv| kv.value.clone());
            assert_eq!(find("firehose-parquet.version"), Some("0.1.0".to_string()));
        }
    }
}
