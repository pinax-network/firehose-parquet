//! Merge small parquet part files within each partition into larger files.
//!
//! Unlike `rollup` which changes partition granularity (minute→hour→day),
//! `merge` operates within each existing partition directory, consolidating
//! many small parts into fewer larger files.

use crate::cli::{block_on_async, format_bytes, AwsConfig};
use crate::config::Compression;
use crate::writer::s3_put_options;
use anyhow::{Context, Result};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

/// Configuration for a merge operation.
pub struct MergeConfig {
    pub path: String,
    pub compression: Compression,
    pub flush_bytes: u64,
    pub dry_run: bool,
    pub aws: Option<AwsConfig>,
    pub cache_control: String,
}

/// Summary of a merge operation.
pub struct MergeResult {
    pub partitions_merged: usize,
    pub partitions_skipped: usize,
    pub files_read: usize,
    pub files_written: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

impl MergeResult {
    pub fn print(&self) {
        println!();
        println!("  Partitions merged:  {}", self.partitions_merged);
        println!("  Partitions skipped: {}", self.partitions_skipped);
        println!("  Files read:         {}", self.files_read);
        println!("  Files written:      {}", self.files_written);
        println!("  Size before:        {}", format_bytes(self.bytes_before));
        println!("  Size after:         {}", format_bytes(self.bytes_after));
        let saved = self.bytes_before.saturating_sub(self.bytes_after);
        if saved > 0 {
            println!("  Space saved:        {}", format_bytes(saved));
        }
        println!();
    }
}

/// Run the merge operation.
pub fn run_merge(config: &MergeConfig) -> Result<MergeResult> {
    if config.path.starts_with("s3://") {
        run_merge_s3(config)
    } else {
        run_merge_local(config)
    }
}

// ---------------------------------------------------------------------------
// Local filesystem merge
// ---------------------------------------------------------------------------

fn run_merge_local(config: &MergeConfig) -> Result<MergeResult> {
    let root = PathBuf::from(&config.path);
    if !root.is_dir() {
        anyhow::bail!("path does not exist or is not a directory: {}", root.display());
    }

    // Group parquet files by their parent directory (partition).
    let mut all_files: Vec<PathBuf> = Vec::new();
    collect_parquet_files_recursive(&root, &mut all_files)?;
    all_files.sort();

    if all_files.is_empty() {
        info!("no parquet files found in {}", root.display());
        return Ok(MergeResult {
            partitions_merged: 0, partitions_skipped: 0,
            files_read: 0, files_written: 0,
            bytes_before: 0, bytes_after: 0,
        });
    }

    // Group by parent directory.
    let mut groups: BTreeMap<PathBuf, Vec<PathBuf>> = BTreeMap::new();
    for file in all_files {
        let parent = file.parent().unwrap_or(&root).to_path_buf();
        groups.entry(parent).or_default().push(file);
    }

    let mut result = MergeResult {
        partitions_merged: 0, partitions_skipped: 0,
        files_read: 0, files_written: 0,
        bytes_before: 0, bytes_after: 0,
    };

    println!("Merging partitions in {} ...\n", root.display());

    for (partition_dir, files) in &groups {
        let partition_name = partition_dir
            .strip_prefix(&root)
            .unwrap_or(partition_dir)
            .to_string_lossy()
            .to_string();
        let partition_label = if partition_name.is_empty() { "(root)".to_string() } else { partition_name };

        // Skip if only 1 file (nothing to merge).
        if files.len() <= 1 {
            result.partitions_skipped += 1;
            continue;
        }

        // Calculate source size.
        let source_bytes: u64 = files.iter()
            .filter_map(|f| std::fs::metadata(f).ok())
            .map(|m| m.len())
            .sum();
        result.bytes_before += source_bytes;

        // Read all batches.
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for file_path in files {
            let file = std::fs::File::open(file_path)
                .with_context(|| format!("opening {}", file_path.display()))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;
            for batch_result in reader {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    all_batches.push(batch);
                }
            }
        }
        result.files_read += files.len();

        if all_batches.is_empty() {
            result.partitions_skipped += 1;
            continue;
        }

        let schema = all_batches[0].schema();
        let merged = concat_batches(&schema, &all_batches)?;

        // Sort by block_num if present.
        let merged = sort_by_block_num(merged)?;

        if config.dry_run {
            let est_files = estimate_output_files(merged.num_rows(), &merged, config.flush_bytes);
            println!("  {}: {} parts → ~{} file(s) (dry run)", partition_label, files.len(), est_files);
            result.partitions_merged += 1;
            continue;
        }

        // Write merged data to temp files, then swap.
        let written_files = write_merged_batches(
            partition_dir,
            &merged,
            config.compression,
            config.flush_bytes,
        )?;

        let output_bytes: u64 = written_files.iter()
            .filter_map(|f| std::fs::metadata(f).ok())
            .map(|m| m.len())
            .sum();
        result.bytes_after += output_bytes;

        println!(
            "  {}: {} parts → {} file(s) ({})",
            partition_label,
            files.len(),
            written_files.len(),
            format_bytes(output_bytes),
        );

        // Delete original source files (only the ones not in written_files).
        for f in files {
            if !written_files.contains(f) {
                std::fs::remove_file(f)
                    .with_context(|| format!("deleting {}", f.display()))?;
            }
        }

        result.files_written += written_files.len();
        result.partitions_merged += 1;
    }

    Ok(result)
}

/// Write a merged RecordBatch to part files, splitting at flush_bytes.
/// Returns paths of written files.
fn write_merged_batches(
    out_dir: &Path,
    batch: &RecordBatch,
    compression: Compression,
    flush_bytes: u64,
) -> Result<Vec<PathBuf>> {
    let props = writer_properties(compression);
    let schema = batch.schema();
    let total_rows = batch.num_rows();

    if total_rows == 0 {
        return Ok(vec![]);
    }

    let mut written_files = Vec::new();
    let mut part_num = 0u32;
    let mut offset = 0usize;

    while offset < total_rows {
        part_num += 1;
        let filename = format!("part-{:06}.parquet", part_num);
        let path = out_dir.join(&filename);

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props.clone()))?;

        // Write rows until we exceed flush_bytes.
        let chunk_size = 10_000.min(total_rows - offset);
        let mut rows_in_file = 0usize;

        loop {
            let end = (offset + chunk_size).min(total_rows);
            let slice = batch.slice(offset, end - offset);
            writer.write(&slice)?;
            rows_in_file += end - offset;
            offset = end;

            // Check estimated compressed size.
            let est_bytes = writer.in_progress_size() as u64;
            if est_bytes >= flush_bytes || offset >= total_rows {
                break;
            }
        }

        writer.close()?;
        std::fs::write(&path, &buf)
            .with_context(|| format!("writing {}", path.display()))?;

        info!(
            path = %path.display(),
            rows = rows_in_file,
            bytes = buf.len(),
            "wrote merged part"
        );

        written_files.push(path);
    }

    Ok(written_files)
}

/// Sort a RecordBatch by `block_num` column if it exists.
fn sort_by_block_num(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    if schema.index_of("block_num").is_err() {
        return Ok(batch); // No block_num column, return as-is.
    }

    use arrow::compute::{sort_to_indices, take};
    let block_num_idx = schema.index_of("block_num").unwrap();
    let block_num_col = batch.column(block_num_idx);
    let indices = sort_to_indices(block_num_col, None, None)?;

    let columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().iter()
        .map(|col| take(col.as_ref(), &indices, None).map(Arc::from))
        .collect::<std::result::Result<_, _>>()?;

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn estimate_output_files(total_rows: usize, _batch: &RecordBatch, _flush_bytes: u64) -> usize {
    // Rough estimate — assume 1 file unless very large.
    if total_rows == 0 { 0 } else { 1 }
}

fn writer_properties(compression: Compression) -> WriterProperties {
    let pq_compression = match compression {
        Compression::None => PqCompression::UNCOMPRESSED,
        Compression::Snappy => PqCompression::SNAPPY,
        Compression::Gzip => PqCompression::GZIP(Default::default()),
        Compression::Zstd => PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap()),
    };
    WriterProperties::builder()
        .set_compression(pq_compression)
        .build()
}

fn collect_parquet_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
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

// ---------------------------------------------------------------------------
// S3 merge
// ---------------------------------------------------------------------------

fn run_merge_s3(config: &MergeConfig) -> Result<MergeResult> {
    use crate::writer::parse_s3_url;
    use futures::TryStreamExt;

    let (bucket, prefix) = parse_s3_url(&config.path)?;
    let aws = config.aws.as_ref().ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?;

    let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);
    if let Some(ref key) = aws.aws_access_key_id { builder = builder.with_access_key_id(key); }
    if let Some(ref secret) = aws.aws_secret_access_key { builder = builder.with_secret_access_key(secret); }
    if let Some(ref token) = aws.aws_session_token { builder = builder.with_token(token); }
    if let Some(ref region) = aws.aws_region { builder = builder.with_region(region); }
    if let Some(ref endpoint_url) = aws.aws_endpoint_url { builder = builder.with_endpoint(endpoint_url); }

    let client = Arc::new(builder.build()
        .map_err(|e| anyhow::anyhow!("building S3 client for bucket {bucket}: {e}"))?);

    let list_prefix = if prefix.is_empty() { None } else { Some(object_store::path::Path::from(prefix.as_str())) };

    let objects: Vec<object_store::ObjectMeta> = block_on_async(async {
        client.list(list_prefix.as_ref()).try_collect().await
    }).map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let mut parquet_objects: Vec<_> = objects.into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .collect();
    parquet_objects.sort_by(|a, b| a.location.cmp(&b.location));

    if parquet_objects.is_empty() {
        info!("no parquet files found in {}", config.path);
        return Ok(MergeResult {
            partitions_merged: 0, partitions_skipped: 0,
            files_read: 0, files_written: 0,
            bytes_before: 0, bytes_after: 0,
        });
    }

    // Group by parent "directory" in S3 key space.
    let mut groups: BTreeMap<String, Vec<object_store::ObjectMeta>> = BTreeMap::new();
    for obj in parquet_objects {
        let key = obj.location.as_ref();
        let parent = key.rsplit_once('/').map(|(p, _)| p.to_string()).unwrap_or_default();
        groups.entry(parent).or_default().push(obj);
    }

    let mut result = MergeResult {
        partitions_merged: 0, partitions_skipped: 0,
        files_read: 0, files_written: 0,
        bytes_before: 0, bytes_after: 0,
    };

    println!("Merging partitions in {} ...\n", config.path);

    for (partition_key, objects) in &groups {
        let partition_label = partition_key.strip_prefix(&prefix)
            .map(|s| s.trim_start_matches('/'))
            .unwrap_or(partition_key);
        let partition_label = if partition_label.is_empty() { "(root)" } else { partition_label };

        if objects.len() <= 1 {
            result.partitions_skipped += 1;
            continue;
        }

        let source_bytes: u64 = objects.iter().map(|o| o.size as u64).sum();
        result.bytes_before += source_bytes;

        // Read all batches.
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for obj in objects {
            let data = block_on_async(async {
                client.get(&obj.location).await?.bytes().await
            }).map_err(|e| anyhow::anyhow!("reading s3://{bucket}/{}: {e}", obj.location))?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
            let reader = builder.build()?;
            for batch_result in reader {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    all_batches.push(batch);
                }
            }
        }
        result.files_read += objects.len();

        if all_batches.is_empty() {
            result.partitions_skipped += 1;
            continue;
        }

        let schema = all_batches[0].schema();
        let merged = concat_batches(&schema, &all_batches)?;
        let merged = sort_by_block_num(merged)?;

        if config.dry_run {
            println!("  {}: {} parts → ~1 file(s) (dry run)", partition_label, objects.len());
            result.partitions_merged += 1;
            continue;
        }

        // Write merged data to S3.
        let props = writer_properties(config.compression);
        let total_rows = merged.num_rows();
        let mut part_num = 0u32;
        let mut offset = 0usize;
        let mut files_written = 0usize;
        let mut output_bytes = 0u64;

        while offset < total_rows {
            part_num += 1;
            let filename = format!("part-{:06}.parquet", part_num);
            let s3_key = format!("{}/{}", partition_key, filename);

            let mut buf = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props.clone()))?;
            let chunk_size = 10_000.min(total_rows - offset);

            loop {
                let end = (offset + chunk_size).min(total_rows);
                let slice = merged.slice(offset, end - offset);
                writer.write(&slice)?;
                offset = end;
                if writer.in_progress_size() as u64 >= config.flush_bytes || offset >= total_rows {
                    break;
                }
            }

            writer.close()?;
            output_bytes += buf.len() as u64;

            let s3_path = object_store::path::Path::from(s3_key.as_str());
            let payload = object_store::PutPayload::from(bytes::Bytes::from(buf));
            block_on_async(async {
                client.put_opts(&s3_path, payload, s3_put_options(&config.cache_control)).await
            }).map_err(|e| anyhow::anyhow!("uploading s3://{bucket}/{s3_key}: {e}"))?;

            files_written += 1;
        }

        println!(
            "  {}: {} parts → {} file(s) ({})",
            partition_label, objects.len(), files_written, format_bytes(output_bytes),
        );

        // Delete original objects.
        for obj in objects {
            // Don't delete if it matches a new file name.
            let obj_filename = obj.location.as_ref().rsplit_once('/').map(|(_, f)| f).unwrap_or("");
            let is_new = (1..=part_num).any(|n| format!("part-{:06}.parquet", n) == obj_filename);
            if !is_new {
                block_on_async(async {
                    client.delete(&obj.location).await
                }).map_err(|e| anyhow::anyhow!("deleting s3://{bucket}/{}: {e}", obj.location))?;
            }
        }

        result.bytes_after += output_bytes;
        result.files_written += files_written;
        result.partitions_merged += 1;
    }

    Ok(result)
}
