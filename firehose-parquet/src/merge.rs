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
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::basic::ZstdLevel;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const S3_READ_MAX_ATTEMPTS: usize = 5;
const S3_READ_RETRY_BASE_DELAY_MS: u64 = 100;

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
        anyhow::bail!(
            "path does not exist or is not a directory: {}",
            root.display()
        );
    }

    // Group parquet files by their parent directory (partition).
    let mut all_files: Vec<PathBuf> = Vec::new();
    collect_parquet_files_recursive(&root, &mut all_files)?;
    all_files.sort();

    if all_files.is_empty() {
        info!("no parquet files found in {}", root.display());
        return Ok(MergeResult {
            partitions_merged: 0,
            partitions_skipped: 0,
            files_read: 0,
            files_written: 0,
            bytes_before: 0,
            bytes_after: 0,
        });
    }

    let mut result = MergeResult {
        partitions_merged: 0,
        partitions_skipped: 0,
        files_read: 0,
        files_written: 0,
        bytes_before: 0,
        bytes_after: 0,
    };

    println!("Merging partitions in {} ...\n", root.display());

    let mut current_table: Option<String> = None;
    let mut current_partition: Option<PathBuf> = None;
    let mut current_files: Vec<PathBuf> = Vec::new();

    for file in all_files {
        let parent = file.parent().unwrap_or(&root).to_path_buf();
        match &current_partition {
            Some(partition) if partition == &parent => current_files.push(file),
            Some(_) => {
                let partition = current_partition.take().expect("partition must exist");
                print_local_table_header(&root, &partition, &mut current_table);
                process_local_partition(&root, &partition, &current_files, config, &mut result)?;
                current_partition = Some(parent);
                current_files = vec![file];
            }
            None => {
                current_partition = Some(parent);
                current_files.push(file);
            }
        }
    }

    if let Some(partition) = current_partition {
        print_local_table_header(&root, &partition, &mut current_table);
        process_local_partition(&root, &partition, &current_files, config, &mut result)?;
    }

    Ok(result)
}

fn process_local_partition(
    root: &Path,
    partition_dir: &Path,
    files: &[PathBuf],
    config: &MergeConfig,
    result: &mut MergeResult,
) -> Result<()> {
    let partition_name = partition_dir
        .strip_prefix(root)
        .unwrap_or(partition_dir)
        .to_string_lossy()
        .to_string();
    let partition_label = if partition_name.is_empty() {
        "(root)".to_string()
    } else {
        partition_name
    };

    if files.len() <= 1 {
        result.partitions_skipped += 1;
        return Ok(());
    }

    let source_bytes: u64 = files
        .iter()
        .filter_map(|f| std::fs::metadata(f).ok())
        .map(|m| m.len())
        .sum();
    result.bytes_before += source_bytes;

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
    for file_path in files {
        let file = std::fs::File::open(file_path)
            .with_context(|| format!("opening {}", file_path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        if file_kv_metadata.is_none() {
            file_kv_metadata = builder
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .cloned();
        }
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
        return Ok(());
    }

    let schema = all_batches[0].schema();
    let merged = concat_batches(&schema, &all_batches)?;
    let merged = sort_by_block_num(merged)?;

    if config.dry_run {
        let est_files = estimate_output_files(merged.num_rows(), &merged, config.flush_bytes);
        println!(
            "  {}: {} parts → ~{} file(s) (dry run)",
            partition_label,
            files.len(),
            est_files
        );
        result.partitions_merged += 1;
        return Ok(());
    }

    let written_files = write_merged_batches(
        partition_dir,
        &merged,
        config.compression,
        config.flush_bytes,
        file_kv_metadata.as_deref(),
    )?;

    let output_bytes: u64 = written_files
        .iter()
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

    for f in files {
        if !written_files.contains(f) {
            std::fs::remove_file(f).with_context(|| format!("deleting {}", f.display()))?;
        }
    }

    result.files_written += written_files.len();
    result.partitions_merged += 1;
    Ok(())
}

fn print_local_table_header(root: &Path, partition_dir: &Path, current_table: &mut Option<String>) {
    let rel = partition_dir.strip_prefix(root).unwrap_or(partition_dir);
    let table = rel
        .components()
        .next()
        .map(|part| part.as_os_str().to_string_lossy().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "(root)".to_string());

    if current_table.as_ref() != Some(&table) {
        println!("Table: {}", table);
        *current_table = Some(table);
    }
}

/// Write a merged RecordBatch to part files, splitting at flush_bytes.
/// Returns paths of written files.
fn write_merged_batches(
    out_dir: &Path,
    batch: &RecordBatch,
    compression: Compression,
    flush_bytes: u64,
    kv_metadata: Option<&[KeyValue]>,
) -> Result<Vec<PathBuf>> {
    let props = writer_properties(compression, kv_metadata);
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
        std::fs::write(&path, &buf).with_context(|| format!("writing {}", path.display()))?;

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

    let columns: Vec<Arc<dyn arrow::array::Array>> = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).map(Arc::from))
        .collect::<std::result::Result<_, _>>()?;

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn estimate_output_files(total_rows: usize, _batch: &RecordBatch, _flush_bytes: u64) -> usize {
    // Rough estimate — assume 1 file unless very large.
    if total_rows == 0 {
        0
    } else {
        1
    }
}

fn writer_properties(
    compression: Compression,
    kv_metadata: Option<&[KeyValue]>,
) -> WriterProperties {
    let pq_compression = match compression {
        Compression::None => PqCompression::UNCOMPRESSED,
        Compression::Snappy => PqCompression::SNAPPY,
        Compression::Gzip => PqCompression::GZIP(Default::default()),
        Compression::Zstd => PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap()),
    };
    let mut builder = WriterProperties::builder().set_compression(pq_compression);
    if let Some(kvs) = kv_metadata {
        if !kvs.is_empty() {
            builder = builder.set_key_value_metadata(Some(kvs.to_vec()));
        }
    }
    builder.build()
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
    let aws = config
        .aws
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?;

    let client = Arc::new(aws.build_s3_client(&bucket)?);

    let list_prefix = if prefix.is_empty() {
        None
    } else {
        Some(object_store::path::Path::from(prefix.as_str()))
    };

    let objects: Vec<object_store::ObjectMeta> =
        block_on_async(async { client.list(list_prefix.as_ref()).try_collect().await })
            .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let mut parquet_objects: Vec<_> = objects
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .collect();
    parquet_objects.sort_by(|a, b| a.location.cmp(&b.location));

    if parquet_objects.is_empty() {
        info!("no parquet files found in {}", config.path);
        return Ok(MergeResult {
            partitions_merged: 0,
            partitions_skipped: 0,
            files_read: 0,
            files_written: 0,
            bytes_before: 0,
            bytes_after: 0,
        });
    }

    let mut result = MergeResult {
        partitions_merged: 0,
        partitions_skipped: 0,
        files_read: 0,
        files_written: 0,
        bytes_before: 0,
        bytes_after: 0,
    };

    println!("Merging partitions in {} ...\n", config.path);

    let mut current_table: Option<String> = None;
    let mut current_partition: Option<String> = None;
    let mut current_objects: Vec<object_store::ObjectMeta> = Vec::new();

    for obj in parquet_objects {
        let key = obj.location.as_ref();
        let parent = key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default();

        match &current_partition {
            Some(partition) if partition == &parent => current_objects.push(obj),
            Some(_) => {
                let partition = current_partition.take().expect("partition must exist");
                print_s3_table_header(&prefix, &partition, &mut current_table);
                process_s3_partition(
                    &bucket,
                    &prefix,
                    &partition,
                    &current_objects,
                    &client,
                    config,
                    &mut result,
                )?;
                current_partition = Some(parent);
                current_objects = vec![obj];
            }
            None => {
                current_partition = Some(parent);
                current_objects.push(obj);
            }
        }
    }

    if let Some(partition) = current_partition {
        print_s3_table_header(&prefix, &partition, &mut current_table);
        process_s3_partition(
            &bucket,
            &prefix,
            &partition,
            &current_objects,
            &client,
            config,
            &mut result,
        )?;
    }

    Ok(result)
}

fn process_s3_partition(
    bucket: &str,
    prefix: &str,
    partition_key: &str,
    objects: &[object_store::ObjectMeta],
    client: &Arc<object_store::aws::AmazonS3>,
    config: &MergeConfig,
    result: &mut MergeResult,
) -> Result<()> {
    let partition_label = partition_key
        .strip_prefix(prefix)
        .map(|s| s.trim_start_matches('/'))
        .unwrap_or(partition_key);
    let partition_label = if partition_label.is_empty() {
        "(root)"
    } else {
        partition_label
    };
    let table = partition_label
        .split('/')
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or("(root)");

    if objects.len() <= 1 {
        result.partitions_skipped += 1;
        return Ok(());
    }

    let source_bytes: u64 = objects.iter().map(|o| o.size as u64).sum();
    result.bytes_before += source_bytes;

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
    for obj in objects {
        let data = read_s3_bytes_with_retry(
            client,
            bucket,
            &obj.location,
            table,
            partition_label,
            S3_READ_MAX_ATTEMPTS,
        )?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        if file_kv_metadata.is_none() {
            file_kv_metadata = builder
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .cloned();
        }
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
        return Ok(());
    }

    let schema = all_batches[0].schema();
    let merged = concat_batches(&schema, &all_batches)?;
    let merged = sort_by_block_num(merged)?;

    if config.dry_run {
        println!(
            "  {}: {} parts → ~1 file(s) (dry run)",
            partition_label,
            objects.len()
        );
        result.partitions_merged += 1;
        return Ok(());
    }

    let props = writer_properties(config.compression, file_kv_metadata.as_deref());
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
            client
                .put_opts(&s3_path, payload, s3_put_options(&config.cache_control))
                .await
        })
        .map_err(|e| anyhow::anyhow!("uploading s3://{bucket}/{s3_key}: {e}"))?;

        files_written += 1;
    }

    println!(
        "  {}: {} parts → {} file(s) ({})",
        partition_label,
        objects.len(),
        files_written,
        format_bytes(output_bytes),
    );

    for obj in objects {
        let obj_filename = obj
            .location
            .as_ref()
            .rsplit_once('/')
            .map(|(_, f)| f)
            .unwrap_or("");
        let is_new = (1..=part_num).any(|n| format!("part-{:06}.parquet", n) == obj_filename);
        if !is_new {
            block_on_async(async { client.delete(&obj.location).await })
                .map_err(|e| anyhow::anyhow!("deleting s3://{bucket}/{}: {e}", obj.location))?;
        }
    }

    result.bytes_after += output_bytes;
    result.files_written += files_written;
    result.partitions_merged += 1;
    Ok(())
}

fn read_s3_bytes_with_retry(
    client: &Arc<object_store::aws::AmazonS3>,
    bucket: &str,
    location: &object_store::path::Path,
    table: &str,
    partition_label: &str,
    max_attempts: usize,
) -> Result<bytes::Bytes> {
    let mut attempt = 0usize;

    loop {
        attempt += 1;
        match block_on_async(async { client.get(location).await?.bytes().await }) {
            Ok(data) => return Ok(data),
            Err(error) => {
                if attempt >= max_attempts {
                    return Err(anyhow::anyhow!(
                        "failed reading s3://{bucket}/{location} after {attempt} attempts (table={table}, partition={partition_label}): {error}"
                    ));
                }

                let retry_in = Duration::from_millis(
                    S3_READ_RETRY_BASE_DELAY_MS * 2u64.pow((attempt - 1) as u32),
                );

                warn!(
                    s3_key = %location,
                    table,
                    partition = partition_label,
                    attempt,
                    max_attempts,
                    retry_in_ms = retry_in.as_millis(),
                    error = %error,
                    "failed reading S3 object during merge; retrying"
                );

                std::thread::sleep(retry_in);
            }
        }
    }
}

fn print_s3_table_header(prefix: &str, partition_key: &str, current_table: &mut Option<String>) {
    let partition_relative = partition_key
        .strip_prefix(prefix)
        .map(|s| s.trim_start_matches('/'))
        .unwrap_or(partition_key);
    let table = partition_relative
        .split('/')
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or("(root)")
        .to_string();

    if current_table.as_ref() != Some(&table) {
        println!("Table: {}", table);
        *current_table = Some(table);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Builder;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_test_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_number",
            DataType::UInt64,
            false,
        )]));
        let mut builder = UInt64Builder::new();
        for i in 0..rows {
            builder.append_value(i as u64);
        }
        RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap()
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
        builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .cloned()
    }

    #[test]
    fn test_merge_preserves_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let partition = dir.path().join("blocks/year=2024/month=01/date=15");
        std::fs::create_dir_all(&partition).unwrap();

        let kvs = vec![
            KeyValue::new(
                "firehose-parquet.version".to_string(),
                Some("0.1.0".to_string()),
            ),
            KeyValue::new(
                "firehose-parquet.chain_name".to_string(),
                Some("eth".to_string()),
            ),
        ];

        write_test_parquet_with_metadata(
            &partition.join("part-000001.parquet"),
            &make_test_batch(10),
            kvs.clone(),
        );
        write_test_parquet_with_metadata(
            &partition.join("part-000002.parquet"),
            &make_test_batch(20),
            kvs.clone(),
        );

        let config = MergeConfig {
            path: dir.path().to_string_lossy().to_string(),
            compression: Compression::None,
            flush_bytes: 0,
            dry_run: false,
            aws: None,
            cache_control: String::new(),
        };

        let result = run_merge(&config).unwrap();
        assert_eq!(result.partitions_merged, 1);
        assert_eq!(result.files_written, 1);

        // Verify metadata is preserved in the merged file.
        let mut out_files = Vec::new();
        collect_parquet_files_recursive(dir.path(), &mut out_files).unwrap();
        assert_eq!(out_files.len(), 1);

        let out_kvs = read_parquet_kv_metadata(&out_files[0]).expect("metadata should be present");
        let find = |key: &str| {
            out_kvs
                .iter()
                .find(|kv| kv.key == key)
                .and_then(|kv| kv.value.clone())
        };
        assert_eq!(find("firehose-parquet.version"), Some("0.1.0".to_string()));
        assert_eq!(find("firehose-parquet.chain_name"), Some("eth".to_string()));
    }
}
