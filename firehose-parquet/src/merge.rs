//! Merge small parquet part files within each partition into larger files.
//!
//! Unlike `rollup` which changes partition granularity (minute→hour→day),
//! `merge` operates within each existing partition directory, consolidating
//! many small parts into fewer larger files.

use crate::cli::{block_on_async, format_bytes, resolve_parquet_input_path_string, AwsConfig};
use crate::config::Compression;
use crate::writer::s3_put_options;
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::basic::ZstdLevel;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, info_span, warn};

const S3_READ_MAX_ATTEMPTS: usize = 5;
const S3_READ_RETRY_BASE_DELAY_MS: u64 = 100;
const S3_UPLOAD_MAX_ATTEMPTS: usize = 8;
const S3_UPLOAD_RETRY_BASE_DELAY_MS: u64 = 250;
const S3_DELETE_MAX_ATTEMPTS: usize = 5;
const S3_DELETE_RETRY_BASE_DELAY_MS: u64 = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MergeS3Operation {
    Read,
    Upload,
    Delete,
}

impl MergeS3Operation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Upload => "upload",
            Self::Delete => "delete",
        }
    }

    fn action(self) -> &'static str {
        match self {
            Self::Read => "reading",
            Self::Upload => "uploading",
            Self::Delete => "deleting",
        }
    }
}

struct StreamingPartWriter {
    schema: Arc<arrow::datatypes::Schema>,
    props: WriterProperties,
    flush_bytes: u64,
    flush_rows: Option<usize>,
    next_part_num: u32,
    current_writer: Option<ArrowWriter<Vec<u8>>>,
}

impl StreamingPartWriter {
    fn new(
        schema: Arc<arrow::datatypes::Schema>,
        props: WriterProperties,
        flush_bytes: u64,
        flush_rows: Option<u32>,
        initial_part_num: u32,
    ) -> Self {
        Self {
            schema,
            props,
            flush_bytes,
            // Treat an explicit zero like the disabled default so merge only flushes on rows when
            // the operator provides a positive threshold.
            flush_rows: flush_rows
                .filter(|rows| *rows > 0)
                .map(|rows| rows as usize),
            next_part_num: initial_part_num,
            current_writer: None,
        }
    }

    fn write_batch<F>(&mut self, batch: &RecordBatch, flush_part: &mut F) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if self.current_writer.is_none() {
            self.current_writer = Some(ArrowWriter::try_new(
                Vec::new(),
                self.schema.clone(),
                Some(self.props.clone()),
            )?);
        }

        let writer = self.current_writer.as_mut().expect("writer must exist");
        writer.write(batch)?;

        let reached_flush_rows = self
            .flush_rows
            .is_some_and(|flush_rows| writer.in_progress_rows() >= flush_rows);
        let reached_flush_bytes =
            self.flush_bytes > 0 && writer.in_progress_size() as u64 >= self.flush_bytes;

        if reached_flush_rows || reached_flush_bytes {
            self.flush_current(flush_part)?;
        }

        Ok(())
    }

    fn finish<F>(&mut self, flush_part: &mut F) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        if self.current_writer.is_some() {
            self.flush_current(flush_part)?;
        }
        Ok(())
    }

    fn flush_current<F>(&mut self, flush_part: &mut F) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        let writer = self
            .current_writer
            .take()
            .expect("writer must exist when flushing");
        let rows = writer.in_progress_rows();
        let buf = writer.into_inner()?;
        self.next_part_num += 1;
        flush_part(self.next_part_num, buf, rows)?;
        Ok(())
    }
}

/// Configuration for a merge operation.
pub struct MergeConfig {
    pub path: String,
    pub compression: Compression,
    pub flush_rows: Option<u32>,
    pub flush_bytes: u64,
    pub dry_run: bool,
    pub verbose: bool,
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
    let resolved = MergeConfig {
        path: resolve_parquet_input_path_string(&config.path),
        compression: config.compression,
        flush_rows: config.flush_rows,
        flush_bytes: config.flush_bytes,
        dry_run: config.dry_run,
        verbose: config.verbose,
        aws: config.aws.clone(),
        cache_control: config.cache_control.clone(),
    };

    if resolved.path.starts_with("s3://") {
        run_merge_s3(&resolved)
    } else {
        run_merge_local(&resolved)
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

    if let Some(estimated_output_files) =
        no_op_compaction_estimate(files.len(), source_bytes, config.flush_bytes)
    {
        info!(
            partition = partition_label,
            source_files = files.len(),
            source_bytes,
            flush_bytes = config.flush_bytes,
            estimated_output_files,
            "skipping local partition merge because no file-count reduction is expected"
        );
        println!(
            "  {}: skipping merge; {} parts already estimate to ~{} file(s)",
            partition_label,
            files.len(),
            estimated_output_files
        );
        result.partitions_skipped += 1;
        return Ok(());
    }

    result.bytes_before += source_bytes;

    if config.dry_run {
        let est_files = estimate_output_files(source_bytes, config.flush_bytes);
        println!(
            "  {}: {} parts → ~{} file(s) (dry run)",
            partition_label,
            files.len(),
            est_files
        );
        result.files_read += files.len();
        result.partitions_merged += 1;
        return Ok(());
    }

    let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
    let mut writer_state: Option<StreamingPartWriter> = None;
    let mut written_files: Vec<PathBuf> = Vec::new();
    let mut output_bytes = 0u64;
    let initial_part_num = max_part_number_in_local_files(files);

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
            if batch.num_rows() == 0 {
                continue;
            }

            if writer_state.is_none() {
                let props = writer_properties(config.compression, file_kv_metadata.as_deref());
                writer_state = Some(StreamingPartWriter::new(
                    batch.schema(),
                    props,
                    config.flush_bytes,
                    config.flush_rows,
                    initial_part_num,
                ));
            }

            writer_state
                .as_mut()
                .expect("writer state must exist")
                .write_batch(&batch, &mut |part_num, buf, rows| {
                    let path = partition_dir.join(format!("part-{part_num:06}.parquet"));
                    std::fs::write(&path, &buf)
                        .with_context(|| format!("writing {}", path.display()))?;

                    output_bytes += buf.len() as u64;
                    info!(
                        path = %path.display(),
                        rows,
                        bytes = buf.len(),
                        "wrote merged part"
                    );

                    written_files.push(path);
                    Ok(())
                })?;
        }
    }
    result.files_read += files.len();

    if writer_state.is_none() {
        result.partitions_skipped += 1;
        return Ok(());
    }

    writer_state
        .as_mut()
        .expect("writer state must exist")
        .finish(&mut |part_num, buf, rows| {
            let path = partition_dir.join(format!("part-{part_num:06}.parquet"));
            std::fs::write(&path, &buf).with_context(|| format!("writing {}", path.display()))?;

            output_bytes += buf.len() as u64;
            info!(
                path = %path.display(),
                rows,
                bytes = buf.len(),
                "wrote merged part"
            );

            written_files.push(path);
            Ok(())
        })?;

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

fn estimate_output_files(total_bytes: u64, flush_bytes: u64) -> usize {
    if total_bytes == 0 {
        0
    } else if flush_bytes == 0 {
        1
    } else {
        ((total_bytes + flush_bytes - 1) / flush_bytes) as usize
    }
}

fn no_op_compaction_estimate(
    source_files: usize,
    source_bytes: u64,
    flush_bytes: u64,
) -> Option<usize> {
    let estimated_output_files = estimate_output_files(source_bytes, flush_bytes);
    (estimated_output_files >= source_files).then_some(estimated_output_files)
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

fn parse_part_number(filename: &str) -> Option<u32> {
    filename
        .strip_prefix("part-")
        .and_then(|s| s.strip_suffix(".parquet"))
        .and_then(|s| s.parse::<u32>().ok())
}

fn max_part_number_in_local_files(files: &[PathBuf]) -> u32 {
    files
        .iter()
        .filter_map(|file| file.file_name().and_then(|name| name.to_str()))
        .filter_map(parse_part_number)
        .max()
        .unwrap_or(0)
}

fn max_part_number_in_s3_objects(objects: &[object_store::ObjectMeta]) -> u32 {
    objects
        .iter()
        .filter_map(|obj| obj.location.as_ref().rsplit_once('/').map(|(_, name)| name))
        .filter_map(parse_part_number)
        .max()
        .unwrap_or(0)
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

    if let Some(estimated_output_files) =
        no_op_compaction_estimate(objects.len(), source_bytes, config.flush_bytes)
    {
        info!(
            table,
            partition = partition_label,
            source_files = objects.len(),
            source_bytes,
            flush_bytes = config.flush_bytes,
            estimated_output_files,
            "skipping S3 partition merge because no file-count reduction is expected"
        );
        println!(
            "  {}: skipping merge; {} parts already estimate to ~{} file(s)",
            partition_label,
            objects.len(),
            estimated_output_files,
        );
        result.partitions_skipped += 1;
        return Ok(());
    }

    result.bytes_before += source_bytes;

    info!(
        table,
        partition = partition_label,
        source_files = objects.len(),
        source_bytes,
        flush_bytes = config.flush_bytes,
        flush_rows = config.flush_rows,
        dry_run = config.dry_run,
        "starting S3 partition merge"
    );

    if config.dry_run {
        let est_files = estimate_output_files(source_bytes, config.flush_bytes);
        println!(
            "  {}: {} parts → ~{} file(s) (dry run)",
            partition_label,
            objects.len(),
            est_files,
        );
        result.files_read += objects.len();
        result.partitions_merged += 1;
        return Ok(());
    }

    let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
    let mut writer_state: Option<StreamingPartWriter> = None;
    let mut files_written = 0usize;
    let mut output_bytes = 0u64;
    let initial_part_num = max_part_number_in_s3_objects(objects);

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
            if batch.num_rows() == 0 {
                continue;
            }

            if writer_state.is_none() {
                let props = writer_properties(config.compression, file_kv_metadata.as_deref());
                writer_state = Some(StreamingPartWriter::new(
                    batch.schema(),
                    props,
                    config.flush_bytes,
                    config.flush_rows,
                    initial_part_num,
                ));
            }

            writer_state
                .as_mut()
                .expect("writer state must exist")
                .write_batch(&batch, &mut |part_num, buf, _rows| {
                    let s3_key = format!("{partition_key}/part-{part_num:06}.parquet");
                    let s3_path = object_store::path::Path::from(s3_key.as_str());
                    let size = buf.len();
                    let payload = bytes::Bytes::from(buf);

                    put_s3_bytes_with_retry(
                        client,
                        bucket,
                        &s3_path,
                        payload,
                        table,
                        partition_label,
                        &config.cache_control,
                    )?;

                    files_written += 1;
                    output_bytes += size as u64;
                    if config.verbose {
                        info!(
                            operation = "upload",
                            s3_key = %s3_path,
                            s3_uri = %format!("s3://{bucket}/{s3_path}"),
                            table,
                            partition = partition_label,
                            bytes = size,
                            "uploaded merged S3 part"
                        );
                    }
                    Ok(())
                })?;
        }
    }
    result.files_read += objects.len();

    if writer_state.is_none() {
        result.partitions_skipped += 1;
        return Ok(());
    }

    writer_state
        .as_mut()
        .expect("writer state must exist")
        .finish(&mut |part_num, buf, _rows| {
            let s3_key = format!("{partition_key}/part-{part_num:06}.parquet");
            let s3_path = object_store::path::Path::from(s3_key.as_str());
            let size = buf.len();
            let payload = bytes::Bytes::from(buf);

            put_s3_bytes_with_retry(
                client,
                bucket,
                &s3_path,
                payload,
                table,
                partition_label,
                &config.cache_control,
            )?;

            files_written += 1;
            output_bytes += size as u64;
            if config.verbose {
                info!(
                    operation = "upload",
                    s3_key = %s3_path,
                    s3_uri = %format!("s3://{bucket}/{s3_path}"),
                    table,
                    partition = partition_label,
                    bytes = size,
                    "uploaded merged S3 part"
                );
            }
            Ok(())
        })?;

    println!(
        "  {}: {} parts → {} file(s) ({})",
        partition_label,
        objects.len(),
        files_written,
        format_bytes(output_bytes),
    );

    let written_keys: HashSet<String> = (initial_part_num + 1
        ..=initial_part_num + files_written as u32)
        .map(|part_num| format!("{partition_key}/part-{part_num:06}.parquet"))
        .collect();

    for obj in objects {
        if !written_keys.contains(obj.location.as_ref()) {
            delete_s3_object_with_retry(client, bucket, &obj.location, table, partition_label)?;
            if config.verbose {
                info!(
                    operation = "delete",
                    s3_key = %obj.location,
                    s3_uri = %format!("s3://{bucket}/{}", obj.location),
                    table,
                    partition = partition_label,
                    "deleted source S3 part after merge"
                );
            }
        }
    }

    result.bytes_after += output_bytes;
    result.files_written += files_written;
    result.partitions_merged += 1;
    info!(
        table,
        partition = partition_label,
        files_read = objects.len(),
        files_written,
        output_bytes,
        "completed S3 partition merge"
    );
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
    retry_merge_s3_operation(
        MergeS3Operation::Read,
        bucket,
        location,
        table,
        partition_label,
        max_attempts,
        S3_READ_RETRY_BASE_DELAY_MS,
        || {
            Ok(block_on_async(async {
                client.get(location).await?.bytes().await
            })?)
        },
    )
}

fn put_s3_bytes_with_retry(
    client: &Arc<object_store::aws::AmazonS3>,
    bucket: &str,
    location: &object_store::path::Path,
    payload: bytes::Bytes,
    table: &str,
    partition_label: &str,
    cache_control: &str,
) -> Result<()> {
    retry_merge_s3_operation(
        MergeS3Operation::Upload,
        bucket,
        location,
        table,
        partition_label,
        S3_UPLOAD_MAX_ATTEMPTS,
        S3_UPLOAD_RETRY_BASE_DELAY_MS,
        || {
            let payload = object_store::PutPayload::from(payload.clone());
            Ok(block_on_async(async {
                client
                    .put_opts(location, payload, s3_put_options(cache_control))
                    .await
            })
            .map(|_| ())?)
        },
    )
}

fn delete_s3_object_with_retry(
    client: &Arc<object_store::aws::AmazonS3>,
    bucket: &str,
    location: &object_store::path::Path,
    table: &str,
    partition_label: &str,
) -> Result<()> {
    retry_merge_s3_operation(
        MergeS3Operation::Delete,
        bucket,
        location,
        table,
        partition_label,
        S3_DELETE_MAX_ATTEMPTS,
        S3_DELETE_RETRY_BASE_DELAY_MS,
        || Ok(block_on_async(async { client.delete(location).await })?),
    )
}

fn retry_merge_s3_operation<T, F>(
    operation: MergeS3Operation,
    bucket: &str,
    location: &object_store::path::Path,
    table: &str,
    partition_label: &str,
    max_attempts: usize,
    base_delay_ms: u64,
    mut action: F,
) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let s3_uri = format!("s3://{bucket}/{location}");
    let mut attempt = 0usize;

    loop {
        attempt += 1;
        let io_span = info_span!(
            "merge_s3_object",
            operation = operation.as_str(),
            s3_key = %location,
            s3_uri = %s3_uri,
            table,
            partition = partition_label,
            attempt,
            max_attempts,
        );

        match io_span.in_scope(&mut action) {
            Ok(value) => return Ok(value),
            Err(error) => {
                if attempt >= max_attempts {
                    return Err(anyhow::anyhow!(
                        "failed {} s3://{bucket}/{location} after {attempt} attempts (table={table}, partition={partition_label}): {error}",
                        operation.action()
                    ));
                }

                let retry_in = Duration::from_millis(
                    base_delay_ms.saturating_mul(2u64.pow((attempt - 1) as u32)),
                );

                warn!(
                    operation = operation.as_str(),
                    s3_key = %location,
                    s3_uri = %s3_uri,
                    table,
                    partition = partition_label,
                    attempt,
                    max_attempts,
                    retry_in_ms = retry_in.as_millis(),
                    error = %error,
                    "failed {} S3 object during merge (attempt {attempt}/{max_attempts}) for {s3_uri} [table={table}, partition={partition_label}]; retrying",
                    operation.action()
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

    fn read_parquet_row_count(path: &Path) -> usize {
        let file = std::fs::File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        builder
            .metadata()
            .file_metadata()
            .num_rows()
            .try_into()
            .expect("test parquet row count should fit usize")
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
            flush_rows: None,
            flush_bytes: 0,
            dry_run: false,
            verbose: false,
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

    #[test]
    fn test_merge_flushes_on_row_limit() {
        let dir = tempfile::tempdir().unwrap();
        let partition = dir.path().join("blocks/year=2024/month=01/date=15");
        std::fs::create_dir_all(&partition).unwrap();

        for part_num in 1..=3 {
            write_test_parquet_with_metadata(
                &partition.join(format!("part-{part_num:06}.parquet")),
                &make_test_batch(10),
                vec![],
            );
        }

        let config = MergeConfig {
            path: dir.path().to_string_lossy().to_string(),
            compression: Compression::None,
            flush_rows: Some(15),
            flush_bytes: 0,
            dry_run: false,
            verbose: false,
            aws: None,
            cache_control: String::new(),
        };

        let result = run_merge(&config).unwrap();
        assert_eq!(result.partitions_merged, 1);
        assert_eq!(result.files_written, 2);

        let mut out_files = Vec::new();
        collect_parquet_files_recursive(dir.path(), &mut out_files).unwrap();
        out_files.sort();
        assert_eq!(out_files.len(), 2);
        assert_eq!(read_parquet_row_count(&out_files[0]), 20);
        assert_eq!(read_parquet_row_count(&out_files[1]), 10);
    }

    #[test]
    fn test_merge_skips_partition_when_estimate_has_no_compaction_benefit() {
        let dir = tempfile::tempdir().unwrap();
        let partition = dir.path().join("blocks/year=2024/month=01/date=15");
        std::fs::create_dir_all(&partition).unwrap();

        for part_num in 1..=3 {
            write_test_parquet_with_metadata(
                &partition.join(format!("part-{part_num:06}.parquet")),
                &make_test_batch(10),
                vec![],
            );
        }

        let mut source_files = Vec::new();
        collect_parquet_files_recursive(dir.path(), &mut source_files).unwrap();
        source_files.sort();

        let source_bytes: u64 = source_files
            .iter()
            .map(|path| std::fs::metadata(path).unwrap().len())
            .sum();
        let flush_bytes = source_bytes.div_ceil(source_files.len() as u64);

        let config = MergeConfig {
            path: dir.path().to_string_lossy().to_string(),
            compression: Compression::None,
            flush_rows: None,
            flush_bytes,
            dry_run: false,
            verbose: false,
            aws: None,
            cache_control: String::new(),
        };

        let result = run_merge(&config).unwrap();
        assert_eq!(result.partitions_merged, 0);
        assert_eq!(result.partitions_skipped, 1);
        assert_eq!(result.files_read, 0);
        assert_eq!(result.files_written, 0);

        let mut out_files = Vec::new();
        collect_parquet_files_recursive(dir.path(), &mut out_files).unwrap();
        out_files.sort();
        assert_eq!(out_files, source_files);
    }

    #[test]
    fn test_retry_merge_s3_operation_retries_then_succeeds() {
        let location = object_store::path::Path::from("blocks/part-000001.parquet");
        let mut attempts = 0usize;

        let result = retry_merge_s3_operation(
            MergeS3Operation::Upload,
            "bucket",
            &location,
            "blocks",
            "blocks",
            3,
            0,
            || {
                attempts += 1;
                if attempts < 3 {
                    anyhow::bail!("temporary failure");
                }
                Ok("ok")
            },
        )
        .unwrap();

        assert_eq!(result, "ok");
        assert_eq!(attempts, 3);
    }

    #[test]
    fn test_retry_merge_s3_operation_reports_context_after_exhaustion() {
        let location = object_store::path::Path::from("blocks/part-000001.parquet");
        let mut attempts = 0usize;

        let err = retry_merge_s3_operation::<(), _>(
            MergeS3Operation::Delete,
            "bucket",
            &location,
            "blocks",
            "blocks/date=2026-03-18",
            2,
            0,
            || {
                attempts += 1;
                anyhow::bail!("permanent failure");
            },
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(message
            .contains("failed deleting s3://bucket/blocks/part-000001.parquet after 2 attempts"));
        assert!(message.contains("table=blocks"));
        assert!(message.contains("partition=blocks/date=2026-03-18"));
        assert_eq!(attempts, 2);
    }
}
