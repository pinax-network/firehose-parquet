//! Roll up fine-grained partitioned Parquet files into coarser intervals.
//!
//! Reads minute- or hour-partitioned files and merges them into hourly or daily
//! partitions, respecting a compressed file-size limit (`flush_bytes`).
//!
//! Only `part-*.parquet` files below a partition directory finer than the target are rolled
//! up. Files already at the target granularity (such as earlier rollup outputs), files
//! outside time partitions, and reserved dataset artifacts (`cursor.parquet`, see
//! [`crate::artifacts`]) are never read, rewritten, or deleted. Every run writes new, uniquely
//! named files, so a re-run cannot overwrite or delete its own output. A target partition whose
//! source files have different schemas is left untouched and reported as an error.

use crate::artifacts::is_reserved_artifact_path;
use crate::cli::{block_on_async, format_bytes, resolve_parquet_input_path_string, AwsConfig};
use crate::config::{Compression, DAY_PARTITION_PREFIX, LEGACY_DAY_PARTITION_PREFIX};
use crate::merge::SchemaCheck;
use crate::writer::parse_s3_url;
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
use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// File-name prefix of outputs written without `--delete-source`.
///
/// Such files are copies of source files that still exist, so rolling up their target
/// partition again replaces them instead of adding another copy of the same rows.
const COPY_OUTPUT_PREFIX: &str = "part-rollup-";

/// Target partition granularity for rollup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupTarget {
    /// Merge into hourly partitions: `table/year=YYYY/month=MM/day=DD/hour=HH/`
    Hour,
    /// Merge into daily partitions: `table/year=YYYY/month=MM/day=DD/`
    Date,
}

impl RollupTarget {
    /// Hive directory prefixes of a partition at this granularity. Days accept the
    /// legacy `date=DD` key as well as `day=DD`; outputs keep the source's key.
    fn level_prefixes(self) -> &'static [&'static str] {
        match self {
            RollupTarget::Hour => &["hour="],
            RollupTarget::Date => &[DAY_PARTITION_PREFIX, LEGACY_DAY_PARTITION_PREFIX],
        }
    }

    /// Hive directory prefixes finer than this granularity.
    fn finer_prefixes(self) -> &'static [&'static str] {
        match self {
            RollupTarget::Hour => &["minute=", "second="],
            RollupTarget::Date => &["hour=", "minute=", "second="],
        }
    }
}

/// Parse a target partition string.
pub fn parse_rollup_target(s: &str) -> Result<RollupTarget> {
    match s.to_lowercase().as_str() {
        "hour" | "hourly" => Ok(RollupTarget::Hour),
        "date" | "daily" | "day" => Ok(RollupTarget::Date),
        other => anyhow::bail!("invalid --target-partition '{other}': expected one of: hour, date"),
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
    let resolved_source = resolve_parquet_input_path_string(&config.source);
    let resolved_output = if config.output == config.source {
        resolved_source.clone()
    } else {
        config.output.clone()
    };
    let resolved = RollupConfig {
        source: resolved_source,
        output: resolved_output,
        target: config.target,
        compression: config.compression,
        flush_bytes: config.flush_bytes,
        delete_source: config.delete_source,
        aws: config.aws.clone(),
        cache_control: config.cache_control.clone(),
    };

    // Keeping the sources next to their rolled-up copy would store every row twice under the
    // same root, and each re-run would add yet another copy.
    if !resolved.delete_source && is_same_location(&resolved.source, &resolved.output) {
        anyhow::bail!(
            "in-place rollup of {} requires --delete-source: without it every row would be \
             stored twice under the same root. Pass --delete-source to replace the source \
             files, or --output to write the rollup somewhere else",
            resolved.source
        );
    }

    if resolved.source.starts_with("s3://") || resolved.output.starts_with("s3://") {
        run_rollup_s3(&resolved)
    } else {
        run_rollup_local(&resolved)
    }
}

/// Returns true when `source` and `output` point at the same dataset root.
fn is_same_location(source: &str, output: &str) -> bool {
    if source.starts_with("s3://") || output.starts_with("s3://") {
        return match (parse_s3_url(source), parse_s3_url(output)) {
            (Ok(source), Ok(output)) => source == output,
            _ => false,
        };
    }
    let (source, output) = (Path::new(source), Path::new(output));
    match (source.canonicalize(), output.canonicalize()) {
        (Ok(source), Ok(output)) => source == output,
        _ => {
            let not_cur_dir = |c: &std::path::Component| *c != std::path::Component::CurDir;
            source
                .components()
                .filter(not_cur_dir)
                .eq(output.components().filter(not_cur_dir))
        }
    }
}

/// Returns true when `rel_path` (relative to the source root) is a table part file that rolls
/// up into `target`: a `part-*.parquet` file inside a target-level partition and below a finer
/// one, e.g. `blocks/year=2024/month=01/day=15/hour=14/part-*.parquet` for `date`.
///
/// Everything else is skipped: reserved artifacts such as `cursor.parquet`, files already at
/// the target granularity (including earlier rollup outputs), and files outside time
/// partitions.
fn is_rollup_source(rel_path: &str, target: RollupTarget) -> bool {
    if is_reserved_artifact_path(rel_path) {
        return false;
    }
    let Some((dirs, file_name)) = rel_path.rsplit_once('/') else {
        return false;
    };
    if !(file_name.starts_with("part-") && file_name.ends_with(".parquet")) {
        return false;
    }
    let mut has_level = false;
    let mut has_finer = false;
    for dir in dirs.split('/') {
        has_level |= target
            .level_prefixes()
            .iter()
            .any(|prefix| dir.starts_with(prefix));
        has_finer |= target
            .finer_prefixes()
            .iter()
            .any(|prefix| dir.starts_with(prefix));
    }
    has_level && has_finer
}

/// Returns true for a file name written by a rollup run without `--delete-source`.
fn is_copy_output(file_name: &str) -> bool {
    file_name.starts_with(COPY_OUTPUT_PREFIX) && file_name.ends_with(".parquet")
}

/// Names the files written by one rollup run.
///
/// Each run uses a fresh random id, so outputs never collide with source files, files
/// written by `build` or `merge`, or outputs of earlier runs.
struct OutputNames {
    run_id: String,
    copy: bool,
}

impl OutputNames {
    fn new(delete_source: bool) -> Self {
        Self {
            run_id: uuid::Uuid::new_v4().simple().to_string()[..8].to_string(),
            copy: !delete_source,
        }
    }

    fn file_name(&self, part: u32) -> String {
        if self.copy {
            format!("{COPY_OUTPUT_PREFIX}{}-{part:06}.parquet", self.run_id)
        } else {
            format!("part-{}-{part:06}.parquet", self.run_id)
        }
    }
}

// ---------------------------------------------------------------------------
// Local filesystem rollup
// ---------------------------------------------------------------------------

fn run_rollup_local(config: &RollupConfig) -> Result<()> {
    let source = PathBuf::from(&config.source);
    if !source.is_dir() {
        anyhow::bail!(
            "source path does not exist or is not a directory: {}",
            source.display()
        );
    }

    let output = PathBuf::from(&config.output);

    // Discover all .parquet files under source and keep the ones to roll up.
    let mut files: Vec<PathBuf> = Vec::new();
    collect_parquet_files_recursive(&source, &mut files)?;
    files.sort();
    let discovered = files.len();
    files.retain(|file| {
        let rel = file.strip_prefix(&source).unwrap_or(file).to_string_lossy();
        let keep = is_rollup_source(&rel, config.target);
        if !keep {
            debug!(path = %file.display(), "skipping parquet file that is not a rollup source");
        }
        keep
    });

    if files.is_empty() {
        info!(
            discovered,
            source = %source.display(),
            "no parquet files to roll up"
        );
        return Ok(());
    }

    info!(
        files = files.len(),
        skipped = discovered - files.len(),
        source = %source.display(),
        "discovered parquet files to roll up"
    );

    // Group files by (table, target_partition_key).
    // The key is the output directory relative to the output root.
    let groups = group_files_by_target(&source, &files, config.target)?;
    let names = OutputNames::new(config.delete_source);

    let mut total_input_files = 0usize;
    let mut total_output_files = 0usize;
    let mut total_rows = 0usize;
    let mut deleted_sources = 0usize;
    let mut written: HashSet<PathBuf> = HashSet::new();
    let mut schema_mismatches: Vec<String> = Vec::new();

    'groups: for (group_key, group_files) in &groups {
        info!(
            group = %group_key,
            files = group_files.len(),
            "processing group"
        );

        // Read all batches from all files in this group.
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
        let mut schema_check = SchemaCheck::default();
        for file_path in group_files {
            debug!(group = %group_key, path = %file_path.display(), "reading source Parquet file");
            let file = std::fs::File::open(file_path)
                .with_context(|| format!("opening {}", file_path.display()))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let rel = file_path.strip_prefix(&source).unwrap_or(file_path);
            if let Some(reason) = schema_check.check(&rel.to_string_lossy(), builder.schema()) {
                record_schema_mismatch(group_key, reason, &mut schema_mismatches);
                continue 'groups;
            }
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

        let group_written = write_merged_batches_local(
            &out_dir,
            &merged,
            config,
            file_kv_metadata.as_deref(),
            &names,
        )?;
        total_output_files += group_written.len();
        written.extend(group_written);

        remove_previous_copies_local(&out_dir, &written)?;

        // Delete this group's sources as soon as its output is written, so a failure in a
        // later group cannot leave them behind to be rolled up a second time.
        if config.delete_source {
            for f in group_files {
                if written.contains(f) {
                    continue;
                }
                debug!(path = %f.display(), "deleting rolled-up source Parquet file");
                std::fs::remove_file(f)
                    .with_context(|| format!("deleting source file {}", f.display()))?;
                deleted_sources += 1;
            }
        }
    }

    if deleted_sources > 0 {
        info!(files = deleted_sources, "deleted source files");
        // Clean up empty directories.
        cleanup_empty_dirs(&source)?;
    }

    info!(
        input_files = total_input_files,
        output_files = total_output_files,
        total_rows,
        "rollup complete"
    );

    schema_mismatch_result(&schema_mismatches)
}

/// Reports a target partition left untouched because its source files have different schemas.
///
/// Arrow's `concat_batches` pairs columns by position, so rolling such files up would silently
/// drop or swap columns before the sources are deleted.
fn record_schema_mismatch(group_key: &str, reason: String, mismatches: &mut Vec<String>) {
    warn!(
        group = %group_key,
        reason = %reason,
        "not rolling up group: source files have different schemas"
    );
    mismatches.push(format!("{group_key}: {reason}"));
}

/// Fails the run when any target partition was skipped for mixed schemas.
fn schema_mismatch_result(mismatches: &[String]) -> Result<()> {
    if mismatches.is_empty() {
        return Ok(());
    }
    anyhow::bail!(
        "{} target partition(s) were not rolled up because their source files have different \
         schemas; nothing was written or deleted for them:\n  {}",
        mismatches.len(),
        mismatches.join("\n  ")
    )
}

/// Create a new output file, failing instead of overwriting an existing one.
fn create_output_file(path: &Path) -> Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("creating output file {}", path.display()))
}

/// Write a merged RecordBatch to one or more new part files, splitting when
/// estimated compressed size exceeds `flush_bytes`.
///
/// Returns the paths of the files written.
fn write_merged_batches_local(
    out_dir: &Path,
    batch: &RecordBatch,
    config: &RollupConfig,
    kv_metadata: Option<&[KeyValue]>,
    names: &OutputNames,
) -> Result<Vec<PathBuf>> {
    let props = writer_properties(config.compression, kv_metadata);
    let flush_bytes = config.flush_bytes;

    if flush_bytes == 0 {
        // No size limit — write everything to a single file.
        let path = out_dir.join(names.file_name(1));
        let file = create_output_file(&path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;
        let size = std::fs::metadata(&path)?.len();
        info!(path = %path.display(), rows = batch.num_rows(), size = %format_bytes(size), "wrote merged file");
        return Ok(vec![path]);
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
        let path = out_dir.join(names.file_name(1));
        create_output_file(&path)?
            .write_all(&buf)
            .with_context(|| format!("writing {}", path.display()))?;
        info!(path = %path.display(), rows = batch.num_rows(), size = %format_bytes(buf.len() as u64), "wrote merged file");
        return Ok(vec![path]);
    }

    // Need to split. Estimate how many rows per file.
    let bytes_per_row = buf.len() as f64 / batch.num_rows() as f64;
    let rows_per_file = ((flush_bytes as f64 / bytes_per_row) as usize).max(1);
    let total_rows = batch.num_rows();
    let mut offset = 0usize;
    let mut part = 0u32;
    let mut paths = Vec::new();

    while offset < total_rows {
        let end = (offset + rows_per_file).min(total_rows);
        let slice = batch.slice(offset, end - offset);
        part += 1;
        let path = out_dir.join(names.file_name(part));
        let file = create_output_file(&path)?;
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
        paths.push(path);
        offset = end;
    }

    Ok(paths)
}

/// Delete copy outputs (`part-rollup-*.parquet`) of earlier runs directly inside `out_dir`.
///
/// They hold rows from sources that were kept, and those rows were just rolled up again into
/// the files in `written`.
fn remove_previous_copies_local(out_dir: &Path, written: &HashSet<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(out_dir)? {
        let path = entry?.path();
        let is_copy = path.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(is_copy_output);
        if is_copy && !written.contains(&path) {
            debug!(path = %path.display(), "deleting copy output of an earlier rollup");
            std::fs::remove_file(&path)
                .with_context(|| format!("deleting earlier rollup output {}", path.display()))?;
        }
    }
    Ok(())
}

/// Group source files by their target (coarser) partition key.
///
/// Returns a map from output relative path (e.g. `blocks/year=2024/month=01/day=15`)
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
///   `blocks/year=2024/month=01/day=15/hour=14/minute=30/part-000001.parquet`
///   → `blocks/year=2024/month=01/day=15`
///
/// Examples (target=Hour):
///   `blocks/year=2024/month=01/day=15/hour=14/minute=30/part-000001.parquet`
///   → `blocks/year=2024/month=01/day=15/hour=14`
///
/// Legacy `date=DD` trees keep their `date=DD` key, so they roll up in place.
///
/// Files without recognized partition components keep everything except the filename.
fn compute_group_key(rel_path: &str, target: RollupTarget) -> String {
    // Split into path components.
    let parts: Vec<&str> = rel_path.split('/').collect();

    // Keep the table name and partitions down to the target; drop finer ones.
    let kept: Vec<&str> = parts
        .iter()
        .copied()
        // Skip the filename (last component with .parquet extension).
        .filter(|part| !part.ends_with(".parquet"))
        .filter(|part| {
            !target
                .finer_prefixes()
                .iter()
                .any(|prefix| part.starts_with(prefix))
        })
        .collect();

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
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
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

// ---------------------------------------------------------------------------
// S3 rollup
// ---------------------------------------------------------------------------

/// An object store location: a client plus the bucket and key prefix it points at.
struct S3Root {
    client: Arc<dyn ObjectStore>,
    bucket: String,
    prefix: String,
}

impl S3Root {
    /// Object key of `rel` below this root.
    fn key(&self, rel: &str) -> String {
        if self.prefix.is_empty() {
            rel.to_string()
        } else {
            format!("{}/{rel}", self.prefix)
        }
    }

    /// `key` relative to this root.
    fn relative<'a>(&self, key: &'a str) -> &'a str {
        key.strip_prefix(&self.prefix)
            .map(|s| s.trim_start_matches('/'))
            .unwrap_or(key)
    }
}

fn run_rollup_s3(config: &RollupConfig) -> Result<()> {
    let aws = config
        .aws
        .as_ref()
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

    let src = S3Root {
        client: src_client,
        bucket: src_bucket,
        prefix: src_prefix,
    };
    let out = S3Root {
        client: out_client,
        bucket: out_bucket,
        prefix: out_prefix,
    };
    rollup_s3(config, &src, &out)
}

fn rollup_s3(config: &RollupConfig, src: &S3Root, out: &S3Root) -> Result<()> {
    // List all .parquet objects under source prefix.
    let objects: Vec<object_store::ObjectMeta> = block_on_async(async {
        use futures::TryStreamExt;
        let prefix = if src.prefix.is_empty() {
            None
        } else {
            Some(object_store::path::Path::from(src.prefix.as_str()))
        };
        src.client.list(prefix.as_ref()).try_collect().await
    })
    .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let mut discovered = 0usize;
    let mut parquet_keys: Vec<String> = Vec::new();
    for obj in &objects {
        let key = obj.location.as_ref();
        if !key.ends_with(".parquet") {
            continue;
        }
        discovered += 1;
        if is_rollup_source(src.relative(key), config.target) {
            parquet_keys.push(key.to_string());
        } else {
            debug!(path = %key, "skipping parquet object that is not a rollup source");
        }
    }
    parquet_keys.sort();

    if parquet_keys.is_empty() {
        info!(discovered, source = %config.source, "no parquet files to roll up");
        return Ok(());
    }

    info!(
        files = parquet_keys.len(),
        skipped = discovered - parquet_keys.len(),
        source = %config.source,
        "discovered parquet files to roll up on S3"
    );

    // Group by target partition. Strip source prefix for relative paths.
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for key in &parquet_keys {
        let group_key = compute_group_key(src.relative(key), config.target);
        groups.entry(group_key).or_default().push(key.clone());
    }

    let names = OutputNames::new(config.delete_source);
    let same_bucket = src.bucket == out.bucket;

    let mut total_input_files = 0usize;
    let mut total_output_files = 0usize;
    let mut total_rows = 0usize;
    let mut deleted_sources = 0usize;
    let mut written: HashSet<String> = HashSet::new();
    let mut schema_mismatches: Vec<String> = Vec::new();

    'groups: for (group_key, group_keys) in &groups {
        info!(group = %group_key, files = group_keys.len(), "processing group");

        // Read all batches and extract file-level metadata from the first file.
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
        let mut schema_check = SchemaCheck::default();
        for s3_key in group_keys {
            debug!(group = %group_key, path = %s3_key, "reading source Parquet file from S3");
            let data = block_on_async(async {
                let path = object_store::path::Path::from(s3_key.as_str());
                src.client.get(&path).await?.bytes().await
            })
            .map_err(|e| anyhow::anyhow!("reading s3://{}/{s3_key}: {e}", src.bucket))?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
            if let Some(reason) = schema_check.check(src.relative(s3_key), builder.schema()) {
                record_schema_mismatch(group_key, reason, &mut schema_mismatches);
                continue 'groups;
            }
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
        let group_written = write_merged_batches_s3(
            out,
            group_key,
            &merged,
            config,
            file_kv_metadata.as_deref(),
            &names,
        )?;
        total_output_files += group_written.len();
        written.extend(group_written);

        remove_previous_copies_s3(out, group_key, &written)?;

        // Delete this group's sources as soon as its output is written, so a failure in a
        // later group cannot leave them behind to be rolled up a second time.
        if config.delete_source {
            for key in group_keys {
                if same_bucket && written.contains(key) {
                    continue;
                }
                debug!(path = %key, "deleting rolled-up source Parquet file from S3");
                block_on_async(async {
                    let path = object_store::path::Path::from(key.as_str());
                    src.client.delete(&path).await
                })
                .map_err(|e| anyhow::anyhow!("deleting s3://{}/{key}: {e}", src.bucket))?;
                deleted_sources += 1;
            }
        }
    }

    if deleted_sources > 0 {
        info!(files = deleted_sources, "deleted source files from S3");
    }

    info!(
        input_files = total_input_files,
        output_files = total_output_files,
        total_rows,
        "rollup complete"
    );

    schema_mismatch_result(&schema_mismatches)
}

/// Upload a merged RecordBatch as one or more new objects under `group_key`, splitting when
/// the estimated compressed size exceeds `flush_bytes`.
///
/// Returns the keys of the objects written.
fn write_merged_batches_s3(
    out: &S3Root,
    group_key: &str,
    batch: &RecordBatch,
    config: &RollupConfig,
    kv_metadata: Option<&[KeyValue]>,
    names: &OutputNames,
) -> Result<Vec<String>> {
    let props = writer_properties(config.compression, kv_metadata);
    let flush_bytes = config.flush_bytes;

    // Write full batch to buffer to check size.
    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props.clone()))?;
        writer.write(batch)?;
        writer.close()?;
    }

    let make_path = |part: u32| -> object_store::path::Path {
        let key = out.key(&format!("{group_key}/{}", names.file_name(part)));
        object_store::path::Path::from(key.as_str())
    };

    let upload = |path: object_store::path::Path, data: Vec<u8>| -> Result<String> {
        let size = data.len();
        let payload = object_store::PutPayload::from(bytes::Bytes::from(data));
        block_on_async(async {
            out.client
                .put_opts(
                    &path,
                    payload,
                    crate::writer::s3_put_options(&config.cache_control),
                )
                .await
        })
        .map_err(|e| anyhow::anyhow!("uploading s3://{}/{path}: {e}", out.bucket))?;
        info!(path = %path, size = %format_bytes(size as u64), "wrote merged file to S3");
        Ok(path.as_ref().to_string())
    };

    if flush_bytes == 0 || (buf.len() as u64) <= flush_bytes {
        info!(rows = batch.num_rows(), "single file fits");
        return Ok(vec![upload(make_path(1), buf)?]);
    }

    // Need to split by estimated rows per file.
    let bytes_per_row = buf.len() as f64 / batch.num_rows() as f64;
    let rows_per_file = ((flush_bytes as f64 / bytes_per_row) as usize).max(1);
    let total_rows = batch.num_rows();
    let mut offset = 0usize;
    let mut part = 0u32;
    let mut keys = Vec::new();

    while offset < total_rows {
        let end = (offset + rows_per_file).min(total_rows);
        let slice = batch.slice(offset, end - offset);
        part += 1;

        let mut part_buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut part_buf, batch.schema(), Some(props.clone()))?;
        writer.write(&slice)?;
        writer.close()?;

        info!(rows = slice.num_rows(), "writing split part");
        keys.push(upload(make_path(part), part_buf)?);
        offset = end;
    }

    Ok(keys)
}

/// Delete copy outputs (`part-rollup-*.parquet`) of earlier runs directly under `group_key`.
///
/// They hold rows from sources that were kept, and those rows were just rolled up again into
/// the objects in `written`.
fn remove_previous_copies_s3(
    out: &S3Root,
    group_key: &str,
    written: &HashSet<String>,
) -> Result<()> {
    let dir = object_store::path::Path::from(out.key(group_key).as_str());
    let listing = block_on_async(out.client.list_with_delimiter(Some(&dir)))
        .map_err(|e| anyhow::anyhow!("listing s3://{}/{dir}: {e}", out.bucket))?;
    for obj in listing.objects {
        let key = obj.location.as_ref();
        let is_copy = obj.location.filename().is_some_and(is_copy_output);
        if is_copy && !written.contains(key) {
            debug!(path = %key, "deleting copy output of an earlier rollup from S3");
            block_on_async(out.client.delete(&obj.location))
                .map_err(|e| anyhow::anyhow!("deleting s3://{}/{key}: {e}", out.bucket))?;
        }
    }
    Ok(())
}

fn build_s3_client(bucket: &str, aws: &AwsConfig) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(aws.build_s3_client(bucket)?))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{UInt64Array, UInt64Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;

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
            compute_group_key(
                "blocks/year=2024/month=01/date=15/hour=14/minute=30/part-000001.parquet",
                RollupTarget::Date
            ),
            "blocks/year=2024/month=01/date=15"
        );
        assert_eq!(
            compute_group_key(
                "blocks/year=2024/month=01/date=15/hour=14/part-000001.parquet",
                RollupTarget::Date
            ),
            "blocks/year=2024/month=01/date=15"
        );
        assert_eq!(
            compute_group_key(
                "blocks/year=2024/month=01/date=15/part-000001.parquet",
                RollupTarget::Date
            ),
            "blocks/year=2024/month=01/date=15"
        );
    }

    #[test]
    fn test_compute_group_key_day() {
        assert_eq!(
            compute_group_key(
                "blocks/year=2024/month=01/day=15/hour=14/minute=30/part-000001.parquet",
                RollupTarget::Date
            ),
            "blocks/year=2024/month=01/day=15"
        );
        assert_eq!(
            compute_group_key(
                "blocks/year=2024/month=01/day=15/hour=14/minute=30/part-000001.parquet",
                RollupTarget::Hour
            ),
            "blocks/year=2024/month=01/day=15/hour=14"
        );
    }

    #[test]
    fn test_compute_group_key_hour() {
        assert_eq!(
            compute_group_key(
                "blocks/year=2024/month=01/date=15/hour=14/minute=30/part-000001.parquet",
                RollupTarget::Hour
            ),
            "blocks/year=2024/month=01/date=15/hour=14"
        );
        assert_eq!(
            compute_group_key(
                "blocks/year=2024/month=01/date=15/hour=14/part-000001.parquet",
                RollupTarget::Hour
            ),
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

    #[test]
    fn test_rollup_local_basic() {
        let source = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        // Create minute-partitioned source files.
        let dir1 = source
            .path()
            .join("blocks/date=2024-01-15/hour=14/minute=30");
        let dir2 = source
            .path()
            .join("blocks/date=2024-01-15/hour=14/minute=31");
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
        let dir = source
            .path()
            .join("blocks/date=2024-01-15/hour=14/minute=00");
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
        assert!(
            out_files.len() > 1,
            "should have split into multiple files, got {}",
            out_files.len()
        );

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

        let dir = source
            .path()
            .join("blocks/date=2024-01-15/hour=14/minute=30");
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
        builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .cloned()
    }

    #[test]
    fn test_rollup_preserves_metadata() {
        let source = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let dir1 = source
            .path()
            .join("blocks/date=2024-01-15/hour=14/minute=30");
        let dir2 = source
            .path()
            .join("blocks/date=2024-01-15/hour=14/minute=31");
        std::fs::create_dir_all(&dir1).unwrap();
        std::fs::create_dir_all(&dir2).unwrap();

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
            &dir1.join("part-000001.parquet"),
            &make_test_batch(10),
            kvs.clone(),
        );
        write_test_parquet_with_metadata(
            &dir2.join("part-000001.parquet"),
            &make_test_batch(20),
            kvs.clone(),
        );

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
    fn test_rollup_flush_bytes_split_preserves_metadata() {
        let source = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let dir = source
            .path()
            .join("blocks/date=2024-01-15/hour=14/minute=00");
        std::fs::create_dir_all(&dir).unwrap();

        let kvs = vec![KeyValue::new(
            "firehose-parquet.version".to_string(),
            Some("0.1.0".to_string()),
        )];

        write_test_parquet_with_metadata(
            &dir.join("part-000001.parquet"),
            &make_test_batch(10000),
            kvs,
        );

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
            let find = |key: &str| {
                out_kvs
                    .iter()
                    .find(|kv| kv.key == key)
                    .and_then(|kv| kv.value.clone())
            };
            assert_eq!(find("firehose-parquet.version"), Some("0.1.0".to_string()));
        }
    }

    const DAY: &str = "blocks/year=2024/month=01/day=15";

    /// A batch whose `block_number` column holds `start..start + rows`, so tests can tell
    /// lost rows from duplicated ones.
    fn make_range_batch(start: u64, rows: u64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_number",
            DataType::UInt64,
            false,
        )]));
        let values = UInt64Array::from_iter_values(start..start + rows);
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    fn write_range_file(path: &Path, start: u64, rows: u64) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        write_test_parquet(path, &make_range_batch(start, rows));
    }

    fn block_numbers_in_batches(batches: &[RecordBatch]) -> Vec<u64> {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                column.values().to_vec()
            })
            .collect()
    }

    /// Every `block_number` stored in the `.parquet` files under `dir`, sorted.
    fn local_block_numbers(dir: &Path) -> Vec<u64> {
        let mut files = Vec::new();
        collect_parquet_files_recursive(dir, &mut files).unwrap();
        let mut values: Vec<u64> = files
            .iter()
            .flat_map(|f| block_numbers_in_batches(&crate::writer::read_parquet(f).unwrap()))
            .collect();
        values.sort_unstable();
        values
    }

    /// Names of the files directly inside `dir`, sorted.
    fn file_names_in(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_file())
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn local_config(source: &Path, output: &Path, delete_source: bool) -> RollupConfig {
        RollupConfig {
            source: source.to_string_lossy().to_string(),
            output: output.to_string_lossy().to_string(),
            target: RollupTarget::Date,
            compression: Compression::None,
            flush_bytes: 0,
            delete_source,
            aws: None,
            cache_control: String::new(),
        }
    }

    #[test]
    fn test_is_rollup_source() {
        let date = RollupTarget::Date;
        let hour = RollupTarget::Hour;
        let minute_file = format!("{DAY}/hour=14/minute=30/part-abc12345-000001.parquet");
        let hour_file = format!("{DAY}/hour=14/part-abc12345-000001.parquet");
        let day_file = format!("{DAY}/part-abc12345-000001.parquet");

        assert!(is_rollup_source(&minute_file, date));
        assert!(is_rollup_source(&hour_file, date));
        assert!(is_rollup_source(&minute_file, hour));
        assert!(is_rollup_source(
            "date=2024-01-15/hour=14/part-000001.parquet",
            date
        ));
        // Legacy `date=DD` day directories written by earlier releases.
        let legacy_day = "blocks/year=2024/month=01/date=15";
        assert!(is_rollup_source(
            &format!("{legacy_day}/hour=14/part-abc12345-000001.parquet"),
            date
        ));
        assert!(!is_rollup_source(
            &format!("{legacy_day}/part-abc12345-000001.parquet"),
            date
        ));

        // Already at the target granularity, including earlier rollup outputs.
        assert!(!is_rollup_source(&day_file, date));
        assert!(!is_rollup_source(&hour_file, hour));
        assert!(!is_rollup_source(
            &format!("{DAY}/part-rollup-abc12345-000001.parquet"),
            date
        ));
        // Coarser than the target.
        assert!(!is_rollup_source(&day_file, hour));

        // Reserved artifacts and files outside time partitions.
        assert!(!is_rollup_source("cursor.parquet", date));
        assert!(!is_rollup_source("partitions.parquet", date));
        assert!(!is_rollup_source("merkle_roots.parquet", date));
        assert!(!is_rollup_source(
            "verify_runs/run-1/date=15/hour=14/part-000001.parquet",
            date
        ));
        assert!(!is_rollup_source(
            "blocks/part-abc12345-000001.parquet",
            date
        ));
        assert!(!is_rollup_source(
            "blocks/block_range=0-999/part-abc12345-000001.parquet",
            date
        ));
        assert!(!is_rollup_source(
            &format!("{DAY}/hour=14/minute=30/cursor.parquet"),
            date
        ));
        assert!(!is_rollup_source(
            &format!("{DAY}/hour=14/minute=30/data.parquet"),
            date
        ));
    }

    #[test]
    fn test_output_names_are_unique_per_run() {
        let first = OutputNames::new(true);
        let second = OutputNames::new(true);
        assert_ne!(first.file_name(1), second.file_name(1));
        assert!(first.file_name(1).starts_with("part-"));
        assert!(first.file_name(1).ends_with("-000001.parquet"));
        assert!(!is_copy_output(&first.file_name(1)));

        let copy = OutputNames::new(false);
        assert!(is_copy_output(&copy.file_name(2)));
        assert!(copy.file_name(2).ends_with("-000002.parquet"));
    }

    #[test]
    fn test_is_same_location() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        assert!(is_same_location(&path, &path));
        assert!(is_same_location(&path, &format!("{path}/")));
        assert!(is_same_location(&path, &format!("{path}/./")));
        assert!(!is_same_location(&path, &format!("{path}/rolled")));
        assert!(is_same_location("./out/blocks", "out/blocks/"));
        assert!(is_same_location(
            "s3://bucket/evm/blocks",
            "s3://bucket/evm/blocks/"
        ));
        assert!(!is_same_location(
            "s3://bucket/evm/blocks",
            "s3://other/evm/blocks"
        ));
        assert!(!is_same_location("s3://bucket/evm/blocks", &path));
    }

    /// Reproduction 1: rolling a day up in place, then again after new minute files arrive,
    /// used to overwrite the first output and then delete it as a source (8 rows -> 0).
    #[test]
    fn test_rollup_in_place_rerun_keeps_earlier_output() {
        let root = tempfile::tempdir().unwrap();
        let day = root.path().join(DAY);
        write_range_file(
            &day.join("hour=14/minute=30/part-aaaaaaaa-000001.parquet"),
            0,
            5,
        );
        write_range_file(
            &day.join("hour=14/minute=31/part-aaaaaaaa-000001.parquet"),
            5,
            3,
        );
        let config = local_config(root.path(), root.path(), true);

        run_rollup(&config).unwrap();
        assert_eq!(local_block_numbers(root.path()), (0..8).collect::<Vec<_>>());
        assert!(!day.join("hour=14").exists());
        assert_eq!(file_names_in(&day).len(), 1);

        // New minute files arrive for the same day.
        write_range_file(
            &day.join("hour=15/minute=00/part-bbbbbbbb-000001.parquet"),
            8,
            4,
        );
        run_rollup(&config).unwrap();
        assert_eq!(
            local_block_numbers(root.path()),
            (0..12).collect::<Vec<_>>()
        );
        assert!(!day.join("hour=15").exists());
        let outputs = file_names_in(&day);
        assert_eq!(outputs.len(), 2);

        // Nothing new: a third run changes nothing.
        run_rollup(&config).unwrap();
        assert_eq!(
            local_block_numbers(root.path()),
            (0..12).collect::<Vec<_>>()
        );
        assert_eq!(file_names_in(&day), outputs);
    }

    /// Reproduction 2 (in place): without --delete-source the rollup was written next to its
    /// sources, and each re-run read the earlier output again, stacking duplicate rows.
    #[test]
    fn test_rollup_in_place_without_delete_source_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let minute_file = root
            .path()
            .join(DAY)
            .join("hour=14/minute=30/part-aaaaaaaa-000001.parquet");
        write_range_file(&minute_file, 0, 8);

        for output in [
            root.path().to_string_lossy().to_string(),
            format!("{}/", root.path().display()),
        ] {
            let mut config = local_config(root.path(), root.path(), false);
            config.output = output;
            let err = run_rollup(&config).unwrap_err().to_string();
            assert!(err.contains("requires --delete-source"), "{err}");
        }

        let mut files = Vec::new();
        collect_parquet_files_recursive(root.path(), &mut files).unwrap();
        assert_eq!(files, vec![minute_file]);
    }

    /// Reproduction 2 (separate output): a re-run without --delete-source must replace the
    /// earlier copy of each partition instead of adding a second one, while leaving files it
    /// did not write alone.
    #[test]
    fn test_rollup_rerun_without_delete_source_replaces_earlier_copy() {
        let source = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();
        let src_day = source.path().join(DAY);
        let out_day = output.path().join(DAY);
        write_range_file(
            &src_day.join("hour=14/minute=30/part-aaaaaaaa-000001.parquet"),
            0,
            5,
        );
        write_range_file(
            &src_day.join("hour=14/minute=31/part-aaaaaaaa-000001.parquet"),
            5,
            3,
        );
        // A daily file written by `build --partition date`, not by rollup.
        write_range_file(&out_day.join("part-cccccccc-000001.parquet"), 100, 2);

        // The first run splits into one file per row.
        let mut config = local_config(source.path(), output.path(), false);
        config.flush_bytes = 1;
        run_rollup(&config).unwrap();
        let mut expected: Vec<u64> = (0..8).chain(100..102).collect();
        assert_eq!(local_block_numbers(output.path()), expected);
        assert_eq!(file_names_in(&out_day).len(), 9);

        // New minute data arrives; the re-run writes a single file.
        write_range_file(
            &src_day.join("hour=15/minute=00/part-bbbbbbbb-000001.parquet"),
            8,
            4,
        );
        config.flush_bytes = 0;
        run_rollup(&config).unwrap();
        expected = (0..12).chain(100..102).collect();
        assert_eq!(local_block_numbers(output.path()), expected);
        let outputs = file_names_in(&out_day);
        assert_eq!(outputs.len(), 2);
        assert!(outputs.contains(&"part-cccccccc-000001.parquet".to_string()));

        // Re-running with no new data is idempotent, and sources are kept.
        run_rollup(&config).unwrap();
        assert_eq!(local_block_numbers(output.path()), expected);
        assert_eq!(file_names_in(&out_day).len(), 2);
        assert_eq!(
            local_block_numbers(source.path()),
            (0..12).collect::<Vec<_>>()
        );
    }

    /// Reproduction 3: rolling up a network root used to roll `cursor.parquet` and the other
    /// root artifacts into a `part-*.parquet` file and then delete them.
    #[test]
    fn test_rollup_network_root_leaves_reserved_artifacts() {
        let root = tempfile::tempdir().unwrap();
        let reserved = [
            "cursor.parquet",
            "partitions.parquet",
            "merkle_roots.parquet",
            "verify_runs/run-1/roots.parquet",
        ];
        for (i, rel) in reserved.iter().enumerate() {
            write_range_file(&root.path().join(rel), 1000 + i as u64, 1);
        }
        std::fs::write(root.path().join("verify_runs/run-1/report.json"), b"{}").unwrap();
        let snapshot: Vec<Vec<u8>> = reserved
            .iter()
            .map(|rel| std::fs::read(root.path().join(rel)).unwrap())
            .collect();
        write_range_file(
            &root
                .path()
                .join(DAY)
                .join("hour=14/minute=30/part-aaaaaaaa-000001.parquet"),
            0,
            8,
        );

        let config = local_config(root.path(), root.path(), true);
        run_rollup(&config).unwrap();
        run_rollup(&config).unwrap();

        for (rel, bytes) in reserved.iter().zip(&snapshot) {
            assert_eq!(
                &std::fs::read(root.path().join(rel)).unwrap(),
                bytes,
                "{rel}"
            );
        }
        assert!(root.path().join("verify_runs/run-1/report.json").exists());
        assert_eq!(
            file_names_in(root.path()),
            vec![
                "cursor.parquet",
                "merkle_roots.parquet",
                "partitions.parquet"
            ]
        );
        assert_eq!(
            local_block_numbers(&root.path().join("blocks")),
            (0..8).collect::<Vec<_>>()
        );
        assert_eq!(file_names_in(&root.path().join(DAY)).len(), 1);
    }

    // -- Object store (S3) path, exercised against an in-memory store --

    fn memory_root(store: &Arc<dyn ObjectStore>, prefix: &str) -> S3Root {
        S3Root {
            client: Arc::clone(store),
            bucket: "bucket".to_string(),
            prefix: prefix.to_string(),
        }
    }

    fn s3_config(source: &str, output: &str, delete_source: bool) -> RollupConfig {
        RollupConfig {
            source: format!("s3://bucket/{source}"),
            output: format!("s3://bucket/{output}"),
            target: RollupTarget::Date,
            compression: Compression::None,
            flush_bytes: 0,
            delete_source,
            aws: None,
            cache_control: String::new(),
        }
    }

    fn put_object(store: &Arc<dyn ObjectStore>, key: &str, data: Vec<u8>) {
        let path = object_store::path::Path::from(key);
        block_on_async(store.put(&path, data.into())).unwrap();
    }

    fn put_range_object(store: &Arc<dyn ObjectStore>, key: &str, start: u64, rows: u64) {
        let batch = make_range_batch(start, rows);
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        put_object(store, key, buf);
    }

    fn get_object(store: &Arc<dyn ObjectStore>, key: &str) -> bytes::Bytes {
        let path = object_store::path::Path::from(key);
        block_on_async(async { store.get(&path).await?.bytes().await }).unwrap()
    }

    fn s3_keys(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
        use futures::TryStreamExt;
        let prefix = object_store::path::Path::from(prefix);
        let objects: Vec<object_store::ObjectMeta> =
            block_on_async(store.list(Some(&prefix)).try_collect()).unwrap();
        let mut keys: Vec<String> = objects
            .into_iter()
            .map(|obj| obj.location.as_ref().to_string())
            .collect();
        keys.sort();
        keys
    }

    fn s3_block_numbers(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<u64> {
        let mut values = Vec::new();
        for key in s3_keys(store, prefix) {
            let reader = ParquetRecordBatchReaderBuilder::try_new(get_object(store, &key))
                .unwrap()
                .build()
                .unwrap();
            let batches: Vec<RecordBatch> = reader.map(|batch| batch.unwrap()).collect();
            values.extend(block_numbers_in_batches(&batches));
        }
        values.sort_unstable();
        values
    }

    /// Reproductions 1 and 3 on S3: an in-place re-run keeps the earlier output, and root
    /// artifacts are neither rolled up nor deleted.
    #[test]
    fn test_rollup_s3_in_place_rerun_keeps_earlier_output_and_reserved_artifacts() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root = memory_root(&store, "mainnet");
        let day = format!("mainnet/{DAY}");
        let reserved = [
            "mainnet/cursor.parquet",
            "mainnet/partitions.parquet",
            "mainnet/merkle_roots.parquet",
        ];
        for (i, key) in reserved.iter().enumerate() {
            put_range_object(&store, key, 1000 + i as u64, 1);
        }
        put_object(
            &store,
            "mainnet/verify_runs/run-1/report.json",
            b"{}".to_vec(),
        );
        let snapshot: Vec<bytes::Bytes> =
            reserved.iter().map(|key| get_object(&store, key)).collect();
        put_range_object(
            &store,
            &format!("{day}/hour=14/minute=30/part-aaaaaaaa-000001.parquet"),
            0,
            5,
        );
        put_range_object(
            &store,
            &format!("{day}/hour=14/minute=31/part-aaaaaaaa-000001.parquet"),
            5,
            3,
        );
        let config = s3_config("mainnet", "mainnet", true);

        rollup_s3(&config, &root, &root).unwrap();
        assert_eq!(
            s3_block_numbers(&store, "mainnet/blocks"),
            (0..8).collect::<Vec<_>>()
        );

        put_range_object(
            &store,
            &format!("{day}/hour=15/minute=00/part-bbbbbbbb-000001.parquet"),
            8,
            4,
        );
        rollup_s3(&config, &root, &root).unwrap();
        rollup_s3(&config, &root, &root).unwrap();

        assert_eq!(
            s3_block_numbers(&store, "mainnet/blocks"),
            (0..12).collect::<Vec<_>>()
        );
        let blocks_keys = s3_keys(&store, "mainnet/blocks");
        assert_eq!(blocks_keys.len(), 2);
        assert!(blocks_keys
            .iter()
            .all(|key| key.rsplit_once('/').unwrap().0 == day));

        let mut expected_reserved: Vec<String> = reserved.iter().map(|k| k.to_string()).collect();
        expected_reserved.push("mainnet/verify_runs/run-1/report.json".to_string());
        expected_reserved.sort();
        let mut other_keys = s3_keys(&store, "mainnet");
        other_keys.retain(|key| !key.starts_with("mainnet/blocks/"));
        assert_eq!(other_keys, expected_reserved);
        for (key, bytes) in reserved.iter().zip(&snapshot) {
            assert_eq!(&get_object(&store, key), bytes, "{key}");
        }
    }

    /// Reproduction 2 on S3: a re-run without --delete-source replaces the earlier copy.
    #[test]
    fn test_rollup_s3_rerun_without_delete_source_replaces_earlier_copy() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let src = memory_root(&store, "src");
        let out = memory_root(&store, "out");
        put_range_object(
            &store,
            &format!("src/{DAY}/hour=14/minute=30/part-aaaaaaaa-000001.parquet"),
            0,
            8,
        );
        put_range_object(
            &store,
            &format!("out/{DAY}/part-cccccccc-000001.parquet"),
            100,
            2,
        );

        let mut config = s3_config("src", "out", false);
        config.flush_bytes = 1;
        rollup_s3(&config, &src, &out).unwrap();
        assert_eq!(s3_keys(&store, "out").len(), 9);

        put_range_object(
            &store,
            &format!("src/{DAY}/hour=15/minute=00/part-bbbbbbbb-000001.parquet"),
            8,
            4,
        );
        config.flush_bytes = 0;
        rollup_s3(&config, &src, &out).unwrap();
        rollup_s3(&config, &src, &out).unwrap();

        let expected: Vec<u64> = (0..12).chain(100..102).collect();
        assert_eq!(s3_block_numbers(&store, "out"), expected);
        let out_keys = s3_keys(&store, "out");
        assert_eq!(out_keys.len(), 2);
        assert!(out_keys.contains(&format!("out/{DAY}/part-cccccccc-000001.parquet")));
        assert_eq!(s3_block_numbers(&store, "src"), (0..12).collect::<Vec<_>>());
    }

    // -- Schema drift --

    /// A one-row batch of non-nullable UInt64 columns, in the given order.
    fn make_columns_batch(columns: &[(&str, u64)]) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::UInt64, false))
            .collect();
        let arrays: Vec<arrow::array::ArrayRef> = columns
            .iter()
            .map(|(_, value)| Arc::new(UInt64Array::from(vec![*value])) as arrow::array::ArrayRef)
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    fn write_columns_file(path: &Path, columns: &[(&str, u64)]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        write_test_parquet(path, &make_columns_batch(columns));
    }

    /// `(path, bytes)` of every `.parquet` file under `dir`, sorted.
    fn snapshot_parquet_files(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut files = Vec::new();
        collect_parquet_files_recursive(dir, &mut files).unwrap();
        files.sort();
        files
            .into_iter()
            .map(|f| {
                let bytes = std::fs::read(&f).unwrap();
                (f, bytes)
            })
            .collect()
    }

    /// Source files that list the same columns in a different order used to be concatenated
    /// by position (swapping values) and then deleted.
    #[test]
    fn test_rollup_skips_group_with_reordered_columns() {
        let root = tempfile::tempdir().unwrap();
        let mixed = root.path().join(DAY);
        let healthy = root.path().join("blocks/year=2024/month=01/day=16");
        write_columns_file(
            &mixed.join("hour=14/minute=30/part-aaaaaaaa-000001.parquet"),
            &[("a", 1), ("b", 10)],
        );
        write_columns_file(
            &mixed.join("hour=14/minute=31/part-bbbbbbbb-000001.parquet"),
            &[("b", 20), ("a", 2)],
        );
        write_columns_file(
            &healthy.join("hour=00/minute=00/part-aaaaaaaa-000001.parquet"),
            &[("a", 3), ("b", 30)],
        );
        write_columns_file(
            &healthy.join("hour=00/minute=01/part-aaaaaaaa-000001.parquet"),
            &[("a", 4), ("b", 40)],
        );
        let before = snapshot_parquet_files(&mixed);

        let err = run_rollup(&local_config(root.path(), root.path(), true))
            .unwrap_err()
            .to_string();

        assert!(err.contains("1 target partition(s)"), "{err}");
        assert!(err.contains(DAY), "{err}");
        assert!(
            err.contains(&format!(
                "hour=14/minute=31/part-bbbbbbbb-000001.parquet does not match \
                 {DAY}/hour=14/minute=30/part-aaaaaaaa-000001.parquet"
            )),
            "{err}"
        );
        assert!(err.contains("different order"), "{err}");
        // Nothing was written or deleted for the mixed day...
        assert_eq!(snapshot_parquet_files(&mixed), before);
        assert!(file_names_in(&mixed).is_empty());
        // ...while the healthy day was rolled up and its sources removed.
        assert_eq!(file_names_in(&healthy).len(), 1);
        assert!(!healthy.join("hour=00").exists());
    }

    /// An extra column used to be dropped or fail halfway; now the group is left untouched.
    #[test]
    fn test_rollup_skips_group_with_extra_column() {
        let source = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();
        let day = source.path().join(DAY);
        write_columns_file(
            &day.join("hour=14/minute=30/part-aaaaaaaa-000001.parquet"),
            &[("a", 1), ("b", 10)],
        );
        write_columns_file(
            &day.join("hour=14/minute=31/part-bbbbbbbb-000001.parquet"),
            &[("a", 2), ("b", 20), ("c", 200)],
        );
        let before = snapshot_parquet_files(source.path());

        let err = run_rollup(&local_config(source.path(), output.path(), false))
            .unwrap_err()
            .to_string();

        assert!(err.contains("extra column `c`"), "{err}");
        assert_eq!(snapshot_parquet_files(source.path()), before);
        assert!(snapshot_parquet_files(output.path()).is_empty());
    }

    #[test]
    fn test_rollup_s3_skips_group_with_mixed_schemas() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root = memory_root(&store, "mainnet");
        let put = |key: String, columns: &[(&str, u64)]| {
            let batch = make_columns_batch(columns);
            let mut buf = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            put_object(&store, &key, buf);
        };
        put(
            format!("mainnet/{DAY}/hour=14/minute=30/part-aaaaaaaa-000001.parquet"),
            &[("a", 1), ("b", 10)],
        );
        put(
            format!("mainnet/{DAY}/hour=14/minute=31/part-bbbbbbbb-000001.parquet"),
            &[("b", 20), ("a", 2)],
        );
        let keys = s3_keys(&store, "mainnet");
        let before: Vec<bytes::Bytes> = keys.iter().map(|key| get_object(&store, key)).collect();

        let config = s3_config("mainnet", "mainnet", true);
        let err = rollup_s3(&config, &root, &root).unwrap_err().to_string();

        assert!(err.contains("different order"), "{err}");
        assert_eq!(s3_keys(&store, "mainnet"), keys);
        for (key, bytes) in keys.iter().zip(&before) {
            assert_eq!(&get_object(&store, key), bytes, "{key}");
        }
    }
}
