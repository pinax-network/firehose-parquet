//! Roll up fine-grained partitioned Parquet files into coarser intervals.
//!
//! Reads minute- or hour-partitioned files and merges them into hourly or daily
//! partitions, flushing one active output part at the `flush_bytes` target.
//!
//! Only `part-*.parquet` files below a partition directory finer than the target are rolled
//! up. Files already at the target granularity (such as earlier rollup outputs), files
//! outside time partitions, and reserved dataset artifacts (`cursor.parquet`, see
//! [`crate::artifacts`]) are never read, rewritten, or deleted. Every run writes new, uniquely
//! named files, so a re-run cannot overwrite or delete its own output. A target partition whose
//! source files have different schemas is left untouched and reported as an error.

use crate::artifacts::is_reserved_artifact_path;
use crate::cli::{block_on_async, format_bytes, resolve_destructive_input_path, AwsConfig};
use crate::config::{Compression, DAY_PARTITION_PREFIX, LEGACY_DAY_PARTITION_PREFIX};
use crate::dataset_lock::DatasetOwnership;
use crate::ingest::maintenance::{self, MaintenancePolicy, MaintenanceTarget};
use crate::maintenance::compaction::{Encoder, SchemaCheck};
use crate::maintenance::discovery::{self, LocalPolicy};
use crate::writer::parse_s3_url;
use anyhow::{Context, Result};
#[cfg(test)]
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
#[cfg(test)]
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
#[cfg(test)]
use parquet::file::properties::WriterProperties;
use std::collections::{BTreeMap, HashSet};
use std::io::Write;

mod range_reader;
use range_reader::RangeReader;
const READER_BATCH_ROWS: usize = 1024;
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
    let resolved_source = resolve_destructive_input_path(&config.source)?;
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

    if resolved.source.starts_with("s3://") != resolved.output.starts_with("s3://") {
        anyhow::bail!(
            "rollup requires both source and output to use the same storage kind (local or S3)"
        );
    }
    let ownership = maintenance::acquire_blocking(
        "rollup",
        vec![
            MaintenanceTarget::input(resolved.source.clone())?,
            MaintenanceTarget::directory(resolved.output.clone()),
        ],
        MaintenancePolicy::Rollup {
            source: resolved.source.clone(),
            output: resolved.output.clone(),
            delete_source: resolved.delete_source,
        },
        resolved.aws.as_ref(),
    )?
    .ownership;
    if resolved.source.starts_with("s3://") || resolved.output.starts_with("s3://") {
        run_rollup_s3(&resolved)
    } else {
        run_rollup_local(&resolved, &ownership)
    }?;
    ownership.release_blocking()
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

fn run_rollup_local(config: &RollupConfig, ownership: &DatasetOwnership) -> Result<()> {
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
    discovery::collect_local(&source, LocalPolicy::PARQUET, &mut files)?;
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

        // Validate every input before any group output. Retain only one decoded
        // batch during this first pass; encoding happens in a second streaming pass.
        let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
        let mut schema_check = SchemaCheck::default();
        let mut schema = None;
        let mut rows = 0usize;
        for file_path in group_files {
            let file = std::fs::File::open(file_path)
                .with_context(|| format!("opening {}", file_path.display()))?;
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(READER_BATCH_ROWS);
            let rel = file_path.strip_prefix(&source).unwrap_or(file_path);
            if let Some(reason) = schema_check.check(&rel.to_string_lossy(), builder.schema()) {
                record_schema_mismatch(group_key, reason, &mut schema_mismatches);
                continue 'groups;
            }
            if schema.is_none() {
                schema = Some(crate::maintenance::compaction::strip_transaction_schema(
                    builder.schema().clone(),
                ));
                file_kv_metadata = builder
                    .metadata()
                    .file_metadata()
                    .key_value_metadata()
                    .cloned();
            }
            for batch in builder.build()? {
                rows = rows
                    .checked_add(batch?.num_rows())
                    .context("rollup row count overflow")?;
            }
        }
        if rows == 0 {
            continue;
        }
        total_rows = total_rows
            .checked_add(rows)
            .context("rollup row count overflow")?;
        total_input_files += group_files.len();
        ownership.revalidate_local_paths()?;
        let out_dir = output.join(group_key);
        std::fs::create_dir_all(&out_dir)
            .with_context(|| format!("creating output dir {}", out_dir.display()))?;
        let schema = schema.context("rollup group has no schema")?;
        let mut encoder = Encoder::rollup(
            schema,
            config.compression,
            file_kv_metadata.as_deref(),
            config.flush_bytes,
        );
        let mut group_written = Vec::new();
        let mut publish = |part: u32, bytes: Vec<u8>, part_rows: usize| -> Result<()> {
            ownership.revalidate_local_paths()?;
            let path = out_dir.join(names.file_name(part));
            create_output_file(&path)?
                .write_all(&bytes)
                .with_context(|| format!("writing {}", path.display()))?;
            info!(path=%path.display(),rows=part_rows,size=%format_bytes(bytes.len() as u64),"wrote rolled-up part");
            group_written.push(path);
            Ok(())
        };
        let mut written_rows = 0usize;
        for file_path in group_files {
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(file_path)?)?
                    .with_batch_size(READER_BATCH_ROWS);
            encoder.write_reader(builder, &mut publish, Some(&mut written_rows))?;
        }
        anyhow::ensure!(
            written_rows == rows,
            "rollup source row count changed after preflight; source files were retained"
        );
        encoder.finish(&mut publish)?;
        total_output_files += group_written.len();
        written.extend(group_written);

        ownership.revalidate_local_paths()?;
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
/// Compare full ordered schemas before streaming so no target part can mix
/// incompatible source fields before source files are deleted.
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
        discovery::relative_key(&self.prefix, key)
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
    let objects = block_on_async(discovery::list_objects(src.client.as_ref(), &src.prefix))
        .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let mut discovered = 0usize;
    let mut groups: BTreeMap<String, Vec<object_store::ObjectMeta>> = BTreeMap::new();
    for object in objects {
        let key = object.location.as_ref();
        if !src.prefix.is_empty()
            && !(key == src.prefix
                || key
                    .strip_prefix(&src.prefix)
                    .is_some_and(|tail| tail.starts_with('/')))
        {
            continue;
        }
        if !key.ends_with(".parquet") {
            continue;
        }
        discovered += 1;
        if is_rollup_source(src.relative(key), config.target) {
            groups
                .entry(compute_group_key(src.relative(key), config.target))
                .or_default()
                .push(object);
        }
    }
    for objects in groups.values_mut() {
        objects.sort_by(|a, b| a.location.cmp(&b.location));
    }
    if groups.is_empty() {
        info!(discovered,source=%config.source,"no parquet files to roll up");
        return Ok(());
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

        let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
        let mut schema_check = SchemaCheck::default();
        let mut schema = None;
        let mut rows = 0usize;
        for object in group_keys {
            let builder = ParquetRecordBatchReaderBuilder::try_new(RangeReader::new(
                src.client.clone(),
                object.clone(),
            ))?
            .with_batch_size(READER_BATCH_ROWS);
            if let Some(reason) =
                schema_check.check(src.relative(object.location.as_ref()), builder.schema())
            {
                record_schema_mismatch(group_key, reason, &mut schema_mismatches);
                continue 'groups;
            }
            if schema.is_none() {
                schema = Some(crate::maintenance::compaction::strip_transaction_schema(
                    builder.schema().clone(),
                ));
                file_kv_metadata = builder
                    .metadata()
                    .file_metadata()
                    .key_value_metadata()
                    .cloned();
            }
            for batch in builder.build()? {
                rows = rows
                    .checked_add(batch?.num_rows())
                    .context("rollup row count overflow")?;
            }
        }
        if rows == 0 {
            continue;
        }
        total_rows = total_rows
            .checked_add(rows)
            .context("rollup row count overflow")?;
        total_input_files += group_keys.len();
        let schema = schema.context("rollup group has no schema")?;
        let mut encoder = Encoder::rollup(
            schema,
            config.compression,
            file_kv_metadata.as_deref(),
            config.flush_bytes,
        );
        let mut group_written = Vec::new();
        let mut publish = |part: u32, bytes: Vec<u8>, part_rows: usize| -> Result<()> {
            let path = object_store::path::Path::from(
                out.key(&format!("{group_key}/{}", names.file_name(part))),
            );
            let size = bytes.len();
            block_on_async(out.client.put_opts(
                &path,
                bytes.into(),
                crate::writer::s3_put_options(&config.cache_control),
            ))
            .map_err(|_| {
                anyhow::anyhow!("publishing rollup output failed; source files were retained")
            })?;
            info!(path=%path,rows=part_rows,size=%format_bytes(size as u64),"wrote rolled-up part");
            group_written.push(path.as_ref().to_owned());
            Ok(())
        };
        let mut written_rows = 0usize;
        for object in group_keys {
            let builder = ParquetRecordBatchReaderBuilder::try_new(RangeReader::new(
                src.client.clone(),
                object.clone(),
            ))?
            .with_batch_size(READER_BATCH_ROWS);
            encoder.write_reader(builder, &mut publish, Some(&mut written_rows))?;
        }
        anyhow::ensure!(
            written_rows == rows,
            "rollup source row count changed after preflight; source files were retained"
        );
        encoder.finish(&mut publish)?;
        total_output_files += group_written.len();
        written.extend(group_written);

        remove_previous_copies_s3(out, group_key, &written)?;

        // Delete this group's sources as soon as its output is written, so a failure in a
        // later group cannot leave them behind to be rolled up a second time.
        if config.delete_source {
            let keys = group_keys
                .iter()
                .filter(|object| !(same_bucket && written.contains(object.location.as_ref())))
                .map(|object| object.location.clone())
                .collect::<Vec<_>>();
            block_on_async(crate::s3::delete::delete_objects_once(&src.client, &keys))?;
            deleted_sources += keys.len();
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
    let keys = listing
        .objects
        .into_iter()
        .filter(|object| {
            object.location.filename().is_some_and(is_copy_output)
                && !written.contains(object.location.as_ref())
        })
        .map(|object| object.location)
        .collect::<Vec<_>>();
    block_on_async(crate::s3::delete::delete_objects_once(&out.client, &keys))?;
    Ok(())
}

fn build_s3_client(bucket: &str, aws: &AwsConfig) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(aws.build_s3_client_for_mutation(bucket)?))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_parquet_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
        Ok(discovery::collect_local(dir, LocalPolicy::PARQUET, out)?)
    }
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

    fn damaged_page(bytes: Vec<u8>) -> Vec<u8> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.clone())).unwrap();
        let column = builder.metadata().row_group(0).column(0);
        let start = column
            .dictionary_page_offset()
            .unwrap_or(column.data_page_offset()) as usize;
        let mut damaged = bytes;
        damaged[start..start + 16].fill(0xff);
        // The footer remains valid, so this is a late decode failure rather than
        // an early file-opening/schema failure.
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(damaged.clone()))
            .unwrap()
            .build()
            .unwrap();
        assert!(reader.into_iter().any(|batch| batch.is_err()));
        damaged
    }

    #[test]
    fn corrupt_late_input_preserves_local_sources_and_existing_output() {
        for delete_source in [false, true] {
            let source = tempfile::tempdir().unwrap();
            let output = tempfile::tempdir().unwrap();
            let first = source
                .path()
                .join(format!("{DAY}/hour=14/minute=30/part-000001.parquet"));
            let last = source
                .path()
                .join(format!("{DAY}/hour=14/minute=31/part-000001.parquet"));
            write_range_file(&first, 0, 3000);
            write_range_file(&last, 3000, 100);
            std::fs::write(&last, damaged_page(std::fs::read(&last).unwrap())).unwrap();
            let original = [
                std::fs::read(&first).unwrap(),
                std::fs::read(&last).unwrap(),
            ];
            let existing = output
                .path()
                .join(format!("{DAY}/part-rollup-previous-000001.parquet"));
            write_range_file(&existing, 9000, 2);
            let previous = std::fs::read(&existing).unwrap();
            let mut config = local_config(source.path(), output.path(), delete_source);
            config.flush_bytes = 1;
            assert!(run_rollup(&config).is_err());
            assert_eq!(std::fs::read(&first).unwrap(), original[0]);
            assert_eq!(std::fs::read(&last).unwrap(), original[1]);
            assert_eq!(std::fs::read(&existing).unwrap(), previous);
            assert_eq!(
                file_names_in(existing.parent().unwrap()),
                vec![existing.file_name().unwrap().to_str().unwrap().to_owned()]
            );
        }
    }

    #[test]
    fn corrupt_late_input_preserves_remote_sources_and_existing_output() {
        for delete_source in [false, true] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let first = format!("src/{DAY}/hour=14/minute=30/part-000001.parquet");
            let last = format!("src/{DAY}/hour=14/minute=31/part-000001.parquet");
            put_range_object(&store, &first, 0, 3000);
            put_range_object(&store, &last, 3000, 100);
            put_object(
                &store,
                &last,
                damaged_page(get_object(&store, &last).to_vec()),
            );
            let existing = format!("out/{DAY}/part-rollup-previous-000001.parquet");
            put_range_object(&store, &existing, 9000, 2);
            let before: BTreeMap<_, _> = s3_keys(&store, "")
                .into_iter()
                .map(|key| {
                    let bytes = get_object(&store, &key);
                    (key, bytes)
                })
                .collect();
            let mut config = s3_config("src", "out", delete_source);
            config.flush_bytes = 1;
            assert!(rollup_s3(
                &config,
                &memory_root(&store, "src"),
                &memory_root(&store, "out")
            )
            .is_err());
            let after: BTreeMap<_, _> = s3_keys(&store, "")
                .into_iter()
                .map(|key| {
                    let bytes = get_object(&store, &key);
                    (key, bytes)
                })
                .collect();
            assert_eq!(after, before);
        }
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

        // The first run splits at input batch boundaries (one batch per small source).
        let mut config = local_config(source.path(), output.path(), false);
        config.flush_bytes = 1;
        run_rollup(&config).unwrap();
        let mut expected: Vec<u64> = (0..8).chain(100..102).collect();
        assert_eq!(local_block_numbers(output.path()), expected);
        assert_eq!(file_names_in(&out_day).len(), 3);

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

    #[test]
    fn s3_copy_cleanup_error_keeps_sources_and_all_later_groups_untouched() {
        use crate::s3::delete::test_store::DelayedStore;
        use std::sync::atomic::Ordering;
        let prior_copy = format!("out/{DAY}/{COPY_OUTPUT_PREFIX}old-000001.parquet");
        let mut fake = DelayedStore::new(std::time::Duration::from_millis(20));
        fake.fail = Some(object_store::path::Path::from(prior_copy.as_str()));
        fake.lose_response = true;
        let fake = Arc::new(fake);
        let store: Arc<dyn ObjectStore> = fake.clone();
        let source = format!("src/{DAY}/hour=14/minute=30/part-first.parquet");
        let later_day = DAY.replace("day=15", "day=16");
        assert_ne!(later_day, DAY);
        let later_source = format!("src/{later_day}/hour=14/minute=30/part-later.parquet");
        put_range_object(&store, &source, 0, 8);
        put_range_object(&store, &later_source, 8, 4);
        put_range_object(&store, &prior_copy, 100, 1);
        let before = [
            get_object(&store, &source),
            get_object(&store, &later_source),
        ];
        assert!(rollup_s3(
            &s3_config("src", "out", true),
            &memory_root(&store, "src"),
            &memory_root(&store, "out")
        )
        .is_err());
        assert_eq!(get_object(&store, &source), before[0]);
        assert_eq!(get_object(&store, &later_source), before[1]);
        assert!(s3_keys(&store, &format!("out/{later_day}")).is_empty());
        assert_eq!(
            s3_block_numbers(&store, &format!("out/{DAY}")),
            (0..8).collect::<Vec<_>>()
        );
        assert_eq!(
            fake.counters.started.lock().unwrap().as_slice(),
            &[object_store::path::Path::from(prior_copy)]
        );
        assert_eq!(fake.counters.active.load(Ordering::SeqCst), 0);
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
        assert_eq!(s3_keys(&store, "out").len(), 2);

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
