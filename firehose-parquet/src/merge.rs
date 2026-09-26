//! Merge small parquet part files within each partition into larger files.
//!
//! Unlike `rollup` which changes partition granularity (minute→hour→day),
//! `merge` operates within each existing partition directory, consolidating
//! many small parts into fewer larger files.

use crate::artifacts::is_reserved_artifact_path;
use crate::cli::{block_on_async, format_bytes, resolve_destructive_input_path, AwsConfig};
use crate::config::Compression;
use crate::dataset_lock::DatasetOwnership;
use crate::dataset_lock_s3::{S3Ownership, OWNER_KEY};
use crate::ingest::maintenance::{self, MaintenancePolicy, MaintenanceTarget, ProtectedRoot};
use crate::merge_journal::{
    crash_point, write_local_output, Journal, LocalPartition, LocalRunLock, PartitionFiles,
    RunContext, S3Partition, JOURNAL_FILE,
};
use crate::writer::s3_put_options;
use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
#[cfg(test)]
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
#[cfg(test)]
use parquet::arrow::ArrowWriter;
#[cfg(test)]
use parquet::file::metadata::KeyValue;
#[cfg(test)]
use parquet::file::properties::WriterProperties;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, info_span, warn};

mod engine;
mod read;

#[cfg(test)]
use crate::maintenance::compaction::describe_schema_mismatch;
use crate::maintenance::compaction::{Encoder, SchemaCheck};
use crate::maintenance::discovery::{self, relative_key, LocalPolicy};

const S3_UPLOAD_MAX_ATTEMPTS: usize = 1;
const S3_UPLOAD_RETRY_BASE_DELAY_MS: u64 = 250;
/// Bytes read from the end of an S3 object to get its Parquet footer in one request.
const S3_FOOTER_PREFETCH_BYTES: u64 = 64 * 1024;

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
#[derive(Debug, Default)]
pub struct MergeResult {
    pub partitions_merged: usize,
    pub partitions_skipped: usize,
    pub files_read: usize,
    pub files_written: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    /// Partitions left untouched because their parts have different schemas, each as
    /// `<partition>: <reason>`.
    pub schema_mismatches: Vec<String>,
    /// Interrupted partition merges from earlier runs that were finished or undone first.
    pub merges_recovered: usize,
    /// Partitions another running merge was working on, left alone.
    pub partitions_in_use: Vec<String>,
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
        if self.merges_recovered > 0 {
            println!("  Interrupted merges recovered: {}", self.merges_recovered);
        }
        if !self.partitions_in_use.is_empty() {
            println!(
                "  In use by another merge: {}",
                self.partitions_in_use.join(", ")
            );
        }
        if !self.schema_mismatches.is_empty() {
            println!(
                "  Not merged (parts have different schemas): {}",
                self.schema_mismatches.len()
            );
            for mismatch in &self.schema_mismatches {
                println!("    {mismatch}");
            }
        }
        println!();
    }
}

/// Run the merge operation.
pub fn run_merge(config: &MergeConfig) -> Result<MergeResult> {
    let resolved = MergeConfig {
        path: resolve_destructive_input_path(&config.path)?,
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
// Schema checks
// ---------------------------------------------------------------------------

/// Reports a partition left untouched because its parts have different schemas.
fn record_schema_mismatch(partition_label: &str, reason: String, result: &mut MergeResult) {
    warn!(
        partition = partition_label,
        reason = %reason,
        "not merging partition: parts have different schemas"
    );
    println!("  {partition_label}: not merged; parts have different schemas: {reason}");
    result.partitions_skipped += 1;
    result
        .schema_mismatches
        .push(format!("{partition_label}: {reason}"));
}

fn file_name_string(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

/// Reads the Arrow schema of a local Parquet file from its footer.
fn read_local_arrow_schema(path: &Path) -> Result<SchemaRef> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading Parquet footer of {}", path.display()))?;
    Ok(Arc::clone(builder.schema()))
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

    let prepared = if config.dry_run {
        None
    } else {
        Some(maintenance::acquire_blocking(
            "merge",
            vec![MaintenanceTarget::input(config.path.clone())?],
            MaintenancePolicy::Merge,
            config.aws.as_ref(),
        )?)
    };
    let ownership = prepared.as_ref().map(|prepared| &prepared.ownership);
    let protected_roots = prepared
        .as_ref()
        .map_or(&[][..], |prepared| prepared.roots.as_slice());
    // Retain the old local file lock for recognition of pre-upgrade journals;
    // the directory guard provides shared cross-command/ancestor ownership.
    let lock = if config.dry_run {
        None
    } else {
        Some(LocalRunLock::acquire(&root)?)
    };
    let run = RunContext {
        run_id: new_run_id(),
        lock: lock
            .as_ref()
            .map(LocalRunLock::location)
            .unwrap_or_default(),
    };

    let mut result = MergeResult {
        merges_recovered: prepared.as_ref().map_or(0, |value| value.recovered_merges),
        ..MergeResult::default()
    };
    recover_local_merges(
        &root,
        lock.as_ref(),
        ownership,
        protected_roots,
        &mut result,
    )?;
    merge_local_partitions(&root, config, &run, ownership, protected_roots, &mut result)?;

    if let Some(lock) = lock {
        lock.release()?;
    }
    if let Some(prepared) = prepared {
        prepared.ownership.release_blocking()?;
    }
    Ok(result)
}

/// A fresh id for one merge run, recorded in its journals and temporary file names.
fn new_run_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
}

fn local_partition_label(root: &Path, partition_dir: &Path) -> String {
    let name = partition_dir
        .strip_prefix(root)
        .unwrap_or(partition_dir)
        .to_string_lossy()
        .to_string();
    if name.is_empty() {
        "(root)".to_string()
    } else {
        name
    }
}

/// Finishes or undoes the partition merges that earlier runs left interrupted under `root`.
///
/// Journals whose run is still alive (a merge on an enclosing or nested directory) are left
/// for that run. Without a lock (dry run), journals are only reported.
fn recover_local_merges(
    root: &Path,
    lock: Option<&LocalRunLock>,
    ownership: Option<&DatasetOwnership>,
    protected_roots: &[ProtectedRoot],
    result: &mut MergeResult,
) -> Result<()> {
    let mut journals = Vec::new();
    discovery::collect_local(
        root,
        LocalPolicy::merge_journals(JOURNAL_FILE),
        &mut journals,
    )?;
    journals.sort();
    for journal_path in journals {
        let dir = journal_path.parent().unwrap_or(root);
        let label = local_partition_label(root, dir);
        let admit = |journal: &Journal, lock: &LocalRunLock| -> Result<bool> {
            journal.validate_protection(
                protected_stream_for_path(protected_roots, &dir.to_string_lossy())?.as_ref(),
            )?;
            if lock.owner_alive(journal)? {
                return Ok(false);
            }
            if let Some(ownership) = ownership {
                ownership.revalidate_local_paths()?;
            }
            Ok(true)
        };
        let partition = LocalPartition::new(dir);
        engine::recover_partition(&label, &partition, lock, admit, result)?;
    }
    Ok(())
}

fn merge_local_partitions(
    root: &Path,
    config: &MergeConfig,
    run: &RunContext,
    ownership: Option<&DatasetOwnership>,
    protected_roots: &[ProtectedRoot],
    result: &mut MergeResult,
) -> Result<()> {
    // Group parquet files by their parent directory (partition). Reserved dataset artifacts
    // such as cursor.parquet are not table data and are never merged.
    let mut all_files: Vec<PathBuf> = Vec::new();
    discovery::collect_local(root, LocalPolicy::MUTATION_PARQUET, &mut all_files)?;
    all_files.retain(|file| {
        let rel = file.strip_prefix(root).unwrap_or(file).to_string_lossy();
        let reserved = is_reserved_artifact_path(&rel);
        if reserved {
            debug!(path = %file.display(), "skipping reserved dataset artifact");
        }
        !reserved
    });
    all_files.sort();

    engine::for_each_partition(
        &root.display().to_string(),
        all_files,
        |file| file.parent().unwrap_or(root).to_path_buf(),
        |dir, files, current_table| {
            let partition = LocalMerge {
                label: local_partition_label(root, dir),
                dir,
                files: LocalPartition::new(dir),
                run,
                ownership,
                protected_roots,
            };
            engine::merge_partition(&partition, files, config, current_table, result)
        },
    )
}

/// Reports a partition that another running merge has claimed.
fn record_partition_in_use(partition_label: &str, result: &mut MergeResult) {
    warn!(
        partition = partition_label,
        "not merging partition: another merge is working on it"
    );
    println!("  {partition_label}: skipped; another merge is working on it");
    result.partitions_skipped += 1;
    result.partitions_in_use.push(partition_label.to_string());
}

/// One local partition directory under the common directory guard and the legacy run lock.
struct LocalMerge<'a> {
    label: String,
    dir: &'a Path,
    files: LocalPartition,
    run: &'a RunContext,
    ownership: Option<&'a DatasetOwnership>,
    protected_roots: &'a [ProtectedRoot],
}

impl LocalMerge<'_> {
    fn revalidate(&self) -> Result<()> {
        if let Some(ownership) = self.ownership {
            ownership.revalidate_local_paths()?;
        }
        Ok(())
    }
}

impl engine::PartitionMerge for LocalMerge<'_> {
    type Source = PathBuf;
    type Files = LocalPartition;

    fn label(&self) -> &str {
        &self.label
    }

    fn files(&self) -> &LocalPartition {
        &self.files
    }

    fn source_bytes(&self, files: &[PathBuf]) -> Result<u64> {
        Ok(files
            .iter()
            .filter_map(|f| std::fs::metadata(f).ok())
            .map(|m| m.len())
            .sum())
    }

    fn log_no_op(&self, sources: usize, source_bytes: u64, flush_bytes: u64, estimated: usize) {
        info!(
            partition = self.label,
            source_files = sources,
            source_bytes,
            flush_bytes,
            estimated_output_files = estimated,
            "skipping local partition merge because no file-count reduction is expected"
        );
    }

    fn schema_mismatch(&self, files: &[PathBuf]) -> Result<Option<String>> {
        let mut schema_check = SchemaCheck::default();
        for file in files {
            let schema = read_local_arrow_schema(file)?;
            if let Some(reason) = schema_check.check(&file_name_string(file), &schema) {
                return Ok(Some(reason));
            }
        }
        Ok(None)
    }

    fn log_start(&self, _sources: usize, _source_bytes: u64, _config: &MergeConfig) {}

    fn max_part_number(&self, files: &[PathBuf]) -> u32 {
        max_part_number_in_local_files(files)
    }

    fn claim_journal(&self, files: &[PathBuf], initial_part_num: u32) -> Result<Journal> {
        let journal = new_journal(
            self.run,
            files.iter().map(|f| file_name_string(f)).collect(),
            initial_part_num,
            self.protected_roots,
            &self.dir.to_string_lossy(),
        )?;
        self.revalidate()?;
        Ok(journal)
    }

    fn changed_source(&self, files: &[PathBuf]) -> Option<String> {
        files
            .iter()
            .find(|f| !f.exists())
            .map(|missing| missing.display().to_string())
    }

    fn encode<F>(&self, files: &[PathBuf], encoder: &mut Encoder, publish: &mut F) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        for file_path in files {
            let file = std::fs::File::open(file_path)
                .with_context(|| format!("opening {}", file_path.display()))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            encoder.write_reader(builder, publish, None)?;
        }
        Ok(())
    }

    fn publish(&self, name: &str, data: Vec<u8>, rows: usize) -> Result<()> {
        let path = write_local_output(self.dir, name, &data, &self.run.run_id)?;
        info!(
            path = %path.display(),
            rows,
            bytes = data.len(),
            "wrote merged part"
        );
        Ok(())
    }

    fn check_owner(&self) -> Result<()> {
        self.revalidate()
    }

    fn delete_sources(&self, files: &[PathBuf], outputs: &[String]) -> Result<()> {
        // `write_local_output` returned exactly these paths for the published outputs.
        let written: Vec<PathBuf> = outputs.iter().map(|name| self.dir.join(name)).collect();
        for f in files {
            if !written.contains(f) {
                std::fs::remove_file(f).with_context(|| format!("deleting {}", f.display()))?;
                crash_point("after-first-delete")?;
            }
        }
        Ok(())
    }

    fn log_done(&self, _sources: usize, _outputs: usize, _output_bytes: u64) {}
}

/// The claiming journal of a partition merge whose outputs follow `initial_part_num`.
fn new_journal(
    run: &RunContext,
    sources: Vec<String>,
    initial_part_num: u32,
    protected_roots: &[ProtectedRoot],
    partition_path: &str,
) -> Result<Journal> {
    let journal = Journal::new(
        run,
        sources,
        initial_part_num
            .checked_add(1)
            .context("merge part number exhausted")?,
    );
    Ok(journal.with_protected_stream(
        protected_stream_for_path(protected_roots, partition_path)?.as_ref(),
    ))
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

pub(crate) fn parse_part_number(filename: &str) -> Option<u32> {
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
    let (bucket, prefix) = crate::writer::parse_s3_url(&config.path)?;
    let aws = config
        .aws
        .as_ref()
        .context("AWS config required for S3 paths")?;
    if config.dry_run {
        let client: Arc<dyn ObjectStore> = Arc::new(aws.build_s3_client(&bucket)?);
        return merge_s3_owned(config, &client, &bucket, &prefix, None, &[]);
    }
    let prepared = maintenance::acquire_blocking(
        "merge",
        vec![MaintenanceTarget::directory(config.path.clone())],
        MaintenancePolicy::Merge,
        Some(aws),
    )?;
    let owner = prepared
        .ownership
        .remote(&bucket)
        .context("merge bucket is not owned")?;
    let mut result = merge_s3_owned(
        config,
        owner.object_store(),
        &bucket,
        &prefix,
        Some(owner),
        &prepared.roots,
    )?;
    result.merges_recovered += prepared.recovered_merges;
    prepared.ownership.release_blocking()?;
    Ok(result)
}

/// One S3 merge run borrows the complete operation capability (including mirrors).
struct S3Merge<'a> {
    client: &'a Arc<dyn ObjectStore>,
    bucket: &'a str,
    prefix: &'a str,
    run: RunContext,
    lock: Option<&'a S3Ownership>,
    protected_roots: &'a [ProtectedRoot],
}

#[cfg(test)]
fn merge_s3(
    config: &MergeConfig,
    client: &Arc<dyn ObjectStore>,
    bucket: &str,
    prefix: &str,
) -> Result<MergeResult> {
    let owner = if config.dry_run {
        None
    } else {
        Some(block_on_async(S3Ownership::acquire(
            client.clone(),
            "merge",
            vec![prefix.to_owned()],
        ))?)
    };
    let result = merge_s3_owned(config, client, bucket, prefix, owner.as_ref(), &[])?;
    if let Some(owner) = owner {
        block_on_async(owner.release())?;
    }
    Ok(result)
}

fn merge_s3_owned(
    config: &MergeConfig,
    client: &Arc<dyn ObjectStore>,
    bucket: &str,
    prefix: &str,
    owner: Option<&S3Ownership>,
    protected_roots: &[ProtectedRoot],
) -> Result<MergeResult> {
    if !config.dry_run && crate::artifacts::is_control_path(prefix) {
        anyhow::bail!("recovery and ownership controls cannot be ordinary mutation targets");
    }
    let run_id = owner
        .map(|owner| owner.record().owner_id().to_owned())
        .unwrap_or_else(new_run_id);
    let s3 = S3Merge {
        client,
        bucket,
        prefix,
        run: RunContext {
            run_id,
            lock: owner.map(|_| OWNER_KEY.to_owned()).unwrap_or_default(),
        },
        lock: owner,
        protected_roots,
    };
    let mut result = MergeResult::default();
    recover_s3_merges(&s3, &mut result)?;
    merge_s3_partitions(&s3, config, &mut result)?;
    Ok(result)
}

fn list_s3_objects(s3: &S3Merge<'_>) -> Result<Vec<object_store::ObjectMeta>> {
    block_on_async(discovery::list_objects(s3.client.as_ref(), s3.prefix))
        .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))
}

fn assert_s3_ownership(owner: &S3Ownership) -> Result<()> {
    if owner.is_mutation_uncertain()
        || block_on_async(S3Ownership::status(owner.object_store()))?.as_ref()
            != Some(owner.record())
    {
        anyhow::bail!(
            "S3 merge ownership is unresolved or changed; stopping before further mutation"
        );
    }
    Ok(())
}

/// Finishes or undoes the partition merges that earlier runs left interrupted under the prefix.
fn recover_s3_merges(s3: &S3Merge<'_>, result: &mut MergeResult) -> Result<()> {
    let objects = list_s3_objects(s3)?;
    for obj in &objects {
        if obj.location.filename() != Some(JOURNAL_FILE)
            || crate::artifacts::is_control_path(obj.location.as_ref())
        {
            continue;
        }
        let key = obj.location.as_ref();
        let partition_key = key.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("");
        let label = s3_partition_label(s3.prefix, partition_key);
        let admit = |journal: &Journal, lock: &S3Ownership| -> Result<bool> {
            journal.validate_protection(
                protected_stream_for_path(
                    s3.protected_roots,
                    &format!("s3://{}/{partition_key}", s3.bucket),
                )?
                .as_ref(),
            )?;
            if journal.lock != OWNER_KEY {
                anyhow::bail!("legacy S3 merge journal cannot be recovered automatically: its old prefix lock does not prove writer and remote-request quiescence; preserve its files and obtain provider-confirmed recovery before migration");
            }
            assert_s3_ownership(lock)?;
            Ok(true)
        };
        let partition = S3Partition {
            client: s3.client,
            bucket: s3.bucket,
            key: partition_key,
        };
        engine::recover_partition(label, &partition, s3.lock, admit, result)?;
    }
    Ok(())
}

fn merge_s3_partitions(
    s3: &S3Merge<'_>,
    config: &MergeConfig,
    result: &mut MergeResult,
) -> Result<()> {
    let prefix = s3.prefix;
    let mut parquet_objects: Vec<_> = list_s3_objects(s3)?
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .filter(|obj| {
            let key = obj.location.as_ref();
            let reserved = is_reserved_artifact_path(relative_key(prefix, key));
            if reserved {
                debug!(path = %key, "skipping reserved dataset artifact");
            }
            !reserved
        })
        .collect();
    parquet_objects.sort_by(|a, b| a.location.cmp(&b.location));

    engine::for_each_partition(
        &config.path,
        parquet_objects,
        |obj| {
            obj.location
                .as_ref()
                .rsplit_once('/')
                .map(|(p, _)| p.to_string())
                .unwrap_or_default()
        },
        |partition_key, objects, current_table| {
            let label = s3_partition_label(prefix, partition_key);
            let partition = S3PartitionMerge {
                s3,
                key: partition_key,
                label,
                table: engine::table_of(label),
                files: S3Partition {
                    client: s3.client,
                    bucket: s3.bucket,
                    key: partition_key,
                },
                config,
            };
            engine::merge_partition(&partition, objects, config, current_table, result)
        },
    )
}

/// Partition key relative to the merged prefix, `(root)` for the prefix itself.
fn s3_partition_label<'a>(prefix: &str, partition_key: &'a str) -> &'a str {
    let label = relative_key(prefix, partition_key);
    if label.is_empty() {
        "(root)"
    } else {
        label
    }
}

/// One S3 partition (key prefix) under the run's persistent bucket owner.
struct S3PartitionMerge<'a> {
    s3: &'a S3Merge<'a>,
    key: &'a str,
    label: &'a str,
    table: &'a str,
    files: S3Partition<'a>,
    config: &'a MergeConfig,
}

impl S3PartitionMerge<'_> {
    fn assert_owner(&self) -> Result<()> {
        if let Some(lock) = self.s3.lock {
            assert_s3_ownership(lock)?;
        }
        Ok(())
    }
}

impl<'a> engine::PartitionMerge for S3PartitionMerge<'a> {
    type Source = object_store::ObjectMeta;
    type Files = S3Partition<'a>;

    fn label(&self) -> &str {
        self.label
    }

    fn files(&self) -> &Self::Files {
        &self.files
    }

    fn source_bytes(&self, objects: &[object_store::ObjectMeta]) -> Result<u64> {
        objects.iter().try_fold(0u64, |sum, object| {
            sum.checked_add(object.size)
                .context("S3 merge source size overflow")
        })
    }

    fn log_no_op(&self, sources: usize, source_bytes: u64, flush_bytes: u64, estimated: usize) {
        info!(
            table = self.table,
            partition = self.label,
            source_files = sources,
            source_bytes,
            flush_bytes,
            estimated_output_files = estimated,
            "skipping S3 partition merge because no file-count reduction is expected"
        );
    }

    fn schema_mismatch(&self, objects: &[object_store::ObjectMeta]) -> Result<Option<String>> {
        let mut schema_check = SchemaCheck::default();
        for window in read::windows(objects) {
            let window = window?;
            let schemas = read::schemas(self.s3.client, window)?;
            for (obj, schema) in window.iter().zip(schemas) {
                let name = obj.location.filename().unwrap_or(obj.location.as_ref());
                if let Some(reason) = schema_check.check(name, &schema) {
                    return Ok(Some(reason));
                }
            }
        }
        Ok(None)
    }

    fn log_start(&self, sources: usize, source_bytes: u64, config: &MergeConfig) {
        info!(
            table = self.table,
            partition = self.label,
            source_files = sources,
            source_bytes,
            flush_bytes = config.flush_bytes,
            flush_rows = config.flush_rows,
            dry_run = config.dry_run,
            "starting S3 partition merge"
        );
    }

    fn max_part_number(&self, objects: &[object_store::ObjectMeta]) -> u32 {
        max_part_number_in_s3_objects(objects)
    }

    fn claim_journal(
        &self,
        objects: &[object_store::ObjectMeta],
        initial_part_num: u32,
    ) -> Result<Journal> {
        self.assert_owner()?;
        new_journal(
            &self.s3.run,
            objects
                .iter()
                .filter_map(|obj| obj.location.filename().map(str::to_string))
                .collect(),
            initial_part_num,
            self.s3.protected_roots,
            &format!("s3://{}/{}", self.s3.bucket, self.key),
        )
    }

    fn changed_source(&self, _objects: &[object_store::ObjectMeta]) -> Option<String> {
        None
    }

    fn encode<F>(
        &self,
        objects: &[object_store::ObjectMeta],
        encoder: &mut Encoder,
        publish: &mut F,
    ) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        for window in read::windows(objects) {
            // The complete compressed-byte reservation remains held until every
            // returned object has been consumed in order; no next window starts early.
            let data_window = read::objects(self.s3.client, window?)?;
            for data in data_window {
                let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
                encoder.write_reader(builder, publish, None)?;
            }
        }
        Ok(())
    }

    fn publish(&self, name: &str, data: Vec<u8>, _rows: usize) -> Result<()> {
        let (client, bucket) = (self.s3.client, self.s3.bucket);
        let s3_path = object_store::path::Path::from(format!("{}/{name}", self.key));
        let size = data.len();
        put_s3_bytes_once(
            client,
            bucket,
            &s3_path,
            bytes::Bytes::from(data),
            self.table,
            self.label,
            &self.config.cache_control,
        )?;
        if self.config.verbose {
            info!(
                operation = "upload",
                s3_key = %s3_path,
                s3_uri = %format!("s3://{bucket}/{s3_path}"),
                table = self.table,
                partition = self.label,
                bytes = size,
                "uploaded merged S3 part"
            );
        }
        Ok(())
    }

    fn check_owner(&self) -> Result<()> {
        self.assert_owner()
    }

    fn delete_sources(
        &self,
        objects: &[object_store::ObjectMeta],
        outputs: &[String],
    ) -> Result<()> {
        let bucket = self.s3.bucket;
        let written: HashSet<String> = outputs
            .iter()
            .map(|name| format!("{}/{name}", self.key))
            .collect();
        let source_keys = objects
            .iter()
            .filter(|object| !written.contains(object.location.as_ref()))
            .map(|object| object.location.clone())
            .collect::<Vec<_>>();
        block_on_async(crate::s3::delete::delete_objects_observed(
            self.s3.client,
            &source_keys,
            |key| {
                crash_point("after-first-delete")?;
                if self.config.verbose {
                    info!(operation = MergeS3Operation::Delete.as_str(), s3_key = %key,
                    s3_uri = %format!("s3://{bucket}/{key}"), table = self.table,
                    partition = self.label, "deleted source S3 part after merge");
                }
                Ok(())
            },
        ))
    }

    fn log_done(&self, sources: usize, outputs: usize, output_bytes: u64) {
        info!(
            table = self.table,
            partition = self.label,
            files_read = sources,
            files_written = outputs,
            output_bytes,
            "completed S3 partition merge"
        );
    }
}

// Data PUT/DELETE are single attempts on a zero-transport-retry client. An
// error stops the entire run with its persistent owner retained; a later success
// cannot prove an earlier abandoned request drained.
fn put_s3_bytes_once(
    client: &Arc<dyn ObjectStore>,
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
    // Read retries are safe. Never let a caller accidentally enable retries
    // for an unfenced data mutation, even if a future call passes a larger bound.
    let max_attempts = if operation == MergeS3Operation::Read {
        max_attempts.max(1)
    } else {
        1
    };
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Builder;
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;

    fn collect_parquet_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
        Ok(discovery::collect_local(
            dir,
            LocalPolicy::MUTATION_PARQUET,
            out,
        )?)
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
            MergeS3Operation::Read,
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
            .contains("failed deleting s3://bucket/blocks/part-000001.parquet after 1 attempts"));
        assert!(message.contains("table=blocks"));
        assert!(message.contains("partition=blocks/date=2026-03-18"));
        assert_eq!(attempts, 1);
    }

    const RESERVED: [&str; 5] = [
        "cursor.parquet",
        "merkle_roots.parquet",
        "partitions.parquet",
        "verify_runs/run-1/a.parquet",
        "verify_runs/run-1/b.parquet",
    ];
    const DAY: &str = "blocks/year=2024/month=01/date=15";

    fn test_merge_config(path: &str) -> MergeConfig {
        MergeConfig {
            path: path.to_string(),
            compression: Compression::None,
            flush_rows: None,
            flush_bytes: 0,
            dry_run: false,
            verbose: false,
            aws: None,
            cache_control: String::new(),
        }
    }

    fn parquet_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        buf
    }

    fn put_object(store: &Arc<dyn ObjectStore>, key: &str, data: Vec<u8>) {
        let path = object_store::path::Path::from(key);
        block_on_async(store.put(&path, data.into())).unwrap();
    }

    fn get_object(store: &Arc<dyn ObjectStore>, key: &str) -> bytes::Bytes {
        let path = object_store::path::Path::from(key);
        block_on_async(async { store.get(&path).await?.bytes().await }).unwrap()
    }

    fn list_keys(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
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

    fn object_row_count(store: &Arc<dyn ObjectStore>, key: &str) -> usize {
        let builder = ParquetRecordBatchReaderBuilder::try_new(get_object(store, key)).unwrap();
        builder.metadata().file_metadata().num_rows() as usize
    }

    /// Merging a network root used to merge `cursor.parquet`, `partitions.parquet`, and
    /// `merkle_roots.parquet` into a root `part-000001.parquet` and delete them.
    #[test]
    fn test_merge_network_root_leaves_reserved_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for rel in RESERVED {
            let path = root.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            write_test_parquet_with_metadata(&path, &make_test_batch(3), vec![]);
        }
        let snapshot: Vec<Vec<u8>> = RESERVED
            .iter()
            .map(|rel| std::fs::read(root.join(rel)).unwrap())
            .collect();
        let partition = root.join(DAY);
        std::fs::create_dir_all(&partition).unwrap();
        write_test_parquet_with_metadata(
            &partition.join("part-000001.parquet"),
            &make_test_batch(10),
            vec![],
        );
        write_test_parquet_with_metadata(
            &partition.join("part-000002.parquet"),
            &make_test_batch(20),
            vec![],
        );

        let result = run_merge(&test_merge_config(&root.to_string_lossy())).unwrap();
        assert_eq!(result.partitions_merged, 1);
        assert_eq!(result.files_read, 2);

        for (rel, bytes) in RESERVED.iter().zip(&snapshot) {
            assert_eq!(&std::fs::read(root.join(rel)).unwrap(), bytes, "{rel}");
        }
        let mut root_files: Vec<String> = std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_file())
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        root_files.sort();
        assert_eq!(root_files, RESERVED[..3].to_vec());

        let mut merged = Vec::new();
        collect_parquet_files_recursive(&partition, &mut merged).unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(read_parquet_row_count(&merged[0]), 30);
    }

    #[test]
    fn test_merge_s3_network_root_leaves_reserved_artifacts() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for rel in RESERVED {
            put_object(
                &store,
                &format!("mainnet/{rel}"),
                parquet_bytes(&make_test_batch(3)),
            );
        }
        let snapshot: Vec<bytes::Bytes> = RESERVED
            .iter()
            .map(|rel| get_object(&store, &format!("mainnet/{rel}")))
            .collect();
        put_object(
            &store,
            &format!("mainnet/{DAY}/part-000001.parquet"),
            parquet_bytes(&make_test_batch(10)),
        );
        put_object(
            &store,
            &format!("mainnet/{DAY}/part-000002.parquet"),
            parquet_bytes(&make_test_batch(20)),
        );

        let config = test_merge_config("s3://bucket/mainnet");
        let result = merge_s3(&config, &store, "bucket", "mainnet").unwrap();
        assert_eq!(result.partitions_merged, 1);
        assert_eq!(result.files_read, 2);

        for (rel, bytes) in RESERVED.iter().zip(&snapshot) {
            assert_eq!(
                &get_object(&store, &format!("mainnet/{rel}")),
                bytes,
                "{rel}"
            );
        }
        let blocks = list_keys(&store, "mainnet/blocks");
        assert_eq!(blocks, vec![format!("mainnet/{DAY}/part-000003.parquet")]);
        assert_eq!(object_row_count(&store, &blocks[0]), 30);
        let mut others = list_keys(&store, "mainnet");
        others.retain(|key| !key.starts_with("mainnet/blocks/"));
        let expected: Vec<String> = RESERVED
            .iter()
            .map(|rel| format!("mainnet/{rel}"))
            .collect();
        assert_eq!(others, expected);
    }

    /// A one-row batch of non-nullable UInt64 columns, in the given order.
    fn make_columns_batch(columns: &[(&str, u64)]) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::UInt64, false))
            .collect();
        let arrays: Vec<arrow::array::ArrayRef> = columns
            .iter()
            .map(|(_, value)| {
                Arc::new(arrow::array::UInt64Array::from(vec![*value])) as arrow::array::ArrayRef
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    fn write_columns_file(path: &Path, columns: &[(&str, u64)]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        write_test_parquet_with_metadata(path, &make_columns_batch(columns), vec![]);
    }

    /// `(file name, bytes)` of every file under `dir`, sorted.
    fn snapshot_dir(dir: &Path) -> Vec<(String, Vec<u8>)> {
        let mut files = Vec::new();
        collect_parquet_files_recursive(dir, &mut files).unwrap();
        files.sort();
        files
            .iter()
            .map(|f| (file_name_string(f), std::fs::read(f).unwrap()))
            .collect()
    }

    #[test]
    fn test_describe_schema_mismatch() {
        let field =
            |name: &str, data_type: DataType, nullable: bool| Field::new(name, data_type, nullable);
        let a = field("a", DataType::UInt64, false);
        let b = field("b", DataType::UInt64, false);
        let c = field("c", DataType::Utf8, true);
        let ab = Schema::new(vec![a.clone(), b.clone()]);

        assert_eq!(describe_schema_mismatch(&ab, &ab.clone()), None);
        // Field metadata does not affect how columns are paired.
        let a_with_metadata = a.clone().with_metadata(
            [("comment".to_string(), "x".to_string())]
                .into_iter()
                .collect::<arrow::datatypes::Metadata>(),
        );
        assert_eq!(
            describe_schema_mismatch(&ab, &Schema::new(vec![a_with_metadata, b.clone()])),
            None
        );

        let ba = Schema::new(vec![b.clone(), a.clone()]);
        assert_eq!(
            describe_schema_mismatch(&ab, &ba).unwrap(),
            "columns are in a different order (column 1 is `b` instead of `a`)"
        );
        let abc = Schema::new(vec![a.clone(), b.clone(), c.clone()]);
        assert_eq!(
            describe_schema_mismatch(&ab, &abc).unwrap(),
            "extra column `c`"
        );
        assert_eq!(
            describe_schema_mismatch(&abc, &ab).unwrap(),
            "missing column `c`"
        );
        let a_int64 = Schema::new(vec![field("a", DataType::Int64, false), b.clone()]);
        assert_eq!(
            describe_schema_mismatch(&ab, &a_int64).unwrap(),
            "column `a` is Int64 instead of UInt64"
        );
        let a_nullable = Schema::new(vec![field("a", DataType::UInt64, true), b.clone()]);
        assert_eq!(
            describe_schema_mismatch(&ab, &a_nullable).unwrap(),
            "column `a` is nullable instead of non-nullable"
        );
        let renamed = Schema::new(vec![a.clone(), field("b2", DataType::UInt64, false)]);
        assert_eq!(
            describe_schema_mismatch(&ab, &renamed).unwrap(),
            "missing column `b`; extra column `b2`"
        );
    }

    /// A partition whose parts list the same columns in a different order used to be merged
    /// by position (a=20, b=2 instead of a=2, b=20) and its sources deleted.
    #[test]
    fn test_merge_skips_partition_with_reordered_columns() {
        let dir = tempfile::tempdir().unwrap();
        let mixed = dir.path().join("blocks/year=2024/month=01/date=15");
        let healthy = dir.path().join("blocks/year=2024/month=01/date=16");
        write_columns_file(&mixed.join("part-000001.parquet"), &[("a", 1), ("b", 10)]);
        write_columns_file(&mixed.join("part-000002.parquet"), &[("b", 20), ("a", 2)]);
        write_columns_file(&healthy.join("part-000001.parquet"), &[("a", 3), ("b", 30)]);
        write_columns_file(&healthy.join("part-000002.parquet"), &[("a", 4), ("b", 40)]);
        let before = snapshot_dir(&mixed);

        let result = run_merge(&test_merge_config(&dir.path().to_string_lossy())).unwrap();

        assert_eq!(result.partitions_merged, 1);
        assert_eq!(result.schema_mismatches.len(), 1, "{result:?}");
        let mismatch = &result.schema_mismatches[0];
        assert!(mismatch.contains("date=15"), "{mismatch}");
        assert!(
            mismatch.contains("part-000002.parquet does not match part-000001.parquet"),
            "{mismatch}"
        );
        assert!(mismatch.contains("different order"), "{mismatch}");
        assert_eq!(snapshot_dir(&mixed), before);

        let mut merged = Vec::new();
        collect_parquet_files_recursive(&healthy, &mut merged).unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(read_parquet_row_count(&merged[0]), 2);
    }

    /// An extra column used to be dropped silently.
    #[test]
    fn test_merge_skips_partition_with_extra_column() {
        let dir = tempfile::tempdir().unwrap();
        let mixed = dir.path().join("blocks/year=2024/month=01/date=15");
        write_columns_file(&mixed.join("part-000001.parquet"), &[("a", 1), ("b", 10)]);
        write_columns_file(
            &mixed.join("part-000002.parquet"),
            &[("a", 2), ("b", 20), ("c", 200)],
        );
        let before = snapshot_dir(&mixed);

        let result = run_merge(&test_merge_config(&dir.path().to_string_lossy())).unwrap();

        assert_eq!(result.partitions_merged, 0);
        assert_eq!(result.files_written, 0);
        assert_eq!(result.schema_mismatches.len(), 1);
        assert!(result.schema_mismatches[0].contains("extra column `c`"));
        assert_eq!(snapshot_dir(&mixed), before);
    }

    #[test]
    fn test_merge_dry_run_reports_schema_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let mixed = dir.path().join("blocks/year=2024/month=01/date=15");
        write_columns_file(&mixed.join("part-000001.parquet"), &[("a", 1), ("b", 10)]);
        write_columns_file(&mixed.join("part-000002.parquet"), &[("b", 20), ("a", 2)]);

        let mut config = test_merge_config(&dir.path().to_string_lossy());
        config.dry_run = true;
        let result = run_merge(&config).unwrap();

        assert_eq!(result.partitions_merged, 0);
        assert_eq!(result.schema_mismatches.len(), 1);
    }

    #[test]
    fn test_merge_s3_skips_partition_with_mixed_schemas() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mixed = "evm/blocks/year=2024/month=01/date=15";
        let healthy = "evm/blocks/year=2024/month=01/date=16";
        let put = |key: String, columns: &[(&str, u64)]| {
            put_object(&store, &key, parquet_bytes(&make_columns_batch(columns)));
        };
        put(
            format!("{mixed}/part-000001.parquet"),
            &[("a", 1), ("b", 10)],
        );
        put(
            format!("{mixed}/part-000002.parquet"),
            &[("b", 20), ("a", 2)],
        );
        put(
            format!("{healthy}/part-000001.parquet"),
            &[("a", 3), ("b", 30)],
        );
        put(
            format!("{healthy}/part-000002.parquet"),
            &[("a", 4), ("b", 40)],
        );
        let mixed_keys = list_keys(&store, mixed);
        let before: Vec<bytes::Bytes> = mixed_keys
            .iter()
            .map(|key| get_object(&store, key))
            .collect();

        let config = test_merge_config("s3://bucket/evm");
        let result = merge_s3(&config, &store, "bucket", "evm").unwrap();

        assert_eq!(result.partitions_merged, 1);
        assert_eq!(result.schema_mismatches.len(), 1);
        assert!(result.schema_mismatches[0].contains("different order"));
        assert_eq!(list_keys(&store, mixed), mixed_keys);
        for (key, bytes) in mixed_keys.iter().zip(&before) {
            assert_eq!(&get_object(&store, key), bytes, "{key}");
        }
        let merged = list_keys(&store, healthy);
        assert_eq!(merged, vec![format!("{healthy}/part-000003.parquet")]);
        assert_eq!(object_row_count(&store, &merged[0]), 2);
    }

    #[test]
    fn test_read_s3_arrow_schema_fetches_the_whole_footer() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let names: Vec<String> = (0..200).map(|i| format!("column_{i}")).collect();
        let columns: Vec<(&str, u64)> = names.iter().map(|name| (name.as_str(), 1)).collect();
        let batch = make_columns_batch(&columns);
        put_object(&store, "t/part-000001.parquet", parquet_bytes(&batch));
        let meta =
            block_on_async(store.head(&object_store::path::Path::from("t/part-000001.parquet")))
                .unwrap();

        // A 16-byte prefetch holds only the footer tail, so the metadata is fetched again.
        for prefetch in [16, S3_FOOTER_PREFETCH_BYTES] {
            let schema = block_on_async(read::arrow_schema(&store, &meta, prefetch)).unwrap();
            assert_eq!(
                schema.fields(),
                batch.schema().fields(),
                "prefetch={prefetch}"
            );
        }
    }

    // -- Crash safety (#480) --

    use crate::merge_journal::{
        Journal, LocalPartition, LocalRunLock, PartitionFiles, RunContext, INJECTED_CRASH,
        JOURNAL_FILE, LOCK_FILE,
    };

    /// Makes `crash_point(step)` fail on this thread while the guard lives.
    struct InjectCrash;

    impl InjectCrash {
        fn at(step: &'static str) -> Self {
            INJECTED_CRASH.with(|crash| *crash.borrow_mut() = Some(step));
            InjectCrash
        }
    }

    impl Drop for InjectCrash {
        fn drop(&mut self) {
            INJECTED_CRASH.with(|crash| *crash.borrow_mut() = None);
        }
    }

    const CRASH_PARTITION: &str = "blocks/year=2024/month=01/day=15";

    fn make_range_batch(start: u64, rows: u64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_number",
            DataType::UInt64,
            false,
        )]));
        let values = arrow::array::UInt64Array::from_iter_values(start..start + rows);
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    fn batch_values(batches: impl IntoIterator<Item = RecordBatch>) -> Vec<u64> {
        batches
            .into_iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// Every `block_number` in the `.parquet` files under `dir`, sorted.
    fn local_values(dir: &Path) -> Vec<u64> {
        let mut files = Vec::new();
        collect_parquet_files_recursive(dir, &mut files).unwrap();
        let mut values: Vec<u64> = files
            .iter()
            .flat_map(|f| {
                let reader =
                    ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(f).unwrap())
                        .unwrap()
                        .build()
                        .unwrap();
                batch_values(reader.map(|batch| batch.unwrap()))
            })
            .collect();
        values.sort_unstable();
        values
    }

    /// Names of every file directly in `dir` (hidden ones included), sorted.
    fn entries(dir: &Path) -> Vec<String> {
        LocalPartition::new(dir).list_names().unwrap()
    }

    /// A partition of three parts (`block_number` 0..30) whose merge crashed at `step`.
    fn crashed_local_merge(step: &'static str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let partition = dir.path().join(CRASH_PARTITION);
        std::fs::create_dir_all(&partition).unwrap();
        for part in 0..3u64 {
            write_test_parquet_with_metadata(
                &partition.join(format!("part-{:06}.parquet", part + 1)),
                &make_range_batch(part * 10, 10),
                vec![],
            );
        }
        let _crash = InjectCrash::at(step);
        let err = run_merge(&test_merge_config(&dir.path().to_string_lossy()))
            .unwrap_err()
            .to_string();
        assert!(err.contains(&format!("injected crash at {step}")), "{err}");
        (dir, partition)
    }

    /// A crash after the output was written and committed, before the sources were deleted,
    /// used to leave both, and the next merge folded them into one file (60 rows for 30).
    #[test]
    fn test_merge_recovers_a_crash_after_commit() {
        let (dir, partition) = crashed_local_merge("after-commit");
        assert_eq!(
            entries(&partition),
            vec![
                JOURNAL_FILE,
                "part-000001.parquet",
                "part-000002.parquet",
                "part-000003.parquet",
                "part-000004.parquet",
            ]
        );
        assert_eq!(
            local_values(&partition).len(),
            60,
            "sources and output coexist"
        );

        let result = run_merge(&test_merge_config(&dir.path().to_string_lossy())).unwrap();

        assert_eq!(result.merges_recovered, 1);
        assert_eq!(entries(&partition), vec!["part-000004.parquet"]);
        assert_eq!(local_values(&partition), (0..30).collect::<Vec<_>>());
        assert!(!dir.path().join(LOCK_FILE).exists());
    }

    #[test]
    fn test_merge_rolls_back_a_crash_before_commit() {
        let (dir, partition) = crashed_local_merge("after-outputs");
        let journal = LocalPartition::new(&partition)
            .read_journal()
            .unwrap()
            .unwrap();
        assert_eq!(journal.state, crate::merge_journal::JournalState::Writing);

        let result = run_merge(&test_merge_config(&dir.path().to_string_lossy())).unwrap();

        // The partial output was deleted, and the partition was merged again from its sources.
        assert_eq!(result.merges_recovered, 1);
        assert_eq!(result.partitions_merged, 1);
        assert_eq!(entries(&partition), vec!["part-000004.parquet"]);
        assert_eq!(local_values(&partition), (0..30).collect::<Vec<_>>());
    }

    #[test]
    fn test_merge_finishes_a_crash_between_source_deletes() {
        let (dir, partition) = crashed_local_merge("after-first-delete");
        assert_eq!(local_values(&partition).len(), 50);

        let result = run_merge(&test_merge_config(&dir.path().to_string_lossy())).unwrap();

        assert_eq!(result.merges_recovered, 1);
        assert_eq!(entries(&partition), vec!["part-000004.parquet"]);
        assert_eq!(local_values(&partition), (0..30).collect::<Vec<_>>());
    }

    #[test]
    fn test_merge_dry_run_leaves_an_interrupted_merge_alone() {
        let (dir, partition) = crashed_local_merge("after-commit");
        let before = entries(&partition);

        let mut config = test_merge_config(&dir.path().to_string_lossy());
        config.dry_run = true;
        let result = run_merge(&config).unwrap();

        assert_eq!(result.merges_recovered, 0);
        assert_eq!(entries(&partition), before);
    }

    #[test]
    fn test_merge_refuses_to_run_while_another_merge_holds_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let partition = dir.path().join(CRASH_PARTITION);
        std::fs::create_dir_all(&partition).unwrap();
        for part in 0..2u64 {
            write_test_parquet_with_metadata(
                &partition.join(format!("part-{:06}.parquet", part + 1)),
                &make_range_batch(part * 10, 10),
                vec![],
            );
        }
        let config = test_merge_config(&dir.path().to_string_lossy());

        let lock = LocalRunLock::acquire(dir.path()).unwrap();
        let err = run_merge(&config).unwrap_err().to_string();
        assert!(err.contains("another merge is running"), "{err}");
        assert_eq!(entries(&partition).len(), 2);

        lock.release().unwrap();
        assert_eq!(run_merge(&config).unwrap().partitions_merged, 1);
    }

    /// A merge started on an enclosing directory must not recover a partition that a live
    /// merge on a nested directory is working on.
    #[test]
    fn test_merge_leaves_a_partition_claimed_by_a_live_run() {
        let dir = tempfile::tempdir().unwrap();
        let partition = dir.path().join(CRASH_PARTITION);
        std::fs::create_dir_all(&partition).unwrap();
        for part in 0..2u64 {
            write_test_parquet_with_metadata(
                &partition.join(format!("part-{:06}.parquet", part + 1)),
                &make_range_batch(part * 10, 10),
                vec![],
            );
        }
        let live_lock = LocalRunLock::acquire(&dir.path().join("blocks")).unwrap();
        let live = RunContext {
            run_id: "live".to_string(),
            lock: live_lock.location(),
        };
        let journal = Journal::new(
            &live,
            vec![
                "part-000001.parquet".to_string(),
                "part-000002.parquet".to_string(),
            ],
            3,
        );
        assert!(LocalPartition::new(&partition)
            .create_journal(&journal)
            .unwrap());
        let config = test_merge_config(&dir.path().to_string_lossy());

        let result = run_merge(&config).unwrap();
        assert_eq!(result.merges_recovered, 0);
        assert_eq!(result.partitions_in_use, vec![CRASH_PARTITION.to_string()]);
        assert_eq!(entries(&partition).len(), 3);

        // Once that run is gone, its unfinished merge is rolled back and redone.
        drop(live_lock);
        let result = run_merge(&config).unwrap();
        assert_eq!(result.merges_recovered, 1);
        assert_eq!(result.partitions_merged, 1);
        assert_eq!(entries(&partition), vec!["part-000003.parquet"]);
        assert_eq!(local_values(&partition), (0..20).collect::<Vec<_>>());
    }

    fn s3_values(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<u64> {
        let mut values: Vec<u64> = list_keys(store, prefix)
            .iter()
            .filter(|key| key.ends_with(".parquet"))
            .flat_map(|key| {
                let reader = ParquetRecordBatchReaderBuilder::try_new(get_object(store, key))
                    .unwrap()
                    .build()
                    .unwrap();
                batch_values(reader.map(|batch| batch.unwrap()))
            })
            .collect();
        values.sort_unstable();
        values
    }

    fn crashed_s3_merge(step: &'static str) -> Arc<dyn ObjectStore> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for part in 0..3u64 {
            put_object(
                &store,
                &format!("evm/{CRASH_PARTITION}/part-{:06}.parquet", part + 1),
                parquet_bytes(&make_range_batch(part * 10, 10)),
            );
        }
        let _crash = InjectCrash::at(step);
        let err = merge_s3(
            &test_merge_config("s3://bucket/evm"),
            &store,
            "bucket",
            "evm",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains(&format!("injected crash at {step}")), "{err}");
        store
    }

    fn release_quiescent_fixture_owner(store: &Arc<dyn ObjectStore>) {
        // InMemory has no detached HTTP requests: the injected synchronous
        // failure returned after every prior store future completed. This proof
        // is specific to the fixture and cannot be inferred for a real bucket.
        let record = block_on_async(S3Ownership::status(store)).unwrap().unwrap();
        let authorization =
            crate::dataset_lock_s3::RecoveryAuthorization::assert_provider_quiescence(
                &record,
                "fixture writer returned from injected error",
                "InMemory fixture has no pending detached requests",
            )
            .unwrap();
        block_on_async(S3Ownership::operator_release(
            Arc::clone(store),
            &record,
            authorization,
        ))
        .unwrap();
    }

    #[test]
    fn test_merge_s3_recovers_a_crash_after_commit() {
        let store = crashed_s3_merge("after-commit");
        assert!(merge_s3(
            &test_merge_config("s3://bucket/evm"),
            &store,
            "bucket",
            "evm"
        )
        .is_err());
        release_quiescent_fixture_owner(&store);
        let partition = format!("evm/{CRASH_PARTITION}");
        assert_eq!(
            list_keys(&store, "evm"),
            vec![
                format!("{partition}/{JOURNAL_FILE}"),
                format!("{partition}/part-000001.parquet"),
                format!("{partition}/part-000002.parquet"),
                format!("{partition}/part-000003.parquet"),
                format!("{partition}/part-000004.parquet"),
            ]
        );

        let config = test_merge_config("s3://bucket/evm");
        let result = merge_s3(&config, &store, "bucket", "evm").unwrap();

        assert_eq!(result.merges_recovered, 1);
        assert_eq!(
            list_keys(&store, "evm"),
            vec![format!("{partition}/part-000004.parquet")]
        );
        assert_eq!(s3_values(&store, "evm"), (0..30).collect::<Vec<_>>());
    }

    #[test]
    fn test_merge_s3_rolls_back_a_crash_before_commit() {
        let store = crashed_s3_merge("after-outputs");
        release_quiescent_fixture_owner(&store);

        let config = test_merge_config("s3://bucket/evm");
        let result = merge_s3(&config, &store, "bucket", "evm").unwrap();

        assert_eq!(result.merges_recovered, 1);
        assert_eq!(result.partitions_merged, 1);
        assert_eq!(
            list_keys(&store, "evm"),
            vec![format!("evm/{CRASH_PARTITION}/part-000004.parquet")]
        );
        assert_eq!(s3_values(&store, "evm"), (0..30).collect::<Vec<_>>());
    }

    #[test]
    fn test_merge_s3_refuses_to_run_while_the_prefix_is_locked() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for part in 0..2u64 {
            put_object(
                &store,
                &format!("evm/{CRASH_PARTITION}/part-{:06}.parquet", part + 1),
                parquet_bytes(&make_range_batch(part * 10, 10)),
            );
        }
        let lock = block_on_async(S3Ownership::acquire(
            Arc::clone(&store),
            "merge",
            vec!["other-prefix".into()],
        ))
        .unwrap();
        let config = test_merge_config("s3://bucket/evm");

        let err = merge_s3(&config, &store, "bucket", "evm")
            .unwrap_err()
            .to_string();
        assert!(err.contains("ownership is held"), "{err}");

        block_on_async(lock.release()).unwrap();
        let result = merge_s3(&config, &store, "bucket", "evm").unwrap();
        assert_eq!(result.partitions_merged, 1);
        assert_eq!(list_keys(&store, "evm").len(), 1);
    }

    #[test]
    fn test_merge_s3_refuses_legacy_journal_without_quiescence_proof() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let key = format!("evm/{CRASH_PARTITION}");
        let partition = S3Partition {
            client: &store,
            bucket: "bucket",
            key: &key,
        };
        let journal = Journal::new(
            &RunContext {
                run_id: "legacy".into(),
                lock: "evm/.fireparq-merge.lock".into(),
            },
            vec!["part-000001.parquet".into()],
            2,
        );
        partition.create_journal(&journal).unwrap();
        let before = list_keys(&store, "evm");
        let error = merge_s3(
            &test_merge_config("s3://bucket/evm"),
            &store,
            "bucket",
            "evm",
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("legacy S3 merge journal"));
        assert_eq!(list_keys(&store, "evm"), before);
        assert_eq!(
            block_on_async(S3Ownership::status(&store))
                .unwrap()
                .unwrap()
                .state(),
            crate::dataset_lock_s3::OwnerState::Owned
        );
    }

    #[test]
    fn s3_late_footer_failure_precedes_any_journal_or_output_publication() {
        let fake = Arc::new(read::tests::Store::default());
        let store: Arc<dyn ObjectStore> = fake.clone();
        let partition = format!("evm/{CRASH_PARTITION}");
        let mut sources = Vec::new();
        for part in 1..=10 {
            let key = format!("{partition}/part-{part:06}.parquet");
            let data = if part == 10 {
                b"invalid later footer".to_vec()
            } else {
                parquet_bytes(&make_range_batch(part * 10, 10))
            };
            put_object(&store, &key, data.clone());
            sources.push((key, data));
        }
        assert!(merge_s3(
            &test_merge_config("s3://bucket/evm"),
            &store,
            "bucket",
            "evm"
        )
        .is_err());
        assert_eq!(list_keys(&store, "evm").len(), sources.len());
        for (key, data) in sources {
            assert_eq!(get_object(&store, &key).as_ref(), data);
        }
        assert!(
            block_on_async(store.head(&object_store::path::Path::from(format!(
                "{partition}/{JOURNAL_FILE}"
            ))))
            .is_err()
        );
        assert_eq!(
            block_on_async(S3Ownership::status(&store))
                .unwrap()
                .unwrap()
                .state(),
            crate::dataset_lock_s3::OwnerState::Owned
        );
    }

    #[test]
    fn s3_later_body_window_failure_retains_writing_and_recovery_replays_exact_rows() {
        use std::sync::atomic::Ordering;
        let partition = format!("evm/{CRASH_PARTITION}");
        let mut fake = read::tests::Store::default();
        fake.body_failure_key = Some(object_store::path::Path::from(format!(
            "{partition}/part-000005.parquet"
        )));
        fake.fault.store(10, Ordering::SeqCst);
        let fake = Arc::new(fake);
        let store: Arc<dyn ObjectStore> = fake.clone();
        let mut sources = Vec::new();
        for part in 0..8 {
            let key = format!("{partition}/part-{:06}.parquet", part + 1);
            let data = parquet_bytes(&make_range_batch(part * 10, 10));
            put_object(&store, &key, data.clone());
            sources.push((key, data));
        }
        let mut config = test_merge_config("s3://bucket/evm");
        config.flush_rows = Some(15);
        assert!(merge_s3(&config, &store, "bucket", "evm").is_err());
        let journal = S3Partition {
            client: &store,
            bucket: "bucket",
            key: &partition,
        }
        .read_journal()
        .unwrap()
        .unwrap();
        assert_eq!(journal.state, crate::merge_journal::JournalState::Writing);
        assert!(journal.outputs.is_empty());
        let keys = list_keys(&store, "evm");
        assert!(
            keys.len() > sources.len() + 1,
            "first window must have actually published partial output"
        );
        let failed_reads = fake
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.as_ref().ends_with("part-000005.parquet"))
            .count();
        assert_eq!(
            failed_reads, 6,
            "one footer plus five pinned read-only body attempts"
        );
        fake.fault.store(0, Ordering::SeqCst);
        for (key, data) in &sources {
            assert_eq!(get_object(&store, key).as_ref(), data);
        }
        release_quiescent_fixture_owner(&store);
        let result = merge_s3(&config, &store, "bucket", "evm").unwrap();
        assert_eq!(result.merges_recovered, 1);
        assert_eq!(s3_values(&store, "evm"), (0..80).collect::<Vec<_>>());
        assert!(!list_keys(&store, "evm")
            .iter()
            .any(|key| key.ends_with(JOURNAL_FILE)));
        assert_eq!(
            block_on_async(S3Ownership::status(&store))
                .unwrap()
                .unwrap()
                .state(),
            crate::dataset_lock_s3::OwnerState::Released
        );
    }

    #[test]
    fn s3_source_delete_failure_retains_committed_journal_and_owned_bucket() {
        use crate::s3::delete::test_store::DelayedStore;
        use std::sync::atomic::Ordering;
        let partition = format!("evm/{CRASH_PARTITION}");
        let mut fake = DelayedStore::new(std::time::Duration::from_millis(40));
        fake.fail = Some(object_store::path::Path::from(format!(
            "{partition}/part-000001.parquet"
        )));
        fake.lose_response = true;
        let fake = Arc::new(fake);
        let store: Arc<dyn ObjectStore> = fake.clone();
        for part in 0..20u64 {
            put_object(
                &store,
                &format!("{partition}/part-{:06}.parquet", part + 1),
                parquet_bytes(&make_range_batch(part * 10, 10)),
            );
        }
        assert!(merge_s3(
            &test_merge_config("s3://bucket/evm"),
            &store,
            "bucket",
            "evm"
        )
        .is_err());
        let journal = S3Partition {
            client: &store,
            bucket: "bucket",
            key: &partition,
        }
        .read_journal()
        .unwrap()
        .unwrap();
        assert_eq!(journal.state, crate::merge_journal::JournalState::Committed);
        assert_eq!(journal.sources.len(), 20);
        assert_eq!(journal.outputs, ["part-000021.parquet"]);
        assert_eq!(
            batch_values(
                ParquetRecordBatchReaderBuilder::try_new(get_object(
                    &store,
                    &format!("{partition}/part-000021.parquet")
                ))
                .unwrap()
                .build()
                .unwrap()
                .map(|batch| batch.unwrap())
            ),
            (0..200).collect::<Vec<_>>()
        );
        assert_eq!(fake.counters.started.lock().unwrap().len(), 10);
        assert_eq!(fake.counters.completed.load(Ordering::SeqCst), 10);
        assert_eq!(fake.counters.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            block_on_async(S3Ownership::status(&store))
                .unwrap()
                .unwrap()
                .state(),
            crate::dataset_lock_s3::OwnerState::Owned
        );
        assert!(
            block_on_async(store.head(&object_store::path::Path::from(format!(
                "{partition}/part-000020.parquet"
            ))))
            .is_ok()
        );
    }

    /// Local and S3 merges run the same partition engine, so the same parts merge into
    /// identically named, byte-identical outputs with identical summaries.
    #[test]
    fn local_and_s3_merges_publish_identical_parts() {
        let layout: &[(&str, &[(u64, u64)])] = &[
            ("blocks/day=01", &[(0, 400), (400, 350), (750, 600)]),
            ("blocks/day=02", &[(2000, 10), (2010, 20)]),
            ("blocks/day=03", &[(3000, 50)]),
            (
                "logs/day=01",
                &[(0, 900), (900, 900), (1800, 100), (1900, 5)],
            ),
        ];
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for (partition, parts) in layout {
            for (index, (start, rows)) in parts.iter().enumerate() {
                let name = format!("part-{:06}.parquet", index + 3);
                let data = parquet_bytes(&make_range_batch(*start, *rows));
                let path = dir.path().join(partition).join(&name);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, &data).unwrap();
                put_object(&store, &format!("evm/{partition}/{name}"), data);
            }
        }
        let mut config = test_merge_config(&dir.path().to_string_lossy());
        config.flush_rows = Some(700);
        let local = run_merge(&config).unwrap();
        config.path = "s3://bucket/evm".into();
        let remote = merge_s3(&config, &store, "bucket", "evm").unwrap();

        let summary = |r: &MergeResult| {
            (
                r.partitions_merged,
                r.partitions_skipped,
                r.files_read,
                r.files_written,
                r.bytes_before,
                r.bytes_after,
            )
        };
        assert_eq!(summary(&local), summary(&remote));
        assert_eq!(local.partitions_merged, 3);
        for (partition, _) in layout {
            let local_dir = dir.path().join(partition);
            let local_files: Vec<(String, Vec<u8>)> = entries(&local_dir)
                .into_iter()
                .map(|name| {
                    let data = std::fs::read(local_dir.join(&name)).unwrap();
                    (name, data)
                })
                .collect();
            let remote_files: Vec<(String, Vec<u8>)> =
                list_keys(&store, &format!("evm/{partition}"))
                    .into_iter()
                    .map(|key| {
                        let data = get_object(&store, &key).to_vec();
                        (key.rsplit_once('/').unwrap().1.to_string(), data)
                    })
                    .collect();
            assert!(!local_files.is_empty());
            assert!(local_files == remote_files, "{partition}");
        }
    }

    #[tokio::test]
    async fn remote_recovery_data_error_never_removes_journal_or_other_phase_keys() {
        use crate::merge_journal::JournalState;
        use crate::s3::delete::test_store::DelayedStore;
        use std::sync::atomic::Ordering;
        for committed in [false, true] {
            let mut fake = DelayedStore::new(std::time::Duration::from_millis(40));
            let delete_part = if committed { 1 } else { 31 };
            fake.fail = Some(object_store::path::Path::from(format!(
                "blocks/part-{delete_part:06}.parquet"
            )));
            fake.lose_response = true;
            let fake = Arc::new(fake);
            let store: Arc<dyn ObjectStore> = fake.clone();
            let owner = S3Ownership::acquire(store.clone(), "recover-test", vec!["blocks".into()])
                .await
                .unwrap();
            let sources = (1..=30)
                .map(|part| format!("part-{part:06}.parquet"))
                .collect::<Vec<_>>();
            let outputs = (31..=60)
                .map(|part| format!("part-{part:06}.parquet"))
                .collect::<Vec<_>>();
            let writing = Journal::new(
                &RunContext {
                    run_id: owner.record().owner_id().into(),
                    lock: OWNER_KEY.into(),
                },
                sources.clone(),
                31,
            );
            let journal = if committed {
                writing.committed(outputs.clone())
            } else {
                writing
            };
            assert_eq!(
                journal.state,
                if committed {
                    JournalState::Committed
                } else {
                    JournalState::Writing
                }
            );
            for name in sources.iter().chain(&outputs) {
                store
                    .put(
                        &object_store::path::Path::from(format!("blocks/{name}")),
                        bytes::Bytes::from_static(b"retained").into(),
                    )
                    .await
                    .unwrap();
            }
            let journal_key = object_store::path::Path::from(format!("blocks/{JOURNAL_FILE}"));
            let journal_bytes = bytes::Bytes::from(serde_json::to_vec(&journal).unwrap());
            store
                .put(&journal_key, journal_bytes.clone().into())
                .await
                .unwrap();
            assert!(recover_remote_journal(&owner, "blocks", &journal)
                .await
                .is_err());
            assert!(owner.is_mutation_uncertain());
            assert_eq!(
                store
                    .get(&journal_key)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                journal_bytes
            );
            let retained = if committed { &outputs } else { &sources };
            for name in retained {
                assert!(store
                    .head(&object_store::path::Path::from(format!("blocks/{name}")))
                    .await
                    .is_ok());
            }
            assert_eq!(fake.counters.started.lock().unwrap().len(), 10);
            assert_eq!(fake.counters.completed.load(Ordering::SeqCst), 10);
            assert_eq!(fake.counters.active.load(Ordering::SeqCst), 0);
            assert!(owner.release().await.is_err());
            assert_eq!(
                S3Ownership::status(&store).await.unwrap().unwrap().state(),
                crate::dataset_lock_s3::OwnerState::Owned
            );
        }
    }
}

/// Finish protected compaction under the caller's already-held common owner.
/// No file listing or recovery here grants permission to acquire another owner.
pub(crate) async fn recover_guarded_for_ingestion(
    output: &crate::ingest::state::StorageIdentity,
    ownership: &DatasetOwnership,
    protected: Option<&crate::ingest::state::Digest>,
) -> Result<usize> {
    use crate::ingest::state::StorageIdentity;
    match output {
        StorageIdentity::Local { canonical_root } => {
            let root = Path::new(canonical_root);
            if !root.exists() {
                return Ok(0);
            }
            ownership.revalidate_local_paths()?;
            let owner = ownership
                .local()
                .context("merge recovery has no local owner")?;

            let mut paths = Vec::new();
            discovery::collect_local(root, LocalPolicy::merge_journals(JOURNAL_FILE), &mut paths)?;
            paths.sort();
            if paths.is_empty() {
                return Ok(0);
            }
            let states = crate::ingest::store::TransactionStateStore::local(root, owner)?;
            anyhow::ensure!(
                states.load().await?.pending.is_none(),
                "ingestion and merge journals coexist; refusing ambiguous recovery ordering"
            );
            let lock = LocalRunLock::acquire(root)?;
            let mut journals = Vec::new();
            for path in paths {
                let partition =
                    LocalPartition::new(path.parent().context("merge journal has no partition")?);
                let journal = partition
                    .read_journal()?
                    .context("merge journal disappeared under ownership")?;
                journal.validate_protection(protected)?;
                anyhow::ensure!(
                    !lock.owner_alive(&journal)?,
                    "an earlier merge is still active; protected recovery cannot continue"
                );
                journals.push((partition, journal));
            }
            let recovered = journals.len();
            for (partition, journal) in journals {
                ownership.revalidate_local_paths()?;
                crate::merge_journal::recover(&partition, &journal)?;
            }
            lock.release()?;
            Ok(recovered)
        }
        StorageIdentity::S3 { bucket, prefix, .. } => {
            let owner = ownership
                .remote(bucket)
                .context("merge recovery has no bucket owner")?;

            let client = owner.object_store();
            let list_prefix =
                (!prefix.is_empty()).then(|| object_store::path::Path::from(prefix.as_str()));
            use futures::StreamExt;
            let journals=tokio::time::timeout(std::time::Duration::from_secs(60),async {
                let mut stream=client.list(list_prefix.as_ref()); let mut journals=Vec::new();
                while let Some(object)=stream.next().await {
                    let object=object.map_err(|_|anyhow::anyhow!("listing guarded merge journals failed"))?;
                    let key=object.location.as_ref();
                    if !(prefix.is_empty() || key==prefix || key.strip_prefix(prefix).is_some_and(|tail|tail.starts_with('/'))) {continue;}
                    if object.location.filename()!=Some(JOURNAL_FILE) || crate::artifacts::is_control_path(key) {continue;}
                    let journal=crate::merge_journal::read_remote_journal(client,&object.location).await?.context("merge journal disappeared under ownership")?;
                    journal.validate_protection(protected)?;
                    anyhow::ensure!(journal.lock==OWNER_KEY,"legacy S3 merge ownership cannot prove prior request quiescence; preserve its journal for explicit recovery");
                    let directory=key.rsplit_once('/').map_or("",|(directory,_)|directory).to_owned();
                    journals.push((directory,journal));
                }
                Ok::<_,anyhow::Error>(journals)
            }).await.map_err(|_|anyhow::anyhow!("guarded merge journal discovery timed out"))??;
            if journals.is_empty() {
                return Ok(0);
            }
            let states = crate::ingest::store::TransactionStateStore::s3(prefix, owner)?;
            anyhow::ensure!(
                states.load().await?.pending.is_none(),
                "ingestion and merge journals coexist; refusing ambiguous recovery ordering"
            );
            let recovered = journals.len();
            for (directory, journal) in journals {
                recover_remote_journal(owner, &directory, &journal).await?;
            }
            Ok(recovered)
        }
    }
}

async fn recover_remote_journal(
    owner: &S3Ownership,
    directory: &str,
    journal: &Journal,
) -> Result<()> {
    use crate::merge_journal::JournalState;
    let _mutation = owner.lock_control_mutation().await;
    anyhow::ensure!(
        !owner.is_mutation_uncertain()
            && S3Ownership::status(owner.object_store()).await?.as_ref() == Some(owner.record()),
        "remote merge recovery requires resolved provider-quiescent ownership"
    );
    let client = owner.object_store();
    let prefix = (!directory.is_empty()).then(|| object_store::path::Path::from(directory));
    let objects = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        client.list_with_delimiter(prefix.as_ref()),
    )
    .await
    .map_err(|_| anyhow::anyhow!("listing merge recovery partition timed out"))?
    .map_err(|_| anyhow::anyhow!("listing merge recovery partition failed"))?;
    let names: std::collections::HashSet<_> = objects
        .objects
        .iter()
        .filter_map(|object| object.location.filename().map(str::to_owned))
        .collect();
    let mut remove = Vec::new();
    match journal.state {
        JournalState::Writing => {
            let suffix = format!(".{}.tmp", journal.run_id);
            for name in &names {
                if (parse_part_number(name).is_some_and(|part| part >= journal.first_output_part)
                    && !journal.sources.contains(name))
                    || (name.starts_with('.') && name.ends_with(&suffix))
                {
                    remove.push(name.clone());
                }
            }
        }
        JournalState::Committed => {
            anyhow::ensure!(
                journal.outputs.iter().all(|name| names.contains(name)),
                "committed merge output is missing; source files were preserved"
            );
            remove.extend(
                journal
                    .sources
                    .iter()
                    .filter(|name| names.contains(*name))
                    .cloned(),
            );
        }
    }
    remove.sort();
    struct Attempt<'a> {
        owner: &'a S3Ownership,
        resolved: bool,
    }
    impl Drop for Attempt<'_> {
        fn drop(&mut self) {
            if !self.resolved {
                self.owner.mark_mutation_uncertain();
            }
        }
    }
    let path = |name: &str| {
        object_store::path::Path::from(if directory.is_empty() {
            name.to_owned()
        } else {
            format!("{directory}/{name}")
        })
    };
    let keys = remove.iter().map(|name| path(name)).collect::<Vec<_>>();
    let mut data_attempt = Attempt {
        owner,
        resolved: false,
    };
    crate::s3::delete::delete_objects_once(client, &keys).await?;
    data_attempt.resolved = true;
    // The journal is a distinct final barrier, never part of concurrent data cleanup.
    let mut journal_attempt = Attempt {
        owner,
        resolved: false,
    };
    match tokio::time::timeout(
        std::time::Duration::from_secs(60),
        client.delete(&path(JOURNAL_FILE)),
    )
    .await
    {
        Ok(Ok(())) | Ok(Err(object_store::Error::NotFound { .. })) => {
            journal_attempt.resolved = true
        }
        _ => anyhow::bail!(
            "remote merge cleanup was unresolved; retain ownership for provider-quiescent recovery"
        ),
    }
    Ok(())
}

fn protected_stream_for_path(
    roots: &[ProtectedRoot],
    path: &str,
) -> Result<Option<crate::ingest::state::Digest>> {
    let canonical = if path.starts_with("s3://") {
        path.to_owned()
    } else {
        std::fs::canonicalize(path)?.to_string_lossy().into_owned()
    };
    let mut matched = None;
    for root in roots {
        let base = crate::ingest::binding::output_path(&root.identity);
        let within = if base.starts_with("s3://") {
            canonical == base
                || canonical
                    .strip_prefix(&base)
                    .is_some_and(|tail| tail.starts_with('/'))
        } else {
            Path::new(&canonical).starts_with(&base)
        };
        if within {
            anyhow::ensure!(
                matched.is_none(),
                "partition overlaps multiple protected streams"
            );
            matched = Some(root.descriptor.id()?);
        }
    }
    Ok(matched)
}
