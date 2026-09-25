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
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, info_span, warn};

mod read;

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

// A separate finite active-row-group budget preserves useful dictionaries even
// when the encoded output target is small. It never scales with the input group.
const ROW_GROUP_MEMORY_BUDGET_BYTES: usize = 32 * 1024 * 1024;

/// Buffers one encoded output part plus a separately bounded active row group.
pub(crate) struct StreamingPartWriter {
    schema: Arc<arrow::datatypes::Schema>,
    props: WriterProperties,
    flush_bytes: u64,
    row_group_memory_bytes: usize,
    flush_rows: Option<usize>,
    next_part_num: u32,
    current_writer: Option<ArrowWriter<Vec<u8>>>,
    current_rows: usize,
}

impl StreamingPartWriter {
    pub(crate) fn new(
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
            row_group_memory_bytes: ROW_GROUP_MEMORY_BUDGET_BYTES,
            // Treat an explicit zero like the disabled default so merge only flushes on rows when
            // the operator provides a positive threshold.
            flush_rows: flush_rows
                .filter(|rows| *rows > 0)
                .map(|rows| rows as usize),
            next_part_num: initial_part_num,
            current_writer: None,
            current_rows: 0,
        }
    }

    pub(crate) fn write_batch<F>(&mut self, batch: &RecordBatch, flush_part: &mut F) -> Result<()>
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
        self.current_rows = self
            .current_rows
            .checked_add(batch.num_rows())
            .context("output row count overflow")?;

        let reached_flush_rows = self
            .flush_rows
            .is_some_and(|flush_rows| self.current_rows >= flush_rows);
        // Bound the active Arrow/dictionary buffers independently of encoded
        // output. Closing only the row group preserves the compressed part
        // target rather than turning every memory-bound batch into a tiny file.
        if writer.memory_size() >= self.row_group_memory_bytes {
            writer.flush()?;
        }
        // Already encoded row groups remain in the output Vec and must count.
        let encoded = writer
            .bytes_written()
            .saturating_add(writer.in_progress_size());
        let reached_flush_bytes = self.flush_bytes > 0 && encoded as u64 >= self.flush_bytes;

        if reached_flush_rows || reached_flush_bytes {
            self.flush_current(flush_part)?;
        }

        Ok(())
    }

    pub(crate) fn finish<F>(&mut self, flush_part: &mut F) -> Result<()>
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
        let rows = self.current_rows;
        self.current_rows = 0;
        let buf = writer.into_inner()?;
        self.next_part_num = self
            .next_part_num
            .checked_add(1)
            .context("merge part number exhausted")?;
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

/// Describes how `other` differs from `reference`, or returns `None` when both have the same
/// columns (name, type, and nullability) in the same order.
///
/// Arrow writers and `concat_batches` pair columns by position, so merge and rollup only
/// combine files when this returns `None`; otherwise columns would be dropped or swapped.
pub(crate) fn describe_schema_mismatch(reference: &Schema, other: &Schema) -> Option<String> {
    let same_field = |a: &Field, b: &Field| {
        a.name() == b.name() && a.data_type() == b.data_type() && a.is_nullable() == b.is_nullable()
    };
    let (reference_fields, other_fields) = (reference.fields(), other.fields());
    if reference_fields.len() == other_fields.len()
        && reference_fields
            .iter()
            .zip(other_fields.iter())
            .all(|(a, b)| same_field(a, b))
    {
        return None;
    }

    let nullability = |field: &Field| {
        if field.is_nullable() {
            "nullable"
        } else {
            "non-nullable"
        }
    };
    let mut problems = Vec::new();
    for field in reference_fields {
        match other.field_with_name(field.name()) {
            Err(_) => problems.push(format!("missing column `{}`", field.name())),
            Ok(o) if o.data_type() != field.data_type() => problems.push(format!(
                "column `{}` is {} instead of {}",
                field.name(),
                o.data_type(),
                field.data_type()
            )),
            Ok(o) if o.is_nullable() != field.is_nullable() => problems.push(format!(
                "column `{}` is {} instead of {}",
                field.name(),
                nullability(o),
                nullability(field)
            )),
            Ok(_) => {}
        }
    }
    for field in other_fields {
        if reference.field_with_name(field.name()).is_err() {
            problems.push(format!("extra column `{}`", field.name()));
        }
    }
    if problems.is_empty() {
        // Same columns, different positions (or duplicate names).
        let (position, (expected, found)) = reference_fields
            .iter()
            .zip(other_fields.iter())
            .enumerate()
            .find(|(_, (a, b))| !same_field(a, b))
            .expect("schemas differ, so some position differs");
        problems.push(format!(
            "columns are in a different order (column {} is `{}` instead of `{}`)",
            position + 1,
            found.name(),
            expected.name()
        ));
    }
    Some(problems.join("; "))
}

/// Remembers the schema of the first file in a partition and reports how later files differ.
#[derive(Default)]
pub(crate) struct SchemaCheck {
    reference: Option<(String, SchemaRef)>,
}

impl SchemaCheck {
    /// Records the first file's schema. For later files, returns how `schema` differs from it.
    pub(crate) fn check(&mut self, name: &str, schema: &SchemaRef) -> Option<String> {
        match &self.reference {
            None => {
                self.reference = Some((name.to_string(), Arc::clone(schema)));
                None
            }
            Some((reference_name, reference)) => describe_schema_mismatch(reference, schema)
                .map(|diff| format!("{name} does not match {reference_name}: {diff}")),
        }
    }
}

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
    collect_named_files_recursive(root, JOURNAL_FILE, &mut journals)?;
    journals.sort();
    for journal_path in journals {
        let dir = journal_path.parent().unwrap_or(root);
        let label = local_partition_label(root, dir);
        let partition = LocalPartition::new(dir);
        let Some(journal) = partition.read_journal()? else {
            continue;
        };
        let Some(lock) = lock else {
            println!(
                "  {label}: has an interrupted merge ({:?}); a real run recovers it first",
                journal.state
            );
            continue;
        };
        journal.validate_protection(
            protected_stream_for_path(protected_roots, &dir.to_string_lossy())?.as_ref(),
        )?;
        if lock.owner_alive(&journal)? {
            continue;
        }
        if let Some(ownership) = ownership {
            ownership.revalidate_local_paths()?;
        }
        let recovery = crate::merge_journal::recover(&partition, &journal)?;
        warn!(partition = label, run_id = journal.run_id, %recovery, "recovered an interrupted merge");
        println!("  {label}: {recovery}");
        result.merges_recovered += 1;
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
    collect_parquet_files_recursive(root, &mut all_files)?;
    all_files.retain(|file| {
        let rel = file.strip_prefix(root).unwrap_or(file).to_string_lossy();
        let reserved = is_reserved_artifact_path(&rel);
        if reserved {
            debug!(path = %file.display(), "skipping reserved dataset artifact");
        }
        !reserved
    });
    all_files.sort();

    if all_files.is_empty() {
        info!("no parquet files found in {}", root.display());
        return Ok(());
    }

    println!("Merging partitions in {} ...\n", root.display());

    let mut current_table: Option<String> = None;
    let mut current_partition: Option<PathBuf> = None;
    let mut current_files: Vec<PathBuf> = Vec::new();

    for file in all_files {
        let parent = file.parent().unwrap_or(root).to_path_buf();
        match &current_partition {
            Some(partition) if partition == &parent => current_files.push(file),
            Some(_) => {
                let partition = current_partition.take().expect("partition must exist");
                print_local_table_header(root, &partition, &mut current_table);
                process_local_partition(
                    root,
                    &partition,
                    &current_files,
                    config,
                    run,
                    ownership,
                    protected_roots,
                    result,
                )?;
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
        print_local_table_header(root, &partition, &mut current_table);
        process_local_partition(
            root,
            &partition,
            &current_files,
            config,
            run,
            ownership,
            protected_roots,
            result,
        )?;
    }

    Ok(())
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

fn process_local_partition(
    root: &Path,
    partition_dir: &Path,
    files: &[PathBuf],
    config: &MergeConfig,
    run: &RunContext,
    ownership: Option<&DatasetOwnership>,
    protected_roots: &[ProtectedRoot],
    result: &mut MergeResult,
) -> Result<()> {
    let partition_label = local_partition_label(root, partition_dir);

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

    // Check every part before writing anything, so a partition with mixed schemas is left
    // exactly as it was.
    let mut schema_check = SchemaCheck::default();
    for file in files {
        let schema = read_local_arrow_schema(file)?;
        if let Some(reason) = schema_check.check(&file_name_string(file), &schema) {
            record_schema_mismatch(&partition_label, reason, result);
            return Ok(());
        }
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

    // Claim the partition with a journal before writing anything; see `merge_journal`.
    let initial_part_num = max_part_number_in_local_files(files);
    let partition = LocalPartition::new(partition_dir);
    let journal = Journal::new(
        run,
        files.iter().map(|f| file_name_string(f)).collect(),
        initial_part_num
            .checked_add(1)
            .context("merge part number exhausted")?,
    )
    .with_protected_stream(
        protected_stream_for_path(protected_roots, &partition_dir.to_string_lossy())?.as_ref(),
    );
    if let Some(ownership) = ownership {
        ownership.revalidate_local_paths()?;
    }
    if !partition.create_journal(&journal)? {
        record_partition_in_use(&partition_label, result);
        return Ok(());
    }
    // A merge that finished just before the claim may have replaced these parts.
    if let Some(missing) = files.iter().find(|f| !f.exists()) {
        partition.remove_journal()?;
        println!(
            "  {partition_label}: skipped; {} changed while the merge started",
            missing.display()
        );
        result.partitions_skipped += 1;
        return Ok(());
    }

    let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
    let mut writer_state: Option<StreamingPartWriter> = None;
    let mut written_files: Vec<PathBuf> = Vec::new();
    let mut output_bytes = 0u64;
    let mut write_part = |part_num: u32, buf: Vec<u8>, rows: usize| -> Result<()> {
        let name = format!("part-{part_num:06}.parquet");
        let path = write_local_output(partition_dir, &name, &buf, &run.run_id)?;
        output_bytes += buf.len() as u64;
        info!(
            path = %path.display(),
            rows,
            bytes = buf.len(),
            "wrote merged part"
        );
        written_files.push(path);
        Ok(())
    };

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
            let batch = strip_transaction_metadata(batch_result?)?;
            if batch.num_rows() == 0 {
                continue;
            }

            if writer_state.is_none() {
                let props = writer_properties(
                    config.compression,
                    batch.schema().as_ref(),
                    file_kv_metadata.as_deref(),
                );
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
                .write_batch(&batch, &mut write_part)?;
        }
    }
    result.files_read += files.len();

    let Some(writer_state) = writer_state.as_mut() else {
        // Every part was empty: nothing to write, and nothing was changed.
        partition.remove_journal()?;
        result.partitions_skipped += 1;
        return Ok(());
    };
    writer_state.finish(&mut write_part)?;

    partition.sync()?;
    crash_point("after-outputs")?;
    let outputs = written_files.iter().map(|f| file_name_string(f)).collect();
    if let Some(ownership) = ownership {
        ownership.revalidate_local_paths()?;
    }
    partition.replace_journal(&journal.committed(outputs))?;
    crash_point("after-commit")?;

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
            crash_point("after-first-delete")?;
        }
    }
    partition.sync()?;
    partition.remove_journal()?;
    partition.sync()?;

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
    schema: &Schema,
    kv_metadata: Option<&[KeyValue]>,
) -> WriterProperties {
    let metadata = kv_metadata.map(|kvs| {
        kvs.iter()
            .filter(|kv| !kv.key.starts_with("fireparq.ingest.") && kv.key != "ARROW:schema")
            .cloned()
            .collect()
    });
    crate::writer::properties::for_schema(compression, schema, metadata)
}

fn collect_parquet_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if crate::artifacts::is_control_path(&path.to_string_lossy()) {
            continue;
        }
        if path.is_dir() {
            collect_parquet_files_recursive(&path, out)?;
        } else if path.extension().map_or(false, |ext| ext == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}

/// Recursively collect files named `name` under `dir`.
fn collect_named_files_recursive(dir: &Path, name: &str, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if crate::artifacts::is_control_path(&path.to_string_lossy()) {
            continue;
        }
        if path.is_dir() {
            collect_named_files_recursive(&path, name, out)?;
        } else if path.file_name().is_some_and(|file_name| file_name == name) {
            out.push(path);
        }
    }
    Ok(())
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
    let mut s3 = S3Merge {
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
    recover_s3_merges(&mut s3, &mut result)?;
    merge_s3_partitions(&mut s3, config, &mut result)?;
    Ok(result)
}

fn list_s3_objects(s3: &S3Merge<'_>) -> Result<Vec<object_store::ObjectMeta>> {
    use futures::TryStreamExt;

    let list_prefix = if s3.prefix.is_empty() {
        None
    } else {
        Some(object_store::path::Path::from(s3.prefix))
    };
    block_on_async(async { s3.client.list(list_prefix.as_ref()).try_collect().await })
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
fn recover_s3_merges(s3: &mut S3Merge<'_>, result: &mut MergeResult) -> Result<()> {
    let objects = list_s3_objects(s3)?;
    for obj in &objects {
        if obj.location.filename() != Some(JOURNAL_FILE)
            || crate::artifacts::is_control_path(obj.location.as_ref())
        {
            continue;
        }
        let key = obj.location.as_ref();
        let partition_key = key.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("");
        let label = relative_s3_key(s3.prefix, partition_key);
        let label = if label.is_empty() { "(root)" } else { label };
        let partition = S3Partition {
            client: s3.client,
            bucket: s3.bucket,
            key: partition_key,
        };
        let Some(journal) = partition.read_journal()? else {
            continue;
        };
        let Some(lock) = s3.lock.as_mut() else {
            println!(
                "  {label}: has an interrupted merge ({:?}); a real run recovers it first",
                journal.state
            );
            continue;
        };
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
        let recovery = crate::merge_journal::recover(&partition, &journal)?;
        warn!(partition = label, run_id = journal.run_id, %recovery, "recovered an interrupted merge");
        println!("  {label}: {recovery}");
        result.merges_recovered += 1;
    }
    Ok(())
}

fn merge_s3_partitions(
    s3: &mut S3Merge<'_>,
    config: &MergeConfig,
    result: &mut MergeResult,
) -> Result<()> {
    let prefix = s3.prefix;
    let mut parquet_objects: Vec<_> = list_s3_objects(s3)?
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .filter(|obj| {
            let key = obj.location.as_ref();
            let reserved = is_reserved_artifact_path(relative_s3_key(prefix, key));
            if reserved {
                debug!(path = %key, "skipping reserved dataset artifact");
            }
            !reserved
        })
        .collect();
    parquet_objects.sort_by(|a, b| a.location.cmp(&b.location));

    if parquet_objects.is_empty() {
        info!("no parquet files found in {}", config.path);
        return Ok(());
    }

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
                print_s3_table_header(prefix, &partition, &mut current_table);
                process_s3_partition(s3, &partition, &current_objects, config, result)?;
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
        print_s3_table_header(prefix, &partition, &mut current_table);
        process_s3_partition(s3, &partition, &current_objects, config, result)?;
    }

    Ok(())
}

fn process_s3_partition(
    s3: &mut S3Merge<'_>,
    partition_key: &str,
    objects: &[object_store::ObjectMeta],
    config: &MergeConfig,
    result: &mut MergeResult,
) -> Result<()> {
    let (client, bucket, prefix) = (s3.client, s3.bucket, s3.prefix);
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

    let source_bytes = objects.iter().try_fold(0u64, |sum, object| {
        sum.checked_add(object.size)
            .context("S3 merge source size overflow")
    })?;

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

    // Check every part's footer before writing anything, so a partition with mixed schemas is
    // left exactly as it was.
    let mut schema_check = SchemaCheck::default();
    for window in read::windows(objects) {
        let window = window?;
        let schemas = read::schemas(client, window)?;
        for (obj, schema) in window.iter().zip(schemas) {
            let name = obj.location.filename().unwrap_or(obj.location.as_ref());
            if let Some(reason) = schema_check.check(name, &schema) {
                record_schema_mismatch(partition_label, reason, result);
                return Ok(());
            }
        }
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

    // Claim the partition with a journal before writing anything; see `merge_journal`.
    let initial_part_num = max_part_number_in_s3_objects(objects);
    if let Some(lock) = s3.lock.as_mut() {
        assert_s3_ownership(lock)?;
    }
    let partition = S3Partition {
        client,
        bucket,
        key: partition_key,
    };
    let journal = Journal::new(
        &s3.run,
        objects
            .iter()
            .filter_map(|obj| obj.location.filename().map(str::to_string))
            .collect(),
        initial_part_num
            .checked_add(1)
            .context("merge part number exhausted")?,
    )
    .with_protected_stream(
        protected_stream_for_path(
            s3.protected_roots,
            &format!("s3://{bucket}/{partition_key}"),
        )?
        .as_ref(),
    );
    if !partition.create_journal(&journal)? {
        record_partition_in_use(partition_label, result);
        return Ok(());
    }

    let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
    let mut writer_state: Option<StreamingPartWriter> = None;
    let mut output_names: Vec<String> = Vec::new();
    let mut output_bytes = 0u64;
    let mut write_part = |part_num: u32, buf: Vec<u8>, _rows: usize| -> Result<()> {
        let name = format!("part-{part_num:06}.parquet");
        let s3_path = object_store::path::Path::from(format!("{partition_key}/{name}"));
        let size = buf.len();
        put_s3_bytes_once(
            client,
            bucket,
            &s3_path,
            bytes::Bytes::from(buf),
            table,
            partition_label,
            &config.cache_control,
        )?;
        output_names.push(name);
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
    };

    for window in read::windows(objects) {
        // The complete compressed-byte reservation remains held until every
        // returned object has been consumed in order; no next window starts early.
        let data_window = read::objects(client, window?)?;
        for data in data_window {
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
                let batch = strip_transaction_metadata(batch_result?)?;
                if batch.num_rows() == 0 {
                    continue;
                }

                if writer_state.is_none() {
                    let props = writer_properties(
                        config.compression,
                        batch.schema().as_ref(),
                        file_kv_metadata.as_deref(),
                    );
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
                    .write_batch(&batch, &mut write_part)?;
            }
        }
    }
    result.files_read += objects.len();

    let Some(writer_state) = writer_state.as_mut() else {
        // Every part was empty: nothing to write, and nothing was changed.
        partition.remove_journal()?;
        result.partitions_skipped += 1;
        return Ok(());
    };
    writer_state.finish(&mut write_part)?;

    crash_point("after-outputs")?;
    // A changed ownership record is an error, never a time-based takeover.
    if let Some(lock) = s3.lock.as_mut() {
        assert_s3_ownership(lock)?;
    }
    let files_written = output_names.len();
    let written: HashSet<String> = output_names
        .iter()
        .map(|name| format!("{partition_key}/{name}"))
        .collect();
    partition.replace_journal(&journal.committed(output_names))?;
    crash_point("after-commit")?;

    println!(
        "  {}: {} parts → {} file(s) ({})",
        partition_label,
        objects.len(),
        files_written,
        format_bytes(output_bytes),
    );

    let source_keys = objects
        .iter()
        .filter(|object| !written.contains(object.location.as_ref()))
        .map(|object| object.location.clone())
        .collect::<Vec<_>>();
    block_on_async(crate::s3::delete::delete_objects_observed(
        client,
        &source_keys,
        |key| {
            crash_point("after-first-delete")?;
            if config.verbose {
                info!(operation = MergeS3Operation::Delete.as_str(), s3_key = %key,
                s3_uri = %format!("s3://{bucket}/{key}"), table, partition = partition_label,
                "deleted source S3 part after merge");
            }
            Ok(())
        },
    ))?;
    partition.remove_journal()?;

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

/// `key` relative to the listed `prefix`.
fn relative_s3_key<'a>(prefix: &str, key: &'a str) -> &'a str {
    key.strip_prefix(prefix)
        .map(|s| s.trim_start_matches('/'))
        .unwrap_or(key)
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
    use object_store::memory::InMemory;

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
    fn streaming_writer_counts_closed_row_groups_for_row_and_byte_limits() {
        let batch = make_test_batch(10);
        for (bytes, rows) in [(0, Some(5)), (128, None)] {
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(2))
                .build();
            let mut writer = StreamingPartWriter::new(batch.schema(), props, bytes, rows, 0);
            let mut emitted = Vec::new();
            let mut output = |part, data: Vec<u8>, rows| {
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))?;
                assert_eq!(reader.metadata().file_metadata().num_rows() as usize, rows);
                emitted.push((part, rows));
                Ok(())
            };
            writer.write_batch(&batch, &mut output).unwrap();
            assert!(
                writer.current_writer.is_none(),
                "closed row groups must count toward the flush limit"
            );
            writer.finish(&mut output).unwrap();
            assert_eq!(emitted, [(1, 10)]);
        }
    }

    #[test]
    fn streaming_writer_flushes_dictionary_memory_without_publishing_a_small_part() {
        let batch = make_test_batch(1024);
        let mut writer = StreamingPartWriter::new(
            batch.schema(),
            WriterProperties::builder().build(),
            32 * 1024,
            None,
            0,
        );
        writer.row_group_memory_bytes = 32 * 1024;
        let mut outputs = Vec::new();
        let mut publish = |_, bytes: Vec<u8>, rows| {
            outputs.push((bytes, rows));
            Ok(())
        };
        writer.write_batch(&batch, &mut publish).unwrap();
        let active = writer
            .current_writer
            .as_ref()
            .expect("memory flush should retain the compressed part");
        assert_eq!(
            active.in_progress_rows(),
            0,
            "the dictionary row group must have been flushed"
        );
        assert!(active.bytes_written() < 32 * 1024);
        assert_eq!(writer.current_rows, 1024);
        assert_eq!(writer.next_part_num, 0);
        writer.finish(&mut publish).unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].1, 1024);
    }

    #[test]
    fn streaming_writer_keeps_only_current_part_across_a_large_group() {
        let batch = make_test_batch(1024);
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2048))
            .build();
        let mut writer = StreamingPartWriter::new(batch.schema(), props, 1024 * 1024, None, 0);
        let mut total = 0;
        let mut output = |_, _: Vec<u8>, rows| {
            total += rows;
            Ok(())
        };
        let mut peak = 0;
        for _ in 0..512 {
            writer.write_batch(&batch, &mut output).unwrap();
            if let Some(active) = &writer.current_writer {
                let retained = active
                    .bytes_written()
                    .saturating_add(active.memory_size().max(active.in_progress_size()));
                peak = peak.max(retained);
                assert!(
                    active.memory_size() < ROW_GROUP_MEMORY_BUDGET_BYTES,
                    "the active row group must flush at the memory target"
                );
                assert!(
                    active
                        .bytes_written()
                        .saturating_add(active.in_progress_size())
                        < 1024 * 1024,
                    "the output part must flush at the encoded byte target"
                );
                assert!(retained < ROW_GROUP_MEMORY_BUDGET_BYTES + 1024 * 1024);
            }
        }
        writer.finish(&mut output).unwrap();
        assert_eq!(total, 512 * 1024);
        assert!(peak > 0);
        assert!(writer.next_part_num > 1);
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
            collect_named_files_recursive(root, JOURNAL_FILE, &mut paths)?;
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

/// Compaction/export changes physical parts, so never inherit a source transaction receipt.
pub(crate) fn strip_transaction_schema(original: Arc<Schema>) -> Arc<Schema> {
    if !original
        .metadata()
        .keys()
        .any(|key| key.starts_with("fireparq.ingest."))
    {
        return original;
    }
    let metadata: std::collections::HashMap<String, String> = original
        .metadata()
        .iter()
        .filter(|(key, _)| !key.starts_with("fireparq.ingest."))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    Arc::new(Schema::new_with_metadata(
        original.fields().clone(),
        metadata,
    ))
}

pub(crate) fn strip_transaction_metadata(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = strip_transaction_schema(batch.schema());
    if Arc::ptr_eq(&schema, &batch.schema()) {
        return Ok(batch);
    }
    Ok(RecordBatch::try_new(schema, batch.columns().to_vec())?)
}
