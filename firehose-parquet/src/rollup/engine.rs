//! Two-pass, journaled group orchestration shared by local and remote rollup.
//!
//! [`run`] validates every source of a target group (schema, value metadata and row count)
//! before any output, claims the group with a `writing` journal, streams it through one
//! [`Encoder`], commits the journal with the output names, and only then removes earlier
//! copies and deletes the group's sources, before the next group starts. [`recover`]
//! finishes or undoes groups an interrupted run left journaled; callers run it before they
//! discover sources. The protocol is described in [`super::journal`]. Storage-specific
//! publication, durability, ownership checks, cleanup and deletion stay in the [`Local`] and
//! [`Remote`] hooks.
//!
//! Log events keep the `firehose_parquet::rollup` target they had before this module
//! was split out, so existing `RUST_LOG` filters still select them.
use super::journal::{RollupJournal, ROLLUP_JOURNAL_FILE, WHAT};
use super::*;
use crate::merge_journal::{rollup_crash_point, JournalState, PartitionFiles};
use parquet::file::reader::ChunkReader;
use std::hash::Hash;

/// Which pass opens a source; local reads keep their different error context.
#[derive(Clone, Copy)]
enum Pass {
    Validate,
    Encode,
}

/// Storage-specific steps of one rollup run.
trait Backend {
    type Source;
    type Output: Eq + Hash;
    type Reader: ChunkReader + 'static;
    type Files<'f>: PartitionFiles
    where
        Self: 'f;

    /// Identity of the source root recorded in journals.
    fn source_root(&self) -> &str;
    /// Source path relative to the source root, for schema mismatch messages.
    fn label(&self, source: &Self::Source) -> String;
    /// Source path relative to the source root as recorded in the journal.
    fn journal_source(&self, source: &Self::Source) -> Result<String>;
    fn reader(
        &self,
        source: &Self::Source,
        pass: Pass,
    ) -> Result<ParquetRecordBatchReaderBuilder<Self::Reader>>;
    /// Prepares the output directory of a validated, nonempty group.
    fn begin_group(&self, group: &str) -> Result<()>;
    /// The output directory (local path or S3 key) of a group.
    fn group_dir(&self, group: &str) -> String;
    /// Journal and file operations on an output directory.
    fn files<'f>(&'f self, dir: &'f str) -> Self::Files<'f>;
    /// Every output directory holding a rollup journal.
    fn find_journals(&self) -> Result<Vec<String>>;
    /// Identity of output `name` in `dir`, as `publish` returns it.
    fn output(&self, dir: &str, name: &str) -> Self::Output;
    /// Writes one complete output part without replacing an existing file.
    fn publish(
        &self,
        dir: &str,
        name: &str,
        bytes: Vec<u8>,
        rows: usize,
        run_id: &str,
    ) -> Result<Self::Output>;
    /// The ownership check that must pass before a journal is created or committed.
    fn check_owner(&self) -> Result<()>;
    /// Removes copy outputs of earlier runs that this run's outputs replace.
    fn remove_previous(&self, dir: &str, written: &HashSet<Self::Output>) -> Result<()>;
    /// Deletes a committed group's sources durably; returns how many were deleted.
    fn delete_sources(
        &self,
        sources: &[Self::Source],
        written: &HashSet<Self::Output>,
    ) -> Result<usize>;
    /// Deletes the sources a committed journal recorded; missing ones are already gone.
    fn delete_recorded(&self, sources: &[String]) -> Result<usize>;
    /// Runs once after the last group when any source was deleted.
    fn finish_deletions(&self, deleted: usize) -> Result<()>;
}

fn run<B: Backend>(
    backend: &B,
    groups: &BTreeMap<String, Vec<B::Source>>,
    config: &RollupConfig,
) -> Result<()> {
    let names = OutputNames::new(config.delete_source);
    let mut total_input_files = 0usize;
    let mut total_output_files = 0usize;
    let mut total_rows = 0usize;
    let mut deleted_sources = 0usize;
    let mut written: HashSet<B::Output> = HashSet::new();
    let mut schema_mismatches: Vec<String> = Vec::new();

    'groups: for (group_key, sources) in groups {
        info!(
            target: "firehose_parquet::rollup",
            group = %group_key,
            files = sources.len(),
            "processing group"
        );

        // Validate every input before any group output. Retain only one decoded
        // batch during this first pass; encoding happens in a second streaming pass.
        let mut schema_check = SchemaCheck::default();
        let mut schema = None;
        let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
        let mut rows = 0usize;
        for source in sources {
            let builder = backend.reader(source, Pass::Validate)?;
            let key_values = builder.metadata().file_metadata().key_value_metadata();
            if let Some(reason) = schema_check.check(
                &backend.label(source),
                builder.schema(),
                key_values.map(Vec::as_slice),
            ) {
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
        total_input_files += sources.len();
        backend.begin_group(group_key)?;

        // Claim the group with a journal before writing anything; see `super::journal`.
        let dir = backend.group_dir(group_key);
        let files = backend.files(&dir);
        let journal = RollupJournal::writing(
            names.run_id(),
            !config.delete_source,
            backend.source_root(),
            sources
                .iter()
                .map(|source| backend.journal_source(source))
                .collect::<Result<_>>()?,
        );
        backend.check_owner()?;
        anyhow::ensure!(
            files.create_record(ROLLUP_JOURNAL_FILE, WHAT, &journal.encode()?)?,
            "{}: an interrupted rollup journal is still present; refusing to roll it up again",
            files.label()
        );

        // Copies keep their sources, so the next copy rollup must be able to find and
        // replace them even after merge renamed them; outputs that replace their sources
        // keep whatever marker those sources carried.
        let file_kv_metadata = if config.delete_source {
            file_kv_metadata
        } else {
            Some(crate::maintenance::compaction::with_rollup_copy_marker(
                file_kv_metadata,
            ))
        };
        let mut encoder = Encoder::rollup(
            schema.context("rollup group has no schema")?,
            config.compression,
            file_kv_metadata.as_deref(),
            config.flush_bytes,
        );
        let mut group_written = Vec::new();
        let mut output_names = Vec::new();
        let mut publish = |part: u32, bytes: Vec<u8>, part_rows: usize| -> Result<()> {
            let name = names.file_name(part);
            group_written.push(backend.publish(&dir, &name, bytes, part_rows, names.run_id())?);
            output_names.push(name);
            if output_names.len() == 1 {
                rollup_crash_point("after-first-part")?;
            }
            Ok(())
        };
        let mut written_rows = 0usize;
        for source in sources {
            let builder = backend.reader(source, Pass::Encode)?;
            encoder.write_reader(builder, &mut publish, Some(&mut written_rows))?;
        }
        anyhow::ensure!(
            written_rows == rows,
            "rollup source row count changed after preflight; source files were retained"
        );
        encoder.finish(&mut publish)?;

        // Outputs are durable before the commit names them.
        files.sync()?;
        rollup_crash_point("after-outputs")?;
        backend.check_owner()?;
        let committed = journal.committed(output_names);
        files.replace_record(ROLLUP_JOURNAL_FILE, WHAT, &committed.encode()?)?;
        rollup_crash_point("after-commit")?;

        total_output_files += group_written.len();
        written.extend(group_written);
        backend.remove_previous(&dir, &written)?;

        // Delete this group's sources as soon as its output is committed, so a failure in a
        // later group cannot leave them behind to be rolled up a second time.
        if config.delete_source {
            deleted_sources += backend.delete_sources(sources, &written)?;
        }
        files.delete(ROLLUP_JOURNAL_FILE)?;
        files.sync()?;
    }

    if deleted_sources > 0 {
        backend.finish_deletions(deleted_sources)?;
    }

    info!(
        target: "firehose_parquet::rollup",
        input_files = total_input_files,
        output_files = total_output_files,
        total_rows,
        "rollup complete"
    );

    schema_mismatch_result(&schema_mismatches)
}

/// Finishes or undoes every group an interrupted rollup left journaled in the output.
///
/// A `writing` journal is rolled back: that run's outputs and temporary files are deleted,
/// and its sources were never touched. A `committed` journal is rolled forward: earlier
/// copies are removed and, without copies, the recorded sources are deleted, which requires
/// the same source root.
fn recover<B: Backend>(backend: &B) -> Result<()> {
    let mut deleted_sources = 0usize;
    let mut dirs = backend.find_journals()?;
    dirs.sort();
    for dir in dirs {
        let files = backend.files(&dir);
        let Some(bytes) = files.read_record(ROLLUP_JOURNAL_FILE, WHAT)? else {
            continue;
        };
        let journal = RollupJournal::decode(&bytes, &files.label())?;
        backend.check_owner()?;
        let outcome = match journal.state {
            JournalState::Writing => {
                let remove: Vec<String> = files
                    .list_names()?
                    .into_iter()
                    .filter(|name| journal.is_run_file(name))
                    .collect();
                files.delete_many(&remove)?;
                files.sync()?;
                format!(
                    "rolled back an interrupted rollup (deleted {} partial output(s))",
                    remove.len()
                )
            }
            JournalState::Committed => {
                let present: HashSet<String> = files.list_names()?.into_iter().collect();
                if let Some(missing) = journal.outputs.iter().find(|name| !present.contains(*name))
                {
                    anyhow::bail!(
                        "{}: the interrupted rollup recorded in {ROLLUP_JOURNAL_FILE} committed \
                         output {missing}, which is missing. Its sources were kept; check the \
                         partition and remove {ROLLUP_JOURNAL_FILE} once it holds each row \
                         exactly once",
                        files.label()
                    );
                }
                anyhow::ensure!(
                    journal.copy || journal.source_root == backend.source_root(),
                    "{}: the interrupted rollup of {} deletes its sources; finish it by rolling \
                     up that source into this output again. Its remaining sources were kept",
                    files.label(),
                    journal.source_root
                );
                let written = journal
                    .outputs
                    .iter()
                    .map(|name| backend.output(&dir, name))
                    .collect();
                backend.remove_previous(&dir, &written)?;
                let deleted = if journal.copy {
                    0
                } else {
                    backend.delete_recorded(&journal.sources)?
                };
                deleted_sources += deleted;
                format!("finished an interrupted rollup (deleted {deleted} remaining source(s))")
            }
        };
        files.delete(ROLLUP_JOURNAL_FILE)?;
        files.sync()?;
        warn!(
            target: "firehose_parquet::rollup",
            partition = %files.label(),
            run_id = %journal.run_id,
            "{outcome}"
        );
    }
    if deleted_sources > 0 {
        backend.finish_deletions(deleted_sources)?;
    }
    Ok(())
}

/// Local directories under the common directory-inode ownership.
struct Local<'a> {
    source: &'a Path,
    source_root: String,
    output: &'a Path,
    ownership: &'a DatasetOwnership,
}

impl<'a> Local<'a> {
    fn new(source: &'a Path, output: &'a Path, ownership: &'a DatasetOwnership) -> Result<Self> {
        let source_root = std::fs::canonicalize(source)
            .with_context(|| format!("resolving {}", source.display()))?
            .to_string_lossy()
            .into_owned();
        Ok(Self {
            source,
            source_root,
            output,
            ownership,
        })
    }
}

impl Backend for Local<'_> {
    type Source = PathBuf;
    type Output = PathBuf;
    type Reader = std::fs::File;
    type Files<'f>
        = crate::merge_journal::LocalPartition
    where
        Self: 'f;

    fn source_root(&self) -> &str {
        &self.source_root
    }

    fn label(&self, source: &PathBuf) -> String {
        source
            .strip_prefix(self.source)
            .unwrap_or(source)
            .to_string_lossy()
            .into_owned()
    }

    fn journal_source(&self, source: &PathBuf) -> Result<String> {
        source
            .strip_prefix(self.source)
            .ok()
            .and_then(|relative| relative.to_str())
            .map(str::to_owned)
            .with_context(|| {
                format!(
                    "rollup source {} is not a UTF-8 path below the source root; it cannot be \
                     journaled",
                    source.display()
                )
            })
    }

    fn reader(
        &self,
        source: &PathBuf,
        pass: Pass,
    ) -> Result<ParquetRecordBatchReaderBuilder<std::fs::File>> {
        let file = match pass {
            Pass::Validate => std::fs::File::open(source)
                .with_context(|| format!("opening {}", source.display()))?,
            Pass::Encode => std::fs::File::open(source)?,
        };
        Ok(ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(READER_BATCH_ROWS))
    }

    fn begin_group(&self, group: &str) -> Result<()> {
        self.ownership.revalidate_local_paths()?;
        let out_dir = self.output.join(group);
        std::fs::create_dir_all(&out_dir)
            .with_context(|| format!("creating output dir {}", out_dir.display()))
    }

    fn group_dir(&self, group: &str) -> String {
        self.output.join(group).to_string_lossy().into_owned()
    }

    fn files<'f>(&'f self, dir: &'f str) -> crate::merge_journal::LocalPartition {
        crate::merge_journal::LocalPartition::new(Path::new(dir))
    }

    fn find_journals(&self) -> Result<Vec<String>> {
        let mut journals = Vec::new();
        discovery::collect_local(
            self.output,
            LocalPolicy::named(ROLLUP_JOURNAL_FILE),
            &mut journals,
        )?;
        Ok(journals
            .iter()
            .filter_map(|journal| journal.parent())
            .map(|dir| dir.to_string_lossy().into_owned())
            .collect())
    }

    fn output(&self, dir: &str, name: &str) -> PathBuf {
        Path::new(dir).join(name)
    }

    fn publish(
        &self,
        dir: &str,
        name: &str,
        bytes: Vec<u8>,
        rows: usize,
        run_id: &str,
    ) -> Result<PathBuf> {
        self.ownership.revalidate_local_paths()?;
        let path = crate::merge_journal::write_local_output_exclusive(
            Path::new(dir),
            name,
            &bytes,
            run_id,
        )?;
        info!(
            target: "firehose_parquet::rollup",
            path = %path.display(),
            rows,
            size = %format_bytes(bytes.len() as u64),
            "wrote rolled-up part"
        );
        Ok(path)
    }

    fn check_owner(&self) -> Result<()> {
        self.ownership.revalidate_local_paths()
    }

    fn remove_previous(&self, dir: &str, written: &HashSet<PathBuf>) -> Result<()> {
        self.ownership.revalidate_local_paths()?;
        remove_previous_copies_local(Path::new(dir), written)
    }

    fn delete_sources(&self, sources: &[PathBuf], written: &HashSet<PathBuf>) -> Result<usize> {
        let mut deleted = Vec::new();
        for source in sources {
            if written.contains(source) {
                continue;
            }
            debug!(
                target: "firehose_parquet::rollup",
                path = %source.display(),
                "deleting rolled-up source Parquet file"
            );
            std::fs::remove_file(source)
                .with_context(|| format!("deleting source file {}", source.display()))?;
            deleted.push(source.as_path());
            rollup_crash_point("after-first-delete")?;
        }
        sync_parents(&deleted)?;
        Ok(deleted.len())
    }

    fn delete_recorded(&self, sources: &[String]) -> Result<usize> {
        self.ownership.revalidate_local_paths()?;
        let mut deleted = Vec::new();
        for source in sources {
            let path = self.source.join(source);
            match std::fs::remove_file(&path) {
                Ok(()) => deleted.push(path),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("deleting source file {}", path.display()))
                }
            }
        }
        sync_parents(&deleted.iter().map(PathBuf::as_path).collect::<Vec<_>>())?;
        Ok(deleted.len())
    }

    fn finish_deletions(&self, deleted: usize) -> Result<()> {
        info!(target: "firehose_parquet::rollup", files = deleted, "deleted source files");
        // Clean up empty directories.
        cleanup_empty_dirs(self.source)
    }
}

/// Makes deletions durable: fsyncs each distinct parent directory of `paths`.
fn sync_parents(paths: &[&Path]) -> Result<()> {
    let parents: std::collections::BTreeSet<&Path> =
        paths.iter().filter_map(|path| path.parent()).collect();
    for parent in parents {
        crate::merge_journal::sync_dir(parent)?;
    }
    Ok(())
}

/// S3 source and output roots (possibly different buckets).
struct Remote<'a> {
    source: &'a S3Root,
    source_root: String,
    output: &'a S3Root,
    cache_control: &'a str,
    /// Persistent bucket owners held by this run (none in unit tests).
    owners: &'a [&'a crate::dataset_lock_s3::S3Ownership],
}

impl<'a> Remote<'a> {
    fn new(
        source: &'a S3Root,
        output: &'a S3Root,
        cache_control: &'a str,
        owners: &'a [&'a crate::dataset_lock_s3::S3Ownership],
    ) -> Self {
        Self {
            source,
            source_root: format!("s3://{}/{}", source.bucket, source.prefix),
            output,
            cache_control,
            owners,
        }
    }
}

impl Backend for Remote<'_> {
    type Source = object_store::ObjectMeta;
    type Output = String;
    type Reader = RangeReader;
    type Files<'f>
        = crate::merge_journal::S3Partition<'f>
    where
        Self: 'f;

    fn source_root(&self) -> &str {
        &self.source_root
    }

    fn label(&self, source: &Self::Source) -> String {
        self.source.relative(source.location.as_ref()).to_owned()
    }

    fn journal_source(&self, source: &Self::Source) -> Result<String> {
        Ok(self.label(source))
    }

    fn reader(
        &self,
        source: &Self::Source,
        _pass: Pass,
    ) -> Result<ParquetRecordBatchReaderBuilder<RangeReader>> {
        Ok(ParquetRecordBatchReaderBuilder::try_new(RangeReader::new(
            self.source.client.clone(),
            source.clone(),
        ))?
        .with_batch_size(READER_BATCH_ROWS))
    }

    fn begin_group(&self, _group: &str) -> Result<()> {
        Ok(())
    }

    fn group_dir(&self, group: &str) -> String {
        self.output.key(group)
    }

    fn files<'f>(&'f self, dir: &'f str) -> crate::merge_journal::S3Partition<'f> {
        crate::merge_journal::S3Partition {
            client: &self.output.client,
            bucket: &self.output.bucket,
            key: dir,
        }
    }

    fn find_journals(&self) -> Result<Vec<String>> {
        let prefix = &self.output.prefix;
        let objects = block_on_async(discovery::list_objects(self.output.client.as_ref(), prefix))
            .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;
        Ok(objects
            .iter()
            .map(|object| object.location.as_ref())
            .filter(|key| {
                (prefix.is_empty()
                    || key
                        .strip_prefix(prefix.as_str())
                        .is_some_and(|tail| tail.starts_with('/')))
                    && key.rsplit('/').next() == Some(ROLLUP_JOURNAL_FILE)
                    && !crate::artifacts::is_control_path(key)
            })
            .map(|key| key.rsplit_once('/').map_or("", |(dir, _)| dir).to_owned())
            .collect())
    }

    fn output(&self, dir: &str, name: &str) -> String {
        object_store::path::Path::from(format!("{dir}/{name}"))
            .as_ref()
            .to_owned()
    }

    fn publish(
        &self,
        dir: &str,
        name: &str,
        bytes: Vec<u8>,
        rows: usize,
        _run_id: &str,
    ) -> Result<String> {
        let path = object_store::path::Path::from(format!("{dir}/{name}"));
        let size = bytes.len();
        block_on_async(self.output.client.put_opts(
            &path,
            bytes.into(),
            crate::writer::s3_put_options(self.cache_control),
        ))
        .map_err(|_| {
            anyhow::anyhow!("publishing rollup output failed; source files were retained")
        })?;
        info!(
            target: "firehose_parquet::rollup",
            path = %path,
            rows,
            size = %format_bytes(size as u64),
            "wrote rolled-up part"
        );
        Ok(path.as_ref().to_owned())
    }

    fn check_owner(&self) -> Result<()> {
        for owner in self.owners {
            crate::merge::assert_s3_owner(owner, "rollup")?;
        }
        Ok(())
    }

    fn remove_previous(&self, dir: &str, written: &HashSet<String>) -> Result<()> {
        remove_previous_copies_s3(self.output, dir, written)
    }

    fn delete_sources(&self, sources: &[Self::Source], written: &HashSet<String>) -> Result<usize> {
        let same_bucket = self.source.bucket == self.output.bucket;
        let keys = sources
            .iter()
            .filter(|object| !(same_bucket && written.contains(object.location.as_ref())))
            .map(|object| object.location.clone())
            .collect::<Vec<_>>();
        self.delete_keys(&keys)?;
        Ok(keys.len())
    }

    fn delete_recorded(&self, sources: &[String]) -> Result<usize> {
        let keys = sources
            .iter()
            .map(|source| object_store::path::Path::from(self.source.key(source)))
            .collect::<Vec<_>>();
        self.delete_keys(&keys)?;
        Ok(keys.len())
    }

    fn finish_deletions(&self, deleted: usize) -> Result<()> {
        info!(
            target: "firehose_parquet::rollup",
            files = deleted,
            "deleted source files from S3"
        );
        Ok(())
    }
}

impl Remote<'_> {
    /// Single-attempt, first-error-stop source deletes; a missing key is already deleted.
    fn delete_keys(&self, keys: &[object_store::path::Path]) -> Result<()> {
        block_on_async(crate::s3::delete::delete_objects_observed(
            &self.source.client,
            keys,
            |_| rollup_crash_point("after-first-delete"),
        ))
    }
}

/// Finishes or undoes interrupted local rollups in `output`.
pub(super) fn recover_local(
    source: &Path,
    output: &Path,
    ownership: &DatasetOwnership,
) -> Result<()> {
    recover(&Local::new(source, output, ownership)?)
}

/// Rolls up grouped local source files into `output`.
pub(super) fn local(
    source: &Path,
    output: &Path,
    ownership: &DatasetOwnership,
    groups: &BTreeMap<String, Vec<PathBuf>>,
    config: &RollupConfig,
) -> Result<()> {
    run(&Local::new(source, output, ownership)?, groups, config)
}

/// Finishes or undoes interrupted S3 rollups in `output`.
pub(super) fn recover_remote(
    source: &S3Root,
    output: &S3Root,
    config: &RollupConfig,
    owners: &[&crate::dataset_lock_s3::S3Ownership],
) -> Result<()> {
    recover(&Remote::new(source, output, &config.cache_control, owners))
}

/// Rolls up grouped S3 source objects into `output`.
pub(super) fn remote(
    source: &S3Root,
    output: &S3Root,
    groups: &BTreeMap<String, Vec<object_store::ObjectMeta>>,
    config: &RollupConfig,
    owners: &[&crate::dataset_lock_s3::S3Ownership],
) -> Result<()> {
    run(
        &Remote::new(source, output, &config.cache_control, owners),
        groups,
        config,
    )
}

#[cfg(test)]
mod tests;
