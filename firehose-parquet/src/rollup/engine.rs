//! Two-pass group orchestration shared by local and remote rollup.
//!
//! [`run`] validates every source of a target group (schema and row count) before any
//! output, then streams the group through one [`Encoder`], cleans up earlier copy outputs
//! and deletes the group's sources before the next group starts. Storage-specific
//! publication, cleanup and deletion stay in the [`Local`] and [`Remote`] hooks, so their
//! ownership checks, write modes and delete batching are unchanged.
//!
//! Log events keep the `firehose_parquet::rollup` target they had before this module
//! was split out, so existing `RUST_LOG` filters still select them.
use super::*;
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

    /// Source path relative to the source root, for schema mismatch messages.
    fn label(&self, source: &Self::Source) -> String;
    fn reader(
        &self,
        source: &Self::Source,
        pass: Pass,
    ) -> Result<ParquetRecordBatchReaderBuilder<Self::Reader>>;
    /// Prepares the output location of a validated, nonempty group.
    fn begin_group(&self, group: &str) -> Result<()>;
    fn publish(&self, group: &str, name: &str, bytes: Vec<u8>, rows: usize)
        -> Result<Self::Output>;
    /// Removes copy outputs of earlier runs that this run's outputs replace.
    fn remove_previous(&self, group: &str, written: &HashSet<Self::Output>) -> Result<()>;
    /// Deletes a group's sources; returns how many were deleted.
    fn delete_sources(
        &self,
        sources: &[Self::Source],
        written: &HashSet<Self::Output>,
    ) -> Result<usize>;
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
            if let Some(reason) = schema_check.check(&backend.label(source), builder.schema()) {
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
        let mut encoder = Encoder::rollup(
            schema.context("rollup group has no schema")?,
            config.compression,
            file_kv_metadata.as_deref(),
            config.flush_bytes,
        );
        let mut group_written = Vec::new();
        let mut publish = |part: u32, bytes: Vec<u8>, part_rows: usize| -> Result<()> {
            let name = names.file_name(part);
            group_written.push(backend.publish(group_key, &name, bytes, part_rows)?);
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
        total_output_files += group_written.len();
        written.extend(group_written);

        backend.remove_previous(group_key, &written)?;

        // Delete this group's sources as soon as its output is written, so a failure in a
        // later group cannot leave them behind to be rolled up a second time.
        if config.delete_source {
            deleted_sources += backend.delete_sources(sources, &written)?;
        }
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

/// Local directories under the common directory-inode ownership.
struct Local<'a> {
    source: &'a Path,
    output: &'a Path,
    ownership: &'a DatasetOwnership,
}

impl Backend for Local<'_> {
    type Source = PathBuf;
    type Output = PathBuf;
    type Reader = std::fs::File;

    fn label(&self, source: &PathBuf) -> String {
        source
            .strip_prefix(self.source)
            .unwrap_or(source)
            .to_string_lossy()
            .into_owned()
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

    fn publish(&self, group: &str, name: &str, bytes: Vec<u8>, rows: usize) -> Result<PathBuf> {
        self.ownership.revalidate_local_paths()?;
        let path = self.output.join(group).join(name);
        create_output_file(&path)?
            .write_all(&bytes)
            .with_context(|| format!("writing {}", path.display()))?;
        info!(
            target: "firehose_parquet::rollup",
            path = %path.display(),
            rows,
            size = %format_bytes(bytes.len() as u64),
            "wrote rolled-up part"
        );
        Ok(path)
    }

    fn remove_previous(&self, group: &str, written: &HashSet<PathBuf>) -> Result<()> {
        self.ownership.revalidate_local_paths()?;
        remove_previous_copies_local(&self.output.join(group), written)
    }

    fn delete_sources(&self, sources: &[PathBuf], written: &HashSet<PathBuf>) -> Result<usize> {
        let mut deleted = 0;
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
            deleted += 1;
        }
        Ok(deleted)
    }

    fn finish_deletions(&self, deleted: usize) -> Result<()> {
        info!(target: "firehose_parquet::rollup", files = deleted, "deleted source files");
        // Clean up empty directories.
        cleanup_empty_dirs(self.source)
    }
}

/// S3 source and output roots (possibly different buckets).
struct Remote<'a> {
    source: &'a S3Root,
    output: &'a S3Root,
    cache_control: &'a str,
}

impl Backend for Remote<'_> {
    type Source = object_store::ObjectMeta;
    type Output = String;
    type Reader = RangeReader;

    fn label(&self, source: &Self::Source) -> String {
        self.source.relative(source.location.as_ref()).to_owned()
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

    fn publish(&self, group: &str, name: &str, bytes: Vec<u8>, rows: usize) -> Result<String> {
        let path = object_store::path::Path::from(self.output.key(&format!("{group}/{name}")));
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

    fn remove_previous(&self, group: &str, written: &HashSet<String>) -> Result<()> {
        remove_previous_copies_s3(self.output, group, written)
    }

    fn delete_sources(&self, sources: &[Self::Source], written: &HashSet<String>) -> Result<usize> {
        let same_bucket = self.source.bucket == self.output.bucket;
        let keys = sources
            .iter()
            .filter(|object| !(same_bucket && written.contains(object.location.as_ref())))
            .map(|object| object.location.clone())
            .collect::<Vec<_>>();
        block_on_async(crate::s3::delete::delete_objects_once(
            &self.source.client,
            &keys,
        ))?;
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

/// Rolls up grouped local source files into `output`.
pub(super) fn local(
    source: &Path,
    output: &Path,
    ownership: &DatasetOwnership,
    groups: &BTreeMap<String, Vec<PathBuf>>,
    config: &RollupConfig,
) -> Result<()> {
    run(
        &Local {
            source,
            output,
            ownership,
        },
        groups,
        config,
    )
}

/// Rolls up grouped S3 source objects into `output`.
pub(super) fn remote(
    source: &S3Root,
    output: &S3Root,
    groups: &BTreeMap<String, Vec<object_store::ObjectMeta>>,
    config: &RollupConfig,
) -> Result<()> {
    run(
        &Remote {
            source,
            output,
            cache_control: &config.cache_control,
        },
        groups,
        config,
    )
}

#[cfg(test)]
mod tests;
