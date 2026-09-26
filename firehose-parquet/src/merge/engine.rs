//! The crash-safe partition merge shared by local and S3 storage.
//!
//! [`merge_partition`] runs the steps documented in [`crate::merge_journal`] in one fixed
//! order for both storages: estimate, schema preflight, journal claim, streamed outputs,
//! commit, source deletion, journal removal. [`PartitionMerge`] supplies only what differs:
//! how sources are sized, read and deleted, how outputs are published, and which ownership
//! check guards the claim and the commit. The local and S3 implementations live next to the
//! rest of their storage code in `merge.rs`, so their log events keep the
//! `firehose_parquet::merge` target; events emitted here set it explicitly.
use super::*;

/// Storage-specific steps of one partition merge.
pub(super) trait PartitionMerge {
    type Source;
    type Files: PartitionFiles;

    /// Partition path relative to the merge root, `(root)` for the root itself.
    fn label(&self) -> &str;
    /// Journal and file operations on the partition directory.
    fn files(&self) -> &Self::Files;
    /// Total size of the sources, for the output estimate and summary.
    fn source_bytes(&self, sources: &[Self::Source]) -> Result<u64>;
    /// Logs a partition skipped because merging would not reduce its file count.
    fn log_no_op(&self, sources: usize, source_bytes: u64, flush_bytes: u64, estimated: usize);
    /// How the sources' schemas differ, from their footers, before anything is written.
    fn schema_mismatch(&self, sources: &[Self::Source]) -> Result<Option<String>>;
    /// Logs the start of a real or dry-run partition merge (silent by default).
    fn log_start(&self, _sources: usize, _source_bytes: u64, _config: &MergeConfig) {}
    /// Highest `part-NNNNNN.parquet` number among the sources; outputs are numbered after it.
    fn max_part_number(&self, sources: &[Self::Source]) -> u32;
    /// The claiming journal, with this storage's ownership check in its original order.
    fn claim_journal(&self, sources: &[Self::Source], initial_part_num: u32) -> Result<Journal>;
    /// A source that changed between listing and the claim, when the storage can tell.
    fn changed_source(&self, _sources: &[Self::Source]) -> Option<String> {
        None
    }
    /// Streams every source, in order, through `encoder`.
    fn encode<F>(
        &self,
        sources: &[Self::Source],
        encoder: &mut Encoder,
        publish: &mut F,
    ) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>;
    /// Writes one complete output part named `name`.
    fn publish(&self, name: &str, data: Vec<u8>, rows: usize) -> Result<()>;
    /// The ownership check that must pass before the journal commits.
    fn check_owner(&self) -> Result<()>;
    /// Deletes the committed merge's sources, stopping at the first error.
    fn delete_sources(&self, sources: &[Self::Source], outputs: &[String]) -> Result<()>;
    /// Logs a completed partition merge (silent by default).
    fn log_done(&self, _sources: usize, _outputs: usize, _output_bytes: u64) {}
}

/// Table name of a partition label: its first path segment.
pub(super) fn table_of(label: &str) -> &str {
    label
        .split('/')
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or("(root)")
}

/// Announces the merge of `path` and hands each partition (the consecutive sorted
/// sources sharing a `parent`) to `merge`, with the table header state.
pub(super) fn for_each_partition<T, K: PartialEq>(
    path: &str,
    sources: Vec<T>,
    parent: impl Fn(&T) -> K,
    mut merge: impl FnMut(&K, &[T], &mut Option<String>) -> Result<()>,
) -> Result<()> {
    if sources.is_empty() {
        info!(target: "firehose_parquet::merge", "no parquet files found in {path}");
        return Ok(());
    }

    println!("Merging partitions in {path} ...\n");

    let mut partitions: Vec<(K, Vec<T>)> = Vec::new();
    for source in sources {
        let key = parent(&source);
        match partitions.last_mut() {
            Some((partition, members)) if *partition == key => members.push(source),
            _ => partitions.push((key, vec![source])),
        }
    }
    let mut current_table = None;
    for (partition, members) in &partitions {
        merge(partition, members, &mut current_table)?;
    }
    Ok(())
}

/// Merges one partition's sources into fewer parts under a crash-safe journal.
pub(super) fn merge_partition<M: PartitionMerge>(
    store: &M,
    sources: &[M::Source],
    config: &MergeConfig,
    current_table: &mut Option<String>,
    result: &mut MergeResult,
) -> Result<()> {
    let label = store.label();
    let table = table_of(label);
    if current_table.as_deref() != Some(table) {
        println!("Table: {}", table);
        *current_table = Some(table.to_string());
    }

    if sources.len() <= 1 {
        result.partitions_skipped += 1;
        return Ok(());
    }

    let source_bytes = store.source_bytes(sources)?;
    if let Some(estimated) =
        no_op_compaction_estimate(sources.len(), source_bytes, config.flush_bytes)
    {
        store.log_no_op(sources.len(), source_bytes, config.flush_bytes, estimated);
        println!(
            "  {}: skipping merge; {} parts already estimate to ~{} file(s)",
            label,
            sources.len(),
            estimated
        );
        result.partitions_skipped += 1;
        return Ok(());
    }

    // Check every part before writing anything, so a partition with mixed schemas is left
    // exactly as it was.
    if let Some(reason) = store.schema_mismatch(sources)? {
        record_schema_mismatch(label, reason, result);
        return Ok(());
    }

    result.bytes_before += source_bytes;
    store.log_start(sources.len(), source_bytes, config);

    if config.dry_run {
        let estimated = estimate_output_files(source_bytes, config.flush_bytes);
        println!(
            "  {}: {} parts → ~{} file(s) (dry run)",
            label,
            sources.len(),
            estimated
        );
        result.files_read += sources.len();
        result.partitions_merged += 1;
        return Ok(());
    }

    // Claim the partition with a journal before writing anything; see `merge_journal`.
    let initial_part_num = store.max_part_number(sources);
    let journal = store.claim_journal(sources, initial_part_num)?;
    let files = store.files();
    if !files.create_journal(&journal)? {
        record_partition_in_use(label, result);
        return Ok(());
    }
    // A merge that finished just before the claim may have replaced these parts.
    if let Some(changed) = store.changed_source(sources) {
        files.remove_journal()?;
        println!("  {label}: skipped; {changed} changed while the merge started");
        result.partitions_skipped += 1;
        return Ok(());
    }

    let mut encoder = Encoder::merge(
        config.compression,
        config.flush_bytes,
        config.flush_rows,
        initial_part_num,
    );
    let mut outputs: Vec<String> = Vec::new();
    let mut output_bytes = 0u64;
    let mut publish = |part_num: u32, buf: Vec<u8>, rows: usize| -> Result<()> {
        let name = format!("part-{part_num:06}.parquet");
        let size = buf.len() as u64;
        store.publish(&name, buf, rows)?;
        outputs.push(name);
        output_bytes += size;
        Ok(())
    };
    store.encode(sources, &mut encoder, &mut publish)?;
    result.files_read += sources.len();

    if !encoder.initialized() {
        // Every part was empty: nothing to write, and nothing was changed.
        files.remove_journal()?;
        result.partitions_skipped += 1;
        return Ok(());
    }
    encoder.finish(&mut publish)?;

    // Outputs are durable (local directory fsync) before the commit names them.
    files.sync()?;
    crash_point("after-outputs")?;
    // A changed ownership record is an error, never a time-based takeover.
    store.check_owner()?;
    files.replace_journal(&journal.committed(outputs.clone()))?;
    crash_point("after-commit")?;

    println!(
        "  {}: {} parts → {} file(s) ({})",
        label,
        sources.len(),
        outputs.len(),
        format_bytes(output_bytes),
    );

    store.delete_sources(sources, &outputs)?;
    files.sync()?;
    files.remove_journal()?;
    files.sync()?;

    result.bytes_after += output_bytes;
    result.files_written += outputs.len();
    result.partitions_merged += 1;
    store.log_done(sources.len(), outputs.len(), output_bytes);
    Ok(())
}

/// Finishes or undoes the interrupted merge journaled in `files`, if any.
///
/// Without a lock (dry run) the journal is only reported. `admit` runs the storage's
/// protection and ownership checks and returns false to leave the journal to a live run.
pub(super) fn recover_partition<F: PartitionFiles, L>(
    label: &str,
    files: &F,
    lock: Option<L>,
    admit: impl FnOnce(&Journal, L) -> Result<bool>,
    result: &mut MergeResult,
) -> Result<()> {
    let Some(journal) = files.read_journal()? else {
        return Ok(());
    };
    let Some(lock) = lock else {
        println!(
            "  {label}: has an interrupted merge ({:?}); a real run recovers it first",
            journal.state
        );
        return Ok(());
    };
    if !admit(&journal, lock)? {
        return Ok(());
    }
    let recovery = crate::merge_journal::recover(files, &journal)?;
    warn!(
        target: "firehose_parquet::merge",
        partition = label,
        run_id = journal.run_id,
        %recovery,
        "recovered an interrupted merge"
    );
    println!("  {label}: {recovery}");
    result.merges_recovered += 1;
    Ok(())
}

#[cfg(test)]
mod tests;
