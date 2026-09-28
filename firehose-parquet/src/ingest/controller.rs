//! All-table publication and restart decisions. Every non-dry-run `build`
//! reaches this controller through `IngestionSession`, which supplies
//! eligibility, the accepted envelope prefix and completion proof; the
//! controller journals each flush, publishes its parts, commits them to the
//! tables' Delta logs (#643 L3), advances authority and then reconciles the
//! optional `ProtectedMirror`.
//!
//! The Delta commits sit between Committed and the authority advance
//! (`docs/design/delta-lake.md` §3.1): every table with a part gets one commit
//! with `txn = last ordinal`, the others first and `blocks` last.
//!
//! Recovery (#643 L4, design §4) runs before any Blocks request. It first
//! reads every table's `txn` and refuses a log ahead of authority. A Committed
//! journal whose authority has not advanced is rolled forward table by table,
//! gated by `txn`: only the tables whose logs lack the transaction are
//! committed, `blocks` last, after their parts (and only theirs) verified
//! against the journal; then authority advances. Once authority reached the
//! transaction's target, no part is read again, because OPTIMIZE and VACUUM
//! may have rewritten or removed committed parts.
//!
//! A Delta commit whose outcome is unknown does not set the S3 owner's
//! uncertainty latch (design §3.5, decided in #643 L4): the conditional log
//! PUT and the `txn` action resolve every arrival order to one copy at the
//! next start, and nothing fireparq does ever deletes a log commit.

use anyhow::{bail, ensure, Context, Result};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::frontier::AcceptedFrontier;
use super::parts::{writer_plan, TransactionParts};
use super::state::{
    AcceptedPrefix, AuthorityState, Digest, PartCompression, PendingTransaction, StreamDescriptor,
    TablePlan, TransactionPhase,
};
use super::store::{TransactionStateStore, Versioned};
use crate::config::{BlockMetadata, Compression, FlushConcurrency};
use crate::delta::commit::{CommitFailure, CommitHooks, DeltaTables, TableCommit, UnknownOutcome};

mod lane;
mod pipeline;
use crate::writer::protected::PreparedFlush;
use crate::writer::ParquetFileMetadata;
pub use pipeline::FlushWorkStats;

pub trait MirrorAction {
    async fn reconcile(&self, authority: &AuthorityState) -> Result<()>;
}

impl<T: MirrorAction> MirrorAction for &T {
    async fn reconcile(&self, authority: &AuthorityState) -> Result<()> {
        T::reconcile(self, authority).await
    }
}

impl MirrorAction for super::mirror::ProtectedMirror<'_> {
    async fn reconcile(&self, authority: &AuthorityState) -> Result<()> {
        super::mirror::ProtectedMirror::reconcile(self, authority).await?;
        Ok(())
    }
}

pub struct TransactionController<'a, M: MirrorAction> {
    states: TransactionStateStore<'a>,
    parts: TransactionParts<'a>,
    authority: Versioned<AuthorityState>,
    mirror: M,
    failed: bool,
    concurrency: FlushConcurrency,
    /// The dataset's Delta tables. `IngestionSession` always sets them, and
    /// `recovery recover` whenever a journal needs them; only controller unit
    /// tests of the #468 journal alone run without.
    delta: Option<DeltaTables>,
    _session: crate::dataset_lock::session::SessionPermit<'a>,
}

pub struct CommittedFlush {
    pub checkpoint_id: Digest,
    pub ordinal: u64,
    pub rows: u64,
    pub bytes: u64,
    pub files: usize,
    pub tables: Vec<CommittedTable>,
    /// Wall time from commit start to the cleared journal.
    pub elapsed: Duration,
    /// High-water marks of the bounded table work.
    pub work: FlushWorkStats,
    /// The Delta commit of each table with a part, in completion order
    /// (`blocks` last).
    pub delta: Vec<TableCommit>,
    /// Wall time of the Delta commit step, from Committed to its last commit.
    pub delta_elapsed: Duration,
}
pub struct CommittedTable {
    pub table: String,
    pub rows: u64,
    pub bytes: u64,
}

/// Refusal of a request whose stream descriptor differs from authority's.
pub(super) const STREAM_MISMATCH: &str = "existing authoritative stream differs from this request; use a new output root for changed semantics or bindings";

impl<'a, M: MirrorAction> TransactionController<'a, M> {
    /// Journal recovery without Delta tables, for the #468 unit tests of the
    /// journal alone.
    #[cfg(test)]
    pub async fn open(
        states: TransactionStateStore<'a>,
        parts: TransactionParts<'a>,
        mirror: M,
        expected: &StreamDescriptor,
    ) -> Result<Self> {
        Self::open_with_delta(
            states,
            parts,
            mirror,
            expected,
            None,
            FlushConcurrency::SERIAL,
        )
        .await
    }

    /// Recovery happens before the caller opens its Firehose Blocks stream.
    /// The expected descriptor must describe the resolved runtime configuration;
    /// merely reading a descriptor from disk does not validate an append request.
    /// `delta` are the dataset's tables, opened for `expected` by the caller;
    /// every later transaction commits to them. `concurrency` bounds both
    /// recovery's table work and every later commit's.
    pub async fn open_with_delta(
        states: TransactionStateStore<'a>,
        parts: TransactionParts<'a>,
        mirror: M,
        expected: &StreamDescriptor,
        delta: Option<DeltaTables>,
        concurrency: FlushConcurrency,
    ) -> Result<Self> {
        let session = parts.acquire_session()?;
        Self::open_reserved(states, parts, mirror, expected, session, delta, concurrency).await
    }

    pub(super) async fn open_reserved(
        states: TransactionStateStore<'a>,
        parts: TransactionParts<'a>,
        mirror: M,
        expected: &StreamDescriptor,
        session: crate::dataset_lock::session::SessionPermit<'a>,
        mut delta: Option<DeltaTables>,
        concurrency: FlushConcurrency,
    ) -> Result<Self> {
        concurrency.validate()?;
        expected.validate()?;
        let snapshot = states.load().await?;
        let mut authority = snapshot.authority.context(
            "protected ingestion authority is absent; eligible-root initialization is required",
        )?;
        if authority.payload.descriptor != *expected {
            bail!("{STREAM_MISMATCH}");
        }
        // A Committed transaction whose authority has not advanced yet: its
        // Delta commits may be incomplete.
        let behind = snapshot.pending.as_ref().filter(|pending| {
            pending.payload.phase == TransactionPhase::Committed
                && authority.payload.checkpoint.id == pending.payload.predecessor
        });
        // Every table's `txn` is checked before anything changes, so a log
        // ahead of authority stops the start with all evidence in place.
        let uncommitted = match &delta {
            Some(delta) => {
                delta
                    .check_progress(
                        authority.payload.checkpoint.ordinal,
                        behind.map(|pending| &pending.payload),
                        concurrency.publications,
                    )
                    .await?
            }
            None => Vec::new(),
        };
        let mut mirror_reconciled = false;
        if let Some(pending) = snapshot.pending {
            match pending.payload.phase {
                TransactionPhase::Writing => {
                    parts.rollback_writing(&pending.payload).await?;
                    checkpoint(Stage::RollbackComplete)?;
                    states.clear(&authority, &pending).await?;
                }
                TransactionPhase::Committed => {
                    if authority.payload.checkpoint.id == pending.payload.predecessor {
                        roll_forward(
                            &parts,
                            delta.as_mut(),
                            &pending.payload,
                            &uncommitted,
                            concurrency.publications,
                        )
                        .await?;
                        authority = states.advance(&authority, &pending).await?;
                        checkpoint(Stage::RecoveryAuthorityAdvanced)?;
                    }
                    // Otherwise authority already reached the target, so every
                    // Delta commit landed before it advanced. No part is read
                    // again: OPTIMIZE and VACUUM may have rewritten or removed
                    // committed parts since (design §4).
                    mirror.reconcile(&authority.payload).await?;
                    mirror_reconciled = true;
                    parts.cleanup_temporaries(&pending.payload)?;
                    states.clear(&authority, &pending).await?;
                }
            }
        }
        if !mirror_reconciled {
            mirror.reconcile(&authority.payload).await?;
        }
        Ok(Self {
            states,
            parts,
            authority,
            mirror,
            failed: false,
            concurrency,
            delta,
            _session: session,
        })
    }

    /// The dataset's Delta tables, when set.
    pub fn delta_tables(&self) -> Option<&DeltaTables> {
        self.delta.as_ref()
    }

    /// Bound table work inside each later commit. Journal order is unchanged.
    #[cfg(test)]
    pub fn with_concurrency(mut self, concurrency: FlushConcurrency) -> Result<Self> {
        concurrency.validate()?;
        self.concurrency = concurrency;
        Ok(self)
    }

    pub fn authority(&self) -> &AuthorityState {
        &self.authority.payload
    }

    pub fn request_already_complete(&self, stop: u64) -> Result<bool> {
        if let Some(completed) = self.authority.payload.checkpoint.completed_stop {
            if stop < completed {
                bail!("requested stop rewinds an already completed output; use a new root for a shorter range");
            }
            return Ok(stop == completed);
        }
        Ok(false)
    }

    pub async fn complete_request(
        &mut self,
        stop: u64,
        frontier: &AcceptedFrontier,
        clean_stream_completed: bool,
    ) -> Result<bool> {
        if self.failed {
            bail!("ingestion controller stopped after an unresolved operation; reopen through recovery");
        }
        self.failed = true;
        if !clean_stream_completed {
            bail!("interrupted or failed stream cannot prove bounded request completion");
        }
        frontier.require_fully_acknowledged(&self.authority.payload.checkpoint)?;
        if self.request_already_complete(stop)? {
            self.failed = false;
            return Ok(false);
        }
        self.authority = self.states.complete_request(&self.authority, stop).await?;
        checkpoint(Stage::CompletionAuthorityAdvanced)?;
        self.mirror.reconcile(&self.authority.payload).await?;
        checkpoint(Stage::CompletionMirrorReconciled)?;
        self.failed = false;
        Ok(true)
    }

    /// Own and retain the complete batch map through commit. Any error or
    /// cancellation poisons this controller; only reopening under fresh resolved
    /// ownership and journal recovery can continue. No table is acknowledged
    /// independently and no cursor mirror leads the authoritative checkpoint.
    pub async fn commit(
        &mut self,
        prefix: AcceptedPrefix,
        batches: HashMap<String, RecordBatch>,
        metadata: BlockMetadata,
        compression: Compression,
        file_metadata: ParquetFileMetadata,
    ) -> Result<CommittedFlush> {
        if self.failed {
            bail!(
                "ingestion controller stopped after an unresolved commit; reopen through recovery"
            );
        }
        self.failed = true;
        let started = Instant::now();
        let tables = self
            .authority
            .payload
            .descriptor
            .tables
            .iter()
            .map(|(table, schema)| {
                let rows = batches
                    .get(table)
                    .map_or(0, |batch| batch.num_rows() as u64);
                let directory = if rows == 0 {
                    table.clone()
                } else {
                    crate::writer::partition_suffix(table, &metadata)?
                };
                let partition = if directory == *table {
                    String::new()
                } else {
                    directory
                        .strip_prefix(&format!("{table}/"))
                        .context("partition path escaped its table")?
                        .to_string()
                };
                Ok(TablePlan {
                    table: table.clone(),
                    rows,
                    schema_sha256: schema.clone(),
                    partition,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let pending = PendingTransaction::prepare(
            &self.authority.payload,
            prefix,
            tables,
            part_compression(compression),
        )?;
        let inventory: BTreeMap<_, _> = self
            .authority
            .payload
            .descriptor
            .tables
            .iter()
            .map(|(table, digest)| (table.clone(), digest.as_str().to_string()))
            .collect();
        let plans = pending
            .parts
            .iter()
            .map(|part| writer_plan(&pending, part))
            .collect();
        // Complete schema/partition/inventory validation occurs before any
        // journal or part creation. Preexisting planned names cannot be adopted.
        let prepared = PreparedFlush::new(
            batches,
            &inventory,
            plans,
            metadata,
            compression,
            file_metadata,
        )?;
        self.parts.require_unoccupied(&pending).await?;
        // The exact temporary names are fixed by this plan before Writing.
        let owned_temporaries = pending.clone();
        let pending = self.states.begin(&self.authority, pending).await?;
        let (pending, work, delta, delta_elapsed) = match self
            .publish_and_commit(Arc::new(prepared), pending)
            .await
        {
            Ok(done) => done,
            Err(error) => {
                // The journal stays pending for recovery, which never needs a
                // staged temporary: Writing rollback verifies finals by receipt
                // and Committed roll-forward verifies finals only. Remove this
                // transaction's private staging names now instead of leaving
                // them until the next build or `recovery recover`. A cleanup
                // failure must not mask the original error.
                if let Err(cleanup) = self.parts.cleanup_temporaries(&owned_temporaries) {
                    tracing::warn!(
                        error = %format!("{cleanup:#}"),
                        "could not remove this failed transaction's staged temporary parts; the next build or `recovery recover` removes them"
                    );
                }
                return Err(error);
            }
        };
        let result = CommittedFlush {
            checkpoint_id: self.authority.payload.checkpoint.id.clone(),
            ordinal: self.authority.payload.checkpoint.ordinal,
            rows: pending
                .payload
                .tables
                .iter()
                .try_fold(0_u64, |sum, table| {
                    sum.checked_add(table.rows)
                        .context("committed row count overflow")
                })?,
            bytes: pending.payload.parts.iter().try_fold(0_u64, |sum, part| {
                sum.checked_add(
                    part.receipt
                        .as_ref()
                        .context("committed receipt missing")?
                        .byte_size,
                )
                .context("committed byte count overflow")
            })?,
            files: pending.payload.parts.len(),
            tables: pending
                .payload
                .parts
                .iter()
                .map(|part| {
                    Ok(CommittedTable {
                        table: part.table.clone(),
                        rows: part.row_count,
                        bytes: part
                            .receipt
                            .as_ref()
                            .context("committed receipt missing")?
                            .byte_size,
                    })
                })
                .collect::<Result<_>>()?,
            elapsed: started.elapsed(),
            work,
            delta,
            delta_elapsed,
        };
        self.failed = false;
        Ok(result)
    }
}

impl<'a, M: MirrorAction> TransactionController<'a, M> {
    /// Everything after Writing is persisted. Any error leaves the journal
    /// pending and the controller poisoned; the caller removes owned temps.
    async fn publish_and_commit(
        &mut self,
        prepared: Arc<PreparedFlush>,
        pending: Versioned<PendingTransaction>,
    ) -> Result<(
        Versioned<PendingTransaction>,
        FlushWorkStats,
        Vec<TableCommit>,
        Duration,
    )> {
        checkpoint(Stage::WritingPersisted)?;
        // Bounded concurrent table work; every receipt is durable before its
        // part publishes, and nothing below runs unless all parts succeeded
        // and every final verified.
        let (mut pending, work) = self.publish_and_verify(prepared, pending).await?;
        pending = self
            .states
            .mark_committed(&self.authority, &pending)
            .await?;
        checkpoint(Stage::CommittedPersisted)?;
        let delta_started = Instant::now();
        let delta = self.commit_delta(&pending.payload).await?;
        let delta_elapsed = delta_started.elapsed();
        self.authority = self.states.advance(&self.authority, &pending).await?;
        checkpoint(Stage::AuthorityAdvanced)?;
        self.mirror.reconcile(&self.authority.payload).await?;
        checkpoint(Stage::MirrorReconciled)?;
        self.parts.cleanup_temporaries(&pending.payload)?;
        self.states.clear(&self.authority, &pending).await?;
        checkpoint(Stage::PendingCleared)?;
        Ok((pending, work, delta, delta_elapsed))
    }

    /// One Delta commit per table with a part of this Committed transaction,
    /// `blocks` last, bounded by the publication concurrency. A commit whose
    /// outcome is unknown does not mark remote ownership uncertain: the next
    /// start reads the table's `txn` and commits it only if it did not land
    /// (design §3.5).
    async fn commit_delta(&mut self, pending: &PendingTransaction) -> Result<Vec<TableCommit>> {
        let Some(delta) = self.delta.as_mut() else {
            return Ok(Vec::new());
        };
        match delta
            .commit(pending, self.concurrency.publications, &COMMIT_HOOKS)
            .await
        {
            Ok(committed) => {
                checkpoint(Stage::DeltaCommittedAll)?;
                Ok(committed)
            }
            Err(failure) => Err(unresolved_note(pending, failure)),
        }
    }
}

/// The hooks around every Delta commit, of a flush or of a roll-forward:
/// the real-binary debug faults (`FIREPARQ_DEBUG_FAULT`, debug builds) and
/// the `DeltaCommitted(entry index)` stage.
const COMMIT_HOOKS: CommitHooks<'static> = CommitHooks {
    before: &|table| {
        if pipeline::fault::fires("delta-commit", table) {
            bail!("injected debug fault: the Delta commit of {table} failed");
        }
        Ok(())
    },
    after: &|table, index| {
        if pipeline::fault::fires("crash-after-delta-commit", table) {
            // An abrupt process death after a durable Delta commit.
            std::process::abort();
        }
        if pipeline::fault::fires("delta-commit-lost-response", table) {
            // The commit landed, but its response is lost: an outcome the
            // process cannot know, which the next start reads from the log.
            return Err(anyhow::Error::new(UnknownOutcome).context(format!(
                "injected debug fault: the Delta commit of {table} landed but its response was lost"
            )));
        }
        checkpoint(Stage::DeltaCommitted(index))
    },
};

/// A failed Delta commit, with a note when its outcome is unknown. The S3
/// owner is not marked uncertain for it: the next start resolves it from
/// the table's `txn` (design §3.5).
fn unresolved_note(pending: &PendingTransaction, failure: CommitFailure) -> anyhow::Error {
    if !failure.unresolved {
        return failure.error;
    }
    tracing::warn!(
        first_ordinal = pending.prefix.first_ordinal,
        last_ordinal = pending.prefix.last_ordinal,
        "a Delta commit has an unknown outcome; the journal stays Committed, and the next start reads each table's txn to commit only what did not land"
    );
    failure.error.context(
        "a Delta commit's outcome is unknown (it may have landed); the next start resolves it from the table's txn",
    )
}

/// Roll the Committed transaction `pending`, whose authority has not
/// advanced, forward into the Delta logs of `uncommitted` (from
/// [`DeltaTables::check_progress`]): verify exactly those tables' parts,
/// then commit them, `blocks` last. Tables whose logs already hold the
/// transaction are not touched. A missing or differing part stops recovery
/// with the journal kept, as design §4.1 requires.
async fn roll_forward(
    parts: &TransactionParts<'_>,
    delta: Option<&mut DeltaTables>,
    pending: &PendingTransaction,
    uncommitted: &[String],
    concurrency: usize,
) -> Result<()> {
    let Some(delta) = delta else {
        // The #468 journal alone (unit tests without Delta tables): verify
        // every part before authority advances.
        ensure!(
            cfg!(test),
            "a Committed transaction can only be recovered with the dataset's Delta tables"
        );
        return parts.verify_all_finals(pending, concurrency).await;
    };
    let holding: Vec<&str> = pending
        .parts
        .iter()
        .map(|part| part.table.as_str())
        .filter(|table| !uncommitted.iter().any(|missing| missing == table))
        .collect();
    let age = pending_age(pending);
    tracing::info!(
        first_ordinal = pending.prefix.first_ordinal,
        last_ordinal = pending.prefix.last_ordinal,
        pending_secs = age.map(|age| age.as_secs()),
        committed = ?holding,
        rolling_forward = ?uncommitted,
        "recovering a Committed transaction: committing it to the Delta tables whose logs lack it"
    );
    if uncommitted.is_empty() {
        return Ok(());
    }
    parts
        .verify_finals_of(pending, uncommitted, concurrency)
        .await
        .map_err(|error| {
            error.context(format!(
                "cannot roll the Committed transaction of ordinals {}..={} (pending for {}) \
                 forward into the Delta tables {}: a part their logs do not reference yet is \
                 gone or does not match the journal. Lite VACUUM never deletes such an \
                 untracked part, but a full VACUUM whose retention is shorter than the time \
                 the transaction has been pending, or another cleanup, may have \
                 (docs/design/delta-lake.md §4.1). Tables that already hold the transaction: \
                 {}. Nothing was committed or changed, and the journal and authority stay as \
                 evidence; rebuild into a new, empty output root",
                pending.prefix.first_ordinal,
                pending.prefix.last_ordinal,
                age.map_or("an unknown time".to_string(), human_duration),
                uncommitted.join(", "),
                if holding.is_empty() {
                    "none".to_string()
                } else {
                    holding.join(", ")
                },
            ))
        })?;
    delta
        .roll_forward(pending, uncommitted, concurrency, &COMMIT_HOOKS)
        .await
        .map_err(|failure| unresolved_note(pending, failure))?;
    Ok(())
}

/// How long ago the newest part of `pending` was received: its journal's age.
fn pending_age(pending: &PendingTransaction) -> Option<Duration> {
    let newest = pending
        .parts
        .iter()
        .filter_map(|part| part.receipt.as_ref())
        .map(|receipt| receipt.modification_time)
        .max()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis();
    let newest = u128::try_from(newest).ok()?;
    Some(Duration::from_millis(
        u64::try_from(now.saturating_sub(newest)).unwrap_or(u64::MAX),
    ))
}

fn human_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    match seconds {
        0..=119 => format!("{seconds} s"),
        120..=7_199 => format!("{} min", seconds / 60),
        7_200..=172_799 => format!("{} h", seconds / 3_600),
        _ => format!("{} days", seconds / 86_400),
    }
}

impl<'a, M: MirrorAction> TransactionController<'a, M> {
    /// Local parts use blocking file I/O that borrows the ownership guard. With
    /// more than one publication on a multi-thread runtime it runs on a scoped
    /// lane whose threads are all joined before this returns; otherwise (and
    /// for remote parts, whose requests are polled futures) it stays inline.
    async fn publish_and_verify(
        &self,
        prepared: Arc<PreparedFlush>,
        pending: Versioned<PendingTransaction>,
    ) -> Result<(Versioned<PendingTransaction>, FlushWorkStats)> {
        let limits = self.concurrency;
        let (parts, states, authority) = (&self.parts, &self.states, &self.authority);
        let lane_supported = parts.is_local()
            && limits.publications > 1
            && tokio::runtime::Handle::try_current().is_ok_and(|handle| {
                handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
            });
        if !lane_supported {
            let (pending, work) =
                pipeline::publish_parts(parts, states, authority, prepared, pending, limits, None)
                    .await?;
            parts
                .verify_all_finals(&pending.payload, limits.publications)
                .await?;
            return Ok((pending, work));
        }
        let handle = tokio::runtime::Handle::current();
        tokio::task::block_in_place(|| {
            std::thread::scope(|scope| {
                let lane = lane::BlockingLane::start(scope, limits.publications);
                let result = handle.block_on(async {
                    let (pending, work) = pipeline::publish_parts(
                        parts,
                        states,
                        authority,
                        prepared,
                        pending,
                        limits,
                        Some(&lane),
                    )
                    .await?;
                    verify_local_finals(parts, Arc::new(pending.payload.clone()), &lane).await?;
                    Ok((pending, work))
                });
                lane.close();
                result
            })
        })
    }
}

/// Verify every local final on the lane. Each check is read-only apart from
/// re-establishing durability of an already published file.
async fn verify_local_finals<'s>(
    parts: &'s TransactionParts<'_>,
    pending: Arc<PendingTransaction>,
    lane: &lane::BlockingLane<'s>,
) -> Result<()> {
    let checks = (0..pending.parts.len()).map(|index| {
        let pending = Arc::clone(&pending);
        lane.run(move || {
            let part = &pending.parts[index];
            if parts.verify_final_local(&pending, part)?
                != crate::writer::protected::PartPresence::Present
            {
                bail!("committed transaction is missing a required final part");
            }
            Ok(())
        })
    });
    // Every started check is awaited before the first error is returned.
    let mut first_error = None;
    for result in futures::future::join_all(checks).await {
        if let (Err(error), None) = (result, &first_error) {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn part_compression(compression: Compression) -> PartCompression {
    match compression.canonical() {
        Compression::None => PartCompression::None,
        Compression::Snappy => PartCompression::Snappy,
        Compression::Gzip => PartCompression::Gzip,
        Compression::Zstd => PartCompression::Zstd,
        Compression::ZstdWithLevel(level) => {
            PartCompression::ZstdWithLevel(level.compression_level())
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    WritingPersisted,
    Staged(u32),
    ReceiptPersisted(u32),
    Published(u32),
    CommittedPersisted,
    /// A table's Delta commit is durable (its part's entry index).
    DeltaCommitted(u32),
    /// Every Delta commit of the transaction is durable.
    DeltaCommittedAll,
    AuthorityAdvanced,
    MirrorReconciled,
    PendingCleared,
    RollbackComplete,
    /// Recovery advanced authority after rolling a Committed transaction
    /// forward.
    RecoveryAuthorityAdvanced,
    CompletionAuthorityAdvanced,
    CompletionMirrorReconciled,
}
fn checkpoint(stage: Stage) -> Result<()> {
    #[cfg(test)]
    tests::checkpoint(stage)?;
    #[cfg(not(test))]
    if pipeline::fault::fires("crash-at", &format!("{stage:?}")) {
        // `FIREPARQ_DEBUG_FAULT=crash-at:<Stage>` (debug builds): an abrupt
        // process death at a transaction boundary, e.g. `CommittedPersisted`
        // or `AuthorityAdvanced`.
        std::process::abort();
    }
    Ok(())
}

/// Whether the debug fault `kind:table` is set (`FIREPARQ_DEBUG_FAULT`, debug
/// builds only), for the session's Delta table creation.
pub(super) fn debug_fault(kind: &str, table: &str) -> bool {
    pipeline::fault::fires(kind, table)
}

#[cfg(test)]
mod tests;
