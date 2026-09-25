//! All-table publication and restart decisions. This private controller is not
//! yet wired to command ingestion; eligibility, accepted envelopes and completion
//! proof must be supplied by the final session integration.

use anyhow::{bail, Context, Result};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use super::frontier::AcceptedFrontier;
use super::parts::{writer_plan, TransactionParts};
use super::state::{
    AcceptedPrefix, AuthorityState, Digest, PartCompression, PartReceipt, PartitionPolicy,
    PendingTransaction, StreamDescriptor, TablePlan, TransactionPhase,
};
use super::store::{TransactionStateStore, Versioned};
use crate::config::{BlockMetadata, Compression, Partition};
use crate::writer::protected::PreparedFlush;
use crate::writer::{ParquetFileMetadata, ParquetTableWriter};

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
    _session: crate::dataset_lock::session::SessionPermit<'a>,
}

pub struct CommittedFlush {
    pub checkpoint_id: Digest,
    pub ordinal: u64,
    pub rows: u64,
    pub bytes: u64,
    pub files: usize,
    pub tables: Vec<CommittedTable>,
}
pub struct CommittedTable {
    pub table: String,
    pub rows: u64,
    pub bytes: u64,
}

impl<'a, M: MirrorAction> TransactionController<'a, M> {
    /// Recovery happens before the caller opens its Firehose Blocks stream.
    /// The expected descriptor must describe the resolved runtime configuration;
    /// merely reading a descriptor from disk does not validate an append request.
    pub async fn open(
        states: TransactionStateStore<'a>,
        parts: TransactionParts<'a>,
        mirror: M,
        expected: &StreamDescriptor,
    ) -> Result<Self> {
        let session = parts.acquire_session()?;
        Self::open_reserved(states, parts, mirror, expected, session).await
    }

    pub(super) async fn open_reserved(
        states: TransactionStateStore<'a>,
        parts: TransactionParts<'a>,
        mirror: M,
        expected: &StreamDescriptor,
        session: crate::dataset_lock::session::SessionPermit<'a>,
    ) -> Result<Self> {
        expected.validate()?;
        let snapshot = states.load().await?;
        let mut authority = snapshot.authority.context(
            "protected ingestion authority is absent; eligible-root initialization is required",
        )?;
        if authority.payload.descriptor != *expected {
            bail!("existing authoritative stream differs from this request; use a new output root for changed semantics or bindings");
        }
        let mut mirror_reconciled = false;
        if let Some(pending) = snapshot.pending {
            match pending.payload.phase {
                TransactionPhase::Writing => {
                    parts.rollback_writing(&pending.payload).await?;
                    checkpoint(Stage::RollbackComplete)?;
                    states.clear(&authority, &pending).await?;
                }
                TransactionPhase::Committed => {
                    // Verify first, even if authority was already advanced by
                    // the interrupted process. Missing/corrupt data never becomes
                    // a reason to remap a committed prefix.
                    parts.verify_all_finals(&pending.payload).await?;
                    if authority.payload.checkpoint.id == pending.payload.predecessor {
                        authority = states.advance(&authority, &pending).await?;
                    }
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
            _session: session,
        })
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
        let partition = runtime_partition(&self.authority.payload.descriptor.partition);
        let routing = ParquetTableWriter::new(PathBuf::new(), partition.clone(), compression);
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
                    routing.partition_suffix(table, &metadata)?
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
            partition,
            metadata,
            compression,
            file_metadata,
        )?;
        self.parts.require_unoccupied(&pending).await?;
        let mut pending = self.states.begin(&self.authority, pending).await?;
        checkpoint(Stage::WritingPersisted)?;
        for part in prepared.parts() {
            let encoded = prepared.encode(part.entry_index)?;
            self.parts.stage(&encoded)?;
            checkpoint(Stage::Staged(part.entry_index))?;
            let receipt = PartReceipt {
                byte_size: encoded.receipt().byte_size,
                sha256: Digest::parse(encoded.receipt().sha256.clone())?,
            };
            pending = self
                .states
                .record_receipt(&self.authority, &pending, part.entry_index, receipt)
                .await?;
            checkpoint(Stage::ReceiptPersisted(part.entry_index))?;
            self.parts.publish(&encoded).await?;
            checkpoint(Stage::Published(part.entry_index))?;
        }
        self.parts.verify_all_finals(&pending.payload).await?;
        pending = self
            .states
            .mark_committed(&self.authority, &pending)
            .await?;
        checkpoint(Stage::CommittedPersisted)?;
        self.authority = self.states.advance(&self.authority, &pending).await?;
        checkpoint(Stage::AuthorityAdvanced)?;
        self.mirror.reconcile(&self.authority.payload).await?;
        checkpoint(Stage::MirrorReconciled)?;
        self.parts.cleanup_temporaries(&pending.payload)?;
        self.states.clear(&self.authority, &pending).await?;
        checkpoint(Stage::PendingCleared)?;
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
        };
        self.failed = false;
        Ok(result)
    }
}

fn runtime_partition(partition: &PartitionPolicy) -> Partition {
    match partition {
        PartitionPolicy::None => Partition::None,
        PartitionPolicy::BlockRange { size, anchor } => Partition::BlockRange {
            size: *size,
            start_block: Some(*anchor),
        },
        PartitionPolicy::Date => Partition::Date,
        PartitionPolicy::Hour => Partition::Hour,
        PartitionPolicy::Minute => Partition::Minute,
        PartitionPolicy::Second => Partition::Second,
    }
}
fn part_compression(compression: Compression) -> PartCompression {
    match compression {
        Compression::None => PartCompression::None,
        Compression::Snappy => PartCompression::Snappy,
        Compression::Gzip => PartCompression::Gzip,
        Compression::Zstd => PartCompression::Zstd,
        Compression::ZstdWithLevel(level) if level.compression_level() == 3 => {
            PartCompression::Zstd
        }
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
    AuthorityAdvanced,
    MirrorReconciled,
    PendingCleared,
    RollbackComplete,
    CompletionAuthorityAdvanced,
    CompletionMirrorReconciled,
}
fn checkpoint(stage: Stage) -> Result<()> {
    #[cfg(test)]
    tests::checkpoint(stage)?;
    #[cfg(not(test))]
    let _ = stage;
    Ok(())
}

#[cfg(test)]
mod tests;
