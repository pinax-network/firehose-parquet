//! Runtime-facing assembly of storage identity, eligibility, recovery and the
//! accepted frontier. Ownership and maintenance recovery precede stream access.

use anyhow::{bail, ensure, Context, Result};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::AtomicBool;

use super::binding::{
    mirror_service, resolve_mirror_binding, resolve_output_identity, validate_runtime_bindings,
};
use super::controller::{CommittedFlush, TransactionController};
use super::frontier::AcceptedFrontier;
use super::mirror::ProtectedMirror;
use super::parts::TransactionParts;
use super::state::*;
use super::store::TransactionStateStore;
use crate::cli::AwsConfig;
use crate::config::{BlockMetadata, Compression, Config, Partition};
use crate::cursor::CursorState;
use crate::dataset_lock::{session::SessionPermit, DatasetOwnership};
use crate::metrics::PipelineMetrics;
use crate::traits::BlockIdentity;
use crate::writer::ParquetFileMetadata;

/// The caller gets schemas by flushing a newly constructed, empty mapper. No
/// blockchain payload is consumed and the complete inventory is frozen before
/// opening Blocks, including tables which emit zero rows in this run.
pub fn declare_inventory(
    empty_batches: &HashMap<String, RecordBatch>,
    declared_names: &[&str],
) -> Result<BTreeMap<String, Digest>> {
    let declared: std::collections::BTreeSet<_> = declared_names.iter().copied().collect();
    ensure!(
        declared.len() == declared_names.len() && !declared.is_empty(),
        "mapper table inventory is empty or duplicated"
    );
    ensure!(
        empty_batches.len() == declared.len()
            && empty_batches
                .keys()
                .all(|name| declared.contains(name.as_str())),
        "empty mapper flush does not declare its exact table inventory"
    );
    empty_batches
        .iter()
        .map(|(name, batch)| {
            ensure!(
                batch.num_rows() == 0,
                "schema declaration must precede mapping any events"
            );
            Ok((
                name.clone(),
                Digest::parse(crate::writer::protected::schema_sha256(
                    batch.schema().as_ref(),
                )?)?,
            ))
        })
        .collect()
}

pub struct MapperSemantics {
    pub chain: String,
    pub family: BlockFamily,
    pub bytes_encoding: String,
    pub extended: bool,
    pub with_votes: bool,
    pub include_failed_transactions: bool,
    pub tables: BTreeMap<String, Digest>,
}

pub(crate) fn aws_config(config: &Config) -> AwsConfig {
    AwsConfig {
        aws_access_key_id: config.aws_access_key_id.clone(),
        aws_secret_access_key: config.aws_secret_access_key.clone(),
        aws_session_token: config.aws_session_token.clone(),
        aws_region: config.aws_region.clone(),
        aws_endpoint_url: config.aws_endpoint_url.clone(),
    }
}

fn descriptor(config: &Config, mapper: MapperSemantics) -> Result<StreamDescriptor> {
    let origin = config
        .start_block
        .context("protected ingestion requires a resolved original start")?;
    let partition = match config.partition {
        Partition::None => PartitionPolicy::None,
        Partition::BlockRange { size, start_block } => {
            ensure!(
                start_block == Some(origin),
                "block range routing must use the authoritative original start"
            );
            PartitionPolicy::BlockRange {
                size,
                anchor: origin,
            }
        }
        Partition::Date => PartitionPolicy::Date,
        Partition::Hour => PartitionPolicy::Hour,
        Partition::Minute => PartitionPolicy::Minute,
        Partition::Second => PartitionPolicy::Second,
    };
    let time_partition = !matches!(
        partition,
        PartitionPolicy::None | PartitionPolicy::BlockRange { .. }
    );
    let routing_policy = match (mapper.family, time_partition) {
        (BlockFamily::Solana, true) => RoutingPolicy::SolanaLastKnownV1,
        (BlockFamily::Solana, false) => RoutingPolicy::DirectV1,
        // Existing non-nullable chain ingestion permits a leading timestamp
        // bootstrap even for a block-number partition. Bind that policy too.
        _ => RoutingPolicy::GenesisLookaheadV1,
    };
    let aws = aws_config(config);
    let output = config
        .output
        .to_str()
        .context("output path must be UTF-8")?;
    let descriptor = StreamDescriptor {
        format_version: FORMAT_VERSION,
        mapper_epoch: MAPPER_EPOCH.into(),
        chain: mapper.chain,
        family: mapper.family,
        bytes_encoding: mapper.bytes_encoding,
        extended: mapper.extended && mapper.family == BlockFamily::Evm,
        with_votes: mapper.with_votes && mapper.family == BlockFamily::Solana,
        include_failed_transactions: mapper.include_failed_transactions,
        tables: mapper.tables,
        partition,
        origin_start: origin,
        final_blocks_only: config.final_blocks_only,
        routing_policy,
        output: resolve_output_identity(output, &aws)?,
        mirror: resolve_mirror_binding(output, config.cursor_path.as_deref(), &aws)?,
    };
    descriptor.validate()?;
    Ok(descriptor)
}

fn reserve<'a>(
    output: &StorageIdentity,
    ownership: &'a DatasetOwnership,
) -> Result<SessionPermit<'a>> {
    match output {
        StorageIdentity::Local { .. } => ownership
            .local()
            .context("local output is not owned")?
            .acquire_transaction_session(),
        StorageIdentity::S3 { bucket, .. } => ownership
            .remote(bucket)
            .context("remote output is not owned")?
            .acquire_transaction_session(),
    }
}
fn states<'a>(
    output: &StorageIdentity,
    ownership: &'a DatasetOwnership,
) -> Result<TransactionStateStore<'a>> {
    match output {
        StorageIdentity::Local { canonical_root } => TransactionStateStore::local(
            Path::new(canonical_root),
            ownership.local().context("local output is not owned")?,
        ),
        StorageIdentity::S3 { bucket, prefix, .. } => TransactionStateStore::s3(
            prefix,
            ownership
                .remote(bucket)
                .context("remote output is not owned")?,
        ),
    }
}
async fn existing(
    output: &StorageIdentity,
    ownership: &DatasetOwnership,
) -> Result<Option<AuthorityState>> {
    ownership.revalidate_local_paths()?;
    if let StorageIdentity::Local { canonical_root } = output {
        match std::fs::symlink_metadata(canonical_root) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => bail!("cannot inspect authoritative output root"),
            Ok(metadata) => ensure!(
                metadata.is_dir(),
                "authoritative output root is not a directory"
            ),
        }
    }
    Ok(states(output, ownership)?
        .load()
        .await?
        .authority
        .map(|record| record.payload))
}

/// Refusal for `--cursor-override` on any mutating build, including a new root
/// where there is nothing to override: protected output never rewinds or resets.
pub const CURSOR_OVERRIDE_REFUSED: &str = "--cursor-override is only valid with --dry-run: a real build cannot rewind protected output or change its semantics. Omit it to resume from output authority, or build into a new empty output root (with an absent cursor mirror) to change the range or mapper settings";

/// Compatibility fields used only to resolve CLI defaults. This reads mandatory
/// authority, never an optional cursor file. The final open revalidates the full
/// mapper descriptor and performs pending recovery before Blocks may connect.
/// `cursor_override` is refused whether or not authority exists yet.
pub async fn load_authoritative_resume(
    config: &Config,
    ownership: &DatasetOwnership,
    cursor_override: bool,
) -> Result<Option<CursorState>> {
    ensure!(!cursor_override, CURSOR_OVERRIDE_REFUSED);
    let aws = aws_config(config);
    let output = config
        .output
        .to_str()
        .context("output path must be UTF-8")?;
    let identity = resolve_output_identity(output, &aws)?;
    let _permit = reserve(&identity, ownership)?;
    let Some(authority) = existing(&identity, ownership).await? else {
        return Ok(None);
    };
    validate_runtime_bindings(&authority.descriptor, output, &aws)?;
    let configured = resolve_mirror_binding(output, config.cursor_path.as_deref(), &aws)?;
    if authority.descriptor.mirror != configured {
        bail!(
            "{}",
            mirror_binding_mismatch(&authority.descriptor.mirror, &configured)
        );
    }
    if let Some(requested) = config.start_block {
        ensure!(requested==authority.descriptor.origin_start,"explicit start differs from the stream's original start; use a new output root instead of rewinding or skipping");
    }
    Ok(Some(super::mirror::resume_parameters(&authority)?))
}

/// The mirror binding is part of the immutable stream identity. Name the
/// actionable difference without echoing private paths or cursor values.
fn mirror_binding_mismatch(stored: &MirrorBinding, configured: &MirrorBinding) -> &'static str {
    match (stored, configured) {
        (MirrorBinding::Disabled, _) => {
            "this protected dataset was created without a cursor mirror; rerun with --cursor none (a mirror cannot be added to an existing dataset)"
        }
        (_, MirrorBinding::Disabled) => {
            "this protected dataset has a bound cursor mirror; --cursor none cannot disable it, so rerun with its original --cursor/--cursor-template"
        }
        _ => {
            "configured cursor binding differs from authority; rerun with the original --cursor/--cursor-template (changing it requires an explicit migration)"
        }
    }
}

pub struct IngestionSession<'a> {
    controller: TransactionController<'a, ProtectedMirror<'a>>,
    frontier: AcceptedFrontier,
    failed: bool,
    metrics: Option<&'a PipelineMetrics>,
}

impl<'a> IngestionSession<'a> {
    pub async fn open(
        config: &Config,
        mapper: MapperSemantics,
        ownership: &'a DatasetOwnership,
        metrics: Option<&'a PipelineMetrics>,
        shutdown: Option<&'a AtomicBool>,
    ) -> Result<Self> {
        ensure!(
            !config.dry_run,
            "dry-run cannot open a mutating ingestion session"
        );
        let expected = descriptor(config, mapper)?;
        let aws = aws_config(config);
        let permit = reserve(&expected.output, ownership)?;
        super::maintenance::validate_ingestion_target(&expected.output, ownership).await?;
        let service = mirror_service(&expected.mirror, &aws)?;
        let mut mirror = ProtectedMirror::new(ownership, &expected.mirror, service.as_ref())?;
        if let Some(metrics) = metrics {
            mirror = mirror.with_metrics(metrics);
        }
        if let Some(shutdown) = shutdown {
            mirror = mirror.with_shutdown(shutdown);
        }
        if existing(&expected.output, ownership).await?.is_none() {
            super::eligibility::require_initializable(&expected, ownership, &aws, &mirror).await?;
            if matches!(expected.output, StorageIdentity::Local { .. }) {
                ownership.revalidate_local_paths()?;
                // Keep lexical alias spelling here to sync both parent chains.
                crate::writer::create_dir_all_durable(&config.output)?;
                ownership.revalidate_local_paths()?;
            }
            states(&expected.output, ownership)?
                .initialize(AuthorityState::initial(expected.clone())?)
                .await?;
        }
        let parts = match &expected.output {
            StorageIdentity::Local { canonical_root } => TransactionParts::local(
                Path::new(canonical_root),
                ownership.local().context("local output is not owned")?,
            )?,
            StorageIdentity::S3 { bucket, prefix, .. } => TransactionParts::s3(
                prefix,
                ownership
                    .remote(bucket)
                    .context("remote output is not owned")?,
                config.cache_control.as_deref().unwrap_or_default(),
            )?,
        };
        super::maintenance::validate_ingestion_recovery_order(&expected.output, ownership).await?;
        let controller = TransactionController::open_reserved(
            states(&expected.output, ownership)?,
            parts,
            mirror,
            &expected,
            permit,
        )
        .await?;
        super::maintenance::prepare_ingestion(&expected.output, ownership, &aws).await?;
        let frontier = AcceptedFrontier::resume(&controller.authority().checkpoint);
        if let (Some(metrics), Some(event)) =
            (metrics, controller.authority().checkpoint.event.as_ref())
        {
            metrics
                .cursor_last_block_num
                .set(i64::try_from(event.block_num).unwrap_or(i64::MAX));
        }
        Ok(Self {
            controller,
            frontier,
            failed: false,
            metrics,
        })
    }

    pub fn routing_anchor_source(&self) -> Option<(u64, i64)> {
        self.frontier
            .routing()
            .anchor
            .as_ref()
            .map(|anchor| (anchor.source_block_num, anchor.seconds))
    }

    /// Bridge the synchronous Blocks callback while allowing the multi-thread
    /// runtime to continue driving storage, metrics and shutdown signals.
    pub fn flush_blocking(
        &mut self,
        batches: HashMap<String, RecordBatch>,
        metadata: BlockMetadata,
        compression: Compression,
        file_metadata: ParquetFileMetadata,
    ) -> Result<Option<CommittedFlush>> {
        let runtime = tokio::runtime::Handle::try_current()
            .context("ingestion callback requires a Tokio runtime")?;
        ensure!(
            runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread,
            "ingestion callback requires a multi-thread runtime; await flush in async code"
        );
        tokio::task::block_in_place(|| {
            runtime.block_on(self.flush(batches, metadata, compression, file_metadata))
        })
    }

    pub(crate) fn authority(&self) -> &AuthorityState {
        self.controller.authority()
    }
    pub fn resume_cursor(&self) -> Option<&str> {
        self.authority()
            .checkpoint
            .event
            .as_ref()
            .map(|event| event.cursor.as_str())
    }
    pub fn routing_timestamp_hint(&self) -> Option<i64> {
        self.frontier
            .routing()
            .anchor
            .as_ref()
            .map(|anchor| anchor.seconds)
    }
    pub fn has_accepted(&self) -> Result<bool> {
        Ok(self.frontier.snapshot()?.is_some())
    }
    pub fn request_already_complete(&self, stop: u64) -> Result<bool> {
        self.controller.request_already_complete(stop)
    }

    fn ready(&self) -> Result<()> {
        ensure!(
            !self.failed,
            "ingestion session stopped after an unresolved operation; reopen through recovery"
        );
        Ok(())
    }
    pub fn receive(
        &mut self,
        cursor: String,
        identity: &BlockIdentity,
        step: i32,
        family: BlockFamily,
    ) -> Result<u64> {
        self.ready()?;
        self.failed = true;
        ensure!(
            family == self.authority().descriptor.family,
            "received payload family differs from the authoritative mapper"
        );
        let ordinal = self.frontier.receive(EventIdentity {
            cursor: OpaqueCursor::new(cursor)?,
            block_num: identity.block_num,
            block_id: identity.block_id.clone(),
            fork_step: step,
            source_timestamp: (identity.timestamp != 0).then_some(identity.timestamp),
        })?;
        self.failed = false;
        Ok(ordinal)
    }
    pub fn accept_filtered(&mut self, ordinal: u64) -> Result<()> {
        self.ready()?;
        self.failed = true;
        let event = self.frontier.received(ordinal)?;
        let descriptor = &self.authority().descriptor;
        ensure!(
            (descriptor.final_blocks_only && event.fork_step == 2)
                || event.block_num < descriptor.origin_start,
            "mapped event cannot be acknowledged as a filtered zero-row event"
        );
        self.frontier.accept_filtered(ordinal)?;
        self.failed = false;
        Ok(())
    }

    /// Call only after mapping succeeds. The source event and optional future
    /// anchor are resolved from received metadata here, not trusted caller labels.
    pub fn accept_mapped(
        &mut self,
        ordinal: u64,
        effective_timestamp: Option<i64>,
        lookahead: Option<u64>,
    ) -> Result<()> {
        self.ready()?;
        self.failed = true;
        ensure!(
            ordinal == self.frontier.next_accepted_ordinal()?,
            "mapping must resolve received envelopes in order"
        );
        let event = self.frontier.received(ordinal)?.clone();
        let policy = self.authority().descriptor.routing_policy;
        let anchor = match policy {
            RoutingPolicy::DirectV1 => {
                ensure!(
                    lookahead.is_none() && effective_timestamp == event.source_timestamp,
                    "direct routing changed the received source time"
                );
                None
            }
            RoutingPolicy::SolanaLastKnownV1 => {
                ensure!(
                    lookahead.is_none(),
                    "Solana routing cannot use a future anchor"
                );
                match event.source_timestamp {
                    Some(seconds) => Some(TimestampAnchor {
                        source_ordinal: ordinal,
                        source_block_num: event.block_num,
                        source_block_id: event.block_id.clone(),
                        seconds,
                        provenance: AnchorProvenance::AcceptedPrefix,
                    }),
                    None => Some(self.frontier.routing().anchor.clone().unwrap_or(
                        TimestampAnchor {
                            source_ordinal: 0,
                            source_block_num: 0,
                            source_block_id: String::new(),
                            seconds: SOLANA_GENESIS_ROUTING_SECONDS,
                            provenance: AnchorProvenance::SolanaGenesisFallback,
                        },
                    )),
                }
            }
            RoutingPolicy::GenesisLookaheadV1 => match event.source_timestamp {
                Some(seconds) => {
                    ensure!(
                        lookahead.is_none() && effective_timestamp == Some(seconds),
                        "observed source timestamp cannot be replaced by bootstrap routing"
                    );
                    None
                }
                None => {
                    if let Some(source_ordinal) = lookahead {
                        ensure!(
                            source_ordinal > ordinal,
                            "bootstrap routing requires a future received source"
                        );
                        let source = self.frontier.received(source_ordinal)?;
                        Some(TimestampAnchor {
                            source_ordinal,
                            source_block_num: source.block_num,
                            source_block_id: source.block_id.clone(),
                            seconds: source
                                .source_timestamp
                                .context("bootstrap anchor has no observed timestamp")?,
                            provenance: AnchorProvenance::Lookahead,
                        })
                    } else {
                        Some(self.frontier.routing().anchor.clone().filter(|anchor|anchor.provenance==AnchorProvenance::Lookahead).context("missing source timestamp has no received or authoritative bootstrap anchor")?)
                    }
                }
            },
        };
        if let Some(anchor) = &anchor {
            ensure!(
                effective_timestamp == Some(anchor.seconds),
                "mapper routing time differs from its received or authoritative source"
            );
        }
        self.frontier
            .accept(ordinal, RoutingCheckpoint { policy, anchor })?;
        self.failed = false;
        Ok(())
    }

    pub async fn flush(
        &mut self,
        batches: HashMap<String, RecordBatch>,
        metadata: BlockMetadata,
        compression: Compression,
        file_metadata: ParquetFileMetadata,
    ) -> Result<Option<CommittedFlush>> {
        self.ready()?;
        self.failed = true;
        let Some(prefix) = self.frontier.snapshot()? else {
            ensure!(
                batches.values().all(|batch| batch.num_rows() == 0),
                "mapper emitted rows without an accepted event prefix"
            );
            self.failed = false;
            return Ok(None);
        };
        let _buffer_metrics = SessionBufferMetrics::new(self.metrics, &batches, compression);
        let committed = self
            .controller
            .commit(
                prefix.clone(),
                batches,
                metadata,
                compression,
                file_metadata,
            )
            .await?;
        self.frontier.acknowledge(&prefix)?;
        if let Some(metrics) = self.metrics {
            for table in &committed.tables {
                let labels = crate::metrics::TableLabels {
                    table: table.table.clone(),
                };
                metrics.files_written_total.get_or_create(&labels).inc();
                metrics
                    .file_bytes_total
                    .get_or_create(&labels)
                    .inc_by(table.bytes);
                metrics
                    .rows_written_total
                    .get_or_create(&labels)
                    .inc_by(table.rows);
            }
        }
        self.failed = false;
        Ok(Some(committed))
    }
    pub async fn complete_request(
        &mut self,
        stop: u64,
        clean_stream_completed: bool,
    ) -> Result<bool> {
        self.ready()?;
        self.failed = true;
        let changed = self
            .controller
            .complete_request(stop, &self.frontier, clean_stream_completed)
            .await?;
        self.failed = false;
        Ok(changed)
    }
}

/// A failed/cancelled controller consumes and drops its in-memory prepared map;
/// these gauges describe that actual ownership rather than a fictitious retry
/// buffer. Durable retry comes only from reopening journal recovery.
struct SessionBufferMetrics<'a> {
    metrics: Option<&'a PipelineMetrics>,
    tables: Vec<String>,
}
impl<'a> SessionBufferMetrics<'a> {
    fn new(
        metrics: Option<&'a PipelineMetrics>,
        batches: &HashMap<String, RecordBatch>,
        compression: Compression,
    ) -> Self {
        let tables = batches.keys().cloned().collect();
        if let Some(metrics) = metrics {
            let mut bytes = 0_u64;
            for (table, batch) in batches {
                bytes = bytes.saturating_add(batch.get_array_memory_size() as u64);
                metrics
                    .buffer_rows
                    .get_or_create(&crate::metrics::TableLabels {
                        table: table.clone(),
                    })
                    .set(i64::try_from(batch.num_rows()).unwrap_or(i64::MAX));
            }
            let compressed = (bytes as f64 * crate::writer::compression_ratio(&compression)) as u64;
            metrics
                .buffer_estimated_bytes
                .set(i64::try_from(compressed).unwrap_or(i64::MAX));
        }
        Self { metrics, tables }
    }
}
impl Drop for SessionBufferMetrics<'_> {
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics {
            metrics.buffer_estimated_bytes.set(0);
            for table in &self.tables {
                metrics
                    .buffer_rows
                    .get_or_create(&crate::metrics::TableLabels {
                        table: table.clone(),
                    })
                    .set(0);
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod maintenance_tests;
