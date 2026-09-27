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
use super::maintenance::{IngestionTarget, MergeJournals};
use super::mirror::ProtectedMirror;
use super::parts::TransactionParts;
use super::state::*;
use super::store::TransactionStateStore;
use crate::cli::AwsConfig;
use crate::config::{BlockMetadata, Compression, Config};
use crate::cursor::CursorState;
use crate::dataset_lock::{session::SessionPermit, DatasetOwnership, MutationScope};
use crate::delta::types::DeltaTypes;
use crate::maintenance::discovery::ListingStats;
use crate::metrics::PipelineMetrics;
use crate::traits::BlockIdentity;
use crate::writer::ParquetFileMetadata;

/// The caller gets schemas by flushing a newly constructed, empty mapper. No
/// blockchain payload is consumed and the complete inventory is frozen before
/// opening Blocks, including tables which emit zero rows in this run. Each
/// digest is of the table's Delta data file schema (`delta_types`, #643):
/// the schema every part of the table is written with.
pub fn declare_inventory(
    empty_batches: &HashMap<String, RecordBatch>,
    declared_names: &[&str],
    delta_types: &DeltaTypes,
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
            let schema = delta_types.data_schema(name, batch.schema().as_ref())?;
            Ok((
                name.clone(),
                Digest::parse(crate::writer::protected::schema_sha256(&schema)?)?,
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
    /// Digests of the Delta data file schemas, from [`declare_inventory`]
    /// with the same `delta_types`.
    pub tables: BTreeMap<String, Digest>,
    /// The family's Delta type decisions, applied to every flush.
    pub delta_types: DeltaTypes,
}

/// Session-local spelling of the shared `From<&Config>` conversion.
pub(crate) fn aws_config(config: &Config) -> AwsConfig {
    AwsConfig::from(config)
}

fn descriptor(config: &Config, mapper: MapperSemantics) -> Result<StreamDescriptor> {
    let origin = config
        .start_block
        .context("protected ingestion requires a resolved original start")?;
    let routing_policy = match mapper.family {
        // Solana blocks may lack a time: they route by the last known one.
        BlockFamily::Solana => RoutingPolicy::SolanaLastKnownV1,
        // Other chains permit a leading timestamp bootstrap at genesis.
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
        partition: PartitionPolicy::Date,
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

/// Ownership scopes for one protected build: the output root plus the cursor
/// mirror resolved by exactly the binding that authority records. Deriving the
/// guarded bucket/key or file from that binding (`config.cursor_path`) means
/// ownership can never cover a different location than the one the session
/// later writes.
pub fn ingestion_mutation_scopes(config: &Config) -> Result<Vec<MutationScope>> {
    let output = config
        .output
        .to_str()
        .context("output path must be UTF-8")?;
    let mut scopes = vec![MutationScope::directory(output)];
    match resolve_mirror_binding(output, config.cursor_path.as_deref(), &aws_config(config))? {
        MirrorBinding::Disabled => {}
        MirrorBinding::Local { absolute_path } => scopes.push(MutationScope::file(absolute_path)),
        MirrorBinding::S3 { bucket, key, .. } => {
            scopes.push(MutationScope::file(format!("s3://{bucket}/{key}")))
        }
    }
    Ok(scopes)
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
        if is_pre_v1_default_mirror(output, &authority.descriptor.mirror, &configured, &aws) {
            bail!("{PRE_V1_DEFAULT_MIRROR}");
        }
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

/// Refusal for a dataset created before v1.0.0 with the then-default mirror
/// `<dataset root>/cursor.parquet`, resumed with the current default
/// `_fireparq/cursor.parquet`. The binding is part of the stream identity, so
/// the mirror is neither moved nor rebound; no private path is echoed.
pub const PRE_V1_DEFAULT_MIRROR: &str = "this protected dataset was created with the pre-v1.0.0 default cursor mirror at the dataset root (cursor.parquet); the default is now _fireparq/cursor.parquet, but a mirror location is bound when a dataset is created. Rerun with --cursor cursor.parquet (CURSOR=cursor.parquet) to resume it";

/// Whether the stored binding is the pre-v1.0.0 default mirror and the
/// configured one is the current default, for the same dataset root.
fn is_pre_v1_default_mirror(
    output: &str,
    stored: &MirrorBinding,
    configured: &MirrorBinding,
    aws: &crate::cli::AwsConfig,
) -> bool {
    let resolve = |cursor| resolve_mirror_binding(output, Some(cursor), aws).ok();
    resolve(crate::cursor::CURSOR_PARQUET_FILENAME).as_ref() == Some(stored)
        && resolve(crate::artifacts::DEFAULT_CURSOR_MIRROR).as_ref() == Some(configured)
}

/// The mirror binding is part of the immutable stream identity. Name the
/// actionable difference without echoing private paths or cursor values.
fn mirror_binding_mismatch(stored: &MirrorBinding, configured: &MirrorBinding) -> &'static str {
    match (stored, configured) {
        (MirrorBinding::Disabled, _) => {
            "this protected dataset was created without a cursor mirror; rerun with --cursor none (a mirror cannot be added to an existing dataset)"
        }
        (_, MirrorBinding::Disabled) => {
            "this protected dataset has a bound cursor mirror; --cursor none cannot disable it, so rerun with its original --cursor"
        }
        _ => {
            "configured cursor binding differs from authority; rerun with the original --cursor (changing it requires an explicit migration)"
        }
    }
}

pub struct IngestionSession<'a> {
    controller: TransactionController<'a, ProtectedMirror<'a>>,
    frontier: AcceptedFrontier,
    delta_types: DeltaTypes,
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
        let delta_types = mapper.delta_types;
        let expected = descriptor(config, mapper)?;
        let aws = aws_config(config);
        let permit = reserve(&expected.output, ownership)?;
        let listing = ListingStats::default();
        let started = std::time::Instant::now();
        let opened = Self::open_reserved(
            config,
            expected,
            delta_types,
            &aws,
            ownership,
            permit,
            metrics,
            shutdown,
            &listing,
        )
        .await;
        record_startup_listing(metrics, &listing, started.elapsed(), opened.is_ok());
        opened
    }

    /// Startup reads only control state on resume: the authority, the pending
    /// journal, the merge intent record and the mirror, plus one control-prefix
    /// request per ancestor directory. The whole tree is listed only when the
    /// dataset is created, or when a merge intent says journals may exist (#655).
    #[allow(clippy::too_many_arguments)]
    async fn open_reserved(
        config: &Config,
        expected: StreamDescriptor,
        delta_types: DeltaTypes,
        aws: &AwsConfig,
        ownership: &'a DatasetOwnership,
        permit: SessionPermit<'a>,
        metrics: Option<&'a PipelineMetrics>,
        shutdown: Option<&'a AtomicBool>,
        listing: &ListingStats,
    ) -> Result<Self> {
        let resuming = existing(&expected.output, ownership).await?.is_some();
        super::maintenance::validate_ingestion_target(
            &expected.output,
            ownership,
            if resuming {
                IngestionTarget::Resume
            } else {
                IngestionTarget::Create
            },
            listing,
        )
        .await?;
        let service = mirror_service(&expected.mirror, aws)?;
        let mut mirror = ProtectedMirror::new(ownership, &expected.mirror, service.as_ref())?;
        if let Some(metrics) = metrics {
            mirror = mirror.with_metrics(metrics);
        }
        if let Some(shutdown) = shutdown {
            mirror = mirror.with_shutdown(shutdown);
        }
        if !resuming {
            super::eligibility::require_initializable(&expected, ownership, aws, &mirror, listing)
                .await?;
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
        super::maintenance::validate_ingestion_recovery_order(
            &expected.output,
            ownership,
            MergeJournals::IfIntended,
            listing,
        )
        .await?;
        let controller = TransactionController::open_reserved(
            states(&expected.output, ownership)?,
            parts,
            mirror,
            &expected,
            permit,
        )
        .await?
        .with_concurrency(config.flush_concurrency)?;
        super::maintenance::prepare_ingestion(
            &expected.output,
            ownership,
            &controller.authority().descriptor.id()?,
            listing,
        )
        .await?;
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
            delta_types,
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
        if !self.authority().descriptor.final_blocks_only {
            require_prefix_stream_ordinals(&batches, &prefix)?;
        }
        // The flush boundary (#643): every table becomes its Delta data file
        // batch with checked casts, before the transaction journals anything.
        // A value that does not fit its Delta type refuses the whole flush.
        let batches = self.delta_types.data_batches(batches, &metadata)?;
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

/// Non-final rows carry the accepted-event ordinal of the envelope that produced
/// them (`stream_ordinal`, assigned by [`AcceptedFrontier::receive`]). Every
/// nonempty table of a flush must hold only ordinals of the frozen prefix it
/// commits, so no row can claim an event outside its own transaction. Together
/// with contiguous prefixes resumed from the durable checkpoint, this keeps the
/// column strictly increasing in delivery order across flushes and restarts.
fn require_prefix_stream_ordinals(
    batches: &HashMap<String, RecordBatch>,
    prefix: &AcceptedPrefix,
) -> Result<()> {
    use arrow::array::{Array, UInt64Array};
    for (table, batch) in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let ordinals = batch
            .column_by_name(crate::traits::STREAM_ORDINAL_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .with_context(|| format!("non-final table {table} lacks a UInt64 stream_ordinal"))?;
        ensure!(
            ordinals.null_count() == 0,
            "non-final table {table} has a null stream_ordinal"
        );
        let within = arrow::compute::min(ordinals)
            .zip(arrow::compute::max(ordinals))
            .is_some_and(|(min, max)| min >= prefix.first_ordinal && max <= prefix.last_ordinal);
        ensure!(
            within,
            "non-final table {table} has a stream_ordinal outside the accepted prefix {}..={}",
            prefix.first_ordinal,
            prefix.last_ordinal
        );
    }
    Ok(())
}

/// Export and log what opening the dataset listed (#655). A resume lists no
/// data objects, so its request count stays at the ancestor depth of the root
/// whatever the dataset's size.
fn record_startup_listing(
    metrics: Option<&PipelineMetrics>,
    listing: &ListingStats,
    elapsed: std::time::Duration,
    opened: bool,
) {
    if let Some(metrics) = metrics {
        metrics
            .startup_list_requests
            .set(i64::try_from(listing.requests()).unwrap_or(i64::MAX));
        metrics
            .startup_listing_seconds
            .set(listing.duration().as_secs_f64());
    }
    tracing::info!(
        list_requests = listing.requests(),
        listed_objects = listing.objects(),
        listing_ms = u64::try_from(listing.duration().as_millis()).unwrap_or(u64::MAX),
        open_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        opened,
        "protected dataset startup checks finished"
    );
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
