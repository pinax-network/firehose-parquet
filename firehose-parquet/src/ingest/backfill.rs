//! Parallel backfill of a new dataset (#725).
//!
//! A dataset has one ingestion lane, so its history is otherwise written by
//! one sequential `build`. A backfill splits a bounded range into fixed,
//! adjacent block ranges (lanes) and lets one `build` per lane write into the
//! same Delta tables at the same time:
//!
//! 1. **open** writes the plan (`.fireparq-ingest/backfill.json`) into an
//!    empty root: the dataset's stream descriptor, the stop block and the lane
//!    size. The root has no authority yet, so an ordinary `build` is refused.
//! 2. Each **lane** is the ordinary transaction machine scoped to one range.
//!    Its owner record, checkpoint chain and pending journal live under
//!    `.fireparq-ingest/lanes/<start>-<stop>/`, its stream is the dataset's
//!    with the lane's start, and that stream names its parts and is its `txn`
//!    application id. The tables carry the dataset's descriptor, so every
//!    lane and the later live writer validate the same tables.
//! 3. **seal** requires every lane complete and writes the dataset's first
//!    authority: the whole range accepted, ending at the last lane's final
//!    event. From then on the root is an ordinary single-stream dataset.
//!
//! Lanes are S3 only, final-only, and for families whose rows are routed by
//! their own block time (not Solana's last-known time).

use anyhow::{bail, ensure, Context, Result};
use futures::StreamExt;
use object_store::ObjectStore;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use super::binding::{mirror_service, resolve_output_identity, validate_runtime_bindings};
use super::maintenance::IngestionTarget;
use super::mirror::ProtectedMirror;
use super::session::MapperSemantics;
use super::state::{
    AuthorityState, EventIdentity, RoutingPolicy, StorageIdentity, StreamDescriptor,
};
use super::store::TransactionStateStore;
use crate::cli::AwsConfig;
use crate::config::Config;
use crate::dataset_lock::{DatasetOwnership, MutationScope};
use crate::dataset_lock_s3::{LaneScope, OwnerRecord, OwnerState, S3Ownership};
use crate::durable_state::{ControlKey, CONTROL_DIRECTORY, MAX_CONTROL_BYTES};
use crate::maintenance::discovery::ListingStats;

const FORMAT_VERSION: u32 = 1;
/// Lanes a plan may hold: `seal` and `status` read every lane's records.
const MAX_LANES: u64 = 100_000;
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const LANE_READ_CONCURRENCY: usize = 16;
/// Incomplete lanes a refusal or a status lists by name.
const LISTED_LANES: usize = 20;

/// Refusal of an ordinary `build` at a root whose backfill is not sealed.
pub const UNSEALED: &str = "this dataset is being backfilled in parallel and is not sealed: only its lanes may write. Run `fireparq build` with the --start-block and --stop-block of a planned lane (`fireparq backfill status` lists them), then `fireparq backfill seal` once every lane is complete";

/// The plan of one parallel backfill, written once when it is opened.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillPlan {
    pub format_version: u32,
    /// The dataset's stream: its start is the backfill's first block. Every
    /// table records it, and the sealed dataset's authority holds it.
    pub descriptor: StreamDescriptor,
    /// Exclusive end of the backfilled range, and where the live writer
    /// continues after the seal.
    pub stop: u64,
    /// Blocks per lane; the last lane may be shorter.
    pub lane_blocks: u64,
}

impl BackfillPlan {
    pub fn new(descriptor: StreamDescriptor, stop: u64, lane_blocks: u64) -> Result<Self> {
        let plan = Self {
            format_version: FORMAT_VERSION,
            descriptor,
            stop,
            lane_blocks,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.format_version == FORMAT_VERSION,
            "unsupported backfill plan version"
        );
        self.descriptor.validate()?;
        ensure!(
            matches!(self.descriptor.output, StorageIdentity::S3 { .. }),
            "a parallel backfill requires S3 output"
        );
        ensure!(
            self.descriptor.final_blocks_only,
            "a parallel backfill requires a final-only stream (--final-blocks-only)"
        );
        ensure!(
            self.descriptor.routing_policy == RoutingPolicy::GenesisLookaheadV1,
            "a parallel backfill is not supported for this block family: its rows are routed \
             by the previous block's time, which a lane starting mid-chain does not have"
        );
        ensure!(
            self.stop > self.descriptor.origin_start,
            "the backfill's stop block must follow its start block"
        );
        ensure!(self.lane_blocks > 0, "--lane-blocks must be at least 1");
        ensure!(
            self.lane_count() <= MAX_LANES,
            "the backfill would have more than {MAX_LANES} lanes; use a larger --lane-blocks"
        );
        Ok(())
    }

    pub fn start(&self) -> u64 {
        self.descriptor.origin_start
    }

    pub fn lane_count(&self) -> u64 {
        (self.stop - self.start()).div_ceil(self.lane_blocks)
    }

    /// Every lane as `[start, stop)`, in block order: adjacent, disjoint and
    /// covering the whole range.
    pub fn lanes(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        (0..self.lane_count()).map(|index| {
            let start = self.start() + index * self.lane_blocks;
            (start, self.stop.min(start.saturating_add(self.lane_blocks)))
        })
    }

    /// Whether `[start, stop)` is exactly one planned lane.
    pub fn has_lane(&self, start: u64, stop: u64) -> bool {
        start >= self.start()
            && start < self.stop
            && (start - self.start()).is_multiple_of(self.lane_blocks)
            && stop == self.stop.min(start.saturating_add(self.lane_blocks))
    }

    fn require_lane(&self, start: u64, stop: u64) -> Result<()> {
        ensure!(
            self.has_lane(start, stop),
            "blocks {start}..{stop} are not a lane of this dataset's backfill, which covers \
             {}..{} in lanes of {} blocks from {}; a lane must be requested with exactly its \
             --start-block and --stop-block",
            self.start(),
            self.stop,
            self.lane_blocks,
            self.start()
        );
        Ok(())
    }

    /// The stream of the lane starting at `start`.
    pub(crate) fn lane_descriptor(&self, start: u64) -> StreamDescriptor {
        self.descriptor.backfill_lane(start)
    }
}

fn remote(path: &str) -> Result<(String, String)> {
    ensure!(
        path.starts_with("s3://"),
        "a parallel backfill requires an s3://bucket/prefix dataset root"
    );
    let (bucket, prefix) = crate::writer::parse_s3_url(path)?;
    let prefix = prefix.trim_end_matches('/').to_owned();
    super::state::validate_relative_path(&prefix, true)?;
    ensure!(
        !crate::artifacts::is_control_path(&prefix) && !crate::artifacts::is_in_artifacts_dir(path),
        "a backfill is addressed by its dataset root, not a control or artifact path"
    );
    Ok((bucket, prefix))
}

fn control_directory(prefix: &str) -> String {
    if prefix.is_empty() {
        CONTROL_DIRECTORY.to_owned()
    } else {
        format!("{prefix}/{CONTROL_DIRECTORY}")
    }
}

/// One control record, read without an owner: GET only.
async fn read_control<T: DeserializeOwned>(
    store: &dyn ObjectStore,
    directory: &str,
    key: ControlKey,
) -> Result<Option<T>> {
    let path = object_store::path::Path::from(format!("{directory}/{}", key.filename()));
    tokio::time::timeout(READ_TIMEOUT, async {
        let object = match store.get(&path).await {
            Ok(object) => object,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(_) => bail!("backfill control record read failed"),
        };
        ensure!(
            object.meta.size <= MAX_CONTROL_BYTES as u64,
            "backfill control record exceeds its byte limit"
        );
        let mut bytes = Vec::new();
        let mut stream = object.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|_| anyhow::anyhow!("backfill control record read failed"))?;
            ensure!(
                chunk.len() <= MAX_CONTROL_BYTES.saturating_sub(bytes.len()),
                "backfill control record exceeds its byte limit"
            );
            bytes.extend_from_slice(&chunk);
        }
        let (_, payload) = crate::durable_state::decode_slot(&bytes)?;
        payload
            .map(|payload| {
                serde_json::from_value(payload)
                    .map_err(|_| anyhow::anyhow!("invalid backfill control record payload"))
            })
            .transpose()
    })
    .await
    .map_err(|_| anyhow::anyhow!("backfill control record read timed out"))?
}

async fn read_plan(store: &dyn ObjectStore, prefix: &str) -> Result<Option<BackfillPlan>> {
    let plan =
        read_control::<BackfillPlan>(store, &control_directory(prefix), ControlKey::Backfill)
            .await?;
    if let Some(plan) = &plan {
        plan.validate()?;
    }
    Ok(plan)
}

async fn root_has_authority(store: &dyn ObjectStore, prefix: &str) -> Result<bool> {
    Ok(
        read_control::<serde_json::Value>(store, &control_directory(prefix), ControlKey::State)
            .await?
            .is_some(),
    )
}

/// Before ownership: whether this `build` fills a lane of an open backfill.
///
/// Reads the root's plan and authority without an owner. With a plan and no
/// authority the backfill is not sealed: the request must be exactly one
/// planned lane, which is then recorded in `config` (a lane has no cursor
/// mirror), and any other `build` is refused. Without a plan, or once
/// sealed, the root is an ordinary dataset and nothing changes.
pub async fn enter_backfill_lane(config: &mut Config) -> Result<Option<(u64, u64)>> {
    let Some(output) = config.output.to_str().filter(|o| o.starts_with("s3://")) else {
        return Ok(None);
    };
    if config.dry_run {
        return Ok(None);
    }
    let (bucket, prefix) = remote(output)?;
    let aws = AwsConfig::from(&*config);
    let store = aws.build_read_client(&bucket)?;
    let Some(plan) = read_plan(&store, &prefix).await? else {
        return Ok(None);
    };
    if root_has_authority(&store, &prefix).await? {
        return Ok(None);
    }
    let (Some(start), Some(stop)) = (config.start_block, config.stop_block) else {
        bail!("{UNSEALED}");
    };
    plan.require_lane(start, stop).context(UNSEALED)?;
    config.backfill_lane = Some((start, stop));
    config.cursor_path = None;
    Ok(Some((start, stop)))
}

/// The plan a lane session runs under, or `None` for an ordinary dataset.
///
/// `expected` is the stream this `build` resolved. In a lane it must be the
/// plan's dataset stream with the lane's start, so every lane writes with the
/// semantics the backfill was opened with. `creating`: no authority exists
/// yet for this session's own state.
pub(super) async fn session_plan(
    config: &Config,
    expected: &StreamDescriptor,
    ownership: &DatasetOwnership,
    creating: bool,
) -> Result<Option<BackfillPlan>> {
    let StorageIdentity::S3 { bucket, prefix, .. } = &expected.output else {
        ensure!(
            config.backfill_lane.is_none(),
            "a parallel backfill lane requires S3 output"
        );
        return Ok(None);
    };
    let owner = ownership
        .remote(bucket)
        .context("remote output is not owned")?;
    let root = TransactionStateStore::s3_dataset_root(prefix, owner)?;
    match (config.backfill_lane, owner.lane()) {
        (None, None) => {
            // Only a dataset being created can sit on an unsealed backfill.
            if creating && root.backfill_plan().await?.is_some() {
                bail!("{UNSEALED}");
            }
            Ok(None)
        }
        (Some((start, stop)), Some(lane)) => {
            ensure!(
                lane.start() == start
                    && lane.stop() == stop
                    && lane.dataset_prefix() == prefix
                    && expected.origin_start == start
                    && config.stop_block == Some(stop),
                "the backfill lane's owner, range and request disagree"
            );
            let plan = root
                .backfill_plan()
                .await?
                .context("no parallel backfill is open at this dataset root")?;
            ensure!(
                !root.has_authority().await?,
                "this dataset's backfill is sealed; its lanes are complete"
            );
            plan.require_lane(start, stop)?;
            ensure!(
                plan.lane_descriptor(start) == *expected,
                "this lane's settings differ from the ones the backfill was opened with \
                 (chain, block type, encoding, feature flags, table schemas or storage); run \
                 every lane with the settings of `fireparq backfill open`"
            );
            Ok(Some(plan))
        }
        _ => bail!("the backfill lane's owner and request disagree"),
    }
}

/// What `open` did.
#[derive(Debug, Serialize)]
pub struct BackfillOpened {
    /// `false` when the same backfill was already open.
    pub created: bool,
    pub start: u64,
    pub stop: u64,
    pub lane_blocks: u64,
    pub lanes: u64,
}

/// Opens a parallel backfill at an empty S3 root, under the bucket's owner.
///
/// The destination must be eligible exactly as for a new dataset. Opening the
/// same backfill again changes nothing; a different one is refused.
pub async fn open_backfill(
    config: &Config,
    mapper: MapperSemantics,
    ownership: &DatasetOwnership,
    lane_blocks: u64,
) -> Result<BackfillOpened> {
    ensure!(!config.dry_run, "a backfill cannot be opened in a dry run");
    ensure!(
        config.backfill_lane.is_none(),
        "a backfill is already open at this dataset root"
    );
    let stop = config
        .stop_block
        .context("a backfill needs --stop-block: the exclusive end of the range to fill")?;
    let delta_columns = super::session::delta_columns(&mapper.tables, &mapper.data_schemas)?;
    let expected = super::session::descriptor(config, mapper)?;
    let plan = BackfillPlan::new(expected.clone(), stop, lane_blocks)?;
    let aws = super::session::aws_config(config);
    let _permit = super::session::reserve(&expected.output, ownership)?;
    let states = super::session::states(&expected.output, ownership)?;
    let opened = |created| BackfillOpened {
        created,
        start: plan.start(),
        stop: plan.stop,
        lane_blocks: plan.lane_blocks,
        lanes: plan.lane_count(),
    };
    let sealed = states.has_authority().await?;
    let created = match states.backfill_plan().await? {
        Some(existing) => {
            ensure!(
                existing == plan,
                "another backfill is already open at this dataset root, with a different \
                 range, lane size or settings; it cannot be changed"
            );
            false
        }
        None => {
            ensure!(
                !sealed,
                "this dataset already exists; a parallel backfill only fills a new, empty root"
            );
            let listing = ListingStats::default();
            super::maintenance::validate_ingestion_target(
                &expected.output,
                ownership,
                IngestionTarget::Create,
                &listing,
            )
            .await?;
            let service = mirror_service(&expected.mirror, &aws)?;
            let mirror = ProtectedMirror::new(ownership, &expected.mirror, service.as_ref())?;
            super::eligibility::require_initializable(
                &expected, ownership, &aws, &mirror, &listing,
            )
            .await?;
            states.create_backfill_plan(&plan).await?;
            true
        }
    };
    // The plan first, then the tables, with the dataset's identity. An open
    // interrupted between them is completed by opening again, or by the
    // first lanes, which create a missing table the same way.
    crate::delta::commit::DeltaTables::open(
        super::session::delta_store(&expected.output, ownership)?,
        crate::delta::DeltaIdentity::of(&expected)?,
        &delta_columns,
        !sealed,
        config.flush_concurrency.publications,
    )
    .await?;
    Ok(opened(created))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LaneState {
    /// No lane state yet.
    NotStarted,
    /// A worker owns the lane.
    Running,
    /// Started, not complete, and no worker owns it: rerun the lane.
    Stopped,
    /// Complete up to the lane's stop, with nothing pending.
    Complete,
}

#[derive(Debug, Serialize)]
pub struct LaneReport {
    pub start: u64,
    pub stop: u64,
    pub state: LaneState,
    /// `absent`, `owned` or `released`.
    pub owner: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_block: Option<u64>,
    #[serde(skip)]
    ordinal: u64,
    #[serde(skip)]
    last_event: Option<EventIdentity>,
    #[serde(skip)]
    unanchored: bool,
}

async fn inspect_lane(
    store: &Arc<dyn ObjectStore>,
    plan: &BackfillPlan,
    prefix: &str,
    (start, stop): (u64, u64),
) -> Result<LaneReport> {
    let lane = LaneScope::new(prefix, start, stop)?;
    let directory = lane.directory();
    // The lane's records first, its owner last: a lane seen complete and then
    // released has no worker left that could still write.
    let authority =
        read_control::<AuthorityState>(store.as_ref(), &directory, ControlKey::State).await?;
    let pending =
        read_control::<serde_json::Value>(store.as_ref(), &directory, ControlKey::Pending)
            .await?
            .is_some();
    let owner_record = S3Ownership::lane_status(store, &lane)
        .await
        .with_context(|| format!("reading the owner of backfill lane {}", lane.name()))?;
    let owned = owner_record
        .as_ref()
        .is_some_and(|record| record.state() == OwnerState::Owned);
    let owner = match owner_record.as_ref().map(OwnerRecord::state) {
        Some(OwnerState::Owned) => "owned",
        Some(OwnerState::Released) => "released",
        None => "absent",
    };
    let Some(authority) = authority else {
        ensure!(
            !pending,
            "backfill lane {} has a journal without a checkpoint",
            lane.name()
        );
        return Ok(LaneReport {
            start,
            stop,
            state: if owned {
                LaneState::Running
            } else {
                LaneState::NotStarted
            },
            owner,
            last_block: None,
            ordinal: 0,
            last_event: None,
            unanchored: true,
        });
    };
    authority.validate()?;
    ensure!(
        authority.descriptor == plan.lane_descriptor(start),
        "the state of backfill lane {} belongs to another stream",
        lane.name()
    );
    let checkpoint = authority.checkpoint;
    let last_block = checkpoint.event.as_ref().map(|event| event.block_num);
    let complete =
        checkpoint.completed_stop == Some(stop) && last_block == Some(stop - 1) && !pending;
    Ok(LaneReport {
        start,
        stop,
        state: match (complete, owned) {
            (true, _) => LaneState::Complete,
            (false, true) => LaneState::Running,
            (false, false) => LaneState::Stopped,
        },
        owner,
        last_block,
        ordinal: checkpoint.ordinal,
        unanchored: checkpoint.routing.anchor.is_none(),
        last_event: checkpoint.event,
    })
}

async fn inspect_lanes(
    store: &Arc<dyn ObjectStore>,
    plan: &BackfillPlan,
    prefix: &str,
) -> Result<Vec<LaneReport>> {
    let mut reports: Vec<LaneReport> = futures::stream::iter(
        plan.lanes()
            .map(|lane| inspect_lane(store, plan, prefix, lane)),
    )
    .buffer_unordered(LANE_READ_CONCURRENCY)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<_>>()?;
    reports.sort_by_key(|report| report.start);
    Ok(reports)
}

/// `fireparq backfill status`: read-only.
#[derive(Debug, Serialize)]
pub struct BackfillStatus {
    pub sealed: bool,
    pub start: u64,
    pub stop: u64,
    pub lane_blocks: u64,
    pub lanes: u64,
    pub complete: u64,
    pub running: u64,
    pub stopped: u64,
    pub not_started: u64,
    /// The first lanes that are not complete, in block order.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub incomplete: Vec<LaneReport>,
}

/// One lane's owner record, for `recovery status` and `recovery release`.
pub fn parse_lane(prefix: &str, lane: &str) -> Result<LaneScope> {
    let parsed = lane
        .split_once('-')
        .and_then(|(start, stop)| Some((start.parse::<u64>().ok()?, stop.parse::<u64>().ok()?)));
    let (start, stop) = parsed.context("--lane takes <start>-<stop>, for example 0-250000")?;
    LaneScope::new(prefix, start, stop)
}

pub async fn backfill_status(path: &str, aws: &AwsConfig) -> Result<BackfillStatus> {
    let (bucket, prefix) = remote(path)?;
    let store: Arc<dyn ObjectStore> = Arc::new(aws.build_read_client(&bucket)?);
    let plan = read_plan(store.as_ref(), &prefix)
        .await?
        .context("no parallel backfill was opened at this dataset root")?;
    let sealed = root_has_authority(store.as_ref(), &prefix).await?;
    let reports = inspect_lanes(&store, &plan, &prefix).await?;
    let count = |state| {
        reports
            .iter()
            .filter(|report| report.state == state)
            .count() as u64
    };
    Ok(BackfillStatus {
        sealed,
        start: plan.start(),
        stop: plan.stop,
        lane_blocks: plan.lane_blocks,
        lanes: plan.lane_count(),
        complete: count(LaneState::Complete),
        running: count(LaneState::Running),
        stopped: count(LaneState::Stopped),
        not_started: count(LaneState::NotStarted),
        incomplete: reports
            .into_iter()
            .filter(|report| report.state != LaneState::Complete)
            .take(LISTED_LANES)
            .collect(),
    })
}

/// What `seal` did.
#[derive(Debug, Serialize)]
pub struct BackfillSealed {
    /// `false` when the dataset was already sealed.
    pub sealed_now: bool,
    pub start: u64,
    pub stop: u64,
    pub lanes: u64,
    pub last_block: u64,
}

/// Read-only: the plan, and a refusal unless every lane can be sealed.
/// `seal` runs it before validating `blocks` and again under the owner.
pub async fn seal_preflight(path: &str, aws: &AwsConfig) -> Result<()> {
    let (bucket, prefix) = remote(path)?;
    let store: Arc<dyn ObjectStore> = Arc::new(aws.build_read_client(&bucket)?);
    let plan = read_plan(store.as_ref(), &prefix)
        .await?
        .context("no parallel backfill was opened at this dataset root")?;
    if root_has_authority(store.as_ref(), &prefix).await? {
        return Ok(());
    }
    sealable(&inspect_lanes(&store, &plan, &prefix).await?).map(|_| ())
}

/// The lanes' accepted events in total and the last lane's final event, or a
/// refusal naming the first lanes that are not sealable.
fn sealable(reports: &[LaneReport]) -> Result<(u64, EventIdentity)> {
    let blocking: Vec<String> = reports
        .iter()
        .filter(|report| {
            report.state != LaneState::Complete || report.owner != "released" || !report.unanchored
        })
        .take(LISTED_LANES)
        .map(|report| {
            let why = match (report.state, report.owner) {
                (LaneState::Complete, "released") => "has a pending routing anchor",
                (LaneState::Complete, _) => "is complete but its owner is not released",
                (LaneState::NotStarted, _) => "has not started",
                (LaneState::Running, _) => "is running",
                (LaneState::Stopped, _) => "stopped before its end; rerun it",
            };
            format!("{}-{} {why}", report.start, report.stop)
        })
        .collect();
    ensure!(
        blocking.is_empty(),
        "the backfill cannot be sealed until every lane is complete and released: {}",
        blocking.join("; ")
    );
    let mut ordinal = 0_u64;
    for report in reports {
        ordinal = ordinal
            .checked_add(report.ordinal)
            .context("accepted ordinal exhausted")?;
    }
    let last = reports
        .last()
        .and_then(|report| report.last_event.clone())
        .context("the backfill has no lanes")?;
    Ok((ordinal, last))
}

/// Seals a complete backfill under the bucket's owner: the root gets its
/// first authority and becomes an ordinary dataset.
///
/// Refuses unless every lane is complete with nothing pending and its owner
/// released, and every table exists with the dataset's identity. It changes
/// one record, the root's authority, by conditional create. Block continuity
/// and parent links across lanes are `validate`'s to check, which the
/// `backfill seal` command runs first.
pub async fn seal_backfill(path: &str, aws: &AwsConfig) -> Result<BackfillSealed> {
    let (bucket, prefix) = remote(path)?;
    let ownership = DatasetOwnership::acquire(
        "backfill-seal",
        vec![MutationScope::directory(path.to_owned())],
        Some(aws),
    )
    .await?;
    let sealed = seal_owned(path, &bucket, &prefix, aws, &ownership).await;
    ownership.finish(sealed).await
}

pub(super) async fn seal_owned(
    path: &str,
    bucket: &str,
    prefix: &str,
    aws: &AwsConfig,
    ownership: &DatasetOwnership,
) -> Result<BackfillSealed> {
    let owner = ownership
        .remote(bucket)
        .context("dataset bucket is not owned")?;
    let states = TransactionStateStore::s3(prefix, owner)?;
    let plan = states
        .backfill_plan()
        .await?
        .context("no parallel backfill was opened at this dataset root")?;
    validate_runtime_bindings(&plan.descriptor, path, aws)?;
    ensure!(
        plan.descriptor.output == resolve_output_identity(path, aws)?,
        "the backfill plan belongs to another dataset root"
    );
    let report = |sealed_now, last_block| BackfillSealed {
        sealed_now,
        start: plan.start(),
        stop: plan.stop,
        lanes: plan.lane_count(),
        last_block,
    };
    let snapshot = states.load().await?;
    if let Some(authority) = snapshot.authority {
        ensure!(
            authority.payload.descriptor == plan.descriptor,
            "this dataset root holds another stream than its backfill plan"
        );
        return Ok(report(false, plan.stop - 1));
    }
    super::maintenance::validate_ingestion_target(
        &plan.descriptor.output,
        ownership,
        IngestionTarget::Resume,
        &ListingStats::default(),
    )
    .await?;
    let reports = inspect_lanes(owner.object_store(), &plan, prefix).await?;
    let (ordinal, last_event) = sealable(&reports)?;
    ensure!(
        last_event.block_num == plan.stop - 1,
        "the last lane does not end at the backfill's stop block"
    );
    // Every table exists with the dataset's identity (no schema here: the
    // next `build` checks it, as after `recovery recover`).
    let client = match owner.delta_log() {
        Some(client) => Arc::clone(client),
        None => crate::delta::store::s3_log_client(aws, bucket)?,
    };
    crate::delta::commit::DeltaTables::open_existing(
        crate::delta::store::DeltaStore::s3(bucket, prefix, client)?,
        crate::delta::DeltaIdentity::of(&plan.descriptor)?,
        plan.descriptor.tables.keys().map(String::as_str),
        LANE_READ_CONCURRENCY,
    )
    .await
    .context("checking the dataset's Delta tables before sealing")?;
    let last_block = last_event.block_num;
    let authority =
        AuthorityState::sealed(plan.descriptor.clone(), ordinal, last_event, plan.stop)?;
    states.install_sealed(authority).await?;
    Ok(report(true, last_block))
}

#[cfg(test)]
mod tests;
