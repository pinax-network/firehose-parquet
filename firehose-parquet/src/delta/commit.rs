//! One Delta table per fireparq table, and one Delta commit per table for each
//! committed ingestion transaction (#643 L3, `docs/design/delta-lake.md` §3).
//!
//! [`DeltaTables::open`] runs after the stream's authority exists. It opens
//! every table of the stream descriptor and checks that its protocol,
//! partition column, properties (including the `fireparq.*` identity) and
//! schema are exactly the ones fireparq creates. A missing table is created
//! (commit 0: `protocol` and `metaData` only) while the stream has accepted
//! nothing yet; afterwards a missing table is refused, since its committed
//! rows would be unreachable.
//!
//! [`DeltaTables::commit`] runs after the transaction's journal is
//! Committed and before authority advances. Each table with a part in the
//! transaction gets one commit: the part's `add` (path, partition value, size,
//! modification time and statistics, all from the journal, never from the
//! file) and `txn {appId: fireparq:<descriptor>, version: last ordinal}`. The
//! other tables commit first, concurrently; `blocks` commits last, so a block
//! visible in `blocks` has its rows visible in every table. The commits are
//! blind appends: a concurrent OPTIMIZE or another application's append makes
//! delta-rs retry at the next version, while a winning commit with the same
//! `appId` fails with `ConcurrentTransaction` instead of adding a second copy.
//! Writers never checkpoint or clean up the log; the maintenance job does.
//!
//! Recovery (#643 L4, design §3.5 and §4) reads each table's `txn` for this
//! stream at every start: [`DeltaTables::check_progress`] refuses a log
//! ahead of authority and names the tables of a Committed transaction whose
//! logs do not hold it yet, and [`DeltaTables::roll_forward`] commits
//! exactly those, `blocks` last, resolving a same-`appId` conflict (a delayed
//! copy of an earlier commit whose outcome was unknown) from the log instead
//! of failing.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, ensure, Context, Result};
use deltalake_core::kernel::transaction::{
    CommitBuilder, CommitConflictError, CommitProperties, TransactionError,
};
use deltalake_core::kernel::{Action, Add, StructField, StructType, Transaction};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::{DeltaTable, DeltaTableError};
use futures::stream::{FuturesUnordered, StreamExt};
use object_store_delta::ObjectStoreExt;
use serde_json::{json, Value};

use super::store::DeltaStore;
use super::{create_table, open_table, DeltaIdentity, PARTITION_COLUMN};
use crate::ingest::state::{PendingTransaction, TransactionPhase};

/// The table that commits last in every transaction.
pub const LAST_TABLE: &str = "blocks";

/// Commit attempts per table before giving up: each lost conditional put
/// (another writer took the version) is one attempt.
const MAX_COMMIT_ATTEMPTS: usize = 25;

/// How often, at most, a table's `_delta_log/_last_checkpoint` is read again
/// after a commit, for the log-tail metric.
const CHECKPOINT_HINT_REFRESH: Duration = Duration::from_secs(60);

/// One committed table of a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableCommit {
    pub table: String,
    /// The table version this commit created.
    pub version: u64,
    /// Lost conditional puts retried at a later version.
    pub retries: u64,
    pub elapsed: Duration,
    /// Commits a reader replays after the last checkpoint (design §8).
    pub tail_commits: u64,
    /// The commit lost to a winning commit of this stream that already holds
    /// the transaction (a delayed copy of an earlier commit whose outcome was
    /// unknown), so `version` is the table's version after it, not a new one.
    /// Only a roll-forward resolves a conflict this way.
    pub found_in_log: bool,
}

/// A failed [`DeltaTables::commit`].
#[derive(Debug)]
pub struct CommitFailure {
    pub error: anyhow::Error,
    /// Whether a log write may have been sent without a definite outcome
    /// (a transport error, a timeout, a lost response). A conflict, an
    /// exhausted retry budget or a failure before any request is definite.
    /// Either way the log resolves it at the next start: the commit landed
    /// exactly when the table's `txn` is the transaction's (design §3.5).
    pub unresolved: bool,
}

/// Called once for each table [`DeltaTables::open_with`] created, after its
/// version 0 is durable. An error stops the open.
pub type CreatedHook<'h> = &'h (dyn Fn(&str) -> Result<()> + Sync);

/// An error a [`CommitHooks::after`] hook returns for a commit whose outcome
/// the process must treat as unknown (a lost response), which makes the
/// failure [`CommitFailure::unresolved`]. Real-binary fault tests use it.
#[derive(Debug)]
pub struct UnknownOutcome;

impl fmt::Display for UnknownOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the commit's response was lost")
    }
}

impl std::error::Error for UnknownOutcome {}

impl fmt::Display for CommitFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#}", self.error)
    }
}

impl std::error::Error for CommitFailure {}

/// Called around each table's commit: `before` (with the table name) ahead of
/// any request, `after` (with the name and the part's entry index) once the
/// commit is durable. An error from either stops the transaction's commits.
pub struct CommitHooks<'h> {
    pub before: &'h (dyn Fn(&str) -> Result<()> + Sync),
    pub after: &'h (dyn Fn(&str, u32) -> Result<()> + Sync),
}

impl CommitHooks<'static> {
    pub const NONE: Self = Self {
        before: &|_| Ok(()),
        after: &|_, _| Ok(()),
    };
}

struct OpenTable {
    table: DeltaTable,
    /// Version of the last checkpoint `_last_checkpoint` names, if any.
    checkpoint: Option<u64>,
    checkpoint_read: Instant,
}

impl OpenTable {
    fn version(&self) -> Result<u64> {
        self.table
            .version()
            .context("an opened Delta table has no version")
    }

    fn tail_commits(&self) -> Result<u64> {
        let version = self.version()?;
        Ok(match self.checkpoint {
            Some(checkpoint) if checkpoint <= version => version - checkpoint,
            _ => version + 1,
        })
    }
}

/// One part to add to its table's log: the part's `add`, built from what the
/// journal recorded (never from the file).
#[derive(Clone, Debug)]
pub struct PartAdd {
    pub table: String,
    /// The part's entry index in the transaction (the table's position in the
    /// sorted inventory), passed to [`CommitHooks::after`].
    pub entry_index: u32,
    pub add: Add,
}

impl PartAdd {
    /// The `add` of the part at `path` (relative to its table,
    /// `date=YYYY-MM-DD/part-...parquet`) in partition `date`.
    pub fn new(
        table: &str,
        entry_index: u32,
        path: &str,
        date: &str,
        size: u64,
        stats: &str,
        modification_time: i64,
    ) -> Result<Self> {
        ensure!(
            path.starts_with(&format!("{PARTITION_COLUMN}={date}/")),
            "a part of partition {date} lies outside its directory"
        );
        Ok(Self {
            table: table.to_string(),
            entry_index,
            add: Add {
                path: path.to_string(),
                partition_values: HashMap::from([(
                    PARTITION_COLUMN.to_string(),
                    Some(date.to_string()),
                )]),
                size: i64::try_from(size).context("a part size does not fit a Delta add")?,
                modification_time,
                data_change: true,
                stats: Some(stats.to_string()),
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            },
        })
    }
}

/// The Delta tables of one dataset, open for the lifetime of a `build`.
pub struct DeltaTables {
    store: DeltaStore,
    identity: DeltaIdentity,
    tables: BTreeMap<String, OpenTable>,
}

impl fmt::Debug for DeltaTables {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeltaTables")
            .field("store", &self.store)
            .field("tables", &self.tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl DeltaTables {
    /// Opens (and, with `create_missing`, creates) the Delta table of each
    /// entry of `columns`, at most `concurrency` at a time, and validates it
    /// against what fireparq would create for `identity`.
    pub async fn open(
        store: DeltaStore,
        identity: DeltaIdentity,
        columns: &BTreeMap<String, Vec<StructField>>,
        create_missing: bool,
        concurrency: usize,
    ) -> Result<Self> {
        Self::open_with(
            store,
            identity,
            columns,
            create_missing,
            concurrency,
            &|_| Ok(()),
        )
        .await
    }

    /// [`Self::open`], calling `created` after each table it creates.
    /// Creation is idempotent: a start interrupted between creations finds
    /// some tables, validates them and creates the others (design §4).
    pub async fn open_with(
        store: DeltaStore,
        identity: DeltaIdentity,
        columns: &BTreeMap<String, Vec<StructField>>,
        create_missing: bool,
        concurrency: usize,
        created: CreatedHook<'_>,
    ) -> Result<Self> {
        let specs = columns
            .iter()
            .map(|(name, columns)| (name.clone(), Some(columns.as_slice())))
            .collect();
        Self::open_specs(store, identity, specs, create_missing, concurrency, created).await
    }

    /// Opens the existing Delta tables `names` of the stream `identity`,
    /// never creating one: `recovery recover`, which has no mapper and so no
    /// table schemas. Everything [`Self::open`] validates is checked except
    /// the schema, which the next `build` checks.
    pub async fn open_existing<'n>(
        store: DeltaStore,
        identity: DeltaIdentity,
        names: impl IntoIterator<Item = &'n str>,
        concurrency: usize,
    ) -> Result<Self> {
        let specs = names
            .into_iter()
            .map(|name| (name.to_string(), None))
            .collect();
        Self::open_specs(store, identity, specs, false, concurrency, &|_| Ok(())).await
    }

    async fn open_specs(
        store: DeltaStore,
        identity: DeltaIdentity,
        specs: Vec<(String, Option<&[StructField]>)>,
        create_missing: bool,
        concurrency: usize,
        created: CreatedHook<'_>,
    ) -> Result<Self> {
        ensure!(!specs.is_empty(), "a dataset has at least one Delta table");
        let expected = identity.configuration();
        let mut opened = futures::stream::iter(specs.into_iter().map(|(name, columns)| {
            let (store, identity, expected) = (&store, &identity, &expected);
            async move {
                let table = open_or_create(
                    store,
                    identity,
                    expected,
                    &name,
                    columns,
                    create_missing,
                    created,
                )
                .await
                .with_context(|| format!("opening the Delta table `{name}`"))?;
                Ok::<_, anyhow::Error>((name, table))
            }
        }))
        .buffer_unordered(concurrency.max(1));
        let mut tables = BTreeMap::new();
        while let Some(result) = opened.next().await {
            let (name, table) = result?;
            tables.insert(name, table);
        }
        drop(opened);
        Ok(Self {
            store,
            identity,
            tables,
        })
    }

    /// The `txn` application id of this stream.
    pub fn app_id(&self) -> String {
        self.identity.app_id()
    }

    /// The names of the tables, sorted.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }

    /// The current version of `table`.
    pub fn version(&self, table: &str) -> Result<u64> {
        self.open_table(table)?.version()
    }

    /// Commits a reader of `table` replays after its last checkpoint.
    pub fn tail_commits(&self, table: &str) -> Result<u64> {
        self.open_table(table)?.tail_commits()
    }

    /// The last `txn` version this stream committed to `table`, read from the
    /// table's log.
    pub async fn txn_version(&self, table: &str) -> Result<Option<i64>> {
        let open = self.open_table(table)?;
        Ok(open
            .table
            .snapshot()?
            .transaction_version(open.table.log_store().as_ref(), self.app_id())
            .await?)
    }

    fn open_table(&self, table: &str) -> Result<&OpenTable> {
        self.tables
            .get(table)
            .with_context(|| format!("`{table}` is not a Delta table of this dataset"))
    }

    /// The `txn` version of this stream in each of `tables`, at most
    /// `concurrency` reads at a time. Each read stops at the newest commit
    /// holding this stream's `txn`, usually the last one.
    async fn txn_versions<'t>(
        &self,
        tables: impl IntoIterator<Item = &'t str>,
        concurrency: usize,
    ) -> Result<Vec<(&'t str, Option<i64>)>> {
        futures::stream::iter(tables.into_iter().map(|table| async move {
            let version = self
                .txn_version(table)
                .await
                .with_context(|| format!("reading the `txn` of Delta table `{table}`"))?;
            Ok::<_, anyhow::Error>((table, version))
        }))
        .buffered(concurrency.max(1))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
    }

    /// Checks every table's `txn` for this stream against authority, whose
    /// accepted ordinal is `authority`, before startup changes anything, and
    /// returns the tables of `committed` whose logs do not hold it yet, in
    /// the order of its parts.
    ///
    /// `committed` is a Committed transaction whose authority has not
    /// advanced (its predecessor is at `authority`). Its last ordinal `L` in a
    /// table with one of its parts means that commit landed, and the part is
    /// never read again: OPTIMIZE and VACUUM may have rewritten it (design
    /// §4). Otherwise every `txn` must be at most `authority` (or absent);
    /// anything else is a log ahead of authority, refused: only another
    /// writer using this stream's `appId`, or an authority restored from an
    /// older copy, leads there. On local disk, a table whose log already holds
    /// `committed` has its log tail synced again, since the process that
    /// committed it may have died before that commit was durable.
    pub async fn check_progress(
        &self,
        authority: u64,
        committed: Option<&PendingTransaction>,
        concurrency: usize,
    ) -> Result<Vec<String>> {
        let last = committed
            .map(|pending| txn_version_of(pending.prefix.last_ordinal))
            .transpose()?;
        let with_parts: BTreeSet<&str> = committed
            .map(|pending| {
                pending
                    .parts
                    .iter()
                    .map(|part| part.table.as_str())
                    .collect()
            })
            .unwrap_or_default();
        let mut holding = BTreeSet::new();
        for (table, txn) in self.txn_versions(self.names(), concurrency).await? {
            match txn {
                Some(version) if Some(version) == last && with_parts.contains(table) => {
                    let open = self.open_table(table)?;
                    sync_log_tail(&self.store, table, open.checkpoint).await?;
                    holding.insert(table);
                }
                Some(version) if u64::try_from(version).is_ok_and(|v| v <= authority) => {}
                None => {}
                Some(version) => {
                    return Err(log_ahead(
                        table,
                        version,
                        authority,
                        committed.map(|pending| pending.prefix.last_ordinal),
                        &self.app_id(),
                    ))
                }
            }
        }
        Ok(committed
            .into_iter()
            .flat_map(|pending| &pending.parts)
            .map(|part| part.table.as_str())
            .filter(|table| !holding.contains(table))
            .map(str::to_string)
            .collect())
    }

    /// Rolls the Committed transaction `pending` forward into `tables` (from
    /// [`Self::check_progress`]): the commits [`Self::commit`] makes, restricted
    /// to those tables, [`LAST_TABLE`] last. A commit that loses to a winning
    /// commit of this stream (`ConcurrentTransaction`: a delayed copy of an
    /// earlier commit whose outcome was unknown) is resolved from the log:
    /// the table is reloaded, and when its `txn` is the transaction's the
    /// commit counts as done ([`TableCommit::found_in_log`]), so the parts
    /// are added exactly once (design §3.5).
    pub async fn roll_forward(
        &mut self,
        pending: &PendingTransaction,
        tables: &[String],
        concurrency: usize,
        hooks: &CommitHooks<'_>,
    ) -> std::result::Result<Vec<TableCommit>, CommitFailure> {
        let wanted: BTreeSet<&str> = tables.iter().map(String::as_str).collect();
        let adds = self
            .adds(pending)
            .map_err(|error| CommitFailure {
                error,
                unresolved: false,
            })?
            .into_iter()
            .filter(|add| wanted.contains(add.table.as_str()))
            .collect();
        self.commit_transaction(pending, adds, concurrency, hooks, true)
            .await
    }

    /// Commits every part of the Committed transaction `pending`: one commit
    /// per table that has a part, the others first (at most `concurrency` at
    /// a time) and [`LAST_TABLE`] last. Returns the commits in the order they
    /// completed. On the first failure no further commit starts; the started
    /// ones finish before the failure is returned.
    pub async fn commit(
        &mut self,
        pending: &PendingTransaction,
        concurrency: usize,
        hooks: &CommitHooks<'_>,
    ) -> std::result::Result<Vec<TableCommit>, CommitFailure> {
        let adds = self.adds(pending).map_err(|error| CommitFailure {
            error,
            unresolved: false,
        })?;
        self.commit_transaction(pending, adds, concurrency, hooks, false)
            .await
    }

    async fn commit_transaction(
        &mut self,
        pending: &PendingTransaction,
        adds: Vec<PartAdd>,
        concurrency: usize,
        hooks: &CommitHooks<'_>,
        resolve_from_log: bool,
    ) -> std::result::Result<Vec<TableCommit>, CommitFailure> {
        let version =
            txn_version_of(pending.prefix.last_ordinal).map_err(|error| CommitFailure {
                error,
                unresolved: false,
            })?;
        let metadata = HashMap::from([
            (
                "fireparq.transaction".to_string(),
                json!(pending.id.as_str()),
            ),
            (
                "fireparq.firstOrdinal".to_string(),
                json!(pending.prefix.first_ordinal),
            ),
            (
                "fireparq.lastOrdinal".to_string(),
                json!(pending.prefix.last_ordinal),
            ),
        ]);
        self.commit_parts_with(
            adds,
            version,
            metadata,
            concurrency,
            hooks,
            resolve_from_log,
        )
        .await
    }

    /// Commits `parts` with `txn {appId, version: txn_version}`: one commit
    /// per part's table, the others first (at most `concurrency` at a time)
    /// and [`LAST_TABLE`] last, each a blind append whose `commitInfo` also
    /// holds `metadata`. [`Self::commit`] is this for a Committed transaction.
    pub async fn commit_parts(
        &mut self,
        parts: Vec<PartAdd>,
        txn_version: i64,
        metadata: HashMap<String, Value>,
        concurrency: usize,
        hooks: &CommitHooks<'_>,
    ) -> std::result::Result<Vec<TableCommit>, CommitFailure> {
        self.commit_parts_with(parts, txn_version, metadata, concurrency, hooks, false)
            .await
    }

    async fn commit_parts_with(
        &mut self,
        parts: Vec<PartAdd>,
        txn_version: i64,
        mut metadata: HashMap<String, Value>,
        concurrency: usize,
        hooks: &CommitHooks<'_>,
        resolve_from_log: bool,
    ) -> std::result::Result<Vec<TableCommit>, CommitFailure> {
        let mut seen = std::collections::BTreeSet::new();
        for part in &parts {
            if !seen.insert(part.table.as_str()) {
                return Err(CommitFailure {
                    error: anyhow!(
                        "a transaction adds one part per table, not two to `{}`",
                        part.table
                    ),
                    unresolved: false,
                });
            }
        }
        metadata.insert("isBlindAppend".to_string(), Value::Bool(true));
        let app_id = self.app_id();
        let (last, others): (Vec<_>, Vec<_>) =
            parts.into_iter().partition(|add| add.table == LAST_TABLE);
        let context = CommitContext {
            store: &self.store,
            app_id: &app_id,
            version: txn_version,
            metadata: &metadata,
            hooks,
            resolve_from_log,
        };
        let mut committed = Vec::new();
        for (group, limit) in [(others, concurrency.max(1)), (last, 1)] {
            let mut tables: BTreeMap<&str, &mut OpenTable> = self
                .tables
                .iter_mut()
                .map(|(name, table)| (name.as_str(), table))
                .collect();
            let mut queue = Vec::new();
            for add in group {
                let table = tables
                    .remove(add.table.as_str())
                    .ok_or_else(|| CommitFailure {
                        error: anyhow!("`{}` is not a Delta table of this dataset", add.table),
                        unresolved: false,
                    })?;
                queue.push((table, add));
            }
            committed.extend(run_bounded(&context, queue, limit).await?);
        }
        Ok(committed)
    }

    /// The `add` of each part of `pending`, from its journal entry.
    fn adds(&self, pending: &PendingTransaction) -> Result<Vec<PartAdd>> {
        ensure!(
            pending.phase == TransactionPhase::Committed,
            "only a Committed transaction can be added to the Delta logs"
        );
        pending
            .parts
            .iter()
            .map(|part| {
                let receipt = part
                    .receipt
                    .as_ref()
                    .context("a committed part has no receipt")?;
                let plan = pending
                    .tables
                    .iter()
                    .find(|plan| plan.table == part.table)
                    .context("a committed part has no table plan")?;
                let date = plan
                    .partition
                    .strip_prefix(&format!("{PARTITION_COLUMN}="))
                    .context("a committed part has no date partition")?;
                let path = part
                    .final_relative_path
                    .strip_prefix(&format!("{}/", part.table))
                    .context("a committed part lies outside its table")?;
                ensure!(
                    self.tables.contains_key(&part.table),
                    "`{}` is not a Delta table of this dataset",
                    part.table
                );
                super::stats::check_stats(&receipt.stats, part.row_count)?;
                PartAdd::new(
                    &part.table,
                    part.entry_index,
                    path,
                    date,
                    receipt.byte_size,
                    &receipt.stats,
                    receipt.modification_time,
                )
            })
            .collect()
    }
}

struct CommitContext<'c> {
    store: &'c DeltaStore,
    app_id: &'c str,
    version: i64,
    metadata: &'c HashMap<String, Value>,
    hooks: &'c CommitHooks<'c>,
    /// Resolve a same-`appId` conflict from the log (roll-forward only).
    resolve_from_log: bool,
}

/// The `txn` version of a transaction whose last accepted ordinal is
/// `ordinal`: a checked `u64` to `i64` conversion (design §3.2).
fn txn_version_of(ordinal: u64) -> Result<i64> {
    i64::try_from(ordinal)
        .map_err(|_| anyhow!("the transaction's last ordinal does not fit a Delta txn version"))
}

fn log_ahead(
    table: &str,
    txn: i64,
    authority: u64,
    pending: Option<u64>,
    app_id: &str,
) -> anyhow::Error {
    let pending = pending.map_or(String::new(), |last| {
        format!(" (the pending transaction ends at ordinal {last})")
    });
    anyhow!(
        "the Delta log of table `{table}` is ahead of this dataset's authority: it holds \
         transaction {txn} of this stream (`txn` appId {app_id}), but authority has accepted \
         only up to ordinal {authority}{pending}. Only another writer using this stream's appId, \
         or an authority restored from an older copy of `.fireparq-ingest/`, leads there. \
         Nothing was changed; keep the dataset as evidence and build into a new, empty output \
         root"
    )
}

/// On local disk, syncs `table`'s commit files after the checkpoint
/// `checkpoint` and its `_delta_log/` directory. Remote commits are durable
/// once they are visible.
async fn sync_log_tail(store: &DeltaStore, table: &str, checkpoint: Option<u64>) -> Result<()> {
    if !matches!(store, DeltaStore::Local { .. }) {
        return Ok(());
    }
    let (store, table) = (store.clone(), table.to_string());
    tokio::task::spawn_blocking(move || store.sync_log_tail(&table, checkpoint))
        .await
        .context("the Delta log sync task failed")?
}

/// Runs the commits of `queue`, at most `limit` at a time. After the first
/// failure nothing new starts and the started commits are drained; the
/// failure is unresolved if any of them may have sent a write without a
/// definite outcome.
async fn run_bounded(
    context: &CommitContext<'_>,
    queue: Vec<(&mut OpenTable, PartAdd)>,
    limit: usize,
) -> std::result::Result<Vec<TableCommit>, CommitFailure> {
    let mut queue = queue.into_iter();
    let mut running = FuturesUnordered::new();
    let mut committed = Vec::new();
    let mut failure: Option<CommitFailure> = None;
    loop {
        while failure.is_none() && running.len() < limit {
            let Some((table, add)) = queue.next() else {
                break;
            };
            running.push(commit_table(context, table, add));
        }
        let Some(result) = running.next().await else {
            break;
        };
        match result {
            Ok(commit) => committed.push(commit),
            Err(error) => match &mut failure {
                None => failure = Some(error),
                Some(first) => {
                    first.unresolved |= error.unresolved;
                    tracing::debug!(error = %error, "additional Delta commit failure while draining");
                }
            },
        }
    }
    match failure {
        Some(failure) => Err(failure),
        None => Ok(committed),
    }
}

async fn commit_table(
    context: &CommitContext<'_>,
    open: &mut OpenTable,
    add: PartAdd,
) -> std::result::Result<TableCommit, CommitFailure> {
    let PartAdd {
        table,
        entry_index,
        add,
    } = add;
    let definite = |error: anyhow::Error| CommitFailure {
        error,
        unresolved: false,
    };
    (context.hooks.before)(&table).map_err(definite)?;
    let started = Instant::now();
    let properties = CommitProperties::default()
        .with_application_transaction(Transaction::new(context.app_id, context.version))
        .with_create_checkpoint(false)
        .with_cleanup_expired_logs(Some(false))
        .with_max_retries(MAX_COMMIT_ATTEMPTS)
        .with_metadata(context.metadata.clone());
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: Some(vec![PARTITION_COLUMN.to_string()]),
        predicate: None,
    };
    let snapshot = open.table.snapshot().map_err(|error| {
        definite(anyhow::Error::new(error).context(format!("Delta table `{table}` is not loaded")))
    })?;
    let committed = CommitBuilder::from(properties)
        .with_actions(vec![Action::Add(add)])
        .build(Some(snapshot), open.table.log_store(), operation)
        .await;
    let (version, retries, found_in_log) = match committed {
        Ok(finalized) => {
            let version = finalized.version();
            let retries = finalized.metrics.num_retries;
            open.table.state = Some(finalized.snapshot());
            // The commit landed. A failure to make it durable (local disk) is
            // not an unresolved remote write.
            sync_commit(context.store, &table, version, false)
                .await
                .map_err(definite)?;
            (version, retries, false)
        }
        Err(error) if context.resolve_from_log && is_same_app(&error) => {
            // A winning commit of this stream: an earlier commit of this
            // transaction whose outcome was unknown landed after all. Its
            // `txn` decides, never a second copy of the part.
            let version = resolve_from_log(context, open, &table)
                .await
                .map_err(|resolution| {
                    definite(
                        anyhow::Error::new(error)
                            .context(format!("committing to the Delta log of table `{table}`"))
                            .context(format!("{resolution:#}")),
                    )
                })?;
            tracing::info!(
                table = %table,
                version,
                txn = context.version,
                "a winning commit of this stream already holds the transaction; its outcome was resolved from the Delta log"
            );
            (version, 0, true)
        }
        Err(error) => {
            return Err(CommitFailure {
                unresolved: !is_definite(&error),
                error: anyhow::Error::new(error)
                    .context(format!("committing to the Delta log of table `{table}`")),
            })
        }
    };
    if open.checkpoint_read.elapsed() >= CHECKPOINT_HINT_REFRESH {
        open.checkpoint = read_checkpoint_hint(&open.table, &table).await;
        open.checkpoint_read = Instant::now();
    }
    let commit = TableCommit {
        tail_commits: open.tail_commits().map_err(definite)?,
        table,
        version,
        retries,
        elapsed: started.elapsed(),
        found_in_log,
    };
    (context.hooks.after)(&commit.table, entry_index).map_err(|error| CommitFailure {
        unresolved: error.chain().any(|cause| cause.is::<UnknownOutcome>()),
        error,
    })?;
    Ok(commit)
}

/// After a same-`appId` conflict: reloads `open` and returns its version when
/// its `txn` for this stream is the transaction's, which then holds the part
/// once. On local disk the log tail is synced first, like a skipped table's.
async fn resolve_from_log(
    context: &CommitContext<'_>,
    open: &mut OpenTable,
    table: &str,
) -> Result<u64> {
    open.table
        .update_state()
        .await
        .with_context(|| format!("reloading Delta table `{table}` after a conflict"))?;
    let txn = open
        .table
        .snapshot()?
        .transaction_version(open.table.log_store().as_ref(), context.app_id)
        .await?;
    ensure!(
        txn == Some(context.version),
        "a winning commit of this stream left table `{table}` at `txn` {txn:?}, not the \
         transaction's {}",
        context.version
    );
    open.checkpoint = read_checkpoint_hint(&open.table, table).await;
    open.checkpoint_read = Instant::now();
    sync_log_tail(context.store, table, open.checkpoint).await?;
    open.version()
}

/// Whether a failed commit certainly wrote nothing: a conflict with a winning
/// commit (including one with this stream's `appId`) or an exhausted budget
/// of lost conditional puts. Anything else (a transport error, a timeout)
/// may have written.
fn is_definite(error: &DeltaTableError) -> bool {
    matches!(
        error,
        DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(_) | TransactionError::MaxCommitAttempts(_)
        }
    )
}

/// Whether `error` is a commit that lost to a winning commit of the same
/// stream (`ConcurrentTransaction`): the parts are not added twice.
pub fn is_same_app_conflict(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<DeltaTableError>()
            .is_some_and(is_same_app)
    })
}

fn is_same_app(error: &DeltaTableError) -> bool {
    matches!(
        error,
        DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(CommitConflictError::ConcurrentTransaction)
        }
    )
}

async fn sync_commit(store: &DeltaStore, table: &str, version: u64, created: bool) -> Result<()> {
    if !matches!(store, DeltaStore::Local { .. }) {
        return Ok(());
    }
    let (store, table) = (store.clone(), table.to_string());
    tokio::task::spawn_blocking(move || store.sync_commit(&table, version, created))
        .await
        .context("the Delta commit sync task failed")?
}

/// Opens table `name`, creating it when it is missing and `create_missing`
/// (which needs its `columns`). Without `columns` the table must exist, and
/// its schema is not validated.
async fn open_or_create(
    store: &DeltaStore,
    identity: &DeltaIdentity,
    expected: &HashMap<String, String>,
    name: &str,
    columns: Option<&[StructField]>,
    create_missing: bool,
    created: CreatedHook<'_>,
) -> Result<OpenTable> {
    let log_store = store.log_store(name)?;
    let opened = if store.lacks_local_log(name)? {
        Err(DeltaTableError::NotATable(format!(
            "{name} has no _delta_log"
        )))
    } else {
        open_table(log_store.clone()).await
    };
    let table = match opened {
        Ok(table) => table,
        Err(DeltaTableError::NotATable(_)) => {
            let columns = match columns {
                Some(columns) if create_missing => columns,
                Some(_) => bail!(
                    "the stream has committed transactions, but table `{name}` has no Delta log: \
                     its rows are unreachable. A dataset written before Delta commits (or whose \
                     `_delta_log/` was removed) cannot be resumed; build into a new, empty output \
                     root"
                ),
                None => bail!(
                    "table `{name}` has no Delta log. A dataset interrupted while its tables were \
                     being created is completed by the next `build`, which creates them; \
                     otherwise the log was removed and the dataset cannot be recovered"
                ),
            };
            match create_table(log_store.clone(), name, columns.to_vec(), identity).await {
                Ok(table) => {
                    sync_commit(store, name, 0, true).await?;
                    created(name)?;
                    table
                }
                // Another writer, or an earlier start whose request had no
                // answer, created it first: validate theirs.
                Err(error) => open_table(log_store)
                    .await
                    .map_err(|_| anyhow::Error::new(error).context("creating the Delta table"))?,
            }
        }
        Err(error) => return Err(error.into()),
    };
    validate(&table, name, columns, expected)?;
    let checkpoint = read_checkpoint_hint(&table, name).await;
    Ok(OpenTable {
        table,
        checkpoint,
        checkpoint_read: Instant::now(),
    })
}

/// The table must be exactly what [`create_table`] makes for this stream
/// (its schema only when `columns` are given).
fn validate(
    table: &DeltaTable,
    name: &str,
    columns: Option<&[StructField]>,
    expected: &HashMap<String, String>,
) -> Result<()> {
    let snapshot = table.snapshot()?;
    let metadata = snapshot.metadata();
    let configuration = metadata.configuration();
    let descriptor = super::DESCRIPTOR_PROPERTY;
    if configuration.get(descriptor) != expected.get(descriptor) {
        bail!(
            "Delta table `{name}` belongs to another stream ({descriptor} = {}); this stream is \
             {}. Build into a new, empty output root",
            configuration
                .get(descriptor)
                .map_or("absent", String::as_str),
            expected.get(descriptor).map_or("", String::as_str),
        );
    }
    if configuration != expected {
        let mut differing: Vec<&str> = expected
            .keys()
            .chain(configuration.keys())
            .filter(|key| configuration.get(*key) != expected.get(*key))
            .map(String::as_str)
            .collect();
        differing.sort_unstable();
        differing.dedup();
        bail!(
            "Delta table `{name}` has table properties fireparq does not set: {} differ",
            differing.join(", ")
        );
    }
    let protocol = snapshot.protocol();
    ensure!(
        protocol.min_reader_version() == super::MIN_READER_VERSION
            && protocol.min_writer_version() == super::MIN_WRITER_VERSION
            && protocol.reader_features().is_none()
            && protocol.writer_features().is_none(),
        "Delta table `{name}` has protocol reader {} / writer {} with table features; fireparq \
         writes reader {} / writer {} without features",
        protocol.min_reader_version(),
        protocol.min_writer_version(),
        super::MIN_READER_VERSION,
        super::MIN_WRITER_VERSION,
    );
    ensure!(
        metadata.partition_columns() == &[PARTITION_COLUMN.to_string()],
        "Delta table `{name}` is not partitioned by `{PARTITION_COLUMN}` alone"
    );
    let Some(columns) = columns else {
        return Ok(());
    };
    let expected_schema = StructType::try_new(columns.to_vec())?;
    ensure!(
        *snapshot.schema() == expected_schema,
        "Delta table `{name}` has a different schema from this stream's `{name}` table; build \
         into a new, empty output root"
    );
    Ok(())
}

/// The checkpoint version `_delta_log/_last_checkpoint` names. Missing is
/// `None`; a failed read or an unreadable hint is logged and treated as
/// missing, since it only feeds the log-tail metric.
async fn read_checkpoint_hint(table: &DeltaTable, name: &str) -> Option<u64> {
    let path = object_store_delta::path::Path::from("_delta_log/_last_checkpoint");
    let store = table.log_store().object_store(None);
    let bytes = match store.get(&path).await {
        Ok(result) => match result.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(table = name, error = %error, "could not read the Delta checkpoint hint");
                return None;
            }
        },
        Err(object_store_delta::Error::NotFound { .. }) => return None,
        Err(error) => {
            tracing::warn!(table = name, error = %error, "could not read the Delta checkpoint hint");
            return None;
        }
    };
    let version = serde_json::from_slice::<Value>(&bytes)
        .ok()
        .and_then(|hint| hint.get("version").and_then(Value::as_u64));
    if version.is_none() {
        tracing::warn!(
            table = name,
            "the Delta checkpoint hint has no version; counting the whole log"
        );
    }
    version
}

#[cfg(test)]
pub(crate) mod tests;
