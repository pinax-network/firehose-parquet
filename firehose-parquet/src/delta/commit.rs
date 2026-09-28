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

use std::collections::{BTreeMap, HashMap};
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
}

/// A failed [`DeltaTables::commit`].
#[derive(Debug)]
pub struct CommitFailure {
    pub error: anyhow::Error,
    /// Whether a log write may have been sent without a definite outcome
    /// (a transport error, a timeout, a lost response). A conflict, an
    /// exhausted retry budget or a failure before any request is definite.
    pub unresolved: bool,
}

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
        ensure!(
            !columns.is_empty(),
            "a dataset has at least one Delta table"
        );
        let expected = identity.configuration();
        let mut opened = futures::stream::iter(columns.iter().map(|(name, columns)| {
            let (store, identity, expected) = (&store, &identity, &expected);
            async move {
                let table =
                    open_or_create(store, identity, expected, name, columns, create_missing)
                        .await
                        .with_context(|| format!("opening the Delta table `{name}`"))?;
                Ok::<_, anyhow::Error>((name.clone(), table))
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
        let version = i64::try_from(pending.prefix.last_ordinal).map_err(|_| CommitFailure {
            error: anyhow!("the transaction's last ordinal does not fit a Delta txn version"),
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
        self.commit_parts(adds, version, metadata, concurrency, hooks)
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
        mut metadata: HashMap<String, Value>,
        concurrency: usize,
        hooks: &CommitHooks<'_>,
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
    let finalized = CommitBuilder::from(properties)
        .with_actions(vec![Action::Add(add)])
        .build(Some(snapshot), open.table.log_store(), operation)
        .await
        .map_err(|error| CommitFailure {
            unresolved: !is_definite(&error),
            error: anyhow::Error::new(error)
                .context(format!("committing to the Delta log of table `{table}`")),
        })?;
    let version = finalized.version();
    let retries = finalized.metrics.num_retries;
    open.table.state = Some(finalized.snapshot());
    // The commit landed. A failure to make it durable (local disk) is not an
    // unresolved remote write: local roots have no uncertainty latch.
    sync_commit(context.store, &table, version, false)
        .await
        .map_err(definite)?;
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
    };
    (context.hooks.after)(&commit.table, entry_index).map_err(definite)?;
    Ok(commit)
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
        matches!(
            cause.downcast_ref::<DeltaTableError>(),
            Some(DeltaTableError::Transaction {
                source: TransactionError::CommitConflict(
                    CommitConflictError::ConcurrentTransaction
                )
            })
        )
    })
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

async fn open_or_create(
    store: &DeltaStore,
    identity: &DeltaIdentity,
    expected: &HashMap<String, String>,
    name: &str,
    columns: &[StructField],
    create_missing: bool,
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
            ensure!(
                create_missing,
                "the stream has committed transactions, but table `{name}` has no Delta log: \
                 its rows are unreachable. A dataset written before Delta commits (or whose \
                 `_delta_log/` was removed) cannot be resumed; build into a new, empty output root"
            );
            match create_table(log_store.clone(), name, columns.to_vec(), identity).await {
                Ok(table) => {
                    sync_commit(store, name, 0, true).await?;
                    table
                }
                // Another writer may have created it first: validate theirs.
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

/// The table must be exactly what [`create_table`] makes for this stream.
fn validate(
    table: &DeltaTable,
    name: &str,
    columns: &[StructField],
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
