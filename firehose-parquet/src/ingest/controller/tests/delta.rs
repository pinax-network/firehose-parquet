//! #643 L3: the Delta commit step between Committed and the authority
//! advance (`docs/design/delta-lake.md` §3.1). One commit per table with a
//! part, `txn = last ordinal`, the other tables first and `blocks` last; a
//! failure after a table's commit leaves authority behind.
//!
//! #643 L4: recovery of every row of design §4. A Committed journal is rolled
//! forward into exactly the tables whose `txn` lacks it, `blocks` last, and
//! only their parts are read; once every log or authority holds it, no part
//! is read at all (maintenance may have compacted and vacuumed them); a
//! missing uncommitted part fails closed with the journal kept; a log ahead
//! of authority is refused before anything changes.
use super::*;
use crate::delta::commit::{DeltaTables, PartAdd, LAST_TABLE};
use crate::delta::store::DeltaStore;
use crate::delta::{delta_columns, DeltaIdentity};
use arrow::array::{Int64Array, TimestampMicrosecondArray};

/// The fixture rows in Delta data file types, as `IngestionSession::flush`
/// hands them to the controller.
fn delta_data() -> HashMap<String, RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));
    [("blocks", vec![10, 11]), ("logs", vec![20, 21])]
        .into_iter()
        .map(|(table, values)| {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![100, 101])),
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![FIXTURE_SECONDS * 1_000_000; 2])
                            .with_timezone("UTC"),
                    ),
                    Arc::new(Int64Array::from(values)),
                ],
            )
            .unwrap();
            (table.to_string(), batch)
        })
        .collect()
}

fn delta_descriptor(root: &Path) -> StreamDescriptor {
    let mut descriptor = actual_descriptor(root);
    descriptor.tables = delta_data()
        .iter()
        .map(|(table, batch)| {
            (
                table.clone(),
                Digest::parse(schema_sha256(batch.schema().as_ref()).unwrap()).unwrap(),
            )
        })
        .collect();
    descriptor
}

async fn delta_tables(root: &Path, descriptor: &StreamDescriptor, create: bool) -> DeltaTables {
    let columns = delta_data()
        .iter()
        .map(|(table, batch)| {
            (
                table.clone(),
                delta_columns(batch.schema().as_ref()).unwrap(),
            )
        })
        .collect();
    DeltaTables::open(
        DeltaStore::local(&fs::canonicalize(root).unwrap()).unwrap(),
        DeltaIdentity::of(descriptor).unwrap(),
        &columns,
        create,
        2,
    )
    .await
    .unwrap()
}

async fn open_delta<'a>(
    root: &Path,
    owner: &'a LocalOwnership,
    mirror: &'a Mirror,
    descriptor: &StreamDescriptor,
    tables: DeltaTables,
) -> Result<TransactionController<'a, &'a Mirror>> {
    TransactionController::open_with_delta(
        TransactionStateStore::local(root, owner)?,
        TransactionParts::local(root, owner)?,
        mirror,
        descriptor,
        Some(tables),
        FlushConcurrency::SERIAL,
    )
    .await
}

/// A fresh controller over freshly opened (never created) tables: a restart.
async fn restart<'a>(
    root: &Path,
    owner: &'a LocalOwnership,
    mirror: &'a Mirror,
    descriptor: &StreamDescriptor,
) -> Result<TransactionController<'a, &'a Mirror>> {
    let tables = delta_tables(root, descriptor, false).await;
    open_delta(root, owner, mirror, descriptor, tables).await
}

/// Initializes a stream at `root`, creates its tables and makes its first
/// commit (ordinals 1..=2) fail at `stage`, leaving the journal pending.
async fn interrupted_first_commit(root: &Path, mirror: &Mirror, stage: Stage) -> StreamDescriptor {
    let descriptor = delta_descriptor(root);
    let owner = LocalOwnership::acquire(&[root.into()]).unwrap();
    initialize(root, &owner, &descriptor).await;
    let tables = delta_tables(root, &descriptor, true).await;
    let mut controller = open_delta(root, &owner, mirror, &descriptor, tables)
        .await
        .unwrap();
    let injected = fail(stage);
    assert!(commit_rows(&mut controller, delta_data()).await.is_err());
    drop(injected);
    assert!(
        commit_rows(&mut controller, delta_data()).await.is_err(),
        "the controller stays poisoned after a failed commit step"
    );
    descriptor
}

async fn journal(root: &Path, owner: &LocalOwnership) -> (u64, Option<TransactionPhase>) {
    let snapshot = TransactionStateStore::local(root, owner)
        .unwrap()
        .load()
        .await
        .unwrap();
    (
        snapshot.authority.unwrap().payload.checkpoint.ordinal,
        snapshot.pending.map(|pending| pending.payload.phase),
    )
}

/// Each table's version and `txn`, from a fresh handle.
async fn logs(root: &Path, descriptor: &StreamDescriptor) -> BTreeMap<String, (u64, Option<i64>)> {
    let tables = delta_tables(root, descriptor, false).await;
    let mut logs = BTreeMap::new();
    for table in ["blocks", "logs"] {
        logs.insert(
            table.to_string(),
            (
                tables.version(table).unwrap(),
                tables.txn_version(table).await.unwrap(),
            ),
        );
    }
    logs
}

fn expect(entries: &[(&str, u64, Option<i64>)]) -> BTreeMap<String, (u64, Option<i64>)> {
    entries
        .iter()
        .map(|(table, version, txn)| (table.to_string(), (*version, *txn)))
        .collect()
}

/// The part files of `table`.
fn table_parts(root: &Path, table: &str) -> Vec<PathBuf> {
    let root = fs::canonicalize(root).unwrap();
    data_files(&root)
        .into_iter()
        .filter(|path| path.starts_with(root.join(table)))
        .collect()
}

/// Each table's active `add` paths, replayed from its JSON log.
fn active(root: &Path, table: &str) -> Vec<String> {
    let log = root.join(table).join("_delta_log");
    let mut versions: Vec<PathBuf> = fs::read_dir(&log)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    versions.sort();
    let mut files = std::collections::BTreeSet::new();
    for version in versions {
        for line in fs::read_to_string(version).unwrap().lines() {
            let action: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(add) = action.get("add") {
                assert!(
                    files.insert(add["path"].as_str().unwrap().to_string()),
                    "{table}: a part is added twice"
                );
            }
            if let Some(remove) = action.get("remove") {
                files.remove(remove["path"].as_str().unwrap());
            }
        }
    }
    files.into_iter().collect()
}

async fn commit_rows(
    controller: &mut TransactionController<'_, &Mirror>,
    batches: HashMap<String, RecordBatch>,
) -> Result<CommittedFlush> {
    controller
        .commit(
            prefix(controller.authority()),
            batches,
            metadata(),
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await
}

async fn txn(tables: &DeltaTables) -> BTreeMap<String, Option<i64>> {
    let mut versions = BTreeMap::new();
    for table in ["blocks", "logs"] {
        versions.insert(table.to_string(), tables.txn_version(table).await.unwrap());
    }
    versions
}

fn versions(entries: &[(&str, Option<i64>)]) -> BTreeMap<String, Option<i64>> {
    entries
        .iter()
        .map(|(table, version)| (table.to_string(), *version))
        .collect()
}

/// Commits a part at a made-up path to `table` with this stream's `appId`
/// and `txn` version `txn`, as a foreign writer using the appId would.
async fn foreign_commit(root: &Path, descriptor: &StreamDescriptor, table: &str, txn: i64) {
    let mut tables = delta_tables(root, descriptor, false).await;
    let date = FIXTURE_DATE.strip_prefix("date=").unwrap();
    let stats = crate::delta::stats::stats_json(&delta_data()[table]).unwrap();
    let add = PartAdd::new(
        table,
        0,
        &format!("{FIXTURE_DATE}/foreign.parquet"),
        date,
        1,
        &stats,
        1,
    )
    .unwrap();
    tables
        .commit_parts(vec![add], txn, HashMap::new(), 1, &CommitHooks::NONE)
        .await
        .unwrap();
}

#[tokio::test]
async fn every_table_commits_once_blocks_last_with_the_last_ordinal_then_authority_advances() {
    let root = tempfile::tempdir().unwrap();
    let descriptor = delta_descriptor(root.path());
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    initialize(root.path(), &owner, &descriptor).await;
    let tables = delta_tables(root.path(), &descriptor, true).await;
    let mut controller = open_delta(root.path(), &owner, &mirror, &descriptor, tables)
        .await
        .unwrap()
        .with_concurrency(FlushConcurrency {
            encoders: 2,
            publications: 2,
            inflight_bytes: 64 * 1024 * 1024,
        })
        .unwrap();

    for (round, ordinal) in [(1_u64, 2_i64), (2, 4)] {
        let committed = commit_rows(&mut controller, delta_data()).await.unwrap();
        assert_eq!(committed.ordinal, ordinal as u64);
        let order: Vec<_> = committed.delta.iter().map(|c| c.table.as_str()).collect();
        assert_eq!(order, ["logs", LAST_TABLE], "blocks commits last");
        assert!(committed.delta.iter().all(|c| c.version == round));
        assert!(committed.delta.iter().all(|c| !c.found_in_log));
        // A fresh handle reads each table's resume point back from its log.
        let reopened = delta_tables(root.path(), &descriptor, false).await;
        assert_eq!(
            txn(&reopened).await,
            versions(&[("blocks", Some(ordinal)), ("logs", Some(ordinal))])
        );
    }

    // Each log adds exactly the published parts, with their journaled sizes.
    let reopened = delta_tables(root.path(), &descriptor, false).await;
    for table in ["blocks", "logs"] {
        let log: Vec<serde_json::Value> = fs::read_to_string(
            root.path()
                .join(table)
                .join("_delta_log/00000000000000000002.json"),
        )
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
        let add = log.iter().find_map(|action| action.get("add")).unwrap();
        let path = root.path().join(table).join(add["path"].as_str().unwrap());
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            add["size"].as_u64().unwrap()
        );
        assert!(add["path"]
            .as_str()
            .unwrap()
            .starts_with(&format!("{FIXTURE_DATE}/part-v1-")));
        let stats: serde_json::Value =
            serde_json::from_str(add["stats"].as_str().unwrap()).unwrap();
        assert_eq!(stats["numRecords"], 2);
        assert_eq!(stats["minValues"]["block_num"], 100);
        assert_eq!(stats["maxValues"]["block_num"], 101);
        assert_eq!(reopened.version(table).unwrap(), 2);
    }

    // A transaction without rows makes no Delta commit, and authority advances.
    let empty: HashMap<_, _> = delta_data()
        .into_iter()
        .map(|(table, batch)| (table, batch.slice(0, 0)))
        .collect();
    let committed = commit_rows(&mut controller, empty).await.unwrap();
    assert_eq!((committed.ordinal, committed.files), (6, 0));
    assert!(committed.delta.is_empty());
    let reopened = delta_tables(root.path(), &descriptor, false).await;
    assert_eq!(reopened.version("blocks").unwrap(), 2);
    assert_eq!(
        txn(&reopened).await,
        versions(&[("blocks", Some(4)), ("logs", Some(4))])
    );
}

/// §4 "Committed, no Delta commit yet": every table rolls forward once.
#[tokio::test]
async fn a_committed_journal_without_delta_commits_rolls_every_table_forward_once() {
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let descriptor =
        interrupted_first_commit(root.path(), &mirror, Stage::CommittedPersisted).await;
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    assert_eq!(
        journal(root.path(), &owner).await,
        (0, Some(TransactionPhase::Committed))
    );
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 0, None), ("logs", 0, None)])
    );
    let mut controller = restart(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    assert_eq!(journal(root.path(), &owner).await, (2, None));
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 1, Some(2)), ("logs", 1, Some(2))])
    );
    // The roll-forward committed `blocks` after `logs`.
    let committed_at = |table: &str| {
        let commit = fs::read_to_string(
            root.path()
                .join(table)
                .join("_delta_log/00000000000000000001.json"),
        )
        .unwrap();
        commit
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find_map(|action| action.get("commitInfo")?.get("timestamp")?.as_i64())
            .unwrap()
    };
    assert!(committed_at("logs") <= committed_at("blocks"));
    // The next transaction continues after it; a restart commits nothing more.
    assert_eq!(
        commit_rows(&mut controller, delta_data())
            .await
            .unwrap()
            .ordinal,
        4
    );
    drop(controller);
    drop(
        restart(root.path(), &owner, &mirror, &descriptor)
            .await
            .unwrap(),
    );
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 2, Some(4)), ("logs", 2, Some(4))])
    );
    for table in ["blocks", "logs"] {
        assert_eq!(active(root.path(), table).len(), 2, "{table}");
    }
}

/// §4 "Between table commits": the tables whose `txn` holds the transaction
/// are skipped without reading their parts; the others commit, `blocks`
/// last. A second interruption during the roll-forward still ends with one
/// copy per table.
#[tokio::test]
async fn a_roll_forward_skips_committed_tables_and_survives_its_own_interruption() {
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    // `logs` (entry 1) commits, then the process stops before `blocks`.
    let descriptor = interrupted_first_commit(root.path(), &mirror, Stage::DeltaCommitted(1)).await;
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 0, None), ("logs", 1, Some(2))])
    );
    // Maintenance compacted `logs` and vacuumed its committed part: the
    // roll-forward must not read it.
    for part in table_parts(root.path(), "logs") {
        fs::remove_file(part).unwrap();
    }
    // The roll-forward itself is interrupted once `blocks` is committed.
    let injected = fail(Stage::DeltaCommitted(0));
    assert!(restart(root.path(), &owner, &mirror, &descriptor)
        .await
        .is_err());
    drop(injected);
    assert_eq!(
        journal(root.path(), &owner).await,
        (0, Some(TransactionPhase::Committed))
    );
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 1, Some(2)), ("logs", 1, Some(2))])
    );
    // The next start finds both logs holding it and only advances authority.
    let controller = restart(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    assert_eq!(journal(root.path(), &owner).await, (2, None));
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 1, Some(2)), ("logs", 1, Some(2))])
    );
    assert_eq!(active(root.path(), "blocks").len(), 1);
}

/// §4 "All Delta commits done, before AuthorityAdvanced" and
/// "AuthorityAdvanced, before mirror or clear": no part is read, so parts
/// that maintenance removed after their commits cannot stop the restart.
#[tokio::test]
async fn once_every_log_or_authority_holds_the_transaction_no_part_is_read() {
    for stage in [Stage::DeltaCommittedAll, Stage::AuthorityAdvanced] {
        let root = tempfile::tempdir().unwrap();
        let mirror = Mirror::default();
        let descriptor = interrupted_first_commit(root.path(), &mirror, stage).await;
        let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
        let expected_authority = if stage == Stage::AuthorityAdvanced {
            2
        } else {
            0
        };
        assert_eq!(
            journal(root.path(), &owner).await,
            (expected_authority, Some(TransactionPhase::Committed)),
            "{stage:?}"
        );
        for table in ["blocks", "logs"] {
            for part in table_parts(root.path(), table) {
                fs::remove_file(part).unwrap();
            }
        }
        let controller = restart(root.path(), &owner, &mirror, &descriptor)
            .await
            .unwrap_or_else(|error| panic!("{stage:?}: {error:#}"));
        assert_eq!(controller.authority().checkpoint.ordinal, 2, "{stage:?}");
        assert_eq!(
            mirror.head.borrow().as_ref().map(|head| head.ordinal),
            Some(2)
        );
        drop(controller);
        assert_eq!(journal(root.path(), &owner).await, (2, None), "{stage:?}");
        assert_eq!(
            logs(root.path(), &descriptor).await,
            expect(&[("blocks", 1, Some(2)), ("logs", 1, Some(2))]),
            "{stage:?}"
        );
    }
}

/// §4 last row and §4.1: an uncommitted table's part is gone (a full VACUUM
/// after a long outage) or corrupt. Recovery fails closed, names the part
/// and the tables that already hold the transaction, and keeps the journal.
#[tokio::test]
async fn a_missing_or_corrupt_uncommitted_part_fails_closed_with_the_journal_kept() {
    for missing in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let mirror = Mirror::default();
        let descriptor =
            interrupted_first_commit(root.path(), &mirror, Stage::DeltaCommitted(1)).await;
        let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
        let [part] = table_parts(root.path(), "blocks").try_into().unwrap();
        if missing {
            fs::remove_file(&part).unwrap();
        } else {
            fs::write(&part, b"corrupt").unwrap();
        }
        let error = restart(root.path(), &owner, &mirror, &descriptor)
            .await
            .err()
            .unwrap();
        let message = format!("{error:#}");
        assert!(
            message.contains("cannot roll the Committed transaction of ordinals 1..=2")
                && message.contains("forward into the Delta tables blocks:")
                && message.contains("Tables that already hold the transaction: logs.")
                && message.contains("§4.1")
                && message.contains("the part of table `blocks` (blocks/date=2023-11-14/part-v1-"),
            "{message}"
        );
        assert_eq!(
            message.ends_with(".parquet) is missing"),
            missing,
            "{message}"
        );
        assert_eq!(
            journal(root.path(), &owner).await,
            (0, Some(TransactionPhase::Committed))
        );
        assert_eq!(
            logs(root.path(), &descriptor).await,
            expect(&[("blocks", 0, None), ("logs", 1, Some(2))])
        );
    }
}

/// §4 "`txn` > L": a log ahead of authority is refused before anything
/// changes, with and without a pending transaction.
#[tokio::test]
async fn a_log_ahead_of_authority_is_refused_before_anything_changes() {
    // No pending transaction: authority is at 2, `logs` holds 9.
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let descriptor = delta_descriptor(root.path());
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    initialize(root.path(), &owner, &descriptor).await;
    let tables = delta_tables(root.path(), &descriptor, true).await;
    let mut controller = open_delta(root.path(), &owner, &mirror, &descriptor, tables)
        .await
        .unwrap();
    commit_rows(&mut controller, delta_data()).await.unwrap();
    drop(controller);
    foreign_commit(root.path(), &descriptor, "logs", 9).await;
    let error = restart(root.path(), &owner, &mirror, &descriptor)
        .await
        .err()
        .unwrap();
    let message = format!("{error:#}");
    assert!(
        message.contains("the Delta log of table `logs` is ahead of this dataset's authority")
            && message.contains("holds transaction 9")
            && message.contains("up to ordinal 2")
            && message.contains("restored from an older copy"),
        "{message}"
    );

    // A pending transaction of ordinals 1..=2 while `blocks` holds 1: between
    // the predecessor (0) and the transaction, which no fireparq commit makes.
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let descriptor =
        interrupted_first_commit(root.path(), &mirror, Stage::CommittedPersisted).await;
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    foreign_commit(root.path(), &descriptor, "blocks", 1).await;
    let error = restart(root.path(), &owner, &mirror, &descriptor)
        .await
        .err()
        .unwrap();
    let message = format!("{error:#}");
    assert!(
        message.contains("table `blocks` is ahead")
            && message.contains("holds transaction 1")
            && message.contains("the pending transaction ends at ordinal 2"),
        "{message}"
    );
    // Nothing changed: `logs` was not rolled forward, the journal is kept.
    assert_eq!(
        journal(root.path(), &owner).await,
        (0, Some(TransactionPhase::Committed))
    );
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 1, Some(1)), ("logs", 0, None)])
    );
}

/// §3.5: an earlier commit of the transaction whose outcome was unknown
/// lands while the roll-forward runs, after it read `txn`. The roll-forward's
/// own commit loses to it (`ConcurrentTransaction`) and is resolved from the
/// log: the part is added once. A later restart finds it in the log.
#[tokio::test]
async fn a_delayed_copy_of_an_unknown_commit_is_resolved_from_the_log() {
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let descriptor = interrupted_first_commit(root.path(), &mirror, Stage::DeltaCommitted(1)).await;
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let pending = TransactionStateStore::local(root.path(), &owner)
        .unwrap()
        .load()
        .await
        .unwrap()
        .pending
        .unwrap()
        .payload;
    let mut recovering = delta_tables(root.path(), &descriptor, false).await;
    let missing = recovering
        .check_progress(0, Some(&pending), 1)
        .await
        .unwrap();
    assert_eq!(missing, ["blocks"]);
    // The delayed copy of `blocks`' commit: the same `add` and `txn`.
    let mut delayed = delta_tables(root.path(), &descriptor, false).await;
    let landed = delayed
        .roll_forward(&pending, &missing, 1, &CommitHooks::NONE)
        .await
        .unwrap();
    assert_eq!((landed[0].version, landed[0].found_in_log), (1, false));
    let resolved = recovering
        .roll_forward(&pending, &missing, 1, &CommitHooks::NONE)
        .await
        .unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(
        (resolved[0].table.as_str(), resolved[0].version),
        ("blocks", 1)
    );
    assert!(resolved[0].found_in_log, "resolved from the log");
    assert_eq!(active(root.path(), "blocks").len(), 1);
    let controller = restart(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 1, Some(2)), ("logs", 1, Some(2))])
    );
}
