//! #643 L3: the Delta commit step between Committed and the authority
//! advance (`docs/design/delta-lake.md` §3.1). One commit per table with a
//! part, `txn = last ordinal`, the other tables first and `blocks` last; a
//! failure after a table's commit leaves authority behind.
//!
//! Recovery of a Committed journal does not roll the Delta commits forward
//! yet: `recovery_does_not_roll_delta_commits_forward_yet` pins today's
//! behavior, which #643 L4 replaces with a roll-forward gated by `txn`.
use super::*;
use crate::delta::commit::{DeltaTables, LAST_TABLE};
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

#[tokio::test]
async fn every_table_commits_once_blocks_last_with_the_last_ordinal_then_authority_advances() {
    let root = tempfile::tempdir().unwrap();
    let descriptor = delta_descriptor(root.path());
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    initialize(root.path(), &owner, &descriptor).await;
    let tables = delta_tables(root.path(), &descriptor, true).await;
    let mut controller = open(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap()
        .with_concurrency(FlushConcurrency {
            encoders: 2,
            publications: 2,
            inflight_bytes: 64 * 1024 * 1024,
        })
        .unwrap()
        .with_delta_tables(tables);

    for (round, ordinal) in [(1_u64, 2_i64), (2, 4)] {
        let committed = commit_rows(&mut controller, delta_data()).await.unwrap();
        assert_eq!(committed.ordinal, ordinal as u64);
        let order: Vec<_> = committed.delta.iter().map(|c| c.table.as_str()).collect();
        assert_eq!(order, ["logs", LAST_TABLE], "blocks commits last");
        assert!(committed.delta.iter().all(|c| c.version == round));
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

#[tokio::test]
async fn recovery_does_not_roll_delta_commits_forward_yet() {
    let root = tempfile::tempdir().unwrap();
    let descriptor = delta_descriptor(root.path());
    let mirror = Mirror::default();
    {
        let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
        initialize(root.path(), &owner, &descriptor).await;
        let tables = delta_tables(root.path(), &descriptor, true).await;
        let mut controller = open(root.path(), &owner, &mirror, &descriptor)
            .await
            .unwrap()
            .with_delta_tables(tables);
        // `logs` (entry 1) commits, then the process "stops" before `blocks`.
        let injected = fail(Stage::DeltaCommitted(1));
        assert!(commit_rows(&mut controller, delta_data()).await.is_err());
        drop(injected);
        assert!(
            commit_rows(&mut controller, delta_data()).await.is_err(),
            "the controller stays poisoned after a Delta commit step failure"
        );
        assert_eq!(controller.authority().checkpoint.ordinal, 0);
        let pending = TransactionStateStore::local(root.path(), &owner)
            .unwrap()
            .load()
            .await
            .unwrap()
            .pending
            .unwrap();
        assert_eq!(pending.payload.phase, TransactionPhase::Committed);
    }
    let tables = delta_tables(root.path(), &descriptor, false).await;
    assert_eq!(
        txn(&tables).await,
        versions(&[("blocks", None), ("logs", Some(2))]),
        "blocks commits last: the failure left it without this transaction"
    );

    // Today's Committed recovery advances authority without the missing
    // Delta commit (#643 L4 rolls `blocks` forward instead).
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let mut controller = open(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap()
        .with_delta_tables(delta_tables(root.path(), &descriptor, false).await);
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    let committed = commit_rows(&mut controller, delta_data()).await.unwrap();
    assert_eq!(committed.ordinal, 4);
    let tables = delta_tables(root.path(), &descriptor, false).await;
    assert_eq!(
        txn(&tables).await,
        versions(&[("blocks", Some(4)), ("logs", Some(4))])
    );
    assert_eq!(tables.version("blocks").unwrap(), 1);
    assert_eq!(tables.version("logs").unwrap(), 2);
}

#[tokio::test]
async fn a_stop_after_every_delta_commit_recovers_to_consistent_tables() {
    let root = tempfile::tempdir().unwrap();
    let descriptor = delta_descriptor(root.path());
    let mirror = Mirror::default();
    {
        let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
        initialize(root.path(), &owner, &descriptor).await;
        let mut controller = open(root.path(), &owner, &mirror, &descriptor)
            .await
            .unwrap()
            .with_delta_tables(delta_tables(root.path(), &descriptor, true).await);
        let injected = fail(Stage::DeltaCommittedAll);
        assert!(commit_rows(&mut controller, delta_data()).await.is_err());
        drop(injected);
        assert_eq!(controller.authority().checkpoint.ordinal, 0);
    }
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let controller = open(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    let tables = delta_tables(root.path(), &descriptor, false).await;
    assert_eq!(
        txn(&tables).await,
        versions(&[("blocks", Some(2)), ("logs", Some(2))])
    );
}
