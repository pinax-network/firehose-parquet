//! Spike checks for #643: pre-written parts committed as-is, `txn` read-back,
//! exactly-once roll-forward, VACUUM modes and the checked type mapping, on
//! local disk, an in-memory store and (when `DELTA_SPIKE_S3_ENDPOINT` names a
//! loopback S3 server, as `run.sh` does) S3 with conditional puts.

use delta_lake_spike::delta::{self, PartAdd};
use delta_lake_spike::mapping::{self, Fixture, Mapping};
use delta_lake_spike::storage::{Lake, S3Settings};
use delta_lake_spike::{app_id, open_or_create, write_transaction, SpikeTransaction, Variant};
use deltalake_core::kernel::transaction::CommitConflictError;
use deltalake_core::kernel::transaction::TransactionError;
use deltalake_core::operations::vacuum::VacuumMode;
use deltalake_core::DeltaTableError;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// 2026-09-25, as days since the epoch.
const DATE: i32 = 20_721;

fn txn(stream: &str, ordinal: u64) -> SpikeTransaction {
    SpikeTransaction {
        stream: stream.into(),
        first_ordinal: ordinal,
        last_ordinal: ordinal,
        fixture: Fixture {
            date: DATE,
            first_block: (ordinal - 1) * 10,
            blocks: 10,
            txs_per_block: 3,
        },
        variant: Variant::Standard,
    }
}

fn lakes() -> Vec<(&'static str, Lake, Option<tempfile::TempDir>)> {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lakes = vec![
        ("local", Lake::local(dir.path()), Some(dir)),
        ("memory", Lake::memory(), None),
    ];
    if let Some(settings) = S3Settings::from_env() {
        let prefix = format!("t-{}", uuid_like());
        lakes.push(("s3", Lake::s3(settings, &prefix), None));
    }
    lakes
}

/// A prefix unique to this process and call: tests run in parallel and the
/// clock alone can repeat (macOS reports microseconds).
fn uuid_like() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "{:x}-{}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

#[test]
fn checked_casts_refuse_values_that_do_not_fit() {
    let fixture = Fixture {
        date: DATE,
        first_block: 0,
        blocks: 2,
        txs_per_block: 2,
    };
    // Every mapped column of both tables converts, and nothing unsigned,
    // dictionary-typed or millisecond-precision remains.
    for table in ["blocks", "transactions"] {
        let batch = mapping::to_delta_batch(&fixture.source(table), mapping::table_mapping(table))
            .expect("mapping");
        mapping::assert_delta_types(&batch);
        assert!(batch.schema().column_with_name("date").is_none());
    }
    // `nonce` holds u64 values above i64::MAX: as Int64 it must fail, not wrap.
    let wrong = [("nonce", Mapping::Int64)];
    let err = mapping::to_delta_batch(&fixture.source_blocks(), &wrong).unwrap_err();
    assert!(err.to_string().contains("nonce"), "{err}");
    // As Decimal(20,0) the largest u64 survives exactly.
    let right = [("nonce", Mapping::Decimal20)];
    let batch = mapping::to_delta_batch(&fixture.source_blocks(), &right).unwrap();
    let decimals = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .unwrap();
    assert_eq!(decimals.value(0), i128::from(u64::MAX));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_written_parts_are_committed_byte_for_byte_with_a_txn() {
    for (name, lake, _guard) in lakes() {
        let mut tables = open_or_create(&lake).await.expect("create tables");
        let t = txn("s1", 1);
        let (add, bytes) = t.publish(&lake, "blocks", 9).await;
        let (_, table) = tables.iter_mut().find(|(n, _)| n == "blocks").unwrap();
        let committed = delta::commit_parts(table, &[add.clone()], &app_id("s1"), 1, &[])
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(committed.version, 1, "{name}");

        // The log adds exactly the published object, and the object is unchanged.
        let reopened = delta::open_table(lake.log_store("blocks")).await.unwrap();
        let (files, rows, paths) = delta::active_files(&reopened).unwrap();
        assert_eq!((files, rows), (1, 10), "{name}");
        assert_eq!(paths, vec![add.path.clone()], "{name}");
        let stored = lake.read("blocks", &add.path).await;
        assert_eq!(stored, bytes, "{name}: the part was not rewritten");
        assert_eq!(stored.len() as i64, add.size, "{name}");
        let reader = SerializedFileReader::new(bytes::Bytes::from(stored)).unwrap();
        let footer = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap();
        assert!(
            footer.iter().any(|kv| kv.key == "fireparq.transaction"),
            "{name}: fireparq footer metadata survives"
        );

        // `txn` reads back from a fresh handle: the resume point of this table.
        assert_eq!(
            delta::txn_version(&reopened, &app_id("s1")).await.unwrap(),
            Some(1),
            "{name}"
        );
        assert_eq!(
            delta::txn_version(&reopened, &app_id("other"))
                .await
                .unwrap(),
            None,
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_rolls_each_table_forward_exactly_once() {
    for (name, lake, _guard) in lakes() {
        let mut tables = open_or_create(&lake).await.unwrap();
        for ordinal in 1..=3 {
            write_transaction(&lake, &mut tables, &txn("s2", ordinal))
                .await
                .unwrap();
        }
        // Transaction 4: every part is published (the journal would be Committed),
        // then the writer "crashes" after the first table's Delta commit.
        let t4 = txn("s2", 4);
        let mut adds: Vec<PartAdd> = Vec::new();
        for (index, (table, _)) in tables.iter().enumerate() {
            adds.push(t4.publish(&lake, table, index).await.0);
        }
        let (first_name, first) = &mut tables[0];
        assert_eq!(first_name, "transactions");
        delta::commit_parts(first, &[adds[0].clone()], &app_id("s2"), 4, &[])
            .await
            .unwrap();
        drop(tables);

        // Recovery with fresh handles: the committed table is skipped by its txn
        // version, the other one is committed once.
        let mut reopened = open_or_create(&lake).await.unwrap();
        let mut outcomes = Vec::new();
        for ((_, table), add) in reopened.iter_mut().zip(&adds) {
            outcomes.push(
                delta::roll_forward(table, std::slice::from_ref(add), &app_id("s2"), 4)
                    .await
                    .unwrap()
                    .is_some(),
            );
        }
        assert_eq!(outcomes, vec![false, true], "{name}");
        // A second recovery (a crash before authority advanced) commits nothing.
        for ((_, table), add) in reopened.iter_mut().zip(&adds) {
            let again = delta::roll_forward(table, std::slice::from_ref(add), &app_id("s2"), 4)
                .await
                .unwrap();
            assert!(again.is_none(), "{name}");
        }
        // A pending transaction behind the log is refused (log ahead of authority).
        let err = delta::roll_forward(&mut reopened[1].1, &adds[1..], &app_id("s2"), 3)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ahead"), "{name}: {err}");

        for (table_name, table) in &reopened {
            let (_, rows, _) = delta::active_files(table).unwrap();
            let per_txn = if table_name == "blocks" { 10 } else { 30 };
            assert_eq!(
                rows,
                4 * per_txn,
                "{name} {table_name}: no loss, no duplicate"
            );
            assert_eq!(
                delta::txn_version(table, &app_id("s2")).await.unwrap(),
                Some(4)
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_writers_rebase_on_blind_appends_but_not_on_their_own_app_id() {
    for (name, lake, _guard) in lakes() {
        let mut tables = open_or_create(&lake).await.unwrap();
        write_transaction(&lake, &mut tables, &txn("s3", 1))
            .await
            .unwrap();
        // Two handles at the same version: another application's blind append
        // wins first; ours rebases (a retry) and commits on top.
        let mut ours = delta::open_table(lake.log_store("blocks")).await.unwrap();
        let mut other = delta::open_table(lake.log_store("blocks")).await.unwrap();
        let (a, _) = txn("s3", 2).publish(&lake, "blocks", 1).await;
        let (b, _) = txn("x", 2).publish(&lake, "blocks", 1).await;
        delta::commit_parts(&mut other, &[b], &app_id("x"), 2, &[])
            .await
            .unwrap();
        let rebased = delta::commit_parts(&mut ours, &[a], &app_id("s3"), 2, &[])
            .await
            .unwrap();
        // Read at version 1, the other writer took 2: delta-rs checked 2 for
        // conflicts (a blind append, none) and committed at 3.
        assert_eq!(rebased.version, 3, "{name}: {rebased:?}");

        // A stale handle committing the same appId again is a conflict, not a
        // second copy: delta-rs treats the winner's txn as a concurrent transaction.
        let mut stale = delta::open_table(lake.log_store("blocks")).await.unwrap();
        let mut current = delta::open_table(lake.log_store("blocks")).await.unwrap();
        let (c, _) = txn("s3", 3).publish(&lake, "blocks", 1).await;
        delta::commit_parts(&mut current, &[c.clone()], &app_id("s3"), 3, &[])
            .await
            .unwrap();
        let err = delta::commit_parts(&mut stale, &[c], &app_id("s3"), 3, &[])
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                DeltaTableError::Transaction {
                    source: TransactionError::CommitConflict(
                        CommitConflictError::ConcurrentTransaction
                    )
                }
            ),
            "{name}: {err:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_serialize_through_conditional_puts() {
    for (name, lake, _guard) in lakes() {
        let _ = open_or_create(&lake).await.unwrap();
        // Four independent applications append to one table at the same time.
        // Every commit must land exactly once at a distinct version; losers of a
        // conditional put (`VersionAlreadyExists`) retry at the next version.
        let mut tasks = Vec::new();
        for writer in 0..4u64 {
            let lake = lake.clone();
            tasks.push(tokio::spawn(async move {
                let stream = format!("w{writer}");
                let mut table = delta::open_table(lake.log_store("blocks")).await.unwrap();
                let mut retries = 0;
                for ordinal in 1..=8 {
                    let (add, _) = txn(&stream, ordinal).publish(&lake, "blocks", 1).await;
                    let c = delta::commit_parts(
                        &mut table,
                        &[add],
                        &app_id(&stream),
                        ordinal as i64,
                        &[],
                    )
                    .await
                    .unwrap();
                    retries += c.retries;
                }
                retries
            }));
        }
        let mut retries = 0;
        for task in tasks {
            retries += task.await.unwrap();
        }
        let table = delta::open_table(lake.log_store("blocks")).await.unwrap();
        assert_eq!(table.version(), Some(32), "{name}");
        let (files, rows, _) = delta::active_files(&table).unwrap();
        assert_eq!((files, rows), (32, 320), "{name}");
        for writer in 0..4 {
            let v = delta::txn_version(&table, &app_id(&format!("w{writer}")))
                .await
                .unwrap();
            assert_eq!(v, Some(8), "{name}");
        }
        eprintln!("{name}: 32 concurrent commits, {retries} lost conditional puts retried");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lite_vacuum_keeps_uncommitted_parts_and_full_vacuum_deletes_them() {
    for (name, lake, _guard) in lakes() {
        if name == "memory" {
            continue; // VACUUM needs listing timestamps; covered locally and on S3.
        }
        let mut tables = open_or_create(&lake).await.unwrap();
        write_transaction(&lake, &mut tables, &txn("s4", 1))
            .await
            .unwrap();
        // A part published by a pending transaction, not yet in the log.
        let (pending, _) = txn("s4", 2).publish(&lake, "blocks", 1).await;
        let table = delta::open_table(lake.log_store("blocks")).await.unwrap();
        let (table, lite) = table
            .vacuum()
            .with_mode(VacuumMode::Lite)
            .with_retention_period(chrono::Duration::zero())
            .with_enforce_retention_duration(false)
            .with_dry_run(false)
            .await
            .unwrap();
        assert!(lite.files_deleted.is_empty(), "{name}: {lite:?}");
        assert!(lake.exists("blocks", &pending.path).await, "{name}");
        let (_, full) = table
            .vacuum()
            .with_mode(VacuumMode::Full)
            .with_retention_period(chrono::Duration::zero())
            .with_enforce_retention_duration(false)
            .with_dry_run(false)
            .await
            .unwrap();
        assert!(
            full.files_deleted
                .iter()
                .any(|f| f.ends_with(&pending.path)),
            "{name}: {full:?}"
        );
        assert!(!lake.exists("blocks", &pending.path).await, "{name}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_create_refuses_to_overwrite_a_part() {
    for (name, lake, _guard) in lakes() {
        let _ = open_or_create(&lake).await.unwrap();
        let (add, bytes) = txn("s5", 1).publish(&lake, "blocks", 1).await;
        let again = lake
            .try_publish_part("blocks", &add.path, b"different bytes".to_vec())
            .await;
        assert!(
            matches!(again, Err(object_store::Error::AlreadyExists { .. })),
            "{name}: a second create of the same part must fail: {again:?}"
        );
        assert_eq!(lake.read("blocks", &add.path).await, bytes, "{name}");
    }
}
