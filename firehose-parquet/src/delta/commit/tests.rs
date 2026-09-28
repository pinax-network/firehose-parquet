//! The spike's commit checks (`spikes/delta-lake/tests/spike.rs`), ported to
//! fireparq's commit layer: pre-written parts committed byte for byte with a
//! `txn`, `txn` read-back from a fresh handle, blind appends rebasing over
//! other writers, a same-`appId` conflict, concurrent writers, and the table
//! validation, on local disk, an in-memory store and a loopback S3 endpoint
//! (conditional puts, one attempt per request). #643 L4 adds recovery's
//! reads: an interrupted table creation completed, existing tables opened for
//! `recovery recover`, and a roll-forward that commits each part once in
//! every arrival order of a delayed commit.
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::kernel::{Action, Transaction};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use object_store_delta::memory::InMemory;
use object_store_delta::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::Value;

use super::*;
use crate::delta::stats::stats_json;
use crate::delta::store::DeltaStore;
use crate::delta::{delta_columns, DeltaIdentity};

pub(crate) mod loopback_s3;

/// 2026-09-25T12:00:00Z in microseconds.
const NOON: i64 = 1_790_337_600_000_000;
const DATE: &str = "2026-09-25";
const TABLES: [&str; 2] = ["blocks", "transactions"];

fn identity(seed: char) -> DeltaIdentity {
    DeltaIdentity {
        descriptor: seed.to_string().repeat(64),
        chain: "test-chain".into(),
        block_type: "evm".into(),
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("hash", DataType::Utf8, false),
    ]))
}

fn columns() -> BTreeMap<String, Vec<deltalake_core::kernel::StructField>> {
    TABLES
        .iter()
        .map(|table| (table.to_string(), delta_columns(&schema()).unwrap()))
        .collect()
}

/// Ten rows per transaction ordinal: blocks `(ordinal - 1) * 10 ..`.
fn batch(ordinal: u64) -> RecordBatch {
    let first = (ordinal as i64 - 1) * 10;
    let numbers: Vec<i64> = (first..first + 10).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(numbers.clone())),
            Arc::new(
                TimestampMicrosecondArray::from(
                    numbers
                        .iter()
                        .map(|n| NOON + n * 1_000_000)
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                numbers
                    .iter()
                    .map(|n| format!("0x{n:04x}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// A Parquet 60 part with a fireparq footer key, as `build` writes one.
fn encode(batch: &RecordBatch) -> Vec<u8> {
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "fireparq.ingest.transaction".to_string(),
            Some("t".to_string()),
        )]))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    bytes
}

enum Lake {
    /// The directory lives as long as the lake.
    Local(#[allow(dead_code)] tempfile::TempDir, std::path::PathBuf),
    Memory(Arc<InMemory>),
    S3(loopback_s3::Server, Arc<dyn ObjectStore>),
}

impl Lake {
    async fn all() -> Vec<(&'static str, Lake)> {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let server = loopback_s3::Server::start().await;
        let aws = crate::s3::AwsConfig {
            aws_access_key_id: Some("loopback".into()),
            aws_secret_access_key: Some("loopback".into()),
            aws_session_token: None,
            aws_region: Some("us-east-1".into()),
            aws_endpoint_url: Some(server.endpoint.clone()),
        };
        let client: Arc<dyn ObjectStore> = Arc::new(
            crate::delta::store::s3_builder(&aws, loopback_s3::BUCKET)
                .unwrap()
                .with_allow_http(true)
                .build()
                .unwrap(),
        );
        vec![
            ("local", Lake::Local(dir, root)),
            ("memory", Lake::Memory(Arc::new(InMemory::new()))),
            ("s3", Lake::S3(server, client)),
        ]
    }

    fn store(&self) -> DeltaStore {
        match self {
            Lake::Local(_, root) => DeltaStore::local(root).unwrap(),
            Lake::Memory(store) => {
                DeltaStore::s3("memory-bucket", "dataset", store.clone()).unwrap()
            }
            Lake::S3(_, client) => {
                DeltaStore::s3(loopback_s3::BUCKET, "dataset", client.clone()).unwrap()
            }
        }
    }

    fn key(&self, table: &str, relative: &str) -> object_store_delta::path::Path {
        match self {
            Lake::Local(_, root) => {
                object_store_delta::path::Path::from_absolute_path(root.join(table).join(relative))
                    .unwrap()
            }
            _ => object_store_delta::path::Path::from(format!("dataset/{table}/{relative}")),
        }
    }

    fn root_store(&self) -> Arc<dyn ObjectStore> {
        match self {
            Lake::Local(..) => Arc::new(object_store_delta::local::LocalFileSystem::new()),
            Lake::Memory(store) => store.clone(),
            Lake::S3(_, client) => client.clone(),
        }
    }

    /// Publishes a part with a conditional create, as fireparq does.
    async fn publish(&self, table: &str, relative: &str, bytes: Vec<u8>) {
        self.root_store()
            .put_opts(
                &self.key(table, relative),
                PutPayload::from(bytes),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .unwrap();
    }

    async fn read(&self, table: &str, relative: &str) -> Vec<u8> {
        self.root_store()
            .get(&self.key(table, relative))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec()
    }

    async fn read_log(&self, table: &str, version: u64) -> Vec<Value> {
        let bytes = self
            .read(table, &format!("_delta_log/{version:020}.json"))
            .await;
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

async fn open(lake: &Lake, seed: char, create: bool) -> Result<DeltaTables> {
    DeltaTables::open(lake.store(), identity(seed), &columns(), create, 4).await
}

/// Publishes the part of `table` for transaction `ordinal` of stream `seed`
/// and returns its `add`.
async fn part(lake: &Lake, table: &str, seed: char, ordinal: u64) -> (PartAdd, Vec<u8>) {
    let batch = batch(ordinal);
    let bytes = encode(&batch);
    let name = format!("part-v1-{seed}-{ordinal}-{ordinal}-{seed}{ordinal:08}-0.parquet");
    let relative = format!("date={DATE}/{name}");
    lake.publish(table, &relative, bytes.clone()).await;
    let add = PartAdd::new(
        table,
        TABLES.iter().position(|t| *t == table).unwrap() as u32,
        &relative,
        DATE,
        bytes.len() as u64,
        &stats_json(&batch).unwrap(),
        1_790_337_600_123,
    )
    .unwrap();
    (add, bytes)
}

async fn transaction(
    lake: &Lake,
    tables: &mut DeltaTables,
    seed: char,
    ordinal: u64,
) -> Vec<TableCommit> {
    let mut parts = Vec::new();
    for table in TABLES {
        parts.push(part(lake, table, seed, ordinal).await.0);
    }
    tables
        .commit_parts(parts, ordinal as i64, HashMap::new(), 4, &CommitHooks::NONE)
        .await
        .unwrap()
}

/// Rows of the active files, from `numRecords`, and their sorted paths.
fn active(table: &DeltaTable) -> (usize, Vec<String>) {
    let state = table.snapshot().unwrap();
    let mut rows = 0;
    let mut paths = Vec::new();
    for file in state.log_data().iter() {
        rows += file.num_records().unwrap_or(0);
        paths.push(file.path().to_string());
    }
    paths.sort();
    (rows, paths)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_written_parts_are_committed_byte_for_byte_with_a_txn_blocks_last() {
    for (name, lake) in Lake::all().await {
        let mut tables = open(&lake, 'a', true).await.unwrap();
        for table in TABLES {
            assert_eq!(tables.version(table).unwrap(), 0, "{name}");
        }
        let (blocks, blocks_bytes) = part(&lake, "blocks", 'a', 1).await;
        let (transactions, _) = part(&lake, "transactions", 'a', 1).await;
        let order = Mutex::new(Vec::new());
        let before = |table: &str| {
            order.lock().unwrap().push(format!("before {table}"));
            Ok(())
        };
        let after = |table: &str, index: u32| {
            order.lock().unwrap().push(format!("after {table} {index}"));
            Ok(())
        };
        let hooks = CommitHooks {
            before: &before,
            after: &after,
        };
        let committed = tables
            .commit_parts(
                vec![blocks.clone(), transactions],
                1,
                HashMap::from([("fireparq.transaction".to_string(), Value::from("t1"))]),
                4,
                &hooks,
            )
            .await
            .unwrap();
        // `blocks` commits last, after the other table is durable.
        assert_eq!(
            *order.lock().unwrap(),
            [
                "before transactions",
                "after transactions 1",
                "before blocks",
                "after blocks 0"
            ],
            "{name}"
        );
        assert_eq!(
            committed
                .iter()
                .map(|c| (c.table.as_str(), c.version))
                .collect::<Vec<_>>(),
            [("transactions", 1), ("blocks", 1)],
            "{name}"
        );
        assert!(committed.iter().all(|c| c.tail_commits == 2), "{name}");

        // A fresh handle: the log adds exactly the published object, unchanged.
        let reopened = open(&lake, 'a', false).await.unwrap();
        let (rows, paths) = active(&reopened.tables["blocks"].table);
        assert_eq!(
            (rows, paths.clone()),
            (10, vec![blocks.add.path.clone()]),
            "{name}"
        );
        let stored = lake.read("blocks", &blocks.add.path).await;
        assert_eq!(stored, blocks_bytes, "{name}: the part was not rewritten");
        let footer = SerializedFileReader::new(bytes::Bytes::from(stored)).unwrap();
        assert!(
            footer
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .unwrap()
                .iter()
                .any(|kv| kv.key == "fireparq.ingest.transaction"),
            "{name}: fireparq's footer metadata survives"
        );

        // The commit is exactly the journaled add, a txn and a blind append.
        let log = lake.read_log("blocks", 1).await;
        let add = log.iter().find_map(|action| action.get("add")).unwrap();
        assert_eq!(add["path"], Value::from(blocks.add.path.clone()), "{name}");
        assert_eq!(
            add["partitionValues"],
            serde_json::json!({"date": DATE}),
            "{name}"
        );
        assert_eq!(add["size"], Value::from(blocks_bytes.len()), "{name}");
        assert_eq!(
            add["modificationTime"],
            Value::from(1_790_337_600_123_i64),
            "{name}"
        );
        assert_eq!(add["dataChange"], Value::from(true), "{name}");
        let stats: Value = serde_json::from_str(add["stats"].as_str().unwrap()).unwrap();
        assert_eq!(
            stats,
            serde_json::json!({
                "numRecords": 10,
                "minValues": {"block_num": 0, "timestamp": "2026-09-25T12:00:00.000Z"},
                "maxValues": {"block_num": 9, "timestamp": "2026-09-25T12:00:09.000Z"},
                "nullCount": {"block_num": 0, "timestamp": 0},
            }),
            "{name}"
        );
        let txn = log.iter().find_map(|action| action.get("txn")).unwrap();
        assert_eq!(txn["appId"], Value::from(identity('a').app_id()), "{name}");
        assert_eq!(txn["version"], Value::from(1), "{name}");
        let info = log
            .iter()
            .find_map(|action| action.get("commitInfo"))
            .unwrap();
        assert_eq!(info["isBlindAppend"], Value::from(true), "{name}");
        assert_eq!(info["fireparq.transaction"], Value::from("t1"), "{name}");
        assert_eq!(
            log.len(),
            3,
            "{name}: commitInfo, add and txn only: {log:?}"
        );
        if let Lake::S3(server, _) = &lake {
            // On the wire: two conditional creates of the log, and the part as sent.
            assert_eq!(
                server.keys("dataset/blocks/_delta_log/"),
                [
                    "dataset/blocks/_delta_log/00000000000000000000.json",
                    "dataset/blocks/_delta_log/00000000000000000001.json"
                ]
            );
            assert_eq!(
                server.object(&format!("dataset/blocks/{}", blocks.add.path)),
                Some(blocks_bytes.clone())
            );
        }

        // `txn` reads back from a fresh handle: the resume point of each table.
        for table in TABLES {
            assert_eq!(
                reopened.txn_version(table).await.unwrap(),
                Some(1),
                "{name}"
            );
        }
        assert_eq!(
            reopened.tables["blocks"]
                .table
                .snapshot()
                .unwrap()
                .transaction_version(
                    reopened.tables["blocks"].table.log_store().as_ref(),
                    "fireparq:other"
                )
                .await
                .unwrap(),
            None,
            "{name}"
        );
        // A second part for one table in one transaction is refused.
        let (again, _) = part(&lake, "transactions", 'a', 2).await;
        let mut writer = reopened;
        let twice = writer
            .commit_parts(
                vec![again.clone(), again],
                2,
                HashMap::new(),
                1,
                &CommitHooks::NONE,
            )
            .await
            .unwrap_err();
        assert!(!twice.unresolved, "{name}");
    }
}

/// Another application's blind append at the same version: delta-rs retries
/// at the next version.
async fn foreign_append(lake: &Lake, table: &str, app: &str, version: i64, add: PartAdd) -> u64 {
    let mut handle = open_table(lake.store().log_store(table).unwrap())
        .await
        .unwrap();
    handle.update_state().await.unwrap();
    let properties = CommitProperties::default()
        .with_application_transaction(Transaction::new(app, version))
        .with_create_checkpoint(false)
        .with_cleanup_expired_logs(Some(false))
        .with_max_retries(50);
    CommitBuilder::from(properties)
        .with_actions(vec![Action::Add(add.add)])
        .build(
            Some(handle.snapshot().unwrap()),
            handle.log_store(),
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: Some(vec!["date".into()]),
                predicate: None,
            },
        )
        .await
        .unwrap()
        .version()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_writers_rebase_on_blind_appends_but_not_on_their_own_app_id() {
    for (name, lake) in Lake::all().await {
        let mut ours = open(&lake, 'b', true).await.unwrap();
        transaction(&lake, &mut ours, 'b', 1).await;
        // Another writer commits while our handle is at version 1: ours rebases.
        let (foreign, _) = part(&lake, "blocks", 'x', 2).await;
        assert_eq!(
            foreign_append(&lake, "blocks", "someone-else", 2, foreign).await,
            2
        );
        let (mine, _) = part(&lake, "blocks", 'b', 2).await;
        let rebased = ours
            .commit_parts(vec![mine], 2, HashMap::new(), 1, &CommitHooks::NONE)
            .await
            .unwrap();
        assert_eq!(rebased[0].version, 3, "{name}: read at 1, 2 was taken");

        // A stale handle of the same stream committing again is a conflict,
        // not a second copy: the winner's txn is a concurrent transaction.
        let mut stale = open(&lake, 'b', false).await.unwrap();
        let mut current = open(&lake, 'b', false).await.unwrap();
        let (third, _) = part(&lake, "blocks", 'b', 3).await;
        current
            .commit_parts(
                vec![third.clone()],
                3,
                HashMap::new(),
                1,
                &CommitHooks::NONE,
            )
            .await
            .unwrap();
        let failure = stale
            .commit_parts(vec![third], 3, HashMap::new(), 1, &CommitHooks::NONE)
            .await
            .unwrap_err();
        assert!(is_same_app_conflict(&failure.error), "{name}: {failure}");
        assert!(!failure.unresolved, "{name}: a conflict wrote nothing");
        let reopened = open(&lake, 'b', false).await.unwrap();
        let (rows, _) = active(&reopened.tables["blocks"].table);
        assert_eq!(
            rows, 40,
            "{name}: 3 of ours and 1 foreign part, no duplicate"
        );
        assert_eq!(
            reopened.txn_version("blocks").await.unwrap(),
            Some(3),
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_serialize_through_conditional_puts() {
    for (name, lake) in Lake::all().await {
        let lake = Arc::new(lake);
        let mut ours = open(&lake, 'c', true).await.unwrap();
        // fireparq and three other applications append to `blocks` at once.
        // Every commit lands exactly once at a distinct version; a lost
        // conditional put is retried at the next version.
        let mut tasks = Vec::new();
        for writer in 0..3u64 {
            let lake = Arc::clone(&lake);
            tasks.push(tokio::spawn(async move {
                for ordinal in 1..=8 {
                    let seed = char::from(b'p' + writer as u8);
                    let (add, _) = part(&lake, "blocks", seed, ordinal).await;
                    foreign_append(
                        &lake,
                        "blocks",
                        &format!("writer-{writer}"),
                        ordinal as i64,
                        add,
                    )
                    .await;
                }
            }));
        }
        let mut retries = 0;
        for ordinal in 1..=8 {
            let (add, _) = part(&lake, "blocks", 'c', ordinal).await;
            let committed = ours
                .commit_parts(
                    vec![add],
                    ordinal as i64,
                    HashMap::new(),
                    1,
                    &CommitHooks::NONE,
                )
                .await
                .unwrap();
            retries += committed[0].retries;
        }
        for task in tasks {
            task.await.unwrap();
        }
        let reopened = open(&lake, 'c', false).await.unwrap();
        assert_eq!(reopened.version("blocks").unwrap(), 32, "{name}");
        let (rows, paths) = active(&reopened.tables["blocks"].table);
        assert_eq!((rows, paths.len()), (320, 32), "{name}");
        assert_eq!(
            reopened.txn_version("blocks").await.unwrap(),
            Some(8),
            "{name}"
        );
        let lost = match &*lake {
            Lake::S3(server, _) => format!(
                ", {} lost conditional puts on the server",
                server.lost_puts()
            ),
            _ => String::new(),
        };
        eprintln!("{name}: 32 concurrent commits, {retries} of ours retried{lost}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tables_are_created_once_then_validated_against_the_stream() {
    for (name, lake) in Lake::all().await {
        // Nothing accepted yet and no table: opening without creation refuses.
        let missing = open(&lake, 'd', false).await.unwrap_err();
        assert!(
            format!("{missing:#}").contains("has no Delta log"),
            "{name}: {missing:#}"
        );
        // Two writers race to create: both end with the same valid tables.
        let (first, second) = tokio::join!(open(&lake, 'd', true), open(&lake, 'd', true));
        let (first, second) = (first.unwrap(), second.unwrap());
        for table in TABLES {
            assert_eq!(first.version(table).unwrap(), 0, "{name}");
            assert_eq!(second.version(table).unwrap(), 0, "{name}");
            assert_eq!(first.tail_commits(table).unwrap(), 1, "{name}");
        }
        // Another stream's identity is refused, naming both descriptors.
        let other = open(&lake, 'e', true).await.unwrap_err();
        let message = format!("{other:#}");
        assert!(
            message.contains("belongs to another stream"),
            "{name}: {message}"
        );
        assert!(message.contains(&"d".repeat(64)) && message.contains(&"e".repeat(64)));
        // Another schema is refused.
        let mut changed = columns();
        changed.get_mut("blocks").unwrap().insert(
            0,
            deltalake_core::kernel::StructField::new(
                "extra",
                deltalake_core::kernel::DataType::LONG,
                true,
            ),
        );
        let refused = DeltaTables::open(lake.store(), identity('d'), &changed, true, 1)
            .await
            .unwrap_err();
        assert!(
            format!("{refused:#}").contains("different schema"),
            "{name}: {refused:#}"
        );
        // A table missing its identity properties is refused.
        let bare = identity('f');
        let log_store = lake.store().log_store("other").unwrap();
        deltalake_core::operations::create::CreateBuilder::new()
            .with_log_store(log_store)
            .with_columns(columns()["blocks"].clone())
            .with_partition_columns(["date"])
            .await
            .unwrap();
        let tampered = DeltaTables::open(
            lake.store(),
            bare,
            &BTreeMap::from([("other".to_string(), columns()["blocks"].clone())]),
            true,
            1,
        )
        .await
        .unwrap_err();
        assert!(
            format!("{tampered:#}")
                .contains("belongs to another stream (fireparq.descriptor = absent)"),
            "{name}: {tampered:#}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_log_tail_counts_commits_after_the_last_checkpoint() {
    for (name, lake) in Lake::all().await {
        let mut tables = open(&lake, 'g', true).await.unwrap();
        for ordinal in 1..=3 {
            transaction(&lake, &mut tables, 'g', ordinal).await;
        }
        assert_eq!(
            tables.tail_commits("blocks").unwrap(),
            4,
            "{name}: versions 0..=3"
        );
        // The maintenance job checkpoints version 3; a restart sees a tail of 0.
        deltalake_core::checkpoints::create_checkpoint(&tables.tables["blocks"].table, None)
            .await
            .unwrap();
        let mut reopened = open(&lake, 'g', false).await.unwrap();
        assert_eq!(reopened.tail_commits("blocks").unwrap(), 0, "{name}");
        assert_eq!(reopened.tail_commits("transactions").unwrap(), 4, "{name}");
        let committed = transaction(&lake, &mut reopened, 'g', 4).await;
        let blocks = committed.iter().find(|c| c.table == "blocks").unwrap();
        assert_eq!((blocks.version, blocks.tail_commits), (4, 1), "{name}");
        assert_eq!(
            reopened.txn_version("blocks").await.unwrap(),
            Some(4),
            "{name}"
        );
    }
}

/// A start interrupted between table creations (design §4) finds some
/// tables and creates the others, calling the creation hook only for those;
/// an empty local `_delta_log/`, or one holding only an interrupted local
/// write's staging file, counts as missing. `open_existing` (`recovery
/// recover`) validates everything but the schema and never creates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_interrupted_creation_is_completed_and_recovery_opens_existing_tables_only() {
    for (name, lake) in Lake::all().await {
        let only_blocks = BTreeMap::from([("blocks".to_string(), columns()["blocks"].clone())]);
        DeltaTables::open(lake.store(), identity('h'), &only_blocks, true, 1)
            .await
            .unwrap();
        if let Lake::Local(_, root) = &lake {
            std::fs::create_dir_all(root.join("transactions/_delta_log")).unwrap();
            std::fs::write(
                root.join("transactions/_delta_log/00000000000000000000.json#1"),
                b"{\"partial\"",
            )
            .unwrap();
        }
        // `recovery recover` refuses the missing table with guidance.
        let refused = DeltaTables::open_existing(lake.store(), identity('h'), TABLES, 1)
            .await
            .unwrap_err();
        assert!(
            format!("{refused:#}").contains("the next `build`, which creates them"),
            "{name}: {refused:#}"
        );
        let created = Mutex::new(Vec::new());
        let hook = |table: &str| {
            created.lock().unwrap().push(table.to_string());
            Ok(())
        };
        let tables =
            DeltaTables::open_with(lake.store(), identity('h'), &columns(), true, 2, &hook)
                .await
                .unwrap_or_else(|error| panic!("{name}: {error:#}"));
        assert_eq!(*created.lock().unwrap(), ["transactions"], "{name}");
        for table in TABLES {
            assert_eq!(tables.version(table).unwrap(), 0, "{name}");
        }
        // Existing tables open without their schema, but with their identity.
        let existing = DeltaTables::open_existing(lake.store(), identity('h'), TABLES, 1)
            .await
            .unwrap();
        assert_eq!(
            existing.names().collect::<Vec<_>>(),
            ["blocks", "transactions"]
        );
        let other = DeltaTables::open_existing(lake.store(), identity('i'), TABLES, 1)
            .await
            .unwrap_err();
        assert!(
            format!("{other:#}").contains("belongs to another stream"),
            "{name}: {other:#}"
        );
    }
}

/// A Committed transaction of ordinals 1..=2 with one ten-row part per table
/// of [`TABLES`] in `date=2023-11-14`, as recovery finds it in the journal.
fn committed_pending() -> crate::ingest::state::PendingTransaction {
    use crate::ingest::frontier::AcceptedFrontier;
    use crate::ingest::state::tests::{descriptor, event, routing, FIXTURE_DATE};
    use crate::ingest::state::{
        AuthorityState, Digest, PartCompression, PartReceipt, PendingTransaction, RoutingPolicy,
        TablePlan,
    };
    let mut descriptor = descriptor(RoutingPolicy::GenesisLookaheadV1);
    descriptor.tables = TABLES
        .iter()
        .map(|table| {
            (
                table.to_string(),
                Digest::hash("schema-fixture", table).unwrap(),
            )
        })
        .collect();
    let authority = AuthorityState::initial(descriptor.clone()).unwrap();
    let mut frontier = AcceptedFrontier::resume(&authority.checkpoint);
    for number in [100, 101] {
        let ordinal = frontier.receive(event(number, 1)).unwrap();
        frontier
            .accept(ordinal, routing(RoutingPolicy::GenesisLookaheadV1))
            .unwrap();
    }
    let tables = descriptor
        .tables
        .iter()
        .map(|(table, schema)| TablePlan {
            table: table.clone(),
            rows: 10,
            schema_sha256: schema.clone(),
            partition: FIXTURE_DATE.into(),
        })
        .collect();
    let mut pending = PendingTransaction::prepare(
        &authority,
        frontier.snapshot().unwrap().unwrap(),
        tables,
        PartCompression::Zstd,
    )
    .unwrap();
    for index in 0..pending.parts.len() as u32 {
        let receipt = PartReceipt {
            byte_size: 1_000 + u64::from(index),
            sha256: Digest::hash("part-fixture", &index).unwrap(),
            stats: stats_json(&batch(1)).unwrap(),
            modification_time: 1_790_337_600_123,
        };
        pending = pending.with_receipt(index, receipt, &descriptor).unwrap();
    }
    pending.committed_after_verification(&descriptor).unwrap()
}

/// Recovery's view of a Committed transaction (design §3.5 and §4), on every
/// store: `check_progress` names the tables whose `txn` lacks it; a delayed
/// copy of an earlier commit that lands in between wins, and the
/// roll-forward's own commit is resolved from the log instead of adding the
/// part twice; a raw copy of an already taken version is refused by its
/// condition; and a `txn` above authority is a log ahead, refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_roll_forward_commits_each_part_once_in_every_arrival_order() {
    for (name, lake) in Lake::all().await {
        let pending = committed_pending();
        let mut recovering = open(&lake, 'j', true).await.unwrap();
        let missing = recovering
            .check_progress(0, Some(&pending), 2)
            .await
            .unwrap();
        assert_eq!(missing, ["blocks", "transactions"], "{name}");
        // The delayed copy of the `transactions` commit lands now.
        let mut delayed = open(&lake, 'j', false).await.unwrap();
        let landed = delayed
            .roll_forward(
                &pending,
                &["transactions".to_string()],
                1,
                &CommitHooks::NONE,
            )
            .await
            .unwrap();
        assert_eq!((landed[0].version, landed[0].found_in_log), (1, false));
        let order = Mutex::new(Vec::new());
        let before = |table: &str| {
            order.lock().unwrap().push(table.to_string());
            Ok(())
        };
        let hooks = CommitHooks {
            before: &before,
            after: &|_, _| Ok(()),
        };
        let committed = recovering
            .roll_forward(&pending, &missing, 2, &hooks)
            .await
            .unwrap_or_else(|failure| panic!("{name}: {failure}"));
        assert_eq!(*order.lock().unwrap(), ["transactions", "blocks"], "{name}");
        let outcome: Vec<_> = committed
            .iter()
            .map(|commit| (commit.table.as_str(), commit.version, commit.found_in_log))
            .collect();
        assert_eq!(
            outcome,
            [("transactions", 1, true), ("blocks", 1, false)],
            "{name}: blocks last, transactions resolved from the log"
        );
        // A raw copy of an already landed commit, PUT again, finds its
        // version taken, and nothing changes.
        let commit_file = "_delta_log/00000000000000000001.json";
        let bytes = lake.read("transactions", commit_file).await;
        let late = lake
            .root_store()
            .put_opts(
                &lake.key("transactions", commit_file),
                PutPayload::from(bytes),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await;
        assert!(
            matches!(late, Err(object_store_delta::Error::AlreadyExists { .. })),
            "{name}: {late:?}"
        );
        let reopened = open(&lake, 'j', false).await.unwrap();
        for table in TABLES {
            assert_eq!(reopened.version(table).unwrap(), 1, "{name} {table}");
            assert_eq!(active(&reopened.tables[table].table).1.len(), 1, "{name}");
            assert_eq!(reopened.txn_version(table).await.unwrap(), Some(2));
        }
        // Every log holds it now: nothing is left to roll forward.
        assert!(reopened
            .check_progress(0, Some(&pending), 2)
            .await
            .unwrap()
            .is_empty());
        reopened.check_progress(2, None, 2).await.unwrap();
        let ahead = reopened.check_progress(1, None, 2).await.unwrap_err();
        assert!(
            format!("{ahead:#}").contains("is ahead of this dataset's authority"),
            "{name}: {ahead:#}"
        );
    }
}
