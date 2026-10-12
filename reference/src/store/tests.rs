//! The Python job's `Store_` tests and the store's own: local roots, the
//! fake clock for the VACUUM hour, and real time for Delta's own clocks
//! (tombstones, checkpoint expiry, VACUUM retention), so nothing depends on
//! the date the tests run on.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use deltalake_core::kernel::transaction::{CommitBuilder, TransactionError};
use deltalake_core::kernel::{Action, DataType, StructField};
use deltalake_core::operations::vacuum::VacuumMode;
use deltalake_core::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use deltalake_core::protocol::DeltaOperation;
use deltalake_core::DeltaTableError;
use serde_json::{json, Value};

use super::*;
use crate::info::InfoClient;
use crate::jobs::tests::{long, no_aws, rows_of, stamp, text};
use crate::jobs::{Options, Run};
use crate::json::Json;
use crate::model::{
    outcome_live_row, outcome_settled_row, probe_row, removal_row, Column, ProbeResult, OUTCOMES,
    OUTCOME_PROBES, PERP_ASSETS, PERP_DEXS, SPOT_PAIRS,
};
use crate::testing::{FakeClock, FakeInfo, MemoryLog, FAKE_START};

/// 2026-10-08T06:05Z, in the VACUUM hour, and 12:05Z, outside it.
const AT_0605: i64 = FAKE_START - 6 * 3_600_000_000 + 300_000_000;
const AT_1205: i64 = FAKE_START + 300_000_000;

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    clock: Arc<FakeClock>,
    log: MemoryLog,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        Self {
            _dir: dir,
            root,
            clock: Arc::new(FakeClock::new()),
            log: MemoryLog::new(),
        }
    }

    fn store(&self) -> Store {
        Store::new(
            Root::Local(self.root.clone()),
            &no_aws(),
            &|_| None,
            "test".into(),
            "test",
            self.log.logger(),
            self.clock.clone(),
        )
        .unwrap()
    }

    /// A store whose tables keep removed files for no time at all.
    fn store_without_retention(&self) -> Store {
        let mut store = self.store();
        store.deleted_file_retention = Some("interval 0 seconds");
        store
    }

    async fn table(&self, store: &Store, spec: &'static TableSpec) -> Table {
        store.open(spec, OpenMode::Write).await.unwrap().unwrap()
    }

    async fn rows(&self, spec: &'static TableSpec) -> Vec<Row> {
        rows_of(&self.store(), spec).await
    }

    fn now(&self) -> i64 {
        self.clock.now_micros()
    }

    /// One probe row per id, each its own commit.
    async fn probes(&self, store: &Store, t: &mut Table, ids: std::ops::Range<i64>) {
        for id in ids {
            let row = probe_row(id, self.now(), ProbeResult::Settled, "x");
            store.append(t, vec![row]).await.unwrap();
        }
    }

    /// The Parquet files under the table's directory but its log.
    fn data_files(&self, table: &str) -> Vec<PathBuf> {
        fn walk(directory: &Path, files: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    if !path.ends_with("_delta_log") {
                        walk(&path, files);
                    }
                } else if path
                    .extension()
                    .is_some_and(|extension| extension == "parquet")
                {
                    files.push(path);
                }
            }
        }
        let mut files = Vec::new();
        walk(&self.root.join(table), &mut files);
        files
    }

    fn log_file(&self, table: &str, name: &str) -> PathBuf {
        self.root.join(table).join("_delta_log").join(name)
    }

    /// Ages every log entry of `table` by eight days, past its 7-day log
    /// retention, as if the table had been idle since.
    fn idle(&self, table: &str) {
        let idle = SystemTime::now() - Duration::from_secs(8 * 24 * 3600);
        for entry in std::fs::read_dir(self.root.join(table).join("_delta_log")).unwrap() {
            let file = std::fs::File::options()
                .write(true)
                .open(entry.unwrap().path())
                .unwrap();
            file.set_modified(idle).unwrap();
        }
    }

    fn events(&self, event: &str) -> Vec<Value> {
        self.log
            .events()
            .into_iter()
            .filter(|line| line["event"] == event)
            .collect()
    }
}

/// A table spec that lives as long as the tests: another release's table.
fn spec(name: &'static str, columns: Vec<Column>) -> &'static TableSpec {
    Box::leak(Box::new(TableSpec {
        name,
        key: OUTCOME_PROBES.key,
        columns: Box::leak(columns.into_boxed_slice()),
    }))
}

/// The probe table as a release before `run_id` created it.
fn older_probes() -> &'static TableSpec {
    spec(OUTCOME_PROBES.name, OUTCOME_PROBES.columns[..3].to_vec())
}

fn checkpoint(version: u64) -> String {
    format!("{version:020}.checkpoint.parquet")
}

/// `_last_checkpoint`'s version.
fn last_checkpoint(path: &Path) -> u64 {
    let text = std::fs::read_to_string(path).unwrap();
    serde_json::from_str::<Value>(&text).unwrap()["version"]
        .as_u64()
        .unwrap()
}

/// A file no version references, older than a retention of 0.
async fn orphan(f: &Fixture, table: &str) {
    std::fs::write(f.root.join(table).join("part-orphan.parquet"), b"orphan").unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
}

fn names(t: &Table) -> Vec<String> {
    let schema = t.delta.snapshot().unwrap().schema();
    schema.fields().map(|field| field.name().clone()).collect()
}

#[test]
fn local_roots_are_normalized_lexically() {
    let f = Fixture::new();
    let path = f.root.join("x/../y/./z/");
    assert_eq!(
        Root::parse(path.to_str().unwrap()).unwrap(),
        Root::Local(f.root.join("y/z"))
    );
    assert_eq!(
        Root::parse(path.to_str().unwrap()).unwrap().display(),
        f.root.join("y/z").display().to_string()
    );
    let relative = Root::parse("reference").unwrap();
    assert_eq!(
        relative,
        Root::Local(std::env::current_dir().unwrap().join("reference"))
    );
    assert_eq!(
        Root::parse("s3://hypercore-ref/").unwrap(),
        Root::S3 {
            bucket: "hypercore-ref".into(),
            prefix: String::new()
        }
    );
    let nested = Root::parse("s3://bucket/a/b/").unwrap();
    assert_eq!(nested.display(), "s3://bucket/a/b");
    assert_eq!(
        Root::parse("s3://hypercore-ref").unwrap().display(),
        "s3://hypercore-ref"
    );
    for bad in ["s3://", "s3:///prefix", ""] {
        assert!(Root::parse(bad).is_err(), "{bad}");
    }
}

#[tokio::test]
async fn tables_are_created_where_the_normalized_root_is() {
    let f = Fixture::new();
    let root = Root::parse(f.root.join("x/../y").to_str().unwrap()).unwrap();
    let store = Store::new(
        root,
        &no_aws(),
        &|_| None,
        "test".into(),
        "test",
        f.log.logger(),
        f.clock.clone(),
    )
    .unwrap();
    store.open(&OUTCOMES, OpenMode::Write).await.unwrap();
    assert!(f.root.join("y/hl_outcomes/_delta_log").is_dir());
    assert!(!f.root.join("x").exists());
}

#[tokio::test]
async fn creates_tables_with_the_pinned_protocol_and_properties() {
    let f = Fixture::new();
    let store = f.store();
    assert!(store
        .open(&OUTCOME_PROBES, OpenMode::Read)
        .await
        .unwrap()
        .is_none());
    let t = f.table(&store, &OUTCOME_PROBES).await;
    assert_eq!(t.version(), 0);
    let snapshot = t.delta.snapshot().unwrap();
    let protocol = snapshot.protocol();
    assert_eq!(
        (protocol.min_reader_version(), protocol.min_writer_version()),
        (1, 2)
    );
    assert_eq!(
        (protocol.reader_features(), protocol.writer_features()),
        (None, None)
    );
    let metadata = snapshot.metadata();
    assert_eq!(metadata.name(), Some("outcome_probes"));
    assert!(metadata.partition_columns().is_empty());
    let expected: HashMap<String, String> = TABLE_PROPERTIES
        .iter()
        .map(|(property, value)| (property.as_ref().to_owned(), (*value).to_owned()))
        .collect();
    assert_eq!(metadata.configuration(), &expected);
    assert_eq!(
        f.events("created"),
        [json!({"event": "created", "table": "_hlref/outcome_probes", "version": 0})]
    );
    // Opened again, it is the same table.
    assert_eq!(f.table(&store, &OUTCOME_PROBES).await.version(), 0);
}

#[tokio::test]
async fn a_stale_snapshot_cannot_commit() {
    let f = Fixture::new();
    let store = f.store();
    let mut stale = f.table(&store, &OUTCOME_PROBES).await;
    let mut fresh = f.table(&f.store(), &OUTCOME_PROBES).await;
    let at = f.now();
    let row = |run_id| probe_row(1, at, ProbeResult::NotSettled, run_id);
    f.store().append(&mut fresh, vec![row("a")]).await.unwrap();
    assert_eq!(
        store.append(&mut stale, vec![row("b")]).await,
        Err(StoreError::Failed(
            "_hlref/outcome_probes: commit after version 0 failed (a concurrent commit)".into()
        ))
    );
    assert_eq!(stale.version(), 0);
    assert_eq!(f.rows(&OUTCOME_PROBES).await.len(), 1);
}

#[tokio::test]
async fn a_foreign_commit_between_two_batches_fails_the_second() {
    let f = Fixture::new();
    let store = f.store();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    f.probes(&store, &mut table, 0..1).await;
    // Another writer commits version 2 on top of this run's version 1.
    let other = f.store();
    let mut foreign = f.table(&other, &OUTCOME_PROBES).await;
    f.probes(&other, &mut foreign, 100..101).await;
    let row = probe_row(1, f.now(), ProbeResult::Settled, "x");
    let error = store.append(&mut table, vec![row]).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "_hlref/outcome_probes: commit after version 1 failed (a concurrent commit)"
    );
    let ids: BTreeSet<i64> = f
        .rows(&OUTCOME_PROBES)
        .await
        .iter()
        .filter_map(|row| long(row, "outcome_id"))
        .collect();
    assert_eq!(ids, BTreeSet::from([0, 100]));
}

#[tokio::test]
async fn tables_are_append_only_and_compact() {
    let f = Fixture::new();
    f.clock.set_wall(AT_0605);
    let store = f.store();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    f.probes(&store, &mut table, 0..5).await;
    let configuration = table
        .delta
        .snapshot()
        .unwrap()
        .metadata()
        .configuration()
        .clone();
    assert_eq!(configuration["delta.appendOnly"], "true");
    let options = MaintainOptions {
        compact_min_files: 5,
        ..MaintainOptions::default()
    };
    let maintained = store.maintain(&mut table, &options).await.unwrap().unwrap();
    assert_eq!(
        (maintained.files_removed, maintained.files_added),
        (Some(5), Some(1))
    );
    // 06:xx UTC is the VACUUM hour; the replaced files are too young.
    assert_eq!(maintained.vacuum_deleted, Some(0));
    assert_eq!(f.rows(&OUTCOME_PROBES).await.len(), 5);
    // Deleting rows is refused: a remove that changes data.
    let file = store.active_files(&table).await.unwrap().remove(0);
    let deleted = CommitBuilder::default()
        .with_actions(vec![Action::Remove(file.remove_action(true))])
        .build(
            Some(table.delta.snapshot().unwrap()),
            table.delta.log_store(),
            DeltaOperation::Delete {
                predicate: Some("outcome_id = 1".into()),
            },
        )
        .await;
    assert!(
        matches!(
            deleted,
            Err(DeltaTableError::Transaction {
                source: TransactionError::DeltaTableAppendOnly
            })
        ),
        "{deleted:?}"
    );
}

#[tokio::test]
async fn vacuum_deletes_replaced_files_after_a_checkpoint_dropped_their_tombstones() {
    // The 168 h retention scaled down to 0: tombstones expire at once. A
    // checkpoint then drops them from the snapshot, which a lite VACUUM needs
    // to find their files (it deletes none of them here).
    let f = Fixture::new();
    f.clock.set_wall(AT_1205);
    let store = f.store_without_retention();
    let sibling = spec("_hlref/sibling", OUTCOME_PROBES.columns.to_vec());
    let mut other = f.table(&store, sibling).await;
    let row = Row::of(
        sibling,
        &["outcome_id", "probed_at", "result", "run_id"],
        &[
            Cell::Long(1),
            Cell::Timestamp(f.now()),
            Cell::Text("settled".into()),
            Cell::Text("x".into()),
        ],
    );
    store.append(&mut other, vec![row]).await.unwrap();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    f.probes(&store, &mut table, 0..25).await;
    let options = MaintainOptions {
        retention_hours: 0,
        ..MaintainOptions::default()
    };
    let compact = MaintainOptions {
        compact_min_files: 25,
        ..options.clone()
    };
    let maintained = store.maintain(&mut table, &compact).await.unwrap().unwrap();
    assert_eq!(
        (maintained.files_removed, maintained.files_added),
        (Some(25), Some(1))
    );
    assert_eq!(maintained.vacuum_deleted, None);
    let optimized = maintained.version;
    // The replaced files' tombstones expire.
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Crosses a checkpoint (one per 20 commits).
    f.probes(&store, &mut table, 25..45).await;
    let name = OUTCOME_PROBES.name;
    let last = last_checkpoint(&f.log_file(name, "_last_checkpoint"));
    assert!(last > optimized, "{last} <= {optimized}");
    let file = std::fs::File::open(f.log_file(name, &checkpoint(last))).unwrap();
    for batch in ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
    {
        let batch = batch.unwrap();
        let removes = batch
            .column_by_name("remove")
            .map_or(0, |column| column.len() - column.null_count());
        assert_eq!(removes, 0, "the checkpoint kept a tombstone");
    }
    // On a freshly loaded handle a lite VACUUM finds none of the replaced
    // files; a full one finds all 25.
    let fresh = store
        .open(&OUTCOME_PROBES, OpenMode::Read)
        .await
        .unwrap()
        .unwrap();
    for (mode, planned) in [(VacuumMode::Lite, 0), (VacuumMode::Full, 25)] {
        let (_, plan) = fresh
            .delta
            .clone()
            .vacuum()
            .with_mode(mode.clone())
            .with_retention_period(chrono::Duration::zero())
            .with_dry_run(true)
            .await
            .unwrap();
        assert_eq!(plan.files_deleted.len(), planned, "{mode:?}");
    }
    assert_eq!(f.data_files(name).len(), 46);
    tokio::time::sleep(Duration::from_millis(50)).await;
    f.clock.set_wall(AT_0605);
    // 21 files: VACUUM only.
    let maintained = store.maintain(&mut table, &options).await.unwrap().unwrap();
    assert_eq!(maintained.vacuum_deleted, Some(25));
    assert_eq!(maintained.files_removed, None);
    assert_eq!(f.data_files(name).len(), 21);
    assert_eq!(store.active_files(&table).await.unwrap().len(), 21);
    assert_eq!(f.rows(&OUTCOME_PROBES).await.len(), 45);
    assert_eq!(f.data_files(sibling.name).len(), 1);
    assert_eq!(f.rows(sibling).await.len(), 1);
}

#[tokio::test]
async fn the_vacuum_history_is_read_only_in_the_vacuum_hour() {
    let f = Fixture::new();
    f.clock.set_wall(AT_1205);
    let store = f.store();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    let options = MaintainOptions::default();
    assert_eq!(store.maintain(&mut table, &options).await.unwrap(), None);
    assert_eq!(store.history_reads.load(Ordering::SeqCst), 0);
    assert!(f.events("maintained").is_empty());
    f.clock.set_wall(AT_0605);
    let maintained = store.maintain(&mut table, &options).await.unwrap().unwrap();
    assert_eq!(maintained.vacuum_deleted, Some(0));
    assert_eq!(store.history_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn schema_drift_is_refused() {
    let f = Fixture::new();
    let store = f.store();
    let log_store = store.delta.log_store(SPOT_PAIRS.name).unwrap();
    CreateBuilder::new()
        .with_log_store(log_store)
        .with_columns([StructField::new("pair_index", DataType::LONG, true)])
        .await
        .unwrap();
    for mode in [OpenMode::Write, OpenMode::Read] {
        assert!(
            matches!(
                store.open(&SPOT_PAIRS, mode).await,
                Err(StoreError::Config(_))
            ),
            "{mode:?}"
        );
    }
}

#[tokio::test]
async fn a_release_appends_nullable_columns() {
    let f = Fixture::new();
    let store = f.store();
    let older = older_probes();
    let mut table = f.table(&store, older).await;
    let columns = ["outcome_id", "probed_at", "result"];
    let row = Row::of(
        older,
        &columns,
        &[
            Cell::Long(1),
            Cell::Timestamp(f.now()),
            Cell::Text("settled".into()),
        ],
    );
    store.append(&mut table, vec![row]).await.unwrap();
    // A reader leaves the older table as it is; the writer adds the column.
    let read = f
        .store()
        .open(&OUTCOME_PROBES, OpenMode::Read)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(names(&read), columns);
    let mut table = f.table(&f.store(), &OUTCOME_PROBES).await;
    let release: Vec<&str> = OUTCOME_PROBES
        .columns
        .iter()
        .map(|column| column.name)
        .collect();
    assert_eq!(names(&table), release);
    assert_eq!(table.version(), 2);
    let configuration = table
        .delta
        .snapshot()
        .unwrap()
        .metadata()
        .configuration()
        .clone();
    assert_eq!(configuration["delta.appendOnly"], "true");
    assert_eq!(
        f.events("columns_added"),
        [
            json!({"event": "columns_added", "table": "_hlref/outcome_probes",
                "columns": ["run_id"], "version": 2})
        ]
    );
    let row = probe_row(2, f.now(), ProbeResult::Settled, "new");
    f.store().append(&mut table, vec![row]).await.unwrap();
    let mut rows: Vec<(Option<i64>, Option<String>)> = f
        .rows(&OUTCOME_PROBES)
        .await
        .iter()
        .map(|row| {
            (
                long(row, "outcome_id"),
                text(row, "run_id").map(str::to_owned),
            )
        })
        .collect();
    rows.sort();
    assert_eq!(rows, [(Some(1), None), (Some(2), Some("new".into()))]);
    // A column anywhere but at the end is drift.
    let moved = spec(
        OUTCOME_PROBES.name,
        [0, 1, 3, 2]
            .iter()
            .map(|index| OUTCOME_PROBES.columns[*index])
            .collect(),
    );
    assert!(matches!(
        f.store().open(moved, OpenMode::Write).await,
        Err(StoreError::Config(_))
    ));
}

#[tokio::test]
async fn latest_is_one_row_per_key() {
    let f = Fixture::new();
    let store = f.store();
    let mut table = f.table(&store, &PERP_ASSETS).await;
    let times: Vec<i64> = (0..3).map(|_| f.now()).collect();
    let row = |dex: i64, asset: i64, at: i64, leverage: i64| {
        Row::of(
            &PERP_ASSETS,
            &[
                "dex_index",
                "asset_index",
                "max_leverage",
                "listed",
                "observed_at",
                "source",
                "payload_sha256",
                "payload_json",
            ],
            &[
                Cell::Long(dex),
                Cell::Long(asset),
                Cell::Long(leverage),
                Cell::Bool(true),
                Cell::Timestamp(at),
                Cell::Text("allPerpMetas".into()),
                Cell::Text(leverage.to_string()),
                Cell::Text("{}".into()),
            ],
        )
    };
    // Out of order across commits, and keys that share each component.
    let first = vec![row(0, 1, times[2], 3), row(1, 0, times[0], 4)];
    store.append(&mut table, first).await.unwrap();
    let second = vec![
        row(0, 1, times[0], 1),
        row(0, 1, times[1], 2),
        row(1, 0, times[1], 5),
        row(1, 1, times[0], 6),
    ];
    store.append(&mut table, second).await.unwrap();
    let latest = store.latest(&table).await.unwrap();
    let leverage: HashMap<Vec<i64>, Option<i64>> = latest
        .values()
        .map(|row| {
            let key = vec![
                long(row, "dex_index").unwrap(),
                long(row, "asset_index").unwrap(),
            ];
            (key, long(row, "max_leverage"))
        })
        .collect();
    assert_eq!(
        leverage,
        HashMap::from([
            (vec![0, 1], Some(3)),
            (vec![1, 0], Some(5)),
            (vec![1, 1], Some(6))
        ])
    );
    assert!(latest
        .values()
        .all(|row| row.get("payload_json") == &Cell::Null));
    let dexs = f.table(&store, &PERP_DEXS).await;
    assert!(store.latest(&dexs).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_checkpoint_follows_every_twentieth_version() {
    let f = Fixture::new();
    f.clock.set_wall(AT_0605);
    let store = f.store_without_retention();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    let name = OUTCOME_PROBES.name;
    f.probes(&store, &mut table, 0..19).await;
    assert_eq!(table.version(), 19);
    assert!(f.log_file(name, &checkpoint(19)).is_file());
    f.probes(&store, &mut table, 19..38).await;
    assert_eq!(table.version(), 38);
    let written: Vec<u64> = (0..=38)
        .filter(|version| f.log_file(name, &checkpoint(*version)).exists())
        .collect();
    assert_eq!(written, [19]);
    // VACUUM START and END take the table from 38 to 40, past 39.
    orphan(&f, name).await;
    let options = MaintainOptions {
        compact_min_files: usize::MAX,
        retention_hours: 0,
        ..MaintainOptions::default()
    };
    let maintained = store.maintain(&mut table, &options).await.unwrap().unwrap();
    assert_eq!(
        (maintained.vacuum_deleted, maintained.version),
        (Some(1), 40)
    );
    assert!(!f.log_file(name, &checkpoint(39)).exists());
    assert!(f.log_file(name, &checkpoint(40)).is_file());
    assert_eq!(last_checkpoint(&f.log_file(name, "_last_checkpoint")), 40);
}

#[tokio::test]
async fn a_checkpoint_failure_keeps_the_commit() {
    let f = Fixture::new();
    let store = f.store();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    f.probes(&store, &mut table, 0..17).await;
    // Version 19's checkpoint cannot be written where a directory is.
    let name = OUTCOME_PROBES.name;
    std::fs::create_dir(f.log_file(name, &checkpoint(19))).unwrap();
    let row = probe_row(17, f.now(), ProbeResult::Settled, "x");
    let appended = store.append(&mut table, vec![row]).await.unwrap().unwrap();
    assert_eq!((appended.version, appended.checkpoint_error), (18, None));
    // Through a run: the rows count, the commit chain goes on, and the
    // checkpoint is a failure of its own.
    let fake = Arc::new(FakeInfo::new());
    let mut info = InfoClient::new(
        fake,
        100_000,
        f.clock.clone(),
        Box::new(|| 0.0),
        f.log.logger(),
    )
    .unwrap();
    let mut run = Run::new(
        &store,
        &mut info,
        Options::default(),
        f.log.logger(),
        "test".into(),
        f.clock.clone(),
    );
    let row = probe_row(18, f.now(), ProbeResult::Settled, "x");
    assert!(run.commit(&mut table, vec![row]).await);
    assert_eq!(table.version(), 19);
    assert_eq!(run.summary[name], json!(1));
    assert_eq!(run.failures.len(), 1, "{:?}", run.failures);
    assert!(
        run.failures[0].starts_with("_hlref/outcome_probes: checkpoint at version 19 failed ("),
        "{}",
        run.failures[0]
    );
    let row = probe_row(19, f.now(), ProbeResult::Settled, "x");
    assert!(run.commit(&mut table, vec![row]).await);
    assert_eq!(f.rows(&OUTCOME_PROBES).await.len(), 20);
}

#[tokio::test]
async fn a_log_cleanup_leaves_the_handle_usable() {
    // A table idle for longer than its 7-day log retention: version 19's
    // checkpoint lets the cleanup delete every earlier log entry, which the
    // handle's snapshot still named. The VACUUM hour's maintenance, a forced
    // one and the reads then go on with the same handle.
    let f = Fixture::new();
    f.clock.set_wall(AT_0605);
    let store = f.store();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    let name = OUTCOME_PROBES.name;
    f.probes(&store, &mut table, 0..18).await;
    f.idle(name);
    let row = probe_row(18, f.now(), ProbeResult::Settled, "x");
    let appended = store.append(&mut table, vec![row]).await.unwrap().unwrap();
    assert_eq!((appended.version, appended.checkpoint_error), (19, None));
    assert!(f.log_file(name, &checkpoint(19)).is_file());
    assert!(!f.log_file(name, &format!("{:020}.json", 18)).exists());

    let maintained = store
        .maintain(&mut table, &MaintainOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (maintained.vacuum_deleted, maintained.version),
        (Some(0), 19)
    );
    let forced = MaintainOptions {
        force: true,
        ..MaintainOptions::default()
    };
    let maintained = store.maintain(&mut table, &forced).await.unwrap().unwrap();
    assert_eq!(
        (
            maintained.files_removed,
            maintained.vacuum_deleted,
            maintained.version
        ),
        (Some(19), Some(0), 20)
    );
    assert_eq!(store.latest(&table).await.unwrap().len(), 19);
}

#[tokio::test]
async fn expired_log_entries_go_after_every_commit() {
    // As delta-rs's hook does, not only after a checkpoint: a table that
    // commits once a day keeps its expired entries until its next commit,
    // not for up to 20 more days. Version 21 is no checkpoint version, and
    // version 19's checkpoint covers the entries before it.
    let f = Fixture::new();
    let store = f.store();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    let name = OUTCOME_PROBES.name;
    f.probes(&store, &mut table, 0..20).await;
    assert_eq!(table.version(), 20);
    assert!(f.log_file(name, &checkpoint(19)).is_file());
    f.idle(name);
    let row = probe_row(20, f.now(), ProbeResult::Settled, "x");
    let appended = store.append(&mut table, vec![row]).await.unwrap().unwrap();
    assert_eq!((appended.version, appended.checkpoint_error), (21, None));
    let commits: Vec<u64> = (0..=21)
        .filter(|version| f.log_file(name, &format!("{version:020}.json")).exists())
        .collect();
    assert_eq!(commits, [19, 20, 21]);
    assert!(f.log_file(name, &checkpoint(19)).is_file());
    assert!(!f.log_file(name, &checkpoint(21)).exists());
    // The handle, reloaded at 21, and a fresh one read every row.
    assert_eq!(store.latest(&table).await.unwrap().len(), 21);
    f.probes(&store, &mut table, 21..22).await;
    assert_eq!(f.rows(&OUTCOME_PROBES).await.len(), 22);
}

#[tokio::test]
async fn a_missing_active_file_fails_the_read() {
    let f = Fixture::new();
    let store = f.store();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    f.probes(&store, &mut table, 0..1).await;
    for file in f.data_files(OUTCOME_PROBES.name) {
        std::fs::remove_file(file).unwrap();
    }
    let error = store
        .latest_with(&table, &["outcome_id", "probed_at"])
        .await;
    assert_eq!(
        error,
        Err(StoreError::Failed(
            "_hlref/outcome_probes: an active data file is gone (a VACUUM after version 1); \
             the next run retries"
                .into()
        ))
    );
}

#[tokio::test]
async fn optimize_of_a_single_small_file_is_a_no_op() {
    let f = Fixture::new();
    f.clock.set_wall(AT_1205);
    let store = f.store();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    f.probes(&store, &mut table, 0..1).await;
    let options = MaintainOptions {
        compact_min_files: 1,
        ..MaintainOptions::default()
    };
    let maintained = store.maintain(&mut table, &options).await.unwrap();
    assert_eq!(
        maintained,
        Some(Maintained {
            files_before: Some(1),
            files_removed: Some(0),
            files_added: Some(0),
            vacuum_deleted: None,
            version: 1,
        })
    );
    assert_eq!(table.version(), 1);
    assert_eq!(
        f.events("maintained"),
        [
            json!({"event": "maintained", "table": "_hlref/outcome_probes", "files_before": 1,
                "files_removed": 0, "files_added": 0, "version": 1})
        ]
    );
}

#[tokio::test]
async fn files_written_before_an_added_column_read_null() {
    let f = Fixture::new();
    f.clock.set_wall(AT_1205);
    let store = f.store();
    let older = older_probes();
    let mut table = f.table(&store, older).await;
    for id in 0..2 {
        let row = Row::of(
            older,
            &["outcome_id", "probed_at", "result"],
            &[
                Cell::Long(id),
                Cell::Timestamp(f.now()),
                Cell::Text("settled".into()),
            ],
        );
        store.append(&mut table, vec![row]).await.unwrap();
    }
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    f.probes(&store, &mut table, 2..3).await;
    let run_ids = |rows: Vec<Row>| {
        let mut run_ids: Vec<(Option<i64>, Option<String>)> = rows
            .iter()
            .map(|row| {
                (
                    long(row, "outcome_id"),
                    text(row, "run_id").map(str::to_owned),
                )
            })
            .collect();
        run_ids.sort();
        run_ids
    };
    let expected = [
        (Some(0), None),
        (Some(1), None),
        (Some(2), Some("x".into())),
    ];
    assert_eq!(run_ids(f.rows(&OUTCOME_PROBES).await), expected);
    // OPTIMIZE NULL-fills the older files' missing column.
    let options = MaintainOptions {
        compact_min_files: 3,
        ..MaintainOptions::default()
    };
    let maintained = store.maintain(&mut table, &options).await.unwrap().unwrap();
    assert_eq!(
        (maintained.files_removed, maintained.files_added),
        (Some(3), Some(1))
    );
    assert_eq!(run_ids(f.rows(&OUTCOME_PROBES).await), expected);
    let files = store.active_files(&table).await.unwrap();
    let stats = stats(&files[0]).unwrap();
    assert_eq!(stats["nullCount"]["run_id"], json!(2));
}

/// Every row of `spec`, sorted by its canonical cells.
async fn all_rows(f: &Fixture, spec: &'static TableSpec) -> Vec<Vec<Cell>> {
    let mut rows: Vec<Vec<Cell>> = f
        .rows(spec)
        .await
        .into_iter()
        .map(|row| row.cells)
        .collect();
    rows.sort_by_key(|cells| format!("{cells:?}"));
    rows
}

#[tokio::test]
async fn optimize_keeps_every_row_and_null_count() {
    let f = Fixture::new();
    f.clock.set_wall(AT_1205);
    let store = f.store();
    let fake = FakeInfo::new();
    let mut table = f.table(&store, &OUTCOMES).await;
    let at = f.now();
    let settled: Vec<Row> = [0, 7768, 9174]
        .iter()
        .map(|id| outcome_settled_row(&fake.edit(|state| state.settled[id].clone()), at))
        .collect();
    let meta = fake.edit(|state| state.outcome_meta.clone());
    let live: Vec<Row> = meta
        .get("outcomes")
        .and_then(Json::as_arr)
        .unwrap()
        .iter()
        .map(|item| outcome_live_row(item, Some(198), at))
        .collect();
    let removal = removal_row(&live[0], at + 1, "outcomeMeta");
    for rows in [settled, live, vec![removal]] {
        store.append(&mut table, rows).await.unwrap();
    }
    let before = all_rows(&f, &OUTCOMES).await;
    let files = store.active_files(&table).await.unwrap();
    assert_eq!(files.len(), 3);
    let sum = |key: &str, column: Option<&str>| -> u64 {
        files
            .iter()
            .map(|file| {
                let stats = stats(file).unwrap();
                let value = match column {
                    Some(column) => &stats[key][column],
                    None => &stats[key],
                };
                value.as_u64().unwrap()
            })
            .sum()
    };
    let rows = sum("numRecords", None);
    let flat: Vec<&str> = OUTCOMES
        .columns
        .iter()
        .filter(|column| {
            !matches!(
                column.ty,
                crate::model::ColumnType::Kv
                    | crate::model::ColumnType::TextList
                    | crate::model::ColumnType::LongList
            )
        })
        .map(|column| column.name)
        .collect();
    let nulls: Vec<u64> = flat
        .iter()
        .map(|column| sum("nullCount", Some(column)))
        .collect();
    let options = MaintainOptions {
        force: true,
        ..MaintainOptions::default()
    };
    let maintained = store.maintain(&mut table, &options).await.unwrap().unwrap();
    assert_eq!(
        (maintained.files_removed, maintained.files_added),
        (Some(3), Some(1))
    );
    let compacted = store.active_files(&table).await.unwrap();
    assert_eq!(compacted.len(), 1);
    let stats = stats(&compacted[0]).unwrap();
    assert_eq!(stats["numRecords"], json!(rows));
    for (column, nulls) in flat.iter().zip(nulls) {
        assert_eq!(stats["nullCount"][column], json!(nulls), "{column}");
    }
    // Map values and list items as they were.
    assert_eq!(all_rows(&f, &OUTCOMES).await, before);
}

#[tokio::test]
async fn commits_carry_the_run_metadata() {
    let f = Fixture::new();
    f.clock.set_wall(AT_1205);
    let store = f.store_without_retention();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    f.probes(&store, &mut table, 0..2).await;
    let compact = MaintainOptions {
        compact_min_files: 2,
        ..MaintainOptions::default()
    };
    store.maintain(&mut table, &compact).await.unwrap();
    // The replaced files' tombstones expire, and the orphan is older too.
    orphan(&f, OUTCOME_PROBES.name).await;
    let options = MaintainOptions {
        force: true,
        retention_hours: 0,
        ..MaintainOptions::default()
    };
    store.maintain(&mut table, &options).await.unwrap();
    let mut history: Vec<_> = table
        .delta
        .history(None)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();
    history.reverse();
    let operations: Vec<(Option<String>, Value)> = history
        .iter()
        .map(|commit| {
            (
                commit.operation.clone(),
                commit.info["hlref.action"].clone(),
            )
        })
        .collect();
    assert_eq!(
        operations,
        [
            (Some("CREATE TABLE".into()), json!("CREATE")),
            (Some("WRITE".into()), json!("APPEND")),
            (Some("WRITE".into()), json!("APPEND")),
            (Some("OPTIMIZE".into()), json!("OPTIMIZE")),
            (Some("VACUUM START".into()), json!("VACUUM")),
            (Some("VACUUM END".into()), json!("VACUUM")),
        ]
    );
    for commit in &history {
        assert_eq!(commit.info["hlref.run_id"], json!("test"));
        assert_eq!(commit.info["hlref.mode"], json!("test"));
        assert_eq!(commit.info["hlref.version"], json!(VERSION));
    }
    // The two replaced files and the orphan.
    assert_eq!(
        history[4].info["operationMetrics"]["numFilesToDelete"],
        json!(3)
    );
    assert_eq!(
        history[5].info["operationMetrics"]["numDeletedFiles"],
        json!(3)
    );
}

#[tokio::test]
async fn every_handle_sees_every_row() {
    let f = Fixture::new();
    let store = f.store_without_retention();
    let older = older_probes();
    let mut table = f.table(&store, older).await;
    let row = Row::of(
        older,
        &["outcome_id", "probed_at", "result"],
        &[
            Cell::Long(0),
            Cell::Timestamp(f.now()),
            Cell::Text("settled".into()),
        ],
    );
    store.append(&mut table, vec![row]).await.unwrap();
    // The handle a commit returned.
    assert_eq!(count(&store, &table).await, 1);
    // The handle adding columns returned.
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    assert_eq!(count(&store, &table).await, 1);
    f.probes(&store, &mut table, 1..2).await;
    assert_eq!(count(&store, &table).await, 2);
    // The handle VACUUM END returned.
    orphan(&f, OUTCOME_PROBES.name).await;
    f.clock.set_wall(AT_0605);
    let options = MaintainOptions {
        compact_min_files: usize::MAX,
        retention_hours: 0,
        ..MaintainOptions::default()
    };
    let maintained = store.maintain(&mut table, &options).await.unwrap().unwrap();
    assert_eq!(maintained.vacuum_deleted, Some(1));
    assert_eq!(count(&store, &table).await, 2);
    let latest = store
        .latest_with(&table, &["outcome_id", "probed_at"])
        .await;
    assert_eq!(latest.unwrap().len(), 2);
}

/// The rows the table's handle reads.
async fn count(store: &Store, t: &Table) -> usize {
    let mut rows = 0;
    store
        .for_each_row(t, &["outcome_id"], None, &mut |_| rows += 1)
        .await
        .unwrap();
    rows
}

/// The probes a read pruned at `greater_than` passes on.
async fn pruned(store: &Store, t: &Table, greater_than: i64) -> usize {
    let mut probes = 0;
    let prune = Prune {
        column: "probed_at",
        greater_than,
    };
    store
        .for_each_row(t, &["outcome_id", "probed_at"], Some(prune), &mut |_| {
            probes += 1
        })
        .await
        .unwrap();
    probes
}

#[tokio::test]
async fn probe_pruning_is_safe_at_millisecond_stats() {
    let f = Fixture::new();
    let store = f.store();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    // Half a millisecond past a millisecond: the statistics keep the
    // millisecond.
    let probed = 1_791_460_800_123_500;
    let row = probe_row(7, probed, ProbeResult::NotSettled, "x");
    store.append(&mut table, vec![row]).await.unwrap();
    let files = store.active_files(&table).await.unwrap();
    assert_eq!(
        stats(&files[0]).unwrap()["maxValues"]["probed_at"],
        json!("2026-10-08T12:00:00.123Z")
    );
    // A cutoff 1 µs before the probe, in its millisecond: the file is read
    // and the probe still cools the id down.
    assert_eq!(pruned(&store, &table, probed - 1).await, 1);
    // At the probe itself or later in its millisecond, the statistics cannot
    // tell: the file is read, and the caller's filter decides.
    assert_eq!(pruned(&store, &table, probed).await, 1);
    assert_eq!(pruned(&store, &table, 1_791_460_800_123_998).await, 1);
    // From the millisecond's last µs on, the statistics prove it.
    assert_eq!(pruned(&store, &table, 1_791_460_800_123_999).await, 0);
    // Without statistics nothing is skipped.
    assert!(!Prune {
        column: "result",
        greater_than: i64::MAX
    }
    .skips(&files[0]));
    let rows = f.rows(&OUTCOME_PROBES).await;
    assert_eq!(stamp(&rows[0], "probed_at"), probed);
}

#[tokio::test]
async fn vacuum_commits_start_and_end_without_hooks() {
    let f = Fixture::new();
    f.clock.set_wall(AT_0605);
    let store = f.store_without_retention();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    f.probes(&store, &mut table, 0..1).await;
    orphan(&f, OUTCOME_PROBES.name).await;
    // Another writer commits after this handle's version: the VACUUM it
    // plans from that version would take the other writer's new file for an
    // orphan, and its VACUUM START fails, so nothing is deleted.
    let other = f.store();
    let mut foreign = f.table(&other, &OUTCOME_PROBES).await;
    f.probes(&other, &mut foreign, 1..2).await;
    let files: BTreeSet<PathBuf> = f.data_files(OUTCOME_PROBES.name).into_iter().collect();
    assert_eq!(files.len(), 3);
    // VACUUM only, in its hour.
    let options = MaintainOptions {
        compact_min_files: usize::MAX,
        retention_hours: 0,
        ..MaintainOptions::default()
    };
    assert_eq!(
        store.maintain(&mut table, &options).await,
        Err(StoreError::Failed(
            "_hlref/outcome_probes: VACUUM failed (MaxCommitAttempts)".into()
        ))
    );
    let left: BTreeSet<PathBuf> = f.data_files(OUTCOME_PROBES.name).into_iter().collect();
    assert_eq!(left, files);
    assert_eq!(table.version(), 1);
    // From the latest version it deletes the orphan, in two commits.
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    let maintained = store.maintain(&mut table, &options).await.unwrap().unwrap();
    assert_eq!(
        (maintained.vacuum_deleted, maintained.version),
        (Some(1), 4)
    );
    assert_eq!(f.data_files(OUTCOME_PROBES.name).len(), 2);
    assert_eq!(f.rows(&OUTCOME_PROBES).await.len(), 2);
    // No hook ran: versions 3 and 4 are not checkpoint versions.
    let checkpoints = std::fs::read_dir(f.root.join(OUTCOME_PROBES.name).join("_delta_log"))
        .unwrap()
        .filter(|entry| {
            let name = entry.as_ref().unwrap().file_name();
            name.to_string_lossy().contains("checkpoint")
        })
        .count();
    assert_eq!(checkpoints, 0);
}

#[tokio::test]
async fn a_forced_vacuum_keeps_files_younger_than_168_hours() {
    let f = Fixture::new();
    let store = f.store();
    let mut table = f.table(&store, &OUTCOME_PROBES).await;
    f.probes(&store, &mut table, 0..3).await;
    orphan(&f, OUTCOME_PROBES.name).await;
    let options = MaintainOptions {
        force: true,
        ..MaintainOptions::default()
    };
    let maintained = store.maintain(&mut table, &options).await.unwrap().unwrap();
    assert_eq!(
        (
            maintained.files_removed,
            maintained.files_added,
            maintained.vacuum_deleted
        ),
        (Some(3), Some(1), Some(0))
    );
    // Three replaced, one compacted, and the orphan.
    assert_eq!(f.data_files(OUTCOME_PROBES.name).len(), 5);
    assert_eq!(f.rows(&OUTCOME_PROBES).await.len(), 3);
}

#[test]
fn s3_roots_refuse_unsafe_settings() {
    let f = Fixture::new();
    let root = Root::parse("s3://hypercore-ref").unwrap();
    let aws = |endpoint: Option<&str>| firehose_parquet::s3::AwsConfig {
        aws_access_key_id: Some("id".into()),
        aws_secret_access_key: Some("secret".into()),
        aws_session_token: None,
        aws_region: None,
        aws_endpoint_url: endpoint.map(str::to_owned),
    };
    let open = |aws: &firehose_parquet::s3::AwsConfig, env: &[(&str, &str)]| {
        let env: HashMap<String, String> = env
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        let lookup = move |name: &str| env.get(name).cloned();
        Store::new(
            root.clone(),
            aws,
            &lookup,
            "test".into(),
            "test",
            f.log.logger(),
            f.clock.clone(),
        )
        .map(|_| ())
        .map_err(|error| error.0)
    };
    let endpoint = "https://datasets-rgw.example";
    assert_eq!(open(&aws(Some(endpoint)), &[]), Ok(()));
    assert_eq!(open(&aws(None), &[]), Ok(()));
    for value in ["1", "true", "TRUE"] {
        assert_eq!(
            open(
                &aws(Some(endpoint)),
                &[("AWS_S3_ALLOW_UNSAFE_RENAME", value)]
            ),
            Err("AWS_S3_ALLOW_UNSAFE_RENAME is refused: commits must be conditional puts".into())
        );
    }
    assert_eq!(
        open(
            &aws(Some(endpoint)),
            &[("AWS_S3_ALLOW_UNSAFE_RENAME", "false")]
        ),
        Ok(())
    );
    assert_eq!(
        open(&aws(Some("http://datasets-rgw.example")), &[]),
        Err("AWS_ENDPOINT_URL_S3 must be an https:// URL".into())
    );
    let mut keyless = aws(Some(endpoint));
    keyless.aws_secret_access_key = None;
    assert_eq!(
        open(&keyless, &[]),
        Err("an s3:// root needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY".into())
    );
}
