//! Helpers shared by the tests that read fireparq's Delta tables with the
//! target engines (#643): `engine_compat.rs`, `delta_tables.rs`,
//! `delta_maintenance.rs`, `delta_recovery.rs`, `delta_readers.rs` and
//! `non_final_stream.rs` (the README live view).
//!
//! Readers and tools:
//!
//! - delta-rs (`deltalake-core` 1.0.0, with `firehose-parquet`'s features):
//!   table snapshots ([`delta_read`]: version, active files and partition
//!   values, `txn`, the Arrow schema), partition pruning, and the rows of the
//!   active files ([`delta_batches`]), the way Polars' `scan_delta` reads: the
//!   file list from delta-rs, then the Parquet files. Always available;
//! - the maintenance job, the `fireparq-maintenance` binary
//!   ([`maintenance_bin`]): `FIREPARQ_MAINTENANCE`, else next to the
//!   `fireparq` binary under test (`cargo test --workspace` and
//!   `cargo build -p fireparq-maintenance` build it there). Optional locally,
//!   required by `FIREPARQ_REQUIRE_MAINTENANCE` (CI). It is the only OPTIMIZE:
//!   these tests link no DataFusion, so the writer under test is built with
//!   exactly its release features;
//! - the DuckDB CLI from `FIREPARQ_DUCKDB` (else `duckdb` on `PATH`), optional
//!   locally and required by `FIREPARQ_REQUIRE_DUCKDB` (CI). Its `delta`
//!   extension is loaded from `FIREPARQ_DUCKDB_EXTENSION_DIR` (else a
//!   directory in the test's temp dir) and installed there with
//!   `INSTALL delta` when it is missing. CI pre-installs a checksum-verified
//!   copy and sets `FIREPARQ_DUCKDB_DELTA_VERSION`, which the loaded
//!   extension must report.
//!
//! Every process runs with a cleared environment.
#![allow(dead_code)]

use arrow::array::{Array, Decimal128Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
use deltalake_core::checkpoints::create_checkpoint;
use deltalake_core::operations::vacuum::VacuumMode;
use deltalake_core::{DeltaTable, FilterOp, FilterValue};
use firehose_parquet::delta::store::DeltaStore;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

/// The DuckDB CLI, or `None` locally when it is missing.
pub fn duckdb_cli() -> Option<PathBuf> {
    let candidate = std::env::var_os("FIREPARQ_DUCKDB")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::split_paths(&std::env::var_os("PATH")?)
                .map(|directory| directory.join("duckdb"))
                .find(|path| path.is_file())
        })
        .unwrap_or_else(|| PathBuf::from("duckdb"));
    let required = std::env::var_os("FIREPARQ_REQUIRE_DUCKDB").is_some();
    let version = Command::new(&candidate)
        .env_clear()
        .arg("-version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).into_owned());
    let Some(version) = version else {
        assert!(
            !required,
            "FIREPARQ_REQUIRE_DUCKDB is set but the DuckDB CLI {candidate:?} is unavailable"
        );
        eprintln!("skipping the DuckDB check: no DuckDB CLI ({candidate:?})");
        return None;
    };
    // The checks read Delta tables with S3 checkpoints and `EXPLAIN ANALYZE`
    // file counts, which need DuckDB 1.5 (CI pins 1.5.5). An older local CLI
    // is skipped rather than failed; a required one must be new enough.
    if duckdb_version(&version) < Some((1, 5)) {
        assert!(
            !required,
            "FIREPARQ_REQUIRE_DUCKDB is set but {candidate:?} is {}; 1.5 or later is required",
            version.trim()
        );
        eprintln!(
            "skipping the DuckDB check: {candidate:?} is {}, older than 1.5",
            version.trim()
        );
        return None;
    }
    Some(candidate)
}

/// `(major, minor)` from `duckdb -version` output such as `v1.5.5 3b7c56d…`.
fn duckdb_version(output: &str) -> Option<(u32, u32)> {
    let mut parts = output.trim().trim_start_matches('v').split(['.', ' ']);
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

/// A JSON number or numeric string (DuckDB versions differ) as `u64`.
pub fn number(value: &Value) -> u64 {
    match value {
        Value::Number(number) => number.as_u64().unwrap(),
        Value::String(text) => text.parse().unwrap(),
        other => panic!("not a number: {other}"),
    }
}

/// The DuckDB CLI with its `delta` extension loaded in every session.
pub struct DuckDb {
    cli: PathBuf,
    cwd: PathBuf,
    extensions: PathBuf,
    /// DuckDB 1.5 refuses to install or load extensions without a home
    /// directory, and every session runs with a cleared environment.
    home: PathBuf,
    pub version: String,
    pub delta_version: String,
}

impl DuckDb {
    /// The DuckDB CLI, if available, with `delta` installed and loadable.
    pub fn open(cwd: &Path) -> Option<Self> {
        let cli = duckdb_cli()?;
        let extensions = std::env::var_os("FIREPARQ_DUCKDB_EXTENSION_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| cwd.join("duckdb-extensions"));
        let home = cwd.join("duckdb-home");
        std::fs::create_dir_all(&home).unwrap();
        let mut duckdb = Self {
            cli,
            cwd: cwd.to_path_buf(),
            extensions,
            home,
            version: String::new(),
            delta_version: String::new(),
        };
        duckdb.install_delta();
        let row = &duckdb.query(
            "SELECT 'v' AS q, version() AS duckdb, \
             (SELECT extension_version FROM duckdb_extensions() \
              WHERE extension_name = 'delta') AS delta",
        )["v"][0];
        duckdb.version = row["duckdb"].as_str().unwrap().to_string();
        duckdb.delta_version = row["delta"].as_str().unwrap().to_string();
        if let Ok(pinned) = std::env::var("FIREPARQ_DUCKDB_DELTA_VERSION") {
            assert_eq!(
                duckdb.delta_version, pinned,
                "the loaded DuckDB delta extension is not the pinned one"
            );
        }
        Some(duckdb)
    }

    fn setup(&self) -> String {
        format!(
            "SET home_directory = '{}'; SET extension_directory = '{}';",
            self.home.display(),
            self.extensions.display()
        )
    }

    fn run(&self, sql: &str) -> Output {
        let init = self.cwd.join("empty.duckdbrc");
        std::fs::write(&init, "").unwrap();
        Command::new(&self.cli)
            .env_clear()
            .current_dir(&self.cwd)
            .arg("-init")
            .arg(&init)
            .args(["-json", "-c", sql])
            .output()
            .unwrap()
    }

    /// Loads `delta`, installing it first when it is missing (a no-op for an
    /// extension already installed, from the repository or a file), and
    /// retries transient download errors.
    fn install_delta(&self) {
        let load = format!("{} LOAD delta;", self.setup());
        if self.run(&load).status.success() {
            return;
        }
        assert!(
            std::env::var_os("FIREPARQ_DUCKDB_DELTA_VERSION").is_none(),
            "the pinned DuckDB delta extension is not installed in FIREPARQ_DUCKDB_EXTENSION_DIR"
        );
        let install = format!("{} INSTALL delta; LOAD delta;", self.setup());
        for attempt in 1..=3 {
            let output = self.run(&install);
            if output.status.success() {
                return;
            }
            assert!(
                attempt < 3,
                "cannot install the DuckDB delta extension: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            std::thread::sleep(Duration::from_secs(5));
        }
    }

    /// Runs `sql` after loading `delta` in a fresh in-memory database and
    /// returns the rows of every statement, grouped by their `q` column (each
    /// query tags its rows with a `q` literal; rows without one, such as
    /// `CREATE SECRET`'s `Success`, are dropped).
    pub fn query(&self, sql: &str) -> BTreeMap<String, Vec<Value>> {
        let output = self.run(&format!("{} LOAD delta; {sql}", self.setup()));
        assert!(
            output.status.success(),
            "{sql}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let mut grouped: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for result in serde_json::Deserializer::from_str(&stdout).into_iter::<Value>() {
            let result = result.unwrap_or_else(|error| panic!("{error}: {stdout}"));
            for row in result.as_array().cloned().unwrap_or_default() {
                if let Some(tag) = row["q"].as_str() {
                    grouped.entry(tag.to_string()).or_default().push(row);
                }
            }
        }
        grouped
    }
}

/// The actions of every JSON commit of a local table, by version.
pub fn delta_log(table: &Path) -> BTreeMap<u64, Vec<Value>> {
    let mut commits = BTreeMap::new();
    for entry in std::fs::read_dir(table.join("_delta_log")).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        let Some(version) = name.strip_suffix(".json") else {
            continue;
        };
        let actions = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        commits.insert(version.parse().unwrap(), actions);
    }
    commits
}

/// The first action of `kind` in one commit.
pub fn action<'a>(actions: &'a [Value], kind: &str) -> Option<&'a Value> {
    actions.iter().find_map(|action| action.get(kind))
}

/// The Delta tables directly below a local dataset root, by name.
pub fn delta_tables(root: &Path) -> Vec<String> {
    let mut tables: Vec<String> = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.join("_delta_log").is_dir())
        .map(|path| path.file_name().unwrap().to_str().unwrap().to_string())
        .collect();
    tables.sort();
    tables
}

/// Per table, the rows (`count(*)`) and distinct `block_num`s DuckDB's
/// `delta_scan` reads, in one session.
pub fn duckdb_counts(duckdb: &DuckDb, root: &Path, tables: &[String]) -> BTreeMap<String, u64> {
    let sql: String = tables
        .iter()
        .map(|table| {
            format!(
                "SELECT '{table}' AS q, count(*) AS n FROM delta_scan('{}/{table}');",
                root.display()
            )
        })
        .collect();
    let rows = duckdb.query(&sql);
    tables
        .iter()
        .map(|table| (table.clone(), number(&rows[table][0]["n"])))
        .collect()
}

/// Opens `table` of `store` at its latest version, through the writer's own
/// log store.
pub async fn open_table(store: &DeltaStore, table: &str) -> DeltaTable {
    let log_store = store.log_store(table).unwrap();
    firehose_parquet::delta::open_table(log_store)
        .await
        .unwrap_or_else(|error| panic!("opening {table}: {error}"))
}

/// Opens `table` of the local dataset `root`.
pub async fn open_local(root: &Path, table: &str) -> DeltaTable {
    open_table(&DeltaStore::local(root).unwrap(), table).await
}

/// One active file of a snapshot.
#[derive(Clone, Debug)]
pub struct DeltaFile {
    /// Relative to the table.
    pub path: String,
    pub date: String,
    /// The `add`'s `numRecords`.
    pub rows: Option<u64>,
    pub size: u64,
}

/// What the Delta log of one table holds, from its delta-rs snapshot.
#[derive(Debug)]
pub struct DeltaRead {
    pub version: u64,
    /// The active files, by path.
    pub files: Vec<DeltaFile>,
    /// The version of fireparq's `txn` (`fireparq:<descriptor>`).
    pub txn: Option<i64>,
    /// The Arrow type of every column as delta-rs reads it, `date` included.
    pub types: BTreeMap<String, String>,
}

impl DeltaRead {
    pub fn paths(&self) -> Vec<String> {
        self.files.iter().map(|file| file.path.clone()).collect()
    }

    pub fn files_per_date(&self) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        for file in &self.files {
            *counts.entry(file.date.clone()).or_insert(0) += 1;
        }
        counts
    }
}

pub async fn delta_read(table: &DeltaTable) -> DeltaRead {
    let snapshot = table.snapshot().unwrap();
    let mut files: Vec<DeltaFile> = snapshot
        .log_data()
        .iter()
        .map(|file| DeltaFile {
            path: file.path().to_string(),
            date: file.partition_values_map()["date"].clone().unwrap(),
            rows: file.num_records().map(|rows| rows as u64),
            size: file.size() as u64,
        })
        .collect();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let descriptor = snapshot
        .metadata()
        .configuration()
        .get("fireparq.descriptor")
        .cloned();
    let txn = match descriptor {
        Some(descriptor) => snapshot
            .transaction_version(table.log_store().as_ref(), format!("fireparq:{descriptor}"))
            .await
            .unwrap(),
        None => None,
    };
    let types = snapshot
        .snapshot()
        .arrow_schema()
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().to_string()))
        .collect();
    DeltaRead {
        version: table.version().unwrap() as u64,
        files,
        txn,
        types,
    }
}

/// The active files of `day`, from delta-rs's partition pruning.
pub async fn delta_day_files(table: &DeltaTable, day: &str) -> Vec<String> {
    let filters = [("date", FilterOp::Eq, FilterValue::Scalar(day))];
    let mut files: Vec<String> = table
        .get_files_by_partitions(&filters)
        .await
        .unwrap()
        .iter()
        .map(|path| path.to_string())
        .collect();
    files.sort();
    files
}

/// The rows of every active file of a local table (`table_dir`), with the
/// file's `date`: the log's files only, never a listing.
pub fn delta_batches(table_dir: &Path, read: &DeltaRead) -> Vec<(String, RecordBatch)> {
    read.files
        .iter()
        .flat_map(|file| {
            firehose_parquet::writer::read_parquet(&table_dir.join(&file.path))
                .unwrap_or_else(|error| panic!("{}: {error}", file.path))
                .into_iter()
                .map(|batch| (file.date.clone(), batch))
        })
        .collect()
}

/// Every row's `block_num` of a local table, sorted.
pub fn delta_block_nums(table_dir: &Path, read: &DeltaRead) -> Vec<i64> {
    let mut blocks: Vec<i64> = delta_batches(table_dir, read)
        .iter()
        .flat_map(|(_, batch)| int64(batch, "block_num").values().to_vec())
        .collect();
    blocks.sort();
    blocks
}

/// Per table of the local dataset `root`, the rows of its active files.
pub async fn delta_counts(root: &Path, tables: &[String]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for name in tables {
        let read = delta_read(&open_local(root, name).await).await;
        let rows = delta_batches(&root.join(name), &read)
            .iter()
            .map(|(_, batch)| batch.num_rows() as u64)
            .sum();
        counts.insert(name.clone(), rows);
    }
    counts
}

pub fn int64<'a>(batch: &'a RecordBatch, column: &str) -> &'a Int64Array {
    batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("no {column}"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{column} is not Int64"))
}

/// The minimum of an `Int64` or `decimal(20,0)` column over `batches`, as
/// text.
pub fn minimum(batches: &[(String, RecordBatch)], column: &str) -> Option<String> {
    let mut minimum: Option<i128> = None;
    for (_, batch) in batches {
        let array = batch.column_by_name(column).unwrap();
        let found = if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            arrow::compute::min(values).map(i128::from)
        } else if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
            assert_eq!(values.scale(), 0, "{column}");
            arrow::compute::min(values)
        } else {
            panic!("{column}: {:?}", array.data_type())
        };
        minimum = match (minimum, found) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }
    minimum.map(|value| value.to_string())
}

/// The largest `timestamp` over `batches`, in microseconds.
pub fn max_timestamp_micros(batches: &[(String, RecordBatch)]) -> Option<i64> {
    batches
        .iter()
        .filter_map(|(_, batch)| {
            let values = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp in microseconds");
            arrow::compute::max(values)
        })
        .max()
}

/// Writes a checkpoint of `table` at its version.
pub async fn delta_checkpoint(table: &DeltaTable) {
    create_checkpoint(table, None).await.unwrap();
}

/// A delta-rs VACUUM of `table` with retention 0, not enforced; `full` also
/// deletes untracked files, such as a pending transaction's uncommitted
/// parts (design §4.1). The maintenance job refuses both; only these tests
/// run them. Returns the files it deleted (or would, `dry_run`).
pub async fn delta_vacuum_now(table: DeltaTable, full: bool, dry_run: bool) -> Vec<String> {
    let (_, metrics) = table
        .vacuum()
        .with_retention_period(chrono::Duration::zero())
        .with_enforce_retention_duration(false)
        .with_mode(if full {
            VacuumMode::Full
        } else {
            VacuumMode::Lite
        })
        .with_dry_run(dry_run)
        .await
        .unwrap();
    metrics.files_deleted
}

/// The `fireparq-maintenance` binary, or `None` locally when it is missing.
pub fn maintenance_bin() -> Option<PathBuf> {
    let candidate = std::env::var_os("FIREPARQ_MAINTENANCE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_BIN_EXE_fireparq")).with_file_name("fireparq-maintenance")
        });
    if candidate.is_file() {
        return Some(candidate);
    }
    assert!(
        std::env::var_os("FIREPARQ_REQUIRE_MAINTENANCE").is_none(),
        "FIREPARQ_REQUIRE_MAINTENANCE is set but {candidate:?} is missing: \
         run `cargo build -p fireparq-maintenance` first"
    );
    eprintln!(
        "skipping the maintenance job: {candidate:?} is missing \
         (`cargo build -p fireparq-maintenance`, or set FIREPARQ_MAINTENANCE)"
    );
    None
}

/// One run of the maintenance job.
pub struct JobRun {
    pub settings: Vec<(String, String)>,
    pub status: i32,
    pub lines: Vec<Value>,
    pub stderr: String,
}

impl JobRun {
    pub fn done(&self) -> &Value {
        self.lines
            .iter()
            .find(|line| line["event"] == "done")
            .unwrap_or_else(|| panic!("no done line: {self:?}"))
    }

    pub fn events<'a>(&'a self, event: &'a str) -> impl Iterator<Item = &'a Value> + 'a {
        self.lines.iter().filter(move |line| line["event"] == event)
    }

    pub fn tables(&self) -> impl Iterator<Item = &Value> {
        self.events("table")
    }

    pub fn table(&self, name: &str) -> &Value {
        self.tables()
            .find(|line| line["table"] == name)
            .unwrap_or_else(|| panic!("no {name} line: {self:?}"))
    }

    pub fn has(&self, name: &str, value: &str) -> bool {
        self.settings
            .iter()
            .any(|(key, set)| key == name && set == value)
    }

    /// Asserts a clean run: exit 0, and no table with an error or conflict.
    pub fn assert_clean(&self) {
        assert_eq!(self.status, 0, "{self:?}");
        let done = self.done();
        assert_eq!(done["failed"], serde_json::json!([]), "{self:?}");
        assert_eq!(done["conflicts"], serde_json::json!(0), "{self:?}");
    }
}

impl std::fmt::Debug for JobRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} exit {}: {:?} {}",
            self.settings, self.status, self.lines, self.stderr
        )
    }
}

/// Runs the maintenance job `bin` with a cleared environment and exactly
/// `env`, and parses its JSON lines.
pub async fn maintenance_job(bin: &Path, env: &[(&str, String)]) -> JobRun {
    let mut command = tokio::process::Command::new(bin);
    command
        .kill_on_drop(true)
        .env_clear()
        .envs(env.iter().map(|(key, value)| (*key, value.as_str())));
    let output = tokio::time::timeout(Duration::from_secs(180), command.output())
        .await
        .expect("maintenance timed out")
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    JobRun {
        settings: env
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone()))
            .collect(),
        status: output.status.code().unwrap_or(-1),
        lines: stdout
            .lines()
            .map(|line| serde_json::from_str(line).unwrap_or_else(|e| panic!("{e}: {line}")))
            .collect(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}
