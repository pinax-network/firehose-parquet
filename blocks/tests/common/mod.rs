//! Helpers shared by the tests that read fireparq's Delta tables with the
//! target engines (#643): `engine_compat.rs`, `delta_tables.rs`,
//! `delta_maintenance.rs` and `non_final_stream.rs` (the README live view).
//!
//! Engines, all optional locally and required in CI:
//!
//! - the DuckDB CLI from `FIREPARQ_DUCKDB` (else `duckdb` on `PATH`), required
//!   by `FIREPARQ_REQUIRE_DUCKDB`. Its `delta` extension is loaded from
//!   `FIREPARQ_DUCKDB_EXTENSION_DIR` (else a directory in the test's temp
//!   dir) and installed there with `INSTALL delta` when it is missing. CI
//!   pre-installs a checksum-verified copy and sets
//!   `FIREPARQ_DUCKDB_DELTA_VERSION`, which the loaded extension must report;
//! - a Python from `FIREPARQ_POLARS_PYTHON` with `polars` and `deltalake`
//!   (`engines/requirements.txt`), required by `FIREPARQ_REQUIRE_POLARS`.
//!   The same interpreter runs `scripts/delta_maintenance.py`.
//!
//! Every engine process runs with a cleared environment.
#![allow(dead_code)]

use serde_json::{json, Value};
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

/// The Python with Polars and `deltalake`, or `None` locally when missing.
pub fn python() -> Option<PathBuf> {
    let candidate = std::env::var_os("FIREPARQ_POLARS_PYTHON").map(PathBuf::from);
    let available = candidate.as_ref().is_some_and(|python| {
        Command::new(python)
            .args(["-c", "import polars, deltalake"])
            .env_clear()
            .output()
            .is_ok_and(|output| output.status.success())
    });
    if available {
        return candidate;
    }
    assert!(
        std::env::var_os("FIREPARQ_REQUIRE_POLARS").is_none(),
        "FIREPARQ_REQUIRE_POLARS is set but FIREPARQ_POLARS_PYTHON ({candidate:?}) cannot import polars and deltalake"
    );
    eprintln!(
        "skipping the Polars/deltalake check: set FIREPARQ_POLARS_PYTHON to a Python with polars and deltalake"
    );
    None
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

/// Runs `blocks/tests/engines/<script>` with `spec` as its JSON argument and
/// returns the JSON object it prints.
pub fn python_report(python: &Path, script: &str, spec: &Value) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/engines")
        .join(script);
    let output = Command::new(python)
        .env_clear()
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .arg(path)
        .arg(spec.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{script}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{script}: {error}: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
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

/// Per table, the rows Polars' `scan_delta` reads.
pub fn polars_counts(python: &Path, root: &Path, tables: &[String]) -> BTreeMap<String, u64> {
    let report = python_report(
        python,
        "delta_check.py",
        &json!({"root": root, "tables": tables}),
    );
    tables
        .iter()
        .map(|table| (table.clone(), number(&report["tables"][table]["rows"])))
        .collect()
}
