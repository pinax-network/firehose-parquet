//! The real `fireparq-maintenance` binary, with a cleared environment: its
//! settings, exit statuses and JSON lines, and a full VACUUM beside a table's
//! Iceberg metadata. The maintenance itself, beside a real `fireparq build` on
//! local disk and loopback S3, is `blocks/tests/delta_maintenance.rs`, which
//! runs this binary too.
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime};

struct Run {
    status: i32,
    lines: Vec<Value>,
    stderr: String,
}

impl std::fmt::Debug for Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "exit {}: {:?} {}", self.status, self.lines, self.stderr)
    }
}

impl Run {
    fn event(&self, event: &str) -> Vec<&Value> {
        self.lines
            .iter()
            .filter(|line| line["event"] == event)
            .collect()
    }

    fn done(&self) -> &Value {
        self.event("done")
            .first()
            .copied()
            .unwrap_or_else(|| panic!("no done line: {self:?}"))
    }
}

/// The job over the local lake `root` with `LAKE_TABLES=blocks`, then
/// `settings` (which may override both).
fn job(root: &Path, settings: &[(&str, &str)]) -> Run {
    let output = Command::new(env!("CARGO_BIN_EXE_fireparq-maintenance"))
        .env_clear()
        .env("LAKE_ROOT", root)
        .env("LAKE_TABLES", "blocks")
        .envs(settings.iter().copied())
        .output()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    Run {
        status: output.status.code().unwrap_or(-1),
        lines: stdout
            .lines()
            .map(|line| serde_json::from_str(line).unwrap_or_else(|e| panic!("{e}: {line}")))
            .collect(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

const S3: [(&str, &str); 4] = [
    ("LAKE_ROOT", "s3://delta-lake/nothing-here"),
    // Nothing listens on the discard port: every request is refused.
    ("S3_ENDPOINT", "https://127.0.0.1:9"),
    ("AWS_ACCESS_KEY_ID", "loopback-access-key"),
    ("AWS_SECRET_ACCESS_KEY", SECRET),
];
const SECRET: &str = "sekrit-loopback-secret-key";

/// Settings that would make the job unsafe or incomplete are refused before
/// any request: exit 2 and a single `config_error` line.
#[test]
fn configuration_errors_exit_2_before_any_request() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake");
    let on_s3 = |extra: &[(&'static str, &'static str)]| -> Vec<(&'static str, &'static str)> {
        S3.iter().copied().chain(extra.iter().copied()).collect()
    };
    let cases: Vec<(&str, Vec<(&str, &str)>)> = vec![
        ("no root", vec![("LAKE_ROOT", "")]),
        ("two roots", vec![("LAKE_BUCKET", "ethereum-mainnet")]),
        ("another scheme", vec![("LAKE_ROOT", "gs://bucket/lake")]),
        ("no bucket", vec![("LAKE_ROOT", "s3://")]),
        ("no tables", vec![("LAKE_TABLES", " , ")]),
        ("a path as a table", vec![("LAKE_TABLES", "blocks/../x")]),
        (
            "full VACUUM below 168 h",
            vec![("FULL_VACUUM", "1"), ("VACUUM_RETENTION_HOURS", "167")],
        ),
        ("an unknown flag value", vec![("FULL_VACUUM", "maybe")]),
        (
            "a negative retention",
            vec![("VACUUM_RETENTION_HOURS", "-1")],
        ),
        ("an unknown date scope", vec![("OPTIMIZE_DATES", "open")]),
        ("a zero target size", vec![("OPTIMIZE_TARGET_SIZE", "0")]),
        ("a zstd level above 22", vec![("OPTIMIZE_ZSTD_LEVEL", "23")]),
        (
            "a negative repair count",
            vec![("OPTIMIZE_REPAIR_DATES", "-1")],
        ),
        (
            "a repair window below 1 MiB",
            vec![("OPTIMIZE_REPAIR_WINDOW_BYTES", "1000")],
        ),
        (
            "S3 without credentials",
            on_s3(&[("AWS_SECRET_ACCESS_KEY", "")]),
        ),
        (
            "unsafe renames on S3",
            on_s3(&[("AWS_S3_ALLOW_UNSAFE_RENAME", "true")]),
        ),
        (
            "an unknown HTTP flag",
            on_s3(&[("AWS_ALLOW_HTTP", "sometimes")]),
        ),
    ];
    for (case, settings) in cases {
        let run = job(&root, &settings);
        assert_eq!(run.status, 2, "{case}: {run:?}");
        assert_eq!(run.lines.len(), 1, "{case}: {run:?}");
        assert_eq!(run.lines[0]["event"], "config_error", "{case}: {run:?}");
    }
    assert!(!root.exists(), "nothing was written");
}

/// A store error fails the table (exit 1), and the credentials appear
/// nowhere in the output, not even in the store's error.
#[test]
fn store_errors_fail_the_table_and_never_print_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let run = job(dir.path(), &S3);
    assert_eq!(run.status, 1, "{run:?}");
    assert_eq!(run.event("blocks_error").len(), 1, "{run:?}");
    assert_eq!(run.done()["failed"], json!(["blocks"]), "{run:?}");
    assert_eq!(run.done()["skipped"], json!([]), "{run:?}");
    let start = run.event("start")[0];
    assert_eq!(start["root"], "s3://delta-lake/nothing-here");
    assert_eq!(start["deltalake"], "1.0.0");
    let output = format!("{run:?}");
    assert!(!output.contains(SECRET), "{output}");
    assert!(!output.contains("loopback-access-key"), "{output}");
}

/// #680: a table the writer has not created yet is skipped, not failed: no
/// table directory, or a `_delta_log/` without a commit. A table whose log
/// cannot be read otherwise still fails.
#[test]
fn missing_tables_are_skipped_and_other_open_errors_fail() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake");
    let tables = [("LAKE_TABLES", "blocks,transactions,logs")];

    // No dataset root at all yet: every table is skipped.
    let run = job(&root, &tables);
    assert_eq!(run.status, 0, "{run:?}");
    let skipped: Vec<&Value> = run
        .event("skipped")
        .iter()
        .map(|line| &line["table"])
        .collect();
    assert_eq!(
        skipped,
        [&json!("blocks"), &json!("transactions"), &json!("logs")]
    );
    assert!(run.event("table").is_empty(), "{run:?}");
    assert!(run.event("blocks_error").is_empty(), "{run:?}");
    let done = run.done();
    assert_eq!(done["failed"], json!([]), "{run:?}");
    assert_eq!(done["skipped"], json!(["blocks", "transactions", "logs"]));
    assert_eq!(done["conflicts"], json!(0));
    assert_eq!(done["tables"], json!(3));

    // A table being created: its `_delta_log/` exists, with no commit yet.
    std::fs::create_dir_all(root.join("transactions/_delta_log")).unwrap();
    let run = job(&root, &tables);
    assert_eq!(run.status, 0, "{run:?}");
    assert_eq!(run.event("skipped").len(), 3, "{run:?}");

    // A log that exists but cannot be read is a failure.
    std::fs::create_dir_all(root.join("blocks/_delta_log")).unwrap();
    std::fs::write(
        root.join("blocks/_delta_log/00000000000000000000.json"),
        "not a Delta commit\n",
    )
    .unwrap();
    let run = job(&root, &tables);
    assert_eq!(run.status, 1, "{run:?}");
    assert_eq!(run.event("blocks_error").len(), 1, "{run:?}");
    let blocks = run.event("table");
    assert_eq!(blocks.len(), 1, "{run:?}");
    assert_eq!(blocks[0]["table"], "blocks");
    assert!(
        blocks[0]["errors"][0]
            .as_str()
            .unwrap()
            .starts_with("open: "),
        "{run:?}"
    );
    assert_eq!(run.done()["failed"], json!(["blocks"]), "{run:?}");
    assert_eq!(run.done()["skipped"], json!(["transactions", "logs"]));
}

/// A `blocks` table written by hand: commit 0 with `protocol` and `metaData`
/// (partitioned by `date`, the default 7-day retention), then one `add` of
/// [`COMMITTED_PART`]. Nothing reads a data file's content.
fn one_file_table(table: &Path) {
    let log = table.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let schema = json!({"type": "struct", "fields": [
        {"name": "block_num", "type": "long", "nullable": true, "metadata": {}},
        {"name": "date", "type": "date", "nullable": true, "metadata": {}},
    ]});
    let commits = [
        vec![
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({"metaData": {
                "id": "00000000-0000-4000-8000-000000000643",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(),
                "partitionColumns": ["date"],
                "configuration": {},
                "createdTime": now,
            }}),
            json!({"commitInfo": {"timestamp": now, "operation": "CREATE TABLE"}}),
        ],
        vec![
            json!({"commitInfo": {"timestamp": now, "operation": "WRITE"}}),
            json!({"add": {
                "path": COMMITTED_PART,
                "partitionValues": {"date": "2023-11-14"},
                "size": FILE_BYTES.len(), "modificationTime": now, "dataChange": true,
            }}),
        ],
    ];
    for (version, actions) in commits.iter().enumerate() {
        let lines: String = actions.iter().map(|action| format!("{action}\n")).collect();
        std::fs::write(log.join(format!("{version:020}.json")), lines).unwrap();
    }
}

const COMMITTED_PART: &str = "date=2023-11-14/part-0.parquet";
const FILE_BYTES: &[u8] = b"not read";

/// Writes `path` with a last-modified time `age` ago.
fn file_aged(path: &Path, age: Duration) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, FILE_BYTES).unwrap();
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(SystemTime::now() - age)
        .unwrap();
}

/// A full VACUUM deletes untracked files older than the enforced 168 h, but
/// never the table's `metadata/` directory, where an Apache XTable sync writes
/// the table's Iceberg metadata; it counts the metadata files it kept, and a
/// dry run deletes nothing. It adds no commit to the Delta log, and a lite
/// VACUUM never deletes an untracked file.
#[test]
fn full_vacuum_keeps_the_iceberg_metadata_of_an_xtable_sync() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let table = root.join("blocks");
    one_file_table(&table);
    let eight_days = Duration::from_secs(8 * 86_400);
    // A `%` in a name is percent-encoded in delta-rs's plan: the job deletes
    // the file that the plan names, not one with the encoding in its name.
    let orphans = [
        "date=2023-11-14/part-failed-optimize.parquet",
        "date=2023-11-14/part-100%.parquet",
    ];
    let metadata = [
        "metadata/v1.metadata.json",
        "metadata/snap-1-1-5f3c.avro",
        "metadata/5f3c-m0.avro",
        "metadata/version-hint.text",
    ];
    for path in orphans.iter().chain(&metadata).chain(&[COMMITTED_PART]) {
        file_aged(&table.join(path), eight_days);
    }
    // Younger than the retention: not a candidate, so not counted either.
    let young = [
        "date=2023-11-14/part-uncommitted.parquet",
        "metadata/v2.metadata.json",
    ];
    for path in young {
        file_aged(&table.join(path), Duration::ZERO);
    }
    let kept: Vec<&str> = metadata
        .iter()
        .chain(&young)
        .chain(&[COMMITTED_PART])
        .copied()
        .collect();
    let exist = |paths: &[&str]| -> Vec<bool> {
        paths.iter().map(|path| table.join(path).exists()).collect()
    };
    let table_line = |run: &Run| -> Value {
        assert_eq!(run.status, 0, "{run:?}");
        let tables = run.event("table");
        assert_eq!(tables.len(), 1, "{run:?}");
        assert_eq!(tables[0]["errors"], json!([]), "{run:?}");
        tables[0].clone()
    };

    let full_vacuum = |files_deleted: usize| {
        json!({
            "mode": "full",
            "retention_hours": null,
            "files_deleted": files_deleted,
            "iceberg_metadata_kept": metadata.len(),
        })
    };

    // A lite VACUUM: no tombstone, so nothing is deleted.
    let lite = table_line(&job(&root, &[]));
    let lite_vacuum = json!({"mode": "lite", "retention_hours": null, "files_deleted": 0});
    assert_eq!(lite["vacuum"], lite_vacuum, "{lite}");
    assert_eq!(exist(&orphans), [true, true]);

    // A dry run reports the plan and deletes nothing.
    let dry = table_line(&job(&root, &[("FULL_VACUUM", "1"), ("DRY_RUN", "1")]));
    assert_eq!(dry["vacuum"], full_vacuum(orphans.len()), "{dry}");
    assert_eq!(dry.get("checkpoint_version"), None, "{dry}");
    assert_eq!(exist(&orphans), [true, true]);

    // The weekly full VACUUM deletes the orphans, keeps the metadata, and
    // checkpoints; it adds no commit (no `VACUUM START` or `VACUUM END`).
    let full = table_line(&job(&root, &[("FULL_VACUUM", "1")]));
    assert_eq!(full["vacuum"], full_vacuum(orphans.len()), "{full}");
    assert_eq!(exist(&orphans), [false, false]);
    assert!(exist(&kept).iter().all(|exists| *exists), "{kept:?}");
    assert_eq!(full["version_before"], json!(1), "{full}");
    assert_eq!(full["version_after"], json!(1), "{full}");
    assert_eq!(full["checkpoint_version"], json!(1), "{full}");

    // The next week: the metadata is kept again, and nothing else is left.
    let again = table_line(&job(&root, &[("FULL_VACUUM", "1")]));
    assert_eq!(again["vacuum"], full_vacuum(0), "{again}");
    assert!(exist(&kept).iter().all(|exists| *exists), "{kept:?}");
}
