//! The real `fireparq-maintenance` binary, with a cleared environment: its
//! settings, exit statuses and JSON lines. The maintenance itself, beside a
//! real `fireparq build` on local disk and loopback S3, is
//! `blocks/tests/delta_maintenance.rs`, which runs this binary too.
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;

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
        ("no tasks", vec![("OPTIMIZE_MAX_CONCURRENT_TASKS", "0")]),
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
