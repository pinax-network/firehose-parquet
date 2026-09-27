//! `FIREPARQ_TEST_MERGE_CRASH_AT` and `FIREPARQ_TEST_ROLLUP_CRASH_AT` abort the real
//! binary at a named maintenance step, for crash-recovery tests. Like
//! `FIREPARQ_DEBUG_FAULT`, only debug builds (as built by `cargo test`) honor them. A
//! release binary ignores both and completes; `cargo test --release` checks that side.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// Whether the binary under test honors the crash hooks.
const HOOKS: bool = cfg!(debug_assertions);

fn write_part(path: &Path, start: u64, rows: u64) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_number",
        DataType::UInt64,
        false,
    )]));
    let column = UInt64Array::from_iter_values(start..start + rows);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column)]).unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn files_named(root: &Path, matches: &dyn Fn(&str) -> bool, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files_named(&path, matches, found);
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(matches)
        {
            found.push(path);
        }
    }
}

fn parquet_files(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    files_named(root, &|name| name.ends_with(".parquet"), &mut found);
    found
}

fn journals(root: &Path, journal: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    files_named(root, &|name| name == journal, &mut found);
    found
}

fn rows(root: &Path) -> i64 {
    parquet_files(root)
        .iter()
        .map(|path| {
            let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
            reader.metadata().file_metadata().num_rows()
        })
        .sum()
}

/// Runs the binary with an empty environment (no S3 bucket or AWS settings), from a
/// directory outside the repository, plus the optional crash hook.
fn fireparq(cwd: &Path, args: &[&str], hook: Option<(&str, &str)>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fireparq"));
    command.env_clear().current_dir(cwd).args(args);
    if let Some((name, step)) = hook {
        command.env(name, step);
    }
    command.output().unwrap()
}

fn logs(output: &Output) -> String {
    format!(
        "status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Runs `args` once with `hook` set. Debug builds must abort at the step and leave the
/// journal for the next run to recover; release builds must ignore the hook and finish.
/// Either way, a plain run afterwards leaves no journal behind.
fn assert_hook_follows_build(cwd: &Path, args: &[&str], hook: (&str, &str), journal_root: &Path) {
    let journal = if hook.0.contains("ROLLUP") {
        "_fireparq_rollup.json"
    } else {
        "_fireparq_merge.json"
    };
    let hooked = fireparq(cwd, args, Some(hook));
    let aborted = String::from_utf8_lossy(&hooked.stderr).contains(&format!(
        "{}={}: aborting to simulate a crash",
        hook.0, hook.1
    ));
    if HOOKS {
        assert!(!hooked.status.success(), "{}", logs(&hooked));
        assert!(aborted, "{}", logs(&hooked));
        assert_eq!(
            journals(journal_root, journal).len(),
            1,
            "{}",
            logs(&hooked)
        );
    } else {
        assert!(hooked.status.success(), "{}", logs(&hooked));
        assert!(!aborted, "{}", logs(&hooked));
        assert!(journals(journal_root, journal).is_empty());
    }
    let plain = fireparq(cwd, args, None);
    assert!(plain.status.success(), "{}", logs(&plain));
    assert!(
        journals(journal_root, journal).is_empty(),
        "{}",
        logs(&plain)
    );
}

#[test]
fn merge_crash_hook_is_honored_only_by_debug_builds() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("data");
    let partition = data.join("blocks/year=2024/month=01/day=15");
    write_part(&partition.join("part-000001.parquet"), 0, 10);
    write_part(&partition.join("part-000002.parquet"), 10, 20);

    let args = ["merge", data.to_str().unwrap()];
    let hook = ("FIREPARQ_TEST_MERGE_CRASH_AT", "after-outputs");
    assert_hook_follows_build(temp.path(), &args, hook, &data);
    assert_eq!(parquet_files(&data).len(), 1);
    assert_eq!(rows(&data), 30);
}

#[test]
fn rollup_crash_hook_is_honored_only_by_debug_builds() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("data");
    let out = temp.path().join("out");
    let day = data.join("blocks/year=2024/month=01/day=15/hour=00");
    write_part(&day.join("minute=00/part-000001.parquet"), 0, 10);
    write_part(&day.join("minute=01/part-000001.parquet"), 10, 20);

    let args = [
        "rollup",
        data.to_str().unwrap(),
        "-o",
        out.to_str().unwrap(),
        "-p",
        "date",
    ];
    let hook = ("FIREPARQ_TEST_ROLLUP_CRASH_AT", "after-outputs");
    assert_hook_follows_build(temp.path(), &args, hook, &out);
    assert_eq!(parquet_files(&out).len(), 1);
    assert_eq!(rows(&out), 30);
    assert_eq!(rows(&data), 30, "copy mode keeps the sources");
}
