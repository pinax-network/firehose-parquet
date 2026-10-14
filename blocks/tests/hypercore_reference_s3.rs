//! `fireparq hypercore-reference` on S3: the real binary against the
//! in-process HTTPS loopback S3 endpoint (`examples/bench_live_flush/s3.rs`,
//! trusted through `SSL_CERT_FILE`) and the loopback `/info`
//! (`common::info`), with a cleared environment. The port of the Python job's
//! `test_s3_race.py`, by fault injection rather than a held request:
//!
//! - a commit that loses its conditional `PUT` (`If-None-Match: *`, sent once,
//!   never rebased) fails the run, and nothing is appended twice;
//! - a commit whose answer is lost, or whose confirming read fails, may have
//!   landed: the run fails, and the next run's diff appends nothing again;
//! - a data file's failed `PUT` is retried (its name is unique), a commit's
//!   never;
//! - every request stays inside the bucket policy the job's S3 user needs
//!   (docs/hypercore-reference.md, "The S3 user"), checkpoints and log
//!   cleanup included;
//! - no credential reaches the output.
//!
//! The loopback has no multipart uploads, and its 404s and 412s are not a
//! policy's: those are proven against the target store only.
use std::time::Duration;

use firehose_parquet::delta::store::READ_ATTEMPTS;
use fireparq_reference::json::Json;
use fireparq_reference::model::ALL_TABLES;
use serde_json::{json, Value};

mod common;
use common::info::InfoServer;

#[path = "../examples/bench_live_flush/s3.rs"]
#[allow(dead_code)]
mod s3;

const BUCKET: &str = "hypercore-ref";
const KEY_ID: &str = "loopback-access-key";
const SECRET: &str = "sekrit-loopback-secret-key";

struct Run {
    status: i32,
    lines: Vec<Value>,
    stdout: String,
    stderr: String,
}

impl std::fmt::Debug for Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "exit {}: {} {}", self.status, self.stdout, self.stderr)
    }
}

impl Run {
    fn done(&self) -> &Value {
        let done = self
            .lines
            .last()
            .unwrap_or_else(|| panic!("no line: {self:?}"));
        assert_eq!(done["event"], "done", "{self:?}");
        done
    }

    /// The `verified` line of `table`.
    fn verified(&self, table: &str) -> &Value {
        self.lines
            .iter()
            .find(|line| line["event"] == "verified" && line["table"] == table)
            .unwrap_or_else(|| panic!("no verified {table}: {self:?}"))
    }
}

/// The loopback S3 endpoint, the loopback `/info`, and a working directory.
struct Lake {
    _dir: tempfile::TempDir,
    cwd: std::path::PathBuf,
    server: s3::Server,
    info: InfoServer,
}

impl Lake {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().canonicalize().unwrap();
        let server = s3::Server::start(
            &cwd.join("tls"),
            BUCKET,
            s3::Latency {
                base_ms: 0,
                slow_every: 0,
                slow_ms: 0,
            },
        )
        .await
        .unwrap();
        Self {
            _dir: dir,
            cwd,
            server,
            info: InfoServer::start(),
        }
    }

    /// `fireparq hypercore-reference --root s3://hypercore-ref <args>`.
    async fn run(&self, args: &[&str]) -> Run {
        let url = format!("{}/info", self.info.endpoint);
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
        command
            .kill_on_drop(true)
            .current_dir(&self.cwd)
            .env_clear()
            .env("AWS_ACCESS_KEY_ID", KEY_ID)
            .env("AWS_SECRET_ACCESS_KEY", SECRET)
            .env("AWS_REGION", "us-east-1")
            .env("AWS_ENDPOINT_URL_S3", &self.server.endpoint)
            .env("SSL_CERT_FILE", &self.server.ca_file)
            .env("HL_INFO_URL", url)
            .args(["hypercore-reference", "--root", &format!("s3://{BUCKET}")])
            .args(args);
        let output = tokio::time::timeout(Duration::from_secs(120), command.output())
            .await
            .expect("fireparq hypercore-reference timed out")
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        Run {
            status: output.status.code().unwrap_or(-1),
            lines: stdout
                .lines()
                .map(|line| serde_json::from_str(line).unwrap_or_else(|e| panic!("{e}: {line}")))
                .collect(),
            stdout,
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    /// The versions committed to `table`'s log.
    fn versions(&self, table: &str) -> Vec<u64> {
        self.server
            .objects(&format!("{table}/_delta_log/"))
            .iter()
            .filter_map(|(key, _)| commit_version(key))
            .collect()
    }

    /// Changes spot pair 1, so that only hl_spot_pairs gets a row.
    fn change_a_pair(&self) {
        self.info.fake.edit(|state| {
            let canonical = state
                .spot_meta
                .pointer_mut("/universe/1/isCanonical")
                .unwrap();
            *canonical = Json::Bool(*canonical != Json::Bool(true));
        });
    }
}

/// The version of a commit's key, `<table>/_delta_log/<20 digits>.json`.
fn commit_version(key: &str) -> Option<u64> {
    let name = key.rsplit_once("/_delta_log/")?.1.strip_suffix(".json")?;
    (name.len() == 20 && name.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| name.parse().unwrap())
}

fn commit_key(table: &str, version: u64) -> String {
    format!("{table}/_delta_log/{version:020}.json")
}

/// Whether `key` is under one of the eleven tables' `<table>/` prefixes.
fn in_a_table(key: &str) -> bool {
    ALL_TABLES.iter().any(|spec| {
        key.strip_prefix(spec.name)
            .is_some_and(|rest| rest.starts_with('/'))
    })
}

/// Every request is one the job's S3 user is granted: an object request
/// under a table prefix, a listing with a table's `<table>/` prefix, or a
/// bulk delete of keys under them. Commits are conditional; nothing asks for
/// a bucket's location, ACL, policy, versioning or tags, or sends an ACL or
/// tagging header.
fn assert_inside_the_policy(log: &[s3::Entry]) {
    for entry in log {
        let query = entry.query.to_ascii_lowercase();
        for name in ["location", "acl", "policy", "versioning", "tagging"] {
            assert!(
                !query
                    .split('&')
                    .any(|pair| pair.split('=').next() == Some(name)),
                "{entry:?}"
            );
        }
        assert_eq!((&entry.acl, &entry.tagging), (&None, &None), "{entry:?}");
        if entry.key.is_empty() {
            match entry.method.as_str() {
                "GET" => {
                    let prefix = entry.prefix.as_deref().unwrap_or_default();
                    assert!(
                        in_a_table(prefix),
                        "a listing outside the tables: {entry:?}"
                    );
                }
                "POST" => {
                    assert!(entry.query.starts_with("delete"), "{entry:?}");
                    assert!(
                        entry.delete_keys.iter().all(|key| in_a_table(key)),
                        "{entry:?}"
                    );
                }
                _ => panic!("a bucket request: {entry:?}"),
            }
        } else {
            assert!(in_a_table(&entry.key), "{entry:?}");
        }
        if entry.method == "PUT" && commit_version(&entry.key).is_some() {
            assert_eq!(entry.if_none_match.as_deref(), Some("*"), "{entry:?}");
        }
    }
}

/// Python's test_the_conditional_put_decides_a_true_race: another writer
/// took version 2 between this run's read and its commit, so the commit's
/// conditional `PUT` is refused. It is sent once, never rebased onto version
/// 3, and the run fails; a rerun commits version 2 once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_that_loses_the_conditional_put_fails_and_appends_nothing() {
    let lake = Lake::start().await;
    let run = lake.run(&["--no-maintenance", "meta"]).await;
    assert_eq!(run.status, 0, "{run:?}");
    assert_eq!(lake.versions("hl_spot_pairs"), [0, 1]);
    lake.change_a_pair();
    let key = commit_key("hl_spot_pairs", 2);
    lake.server.inject("PUT", &key, s3::Fault::Status(412), 1);
    let run = lake.run(&["--no-maintenance", "meta"]).await;
    assert_eq!(run.status, 1, "{run:?}");
    assert_eq!(
        run.done()["failed"],
        json!(["hl_spot_pairs: commit after version 1 failed (a concurrent commit)"]),
        "{run:?}"
    );
    let puts: Vec<s3::Entry> = lake
        .server
        .log()
        .into_iter()
        .filter(|entry| entry.method == "PUT" && entry.key == key)
        .collect();
    assert_eq!(puts.len(), 1, "{puts:?}");
    assert_eq!(puts[0].if_none_match.as_deref(), Some("*"));
    assert!(lake
        .server
        .log()
        .iter()
        .all(|entry| entry.key != commit_key("hl_spot_pairs", 3)));
    assert_eq!(lake.versions("hl_spot_pairs"), [0, 1]);

    let run = lake.run(&["--no-maintenance", "meta"]).await;
    assert_eq!(run.status, 0, "{run:?}");
    assert_eq!(run.done()["summary"]["hl_spot_pairs"], json!(1), "{run:?}");
    assert_eq!(lake.versions("hl_spot_pairs"), [0, 1, 2]);
}

/// A commit applied whose answer is lost: the run cannot know it landed, so
/// it fails; the next run diffs against the landed version and appends
/// nothing, and `verify` finds no duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_commit_response_is_never_appended_twice() {
    let lake = Lake::start().await;
    assert_eq!(lake.run(&["--no-maintenance", "meta"]).await.status, 0);
    lake.change_a_pair();
    let key = commit_key("hl_spot_pairs", 2);
    lake.server
        .inject("PUT", &key, s3::Fault::ApplyThenReset, 1);
    let run = lake.run(&["--no-maintenance", "meta"]).await;
    assert_eq!(run.status, 1, "{run:?}");
    let failed = run.done()["failed"][0].as_str().unwrap().to_owned();
    assert!(
        failed.starts_with("hl_spot_pairs: commit after version 1 failed (")
            && failed.ends_with("); it may have landed"),
        "{failed}"
    );
    assert_eq!(lake.versions("hl_spot_pairs"), [0, 1, 2]);

    let run = lake.run(&["--no-maintenance", "meta"]).await;
    assert_eq!(run.status, 0, "{run:?}");
    assert_eq!(run.done()["summary"]["hl_spot_pairs"], json!(0), "{run:?}");
    assert_eq!(lake.versions("hl_spot_pairs"), [0, 1, 2]);
    let run = lake.run(&["verify"]).await;
    assert_eq!(run.status, 0, "{run:?}");
    let pairs = run.verified("hl_spot_pairs");
    assert_eq!(pairs["duplicates"], json!(0), "{run:?}");
    assert_eq!(pairs["version"], json!(2), "{run:?}");
}

/// Q26: the commit's `PUT` succeeded, but delta-rs's read of the new version
/// after it failed (every attempt). The run reports that the commit may have
/// landed; the next run appends nothing, and `verify` finds no duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_whose_confirmation_read_fails_reports_an_unknown_outcome() {
    let lake = Lake::start().await;
    assert_eq!(lake.run(&["--no-maintenance", "meta"]).await.status, 0);
    lake.change_a_pair();
    let key = commit_key("hl_spot_pairs", 2);
    lake.server
        .inject("GET", &key, s3::Fault::Status(500), READ_ATTEMPTS);
    let run = lake.run(&["--no-maintenance", "meta"]).await;
    assert_eq!(run.status, 1, "{run:?}");
    let failed = run.done()["failed"][0].as_str().unwrap().to_owned();
    assert!(
        failed.starts_with("hl_spot_pairs: commit after version 1 failed (")
            && failed.ends_with("); it may have landed"),
        "{failed}"
    );
    let faulted = lake
        .server
        .log()
        .iter()
        .filter(|entry| entry.method == "GET" && entry.key == key && entry.fault.is_some())
        .count();
    assert_eq!(faulted, READ_ATTEMPTS as usize);
    assert_eq!(lake.versions("hl_spot_pairs"), [0, 1, 2]);

    let run = lake.run(&["--no-maintenance", "meta"]).await;
    assert_eq!(run.status, 0, "{run:?}");
    assert_eq!(run.done()["summary"]["hl_spot_pairs"], json!(0), "{run:?}");
    let run = lake.run(&["verify"]).await;
    assert_eq!(run.status, 0, "{run:?}");
    assert_eq!(run.verified("hl_spot_pairs")["duplicates"], json!(0));
}

/// A data file's `PUT` that fails is sent again (its name is unique, so a
/// resend cannot duplicate a row), and the run commits once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transient_data_put_failure_is_retried() {
    let lake = Lake::start().await;
    lake.server
        .inject_prefix("PUT", "hl_spot_pairs/part-", s3::Fault::Status(503), 1);
    let run = lake.run(&["--no-maintenance", "meta"]).await;
    assert_eq!(run.status, 0, "{run:?}");
    let puts: Vec<(String, u16)> = lake
        .server
        .log()
        .into_iter()
        .filter(|entry| entry.method == "PUT" && entry.key.starts_with("hl_spot_pairs/part-"))
        .map(|entry| (entry.key, entry.status))
        .collect();
    assert_eq!(puts.len(), 2, "{puts:?}");
    assert_eq!(puts[0].0, puts[1].0);
    assert_eq!((puts[0].1, puts[1].1), (503, 200));
    assert_eq!(lake.versions("hl_spot_pairs"), [0, 1]);
}

/// Every request of meta, outcomes, funding and a forced maintain is one the
/// job's S3 user is granted ([`assert_inside_the_policy`]), the VACUUM's
/// bulk delete of a week-old file no version references included.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_stay_inside_the_bucket_policy() {
    let lake = Lake::start().await;
    assert_eq!(lake.run(&["meta"]).await.status, 0);
    assert_eq!(
        lake.run(&["outcomes", "--sweep-from", "9975"]).await.status,
        0
    );
    lake.info.fake.edit(|state| {
        state.outcome_meta = fireparq_reference::testing::FakeInfo::outcome_meta_t1()
    });
    assert_eq!(
        lake.run(&["outcomes", "--sweep-from", "9975"]).await.status,
        0
    );
    let run = lake.run(&["funding", "--coins", "BTC,xyz:XYZ100"]).await;
    assert_eq!(run.status, 0, "{run:?}");
    // A data file a crashed run left eight days ago: older than the 168 h
    // retention, so the full VACUUM deletes it.
    let orphan = "hl_spot_pairs/part-00000-orphan.zstd.parquet";
    lake.server
        .put(orphan, bytes::Bytes::from_static(b"never committed"));
    assert!(lake
        .server
        .backdate(orphan, Duration::from_secs(8 * 24 * 3600)));
    let run = lake.run(&["maintain"]).await;
    assert_eq!(run.status, 0, "{run:?}");
    assert!(
        run.lines
            .iter()
            .any(|line| line["event"] == "maintained" && line["files_removed"].as_u64() > Some(1)),
        "an OPTIMIZE ran: {run:?}"
    );
    let pairs = run
        .lines
        .iter()
        .find(|line| line["event"] == "maintained" && line["table"] == "hl_spot_pairs")
        .unwrap_or_else(|| panic!("no maintained hl_spot_pairs: {run:?}"));
    assert_eq!(pairs["vacuum_deleted"], json!(1), "{run:?}");
    assert!(lake.server.objects(orphan).is_empty());

    let log = lake.server.log();
    assert!(log.len() > 100, "{} requests", log.len());
    assert_inside_the_policy(&log);
    // The VACUUM's bulk delete, the only one, named the orphan's full key.
    let deleted: Vec<&str> = log
        .iter()
        .filter(|entry| entry.method == "POST")
        .flat_map(|entry| entry.delete_keys.iter().map(String::as_str))
        .collect();
    assert_eq!(deleted, [orphan]);
    // Every table of the four jobs, and nothing beside them.
    let written: std::collections::BTreeSet<&str> = ALL_TABLES
        .iter()
        .map(|spec| spec.name)
        .filter(|name| !lake.server.objects(&format!("{name}/")).is_empty())
        .collect();
    assert_eq!(written.len(), ALL_TABLES.len(), "{written:?}");
    assert!(lake
        .server
        .objects("")
        .iter()
        .all(|(key, _)| in_a_table(key)));
}

/// A checkpoint and the log cleanup after every commit stay inside the bucket
/// policy too. Nineteen runs take `_hlref/outcome_probes` to version 19, whose
/// checkpoint is written; once its log is older than the 7-day retention,
/// the next commit, though no checkpoint is due, deletes the entries the
/// checkpoint covers with one bulk delete. That request goes out once: a
/// failed one fails the run, the commit stands, and the next commit's
/// cleanup deletes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoints_and_log_cleanup_stay_inside_the_bucket_policy() {
    let lake = Lake::start().await;
    let probes = "_hlref/outcome_probes";
    // A named id is probed on every run: one commit each.
    let args = ["--no-maintenance", "outcomes", "--outcome-ids", "7768"];
    for _ in 0..19 {
        let run = lake.run(&args).await;
        assert_eq!(run.status, 0, "{run:?}");
    }
    assert_eq!(lake.versions(probes), (0..=19).collect::<Vec<u64>>());
    let directory = format!("{probes}/_delta_log/");
    let checkpoint = format!("{directory}{:020}.checkpoint.parquet", 19);
    for key in [checkpoint.clone(), format!("{directory}_last_checkpoint")] {
        assert!(
            lake.server
                .log()
                .iter()
                .any(|entry| entry.method == "PUT" && entry.key == key),
            "no PUT of {key}"
        );
    }
    for (key, _) in lake.server.objects(&directory) {
        assert!(lake
            .server
            .backdate(&key, Duration::from_secs(8 * 24 * 3600)));
    }

    lake.server.inject("POST", "", s3::Fault::Status(500), 1);
    let run = lake.run(&args).await;
    assert_eq!(run.status, 1, "{run:?}");
    let failed = run.done()["failed"].as_array().unwrap();
    assert_eq!(failed.len(), 1, "{run:?}");
    assert!(
        failed[0]
            .as_str()
            .unwrap()
            .starts_with("_hlref/outcome_probes: log cleanup at version 20 failed ("),
        "{run:?}"
    );
    assert_eq!(run.done()["summary"][probes], json!(1), "{run:?}");
    assert_eq!(lake.versions(probes), (0..=20).collect::<Vec<u64>>());

    let run = lake.run(&args).await;
    assert_eq!(run.status, 0, "{run:?}");
    assert_eq!(lake.versions(probes), [19, 20, 21]);
    assert!(!lake.server.objects(&checkpoint).is_empty());
    let expired: Vec<String> = (0..19).map(|version| commit_key(probes, version)).collect();
    let deletes: Vec<s3::Entry> = lake
        .server
        .log()
        .into_iter()
        .filter(|entry| entry.method == "POST")
        .collect();
    assert_eq!(
        deletes.iter().map(|entry| entry.status).collect::<Vec<_>>(),
        [500, 200],
        "{deletes:?}"
    );
    for entry in &deletes {
        let mut keys = entry.delete_keys.clone();
        keys.sort();
        assert_eq!(keys, expired, "{entry:?}");
    }
    // A new process reads the table from the checkpoint on.
    let run = lake.run(&args).await;
    assert_eq!(run.status, 0, "{run:?}");
    assert_eq!(lake.versions(probes), [19, 20, 21, 22]);
    assert_inside_the_policy(&lake.server.log());
}

/// A store error on S3 fails the run, and neither credential appears in its
/// output: not in the error, which carries delta-rs's text, nor on stderr.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_errors_never_print_credentials() {
    let lake = Lake::start().await;
    let key = commit_key("hl_spot_meta", 0);
    lake.server.inject("PUT", &key, s3::Fault::Status(500), 1);
    let run = lake.run(&["meta"]).await;
    assert_eq!(run.status, 1, "{run:?}");
    assert_ne!(run.done()["failed"], json!([]), "{run:?}");
    assert_eq!(lake.info.fake.calls(), Vec::<Json>::new(), "no request");
    for text in [&run.stdout, &run.stderr] {
        assert!(!text.contains(SECRET), "{run:?}");
        assert!(!text.contains(KEY_ID), "{run:?}");
    }
}
