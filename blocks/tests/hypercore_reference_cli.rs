//! `fireparq hypercore-reference`, the real binary with a cleared environment,
//! over a loopback `/info` that answers from the recorded fixtures
//! (`common::info`): its exit statuses, the configuration errors it reports
//! before any request, the `/info` URL and credentials it never prints, and
//! the views of docs/hypercore-reference.md, run unmodified in DuckDB over
//! tables it wrote. The jobs themselves are tested at the library
//! (`reference/`), its S3 traffic in `hypercore_reference_s3.rs`, and the
//! joins with a HyperCore lake in `engine_compat.rs`.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use fireparq_reference::json::Json;
use fireparq_reference::model::REFERENCE_TABLES;
use fireparq_reference::testing::FakeInfo;
use serde_json::{json, Value};

mod common;
use common::info::InfoServer;
use common::{number, section_sql, DuckDb};

const DOC: &str = include_str!("../../docs/hypercore-reference.md");
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
    fn first(&self) -> &Value {
        self.lines
            .first()
            .unwrap_or_else(|| panic!("no line: {self:?}"))
    }

    fn last(&self) -> &Value {
        self.lines
            .last()
            .unwrap_or_else(|| panic!("no line: {self:?}"))
    }
}

/// `fireparq hypercore-reference <args>` in `cwd` (so no `./.env` of the
/// checkout is read) with exactly `env`.
fn output(cwd: &Path, args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_fireparq"))
        .arg("hypercore-reference")
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .envs(env.iter().copied())
        .output()
        .unwrap()
}

/// A run, whose stdout must be JSON lines only.
fn reference(cwd: &Path, args: &[&str], env: &[(&str, &str)]) -> Run {
    let output = output(cwd, args, env);
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

/// A temporary directory, with the symlinks in its path resolved.
fn workspace() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().canonicalize().unwrap();
    (dir, path)
}

/// A child's environment: its variables and their values.
type Vars<'a> = Vec<(&'a str, &'a str)>;

/// `base` with `set` replacing or adding settings, and `unset` removed.
fn with<'a>(base: &[(&'a str, &'a str)], set: &[(&'a str, &'a str)], unset: &[&str]) -> Vars<'a> {
    base.iter()
        .filter(|(name, _)| !unset.contains(name) && !set.iter().any(|(set, _)| set == name))
        .chain(set)
        .copied()
        .collect()
}

/// Python's test_runs_and_reports, over the real binary: the root from
/// `HLREF_ROOT`, `/info` on plain HTTP at a loopback address.
#[test]
fn outcomes_then_verify_over_a_loopback_info() {
    let (_dir, cwd) = workspace();
    let info = InfoServer::start();
    let url = format!("{}/info", info.endpoint);
    let root = cwd.join("reference");
    let env = [
        ("HLREF_ROOT", root.to_str().unwrap()),
        ("HL_INFO_URL", url.as_str()),
    ];
    let run = reference(&cwd, &["outcomes", "--sweep-from", "9975"], &env);
    assert_eq!(run.status, 0, "{run:?}");
    let start = run.first();
    assert_eq!(start["event"], "start", "{run:?}");
    assert_eq!(start["info"], "http://127.0.0.1");
    assert_eq!(start["root"], json!(root.to_str().unwrap()));
    assert_eq!(start["mode"], "outcomes");
    assert_eq!(start["version"], env!("CARGO_PKG_VERSION"));
    let done = run.last();
    assert_eq!(done["event"], "done", "{run:?}");
    assert_eq!(done["failed"], json!([]), "{run:?}");
    assert!(number(&done["summary"]["probed"]) > 0, "{run:?}");
    assert_eq!(info.fake.count("outcomeMeta"), 1);
    assert!(root.join("hl_outcomes/_delta_log").is_dir());
    assert!(run.stderr.is_empty(), "{run:?}");

    let run = reference(&cwd, &["verify"], &env);
    assert_eq!(run.status, 0, "{run:?}");
    assert_eq!(run.last()["failed"], json!([]), "{run:?}");
    let verified: Vec<&Value> = run
        .lines
        .iter()
        .filter(|line| line["event"] == "verified" && line["table"].is_string())
        .collect();
    assert_eq!(verified.len(), 3, "{run:?}");
}

/// Python's test_failures_exit_1_and_never_log_the_url: the URL, which may
/// hold a key, is in no line of stdout or stderr; only its scheme and host.
#[test]
fn failures_exit_1_and_never_log_the_url() {
    let (_dir, cwd) = workspace();
    let info = InfoServer::start();
    info.fake.edit(|state| {
        state
            .statuses
            .insert("outcomeMeta".into(), vec![503; 10].into())
    });
    let url = format!("{}/v1/SECRETKEY/info?key=SECRETQUERY", info.endpoint);
    let root = cwd.join("reference");
    // The first retry's backoff crosses a 1 s budget: no 6 s of real waits.
    let env = [
        ("HL_INFO_URL", url.as_str()),
        ("HLREF_TIME_BUDGET_SECONDS", "1"),
    ];
    let run = reference(&cwd, &["--root", root.to_str().unwrap(), "outcomes"], &env);
    assert_eq!(run.status, 1, "{run:?}");
    assert_eq!(run.first()["info"], "http://127.0.0.1");
    let failed = &run.last()["failed"];
    assert_eq!(
        failed,
        &json!(["outcomeMeta failed (HTTP 503) and the time budget ended"]),
        "{run:?}"
    );
    let address = info.endpoint.trim_start_matches("http://");
    for text in [&run.stdout, &run.stderr] {
        for hidden in ["SECRETKEY", "SECRETQUERY", "/v1/", address] {
            assert!(!text.contains(hidden), "{hidden}: {run:?}");
        }
    }
}

/// Python's test_configuration_errors_exit_2, and every other setting the
/// command refuses: exit 2 with one `config_error` line (or a usage error on
/// stderr), before any `/info` request or table, and no value echoed.
#[test]
fn configuration_errors_exit_2_before_any_request() {
    let (_dir, cwd) = workspace();
    let info = InfoServer::start();
    let url = format!("{}/info", info.endpoint);
    let root = cwd.join("reference");
    let local = [
        ("HLREF_ROOT", root.to_str().unwrap()),
        ("HL_INFO_URL", url.as_str()),
    ];
    let s3 = [
        ("HLREF_ROOT", "s3://hypercore-ref"),
        ("HL_INFO_URL", url.as_str()),
        // Nothing listens on the discard port: no request could succeed.
        ("AWS_ENDPOINT_URL_S3", "https://127.0.0.1:9"),
        ("AWS_ACCESS_KEY_ID", "loopback-access-key"),
        ("AWS_SECRET_ACCESS_KEY", SECRET),
    ];
    let keys = "an s3:// root needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY";
    let stale = "S3_ENDPOINT and AWS_ENDPOINT_URL are not read by fireparq hypercore-reference; \
                 set AWS_ENDPOINT_URL_S3 (same value)";
    let no_root = "--root or HLREF_ROOT is required";
    let budget = "time budget must be a finite number of seconds, 0 or more";
    let meta = vec!["meta"];
    let cases: Vec<(&str, Vec<&str>, Vars, &str)> = vec![
        (
            "empty keys",
            meta.clone(),
            with(
                &s3,
                &[("AWS_ACCESS_KEY_ID", ""), ("AWS_SECRET_ACCESS_KEY", "")],
                &[],
            ),
            keys,
        ),
        (
            "no keys",
            meta.clone(),
            with(&s3, &[], &["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]),
            keys,
        ),
        (
            "one key",
            meta.clone(),
            with(&s3, &[], &["AWS_SECRET_ACCESS_KEY"]),
            keys,
        ),
        (
            "unsafe renames",
            meta.clone(),
            with(&s3, &[("AWS_S3_ALLOW_UNSAFE_RENAME", "true")], &[]),
            "AWS_S3_ALLOW_UNSAFE_RENAME is refused: commits must be conditional puts",
        ),
        (
            "a stale S3_ENDPOINT",
            meta.clone(),
            with(&s3, &[("S3_ENDPOINT", "https://s3.amazonaws.com")], &[]),
            stale,
        ),
        (
            "a stale AWS_ENDPOINT_URL without AWS_ENDPOINT_URL_S3",
            meta.clone(),
            with(
                &s3,
                &[("AWS_ENDPOINT_URL", "https://127.0.0.1:9")],
                &["AWS_ENDPOINT_URL_S3"],
            ),
            stale,
        ),
        (
            "an http S3 endpoint",
            meta.clone(),
            with(&s3, &[("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:9")], &[]),
            "AWS_ENDPOINT_URL_S3 must be an https:// URL",
        ),
        (
            "an s3:// root without a bucket",
            meta.clone(),
            with(&s3, &[("HLREF_ROOT", "s3://")], &[]),
            "an s3:// root needs a bucket: s3://<bucket>[/<prefix>]",
        ),
        (
            "no root",
            vec!["outcomes"],
            with(&local, &[], &["HLREF_ROOT"]),
            no_root,
        ),
        (
            "an empty root",
            vec!["outcomes"],
            with(&local, &[("HLREF_ROOT", "")], &[]),
            no_root,
        ),
        (
            "a file URL",
            meta.clone(),
            with(&local, &[("HL_INFO_URL", "file:///etc/passwd")], &[]),
            "HL_INFO_URL must be an http(s) URL",
        ),
        (
            "an http URL that is not loopback",
            meta.clone(),
            with(&local, &[("HL_INFO_URL", "http://example.com/info")], &[]),
            "HL_INFO_URL must use https (plain http only for a loopback host)",
        ),
        (
            "a weight below 20",
            meta.clone(),
            with(&local, &[("HLREF_WEIGHT_PER_MINUTE", "19")], &[]),
            "weight budget must be at least 20 per minute",
        ),
        (
            "a negative weight",
            meta.clone(),
            with(&local, &[("HLREF_WEIGHT_PER_MINUTE", "-5")], &[]),
            "weight budget must be at least 20 per minute",
        ),
        (
            "a negative time budget",
            meta.clone(),
            with(&local, &[("HLREF_TIME_BUDGET_SECONDS", "-1")], &[]),
            budget,
        ),
        (
            "a time budget that is not a number",
            meta.clone(),
            with(&local, &[("HLREF_TIME_BUDGET_SECONDS", "NaN")], &[]),
            budget,
        ),
        (
            "an unknown table",
            vec!["maintain", "--tables", "hl_outcomes,hl_nothing"],
            local.to_vec(),
            "--tables names no table of this release: hl_nothing",
        ),
    ];
    for (case, args, env, message) in cases {
        let run = reference(&cwd, &args, &env);
        assert_eq!(run.status, 2, "{case}: {run:?}");
        assert_eq!(
            run.lines,
            [json!({"event": "config_error", "message": message})],
            "{case}: {run:?}"
        );
        for hidden in [SECRET, "passwd", "example.com", "s3.amazonaws.com"] {
            assert!(!run.stdout.contains(hidden), "{case}: {run:?}");
            assert!(!run.stderr.contains(hidden), "{case}: {run:?}");
        }
    }
    // Usage errors: clap's, on stderr, with no JSON line.
    let usage: Vec<(&str, Vec<&str>, Vars)> = vec![
        (
            "a weight that is not a number",
            meta.clone(),
            with(&local, &[("HLREF_WEIGHT_PER_MINUTE", "abc")], &[]),
        ),
        (
            "a negative probe count",
            vec!["outcomes"],
            with(&local, &[("HLREF_MAX_PROBES", "-1")], &[]),
        ),
        (
            "a negative cool-down",
            vec!["outcomes", "--reprobe-after-minutes=-5"],
            local.to_vec(),
        ),
        (
            "funding has no --allow-mass-removal",
            vec!["funding", "--allow-mass-removal"],
            local.to_vec(),
        ),
        (
            "a week date",
            vec!["funding", "--funding-start", "2026-W09-7"],
            local.to_vec(),
        ),
        ("no job", Vec::new(), local.to_vec()),
        ("the removed views command", vec!["views"], local.to_vec()),
    ];
    for (case, args, env) in usage {
        let run = reference(&cwd, &args, &env);
        assert_eq!(run.status, 2, "{case}: {run:?}");
        assert!(run.lines.is_empty(), "{case}: {run:?}");
        assert!(!run.stderr.is_empty(), "{case}: {run:?}");
    }
    assert_eq!(info.requests(), 0);
    assert!(!root.exists(), "nothing was written");
}

/// `--help` shows neither the `/info` URL nor a credential from the
/// environment (the child's own: no test mutates this process's).
#[test]
fn help_hides_the_info_url_and_credentials() {
    let (_dir, cwd) = workspace();
    let env = [
        ("HL_INFO_URL", "https://gw.example/v1/SECRETKEY/info"),
        ("AWS_SECRET_ACCESS_KEY", SECRET),
        ("AWS_ACCESS_KEY_ID", "loopback-access-key"),
        ("HLREF_ROOT", "s3://hypercore-ref-hidden"),
    ];
    for args in [vec!["--help"], vec!["-h"], vec!["outcomes", "--help"]] {
        let help = output(&cwd, &args, &env);
        let (stdout, stderr) = (
            String::from_utf8(help.stdout).unwrap(),
            String::from_utf8_lossy(&help.stderr).into_owned(),
        );
        assert_eq!(help.status.code(), Some(0), "{args:?}: {stdout} {stderr}");
        assert!(stdout.contains("HLREF_"), "{stdout}");
        for hidden in [
            "SECRETKEY",
            "gw.example",
            SECRET,
            "loopback-access-key",
            "hypercore-ref-hidden",
        ] {
            assert!(!stdout.contains(hidden), "{hidden}: {stdout}");
            assert!(!stderr.contains(hidden), "{hidden}: {stderr}");
        }
    }
}

/// Python's test_views_fill_in_the_root, now that the views are documented
/// SQL: the base views read exactly the ten reference tables at `<root>`,
/// and a root fills in with nothing left over.
#[test]
fn documented_views_name_every_table_and_fill_in_the_root() {
    let blocks = section_sql(DOC, "Views");
    assert_eq!(blocks.len(), 3, "base, current and history views");
    let mut base: Vec<&str> = blocks[0]
        .lines()
        .map(|line| {
            let name = line
                .strip_prefix("CREATE OR REPLACE VIEW ")
                .and_then(|rest| rest.split(' ').next())
                .unwrap_or_else(|| panic!("not a base view: {line}"));
            assert_eq!(
                line,
                format!(
                    "CREATE OR REPLACE VIEW {name} AS SELECT * FROM delta_scan('<root>/{name}');"
                )
            );
            name
        })
        .collect();
    base.sort();
    let mut tables: Vec<&str> = REFERENCE_TABLES.iter().map(|spec| spec.name).collect();
    tables.sort();
    assert_eq!(base, tables);
    let sql = blocks.join("\n");
    assert_eq!(sql.matches("CREATE OR REPLACE VIEW ").count(), 29);
    let filled = sql.replace("<root>", "s3://hypercore-ref");
    assert!(filled.contains("delta_scan('s3://hypercore-ref/hl_outcomes')"));
    assert_eq!(
        filled.matches("delta_scan('s3://hypercore-ref/hl_").count(),
        10
    );
    assert!(!filled.contains("<root>") && !filled.contains("{root}"));
}

/// Python's test_views.py scenario, once per test binary, with the real
/// binary and real time: meta; outcomes `--outcome-ids 7768` on outcomeMeta
/// t0; outcomes from 9970 on t1; the same with question 198 gone from
/// outcomeMeta; funding of three named coins (a named run skips the delisted
/// hour and the staleness gate, where Python's ran at 07:30 UTC).
struct Scenario {
    _dir: tempfile::TempDir,
    duckdb: DuckDb,
    /// The documented views over the tables it wrote.
    views: String,
}

static SCENARIO: OnceLock<Option<Scenario>> = OnceLock::new();

fn scenario() -> Option<&'static Scenario> {
    SCENARIO
        .get_or_init(|| {
            let (dir, cwd) = workspace();
            let duckdb = DuckDb::open(&cwd)?;
            let info = InfoServer::start();
            let url = format!("{}/info", info.endpoint);
            let root = cwd.join("reference");
            let root = root.to_str().unwrap();
            let env = [("HL_INFO_URL", url.as_str())];
            let run = |args: &[&str]| {
                let ran = reference(&cwd, &[&["--root", root][..], args].concat(), &env);
                assert_eq!(ran.status, 0, "{args:?}: {ran:?}");
                assert_eq!(ran.last()["failed"], json!([]), "{args:?}: {ran:?}");
            };
            run(&["meta"]);
            run(&["outcomes", "--outcome-ids", "7768"]);
            info.fake
                .edit(|state| state.outcome_meta = FakeInfo::outcome_meta_t1());
            run(&["outcomes", "--sweep-from", "9970"]);
            info.fake.edit(|state| {
                let mut meta = FakeInfo::outcome_meta_t1();
                *meta.pointer_mut("/questions").unwrap() = Json::Arr(Vec::new());
                state.outcome_meta = meta;
            });
            run(&["outcomes", "--sweep-from", "9970"]);
            run(&["funding", "--coins", "BTC,xyz:XYZ100,MATIC"]);
            let views = section_sql(DOC, "Views").join("\n").replace("<root>", root);
            Some(Scenario {
                _dir: dir,
                duckdb,
                views,
            })
        })
        .as_ref()
}

impl Scenario {
    /// The rows of `select`, in its order, over the views.
    fn values(&self, select: &str) -> Vec<Value> {
        // The harness keeps the rows tagged `q`.
        let tagged = select.replacen("SELECT ", "SELECT 'r' AS q, ", 1);
        let sql = format!("SET TimeZone = 'UTC';\n{}\n{tagged};", self.views);
        let mut rows = self.duckdb.query(&sql).remove("r").unwrap_or_default();
        for row in &mut rows {
            row.as_object_mut().unwrap().remove("q");
        }
        rows
    }
}

#[test]
fn views_every_table_has_a_base_view() {
    let Some(scenario) = scenario() else {
        return;
    };
    let tables = REFERENCE_TABLES
        .iter()
        .map(|spec| format!("SELECT '{0}' AS t, count(*) AS n FROM {0}", spec.name))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let counts: BTreeMap<String, u64> = scenario
        .values(&format!("SELECT * FROM ({tables})"))
        .iter()
        .map(|row| (row["t"].as_str().unwrap().to_owned(), number(&row["n"])))
        .collect();
    assert_eq!(counts.len(), REFERENCE_TABLES.len(), "{counts:?}");
    for (table, rows) in &counts {
        assert!(*rows > 0, "{table} is empty: {counts:?}");
    }
    assert_eq!(counts["hl_funding_history"], 18);
}

#[test]
fn views_current_is_one_row_per_entity() {
    let Some(scenario) = scenario() else {
        return;
    };
    for (view, key) in [
        ("hl_spot_meta_current", "request"),
        ("hl_spot_tokens_current", "token_index"),
        ("hl_spot_token_details_current", "token_index"),
        ("hl_spot_pairs_current", "pair_index"),
        ("hl_perp_dexs_current", "dex_index"),
        ("hl_perp_assets_current", "coin"),
        ("hl_outcome_meta_current", "request"),
        ("hl_outcomes_current", "outcome_id"),
        ("hl_questions_current", "question_id"),
        ("hl_funding_history_current", "coin"),
    ] {
        let row = &scenario.values(&format!(
            "SELECT count(*) AS total, count(DISTINCT {key}) AS keys FROM {view}"
        ))[0];
        let (total, keys) = (number(&row["total"]), number(&row["keys"]));
        assert_eq!(total, keys, "{view}");
        assert!(total > 0, "{view}");
    }
}

#[test]
fn views_spot_tokens_current_has_the_token_details() {
    let Some(scenario) = scenario() else {
        return;
    };
    assert_eq!(
        scenario.values(
            "SELECT name, deployer, strftime(deploy_time, '%Y-%m-%d') AS day, \
             CAST(deploy_gas AS VARCHAR) AS gas FROM hl_spot_tokens_current WHERE token_index = 437"
        ),
        [json!({
            "name": "HSEI",
            "deployer": "0x96fdb9e5c1a3d33f34109118457500b3d96c7094",
            "day": "2026-02-04",
            "gas": "529.8506899600",
        })]
    );
    // tokenDetails answered null for token 1: no details, the token is there.
    assert_eq!(
        scenario.values("SELECT deployer FROM hl_spot_tokens_current WHERE token_index = 1"),
        [json!({"deployer": null})]
    );
}

#[test]
fn views_outcome_meta_current() {
    let Some(scenario) = scenario() else {
        return;
    };
    assert_eq!(
        scenario.values(
            "SELECT request, CAST(fee_scale AS VARCHAR) AS fee_scale, venue_deployers['skew'] AS skew, \
             json_extract_string(payload_json, '$.deployers[0].venue') AS venue \
             FROM hl_outcome_meta_current"
        ),
        [json!({
            "request": "outcomeMeta",
            "fee_scale": "1.0000000000",
            "skew": "0x08e9c89f46dccee91bdb85c6532eb93a4c335efe",
            "venue": "skew",
        })]
    );
}

#[test]
fn views_outcomes_current_and_sides() {
    let Some(scenario) = scenario() else {
        return;
    };
    assert_eq!(
        scenario.values("SELECT status, listed FROM hl_outcomes_current WHERE outcome_id = 9975"),
        [json!({"status": "settled", "listed": false})]
    );
    let questions: Vec<u64> = scenario
        .values(
            "SELECT question_id FROM hl_outcomes_current WHERE outcome_id IN (1472, 1473) \
             ORDER BY outcome_id",
        )
        .iter()
        .map(|row| number(&row["question_id"]))
        .collect();
    assert_eq!(questions, [198, 198]);
    let sides: Vec<Value> = scenario
        .values(
            "SELECT coin, outcome_id, side_index, side_name, question_name \
             FROM hl_outcome_sides_current WHERE outcome_id = 7768 ORDER BY side_index",
        )
        .into_iter()
        .map(|row| {
            json!([
                row["coin"],
                number(&row["outcome_id"]),
                number(&row["side_index"]),
                row["side_name"],
                row["question_name"],
            ])
        })
        .collect();
    assert_eq!(
        sides,
        [
            json!(["#77680", 7768, 0, "Yes", "template:sportsContestResult"]),
            json!(["#77681", 7768, 1, "No", "template:sportsContestResult"]),
        ]
    );
}

#[test]
fn views_questions_current_keeps_the_last_listed_ids() {
    let Some(scenario) = scenario() else {
        return;
    };
    let row = &scenario.values(
        "SELECT listed, status, named_outcome_ids, fallback_outcome_id \
         FROM hl_questions_current WHERE question_id = 198",
    )[0];
    assert_eq!(
        (&row["listed"], &row["status"]),
        (&json!(false), &json!("live"))
    );
    let named: Vec<u64> = row["named_outcome_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(number)
        .collect();
    assert_eq!(named, [1473]);
    assert_eq!(number(&row["fallback_outcome_id"]), 1472);
    assert_eq!(
        scenario.values(
            "SELECT status, named_outcome_ids FROM hl_questions_current WHERE question_id = 373"
        ),
        [json!({"status": "settled", "named_outcome_ids": null})]
    );
}

#[test]
fn views_history_intervals() {
    let Some(scenario) = scenario() else {
        return;
    };
    let rows = scenario.values(
        "SELECT listed, CAST(epoch_us(valid_from) AS VARCHAR) AS valid_from, \
         CAST(epoch_us(valid_to) AS VARCHAR) AS valid_to \
         FROM hl_questions_history WHERE question_id = 198 ORDER BY valid_from",
    );
    let listed: Vec<&Value> = rows.iter().map(|row| &row["listed"]).collect();
    assert_eq!(listed, [&json!(true), &json!(false)]);
    assert_eq!(rows[0]["valid_to"], rows[1]["valid_from"]);
    assert_eq!(rows[1]["valid_to"], Value::Null);
    for view in [
        "hl_spot_meta_history",
        "hl_spot_tokens_history",
        "hl_spot_pairs_history",
        "hl_perp_dexs_history",
        "hl_perp_assets_history",
        "hl_outcome_meta_history",
        "hl_outcomes_history",
    ] {
        let row = &scenario.values(&format!(
            "SELECT count(*) FILTER (WHERE valid_to IS NULL) AS open FROM {view}"
        ))[0];
        assert!(number(&row["open"]) > 0, "{view}");
    }
}

#[test]
fn views_funding_current_and_join_keys() {
    let Some(scenario) = scenario() else {
        return;
    };
    assert_eq!(
        scenario.values(
            "SELECT coin, dex, strftime(funding_time, '%H:%M') AS hour \
             FROM hl_funding_history_current ORDER BY coin"
        ),
        [
            json!({"coin": "BTC", "dex": "", "hour": "05:00"}),
            json!({"coin": "MATIC", "dex": "", "hour": "05:00"}),
            json!({"coin": "xyz:XYZ100", "dex": "xyz", "hour": "05:00"}),
        ]
    );
    assert_eq!(
        scenario.values(
            "SELECT d.name, d.collateral_symbol FROM hl_perp_assets_current a \
             JOIN hl_perp_dexs_current d USING (dex_index) WHERE a.coin = 'flx:TSLA'"
        ),
        [json!({"name": "flx", "collateral_symbol": "USDH"})]
    );
}
