//! The Python job's `test_jobs.py` (outcomes, meta, funding, verify) and the
//! jobs' own tests: each on a local root, with the fake `/info` and clock.
//! Where Python read the rows of one data file in their order, these sort by
//! time (probe times increase in probe order).

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::{json, Map, Value};

use super::*;
use crate::info::InfoClient;
use crate::json::Json;
use crate::model::{
    funding_row, outcome_settled_row, removal_row, Cell, Row, TableSpec, ALL_TABLES,
    FUNDING_HISTORY, OUTCOMES, OUTCOME_META, OUTCOME_PROBES, PERP_ASSETS, PERP_DEXS, QUESTIONS,
    SPOT_META, SPOT_PAIRS, SPOT_TOKENS, SPOT_TOKEN_DETAILS,
};
use crate::store::{OpenMode, Root, Store};
use crate::testing::{FakeClock, FakeInfo, MemoryLog};
use crate::values::{crc32_hour, from_millis, parse_decimal};

/// 2026-01-01T05:30:00Z, half an hour after the funding fixtures' last hour.
const FUNDING_CLOCK: i64 = 1_767_245_400_000_000;
const HOUR: f64 = 3600.0;

#[derive(Clone)]
enum Which {
    Outcomes,
    Meta,
    Funding,
    Maintain(Option<Vec<String>>),
    Verify,
}

/// What a run left.
struct Done {
    failures: Vec<String>,
    summary: Map<String, Value>,
}

/// A local root, the fake `/info` and clock, and the log every run writes.
struct Harness {
    _dir: tempfile::TempDir,
    root: PathBuf,
    clock: Arc<FakeClock>,
    fake: Arc<FakeInfo>,
    log: MemoryLog,
}

pub(crate) fn no_aws() -> firehose_parquet::s3::AwsConfig {
    firehose_parquet::s3::AwsConfig {
        aws_access_key_id: None,
        aws_secret_access_key: None,
        aws_session_token: None,
        aws_region: None,
        aws_endpoint_url: None,
    }
}

impl Harness {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        Self {
            _dir: dir,
            root,
            clock: Arc::new(FakeClock::new()),
            fake: Arc::new(FakeInfo::new()),
            log: MemoryLog::new(),
        }
    }

    /// The funding tests' harness: pages of two, the clock at 05:30.
    fn funding() -> Self {
        let harness = Self::new();
        harness.fake.edit(|state| state.page = 2);
        harness.clock.set_wall(FUNDING_CLOCK);
        harness
    }

    fn store_at(&self, root: &Path) -> Store {
        Store::new(
            Root::Local(root.to_path_buf()),
            &no_aws(),
            &|_| None,
            "test".into(),
            "test",
            self.log.logger(),
            self.clock.clone(),
        )
        .unwrap()
    }

    fn store(&self) -> Store {
        self.store_at(&self.root)
    }

    async fn run(&self, which: Which, options: Options) -> Done {
        self.run_on(&self.store(), which, options, 100_000, None)
            .await
    }

    /// Runs `which` on `store` with a weight budget, and a time budget from
    /// now.
    async fn run_on(
        &self,
        store: &Store,
        which: Which,
        options: Options,
        per_minute: u32,
        budget: Option<f64>,
    ) -> Done {
        let mut info = InfoClient::new(
            self.fake.clone(),
            per_minute,
            self.clock.clone(),
            Box::new(|| 0.0),
            self.log.logger(),
        )
        .unwrap();
        info.deadline = budget.map(|seconds| self.clock.monotonic() + seconds);
        let mut run = Run::new(
            store,
            &mut info,
            options,
            self.log.logger(),
            "test".into(),
            self.clock.clone(),
        );
        let result = match which {
            Which::Outcomes => run_outcomes(&mut run).await,
            Which::Meta => run_meta(&mut run).await,
            Which::Funding => run_funding(&mut run).await,
            Which::Maintain(tables) => run_maintain(&mut run, tables.as_deref()).await,
            Which::Verify => run_verify(&mut run).await,
        };
        if let Err(error) = result {
            panic!("the job ended with {error:?}");
        }
        Done {
            failures: run.failures,
            summary: run.summary,
        }
    }

    async fn rows(&self, spec: &'static TableSpec) -> Vec<Row> {
        rows_of(&self.store(), spec).await
    }

    async fn version(&self, spec: &'static TableSpec) -> u64 {
        let table = self.store().open(spec, OpenMode::Read).await.unwrap();
        table.unwrap().version()
    }

    async fn versions(&self, specs: &[&'static TableSpec]) -> Vec<u64> {
        let mut versions = Vec::new();
        for spec in specs {
            versions.push(self.version(spec).await);
        }
        versions
    }

    /// `field` of the requests of type `kind`, from request `since` on.
    fn asked(&self, kind: &str, field: &str, since: usize) -> Vec<Json> {
        self.fake.calls()[since..]
            .iter()
            .filter(|call| call.get("type").and_then(Json::as_str) == Some(kind))
            .map(|call| call.get(field).cloned().unwrap_or(Json::Null))
            .collect()
    }

    fn asked_ids(&self, since: usize) -> Vec<i64> {
        let ids = self.asked("settledOutcome", "outcome", since);
        ids.iter().filter_map(Json::as_i64).collect()
    }

    fn asked_coins(&self, since: usize) -> Vec<String> {
        let coins = self.asked("fundingHistory", "coin", since);
        coins
            .iter()
            .filter_map(|coin| coin.as_str().map(str::to_owned))
            .collect()
    }

    fn calls(&self) -> usize {
        self.fake.calls().len()
    }

    /// A run of verify: its failures.
    async fn verify(&self) -> Vec<String> {
        self.run(Which::Verify, Options::default()).await.failures
    }

    fn statuses(&self, kind: &str, statuses: Vec<u16>) {
        self.fake
            .edit(|state| state.statuses.insert(kind.into(), statuses.into()));
    }

    fn now(&self) -> i64 {
        self.clock.now_micros()
    }
}

/// Every row of `spec`, every column.
pub(crate) async fn rows_of(store: &Store, spec: &'static TableSpec) -> Vec<Row> {
    let table = store.open(spec, OpenMode::Read).await.unwrap().unwrap();
    let columns: Vec<&str> = spec.columns.iter().map(|column| column.name).collect();
    let mut rows = Vec::new();
    store
        .for_each_row(&table, &columns, None, &mut |cells| {
            rows.push(Row::of(spec, &columns, cells))
        })
        .await
        .unwrap();
    rows
}

pub(crate) fn long(row: &Row, column: &str) -> Option<i64> {
    match row.get(column) {
        Cell::Long(value) => Some(*value),
        _ => None,
    }
}

pub(crate) fn text<'a>(row: &'a Row, column: &str) -> Option<&'a str> {
    match row.get(column) {
        Cell::Text(value) => Some(value),
        _ => None,
    }
}

pub(crate) fn stamp(row: &Row, column: &str) -> i64 {
    match row.get(column) {
        Cell::Timestamp(value) => *value,
        other => panic!("{column} holds {other:?}"),
    }
}

/// The rows by `column` (a timestamp), oldest first.
fn by_time(mut rows: Vec<Row>, column: &str) -> Vec<Row> {
    rows.sort_by_key(|row| stamp(row, column));
    rows
}

fn sorted<T: Ord>(mut items: Vec<T>) -> Vec<T> {
    items.sort();
    items
}

/// The newest row of each id.
fn latest_by(rows: Vec<Row>, key: &str) -> HashMap<i64, Row> {
    let mut latest: HashMap<i64, Row> = HashMap::new();
    for row in by_time(rows, "observed_at") {
        latest.insert(long(&row, key).unwrap(), row);
    }
    latest
}

fn sweep(from: i64) -> Options {
    Options {
        sweep_from: from,
        ..Options::default()
    }
}

fn no_failures() -> Vec<String> {
    Vec::new()
}

// outcomes ---------------------------------------------------------------

#[tokio::test]
async fn outcomes_append_only_snapshots_and_departures() {
    let h = Harness::new();
    let done = h.run(Which::Outcomes, sweep(9970)).await;
    assert_eq!(done.failures, no_failures());
    let first = h.rows(&OUTCOMES).await;
    assert_eq!(
        sorted(
            first
                .iter()
                .filter_map(|row| long(row, "outcome_id"))
                .collect()
        ),
        [1472, 1473, 9973, 9974, 9975]
    );
    assert!(first
        .iter()
        .all(|row| row.listed() == Some(true) && text(row, "status") == Some("live")));
    let latest = latest_by(first, "outcome_id");
    assert_eq!(long(&latest[&1473], "question_id"), Some(198));
    let probes = by_time(h.rows(&OUTCOME_PROBES).await, "probed_at");
    // The sweep probes newest first, up to three ids past the highest live one.
    assert_eq!(
        probes
            .iter()
            .filter_map(|row| long(row, "outcome_id"))
            .collect::<Vec<_>>(),
        [9978, 9977, 9976, 9972, 9971, 9970]
    );
    assert!(probes
        .iter()
        .all(|row| text(row, "result") == Some("not_settled")));
    let specs: [&'static TableSpec; 3] = [&OUTCOMES, &QUESTIONS, &OUTCOME_PROBES];
    let versions = h.versions(&specs).await;

    // The same snapshot again appends nothing; not-settled ids are cooling down.
    h.clock.advance(300.0);
    let done = h.run(Which::Outcomes, sweep(9970)).await;
    assert_eq!(
        (done.failures, &done.summary["probed"]),
        (no_failures(), &json!(0))
    );
    assert_eq!(h.versions(&specs).await, versions);

    // An hour later 9973-9975 have settled and 9976-9977 are live.
    h.clock.advance(HOUR);
    h.fake
        .edit(|state| state.outcome_meta = FakeInfo::outcome_meta_t1());
    let done = h.run(Which::Outcomes, sweep(9970)).await;
    assert_eq!(done.failures, no_failures());
    let latest = latest_by(h.rows(&OUTCOMES).await, "outcome_id");
    for id in [9973, 9974, 9975] {
        let row = &latest[&id];
        assert_eq!(
            (text(row, "status"), row.listed(), text(row, "source")),
            (Some("settled"), Some(false), Some("settledOutcome")),
            "{id}"
        );
    }
    let fraction = h
        .fake
        .edit(|state| parse_decimal(state.settled[&9975].get("settleFraction")));
    assert_eq!(
        latest[&9975].get("settle_fraction"),
        &Cell::Decimal(fraction.unwrap())
    );
    for id in [9976, 9977, 1472, 1473] {
        assert_eq!(text(&latest[&id], "status"), Some("live"), "{id}");
    }
    assert_eq!(h.rows(&OUTCOMES).await.len(), 10);
    // Question 198 is unchanged.
    assert_eq!(h.rows(&QUESTIONS).await.len(), 1);

    // Settled outcomes are final: never probed again.
    h.clock.advance(2.0 * HOUR);
    let before = h.calls();
    h.run(Which::Outcomes, sweep(9970)).await;
    let probed = h.asked_ids(before);
    assert!(
        !probed.iter().any(|id| (9973..=9975).contains(id)),
        "{probed:?}"
    );
    assert_eq!(h.rows(&OUTCOMES).await.len(), 10);
    assert_eq!(h.verify().await, no_failures());
}

#[tokio::test]
async fn outcomes_sweep_is_capped_and_resumes() {
    let h = Harness::new();
    for probes in [2, 2, 4] {
        let options = Options {
            max_probes: probes,
            ..sweep(9969)
        };
        h.run(Which::Outcomes, options).await;
        h.clock.advance(300.0);
    }
    let probed: Vec<i64> = h
        .rows(&OUTCOME_PROBES)
        .await
        .iter()
        .filter_map(|row| long(row, "outcome_id"))
        .collect();
    let mut newest_first = probed.clone();
    newest_first.sort_by(|a, b| b.cmp(a));
    assert_eq!(newest_first, [9978, 9977, 9976, 9972, 9971, 9970, 9969]);
    newest_first.dedup();
    assert_eq!(newest_first.len(), probed.len());
}

#[tokio::test]
async fn outcomes_named_ids_are_all_probed() {
    let h = Harness::new();
    let ids = vec![0, 7768, 9174, 9973, 9974, 9975];
    let options = Options {
        outcome_ids: Some(ids.clone()),
        max_probes: 2,
        ..sweep(9970)
    };
    let done = h.run(Which::Outcomes, options).await;
    assert_eq!(
        (
            done.failures,
            &done.summary["probed"],
            &done.summary["probed_settled"]
        ),
        (no_failures(), &json!(6), &json!(6))
    );
    assert_eq!(h.asked_ids(0), ids);
}

#[tokio::test]
async fn outcomes_settled_question_and_departed_question() {
    let h = Harness::new();
    let named = |ids: Vec<i64>| Options {
        outcome_ids: Some(ids),
        ..sweep(9970)
    };
    // Settled 7768's question 373 left outcomeMeta.
    h.run(Which::Outcomes, named(vec![7768, 0])).await;
    let questions: Vec<(Option<i64>, Option<String>, Option<bool>)> = h
        .rows(&QUESTIONS)
        .await
        .iter()
        .map(|row| {
            (
                long(row, "question_id"),
                text(row, "status").map(str::to_owned),
                row.listed(),
            )
        })
        .collect();
    assert_eq!(
        sorted(questions),
        [
            (Some(198), Some("live".into()), Some(true)),
            (Some(373), Some("settled".into()), Some(false))
        ]
    );
    let outcomes = latest_by(h.rows(&OUTCOMES).await, "outcome_id");
    assert_eq!(
        (
            long(&outcomes[&7768], "question_id"),
            long(&outcomes[&0], "question_id")
        ),
        (Some(373), None)
    );

    // Probing it again changes nothing.
    h.clock.advance(300.0);
    h.run(Which::Outcomes, named(vec![7768])).await;
    assert_eq!(h.rows(&QUESTIONS).await.len(), 2);

    // Question 198 leaves outcomeMeta without a settled wrapper: one removal
    // row. An empty id list is the sweep.
    h.fake.edit(|state| {
        *state.outcome_meta.pointer_mut("/questions").unwrap() = Json::Arr(Vec::new())
    });
    for _ in 0..2 {
        h.clock.advance(300.0);
        h.run(Which::Outcomes, named(Vec::new())).await;
    }
    let history: Vec<Row> = h
        .rows(&QUESTIONS)
        .await
        .into_iter()
        .filter(|row| long(row, "question_id") == Some(198))
        .collect();
    assert_eq!(history.len(), 2);
    let gone = by_time(history, "observed_at").pop().unwrap();
    assert_eq!(
        (
            gone.listed(),
            gone.get("payload_json"),
            gone.get("named_outcome_ids")
        ),
        (Some(false), &Cell::Null, &Cell::LongList(vec![Some(1473)]))
    );
}

#[tokio::test]
async fn outcomes_an_outcome_that_leaves_unsettled_is_recorded_and_reprobed() {
    let h = Harness::new();
    h.run(Which::Outcomes, sweep(9970)).await;
    h.fake.edit(|state| {
        let outcomes = state.outcome_meta.pointer_mut("/outcomes").unwrap();
        outcomes
            .as_arr_mut()
            .unwrap()
            .retain(|item| item.get("outcome").and_then(Json::as_i64) != Some(1473));
    });
    h.clock.advance(300.0);
    h.run(Which::Outcomes, sweep(9975)).await;
    let history: Vec<(Option<bool>, Option<String>)> =
        by_time(h.rows(&OUTCOMES).await, "observed_at")
            .iter()
            .filter(|row| long(row, "outcome_id") == Some(1473))
            .map(|row| (row.listed(), text(row, "status").map(str::to_owned)))
            .collect();
    assert_eq!(
        history,
        [
            (Some(true), Some("live".into())),
            (Some(false), Some("live".into()))
        ]
    );
    // It is probed again after the cool-down, until it settles.
    h.clock.advance(4000.0);
    let before = h.calls();
    h.run(Which::Outcomes, sweep(1473)).await;
    assert!(h.asked_ids(before).contains(&1473));
}

#[tokio::test]
async fn outcomes_an_outage_commits_what_completed_and_fails() {
    let h = Harness::new();
    h.statuses("settledOutcome", vec![503; 20]);
    let done = h.run(Which::Outcomes, sweep(9970)).await;
    assert!(!done.failures.is_empty());
    assert_eq!(h.rows(&OUTCOMES).await.len(), 5);
}

#[tokio::test]
async fn outcomes_an_id_that_info_fails_for_cools_down() {
    let h = Harness::new();
    // HTTP 500 for one id (three attempts), then the sweep goes on.
    h.statuses("settledOutcome", vec![500; 3]);
    let done = h.run(Which::Outcomes, sweep(9970)).await;
    assert_eq!(done.failures, no_failures());
    let probes: Vec<(Option<i64>, Option<String>)> =
        by_time(h.rows(&OUTCOME_PROBES).await, "probed_at")
            .iter()
            .map(|row| {
                (
                    long(row, "outcome_id"),
                    text(row, "result").map(str::to_owned),
                )
            })
            .collect();
    assert_eq!(
        probes[..2],
        [
            (Some(9978), Some("error".into())),
            (Some(9977), Some("not_settled".into()))
        ]
    );
    h.clock.advance(300.0);
    let before = h.calls();
    h.run(Which::Outcomes, sweep(9970)).await;
    assert!(!h.asked_ids(before).contains(&9978));
}

#[tokio::test]
async fn outcomes_a_settled_outcome_outage_fails_the_run() {
    let h = Harness::new();
    // Every settledOutcome answers HTTP 500: not single ids, so the run fails.
    h.statuses("settledOutcome", vec![500; 1000]);
    let done = h.run(Which::Outcomes, sweep(9970)).await;
    assert_eq!(done.failures.len(), 1, "{:?}", done.failures);
    assert!(done.failures[0].contains("settledOutcome: 6 of 6 requests failed with HTTP 500"));
    // The live snapshot is still written, and so is every probe, an error.
    assert_eq!(h.rows(&OUTCOMES).await.len(), 5);
    let results: Vec<Option<String>> = h
        .rows(&OUTCOME_PROBES)
        .await
        .iter()
        .map(|row| text(row, "result").map(str::to_owned))
        .collect();
    assert_eq!(results, vec![Some("error".to_owned()); 6]);
}

/// outcomeMeta t0 with 1472, 1473 (question 198) and 40 more like 1472.
fn many_outcomes() -> Json {
    let mut snapshot = FakeInfo::new().edit(|state| state.outcome_meta.clone());
    let outcomes = snapshot
        .pointer_mut("/outcomes")
        .unwrap()
        .as_arr_mut()
        .unwrap();
    outcomes.retain(|item| {
        matches!(
            item.get("outcome").and_then(Json::as_i64),
            Some(1472 | 1473)
        )
    });
    let template = outcomes[0].clone();
    for index in 0..40 {
        let mut copy = template.clone();
        *copy.pointer_mut("/outcome").unwrap() = Json::Int((2000 + index).to_string());
        outcomes.push(copy);
    }
    snapshot
}

fn emptied(mut snapshot: Json) -> Json {
    for list in ["/outcomes", "/questions"] {
        *snapshot.pointer_mut(list).unwrap() = Json::Arr(Vec::new());
    }
    snapshot
}

#[tokio::test]
async fn outcomes_an_empty_outcome_meta_writes_no_removal_rows() {
    let h = Harness::new();
    h.fake.edit(|state| state.outcome_meta = many_outcomes());
    assert_eq!(
        h.run(Which::Outcomes, sweep(9978)).await.failures,
        no_failures()
    );
    let before = h.rows(&OUTCOMES).await.len();
    // HTTP 200 with no outcomes and no questions: the 42 listed outcomes and
    // question 198 would each get a permanent removal row.
    h.fake
        .edit(|state| state.outcome_meta = emptied(many_outcomes()));
    h.clock.advance(300.0);
    let done = h.run(Which::Outcomes, sweep(9978)).await;
    assert_eq!(done.failures.len(), 1, "{:?}", done.failures);
    assert!(
        done.failures[0].contains("hl_outcomes: the snapshot lacks 42 of the 42 listed entities")
    );
    assert_eq!(h.rows(&OUTCOMES).await.len(), before);
    // Question 198 alone is within the bound.
    let questions: Vec<Option<bool>> = by_time(h.rows(&QUESTIONS).await, "observed_at")
        .iter()
        .map(Row::listed)
        .collect();
    assert_eq!(questions, [Some(true), Some(false)]);
    // The next healthy snapshot appends nothing: nothing was delisted.
    h.fake.edit(|state| state.outcome_meta = many_outcomes());
    h.clock.advance(300.0);
    assert_eq!(
        h.run(Which::Outcomes, sweep(9978)).await.failures,
        no_failures()
    );
    assert_eq!(h.rows(&OUTCOMES).await.len(), before);
    // A real mass removal is recorded when the run is told so.
    h.fake
        .edit(|state| state.outcome_meta = emptied(many_outcomes()));
    h.clock.advance(300.0);
    let options = Options {
        allow_mass_removal: true,
        ..sweep(9978)
    };
    assert_eq!(
        h.run(Which::Outcomes, options).await.failures,
        no_failures()
    );
    let removed = h.rows(&OUTCOMES).await;
    assert_eq!(
        removed
            .iter()
            .filter(|row| row.listed() == Some(false))
            .count(),
        42
    );
}

#[tokio::test]
async fn outcomes_outcome_meta_keeps_the_fields_beside_the_lists() {
    let h = Harness::new();
    h.run(Which::Outcomes, sweep(9970)).await;
    h.clock.advance(300.0);
    h.run(Which::Outcomes, sweep(9970)).await;
    let stored = h.rows(&OUTCOME_META).await;
    assert_eq!(stored.len(), 1);
    let payload = text(&stored[0], "payload_json").unwrap();
    assert!(payload
        .contains(r#""deployers":[{"deployer":"0x08e9c89f46dccee91bdb85c6532eb93a4c335efe""#));
    assert!(payload.contains(r#""feeScale":"1.0""#));
    h.fake.edit(|state| {
        let Json::Obj(meta) = &mut state.outcome_meta else {
            panic!("an object")
        };
        meta.insert("feeScale".into(), Json::Str("0.5".into()));
        meta.insert("somethingNew".into(), Json::Bool(true));
    });
    h.clock.advance(300.0);
    h.run(Which::Outcomes, sweep(9970)).await;
    let latest = by_time(h.rows(&OUTCOME_META).await, "observed_at")
        .pop()
        .unwrap();
    assert_eq!(latest.get("fee_scale"), &Cell::Decimal(5_000_000_000));
    assert!(text(&latest, "payload_json")
        .unwrap()
        .contains(r#""somethingNew":true"#));
}

#[tokio::test]
async fn outcomes_a_failed_outcomes_commit_never_strands_a_question() {
    // 7768 is settled and its question 373 left outcomeMeta: the question is
    // known only from 7768's settledOutcome answer.
    let h = Harness::new();
    let mut store = h.store();
    store.fail_append = Some(Box::new(|table, rows| {
        (table == OUTCOMES.name && rows > 0).then(|| "hl_outcomes: injected failure".to_owned())
    }));
    let options = Options {
        outcome_ids: Some(vec![7768]),
        ..sweep(9970)
    };
    let done = h
        .run_on(&store, Which::Outcomes, options, 100_000, None)
        .await;
    assert_eq!(done.failures, ["hl_outcomes: injected failure"]);
    let questions = h.rows(&QUESTIONS).await;
    assert!(questions
        .iter()
        .any(|row| long(row, "question_id") == Some(373)));
    let outcomes = h.rows(&OUTCOMES).await;
    assert!(!outcomes
        .iter()
        .any(|row| long(row, "outcome_id") == Some(7768)));
    // The sweep probes 7768 again (it has no settled row) and stores it.
    h.clock.advance(300.0);
    let options = Options {
        sweep_to: Some(7768),
        ..sweep(7768)
    };
    h.run(Which::Outcomes, options).await;
    let settled: Vec<(Option<String>, Option<i64>)> = h
        .rows(&OUTCOMES)
        .await
        .iter()
        .filter(|row| long(row, "outcome_id") == Some(7768))
        .map(|row| {
            (
                text(row, "status").map(str::to_owned),
                long(row, "question_id"),
            )
        })
        .collect();
    assert_eq!(settled, [(Some("settled".into()), Some(373))]);
    assert_eq!(h.verify().await, no_failures());
}

#[tokio::test]
async fn outcomes_a_zero_budget_writes_the_snapshot_and_stops_before_any_probe() {
    let h = Harness::new();
    let done = h
        .run_on(&h.store(), Which::Outcomes, sweep(9970), 100_000, Some(0.0))
        .await;
    assert_eq!(done.failures, no_failures());
    assert_eq!(done.summary["probe_budget_exhausted"], json!(true));
    assert_eq!(done.summary["probed"], json!(0));
    assert_eq!(h.fake.count("settledOutcome"), 0);
    assert_eq!(h.rows(&OUTCOME_META).await.len(), 1);
    assert_eq!(h.rows(&OUTCOMES).await.len(), 5);
}

#[tokio::test]
async fn an_add_columns_failure_stops_the_job_before_any_request() {
    let h = Harness::new();
    // The probe table as an older release created it, without `run_id`.
    let older: &'static TableSpec = Box::leak(Box::new(TableSpec {
        name: OUTCOME_PROBES.name,
        key: OUTCOME_PROBES.key,
        columns: &OUTCOME_PROBES.columns[..3],
    }));
    let store = h.store();
    store.open(older, OpenMode::Write).await.unwrap();
    let before = h.version(&OUTCOME_PROBES).await;
    let mut store = h.store();
    store.fail_add_columns = Some("_hlref/outcome_probes: adding columns failed".into());
    let mut info = InfoClient::new(
        h.fake.clone(),
        100_000,
        h.clock.clone(),
        Box::new(|| 0.0),
        h.log.logger(),
    )
    .unwrap();
    let mut run = Run::new(
        &store,
        &mut info,
        sweep(9970),
        h.log.logger(),
        "test".into(),
        h.clock.clone(),
    );
    assert_eq!(
        run_outcomes(&mut run).await,
        Err(JobError::Fatal(
            "_hlref/outcome_probes: adding columns failed".into()
        ))
    );
    assert!(h.fake.calls().is_empty());
    assert_eq!(h.version(&OUTCOME_PROBES).await, before);
    let names: Vec<String> = {
        let table = h
            .store()
            .open(older, OpenMode::Read)
            .await
            .unwrap()
            .unwrap();
        let schema = table.delta.snapshot().unwrap().schema();
        schema.fields().map(|field| field.name().clone()).collect()
    };
    assert_eq!(names, ["outcome_id", "probed_at", "result"]);
}

#[test]
fn empty_option_lists_mean_not_given() {
    let options = Options {
        outcome_ids: Some(Vec::new()),
        coins: Some(Vec::new()),
        ..Options::default()
    }
    .normalized();
    assert_eq!((options.outcome_ids, options.coins), (None, None));
    let options = Options {
        outcome_ids: Some(vec![1]),
        coins: Some(vec!["BTC".into()]),
        ..Options::default()
    }
    .normalized();
    assert_eq!(
        (options.outcome_ids, options.coins),
        (Some(vec![1]), Some(vec!["BTC".into()]))
    );
}

// meta -------------------------------------------------------------------

impl Harness {
    async fn details(&self) -> HashMap<i64, Row> {
        self.rows(&SPOT_TOKEN_DETAILS)
            .await
            .into_iter()
            .map(|row| (long(&row, "token_index").unwrap(), row))
            .collect()
    }

    fn asked_tokens(&self, since: usize) -> Vec<String> {
        let tokens = self.asked("tokenDetails", "tokenId", since);
        tokens
            .iter()
            .filter_map(|token| token.as_str().map(str::to_owned))
            .collect()
    }

    /// The spotMeta tokens: `(index, tokenId)`, in the answer's order.
    fn tokens(&self) -> Vec<(i64, String)> {
        self.fake.edit(|state| {
            state
                .spot_meta
                .get("tokens")
                .and_then(Json::as_arr)
                .unwrap()
                .iter()
                .map(|token| {
                    (
                        token.get("index").and_then(Json::as_i64).unwrap(),
                        token
                            .get("tokenId")
                            .and_then(Json::as_str)
                            .unwrap()
                            .to_owned(),
                    )
                })
                .collect()
        })
    }
}

const META_SPECS: [&TableSpec; 6] = [
    &SPOT_META,
    &SPOT_TOKENS,
    &SPOT_TOKEN_DETAILS,
    &SPOT_PAIRS,
    &PERP_DEXS,
    &PERP_ASSETS,
];

#[tokio::test]
async fn meta_snapshot_details_and_changes() {
    let h = Harness::new();
    let done = h.run(Which::Meta, Options::default()).await;
    assert_eq!(done.failures, no_failures());
    let tokens = h.rows(&SPOT_TOKENS).await;
    assert_eq!(tokens.len(), h.tokens().len());
    let details = h.details().await;
    // tokenDetails answered null for the others.
    assert_eq!(sorted(details.keys().copied().collect()), [0, 40, 437]);
    assert_eq!(
        text(&details[&437], "deployer"),
        Some("0x96fdb9e5c1a3d33f34109118457500b3d96c7094")
    );
    assert_eq!(h.fake.count("tokenDetails"), tokens.len());
    assert_eq!(
        done.summary["token_details_missing"],
        json!(tokens.len() - 3)
    );
    let meta = h.rows(&SPOT_META).await;
    assert_eq!(
        meta.iter()
            .map(|row| text(row, "payload_json"))
            .collect::<Vec<_>>(),
        [Some("{}")]
    );
    let pairs = h.fake.edit(|state| {
        state
            .spot_meta
            .get("universe")
            .and_then(Json::as_arr)
            .unwrap()
            .len()
    });
    assert_eq!(h.rows(&SPOT_PAIRS).await.len(), pairs);
    let dexs: HashMap<i64, Option<String>> = h
        .rows(&PERP_DEXS)
        .await
        .iter()
        .map(|row| {
            (
                long(row, "dex_index").unwrap(),
                text(row, "name").map(str::to_owned),
            )
        })
        .collect();
    assert_eq!(
        dexs,
        HashMap::from([
            (0, Some(String::new())),
            (1, Some("xyz".into())),
            (2, Some("flx".into()))
        ])
    );
    let assets = h.rows(&PERP_ASSETS).await;
    assert_eq!(assets.len(), 10);
    let xyz: Vec<&str> = assets
        .iter()
        .filter(|row| long(row, "dex_index") == Some(1))
        .filter_map(|row| text(row, "coin"))
        .collect();
    assert_eq!(sorted(xyz), ["xyz:NVDA", "xyz:TSLA", "xyz:XYZ100"]);

    // Unchanged metadata appends nothing. tokenDetails is asked again only for
    // a token without details, in that token's hour.
    h.clock.advance(HOUR);
    let versions = h.versions(&META_SPECS).await;
    let before = h.calls();
    h.run(Which::Meta, Options::default()).await;
    assert_eq!(h.versions(&META_SPECS).await, versions);
    let hour = hour_of(h.now());
    let due: Vec<String> = h
        .tokens()
        .into_iter()
        .filter(|(index, token_id)| !details.contains_key(index) && crc32_hour(token_id) == hour)
        .map(|(_, token_id)| token_id)
        .collect();
    assert_eq!(h.asked_tokens(before), due);

    // A changed token appends a row; a removed pair gets one removal row.
    h.fake.edit(|state| {
        let tokens = state
            .spot_meta
            .pointer_mut("/tokens")
            .unwrap()
            .as_arr_mut()
            .unwrap();
        let hsei = tokens
            .iter_mut()
            .find(|token| token.get("index").and_then(Json::as_i64) == Some(437))
            .unwrap();
        *hsei.pointer_mut("/fullName").unwrap() = Json::Str("hSEI renamed".into());
        let pairs = state
            .spot_meta
            .pointer_mut("/universe")
            .unwrap()
            .as_arr_mut()
            .unwrap();
        pairs.retain(|pair| pair.get("index").and_then(Json::as_i64) != Some(36));
        *state
            .all_perp_metas
            .pointer_mut("/0/universe/1/maxLeverage")
            .unwrap() = Json::Int("20".into());
    });
    for _ in 0..2 {
        h.clock.advance(HOUR);
        h.run(Which::Meta, Options::default()).await;
    }
    let history: Vec<Row> = h
        .rows(&SPOT_TOKENS)
        .await
        .into_iter()
        .filter(|row| long(row, "token_index") == Some(437))
        .collect();
    assert_eq!(history.len(), 2);
    let newest = by_time(history, "observed_at").pop().unwrap();
    assert_eq!(text(&newest, "full_name"), Some("hSEI renamed"));
    assert_eq!(h.rows(&SPOT_TOKEN_DETAILS).await.len(), 3);
    let removed: Vec<Option<bool>> = by_time(h.rows(&SPOT_PAIRS).await, "observed_at")
        .iter()
        .filter(|row| long(row, "pair_index") == Some(36))
        .map(Row::listed)
        .collect();
    assert_eq!(removed, [Some(true), Some(false)]);
    let eth: Vec<i64> = h
        .rows(&PERP_ASSETS)
        .await
        .iter()
        .filter(|row| text(row, "coin") == Some("ETH"))
        .filter_map(|row| long(row, "max_leverage"))
        .collect();
    assert_eq!(sorted(eth), [20, 25]);
}

#[tokio::test]
async fn meta_token_details_errors_are_asked_again_daily() {
    // /info answers HTTP 500 for one token's details: the token is kept
    // without them, and the run succeeds while /info answers the others.
    let h = Harness::new();
    let usdc = h
        .tokens()
        .into_iter()
        .find(|(index, _)| *index == 0)
        .unwrap()
        .1;
    h.statuses("tokenDetails", vec![500; 3]);
    let done = h.run(Which::Meta, Options::default()).await;
    assert_eq!(done.failures, no_failures());
    assert_eq!(h.rows(&SPOT_TOKENS).await.len(), h.tokens().len());
    assert_eq!(
        sorted(h.details().await.keys().copied().collect()),
        [40, 437]
    );

    let to_hour_of = |token_id: &str| {
        let hours = (i64::from(crc32_hour(token_id)) - i64::from(hour_of(h.now()))).rem_euclid(24);
        h.clock
            .advance((if hours == 0 { 24 } else { hours }) as f64 * HOUR);
    };
    // In USDC's hour it is asked once more, with a single attempt.
    to_hour_of(&usdc);
    h.statuses("tokenDetails", vec![500]);
    let before = h.calls();
    let done = h.run(Which::Meta, Options::default()).await;
    assert_eq!(done.failures, no_failures());
    assert_eq!(
        h.asked_tokens(before)
            .iter()
            .filter(|token_id| **token_id == usdc)
            .count(),
        1
    );
    assert_eq!(done.summary["token_details_retry_failed"], json!(1));
    // A day later a gateway's transient 503 gets the usual attempts, and
    // /info answers: a new row, which verify accepts.
    to_hour_of(&usdc);
    h.statuses("tokenDetails", vec![503]);
    let before = h.calls();
    let done = h.run(Which::Meta, Options::default()).await;
    assert_eq!(done.failures, no_failures());
    assert_eq!(
        h.asked_tokens(before)
            .iter()
            .filter(|token_id| **token_id == usdc)
            .count(),
        2
    );
    assert_eq!(
        sorted(h.details().await.keys().copied().collect()),
        [0, 40, 437]
    );
    assert_eq!(h.verify().await, no_failures());
}

#[tokio::test]
async fn an_outage_needs_three_500s_and_half_of_a_type_unless_named() {
    // tokenDetails answers HTTP 500 for about one token in three: the one new
    // token of an hourly run keeps its gap. An outage needs at least three
    // 500s and at least as many as the type's answers; a run that names its
    // entities fails on one, since nothing else refills a repair's gap.
    let h = Harness::new();
    let store = h.store();
    let error = InfoError {
        message: "tokenDetails(token=0x00): HTTP 500".into(),
        status: Some(500),
    };
    let failures = |options: Options, answered: u64, failed: u64| -> Vec<String> {
        let mut info = InfoClient::new(
            h.fake.clone(),
            100_000,
            h.clock.clone(),
            Box::new(|| 0.0),
            h.log.logger(),
        )
        .unwrap();
        let mut run = Run::new(
            &store,
            &mut info,
            options,
            h.log.logger(),
            "test".into(),
            h.clock.clone(),
        );
        for _ in 0..answered {
            run.answered("tokenDetails");
        }
        for index in 0..failed {
            run.entity_failed("tokenDetails", &error, ("token_index", json!(index)));
        }
        run.check_outages();
        run.failures
    };
    for failed in 1..OUTAGE_FAILURES {
        assert_eq!(
            failures(Options::default(), 0, failed),
            no_failures(),
            "{failed}"
        );
    }
    let outage = failures(Options::default(), 0, OUTAGE_FAILURES);
    assert_eq!(outage.len(), 1, "{outage:?}");
    assert!(outage[0].contains("tokenDetails: 3 of 3 requests failed with HTTP 500"));
    // At least half: 3 of 7 is not an outage, 3 of 6 is.
    assert_eq!(failures(Options::default(), 4, 3), no_failures());
    assert_eq!(failures(Options::default(), 3, 3).len(), 1);
    // A named run fails on one.
    assert_eq!(failures(coins(&["BTC"]), 0, 1).len(), 1);
    let named = Options {
        outcome_ids: Some(vec![8976]),
        ..Options::default()
    };
    assert_eq!(failures(named, 0, 1).len(), 1);
    assert_eq!(failures(coins(&["BTC"]), 2, 1), no_failures());
}

#[tokio::test]
async fn meta_a_token_details_outage_fails_the_run() {
    // Every tokenDetails answers HTTP 500: an outage, not single tokens.
    let h = Harness::new();
    h.statuses("tokenDetails", vec![500; 1000]);
    let done = h.run(Which::Meta, Options::default()).await;
    assert_eq!(done.failures.len(), 1, "{:?}", done.failures);
    assert!(done.failures[0].contains("tokenDetails: 9 of 9 requests failed with HTTP 500"));
    assert_eq!(h.rows(&SPOT_TOKENS).await.len(), h.tokens().len());
    assert!(h.details().await.is_empty());
    // Any other error stops asking: the new tokens wait for the next run.
    let other = h.root.join("other");
    h.statuses("tokenDetails", vec![503; 3]);
    let done = h
        .run_on(
            &h.store_at(&other),
            Which::Meta,
            Options::default(),
            100_000,
            None,
        )
        .await;
    assert!(!done.failures.is_empty());
    assert_eq!(done.summary["tokens_deferred"], json!(h.tokens().len()));
    // Three attempts each.
    assert_eq!(h.fake.count("tokenDetails"), 9 * 3 + 3);
}

#[tokio::test]
async fn meta_a_truncated_snapshot_writes_no_removal_rows() {
    let h = Harness::new();
    h.fake.edit(|state| {
        let pairs = state
            .spot_meta
            .pointer_mut("/universe")
            .unwrap()
            .as_arr_mut()
            .unwrap();
        let template = pairs.last().unwrap().clone();
        for index in 0..40 {
            let mut pair = template.clone();
            *pair.pointer_mut("/index").unwrap() = Json::Int((1000 + index).to_string());
            *pair.pointer_mut("/name").unwrap() = Json::Str(format!("@{}", 1000 + index));
            pairs.push(pair);
        }
    });
    assert_eq!(
        h.run(Which::Meta, Options::default()).await.failures,
        no_failures()
    );
    let count = h.rows(&SPOT_PAIRS).await.len();
    // HTTP 200 with only the first pairs: 40 or more pairs would be delisted.
    h.fake.edit(|state| {
        let pairs = state
            .spot_meta
            .pointer_mut("/universe")
            .unwrap()
            .as_arr_mut()
            .unwrap();
        pairs.truncate(3);
    });
    h.clock.advance(HOUR);
    let done = h.run(Which::Meta, Options::default()).await;
    assert_eq!(done.failures.len(), 1, "{:?}", done.failures);
    assert!(done.failures[0].contains("hl_spot_pairs: the snapshot lacks"));
    assert_eq!(h.rows(&SPOT_PAIRS).await.len(), count);
}

#[tokio::test]
async fn meta_new_fields_are_kept() {
    let h = Harness::new();
    h.fake.edit(|state| {
        let Json::Obj(spot) = &mut state.spot_meta else {
            panic!("an object")
        };
        spot.insert(
            "newTopLevel".into(),
            Json::Arr(vec![Json::Int("1".into()), Json::Int("2".into())]),
        );
        let Some(Json::Obj(dex)) = state.all_perp_metas.pointer_mut("/1") else {
            panic!("an object")
        };
        dex.insert("newDexField".into(), Json::Str("x".into()));
    });
    h.run(Which::Meta, Options::default()).await;
    let meta = h.rows(&SPOT_META).await;
    assert_eq!(
        meta.iter()
            .map(|row| text(row, "payload_json"))
            .collect::<Vec<_>>(),
        [Some(r#"{"newTopLevel":[1,2]}"#)]
    );
    let dexs = h.rows(&PERP_DEXS).await;
    let xyz = dexs
        .iter()
        .find(|row| long(row, "dex_index") == Some(1))
        .unwrap();
    assert!(text(xyz, "payload_json")
        .unwrap()
        .contains(r#""newDexField":"x""#));
}

#[tokio::test]
async fn meta_token_details_wait_for_budget() {
    let h = Harness::new();
    // The three snapshot requests fill the window; the first tokenDetails
    // would wait a minute, past the budget.
    let done = h
        .run_on(&h.store(), Which::Meta, Options::default(), 60, Some(10.0))
        .await;
    assert_eq!(done.failures, no_failures());
    assert_eq!(done.summary["tokens_deferred"], json!(h.tokens().len()));
    assert!(h.rows(&SPOT_TOKENS).await.is_empty());
    assert!(!h.rows(&SPOT_PAIRS).await.is_empty());
    h.clock.advance(HOUR);
    h.run(Which::Meta, Options::default()).await;
    assert_eq!(h.rows(&SPOT_TOKENS).await.len(), h.tokens().len());
}

#[tokio::test]
async fn meta_dex_lists_of_different_lengths() {
    let h = Harness::new();
    h.fake
        .edit(|state| state.perp_dexs.as_arr_mut().unwrap().truncate(2));
    let done = h.run(Which::Meta, Options::default()).await;
    assert_eq!(done.failures, no_failures());
    let dexs: Vec<i64> = h
        .rows(&PERP_DEXS)
        .await
        .iter()
        .filter_map(|row| long(row, "dex_index"))
        .collect();
    assert_eq!(sorted(dexs), [0, 1]);
    // Read again once.
    assert_eq!(h.fake.count("perpDexs"), 2);
}

// funding ----------------------------------------------------------------

fn funding_options() -> Options {
    Options {
        funding_page: 2,
        ..Options::default()
    }
}

fn coins(names: &[&str]) -> Options {
    Options {
        coins: Some(names.iter().map(|name| (*name).to_owned()).collect()),
        ..funding_options()
    }
}

impl Harness {
    /// The fixture's fundingHistory entries of `coin`.
    fn entries(&self, coin: &str) -> Vec<Json> {
        self.fake.edit(|state| state.funding[coin].clone())
    }
}

fn time_of(entry: &Json) -> i64 {
    entry.get("time").and_then(Json::as_i64).unwrap()
}

#[tokio::test]
async fn funding_incremental_pages_and_reruns() {
    let h = Harness::funding();
    // The delisted MATIC is read only in its own hour of the day (07 UTC).
    assert_eq!(crc32_hour("MATIC"), 7);
    let done = h.run(Which::Funding, funding_options()).await;
    assert_eq!(done.failures, no_failures());
    let stored = h.rows(&FUNDING_HISTORY).await;
    assert_eq!(stored.len(), 12);
    let keys: HashSet<(Option<String>, i64)> = stored
        .iter()
        .map(|row| {
            (
                text(row, "coin").map(str::to_owned),
                stamp(row, "funding_time"),
            )
        })
        .collect();
    assert_eq!(keys.len(), 12);
    let btc = sorted(
        stored
            .iter()
            .filter(|row| text(row, "coin") == Some("BTC"))
            .map(|row| stamp(row, "funding_time"))
            .collect(),
    );
    assert_eq!(Some(btc[0]), from_millis(time_of(&h.entries("BTC")[0])));
    // Three pages of two for each listed coin with data (the fourth is
    // empty), one request for the other listed coins; the delisted MATIC and
    // both flx coins wait for their hours.
    assert_eq!(h.fake.count("fundingHistory"), 2 * 4 + 5);
    assert!(!h.asked_coins(0).contains(&"MATIC".to_owned()));

    // Within the same hour every stored coin is skipped.
    let before = h.calls();
    h.clock.advance(60.0);
    h.run(Which::Funding, funding_options()).await;
    let asked = h.asked_coins(before);
    assert!(!asked
        .iter()
        .any(|coin| ["BTC", "xyz:XYZ100", "MATIC"].contains(&coin.as_str())));
    assert_eq!(h.rows(&FUNDING_HISTORY).await.len(), 12);

    // The next hour resumes after the stored hour; MATIC still waits.
    let last = time_of(h.entries("BTC").last().unwrap());
    h.fake.edit(|state| {
        state.funding.get_mut("BTC").unwrap().push(Json::obj([
            ("coin", Json::Str("BTC".into())),
            ("fundingRate", Json::Str("0.0000125".into())),
            ("premium", Json::Str("-0.0001".into())),
            ("time", Json::Int((last + 3_600_000).to_string())),
        ]))
    });
    h.clock.advance(HOUR);
    let before = h.calls();
    h.run(Which::Funding, funding_options()).await;
    assert!(!h.asked_coins(before).contains(&"MATIC".to_owned()));
    let btc_start = h.fake.calls()[before..]
        .iter()
        .find(|call| {
            call.get("type").and_then(Json::as_str) == Some("fundingHistory")
                && call.get("coin").and_then(Json::as_str) == Some("BTC")
        })
        .and_then(|call| call.get("startTime").and_then(Json::as_i64));
    assert_eq!(btc_start, Some(last + 1));
    assert_eq!(h.rows(&FUNDING_HISTORY).await.len(), 13);

    // At 07 UTC MATIC's history is read, once: the 08 UTC run skips it again.
    h.clock.advance(HOUR);
    let before = h.calls();
    h.run(Which::Funding, funding_options()).await;
    assert!(h.asked_coins(before).contains(&"MATIC".to_owned()));
    let matic = h.rows(&FUNDING_HISTORY).await;
    assert_eq!(
        matic
            .iter()
            .filter(|row| text(row, "coin") == Some("MATIC"))
            .count(),
        6
    );
    h.clock.advance(HOUR);
    let before = h.calls();
    h.run(Which::Funding, funding_options()).await;
    assert!(!h.asked_coins(before).contains(&"MATIC".to_owned()));
    // Naming it reads it at any hour.
    let before = h.calls();
    h.run(Which::Funding, coins(&["MATIC"])).await;
    assert_eq!(h.asked_coins(before), ["MATIC"]);
    assert_eq!(h.rows(&FUNDING_HISTORY).await.len(), 19);
    assert_eq!(h.verify().await, no_failures());
}

#[tokio::test]
async fn funding_coins_restrict_the_incremental_run() {
    let h = Harness::funding();
    h.run(Which::Funding, coins(&["BTC"])).await;
    let rows = h.rows(&FUNDING_HISTORY).await;
    let stored: BTreeSet<&str> = rows.iter().filter_map(|row| text(row, "coin")).collect();
    assert_eq!(stored, BTreeSet::from(["BTC"]));
    let asked: BTreeSet<String> = h.asked_coins(0).into_iter().collect();
    assert_eq!(asked, BTreeSet::from(["BTC".to_owned()]));
}

#[tokio::test]
async fn funding_repair_window_fills_gaps_behind_the_cursor_only() {
    let h = Harness::funding();
    let entries = h.entries("BTC");
    let store = h.store();
    let mut table = store
        .open(&FUNDING_HISTORY, OpenMode::Write)
        .await
        .unwrap()
        .unwrap();
    // Hours 0, 1, 2 and 4 are stored; 3 is missing and 5 is ahead of the cursor.
    let at = h.now();
    let rows = [0, 1, 2, 4]
        .iter()
        .map(|index| funding_row(&entries[*index], at))
        .collect();
    store.append(&mut table, rows).await.unwrap();
    let options = Options {
        // 2026-01-01T06:00:00Z.
        funding_end: Some(1_767_247_200_000_000),
        ..coins(&["BTC", "ETH"])
    };
    let done = h.run(Which::Funding, options.clone()).await;
    let times = sorted(
        h.rows(&FUNDING_HISTORY)
            .await
            .iter()
            .map(|row| stamp(row, "funding_time"))
            .collect(),
    );
    let expected: Vec<i64> = entries[..5]
        .iter()
        .map(|entry| from_millis(time_of(entry)).unwrap())
        .collect();
    assert_eq!(times, expected);
    // ETH has no history to repair.
    assert_eq!(done.summary["coins_skipped"], json!(1));
    h.run(Which::Funding, options).await;
    assert_eq!(h.rows(&FUNDING_HISTORY).await.len(), 5);
    // The incremental run then continues after the cursor.
    h.run(Which::Funding, coins(&["BTC"])).await;
    assert_eq!(h.rows(&FUNDING_HISTORY).await.len(), 6);
    assert_eq!(h.verify().await, no_failures());
}

#[tokio::test]
async fn funding_a_coin_named_twice_is_repaired_once() {
    let h = Harness::funding();
    let entries = h.entries("BTC");
    let store = h.store();
    let mut table = store
        .open(&FUNDING_HISTORY, OpenMode::Write)
        .await
        .unwrap()
        .unwrap();
    // Hours 0 and 4 are stored: 1, 2 and 3 are missing behind the cursor.
    let at = h.now();
    let rows = [0, 4]
        .iter()
        .map(|index| funding_row(&entries[*index], at))
        .collect();
    store.append(&mut table, rows).await.unwrap();
    let options = Options {
        // 2026-01-01T06:00:00Z; a commit after every coin.
        funding_end: Some(1_767_247_200_000_000),
        commit_rows: 1,
        ..coins(&["BTC", "BTC"])
    };
    let done = h.run(Which::Funding, options).await;
    assert_eq!(done.failures, no_failures());
    let times = sorted(
        h.rows(&FUNDING_HISTORY)
            .await
            .iter()
            .map(|row| stamp(row, "funding_time"))
            .collect(),
    );
    let expected: Vec<i64> = entries[..5]
        .iter()
        .map(|entry| from_millis(time_of(entry)).unwrap())
        .collect();
    assert_eq!(times, expected);
    assert_eq!(done.summary["coins"], json!(1));
    assert_eq!(h.verify().await, no_failures());
}

#[tokio::test]
async fn funding_a_repair_that_fetches_nothing_fails() {
    // A named repair whose only coin answers HTTP 500 (three attempts) fills
    // no gap, and the scheduled runs read only after the cursor: it fails.
    let h = Harness::funding();
    let entries = h.entries("BTC");
    let store = h.store();
    let mut table = store
        .open(&FUNDING_HISTORY, OpenMode::Write)
        .await
        .unwrap()
        .unwrap();
    let at = h.now();
    let rows = [0, 4]
        .iter()
        .map(|index| funding_row(&entries[*index], at))
        .collect();
    store.append(&mut table, rows).await.unwrap();
    h.statuses("fundingHistory", vec![500; 3]);
    let options = Options {
        // 2026-01-01T06:00:00Z.
        funding_end: Some(1_767_247_200_000_000),
        ..coins(&["BTC"])
    };
    let done = h.run(Which::Funding, options).await;
    assert_eq!(done.failures.len(), 1, "{:?}", done.failures);
    assert!(done.failures[0].contains("fundingHistory: 1 of 1 requests failed with HTTP 500"));
    assert_eq!(h.rows(&FUNDING_HISTORY).await.len(), 2);
}

#[tokio::test]
async fn funding_a_funding_outage_fails_the_run() {
    let h = Harness::funding();
    h.statuses("fundingHistory", vec![500; 1000]);
    let done = h.run(Which::Funding, funding_options()).await;
    assert_eq!(done.failures.len(), 1, "{:?}", done.failures);
    assert!(done.failures[0].contains("fundingHistory: 7 of 7 requests failed with HTTP 500"));
    assert_eq!(done.summary["coins_failed"], json!(7));
}

#[tokio::test]
async fn funding_one_failing_coin_is_its_gap_until_it_falls_behind() {
    let h = Harness::funding();
    // HTTP 500 for the first coin asked (three attempts); the others answer.
    h.statuses("fundingHistory", vec![500; 3]);
    let done = h.run(Which::Funding, funding_options()).await;
    assert_eq!(
        (done.failures, &done.summary["coins_failed"]),
        (no_failures(), &json!(1))
    );
    let asked = h.asked_coins(0);
    assert_eq!(
        asked[..3],
        [asked[0].clone(), asked[0].clone(), asked[0].clone()]
    );
    assert_eq!(asked[0], "ATOM");
    let stored: BTreeSet<String> = h
        .rows(&FUNDING_HISTORY)
        .await
        .iter()
        .filter_map(|row| text(row, "coin").map(str::to_owned))
        .collect();
    assert_eq!(
        stored,
        BTreeSet::from(["BTC".to_owned(), "xyz:XYZ100".to_owned()])
    );
    // Three hours later /info has no new hour for any listed coin: a
    // complete run reports the coins it stored hours for as behind.
    h.clock.advance(3.0 * HOUR);
    let done = h.run(Which::Funding, funding_options()).await;
    assert_eq!(done.failures.len(), 1, "{:?}", done.failures);
    assert!(done.failures[0].contains(
        "2 listed coins are more than 2 hours behind after a complete run: BTC, xyz:XYZ100"
    ));
    // A run its budget ends, or that names coins, is not complete.
    h.clock.advance(HOUR);
    let done = h
        .run_on(&h.store(), Which::Funding, funding_options(), 20, Some(1.0))
        .await;
    assert_eq!(
        (done.failures, &done.summary["funding_budget_exhausted"]),
        (no_failures(), &json!(true))
    );
    let done = h.run(Which::Funding, coins(&["xyz:XYZ100"])).await;
    assert_eq!(done.failures, no_failures());
}

// verify -----------------------------------------------------------------

/// The `verified` events of the log since `since` lines, by table.
fn verified(log: &MemoryLog, since: usize) -> HashMap<String, Value> {
    log.events()[since..]
        .iter()
        .filter(|event| event["event"] == "verified" && event.get("table").is_some())
        .map(|event| (event["table"].as_str().unwrap().to_owned(), event.clone()))
        .collect()
}

#[tokio::test]
async fn verify_finds_bad_hashes_and_duplicates() {
    let h = Harness::new();
    h.run(Which::Meta, Options::default()).await;
    // MATIC's hour.
    h.clock.set_wall(1_767_252_600_000_000);
    h.run(Which::Funding, Options::default()).await;
    let since = h.log.events().len();
    assert_eq!(h.verify().await, no_failures());
    let clean = verified(&h.log, since);
    assert_eq!(
        clean["hl_spot_pairs"]["entities"],
        clean["hl_spot_pairs"]["rows"]
    );

    let store = h.store();
    let mut pairs = store
        .open(&SPOT_PAIRS, OpenMode::Write)
        .await
        .unwrap()
        .unwrap();
    let columns: Vec<&str> = SPOT_PAIRS
        .columns
        .iter()
        .map(|column| column.name)
        .collect();
    let latest = store.latest_with(&pairs, &columns).await.unwrap();
    let mut keys: Vec<_> = latest.keys().cloned().collect();
    keys.sort();
    let later = h.now();
    // The same payload again, a hash that is not its payload's, and two
    // removals in a row.
    let mut again = latest[&keys[0]].clone();
    again.set("observed_at", Cell::Timestamp(later));
    let mut wrong = latest[&keys[1]].clone();
    wrong.set("observed_at", Cell::Timestamp(later));
    wrong.set("payload_sha256", Cell::Text("0".repeat(64)));
    store.append(&mut pairs, vec![again, wrong]).await.unwrap();
    let removal = removal_row(&latest[&keys[2]], later, "spotMeta");
    store
        .append(&mut pairs, vec![removal.clone()])
        .await
        .unwrap();
    let mut repeated = removal;
    repeated.set("observed_at", Cell::Timestamp(h.now()));
    store.append(&mut pairs, vec![repeated]).await.unwrap();
    let mut funding = store
        .open(&FUNDING_HISTORY, OpenMode::Write)
        .await
        .unwrap()
        .unwrap();
    let hour = funding_row(&h.entries("BTC")[0], h.now());
    store.append(&mut funding, vec![hour]).await.unwrap();

    let since = h.log.events().len();
    let failures = h.verify().await;
    let found = verified(&h.log, since);
    let counts = |table: &str| {
        (
            found[table]["bad_hash"].clone(),
            found[table]["duplicates"].clone(),
        )
    };
    assert_eq!(counts("hl_spot_pairs"), (json!(1), json!(2)));
    assert_eq!(
        found["hl_spot_pairs"]["entities"],
        clean["hl_spot_pairs"]["entities"]
    );
    assert_eq!(counts("hl_funding_history"), (json!(0), json!(1)));
    assert_eq!(found["hl_spot_tokens"]["duplicates"], json!(0));
    assert_eq!(failures.len(), 2, "{failures:?}");
}

#[tokio::test]
async fn verify_finds_outcomes_naming_unknown_questions() {
    let h = Harness::new();
    let options = Options {
        outcome_ids: Some(vec![7768]),
        ..Options::default()
    };
    h.run(Which::Outcomes, options).await;
    let store = h.store();
    let mut outcomes = store
        .open(&OUTCOMES, OpenMode::Write)
        .await
        .unwrap()
        .unwrap();
    let response = h.fake.edit(|state| state.settled[&7768].clone());
    let mut row = outcome_settled_row(&response, h.now());
    row.set("outcome_id", Cell::Long(7769));
    row.set("question_id", Cell::Long(999));
    store.append(&mut outcomes, vec![row]).await.unwrap();
    assert_eq!(
        h.verify().await,
        ["hl_outcomes names 1 questions without a hl_questions row: 999"]
    );
}

// maintain ---------------------------------------------------------------

#[tokio::test]
async fn maintain_forces_existing_tables_and_skips_missing_ones() {
    let h = Harness::new();
    h.run(
        Which::Outcomes,
        Options {
            maintenance: false,
            ..sweep(9970)
        },
    )
    .await;
    let since = h.log.events().len();
    let tables = vec![OUTCOME_PROBES.name.to_owned(), SPOT_PAIRS.name.to_owned()];
    let done = h
        .run(Which::Maintain(Some(tables)), Options::default())
        .await;
    assert_eq!(done.failures, no_failures());
    let events = h.log.events()[since..].to_vec();
    assert_eq!(tables_of(&events, "skipped"), [SPOT_PAIRS.name]);
    // One file: nothing to compact, and nothing to vacuum yet.
    assert_eq!(tables_of(&events, "maintained"), [OUTCOME_PROBES.name]);
    let maintained = events
        .iter()
        .find(|event| event["event"] == "maintained")
        .unwrap();
    assert_eq!(
        (
            &maintained["files_before"],
            &maintained["files_removed"],
            &maintained["vacuum_deleted"]
        ),
        (&json!(1), &json!(0), &json!(0))
    );

    // No list and an empty list both name every table.
    let existing: Vec<&str> = ALL_TABLES
        .iter()
        .map(|spec| spec.name)
        .filter(|name| h.root.join(name).join("_delta_log").is_dir())
        .collect();
    let missing: Vec<&str> = ALL_TABLES
        .iter()
        .map(|spec| spec.name)
        .filter(|name| !existing.contains(name))
        .collect();
    assert_eq!(existing.len(), 4, "{existing:?}");
    for tables in [None, Some(Vec::new())] {
        let since = h.log.events().len();
        let done = h.run(Which::Maintain(tables), Options::default()).await;
        assert_eq!(done.failures, no_failures());
        let events = h.log.events()[since..].to_vec();
        assert_eq!(tables_of(&events, "maintained"), existing);
        assert_eq!(tables_of(&events, "skipped"), missing);
    }
}

/// The `table` of each `event` line, in order.
fn tables_of(events: &[Value], event: &str) -> Vec<String> {
    events
        .iter()
        .filter(|line| line["event"] == event)
        .map(|line| line["table"].as_str().unwrap().to_owned())
        .collect()
}
