//! The three snapshot jobs, and the maintain and verify commands.
//!
//! Each job owns its tables, and only it writes them (one CronJob each):
//!
//! - outcomes: hl_outcome_meta, hl_outcomes, hl_questions and the probe
//!   state. outcomeMeta, then settledOutcome for outcomes that left it and,
//!   newest first, for every id from 0 to the highest known that has no
//!   settled row yet. That sweep is the HIP-4 backfill: it resumes from the
//!   tables themselves on every run.
//! - meta: hl_spot_meta, hl_spot_tokens, hl_spot_token_details, hl_spot_pairs,
//!   hl_perp_dexs and hl_perp_assets, from spotMeta, perpDexs and
//!   allPerpMetas, with tokenDetails once per new token (and once a day for a
//!   token `/info` failed).
//! - funding: hl_funding_history, from fundingHistory for every perp coin of
//!   every dex, resuming each coin after its latest stored hour (from
//!   `--funding-start`, 2026-01-01, for a coin with none). Delisted coins are
//!   read once a day, each in its own UTC hour.
//!
//! A run whose time budget ends commits what it fetched and exits 0; the next
//! run continues. A run that fails (an `/info` outage or refusal after
//! retries, a failed commit) still commits the rows it completed, then exits
//! with status 1. HTTP 500 for one entity (a token's details, an outcome id,
//! a coin) is that entity's gap, logged as a warning, while `/info` answers
//! most requests of the same type; when at least half of a type's requests
//! fail, the run fails ([`Run::check_outages`]). A snapshot that would delist
//! more than a bounded share of a table's entities writes no removal row and
//! fails the run ([`Run::bounded_removals`]).

mod funding;
mod maintain;
mod meta;
mod outcomes;
#[cfg(test)]
pub(crate) mod tests;
mod verify;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde_json::{json, Map, Value};

pub use self::funding::run_funding;
pub use self::maintain::run_maintain;
pub use self::meta::run_meta;
pub use self::outcomes::run_outcomes;
pub use self::verify::run_verify;
use crate::clock::Clock;
use crate::info::{InfoClient, InfoError, InfoFailure, FUNDING_PAGE};
use crate::json::Json;
use crate::log::Logger;
use crate::model::{Key, Row, TableSpec};
use crate::store::{MaintainOptions, OpenMode, Store, StoreError, Table};
use crate::values::{valid_id, Ordered};

/// 2026-01-01T00:00:00Z: where a coin without stored hours starts.
pub const FUNDING_START: i64 = 1_767_225_600_000_000;

/// A snapshot may delist at most this many entities of a table, or a fifth of
/// its listed entities if more ([`Run::bounded_removals`]).
const MASS_REMOVAL_MIN: usize = 25;

/// A job's settings: the command line's, and the fixed ones.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    /// The sweep's probes per run (named ids are all probed).
    pub max_probes: u32,
    /// How long an id that was not settled cools down before another probe.
    pub reprobe_after_us: i64,
    /// Ids past the highest known one the sweep probes (fixed: 3).
    pub lookahead: i64,
    pub sweep_from: i64,
    pub sweep_to: Option<i64>,
    /// Probe exactly these ids (a repair); no sweep.
    pub outcome_ids: Option<Vec<i64>>,
    /// µs.
    pub funding_start: i64,
    /// µs; set, the funding run repairs `[funding_start, funding_end)`.
    pub funding_end: Option<i64>,
    pub coins: Option<Vec<String>>,
    /// Funding rows per commit.
    pub commit_rows: usize,
    /// Whether a run without failures compacts and vacuums its tables.
    pub maintenance: bool,
    /// OPTIMIZE once a table has this many files (fixed: 24).
    pub compact_min_files: usize,
    pub allow_mass_removal: bool,
    /// The most fundingHistory items in one answer: fewer ends a coin.
    pub funding_page: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_probes: 80,
            reprobe_after_us: 3_600_000_000,
            lookahead: 3,
            sweep_from: 0,
            sweep_to: None,
            outcome_ids: None,
            funding_start: FUNDING_START,
            funding_end: None,
            coins: None,
            commit_rows: 50_000,
            maintenance: true,
            compact_min_files: 24,
            allow_mass_removal: false,
            funding_page: FUNDING_PAGE,
        }
    }
}

impl Options {
    /// Python's falsy test: an empty list of outcome ids or coins is "not
    /// given" (the sweep, every coin). [`Run::new`] applies it.
    pub fn normalized(mut self) -> Self {
        self.outcome_ids = self.outcome_ids.filter(|ids| !ids.is_empty());
        self.coins = self.coins.filter(|coins| !coins.is_empty());
        self
    }
}

/// How a job ended other than normally.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobError {
    /// A table this release cannot extend: `config_error`, exit 2, no `done`.
    Config(String),
    /// Recorded as a run failure; the job stopped there.
    Fatal(String),
}

impl From<StoreError> for JobError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Config(message) => JobError::Config(message),
            StoreError::Failed(message) => JobError::Fatal(message),
        }
    }
}

impl From<InfoFailure> for JobError {
    /// A snapshot request that failed, or whose limiter wait would end after
    /// the budget: the Python job's `"BudgetExhausted: "` for the latter.
    fn from(failure: InfoFailure) -> Self {
        match failure {
            InfoFailure::Budget => JobError::Fatal("BudgetExhausted: ".into()),
            InfoFailure::Error(error) => JobError::Fatal(error.message),
        }
    }
}

/// One run of a job: its store and `/info` client, what failed, and the
/// summary `done` reports.
pub struct Run<'a> {
    pub store: &'a Store,
    pub info: &'a mut InfoClient,
    pub options: Options,
    pub log: Logger,
    pub run_id: String,
    pub clock: Arc<dyn Clock>,
    pub failures: Vec<String>,
    pub summary: Map<String, Value>,
    /// Per entity request type: answered, and failed with HTTP 500.
    answers: BTreeMap<&'static str, (u64, u64)>,
    /// Per entity request type, the first [`GAPS_NAMED`] entities that failed.
    gaps: BTreeMap<&'static str, Vec<String>>,
}

/// How many failed entities a failure message names.
const GAPS_NAMED: usize = 5;

/// The fewest HTTP 500 entity failures of a request type that make an outage.
pub const OUTAGE_FAILURES: u64 = 3;

impl<'a> Run<'a> {
    pub fn new(
        store: &'a Store,
        info: &'a mut InfoClient,
        options: Options,
        log: Logger,
        run_id: String,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            store,
            info,
            options: options.normalized(),
            log,
            run_id,
            clock,
            failures: Vec::new(),
            summary: Map::new(),
            answers: BTreeMap::new(),
            gaps: BTreeMap::new(),
        }
    }

    /// The UTC wall clock, µs.
    pub fn now(&self) -> i64 {
        self.clock.now_micros()
    }

    /// Records a run failure, and logs it.
    pub fn fail(&mut self, message: String) {
        self.log.emit("error", json!({"message": message}));
        self.failures.push(message);
    }

    /// Adds `count` to the summary's `key`.
    pub fn count(&mut self, key: &str, count: u64) {
        let total = self.summary.get(key).and_then(Value::as_u64).unwrap_or(0);
        self.summary.insert(key.into(), json!(total + count));
    }

    /// An entity request of `kind` was answered.
    pub fn answered(&mut self, kind: &'static str) {
        self.answers.entry(kind).or_default().0 += 1;
    }

    /// HTTP 500 for one entity: its gap, unless the whole type fails
    /// ([`Self::check_outages`]). Logged as a warning naming the entity.
    pub fn entity_failed(&mut self, kind: &'static str, error: &InfoError, context: (&str, Value)) {
        self.answers.entry(kind).or_default().1 += 1;
        let gaps = self.gaps.entry(kind).or_default();
        if gaps.len() < GAPS_NAMED {
            gaps.push(match &context.1 {
                Value::String(name) => name.clone(),
                other => other.to_string(),
            });
        }
        let mut fields = Map::new();
        fields.insert("message".into(), json!(error.message));
        fields.insert(context.0.into(), context.1);
        self.log.emit("warning", Value::Object(fields));
    }

    /// Fails the run when at least half of a request type's requests got
    /// HTTP 500, and at least [`OUTAGE_FAILURES`] of them: `/info` is failing
    /// that type, not a few entities. A lone failure stays its entity's gap:
    /// `tokenDetails` answers 500 for about one token in three, so the one
    /// new token of an hourly run would otherwise fail it. A run that names
    /// its entities (`--coins`, `--outcome-ids`) fails on any of them: a
    /// scheduled run never refills a repair's gap behind a coin's cursor, so
    /// a repair that missed one must not end as if it had not.
    pub fn check_outages(&mut self) {
        let named = self.options.coins.is_some() || self.options.outcome_ids.is_some();
        let mut failures = Vec::new();
        for (kind, (answered, failed)) in &self.answers {
            let total = answered + failed;
            if named && *failed > 0 {
                let mut gaps = self.gaps.get(kind).cloned().unwrap_or_default();
                if *failed > gaps.len() as u64 {
                    gaps.push("…".into());
                }
                failures.push(format!(
                    "{kind}: {failed} of {total} requests failed with HTTP 500 ({}), and a run \
                     that names its entities must read them all",
                    gaps.join(", ")
                ));
            } else if *failed >= OUTAGE_FAILURES && failed >= answered {
                failures.push(format!(
                    "{kind}: {failed} of {total} requests failed with HTTP 500: /info is failing \
                     this request type, not single entities"
                ));
            }
        }
        for failure in failures {
            self.fail(failure);
        }
    }

    /// Appends `rows` (no commit for none) and counts them in the summary
    /// under the table's name, 0 included. A failed commit is recorded and
    /// returns false; a failed checkpoint or log cleanup is recorded, but the
    /// rows landed.
    pub async fn commit(&mut self, t: &mut Table, rows: Vec<Row>) -> bool {
        let count = rows.len() as u64;
        match self.store.append(t, rows).await {
            Ok(appended) => {
                self.count(t.spec.name, count);
                if let Some(error) = appended.and_then(|appended| appended.checkpoint_error) {
                    self.fail(error);
                }
                true
            }
            Err(error) => {
                self.fail(error.to_string());
                false
            }
        }
    }

    /// Compacts and vacuums `tables`, in order, unless maintenance is off or
    /// the run failed; the first failure stops it.
    pub async fn maintain(&mut self, tables: &mut [&mut Table]) {
        if !self.options.maintenance || !self.failures.is_empty() {
            return;
        }
        let options = MaintainOptions {
            compact_min_files: self.options.compact_min_files,
            ..MaintainOptions::default()
        };
        for t in tables.iter_mut() {
            if let Err(error) = self.store.maintain(t, &options).await {
                self.fail(error.to_string());
                return;
            }
        }
    }

    /// The removal rows, unless they would delist more than the bound.
    ///
    /// An empty or truncated answer (HTTP 200 with fewer entities) would
    /// otherwise append a permanent `listed = false` row for every entity it
    /// lacks. Above the larger of 25 and a fifth of the listed entities the
    /// run writes none and fails, so a persisting cause alerts;
    /// `--allow-mass-removal` records a real mass removal.
    pub fn bounded_removals(
        &mut self,
        spec: &TableSpec,
        latest: &HashMap<Key, Row>,
        removals: Vec<Row>,
    ) -> Vec<Row> {
        if removals.is_empty() || self.options.allow_mass_removal {
            return removals;
        }
        let listed = latest
            .values()
            .filter(|row| row.listed() == Some(true))
            .count();
        let bound = MASS_REMOVAL_MIN.max(listed / 5);
        if removals.len() <= bound {
            return removals;
        }
        self.fail(format!(
            "{}: the snapshot lacks {} of the {listed} listed entities (more than {bound}); \
             no removal row was written. If /info really dropped them, run with \
             --allow-mass-removal",
            spec.name,
            removals.len()
        ));
        Vec::new()
    }
}

/// The table, created, or extended with this release's columns.
async fn writable(run: &Run<'_>, spec: &'static TableSpec) -> Result<Table, JobError> {
    run.store
        .open(spec, OpenMode::Write)
        .await?
        .ok_or_else(|| JobError::Fatal(format!("{}: the table was not created", spec.name)))
}

/// The list at `key` of an answer the client validated.
fn items<'a>(value: &'a Json, key: &str) -> &'a [Json] {
    value.get(key).and_then(Json::as_arr).unwrap_or_default()
}

/// The items of `list` keyed by their valid id at `key`, as a Python dict: an
/// item without one is left out, a repeated id keeps its first position and
/// its last item.
fn keyed<'a>(list: &'a [Json], key: &str) -> Ordered<i64, &'a Json> {
    list.iter()
        .filter_map(|item| Some((valid_id(item.get(key))?, item)))
        .collect()
}

/// Whether `row` is new for its entity: no latest row, or another payload
/// hash (a removal row's NULL hash differs from any).
fn changed(latest: &HashMap<Key, Row>, row: &Row) -> bool {
    latest
        .get(&row.key())
        .is_none_or(|previous| previous.payload_sha256() != row.payload_sha256())
}

/// The removal rows of the latest rows `gone` picks, in key order.
fn removals(
    latest: &HashMap<Key, Row>,
    gone: impl Fn(&Key, &Row) -> bool,
    at: i64,
    source: &str,
) -> Vec<Row> {
    let mut keys: Vec<&Key> = latest
        .iter()
        .filter(|(key, row)| row.listed() == Some(true) && gone(key, row))
        .map(|(key, _)| key)
        .collect();
    keys.sort();
    keys.into_iter()
        .map(|key| crate::model::removal_row(&latest[key], at, source))
        .collect()
}

/// A key's first part, as an id.
fn id_of(key: &Key) -> i64 {
    match key.first() {
        Some(crate::model::KeyPart::Long(id)) => *id,
        _ => i64::MIN,
    }
}

/// The UTC hour of a time in µs.
fn hour_of(micros: i64) -> u32 {
    micros.div_euclid(3_600_000_000).rem_euclid(24) as u32
}
