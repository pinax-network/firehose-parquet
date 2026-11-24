//! Step 1, OPTIMIZE, of one table: its dates, up to `OPTIMIZE_CONCURRENT_DATES`
//! at once (#737).
//!
//! - One [`Planner`] per table hands out each planned date once, newest first,
//!   so no two compactions ever touch the same date. Two would both commit
//!   and duplicate its rows (#717): delta-rs's conflict checker ignores
//!   `remove` actions with `dataChange: false`. That is also why a table's
//!   dates are spread over the tasks of one run, never over two jobs.
//! - Each date is one task on the runtime's threads, with a clone of the table
//!   as it was when the date started, and is one commit. Only that task
//!   removes the date's files, so they are all in that snapshot, and delta-rs
//!   checks its commit against every commit since, as with one date at a
//!   time. A file belongs to one date, so the commits of two dates never touch
//!   the same file: one that loses the version race is rebased like any
//!   other, and one still losing after delta-rs's retries is a conflict, left
//!   to the next run. A failed date stops no other. Each running date keeps
//!   its snapshot's file list in memory (about 40 MB per 100,000 files).
//! - After each commit the table is reloaded, as one date at a time does, and
//!   again if a date commits while it loads: a date starts only from a table
//!   reloaded after every commit so far. The reload is of a clone, so the
//!   dates that finish meanwhile are still reported at once.
//! - A repair (`OPTIMIZE_REPAIR_DATES`) takes its turn when it starts and gives
//!   it back if it fails, so repairs running and committed never exceed the
//!   budget. A date to repair while every turn is taken by a running repair
//!   waits for one to finish (other dates go ahead): a failed repair then
//!   passes its turn on, as with one date at a time, and a date that keeps
//!   failing holds back no other.

use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::time::Instant;

use deltalake_core::{DeltaResult, DeltaTable, DeltaTableError};
use futures::future::BoxFuture;
use futures::FutureExt as _;
use serde_json::json;
use tokio::task::{Id, JoinError, JoinSet};

use crate::compact::{Bin, DateCompaction};
use crate::{emit, object, seconds, Report};

/// A date to compact, handed out once.
pub(crate) struct DatePlan {
    pub date: String,
    pub bins: Vec<Bin>,
    /// A bin is sorted back into the writer's order: it holds a repair turn.
    pub repairing: bool,
}

/// Hands out a table's planned dates (see the module documentation).
pub(crate) struct Planner {
    /// Not handed out yet, newest first.
    queue: VecDeque<(String, Vec<Bin>)>,
    /// Dates to repair while every turn is held by a running repair, newest
    /// first.
    waiting: VecDeque<(String, Vec<Bin>)>,
    /// The table has a known row order to repair with.
    known_order: bool,
    /// Repair turns neither committed nor held by a running repair.
    repairs_left: u64,
    repairs_running: u64,
    /// Dates whose repair is left for a later run (`repairs_deferred`).
    pub deferred: u64,
    /// Dates out of order in a table without a known order
    /// (`repairs_unsupported`).
    pub unrepairable: u64,
    handed_out: Vec<String>,
}

impl Planner {
    /// `plans` newest first; `repairs` is `OPTIMIZE_REPAIR_DATES`.
    pub fn new(plans: Vec<(String, Vec<Bin>)>, known_order: bool, repairs: u64) -> Self {
        Self {
            queue: plans.into(),
            waiting: VecDeque::new(),
            known_order,
            repairs_left: repairs,
            repairs_running: 0,
            deferred: 0,
            unrepairable: 0,
            handed_out: Vec::new(),
        }
    }

    /// The next date, or `None` when every date was handed out, or when the
    /// dates left wait for a running repair to finish.
    pub fn next(&mut self) -> Option<DatePlan> {
        loop {
            // A waiting date is newer than every queued one.
            let turn_known = self.repairs_left > 0 || self.repairs_running == 0;
            let (date, mut bins) = match self.waiting.pop_front() {
                Some(waiting) if turn_known => waiting,
                Some(waiting) => {
                    self.waiting.push_front(waiting);
                    self.queue.pop_front()?
                }
                None => self.queue.pop_front()?,
            };
            if bins.iter().any(|bin| bin.repair) {
                if !self.known_order {
                    self.unrepairable += 1;
                    bins.retain(|bin| !bin.repair);
                } else if self.repairs_left == 0 && self.repairs_running > 0 {
                    // A running repair may still fail and give its turn back.
                    self.waiting.push_back((date, bins));
                    continue;
                } else if self.repairs_left == 0 {
                    self.deferred += 1;
                    bins.retain(|bin| !bin.repair);
                }
            }
            if bins.is_empty() {
                continue;
            }
            let repairing = bins.iter().any(|bin| bin.repair);
            if repairing {
                self.repairs_left -= 1;
                self.repairs_running += 1;
            }
            self.handed_out.push(date.clone());
            return Some(DatePlan {
                date,
                bins,
                repairing,
            });
        }
    }

    /// A date of [`Planner::next`] is done: a committed repair uses its turn
    /// up, a failed one gives it back.
    pub fn finished(&mut self, repairing: bool, committed: bool) {
        if repairing {
            self.repairs_running -= 1;
            self.repairs_left += u64::from(!committed);
        }
    }

    /// The dates handed out, oldest first (`dates_to_compact`).
    pub fn handed_out(&self) -> Vec<String> {
        let mut dates = self.handed_out.clone();
        dates.sort();
        dates
    }
}

/// A date's task: what it compacted and the footer keys its files were given,
/// or its error, with the part of the date's step that failed (`None` for the
/// compaction itself, else for example `footer metadata`).
pub(crate) type DateResult =
    Result<(DateCompaction, usize), (Option<&'static str>, DeltaTableError)>;

/// Reloads a clone of the table: the dates that finish meanwhile are still
/// reported.
pub(crate) fn reload(mut table: DeltaTable) -> BoxFuture<'static, DeltaResult<DeltaTable>> {
    async move {
        table.update_state().await?;
        Ok(table)
    }
    .boxed()
}

/// What [`compact_dates`] waits for.
enum Event {
    Finished(Result<(Id, DateResult), JoinError>),
    Reloaded(DeltaResult<DeltaTable>),
}

/// Compacts the planner's dates, up to `concurrent` at once, each a task that
/// `start` makes from the table as it is then. Each date is reported as soon
/// as it is done, even while the table reloads: in the table's report, and on
/// its own `date` or `date_error` line.
///
/// After a commit the table is reloaded (`reload`, of a clone), and again if a
/// date commits while it loads: a date starts only from a table reloaded after
/// every commit so far. A reload that fails starts no more dates, as with one
/// date at a time; the running ones finish and are reported.
pub(crate) async fn compact_dates(
    table: &mut DeltaTable,
    planner: &mut Planner,
    concurrent: usize,
    report: &mut Report<'_>,
    out: &mut dyn Write,
    mut start: impl FnMut(&DeltaTable, &str, Vec<Bin>) -> BoxFuture<'static, DateResult>,
    mut reload: impl FnMut(DeltaTable) -> BoxFuture<'static, DeltaResult<DeltaTable>>,
) {
    let mut tasks = JoinSet::new();
    let mut running: HashMap<Id, (String, bool, Instant)> = HashMap::new();
    // The reload in flight, with the date whose commit it follows.
    let mut reloading: Option<(String, BoxFuture<'static, DeltaResult<DeltaTable>>)> = None;
    // The latest date committed since the table, or the reload in flight,
    // started loading.
    let mut unloaded: Option<String> = None;
    let mut reload_failed = false;
    loop {
        if reloading.is_none() && !reload_failed {
            if let Some(date) = unloaded.take() {
                reloading = Some((date, reload(table.clone())));
            }
        }
        while reloading.is_none() && !reload_failed && tasks.len() < concurrent.max(1) {
            let Some(plan) = planner.next() else {
                break;
            };
            let id = tasks.spawn(start(table, &plan.date, plan.bins)).id();
            running.insert(id, (plan.date, plan.repairing, Instant::now()));
        }
        let event = match &mut reloading {
            None => match tasks.join_next_with_id().await {
                Some(finished) => Event::Finished(finished),
                None => break,
            },
            Some((_, reloaded)) => tokio::select! {
                Some(finished) = tasks.join_next_with_id() => Event::Finished(finished),
                result = reloaded => Event::Reloaded(result),
            },
        };
        let joined = match event {
            Event::Reloaded(result) => {
                let (date, _) = reloading.take().expect("a reload in flight");
                match result {
                    Ok(reloaded) => *table = reloaded,
                    Err(error) => {
                        report.failed(&format!("optimize {date}: reload"), &error);
                        reload_failed = true;
                    }
                }
                continue;
            }
            Event::Finished(joined) => joined,
        };
        let (id, result) = match joined {
            Ok((id, result)) => (id, result),
            Err(error) => {
                let failure = DeltaTableError::Generic(format!("its task failed: {error}"));
                (error.id(), Err((None, failure)))
            }
        };
        let (date, repairing, started) = running.remove(&id).expect("a task of this loop");
        planner.finished(repairing, result.is_ok());
        match result {
            Ok((done, footer_keys)) => {
                let compacted = json!({
                    "date": date,
                    "files_removed": done.files_removed,
                    "files_added": done.files_added,
                    "footer_keys": footer_keys,
                    "rows": done.rows,
                    "repaired_bins": done.repaired_bins,
                });
                let mut line = object(compacted.clone());
                line.insert("table".into(), json!(report.name));
                line.insert("seconds".into(), json!(seconds(started)));
                emit(out, "date", line);
                report.compacted.push(compacted);
                unloaded = Some(date);
            }
            Err((part, error)) => {
                let step = match part {
                    Some(part) => format!("optimize {date}: {part}"),
                    None => format!("optimize {date}"),
                };
                report.date_failed(out, &date, &step, &error, started);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
    use serde_json::{Map, Value};

    use super::*;
    use crate::{Lake, Settings};

    fn bins(repair: bool) -> Vec<Bin> {
        vec![Bin {
            files: Vec::new(),
            repair,
        }]
    }

    /// Dates newest first, each with a bin to repair or one to concatenate.
    fn plans(dates: &[(&str, bool)]) -> Vec<(String, Vec<Bin>)> {
        dates
            .iter()
            .map(|(date, repair)| (date.to_string(), bins(*repair)))
            .collect()
    }

    fn next(planner: &mut Planner) -> Option<(String, bool)> {
        planner.next().map(|plan| (plan.date, plan.repairing))
    }

    #[test]
    fn one_date_at_a_time_plans_as_before() {
        let dates = [("d4", true), ("d3", false), ("d2", true), ("d1", true)];
        let mut planner = Planner::new(plans(&dates), true, 1);
        assert_eq!(next(&mut planner), Some(("d4".into(), true)));
        planner.finished(true, true);
        assert_eq!(next(&mut planner), Some(("d3".into(), false)));
        planner.finished(false, true);
        // The turn is used up: d2 and d1 only had bins to repair.
        assert_eq!(next(&mut planner), None);
        assert_eq!((planner.deferred, planner.unrepairable), (2, 0));
        assert_eq!(planner.handed_out(), ["d3", "d4"]);

        // A table without a known order repairs nothing.
        let mut planner = Planner::new(plans(&dates), false, 1);
        assert_eq!(next(&mut planner), Some(("d3".into(), false)));
        assert_eq!(next(&mut planner), None);
        assert_eq!((planner.deferred, planner.unrepairable), (0, 3));
    }

    #[test]
    fn a_date_to_repair_waits_for_a_running_repair_to_give_its_turn_back() {
        let dates = [("d4", true), ("d3", true), ("d2", false), ("d1", true)];
        let mut planner = Planner::new(plans(&dates), true, 1);
        assert_eq!(next(&mut planner), Some(("d4".into(), true)));
        // d3 and d1 wait for d4's turn; d2 goes ahead.
        assert_eq!(next(&mut planner), Some(("d2".into(), false)));
        assert_eq!(next(&mut planner), None);
        planner.finished(false, true);
        assert_eq!(next(&mut planner), None, "d4 still holds the turn");
        // d4 fails: the newest waiting date gets its turn.
        planner.finished(true, false);
        assert_eq!(next(&mut planner), Some(("d3".into(), true)));
        assert_eq!(next(&mut planner), None);
        // d3 commits: the turn is used up, and d1 is deferred.
        planner.finished(true, true);
        assert_eq!(next(&mut planner), None);
        assert_eq!(planner.deferred, 1);
        assert_eq!(planner.handed_out(), ["d2", "d3", "d4"]);
    }

    /// What the stand-in tasks saw.
    #[derive(Default)]
    struct Seen {
        started: Vec<String>,
        running: usize,
        most_running: usize,
        repairs_running: u64,
        repairs_committed: u64,
        most_repairs: u64,
    }

    fn settings() -> Settings {
        let env = BTreeMap::from([("LAKE_ROOT", "/lake"), ("LAKE_TABLES", "logs")]);
        let lookup = |name: &str| env.get(name).map(|value| value.to_string());
        Settings::from_env(&lookup).unwrap()
    }

    fn report(settings: &Settings) -> Report<'_> {
        Report {
            settings,
            name: "logs",
            fields: Map::new(),
            compacted: Vec::new(),
            conflicts: Vec::new(),
            errors: Vec::new(),
        }
    }

    async fn local_table(root: &std::path::Path) -> DeltaTable {
        let lake = Lake::local(root).unwrap();
        DeltaTable::new(lake.log_store("logs").unwrap())
            .create()
            .with_columns([
                StructField::new("block_num", DataType::Primitive(PrimitiveType::Long), false),
                StructField::new("date", DataType::Primitive(PrimitiveType::Date), false),
            ])
            .with_partition_columns(["date"])
            .await
            .unwrap()
    }

    /// Eight dates, three at once, two repair turns. The stand-in commit of
    /// d5 loses its race, and d7's repair fails: both are reported, d7's turn
    /// goes to the next date to repair, and every other date commits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dates_run_at_once_each_once_and_a_lost_race_stops_no_other() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = local_table(dir.path()).await;
        let settings = settings();
        let mut report = report(&settings);
        let dates = [
            ("d8", false),
            ("d7", true),
            ("d6", true),
            ("d5", false),
            ("d4", true),
            ("d3", false),
            ("d2", true),
            ("d1", false),
        ];
        let mut planner = Planner::new(plans(&dates), true, 2);
        let seen = Arc::new(Mutex::new(Seen::default()));
        let start = |_: &DeltaTable, date: &str, bins: Vec<Bin>| {
            let (seen, date) = (Arc::clone(&seen), date.to_string());
            let repairing = bins.iter().any(|bin| bin.repair);
            async move {
                {
                    let mut seen = seen.lock().unwrap();
                    seen.started.push(date.clone());
                    seen.running += 1;
                    seen.most_running = seen.most_running.max(seen.running);
                    seen.repairs_running += u64::from(repairing);
                    let repairs = seen.repairs_running + seen.repairs_committed;
                    seen.most_repairs = seen.most_repairs.max(repairs);
                }
                let wait = u64::from(date.as_bytes()[1]) % 3 * 10 + 5;
                tokio::time::sleep(Duration::from_millis(wait)).await;
                let result = match date.as_str() {
                    "d5" => Err((None, DeltaTableError::VersionAlreadyExists(9))),
                    "d7" => Err((None, DeltaTableError::Generic("two rows tie".into()))),
                    _ => Ok((
                        DateCompaction {
                            files_removed: 3,
                            files_added: 1,
                            rows: 100,
                            repaired_bins: usize::from(repairing),
                        },
                        2,
                    )),
                };
                let mut seen = seen.lock().unwrap();
                seen.running -= 1;
                seen.repairs_running -= u64::from(repairing);
                seen.repairs_committed += u64::from(repairing && result.is_ok());
                result
            }
            .boxed()
        };
        let mut out = Vec::new();
        compact_dates(
            &mut table,
            &mut planner,
            3,
            &mut report,
            &mut out,
            start,
            reload,
        )
        .await;

        let seen = seen.lock().unwrap();
        let started: BTreeSet<_> = seen.started.iter().cloned().collect();
        assert_eq!(started.len(), seen.started.len(), "each date once");
        assert!(
            seen.most_running <= 3 && seen.most_running > 1,
            "{}",
            seen.most_running
        );
        assert!(seen.most_repairs <= 2, "{}", seen.most_repairs);
        // d7's turn went to d4; d2 is deferred.
        assert_eq!(seen.repairs_committed, 2);
        assert_eq!(planner.deferred, 1);
        assert!(!started.contains("d2"), "{started:?}");

        let lines: Vec<Value> = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let dates_of = |event: &str| -> BTreeSet<String> {
            lines
                .iter()
                .filter(|line| line["event"] == event)
                .map(|line| line["date"].as_str().unwrap().to_string())
                .collect()
        };
        let committed = BTreeSet::from(["d1", "d3", "d4", "d6", "d8"].map(String::from));
        assert_eq!(dates_of("date"), committed);
        assert_eq!(
            dates_of("date_error"),
            BTreeSet::from(["d5", "d7"].map(String::from))
        );
        let compacted: BTreeSet<String> = report
            .compacted
            .iter()
            .map(|entry| entry["date"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(compacted, committed);
        // A date line is its `compacted` entry with the table and its seconds.
        for line in lines.iter().filter(|line| line["event"] == "date") {
            let entry = report
                .compacted
                .iter()
                .find(|entry| entry["date"] == line["date"])
                .unwrap();
            let mut fields = line.as_object().unwrap().clone();
            assert_eq!(fields.remove("table"), Some(json!("logs")));
            assert!(fields.remove("seconds").unwrap().as_f64().is_some());
            assert_eq!(fields.remove("event"), Some(json!("date")));
            assert_eq!(&Value::Object(fields), entry);
        }
        let lost = lines.iter().find(|line| line["date"] == "d5").unwrap();
        assert_eq!(lost["conflict"], true, "{lost}");
        assert_eq!(report.conflicts, [lost["error"].as_str().unwrap()]);
        let failed = lines.iter().find(|line| line["date"] == "d7").unwrap();
        assert_eq!(failed["conflict"], false, "{failed}");
        assert_eq!(report.errors, [failed["error"].as_str().unwrap()]);
        assert!(
            report.errors[0].starts_with("optimize d7: "),
            "{:?}",
            report.errors
        );
    }

    /// Stdout that logs each line as soon as it is written: `<event> <date>`.
    struct Lines {
        log: Arc<Mutex<Vec<String>>>,
        partial: Vec<u8>,
    }

    impl Write for Lines {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.partial.extend_from_slice(buf);
            while let Some(end) = self.partial.iter().position(|&byte| byte == b'\n') {
                let line: Vec<u8> = self.partial.drain(..=end).collect();
                let line: Value = serde_json::from_slice(&line).unwrap();
                let (event, date) = (line["event"].as_str(), line["date"].as_str());
                let entry = format!("{} {}", event.unwrap(), date.unwrap());
                self.log.lock().unwrap().push(entry);
            }
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Three dates, two at once; d2 takes longer than d3.
    fn two_at_once(
        log: &Arc<Mutex<Vec<String>>>,
    ) -> impl FnMut(&DeltaTable, &str, Vec<Bin>) -> BoxFuture<'static, DateResult> + '_ {
        move |_: &DeltaTable, date: &str, _: Vec<Bin>| {
            log.lock().unwrap().push(format!("start {date}"));
            let wait = if date == "d2" { 100 } else { 10 };
            async move {
                tokio::time::sleep(Duration::from_millis(wait)).await;
                Ok((DateCompaction::default(), 0))
            }
            .boxed()
        }
    }

    /// d2 commits while the table reloads after d3: its line is written at
    /// once, and the table is reloaded again before d1 starts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dates_are_reported_while_the_table_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = local_table(dir.path()).await;
        let settings = settings();
        let mut report = report(&settings);
        let mut planner = Planner::new(
            plans(&[("d3", false), ("d2", false), ("d1", false)]),
            true,
            0,
        );
        let log = Arc::new(Mutex::new(Vec::new()));
        let slow_reload = |table: DeltaTable| {
            log.lock().unwrap().push("reload begins".to_string());
            let log = Arc::clone(&log);
            async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                log.lock().unwrap().push("reload ends".to_string());
                reload(table).await
            }
            .boxed()
        };
        let mut out = Lines {
            log: Arc::clone(&log),
            partial: Vec::new(),
        };
        let start = two_at_once(&log);
        compact_dates(
            &mut table,
            &mut planner,
            2,
            &mut report,
            &mut out,
            start,
            slow_reload,
        )
        .await;
        assert_eq!(
            *log.lock().unwrap(),
            [
                "start d3",
                "start d2",
                "date d3",
                "reload begins",
                "date d2",
                "reload ends",
                // d2 committed after that reload began.
                "reload begins",
                "reload ends",
                "start d1",
                "date d1",
                "reload begins",
                "reload ends",
            ]
        );
        assert_eq!(report.compacted.len(), 3);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }

    /// A reload that fails starts no more dates; the running one finishes and
    /// is reported.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_reload_starts_no_more_dates() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = local_table(dir.path()).await;
        let settings = settings();
        let mut report = report(&settings);
        let mut planner = Planner::new(
            plans(&[("d3", false), ("d2", false), ("d1", false)]),
            true,
            0,
        );
        let log = Arc::new(Mutex::new(Vec::new()));
        let failing_reload = |_: DeltaTable| {
            log.lock().unwrap().push("reload begins".to_string());
            async { Err(DeltaTableError::Generic("the store is down".into())) }.boxed()
        };
        let mut out = Lines {
            log: Arc::clone(&log),
            partial: Vec::new(),
        };
        let start = two_at_once(&log);
        compact_dates(
            &mut table,
            &mut planner,
            2,
            &mut report,
            &mut out,
            start,
            failing_reload,
        )
        .await;
        assert_eq!(
            *log.lock().unwrap(),
            [
                "start d3",
                "start d2",
                "date d3",
                "reload begins",
                "date d2"
            ]
        );
        assert_eq!(report.compacted.len(), 2);
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        let error = &report.errors[0];
        assert!(error.starts_with("optimize d3: reload: ") && error.contains("the store is down"));
    }
}
