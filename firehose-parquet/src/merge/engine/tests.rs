//! Pins the step order of the shared partition merge. Local and S3 merges both run
//! through [`merge_partition`], so this order (claim, outputs, durable sync, owner check,
//! commit, deletes, journal removal) is the crash-safety contract of both storages.
use super::*;
use crate::merge_journal::{JournalState, INJECTED_CRASH};
use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::cell::RefCell;
use std::rc::Rc;

type Events = Rc<RefCell<Vec<String>>>;

fn parquet(rows: u64) -> bytes::Bytes {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_number",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from_iter_values(0..rows))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.into_inner().unwrap().into()
}

struct Files {
    events: Events,
    claim: bool,
    journal: Option<Journal>,
}

impl PartitionFiles for Files {
    fn label(&self) -> String {
        "fixture".into()
    }
    fn list_names(&self) -> Result<Vec<String>> {
        self.events.borrow_mut().push("list".into());
        Ok(vec![
            "part-000001.parquet".into(),
            "part-000002.parquet".into(),
        ])
    }
    fn read_journal(&self) -> Result<Option<Journal>> {
        Ok(self.journal.clone())
    }
    fn create_journal(&self, journal: &Journal) -> Result<bool> {
        self.events.borrow_mut().push(format!(
            "create_journal:{:?}:{}:{}",
            journal.state,
            journal.first_output_part,
            journal.sources.join(",")
        ));
        Ok(self.claim)
    }
    fn replace_journal(&self, journal: &Journal) -> Result<()> {
        self.events.borrow_mut().push(format!(
            "replace_journal:{:?}:{}",
            journal.state,
            journal.outputs.join(",")
        ));
        Ok(())
    }
    fn remove_journal(&self) -> Result<()> {
        self.events.borrow_mut().push("remove_journal".into());
        Ok(())
    }
    fn delete(&self, name: &str) -> Result<()> {
        self.events.borrow_mut().push(format!("delete_file:{name}"));
        Ok(())
    }
    fn sync(&self) -> Result<()> {
        self.events.borrow_mut().push("sync".into());
        Ok(())
    }
}

struct Fixture {
    events: Events,
    files: Files,
    mismatch: Option<String>,
    changed: Option<String>,
    owner_fails: bool,
}

impl Fixture {
    fn new() -> Self {
        let events = Events::default();
        Self {
            files: Files {
                events: events.clone(),
                claim: true,
                journal: None,
            },
            events,
            mismatch: None,
            changed: None,
            owner_fails: false,
        }
    }

    fn event(&self, event: impl Into<String>) {
        self.events.borrow_mut().push(event.into());
    }

    fn events(&self) -> Vec<String> {
        self.events.borrow().clone()
    }
}

impl PartitionMerge for Fixture {
    type Source = bytes::Bytes;
    type Files = Files;

    fn label(&self) -> &str {
        "blocks/day=01"
    }
    fn files(&self) -> &Files {
        &self.files
    }
    fn source_bytes(&self, sources: &[bytes::Bytes]) -> Result<u64> {
        self.event("source_bytes");
        Ok(sources.iter().map(|data| data.len() as u64).sum())
    }
    fn log_no_op(&self, _: usize, _: u64, _: u64, estimated: usize) {
        self.event(format!("no_op:{estimated}"));
    }
    fn schema_mismatch(&self, _: &[bytes::Bytes]) -> Result<Option<String>> {
        self.event("schema");
        Ok(self.mismatch.clone())
    }
    fn log_start(&self, _: usize, _: u64, _: &MergeConfig) {
        self.event("start");
    }
    fn max_part_number(&self, _: &[bytes::Bytes]) -> u32 {
        7
    }
    fn claim_journal(&self, sources: &[bytes::Bytes], initial_part_num: u32) -> Result<Journal> {
        self.event(format!("claim:{initial_part_num}"));
        let run = RunContext {
            run_id: "run".into(),
            lock: "lock".into(),
        };
        let names = (0..sources.len())
            .map(|index| format!("src-{index}.parquet"))
            .collect();
        Ok(Journal::new(&run, names, initial_part_num + 1))
    }
    fn changed_source(&self, _: &[bytes::Bytes]) -> Option<String> {
        self.event("changed?");
        self.changed.clone()
    }
    fn encode<F>(
        &self,
        sources: &[bytes::Bytes],
        encoder: &mut Encoder,
        publish: &mut F,
    ) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        for (index, data) in sources.iter().enumerate() {
            self.event(format!("read:{index}"));
            let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())?;
            encoder.write_reader(builder, publish, None)?;
        }
        Ok(())
    }
    fn publish(&self, name: &str, data: Vec<u8>, rows: usize) -> Result<()> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))?;
        assert_eq!(reader.metadata().file_metadata().num_rows() as usize, rows);
        self.event(format!("publish:{name}:{rows}"));
        Ok(())
    }
    fn check_owner(&self) -> Result<()> {
        self.event("owner");
        anyhow::ensure!(!self.owner_fails, "ownership changed");
        Ok(())
    }
    fn delete_sources(&self, sources: &[bytes::Bytes], outputs: &[String]) -> Result<()> {
        self.event(format!("delete:{}:{}", sources.len(), outputs.join(",")));
        crash_point("after-first-delete")
    }
    fn log_done(&self, sources: usize, outputs: usize, output_bytes: u64) {
        assert!(output_bytes > 0);
        self.event(format!("done:{sources}:{outputs}"));
    }
}

fn config() -> MergeConfig {
    MergeConfig {
        path: "fixture".into(),
        compression: Compression::None,
        flush_rows: None,
        flush_bytes: 1 << 30,
        dry_run: false,
        verbose: false,
        aws: None,
        cache_control: String::new(),
    }
}

fn run(
    fixture: &Fixture,
    sources: &[bytes::Bytes],
    config: &MergeConfig,
) -> (Result<()>, MergeResult) {
    let mut result = MergeResult::default();
    let mut table = None;
    let outcome = merge_partition(fixture, sources, config, &mut table, &mut result);
    (outcome, result)
}

fn with_crash<T>(step: &'static str, f: impl FnOnce() -> T) -> T {
    INJECTED_CRASH.with(|crash| *crash.borrow_mut() = Some(step));
    let value = f();
    INJECTED_CRASH.with(|crash| *crash.borrow_mut() = None);
    value
}

const COMPLETE: [&str; 16] = [
    "source_bytes",
    "schema",
    "start",
    "claim:7",
    "create_journal:Writing:8:src-0.parquet,src-1.parquet",
    "changed?",
    "read:0",
    "read:1",
    "publish:part-000008.parquet:5",
    "sync",
    "owner",
    "replace_journal:Committed:part-000008.parquet",
    "delete:2:part-000008.parquet",
    "sync",
    "remove_journal",
    "sync",
];

#[test]
fn complete_merge_runs_the_crash_safe_steps_in_order() {
    let fixture = Fixture::new();
    let (outcome, result) = run(&fixture, &[parquet(2), parquet(3)], &config());
    outcome.unwrap();
    let mut expected: Vec<String> = COMPLETE.iter().map(|e| e.to_string()).collect();
    expected.push("done:2:1".into());
    assert_eq!(fixture.events(), expected);
    assert_eq!(
        (
            result.partitions_merged,
            result.files_read,
            result.files_written
        ),
        (1, 2, 1)
    );
    assert!(result.bytes_before > 0 && result.bytes_after > 0);
}

#[test]
fn each_crash_point_stops_after_its_barrier() {
    for (step, last) in [
        ("after-outputs", "sync"),
        (
            "after-commit",
            "replace_journal:Committed:part-000008.parquet",
        ),
        ("after-first-delete", "delete:2:part-000008.parquet"),
    ] {
        let fixture = Fixture::new();
        let (outcome, _) = with_crash(step, || run(&fixture, &[parquet(2), parquet(3)], &config()));
        assert!(outcome.unwrap_err().to_string().contains(step));
        let events = fixture.events();
        let expected_len = COMPLETE.iter().position(|e| *e == last).unwrap() + 1;
        assert_eq!(events, COMPLETE[..expected_len], "{step}");
    }
}

#[test]
fn owner_change_prevents_the_commit() {
    let mut fixture = Fixture::new();
    fixture.owner_fails = true;
    let (outcome, _) = run(&fixture, &[parquet(2), parquet(3)], &config());
    assert!(outcome.is_err());
    let events = fixture.events();
    assert_eq!(events.last().map(String::as_str), Some("owner"));
    assert!(!events.iter().any(|e| e.starts_with("replace_journal")));
}

#[test]
fn a_claimed_partition_is_left_to_its_owner() {
    let mut fixture = Fixture::new();
    fixture.files.claim = false;
    let (outcome, result) = run(&fixture, &[parquet(2), parquet(3)], &config());
    outcome.unwrap();
    assert_eq!(fixture.events(), COMPLETE[..5]);
    assert_eq!(result.partitions_in_use, ["blocks/day=01"]);
    assert_eq!(result.partitions_skipped, 1);
}

#[test]
fn a_changed_source_releases_the_claim_without_reading() {
    let mut fixture = Fixture::new();
    fixture.changed = Some("part-1".into());
    let (outcome, result) = run(&fixture, &[parquet(2), parquet(3)], &config());
    outcome.unwrap();
    let mut expected: Vec<String> = COMPLETE[..6].iter().map(|e| e.to_string()).collect();
    expected.push("remove_journal".into());
    assert_eq!(fixture.events(), expected);
    assert_eq!(result.partitions_skipped, 1);
}

#[test]
fn all_empty_sources_release_the_claim_without_outputs() {
    let fixture = Fixture::new();
    let (outcome, result) = run(&fixture, &[parquet(0), parquet(0)], &config());
    outcome.unwrap();
    let mut expected: Vec<String> = COMPLETE[..8].iter().map(|e| e.to_string()).collect();
    expected.push("remove_journal".into());
    assert_eq!(fixture.events(), expected);
    assert_eq!((result.partitions_skipped, result.files_read), (1, 2));
}

#[test]
fn preflight_outcomes_never_claim() {
    // Dry run: estimated, counted, never claimed.
    let fixture = Fixture::new();
    let mut dry = config();
    dry.dry_run = true;
    let (outcome, result) = run(&fixture, &[parquet(2), parquet(3)], &dry);
    outcome.unwrap();
    assert_eq!(fixture.events(), COMPLETE[..3]);
    assert_eq!((result.partitions_merged, result.files_read), (1, 2));

    // Mixed schemas: recorded, nothing written.
    let mut fixture = Fixture::new();
    fixture.mismatch = Some("column differs".into());
    let (outcome, result) = run(&fixture, &[parquet(2), parquet(3)], &config());
    outcome.unwrap();
    assert_eq!(fixture.events(), COMPLETE[..2]);
    assert_eq!(result.schema_mismatches, ["blocks/day=01: column differs"]);
    assert_eq!(result.bytes_before, 0);

    // No file-count reduction expected.
    let fixture = Fixture::new();
    let mut tiny = config();
    tiny.flush_bytes = 1;
    let (outcome, result) = run(&fixture, &[parquet(2), parquet(3)], &tiny);
    outcome.unwrap();
    assert_eq!(fixture.events()[0], "source_bytes");
    assert!(fixture.events()[1].starts_with("no_op:"));
    assert_eq!(fixture.events().len(), 2);
    assert_eq!(result.partitions_skipped, 1);

    // A single part is never merged.
    let fixture = Fixture::new();
    let (outcome, result) = run(&fixture, &[parquet(2)], &config());
    outcome.unwrap();
    assert!(fixture.events().is_empty());
    assert_eq!(result.partitions_skipped, 1);
}

#[test]
fn partitions_group_consecutive_sorted_sources_and_label_tables() {
    let mut seen = Vec::new();
    for_each_partition(
        "root",
        vec!["a/1", "a/2", "b/1", "c/1", "c/2"],
        |source| source.split('/').next().unwrap().to_string(),
        |partition, members, _| {
            seen.push((partition.clone(), members.to_vec()));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        seen,
        [
            ("a".to_string(), vec!["a/1", "a/2"]),
            ("b".to_string(), vec!["b/1"]),
            ("c".to_string(), vec!["c/1", "c/2"]),
        ]
    );
    let mut calls = 0;
    for_each_partition(
        "root",
        Vec::<&str>::new(),
        |_| (),
        |_, _, _| {
            calls += 1;
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(calls, 0);
    assert_eq!(table_of("blocks/year=2024/day=01"), "blocks");
    assert_eq!(table_of("(root)"), "(root)");
    assert_eq!(table_of(""), "(root)");
}

#[test]
fn recovery_reports_in_dry_run_defers_to_live_runs_and_recovers_otherwise() {
    let events = Events::default();
    let run = RunContext {
        run_id: "old".into(),
        lock: "lock".into(),
    };
    let journal = Journal::new(&run, vec!["part-000001.parquet".into()], 2);
    let files = Files {
        events: events.clone(),
        claim: true,
        journal: Some(journal),
    };
    let mut result = MergeResult::default();
    recover_partition("p", &files, None::<()>, |_, _| unreachable!(), &mut result).unwrap();
    recover_partition("p", &files, Some(()), |_, _| Ok(false), &mut result).unwrap();
    assert!(events.borrow().is_empty());
    assert_eq!(result.merges_recovered, 0);
    recover_partition(
        "p",
        &files,
        Some(()),
        |journal, _| {
            assert_eq!(journal.state, JournalState::Writing);
            Ok(true)
        },
        &mut result,
    )
    .unwrap();
    assert_eq!(result.merges_recovered, 1);
    // Rolled back: the partial output (part-000002) is deleted, then the journal.
    assert_eq!(
        *events.borrow(),
        [
            "list",
            "delete_file:part-000002.parquet",
            "sync",
            "remove_journal",
            "sync"
        ]
    );
    let missing = Files {
        events: Events::default(),
        claim: true,
        journal: None,
    };
    recover_partition("p", &missing, Some(()), |_, _| unreachable!(), &mut result).unwrap();
    assert_eq!(result.merges_recovered, 1);
}
