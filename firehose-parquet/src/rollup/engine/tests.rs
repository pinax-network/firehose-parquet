//! Pins the order of the shared engine's storage hooks, so the local and S3 rollups keep
//! their per-group barriers: validate all, claim, publish, sync, commit, clean up, delete,
//! release; and recovery rolls interrupted groups back or forward.
use super::*;
use crate::merge_journal::INJECTED_CRASH;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use std::cell::RefCell;
use std::collections::BTreeMap;

fn source(rows: usize, column: &str) -> bytes::Bytes {
    let schema = Arc::new(Schema::new(vec![Field::new(
        column,
        DataType::UInt64,
        false,
    )]));
    let values: ArrayRef = Arc::new(UInt64Array::from_iter_values(0..rows as u64));
    let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.into_inner().unwrap().into()
}

#[derive(Default)]
struct Fixture {
    validate: Vec<bytes::Bytes>,
    encode: Vec<bytes::Bytes>,
    events: RefCell<Vec<String>>,
    fail: Option<&'static str>,
    root: String,
    /// Output directory -> file names (journal included).
    dirs: RefCell<BTreeMap<String, BTreeMap<String, Vec<u8>>>>,
}

impl Fixture {
    fn new(inputs: Vec<bytes::Bytes>) -> Self {
        Self {
            validate: inputs.clone(),
            encode: inputs,
            root: "fixture-source".into(),
            ..Self::default()
        }
    }

    fn event(&self, event: String) -> Result<()> {
        let fail = self.fail.is_some_and(|prefix| event.starts_with(prefix));
        self.events.borrow_mut().push(event);
        anyhow::ensure!(!fail, "fixture backend failure");
        Ok(())
    }

    fn events(&self) -> Vec<String> {
        self.events.borrow().clone()
    }

    fn journal(&self, dir: &str) -> Option<RollupJournal> {
        self.dirs
            .borrow()
            .get(dir)
            .and_then(|files| files.get(ROLLUP_JOURNAL_FILE))
            .map(|data| RollupJournal::decode(data, "fixture").unwrap())
    }

    fn names(&self, dir: &str) -> Vec<String> {
        self.dirs
            .borrow()
            .get(dir)
            .map(|files| files.keys().cloned().collect())
            .unwrap_or_default()
    }
}

struct FixtureFiles<'f> {
    fixture: &'f Fixture,
    dir: &'f str,
}

impl PartitionFiles for FixtureFiles<'_> {
    fn label(&self) -> String {
        self.dir.to_string()
    }
    fn list_names(&self) -> Result<Vec<String>> {
        Ok(self.fixture.names(self.dir))
    }
    fn read_record(&self, name: &str, _: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .fixture
            .dirs
            .borrow()
            .get(self.dir)
            .and_then(|files| files.get(name).cloned()))
    }
    fn create_record(&self, name: &str, _: &str, data: &[u8]) -> Result<bool> {
        let state = RollupJournal::decode(data, "fixture").unwrap().state;
        self.fixture
            .event(format!("create_journal:{}:{state:?}", self.dir))?;
        let mut dirs = self.fixture.dirs.borrow_mut();
        let files = dirs.entry(self.dir.to_string()).or_default();
        if files.contains_key(name) {
            return Ok(false);
        }
        files.insert(name.to_string(), data.to_vec());
        Ok(true)
    }
    fn replace_record(&self, name: &str, _: &str, data: &[u8]) -> Result<()> {
        let journal = RollupJournal::decode(data, "fixture").unwrap();
        self.fixture.event(format!(
            "replace_journal:{}:{:?}:{}",
            self.dir,
            journal.state,
            journal.outputs.len()
        ))?;
        self.fixture
            .dirs
            .borrow_mut()
            .entry(self.dir.to_string())
            .or_default()
            .insert(name.to_string(), data.to_vec());
        Ok(())
    }
    fn journal_location(&self) -> &'static str {
        "fixture"
    }
    fn delete(&self, name: &str) -> Result<()> {
        self.fixture
            .event(format!("delete_file:{}/{name}", self.dir))?;
        if let Some(files) = self.fixture.dirs.borrow_mut().get_mut(self.dir) {
            files.remove(name);
        }
        Ok(())
    }
    fn sync(&self) -> Result<()> {
        self.fixture.event(format!("sync:{}", self.dir))
    }
}

impl Backend for Fixture {
    type Source = usize;
    type Output = String;
    type Reader = bytes::Bytes;
    type Files<'f>
        = FixtureFiles<'f>
    where
        Self: 'f;

    fn source_root(&self) -> &str {
        &self.root
    }
    fn label(&self, source: &usize) -> String {
        format!("source-{source}.parquet")
    }
    fn journal_source(&self, source: &usize) -> Result<String> {
        Ok(self.label(source))
    }
    fn reader(
        &self,
        source: &usize,
        pass: Pass,
    ) -> Result<ParquetRecordBatchReaderBuilder<bytes::Bytes>> {
        let data = match pass {
            Pass::Validate => {
                self.event(format!("validate:{source}"))?;
                &self.validate[*source]
            }
            Pass::Encode => {
                self.event(format!("encode:{source}"))?;
                &self.encode[*source]
            }
        };
        Ok(ParquetRecordBatchReaderBuilder::try_new(data.clone())?
            .with_batch_size(READER_BATCH_ROWS))
    }
    fn begin_group(&self, group: &str) -> Result<()> {
        self.event(format!("begin:{group}"))
    }
    fn group_dir(&self, group: &str) -> String {
        group.to_string()
    }
    fn files<'f>(&'f self, dir: &'f str) -> FixtureFiles<'f> {
        FixtureFiles { fixture: self, dir }
    }
    fn find_journals(&self) -> Result<Vec<String>> {
        Ok(self
            .dirs
            .borrow()
            .iter()
            .filter(|(_, files)| files.contains_key(ROLLUP_JOURNAL_FILE))
            .map(|(dir, _)| dir.clone())
            .collect())
    }
    fn output(&self, dir: &str, name: &str) -> String {
        format!("{dir}/{name}")
    }
    fn publish(
        &self,
        dir: &str,
        name: &str,
        bytes: Vec<u8>,
        rows: usize,
        _run_id: &str,
    ) -> Result<String> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.clone()))?;
        assert_eq!(reader.metadata().file_metadata().num_rows() as usize, rows);
        self.event(format!("publish:{dir}:{rows}"))?;
        self.dirs
            .borrow_mut()
            .entry(dir.to_string())
            .or_default()
            .insert(name.to_string(), bytes);
        Ok(format!("{dir}/{name}"))
    }
    fn check_owner(&self) -> Result<()> {
        self.event("owner".into())
    }
    fn remove_previous(&self, dir: &str, written: &HashSet<String>) -> Result<()> {
        assert!(written
            .iter()
            .any(|name| name.starts_with(&format!("{dir}/"))));
        self.event(format!("cleanup:{dir}"))
    }
    fn delete_sources(&self, sources: &[usize], _written: &HashSet<String>) -> Result<usize> {
        self.event(format!("delete:{}", sources[0]))?;
        rollup_crash_point("after-first-delete")?;
        Ok(sources.len())
    }
    fn delete_recorded(&self, sources: &[String]) -> Result<usize> {
        self.event(format!("delete_recorded:{}", sources.join(",")))?;
        Ok(sources.len())
    }
    fn finish_deletions(&self, deleted: usize) -> Result<()> {
        self.event(format!("finish:{deleted}"))
    }
}

fn config() -> RollupConfig {
    RollupConfig {
        source: "fixture".into(),
        output: "fixture-output".into(),
        target: RollupTarget::Hour,
        compression: Compression::None,
        flush_bytes: 0,
        delete_source: true,
        aws: None,
        cache_control: String::new(),
    }
}

fn groups() -> BTreeMap<String, Vec<usize>> {
    BTreeMap::from([("a".into(), vec![0, 1]), ("b".into(), vec![2])])
}

fn with_crash<T>(step: &'static str, f: impl FnOnce() -> T) -> T {
    INJECTED_CRASH.with(|crash| *crash.borrow_mut() = Some(step));
    let value = f();
    INJECTED_CRASH.with(|crash| *crash.borrow_mut() = None);
    value
}

const GROUP_A: [&str; 15] = [
    "validate:0",
    "validate:1",
    "begin:a",
    "owner",
    "create_journal:a:Writing",
    "encode:0",
    "encode:1",
    "publish:a:4",
    "sync:a",
    "owner",
    "replace_journal:a:Committed:1",
    "cleanup:a",
    "delete:0",
    "delete_file:a/_fireparq_rollup.json",
    "sync:a",
];

#[test]
fn journaled_group_barriers_and_cleanup_order_are_explicit() {
    let fixture = Fixture::new(vec![source(2, "value"); 3]);
    run(&fixture, &groups(), &config()).unwrap();
    let mut expected: Vec<String> = GROUP_A.iter().map(|e| e.to_string()).collect();
    expected.extend(
        [
            "validate:2",
            "begin:b",
            "owner",
            "create_journal:b:Writing",
            "encode:2",
            "publish:b:2",
            "sync:b",
            "owner",
            "replace_journal:b:Committed:1",
            "cleanup:b",
            "delete:2",
            "delete_file:b/_fireparq_rollup.json",
            "sync:b",
            "finish:3",
        ]
        .map(String::from),
    );
    assert_eq!(fixture.events(), expected);
    assert!(fixture.find_journals().unwrap().is_empty());
}

#[test]
fn copy_mode_never_deletes_sources() {
    let fixture = Fixture::new(vec![source(2, "value"); 3]);
    let mut config = config();
    config.delete_source = false;
    run(&fixture, &groups(), &config).unwrap();
    let events = fixture.events();
    assert!(!events
        .iter()
        .any(|event| event.starts_with("delete:") || event.starts_with("finish:")));
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("cleanup:"))
            .count(),
        2
    );
}

#[test]
fn empty_and_corrupt_preflight_never_claim_or_mutate() {
    let zero = Fixture::new(vec![source(0, "value"); 3]);
    run(&zero, &groups(), &config()).unwrap();
    assert_eq!(zero.events(), ["validate:0", "validate:1", "validate:2"]);
    let corrupt = Fixture::new(vec![
        source(2, "value"),
        bytes::Bytes::from_static(b"corrupt"),
        source(2, "value"),
    ]);
    assert!(run(&corrupt, &groups(), &config()).is_err());
    assert_eq!(corrupt.events(), ["validate:0", "validate:1"]);
}

#[test]
fn mixed_schema_group_is_skipped_but_later_valid_group_finishes_before_report() {
    let fixture = Fixture::new(vec![
        source(2, "value"),
        source(2, "other"),
        source(2, "value"),
    ]);
    let error = run(&fixture, &groups(), &config()).unwrap_err();
    assert!(error.to_string().contains("1 target partition(s)"));
    let events = fixture.events();
    assert_eq!(events[..3], ["validate:0", "validate:1", "validate:2"]);
    assert!(!events.iter().any(|event| event.ends_with(":a:Writing")));
    assert_eq!(events.last().map(String::as_str), Some("finish:1"));
}

#[test]
fn mutation_failure_stops_later_hooks_and_groups() {
    for failure in [
        "begin:",
        "create_journal:",
        "publish:",
        "replace_journal:",
        "cleanup:",
        "delete:",
    ] {
        let mut fixture = Fixture::new(vec![source(2, "value"); 3]);
        fixture.fail = Some(failure);
        assert!(run(&fixture, &groups(), &config()).is_err());
        let events = fixture.events();
        assert!(events.last().unwrap().starts_with(failure), "{failure}");
        assert!(!events
            .iter()
            .any(|event| event == "validate:2" || event.starts_with("finish:")));
    }
}

#[test]
fn changed_row_count_never_commits_even_after_partial_output() {
    for flush_bytes in [0, 1] {
        let mut fixture = Fixture::new(vec![source(2, "value"); 3]);
        fixture.encode[1] = source(1, "value");
        let mut config = config();
        config.flush_bytes = flush_bytes;
        let error = run(&fixture, &groups(), &config).unwrap_err();
        assert!(error
            .to_string()
            .contains("row count changed after preflight"));
        let events = fixture.events();
        assert!(!events
            .iter()
            .any(|event| event.starts_with("replace_journal")
                || event.starts_with("cleanup:")
                || event.starts_with("delete:")
                || event == "validate:2"));
        assert_eq!(fixture.journal("a").unwrap().state, JournalState::Writing);
    }
}

#[test]
fn each_crash_point_stops_after_its_barrier_and_leaves_the_journal() {
    for (step, last, state) in [
        (
            "rollup-after-first-part",
            "publish:a:4",
            JournalState::Writing,
        ),
        ("rollup-after-outputs", "sync:a", JournalState::Writing),
        (
            "rollup-after-commit",
            "replace_journal:a:Committed:1",
            JournalState::Committed,
        ),
        (
            "rollup-after-first-delete",
            "delete:0",
            JournalState::Committed,
        ),
    ] {
        let fixture = Fixture::new(vec![source(2, "value"); 3]);
        let error = with_crash(step, || run(&fixture, &groups(), &config())).unwrap_err();
        assert!(error.to_string().contains(step), "{error}");
        let expected = GROUP_A.iter().position(|event| *event == last).unwrap() + 1;
        assert_eq!(fixture.events(), GROUP_A[..expected], "{step}");
        assert_eq!(fixture.journal("a").unwrap().state, state, "{step}");
    }
}

#[test]
fn recovery_rolls_writing_back_and_committed_forward() {
    // An interrupted run left a writing journal with one partial output in `a`, and a
    // committed journal in `b`; `c` has an unrelated file only.
    let fixture = Fixture::new(Vec::new());
    let writing = RollupJournal::writing(
        "0a1b2c3d",
        false,
        "fixture-source",
        vec!["x.parquet".into()],
    );
    let committed = RollupJournal::writing(
        "0a1b2c3d",
        false,
        "fixture-source",
        vec!["y.parquet".into(), "z.parquet".into()],
    )
    .committed(vec!["part-0a1b2c3d-000001.parquet".into()]);
    {
        let mut dirs = fixture.dirs.borrow_mut();
        let a = dirs.entry("a".into()).or_default();
        a.insert(ROLLUP_JOURNAL_FILE.into(), writing.encode().unwrap());
        a.insert("part-0a1b2c3d-000001.parquet".into(), Vec::new());
        a.insert(
            ".part-0a1b2c3d-000002.parquet.0a1b2c3d.tmp".into(),
            Vec::new(),
        );
        a.insert("part-000009.parquet".into(), Vec::new());
        let b = dirs.entry("b".into()).or_default();
        b.insert(ROLLUP_JOURNAL_FILE.into(), committed.encode().unwrap());
        b.insert("part-0a1b2c3d-000001.parquet".into(), Vec::new());
        dirs.entry("c".into())
            .or_default()
            .insert("part-000001.parquet".into(), Vec::new());
    }
    recover(&fixture).unwrap();
    assert_eq!(
        fixture.events(),
        [
            "owner",
            "delete_file:a/.part-0a1b2c3d-000002.parquet.0a1b2c3d.tmp",
            "delete_file:a/part-0a1b2c3d-000001.parquet",
            "sync:a",
            "delete_file:a/_fireparq_rollup.json",
            "sync:a",
            "owner",
            "cleanup:b",
            "delete_recorded:y.parquet,z.parquet",
            "delete_file:b/_fireparq_rollup.json",
            "sync:b",
            "finish:2",
        ]
    );
    assert_eq!(fixture.names("a"), ["part-000009.parquet"]);
    assert_eq!(fixture.names("b"), ["part-0a1b2c3d-000001.parquet"]);
    assert!(fixture.find_journals().unwrap().is_empty());
}

#[test]
fn recovery_refuses_missing_outputs_and_foreign_source_deletes() {
    let committed = |root: &str, copy: bool| {
        RollupJournal {
            copy,
            ..RollupJournal::writing("0a1b2c3d", false, root, vec!["y.parquet".into()])
        }
        .committed(vec![
            super::journal::output_prefix("0a1b2c3d", copy) + "000001.parquet",
        ])
    };
    let setup = |journal: &RollupJournal, output_present: bool| {
        let fixture = Fixture::new(Vec::new());
        {
            let mut dirs = fixture.dirs.borrow_mut();
            let b = dirs.entry("b".into()).or_default();
            b.insert(ROLLUP_JOURNAL_FILE.into(), journal.encode().unwrap());
            if output_present {
                b.insert(journal.outputs[0].clone(), Vec::new());
            }
        }
        fixture
    };
    let missing = setup(&committed("fixture-source", false), false);
    let error = recover(&missing).unwrap_err().to_string();
    assert!(error.contains("which is missing"), "{error}");
    assert!(missing.journal("b").is_some());

    let foreign = setup(&committed("other-source", false), true);
    let error = recover(&foreign).unwrap_err().to_string();
    assert!(error.contains("rolling up that source"), "{error}");
    assert!(foreign.journal("b").is_some());
    assert!(!foreign
        .events()
        .iter()
        .any(|event| event.starts_with("delete_recorded")));

    // A committed copy only cleans the output, so any source root may finish it.
    let copy = setup(&committed("other-source", true), true);
    recover(&copy).unwrap();
    assert!(copy.journal("b").is_none());
    assert!(!copy
        .events()
        .iter()
        .any(|event| event.starts_with("delete_recorded")));
}
