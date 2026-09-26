//! Pins the order of the shared engine's storage hooks, so the local and S3 rollups
//! keep their per-group barriers (validate all, then publish, clean up, delete).
use super::*;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use std::cell::RefCell;

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

struct Fixture {
    validate: Vec<bytes::Bytes>,
    encode: Vec<bytes::Bytes>,
    events: RefCell<Vec<String>>,
    fail: Option<&'static str>,
}

impl Fixture {
    fn new(inputs: Vec<bytes::Bytes>) -> Self {
        Self {
            validate: inputs.clone(),
            encode: inputs,
            events: RefCell::new(Vec::new()),
            fail: None,
        }
    }

    fn event(&self, event: String) -> Result<()> {
        let fail = self.fail.is_some_and(|prefix| event.starts_with(prefix));
        self.events.borrow_mut().push(event);
        anyhow::ensure!(!fail, "fixture backend failure");
        Ok(())
    }
}

impl Backend for Fixture {
    type Source = usize;
    type Output = String;
    type Reader = bytes::Bytes;

    fn label(&self, source: &usize) -> String {
        format!("source-{source}")
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

    fn publish(&self, group: &str, name: &str, bytes: Vec<u8>, rows: usize) -> Result<String> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))?;
        assert_eq!(reader.metadata().file_metadata().num_rows() as usize, rows);
        self.event(format!("publish:{group}:{rows}"))?;
        Ok(format!("{group}/{name}"))
    }

    fn remove_previous(&self, group: &str, written: &HashSet<String>) -> Result<()> {
        assert!(written
            .iter()
            .any(|name| name.starts_with(&format!("{group}/"))));
        self.event(format!("cleanup:{group}"))
    }

    fn delete_sources(&self, sources: &[usize], _written: &HashSet<String>) -> Result<usize> {
        self.event(format!("delete:{}", sources[0]))?;
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

#[test]
fn two_pass_group_barriers_and_cleanup_order_are_explicit() {
    let fixture = Fixture::new(vec![source(2, "value"); 3]);
    run(&fixture, &groups(), &config()).unwrap();
    assert_eq!(
        *fixture.events.borrow(),
        [
            "validate:0",
            "validate:1",
            "begin:a",
            "encode:0",
            "encode:1",
            "publish:a:4",
            "cleanup:a",
            "delete:0",
            "validate:2",
            "begin:b",
            "encode:2",
            "publish:b:2",
            "cleanup:b",
            "delete:2",
            "finish:3"
        ]
    );
}

#[test]
fn copy_mode_never_deletes_sources() {
    let fixture = Fixture::new(vec![source(2, "value"); 3]);
    let mut config = config();
    config.delete_source = false;
    run(&fixture, &groups(), &config).unwrap();
    let events = fixture.events.borrow();
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
fn empty_and_corrupt_preflight_never_enter_mutation_hooks() {
    let zero = Fixture::new(vec![source(0, "value"); 3]);
    run(&zero, &groups(), &config()).unwrap();
    assert_eq!(
        *zero.events.borrow(),
        ["validate:0", "validate:1", "validate:2"]
    );
    let corrupt = Fixture::new(vec![
        source(2, "value"),
        bytes::Bytes::from_static(b"corrupt"),
        source(2, "value"),
    ]);
    assert!(run(&corrupt, &groups(), &config()).is_err());
    assert_eq!(*corrupt.events.borrow(), ["validate:0", "validate:1"]);
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
    assert_eq!(
        *fixture.events.borrow(),
        [
            "validate:0",
            "validate:1",
            "validate:2",
            "begin:b",
            "encode:2",
            "publish:b:2",
            "cleanup:b",
            "delete:2",
            "finish:1"
        ]
    );
}

#[test]
fn mutation_failure_stops_later_hooks_and_groups() {
    for failure in ["begin:", "publish:", "cleanup:", "delete:"] {
        let mut fixture = Fixture::new(vec![source(2, "value"); 3]);
        fixture.fail = Some(failure);
        assert!(run(&fixture, &groups(), &config()).is_err());
        let events = fixture.events.borrow();
        assert!(events.last().unwrap().starts_with(failure), "{failure}");
        assert!(!events
            .iter()
            .any(|event| event == "validate:2" || event.starts_with("finish:")));
    }
}

#[test]
fn changed_row_count_precedes_tail_flush_and_cleanup_even_after_partial_output() {
    for flush_bytes in [0, 1] {
        let mut fixture = Fixture::new(vec![source(2, "value"); 3]);
        fixture.encode[1] = source(1, "value");
        let mut config = config();
        config.flush_bytes = flush_bytes;
        let error = run(&fixture, &groups(), &config).unwrap_err();
        assert!(error
            .to_string()
            .contains("row count changed after preflight"));
        let events = fixture.events.borrow();
        assert!(!events.iter().any(|event| event.starts_with("cleanup:")
            || event.starts_with("delete:")
            || event == "validate:2"));
        assert_eq!(
            events
                .iter()
                .filter(|event| event.starts_with("publish:"))
                .count(),
            if flush_bytes == 0 { 0 } else { 2 }
        );
    }
}
