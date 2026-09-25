use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, DictionaryArray, Float64Array, Int32Array,
    Int64Array, ListArray, StringArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{Field, Int32Type, Schema, UInt64Type};
use arrow::record_batch::RecordBatch;
use firehose_parquet::cursor::CursorState;
use firehose_parquet::writer::ParquetFileMetadata;

fn compatibility_batch() -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "block_num",
            Arc::new(UInt64Array::from(vec![100, 101, 102])),
        ),
        (
            "signed",
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
        ),
        (
            "unsigned",
            Arc::new(UInt32Array::from(vec![0, 1, u32::MAX])),
        ),
        ("index", Arc::new(Int32Array::from(vec![-1, 0, i32::MAX]))),
        (
            "amount",
            Arc::new(Float64Array::from(vec![Some(-0.0), None, Some(1.5)])),
        ),
        (
            "success",
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        ),
        (
            "label",
            Arc::new(StringArray::from(vec![Some("transfer"), Some(""), None])),
        ),
        (
            "hash",
            Arc::new(BinaryArray::from(vec![
                Some(&[0, 255][..]),
                None,
                Some(&[][..]),
            ])),
        ),
        (
            "timestamp",
            Arc::new(
                TimestampMillisecondArray::from(vec![
                    1_700_000_000_250,
                    1_700_000_001_250,
                    1_700_000_002_250,
                ])
                .with_timezone("UTC"),
            ),
        ),
        ("date", Arc::new(Date32Array::from(vec![0, 1, 19_675]))),
        (
            "indices",
            Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![
                Some(vec![Some(1), None, Some(u64::MAX)]),
                Some(vec![]),
                None,
            ])),
        ),
        (
            "kind",
            Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                Some("NEW"),
                Some("UNDO"),
                None,
            ])),
        ),
    ];
    let schema = Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(
        Arc::new(schema),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap()
}

fn compatibility_cursor() -> CursorState {
    let mut file_metadata = ParquetFileMetadata::new();
    file_metadata.add("firehose-parquet.chain_name", "mainnet");
    file_metadata.add("firehose-parquet.block_type", "evm");
    file_metadata.add("firehose-parquet.bytes_encoding", "binary");
    CursorState {
        cursor: "fixture-resume-cursor".into(),
        last_block_num: 102,
        last_block_id: vec![0, 1, 255],
        last_timestamp: Some(1_700_000_002),
        updated_at: "2026-09-25T00:00:00Z".into(),
        start_block: Some(100),
        stop_block: Some(200),
        extended: true,
        final_blocks_only: false,
        include_failed_transactions: true,
        file_metadata,
    }
}

#[test]
fn reads_parquet58_values_and_writes_them_without_schema_changes() {
    use arrow::compute::concat_batches;
    use firehose_parquet::config::{BlockMetadata, Compression, Partition};
    use firehose_parquet::writer::{read_parquet, ParquetTableWriter};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let bytes = bytes::Bytes::from_static(include_bytes!("fixtures/parquet58/types.parquet"));
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    assert_eq!(
        reader.metadata().file_metadata().created_by(),
        Some("parquet-rs version 58.0.0")
    );
    let batches = reader
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = compatibility_batch();
    assert!(batches
        .iter()
        .all(|batch| batch.schema() == expected.schema()));
    let actual = concat_batches(&expected.schema(), &batches).unwrap();
    assert_eq!(actual, expected);

    for compression in [
        Compression::None,
        Compression::Snappy,
        Compression::Gzip,
        Compression::Zstd,
    ] {
        let output = tempfile::tempdir().unwrap();
        let mut writer = ParquetTableWriter::new(output.path(), Partition::None, compression);
        let (path, _) = writer
            .write_batch(
                "types",
                &actual,
                &BlockMetadata {
                    min_block_number: 100,
                    max_block_number: 102,
                    min_timestamp: Some(1_700_000_000),
                    max_timestamp: Some(1_700_000_002),
                },
            )
            .unwrap();
        let rewritten = read_parquet(&path).unwrap();
        assert!(rewritten
            .iter()
            .all(|batch| batch.schema() == expected.schema()));
        assert_eq!(
            concat_batches(&expected.schema(), &rewritten).unwrap(),
            expected
        );
    }
}

#[test]
fn resumes_from_parquet58_cursor_and_preserves_its_metadata() {
    use firehose_parquet::cursor::{load_cursor_parquet, save_cursor_parquet};
    let output = tempfile::tempdir().unwrap();
    let path = output.path().join("cursor.parquet");
    std::fs::write(&path, include_bytes!("fixtures/parquet58/cursor.parquet")).unwrap();
    let original = load_cursor_parquet(&path).unwrap().unwrap();
    let expected = compatibility_cursor();
    let assert_state = |actual: &CursorState| {
        assert_eq!(actual.cursor, expected.cursor);
        assert_eq!(actual.last_block_num, expected.last_block_num);
        assert_eq!(actual.last_block_id, expected.last_block_id);
        assert_eq!(actual.last_timestamp, expected.last_timestamp);
        assert_eq!(actual.updated_at, expected.updated_at);
        assert_eq!(actual.start_block, expected.start_block);
        assert_eq!(actual.stop_block, expected.stop_block);
        assert_eq!(actual.extended, expected.extended);
        assert_eq!(actual.final_blocks_only, expected.final_blocks_only);
        assert_eq!(
            actual.include_failed_transactions,
            expected.include_failed_transactions
        );
        for (key, value) in &expected.file_metadata.entries {
            assert_eq!(actual.get_metadata(key), Some(value.as_str()));
        }
    };
    assert_state(&original);
    save_cursor_parquet(&path, &original).unwrap();
    assert_state(&load_cursor_parquet(&path).unwrap().unwrap());
}

#[test]
fn rejects_footer_schema_list_larger_than_remaining_metadata() {
    use firehose_parquet::cursor::load_cursor_parquet;
    use firehose_parquet::writer::read_parquet;

    // Compact Thrift FileMetaData: version=1, schema=list<struct> of 4096
    // elements, followed by just one STOP byte. The footer length and both
    // magic markers are valid, so this exercises the metadata list bound,
    // rather than rejecting a bad Parquet envelope. A modest count keeps a
    // future regression from exhausting the test runner's memory.
    let footer = [0x15, 0x02, 0x19, 0xfc, 0x80, 0x20, 0x00];
    let mut bytes = b"PAR1".to_vec();
    bytes.extend_from_slice(&footer);
    bytes.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    bytes.extend_from_slice(b"PAR1");
    let output = tempfile::tempdir().unwrap();
    let path = output.path().join("malformed.parquet");
    std::fs::write(&path, bytes).unwrap();

    // Both maintenance/table reads and cursor startup must propagate the
    // bounded rejection, rather than allocating from the untrusted count or
    // interpreting the corrupt cursor as a fresh start.
    for error in [
        read_parquet(&path).unwrap_err(),
        load_cursor_parquet(&path).unwrap_err(),
    ] {
        assert!(
            format!("{error:#}").contains("Thrift list size 4096 exceeds remaining input length 1"),
            "unexpected rejection: {error:#}"
        );
    }
}
