use super::*;
use arrow::array::UInt64Builder;
use arrow::datatypes::DataType;
fn make_test_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_number",
        DataType::UInt64,
        false,
    )]));
    let mut builder = UInt64Builder::new();
    for i in 0..rows {
        builder.append_value(i as u64);
    }
    RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap()
}

#[test]
fn streaming_writer_counts_closed_row_groups_for_row_and_byte_limits() {
    let batch = make_test_batch(10);
    for (bytes, rows) in [(0, Some(5)), (128, None)] {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer = StreamingPartWriter::new(batch.schema(), props, bytes, rows, 0);
        let mut emitted = Vec::new();
        let mut output = |part, data: Vec<u8>, rows| {
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))?;
            assert_eq!(reader.metadata().file_metadata().num_rows() as usize, rows);
            emitted.push((part, rows));
            Ok(())
        };
        writer.write_batch(&batch, &mut output).unwrap();
        assert!(
            writer.current_writer.is_none(),
            "closed row groups must count toward the flush limit"
        );
        writer.finish(&mut output).unwrap();
        assert_eq!(emitted, [(1, 10)]);
    }
}

#[test]
fn streaming_writer_flushes_dictionary_memory_without_publishing_a_small_part() {
    let batch = make_test_batch(1024);
    let mut writer = StreamingPartWriter::new(
        batch.schema(),
        WriterProperties::builder().build(),
        32 * 1024,
        None,
        0,
    );
    writer.row_group_memory_bytes = 32 * 1024;
    let mut outputs = Vec::new();
    let mut publish = |_, bytes: Vec<u8>, rows| {
        outputs.push((bytes, rows));
        Ok(())
    };
    writer.write_batch(&batch, &mut publish).unwrap();
    let active = writer
        .current_writer
        .as_ref()
        .expect("memory flush should retain the compressed part");
    assert_eq!(
        active.in_progress_rows(),
        0,
        "the dictionary row group must have been flushed"
    );
    assert!(active.bytes_written() < 32 * 1024);
    assert_eq!(writer.current_rows, 1024);
    assert_eq!(writer.next_part_num, 0);
    writer.finish(&mut publish).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].1, 1024);
}

#[test]
fn streaming_writer_keeps_only_current_part_across_a_large_group() {
    let batch = make_test_batch(1024);
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2048))
        .build();
    let mut writer = StreamingPartWriter::new(batch.schema(), props, 1024 * 1024, None, 0);
    let mut total = 0;
    let mut output = |_, _: Vec<u8>, rows| {
        total += rows;
        Ok(())
    };
    let mut peak = 0;
    for _ in 0..512 {
        writer.write_batch(&batch, &mut output).unwrap();
        if let Some(active) = &writer.current_writer {
            let retained = active
                .bytes_written()
                .saturating_add(active.memory_size().max(active.in_progress_size()));
            peak = peak.max(retained);
            assert!(
                active.memory_size() < ROW_GROUP_MEMORY_BUDGET_BYTES,
                "the active row group must flush at the memory target"
            );
            assert!(
                active
                    .bytes_written()
                    .saturating_add(active.in_progress_size())
                    < 1024 * 1024,
                "the output part must flush at the encoded byte target"
            );
            assert!(retained < ROW_GROUP_MEMORY_BUDGET_BYTES + 1024 * 1024);
        }
    }
    writer.finish(&mut output).unwrap();
    assert_eq!(total, 512 * 1024);
    assert!(peak > 0);
    assert!(writer.next_part_num > 1);
}

fn source_file(rows: usize, marker: Option<Option<&str>>) -> bytes::Bytes {
    use parquet::arrow::arrow_writer::ArrowWriterOptions;
    let metadata = marker.map(|marker| {
        marker
            .into_iter()
            .map(|value| KeyValue::new("audit-source".to_owned(), Some(value.to_owned())))
            .collect()
    });
    let props = WriterProperties::builder()
        .set_key_value_metadata(metadata)
        .build();
    let options = ArrowWriterOptions::new()
        .with_properties(props)
        .with_skip_arrow_metadata(true);
    let batch = make_test_batch(rows);
    let mut bytes = Vec::new();
    let mut writer =
        ArrowWriter::try_new_with_options(&mut bytes, batch.schema(), options).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = bytes::Bytes::from(bytes);
    let parsed = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    assert_eq!(
        parsed
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .is_none(),
        marker.is_none()
    );
    bytes
}

#[test]
fn source_footer_choice_is_frozen_at_each_commands_existing_boundary() {
    // None is no footer list; Some(None) is an explicitly empty list. Merge
    // freezes its first-Some selection when the first nonempty batch arrives;
    // rollup freezes its first input even if that input has no rows or metadata.
    let cases = [
        (
            vec![(0, None), (0, Some(Some("early"))), (1, Some(Some("late")))],
            Some("early"),
            None,
        ),
        (vec![(1, None), (1, Some(Some("late")))], None, None),
        (
            vec![(0, Some(Some("first"))), (1, Some(Some("later")))],
            Some("first"),
            Some("first"),
        ),
        (vec![(0, Some(None)), (1, Some(Some("later")))], None, None),
        (
            vec![(0, None), (2, Some(Some("first")))],
            Some("first"),
            None,
        ),
        (vec![(0, None), (0, Some(Some("empty")))], None, None),
    ];
    for (inputs, merge_expected, rollup_expected) in cases {
        let files: Vec<_> = inputs
            .iter()
            .map(|(rows, metadata)| source_file(*rows, *metadata))
            .collect();
        let expected_rows: usize = inputs.iter().map(|(rows, _)| rows).sum();
        for (rollup, expected) in [(false, merge_expected), (true, rollup_expected)] {
            let first = ParquetRecordBatchReaderBuilder::try_new(files[0].clone()).unwrap();
            let mut encoder = if rollup {
                Encoder::rollup(
                    first.schema().clone(),
                    Compression::None,
                    first
                        .metadata()
                        .file_metadata()
                        .key_value_metadata()
                        .map(Vec::as_slice),
                    0,
                )
            } else {
                Encoder::merge(Compression::None, 0, None, 0)
            };
            let mut emitted = Vec::new();
            let mut publish = |_, bytes, _| {
                emitted.push(bytes);
                Ok(())
            };
            let mut checked_rows = 0;
            for bytes in &files {
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
                encoder
                    .write_reader(builder, &mut publish, rollup.then_some(&mut checked_rows))
                    .unwrap();
            }
            if !rollup {
                assert_eq!(encoder.initialized(), expected_rows > 0);
            }
            encoder.finish(&mut publish).unwrap();
            if rollup {
                assert_eq!(checked_rows, expected_rows);
            }
            assert_eq!(emitted.len(), usize::from(expected_rows > 0));
            for bytes in emitted {
                let reader =
                    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
                assert_eq!(
                    reader.metadata().file_metadata().num_rows() as usize,
                    expected_rows
                );
                let marker = reader
                    .metadata()
                    .file_metadata()
                    .key_value_metadata()
                    .unwrap()
                    .iter()
                    .find(|kv| kv.key == "audit-source")
                    .and_then(|kv| kv.value.as_deref());
                assert_eq!(marker, expected, "rollup={rollup}, inputs={inputs:?}");
                assert_eq!(
                    reader.schema().fields(),
                    make_test_batch(0).schema().fields()
                );
            }
        }
    }
}

/// One emitted output part: (part number, encoded bytes, rows).
type Parts = Vec<(u32, Vec<u8>, usize)>;

/// A source file whose Arrow schema and footer carry a transaction receipt that
/// compaction must strip, plus ordinary footer metadata it must keep.
fn receipt_file(rows: u64, start: u64, row_group: usize, footer: Option<&str>) -> bytes::Bytes {
    use arrow::array::{StringArray, UInt64Array};
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new("note", DataType::Utf8, true),
        ],
        std::collections::HashMap::from([
            (
                "fireparq.ingest.transaction".to_owned(),
                "receipt".to_owned(),
            ),
            ("kept".to_owned(), "schema".to_owned()),
        ]),
    ));
    let values = UInt64Array::from_iter_values(start..start + rows);
    let notes = StringArray::from_iter(
        (start..start + rows).map(|n| (n % 3 != 0).then(|| format!("n{}", n % 5))),
    );
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(values), Arc::new(notes)]).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group))
        .set_key_value_metadata(footer.map(|value| {
            vec![
                KeyValue::new("fireparq.ingest.part".to_owned(), "receipt".to_owned()),
                KeyValue::new("firehose-parquet.block_type".to_owned(), value.to_owned()),
            ]
        }))
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.into_inner().unwrap().into()
}

// Frozen from origin/main 9372f99 process_local_partition / process_s3_partition:
// lazy writer on the first nonempty batch, first-Some footer until then.
fn legacy_merge(
    files: &[bytes::Bytes],
    compression: Compression,
    flush_bytes: u64,
    flush_rows: Option<u32>,
    initial_part_num: u32,
) -> Parts {
    let mut parts = Parts::new();
    let mut write_part = |part_num: u32, buf: Vec<u8>, rows: usize| -> Result<()> {
        parts.push((part_num, buf, rows));
        Ok(())
    };
    let mut file_kv_metadata: Option<Vec<KeyValue>> = None;
    let mut writer_state: Option<StreamingPartWriter> = None;
    for data in files {
        let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();
        if file_kv_metadata.is_none() {
            file_kv_metadata = builder
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .cloned();
        }
        let reader = builder.build().unwrap();
        for batch_result in reader {
            let batch = strip_transaction_metadata(batch_result.unwrap()).unwrap();
            if batch.num_rows() == 0 {
                continue;
            }
            if writer_state.is_none() {
                let props = writer_properties(
                    compression,
                    batch.schema().as_ref(),
                    file_kv_metadata.as_deref(),
                );
                writer_state = Some(StreamingPartWriter::new(
                    batch.schema(),
                    props,
                    flush_bytes,
                    flush_rows,
                    initial_part_num,
                ));
            }
            writer_state
                .as_mut()
                .expect("writer state must exist")
                .write_batch(&batch, &mut write_part)
                .unwrap();
        }
    }
    if let Some(writer_state) = writer_state.as_mut() {
        writer_state.finish(&mut write_part).unwrap();
    }
    parts
}

// Frozen from origin/main 9372f99 run_rollup_local / rollup_s3 (second pass): the
// first file's stripped schema and footer, all batches counted and written.
fn legacy_rollup(
    files: &[bytes::Bytes],
    compression: Compression,
    flush_bytes: u64,
) -> (Parts, usize) {
    let first = ParquetRecordBatchReaderBuilder::try_new(files[0].clone())
        .unwrap()
        .with_batch_size(1024);
    let schema = strip_transaction_schema(first.schema().clone());
    let file_kv_metadata = first
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();
    let props = writer_properties(compression, schema.as_ref(), file_kv_metadata.as_deref());
    let mut writer = StreamingPartWriter::new(schema, props, flush_bytes, None, 0);
    let mut parts = Parts::new();
    let mut publish = |part: u32, bytes: Vec<u8>, part_rows: usize| -> Result<()> {
        parts.push((part, bytes, part_rows));
        Ok(())
    };
    let mut written_rows = 0usize;
    for data in files {
        let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())
            .unwrap()
            .with_batch_size(1024);
        for batch in builder.build().unwrap() {
            let batch = strip_transaction_metadata(batch.unwrap()).unwrap();
            written_rows = written_rows.checked_add(batch.num_rows()).unwrap();
            writer.write_batch(&batch, &mut publish).unwrap();
        }
    }
    writer.finish(&mut publish).unwrap();
    (parts, written_rows)
}

#[test]
fn encoder_output_is_byte_identical_to_the_frozen_merge_and_rollup_loops() {
    let inputs: Vec<(&str, Vec<bytes::Bytes>)> = vec![
        (
            "plain",
            vec![
                receipt_file(1500, 0, 700, Some("evm")),
                receipt_file(900, 5000, 256, Some("evm")),
                receipt_file(2100, 9000, 1024, Some("evm")),
            ],
        ),
        (
            "leading-empty-and-late-footer",
            vec![
                receipt_file(0, 0, 64, None),
                receipt_file(0, 0, 64, Some("early")),
                receipt_file(1200, 100, 300, Some("late")),
                receipt_file(0, 0, 64, None),
                receipt_file(800, 4000, 300, None),
            ],
        ),
        (
            "all-empty",
            vec![
                receipt_file(0, 0, 64, Some("a")),
                receipt_file(0, 0, 64, None),
            ],
        ),
        ("single", vec![receipt_file(3000, 0, 512, Some("solo"))]),
    ];
    let merge_settings: [(Compression, u64, Option<u32>, u32); 5] = [
        (Compression::None, 0, None, 0),
        (Compression::Snappy, 32 * 1024 * 1024, None, 7),
        (Compression::None, 0, Some(1000), 41),
        (Compression::Zstd, 4096, None, 0),
        (Compression::None, 1, Some(0), 2),
    ];
    for (name, files) in &inputs {
        for (compression, flush_bytes, flush_rows, initial) in merge_settings {
            let expected = legacy_merge(files, compression, flush_bytes, flush_rows, initial);
            let mut encoder = Encoder::merge(compression, flush_bytes, flush_rows, initial);
            let mut actual = Parts::new();
            let mut publish = |part: u32, buf: Vec<u8>, rows: usize| -> Result<()> {
                actual.push((part, buf, rows));
                Ok(())
            };
            for data in files {
                let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();
                encoder.write_reader(builder, &mut publish, None).unwrap();
            }
            let initialized = encoder.initialized();
            encoder.finish(&mut publish).unwrap();
            assert_eq!(initialized, !expected.is_empty(), "merge {name}");
            assert!(
                actual == expected,
                "merge {name} {flush_bytes} {flush_rows:?}"
            );
        }
        for (compression, flush_bytes) in [
            (Compression::None, 0),
            (Compression::Zstd, 16 * 1024),
            (Compression::Snappy, 1),
        ] {
            let (expected, expected_rows) = legacy_rollup(files, compression, flush_bytes);
            let first = ParquetRecordBatchReaderBuilder::try_new(files[0].clone()).unwrap();
            let mut encoder = Encoder::rollup(
                strip_transaction_schema(first.schema().clone()),
                compression,
                first
                    .metadata()
                    .file_metadata()
                    .key_value_metadata()
                    .map(Vec::as_slice),
                flush_bytes,
            );
            let mut actual = Parts::new();
            let mut publish = |part: u32, buf: Vec<u8>, rows: usize| -> Result<()> {
                actual.push((part, buf, rows));
                Ok(())
            };
            let mut rows = 0usize;
            for data in files {
                let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())
                    .unwrap()
                    .with_batch_size(1024);
                encoder
                    .write_reader(builder, &mut publish, Some(&mut rows))
                    .unwrap();
            }
            encoder.finish(&mut publish).unwrap();
            assert_eq!(rows, expected_rows, "rollup {name}");
            assert!(actual == expected, "rollup {name} {flush_bytes}");
        }
    }
    // The receipt is stripped and ordinary metadata kept in every emitted part.
    let parts = legacy_merge(&inputs[0].1, Compression::None, 0, None, 0);
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(parts[0].1.clone())).unwrap();
    let footer = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap();
    assert!(footer
        .iter()
        .all(|kv| !kv.key.starts_with("fireparq.ingest.")));
    assert!(footer
        .iter()
        .any(|kv| kv.key == "firehose-parquet.block_type"));
    assert!(!reader
        .schema()
        .metadata()
        .contains_key("fireparq.ingest.transaction"));
}
