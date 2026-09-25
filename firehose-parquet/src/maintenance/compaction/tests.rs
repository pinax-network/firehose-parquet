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
