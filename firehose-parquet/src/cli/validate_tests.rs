use super::*;
use arrow::array::{
    ArrayRef, BinaryArray, Int64Array, ListBuilder, StringArray, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::file::{
    properties::WriterProperties,
    reader::{ChunkReader, Length},
};
use std::sync::Arc;

fn fixture(binary_ids: bool, timestamp: bool, numeric_payload: bool) -> Bytes {
    let mut nested = ListBuilder::new(UInt64Builder::new());
    for _ in 0..3 {
        nested.values().append_slice(&[1, 2, 3]);
        nested.append(true);
    }
    let mut columns: Vec<ArrayRef> = vec![Arc::new(nested.finish())];
    let ids = |values: &[&str]| -> ArrayRef {
        if binary_ids {
            Arc::new(BinaryArray::from(
                values.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
            ))
        } else {
            Arc::new(StringArray::from(values.to_vec()))
        }
    };
    columns.push(ids(&["p0", "id1", "id2"]));
    columns.push(if numeric_payload {
        Arc::new(UInt64Array::from(vec![42; 3]))
    } else {
        Arc::new(StringArray::from(vec!["unused"; 3]))
    });
    columns.push(Arc::new(UInt64Array::from(vec![1, 2, 3])));
    columns.push(ids(&["id1", "id2", "id3"]));
    let mut names = vec![
        "nested_payload",
        "parent_id",
        "payload",
        "block_num",
        "block_id",
    ];
    if timestamp {
        names.push("timestamp");
        columns.push(Arc::new(Int64Array::from(vec![Some(100), None, Some(90)])));
    }
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .zip(&columns)
            .map(|(n, c)| Field::new(*n, c.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut data = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut data,
        schema,
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(2))
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(data)
}

/// Fail if the reader requests a page belonging to an unselected column.
struct RejectPayloadReads {
    data: Bytes,
    forbidden: Vec<(u64, u64)>,
}
impl Length for RejectPayloadReads {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}
impl ChunkReader for RejectPayloadReads {
    type T = std::io::Cursor<Bytes>;
    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        assert!(
            !self.forbidden.iter().any(|&(a, b)| a <= start && start < b),
            "read unselected payload"
        );
        Ok(std::io::Cursor::new(self.data.slice(start as usize..)))
    }
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let end = start + length as u64;
        assert!(
            !self.forbidden.iter().any(|&(a, b)| a < end && start < b),
            "read unselected payload"
        );
        Ok(self.data.slice(start as usize..end as usize))
    }
}

#[test]
fn projected_validation_skips_payload_pages_and_remaps_reordered_roots() {
    for binary in [false, true] {
        for timestamp in [false, true] {
            let data = fixture(binary, timestamp, false);
            let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();
            let full_schema = builder.schema().as_ref().clone();
            let indices = find_canonical_indices(&full_schema).unwrap();
            let forbidden = builder
                .metadata()
                .row_groups()
                .iter()
                .flat_map(|group| {
                    [0, 2].into_iter().map(move |i| {
                        let (a, n) = group.column(i).byte_range();
                        (a, a + n)
                    })
                })
                .collect();
            let expected = extract_block_tuples(
                builder.build().unwrap(),
                indices.block_num,
                indices.block_id,
                indices.parent_id,
                indices.timestamp,
            )
            .unwrap();
            let (schema, tuples, rows) = read_validation_columns(RejectPayloadReads {
                data: data.clone(),
                forbidden,
            })
            .unwrap();
            assert_eq!(schema, full_schema);
            assert_eq!(tuples, expected);
            assert_eq!(rows, 3);
            assert_eq!(tuples[1].3, None);
            assert_eq!(
                check_tuples(&tuples).timestamp_reversals.len(),
                usize::from(timestamp)
            );
            // Exercise the actual file-backed entry point as well as the Bytes/S3 path.
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("blocks.parquet");
            std::fs::write(&path, &data).unwrap();
            let result = validate_parquet_local(&path, &ValidateOptions::default()).unwrap();
            assert!(result.is_valid());
            assert_eq!(result.total_blocks, 3);
            assert_eq!(result.timestamp_reversals.len(), usize::from(timestamp));
        }
    }
}

#[test]
fn projected_validation_still_compares_unselected_schema_fields() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.parquet"), fixture(false, true, false)).unwrap();
    std::fs::write(dir.path().join("b.parquet"), fixture(false, true, true)).unwrap();
    let result =
        validate_parquet_local(&dir.path().to_path_buf(), &ValidateOptions::default()).unwrap();
    assert_eq!(result.schema_mismatches.len(), 1);
    assert!(result.schema_mismatches[0]
        .details
        .iter()
        .any(|s| s.contains("type mismatch: payload")));
}

fn file(partition: &str, tuples: &[(u64, &str, &str)]) -> FileInfo {
    FileInfo {
        path: format!("{partition}/blocks.parquet"),
        partition: partition.into(),
        schema: Schema::new(vec![Field::new("block_num", DataType::UInt64, false)]),
        row_count: tuples.len() as u64,
        tuples: tuples
            .iter()
            .map(|(n, id, parent)| (*n, (*id).into(), (*parent).into(), None))
            .collect(),
    }
}

#[test]
fn cross_partition_uses_own_endpoints_despite_overlapping_heights() {
    for parent in ["a2", "foreign2"] {
        let result = validate_from_files(
            vec![
                file("part=a", &[(1, "a1", "a0"), (2, "a2", "a1")]),
                file("part=b", &[(3, "b3", parent)]),
                file("part=z", &[(0, "z0", ""), (2, "foreign2", "z1")]),
            ],
            &ValidateOptions {
                cross_partition: true,
                allow_gaps: false,
            },
        );
        assert_eq!(
            result.cross_partition_issues.len(),
            usize::from(parent != "a2")
        );
        if parent != "a2" {
            let issue = &result.cross_partition_issues[0];
            assert_eq!(
                (&*issue.from_partition, &*issue.to_partition),
                ("part=a", "part=b")
            );
            let mismatch = issue.parent_mismatch.as_ref().unwrap();
            assert_eq!(mismatch.expected_parent_id, "a2");
            assert_eq!(mismatch.actual_parent_id, "foreign2");
        }
        assert_eq!(result.duplicates[0].block_num, 2);
    }
}

#[test]
fn cross_partition_preserves_gaps_empty_partitions_and_options() {
    for allow_gaps in [false, true] {
        for cross_partition in [false, true] {
            let result = validate_from_files(
                vec![
                    file("part=a", &[(1, "a1", "a0")]),
                    file("part=b", &[]),
                    file("part=c", &[(4, "c4", "c3")]),
                ],
                &ValidateOptions {
                    cross_partition,
                    allow_gaps,
                },
            );
            assert_eq!(result.files_scanned, 3);
            assert_eq!(result.total_blocks, 2);
            assert_eq!(result.empty_partitions.len(), 1);
            assert_eq!(result.empty_partitions[0].partition, "part=b");
            assert_eq!(
                result.cross_partition_issues.len(),
                usize::from(cross_partition && !allow_gaps)
            );
            assert_eq!(result.gaps.len(), usize::from(!allow_gaps));
            if cross_partition && !allow_gaps {
                let gap = result.cross_partition_issues[0].gap.as_ref().unwrap();
                assert_eq!((gap.from, gap.to), (2, 4));
            }
        }
    }
}

#[test]
fn overlapping_partition_at_max_height_has_no_wrapping_boundary() {
    let result = validate_from_files(
        vec![
            file("part=a", &[(0, "a0", ""), (u64::MAX, "max", "")]),
            file("part=b", &[(1, "b1", "a0")]),
        ],
        &ValidateOptions {
            cross_partition: true,
            allow_gaps: true,
        },
    );
    assert!(result.cross_partition_issues.is_empty());
    assert_eq!(result.max_block, Some(u64::MAX));
}
