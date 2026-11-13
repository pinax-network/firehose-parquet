use super::*;
use arrow::array::{
    ArrayRef, BinaryArray, Int64Array, StringArray, StructArray, TimestampMillisecondArray,
    UInt64Array,
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

fn fixture(binary_ids: bool, timestamp: bool) -> Bytes {
    // Two physical leaves precede canonical roots, so root and leaf indices differ.
    let nested = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::UInt64, false)),
            Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef,
        ),
    ]);
    let mut columns: Vec<ArrayRef> = vec![Arc::new(nested)];
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
    columns.push(Arc::new(StringArray::from(vec!["unused"; 3])));
    // A Delta `long`.
    columns.push(Arc::new(Int64Array::from(vec![1, 2, 3])));
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
        columns.push(Arc::new(
            TimestampMillisecondArray::from(vec![Some(100_000), None, Some(90_000)])
                .with_timezone("UTC"),
        ));
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
            let data = fixture(binary, timestamp);
            let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();
            let full_schema = builder.schema().as_ref().clone();
            let indices = find_canonical_indices(&full_schema).unwrap();
            let forbidden = builder
                .metadata()
                .row_groups()
                .iter()
                .flat_map(|group| {
                    [0, 1, 3].into_iter().map(move |i| {
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
            let tuples = read_validation_columns(RejectPayloadReads { data, forbidden }).unwrap();
            assert_eq!(tuples, expected);
            assert_eq!(tuples.len(), 3);
            assert_eq!(tuples[1].3, None);
            assert_eq!(
                check_tuples(&tuples).timestamp_reversals.len(),
                usize::from(timestamp)
            );
            let result = validate_from_files(
                vec![FileInfo {
                    partition: "date=1970-01-01".into(),
                    tuples,
                }],
                &ValidateOptions::default(),
            );
            assert!(result.is_valid());
            assert_eq!(result.total_blocks, 3);
            assert_eq!(result.timestamp_reversals.len(), usize::from(timestamp));
        }
    }
}

fn file(partition: &str, tuples: &[(u64, &str, &str)]) -> FileInfo {
    FileInfo {
        partition: partition.into(),
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
fn cross_partition_preserves_gaps_and_options() {
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
            // A file without rows adds no block and no boundary.
            assert_eq!(result.partitions.len(), 3);
            assert_eq!(result.partitions[1].total_blocks, 0);
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

/// Blocks `first..=last` of one chain: block `n` is `b<n>` with parent `b<n-1>`.
fn chain(first: u64, last: u64) -> Vec<BlockTuple> {
    (first..=last)
        .map(|n| (n, format!("b{n}"), format!("b{}", n.wrapping_sub(1)), None))
        .collect()
}

/// The range check of partitions given as their sorted tuples.
fn range_problems(partitions: &[Vec<BlockTuple>], start: u64, stop: u64) -> Vec<String> {
    let mut check = BlockRangeCheck::default();
    let mut runs = Vec::new();
    for partition in partitions {
        block_runs(partition, &mut runs, &mut check);
    }
    join_block_runs(runs, start, stop, &mut check);
    assert_eq!(check.problem_count, check.problems.len() as u64);
    check.problems
}

#[test]
fn block_range_joins_partitions_in_block_order() {
    // Partitions in any order, and one whose blocks interleave with its
    // neighbour's (a block time earlier than its parent's).
    let mut split = chain(100, 104);
    split.extend(chain(106, 106));
    let mut other = chain(105, 105);
    other.extend(chain(107, 109));
    assert!(range_problems(&[chain(110, 119), other, split], 100, 120).is_empty());
}

#[test]
fn block_range_refuses_a_table_that_is_not_the_range() {
    let one = |partitions: &[Vec<BlockTuple>]| {
        let problems = range_problems(partitions, 100, 110);
        assert_eq!(problems.len(), 1, "{problems:?}");
        problems.into_iter().next().unwrap()
    };
    // The beginning and the end, which no pair of rows shows.
    assert_eq!(
        one(&[chain(101, 109)]),
        "the first block is 101, not the start block 100"
    );
    assert_eq!(one(&[chain(100, 108)]), "the last block is 108, not 109");
    assert_eq!(one(&[chain(100, 110)]), "the last block is 110, not 109");
    assert_eq!(one(&[]), "the table has no blocks");
    // Between partitions, and inside one.
    assert_eq!(
        one(&[chain(100, 103), chain(105, 109)]),
        "blocks 104 to 104 are missing"
    );
    assert_eq!(
        one(&[chain(100, 105), chain(104, 109)]),
        "blocks 104 to 105 are stored more than once"
    );
    let mut twice = chain(100, 109);
    twice.insert(3, twice[3].clone());
    assert_eq!(one(&[twice]), "block 103 is stored more than once");
    // A broken parent link inside a partition, and where two partitions join.
    let mut broken = chain(100, 109);
    broken[4].2 = "fork".into();
    assert_eq!(
        one(&[broken]),
        "block 104 has parent fork, but block 103 is b103"
    );
    let mut tail = chain(105, 109);
    tail[0].2 = "fork".into();
    assert_eq!(
        one(&[chain(100, 104), tail]),
        "block 105 has parent fork, but block 104 is b104"
    );
}

#[test]
fn block_range_lists_a_bounded_number_of_problems() {
    let gapped: Vec<BlockTuple> = (0..100).flat_map(|n| chain(n * 2, n * 2)).collect();
    let mut check = BlockRangeCheck::default();
    let mut runs = Vec::new();
    block_runs(&gapped, &mut runs, &mut check);
    join_block_runs(runs, 0, 199, &mut check);
    assert_eq!(check.problem_count, 99);
    assert_eq!(check.problems.len(), LISTED_RANGE_PROBLEMS);
}
