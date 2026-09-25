//! Exact partition spans within a declared finalized snapshot.
//!
//! Completeness belongs to an observed contiguous span. A calendar key may recur
//! elsewhere or in future blocks; no index claims a globally complete date.
use crate::cli::{PartitionBuildRow, PartitionBuildType};
use crate::grpc::FinalizedAnchor;
use anyhow::{ensure, Context, Result};

pub const INDEX_COVERAGE_METADATA: &str = "firehose-parquet.partition_coverage";
pub const INDEX_FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexRoutingPolicy {
    BlockNumber,
    CanonicalTimestamp,
    SolanaPriorTimestamp,
}

/// A canonical identity observed in a finalized stream. Parent links establish
/// that gaps in block numbers are skipped slots, not omitted canonical blocks.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CoveredBlock {
    pub block_num: u64,
    pub block_id: String,
    pub parent_num: u64,
    pub parent_id: String,
}

impl From<&crate::traits::BlockIdentity> for CoveredBlock {
    fn from(block: &crate::traits::BlockIdentity) -> Self {
        Self {
            block_num: block.block_num,
            block_id: block.block_id.clone(),
            parent_num: block.parent_num,
            parent_id: block.parent_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionCoverage {
    pub version: u32,
    pub start_block: u64,
    pub stop_block: u64,
    pub finalized: FinalizedAnchor,
    pub routing_policy: IndexRoutingPolicy,
    /// Time indexes record their first/last covered canonical identities.
    /// Deterministic block-number indexes need only the finalized bound.
    pub first_observed: Option<CoveredBlock>,
    pub last_observed: Option<CoveredBlock>,
    /// A bounded post-range witness proves trailing skipped slots and, only
    /// when exactly at stop_block, may prove a natural run boundary there.
    pub next_observed: Option<CoveredBlock>,
    pub last_routing_timestamp: Option<i64>,
}

impl PartitionCoverage {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == INDEX_FORMAT_VERSION,
            "unsupported partition coverage version {}; rebuild the index",
            self.version
        );
        ensure!(
            self.start_block < self.stop_block,
            "partition coverage must be a nonempty exclusive range"
        );
        ensure!(
            !self.finalized.block_id.is_empty(),
            "partition coverage has no finalized block identity"
        );
        ensure!(
            self.finalized.block_num <= i64::MAX as u64,
            "partition finalized anchor exceeds signed stream range"
        );
        ensure!(
            self.stop_block <= self.finalized.exclusive_stop()?,
            "partition coverage exceeds its proven finalized bound"
        );
        if let Some(timestamp) = self.last_routing_timestamp {
            crate::traits::checked_timestamp(timestamp)?;
        }
        if self.routing_policy == IndexRoutingPolicy::BlockNumber {
            return Ok(());
        }
        let first = self
            .first_observed
            .as_ref()
            .context("time partition coverage has no first observed block")?;
        let last = self
            .last_observed
            .as_ref()
            .context("time partition coverage has no last observed block")?;
        ensure!(
            !first.block_id.is_empty() && !last.block_id.is_empty(),
            "time partition coverage has empty observed identities"
        );
        ensure!(
            self.start_block <= first.block_num
                && first.block_num <= last.block_num
                && last.block_num < self.stop_block,
            "observed blocks lie outside partition coverage"
        );
        ensure!(
            first.parent_num < self.start_block
                || (first.block_num == 0 && first.parent_num == 0 && first.parent_id.is_empty()),
            "first covered block has an unobserved parent inside coverage"
        );
        match &self.next_observed {
            Some(next) => {
                ensure!(
                    !next.block_id.is_empty(),
                    "right-edge witness has an empty identity"
                );
                ensure!(
                    next.block_num >= self.stop_block && next.block_num <= self.finalized.block_num,
                    "right-edge witness lies outside the proven finalized range"
                );
                ensure!(
                    next.parent_num == last.block_num && next.parent_id == last.block_id,
                    "right-edge witness does not link to the last covered block"
                );
            }
            None => {
                ensure!(self.stop_block == self.finalized.exclusive_stop()? && last.block_num == self.finalized.block_num && last.block_id == self.finalized.block_id,
                    "partition coverage has neither its finalized anchor nor a linked right-edge witness");
            }
        }
        Ok(())
    }
}

/// Per-span evidence stored alongside the existing index columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpanProof {
    pub start_complete: bool,
    pub end_complete: bool,
    pub first_block: Option<FinalizedAnchor>,
    /// The verified routing timestamp to seed a fresh indexed ingestion when
    /// the first block's canonical timestamp is null (Solana only).
    pub routing_start_timestamp: Option<i64>,
}

impl PartitionSpanProof {
    pub fn complete(&self) -> bool {
        self.start_complete && self.end_complete
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPartitionSpan {
    pub row: PartitionBuildRow,
    pub proof: PartitionSpanProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPartitionIndex {
    pub coverage: PartitionCoverage,
    pub spans: Vec<VerifiedPartitionSpan>,
}

impl VerifiedPartitionIndex {
    pub fn validate(&self) -> Result<()> {
        self.coverage.validate()?;
        ensure!(
            !self.spans.is_empty(),
            "verified partition index contains no spans"
        );
        let first = &self.spans[0].row;
        let kind = PartitionBuildType::from_cli_value(&first.partition_type)?;
        ensure!(
            (kind == PartitionBuildType::BlockRange)
                == (self.coverage.routing_policy == IndexRoutingPolicy::BlockNumber),
            "partition type and coverage routing policy disagree"
        );
        let mut previous_stop = None;
        for span in &self.spans {
            let row = &span.row;
            ensure!(
                row.partition_type == first.partition_type
                    && row.chain == first.chain
                    && row.partition_interval_seconds == first.partition_interval_seconds,
                "one partition index must contain one chain/type/interval"
            );
            ensure!(
                row.chain.as_ref().is_some_and(|chain| !chain.is_empty()),
                "partition span has no chain"
            );
            ensure!(
                self.coverage.start_block <= row.start_block
                    && row.start_block < row.stop_block
                    && row.stop_block <= self.coverage.stop_block,
                "partition span lies outside declared coverage"
            );
            if let Some(previous_stop) = previous_stop {
                ensure!(
                    row.start_block == previous_stop,
                    "partition spans must be contiguous in source block order"
                );
            }
            previous_stop = Some(row.stop_block);
            if kind == PartitionBuildType::BlockRange {
                let size = u64::try_from(row.partition_interval_seconds)
                    .context("invalid block-range interval")?;
                ensure!(
                    size > 0
                        && row.partition_value.parse::<u64>()? == row.start_block
                        && row.start_block % size == 0,
                    "block-range span must begin at its aligned partition value"
                );
                ensure!(
                    row.stop_block - row.start_block <= size,
                    "block-range span exceeds its partition interval"
                );
                ensure!(
                    !span.proof.complete() || row.stop_block - row.start_block == size,
                    "a clipped block-range span cannot be complete"
                );
            } else {
                ensure!(
                    row.partition_interval_seconds == kind.interval_seconds(),
                    "time partition interval disagrees with its type"
                );
                let first_block = span
                    .proof
                    .first_block
                    .as_ref()
                    .context("time partition span has no first block identity")?;
                ensure!(
                    first_block.block_num == row.start_block && !first_block.block_id.is_empty(),
                    "time span first identity disagrees with its start block"
                );
                let timestamp = span
                    .proof
                    .routing_start_timestamp
                    .context("time partition span has no verified routing timestamp")?;
                crate::traits::checked_timestamp(timestamp)?;
                ensure!(
                    u64::try_from(kind.round_timestamp(timestamp)?)? == row.partition_key()?,
                    "time partition key disagrees with its routing timestamp"
                );
            }
        }
        ensure!(
            previous_stop == Some(self.coverage.stop_block),
            "partition spans do not reach the declared coverage stop"
        );
        let first_start = self.spans[0].row.start_block;
        match &self.coverage.first_observed {
            Some(first) => ensure!(
                first_start == first.block_num
                    && self.spans[0]
                        .proof
                        .first_block
                        .as_ref()
                        .is_some_and(|identity| identity.block_id == first.block_id),
                "first span disagrees with first observed block"
            ),
            None => ensure!(
                first_start == self.coverage.start_block,
                "block-number spans do not cover the declared start"
            ),
        }
        let last = self.spans.last().unwrap();
        if kind != PartitionBuildType::BlockRange && last.proof.end_complete {
            ensure!(
                self.coverage
                    .next_observed
                    .as_ref()
                    .is_some_and(|next| next.block_num == self.coverage.stop_block),
                "an open or clipped right edge cannot be complete"
            );
        }
        Ok(())
    }
}

pub(crate) fn proof_fields() -> Vec<arrow::datatypes::Field> {
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use std::sync::Arc;
    vec![
        Field::new("complete", DataType::Boolean, false),
        Field::new("start_complete", DataType::Boolean, false),
        Field::new("end_complete", DataType::Boolean, false),
        Field::new("first_observed_block", DataType::UInt64, true),
        Field::new("first_observed_block_id", DataType::Utf8, true),
        Field::new(
            "routing_start_timestamp",
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
            true,
        ),
    ]
}

pub(crate) fn proof_columns(proofs: &[PartitionSpanProof]) -> Vec<arrow::array::ArrayRef> {
    use arrow::array::{BooleanArray, StringArray, TimestampSecondArray, UInt64Array};
    use std::sync::Arc;
    vec![
        Arc::new(BooleanArray::from(
            proofs
                .iter()
                .map(|proof| proof.complete())
                .collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            proofs
                .iter()
                .map(|proof| proof.start_complete)
                .collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            proofs
                .iter()
                .map(|proof| proof.end_complete)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            proofs
                .iter()
                .map(|proof| proof.first_block.as_ref().map(|block| block.block_num))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            proofs
                .iter()
                .map(|proof| {
                    proof
                        .first_block
                        .as_ref()
                        .map(|block| block.block_id.as_str())
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(
            TimestampSecondArray::from(
                proofs
                    .iter()
                    .map(|proof| proof.routing_start_timestamp)
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
    ]
}

pub(crate) fn read_proof(
    batch: &arrow::record_batch::RecordBatch,
    row: usize,
) -> Result<PartitionSpanProof> {
    use arrow::array::{Array, BooleanArray, StringArray, TimestampSecondArray, UInt64Array};
    let schema = batch.schema();
    for expected in proof_fields() {
        let actual = schema
            .field_with_name(expected.name())
            .with_context(|| format!("verified partition index is missing {}", expected.name()))?;
        ensure!(
            actual == &expected,
            "verified partition column {} has an unexpected type/nullability",
            expected.name()
        );
    }
    let boolean = |name: &str| -> Result<bool> {
        let array = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        ensure!(
            !array.is_null(row),
            "verified partition column {name} has a null value"
        );
        Ok(array.value(row))
    };
    let numbers = batch
        .column_by_name("first_observed_block")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let ids = batch
        .column_by_name("first_observed_block_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    ensure!(
        numbers.is_null(row) == ids.is_null(row),
        "partition span has only half a first-block identity"
    );
    let times = batch
        .column_by_name("routing_start_timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap();
    let proof = PartitionSpanProof {
        start_complete: boolean("start_complete")?,
        end_complete: boolean("end_complete")?,
        first_block: (!numbers.is_null(row)).then(|| FinalizedAnchor {
            block_num: numbers.value(row),
            block_id: ids.value(row).into(),
        }),
        routing_start_timestamp: (!times.is_null(row)).then(|| times.value(row)),
    };
    ensure!(
        proof.complete() == boolean("complete")?,
        "partition completeness disagrees with its boundary flags"
    );
    Ok(proof)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn block(num: u64, parent: u64) -> CoveredBlock {
        CoveredBlock {
            block_num: num,
            block_id: format!("id-{num}"),
            parent_num: parent,
            parent_id: format!("id-{parent}"),
        }
    }
    fn coverage() -> PartitionCoverage {
        PartitionCoverage {
            version: 2,
            start_block: 10,
            stop_block: 13,
            finalized: FinalizedAnchor {
                block_num: 20,
                block_id: "id-20".into(),
            },
            routing_policy: IndexRoutingPolicy::CanonicalTimestamp,
            first_observed: Some(block(10, 9)),
            last_observed: Some(block(12, 11)),
            next_observed: Some(block(13, 12)),
            last_routing_timestamp: Some(1_700_000_000),
        }
    }
    #[test]
    fn coverage_rejects_missing_first_parent_and_unproven_trailing_gaps() {
        coverage().validate().unwrap();
        let mut missing_first = coverage();
        missing_first.first_observed = Some(block(11, 10));
        assert!(missing_first
            .validate()
            .unwrap_err()
            .to_string()
            .contains("unobserved parent"));
        let mut missing_last = coverage();
        missing_last.next_observed = Some(block(14, 13));
        assert!(missing_last
            .validate()
            .unwrap_err()
            .to_string()
            .contains("does not link"));
        let mut no_witness = coverage();
        no_witness.next_observed = None;
        assert!(no_witness.validate().is_err());
        let mut skipped_slots = coverage();
        skipped_slots.first_observed = Some(block(11, 9));
        skipped_slots.next_observed = Some(block(15, 12));
        skipped_slots.validate().unwrap();
    }
    #[test]
    fn coverage_at_head_requires_the_exact_proven_final_identity() {
        let mut value = coverage();
        value.stop_block = 21;
        value.last_observed = Some(block(20, 12));
        value.next_observed = None;
        value.validate().unwrap();
        value.last_observed.as_mut().unwrap().block_id = "other-id".into();
        assert!(value.validate().is_err());
        let mut future = coverage();
        future.stop_block = 22;
        assert!(future
            .validate()
            .unwrap_err()
            .to_string()
            .contains("finalized bound"));
        let mut legacy = coverage();
        legacy.version = 1;
        assert!(legacy
            .validate()
            .unwrap_err()
            .to_string()
            .contains("rebuild"));
    }
    #[test]
    fn coverage_metadata_round_trips_without_inventing_completeness() {
        let original = coverage();
        let encoded = serde_json::to_string(&original).unwrap();
        let decoded: PartitionCoverage = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, original);
        assert!(serde_json::from_str::<PartitionCoverage>("{}").is_err());
        for (start, end, complete) in [
            (false, false, false),
            (false, true, false),
            (true, false, false),
            (true, true, true),
        ] {
            assert_eq!(
                PartitionSpanProof {
                    start_complete: start,
                    end_complete: end,
                    first_block: None,
                    routing_start_timestamp: None
                }
                .complete(),
                complete
            );
        }
    }
    fn index() -> VerifiedPartitionIndex {
        let spans = [
            (10, "2023-11-14 22:00:00", 1_700_000_000),
            (11, "2023-11-14 23:00:00", 1_700_003_600),
            (12, "2023-11-14 22:00:00", 1_700_000_000),
        ]
        .into_iter()
        .map(|(num, key, timestamp)| VerifiedPartitionSpan {
            row: PartitionBuildRow {
                partition_type: "hour".into(),
                partition_interval_seconds: 3_600,
                partition_start_ts: key.into(),
                partition_value: key.into(),
                start_block: num,
                stop_block: num + 1,
                start_time: None,
                end_time: None,
                chain: Some("test-chain".into()),
            },
            proof: PartitionSpanProof {
                start_complete: num > 10,
                end_complete: num < 12,
                first_block: Some(FinalizedAnchor {
                    block_num: num,
                    block_id: format!("id-{num}"),
                }),
                routing_start_timestamp: Some(timestamp),
            },
        })
        .collect();
        VerifiedPartitionIndex {
            coverage: coverage(),
            spans,
        }
    }

    #[test]
    fn verified_parquet_round_trip_keeps_source_order_repeated_keys_and_proofs() {
        use crate::cli::{read_verified_partitions_index, write_verified_partitions_index};
        use crate::config::Compression;
        use crate::writer::ParquetFileMetadata;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partitions.parquet");
        let original = index();
        let mut metadata = ParquetFileMetadata::new();
        metadata.add("test.ownership", "preserved");
        write_verified_partitions_index(
            path.to_str().unwrap(),
            &original,
            Compression::Zstd,
            None,
            Some(&metadata),
        )
        .unwrap();
        assert_eq!(
            read_verified_partitions_index(path.to_str().unwrap(), None).unwrap(),
            original
        );
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap()).unwrap();
        assert!(reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .any(|kv| kv.key == "test.ownership" && kv.value.as_deref() == Some("preserved")));
        for field in proof_fields() {
            assert_eq!(
                reader.schema().field_with_name(field.name()).unwrap(),
                &field
            );
        }
        assert_eq!(
            original
                .spans
                .iter()
                .map(|span| span.proof.complete())
                .collect::<Vec<_>>(),
            vec![false, true, false]
        );
        assert_eq!(
            original.spans[0].row.partition_value,
            original.spans[2].row.partition_value
        );
    }

    #[test]
    fn legacy_index_remains_inspectable_but_never_gains_coverage() {
        use crate::cli::{
            read_partitions_build_rows, read_verified_partitions_index, write_partitions_index,
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.parquet");
        let rows = index()
            .spans
            .into_iter()
            .map(|span| span.row)
            .collect::<Vec<_>>();
        write_partitions_index(path.to_str().unwrap(), &rows, None).unwrap();
        assert_eq!(
            read_partitions_build_rows(path.to_str().unwrap(), None)
                .unwrap()
                .len(),
            3
        );
        assert!(read_verified_partitions_index(path.to_str().unwrap(), None)
            .unwrap_err()
            .to_string()
            .contains("rebuild"));
    }

    #[test]
    fn invalid_coverage_or_conflicting_metadata_cannot_replace_an_index() {
        use crate::cli::write_verified_partitions_index;
        use crate::config::Compression;
        use crate::writer::ParquetFileMetadata;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partitions.parquet");
        std::fs::write(&path, b"existing index").unwrap();
        let mut invalid = index();
        invalid.coverage.stop_block = 30;
        assert!(write_verified_partitions_index(
            path.to_str().unwrap(),
            &invalid,
            Compression::Zstd,
            None,
            None
        )
        .is_err());
        let mut conflicting = ParquetFileMetadata::new();
        conflicting.add("firehose-parquet.chain_name", "another-chain");
        assert!(write_verified_partitions_index(
            path.to_str().unwrap(),
            &index(),
            Compression::Zstd,
            None,
            Some(&conflicting)
        )
        .is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"existing index");
    }

    #[test]
    fn proof_reader_rejects_false_complete_claims_missing_fields_and_partial_identity() {
        use arrow::{
            array::{BooleanArray, StringArray},
            datatypes::Schema,
            record_batch::RecordBatch,
        };
        use std::sync::Arc;
        let original = index();
        let proofs = vec![original.spans[0].proof.clone()];
        let schema = Arc::new(Schema::new(proof_fields()));
        let mut columns = proof_columns(&proofs);
        columns[0] = Arc::new(BooleanArray::from(vec![true]));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        assert!(read_proof(&batch, 0)
            .unwrap_err()
            .to_string()
            .contains("boundary flags"));
        let mut columns = proof_columns(&proofs);
        columns[4] = Arc::new(StringArray::from(vec![None::<&str>]));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        assert!(read_proof(&batch, 0)
            .unwrap_err()
            .to_string()
            .contains("half a first-block"));
        let fields = proof_fields().into_iter().take(5).collect::<Vec<_>>();
        let columns = proof_columns(&proofs)
            .into_iter()
            .take(5)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        assert!(read_proof(&batch, 0)
            .unwrap_err()
            .to_string()
            .contains("missing routing_start"));
    }

    #[test]
    fn verified_index_rejects_mismatched_keys_identity_and_clipped_right_boundary() {
        let mut value = index();
        value.validate().unwrap();
        value.spans[0].proof.first_block.as_mut().unwrap().block_id = "wrong-id".into();
        assert!(value.validate().is_err());
        let mut value = index();
        value.spans[0].proof.routing_start_timestamp = Some(1_700_003_600);
        assert!(value.validate().is_err());
        let mut value = index();
        value.coverage.next_observed = Some(block(15, 12));
        value.spans[2].proof.end_complete = true;
        assert!(value
            .validate()
            .unwrap_err()
            .to_string()
            .contains("clipped right edge"));
    }
}
