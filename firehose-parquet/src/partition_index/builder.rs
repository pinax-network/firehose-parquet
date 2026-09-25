//! Exact, source-ordered spans. Every canonical parent link is checked; no
//! timestamp monotonicity or sparse-sample inference is used.
use super::*;
use crate::traits::BlockIdentity;

pub const SOLANA_GENESIS_TIMESTAMP: i64 = 1_584_368_940;

/// A canonical parent whose effective routing timestamp was proved by walking
/// its parent links (or restoring a verified, linked index frontier).
#[derive(Debug, Clone)]
pub struct RoutingWitness {
    pub block: CoveredBlock,
    pub timestamp: i64,
}

pub struct ExactTimeIndexBuilder {
    chain: String,
    kind: PartitionBuildType,
    coverage: PartitionCoverage,
    left: Option<RoutingWitness>,
    spans: Vec<VerifiedPartitionSpan>,
}

fn format_time(timestamp: i64) -> Result<String> {
    let dt = crate::traits::checked_timestamp(timestamp)?;
    Ok(format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        dt.year(),
        dt.month() as u8,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    ))
}

fn linked(child: &CoveredBlock, parent: &CoveredBlock) -> bool {
    child.block_num > parent.block_num
        && child.parent_num == parent.block_num
        && child.parent_id == parent.block_id
}

fn is_genesis(block: &CoveredBlock) -> bool {
    block.block_num == 0 && block.parent_num == 0 && block.parent_id.is_empty()
}

impl ExactTimeIndexBuilder {
    pub fn new(
        chain: String,
        kind: PartitionBuildType,
        start: u64,
        stop: u64,
        finalized: FinalizedAnchor,
        policy: IndexRoutingPolicy,
        left: Option<RoutingWitness>,
    ) -> Result<Self> {
        ensure!(
            kind != PartitionBuildType::BlockRange && policy != IndexRoutingPolicy::BlockNumber,
            "exact time builder requires a time partition policy"
        );
        ensure!(!chain.is_empty(), "partition chain cannot be empty");
        ensure!(
            start < stop && stop <= finalized.exclusive_stop()?,
            "requested coverage lies outside the proven finalized range"
        );
        if let Some(left) = &left {
            ensure!(
                !left.block.block_id.is_empty(),
                "routing witness has no identity"
            );
            crate::traits::checked_timestamp(left.timestamp)?;
        }
        Ok(Self {
            chain,
            kind,
            coverage: PartitionCoverage {
                version: INDEX_FORMAT_VERSION,
                start_block: start,
                stop_block: stop,
                finalized,
                routing_policy: policy,
                first_observed: None,
                last_observed: None,
                next_observed: None,
                last_routing_timestamp: None,
            },
            left,
            spans: Vec::new(),
        })
    }

    fn routing_timestamp(&self, block: &BlockIdentity) -> Result<i64> {
        block.timestamp_millis()?;
        if block.timestamp != 0 {
            return Ok(block.timestamp);
        }
        ensure!(self.coverage.routing_policy == IndexRoutingPolicy::SolanaPriorTimestamp,
            "time indexing cannot reproduce a non-Solana missing-timestamp bootstrap; use block_range");
        self.coverage
            .last_routing_timestamp
            .or_else(|| self.left.as_ref().map(|left| left.timestamp))
            .or_else(|| is_genesis(&CoveredBlock::from(block)).then_some(SOLANA_GENESIS_TIMESTAMP))
            .context(
                "Solana time indexing requires a proven prior timestamp anchor; use block_range",
            )
    }

    /// Returns true after accepting the first right-edge witness. Callers must
    /// immediately stop reading; that witness never becomes a covered block.
    pub fn observe_final(&mut self, block: &BlockIdentity, step: i32) -> Result<bool> {
        ensure!(step == 3, "partition traversal received a non-final block");
        ensure!(
            self.coverage.next_observed.is_none(),
            "partition traversal continued after its witness"
        );
        let current = CoveredBlock::from(block);
        ensure!(
            !current.block_id.is_empty(),
            "partition traversal has an empty block identity"
        );
        ensure!(
            current.block_num >= self.coverage.start_block
                && current.block_num <= self.coverage.finalized.block_num,
            "partition traversal returned a block outside the requested finalized bounds"
        );
        let first = self.coverage.first_observed.is_none();
        if let Some(last) = &self.coverage.last_observed {
            ensure!(
                linked(&current, last),
                "partition traversal omitted or reordered a canonical block"
            );
        } else {
            ensure!(
                current.parent_num < self.coverage.start_block || is_genesis(&current),
                "first covered block has an unobserved parent inside coverage"
            );
            if let Some(left) = &self.left {
                ensure!(
                    linked(&current, &left.block),
                    "routing witness is not the first block's canonical parent"
                );
            }
        }
        let timestamp = self.routing_timestamp(block)?;
        let rounded = self.kind.round_timestamp(timestamp)?;
        let key = format_time(rounded)?;
        if current.block_num >= self.coverage.stop_block {
            ensure!(!first, "declared coverage contains no canonical blocks");
            let last = self.spans.last_mut().unwrap();
            last.proof.end_complete =
                current.block_num == self.coverage.stop_block && last.row.partition_value != key;
            self.coverage.next_observed = Some(current);
            return Ok(true);
        }
        let changed = self
            .spans
            .last()
            .is_some_and(|span| span.row.partition_value != key);
        if changed {
            let last = self.spans.last_mut().unwrap();
            last.row.stop_block = current.block_num;
            last.proof.end_complete = true;
        }
        if first || changed {
            let start_complete = if first {
                is_genesis(&current)
                    || self.left.as_ref().is_some_and(|left| {
                        self.kind
                            .round_timestamp(left.timestamp)
                            .is_ok_and(|parent_key| parent_key != rounded)
                    })
            } else {
                true
            };
            self.spans.push(VerifiedPartitionSpan {
                row: PartitionBuildRow {
                    partition_type: self.kind.to_string(),
                    partition_interval_seconds: self.kind.interval_seconds(),
                    partition_start_ts: key.clone(),
                    partition_value: key,
                    start_block: current.block_num,
                    stop_block: self.coverage.stop_block,
                    start_time: (block.timestamp != 0)
                        .then(|| format_time(block.timestamp))
                        .transpose()?,
                    end_time: None,
                    chain: Some(self.chain.clone()),
                },
                proof: PartitionSpanProof {
                    start_complete,
                    end_complete: false,
                    first_block: Some(FinalizedAnchor {
                        block_num: current.block_num,
                        block_id: current.block_id.clone(),
                    }),
                    routing_start_timestamp: Some(timestamp),
                },
            });
        }
        self.spans.last_mut().unwrap().row.end_time = (block.timestamp != 0)
            .then(|| format_time(block.timestamp))
            .transpose()?;
        if first {
            self.coverage.first_observed = Some(current.clone());
        }
        self.coverage.last_observed = Some(current);
        self.coverage.last_routing_timestamp = Some(timestamp);
        Ok(false)
    }

    pub fn finish(self) -> Result<VerifiedPartitionIndex> {
        let index = VerifiedPartitionIndex {
            coverage: self.coverage,
            spans: self.spans,
        };
        index.validate()?;
        Ok(index)
    }
}

/// Append an exact verified extension in source order. The former open span may
/// continue through skipped slots or be closed by the extension's first block.
pub fn append_verified_extension(
    mut previous: VerifiedPartitionIndex,
    extension: VerifiedPartitionIndex,
) -> Result<VerifiedPartitionIndex> {
    previous.validate()?;
    extension.validate()?;
    ensure!(
        previous.coverage.stop_block == extension.coverage.start_block,
        "partition resume must start at the exact stored coverage frontier"
    );
    ensure!(
        previous.coverage.routing_policy == extension.coverage.routing_policy,
        "partition resume routing policy changed"
    );
    ensure!(
        extension.coverage.finalized.block_num >= previous.coverage.finalized.block_num,
        "partition resume finalized anchor moved backwards"
    );
    let old = previous.spans.last_mut().unwrap();
    let first = &extension.spans[0];
    ensure!(
        old.row.partition_type == first.row.partition_type
            && old.row.chain == first.row.chain
            && old.row.partition_interval_seconds == first.row.partition_interval_seconds,
        "partition resume chain/type/interval changed"
    );
    if previous.coverage.routing_policy != IndexRoutingPolicy::BlockNumber {
        let last = previous.coverage.last_observed.as_ref().unwrap();
        let next = extension.coverage.first_observed.as_ref().unwrap();
        ensure!(
            linked(next, last),
            "partition resume does not link to the stored finalized frontier"
        );
        old.row.stop_block = next.block_num;
        old.proof.end_complete = old.row.partition_value != first.row.partition_value;
    }
    let mut incoming = extension.spans.into_iter();
    let first = incoming.next().unwrap();
    if old.row.partition_value == first.row.partition_value {
        old.row.stop_block = first.row.stop_block;
        old.row.end_time = first.row.end_time;
        old.proof.end_complete = first.proof.end_complete;
    } else {
        previous.spans.push(first);
    }
    previous.spans.extend(incoming);
    previous.coverage.stop_block = extension.coverage.stop_block;
    previous.coverage.finalized = extension.coverage.finalized;
    previous.coverage.last_observed = extension.coverage.last_observed;
    previous.coverage.next_observed = extension.coverage.next_observed;
    previous.coverage.last_routing_timestamp = extension.coverage.last_routing_timestamp;
    previous.validate()?;
    Ok(previous)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity(num: u64, parent: u64, timestamp: i64) -> BlockIdentity {
        BlockIdentity {
            block_num: num,
            block_id: format!("id-{num}"),
            parent_num: parent,
            parent_id: format!("id-{parent}"),
            timestamp,
            ..Default::default()
        }
    }
    fn builder(
        start: u64,
        stop: u64,
        policy: IndexRoutingPolicy,
        left: Option<RoutingWitness>,
    ) -> ExactTimeIndexBuilder {
        ExactTimeIndexBuilder::new(
            "test-chain".into(),
            PartitionBuildType::Hour,
            start,
            stop,
            FinalizedAnchor {
                block_num: 20,
                block_id: "id-20".into(),
            },
            policy,
            left,
        )
        .unwrap()
    }
    fn parent(timestamp: i64) -> RoutingWitness {
        RoutingWitness {
            block: CoveredBlock::from(&identity(9, 8, timestamp)),
            timestamp,
        }
    }
    const A: i64 = 1_700_000_000;
    const B: i64 = A + 3_600;

    #[test]
    fn backward_and_unsampled_timestamps_form_exact_repeated_spans() {
        let mut builder = builder(
            10,
            13,
            IndexRoutingPolicy::CanonicalTimestamp,
            Some(parent(B)),
        );
        for block in [identity(10, 9, A), identity(11, 10, B), identity(12, 11, A)] {
            assert!(!builder.observe_final(&block, 3).unwrap());
        }
        assert!(builder.observe_final(&identity(13, 12, B), 3).unwrap());
        let index = builder.finish().unwrap();
        assert_eq!(
            index
                .spans
                .iter()
                .map(|span| (span.row.start_block, span.row.stop_block))
                .collect::<Vec<_>>(),
            vec![(10, 11), (11, 12), (12, 13)]
        );
        assert!(index.spans.iter().all(|span| span.proof.complete()));
        assert_eq!(
            index.spans[0].row.partition_value,
            index.spans[2].row.partition_value
        );
        for (span, timestamp) in index.spans.iter().zip([A, B, A]) {
            // The existing writer's routing key changes at exactly these edges.
            let expected = crate::config::Partition::Hour
                .partition_key(span.row.start_block, timestamp)
                .unwrap();
            assert_eq!(
                expected,
                crate::config::Partition::Hour
                    .partition_key(
                        span.row.start_block,
                        span.proof.routing_start_timestamp.unwrap()
                    )
                    .unwrap()
            );
        }
    }

    #[test]
    fn omitted_blocks_and_wrong_parent_ids_fail_but_skipped_slots_are_valid() {
        for bad in [identity(11, 10, A), identity(10, 9, A)] {
            let mut builder = builder(
                10,
                13,
                IndexRoutingPolicy::CanonicalTimestamp,
                Some(parent(B)),
            );
            let mut bad = bad;
            if bad.block_num == 10 {
                bad.parent_id = "wrong".into();
            }
            assert!(builder.observe_final(&bad, 3).is_err());
        }
        let mut builder = builder(
            10,
            15,
            IndexRoutingPolicy::CanonicalTimestamp,
            Some(parent(B)),
        );
        builder.observe_final(&identity(11, 9, A), 3).unwrap();
        assert!(builder.observe_final(&identity(14, 12, B), 3).is_err());
        builder.observe_final(&identity(14, 11, B), 3).unwrap();
        builder.observe_final(&identity(16, 14, A), 3).unwrap();
        let result = builder.finish().unwrap();
        assert_eq!(result.coverage.stop_block, 15);
        assert!(!result.spans.last().unwrap().proof.end_complete);
    }

    #[test]
    fn missing_time_uses_a_proven_prior_solana_anchor_not_a_future_one() {
        let mut builder = builder(
            10,
            13,
            IndexRoutingPolicy::SolanaPriorTimestamp,
            Some(parent(A)),
        );
        builder.observe_final(&identity(10, 9, 0), 3).unwrap();
        builder.observe_final(&identity(11, 10, 0), 3).unwrap();
        builder.observe_final(&identity(12, 11, B), 3).unwrap();
        builder.observe_final(&identity(13, 12, B), 3).unwrap();
        let result = builder.finish().unwrap();
        assert_eq!(result.spans.len(), 2);
        assert_eq!(result.spans[0].proof.routing_start_timestamp, Some(A));
        assert_eq!(result.spans[0].row.start_time, None);
        assert!(!result.spans[0].proof.start_complete);
        assert!(!result.spans[1].proof.end_complete);
        let mut unanchored = builder_without_anchor(IndexRoutingPolicy::SolanaPriorTimestamp);
        assert!(unanchored.observe_final(&identity(10, 9, 0), 3).is_err());
        let mut bootstrap = builder_without_anchor(IndexRoutingPolicy::CanonicalTimestamp);
        assert!(bootstrap
            .observe_final(&identity(10, 9, 0), 3)
            .unwrap_err()
            .to_string()
            .contains("bootstrap"));
    }
    fn builder_without_anchor(policy: IndexRoutingPolicy) -> ExactTimeIndexBuilder {
        builder(10, 13, policy, None)
    }

    #[test]
    fn head_span_stays_open_and_resume_joins_by_source_order_after_backward_time() {
        let mut original = ExactTimeIndexBuilder::new(
            "test-chain".into(),
            PartitionBuildType::Hour,
            10,
            13,
            FinalizedAnchor {
                block_num: 12,
                block_id: "id-12".into(),
            },
            IndexRoutingPolicy::CanonicalTimestamp,
            Some(parent(B)),
        )
        .unwrap();
        for block in [identity(10, 9, A), identity(11, 10, B), identity(12, 11, A)] {
            original.observe_final(&block, 3).unwrap();
        }
        let original = original.finish().unwrap();
        assert!(!original.spans[2].proof.complete());
        let mut extension = builder(
            13,
            16,
            IndexRoutingPolicy::CanonicalTimestamp,
            Some(RoutingWitness {
                block: CoveredBlock::from(&identity(12, 11, A)),
                timestamp: A,
            }),
        );
        extension.observe_final(&identity(14, 12, A), 3).unwrap();
        extension.observe_final(&identity(15, 14, B), 3).unwrap();
        extension.observe_final(&identity(16, 15, A), 3).unwrap();
        let merged = append_verified_extension(original, extension.finish().unwrap()).unwrap();
        assert_eq!(merged.coverage.start_block, 10);
        assert_eq!(merged.coverage.stop_block, 16);
        assert_eq!(
            merged
                .spans
                .iter()
                .map(|span| (span.row.start_block, span.row.stop_block))
                .collect::<Vec<_>>(),
            vec![(10, 11), (11, 12), (12, 15), (15, 16)]
        );
        assert!(merged.spans.iter().all(|span| span.proof.complete()));
    }

    #[test]
    fn premature_eof_nonfinal_and_post_witness_messages_cannot_be_published() {
        let mut premature = builder_without_anchor(IndexRoutingPolicy::CanonicalTimestamp);
        premature.observe_final(&identity(10, 9, A), 3).unwrap();
        assert!(premature.finish().is_err());
        let mut value = builder_without_anchor(IndexRoutingPolicy::CanonicalTimestamp);
        assert!(value.observe_final(&identity(10, 9, A), 1).is_err());
        value.observe_final(&identity(10, 9, A), 3).unwrap();
        value.observe_final(&identity(13, 10, B), 3).unwrap();
        assert!(value.observe_final(&identity(14, 13, A), 3).is_err());
    }
}
