//! RPC orchestration for exact finalized spans. Only the covered interval is
//! streamed, followed by a separately bounded right-edge witness request.
use super::*;
use crate::grpc::{
    classify_fetch_error, unless_shutdown, CancellationToken, FetchErrorKind, FirehoseClient,
};
use crate::traits::BlockIdentity;
use std::time::Duration;

const PARENT_BUDGET: usize = 64;
const RIGHT_EDGE_SLOT_BUDGET: u64 = 65_536;

fn genesis(block: &BlockIdentity) -> bool {
    block.block_num == 0 && block.parent_num == 0 && block.parent_id.is_empty()
}

/// Whether a partition head check, traversal or boundary probe failed for a
/// transient endpoint reason, so a live build may retry from its last verified
/// snapshot.
///
/// Timeouts (including a stalled traversal message or an elapsed bounded
/// deadline), transport failures and non-fatal gRPC statuses are transient.
/// Fatal statuses (authentication, permissions, invalid request, decompression
/// limits), missing blocks and every proof or integrity failure (non-final
/// blocks, contradictory ancestry, out-of-range responses) are not.
pub fn is_transient_partition_error(error: &anyhow::Error) -> bool {
    match classify_fetch_error(error) {
        FetchErrorKind::Timeout | FetchErrorKind::Transient => true,
        FetchErrorKind::Fatal | FetchErrorKind::NotFound => false,
        FetchErrorKind::Unexpected => error
            .chain()
            .any(|cause| cause.is::<tokio::time::error::Elapsed>() || cause.is::<tonic::Status>()),
    }
}

/// Fetch and verify canonical ancestry only when needed for the initial routing
/// anchor or natural left-boundary comparison. A missing optional parent leaves
/// that edge incomplete; a needed Solana seed fails closed after this budget.
pub async fn resolve_routing_parent(
    client: &FirehoseClient,
    first: &BlockIdentity,
    policy: &IndexRoutingPolicy,
    timeout: Duration,
    shutdown: &CancellationToken,
) -> Result<Option<RoutingWitness>> {
    if genesis(first) {
        return Ok(None);
    }
    let needed = *policy == IndexRoutingPolicy::SolanaPriorTimestamp && first.timestamp == 0;
    let lookup = async {
        let mut child = first.clone();
        let mut immediate = None;
        for _ in 0..PARENT_BUDGET {
            ensure!(
                child.parent_num < child.block_num && !child.parent_id.is_empty(),
                "partition parent context has invalid canonical ancestry"
            );
            let parent = match client
                .fetch_block_identity(child.parent_num, Some(timeout))
                .await
            {
                Ok(Some(parent)) => parent,
                Ok(None) => anyhow::bail!("partition parent Fetch has no block metadata"),
                Err(error) if classify_fetch_error(&error) == FetchErrorKind::NotFound => {
                    if needed {
                        return Err(error.context(
                            "required Solana routing ancestor is unavailable; use block_range",
                        ));
                    }
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            ensure!(
                parent.block_num == child.parent_num && parent.block_id == child.parent_id,
                "partition parent Fetch does not match the canonical parent identity"
            );
            if immediate.is_none() {
                immediate = Some(CoveredBlock::from(&parent));
            }
            if parent.timestamp != 0 {
                return Ok(Some(RoutingWitness {
                    block: immediate.unwrap(),
                    timestamp: parent.timestamp,
                }));
            }
            if *policy != IndexRoutingPolicy::SolanaPriorTimestamp {
                // Its bootstrap context is unknown, so this edge is incomplete.
                return Ok(None);
            }
            if genesis(&parent) {
                return Ok(Some(RoutingWitness {
                    block: immediate.unwrap(),
                    timestamp: SOLANA_GENESIS_TIMESTAMP,
                }));
            }
            child = parent;
        }
        if needed {
            anyhow::bail!("Solana routing ancestor budget exhausted; use block_range");
        }
        Ok(None)
    };
    unless_shutdown(shutdown, tokio::time::timeout(timeout, lookup))
        .await?
        .context("partition routing ancestor deadline exceeded")?
}

/// Build a snapshot from every finalized canonical block in the declared range.
/// No output is written here. A caller can publish only the validated result.
#[allow(clippy::too_many_arguments)]
pub async fn scan_time_index(
    client: &FirehoseClient,
    chain: String,
    kind: PartitionBuildType,
    start: u64,
    stop: u64,
    finalized: FinalizedAnchor,
    policy: IndexRoutingPolicy,
    resume_parent: Option<RoutingWitness>,
    timeout: Duration,
    shutdown: &CancellationToken,
) -> Result<VerifiedPartitionIndex> {
    ensure!(
        start < stop && stop <= finalized.exclusive_stop()?,
        "requested stop exceeds the proven finalized range"
    );
    let mut stream = client
        .finalized_metadata_stream(start, stop - 1, timeout, shutdown)
        .await?;
    let first = stream
        .next()
        .await?
        .context("declared coverage contains no canonical blocks")?;
    let left = match resume_parent {
        Some(left) => Some(left),
        None => resolve_routing_parent(client, &first, &policy, timeout, shutdown).await?,
    };
    let mut builder =
        ExactTimeIndexBuilder::new(chain, kind, start, stop, finalized.clone(), policy, left)?;
    builder.observe_final(&first, 3)?;
    while let Some(block) = stream.next().await? {
        builder.observe_final(&block, 3)?;
    }
    drop(stream);
    if stop <= finalized.block_num {
        // One response proves the right edge; do not read to current head.
        let last = stop
            .saturating_add(RIGHT_EDGE_SLOT_BUDGET - 1)
            .min(finalized.block_num);
        let witness = unless_shutdown(
            shutdown,
            tokio::time::timeout(timeout, async {
                let mut right = client
                    .finalized_metadata_stream(stop, last, timeout, shutdown)
                    .await?;
                right
                    .next()
                    .await?
                    .context("no right-edge witness within the bounded slot budget")
            }),
        )
        .await?
        .context("partition right-edge witness deadline exceeded")??;
        ensure!(
            builder.observe_final(&witness, 3)?,
            "right-edge response was inside coverage"
        );
    }
    builder.finish()
}
