//! Partition resolution, listing, sharding and completeness checks.
use super::*;

pub(in crate::cli) fn require_complete_span(span: &VerifiedPartitionSpan) -> anyhow::Result<()> {
    anyhow::ensure!(span.proof.complete(),
        "incomplete partition span [{}, {}) for {}; rebuild with both natural boundaries inside proven finalized coverage",
        span.row.start_block, span.row.stop_block, span.row.partition_value);
    Ok(())
}

/// A plain numeric range cannot carry an unseen timestamp anchor into ingestion.
/// Natural internal Solana runs start at non-null times; the actual genesis seed
/// is the only missing-time start reproducible without external routing context.
pub(in crate::cli) fn require_independent_routing_start(
    coverage: &PartitionCoverage,
    span: &VerifiedPartitionSpan,
) -> anyhow::Result<()> {
    use crate::partition_index::{IndexRoutingPolicy, SOLANA_GENESIS_TIMESTAMP};
    if coverage.routing_policy == IndexRoutingPolicy::BlockNumber || span.row.start_time.is_some() {
        return Ok(());
    }
    let genesis = coverage.first_observed.as_ref().is_some_and(|first| {
        first.block_num == 0 && first.parent_num == 0 && first.parent_id.is_empty()
    });
    anyhow::ensure!(coverage.routing_policy == IndexRoutingPolicy::SolanaPriorTimestamp
        && span.row.start_block == 0 && genesis
        && span.proof.routing_start_timestamp == Some(SOLANA_GENESIS_TIMESTAMP),
        "partition span requires unseen prior timestamp context; inspect --all-spans --json or rebuild from an independently routable boundary");
    Ok(())
}

pub(in crate::cli) fn select_exact_partition<'a>(
    index: &'a VerifiedPartitionIndex,
    request: &PartitionBoundsRequest,
) -> anyhow::Result<Vec<&'a VerifiedPartitionSpan>> {
    let key = parse_partition_bound(
        "--partition-value",
        &request.partition_type,
        &request.partition_value,
    )?;
    let spans = index
        .spans
        .iter()
        .filter(|span| {
            span.row.partition_type == request.partition_type
                && span.row.partition_key().is_ok_and(|value| value == key)
                && request
                    .chain
                    .as_deref()
                    .is_none_or(|chain| span.row.chain.as_deref() == Some(chain))
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !spans.is_empty(),
        "no partition row found for {}={}",
        request.partition_type,
        request.partition_value
    );
    for span in &spans {
        require_complete_span(span)?;
    }
    Ok(spans)
}

pub(in crate::cli) fn require_single_span(spans: &[&VerifiedPartitionSpan]) -> anyhow::Result<()> {
    anyhow::ensure!(spans.len() == 1,
        "partition lookup is ambiguous: {} disjoint spans; use --all-spans --json to inspect every complete span within declared coverage", spans.len());
    Ok(())
}

pub fn resolve_partition_command(
    request: PartitionBoundsRequest,
    aws: Option<&AwsConfig>,
    options: &PartitionResolveOptions,
) -> anyhow::Result<PartitionResolveResult> {
    let request = normalize_partition_bounds_request(request)?;
    // V2 itself enforces one chain; strict_single_chain remains accepted for CLI compatibility.
    let index = read_verified_partitions_index(&request.index_path, aws)?;
    let selected = select_exact_partition(&index, &request)?;
    if !options.all_spans {
        require_single_span(&selected)?;
        require_independent_routing_start(&index.coverage, selected[0])?;
    }
    let (start_block, stop_block) = if options.all_spans {
        (None, None)
    } else {
        (
            Some(selected[0].row.start_block),
            Some(selected[0].row.stop_block),
        )
    };
    let spans = options.all_spans.then(|| {
        selected
            .iter()
            .map(|span| PartitionResolvedSpan {
                routing_context_required: require_independent_routing_start(&index.coverage, span)
                    .is_err(),
                start_block: span.row.start_block,
                stop_block: span.row.stop_block,
                complete: span.proof.complete(),
                first_observed: span.proof.first_block.clone(),
                routing_start_timestamp: span.proof.routing_start_timestamp,
            })
            .collect()
    });
    Ok(PartitionResolveResult {
        partitions_index: request.index_path,
        partition_type: request.partition_type,
        partition_value: request.partition_value,
        partition_chain: index.spans[0].row.chain.clone(),
        coverage: index.coverage,
        start_block,
        stop_block,
        spans,
    })
}

/// List partition rows from a `partitions.parquet` index with optional filters.
pub fn list_partitions_from_index(
    request: &PartitionListRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionListResult> {
    let request = normalize_partition_list_request(request.clone())?;

    if request.limit == 0 {
        anyhow::bail!("--limit must be greater than 0");
    }

    // `--from`/`--to` are start blocks for block_range rows and timestamps otherwise,
    // so they are parsed (once) against each row's partition type.
    let mut bounds_by_type =
        std::collections::BTreeMap::<String, (Option<u64>, Option<u64>)>::new();
    let mut rows = Vec::new();
    let snapshot = read_partition_index_snapshot(&request.index_path, aws)?;
    let coverage = snapshot.coverage.clone();
    let inspected = if coverage.is_some() {
        let index = verified_index_from_snapshot(snapshot)?;
        index
            .spans
            .into_iter()
            .map(|span| {
                let context = require_independent_routing_start(&index.coverage, &span).is_err();
                Ok((
                    span.row.partition_key()?,
                    span.row,
                    Some(span.proof.complete()),
                    Some(context),
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    } else {
        snapshot
            .rows
            .into_iter()
            .map(|(key, row)| (key, row, None, None))
            .collect()
    };
    for (key, row, complete, routing_context_required) in inspected {
        let type_matches = request
            .partition_type
            .as_deref()
            .map(|value| row.partition_type.eq_ignore_ascii_case(value))
            .unwrap_or(true);
        let chain_matches = request
            .chain
            .as_deref()
            .map(|value| row.chain.as_deref() == Some(value))
            .unwrap_or(true);
        if !type_matches || !chain_matches {
            continue;
        }

        let (from, to) = match bounds_by_type.get(&row.partition_type) {
            Some(bounds) => *bounds,
            None => {
                let parse = |flag: &str, value: &Option<String>| {
                    value
                        .as_deref()
                        .map(|value| parse_partition_bound(flag, &row.partition_type, value))
                        .transpose()
                };
                let bounds = (parse("--from", &request.from)?, parse("--to", &request.to)?);
                bounds_by_type.insert(row.partition_type.clone(), bounds);
                bounds
            }
        };
        if from.is_some_and(|from| key < from) || to.is_some_and(|to| key > to) {
            continue;
        }

        rows.push((
            key,
            PartitionListRow {
                routing_context_required,
                complete,
                partition_type: row.partition_type,
                partition_value: row.partition_value,
                partition_start_ts: row.partition_start_ts,
                start_block: row.start_block,
                stop_block: row.stop_block,
                chain: row.chain,
            },
        ));
    }
    let total_matches = rows.len();
    rows.sort_by(|(left_key, left), (right_key, right)| {
        left_key
            .cmp(right_key)
            .then_with(|| left.partition_type.cmp(&right.partition_type))
            .then_with(|| left.chain.cmp(&right.chain))
            .then_with(|| left.start_block.cmp(&right.start_block))
            .then_with(|| left.stop_block.cmp(&right.stop_block))
    });
    rows.truncate(request.limit);
    let rows = rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>();

    Ok(PartitionListResult {
        coverage,
        partitions_index: request.index_path.clone(),
        limit: request.limit,
        total_matches,
        returned_rows: rows.len(),
        rows,
    })
}

pub fn parse_partition_shard_strategy(value: &str) -> anyhow::Result<PartitionShardStrategy> {
    match value.to_ascii_lowercase().as_str() {
        "ordinal" => Ok(PartitionShardStrategy::Ordinal),
        "hash" => Ok(PartitionShardStrategy::Hash),
        other => anyhow::bail!("invalid --strategy '{other}': expected one of: ordinal, hash"),
    }
}

pub(in crate::cli) fn partition_shard_key(row: &PartitionListRow) -> String {
    format!(
        "{}|{}|{}",
        row.chain.as_deref().unwrap_or_default(),
        row.partition_type,
        row.partition_value
    )
}

pub(in crate::cli) fn assign_partition_shard(
    row: &PartitionListRow,
    ordinal: usize,
    shard_count: usize,
    strategy: PartitionShardStrategy,
) -> usize {
    match strategy {
        PartitionShardStrategy::Ordinal => ordinal % shard_count,
        PartitionShardStrategy::Hash => {
            use sha2::{Digest, Sha256};

            let digest = Sha256::digest(partition_shard_key(row).as_bytes());
            let value = u64::from_be_bytes(digest[..8].try_into().expect("digest slice"));
            (value as usize) % shard_count
        }
    }
}

pub fn shard_partitions_from_index(
    request: &PartitionShardRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionShardResult> {
    if request.shard_count == 0 {
        anyhow::bail!("--shard-count must be greater than 0");
    }
    if request.shard_index >= request.shard_count {
        anyhow::bail!(
            "--shard-index must be less than --shard-count (got {} >= {})",
            request.shard_index,
            request.shard_count
        );
    }

    let mut list_request = request.list.clone();
    list_request.limit = usize::MAX;
    let list_result = list_partitions_from_index(&list_request, aws)?;
    let coverage = list_result
        .coverage
        .clone()
        .ok_or_else(|| anyhow::anyhow!("rebuild legacy indexes before sharding"))?;
    for row in &list_result.rows {
        anyhow::ensure!(
            row.complete == Some(true),
            "cannot shard incomplete partition span [{}, {})",
            row.start_block,
            row.stop_block
        );
        anyhow::ensure!(
            row.routing_context_required == Some(false),
            "cannot shard a span requiring unseen routing context"
        );
    }
    let rows = list_result
        .rows
        .into_iter()
        .enumerate()
        .filter_map(|(ordinal, row)| {
            let shard =
                assign_partition_shard(&row, ordinal, request.shard_count, request.strategy);
            (shard == request.shard_index).then_some(row)
        })
        .collect::<Vec<_>>();

    Ok(PartitionShardResult {
        coverage,
        partitions_index: request.list.index_path.clone(),
        shard_count: request.shard_count,
        shard_index: request.shard_index,
        strategy: request.strategy,
        total_matches: list_result.total_matches,
        returned_rows: rows.len(),
        rows,
    })
}

pub fn validate_partitions_index(
    request: &PartitionValidateRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionValidateResult> {
    let mut list_request = request.list.clone();
    list_request.limit = usize::MAX;
    let list_result = list_partitions_from_index(&list_request, aws)?;

    let incomplete_spans = list_result
        .rows
        .iter()
        .filter(|row| row.complete == Some(false))
        .count();
    let unknown_spans = list_result
        .rows
        .iter()
        .filter(|row| row.complete.is_none())
        .count();
    // V2 validation already checked global source-ordered coverage. Repeated
    // calendar values are valid and filtering can deliberately select disjoint runs.
    if list_result.coverage.is_some() {
        return Ok(PartitionValidateResult {
            partitions_index: request.list.index_path.clone(),
            coverage: list_result.coverage,
            incomplete_spans,
            unknown_spans,
            total_rows: list_result.total_matches,
            issue_count: 0,
            valid: true,
            issues: Vec::new(),
        });
    }
    let mut issues = Vec::new();

    for row in &list_result.rows {
        if row.start_block >= row.stop_block {
            issues.push(PartitionValidationIssue {
                kind: PartitionValidationIssueKind::InvalidRange,
                partition_type: row.partition_type.clone(),
                partition_value: row.partition_value.clone(),
                chain: row.chain.clone(),
                message: format!(
                    "invalid range: start_block={} stop_block={}",
                    row.start_block, row.stop_block
                ),
            });
        }
    }

    use std::collections::BTreeMap;
    let mut groups: BTreeMap<(Option<String>, String), Vec<&PartitionListRow>> = BTreeMap::new();
    for row in &list_result.rows {
        groups
            .entry((row.chain.clone(), row.partition_type.clone()))
            .or_default()
            .push(row);
    }

    for ((_chain, _ptype), rows) in groups {
        for pair in rows.windows(2) {
            let current = pair[0];
            let next = pair[1];

            if partition_value_key(&current.partition_type, &current.partition_value)?
                > partition_value_key(&next.partition_type, &next.partition_value)?
            {
                issues.push(PartitionValidationIssue {
                    kind: PartitionValidationIssueKind::OutOfOrder,
                    partition_type: next.partition_type.clone(),
                    partition_value: next.partition_value.clone(),
                    chain: next.chain.clone(),
                    message: format!(
                        "out of order: previous partition_start_ts={} next partition_start_ts={}",
                        current.partition_start_ts, next.partition_start_ts
                    ),
                });
            }

            if current.stop_block < next.start_block {
                if !request.allow_gaps {
                    issues.push(PartitionValidationIssue {
                        kind: PartitionValidationIssueKind::Gap,
                        partition_type: next.partition_type.clone(),
                        partition_value: next.partition_value.clone(),
                        chain: next.chain.clone(),
                        message: format!(
                            "gap detected: previous stop_block={} next start_block={}",
                            current.stop_block, next.start_block
                        ),
                    });
                }
            } else if current.stop_block > next.start_block {
                issues.push(PartitionValidationIssue {
                    kind: PartitionValidationIssueKind::Overlap,
                    partition_type: next.partition_type.clone(),
                    partition_value: next.partition_value.clone(),
                    chain: next.chain.clone(),
                    message: format!(
                        "overlap detected: previous stop_block={} next start_block={}",
                        current.stop_block, next.start_block
                    ),
                });
            }
        }
    }

    Ok(PartitionValidateResult {
        coverage: list_result.coverage,
        incomplete_spans,
        unknown_spans,
        partitions_index: request.list.index_path.clone(),
        total_rows: list_result.total_matches,
        issue_count: issues.len(),
        valid: issues.is_empty(),
        issues,
    })
}

/// Resolve one complete independently routable span inside declared finalized coverage.
pub fn resolve_partition_bounds_from_index(
    request: &PartitionBoundsRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionBounds> {
    let request = normalize_partition_bounds_request(request.clone())?;
    let index = read_verified_partitions_index(&request.index_path, aws)?;
    let selected = select_exact_partition(&index, &request)?;
    require_single_span(&selected)?;
    require_independent_routing_start(&index.coverage, selected[0])?;
    let (start_block, stop_block) = (selected[0].row.start_block, selected[0].row.stop_block);
    Ok(PartitionBounds {
        start_block,
        stop_block,
        coverage: index.coverage,
    })
}

/// Select calendar keys in [from,to), but join ranges only in source block order.
/// A filtered-out intervening run must never be silently included by min/max.
pub fn resolve_partition_window_bounds_from_index(
    request: &PartitionWindowRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionWindowBounds> {
    let request = normalize_partition_window_request(request.clone())?;
    let from = parse_partition_bound(
        "--partition-from",
        &request.partition_type,
        &request.partition_from,
    )?;
    let to = parse_partition_bound(
        "--partition-to",
        &request.partition_type,
        &request.partition_to,
    )?;
    anyhow::ensure!(
        from < to,
        "partition window must have increasing calendar bounds"
    );
    let index = read_verified_partitions_index(&request.index_path, aws)?;
    let selected = index
        .spans
        .iter()
        .filter(|span| {
            span.row.partition_type == request.partition_type
                && span
                    .row
                    .partition_key()
                    .is_ok_and(|key| (from..to).contains(&key))
                && request
                    .chain
                    .as_deref()
                    .is_none_or(|chain| span.row.chain.as_deref() == Some(chain))
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !selected.is_empty(),
        "no partition rows found in the requested window"
    );
    for span in &selected {
        require_complete_span(span)?;
    }
    for pair in selected.windows(2) {
        anyhow::ensure!(pair[0].row.stop_block == pair[1].row.start_block,
            "partition window has non-contiguous bounds: an unselected run separates [{}, {}) and [{}, {})",
            pair[0].row.start_block, pair[0].row.stop_block, pair[1].row.start_block, pair[1].row.stop_block);
    }
    require_independent_routing_start(&index.coverage, selected[0])?;
    Ok(PartitionWindowBounds {
        start_block: selected[0].row.start_block,
        stop_block: selected.last().unwrap().row.stop_block,
        partitions_count: selected.len(),
        partition_from: request.partition_from,
        partition_to: request.partition_to,
        coverage: index.coverage,
    })
}
