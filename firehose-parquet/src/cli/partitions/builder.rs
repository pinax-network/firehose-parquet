//! Incremental partition-span construction and resume checks.
use super::*;

#[derive(Debug, Clone)]
pub(in crate::cli) struct ActivePartitionBuildRow {
    pub(in crate::cli) partition_start_ts: i64,
    pub(in crate::cli) partition_value: String,
    pub(in crate::cli) start_block: u64,
    pub(in crate::cli) start_time: Option<i64>,
    pub(in crate::cli) last_block: u64,
    pub(in crate::cli) last_time: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct PartitionIndexBuilder {
    pub(in crate::cli) chain: String,
    pub(in crate::cli) partition_types: Vec<PartitionBuildType>,
    pub(in crate::cli) active:
        std::collections::BTreeMap<PartitionBuildType, ActivePartitionBuildRow>,
    pub(in crate::cli) rows: Vec<PartitionBuildRow>,
    pub(in crate::cli) first_seen_block: Option<u64>,
    pub(in crate::cli) last_seen_block: Option<u64>,
    /// Block range size for block_range partition type. Required when partition type is BlockRange.
    pub(in crate::cli) block_range_size: Option<u64>,
}

impl PartitionIndexBuilder {
    pub fn new(
        chain: impl Into<String>,
        partition_types: Vec<PartitionBuildType>,
    ) -> anyhow::Result<Self> {
        if partition_types.is_empty() {
            anyhow::bail!("at least one partition type is required");
        }

        Ok(Self {
            chain: chain.into(),
            partition_types,
            active: std::collections::BTreeMap::new(),
            rows: Vec::new(),
            first_seen_block: None,
            last_seen_block: None,
            block_range_size: None,
        })
    }

    pub fn with_block_range_size(mut self, size: u64) -> Self {
        self.block_range_size = Some(size);
        self
    }

    pub fn observe_block(&mut self, block: &crate::traits::BlockIdentity) -> anyhow::Result<()> {
        if let Some(last_seen_block) = self.last_seen_block {
            if block.block_num < last_seen_block {
                anyhow::bail!(
                    "partition build requires non-decreasing block numbers, saw {} after {}",
                    block.block_num,
                    last_seen_block
                );
            }
        }

        self.first_seen_block.get_or_insert(block.block_num);
        self.last_seen_block = Some(block.block_num);

        let timestamp = if block.timestamp != 0 {
            Some(block.timestamp)
        } else {
            None
        };

        for partition_type in self.partition_types.clone() {
            if partition_type == PartitionBuildType::BlockRange {
                // Block-range partitions don't use observe_block — they are built deterministically.
                // But if called, we can accumulate timestamps for best-effort time columns.
                let block_range_size = self.block_range_size.ok_or_else(|| {
                    anyhow::anyhow!("block_range_size is required for block_range partition type")
                })?;
                let partition_start_block = (block.block_num / block_range_size) * block_range_size;
                let partition_start_ts = partition_start_block as i64;
                let partition_value = partition_start_block.to_string();

                match self.active.get_mut(&partition_type) {
                    Some(active) if active.partition_start_ts == partition_start_ts => {
                        active.last_block = block.block_num;
                        active.last_time = timestamp;
                    }
                    Some(active) => {
                        let finalized = build_partition_row(
                            &self.chain,
                            partition_type,
                            active.clone(),
                            block.block_num,
                            self.block_range_size,
                        )?;
                        self.rows.push(finalized);
                        *active = ActivePartitionBuildRow {
                            partition_start_ts,
                            partition_value,
                            start_block: block.block_num,
                            start_time: timestamp,
                            last_block: block.block_num,
                            last_time: timestamp,
                        };
                    }
                    None => {
                        self.active.insert(
                            partition_type,
                            ActivePartitionBuildRow {
                                partition_start_ts,
                                partition_value,
                                start_block: block.block_num,
                                start_time: timestamp,
                                last_block: block.block_num,
                                last_time: timestamp,
                            },
                        );
                    }
                }
                continue;
            }

            // Time-based partition types: missing timestamps are allowed (e.g. Solana).
            let ts = timestamp.unwrap_or(0);
            let partition_start_ts = partition_type.round_timestamp(ts)?;
            let partition_value = format_partition_timestamp(partition_start_ts)?;

            match self.active.get_mut(&partition_type) {
                Some(active) if active.partition_start_ts == partition_start_ts => {
                    active.last_block = block.block_num;
                    active.last_time = timestamp;
                }
                Some(active) => {
                    let finalized = build_partition_row(
                        &self.chain,
                        partition_type,
                        active.clone(),
                        block.block_num,
                        self.block_range_size,
                    )?;
                    self.rows.push(finalized);
                    *active = ActivePartitionBuildRow {
                        partition_start_ts,
                        partition_value,
                        start_block: block.block_num,
                        start_time: timestamp,
                        last_block: block.block_num,
                        last_time: timestamp,
                    };
                }
                None => {
                    self.active.insert(
                        partition_type,
                        ActivePartitionBuildRow {
                            partition_start_ts,
                            partition_value,
                            start_block: block.block_num,
                            start_time: timestamp,
                            last_block: block.block_num,
                            last_time: timestamp,
                        },
                    );
                }
            }
        }

        Ok(())
    }

    pub fn finish(mut self, stop_block: u64) -> anyhow::Result<Vec<PartitionBuildRow>> {
        if self.first_seen_block.is_none() {
            anyhow::bail!("partition build produced no rows because the stream returned no blocks");
        }
        if stop_block == 0 {
            anyhow::bail!("partition build requires a finite non-zero stop block");
        }

        for partition_type in self.partition_types.clone() {
            if let Some(active) = self.active.remove(&partition_type) {
                self.rows.push(build_partition_row(
                    &self.chain,
                    partition_type,
                    active,
                    stop_block,
                    self.block_range_size,
                )?);
            }
        }

        sort_partition_build_rows(self.rows)
    }

    pub fn snapshot(&self, stop_block: u64) -> anyhow::Result<Vec<PartitionBuildRow>> {
        if self.first_seen_block.is_none() {
            anyhow::bail!("partition build produced no rows because the stream returned no blocks");
        }
        if stop_block == 0 {
            anyhow::bail!("partition build snapshot requires a finite non-zero stop block");
        }

        let mut rows = self.rows.clone();
        for partition_type in self.partition_types.clone() {
            if let Some(active) = self.active.get(&partition_type) {
                rows.push(build_partition_row(
                    &self.chain,
                    partition_type,
                    active.clone(),
                    stop_block,
                    self.block_range_size,
                )?);
            }
        }

        sort_partition_build_rows(rows)
    }

    pub fn current_frontier(&self) -> Option<u64> {
        self.last_seen_block.map(|block| block.saturating_add(1))
    }

    pub fn active_partition_value(&self) -> Option<String> {
        self.partition_types.first().and_then(|partition_type| {
            self.active
                .get(partition_type)
                .map(|active| active.partition_value.clone())
        })
    }

    pub fn has_rows(&self) -> bool {
        self.first_seen_block.is_some()
    }

    pub fn finalized_row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn resume_from_existing(
        chain: impl Into<String>,
        partition_types: Vec<PartitionBuildType>,
        mut existing_rows: Vec<PartitionBuildRow>,
    ) -> anyhow::Result<(Self, u64)> {
        if existing_rows.is_empty() {
            anyhow::bail!("cannot resume partition build without existing rows");
        }

        let chain = chain.into();
        let mut active = std::collections::BTreeMap::new();
        let mut retained_rows = Vec::new();
        let mut resume_block = None;

        for partition_type in partition_types.iter().copied() {
            let mut matching = existing_rows
                .iter()
                .enumerate()
                .filter(|(_, row)| row.partition_type == partition_type.as_str())
                .map(|(index, row)| Ok((row.partition_key()?, index, row)))
                .collect::<anyhow::Result<Vec<_>>>()?;
            sort_resume_rows(partition_type, &mut matching);

            let Some((_, last_index, last_row)) = matching.pop() else {
                anyhow::bail!(
                    "cannot resume partition build for chain {}: missing existing rows for partition type {}",
                    chain,
                    partition_type
                );
            };

            let partition_resume_block = if partition_type == PartitionBuildType::BlockRange {
                validate_block_range_resume_rows(&matching, last_row)?
            } else {
                last_row.stop_block
            };
            let common_resume_block = resume_block.get_or_insert(partition_resume_block);
            if partition_resume_block != *common_resume_block {
                anyhow::bail!(
                    "cannot resume partition build: partition type {} ends at {}, expected common frontier {}",
                    partition_type,
                    partition_resume_block,
                    common_resume_block
                );
            }

            let start_time = last_row
                .start_time
                .as_deref()
                .map(parse_partition_timestamp)
                .transpose()?;
            let last_time = last_row
                .end_time
                .as_deref()
                .map(parse_partition_timestamp)
                .transpose()?;

            let partition_start_ts = if partition_type == PartitionBuildType::BlockRange {
                last_row
                    .partition_start_ts
                    .parse::<i64>()
                    .unwrap_or(last_row.start_block as i64)
            } else {
                parse_partition_timestamp(&last_row.partition_start_ts)?
            };

            active.insert(
                partition_type,
                ActivePartitionBuildRow {
                    partition_start_ts,
                    partition_value: last_row.partition_value.clone(),
                    start_block: last_row.start_block,
                    start_time,
                    last_block: last_row.stop_block.saturating_sub(1),
                    last_time,
                },
            );

            existing_rows.remove(last_index);
        }

        retained_rows.extend(existing_rows);
        let resume_block = resume_block
            .ok_or_else(|| anyhow::anyhow!("missing existing stop_block for resume"))?;

        Ok((
            Self {
                chain,
                partition_types,
                active,
                rows: retained_rows,
                first_seen_block: Some(resume_block),
                last_seen_block: resume_block.checked_sub(1),
                block_range_size: None,
            },
            resume_block,
        ))
    }
}

/// Sort `(partition_key, index, row)` resume candidates so the terminal row is last.
pub(in crate::cli) fn sort_resume_rows(
    partition_type: PartitionBuildType,
    matching: &mut [(u64, usize, &PartitionBuildRow)],
) {
    matching.sort_by(
        |(left_key, _, left), (right_key, _, right)| match partition_type {
            PartitionBuildType::BlockRange => left
                .start_block
                .cmp(&right.start_block)
                .then_with(|| left.stop_block.cmp(&right.stop_block))
                .then_with(|| left_key.cmp(right_key)),
            _ => left_key
                .cmp(right_key)
                .then_with(|| left.start_block.cmp(&right.start_block))
                .then_with(|| left.stop_block.cmp(&right.stop_block)),
        },
    );
}

pub(in crate::cli) fn validate_block_range_resume_rows(
    completed_rows: &[(u64, usize, &PartitionBuildRow)],
    terminal_row: &PartitionBuildRow,
) -> anyhow::Result<u64> {
    let block_range_size = u64::try_from(terminal_row.partition_interval_seconds)
        .ok()
        .filter(|size| *size > 0)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cannot resume partition build: partition type block_range has invalid block range size {}",
                terminal_row.partition_interval_seconds
            )
        })?;

    let mut previous: Option<&PartitionBuildRow> = None;
    for row in completed_rows
        .iter()
        .map(|(_, _, row)| *row)
        .chain(std::iter::once(terminal_row))
    {
        if row.start_block >= row.stop_block {
            anyhow::bail!(
                "cannot resume partition build: partition type block_range has invalid range {}..{}",
                row.start_block,
                row.stop_block
            );
        }
        if row.partition_interval_seconds != block_range_size as i64 {
            anyhow::bail!(
                "cannot resume partition build: partition type block_range mixes block range sizes {} and {}",
                block_range_size,
                row.partition_interval_seconds
            );
        }
        if row.start_block % block_range_size != 0 {
            anyhow::bail!(
                "cannot resume partition build: partition type block_range starts at misaligned block {}, expected multiple of {}",
                row.start_block,
                block_range_size
            );
        }

        let row_size = row.stop_block - row.start_block;
        if row_size > block_range_size {
            anyhow::bail!(
                "cannot resume partition build: partition type block_range row {}..{} exceeds block range size {}",
                row.start_block,
                row.stop_block,
                block_range_size
            );
        }

        if let Some(previous_row) = previous {
            if previous_row.stop_block != row.start_block {
                let relation = if previous_row.stop_block < row.start_block {
                    "gap"
                } else {
                    "overlap"
                };
                anyhow::bail!(
                    "cannot resume partition build: partition type block_range has {} between {} and {}",
                    relation,
                    previous_row.stop_block,
                    row.start_block
                );
            }
            if previous_row.stop_block - previous_row.start_block != block_range_size {
                anyhow::bail!(
                    "cannot resume partition build: partition type block_range row {}..{} is incomplete before the final frontier {}",
                    previous_row.start_block,
                    previous_row.stop_block,
                    terminal_row.stop_block
                );
            }
        }

        previous = Some(row);
    }

    Ok(terminal_row.stop_block)
}

pub fn build_partition_rows_from_blocks(
    chain: &str,
    partition_types: Vec<PartitionBuildType>,
    blocks: &[crate::traits::BlockIdentity],
    stop_block: u64,
) -> anyhow::Result<Vec<PartitionBuildRow>> {
    let mut builder = PartitionIndexBuilder::new(chain, partition_types)?;
    for block in blocks {
        builder.observe_block(block)?;
    }
    builder.finish(stop_block)
}

pub(in crate::cli) fn build_partition_row(
    chain: &str,
    partition_type: PartitionBuildType,
    active: ActivePartitionBuildRow,
    stop_block: u64,
    block_range_size: Option<u64>,
) -> anyhow::Result<PartitionBuildRow> {
    if active.start_block >= stop_block {
        anyhow::bail!(
            "invalid partition row for {} {}: start_block {} must be < stop_block {}",
            partition_type,
            active.partition_value,
            active.start_block,
            stop_block
        );
    }

    let partition_start_ts = if partition_type == PartitionBuildType::BlockRange {
        active.partition_start_ts.to_string()
    } else {
        format_partition_timestamp(active.partition_start_ts)?
    };

    let start_time = active
        .start_time
        .map(format_partition_timestamp)
        .transpose()?;
    let end_time = active
        .last_time
        .map(format_partition_timestamp)
        .transpose()?;

    let interval = if partition_type == PartitionBuildType::BlockRange {
        block_range_size.unwrap_or(0) as i64
    } else {
        partition_type.interval_seconds()
    };

    Ok(PartitionBuildRow {
        partition_type: partition_type.to_string(),
        partition_interval_seconds: interval,
        partition_start_ts,
        partition_value: active.partition_value,
        start_block: active.start_block,
        stop_block,
        start_time,
        end_time,
        chain: Some(chain.to_string()),
    })
}
