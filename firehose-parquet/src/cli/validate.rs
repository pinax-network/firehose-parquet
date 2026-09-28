//! Block continuity of a Delta `blocks` table, and its diagnostic rendering.
//!
//! `validate` pins one snapshot of the table (its latest version) and reads
//! exactly the active data files that snapshot lists, grouped by their `date`
//! partition value from the log. It never lists a directory: a listing would
//! count a file that OPTIMIZE replaced (tombstoned, not yet vacuumed) beside
//! its replacement, and would find the log's checkpoint Parquet files.
use super::*;
use crate::delta::store::DeltaStore;

// ---------------------------------------------------------------------------
// Validate
// ---------------------------------------------------------------------------

/// Result of a gap in block sequence.
#[derive(Debug)]
pub struct BlockGap {
    pub from: u64,
    pub to: u64,
}

/// Result of a parent hash mismatch.
#[derive(Debug)]
pub struct ParentMismatch {
    pub block_num: u64,
    pub expected_parent_id: String,
    pub actual_parent_id: String,
}

/// A duplicate block_num found during validation.
#[derive(Debug)]
pub struct DuplicateBlock {
    pub block_num: u64,
    pub count: u64,
}

/// A timestamp reversal between consecutive blocks.
///
/// Timestamps are compared as UTC epoch milliseconds (the canonical
/// `timestamp` holds whole milliseconds), so a reversal within one second is
/// reported.
#[derive(Debug)]
pub struct TimestampReversal {
    pub block_num: u64,
    pub timestamp_ms: i64,
    pub prev_block_num: u64,
    pub prev_timestamp_ms: i64,
}

/// Cross-partition boundary issue.
#[derive(Debug)]
pub struct CrossPartitionIssue {
    pub from_partition: String,
    pub to_partition: String,
    pub gap: Option<BlockGap>,
    pub parent_mismatch: Option<ParentMismatch>,
}

/// Per-partition validation result.
#[derive(Debug)]
pub struct PartitionResult {
    pub partition: String,
    pub files_scanned: usize,
    pub total_blocks: u64,
    pub min_block: Option<u64>,
    pub max_block: Option<u64>,
    pub gaps: Vec<BlockGap>,
    pub parent_mismatches: Vec<ParentMismatch>,
    pub duplicates: Vec<DuplicateBlock>,
    pub timestamp_reversals: Vec<TimestampReversal>,
}

impl PartitionResult {
    pub fn is_valid(&self) -> bool {
        self.gaps.is_empty() && self.parent_mismatches.is_empty() && self.duplicates.is_empty()
    }
}

/// Options for validate.
#[derive(Debug, Default)]
pub struct ValidateOptions {
    pub cross_partition: bool,
    pub allow_gaps: bool,
}

/// Summary of a validate run (with per-partition breakdown).
#[derive(Debug)]
pub struct ValidateResult {
    /// The pinned table version whose active files were read.
    pub version: u64,
    pub files_scanned: usize,
    pub total_blocks: u64,
    pub min_block: Option<u64>,
    pub max_block: Option<u64>,
    pub gaps: Vec<BlockGap>,
    pub parent_mismatches: Vec<ParentMismatch>,
    pub duplicates: Vec<DuplicateBlock>,
    pub timestamp_reversals: Vec<TimestampReversal>,
    /// Per-partition results (only when there is more than one partition).
    pub partitions: Vec<PartitionResult>,
    /// Cross-partition boundary issues (only when --cross-partition).
    pub cross_partition_issues: Vec<CrossPartitionIssue>,
}

impl ValidateResult {
    pub fn is_valid(&self) -> bool {
        self.gaps.is_empty()
            && self.parent_mismatches.is_empty()
            && self.duplicates.is_empty()
            && self.cross_partition_issues.is_empty()
            && self.partitions.iter().all(|p| p.is_valid())
    }

    pub fn print(&self, path: &str) {
        println!(
            "Validating blocks in {path} at version {} ...\n",
            self.version
        );

        // Per-partition breakdown (if partitioned).
        // Only show partitions with issues or warnings; clean ones are counted in the summary.
        if !self.partitions.is_empty() {
            let valid_count = self.partitions.iter().filter(|p| p.is_valid()).count();
            let invalid_count = self.partitions.len() - valid_count;

            println!(
                "  Partitions:        {} total, {} valid, {} with issues\n",
                self.partitions.len(),
                valid_count,
                invalid_count
            );

            let mut listed_any = false;
            for pr in &self.partitions {
                // Skip clean partitions to keep output compact. Timestamp reversals are
                // warnings, so a partition whose only finding is a reversal is still listed.
                if pr.is_valid() && pr.timestamp_reversals.is_empty() {
                    continue;
                }
                listed_any = true;

                let range = match (pr.min_block, pr.max_block) {
                    (Some(min), Some(max)) => format!("{} — {}", min, max),
                    _ => "N/A".to_string(),
                };
                let marker = if pr.is_valid() { "⚠" } else { "✗" };
                println!("  {} {}", marker, pr.partition);
                println!(
                    "    files: {}  blocks: {}  range: {}",
                    pr.files_scanned, pr.total_blocks, range
                );

                for gap in &pr.gaps {
                    let missing = gap.to - gap.from;
                    println!(
                        "    gap: {} — {} ({} blocks missing)",
                        gap.from,
                        gap.to - 1,
                        missing
                    );
                }
                for mm in &pr.parent_mismatches {
                    println!(
                        "    parent mismatch at block {}: expected {} got {}",
                        mm.block_num, mm.expected_parent_id, mm.actual_parent_id
                    );
                }
                for dup in &pr.duplicates {
                    println!(
                        "    duplicate block {} ({} occurrences)",
                        dup.block_num, dup.count
                    );
                }
                for tr in &pr.timestamp_reversals {
                    println!(
                        "    timestamp reversal at block {}: {} < previous block {} timestamp {}",
                        tr.block_num,
                        format_epoch_millis(tr.timestamp_ms),
                        tr.prev_block_num,
                        format_epoch_millis(tr.prev_timestamp_ms)
                    );
                }
            }
            if listed_any {
                println!();
            }
        }

        // Cross-partition issues.
        if !self.cross_partition_issues.is_empty() {
            println!(
                "  Cross-partition issues: {}",
                self.cross_partition_issues.len()
            );
            for cpi in &self.cross_partition_issues {
                if let Some(ref gap) = cpi.gap {
                    let missing = gap.to - gap.from;
                    println!(
                        "    between {} and {}: gap {} — {} ({} blocks missing)",
                        cpi.from_partition,
                        cpi.to_partition,
                        gap.from,
                        gap.to - 1,
                        missing
                    );
                }
                if let Some(ref mm) = cpi.parent_mismatch {
                    println!(
                        "    between {} and {}: parent mismatch at block {}",
                        cpi.from_partition, cpi.to_partition, mm.block_num
                    );
                }
            }
            println!();
        }

        // Global summary.
        let range = match (self.min_block, self.max_block) {
            (Some(min), Some(max)) => format!("{} — {}", min, max),
            _ => "N/A".to_string(),
        };

        println!("  Files scanned:     {}", self.files_scanned);
        println!("  Block range:       {}", range);
        println!("  Total blocks:      {}", self.total_blocks);
        println!("  Gaps:              {}", self.gaps.len());

        if self.partitions.is_empty() {
            for gap in &self.gaps {
                let missing = gap.to - gap.from;
                println!(
                    "    gap: {} — {} ({} blocks missing)",
                    gap.from,
                    gap.to - 1,
                    missing
                );
            }
        }

        println!("  Parent mismatches: {}", self.parent_mismatches.len());
        if self.partitions.is_empty() {
            for mm in &self.parent_mismatches {
                println!(
                    "    block {}: expected parent_id {} but got {}",
                    mm.block_num, mm.expected_parent_id, mm.actual_parent_id
                );
            }
        }

        println!("  Duplicates:        {}", self.duplicates.len());
        if self.partitions.is_empty() {
            for dup in &self.duplicates {
                println!("    block {} ({} occurrences)", dup.block_num, dup.count);
            }
        }

        println!("  Timestamp reversals: {}", self.timestamp_reversals.len());
        if self.partitions.is_empty() {
            for tr in &self.timestamp_reversals {
                println!(
                    "    block {}: timestamp {} < previous block {} timestamp {}",
                    tr.block_num,
                    format_epoch_millis(tr.timestamp_ms),
                    tr.prev_block_num,
                    format_epoch_millis(tr.prev_timestamp_ms)
                );
            }
        }

        println!();
        if self.is_valid() {
            println!("  ✓ All blocks valid");
        } else {
            println!("  ✗ Validation failed");
        }

        // Warnings after the pass/fail line.
        if !self.timestamp_reversals.is_empty() && self.is_valid() {
            println!(
                "  ⚠ {} timestamp reversal(s) detected (see above); reported as warnings because some chains allow non-monotonic block times",
                self.timestamp_reversals.len()
            );
        }
    }
}

/// Render epoch milliseconds as UTC `YYYY-MM-DD HH:MM:SS.mmm`, falling back to the raw value.
pub(in crate::cli) fn format_epoch_millis(timestamp_ms: i64) -> String {
    format_utc_seconds(timestamp_ms.div_euclid(1_000))
        .map(|seconds| format!("{seconds}.{:03}", timestamp_ms.rem_euclid(1_000)))
        .unwrap_or_else(|_| format!("{timestamp_ms}ms"))
}

/// Render epoch seconds as UTC `YYYY-MM-DD HH:MM:SS`.
fn format_utc_seconds(timestamp: i64) -> anyhow::Result<String> {
    let dt = time::OffsetDateTime::from_unix_timestamp(timestamp)
        .map_err(|e| anyhow::anyhow!("invalid unix timestamp {timestamp}: {e}"))?;
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

/// A block tuple: (block_num, block_id, parent_id, timestamp in epoch milliseconds).
///
/// The timestamp is `None` when the table has no `timestamp` column or the value is null.
pub(in crate::cli) type BlockTuple = (u64, String, String, Option<i64>);

/// A `block_id` / `parent_id` column (Delta `string` or `binary`) as
/// strings: text as is, bytes as hex.
fn id_strings(column: &dyn arrow::array::Array, name: &str) -> anyhow::Result<Vec<String>> {
    use arrow::array::{Array, BinaryArray, StringArray};

    if let Some(text) = column.as_any().downcast_ref::<StringArray>() {
        Ok((0..text.len())
            .map(|row| text.value(row).to_string())
            .collect())
    } else if let Some(bytes) = column.as_any().downcast_ref::<BinaryArray>() {
        Ok((0..bytes.len())
            .map(|row| hex::encode(bytes.value(row)))
            .collect())
    } else {
        anyhow::bail!("{name} column is not Utf8 or Binary")
    }
}

/// A `timestamp` column as epoch milliseconds, whatever its unit: finer
/// units are truncated to whole milliseconds (monotonic, so truncation never
/// invents a reversal). Null values stay null.
pub(in crate::cli) fn timestamp_column_as_epoch_millis(
    column: &dyn arrow::array::Array,
) -> anyhow::Result<arrow::array::Int64Array> {
    use arrow::array::AsArray;
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Int64Type, TimeUnit};

    anyhow::ensure!(
        matches!(column.data_type(), DataType::Timestamp(_, _)),
        "timestamp column has unsupported type {}: expected a Timestamp",
        column.data_type()
    );
    let millis = cast(column, &DataType::Timestamp(TimeUnit::Millisecond, None))?;
    Ok(cast(&millis, &DataType::Int64)?
        .as_primitive::<Int64Type>()
        .clone())
}

/// Extract block tuples from a parquet record batch reader.
pub(in crate::cli) fn extract_block_tuples(
    reader: impl Iterator<Item = Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>>,
    block_num_idx: usize,
    block_id_idx: usize,
    parent_id_idx: usize,
    timestamp_idx: Option<usize>,
) -> anyhow::Result<Vec<BlockTuple>> {
    use arrow::array::{Array, Int64Array};

    let mut tuples = Vec::new();
    for batch_result in reader {
        let batch = batch_result?;
        // A Delta `long` (#643).
        let block_nums = batch
            .column(block_num_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("block_num column is not Int64 (a Delta long)"))?;
        let block_ids = id_strings(batch.column(block_id_idx).as_ref(), "block_id")?;
        let parent_ids = id_strings(batch.column(parent_id_idx).as_ref(), "parent_id")?;
        let timestamps = timestamp_idx
            .map(|idx| timestamp_column_as_epoch_millis(batch.column(idx).as_ref()))
            .transpose()?;

        for (i, (block_id, parent_id)) in block_ids.into_iter().zip(parent_ids).enumerate() {
            let number = block_nums.value(i);
            let block_num = u64::try_from(number)
                .map_err(|_| anyhow::anyhow!("negative block_num {number}"))?;
            let ts = timestamps
                .as_ref()
                .filter(|millis| millis.is_valid(i))
                .map(|millis| millis.value(i));
            tuples.push((block_num, block_id, parent_id, ts));
        }
    }
    Ok(tuples)
}

/// Column indices for validation.
pub(in crate::cli) struct CanonicalIndices {
    pub(in crate::cli) block_num: usize,
    pub(in crate::cli) block_id: usize,
    pub(in crate::cli) parent_id: usize,
    pub(in crate::cli) timestamp: Option<usize>,
}

/// Find column indices for canonical fields in a schema.
pub(in crate::cli) fn find_canonical_indices(
    schema: &arrow::datatypes::Schema,
) -> anyhow::Result<CanonicalIndices> {
    let block_num = schema
        .index_of("block_num")
        .map_err(|_| anyhow::anyhow!("missing 'block_num' column — is this a blocks table?"))?;
    let block_id = schema
        .index_of("block_id")
        .map_err(|_| anyhow::anyhow!("missing 'block_id' column"))?;
    let parent_id = schema
        .index_of("parent_id")
        .map_err(|_| anyhow::anyhow!("missing 'parent_id' column"))?;
    let timestamp = schema.index_of("timestamp").ok();
    Ok(CanonicalIndices {
        block_num,
        block_id,
        parent_id,
        timestamp,
    })
}

/// Decode only the validation columns of one data file.
pub(in crate::cli) fn read_validation_columns<R: parquet::file::reader::ChunkReader + 'static>(
    input: R,
) -> anyhow::Result<Vec<BlockTuple>> {
    use arrow::record_batch::RecordBatchReader;
    use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};

    let builder = ParquetRecordBatchReaderBuilder::try_new(input)?;
    let indices = find_canonical_indices(builder.schema())?;
    let roots = [
        Some(indices.block_num),
        Some(indices.block_id),
        Some(indices.parent_id),
        indices.timestamp,
    ]
    .into_iter()
    .flatten();
    let projection = ProjectionMask::roots(builder.parquet_schema(), roots);
    let reader = builder.with_projection(projection).build()?;
    // Projection retains source field order, which need not match canonical order.
    let projected = find_canonical_indices(&reader.schema())?;
    extract_block_tuples(
        reader,
        projected.block_num,
        projected.block_id,
        projected.parent_id,
        projected.timestamp,
    )
}

/// Validate the Delta table at `path` (local or S3): the active files of its
/// latest version.
pub fn validate_table(
    path: &str,
    aws: Option<&AwsConfig>,
    opts: &ValidateOptions,
) -> anyhow::Result<ValidateResult> {
    use anyhow::Context;

    let path = resolve_parquet_input_path_string(path);
    let (store, table) = if path.starts_with("s3://") {
        let aws = aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?;
        let (bucket, key) = crate::writer::parse_s3_url(&path)?;
        let (prefix, table) = key.rsplit_once('/').unwrap_or(("", key.as_str()));
        anyhow::ensure!(
            !table.is_empty(),
            "{path} names a bucket, not a table: pass s3://{bucket}/<dataset>/blocks"
        );
        (
            DeltaStore::s3(&bucket, prefix, s3_read_client(aws, &bucket)?)?,
            table.to_string(),
        )
    } else {
        let directory =
            std::fs::canonicalize(&path).with_context(|| format!("path does not exist: {path}"))?;
        let (Some(parent), Some(table)) = (
            directory.parent(),
            directory.file_name().and_then(|name| name.to_str()),
        ) else {
            anyhow::bail!("{path} is not a table directory");
        };
        (DeltaStore::local(parent)?, table.to_string())
    };
    block_on_async(validate_snapshot(&store, &table, &path, opts))
}

/// A read-only client of `bucket` for the Delta log and its data files, on
/// delta-rs's object_store: default retries, and anonymous requests without
/// an access key, like fireparq's other read-only clients.
fn s3_read_client(
    aws: &AwsConfig,
    bucket: &str,
) -> anyhow::Result<std::sync::Arc<dyn object_store_delta::ObjectStore>> {
    let mut builder = crate::delta::store::s3_builder(aws, bucket)?
        .with_retry(object_store_delta::RetryConfig::default());
    if aws.aws_access_key_id.is_none() {
        builder = builder.with_skip_signature(true);
    }
    Ok(std::sync::Arc::new(builder.build()?))
}

async fn validate_snapshot(
    store: &DeltaStore,
    table: &str,
    path: &str,
    opts: &ValidateOptions,
) -> anyhow::Result<ValidateResult> {
    use anyhow::Context;
    use object_store_delta::ObjectStoreExt;

    anyhow::ensure!(
        !store.lacks_local_log(table)?,
        "{path} is not a Delta table: it has no _delta_log/ (pass a table such as \
         <dataset root>/blocks)"
    );
    let delta = crate::delta::open_table(store.log_store(table)?)
        .await
        .with_context(|| format!("opening the Delta table {path}"))?;
    let version = delta
        .version()
        .context("an opened Delta table has no version")?;
    let version = u64::try_from(version).context("negative Delta table version")?;
    // The pinned snapshot's active files, with their `date` partition values.
    let files: Vec<_> = delta
        .snapshot()?
        .log_data()
        .iter()
        .map(|file| {
            let date = file
                .partition_values_map()
                .remove(crate::delta::PARTITION_COLUMN)
                .flatten()
                .unwrap_or_default();
            (
                file.path().into_owned(),
                file.object_store_path(),
                format!("{}={date}", crate::delta::PARTITION_COLUMN),
            )
        })
        .collect();
    let data = delta.object_store();
    let mut infos = Vec::with_capacity(files.len());
    for (name, location, partition) in files {
        let bytes = match data.get(&location).await {
            Ok(object) => object.bytes().await,
            Err(error) => Err(error),
        };
        let bytes = match bytes {
            Ok(bytes) => bytes,
            Err(object_store_delta::Error::NotFound { .. }) => anyhow::bail!(
                "active file {name} of version {version} is missing: a VACUUM removed it after \
                 this snapshot was read; validate again"
            ),
            Err(error) => {
                return Err(error).with_context(|| format!("reading active file {name}"));
            }
        };
        let tuples = read_validation_columns(bytes)
            .with_context(|| format!("reading active file {name}"))?;
        infos.push(FileInfo { partition, tuples });
    }
    let mut result = validate_from_files(infos, opts);
    result.version = version;
    Ok(result)
}

/// Check results from check_tuples.
pub(in crate::cli) struct CheckResult {
    pub(in crate::cli) gaps: Vec<BlockGap>,
    pub(in crate::cli) parent_mismatches: Vec<ParentMismatch>,
    pub(in crate::cli) duplicates: Vec<DuplicateBlock>,
    pub(in crate::cli) timestamp_reversals: Vec<TimestampReversal>,
}

/// Check a list of block tuples sorted by block_num for gaps, duplicates, the
/// parent hash chain and timestamp reversals.
pub(in crate::cli) fn check_tuples(tuples: &[BlockTuple]) -> CheckResult {
    let mut gaps = Vec::new();
    let mut parent_mismatches = Vec::new();
    let mut duplicates = Vec::new();
    let mut timestamp_reversals = Vec::new();

    // Track runs of duplicate block_num.
    let mut dup_start = 0usize;
    // Last (block_num, timestamp) seen with a non-null timestamp, so null timestamps
    // (e.g. Solana blocks without block_time) neither hide nor fake a reversal.
    let mut last_timestamp = tuples.first().and_then(|t| t.3.map(|ts| (t.0, ts)));

    for i in 1..tuples.len() {
        let (prev_num, ref prev_block_id, _, _) = tuples[i - 1];
        let (curr_num, _, ref curr_parent_id, curr_ts) = tuples[i];

        if curr_num == prev_num {
            continue;
        }

        let run_len = i - dup_start;
        if run_len > 1 {
            duplicates.push(DuplicateBlock {
                block_num: tuples[dup_start].0,
                count: run_len as u64,
            });
        }
        dup_start = i;

        if curr_num > prev_num + 1 {
            gaps.push(BlockGap {
                from: prev_num + 1,
                to: curr_num,
            });
        }
        if curr_num == prev_num + 1 && curr_parent_id != prev_block_id {
            parent_mismatches.push(ParentMismatch {
                block_num: curr_num,
                expected_parent_id: prev_block_id.clone(),
                actual_parent_id: curr_parent_id.clone(),
            });
        }
        // Timestamp monotonicity check (#87).
        if let Some(curr_ts) = curr_ts {
            if let Some((last_num, last_ts)) = last_timestamp {
                if curr_ts < last_ts && curr_num > last_num {
                    timestamp_reversals.push(TimestampReversal {
                        block_num: curr_num,
                        timestamp_ms: curr_ts,
                        prev_block_num: last_num,
                        prev_timestamp_ms: last_ts,
                    });
                }
            }
            last_timestamp = Some((curr_num, curr_ts));
        }
    }

    // Final run check.
    if !tuples.is_empty() {
        let run_len = tuples.len() - dup_start;
        if run_len > 1 {
            duplicates.push(DuplicateBlock {
                block_num: tuples[dup_start].0,
                count: run_len as u64,
            });
        }
    }

    CheckResult {
        gaps,
        parent_mismatches,
        duplicates,
        timestamp_reversals,
    }
}

/// The block tuples of one active data file and its partition (`date=...`,
/// from the log).
pub(in crate::cli) struct FileInfo {
    pub(in crate::cli) partition: String,
    pub(in crate::cli) tuples: Vec<BlockTuple>,
}

pub(in crate::cli) fn validate_from_files(
    files: Vec<FileInfo>,
    opts: &ValidateOptions,
) -> ValidateResult {
    let total_files = files.len();

    // Group by partition.
    let mut groups: std::collections::BTreeMap<String, (Vec<BlockTuple>, usize)> =
        std::collections::BTreeMap::new();
    for fi in files {
        let entry = groups.entry(fi.partition).or_default();
        entry.0.extend(fi.tuples);
        entry.1 += 1;
    }

    let mut partitions = Vec::new();
    let mut all_tuples: Vec<BlockTuple> = Vec::new();

    // Keep only each partition's own boundary tuples for cross-partition checks.
    // Global duplicate/continuity validation still needs the complete tuple set.
    let mut boundaries = Vec::new();
    for (partition_name, (mut tuples, file_count)) in groups {
        tuples.sort_by_key(|t| t.0);
        let cr = check_tuples(&tuples);
        let min_block = tuples.first().map(|t| t.0);
        let max_block = tuples.last().map(|t| t.0);
        if opts.cross_partition {
            if let (Some(first), Some(last)) = (tuples.first(), tuples.last()) {
                boundaries.push((partition_name.clone(), first.clone(), last.clone()));
            }
        }

        partitions.push(PartitionResult {
            partition: partition_name,
            files_scanned: file_count,
            total_blocks: tuples.len() as u64,
            min_block,
            max_block,
            gaps: cr.gaps,
            parent_mismatches: cr.parent_mismatches,
            duplicates: cr.duplicates,
            timestamp_reversals: cr.timestamp_reversals,
        });

        all_tuples.extend(tuples);
    }

    // Cross-partition continuity (#88): sort P boundaries, then inspect P-1 pairs
    // directly instead of searching all N block tuples for every pair (#524).
    let mut cross_partition_issues = Vec::new();
    boundaries.sort_by_key(|(_, first, _)| first.0);
    for pair in boundaries.windows(2) {
        let (prev_name, _, prev_last) = &pair[0];
        let (curr_name, curr_first, _) = &pair[1];
        let Some(next_block) = prev_last.0.checked_add(1) else {
            continue;
        };
        let gap = (curr_first.0 > next_block).then_some(BlockGap {
            from: next_block,
            to: curr_first.0,
        });
        let parent_mismatch =
            (curr_first.0 == next_block && curr_first.2 != prev_last.1).then(|| ParentMismatch {
                block_num: curr_first.0,
                expected_parent_id: prev_last.1.clone(),
                actual_parent_id: curr_first.2.clone(),
            });
        if gap.is_some() || parent_mismatch.is_some() {
            cross_partition_issues.push(CrossPartitionIssue {
                from_partition: prev_name.clone(),
                to_partition: curr_name.clone(),
                gap,
                parent_mismatch,
            });
        }
    }

    // When --allow-gaps is set, clear gaps from per-partition results and
    // cross-partition issues (e.g. Solana skipped slots are expected).
    if opts.allow_gaps {
        for p in &mut partitions {
            p.gaps.clear();
        }
        for cpi in &mut cross_partition_issues {
            cpi.gap = None;
        }
        // Remove cross-partition issues that only had a gap (no parent mismatch).
        cross_partition_issues.retain(|cpi| cpi.parent_mismatch.is_some());
    }

    // Global validation across all partitions.
    all_tuples.sort_by_key(|t| t.0);
    let cr = check_tuples(&all_tuples);

    // Only include per-partition breakdown if there are multiple partitions.
    let show_partitions = partitions.len() > 1;

    ValidateResult {
        version: 0,
        files_scanned: total_files,
        total_blocks: all_tuples.len() as u64,
        min_block: all_tuples.first().map(|t| t.0),
        max_block: all_tuples.last().map(|t| t.0),
        gaps: if opts.allow_gaps { vec![] } else { cr.gaps },
        parent_mismatches: cr.parent_mismatches,
        duplicates: cr.duplicates,
        timestamp_reversals: cr.timestamp_reversals,
        partitions: if show_partitions { partitions } else { vec![] },
        cross_partition_issues,
    }
}
