//! Canonical block validation and diagnostic rendering.
use super::*;

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
#[derive(Debug)]
pub struct TimestampReversal {
    pub block_num: u64,
    pub timestamp: i64,
    pub prev_block_num: u64,
    pub prev_timestamp: i64,
}

/// An empty partition (no files or 0 rows).
#[derive(Debug)]
pub struct EmptyPartition {
    pub partition: String,
    pub files: usize,
    pub reason: &'static str,
}

/// A schema mismatch between files.
#[derive(Debug)]
pub struct SchemaMismatch {
    pub file: String,
    pub details: Vec<String>,
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
    pub ordering_errors: u64,
    pub timestamp_reversals: Vec<TimestampReversal>,
}

impl PartitionResult {
    pub fn is_valid(&self) -> bool {
        self.gaps.is_empty()
            && self.parent_mismatches.is_empty()
            && self.duplicates.is_empty()
            && self.ordering_errors == 0
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
    pub files_scanned: usize,
    pub total_blocks: u64,
    pub min_block: Option<u64>,
    pub max_block: Option<u64>,
    pub gaps: Vec<BlockGap>,
    pub parent_mismatches: Vec<ParentMismatch>,
    pub duplicates: Vec<DuplicateBlock>,
    pub ordering_errors: u64,
    pub timestamp_reversals: Vec<TimestampReversal>,
    /// Per-partition results (empty when data is not partitioned).
    pub partitions: Vec<PartitionResult>,
    /// Empty partitions (warning, not failure).
    pub empty_partitions: Vec<EmptyPartition>,
    /// Schema mismatches across files.
    pub schema_mismatches: Vec<SchemaMismatch>,
    /// Cross-partition boundary issues (only when --cross-partition).
    pub cross_partition_issues: Vec<CrossPartitionIssue>,
}

impl ValidateResult {
    pub fn is_valid(&self) -> bool {
        self.gaps.is_empty()
            && self.parent_mismatches.is_empty()
            && self.duplicates.is_empty()
            && self.ordering_errors == 0
            && self.schema_mismatches.is_empty()
            && self.cross_partition_issues.is_empty()
            && self.partitions.iter().all(|p| p.is_valid())
    }

    pub fn print(&self, path: &str) {
        println!("Validating blocks in {} ...\n", path);

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
                if pr.ordering_errors > 0 {
                    println!("    ordering errors: {}", pr.ordering_errors);
                }
                for tr in &pr.timestamp_reversals {
                    println!(
                        "    timestamp reversal at block {}: {} < previous block {} timestamp {}",
                        tr.block_num,
                        format_epoch_seconds(tr.timestamp),
                        tr.prev_block_num,
                        format_epoch_seconds(tr.prev_timestamp)
                    );
                }
            }
            if listed_any {
                println!();
            }
        }

        // Schema mismatches.
        if !self.schema_mismatches.is_empty() {
            println!("  Schema mismatches: {}", self.schema_mismatches.len());
            for sm in &self.schema_mismatches {
                println!("    {}:", sm.file);
                for detail in &sm.details {
                    println!("      {}", detail);
                }
            }
            println!();
        }

        // Empty partitions (warnings).
        if !self.empty_partitions.is_empty() {
            println!(
                "  Empty partitions:  {} (warning)",
                self.empty_partitions.len()
            );
            for ep in &self.empty_partitions {
                println!("    {} ({})", ep.partition, ep.reason);
            }
            println!();
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
        println!("  Ordering errors:   {}", self.ordering_errors);
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
                    format_epoch_seconds(tr.timestamp),
                    tr.prev_block_num,
                    format_epoch_seconds(tr.prev_timestamp)
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
        if !self.empty_partitions.is_empty() && self.is_valid() {
            println!(
                "  ⚠ {} empty partition(s) detected (see above)",
                self.empty_partitions.len()
            );
        }
        if !self.timestamp_reversals.is_empty() && self.is_valid() {
            println!(
                "  ⚠ {} timestamp reversal(s) detected (see above); reported as warnings because some chains allow non-monotonic block times",
                self.timestamp_reversals.len()
            );
        }
    }
}

/// Render epoch seconds as UTC `YYYY-MM-DD HH:MM:SS`, falling back to the raw value.
pub(in crate::cli) fn format_epoch_seconds(timestamp: i64) -> String {
    format_partition_timestamp(timestamp).unwrap_or_else(|_| timestamp.to_string())
}

/// A block tuple: (block_num, block_id, parent_id, timestamp in epoch seconds).
///
/// The timestamp is `None` when the table has no `timestamp` column or the value is null.
pub(in crate::cli) type BlockTuple = (u64, String, String, Option<i64>);

/// Read a string value from a column that may be Utf8 or Binary.
pub(in crate::cli) fn read_id_string(
    col: &dyn arrow::array::Array,
    row: usize,
    col_name: &str,
) -> anyhow::Result<String> {
    use arrow::array::{BinaryArray, StringArray};
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        Ok(s.value(row).to_string())
    } else if let Some(b) = col.as_any().downcast_ref::<BinaryArray>() {
        Ok(hex::encode(b.value(row)))
    } else {
        Err(anyhow::anyhow!("{} column is not Utf8 or Binary", col_name))
    }
}

/// Read a `timestamp` column as epoch seconds, whatever its unit.
///
/// Accepts any Arrow `Timestamp` unit (the canonical column is `Timestamp(Second, UTC)`,
/// and finer units such as milliseconds are truncated to whole seconds) as well as
/// legacy `Int64` epoch seconds. Null values stay null.
pub(in crate::cli) fn timestamp_column_as_epoch_seconds(
    column: &dyn arrow::array::Array,
) -> anyhow::Result<arrow::array::Int64Array> {
    use arrow::array::AsArray;
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Int64Type, TimeUnit};

    if !matches!(
        column.data_type(),
        DataType::Timestamp(_, _) | DataType::Int64
    ) {
        anyhow::bail!(
            "timestamp column has unsupported type {}: expected Timestamp or Int64 epoch seconds",
            column.data_type()
        );
    }
    let seconds = cast(column, &DataType::Timestamp(TimeUnit::Second, None))?;
    Ok(cast(&seconds, &DataType::Int64)?
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
    use arrow::array::{Array, UInt64Array};

    let mut tuples = Vec::new();
    for batch_result in reader {
        let batch = batch_result?;
        let block_nums = batch
            .column(block_num_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow::anyhow!("block_num column is not UInt64"))?;
        let block_id_col = batch.column(block_id_idx).as_ref();
        let parent_id_col = batch.column(parent_id_idx).as_ref();
        let timestamps = timestamp_idx
            .map(|idx| timestamp_column_as_epoch_seconds(batch.column(idx).as_ref()))
            .transpose()?;

        for i in 0..batch.num_rows() {
            let ts = timestamps
                .as_ref()
                .filter(|seconds| seconds.is_valid(i))
                .map(|seconds| seconds.value(i));
            tuples.push((
                block_nums.value(i),
                read_id_string(block_id_col, i, "block_id")?,
                read_id_string(parent_id_col, i, "parent_id")?,
                ts,
            ));
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

/// Decode only validation columns while retaining the full footer schema for
/// schema consistency checks. Shared by file-backed and S3-buffer-backed readers.
pub(in crate::cli) fn read_validation_columns<R: parquet::file::reader::ChunkReader + 'static>(
    input: R,
) -> anyhow::Result<(arrow::datatypes::Schema, Vec<BlockTuple>, u64)> {
    use arrow::record_batch::RecordBatchReader;
    use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};

    let builder = ParquetRecordBatchReaderBuilder::try_new(input)?;
    let schema = builder.schema().as_ref().clone();
    let indices = find_canonical_indices(&schema)?;
    let roots = [
        Some(indices.block_num),
        Some(indices.block_id),
        Some(indices.parent_id),
        indices.timestamp,
    ]
    .into_iter()
    .flatten();
    let projection = ProjectionMask::roots(builder.parquet_schema(), roots);
    let row_count = builder.metadata().file_metadata().num_rows() as u64;
    let reader = builder.with_projection(projection).build()?;
    // Projection retains source field order, which need not match canonical order.
    let projected = find_canonical_indices(&reader.schema())?;
    let tuples = extract_block_tuples(
        reader,
        projected.block_num,
        projected.block_id,
        projected.parent_id,
        projected.timestamp,
    )?;
    Ok((schema, tuples, row_count))
}

/// Compare two schemas and return a list of differences.
pub(in crate::cli) fn compare_schemas(
    reference: &arrow::datatypes::Schema,
    other: &arrow::datatypes::Schema,
) -> Vec<String> {
    use std::collections::HashMap;

    let ref_fields: HashMap<&str, &arrow::datatypes::Field> = reference
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();
    let other_fields: HashMap<&str, &arrow::datatypes::Field> = other
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    let mut diffs = Vec::new();

    // Check for missing columns and type mismatches.
    for (name, ref_field) in &ref_fields {
        match other_fields.get(name) {
            None => diffs.push(format!("missing column: {}", name)),
            Some(other_field) => {
                if ref_field.data_type() != other_field.data_type() {
                    diffs.push(format!(
                        "type mismatch: {} ({} vs {})",
                        name,
                        ref_field.data_type(),
                        other_field.data_type()
                    ));
                }
            }
        }
    }

    // Check for extra columns.
    for name in other_fields.keys() {
        if !ref_fields.contains_key(name) {
            diffs.push(format!("extra column: {}", name));
        }
    }

    diffs
}

/// Validate parquet files at the given path (local or S3).
pub fn validate_parquet(
    path: &str,
    aws: Option<&AwsConfig>,
    opts: &ValidateOptions,
) -> anyhow::Result<ValidateResult> {
    let path = resolve_parquet_input_path_string(path);
    if path.starts_with("s3://") {
        validate_parquet_s3(
            &path,
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?,
            opts,
        )
    } else {
        validate_parquet_local(&PathBuf::from(path), opts)
    }
}

/// Check results from check_tuples.
pub(in crate::cli) struct CheckResult {
    pub(in crate::cli) gaps: Vec<BlockGap>,
    pub(in crate::cli) parent_mismatches: Vec<ParentMismatch>,
    pub(in crate::cli) duplicates: Vec<DuplicateBlock>,
    pub(in crate::cli) ordering_errors: u64,
    pub(in crate::cli) timestamp_reversals: Vec<TimestampReversal>,
}

/// Check a sorted list of block tuples for gaps, ordering, parent hash chain, and timestamp issues.
pub(in crate::cli) fn check_tuples(tuples: &[BlockTuple]) -> CheckResult {
    let mut gaps = Vec::new();
    let mut parent_mismatches = Vec::new();
    let mut duplicates = Vec::new();
    let mut timestamp_reversals = Vec::new();
    let mut ordering_errors = 0u64;

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

        if curr_num < prev_num {
            ordering_errors += 1;
        }
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
                        timestamp: curr_ts,
                        prev_block_num: last_num,
                        prev_timestamp: last_ts,
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
        ordering_errors,
        timestamp_reversals,
    }
}

/// Detect the partition key from a file path by looking for Hive-style directories
/// (e.g. `date=2026-01-01`, `block_range=0-100000`). Returns the partition directory
/// path relative to the base, or "(root)" if no partition structure is detected.
pub(in crate::cli) fn detect_partition(file_path: &str, base_path: &str) -> String {
    let relative = file_path
        .strip_prefix(base_path)
        .unwrap_or(file_path)
        .trim_start_matches('/');

    // Walk directory components, collect Hive-style partition segments.
    let parts: Vec<&str> = relative
        .split('/')
        .filter(|seg| seg.contains('=') && !seg.ends_with(".parquet"))
        .collect();

    if parts.is_empty() {
        "(root)".to_string()
    } else {
        parts.join("/")
    }
}

/// Per-file metadata collected during scanning.
pub(in crate::cli) struct FileInfo {
    pub(in crate::cli) path: String,
    pub(in crate::cli) partition: String,
    pub(in crate::cli) schema: arrow::datatypes::Schema,
    pub(in crate::cli) tuples: Vec<BlockTuple>,
    pub(in crate::cli) row_count: u64,
}

pub(in crate::cli) fn validate_from_files(
    files: Vec<FileInfo>,
    opts: &ValidateOptions,
) -> ValidateResult {
    // Schema consistency check (#90).
    let mut schema_mismatches = Vec::new();
    if let Some(first) = files.first() {
        let ref_schema = &first.schema;
        for f in files.iter().skip(1) {
            let diffs = compare_schemas(ref_schema, &f.schema);
            if !diffs.is_empty() {
                schema_mismatches.push(SchemaMismatch {
                    file: f.path.clone(),
                    details: diffs,
                });
            }
        }
    }

    // Group by partition.
    let mut groups: std::collections::BTreeMap<String, (Vec<BlockTuple>, usize, u64)> =
        std::collections::BTreeMap::new();
    for fi in files {
        let entry = groups
            .entry(fi.partition)
            .or_insert_with(|| (Vec::new(), 0, 0));
        entry.0.extend(fi.tuples);
        entry.1 += 1;
        entry.2 += fi.row_count;
    }

    // Detect empty partitions (#89).
    let mut empty_partitions = Vec::new();
    let mut partitions = Vec::new();
    let mut all_tuples: Vec<BlockTuple> = Vec::new();
    let mut total_files = 0usize;

    // Keep only each partition's own boundary tuples for cross-partition checks.
    // Global duplicate/continuity validation still needs the complete tuple set.
    let mut boundaries = Vec::new();
    for (partition_name, (mut tuples, file_count, row_count)) in groups {
        total_files += file_count;

        if file_count == 0 {
            empty_partitions.push(EmptyPartition {
                partition: partition_name,
                files: 0,
                reason: "no files",
            });
            continue;
        }
        if row_count == 0 {
            empty_partitions.push(EmptyPartition {
                partition: partition_name,
                files: file_count,
                reason: "0 rows across all files",
            });
            continue;
        }

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
            ordering_errors: cr.ordering_errors,
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
        files_scanned: total_files,
        total_blocks: all_tuples.len() as u64,
        min_block: all_tuples.first().map(|t| t.0),
        max_block: all_tuples.last().map(|t| t.0),
        gaps: if opts.allow_gaps { vec![] } else { cr.gaps },
        parent_mismatches: cr.parent_mismatches,
        duplicates: cr.duplicates,
        ordering_errors: cr.ordering_errors,
        timestamp_reversals: cr.timestamp_reversals,
        partitions: if show_partitions { partitions } else { vec![] },
        empty_partitions,
        schema_mismatches,
        cross_partition_issues,
    }
}

pub(in crate::cli) fn validate_parquet_local(
    path: &PathBuf,
    opts: &ValidateOptions,
) -> anyhow::Result<ValidateResult> {
    let mut paths: Vec<PathBuf> = Vec::new();
    if path.is_file() {
        paths.push(path.clone());
    } else if path.is_dir() {
        crate::maintenance::discovery::collect_local(
            path,
            crate::maintenance::discovery::LocalPolicy::PARQUET,
            &mut paths,
        )?;
        paths.sort();
    } else {
        anyhow::bail!("path does not exist: {}", path.display());
    }

    if paths.is_empty() {
        println!("No .parquet files found in {}", path.display());
        return Ok(ValidateResult {
            files_scanned: 0,
            total_blocks: 0,
            min_block: None,
            max_block: None,
            gaps: vec![],
            parent_mismatches: vec![],
            duplicates: vec![],
            ordering_errors: 0,
            timestamp_reversals: vec![],
            partitions: vec![],
            empty_partitions: vec![],
            schema_mismatches: vec![],
            cross_partition_issues: vec![],
        });
    }

    let base = path.to_string_lossy().to_string();
    let mut file_infos = Vec::new();

    for file_path in &paths {
        let file = std::fs::File::open(file_path)?;
        let (arrow_schema, tuples, row_count) = read_validation_columns(file)?;

        let partition_key = detect_partition(&file_path.to_string_lossy(), &base);
        let display = file_path
            .strip_prefix(path)
            .unwrap_or(file_path)
            .to_string_lossy()
            .to_string();

        file_infos.push(FileInfo {
            path: display,
            partition: partition_key,
            schema: arrow_schema,
            tuples,
            row_count,
        });
    }

    Ok(validate_from_files(file_infos, opts))
}

pub(in crate::cli) fn validate_parquet_s3(
    path: &str,
    aws: &AwsConfig,
    opts: &ValidateOptions,
) -> anyhow::Result<ValidateResult> {
    use crate::maintenance::discovery::{list_objects, read_object_bytes, relative_key};
    use crate::writer::parse_s3_url;

    let (bucket, prefix) = parse_s3_url(path)?;
    let client = aws.build_s3_client(&bucket)?;

    let objects = block_on_async(list_objects(&client, &prefix))
        .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let mut parquet_objects: Vec<_> = objects
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .collect();
    parquet_objects.sort_by(|a, b| a.location.cmp(&b.location));

    if parquet_objects.is_empty() {
        println!("No .parquet files found in {path}");
        return Ok(ValidateResult {
            files_scanned: 0,
            total_blocks: 0,
            min_block: None,
            max_block: None,
            gaps: vec![],
            parent_mismatches: vec![],
            duplicates: vec![],
            ordering_errors: 0,
            timestamp_reversals: vec![],
            partitions: vec![],
            empty_partitions: vec![],
            schema_mismatches: vec![],
            cross_partition_issues: vec![],
        });
    }

    let mut file_infos = Vec::new();

    for obj in &parquet_objects {
        let data = block_on_async(read_object_bytes(&client, &obj.location))
            .map_err(|e| anyhow::anyhow!("reading s3://{bucket}/{}: {e}", obj.location))?;

        let (arrow_schema, tuples, row_count) = read_validation_columns(data)?;

        let partition_key = detect_partition(obj.location.as_ref(), &prefix);
        let display = relative_key(&prefix, obj.location.as_ref()).to_string();

        file_infos.push(FileInfo {
            path: display,
            partition: partition_key,
            schema: arrow_schema,
            tuples,
            row_count,
        });
    }

    Ok(validate_from_files(file_infos, opts))
}
