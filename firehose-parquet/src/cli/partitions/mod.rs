//! Partition command models, vocabulary and shared bound helpers.
use super::*;
mod builder;
mod io;
mod queries;
pub use builder::*;
pub use io::*;
pub use queries::*;

#[derive(Debug, Clone, serde::Serialize)]
pub struct PartitionResolveResult {
    pub partitions_index: String,
    pub partition_type: String,
    pub partition_value: String,
    pub partition_chain: Option<String>,
    pub coverage: PartitionCoverage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_block: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_block: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spans: Option<Vec<PartitionResolvedSpan>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PartitionResolvedSpan {
    pub routing_context_required: bool,
    pub start_block: u64,
    pub stop_block: u64,
    pub complete: bool,
    pub first_observed: Option<crate::grpc::FinalizedAnchor>,
    pub routing_start_timestamp: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionResolveOptions {
    pub strict_single_chain: bool,
    pub all_spans: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionBuildType {
    Date,
    Hour,
    Minute,
    Second,
    BlockRange,
}

impl PartitionBuildType {
    pub fn from_cli_value(value: &str) -> anyhow::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "day" | "date" => Ok(Self::Date),
            "hour" => Ok(Self::Hour),
            "minute" => Ok(Self::Minute),
            "second" => Ok(Self::Second),
            "block_range" | "block-range" | "blocks" => Ok(Self::BlockRange),
            other => anyhow::bail!(
                "invalid partition type '{other}': expected one of date, hour, minute, second, block_range"
            ),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
            Self::BlockRange => "block_range",
        }
    }

    /// Returns true if this is a time-based partition type.
    pub fn is_time_based(&self) -> bool {
        !matches!(self, Self::BlockRange)
    }

    /// For time-based types, returns the interval in seconds.
    /// For block_range, returns 0 (use `block_range_size` instead).
    pub fn interval_seconds(&self) -> i64 {
        match self {
            Self::Date => 86_400,
            Self::Hour => 3_600,
            Self::Minute => 60,
            Self::Second => 1,
            Self::BlockRange => 0,
        }
    }

    pub fn round_timestamp(&self, timestamp: i64) -> anyhow::Result<i64> {
        use time::OffsetDateTime;

        if matches!(self, Self::BlockRange) {
            anyhow::bail!("round_timestamp is not applicable for block_range partitions");
        }

        let dt = OffsetDateTime::from_unix_timestamp(timestamp)
            .map_err(|e| anyhow::anyhow!("invalid unix timestamp {timestamp}: {e}"))?;
        let rounded = match self {
            Self::Date => dt.replace_time(time::Time::MIDNIGHT),
            Self::Hour => dt.replace_minute(0)?.replace_second(0)?,
            Self::Minute => dt.replace_second(0)?,
            Self::Second => dt,
            Self::BlockRange => unreachable!(),
        };
        Ok(rounded.unix_timestamp())
    }
}

impl std::fmt::Display for PartitionBuildType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub(in crate::cli) fn canonical_partition_type_label(value: &str) -> anyhow::Result<String> {
    Ok(PartitionBuildType::from_cli_value(value)?.to_string())
}

pub(in crate::cli) fn normalize_partition_bounds_request(
    mut request: PartitionBoundsRequest,
) -> anyhow::Result<PartitionBoundsRequest> {
    request.partition_type = canonical_partition_type_label(&request.partition_type)?;
    Ok(request)
}

pub(in crate::cli) fn normalize_partition_window_request(
    mut request: PartitionWindowRequest,
) -> anyhow::Result<PartitionWindowRequest> {
    request.partition_type = canonical_partition_type_label(&request.partition_type)?;
    Ok(request)
}

pub(in crate::cli) fn normalize_partition_list_request(
    mut request: PartitionListRequest,
) -> anyhow::Result<PartitionListRequest> {
    request.partition_type = request
        .partition_type
        .as_deref()
        .map(canonical_partition_type_label)
        .transpose()?;
    Ok(request)
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionBuildRow {
    pub partition_type: String,
    pub partition_interval_seconds: i64,
    /// For time-based: epoch seconds formatted as "YYYY-MM-DD HH:MM:SS".
    /// For block_range: the start block number formatted as a string.
    pub partition_start_ts: String,
    /// For time-based: same as partition_start_ts.
    /// For block_range: the start block number as a string.
    pub partition_value: String,
    pub start_block: u64,
    pub stop_block: u64,
    /// Nullable — populated best-effort, None when block is missing/has no timestamp.
    pub start_time: Option<String>,
    /// Nullable — populated best-effort, None when block is missing/has no timestamp.
    pub end_time: Option<String>,
    pub chain: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionBuildResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coverage: Option<PartitionCoverage>,
    pub partitions_index: String,
    pub chain: String,
    pub partition: String,
    pub row_count: usize,
    pub start_block: u64,
    pub stop_block: u64,
    pub resumed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_from_block: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionListRequest {
    pub index_path: String,
    pub partition_type: Option<String>,
    pub chain: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionShardStrategy {
    Ordinal,
    Hash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionShardRequest {
    pub list: PartitionListRequest,
    pub shard_count: usize,
    pub shard_index: usize,
    pub strategy: PartitionShardStrategy,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionListRow {
    pub routing_context_required: Option<bool>,
    /// None means legacy/unknown, never implicitly complete.
    pub complete: Option<bool>,
    pub partition_type: String,
    pub partition_value: String,
    pub partition_start_ts: String,
    pub start_block: u64,
    pub stop_block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionListResult {
    pub coverage: Option<PartitionCoverage>,
    pub partitions_index: String,
    pub limit: usize,
    pub total_matches: usize,
    pub returned_rows: usize,
    pub rows: Vec<PartitionListRow>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionShardResult {
    pub coverage: PartitionCoverage,
    pub partitions_index: String,
    pub shard_count: usize,
    pub shard_index: usize,
    pub strategy: PartitionShardStrategy,
    pub total_matches: usize,
    pub returned_rows: usize,
    pub rows: Vec<PartitionListRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionValidateRequest {
    pub list: PartitionListRequest,
    pub allow_gaps: bool,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionValidationIssueKind {
    InvalidRange,
    Gap,
    Overlap,
    OutOfOrder,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionValidationIssue {
    pub kind: PartitionValidationIssueKind,
    pub partition_type: String,
    pub partition_value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionValidateResult {
    pub coverage: Option<PartitionCoverage>,
    pub incomplete_spans: usize,
    pub unknown_spans: usize,
    pub partitions_index: String,
    pub total_rows: usize,
    pub issue_count: usize,
    pub valid: bool,
    pub issues: Vec<PartitionValidationIssue>,
}

pub fn parse_partition_build_types(spec: &str) -> anyhow::Result<Vec<PartitionBuildType>> {
    let value = spec.trim();
    if value.is_empty() {
        anyhow::bail!("--partition is required");
    }
    if value.contains(',') {
        anyhow::bail!(
            "--partition accepts exactly one value per run; got `{}`",
            spec.trim()
        );
    }

    Ok(vec![PartitionBuildType::from_cli_value(value)?])
}

pub fn build_partitions_output_root(output_root: &str, chain: &str) -> String {
    let normalized_root = output_root.trim_end_matches('/');
    if normalized_root.starts_with("s3://") {
        format!("{normalized_root}/{chain}")
    } else {
        std::path::PathBuf::from(normalized_root)
            .join(chain)
            .to_string_lossy()
            .into_owned()
    }
}

pub fn build_partitions_index_path(output_root: &str, chain: &str) -> String {
    let chain_root = build_partitions_output_root(output_root, chain);
    if chain_root.starts_with("s3://") {
        format!("{chain_root}/partitions.parquet")
    } else {
        std::path::PathBuf::from(chain_root)
            .join("partitions.parquet")
            .to_string_lossy()
            .into_owned()
    }
}

pub fn build_partitions_cursor_path(output_root: &str, chain: &str) -> String {
    let chain_root = build_partitions_output_root(output_root, chain);
    if chain_root.starts_with("s3://") {
        format!("{chain_root}/cursor.parquet")
    } else {
        std::path::PathBuf::from(chain_root)
            .join("cursor.parquet")
            .to_string_lossy()
            .into_owned()
    }
}

pub(in crate::cli) fn format_partition_timestamp(timestamp: i64) -> anyhow::Result<String> {
    use time::OffsetDateTime;

    let dt = OffsetDateTime::from_unix_timestamp(timestamp)
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

pub(in crate::cli) fn parse_partition_timestamp(value: &str) -> anyhow::Result<i64> {
    use time::{Date, Month, PrimitiveDateTime, Time};

    let (date_part, time_part) = value.split_once(' ').ok_or_else(|| {
        anyhow::anyhow!("invalid partition timestamp '{value}': expected YYYY-MM-DD HH:MM:SS")
    })?;
    let mut date_iter = date_part.split('-');
    let year: i32 = date_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing year in '{value}'"))?
        .parse()?;
    let month: u8 = date_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing month in '{value}'"))?
        .parse()?;
    let day: u8 = date_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing day in '{value}'"))?
        .parse()?;

    let mut time_iter = time_part.split(':');
    let hour: u8 = time_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing hour in '{value}'"))?
        .parse()?;
    let minute: u8 = time_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing minute in '{value}'"))?
        .parse()?;
    let second: u8 = time_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing second in '{value}'"))?
        .parse()?;

    let month = Month::try_from(month)?;
    let date = Date::from_calendar_date(year, month, day)?;
    let time = Time::from_hms(hour, minute, second)?;
    Ok(PrimitiveDateTime::new(date, time)
        .assume_utc()
        .unix_timestamp())
}

/// Numeric key of a partition value, matching the canonical `partition` column: the
/// start block for `block_range` partitions and UTC epoch seconds for time-based
/// partitions (`YYYY-MM-DD HH:MM:SS`).
///
/// Partition rows must be ordered and range-filtered on this key rather than on the
/// rendered string, because block numbers do not sort lexicographically
/// (`"10000000" < "8000000"`).
pub fn partition_value_key(partition_type: &str, partition_value: &str) -> anyhow::Result<u64> {
    if PartitionBuildType::from_cli_value(partition_type)? == PartitionBuildType::BlockRange {
        return partition_value.parse::<u64>().map_err(|error| {
            anyhow::anyhow!(
                "'{partition_value}' is not a valid block_range partition value: expected a start block number ({error})"
            )
        });
    }

    let timestamp = parse_partition_timestamp(partition_value).map_err(|error| {
        anyhow::anyhow!(
            "'{partition_value}' is not a valid {partition_type} partition value: expected YYYY-MM-DD HH:MM:SS ({error})"
        )
    })?;
    u64::try_from(timestamp).map_err(|_| {
        anyhow::anyhow!(
            "'{partition_value}' is not a valid {partition_type} partition value: timestamps before 1970-01-01 00:00:00 are not supported"
        )
    })
}

impl PartitionBuildRow {
    /// Numeric partition key used for ordering and range filters (see [`partition_value_key`]).
    pub fn partition_key(&self) -> anyhow::Result<u64> {
        partition_value_key(&self.partition_type, &self.partition_value)
    }
}

/// Order rows by partition type, then numeric partition key, then block bounds.
pub(in crate::cli) fn sort_keyed_partition_rows(rows: &mut [(u64, PartitionBuildRow)]) {
    rows.sort_by(|(left_key, left), (right_key, right)| {
        left.partition_type
            .cmp(&right.partition_type)
            .then_with(|| left_key.cmp(right_key))
            .then_with(|| left.start_block.cmp(&right.start_block))
            .then_with(|| left.stop_block.cmp(&right.stop_block))
    });
}

pub(in crate::cli) fn sort_partition_build_rows(
    rows: Vec<PartitionBuildRow>,
) -> anyhow::Result<Vec<PartitionBuildRow>> {
    let mut keyed = rows
        .into_iter()
        .map(|row| Ok((row.partition_key()?, row)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    sort_keyed_partition_rows(&mut keyed);
    Ok(keyed.into_iter().map(|(_, row)| row).collect())
}

/// Parse a user-supplied partition bound (for example `--from`) against a partition type.
pub(in crate::cli) fn parse_partition_bound(
    flag: &str,
    partition_type: &str,
    value: &str,
) -> anyhow::Result<u64> {
    partition_value_key(partition_type, value)
        .map_err(|error| anyhow::anyhow!("invalid {flag}: {error}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionBoundsRequest {
    pub index_path: String,
    pub partition_type: String,
    pub partition_value: String,
    pub chain: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionWindowRequest {
    pub index_path: String,
    pub partition_type: String,
    pub partition_from: String,
    pub partition_to: String,
    pub chain: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionBounds {
    pub coverage: PartitionCoverage,
    pub start_block: u64,
    pub stop_block: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionWindowBounds {
    pub coverage: PartitionCoverage,
    pub start_block: u64,
    pub stop_block: u64,
    pub partitions_count: usize,
    pub partition_from: String,
    pub partition_to: String,
}
