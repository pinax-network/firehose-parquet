//! Truncate (delete) parquet files from local filesystem or S3,
//! with optional partition filtering.
//!
//! Real deletes require [`TruncateConfig::yes`]; without it, truncate prints what it matched and
//! returns an error. The path is resolved with
//! [`resolve_destructive_input_path`](crate::cli::resolve_destructive_input_path), so a missing
//! local path is never turned into an `s3://$S3_BUCKET/...` prefix.

use crate::artifacts::{is_control_path, is_reserved_artifact_path};
use crate::cli::{block_on_async, format_bytes, resolve_destructive_input_path, AwsConfig};
use crate::config::{DAY_PARTITION_PREFIX, LEGACY_DAY_PARTITION_PREFIX};
use crate::dataset_lock::DatasetOwnership;
use crate::ingest::maintenance::{self, MaintenancePolicy, MaintenanceTarget};
use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// How many matched files the summary lists before "... and N more".
const SUMMARY_LISTED_FILES: usize = 10;

/// Group name shared by all partition-path filters (`year=2026/month=01`).
const PATH_FILTER_GROUP: &str = "/";

/// Configuration for a truncate operation.
pub struct TruncateConfig {
    pub path: String,
    /// Partition filters (e.g. "day=01", "month=01", or "year=2026/month=01/day=15").
    /// Empty = delete all.
    pub partitions: Vec<String>,
    pub dry_run: bool,
    /// Delete the matched files. Without it (and without `dry_run`), truncate prints a summary
    /// of what matched and returns an error without deleting anything.
    pub yes: bool,
    pub aws: Option<AwsConfig>,
}

/// Summary of a truncate operation.
#[derive(Debug, Default)]
pub struct TruncateResult {
    pub files_deleted: usize,
    pub bytes_freed: u64,
    pub dirs_removed: usize,
}

impl TruncateResult {
    pub fn print(&self, _path: &str, dry_run: bool) {
        println!();
        if dry_run {
            println!("  (dry run — no files were deleted)");
        }
        println!("  Files deleted:      {}", self.files_deleted);
        println!("  Bytes freed:        {}", format_bytes(self.bytes_freed));
        if self.dirs_removed > 0 {
            println!("  Empty dirs removed: {}", self.dirs_removed);
        }
        println!();
    }
}

/// Run the truncate operation.
pub fn run_truncate(config: &TruncateConfig) -> Result<TruncateResult> {
    let filters = PartitionFilters::parse(&config.partitions)?;
    let path = resolve_destructive_input_path(&config.path)?;
    let ownership = if config.dry_run || !config.yes {
        None
    } else {
        Some(
            maintenance::acquire_blocking(
                "truncate",
                vec![MaintenanceTarget::input(path.clone())?],
                MaintenancePolicy::Truncate,
                config.aws.as_ref(),
            )?
            .ownership,
        )
    };
    let result = if path.starts_with("s3://") {
        run_truncate_s3(config, &path, &filters)
    } else {
        run_truncate_local(config, &path, &filters, ownership.as_ref())
    }?;
    if let Some(ownership) = ownership {
        ownership.release_blocking()?;
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Partition filters
// ---------------------------------------------------------------------------

/// Parsed `--partition` filters.
///
/// Filters on different keys must all match (AND), and filters on the same key match when
/// any of them does (OR): `-p year=2026 -p month=01` selects January 2026 only, and
/// `-p day=01 -p day=02` selects both days. A filter containing `/`, such as
/// `year=2026/month=01/day=15`, is a partition path: it matches files whose partition
/// directories (the `key=value` directories below the truncate path) start with exactly those
/// segments. Several path filters match when any of them does, and must also satisfy the
/// single-key filters. A bare key (`minute`) matches every value of that key, and each segment
/// may contain one `*` glob. `day=` and the legacy `date=` key are aliases.
#[derive(Debug, Default)]
struct PartitionFilters {
    /// Filter group → the patterns in it; each pattern is a list of normalized segments.
    groups: BTreeMap<String, Vec<Vec<String>>>,
}

impl PartitionFilters {
    fn parse(raw_filters: &[String]) -> Result<Self> {
        let mut filters = Self::default();
        for raw in raw_filters {
            let trimmed = raw.trim().trim_matches('/');
            if trimmed.is_empty() {
                anyhow::bail!("invalid --partition filter '{raw}': it is empty");
            }
            let raw_segments: Vec<&str> = trimmed.split('/').collect();
            if raw_segments.len() > 1 {
                if let Some(segment) = raw_segments
                    .iter()
                    .find(|segment| !segment.is_empty() && !segment.contains('='))
                {
                    anyhow::bail!(
                        "invalid --partition filter '{raw}': `{segment}` is not a key=value \
                         segment. Path filters are partition paths like year=2026/month=01; \
                         to limit truncate to one table, pass the table directory as the path"
                    );
                }
            }
            let segments = raw_segments
                .iter()
                .map(|segment| normalize_filter_segment(segment, raw))
                .collect::<Result<Vec<_>>>()?;
            let group = if segments.len() == 1 {
                filter_key(&segments[0])
            } else {
                PATH_FILTER_GROUP.to_string()
            };
            filters.groups.entry(group).or_default().push(segments);
        }
        Ok(filters)
    }

    /// Returns true when the file at `rel_path` (relative to the truncate path) matches.
    fn matches(&self, rel_path: &str) -> bool {
        if self.groups.is_empty() {
            return true;
        }
        let mut dirs: Vec<&str> = rel_path.split('/').filter(|s| !s.is_empty()).collect();
        dirs.pop(); // The file name is not a partition directory.
        self.groups.iter().all(|(group, patterns)| {
            patterns.iter().any(|pattern| {
                if group == PATH_FILTER_GROUP {
                    partition_path_matches(pattern, &dirs)
                } else {
                    dirs.iter().any(|dir| segment_matches(&pattern[0], dir))
                }
            })
        })
    }
}

/// Normalizes one filter segment: a bare key becomes `key=*`, and the legacy `date=` key is
/// rewritten as `day=`.
fn normalize_filter_segment(segment: &str, raw: &str) -> Result<String> {
    if segment.is_empty() {
        anyhow::bail!("invalid --partition filter '{raw}': it has an empty path segment");
    }
    if segment.matches('*').count() > 1 {
        anyhow::bail!("invalid --partition filter '{raw}': use at most one `*` per segment");
    }
    let segment = if segment.contains('=') || segment.contains('*') {
        segment.to_string()
    } else {
        format!("{segment}=*")
    };
    Ok(canonical_day_key(&segment))
}

/// The key a single-segment filter constrains, with the legacy day key folded into `day`.
fn filter_key(segment: &str) -> String {
    let key = segment.split(['=', '*']).next().unwrap_or(segment);
    if key == LEGACY_DAY_PARTITION_PREFIX.trim_end_matches('=') {
        DAY_PARTITION_PREFIX.trim_end_matches('=').to_string()
    } else {
        key.to_string()
    }
}

/// Returns true when the partition directories in `dirs` (from the first `key=value`
/// directory on) start with the segments of `pattern`.
fn partition_path_matches(pattern: &[String], dirs: &[&str]) -> bool {
    let Some(start) = dirs.iter().position(|dir| dir.contains('=')) else {
        return false;
    };
    let partition_dirs = &dirs[start..];
    partition_dirs.len() >= pattern.len()
        && pattern
            .iter()
            .zip(partition_dirs)
            .all(|(filter, dir)| segment_matches(filter, dir))
}

/// Match one directory against a normalized filter segment: exact, or a single `*` glob.
/// `day=` and the legacy `date=` key are aliases.
fn segment_matches(filter: &str, dir: &str) -> bool {
    let filter_forms = [filter, &canonical_day_key(filter)];
    let dir_forms = [dir, &canonical_day_key(dir)];
    filter_forms
        .iter()
        .any(|filter| dir_forms.iter().any(|dir| glob_matches(filter, dir)))
}

fn glob_matches(filter: &str, segment: &str) -> bool {
    match filter.split_once('*') {
        Some((prefix, suffix)) => {
            segment.len() >= prefix.len() + suffix.len()
                && segment.starts_with(prefix)
                && segment.ends_with(suffix)
        }
        None => segment == filter,
    }
}

/// Rewrite the legacy `date=` day key as `day=`; other text is returned unchanged.
fn canonical_day_key(text: &str) -> String {
    match text.strip_prefix(LEGACY_DAY_PARTITION_PREFIX) {
        Some(value) => format!("{DAY_PARTITION_PREFIX}{value}"),
        None => text.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Plan and confirmation
// ---------------------------------------------------------------------------

/// A file selected for deletion.
struct Matched<L> {
    location: L,
    /// Full path or `s3://` URI shown to the operator.
    display: String,
    size: u64,
    /// Whether this is a dataset artifact such as `cursor.parquet` rather than table data.
    reserved: bool,
}

/// Prints what matched and decides whether to delete it.
///
/// Returns `Ok(true)` to delete, `Ok(false)` for a dry run, and an error when deleting was not
/// confirmed with `--yes`.
fn confirm_plan<L>(config: &TruncateConfig, root: &str, matched: &[Matched<L>]) -> Result<bool> {
    let total_bytes: u64 = matched.iter().map(|m| m.size).sum();

    println!("Truncating {root} ...\n");
    if !config.partitions.is_empty() {
        println!("  Partition filter(s): {}", config.partitions.join(", "));
    }
    if config.dry_run {
        for m in matched {
            println!("  would delete: {} ({})", m.display, format_bytes(m.size));
        }
    } else {
        println!(
            "  Matched {} file(s), {}:",
            matched.len(),
            format_bytes(total_bytes)
        );
        for m in matched.iter().take(SUMMARY_LISTED_FILES) {
            println!("    {} ({})", m.display, format_bytes(m.size));
        }
        if matched.len() > SUMMARY_LISTED_FILES {
            println!("    ... and {} more", matched.len() - SUMMARY_LISTED_FILES);
        }
    }
    let reserved: Vec<&str> = matched
        .iter()
        .filter(|m| m.reserved)
        .map(|m| m.display.as_str())
        .collect();
    if !reserved.is_empty() {
        println!(
            "  Includes dataset artifacts (not table data): {}",
            reserved.join(", ")
        );
    }

    if config.dry_run {
        return Ok(false);
    }
    if !config.yes {
        anyhow::bail!(
            "refusing to delete {} file(s) ({}) without --yes. Re-run with --yes to delete \
             them, or with --dry-run to list every file",
            matched.len(),
            format_bytes(total_bytes)
        );
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Local filesystem
// ---------------------------------------------------------------------------

fn run_truncate_local(
    config: &TruncateConfig,
    path: &str,
    filters: &PartitionFilters,
    ownership: Option<&DatasetOwnership>,
) -> Result<TruncateResult> {
    let root = PathBuf::from(path);
    if !root.exists() {
        anyhow::bail!("path does not exist: {}", root.display());
    }

    let all_files = collect_local_parquet_targets(&root)?;

    if all_files.is_empty() {
        println!("No .parquet files found in {}", root.display());
        return Ok(TruncateResult::default());
    }

    // Filter by partition.
    let matched: Vec<Matched<PathBuf>> = all_files
        .into_iter()
        .filter_map(|file| {
            let rel = local_match_path(&file, &root);
            filters.matches(&rel).then(|| Matched {
                display: file.display().to_string(),
                size: std::fs::metadata(&file).map(|m| m.len()).unwrap_or(0),
                reserved: is_reserved_artifact_path(&rel),
                location: file,
            })
        })
        .collect();

    if matched.is_empty() {
        println!("No files match the partition filter(s)");
        return Ok(TruncateResult::default());
    }

    let delete = confirm_plan(config, &root.display().to_string(), &matched)?;

    let mut result = TruncateResult {
        files_deleted: matched.len(),
        bytes_freed: matched.iter().map(|m| m.size).sum(),
        dirs_removed: 0,
    };
    if !delete {
        return Ok(result);
    }

    if let Some(ownership) = ownership {
        ownership.revalidate_local_paths()?;
    }
    for m in &matched {
        std::fs::remove_file(&m.location)
            .with_context(|| format!("deleting {}", m.location.display()))?;
    }

    // Clean up empty directories.
    if root.is_dir() {
        result.dirs_removed = cleanup_empty_dirs(&root)?;
    }

    Ok(result)
}

fn collect_local_parquet_targets(path: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if path.is_file() {
        if is_parquet_file(path) {
            files.push(path.to_path_buf());
        }
    } else {
        collect_parquet_files_recursive(path, &mut files)?;
        files.sort();
    }
    Ok(files)
}

fn local_match_path(file: &Path, root: &Path) -> String {
    if root.is_file() {
        file.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| file.to_string_lossy().into_owned())
    } else {
        file.strip_prefix(root)
            .unwrap_or(file)
            .to_string_lossy()
            .into_owned()
    }
}

fn is_parquet_file(path: &Path) -> bool {
    path.extension().is_some_and(|ext| ext == "parquet")
}

fn collect_parquet_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if is_control_path(&path.to_string_lossy()) {
            continue;
        }
        if path.is_dir() {
            collect_parquet_files_recursive(&path, out)?;
        } else if is_parquet_file(&path) {
            out.push(path);
        }
    }
    Ok(())
}

/// Remove empty directories recursively (bottom-up). Returns count of dirs removed.
fn cleanup_empty_dirs(dir: &Path) -> Result<usize> {
    let mut removed = 0usize;
    if !dir.is_dir() {
        return Ok(0);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if is_control_path(&path.to_string_lossy()) {
            continue;
        }
        if path.is_dir() {
            removed += cleanup_empty_dirs(&path)?;
            // Check if now empty.
            if std::fs::read_dir(&path)?.next().is_none() {
                std::fs::remove_dir(&path)?;
                removed += 1;
            }
        }
    }
    Ok(removed)
}

// ---------------------------------------------------------------------------
// S3
// ---------------------------------------------------------------------------

fn run_truncate_s3(
    config: &TruncateConfig,
    path: &str,
    filters: &PartitionFilters,
) -> Result<TruncateResult> {
    use crate::writer::parse_s3_url;

    let (bucket, prefix) = parse_s3_url(path)?;
    let aws = config
        .aws
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?;

    let client: Arc<dyn ObjectStore> = Arc::new(aws.build_s3_client_for_mutation(&bucket)?);
    truncate_s3(config, filters, &client, &bucket, &prefix)
}

fn truncate_s3(
    config: &TruncateConfig,
    filters: &PartitionFilters,
    client: &Arc<dyn ObjectStore>,
    bucket: &str,
    prefix: &str,
) -> Result<TruncateResult> {
    use futures::TryStreamExt;

    let root = if prefix.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{prefix}")
    };
    let list_prefix = if prefix.is_empty() {
        None
    } else {
        Some(object_store::path::Path::from(prefix))
    };

    let objects: Vec<object_store::ObjectMeta> =
        block_on_async(async { client.list(list_prefix.as_ref()).try_collect().await })
            .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let parquet_objects: Vec<_> = objects
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .filter(|obj| !is_control_path(obj.location.as_ref()))
        .collect();

    if parquet_objects.is_empty() {
        println!("No .parquet files found in {root}");
        return Ok(TruncateResult::default());
    }

    // Filter by partition.
    let matched: Vec<Matched<object_store::path::Path>> = parquet_objects
        .into_iter()
        .filter_map(|obj| {
            let key = obj.location.as_ref();
            let rel = key
                .strip_prefix(prefix)
                .map(|s| s.trim_start_matches('/'))
                .unwrap_or(key);
            filters.matches(rel).then(|| Matched {
                display: format!("s3://{bucket}/{key}"),
                size: obj.size,
                reserved: is_reserved_artifact_path(rel),
                location: obj.location.clone(),
            })
        })
        .collect();

    if matched.is_empty() {
        println!("No files match the partition filter(s)");
        return Ok(TruncateResult::default());
    }

    let delete = confirm_plan(config, &root, &matched)?;

    let result = TruncateResult {
        files_deleted: matched.len(),
        bytes_freed: matched.iter().map(|m| m.size).sum(),
        dirs_removed: 0,
    };
    if !delete {
        return Ok(result);
    }

    let keys = matched
        .iter()
        .map(|entry| entry.location.clone())
        .collect::<Vec<_>>();
    block_on_async(crate::s3::delete::delete_objects_once(client, &keys))?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn write_test_file(path: &Path, contents: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, contents).unwrap();
    }

    fn config(path: &Path, partitions: &[&str], dry_run: bool, yes: bool) -> TruncateConfig {
        TruncateConfig {
            path: path.to_string_lossy().into_owned(),
            partitions: partitions.iter().map(|p| p.to_string()).collect(),
            dry_run,
            yes,
            aws: None,
        }
    }

    fn filters(raw: &[&str]) -> PartitionFilters {
        let raw: Vec<String> = raw.iter().map(|filter| filter.to_string()).collect();
        PartitionFilters::parse(&raw).unwrap()
    }

    fn matches(path: &str, raw: &[&str]) -> bool {
        filters(raw).matches(path)
    }

    #[test]
    fn collect_local_parquet_targets_includes_root_level_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mainnet");
        write_test_file(&root.join("partitions.parquet"), b"partitions");
        write_test_file(&root.join("cursor.parquet"), b"cursor");
        write_test_file(
            &root.join("blocks/year=2024/month=01/date=15/part-0001.parquet"),
            b"blocks",
        );
        write_test_file(&root.join("README.txt"), b"not parquet");

        let files = collect_local_parquet_targets(&root).unwrap();
        let rel_paths: Vec<_> = files
            .iter()
            .map(|path| {
                path.strip_prefix(&root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();

        assert_eq!(
            rel_paths,
            vec![
                "blocks/year=2024/month=01/date=15/part-0001.parquet".to_string(),
                "cursor.parquet".to_string(),
                "partitions.parquet".to_string(),
            ]
        );
    }

    #[test]
    fn run_truncate_local_dry_run_counts_root_level_artifacts_without_deleting() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mainnet");
        write_test_file(&root.join("partitions.parquet"), b"partitions");
        write_test_file(&root.join("cursor.parquet"), b"cursor");
        write_test_file(
            &root.join("blocks/year=2024/month=01/date=15/part-0001.parquet"),
            b"blocks",
        );

        let result = run_truncate(&config(&root, &[], true, false)).unwrap();

        assert_eq!(result.files_deleted, 3);
        assert!(root.join("partitions.parquet").exists());
        assert!(root.join("cursor.parquet").exists());
        assert!(root
            .join("blocks/year=2024/month=01/date=15/part-0001.parquet")
            .exists());
    }

    #[test]
    fn run_truncate_local_deletes_single_explicit_parquet_file() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("unichain");
        let partitions = root.join("partitions.parquet");
        let cursor = root.join("cursor.parquet");
        write_test_file(&partitions, b"partitions");
        write_test_file(&cursor, b"cursor");

        let result = run_truncate(&config(&partitions, &[], false, true)).unwrap();

        assert_eq!(result.files_deleted, 1);
        assert_eq!(result.bytes_freed, b"partitions".len() as u64);
        assert_eq!(result.dirs_removed, 0);
        assert!(!partitions.exists());
        assert!(cursor.exists());
    }

    #[test]
    fn matches_partition_treats_day_and_legacy_date_keys_as_aliases() {
        let current = "blocks/year=2024/month=01/day=15/part-0001.parquet";
        let legacy = "blocks/year=2024/month=01/date=15/part-0001.parquet";
        let other_day = "blocks/year=2024/month=01/day=16/part-0001.parquet";

        for filter in ["day=15", "date=15", "day=1*", "date=1*", "day", "date"] {
            assert!(matches(current, &[filter]), "{filter:?} on day=");
            assert!(matches(legacy, &[filter]), "{filter:?} on date=");
        }
        assert!(!matches(other_day, &["day=15"]));
        assert!(!matches(other_day, &["date=15"]));

        // Globs spelled against the raw legacy key keep matching legacy trees.
        assert!(matches(legacy, &["date*"]));
        // Other keys are unaffected.
        assert!(matches(current, &["month=01"]));
        assert!(!matches(current, &["hour"]));
        // `day` and `date` are the same key, so they are OR'd rather than AND'd.
        assert!(matches(current, &["date=15", "day=16"]));
    }

    /// `-p year=2026 -p month=01` used to also match January 2025, because filters were OR'd.
    #[test]
    fn filters_are_anded_across_keys_and_ored_within_a_key() {
        let jan_2026 = "blocks/year=2026/month=01/day=15/part-1.parquet";
        let jan_2025 = "blocks/year=2025/month=01/day=15/part-1.parquet";
        let feb_2026 = "blocks/year=2026/month=02/day=15/part-1.parquet";

        let january_2026 = ["year=2026", "month=01"];
        assert!(matches(jan_2026, &january_2026));
        assert!(!matches(jan_2025, &january_2026));
        assert!(!matches(feb_2026, &january_2026));

        // Same key: either value.
        assert!(matches(jan_2026, &["month=01", "month=02"]));
        assert!(matches(feb_2026, &["month=01", "month=02"]));
        assert!(!matches(jan_2026, &["month=03", "month=04"]));

        // Mixed: (year=2026) AND (month=01 OR month=02).
        let filters = ["year=2026", "month=01", "month=02"];
        assert!(matches(jan_2026, &filters));
        assert!(matches(feb_2026, &filters));
        assert!(!matches(jan_2025, &filters));

        // A key-only filter requires the key to be present.
        assert!(matches(jan_2026, &["year=2026", "day"]));
        assert!(!matches(jan_2026, &["year=2026", "hour"]));
    }

    #[test]
    fn path_filter_matches_the_start_of_the_partition_path() {
        let day_15 = "year=2026/month=01/day=15/hour=00/part-1.parquet";
        let path = ["year=2026/month=01/day=15"];

        // From a table root, a network root, and a parent of network roots.
        assert!(matches(day_15, &path));
        assert!(matches(&format!("blocks/{day_15}"), &path));
        assert!(matches(&format!("mainnet/blocks/{day_15}"), &path));
        // Legacy `date=` directories, and a trailing slash in the filter.
        assert!(matches(
            "blocks/year=2026/month=01/date=15/part-1.parquet",
            &["year=2026/month=01/day=15/"]
        ));

        assert!(!matches(
            "blocks/year=2026/month=01/day=16/part-1.parquet",
            &path
        ));
        assert!(!matches(
            "blocks/year=2025/month=01/day=15/part-1.parquet",
            &path
        ));
        // It is anchored at the first partition directory: `month=01/day=15` is not a prefix.
        assert!(!matches(day_15, &["month=01/day=15"]));
        // The path is not deeper than the filter asks for.
        assert!(!matches("blocks/year=2026/month=01/part-1.parquet", &path));

        // Globs per segment.
        assert!(matches(day_15, &["year=2026/month=01/day=*"]));
        assert!(matches(day_15, &["year=2026/month=0*"]));

        // Several path filters: any of them.
        let two_days = ["year=2026/month=01/day=01", "year=2026/month=01/day=15"];
        assert!(matches(day_15, &two_days));
        assert!(!matches(
            "year=2026/month=01/day=02/part-1.parquet",
            &two_days
        ));

        // Path filters AND key filters.
        assert!(matches(day_15, &["year=2026/month=01", "hour=00"]));
        assert!(!matches(day_15, &["year=2026/month=01", "hour=01"]));
    }

    #[test]
    fn filters_never_match_root_artifacts() {
        for filter in ["year=2026", "year", "year=2026/month=01", "*"] {
            assert!(!matches("cursor.parquet", &[filter]), "{filter}");
            assert!(!matches("partitions.parquet", &[filter]), "{filter}");
        }
        // Without filters everything under the path matches, root artifacts included.
        assert!(matches("cursor.parquet", &[]));
    }

    #[test]
    fn invalid_filters_are_rejected() {
        let parse = |raw: &str| {
            PartitionFilters::parse(&[raw.to_string()])
                .unwrap_err()
                .to_string()
        };
        assert!(parse("").contains("it is empty"));
        assert!(parse("/").contains("it is empty"));
        assert!(parse("year=2026//day=15").contains("empty path segment"));
        assert!(parse("day=*1*").contains("at most one `*`"));
        let table = parse("blocks/year=2026");
        assert!(
            table.contains("`blocks` is not a key=value segment"),
            "{table}"
        );
        assert!(
            table.contains("pass the table directory as the path"),
            "{table}"
        );
    }

    #[test]
    fn run_truncate_local_day_filter_deletes_current_and_legacy_day_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mainnet");
        let current = root.join("blocks/year=2024/month=01/day=15/part-0001.parquet");
        let legacy = root.join("blocks/year=2024/month=01/date=15/part-0001.parquet");
        let kept = root.join("blocks/year=2024/month=01/day=16/part-0001.parquet");
        for path in [&current, &legacy, &kept] {
            write_test_file(path, b"blocks");
        }

        let result = run_truncate(&config(&root, &["day=15"], false, true)).unwrap();

        assert_eq!(result.files_deleted, 2);
        assert!(!current.exists());
        assert!(!legacy.exists());
        assert!(kept.exists());
    }

    /// The issue's scenario: deleting one month must not touch the same month of other years.
    #[test]
    fn run_truncate_local_deletes_only_the_selected_month() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mainnet");
        let jan_2026 = root.join("blocks/year=2026/month=01/day=01/part-1.parquet");
        let jan_2025 = root.join("blocks/year=2025/month=01/day=01/part-1.parquet");
        let feb_2026 = root.join("blocks/year=2026/month=02/day=01/part-1.parquet");
        let cursor = root.join("cursor.parquet");
        for path in [&jan_2026, &jan_2025, &feb_2026, &cursor] {
            write_test_file(path, b"data");
        }

        for partitions in [&["year=2026", "month=01"][..], &["year=2026/month=01"][..]] {
            write_test_file(&jan_2026, b"data");
            let result = run_truncate(&config(&root, partitions, false, true)).unwrap();
            assert_eq!(result.files_deleted, 1, "{partitions:?}");
            assert!(!jan_2026.exists(), "{partitions:?}");
            assert!(jan_2025.exists() && feb_2026.exists() && cursor.exists());
        }
    }

    #[test]
    fn run_truncate_refuses_to_delete_without_yes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mainnet");
        let files = [
            root.join("blocks/year=2026/month=01/day=01/part-1.parquet"),
            root.join("blocks/year=2026/month=01/day=02/part-1.parquet"),
            root.join("cursor.parquet"),
        ];
        for path in &files {
            write_test_file(path, b"data");
        }

        let err = run_truncate(&config(&root, &[], false, false))
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("refusing to delete 3 file(s) (12 B) without --yes"),
            "{err}"
        );
        assert!(files.iter().all(|path| path.exists()));

        // A dry run needs no confirmation and deletes nothing either.
        let result = run_truncate(&config(&root, &[], true, false)).unwrap();
        assert_eq!(result.files_deleted, 3);
        assert!(files.iter().all(|path| path.exists()));
    }

    #[test]
    fn run_truncate_rejects_a_missing_local_path() {
        let dir = tempfile::tempdir().unwrap();
        let err = run_truncate(&config(&dir.path().join("missing"), &[], false, true))
            .unwrap_err()
            .to_string();
        assert!(err.contains("path does not exist"), "{err}");
    }

    #[test]
    fn truncate_s3_failure_preserves_unselected_keys_and_stops_further_dispatch() {
        use crate::s3::delete::test_store::DelayedStore;
        use std::sync::atomic::Ordering;
        let mut fake = DelayedStore::new(std::time::Duration::from_millis(30));
        fake.fail = Some(object_store::path::Path::from(
            "evm/blocks/year=2026/part-000000.parquet",
        ));
        fake.lose_response = true;
        let fake = Arc::new(fake);
        let store: Arc<dyn ObjectStore> = fake.clone();
        for part in 0..30 {
            let key = object_store::path::Path::from(format!(
                "evm/blocks/year=2026/part-{part:06}.parquet"
            ));
            block_on_async(store.put(&key, b"selected".to_vec().into())).unwrap();
        }
        let retained = [
            "evm/cursor.parquet",
            "evm/blocks/year=2025/part-old.parquet",
            "other/blocks/year=2026/part-outside.parquet",
        ];
        for key in retained {
            block_on_async(store.put(
                &object_store::path::Path::from(key),
                b"unchanged".to_vec().into(),
            ))
            .unwrap();
        }
        let config = TruncateConfig {
            path: "s3://bucket/evm".into(),
            partitions: vec!["year=2026".into()],
            dry_run: false,
            yes: true,
            aws: None,
        };
        assert!(truncate_s3(
            &config,
            &PartitionFilters::parse(&config.partitions).unwrap(),
            &store,
            "bucket",
            "evm"
        )
        .is_err());
        for key in retained {
            assert_eq!(
                block_on_async(async {
                    store
                        .get(&object_store::path::Path::from(key))
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap()
                })
                .as_ref(),
                b"unchanged"
            );
        }
        assert_eq!(fake.counters.started.lock().unwrap().len(), 10);
        assert_eq!(fake.counters.completed.load(Ordering::SeqCst), 10);
        assert_eq!(fake.counters.active.load(Ordering::SeqCst), 0);
        assert!(block_on_async(store.head(&object_store::path::Path::from(
            "evm/blocks/year=2026/part-000029.parquet"
        )))
        .is_ok());
    }

    #[test]
    fn truncate_s3_applies_filters_and_requires_yes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = [
            "evm/mainnet/blocks/year=2026/month=01/day=15/part-1.parquet",
            "evm/mainnet/blocks/year=2025/month=01/day=15/part-1.parquet",
            "evm/mainnet/logs/year=2026/month=01/day=15/part-1.parquet",
            "evm/mainnet/cursor.parquet",
        ];
        for key in keys {
            let path = object_store::path::Path::from(key);
            block_on_async(store.put(&path, b"data".to_vec().into())).unwrap();
        }
        let list = || -> Vec<String> {
            use futures::TryStreamExt;
            let objects: Vec<object_store::ObjectMeta> =
                block_on_async(store.list(None).try_collect()).unwrap();
            let mut keys: Vec<String> = objects
                .into_iter()
                .map(|obj| obj.location.as_ref().to_string())
                .collect();
            keys.sort();
            keys
        };
        let run = |yes: bool| {
            let config = TruncateConfig {
                path: "s3://bucket/evm/mainnet".to_string(),
                partitions: vec!["year=2026/month=01/day=15".to_string()],
                dry_run: false,
                yes,
                aws: None,
            };
            let filters = PartitionFilters::parse(&config.partitions).unwrap();
            truncate_s3(&config, &filters, &store, "bucket", "evm/mainnet")
        };

        let err = run(false).unwrap_err().to_string();
        assert!(err.contains("refusing to delete 2 file(s)"), "{err}");
        assert_eq!(list().len(), 4);

        let result = run(true).unwrap();
        assert_eq!(result.files_deleted, 2);
        assert_eq!(
            list(),
            vec![
                "evm/mainnet/blocks/year=2025/month=01/day=15/part-1.parquet".to_string(),
                "evm/mainnet/cursor.parquet".to_string(),
            ]
        );
    }
}
