//! Truncate (delete) parquet files from local filesystem or S3,
//! with optional partition filtering.
//!
//! Real deletes require [`TruncateConfig::yes`]; without it, truncate prints what it matched and
//! returns an error. The path is resolved with
//! [`resolve_destructive_input_path`](crate::cli::resolve_destructive_input_path), so a missing
//! local path is never turned into an `s3://$S3_BUCKET/...` prefix.

use crate::artifacts::{is_control_path, is_reserved_artifact_path};
use crate::cli::{block_on_async, format_bytes, resolve_destructive_input_path, AwsConfig};
use crate::dataset_lock::DatasetOwnership;
use crate::date_partition::{is_date_value_pattern, DatePartition, DATE_KEY};
use crate::ingest::maintenance::{self, MaintenancePolicy, MaintenanceTarget};
use crate::maintenance::discovery::{self, LocalPolicy};
use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// How many matched files the summary lists before "... and N more".
const SUMMARY_LISTED_FILES: usize = 10;

/// Configuration for a truncate operation.
pub struct TruncateConfig {
    pub path: String,
    /// Partition filters: `date=` values or globs (e.g. "date=2026-01-15" or
    /// "date=2026-01-*"). Empty = delete all.
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

/// Parsed `--partition` filters: `date=` values of the `date=YYYY-MM-DD` partitions, the
/// only partition key (#652).
///
/// A filter is a date (`date=2026-01-15`) or a glob over one with a single `*`
/// (`date=2026-01-*` for a month, `date=2026-*` for a year, `date=*-15` for every 15th).
/// Several filters match when any of them does. A value that is not a date or such a glob,
/// for example `date=15`, is refused instead of silently matching nothing.
#[derive(Debug, Default)]
struct PartitionFilters {
    /// Glob patterns over the `YYYY-MM-DD` value.
    dates: Vec<String>,
}

impl PartitionFilters {
    fn parse(raw_filters: &[String]) -> Result<Self> {
        let mut filters = Self::default();
        for raw in raw_filters {
            let trimmed = raw.trim().trim_matches('/');
            if trimmed.is_empty() {
                anyhow::bail!("invalid --partition filter '{raw}': it is empty");
            }
            let value = match trimmed.split_once('=').filter(|_| !trimmed.contains('/')) {
                Some((DATE_KEY, value)) => value,
                None if trimmed == DATE_KEY => "*",
                _ => anyhow::bail!(
                    "invalid --partition filter '{raw}': filters select `date=YYYY-MM-DD` \
                     partitions, the only partition key: use -p date=2026-01-15, \
                     -p \"date=2026-01-*\" for a month or -p \"date=2026-*\" for a year. To \
                     limit truncate to one table, pass the table directory as the path"
                ),
            };
            if value.matches('*').count() > 1 {
                anyhow::bail!("invalid --partition filter '{raw}': use at most one `*`");
            }
            if !is_date_value_pattern(value) {
                anyhow::bail!(
                    "invalid --partition filter '{raw}': `{value}` is not a YYYY-MM-DD date or \
                     a glob over one, such as 2026-01-* or *-15"
                );
            }
            filters.dates.push(value.to_string());
        }
        Ok(filters)
    }

    /// Returns true when the file at `rel_path` (relative to the truncate path) matches:
    /// it is below a `date=YYYY-MM-DD` directory whose date matches a filter.
    ///
    /// A partition filter selects table data only: it never matches a reserved dataset
    /// artifact, even a partition-shaped path under `_fireparq/` or `verify_runs/`.
    fn matches(&self, rel_path: &str) -> bool {
        if self.dates.is_empty() {
            return true;
        }
        if is_reserved_artifact_path(rel_path) {
            return false;
        }
        let mut dirs: Vec<&str> = rel_path.split('/').filter(|s| !s.is_empty()).collect();
        dirs.pop(); // The file name is not a partition directory.
        dirs.iter()
            .filter(|dir| DatePartition::parse(dir).is_ok())
            .filter_map(|dir| dir.strip_prefix("date="))
            .any(|value| {
                self.dates
                    .iter()
                    .any(|pattern| glob_matches(pattern, value))
            })
    }
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

/// Selects the `files` matching the partition filters, prints the plan and applies the
/// confirmation rules shared by local and S3 truncation.
///
/// `describe` gives a file's path relative to the truncate path (what filters match) and its
/// displayed location; `size` is only read for matched files. Returns the summary and the
/// files to delete, which is empty unless deleting was confirmed.
fn plan<L>(
    config: &TruncateConfig,
    filters: &PartitionFilters,
    root: &str,
    files: Vec<L>,
    describe: impl Fn(&L) -> (String, String),
    size: impl Fn(&L) -> u64,
) -> Result<(TruncateResult, Vec<Matched<L>>)> {
    if files.is_empty() {
        println!("No .parquet files found in {root}");
        return Ok((TruncateResult::default(), Vec::new()));
    }

    // Filter by partition.
    let matched: Vec<Matched<L>> = files
        .into_iter()
        .filter_map(|location| {
            let (rel, display) = describe(&location);
            filters.matches(&rel).then(|| Matched {
                display,
                size: size(&location),
                reserved: is_reserved_artifact_path(&rel),
                location,
            })
        })
        .collect();

    if matched.is_empty() {
        println!("No files match the partition filter(s)");
        return Ok((TruncateResult::default(), Vec::new()));
    }

    let delete = confirm_plan(config, root, &matched)?;

    let result = TruncateResult {
        files_deleted: matched.len(),
        bytes_freed: matched.iter().map(|m| m.size).sum(),
        dirs_removed: 0,
    };
    Ok((result, if delete { matched } else { Vec::new() }))
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

    let (mut result, matched) = plan(
        config,
        filters,
        &root.display().to_string(),
        collect_local_parquet_targets(&root)?,
        |file| (local_match_path(file, &root), file.display().to_string()),
        |file| std::fs::metadata(file).map(|m| m.len()).unwrap_or(0),
    )?;
    if matched.is_empty() {
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
        discovery::collect_local(path, LocalPolicy::MUTATION_PARQUET, &mut files)?;
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
    let root = if prefix.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{prefix}")
    };
    let objects = block_on_async(discovery::list_objects(client.as_ref(), prefix))
        .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let parquet_objects: Vec<_> = objects
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .filter(|obj| !is_control_path(obj.location.as_ref()))
        .collect();

    let (result, matched) = plan(
        config,
        filters,
        &root,
        parquet_objects,
        |obj| {
            let key = obj.location.as_ref();
            (
                discovery::relative_key(prefix, key).to_string(),
                format!("s3://{bucket}/{key}"),
            )
        },
        |obj| obj.size,
    )?;
    if matched.is_empty() {
        return Ok(result);
    }

    let keys = matched
        .iter()
        .map(|entry| entry.location.location.clone())
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
        write_test_file(&root.join("merkle_roots.parquet"), b"registry");
        write_test_file(&root.join("cursor.parquet"), b"cursor");
        write_test_file(&root.join("_fireparq/merkle_roots.parquet"), b"registry");
        write_test_file(
            &root.join("blocks/date=2024-01-15/part-0001.parquet"),
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
                "_fireparq/merkle_roots.parquet".to_string(),
                "blocks/date=2024-01-15/part-0001.parquet".to_string(),
                "cursor.parquet".to_string(),
                "merkle_roots.parquet".to_string(),
            ]
        );
        // Each artifact is labelled as not table data.
        for rel in [
            "_fireparq/merkle_roots.parquet",
            "cursor.parquet",
            "merkle_roots.parquet",
        ] {
            assert!(crate::artifacts::is_reserved_artifact_path(rel), "{rel}");
        }
    }

    #[test]
    fn run_truncate_local_dry_run_counts_root_level_artifacts_without_deleting() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mainnet");
        write_test_file(&root.join("merkle_roots.parquet"), b"registry");
        write_test_file(&root.join("cursor.parquet"), b"cursor");
        write_test_file(
            &root.join("blocks/date=2024-01-15/part-0001.parquet"),
            b"blocks",
        );

        let result = run_truncate(&config(&root, &[], true, false)).unwrap();

        assert_eq!(result.files_deleted, 3);
        assert!(root.join("merkle_roots.parquet").exists());
        assert!(root.join("cursor.parquet").exists());
        assert!(root
            .join("blocks/date=2024-01-15/part-0001.parquet")
            .exists());
    }

    #[test]
    fn run_truncate_local_deletes_single_explicit_parquet_file() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("unichain");
        let registry = root.join("merkle_roots.parquet");
        let cursor = root.join("cursor.parquet");
        write_test_file(&registry, b"registry");
        write_test_file(&cursor, b"cursor");

        let result = run_truncate(&config(&registry, &[], false, true)).unwrap();

        assert_eq!(result.files_deleted, 1);
        assert_eq!(result.bytes_freed, b"registry".len() as u64);
        assert_eq!(result.dirs_removed, 0);
        assert!(!registry.exists());
        assert!(cursor.exists());
    }

    #[test]
    fn date_filters_match_the_date_directory_and_its_globs() {
        let day = "blocks/date=2024-01-15/part-0001.parquet";
        let next_day = "blocks/date=2024-01-16/part-0001.parquet";
        let next_year = "blocks/date=2025-01-15/part-0001.parquet";

        for filter in [
            "date=2024-01-15",
            "date=2024-01-15/",
            "date=2024-01-*",
            "date=2024-*",
            "date=*-15",
            "date=*",
            "date",
        ] {
            assert!(matches(day, &[filter]), "{filter:?}");
        }
        assert!(!matches(next_day, &["date=2024-01-15"]));
        assert!(!matches(next_year, &["date=2024-*"]));
        assert!(matches(next_year, &["date=*-01-15"]));
        // Several filters: any of them.
        let two_days = ["date=2024-01-15", "date=2024-01-16"];
        assert!(matches(day, &two_days) && matches(next_day, &two_days));
        assert!(!matches(next_year, &two_days));
        // From a network root and a parent of network roots.
        assert!(matches(&format!("mainnet/{day}"), &["date=2024-01-15"]));
        assert!(matches(&format!("out/mainnet/{day}"), &["date=2024-01-15"]));
        // Only a `date=YYYY-MM-DD` directory is a partition.
        assert!(!matches("blocks/part-0001.parquet", &["date=*"]));
        assert!(!matches("blocks/date=15/part-0001.parquet", &["date=*"]));
    }

    #[test]
    fn filters_never_match_root_artifacts() {
        for filter in ["date=2026-01-15", "date", "date=*"] {
            assert!(!matches("cursor.parquet", &[filter]), "{filter}");
            assert!(!matches("merkle_roots.parquet", &[filter]), "{filter}");
            assert!(
                !matches("_fireparq/merkle_roots.parquet", &[filter]),
                "{filter}"
            );
            for artifact in [
                "_fireparq/date=2026-01-15/part-1.parquet",
                "_fireparq/verify_runs/run/date=2026-01-15/part-1.parquet",
                "verify_runs/run/date=2026-01-15/part-1.parquet",
            ] {
                assert!(!matches(artifact, &[filter]), "{filter} {artifact}");
            }
            assert!(matches("blocks/date=2026-01-15/part-1.parquet", &[filter]));
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
        assert!(parse("date=*1*").contains("at most one `*`"));
        for other in [
            "hour=14",
            "block_range=0-100",
            "year=2026",
            "day=15",
            "blocks/date=2026-01-15",
            "date=2026-01-15/hour=14",
        ] {
            let error = parse(other);
            assert!(
                error.contains("filters select `date=YYYY-MM-DD` partitions")
                    && error.contains("pass the table directory as the path"),
                "{other}: {error}"
            );
        }
        // A day of the month or a malformed date never silently matches nothing.
        for value in ["date=15", "date=1*", "date=2026-1-15", "date=2026-02-30"] {
            assert!(parse(value).contains("is not a YYYY-MM-DD date"), "{value}");
        }
    }

    #[test]
    fn run_truncate_local_date_filter_deletes_only_that_day() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mainnet");
        let day = root.join("blocks/date=2024-01-15/part-0001.parquet");
        let day_logs = root.join("logs/date=2024-01-15/part-0001.parquet");
        let kept = root.join("blocks/date=2024-01-16/part-0001.parquet");
        for path in [&day, &day_logs, &kept] {
            write_test_file(path, b"blocks");
        }

        let result = run_truncate(&config(&root, &["date=2024-01-15"], false, true)).unwrap();

        assert_eq!(result.files_deleted, 2);
        assert!(!day.exists());
        assert!(!day_logs.exists());
        assert!(kept.exists());
    }

    /// Deleting one month must not touch the same month of other years.
    #[test]
    fn run_truncate_local_deletes_only_the_selected_month() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mainnet");
        let jan_2026 = root.join("blocks/date=2026-01-01/part-1.parquet");
        let jan_2025 = root.join("blocks/date=2025-01-01/part-1.parquet");
        let feb_2026 = root.join("blocks/date=2026-02-01/part-1.parquet");
        let cursor = root.join("cursor.parquet");
        for path in [&jan_2026, &jan_2025, &feb_2026, &cursor] {
            write_test_file(path, b"data");
        }

        let result = run_truncate(&config(&root, &["date=2026-01-*"], false, true)).unwrap();
        assert_eq!(result.files_deleted, 1);
        assert!(!jan_2026.exists());
        assert!(jan_2025.exists() && feb_2026.exists() && cursor.exists());
    }

    #[test]
    fn run_truncate_refuses_to_delete_without_yes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mainnet");
        let files = [
            root.join("blocks/date=2026-01-01/part-1.parquet"),
            root.join("blocks/date=2026-01-02/part-1.parquet"),
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
            "evm/blocks/date=2026-01-01/part-000000.parquet",
        ));
        fake.lose_response = true;
        let fake = Arc::new(fake);
        let store: Arc<dyn ObjectStore> = fake.clone();
        for part in 0..30 {
            let key = object_store::path::Path::from(format!(
                "evm/blocks/date=2026-01-01/part-{part:06}.parquet"
            ));
            block_on_async(store.put(&key, b"selected".to_vec().into())).unwrap();
        }
        let retained = [
            "evm/cursor.parquet",
            "evm/blocks/date=2025-01-01/part-old.parquet",
            "other/blocks/date=2026-01-01/part-outside.parquet",
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
            partitions: vec!["date=2026-*".into()],
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
            "evm/blocks/date=2026-01-01/part-000029.parquet"
        )))
        .is_ok());
    }

    #[test]
    fn truncate_s3_applies_filters_and_requires_yes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = [
            "evm/mainnet/blocks/date=2026-01-15/part-1.parquet",
            "evm/mainnet/blocks/date=2025-01-15/part-1.parquet",
            "evm/mainnet/logs/date=2026-01-15/part-1.parquet",
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
                partitions: vec!["date=2026-01-15".to_string()],
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
                "evm/mainnet/blocks/date=2025-01-15/part-1.parquet".to_string(),
                "evm/mainnet/cursor.parquet".to_string(),
            ]
        );
    }

    /// A dataset written with `--output s3://<bucket>` to a bucket root shares
    /// the root with the dataset artifacts and the bucket-wide owner record.
    /// A partition filter at the bucket root deletes only matching table
    /// parts; even an unfiltered run, which also removes the dataset
    /// artifacts as it does under a chain directory, never touches the
    /// ingestion controls, the owner record or its probes. (Protected datasets
    /// are refused before this step, wherever their root is.)
    #[test]
    fn truncate_s3_at_a_bucket_root_keeps_artifacts_and_controls() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let artifacts = [
            "_fireparq/cursor.parquet",
            "_fireparq/merkle_roots.parquet",
            "_fireparq/verify_runs/run-1/roots.parquet",
            // A partition-shaped path inside `_fireparq/` still never matches
            // a partition filter.
            "_fireparq/date=2026-01-15/part-3.parquet",
            "cursor.parquet",
            "merkle_roots.parquet",
            "verify_runs/run-1/roots.parquet",
        ];
        let controls = [
            ".fireparq-ingest/state.parquet",
            ".fireparq-owner-probes-v1/probe.parquet",
            crate::dataset_lock_s3::OWNER_KEY,
        ];
        let parts = [
            "blocks/date=2026-01-15/part-1.parquet",
            "logs/date=2026-01-15/part-2.parquet",
        ];
        for key in artifacts.iter().chain(&controls).chain(&parts) {
            let path = object_store::path::Path::from(*key);
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
        let run = |partitions: Vec<String>| {
            let config = TruncateConfig {
                path: "s3://bucket".to_string(),
                partitions,
                dry_run: false,
                yes: true,
                aws: None,
            };
            let filters = PartitionFilters::parse(&config.partitions).unwrap();
            truncate_s3(&config, &filters, &store, "bucket", "").unwrap()
        };
        let expected = |keys: &[&[&str]]| -> Vec<String> {
            let mut keys: Vec<String> = keys
                .iter()
                .flat_map(|keys| keys.iter().map(|key| key.to_string()))
                .collect();
            keys.sort();
            keys
        };

        assert_eq!(
            run(vec!["date=2026-01-15".into()]).files_deleted,
            parts.len()
        );
        assert_eq!(list(), expected(&[&artifacts, &controls]));
        assert_eq!(run(vec![]).files_deleted, artifacts.len());
        assert_eq!(list(), expected(&[&controls]));
    }

    /// Local and S3 truncation share one selection/confirmation step, so the same tree
    /// under the same filters selects the same files, sizes and dataset artifacts.
    #[test]
    fn local_and_s3_select_the_same_files_for_every_filter() {
        let tree = [
            ("cursor.parquet", 3usize),
            ("merkle_roots.parquet", 5),
            ("blocks/date=2026-01-15/part-1.parquet", 7),
            ("blocks/date=2026-01-16/part-2.parquet", 11),
            ("blocks/date=2025-12-31/part-3.parquet", 13),
            ("logs/date=2026-01-15/part-4.parquet", 17),
            ("logs/date=2026-02-15/notes.txt", 19),
            (".fireparq-ingest/state.parquet", 23),
        ];
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("evm");
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for (rel, size) in tree {
            write_test_file(&root.join(rel), &vec![b'x'; size]);
            let key = object_store::path::Path::from(format!("evm/{rel}"));
            block_on_async(store.put(&key, vec![b'x'; size].into())).unwrap();
        }
        for filters in [
            vec![],
            vec!["date=2026-*"],
            vec!["date=*-15"],
            vec!["date=2026-01-16"],
            vec!["date"],
            vec!["date=2026-01-15", "date=2025-12-31"],
            vec!["date=2026-0*"],
            vec!["date=2027-*"],
        ] {
            let mut local = config(&root, &filters, true, false);
            let local_result = run_truncate(&local).unwrap();
            local.path = "s3://bucket/evm".into();
            let parsed = PartitionFilters::parse(&local.partitions).unwrap();
            let remote_result = truncate_s3(&local, &parsed, &store, "bucket", "evm").unwrap();
            assert_eq!(
                (local_result.files_deleted, local_result.bytes_freed),
                (remote_result.files_deleted, remote_result.bytes_freed),
                "{filters:?}"
            );
        }
    }
}
