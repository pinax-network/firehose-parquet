//! Truncate (delete) parquet files from local filesystem or S3,
//! with optional partition filtering.

use crate::cli::{block_on_async, format_bytes, resolve_parquet_input_path_string, AwsConfig};
use crate::config::{DAY_PARTITION_PREFIX, LEGACY_DAY_PARTITION_PREFIX};
use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Configuration for a truncate operation.
pub struct TruncateConfig {
    pub path: String,
    /// Partition filters (e.g. "day=01" or "month=01"). Empty = delete all.
    pub partitions: Vec<String>,
    pub dry_run: bool,
    pub aws: Option<AwsConfig>,
}

/// Summary of a truncate operation.
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
    let resolved = TruncateConfig {
        path: resolve_parquet_input_path_string(&config.path),
        partitions: config.partitions.clone(),
        dry_run: config.dry_run,
        aws: config.aws.clone(),
    };

    if resolved.path.starts_with("s3://") {
        run_truncate_s3(&resolved)
    } else {
        run_truncate_local(&resolved)
    }
}

/// Check if a file path matches any of the partition filters.
/// If no filters, everything matches.
fn matches_partition(path: &str, filters: &[String]) -> bool {
    if filters.is_empty() {
        return true;
    }
    for raw_filter in filters {
        // If filter is just a key name (e.g. "day"), expand to "day=*"
        let filter = if !raw_filter.contains('=') && !raw_filter.contains('*') {
            format!("{}=*", raw_filter)
        } else {
            raw_filter.clone()
        };
        // `day=` and the legacy `date=` day key are aliases, so one filter matches
        // both the current and the legacy layout.
        let filter_forms = [filter.as_str(), &canonical_day_key(&filter)];
        for segment in path.split('/') {
            let segment_forms = [segment, &canonical_day_key(segment)];
            if filter_forms.iter().any(|filter| {
                segment_forms
                    .iter()
                    .any(|segment| segment_matches(filter, segment))
            }) {
                return true;
            }
        }
    }
    false
}

/// Match one path segment against a filter: exact, or a single `*` glob.
fn segment_matches(filter: &str, segment: &str) -> bool {
    if filter.contains('*') {
        // Glob matching: convert to a simple prefix/suffix match.
        let parts: Vec<&str> = filter.split('*').collect();
        parts.len() == 2 && segment.starts_with(parts[0]) && segment.ends_with(parts[1])
    } else {
        segment == filter
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
// Local filesystem
// ---------------------------------------------------------------------------

fn run_truncate_local(config: &TruncateConfig) -> Result<TruncateResult> {
    let root = PathBuf::from(&config.path);
    if !root.exists() {
        anyhow::bail!("path does not exist: {}", root.display());
    }

    let all_files = collect_local_parquet_targets(&root)?;

    if all_files.is_empty() {
        println!("No .parquet files found in {}", root.display());
        return Ok(TruncateResult {
            files_deleted: 0,
            bytes_freed: 0,
            dirs_removed: 0,
        });
    }

    // Filter by partition.
    let matching: Vec<&PathBuf> = all_files
        .iter()
        .filter(|f| matches_partition(&local_match_path(f, &root), &config.partitions))
        .collect();

    if matching.is_empty() {
        println!("No files match the partition filter(s)");
        return Ok(TruncateResult {
            files_deleted: 0,
            bytes_freed: 0,
            dirs_removed: 0,
        });
    }

    println!("Truncating {} ...\n", root.display());
    if !config.partitions.is_empty() {
        println!("  Partition filter(s): {}", config.partitions.join(", "));
    }

    let mut bytes_freed = 0u64;
    let mut files_deleted = 0usize;

    for file in &matching {
        let size = std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
        if config.dry_run {
            println!(
                "  would delete: {} ({})",
                file.display(),
                format_bytes(size)
            );
        } else {
            std::fs::remove_file(file).with_context(|| format!("deleting {}", file.display()))?;
        }
        bytes_freed += size;
        files_deleted += 1;
    }

    // Clean up empty directories.
    let dirs_removed = if !config.dry_run && root.is_dir() {
        cleanup_empty_dirs(&root)?
    } else {
        0
    };

    Ok(TruncateResult {
        files_deleted,
        bytes_freed,
        dirs_removed,
    })
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

fn run_truncate_s3(config: &TruncateConfig) -> Result<TruncateResult> {
    use crate::writer::parse_s3_url;
    use futures::TryStreamExt;

    let (bucket, prefix) = parse_s3_url(&config.path)?;
    let aws = config
        .aws
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?;

    let client = Arc::new(aws.build_s3_client(&bucket)?);

    let list_prefix = if prefix.is_empty() {
        None
    } else {
        Some(object_store::path::Path::from(prefix.as_str()))
    };

    let objects: Vec<object_store::ObjectMeta> =
        block_on_async(async { client.list(list_prefix.as_ref()).try_collect().await })
            .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let parquet_objects: Vec<_> = objects
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .collect();

    if parquet_objects.is_empty() {
        println!("No .parquet files found in {}", config.path);
        return Ok(TruncateResult {
            files_deleted: 0,
            bytes_freed: 0,
            dirs_removed: 0,
        });
    }

    // Filter by partition.
    let matching: Vec<&object_store::ObjectMeta> = parquet_objects
        .iter()
        .filter(|obj| {
            let rel = obj
                .location
                .as_ref()
                .strip_prefix(&prefix)
                .map(|s| s.trim_start_matches('/'))
                .unwrap_or(obj.location.as_ref());
            matches_partition(rel, &config.partitions)
        })
        .collect();

    if matching.is_empty() {
        println!("No files match the partition filter(s)");
        return Ok(TruncateResult {
            files_deleted: 0,
            bytes_freed: 0,
            dirs_removed: 0,
        });
    }

    println!("Truncating {} ...\n", config.path);
    if !config.partitions.is_empty() {
        println!("  Partition filter(s): {}", config.partitions.join(", "));
    }

    let mut bytes_freed = 0u64;
    let mut files_deleted = 0usize;

    for obj in &matching {
        if config.dry_run {
            println!(
                "  would delete: s3://{}/{} ({})",
                bucket,
                obj.location,
                format_bytes(obj.size as u64)
            );
        } else {
            block_on_async(async { client.delete(&obj.location).await })
                .map_err(|e| anyhow::anyhow!("deleting s3://{bucket}/{}: {e}", obj.location))?;
        }
        bytes_freed += obj.size as u64;
        files_deleted += 1;
    }

    Ok(TruncateResult {
        files_deleted,
        bytes_freed,
        dirs_removed: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_test_file(path: &Path, contents: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, contents).unwrap();
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

        let result = run_truncate_local(&TruncateConfig {
            path: root.to_string_lossy().into_owned(),
            partitions: vec![],
            dry_run: true,
            aws: None,
        })
        .unwrap();

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

        let result = run_truncate_local(&TruncateConfig {
            path: partitions.to_string_lossy().into_owned(),
            partitions: vec![],
            dry_run: false,
            aws: None,
        })
        .unwrap();

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
        let filters = |filters: &[&str]| -> Vec<String> {
            filters.iter().map(|filter| filter.to_string()).collect()
        };

        for filter in ["day=15", "date=15", "day=1*", "date=1*", "day", "date"] {
            let filter = filters(&[filter]);
            assert!(matches_partition(current, &filter), "{filter:?} on day=");
            assert!(matches_partition(legacy, &filter), "{filter:?} on date=");
        }
        assert!(!matches_partition(other_day, &filters(&["day=15"])));
        assert!(!matches_partition(other_day, &filters(&["date=15"])));

        // Globs spelled against the raw legacy key keep matching legacy trees.
        assert!(matches_partition(legacy, &filters(&["date*"])));
        // Other keys are unaffected.
        assert!(matches_partition(current, &filters(&["month=01"])));
        assert!(!matches_partition(current, &filters(&["hour"])));
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

        let result = run_truncate_local(&TruncateConfig {
            path: root.to_string_lossy().into_owned(),
            partitions: vec!["day=15".to_string()],
            dry_run: false,
            aws: None,
        })
        .unwrap();

        assert_eq!(result.files_deleted, 2);
        assert!(!current.exists());
        assert!(!legacy.exists());
        assert!(kept.exists());
    }
}
