//! Truncate (delete) parquet files from local filesystem or S3,
//! with optional partition filtering.

use crate::cli::{block_on_async, format_bytes, resolve_parquet_input_path_string, AwsConfig};
use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Configuration for a truncate operation.
pub struct TruncateConfig {
    pub path: String,
    /// Partition filters (e.g. "date=2026-01-01"). Empty = delete all.
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
        // If filter is just a key name (e.g. "date"), expand to "date=*"
        let filter = if !raw_filter.contains('=') && !raw_filter.contains('*') {
            format!("{}=*", raw_filter)
        } else {
            raw_filter.clone()
        };
        if filter.contains('*') {
            // Glob matching: convert to a simple prefix/suffix match.
            let parts: Vec<&str> = filter.split('*').collect();
            if parts.len() == 2 {
                let prefix = parts[0];
                let suffix = parts[1];
                // Check if any path segment matches.
                for segment in path.split('/') {
                    if segment.starts_with(prefix) && segment.ends_with(suffix) {
                        return true;
                    }
                }
            }
        } else {
            // Exact match on a path segment.
            for segment in path.split('/') {
                if segment == filter.as_str() {
                    return true;
                }
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Local filesystem
// ---------------------------------------------------------------------------

fn run_truncate_local(config: &TruncateConfig) -> Result<TruncateResult> {
    let root = PathBuf::from(&config.path);
    if !root.exists() {
        anyhow::bail!("path does not exist: {}", root.display());
    }

    let mut all_files: Vec<PathBuf> = Vec::new();
    collect_parquet_files_recursive(&root, &mut all_files)?;
    all_files.sort();

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
        .filter(|f| {
            let rel = f.strip_prefix(&root).unwrap_or(f);
            matches_partition(&rel.to_string_lossy(), &config.partitions)
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
    let dirs_removed = if !config.dry_run {
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

fn collect_parquet_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files_recursive(&path, out)?;
        } else if path.extension().map_or(false, |ext| ext == "parquet") {
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
