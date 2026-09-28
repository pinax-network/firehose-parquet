//! Dataset-level artifacts and where they live.
//!
//! A dataset root (the `--output` of `build`, with any `{chain}` expanded; see
//! [`crate::cli::resolve_output_root`]) holds only three kinds of entries:
//!
//! - table directories (`blocks/`, `transactions/`, ...),
//! - fireparq's artifact directory [`ARTIFACTS_DIR`] (`_fireparq/`) with the cursor mirror,
//! - dot-prefixed control state (`.fireparq-ingest/`, `.fireparq-owner-v1.json`,
//!   `.fireparq-owner-probes-v1/`).
//!
//! Engines that follow the Hadoop/Hive hidden-path convention (Spark, Trino, Hive, Athena,
//! Delta) skip paths starting with `_` or `.`, so a table location or a dataset-wide glob
//! never picks up fireparq's own files.
//!
//! Commands that walk a dataset tree use [`is_reserved_artifact_path`] to leave the whole
//! `_fireparq/` subtree, a `cursor.parquet` mirror and the control state alone.

use crate::cursor::CURSOR_PARQUET_FILENAME;

macro_rules! artifacts_dir {
    () => {
        "_fireparq"
    };
}

/// Directory at the dataset root that holds fireparq's artifacts.
pub const ARTIFACTS_DIR: &str = artifacts_dir!();

/// Default `--cursor` mirror, relative to the dataset root: `_fireparq/cursor.parquet`.
///
/// A literal so that clap can use it as `default_value`; a test checks that it is
/// [`CURSOR_PARQUET_FILENAME`] inside [`ARTIFACTS_DIR`].
pub const DEFAULT_CURSOR_MIRROR: &str = concat!(artifacts_dir!(), "/cursor.parquet");

/// Bucket-wide ownership record and isolated conditional-write canaries.
pub const OWNERSHIP_FILENAME: &str = ".fireparq-owner-v1.json";
pub const OWNERSHIP_PROBES_DIRECTORY: &str = ".fireparq-owner-probes-v1";

/// Internal recovery/ownership paths: never table data, and never a command's
/// input or output.
pub fn is_control_path(path: &str) -> bool {
    path.split('/').any(|component| {
        matches!(component, OWNERSHIP_FILENAME | OWNERSHIP_PROBES_DIRECTORY)
            || component == crate::durable_state::CONTROL_DIRECTORY
    })
}

/// Returns true when `rel_path` points at a reserved dataset artifact rather than table data:
/// anything under (or named) `_fireparq`, a `cursor.parquet` mirror (`--cursor cursor.parquet`
/// keeps it at the dataset root), or control state ([`is_control_path`]).
///
/// `rel_path` is a `/`-separated path (local path or S3 key) relative to the directory being
/// scanned, so ancestors of that directory do not affect the result.
pub fn is_reserved_artifact_path(rel_path: &str) -> bool {
    if is_control_path(rel_path) {
        return true;
    }
    let mut components = rel_path.split('/').filter(|part| !part.is_empty());
    let Some(file_name) = components.next_back() else {
        return false;
    };
    file_name == ARTIFACTS_DIR
        || file_name == CURSOR_PARQUET_FILENAME
        || components.any(|dir| dir == ARTIFACTS_DIR)
}

/// Whether a `/`-separated path (local path, S3 key or `s3://` URI) is the artifact directory
/// or lies inside it. Commands that take a dataset root refuse such a path.
pub fn is_in_artifacts_dir(path: &str) -> bool {
    path.split('/').any(|component| component == ARTIFACTS_DIR)
}

/// Whether a read-only directory walk (`scan`, `validate`) rooted at `walk_root` leaves out
/// `rel_path`. Such walks skip reserved artifacts, unless the walk root is itself inside the
/// reserved area (for example `scan <root>/_fireparq/`), where the operator asked for them.
pub fn read_walk_skips(walk_root: &str, rel_path: &str) -> bool {
    let walk_root = walk_root.trim_end_matches('/');
    !is_reserved_artifact_path(walk_root) && is_reserved_artifact_path(rel_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_cursor_mirror_is_in_the_artifact_directory() {
        assert_eq!(ARTIFACTS_DIR, "_fireparq");
        assert_eq!(
            DEFAULT_CURSOR_MIRROR,
            format!("{ARTIFACTS_DIR}/{CURSOR_PARQUET_FILENAME}")
        );
    }

    #[test]
    fn the_whole_artifact_directory_is_reserved() {
        for path in [
            "_fireparq",
            "_fireparq/cursor.parquet",
            // Anything an older release, a future release or an operator puts
            // there, such as the removed partition index, Merkle registry and
            // verify reports.
            "_fireparq/partitions.parquet",
            "_fireparq/merkle_roots.parquet",
            "_fireparq/verify_runs/run-1/report.json",
            "_fireparq/verify_runs/run-1/roots.parquet",
            "_fireparq/other.parquet",
            "_fireparq/nested/part-000001.parquet",
            // Below a chain directory or several datasets.
            "mainnet/_fireparq/cursor.parquet",
            "evm/mainnet/_fireparq/other.parquet",
        ] {
            assert!(is_reserved_artifact_path(path), "{path}");
        }
        // Similar names are table data.
        for path in [
            "_fireparq_data/part-000001.parquet",
            "fireparq/part-000001.parquet",
            "blocks/_fireparq_merge.json",
            "blocks/_fireparq.parquet",
        ] {
            assert!(!is_reserved_artifact_path(path), "{path}");
        }
    }

    #[test]
    fn a_cursor_mirror_is_reserved_wherever_it_is() {
        assert!(is_reserved_artifact_path("cursor.parquet"));
        assert!(is_reserved_artifact_path("mainnet/cursor.parquet"));
    }

    #[test]
    fn transaction_and_ownership_control_paths_are_reserved() {
        for path in [
            ".fireparq-ingest",
            "mainnet/.fireparq-ingest/state.json",
            "mainnet/.fireparq-ingest/hidden.parquet",
            ".fireparq-owner-v1.json",
            ".fireparq-owner-probes-v1/probe",
        ] {
            assert!(is_control_path(path), "{path}");
            assert!(is_reserved_artifact_path(path), "{path}");
        }
        assert!(!is_control_path(".fireparq-ingest-old/part-1.parquet"));
        assert!(!is_control_path("blocks/part-v1-transaction.parquet"));
        // The artifact directory is reserved but is not control state.
        assert!(!is_control_path("_fireparq/cursor.parquet"));
    }

    #[test]
    fn test_is_reserved_artifact_path_table_files() {
        assert!(!is_reserved_artifact_path(""));
        assert!(!is_reserved_artifact_path(
            "blocks/date=2024-01-15/part-abc12345-000001.parquet"
        ));
        assert!(!is_reserved_artifact_path("blocks/part-000001.parquet"));
        assert!(!is_reserved_artifact_path("my_cursor.parquet"));
        // The partition index (#653), the Merkle registry and the verify
        // reports (#643) are gone, so their old root names are not reserved.
        assert!(!is_reserved_artifact_path("partitions.parquet"));
        assert!(!is_reserved_artifact_path("merkle_roots.parquet"));
        assert!(!is_reserved_artifact_path(
            "verify_runs/run-1/roots.parquet"
        ));
        // Only the file name is matched, not a directory with a reserved name.
        assert!(!is_reserved_artifact_path(
            "cursor.parquet/part-000001.parquet"
        ));
    }

    #[test]
    fn paths_inside_the_artifact_directory_are_recognized() {
        for path in [
            "/data/mainnet/_fireparq",
            "/data/mainnet/_fireparq/",
            "s3://bucket/_fireparq",
            "s3://bucket/mainnet/_fireparq/nested",
            "_fireparq",
        ] {
            assert!(is_in_artifacts_dir(path), "{path}");
        }
        for path in [
            "/data/mainnet",
            "s3://bucket",
            "s3://bucket/_fireparq_old",
            "",
        ] {
            assert!(!is_in_artifacts_dir(path), "{path}");
        }
    }

    #[test]
    fn read_walks_skip_artifacts_unless_rooted_inside_them() {
        for root in ["/data/mainnet", "/data/mainnet/", "", "mainnet"] {
            assert!(read_walk_skips(root, "_fireparq/other.parquet"));
            assert!(read_walk_skips(root, "cursor.parquet"));
            assert!(read_walk_skips(root, ".fireparq-ingest/hidden.parquet"));
            assert!(!read_walk_skips(root, "blocks/part-1.parquet"));
        }
        for root in [
            "/data/mainnet/_fireparq",
            "/data/mainnet/_fireparq/",
            "mainnet/_fireparq",
            "_fireparq/nested",
        ] {
            assert!(!read_walk_skips(root, "other.parquet"), "{root}");
            assert!(!read_walk_skips(root, "cursor.parquet"), "{root}");
        }
    }
}
