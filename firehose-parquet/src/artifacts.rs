//! Dataset-level artifacts that live beside table directories.
//!
//! A network root holds table directories (`blocks/`, `transactions/`, ...) next to files
//! written by other commands. Commands that walk a whole dataset tree use
//! [`is_reserved_artifact_path`] to leave those files alone.

use crate::cursor::CURSOR_PARQUET_FILENAME;

/// Block-to-partition index written by `fireparq partitions build`.
pub const PARTITIONS_INDEX_FILENAME: &str = "partitions.parquet";

/// Merkle root registry written by `fireparq verify --update-registry`.
pub const MERKLE_ROOTS_FILENAME: &str = "merkle_roots.parquet";

/// Directory holding per-run verify reports (`verify_runs/<run_id>/report.json`).
pub const VERIFY_RUNS_DIR: &str = "verify_runs";

/// Bucket-wide ownership record and isolated conditional-write canaries.
pub const OWNERSHIP_FILENAME: &str = ".fireparq-owner-v1.json";
pub const OWNERSHIP_PROBES_DIRECTORY: &str = ".fireparq-owner-probes-v1";

/// Internal recovery/ownership paths are never ordinary dataset artifacts that
/// a table maintenance command may rewrite or delete.
pub fn is_control_path(path: &str) -> bool {
    path.split('/').any(|component| {
        matches!(component, OWNERSHIP_FILENAME | OWNERSHIP_PROBES_DIRECTORY)
            || component == crate::durable_state::CONTROL_DIRECTORY
    })
}

/// File names that are never table data, wherever they appear in a dataset tree.
pub const RESERVED_ARTIFACT_FILENAMES: [&str; 3] = [
    CURSOR_PARQUET_FILENAME,
    PARTITIONS_INDEX_FILENAME,
    MERKLE_ROOTS_FILENAME,
];

/// Returns true when `rel_path` points at a reserved dataset artifact rather than table data:
/// `cursor.parquet`, `partitions.parquet`, `merkle_roots.parquet`, or anything under a
/// `verify_runs/` directory.
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
    RESERVED_ARTIFACT_FILENAMES.contains(&file_name) || components.any(|dir| dir == VERIFY_RUNS_DIR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_reserved_artifact_path_root_files() {
        assert!(is_reserved_artifact_path("cursor.parquet"));
        assert!(is_reserved_artifact_path("partitions.parquet"));
        assert!(is_reserved_artifact_path("merkle_roots.parquet"));
        assert!(is_reserved_artifact_path("mainnet/cursor.parquet"));
        assert!(is_reserved_artifact_path(
            "evm/mainnet/merkle_roots.parquet"
        ));
    }

    #[test]
    fn test_is_reserved_artifact_path_verify_runs() {
        assert!(is_reserved_artifact_path("verify_runs/run-1/report.json"));
        assert!(is_reserved_artifact_path(
            "evm/mainnet/verify_runs/run-1/roots.parquet"
        ));
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
    }

    #[test]
    fn test_is_reserved_artifact_path_table_files() {
        assert!(!is_reserved_artifact_path(""));
        assert!(!is_reserved_artifact_path(
            "blocks/year=2024/month=01/date=15/part-abc12345-000001.parquet"
        ));
        assert!(!is_reserved_artifact_path("blocks/part-000001.parquet"));
        assert!(!is_reserved_artifact_path("my_cursor.parquet"));
        assert!(!is_reserved_artifact_path(
            "verify_runs_old/part-000001.parquet"
        ));
        // Only the file name is matched, not a directory with a reserved name.
        assert!(!is_reserved_artifact_path(
            "cursor.parquet/part-000001.parquet"
        ));
    }
}
