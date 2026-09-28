//! Dataset-level artifacts and where they live.
//!
//! A dataset root (the `--output` of `build`, with any `{chain}` expanded; see
//! [`crate::cli::resolve_output_root`]) holds only three kinds of entries:
//!
//! - table directories (`blocks/`, `transactions/`, ...), each one Delta table with its
//!   data files in `date=YYYY-MM-DD/` and its log in [`DELTA_LOG_DIR`] (`_delta_log/`),
//! - fireparq's artifact directory [`ARTIFACTS_DIR`] (`_fireparq/`) with the cursor mirror,
//! - dot-prefixed control state (`.fireparq-ingest/`, `.fireparq-owner-v1.json`,
//!   `.fireparq-owner-probes-v1/`).
//!
//! Engines that follow the Hadoop/Hive hidden-path convention (Spark, Trino, Hive, Athena,
//! Delta) skip paths starting with `_` or `.`, so a table location or a dataset-wide glob
//! never picks up fireparq's own files.
//!
//! No command reads a table by walking its directory: `validate` reads a pinned Delta
//! snapshot and engines read the logs. The protected-root walker (`recovery`, `build`'s
//! overlap check) looks for control state only and skips every [`DELTA_LOG_DIR`].

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
/// [`crate::cursor::CURSOR_PARQUET_FILENAME`] inside [`ARTIFACTS_DIR`].
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

/// The log directory of every Delta table, `<table>/_delta_log/`: commits,
/// checkpoints (`*.checkpoint.parquet`) and `_last_checkpoint`. It never holds
/// table rows read by listing, nor fireparq control state.
pub const DELTA_LOG_DIR: &str = "_delta_log";

/// Whether a `/`-separated path (local path or S3 key) lies in a Delta log.
pub fn is_in_delta_log(path: &str) -> bool {
    path.split('/').any(|component| component == DELTA_LOG_DIR)
}

/// Whether a `/`-separated path (local path, S3 key or `s3://` URI) is the artifact directory
/// or lies inside it. Commands that take a dataset root refuse such a path.
pub fn is_in_artifacts_dir(path: &str) -> bool {
    path.split('/').any(|component| component == ARTIFACTS_DIR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursor::CURSOR_PARQUET_FILENAME;

    #[test]
    fn the_default_cursor_mirror_is_in_the_artifact_directory() {
        assert_eq!(ARTIFACTS_DIR, "_fireparq");
        assert_eq!(
            DEFAULT_CURSOR_MIRROR,
            format!("{ARTIFACTS_DIR}/{CURSOR_PARQUET_FILENAME}")
        );
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
        }
        assert!(!is_control_path(".fireparq-ingest-old/part-1.parquet"));
        assert!(!is_control_path("blocks/part-v1-transaction.parquet"));
        // The artifact directory is reserved but is not control state.
        assert!(!is_control_path("_fireparq/cursor.parquet"));
    }

    #[test]
    fn delta_logs_are_recognized_at_any_depth() {
        for path in [
            "blocks/_delta_log",
            "blocks/_delta_log/00000000000000000000.json",
            "blocks/_delta_log/00000000000000000100.checkpoint.parquet",
            "mainnet/logs/_delta_log/_last_checkpoint",
        ] {
            assert!(is_in_delta_log(path), "{path}");
        }
        for path in [
            "blocks",
            "blocks/date=2026-09-25/part-v1-a.parquet",
            "blocks/_delta_log_old/x.json",
            "_fireparq/cursor.parquet",
        ] {
            assert!(!is_in_delta_log(path), "{path}");
        }
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
}
