//! Dataset-level artifacts and where they live.
//!
//! A dataset root (`<output>/<chain_name>`, or `<output>` itself with
//! `--without-chain-dir`) holds only three kinds of entries:
//!
//! - table directories (`blocks/`, `transactions/`, ...),
//! - fireparq's artifact directory [`ARTIFACTS_DIR`] (`_fireparq/`) with the cursor mirror,
//!   the partition index, the merkle roots registry and the verify reports,
//! - dot-prefixed control state (`.fireparq-ingest/`, `.fireparq-owner-v1.json`,
//!   `.fireparq-owner-probes-v1/`).
//!
//! Engines that follow the Hadoop/Hive hidden-path convention (Spark, Trino, Hive, Athena,
//! Delta) skip paths starting with `_` or `.`, so a table location or a dataset-wide glob
//! never picks up fireparq's own files.
//!
//! Every writer and reader resolves an artifact through [`DatasetArtifact`], for local and
//! S3 roots alike. Commands that walk a dataset tree use [`is_reserved_artifact_path`] to
//! leave the whole `_fireparq/` subtree, the legacy root names and the control state alone.

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
/// A literal so that clap can use it as `default_value`; a test checks that it equals
/// [`DatasetArtifact::CursorMirror`]'s relative path.
pub const DEFAULT_CURSOR_MIRROR: &str = concat!(artifacts_dir!(), "/cursor.parquet");

/// Block-to-partition index written by `fireparq partitions build`.
pub const PARTITIONS_INDEX_FILENAME: &str = "partitions.parquet";

/// Merkle root registry written by `fireparq verify`.
pub const MERKLE_ROOTS_FILENAME: &str = "merkle_roots.parquet";

/// Directory holding per-run verify reports (`verify_runs/<run_id>/report.json`).
pub const VERIFY_RUNS_DIR: &str = "verify_runs";

/// Bucket-wide ownership record and isolated conditional-write canaries.
pub const OWNERSHIP_FILENAME: &str = ".fireparq-owner-v1.json";
pub const OWNERSHIP_PROBES_DIRECTORY: &str = ".fireparq-owner-probes-v1";

/// An artifact that fireparq keeps in `<dataset root>/_fireparq/`.
///
/// Releases before v1.0.0 wrote each of them directly at the dataset root under the same
/// name ([`DatasetArtifact::legacy_path_in`]). Those root names stay reserved, and commands
/// that would create the new artifact refuse to run beside a legacy one instead of silently
/// shadowing it ([`legacy_artifact_refusal`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatasetArtifact {
    /// The optional, non-authoritative `--cursor` mirror of `build` (default location only).
    CursorMirror,
    /// The partition index written by `partitions build`.
    PartitionsIndex,
    /// The merkle roots registry written by `verify`.
    MerkleRoots,
    /// The directory of per-run verify reports.
    VerifyRuns,
}

impl DatasetArtifact {
    pub const ALL: [Self; 4] = [
        Self::CursorMirror,
        Self::PartitionsIndex,
        Self::MerkleRoots,
        Self::VerifyRuns,
    ];

    /// File or directory name, inside `_fireparq/` and (legacy) at the dataset root.
    pub const fn name(self) -> &'static str {
        match self {
            Self::CursorMirror => CURSOR_PARQUET_FILENAME,
            Self::PartitionsIndex => PARTITIONS_INDEX_FILENAME,
            Self::MerkleRoots => MERKLE_ROOTS_FILENAME,
            Self::VerifyRuns => VERIFY_RUNS_DIR,
        }
    }

    /// What the artifact is, for messages.
    pub const fn description(self) -> &'static str {
        match self {
            Self::CursorMirror => "cursor mirror",
            Self::PartitionsIndex => "partition index",
            Self::MerkleRoots => "merkle roots registry",
            Self::VerifyRuns => "verify reports directory",
        }
    }

    /// Path relative to the dataset root: `_fireparq/<name>`.
    pub fn relative_path(self) -> String {
        format!("{ARTIFACTS_DIR}/{}", self.name())
    }

    /// `<dataset_root>/_fireparq/<name>`, for a local path or an `s3://bucket[/prefix]` URI.
    pub fn path_in(self, dataset_root: &str) -> String {
        join_dataset_path(dataset_root, &self.relative_path())
    }

    /// Where releases before v1.0.0 kept the artifact: `<dataset_root>/<name>`.
    pub fn legacy_path_in(self, dataset_root: &str) -> String {
        join_dataset_path(dataset_root, self.name())
    }

    /// Object key of the artifact under an S3 dataset prefix (empty at a bucket root).
    pub fn key_in(self, prefix: &str) -> String {
        join_dataset_key(prefix, &self.relative_path())
    }

    /// Object key of the legacy root artifact under an S3 dataset prefix.
    pub fn legacy_key_in(self, prefix: &str) -> String {
        join_dataset_key(prefix, self.name())
    }
}

/// Joins a `/`-separated relative path onto an S3 dataset prefix, which is empty at a bucket
/// root.
pub fn join_dataset_key(prefix: &str, relative: &str) -> String {
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        relative.to_string()
    } else {
        format!("{prefix}/{relative}")
    }
}

/// Joins a `/`-separated relative path onto a dataset root: a local path (joined natively)
/// or an `s3://bucket[/prefix]` URI (trailing `/` ignored, so `s3://bucket/` is the bucket root).
pub fn join_dataset_path(dataset_root: &str, relative: &str) -> String {
    if dataset_root.starts_with("s3://") {
        format!("{}/{relative}", dataset_root.trim_end_matches('/'))
    } else {
        std::path::Path::new(dataset_root)
            .join(relative)
            .to_string_lossy()
            .into_owned()
    }
}

/// The refusal for a legacy root artifact found where a command would create or use the
/// `_fireparq/` one. Nothing is migrated automatically.
pub fn legacy_artifact_refusal(
    artifact: DatasetArtifact,
    legacy_path: &str,
    new_path: &str,
) -> anyhow::Error {
    anyhow::anyhow!(
        "found a legacy {} at {legacy_path}: fireparq now keeps it in the {ARTIFACTS_DIR}/ directory of the dataset, at {new_path}, and does not migrate it automatically. Move it there (for example with `mv` or `aws s3 mv`) and rerun; nothing was read or written",
        artifact.description()
    )
}

/// Internal recovery/ownership paths are never ordinary dataset artifacts that
/// a table maintenance command may rewrite or delete.
pub fn is_control_path(path: &str) -> bool {
    path.split('/').any(|component| {
        matches!(component, OWNERSHIP_FILENAME | OWNERSHIP_PROBES_DIRECTORY)
            || component == crate::durable_state::CONTROL_DIRECTORY
    })
}

/// File names that are never table data, wherever they appear in a dataset tree: inside
/// `_fireparq/`, and at the dataset root where releases before v1.0.0 wrote them.
pub const RESERVED_ARTIFACT_FILENAMES: [&str; 3] = [
    CURSOR_PARQUET_FILENAME,
    PARTITIONS_INDEX_FILENAME,
    MERKLE_ROOTS_FILENAME,
];

/// Returns true when `rel_path` points at a reserved dataset artifact rather than table data:
/// anything under (or named) `_fireparq`, a legacy `cursor.parquet`, `partitions.parquet` or
/// `merkle_roots.parquet`, anything under a `verify_runs/` directory, or control state
/// ([`is_control_path`]).
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
        || RESERVED_ARTIFACT_FILENAMES.contains(&file_name)
        || components.any(|dir| dir == ARTIFACTS_DIR || dir == VERIFY_RUNS_DIR)
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
    fn artifact_paths_resolve_under_the_artifact_directory() {
        assert_eq!(ARTIFACTS_DIR, "_fireparq");
        assert_eq!(
            DEFAULT_CURSOR_MIRROR,
            DatasetArtifact::CursorMirror.relative_path()
        );
        let expected = [
            ("_fireparq/cursor.parquet", "cursor.parquet"),
            ("_fireparq/partitions.parquet", "partitions.parquet"),
            ("_fireparq/merkle_roots.parquet", "merkle_roots.parquet"),
            ("_fireparq/verify_runs", "verify_runs"),
        ];
        for (artifact, (relative, legacy)) in DatasetArtifact::ALL.into_iter().zip(expected) {
            assert_eq!(artifact.relative_path(), relative);
            assert_eq!(artifact.name(), legacy);
            // Local, S3 prefix, S3 bucket root (with and without a trailing slash).
            for (root, joined, legacy_joined) in [
                (
                    "./output/mainnet",
                    format!("./output/mainnet/{relative}"),
                    format!("./output/mainnet/{legacy}"),
                ),
                (
                    "/data/mainnet/",
                    format!("/data/mainnet/{relative}"),
                    format!("/data/mainnet/{legacy}"),
                ),
                ("/", format!("/{relative}"), format!("/{legacy}")),
                (
                    "s3://bucket/data/mainnet",
                    format!("s3://bucket/data/mainnet/{relative}"),
                    format!("s3://bucket/data/mainnet/{legacy}"),
                ),
                (
                    "s3://ethereum-mainnet",
                    format!("s3://ethereum-mainnet/{relative}"),
                    format!("s3://ethereum-mainnet/{legacy}"),
                ),
                (
                    "s3://ethereum-mainnet/",
                    format!("s3://ethereum-mainnet/{relative}"),
                    format!("s3://ethereum-mainnet/{legacy}"),
                ),
            ] {
                assert_eq!(artifact.path_in(root), joined, "{root}");
                assert_eq!(artifact.legacy_path_in(root), legacy_joined, "{root}");
            }
            for (prefix, key, legacy_key) in [
                ("", relative.to_string(), legacy.to_string()),
                (
                    "data/mainnet",
                    format!("data/mainnet/{relative}"),
                    format!("data/mainnet/{legacy}"),
                ),
                (
                    "mainnet/",
                    format!("mainnet/{relative}"),
                    format!("mainnet/{legacy}"),
                ),
            ] {
                assert_eq!(artifact.key_in(prefix), key, "{prefix:?}");
                assert_eq!(artifact.legacy_key_in(prefix), legacy_key, "{prefix:?}");
            }
        }
    }

    #[test]
    fn the_whole_artifact_directory_is_reserved() {
        for path in [
            "_fireparq",
            "_fireparq/cursor.parquet",
            "_fireparq/partitions.parquet",
            "_fireparq/merkle_roots.parquet",
            "_fireparq/verify_runs/run-1/report.json",
            "_fireparq/verify_runs/run-1/roots.parquet",
            // Anything a future release or an operator puts there.
            "_fireparq/other.parquet",
            "_fireparq/nested/part-000001.parquet",
            // Below a chain directory or several datasets.
            "mainnet/_fireparq/partitions.parquet",
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
    fn test_is_reserved_artifact_path_root_files() {
        // The legacy root names (releases before v1.0.0) stay reserved.
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
        // The artifact directory is reserved but is not control state.
        assert!(!is_control_path("_fireparq/cursor.parquet"));
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

    #[test]
    fn paths_inside_the_artifact_directory_are_recognized() {
        for path in [
            "/data/mainnet/_fireparq",
            "/data/mainnet/_fireparq/",
            "s3://bucket/_fireparq",
            "s3://bucket/mainnet/_fireparq/verify_runs",
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
            assert!(read_walk_skips(root, "_fireparq/partitions.parquet"));
            assert!(read_walk_skips(root, "cursor.parquet"));
            assert!(read_walk_skips(root, ".fireparq-ingest/hidden.parquet"));
            assert!(!read_walk_skips(root, "blocks/part-1.parquet"));
        }
        for root in [
            "/data/mainnet/_fireparq",
            "/data/mainnet/_fireparq/",
            "mainnet/_fireparq",
            "_fireparq/verify_runs",
        ] {
            assert!(!read_walk_skips(root, "partitions.parquet"), "{root}");
            assert!(!read_walk_skips(root, "cursor.parquet"), "{root}");
        }
    }

    #[test]
    fn legacy_refusal_names_both_locations_and_the_move() {
        let message = legacy_artifact_refusal(
            DatasetArtifact::MerkleRoots,
            "s3://bucket/merkle_roots.parquet",
            "s3://bucket/_fireparq/merkle_roots.parquet",
        )
        .to_string();
        for expected in [
            "legacy merkle roots registry",
            "s3://bucket/merkle_roots.parquet",
            "s3://bucket/_fireparq/merkle_roots.parquet",
            "Move it there",
            "_fireparq/",
            "does not migrate it automatically",
        ] {
            assert!(message.contains(expected), "{expected}: {message}");
        }
    }
}
