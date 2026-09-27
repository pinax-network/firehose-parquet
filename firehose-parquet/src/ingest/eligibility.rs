//! New protected streams may initialize only empty destinations. Existing
//! random-name data, cursor files and leftover artifacts are not evidence from
//! which an ingestion checkpoint can be made.

use anyhow::{bail, ensure, Context, Result};
use futures::StreamExt;
use std::path::{Path, PathBuf};

use super::binding::{output_path, validate_runtime_bindings};
use super::mirror::ProtectedMirror;
use super::state::{StorageIdentity, StreamDescriptor};
use crate::artifacts::{OWNERSHIP_FILENAME, OWNERSHIP_PROBES_DIRECTORY};
use crate::cli::AwsConfig;
use crate::dataset_lock::DatasetOwnership;

const MAX_INITIALIZATION_ENTRIES: usize = 100_000;

pub(crate) async fn require_initializable(
    descriptor: &StreamDescriptor,
    ownership: &DatasetOwnership,
    aws: &AwsConfig,
    mirror: &ProtectedMirror<'_>,
) -> Result<()> {
    let path = output_path(&descriptor.output);
    validate_runtime_bindings(descriptor, &path, aws)?;
    ownership.revalidate_local_paths()?;
    match &descriptor.output {
        StorageIdentity::Local { canonical_root } => {
            let owner = ownership.local().context("local output is not owned")?;
            let root = Path::new(canonical_root);
            ensure!(
                owner.roots().iter().any(|scope| root.starts_with(scope)),
                "initialization output is outside ownership"
            );
            local_contents(root)?;
        }
        StorageIdentity::S3 { bucket, prefix, .. } => {
            let owner = ownership
                .remote(bucket)
                .context("remote output is not owned")?;
            ensure!(
                !owner.is_mutation_uncertain(),
                "remote initialization requires resolved ownership"
            );
            remote_contents(owner.object_store().as_ref(), prefix).await?;
        }
    }
    // Includes an independently located mirror: even a parseable legacy cursor
    // cannot initialize authority or silently select a rewind point.
    mirror.require_absent_for_initialization().await?;
    ownership.revalidate_local_paths()?;
    Ok(())
}

fn reject_existing() -> anyhow::Error {
    anyhow::anyhow!("output contains legacy data, cursor, recovery controls or unrelated files; protected ingestion requires a new empty root or an existing authoritative stream")
}

/// Fails unless the local root holds nothing but (empty) directories.
fn local_contents(root: &Path) -> Result<()> {
    let mut directories = vec![PathBuf::from(root)];
    let mut visited = 0usize;
    while let Some(directory) = directories.pop() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && directory == root => {
                continue
            }
            Err(_) => bail!("cannot inspect protected initialization directory"),
        };
        for entry in entries {
            let entry = entry.context("inspecting initialization entry")?;
            visited += 1;
            ensure!(
                visited <= MAX_INITIALIZATION_ENTRIES,
                "initialization tree is too large to prove empty"
            );
            let kind = entry
                .file_type()
                .context("inspecting initialization entry type")?;
            let relative = entry.path().strip_prefix(root)?.to_path_buf();
            // Control directories with no authority are not silently claimed,
            // even when empty: an operator must reconcile a failed initialization.
            ensure!(!crate::artifacts::is_control_path(relative.to_str().context("initialization entry must be UTF-8")?), "uninitialized output contains recovery controls; inspect them before choosing a new root");
            if kind.is_dir() {
                directories.push(entry.path());
            } else {
                return Err(reject_existing());
            }
        }
    }
    Ok(())
}

/// Fails unless the S3 dataset prefix is empty. At a bucket root the
/// bucket-wide ownership record and its probes may exist.
async fn remote_contents(store: &dyn object_store::ObjectStore, prefix: &str) -> Result<()> {
    let object_prefix =
        (!prefix.is_empty()).then(|| object_store::path::Path::from(format!("{prefix}/")));
    let list = async {
        let mut entries = store.list(object_prefix.as_ref());
        let mut visited = 0usize;
        while let Some(entry) = entries.next().await {
            let entry = entry
                .map_err(|_| anyhow::anyhow!("cannot list protected initialization objects"))?;
            visited += 1;
            ensure!(
                visited <= MAX_INITIALIZATION_ENTRIES,
                "initialization prefix is too large to prove empty"
            );
            let key = entry.location.as_ref();
            let relative = if prefix.is_empty() {
                key
            } else {
                key.strip_prefix(&format!("{prefix}/"))
                    .context("initialization listing escaped its prefix")?
            };
            // Bucket ownership is deliberately outside a dataset. At bucket
            // root these exact protocol keys may coexist with an empty dataset.
            if prefix.is_empty()
                && (relative == OWNERSHIP_FILENAME
                    || relative.starts_with(&format!("{OWNERSHIP_PROBES_DIRECTORY}/")))
            {
                continue;
            }
            return Err(reject_existing());
        }
        Ok(())
    };
    tokio::time::timeout(std::time::Duration::from_secs(60), list)
        .await
        .map_err(|_| anyhow::anyhow!("protected initialization listing timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::binding::{mirror_service, resolve_output_identity};
    use crate::ingest::state::{tests::descriptor, MirrorBinding, RoutingPolicy};

    fn aws() -> AwsConfig {
        AwsConfig {
            aws_access_key_id: None,
            aws_secret_access_key: None,
            aws_session_token: None,
            aws_region: None,
            aws_endpoint_url: None,
        }
    }

    /// Only an empty root initializes. A partition index left by a release
    /// before #653, in `_fireparq/` or at the root, is an unrelated file like
    /// any other: it is refused, never adopted.
    #[tokio::test]
    async fn only_an_empty_root_is_eligible_and_a_leftover_partition_index_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = std::fs::canonicalize(dir.path()).unwrap().join("chain");
        let owner = DatasetOwnership::acquire(
            "test",
            vec![crate::dataset_lock::MutationScope::directory(
                path.to_string_lossy(),
            )],
            None,
        )
        .await
        .unwrap();
        let mut descriptor = descriptor(RoutingPolicy::GenesisLookaheadV1);
        descriptor.output = resolve_output_identity(path.to_str().unwrap(), &aws()).unwrap();
        let mirror = ProtectedMirror::new(&owner, &MirrorBinding::Disabled, None).unwrap();
        require_initializable(&descriptor, &owner, &aws(), &mirror)
            .await
            .unwrap();
        for leftover in ["_fireparq/partitions.parquet", "partitions.parquet"] {
            let file = path.join(leftover);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(&file, b"index").unwrap();
            let error = require_initializable(&descriptor, &owner, &aws(), &mirror)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("unrelated files"), "{leftover}: {error}");
            std::fs::remove_file(&file).unwrap();
        }
        // An empty `_fireparq/` directory is still accepted.
        assert!(path.join("_fireparq").is_dir());
        require_initializable(&descriptor, &owner, &aws(), &mirror)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn legacy_random_data_cursor_controls_and_external_cursor_are_not_adopted() {
        for existing in [
            "blocks/old-random.parquet",
            "cursor.parquet",
            "_fireparq/cursor.parquet",
            "_fireparq/merkle_roots.parquet",
            "_fireparq/partitions.parquet",
            "_fireparq/other.parquet",
            ".fireparq-ingest/.state.json.unfinished.tmp",
            "partitions.parquet",
            ".fireparq-owner-v1.json",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(existing);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"existing").unwrap();
            assert!(local_contents(dir.path()).is_err(), "{existing}");
        }
        // Empty directories, such as an empty `_fireparq/`, are accepted.
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("_fireparq")).unwrap();
        local_contents(dir.path()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("output");
        let cursor = dir.path().join("external/cursor.parquet");
        let owner = DatasetOwnership::acquire(
            "test",
            vec![
                crate::dataset_lock::MutationScope::directory(root.to_string_lossy()),
                crate::dataset_lock::MutationScope::file(cursor.to_string_lossy()),
            ],
            None,
        )
        .await
        .unwrap();
        let mut descriptor = descriptor(RoutingPolicy::GenesisLookaheadV1);
        descriptor.output = resolve_output_identity(root.to_str().unwrap(), &aws()).unwrap();
        descriptor.mirror = MirrorBinding::Local {
            absolute_path: cursor.to_str().unwrap().into(),
        };
        std::fs::create_dir_all(cursor.parent().unwrap()).unwrap();
        std::fs::write(&cursor, b"legacy private cursor").unwrap();
        let mirror = ProtectedMirror::new(
            &owner,
            &descriptor.mirror,
            mirror_service(&descriptor.mirror, &aws()).unwrap().as_ref(),
        )
        .unwrap();
        assert!(require_initializable(&descriptor, &owner, &aws(), &mirror)
            .await
            .is_err());
        assert!(!root.join(".fireparq-ingest").exists());
        assert_eq!(std::fs::read(cursor).unwrap(), b"legacy private cursor");
    }

    #[tokio::test]
    async fn remote_listing_never_ignores_arbitrary_control_or_legacy_objects() {
        let store = object_store::memory::InMemory::new();
        use object_store::ObjectStore;
        remote_contents(&store, "chain").await.unwrap();
        store
            .put(
                &"other/legacy.parquet".into(),
                bytes::Bytes::from_static(b"legacy").into(),
            )
            .await
            .unwrap();
        remote_contents(&store, "chain").await.unwrap();
        store
            .put(
                &"chain/.fireparq-ingest/state.json".into(),
                bytes::Bytes::from_static(b"orphan").into(),
            )
            .await
            .unwrap();
        assert!(remote_contents(&store, "chain").await.is_err());
        let store = object_store::memory::InMemory::new();
        store
            .put(
                &OWNERSHIP_FILENAME.into(),
                bytes::Bytes::from_static(b"owner").into(),
            )
            .await
            .unwrap();
        remote_contents(&store, "").await.unwrap();
        store
            .put(
                &"cursor.parquet".into(),
                bytes::Bytes::from_static(b"legacy").into(),
            )
            .await
            .unwrap();
        assert!(remote_contents(&store, "").await.is_err());
    }

    /// A bucket root that holds only the bucket-wide owner record stays
    /// eligible. A leftover partition index (in `_fireparq/` or at the bucket
    /// root) or any other object is not.
    #[tokio::test(flavor = "current_thread")]
    async fn bucket_root_with_only_the_owner_record_is_eligible() {
        use object_store::ObjectStore;
        let store = std::sync::Arc::new(object_store::memory::InMemory::new());
        let remote = crate::dataset_lock_s3::S3Ownership::acquire(
            store.clone(),
            "build",
            vec![String::new()],
        )
        .await
        .unwrap();
        let owner = DatasetOwnership::from_remote_for_test("data", remote);
        assert!(store
            .head(&object_store::path::Path::from(OWNERSHIP_FILENAME))
            .await
            .is_ok());
        let mut descriptor = descriptor(RoutingPolicy::GenesisLookaheadV1);
        descriptor.output = resolve_output_identity("s3://data", &aws()).unwrap();
        let mirror = ProtectedMirror::new(&owner, &MirrorBinding::Disabled, None).unwrap();
        require_initializable(&descriptor, &owner, &aws(), &mirror)
            .await
            .unwrap();
        for key in [
            "_fireparq/partitions.parquet",
            "partitions.parquet",
            "mainnet/blocks/part-1.parquet",
        ] {
            let key = object_store::path::Path::from(key);
            store
                .put(&key, bytes::Bytes::from_static(b"leftover").into())
                .await
                .unwrap();
            let error = require_initializable(&descriptor, &owner, &aws(), &mirror)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("unrelated files"), "{key}: {error}");
            store.delete(&key).await.unwrap();
        }
        require_initializable(&descriptor, &owner, &aws(), &mirror)
            .await
            .unwrap();
    }
}
