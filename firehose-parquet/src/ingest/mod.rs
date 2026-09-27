//! Authoritative ingestion transactions and accepted stream progress.

pub(crate) mod binding;
pub(crate) mod controller;
pub(crate) mod eligibility;
pub(crate) mod frontier;
pub(crate) mod maintenance;
pub(crate) mod mirror;
pub(crate) mod observe;
pub(crate) mod parts;
pub(crate) mod session;
pub(crate) mod state;
pub(crate) mod store;

pub use controller::{CommittedFlush, FlushWorkStats};
pub use session::{
    declare_inventory, ingestion_mutation_scopes, load_authoritative_resume, IngestionSession,
    MapperSemantics, CURSOR_OVERRIDE_REFUSED, PRE_V1_DEFAULT_MIRROR,
};
pub use state::{BlockFamily, Digest};

use crate::artifacts::{legacy_artifact_refusal, DatasetArtifact};
use crate::dataset_lock::DatasetOwnership;
use anyhow::Context;

/// Recover any protected dataset before publishing its standalone index.
///
/// `directory` is the dataset root and `index` its `_fireparq/partitions.parquet`.
/// A legacy index at `<directory>/partitions.parquet` (releases before v1.0.0)
/// is refused before anything is read or written, and the ownership taken for
/// the check is released: a new index beside it would silently shadow it.
pub async fn prepare_partitions_index_write(
    directory: &str,
    index: &str,
    aws: &crate::cli::AwsConfig,
) -> anyhow::Result<DatasetOwnership> {
    let prepared = maintenance::acquire(
        "partitions-build",
        vec![
            maintenance::MaintenanceTarget::directory(directory),
            maintenance::MaintenanceTarget::file(index),
        ],
        maintenance::MaintenancePolicy::Artifacts,
        Some(aws),
    )
    .await?;
    let ownership = prepared.ownership;
    match refuse_legacy_partitions_index(&ownership, directory).await {
        Ok(()) => Ok(ownership),
        // Only reads were sent, so every owner is released.
        Err(error) => ownership.finish(Err(error)).await,
    }
}

/// Fails when the dataset root holds a legacy root `partitions.parquet`, read
/// through the held ownership (the owned store for S3).
pub(crate) async fn refuse_legacy_partitions_index(
    ownership: &DatasetOwnership,
    dataset_root: &str,
) -> anyhow::Result<()> {
    let artifact = DatasetArtifact::PartitionsIndex;
    let legacy = artifact.legacy_path_in(dataset_root);
    let present = if dataset_root.starts_with("s3://") {
        let (bucket, prefix) = crate::writer::parse_s3_url(dataset_root)?;
        let owner = ownership
            .remote(&bucket)
            .context("partition index bucket is not owned")?;
        let key = object_store::path::Path::from(artifact.legacy_key_in(&prefix));
        let head = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            owner.object_store().head(&key),
        )
        .await
        .map_err(|_| anyhow::anyhow!("checking for a legacy partition index timed out"))?;
        match head {
            Ok(_) => true,
            Err(object_store::Error::NotFound { .. }) => false,
            Err(error) => return Err(anyhow::anyhow!("checking for {legacy}: {error}")),
        }
    } else {
        match std::fs::symlink_metadata(&legacy) {
            Ok(_) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(anyhow::Error::new(error).context(format!("checking for {legacy}")))
            }
        }
    };
    if present {
        return Err(legacy_artifact_refusal(
            artifact,
            &legacy,
            &artifact.path_in(dataset_root),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStore;
    use std::sync::Arc;

    /// A legacy root index is refused locally and on S3, below a chain
    /// directory and at a bucket root; the `_fireparq/` index alone passes.
    #[tokio::test]
    async fn a_legacy_root_partition_index_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mainnet");
        std::fs::create_dir_all(root.join("_fireparq")).unwrap();
        let root_text = root.to_str().unwrap();
        let ownership = DatasetOwnership::acquire(
            "test",
            vec![crate::dataset_lock::MutationScope::directory(root_text)],
            None,
        )
        .await
        .unwrap();
        std::fs::write(root.join("_fireparq/partitions.parquet"), b"index").unwrap();
        refuse_legacy_partitions_index(&ownership, root_text)
            .await
            .unwrap();
        std::fs::write(root.join("partitions.parquet"), b"legacy").unwrap();
        let error = refuse_legacy_partitions_index(&ownership, root_text)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("legacy partition index")
                && error.contains(&format!("{root_text}/partitions.parquet"))
                && error.contains(&format!("{root_text}/_fireparq/partitions.parquet")),
            "{error}"
        );
        drop(ownership);

        for (scope, root, legacy_key) in [
            ("mainnet", "s3://data/mainnet", "mainnet/partitions.parquet"),
            ("", "s3://data", "partitions.parquet"),
        ] {
            let store = Arc::new(object_store::memory::InMemory::new());
            let remote = crate::dataset_lock_s3::S3Ownership::acquire(
                store.clone(),
                "partitions-build",
                vec![scope.to_string()],
            )
            .await
            .unwrap();
            let ownership = DatasetOwnership::from_remote_for_test("data", remote);
            let new_key = crate::artifacts::DatasetArtifact::PartitionsIndex.key_in(scope);
            store
                .put(
                    &new_key.as_str().into(),
                    bytes::Bytes::from_static(b"index").into(),
                )
                .await
                .unwrap();
            refuse_legacy_partitions_index(&ownership, root)
                .await
                .unwrap();
            store
                .put(
                    &legacy_key.into(),
                    bytes::Bytes::from_static(b"legacy").into(),
                )
                .await
                .unwrap();
            let error = refuse_legacy_partitions_index(&ownership, root)
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!("s3://data/{legacy_key}"))
                    && error.contains(&format!("s3://data/{new_key}"))
                    && error.contains("Move it there"),
                "{error}"
            );
            // The refusal reads only: the bucket still holds exactly the two
            // index objects and the owner record.
            let store: Arc<dyn ObjectStore> = store;
            let keys: Vec<String> = futures::TryStreamExt::try_collect::<Vec<_>>(store.list(None))
                .await
                .unwrap()
                .into_iter()
                .map(|object| object.location.to_string())
                .filter(|key| !key.starts_with(".fireparq-owner"))
                .collect();
            let mut expected = vec![legacy_key.to_string(), new_key.clone()];
            expected.sort();
            let mut keys = keys;
            keys.sort();
            assert_eq!(keys, expected);
            ownership.release().await.unwrap();
        }
    }
}
