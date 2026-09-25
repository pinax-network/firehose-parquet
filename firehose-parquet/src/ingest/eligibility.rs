//! New protected streams may initialize only empty destinations (optionally
//! retaining one verified standalone index). Existing random-name data and
//! cursor files are not evidence from which an ingestion checkpoint can be made.

use anyhow::{bail, ensure, Context, Result};
use futures::StreamExt;
use std::path::{Path, PathBuf};

use super::binding::{output_path, validate_runtime_bindings};
use super::mirror::ProtectedMirror;
use super::state::{StorageIdentity, StreamDescriptor};
use crate::artifacts::{OWNERSHIP_FILENAME, OWNERSHIP_PROBES_DIRECTORY, PARTITIONS_INDEX_FILENAME};
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
    let index = match &descriptor.output {
        StorageIdentity::Local { canonical_root } => {
            let owner = ownership.local().context("local output is not owned")?;
            let root = Path::new(canonical_root);
            ensure!(
                owner.roots().iter().any(|scope| root.starts_with(scope)),
                "initialization output is outside ownership"
            );
            local_contents(root)?
        }
        StorageIdentity::S3 { bucket, prefix, .. } => {
            let owner = ownership
                .remote(bucket)
                .context("remote output is not owned")?;
            ensure!(
                !owner.is_mutation_uncertain(),
                "remote initialization requires resolved ownership"
            );
            remote_contents(owner.object_store().as_ref(), prefix).await?
        }
    };
    if index {
        let index_path = match &descriptor.output {
            StorageIdentity::Local { canonical_root } => Path::new(canonical_root)
                .join(PARTITIONS_INDEX_FILENAME)
                .to_str()
                .context("index path must be UTF-8")?
                .to_owned(),
            StorageIdentity::S3 { .. } => {
                format!("{}/{PARTITIONS_INDEX_FILENAME}", path.trim_end_matches('/'))
            }
        };
        // This reads coverage, rows and proofs from one snapshot. Incomplete
        // final live spans are valid artifacts; no bounds are consumed here.
        let index = crate::cli::read_verified_partitions_index(&index_path, Some(aws))
            .context("existing standalone index is not eligible for protected initialization; rebuild or use an empty output root")?;
        ensure!(
            index
                .spans
                .iter()
                .all(|span| span.row.chain.as_deref() == Some(descriptor.chain.as_str())),
            "standalone partition index belongs to another chain"
        );
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

fn local_contents(root: &Path) -> Result<bool> {
    let mut directories = vec![PathBuf::from(root)];
    let mut visited = 0usize;
    let mut index = false;
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
            } else if kind.is_file() && relative == Path::new(PARTITIONS_INDEX_FILENAME) {
                index = true;
            } else {
                return Err(reject_existing());
            }
        }
    }
    Ok(index)
}

async fn remote_contents(store: &dyn object_store::ObjectStore, prefix: &str) -> Result<bool> {
    let object_prefix =
        (!prefix.is_empty()).then(|| object_store::path::Path::from(format!("{prefix}/")));
    let list = async {
        let mut entries = store.list(object_prefix.as_ref());
        let mut visited = 0usize;
        let mut index = false;
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
            if relative == PARTITIONS_INDEX_FILENAME {
                index = true;
            } else {
                return Err(reject_existing());
            }
        }
        Ok(index)
    };
    tokio::time::timeout(std::time::Duration::from_secs(60), list)
        .await
        .map_err(|_| anyhow::anyhow!("protected initialization listing timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compression;
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
    fn index(chain: &str) -> crate::partition_index::VerifiedPartitionIndex {
        use crate::partition_index::*;
        VerifiedPartitionIndex {
            coverage: PartitionCoverage {
                version: 2,
                start_block: 100,
                stop_block: 200,
                finalized: crate::grpc::FinalizedAnchor {
                    block_num: 199,
                    block_id: "last".into(),
                },
                routing_policy: IndexRoutingPolicy::BlockNumber,
                first_observed: None,
                last_observed: None,
                next_observed: None,
                last_routing_timestamp: None,
            },
            spans: vec![VerifiedPartitionSpan {
                row: crate::cli::PartitionBuildRow {
                    chain: Some(chain.into()),
                    partition_type: "block_range".into(),
                    partition_value: "100".into(),
                    partition_start_ts: "100".into(),
                    partition_interval_seconds: 100,
                    start_block: 100,
                    stop_block: 200,
                    start_time: None,
                    end_time: None,
                },
                proof: PartitionSpanProof {
                    start_complete: true,
                    end_complete: true,
                    first_block: None,
                    routing_start_timestamp: None,
                },
            }],
        }
    }

    #[tokio::test]
    async fn empty_root_and_strict_same_chain_index_only_are_eligible() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chain");
        let owner = DatasetOwnership::acquire(
            "test",
            vec![crate::dataset_lock::MutationScope::directory(
                path.to_string_lossy(),
            )],
            None,
        )
        .await
        .unwrap();
        let mut descriptor = descriptor(RoutingPolicy::DirectV1);
        descriptor.output = resolve_output_identity(path.to_str().unwrap(), &aws()).unwrap();
        let mirror = ProtectedMirror::new(&owner, &MirrorBinding::Disabled, None).unwrap();
        require_initializable(&descriptor, &owner, &aws(), &mirror)
            .await
            .unwrap();
        let destination = path.join(PARTITIONS_INDEX_FILENAME);
        crate::cli::write_verified_partitions_index(
            destination.to_str().unwrap(),
            &index("mainnet"),
            Compression::Zstd,
            None,
            None,
        )
        .unwrap();
        require_initializable(&descriptor, &owner, &aws(), &mirror)
            .await
            .unwrap();
        crate::cli::write_verified_partitions_index(
            destination.to_str().unwrap(),
            &index("other"),
            Compression::Zstd,
            None,
            None,
        )
        .unwrap();
        assert!(require_initializable(&descriptor, &owner, &aws(), &mirror)
            .await
            .unwrap_err()
            .to_string()
            .contains("another chain"));
        std::fs::write(&destination, b"not parquet").unwrap();
        assert!(require_initializable(&descriptor, &owner, &aws(), &mirror)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn legacy_random_data_cursor_controls_and_external_cursor_are_not_adopted() {
        for existing in [
            "blocks/old-random.parquet",
            "cursor.parquet",
            ".fireparq-ingest/.state.json.unfinished.tmp",
            "partitions.parquet",
            ".fireparq-owner-v1.json",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(existing);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"existing").unwrap();
            assert!(local_contents(dir.path()).is_err() || existing == PARTITIONS_INDEX_FILENAME);
        }
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
        let mut descriptor = descriptor(RoutingPolicy::DirectV1);
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
        assert!(!remote_contents(&store, "chain").await.unwrap());
        store
            .put(
                &"other/legacy.parquet".into(),
                bytes::Bytes::from_static(b"legacy").into(),
            )
            .await
            .unwrap();
        assert!(!remote_contents(&store, "chain").await.unwrap());
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
        assert!(!remote_contents(&store, "").await.unwrap());
        store
            .put(
                &"cursor.parquet".into(),
                bytes::Bytes::from_static(b"legacy").into(),
            )
            .await
            .unwrap();
        assert!(remote_contents(&store, "").await.is_err());
    }
}
