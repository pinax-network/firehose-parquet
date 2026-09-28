use super::*;
use crate::dataset_lock::LocalOwnership;
use crate::ingest::state::{tests::descriptor, RoutingPolicy};

fn protected(root: &Path, mirror: MirrorBinding) -> StreamDescriptor {
    fs::create_dir_all(root.join("blocks/date=2023-11-14")).unwrap();
    let owner = LocalOwnership::acquire(&[root.to_owned()]).unwrap();
    let mut descriptor = descriptor(RoutingPolicy::GenesisLookaheadV1);
    descriptor.output = resolve_output_identity(root.to_str().unwrap(), &empty_aws()).unwrap();
    descriptor.mirror = mirror;
    let state = AuthorityState::initial(descriptor.clone()).unwrap();
    LocalStateStore::new(root, &owner)
        .unwrap()
        .create(ControlKey::State, &state)
        .unwrap();
    descriptor
}

#[tokio::test]
async fn selected_partition_expands_to_whole_protected_root_and_external_mirror() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dataset");
    let external = temp.path().join("mirror");
    fs::create_dir(&external).unwrap();
    protected(
        &root,
        MirrorBinding::Local {
            absolute_path: external
                .join("cursor.parquet")
                .to_string_lossy()
                .into_owned(),
        },
    );
    let prepared = acquire(
        "fixture",
        vec![MaintenanceTarget::directory(
            root.join("blocks/date=2023-11-14").to_string_lossy(),
        )],
        None,
    )
    .await
    .unwrap();
    assert_eq!(prepared.roots.len(), 1);
    assert!(LocalOwnership::acquire(&[root.clone()]).is_err());
    assert!(LocalOwnership::acquire(&[external.clone()]).is_err());
    assert!(!external.join("cursor.parquet").exists());
    prepared.ownership.release().await.unwrap();
    assert!(LocalOwnership::acquire(&[root]).is_ok());
}

#[tokio::test]
async fn parent_selection_finds_siblings_but_nested_authorities_are_refused() {
    let temp = tempfile::tempdir().unwrap();
    protected(&temp.path().join("one"), MirrorBinding::Disabled);
    protected(&temp.path().join("two"), MirrorBinding::Disabled);
    let selection = || vec![MaintenanceTarget::directory(temp.path().to_string_lossy())];
    let prepared = acquire("fixture", selection(), None).await.unwrap();
    assert_eq!(prepared.roots.len(), 2);
    prepared.ownership.release().await.unwrap();
    protected(&temp.path().join("one/nested"), MirrorBinding::Disabled);
    assert!(acquire("fixture", selection(), None).await.is_err());
}

#[tokio::test]
async fn orphan_marker_is_refused_before_recovery() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir(temp.path().join(CONTROL_DIRECTORY)).unwrap();
    fs::write(temp.path().join("sentinel.parquet"), b"untouched").unwrap();
    let selection = || vec![MaintenanceTarget::directory(temp.path().to_string_lossy())];
    let error = acquire("recovery", selection(), None).await.err().unwrap();
    assert!(error.to_string().contains("no authoritative state"));
    assert_eq!(
        fs::read(temp.path().join("sentinel.parquet")).unwrap(),
        b"untouched"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_alias_resolves_root_but_nested_alias_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("data");
    protected(&root, MirrorBinding::Disabled);
    let alias = temp.path().join("alias");
    std::os::unix::fs::symlink(&root, &alias).unwrap();
    let prepared = acquire(
        "fixture",
        vec![MaintenanceTarget::directory(alias.to_string_lossy())],
        None,
    )
    .await
    .unwrap();
    assert_eq!(prepared.roots.len(), 1);
    prepared.ownership.release().await.unwrap();
    std::os::unix::fs::symlink(temp.path(), root.join("nested")).unwrap();
    assert!(acquire(
        "fixture",
        vec![MaintenanceTarget::directory(root.to_string_lossy())],
        None
    )
    .await
    .is_err());
}

#[tokio::test]
async fn remote_discovery_uses_components_and_includes_ancestors_descendants() {
    use crate::dataset_lock_s3::S3Ownership;
    use object_store::ObjectStore;
    let store: std::sync::Arc<dyn ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    for key in [
        "one/.fireparq-ingest/state.json",
        "two/.fireparq-ingest/state.json",
        "one-other/.fireparq-ingest/state.json",
    ] {
        store
            .put(
                &object_store::path::Path::from(key),
                bytes::Bytes::from_static(b"marker only").into(),
            )
            .await
            .unwrap();
    }
    let remote = S3Ownership::acquire(store.clone(), "fixture", vec!["one".into()])
        .await
        .unwrap();
    let owner = DatasetOwnership::from_remote_for_test("bucket", remote);
    let selected = BTreeSet::from([MaintenanceTarget::directory(
        "s3://bucket/one/blocks/date=2023-11-14",
    )]);
    assert_eq!(
        discover_markers(
            &owner,
            &selected,
            MarkerScope::Tree,
            &ListingStats::default()
        )
        .await
        .unwrap(),
        BTreeSet::from(["s3://bucket/one".into()])
    );
    let parent = BTreeSet::from([MaintenanceTarget::directory("s3://bucket")]);
    assert_eq!(
        discover_markers(&owner, &parent, MarkerScope::Tree, &ListingStats::default())
            .await
            .unwrap()
            .len(),
        3
    );
    owner.release().await.unwrap();
}

/// A dataset written with `--output s3://bucket` has its
/// marker at the bucket root. A table or partition below it resolves to the
/// bucket root, and a second dataset nested in it (for example
/// `--output 's3://bucket/{chain}'`) is refused as conflicting authority.
#[tokio::test]
async fn remote_discovery_finds_a_protected_bucket_root_and_refuses_nesting() {
    use crate::dataset_lock_s3::S3Ownership;
    use object_store::ObjectStore;
    let store: std::sync::Arc<dyn ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let put = |key: &'static str| {
        let store = store.clone();
        async move {
            store
                .put(
                    &object_store::path::Path::from(key),
                    bytes::Bytes::from_static(b"marker only").into(),
                )
                .await
                .unwrap();
        }
    };
    put(".fireparq-ingest/state.json").await;
    put("blocks/date=2024-01-01/part-v1-a.parquet").await;
    let remote = S3Ownership::acquire(store.clone(), "fixture", vec![String::new()])
        .await
        .unwrap();
    let owner = DatasetOwnership::from_remote_for_test("bucket", remote);
    for selected in [
        "s3://bucket",
        "s3://bucket/blocks",
        "s3://bucket/blocks/date=2024-01-01",
    ] {
        let targets = BTreeSet::from([MaintenanceTarget::directory(selected)]);
        assert_eq!(
            discover_markers(
                &owner,
                &targets,
                MarkerScope::Tree,
                &ListingStats::default()
            )
            .await
            .unwrap(),
            BTreeSet::from(["s3://bucket".to_string()]),
            "{selected}"
        );
    }
    put("mainnet/.fireparq-ingest/state.json").await;
    for selected in ["s3://bucket", "s3://bucket/mainnet"] {
        let targets = BTreeSet::from([MaintenanceTarget::directory(selected)]);
        assert!(
            discover_markers(
                &owner,
                &targets,
                MarkerScope::Tree,
                &ListingStats::default()
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("nested protected datasets"),
            "{selected}"
        );
    }
    owner.release().await.unwrap();
}

/// A selected file acquires its enclosing protected root.
#[tokio::test]
async fn selected_file_acquires_its_protected_root() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("data");
    protected(&root, MirrorBinding::Disabled);
    let input = root.join("blocks/date=2023-11-14/part-000001.parquet");
    fs::write(&input, b"selected input").unwrap();
    let prepared = acquire(
        "recovery",
        vec![MaintenanceTarget::input(input.to_string_lossy()).unwrap()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(prepared.roots.len(), 1);
    assert!(LocalOwnership::acquire(&[root]).is_err());
    prepared.ownership.release().await.unwrap();
}

/// A protected root with blocks 100 and 101 committed in two transactions, both in
/// `blocks/date=2023-11-14/`.
async fn committed_dataset(root: &Path) -> StreamDescriptor {
    use crate::config::{BlockMetadata, Compression};
    use crate::ingest::{
        frontier::AcceptedFrontier,
        state::{
            tests::{event, routing},
            Digest,
        },
    };
    use crate::writer::{protected::schema_sha256, ParquetFileMetadata};
    use arrow::{
        array::{Array, TimestampMillisecondArray, UInt64Array},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use std::{collections::HashMap, sync::Arc};
    fs::create_dir_all(root).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new("value", DataType::UInt64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
    ]));
    let mut descriptor = descriptor(RoutingPolicy::GenesisLookaheadV1);
    descriptor.output = resolve_output_identity(root.to_str().unwrap(), &empty_aws()).unwrap();
    descriptor.tables = std::collections::BTreeMap::from([(
        "blocks".into(),
        Digest::parse(schema_sha256(&schema).unwrap()).unwrap(),
    )]);
    let ownership = DatasetOwnership::acquire(
        "fixture",
        vec![MutationScope::directory(root.to_string_lossy())],
        None,
    )
    .await
    .unwrap();
    let local = ownership.local().unwrap();
    TransactionStateStore::local(root, local)
        .unwrap()
        .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
        .await
        .unwrap();
    let mirror = ProtectedMirror::new(&ownership, &MirrorBinding::Disabled, None).unwrap();
    let mut controller = TransactionController::open(
        TransactionStateStore::local(root, local).unwrap(),
        TransactionParts::local(root, local).unwrap(),
        &mirror,
        &descriptor,
    )
    .await
    .unwrap();
    for number in [100, 101] {
        let mut frontier = AcceptedFrontier::resume(&controller.authority().checkpoint);
        let timestamp = 1_700_000_000 + (number - 100) as i64;
        let mut event = event(number, 1);
        event.source_timestamp = Some(timestamp);
        let ordinal = frontier.receive(event).unwrap();
        frontier
            .accept(ordinal, routing(RoutingPolicy::GenesisLookaheadV1))
            .unwrap();
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(UInt64Array::from(vec![number])),
            Arc::new(UInt64Array::from(vec![number * 2])),
            Arc::new(TimestampMillisecondArray::from(vec![timestamp * 1000]).with_timezone("UTC")),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut metadata = ParquetFileMetadata::new();
        metadata.add("chain", "mainnet");
        controller
            .commit(
                frontier.snapshot().unwrap().unwrap(),
                HashMap::from([("blocks".into(), batch)]),
                BlockMetadata {
                    min_block_number: number,
                    max_block_number: number,
                    min_timestamp: Some(timestamp),
                    max_timestamp: Some(timestamp),
                },
                Compression::Zstd,
                metadata,
            )
            .await
            .unwrap();
    }
    drop(controller);
    ownership.release().await.unwrap();
    descriptor
}

#[tokio::test]
async fn ingestion_target_refuses_nested_authority_before_creating_any_path() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("protected");
    protected(&root, MirrorBinding::Disabled);
    for selected in [root.join("not-created"), temp.path().to_path_buf()] {
        let ownership = DatasetOwnership::acquire(
            "fixture",
            vec![MutationScope::directory(selected.to_string_lossy())],
            None,
        )
        .await
        .unwrap();
        let identity = resolve_output_identity(selected.to_str().unwrap(), &empty_aws()).unwrap();
        assert!(validate_ingestion_target(
            &identity,
            &ownership,
            IngestionTarget::Create,
            &ListingStats::default()
        )
        .await
        .is_err());
        ownership.release().await.unwrap();
    }
    assert!(!root.join("not-created").exists());
    assert!(!temp.path().join(CONTROL_DIRECTORY).exists());
}

#[tokio::test]
async fn recovery_rolls_back_writing_or_finishes_committed_before_returning_guard() {
    use crate::ingest::{
        frontier::AcceptedFrontier,
        state::{
            tests::{event, routing},
            Digest, PartCompression, PartReceipt, PendingTransaction, TablePlan,
        },
    };
    use crate::{
        config::{BlockMetadata, Compression},
        writer::{protected::PreparedFlush, ParquetFileMetadata},
    };
    use arrow::{
        array::{TimestampMillisecondArray, UInt64Array},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use std::{collections::HashMap, sync::Arc};
    for committed in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let descriptor = committed_dataset(root).await;
        let ownership = DatasetOwnership::acquire(
            "fixture",
            vec![MutationScope::directory(root.to_string_lossy())],
            None,
        )
        .await
        .unwrap();
        let owner = ownership.local().unwrap();
        let store = LocalStateStore::new(root, owner).unwrap();
        let authority = store
            .load::<AuthorityState>(ControlKey::State)
            .unwrap()
            .unwrap()
            .payload;
        let mut frontier = AcceptedFrontier::resume(&authority.checkpoint);
        let ordinal = frontier.receive(event(102, 1)).unwrap();
        frontier
            .accept(ordinal, routing(RoutingPolicy::GenesisLookaheadV1))
            .unwrap();
        let mut pending = PendingTransaction::prepare(
            &authority,
            frontier.snapshot().unwrap().unwrap(),
            vec![TablePlan {
                table: "blocks".into(),
                rows: 1,
                schema_sha256: descriptor.tables["blocks"].clone(),
                partition: "date=2023-11-14".into(),
            }],
            PartCompression::Zstd,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new("value", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![102])),
                Arc::new(UInt64Array::from(vec![204])),
                Arc::new(
                    TimestampMillisecondArray::from(vec![1_700_000_002_000]).with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        let prepared = PreparedFlush::new(
            HashMap::from([("blocks".into(), batch)]),
            &descriptor
                .tables
                .iter()
                .map(|(table, digest)| (table.clone(), digest.as_str().into()))
                .collect(),
            pending
                .parts
                .iter()
                .map(|part| crate::ingest::parts::writer_plan(&pending, part))
                .collect(),
            BlockMetadata {
                min_block_number: 102,
                max_block_number: 102,
                min_timestamp: Some(1_700_000_002),
                max_timestamp: Some(1_700_000_002),
            },
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .unwrap();
        let encoded = prepared.encode(0, None).unwrap();
        pending = pending
            .with_receipt(
                0,
                PartReceipt {
                    byte_size: encoded.receipt().byte_size,
                    sha256: Digest::parse(encoded.receipt().sha256.clone()).unwrap(),
                },
                &descriptor,
            )
            .unwrap();
        let version = store.create(ControlKey::Pending, &pending).unwrap();
        let parts = TransactionParts::local(root, owner).unwrap();
        parts.stage(&encoded).unwrap();
        parts.publish(&encoded).await.unwrap();
        let final_path = root.join(&pending.parts[0].final_relative_path);
        if committed {
            pending = pending.committed_after_verification(&descriptor).unwrap();
            store
                .replace(ControlKey::Pending, &version, &pending)
                .unwrap();
        }
        ownership.release().await.unwrap();
        let prepared = acquire(
            "recovery",
            vec![MaintenanceTarget::directory(
                root.join("blocks").to_string_lossy(),
            )],
            None,
        )
        .await
        .unwrap();
        assert_eq!(final_path.exists(), committed);
        let store = LocalStateStore::new(root, prepared.ownership.local().unwrap()).unwrap();
        assert!(store
            .load::<PendingTransaction>(ControlKey::Pending)
            .unwrap()
            .is_none());
        let state = store
            .load::<AuthorityState>(ControlKey::State)
            .unwrap()
            .unwrap()
            .payload;
        assert_eq!(state.checkpoint.ordinal, if committed { 3 } else { 2 });
        prepared.ownership.release().await.unwrap();
    }
}
