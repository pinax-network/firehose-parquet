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
        MaintenancePolicy::Recover,
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
    let prepared = acquire("fixture", selection(), MaintenancePolicy::Recover, None)
        .await
        .unwrap();
    assert_eq!(prepared.roots.len(), 2);
    prepared.ownership.release().await.unwrap();
    protected(&temp.path().join("one/nested"), MirrorBinding::Disabled);
    assert!(
        acquire("fixture", selection(), MaintenancePolicy::Recover, None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn orphan_marker_refuses_reads_and_destructive_policy_precedes_recovery() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir(temp.path().join(CONTROL_DIRECTORY)).unwrap();
    fs::write(temp.path().join("sentinel.parquet"), b"untouched").unwrap();
    let selection = || vec![MaintenanceTarget::directory(temp.path().to_string_lossy())];
    let error = acquire("truncate", selection(), MaintenancePolicy::Truncate, None)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("truncate is unsupported"));
    let error = acquire("verify", selection(), MaintenancePolicy::Recover, None)
        .await
        .err()
        .unwrap();
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
        MaintenancePolicy::Recover,
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
        MaintenancePolicy::Recover,
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
        discover_markers(&owner, &selected).await.unwrap(),
        BTreeSet::from(["s3://bucket/one".into()])
    );
    let parent = BTreeSet::from([MaintenanceTarget::directory("s3://bucket")]);
    assert_eq!(discover_markers(&owner, &parent).await.unwrap().len(), 3);
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
            discover_markers(&owner, &targets).await.unwrap(),
            BTreeSet::from(["s3://bucket".to_string()]),
            "{selected}"
        );
    }
    put("mainnet/.fireparq-ingest/state.json").await;
    for selected in ["s3://bucket", "s3://bucket/mainnet"] {
        let targets = BTreeSet::from([MaintenanceTarget::directory(selected)]);
        assert!(
            discover_markers(&owner, &targets)
                .await
                .unwrap_err()
                .to_string()
                .contains("nested protected datasets"),
            "{selected}"
        );
    }
    owner.release().await.unwrap();
}

/// Acquire `root` and `destination` as `verify` does, then check the
/// destination against the recovered protected roots.
async fn validate_destination(root: &Path, destination: &Path) -> Result<()> {
    let targets = vec![
        MaintenanceTarget::directory(root.to_string_lossy()),
        MaintenanceTarget::file(destination.to_string_lossy()),
    ];
    let prepared = acquire("verify", targets.clone(), MaintenancePolicy::Recover, None).await?;
    let result = validate_artifact_destinations(&targets, &prepared.roots, &empty_aws());
    prepared.ownership.release().await?;
    result
}

#[tokio::test]
async fn artifact_destinations_cannot_replace_parts_or_the_bound_cursor() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("data");
    let mirror = temp.path().join("private-cursor.parquet");
    protected(
        &root,
        MirrorBinding::Local {
            absolute_path: mirror.to_string_lossy().into_owned(),
        },
    );
    for destination in [
        root.join("blocks/date=2023-11-14/part-000001.parquet"),
        mirror,
        root.join("cursor.parquet"),
        root.join("_fireparq/cursor.parquet"),
        root.join("blocks/date=2023-11-14/_fireparq_merge.json"),
    ] {
        assert!(validate_destination(&root, &destination).await.is_err());
        assert!(!destination.exists());
    }
    for destination in [
        root.join("_fireparq/merkle_roots.parquet"),
        root.join("_fireparq/verify_runs/run/report.json"),
        root.join("_fireparq/verify_runs/run/roots.parquet"),
        root.join("merkle_roots.parquet"),
        root.join("verify_runs/run/report.json"),
        root.join("report.json"),
    ] {
        validate_destination(&root, &destination).await.unwrap();
        assert!(!destination.exists());
    }
    let input = root.join("blocks/date=2023-11-14/part-000001.parquet");
    fs::write(&input, b"selected input").unwrap();
    let prepared = acquire(
        "verify",
        vec![MaintenanceTarget::input(input.to_string_lossy()).unwrap()],
        MaintenancePolicy::Recover,
        None,
    )
    .await
    .unwrap();
    prepared.ownership.release().await.unwrap();
}

fn zero_pending(descriptor: &StreamDescriptor) -> crate::ingest::state::PendingTransaction {
    use crate::ingest::{
        frontier::AcceptedFrontier,
        state::{
            tests::{event, routing},
            PartCompression, PendingTransaction, TablePlan,
        },
    };
    let authority = AuthorityState::initial(descriptor.clone()).unwrap();
    let mut frontier = AcceptedFrontier::resume(&authority.checkpoint);
    let ordinal = frontier.receive(event(100, 1)).unwrap();
    frontier
        .accept(ordinal, routing(RoutingPolicy::GenesisLookaheadV1))
        .unwrap();
    PendingTransaction::prepare(
        &authority,
        frontier.snapshot().unwrap().unwrap(),
        descriptor
            .tables
            .iter()
            .map(|(table, digest)| TablePlan {
                table: table.clone(),
                rows: 0,
                schema_sha256: digest.clone(),
                partition: String::new(),
            })
            .collect(),
        PartCompression::Zstd,
    )
    .unwrap()
}
fn interrupted_merge(
    root: &Path,
    stream: Option<&crate::ingest::state::Digest>,
) -> crate::merge_journal::Journal {
    use crate::merge_journal::{Journal, LocalPartition, PartitionFiles, RunContext};
    let partition = root.join("blocks/date=2023-11-14");
    fs::create_dir_all(&partition).unwrap();
    fs::write(partition.join("part-000001.parquet"), b"original").unwrap();
    fs::write(
        partition.join("part-000002.parquet"),
        b"unfinished duplicate",
    )
    .unwrap();
    let journal = Journal::new(
        &RunContext {
            run_id: "old-fixture".into(),
            lock: root.join("absent-lock").to_string_lossy().into_owned(),
        },
        vec!["part-000001.parquet".into()],
        2,
    )
    .with_protected_stream(stream);
    LocalPartition::new(&partition)
        .create_journal(&journal)
        .unwrap();
    journal
}

#[tokio::test]
async fn pending_ingestion_and_merge_coexistence_refuses_before_any_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let descriptor = protected(root, MirrorBinding::Disabled);
    let pending = zero_pending(&descriptor);
    {
        let owner = LocalOwnership::acquire(&[root.to_owned()]).unwrap();
        LocalStateStore::new(root, &owner)
            .unwrap()
            .create(ControlKey::Pending, &pending)
            .unwrap();
    }
    interrupted_merge(root, Some(&descriptor.id().unwrap()));
    let before = fs::read(root.join(CONTROL_DIRECTORY).join("pending.json")).unwrap();
    let error = acquire(
        "recovery",
        vec![MaintenanceTarget::directory(root.to_string_lossy())],
        MaintenancePolicy::Recover,
        None,
    )
    .await
    .err()
    .unwrap();
    assert!(error.to_string().contains("coexist"));
    assert_eq!(
        fs::read(root.join(CONTROL_DIRECTORY).join("pending.json")).unwrap(),
        before
    );
    assert!(root
        .join("blocks/date=2023-11-14/part-000002.parquet")
        .exists());
}

#[tokio::test]
async fn bound_merge_recovers_before_artifact_reads_and_unbound_merge_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let descriptor = protected(root, MirrorBinding::Disabled);
    interrupted_merge(root, None);
    assert!(acquire(
        "verify",
        vec![MaintenanceTarget::directory(root.to_string_lossy())],
        MaintenancePolicy::Recover,
        None
    )
    .await
    .is_err());
    assert!(root
        .join("blocks/date=2023-11-14/part-000002.parquet")
        .exists());
    fs::remove_file(
        root.join("blocks/date=2023-11-14")
            .join(crate::merge_journal::JOURNAL_FILE),
    )
    .unwrap();
    interrupted_merge(root, Some(&descriptor.id().unwrap()));
    let prepared = acquire(
        "verify",
        vec![MaintenanceTarget::directory(
            root.join("blocks/date=2023-11-14").to_string_lossy(),
        )],
        MaintenancePolicy::Recover,
        None,
    )
    .await
    .unwrap();
    assert!(!root
        .join("blocks/date=2023-11-14/part-000002.parquet")
        .exists());
    assert_eq!(
        fs::read(root.join("blocks/date=2023-11-14/part-000001.parquet")).unwrap(),
        b"original"
    );
    prepared.ownership.release().await.unwrap();
}

#[tokio::test]
async fn legacy_merge_is_recovered_without_creating_ingestion_authority() {
    let temp = tempfile::tempdir().unwrap();
    interrupted_merge(temp.path(), None);
    let prepared = acquire(
        "verify",
        vec![MaintenanceTarget::input(
            temp.path()
                .join("blocks/date=2023-11-14/part-000001.parquet")
                .to_string_lossy(),
        )
        .unwrap()],
        MaintenancePolicy::Recover,
        None,
    )
    .await
    .unwrap();
    assert!(prepared.roots.is_empty());
    assert!(!temp.path().join(CONTROL_DIRECTORY).exists());
    assert!(!temp
        .path()
        .join("blocks/date=2023-11-14/part-000002.parquet")
        .exists());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_protected_parts_merge_preserves_rows_and_frontier_without_source_receipt() {
    use arrow::array::UInt64Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    committed_dataset(root).await;
    let state_before = fs::read(root.join(CONTROL_DIRECTORY).join("state.json")).unwrap();
    let result = crate::merge::run_merge(&crate::merge::MergeConfig {
        path: root.join("blocks").to_string_lossy().into_owned(),
        compression: crate::config::Compression::Zstd,
        flush_rows: None,
        flush_bytes: 0,
        dry_run: false,
        verbose: false,
        aws: None,
        cache_control: String::new(),
    })
    .unwrap();
    assert_eq!(result.files_read, 2);
    assert_eq!(result.files_written, 1);
    assert_eq!(
        fs::read(root.join(CONTROL_DIRECTORY).join("state.json")).unwrap(),
        state_before
    );
    let path = root.join("blocks/date=2023-11-14/part-000001.parquet");
    let builder = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap()).unwrap();
    assert!(builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .iter()
        .all(|kv| !kv.key.starts_with("fireparq.ingest.")));
    assert!(builder
        .schema()
        .metadata()
        .keys()
        .all(|key| !key.starts_with("fireparq.ingest.")));
    let mut rows = Vec::new();
    for batch in builder.build().unwrap() {
        let batch = batch.unwrap();
        let numbers = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((numbers.value(i), values.value(i)));
        }
    }
    rows.sort();
    assert_eq!(rows, vec![(100, 200), (101, 202)]);
    let prepared = acquire(
        "recovery",
        vec![MaintenanceTarget::directory(root.to_string_lossy())],
        MaintenancePolicy::Recover,
        None,
    )
    .await
    .unwrap();
    prepared.ownership.release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_truncate_refuses_a_selected_protected_table() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("data");
    committed_dataset(&root).await;
    let before = fs::read(root.join(CONTROL_DIRECTORY).join("state.json")).unwrap();
    let table = root.join("blocks").to_string_lossy().into_owned();
    let result = crate::truncate::run_truncate(&crate::truncate::TruncateConfig {
        path: table.clone(),
        partitions: vec![],
        dry_run: false,
        yes: true,
        aws: None,
    });
    assert!(result.is_err());
    assert_eq!(
        fs::read(root.join(CONTROL_DIRECTORY).join("state.json")).unwrap(),
        before
    );
    assert_eq!(
        fs::read_dir(root.join("blocks/date=2023-11-14"))
            .unwrap()
            .count(),
        2
    );
}

#[tokio::test]
async fn native_remote_merge_recovery_works_with_current_thread_and_borrowed_session() {
    remote_merge_recovery_at("data").await;
}

/// `build --output s3://bucket` puts the protected root at
/// the bucket root: its authority, marker discovery and merge recovery use the
/// empty prefix exactly like a chain directory.
#[tokio::test]
async fn native_remote_merge_recovery_at_the_bucket_root() {
    remote_merge_recovery_at("").await;
}

async fn remote_merge_recovery_at(prefix: &str) {
    use crate::{
        dataset_lock_s3::{S3Ownership, OWNER_KEY},
        merge_journal::{Journal, RunContext, JOURNAL_FILE},
    };
    use object_store::{path::Path as ObjectPath, ObjectStore};
    use std::sync::Arc;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let owner = S3Ownership::acquire(store.clone(), "fixture", vec![prefix.into()])
        .await
        .unwrap();
    let ownership = DatasetOwnership::from_remote_for_test("bucket", owner);
    let root = if prefix.is_empty() {
        "s3://bucket".to_string()
    } else {
        format!("s3://bucket/{prefix}")
    };
    let identity = resolve_output_identity(&root, &empty_aws()).unwrap();
    let key = |relative: &str| {
        ObjectPath::from(if prefix.is_empty() {
            relative.to_string()
        } else {
            format!("{prefix}/{relative}")
        })
    };
    let mut descriptor = descriptor(RoutingPolicy::GenesisLookaheadV1);
    descriptor.output = identity.clone();
    let remote = ownership.remote("bucket").unwrap();
    S3StateStore::new(remote, prefix)
        .unwrap()
        .create(
            ControlKey::State,
            &AuthorityState::initial(descriptor.clone()).unwrap(),
        )
        .await
        .unwrap();
    let journal = Journal::new(
        &RunContext {
            run_id: "old-remote-run".into(),
            lock: OWNER_KEY.into(),
        },
        vec!["part-000001.parquet".into()],
        2,
    )
    .with_protected_stream(Some(&descriptor.id().unwrap()));
    for (name, bytes) in [
        ("part-000001.parquet", b"original".to_vec()),
        ("part-000002.parquet", b"duplicate".to_vec()),
        (JOURNAL_FILE, serde_json::to_vec(&journal).unwrap()),
    ] {
        store
            .put(
                &key(&format!("blocks/date=2023-11-14/{name}")),
                bytes.into(),
            )
            .await
            .unwrap();
    }
    let permit = remote.acquire_transaction_session().unwrap();
    validate_ingestion_recovery_order(&identity, &ownership)
        .await
        .unwrap();
    prepare_ingestion(&identity, &ownership, &empty_aws())
        .await
        .unwrap();
    assert!(matches!(
        store
            .head(&key("blocks/date=2023-11-14/part-000002.parquet"))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(store
        .head(&key("blocks/date=2023-11-14/part-000001.parquet"))
        .await
        .is_ok());
    assert!(matches!(
        store
            .head(&key(&format!("blocks/date=2023-11-14/{JOURNAL_FILE}")))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
    drop(permit);
    ownership.release().await.unwrap();
}

#[test]
fn malformed_merge_control_never_authorizes_path_traversal_or_empty_commit() {
    use crate::merge_journal::{Journal, RunContext};
    let journal = Journal::new(
        &RunContext {
            run_id: "fixture".into(),
            lock: "missing".into(),
        },
        vec!["part-000001.parquet".into()],
        2,
    );
    for mutate in 0..5 {
        let mut value = serde_json::to_value(&journal).unwrap();
        match mutate {
            0 => value["sources"] = serde_json::json!(["../../outside.parquet"]),
            1 => value["state"] = serde_json::json!("committed"),
            2 => value["unknown_capability"] = serde_json::json!(true),
            3 => value["protected_stream"] = serde_json::json!("invalid"),
            _ => {
                value["sources"] = serde_json::json!(["part-000001.parquet", "part-000001.parquet"])
            }
        }
        assert!(Journal::decode(&serde_json::to_vec(&value).unwrap(), "fixture").is_err());
    }
}

mod remote_deletion;

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
        assert!(validate_ingestion_target(&identity, &ownership)
            .await
            .is_err());
        ownership.release().await.unwrap();
    }
    assert!(!root.join("not-created").exists());
    assert!(!temp.path().join(CONTROL_DIRECTORY).exists());
}

#[tokio::test]
async fn maintenance_rolls_back_writing_or_finishes_committed_before_returning_guard() {
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
            "verify",
            vec![MaintenanceTarget::directory(
                root.join("blocks").to_string_lossy(),
            )],
            MaintenancePolicy::Recover,
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
