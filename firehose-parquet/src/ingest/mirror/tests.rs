use super::*;
use crate::dataset_lock::MutationScope;
use crate::ingest::state::{
    tests::{descriptor, event},
    RoutingPolicy, StorageIdentity,
};
use arrow::array::{ArrayRef, BinaryArray, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    memory::InMemory, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use prometheus_client::registry::Registry;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct FaultState {
    fail: Vec<Stage>,
    seen: Vec<Stage>,
    directories: Vec<PathBuf>,
}
thread_local! { static FAULT: RefCell<FaultState> = RefCell::new(FaultState::default()); }
pub(super) fn checkpoint(stage: Stage) -> Result<()> {
    FAULT.with(|fault| {
        let mut fault = fault.borrow_mut();
        fault.seen.push(stage);
        if fault.fail.first() == Some(&stage) {
            fault.fail.remove(0);
            bail!("injected mirror persistence failure");
        }
        Ok(())
    })
}
pub(super) fn directory_sync(path: &Path) -> Result<()> {
    FAULT.with(|fault| fault.borrow_mut().directories.push(path.to_owned()));
    Ok(())
}
fn faults(stages: Vec<Stage>) {
    FAULT.with(|fault| {
        *fault.borrow_mut() = FaultState {
            fail: stages,
            ..Default::default()
        }
    });
}
fn metrics() -> PipelineMetrics {
    PipelineMetrics::new(&mut Registry::default())
}
fn local_fixture() -> (tempfile::TempDir, DatasetOwnership, AuthorityState, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state/cursor.parquet");
    let ownership = DatasetOwnership::acquire_blocking(
        "test",
        vec![MutationScope::file(path.to_string_lossy())],
        None,
    )
    .unwrap();
    let mut desc = descriptor(RoutingPolicy::DirectV1);
    desc.mirror = MirrorBinding::Local {
        absolute_path: path.to_string_lossy().into_owned(),
    };
    desc.output = StorageIdentity::Local {
        canonical_root: dir.path().to_string_lossy().into_owned(),
    };
    let authority = AuthorityState::initial(desc).unwrap();
    (dir, ownership, authority, path)
}
fn rehash(authority: &mut AuthorityState) {
    let cp = &authority.checkpoint;
    authority.checkpoint.id = Digest::hash(
        "checkpoint",
        &(
            authority.descriptor.id().unwrap(),
            &cp.previous,
            cp.ordinal,
            &cp.event,
            &cp.routing,
            cp.completed_stop,
        ),
    )
    .unwrap();
    authority.validate().unwrap();
}
fn advance(authority: &AuthorityState, number: u64) -> AuthorityState {
    let mut next = authority.clone();
    next.checkpoint.previous = Some(authority.checkpoint.id.clone());
    next.checkpoint.ordinal += 1;
    next.checkpoint.event = Some(event(number, 1));
    rehash(&mut next);
    next
}
fn complete(authority: &AuthorityState, stop: u64) -> AuthorityState {
    let mut next = authority.clone();
    next.checkpoint.previous = Some(authority.checkpoint.id.clone());
    next.checkpoint.completed_stop = Some(stop);
    rehash(&mut next);
    next
}
fn mirror<'a>(ownership: &'a DatasetOwnership, authority: &AuthorityState) -> ProtectedMirror<'a> {
    ProtectedMirror::new(ownership, &authority.descriptor.mirror, None).unwrap()
}

#[tokio::test]
async fn local_initialization_absence_and_legacy_compatibility() {
    let (_dir, ownership, initial, path) = local_fixture();
    let metrics = metrics();
    let adapter = {
        let binding = initial.descriptor.mirror.clone();
        ProtectedMirror::new(&ownership, &binding, None)
            .unwrap()
            .with_metrics(&metrics)
    };
    adapter.require_absent_for_initialization().await.unwrap();
    assert_eq!(
        adapter.reconcile(&initial).await.unwrap(),
        MirrorOutcome::Unchanged
    );
    assert!(!path.exists());
    let current = advance(&initial, 100);
    assert_eq!(
        adapter.reconcile(&current).await.unwrap(),
        MirrorOutcome::Repaired
    );
    let bytes = read_local(&path).unwrap().unwrap();
    let legacy = crate::cursor::parse_cursor(bytes.clone()).unwrap().unwrap();
    assert_eq!(legacy.cursor, "private-cursor-100-1");
    assert_eq!(legacy.last_block_num, 100);
    assert_eq!(legacy.last_block_id, b"block-100");
    assert_eq!(legacy.last_timestamp, Some(1_700_000_000));
    assert_eq!(legacy.start_block, Some(100));
    assert_eq!(legacy.stop_block, None);
    assert_eq!(
        decode(bytes.clone(), &current.descriptor)
            .unwrap()
            .checkpoint
            .id,
        current.checkpoint.id
    );
    assert_eq!(
        adapter.reconcile(&current).await.unwrap(),
        MirrorOutcome::Unchanged
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(metrics.cursor_saves_total.get(), 1);
    assert_eq!(metrics.cursor_last_block_num.get(), 100);
    assert!(metrics.cursor_last_success_timestamp_seconds.get() > 0);
    assert!(adapter.reconcile(&initial).await.is_err());
    assert!(adapter.require_absent_for_initialization().await.is_err());
    fs::write(&path, []).unwrap();
    assert!(adapter.require_absent_for_initialization().await.is_err());
    assert!(adapter.reconcile(&current).await.is_err());
    assert_eq!(fs::metadata(path).unwrap().len(), 0);
}

#[tokio::test]
async fn repairs_behind_and_only_immediate_monotonic_completion() {
    let (_dir, ownership, initial, path) = local_fixture();
    let one = advance(&initial, 100);
    let two = advance(&one, 101);
    let adapter = mirror(&ownership, &initial);
    adapter.reconcile(&one).await.unwrap();
    adapter.reconcile(&two).await.unwrap();
    assert!(adapter
        .reconcile(&one)
        .await
        .unwrap_err()
        .to_string()
        .contains("ahead"));
    let done = complete(&two, 102);
    assert_eq!(
        adapter.reconcile(&done).await.unwrap(),
        MirrorOutcome::Repaired
    );
    let saved = fs::read(&path).unwrap();
    let mut conflicts = vec![two.clone(), complete(&two, 103)];
    let mut changed_event = complete(&done, 104);
    changed_event.checkpoint.event = Some(event(103, 1));
    rehash(&mut changed_event);
    conflicts.push(changed_event);
    let decrease = complete(&done, 101);
    conflicts.push(decrease);
    for conflicting in conflicts {
        assert!(adapter.reconcile(&conflicting).await.is_err());
        assert_eq!(fs::read(&path).unwrap(), saved);
    }
    let extended = complete(&done, 103);
    assert_eq!(
        adapter.reconcile(&extended).await.unwrap(),
        MirrorOutcome::Repaired
    );
    let legacy = crate::cursor::parse_cursor(read_local(&path).unwrap().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(legacy.stop_block, Some(103));
}

#[tokio::test]
async fn foreign_stream_and_binding_refused_without_mutation() {
    let (_dir, ownership, initial, path) = local_fixture();
    let one = advance(&initial, 100);
    let adapter = mirror(&ownership, &one);
    adapter.reconcile(&one).await.unwrap();
    let original = fs::read(&path).unwrap();
    let mut foreign = one.clone();
    foreign.descriptor.extended = true;
    rehash(&mut foreign);
    assert!(adapter.reconcile(&foreign).await.is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    let mut another_binding = one.clone();
    another_binding.descriptor.mirror = MirrorBinding::Disabled;
    rehash(&mut another_binding);
    assert!(adapter.reconcile(&another_binding).await.is_err());
    let disabled = MirrorBinding::Disabled;
    let disabled_adapter = ProtectedMirror::new(&ownership, &disabled, None).unwrap();
    assert_eq!(
        disabled_adapter.reconcile(&another_binding).await.unwrap(),
        MirrorOutcome::Disabled
    );
    assert!(ProtectedMirror::new(&ownership, &disabled, Some(&one.checkpoint.id)).is_err());
}

fn rewrite(
    bytes: Bytes,
    row: impl FnOnce(&mut Vec<ArrayRef>),
    metadata: impl FnOnce(&mut Vec<parquet::file::metadata::KeyValue>),
) -> Bytes {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let mut kvs = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .clone();
    kvs.retain(|kv| kv.key != parquet::arrow::ARROW_SCHEMA_META_KEY);
    let batch = builder.build().unwrap().next().unwrap().unwrap();
    let mut columns = batch.columns().to_vec();
    row(&mut columns);
    metadata(&mut kvs);
    let batch = RecordBatch::try_new(Arc::new(crate::cursor::cursor_schema()), columns).unwrap();
    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(kvs))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes.into()
}
#[test]
fn every_duplicated_row_field_and_metadata_is_checked_without_secret_errors() {
    let (_dir, _ownership, initial, _path) = local_fixture();
    let current = complete(&advance(&initial, 100), 101);
    let original = encode(&current).unwrap();
    let mismatches: Vec<(usize, ArrayRef)> = vec![
        (
            0,
            Arc::new(StringArray::from(vec!["injected-private-secret"])),
        ),
        (1, Arc::new(UInt64Array::from(vec![101]))),
        (2, Arc::new(BinaryArray::from(vec![b"foreign".as_slice()]))),
        (3, Arc::new(Int64Array::from(vec![Some(-1)]))),
        (
            4,
            Arc::new(StringArray::from(vec!["injected-private-secret"])),
        ),
        (5, Arc::new(UInt64Array::from(vec![Some(101)]))),
        (6, Arc::new(UInt64Array::from(vec![Some(102)]))),
    ];
    for (index, wrong) in mismatches {
        let changed = rewrite(original.clone(), |columns| columns[index] = wrong, |_| {});
        let error = decode(changed, &current.descriptor)
            .err()
            .unwrap()
            .to_string();
        assert!(!error.contains("private-secret"));
    }
    for change in 0..5 {
        let changed = rewrite(
            original.clone(),
            |_| {},
            |kvs| match change {
                0 => kvs.push(kvs[0].clone()),
                1 => kvs.push(parquet::file::metadata::KeyValue::new(
                    "unknown".into(),
                    Some("private-secret".into()),
                )),
                2 => {
                    kvs.iter_mut()
                        .find(|kv| kv.key == "firehose-parquet.with_votes")
                        .unwrap()
                        .value = Some("true".into())
                }
                3 => {
                    kvs.iter_mut()
                        .find(|kv| kv.key == ENVELOPE_KEY)
                        .unwrap()
                        .value = Some("private-secret".into())
                }
                4 => {
                    let kv = kvs.iter_mut().find(|kv| kv.key == ENVELOPE_KEY).unwrap();
                    let mut value: serde_json::Value =
                        serde_json::from_str(kv.value.as_ref().unwrap()).unwrap();
                    value["format_version"] = 2.into();
                    kv.value = Some(value.to_string());
                }
                _ => unreachable!(),
            },
        );
        let error = decode(changed, &current.descriptor)
            .err()
            .unwrap()
            .to_string();
        assert!(!error.contains("private-secret"));
    }
    assert!(decode(
        Bytes::from(vec![0; MAX_MIRROR_BYTES + 1]),
        &current.descriptor
    )
    .is_err());
}

#[tokio::test]
async fn local_failure_boundaries_keep_complete_files_and_reestablish_durability() {
    let (_dir, ownership, initial, path) = local_fixture();
    let one = advance(&initial, 100);
    let two = advance(&one, 101);
    let adapter = mirror(&ownership, &one);
    adapter.reconcile(&one).await.unwrap();
    let old = fs::read(&path).unwrap();
    for stage in [Stage::Write, Stage::FileSync, Stage::Rename] {
        faults(vec![stage]);
        assert!(write_local(&path, &encode(&two).unwrap()).is_err());
        assert_eq!(fs::read(&path).unwrap(), old);
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
    }
    faults(vec![Stage::DirectorySync]);
    assert!(write_local(&path, &encode(&two).unwrap()).is_err());
    assert_eq!(
        decode(read_local(&path).unwrap().unwrap(), &two.descriptor)
            .unwrap()
            .checkpoint
            .id,
        two.checkpoint.id
    );
    faults(vec![Stage::ExistingFileSync]);
    assert!(
        adapter.reconcile(&two).await.is_err(),
        "matching bytes must not bypass sync failure"
    );
    faults(vec![]);
    assert_eq!(
        adapter.reconcile(&two).await.unwrap(),
        MirrorOutcome::Unchanged
    );
    FAULT.with(|fault| assert!(fault.borrow().seen.contains(&Stage::ExistingFileSync)));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[tokio::test]
async fn local_retry_and_shutdown_metrics_are_precise() {
    let (_dir, ownership, initial, path) = local_fixture();
    let one = advance(&initial, 100);
    let metrics = metrics();
    let shutdown = AtomicBool::new(true);
    let adapter = mirror(&ownership, &one)
        .with_metrics(&metrics)
        .with_shutdown(&shutdown);
    faults(vec![Stage::Write]);
    assert!(adapter
        .reconcile(&one)
        .await
        .unwrap_err()
        .to_string()
        .contains("shutdown"));
    assert!(!path.exists());
    assert_eq!(metrics.cursor_save_failures_total.get(), 1);
    assert_eq!(metrics.cursor_saves_total.get(), 0);
    shutdown.store(false, Ordering::SeqCst);
    faults(vec![Stage::DirectorySync]);
    assert_eq!(
        adapter.reconcile(&one).await.unwrap(),
        MirrorOutcome::Repaired
    );
    assert_eq!(metrics.cursor_save_failures_total.get(), 2);
    assert_eq!(metrics.cursor_saves_total.get(), 1);
    FAULT.with(|fault| assert!(fault.borrow().seen.contains(&Stage::ExistingFileSync)));
}

#[cfg(unix)]
#[tokio::test]
async fn alias_ancestry_is_synced_and_retarget_or_leaf_symlink_is_rejected() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let aliases = dir.path().join("aliases");
    fs::create_dir_all(&target).unwrap();
    fs::create_dir_all(&aliases).unwrap();
    let alias = aliases.join("state");
    symlink(&target, &alias).unwrap();
    let path = alias.join("cursor.parquet");
    let ownership = DatasetOwnership::acquire_blocking(
        "test",
        vec![MutationScope::file(path.to_string_lossy())],
        None,
    )
    .unwrap();
    let mut desc = descriptor(RoutingPolicy::DirectV1);
    desc.mirror = MirrorBinding::Local {
        absolute_path: path.to_string_lossy().into_owned(),
    };
    let current = advance(&AuthorityState::initial(desc).unwrap(), 100);
    let adapter = mirror(&ownership, &current);
    faults(vec![]);
    adapter.reconcile(&current).await.unwrap();
    FAULT.with(|fault| {
        let seen = &fault.borrow().directories;
        assert!(seen.contains(&target.canonicalize().unwrap()));
        assert!(seen.contains(&aliases));
    });
    fs::remove_file(&path).unwrap();
    symlink(dir.path().join("unrelated"), &path).unwrap();
    assert!(adapter.reconcile(&current).await.is_err());
    fs::remove_file(&path).unwrap();
    let other = dir.path().join("other");
    fs::create_dir(&other).unwrap();
    fs::remove_file(&alias).unwrap();
    symlink(&other, &alias).unwrap();
    assert!(adapter.reconcile(&current).await.is_err());
    assert!(!other.join("cursor.parquet").exists());
}

#[derive(Default)]
struct RemoteFaults {
    lose: bool,
    cancel: bool,
    hide_version: bool,
    readback_fails: bool,
    stale_version: bool,
    writes: Vec<PutMode>,
}
#[derive(Default)]
struct RemoteStore {
    inner: InMemory,
    faults: Mutex<RemoteFaults>,
}
impl std::fmt::Display for RemoteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("private-secret-store")
    }
}
impl std::fmt::Debug for RemoteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
fn remote_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "private-secret-provider",
        source: "private-secret-body".into(),
    }
}
const KEY: &str = "mirrors/cursor.parquet";
#[async_trait]
impl ObjectStore for RemoteStore {
    async fn put_opts(
        &self,
        key: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let mirror = key.as_ref() == KEY;
        if mirror {
            self.faults.lock().unwrap().writes.push(opts.mode.clone());
        }
        let mut result = self.inner.put_opts(key, payload, opts).await?;
        let (lose, cancel, hide) = {
            let f = self.faults.lock().unwrap();
            (
                mirror && f.lose,
                mirror && f.cancel,
                mirror && f.hide_version,
            )
        };
        if cancel {
            futures::future::pending::<()>().await;
        }
        if lose {
            return Err(remote_error());
        }
        if hide {
            result.e_tag = None;
            result.version = None;
        }
        Ok(result)
    }
    async fn get_opts(
        &self,
        key: &ObjectPath,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        let fail = {
            let f = self.faults.lock().unwrap();
            key.as_ref() == KEY && f.readback_fails && !f.writes.is_empty()
        };
        if fail {
            return Err(remote_error());
        }
        let mut result = self.inner.get_opts(key, opts).await?;
        if key.as_ref() == KEY && self.faults.lock().unwrap().stale_version {
            result.meta.e_tag = Some("stale-acknowledgement".into());
        }
        Ok(result)
    }
    async fn delete(&self, key: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(key).await
    }
    async fn put_multipart_opts(
        &self,
        key: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
async fn remote_fixture() -> (Arc<RemoteStore>, DatasetOwnership, AuthorityState, Digest) {
    let store = Arc::new(RemoteStore::default());
    let owner = S3Ownership::acquire(store.clone(), "ingest", vec![KEY.into()])
        .await
        .unwrap();
    let ownership = DatasetOwnership::from_remote_for_test("cursor-bucket", owner);
    let service = Digest::hash("service", &"fixture").unwrap();
    let mut desc = descriptor(RoutingPolicy::DirectV1);
    desc.mirror = MirrorBinding::S3 {
        service: service.clone(),
        bucket: "cursor-bucket".into(),
        key: KEY.into(),
    };
    let authority = AuthorityState::initial(desc).unwrap();
    (store, ownership, authority, service)
}
#[tokio::test]
async fn remote_binding_and_conditional_repair_preserve_current_authority() {
    let (store, ownership, initial, service) = remote_fixture().await;
    assert!(ProtectedMirror::new(&ownership, &initial.descriptor.mirror, None).is_err());
    assert!(ProtectedMirror::new(
        &ownership,
        &initial.descriptor.mirror,
        Some(&initial.checkpoint.id)
    )
    .is_err());
    let adapter =
        ProtectedMirror::new(&ownership, &initial.descriptor.mirror, Some(&service)).unwrap();
    adapter.require_absent_for_initialization().await.unwrap();
    adapter.reconcile(&initial).await.unwrap();
    assert!(store.faults.lock().unwrap().writes.is_empty());
    let one = advance(&initial, 100);
    let two = advance(&one, 101);
    assert_eq!(
        adapter.reconcile(&one).await.unwrap(),
        MirrorOutcome::Repaired
    );
    assert_eq!(
        adapter.reconcile(&one).await.unwrap(),
        MirrorOutcome::Unchanged
    );
    assert_eq!(
        adapter.reconcile(&two).await.unwrap(),
        MirrorOutcome::Repaired
    );
    assert!(adapter.reconcile(&one).await.is_err());
    assert!(adapter.require_absent_for_initialization().await.is_err());
    let f = store.faults.lock().unwrap();
    assert_eq!(f.writes.len(), 2);
    assert!(matches!(f.writes[0], PutMode::Create));
    assert!(matches!(f.writes[1], PutMode::Update(_)));
    drop(f);
    assert!(!ownership
        .remote("cursor-bucket")
        .unwrap()
        .is_mutation_uncertain());
    ownership.release().await.unwrap();
}
#[tokio::test]
async fn ambiguous_remote_writes_or_cancel_never_retry_or_release() {
    for mode in 0..5 {
        let (store, ownership, initial, service) = remote_fixture().await;
        let current = advance(&initial, 100);
        let metrics = metrics();
        {
            let mut f = store.faults.lock().unwrap();
            f.lose = mode == 0;
            f.cancel = mode == 1;
            f.hide_version = mode == 2;
            f.readback_fails = mode == 3;
            f.stale_version = mode == 4;
        }
        let adapter = ProtectedMirror::new(&ownership, &current.descriptor.mirror, Some(&service))
            .unwrap()
            .with_metrics(&metrics);
        if mode == 1 {
            assert!(
                tokio::time::timeout(Duration::from_millis(20), adapter.reconcile(&current))
                    .await
                    .is_err()
            );
        } else {
            let error = adapter.reconcile(&current).await.unwrap_err();
            assert!(!format!("{error:#}").contains("private-secret"));
        }
        assert_eq!(store.faults.lock().unwrap().writes.len(), 1);
        assert!(
            store.inner.get(&ObjectPath::from(KEY)).await.is_ok(),
            "successful remote mutation exists despite lost response"
        );
        assert!(ownership
            .remote("cursor-bucket")
            .unwrap()
            .is_mutation_uncertain());
        assert!(adapter.reconcile(&current).await.is_err());
        assert_eq!(store.faults.lock().unwrap().writes.len(), 1);
        assert_eq!(metrics.cursor_saves_total.get(), 0);
        assert_eq!(metrics.cursor_save_failures_total.get(), 1);
        assert_eq!(metrics.cursor_last_success_timestamp_seconds.get(), 0);
        let retained = ownership.remote("cursor-bucket").unwrap().record().clone();
        assert!(ownership.release().await.is_err());
        let store: Arc<dyn ObjectStore> = store;
        assert_eq!(
            S3Ownership::status(&store).await.unwrap().as_ref(),
            Some(&retained)
        );
    }
}

#[test]
fn row_count_schema_compression_and_uncompressed_limits_fail_closed() {
    let (_dir, _ownership, initial, _path) = local_fixture();
    let current = advance(&initial, 100);
    let bytes = encode(&current).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let kvs = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .clone();
    let batch = builder.build().unwrap().next().unwrap().unwrap();
    for mode in 0..4 {
        let mut props = WriterProperties::builder().set_key_value_metadata(Some(kvs.clone()));
        if mode == 2 {
            props = props.set_compression(parquet::basic::Compression::ZSTD(Default::default()));
        }
        let mut output = Vec::new();
        let schema = if mode == 3 {
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("wrong", arrow::datatypes::DataType::UInt64, false),
            ]))
        } else {
            Arc::new(crate::cursor::cursor_schema())
        };
        let mut writer =
            ArrowWriter::try_new(&mut output, schema.clone(), Some(props.build())).unwrap();
        if mode == 3 {
            writer
                .write(
                    &RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![100]))])
                        .unwrap(),
                )
                .unwrap();
        } else {
            if mode != 0 {
                writer.write(&batch).unwrap();
            }
            if mode == 1 {
                writer.write(&batch).unwrap();
            }
        }
        writer.close().unwrap();
        assert!(decode(output.into(), &current.descriptor).is_err());
    }
    let oversized = rewrite(
        encode(&current).unwrap(),
        |columns| {
            columns[0] = Arc::new(StringArray::from(vec![
                "x".repeat(MAX_ROW_BYTES as usize + 1)
            ]))
        },
        |_| {},
    );
    assert!(decode(oversized, &current.descriptor).is_err());
}

#[tokio::test]
async fn solana_null_and_negative_timestamp_anchor_are_authority_derived() {
    use crate::ingest::state::{AnchorProvenance, TimestampAnchor};
    let (_dir, ownership, initial, path) = local_fixture();
    let mut desc = initial.descriptor;
    desc.family = BlockFamily::Solana;
    desc.routing_policy = RoutingPolicy::SolanaLastKnownV1;
    let mut one = advance(&AuthorityState::initial(desc).unwrap(), 100);
    one.checkpoint.event.as_mut().unwrap().source_timestamp = None;
    rehash(&mut one);
    let adapter = mirror(&ownership, &one);
    adapter.reconcile(&one).await.unwrap();
    assert_eq!(
        crate::cursor::parse_cursor(read_local(&path).unwrap().unwrap())
            .unwrap()
            .unwrap()
            .last_timestamp,
        None
    );
    let mut two = advance(&one, 101);
    two.checkpoint.event.as_mut().unwrap().source_timestamp = Some(-1);
    two.checkpoint.routing.anchor = Some(TimestampAnchor {
        source_ordinal: 2,
        source_block_num: 101,
        source_block_id: "block-101".into(),
        seconds: -1,
        provenance: AnchorProvenance::AcceptedPrefix,
    });
    rehash(&mut two);
    adapter.reconcile(&two).await.unwrap();
    assert_eq!(
        crate::cursor::parse_cursor(read_local(&path).unwrap().unwrap())
            .unwrap()
            .unwrap()
            .last_timestamp,
        Some(-1)
    );
    let mut conflicting = complete(&two, 102);
    conflicting
        .checkpoint
        .routing
        .anchor
        .as_mut()
        .unwrap()
        .seconds = -2;
    rehash(&mut conflicting);
    assert!(
        adapter.reconcile(&conflicting).await.is_err(),
        "same-ordinal completion cannot change routing"
    );
}

#[test]
fn physical_page_counts_cannot_hide_behind_one_row_footer() {
    use parquet::file::metadata::{FileMetaData, ParquetMetaData, ParquetMetaDataWriter};
    let (_dir, _ownership, initial, _path) = local_fixture();
    let current = advance(&initial, 100);
    let builder = ParquetRecordBatchReaderBuilder::try_new(encode(&current).unwrap()).unwrap();
    let kvs = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .clone();
    let batch = builder.build().unwrap().next().unwrap().unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut bytes,
        Arc::new(crate::cursor::cursor_schema()),
        Some(
            WriterProperties::builder()
                .set_key_value_metadata(Some(kvs))
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let parsed = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.clone())).unwrap();
    let metadata = parsed.metadata();
    let file = metadata.file_metadata();
    let group = metadata
        .row_group(0)
        .clone()
        .into_builder()
        .set_num_rows(1)
        .build()
        .unwrap();
    let replacement = ParquetMetaData::new(
        FileMetaData::new(
            file.version(),
            1,
            file.created_by().map(str::to_owned),
            file.key_value_metadata().cloned(),
            file.schema_descr_ptr(),
            file.column_orders().cloned(),
        ),
        vec![group],
    );
    let footer =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap()) as usize;
    bytes.truncate(bytes.len() - 8 - footer);
    ParquetMetaDataWriter::new(&mut bytes, &replacement)
        .finish()
        .unwrap();
    let error = decode(bytes.into(), &current.descriptor)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("page exceeds bounded row"), "{error}");
}

#[tokio::test]
async fn local_three_failed_attempts_stop_and_remote_empty_file_blocks_initialization() {
    let (_dir, ownership, initial, path) = local_fixture();
    let current = advance(&initial, 100);
    let metrics = metrics();
    faults(vec![Stage::Write, Stage::Write, Stage::Write]);
    assert!(mirror(&ownership, &current)
        .with_metrics(&metrics)
        .reconcile(&current)
        .await
        .unwrap_err()
        .to_string()
        .contains("three local attempts"));
    assert_eq!(metrics.cursor_save_failures_total.get(), 3);
    assert_eq!(metrics.cursor_saves_total.get(), 0);
    assert!(!path.exists());
    let (store, ownership, initial, service) = remote_fixture().await;
    store
        .inner
        .put(&ObjectPath::from(KEY), Bytes::new().into())
        .await
        .unwrap();
    let adapter =
        ProtectedMirror::new(&ownership, &initial.descriptor.mirror, Some(&service)).unwrap();
    assert!(adapter.require_absent_for_initialization().await.is_err());
    assert!(adapter.reconcile(&advance(&initial, 100)).await.is_err());
    assert!(store.faults.lock().unwrap().writes.is_empty());
}

#[test]
fn out_of_scope_and_reserved_local_bindings_fail_without_creation() {
    let (dir, ownership, initial, _path) = local_fixture();
    for path in [
        dir.path().join("unheld/cursor.parquet"),
        dir.path().join("state/.fireparq-ingest/cursor.parquet"),
        dir.path().join("state/../escaped.parquet"),
    ] {
        let binding = MirrorBinding::Local {
            absolute_path: path.to_string_lossy().into_owned(),
        };
        assert!(ProtectedMirror::new(&ownership, &binding, None).is_err());
        assert!(!path.exists());
    }
    assert!(ProtectedMirror::new(
        &ownership,
        &initial.descriptor.mirror,
        Some(&initial.checkpoint.id)
    )
    .is_err());
}

#[tokio::test]
async fn direct_source_time_preserves_positive_negative_zero_and_null() {
    let (_dir, ownership, initial, path) = local_fixture();
    let adapter = mirror(&ownership, &initial);
    let mut current = initial.clone();
    for (index, source) in [Some(1_700_000_000), Some(-1), Some(0), None]
        .into_iter()
        .enumerate()
    {
        current = advance(&current, 100 + index as u64);
        current.checkpoint.event.as_mut().unwrap().source_timestamp = source;
        rehash(&mut current);
        adapter.reconcile(&current).await.unwrap();
        let bytes = read_local(&path).unwrap().unwrap();
        assert_eq!(
            decode(bytes.clone(), &current.descriptor)
                .unwrap()
                .checkpoint
                .event
                .as_ref()
                .unwrap()
                .source_timestamp,
            source
        );
        assert_eq!(
            crate::cursor::parse_cursor(bytes)
                .unwrap()
                .unwrap()
                .last_timestamp,
            source
        );
        assert!(current.checkpoint.routing.anchor.is_none());
    }
}
