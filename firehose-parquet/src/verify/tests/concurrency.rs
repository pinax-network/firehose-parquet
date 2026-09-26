//! `verify` reads table data without dataset ownership, so it can run while
//! `build` or another command owns the dataset. These tests pin why its roots
//! stay sound: open partitions come from the writer frontier, a concurrent
//! rewrite fails the run before anything is compared or written, and an
//! unfinished merge is refused instead of recovered.

use super::super::{WriterProgress, JOURNAL_FILE};
use super::*;
use crate::dataset_lock::{DatasetOwnership, LocalOwnership, MutationScope};
use crate::ingest::controller::TransactionController;
use crate::ingest::frontier::AcceptedFrontier;
use crate::ingest::mirror::ProtectedMirror;
use crate::ingest::observe;
use crate::ingest::parts::TransactionParts;
use crate::ingest::state::tests::{descriptor, event, routing};
use crate::ingest::state::{
    AuthorityState, Digest, MirrorBinding, PartitionPolicy, RoutingPolicy, StreamDescriptor,
};
use crate::ingest::store::TransactionStateStore;
use std::collections::BTreeSet;

/// Runs `hook` between the scan and the check that its files are unchanged.
fn after_scan(hook: impl FnOnce() + 'static) {
    super::super::AFTER_SCAN.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

/// Every file below `dir`, relative to it.
fn tree(dir: &Path) -> BTreeSet<String> {
    let mut files = BTreeSet::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.insert(path.strip_prefix(dir).unwrap().display().to_string());
            }
        }
    }
    files
}

fn legacy_fixture(root: &Path) -> std::path::PathBuf {
    let data = root.join("mainnet/blocks");
    write_block_nums(&data.join("day=1/part-0.parquet"), &[1, 2]);
    write_block_nums(&data.join("day=2/part-0.parquet"), &[3, 4]);
    data
}

#[test]
fn verify_writes_roots_and_reports_while_another_command_owns_every_scope() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = legacy_fixture(&root);
    let chain_root = root.join("mainnet");
    let report = root.join("reports/report.json");
    std::fs::create_dir_all(report.parent().unwrap()).unwrap();
    // `build` holds the chain root exclusively; the report directory is owned too.
    let _build =
        LocalOwnership::acquire(&[chain_root.clone(), report.parent().unwrap().into()]).unwrap();

    let mut opts = base_opts();
    opts.checks = vec![VerifyCheck::Roots, VerifyCheck::Protocol];
    opts.report_json = Some(report.clone());
    opts.publish_report = true;
    let first = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
    assert!(first.summary.wrote_registry);
    assert_eq!(first.summary.missing_expected, 2);
    assert!(chain_root.join(MERKLE_ROOTS_FILENAME).exists());
    assert!(report.exists());
    assert!(Path::new(first.published_report_path.as_ref().unwrap()).exists());

    let second = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
    assert_eq!(second.summary.matches, 2);
    assert!(second.is_valid());
}

/// One protected transaction per window of blocks, committed by the real
/// ingestion controller into `block_range=` partitions of ten blocks.
fn protected_dataset(root: &Path, windows: &[&[u64]]) -> StreamDescriptor {
    use crate::config::{BlockMetadata, Compression};
    use crate::writer::{protected::schema_sha256, ParquetFileMetadata};
    std::fs::create_dir_all(root).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_num",
        DataType::UInt64,
        false,
    )]));
    let mut descriptor = descriptor(RoutingPolicy::DirectV1);
    descriptor.partition = PartitionPolicy::BlockRange {
        size: 10,
        anchor: 100,
    };
    descriptor.output = crate::ingest::binding::resolve_output_identity(
        root.to_str().unwrap(),
        &crate::cli::AwsConfig {
            aws_access_key_id: None,
            aws_secret_access_key: None,
            aws_session_token: None,
            aws_region: None,
            aws_endpoint_url: None,
        },
    )
    .unwrap();
    descriptor.tables = std::collections::BTreeMap::from([(
        "blocks".into(),
        Digest::parse(schema_sha256(&schema).unwrap()).unwrap(),
    )]);
    super::super::block_on_async(async {
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
        for window in windows {
            let mut frontier = AcceptedFrontier::resume(&controller.authority().checkpoint);
            for block in *window {
                let ordinal = frontier.receive(event(*block, 1)).unwrap();
                frontier
                    .accept(ordinal, routing(RoutingPolicy::DirectV1))
                    .unwrap();
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(UInt64Array::from(window.to_vec()))],
            )
            .unwrap();
            controller
                .commit(
                    frontier.snapshot().unwrap().unwrap(),
                    HashMap::from([("blocks".into(), batch)]),
                    BlockMetadata {
                        min_block_number: window[0],
                        max_block_number: *window.last().unwrap(),
                        min_timestamp: None,
                        max_timestamp: None,
                    },
                    Compression::Zstd,
                    ParquetFileMetadata::new(),
                )
                .await
                .unwrap();
        }
        drop(controller);
        ownership.release().await.unwrap();
    });
    descriptor
}

/// Marks the bounded request `[.., stop)` complete, as a finished `build` does.
fn complete_request(root: &Path, descriptor: &StreamDescriptor, stop: u64) {
    super::super::block_on_async(async {
        let ownership = DatasetOwnership::acquire(
            "fixture",
            vec![MutationScope::directory(root.to_string_lossy())],
            None,
        )
        .await
        .unwrap();
        let local = ownership.local().unwrap();
        let mirror = ProtectedMirror::new(&ownership, &MirrorBinding::Disabled, None).unwrap();
        let mut controller = TransactionController::open(
            TransactionStateStore::local(root, local).unwrap(),
            TransactionParts::local(root, local).unwrap(),
            &mirror,
            descriptor,
        )
        .await
        .unwrap();
        let frontier = AcceptedFrontier::resume(&controller.authority().checkpoint);
        assert!(controller
            .complete_request(stop, &frontier, true)
            .await
            .unwrap());
        drop(controller);
        ownership.release().await.unwrap();
    });
}

fn partitions(report: &VerifyReport, status: &str) -> Vec<String> {
    let mut names: Vec<String> = report
        .findings
        .iter()
        .filter(|finding| {
            serde_json::to_value(&finding.status).unwrap() == serde_json::json!(status)
        })
        .map(|finding| finding.partition.clone())
        .collect();
    names.sort();
    names
}

fn registry_partitions(registry: &Path) -> Vec<String> {
    let mut names: Vec<String> = load_registry(&registry.display().to_string(), None)
        .unwrap()
        .rows
        .into_values()
        .map(|row| row.partition)
        .collect();
    names.sort();
    names
}

#[test]
fn the_protected_frontier_decides_open_partitions_while_build_owns_the_dataset() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap().join("mainnet");
    let descriptor = protected_dataset(&root, &[&[100, 101, 105], &[110, 112]]);
    let data = root.join("blocks");
    let registry = root.join(MERKLE_ROOTS_FILENAME);
    let (first, last) = (
        "block_range=100-110".to_string(),
        "block_range=110-120".to_string(),
    );
    let written: BTreeSet<String> = tree(&data)
        .iter()
        .map(|file| file.split('/').next().unwrap().to_string())
        .collect();
    assert_eq!(written, BTreeSet::from([first.clone(), last.clone()]));
    // A running transaction already published a part after the frontier (112).
    let pending_partition = "block_range=120-130".to_string();
    let pending = data.join(&pending_partition).join("part-pending.parquet");
    write_block_nums(&pending, &[120, 121]);

    let _build = LocalOwnership::acquire(&[root.clone()]).unwrap();
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert!(report.is_valid(), "{:?}", report.findings);
    assert_eq!(partitions(&report, "missing_expected"), [first.clone()]);
    // The partition holding the last committed block and every partition
    // with rows after it stay open.
    let mut open = vec![last.clone(), pending_partition.clone()];
    open.sort();
    assert_eq!(partitions(&report, "open"), open);
    assert!(report
        .warnings
        .iter()
        .any(|w| w.contains("authoritative ingestion state") && w.contains("block 112")));
    assert_eq!(registry_partitions(&registry), [first.clone()]);

    // The stream completes at its stop block: only the uncommitted part is
    // open now, and the last committed partition is recorded.
    drop(_build);
    std::fs::remove_file(&pending).unwrap();
    std::fs::remove_dir(pending.parent().unwrap()).unwrap();
    complete_request(&root, &descriptor, 113);
    write_block_nums(&pending, &[120, 121]);
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(partitions(&report, "open"), [pending_partition.clone()]);
    assert_eq!(partitions(&report, "match"), [first.clone()]);
    assert_eq!(partitions(&report, "missing_expected"), [last.clone()]);
    let mut recorded = vec![first.clone(), last.clone()];
    recorded.sort();
    assert_eq!(registry_partitions(&registry), recorded);
}

#[test]
fn an_extension_after_a_completed_stop_reopens_the_last_partition() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap().join("mainnet");
    let descriptor = protected_dataset(&root, &[&[100, 101]]);
    complete_request(&root, &descriptor, 102);
    let data = root.join("blocks");
    let finished = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert!(partitions(&finished, "open").is_empty());
    assert_eq!(finished.summary.missing_expected, 1);

    // A longer request resumes: `completed_stop` keeps the old bound while the
    // frontier moves past it, so the stream is live again.
    let mut state = observe::read_local_authority(&root).unwrap().unwrap();
    assert_eq!(state.checkpoint.completed_stop, Some(102));
    state.checkpoint.event.as_mut().unwrap().block_num = 104;
    let progress = WriterProgress::from_authority(&state, "root");
    assert!(matches!(
        progress,
        WriterProgress::Known {
            frontier: Some(104),
            finished: false,
            ..
        }
    ));
}

#[test]
fn a_rewrite_during_the_scan_fails_before_anything_is_compared_or_written() {
    type Rewrite = fn(&Path);
    let cases: [(&str, Rewrite); 3] = [
        ("was replaced", |data| {
            // A merge or truncate atomically replaces a part.
            let path = data.join("day=1/part-0.parquet");
            let tmp = data.join("day=1/.tmp");
            write_block_nums(&tmp, &[1, 2]);
            std::fs::rename(tmp, path).unwrap();
        }),
        ("was added", |data| {
            write_block_nums(&data.join("day=1/part-1.parquet"), &[2]);
        }),
        ("was removed", |data| {
            std::fs::remove_file(data.join("day=2/part-0.parquet")).unwrap();
        }),
    ];
    for (expected, rewrite) in cases {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let data = legacy_fixture(&root);
        let registry = root.join("mainnet").join(MERKLE_ROOTS_FILENAME);
        let hooked = data.clone();
        after_scan(move || rewrite(&hooked));
        let err = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap_err();
        let err = format!("{err:#}");
        assert!(err.contains("changed while verify was reading"), "{err}");
        assert!(err.contains(expected), "{expected}: {err}");
        assert!(!registry.exists(), "{expected}: nothing is written");
    }

    // Parts landing in an open partition are expected while `build` runs.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = legacy_fixture(&root);
    save_cursor(&root.join("mainnet"), 4, None);
    let hooked = data.clone();
    after_scan(move || write_block_nums(&hooked.join("day=2/part-1.parquet"), &[5]));
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(partitions(&report, "open"), ["day=2"]);
    assert_eq!(partitions(&report, "missing_expected"), ["day=1"]);
}

#[test]
fn partitions_without_block_numbers_stay_open_while_the_stream_is_live() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = root.join("mainnet/blocks");
    for day in ["day=1", "day=2"] {
        let path = data.join(day).join("part-0.parquet");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(UInt64Array::from(vec![7u64])) as ArrayRef,
        )])
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(path).unwrap(), batch.schema(), None)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    save_cursor(&root.join("mainnet"), 10, None);
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(partitions(&report, "open"), ["day=1", "day=2"]);
    assert!(!report.summary.wrote_registry);

    // Once the stream reached its stop block, they are complete.
    save_cursor(&root.join("mainnet"), 10, Some(11));
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert!(partitions(&report, "open").is_empty());
    assert_eq!(report.summary.missing_expected, 2);
}

#[test]
fn an_unfinished_merge_is_refused_without_recovering_anything() {
    use crate::merge::{run_merge, MergeConfig};
    use crate::merge_journal::INJECTED_CRASH;
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = root.join("mainnet/blocks");
    write_block_nums(&data.join("day=1/part-a-000001.parquet"), &[1, 2]);
    write_block_nums(&data.join("day=1/part-b-000001.parquet"), &[3]);
    write_block_nums(&data.join("day=2/part-a-000001.parquet"), &[4]);
    let merge = MergeConfig {
        path: data.display().to_string(),
        compression: crate::config::Compression::Zstd,
        flush_rows: None,
        flush_bytes: 0,
        dry_run: false,
        verbose: false,
        aws: None,
        cache_control: String::new(),
    };
    // The merge dies after writing its output: sources and output coexist.
    INJECTED_CRASH.with(|crash| *crash.borrow_mut() = Some("after-outputs"));
    let crashed = run_merge(&merge);
    INJECTED_CRASH.with(|crash| *crash.borrow_mut() = None);
    assert!(crashed.is_err());
    let before = tree(&data);
    assert!(
        before.contains(&format!("day=1/{JOURNAL_FILE}")),
        "{before:?}"
    );

    let registry = root.join("mainnet").join(MERKLE_ROOTS_FILENAME);
    let mut protocol_only = base_opts();
    protocol_only.checks = vec![VerifyCheck::Protocol];
    for opts in [base_opts(), protocol_only] {
        let err = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap_err();
        let err = format!("{err:#}");
        assert!(err.contains("unfinished merge"), "{err}");
        assert!(err.contains("fireparq recovery recover"), "{err}");
        assert_eq!(tree(&data), before, "verify must not delete or add files");
        assert!(!registry.exists());
    }
    // A single file inside the claimed partition is refused too.
    let err = verify_parquet(
        data.join("day=1/part-a-000001.parquet").to_str().unwrap(),
        None,
        &base_opts(),
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains("unfinished merge"));

    // After the operator recovers the merge, verify runs.
    crate::ingest::maintenance::acquire_blocking(
        "recovery",
        vec![
            crate::ingest::maintenance::MaintenanceTarget::input(data.display().to_string())
                .unwrap(),
        ],
        crate::ingest::maintenance::MaintenancePolicy::Recover,
        None,
    )
    .unwrap()
    .ownership
    .release_blocking()
    .unwrap();
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert!(report.summary.wrote_registry);
    assert_eq!(report.summary.missing_expected, 2);
}

#[test]
fn artifact_destinations_inside_a_protected_dataset_keep_their_guards() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap().join("mainnet");
    let descriptor = protected_dataset(&root, &[&[100, 101]]);
    complete_request(&root, &descriptor, 102);
    let data = root.join("blocks");
    let partition = tree(&data)
        .into_iter()
        .next()
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .to_string();
    for (registry, expected) in [
        (
            data.join(&partition).join("roots.parquet"),
            "ordinary protected data part",
        ),
        (root.join("cursor.parquet"), "protected recovery metadata"),
        (root.join(".fireparq-ingest/roots.parquet"), "control path"),
    ] {
        let mut opts = base_opts();
        opts.registry_path = Some(registry.display().to_string());
        let err = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap_err();
        assert!(format!("{err:#}").contains(expected), "{expected}: {err:#}");
        assert!(!registry.exists());
    }
    // The default registry and a report outside the dataset are allowed.
    let mut opts = base_opts();
    opts.report_json = Some(dir.path().join("report.json"));
    assert!(
        verify_parquet(data.to_str().unwrap(), None, &opts)
            .unwrap()
            .summary
            .wrote_registry
    );
}

#[derive(Debug, Default)]
struct ArtifactStore {
    inner: object_store::memory::InMemory,
    // 0 = success, 1 = published but lost response, 2 = unsupported write.
    fault: std::sync::atomic::AtomicU8,
    writes: std::sync::Mutex<Vec<object_store::PutMode>>,
}

impl std::fmt::Display for ArtifactStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("synthetic artifact store")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for ArtifactStore {
    async fn put_opts(
        &self,
        key: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let artifact = matches!(key.as_ref(), "registry.parquet" | "report.json");
        let fault = if artifact {
            self.writes.lock().unwrap().push(opts.mode.clone());
            self.fault.load(std::sync::atomic::Ordering::SeqCst)
        } else {
            0
        };
        if fault == 2 {
            return Err(object_store::Error::NotImplemented);
        }
        let result = self.inner.put_opts(key, payload, opts).await?;
        if fault == 1 {
            Err(object_store::Error::Generic {
                store: "synthetic",
                source: "accepted artifact lost its response".into(),
            })
        } else {
            Ok(result)
        }
    }
    async fn get_opts(
        &self,
        key: &object_store::path::Path,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(key, opts).await
    }
    async fn delete(&self, key: &object_store::path::Path) -> object_store::Result<()> {
        self.inner.delete(key).await
    }
    async fn put_multipart_opts(
        &self,
        key: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[test]
fn remote_registry_and_report_writes_make_one_attempt_without_an_owner() {
    use object_store::ObjectStore;
    for registry in [false, true] {
        for fault in [0, 1, 2] {
            let fake = Arc::new(ArtifactStore::default());
            let store: Arc<dyn ObjectStore> = fake.clone();
            fake.fault.store(fault, std::sync::atomic::Ordering::SeqCst);
            let location = object_store::path::Path::from(if registry {
                "registry.parquet"
            } else {
                "report.json"
            });
            let result = if registry {
                super::super::commit_registry_to_store(
                    store.as_ref(),
                    &location,
                    &super::super::RegistrySnapshot::default(),
                    &[fill(registry_row("mainnet", "day=1", "aa"))],
                )
            } else {
                super::super::write_report_to_store(store.as_ref(), &location, b"synthetic report")
            };
            assert_eq!(result.is_ok(), fault == 0);
            assert_eq!(fake.writes.lock().unwrap().len(), 1, "no retry");
            if registry {
                assert!(matches!(
                    fake.writes.lock().unwrap()[0],
                    object_store::PutMode::Create
                ));
            }
            // Verify never creates a dataset owner record.
            assert!(
                super::super::block_on_async(crate::dataset_lock_s3::S3Ownership::status(&store))
                    .unwrap()
                    .is_none()
            );
            // Losing the acknowledgement does not undo accepted publication.
            assert_eq!(
                super::super::block_on_async(store.get(&location)).is_ok(),
                fault != 2
            );
        }
    }
}

/// Puts a `block_num` Parquet object into an in-memory bucket.
fn put_block_nums(store: &dyn object_store::ObjectStore, key: &str, values: &[u64]) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_num",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(values.to_vec()))],
    )
    .unwrap();
    let mut data = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut data, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    super::super::block_on_async(store.put(
        &object_store::path::Path::from(key),
        object_store::PutPayload::from(data),
    ))
    .unwrap();
}

fn remote_source(store: Arc<dyn object_store::ObjectStore>) -> super::super::DataSource {
    super::super::DataSource::Remote {
        path: "s3://bucket/mainnet/blocks".to_string(),
        bucket: "bucket".to_string(),
        prefix: "mainnet/blocks".to_string(),
        store,
    }
}

fn verify_remote(
    store: &Arc<dyn object_store::ObjectStore>,
    opts: &VerifyOptions,
) -> Result<VerifyReport> {
    super::super::verify_source(
        &remote_source(store.clone()),
        None,
        opts,
        time::OffsetDateTime::now_utc(),
        uuid::Uuid::new_v4().to_string(),
    )
}

#[test]
fn remote_scans_are_read_only_and_detect_rewrites_and_merges() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    put_block_nums(
        store.as_ref(),
        "mainnet/blocks/day=1/part-0.parquet",
        &[1, 2],
    );
    put_block_nums(
        store.as_ref(),
        "mainnet/blocks/day=2/part-0.parquet",
        &[3, 4],
    );
    let registry = dir.path().join("roots.parquet");
    let opts = roots_opts(&registry);

    let report = verify_remote(&store, &opts).unwrap();
    assert_eq!(report.summary.missing_expected, 2);
    assert!(report.summary.wrote_registry);
    let objects: Vec<_> = super::super::block_on_async(async {
        use futures::TryStreamExt;
        store.list(None).try_collect::<Vec<_>>().await
    })
    .unwrap();
    assert_eq!(objects.len(), 2, "verify writes nothing into the bucket");

    // An object replaced after the scan read it fails the run.
    let hooked = store.clone();
    after_scan(move || {
        put_block_nums(
            hooked.as_ref(),
            "mainnet/blocks/day=1/part-0.parquet",
            &[1, 2, 2],
        )
    });
    let err = format!("{:#}", verify_remote(&store, &opts).unwrap_err());
    assert!(err.contains("was replaced"), "{err}");

    // A merge journal is refused before anything is read.
    super::super::block_on_async(store.put(
        &object_store::path::Path::from(format!("mainnet/blocks/day=2/{JOURNAL_FILE}")),
        object_store::PutPayload::from_static(b"{}"),
    ))
    .unwrap();
    let err = format!("{:#}", verify_remote(&store, &opts).unwrap_err());
    assert!(err.contains("unfinished merge"), "{err}");
}

#[test]
fn remote_protected_frontier_is_read_without_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let local = std::fs::canonicalize(dir.path()).unwrap().join("mainnet");
    protected_dataset(&local, &[&[100, 101], &[110]]);
    // Copy the committed dataset, including its authoritative state, into a bucket.
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    for file in tree(&local) {
        if file.ends_with(".parquet") || file == ".fireparq-ingest/state.json" {
            super::super::block_on_async(store.put(
                &object_store::path::Path::from(format!("mainnet/{file}")),
                object_store::PutPayload::from(std::fs::read(local.join(&file)).unwrap()),
            ))
            .unwrap();
        }
    }
    let registry = dir.path().join("roots.parquet");
    let report = verify_remote(&store, &roots_opts(&registry)).unwrap();
    assert_eq!(report.summary.missing_expected, 1);
    assert_eq!(report.summary.open_partitions, 1);
    assert!(report
        .warnings
        .iter()
        .any(|w| w.contains("s3://bucket/mainnet") && w.contains("block 110")));
}

#[test]
fn protocol_only_runs_stay_read_only_while_owned() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = legacy_fixture(&root);
    let _build = LocalOwnership::acquire(&[root.join("mainnet")]).unwrap();
    let mut opts = base_opts();
    opts.checks = vec![VerifyCheck::Protocol];
    let report = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
    assert!(!report.summary.wrote_registry);
    assert!(!root.join("mainnet").join(MERKLE_ROOTS_FILENAME).exists());
}
