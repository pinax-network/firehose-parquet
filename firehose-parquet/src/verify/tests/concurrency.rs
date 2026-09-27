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
    AuthorityState, Digest, MirrorBinding, RoutingPolicy, StreamDescriptor,
};
use crate::ingest::store::TransactionStateStore;
use std::collections::BTreeSet;

/// The default registry, relative to a chain root: `_fireparq/merkle_roots.parquet`.
fn default_registry() -> String {
    crate::artifacts::DatasetArtifact::MerkleRoots.relative_path()
}

/// Runs `hook` at a named point of the next run on this thread:
/// `after-frontier` (writer progress read, nothing listed yet), `before-scan`
/// (files listed, none read) or `after-scan` (before the snapshot check).
fn at(point: &'static str, hook: impl FnOnce() + 'static) {
    super::super::TEST_HOOKS.with(|hooks| hooks.borrow_mut().push((point, Box::new(hook))));
}

fn after_scan(hook: impl FnOnce() + 'static) {
    at("after-scan", hook);
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
    write_block_nums(&data.join("date=2024-01-01/part-0.parquet"), &[1, 2]);
    write_block_nums(&data.join("date=2024-01-02/part-0.parquet"), &[3, 4]);
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
    assert!(chain_root.join(default_registry()).exists());
    assert!(report.exists());
    assert!(Path::new(first.published_report_path.as_ref().unwrap()).exists());

    let second = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
    assert_eq!(second.summary.matches, 2);
    assert!(second.is_valid());
}

fn block_num_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new(
            "timestamp",
            crate::traits::timestamp_millis_utc_type(),
            false,
        ),
    ]))
}

/// Block time of a fixture block: ten blocks per UTC day from 2024-01-01, so
/// blocks 100-109 are in `date=2024-01-01`, 110-119 in `date=2024-01-02`, ...
fn block_seconds(block: u64) -> i64 {
    const DAY_ONE: i64 = 1_704_067_200;
    DAY_ONE + ((block - 100) / 10) as i64 * 86_400 + (block % 10) as i64
}

/// The `date=` partition of the `n`th fixture day (1 for blocks 100-109).
fn day(n: u32) -> String {
    format!("date=2024-01-{n:02}")
}

/// Initializes a protected dataset whose blocks are ten per day from block
/// 100 ([`block_seconds`]), without committing anything.
fn protected_root(root: &Path, final_blocks_only: bool) -> StreamDescriptor {
    use crate::writer::protected::schema_sha256;
    std::fs::create_dir_all(root).unwrap();
    let mut descriptor = descriptor(RoutingPolicy::GenesisLookaheadV1);
    descriptor.final_blocks_only = final_blocks_only;
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
        Digest::parse(schema_sha256(&block_num_schema()).unwrap()).unwrap(),
    )]);
    super::super::block_on_async(async {
        let ownership = DatasetOwnership::acquire(
            "fixture",
            vec![MutationScope::directory(root.to_string_lossy())],
            None,
        )
        .await
        .unwrap();
        TransactionStateStore::local(root, ownership.local().unwrap())
            .unwrap()
            .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
            .await
            .unwrap();
        ownership.release().await.unwrap();
    });
    descriptor
}

/// Commits one protected transaction per window of blocks with the real
/// ingestion controller, as `build` does.
fn commit_windows(root: &Path, descriptor: &StreamDescriptor, windows: &[&[u64]]) {
    use crate::config::{BlockMetadata, Compression};
    use crate::writer::ParquetFileMetadata;
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
        for window in windows {
            let mut frontier = AcceptedFrontier::resume(&controller.authority().checkpoint);
            for block in *window {
                let ordinal = frontier.receive(event(*block, 1)).unwrap();
                frontier
                    .accept(ordinal, routing(RoutingPolicy::GenesisLookaheadV1))
                    .unwrap();
            }
            let times: Vec<i64> = window.iter().map(|block| block_seconds(*block)).collect();
            let batch = RecordBatch::try_new(
                block_num_schema(),
                vec![
                    Arc::new(UInt64Array::from(window.to_vec())),
                    Arc::new(
                        arrow::array::TimestampMillisecondArray::from(
                            times.iter().map(|time| time * 1_000).collect::<Vec<_>>(),
                        )
                        .with_timezone("UTC"),
                    ),
                ],
            )
            .unwrap();
            controller
                .commit(
                    frontier.snapshot().unwrap().unwrap(),
                    HashMap::from([("blocks".into(), batch)]),
                    BlockMetadata {
                        min_block_number: window[0],
                        max_block_number: *window.last().unwrap(),
                        min_timestamp: times.iter().copied().min(),
                        max_timestamp: times.iter().copied().max(),
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
}

/// A final-only protected dataset with one committed transaction per window.
fn protected_dataset(root: &Path, windows: &[&[u64]]) -> StreamDescriptor {
    let descriptor = protected_root(root, true);
    commit_windows(root, &descriptor, windows);
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
    let descriptor = protected_dataset(&root, &[&[100, 101, 105], &[110, 112, 119]]);
    let data = root.join("blocks");
    let registry = root.join(default_registry());
    let (first, last) = (day(1), day(2));
    let written: BTreeSet<String> = tree(&data)
        .iter()
        .map(|file| file.split('/').next().unwrap().to_string())
        .collect();
    assert_eq!(written, BTreeSet::from([first.clone(), last.clone()]));
    // A running transaction already published a part after the frontier (119).
    let pending_partition = day(3);
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
        .any(|w| w.contains("authoritative ingestion state") && w.contains("block 119")));
    assert_eq!(registry_partitions(&registry), [first.clone()]);

    // The request completes at stop block 120. The day holding its last
    // block can still receive blocks of a later, longer request, so it stays
    // open with the uncommitted part; the earlier day is compared.
    drop(_build);
    std::fs::remove_file(&pending).unwrap();
    std::fs::remove_dir(pending.parent().unwrap()).unwrap();
    complete_request(&root, &descriptor, 120);
    write_block_nums(&pending, &[120, 121]);
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(partitions(&report, "open"), open);
    assert_eq!(partitions(&report, "match"), [first.clone()]);
    assert!(partitions(&report, "missing_expected").is_empty());
    assert_eq!(registry_partitions(&registry), [first.clone()]);
}

#[test]
fn a_completed_request_keeps_its_last_partition_open_until_it_cannot_grow() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap().join("mainnet");
    let descriptor = protected_dataset(&root, &[&[100, 101]]);
    complete_request(&root, &descriptor, 102);
    let data = root.join("blocks");
    // A later, longer request appends to the day of stop block 102, so it is
    // not recorded.
    let finished = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(partitions(&finished, "open"), [day(1)]);
    assert!(finished.findings[0]
        .error
        .as_deref()
        .unwrap()
        .contains("completed request (stop block 102)"));
    assert!(!finished.summary.wrote_registry);

    // The longer request commits blocks 105 and 111: `completed_stop` keeps
    // the old bound while the frontier moves past it, so the stream is live.
    commit_windows(&root, &descriptor, &[&[105], &[111]]);
    let state = observe::read_local_authority(&root).unwrap().unwrap();
    assert_eq!(state.checkpoint.completed_stop, Some(102));
    assert!(matches!(
        WriterProgress::from_authority(&state, "root"),
        WriterProgress::Known {
            frontier: Some(111),
            finished_at: None,
            ..
        }
    ));
    let extended = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(partitions(&extended, "missing_expected"), [day(1)]);
    assert_eq!(partitions(&extended, "open"), [day(2)]);
}

#[test]
fn a_rewrite_during_the_scan_fails_before_anything_is_compared_or_written() {
    type Rewrite = fn(&Path);
    let cases: [(&str, Rewrite); 3] = [
        ("was replaced", |data| {
            // A merge or truncate atomically replaces a part.
            let path = data.join("date=2024-01-01/part-0.parquet");
            let tmp = data.join("date=2024-01-01/.tmp");
            write_block_nums(&tmp, &[1, 2]);
            std::fs::rename(tmp, path).unwrap();
        }),
        ("was added", |data| {
            write_block_nums(&data.join("date=2024-01-01/part-1.parquet"), &[2]);
        }),
        ("was removed", |data| {
            std::fs::remove_file(data.join("date=2024-01-02/part-0.parquet")).unwrap();
        }),
    ];
    for (expected, rewrite) in cases {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let data = legacy_fixture(&root);
        let registry = root.join("mainnet").join(default_registry());
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
    after_scan(move || write_block_nums(&hooked.join("date=2024-01-02/part-1.parquet"), &[5]));
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(partitions(&report, "open"), ["date=2024-01-02"]);
    assert_eq!(partitions(&report, "missing_expected"), ["date=2024-01-01"]);
}

#[test]
fn partitions_without_block_numbers_stay_open_while_the_stream_is_live() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = root.join("mainnet/blocks");
    for day in ["date=2024-01-01", "date=2024-01-02"] {
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
    assert_eq!(
        partitions(&report, "open"),
        ["date=2024-01-01", "date=2024-01-02"]
    );
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
    write_block_nums(&data.join("date=2024-01-01/part-a-000001.parquet"), &[1, 2]);
    write_block_nums(&data.join("date=2024-01-01/part-b-000001.parquet"), &[3]);
    write_block_nums(&data.join("date=2024-01-02/part-a-000001.parquet"), &[4]);
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
        before.contains(&format!("date=2024-01-01/{JOURNAL_FILE}")),
        "{before:?}"
    );

    let registry = root.join("mainnet").join(default_registry());
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
        data.join("date=2024-01-01/part-a-000001.parquet")
            .to_str()
            .unwrap(),
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
    // The first day is complete once the stream moved to the second.
    let descriptor = protected_dataset(&root, &[&[100, 109], &[110]]);
    complete_request(&root, &descriptor, 111);
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
                    &[fill(registry_row("mainnet", "date=2024-01-01", "aa"))],
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
    remote_source_at(store, "mainnet/blocks")
}

fn remote_source_at(
    store: Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> super::super::DataSource {
    super::super::DataSource::Remote {
        path: format!("s3://bucket/{prefix}"),
        bucket: "bucket".to_string(),
        prefix: prefix.to_string(),
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
        "mainnet/blocks/date=2024-01-01/part-0.parquet",
        &[1, 2],
    );
    put_block_nums(
        store.as_ref(),
        "mainnet/blocks/date=2024-01-02/part-0.parquet",
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
            "mainnet/blocks/date=2024-01-01/part-0.parquet",
            &[1, 2, 2],
        )
    });
    let err = format!("{:#}", verify_remote(&store, &opts).unwrap_err());
    assert!(err.contains("was replaced"), "{err}");

    // A merge journal is refused before anything is read.
    super::super::block_on_async(store.put(
        &object_store::path::Path::from(format!("mainnet/blocks/date=2024-01-02/{JOURNAL_FILE}")),
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

/// `build --output s3://bucket` puts the protected
/// dataset at the bucket root, next to the bucket-wide owner record and the
/// root artifacts of other commands. verify reads the authority at the bucket
/// root and never scans those artifacts, whether it is given the table prefix
/// or the whole bucket.
#[test]
fn remote_protected_dataset_at_the_bucket_root_is_verified_beside_root_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let local = std::fs::canonicalize(dir.path()).unwrap().join("root");
    protected_dataset(&local, &[&[100, 101], &[110]]);
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    for file in tree(&local) {
        if file.ends_with(".parquet") || file == ".fireparq-ingest/state.json" {
            super::super::block_on_async(store.put(
                &object_store::path::Path::from(file.as_str()),
                object_store::PutPayload::from(std::fs::read(local.join(&file)).unwrap()),
            ))
            .unwrap();
        }
    }
    // Readable block_num files: scanning any of them would add a partition.
    put_block_nums(store.as_ref(), "_fireparq/cursor.parquet", &[994]);
    put_block_nums(store.as_ref(), "_fireparq/partitions.parquet", &[993]);
    put_block_nums(store.as_ref(), "_fireparq/merkle_roots.parquet", &[992]);
    put_block_nums(
        store.as_ref(),
        "_fireparq/verify_runs/a/roots.parquet",
        &[991],
    );
    // Legacy root artifacts of a release before v1.0.0 stay reserved.
    put_block_nums(store.as_ref(), "cursor.parquet", &[999]);
    put_block_nums(store.as_ref(), "partitions.parquet", &[998]);
    put_block_nums(store.as_ref(), "merkle_roots.parquet", &[997]);
    put_block_nums(store.as_ref(), "verify_runs/old/roots.parquet", &[996]);
    put_block_nums(
        store.as_ref(),
        ".fireparq-owner-probes-v1/probe.parquet",
        &[995],
    );
    for key in [
        crate::artifacts::OWNERSHIP_FILENAME,
        "verify_runs/old/report.json",
    ] {
        super::super::block_on_async(store.put(
            &object_store::path::Path::from(key),
            object_store::PutPayload::from_static(b"{}"),
        ))
        .unwrap();
    }
    let registry = dir.path().join("roots.parquet");
    let opts = roots_opts(&registry);
    for (prefix, recorded) in [("blocks", false), ("", true)] {
        let report = super::super::verify_source(
            &remote_source_at(store.clone(), prefix),
            None,
            &opts,
            time::OffsetDateTime::now_utc(),
            uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
        assert_eq!(report.table, "blocks", "{prefix:?}");
        assert_eq!(report.summary.partitions_scanned, 2, "{prefix:?}");
        assert_eq!(report.summary.open_partitions, 1, "{prefix:?}");
        assert_eq!(
            (report.summary.missing_expected, report.summary.matches),
            if recorded { (0, 1) } else { (1, 0) },
            "{prefix:?}"
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|w| w
                    .contains("the authoritative ingestion state of s3://bucket is at block 110")),
            "{prefix:?}: {:?}",
            report.warnings
        );
    }
    // Artifact destinations inside the bucket-root dataset keep the guards
    // they have below a chain directory. An allowed destination passes the
    // check and only then needs AWS settings to read the registry.
    let objects = || {
        super::super::block_on_async(async {
            use futures::TryStreamExt;
            store.list(None).try_collect::<Vec<_>>().await
        })
        .unwrap()
        .len()
    };
    for (registry, expected) in [
        ("s3://bucket/cursor.parquet", "protected recovery metadata"),
        (
            "s3://bucket/_fireparq/cursor.parquet",
            "protected recovery metadata",
        ),
        (
            "s3://bucket/blocks/date=2024-01-01/roots.parquet",
            "ordinary protected data part",
        ),
        ("s3://bucket/.fireparq-ingest/roots.parquet", "control path"),
        ("s3://bucket/merkle_roots.parquet", "AWS config required"),
        (
            "s3://bucket/_fireparq/merkle_roots.parquet",
            "AWS config required",
        ),
    ] {
        let mut opts = base_opts();
        opts.checks = vec![VerifyCheck::Roots];
        opts.registry_path = Some(registry.to_string());
        let before = objects();
        let err = super::super::verify_source(
            &remote_source_at(store.clone(), "blocks"),
            None,
            &opts,
            time::OffsetDateTime::now_utc(),
            uuid::Uuid::new_v4().to_string(),
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains(expected), "{registry}: {err:#}");
        assert_eq!(objects(), before, "{registry}");
    }
    // The default registry is `_fireparq/merkle_roots.parquet`. The bucket
    // still holds a legacy root `merkle_roots.parquet`, so a default run is
    // refused before the registry is read, whatever prefix is verified.
    for prefix in ["blocks", ""] {
        let mut opts = base_opts();
        opts.checks = vec![VerifyCheck::Roots];
        let before = objects();
        let err = super::super::verify_source(
            &remote_source_at(store.clone(), prefix),
            None,
            &opts,
            time::OffsetDateTime::now_utc(),
            uuid::Uuid::new_v4().to_string(),
        )
        .unwrap_err();
        let err = format!("{err:#}");
        assert!(
            err.contains("legacy merkle roots registry at s3://bucket/merkle_roots.parquet")
                && err.contains("s3://bucket/_fireparq/merkle_roots.parquet"),
            "{prefix:?}: {err}"
        );
        assert_eq!(objects(), before, "{prefix:?}");
    }
}

/// On S3, below a chain directory and at a bucket root, a default-registry
/// run refuses a legacy root registry and names the move; once it is moved,
/// verify reads the moved registry (and only then needs AWS settings).
#[test]
fn remote_legacy_root_registry_is_refused_instead_of_shadowed() {
    for (chain_prefix, table_prefix) in [("mainnet", "mainnet/blocks"), ("", "blocks")] {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        put_block_nums(
            store.as_ref(),
            &format!("{table_prefix}/date=2024-01-01/part-0.parquet"),
            &[1, 2],
        );
        let join = |name: &str| {
            if chain_prefix.is_empty() {
                name.to_string()
            } else {
                format!("{chain_prefix}/{name}")
            }
        };
        let legacy = join("merkle_roots.parquet");
        let moved = join("_fireparq/merkle_roots.parquet");
        put_block_nums(store.as_ref(), &legacy, &[1]);
        let mut opts = base_opts();
        opts.checks = vec![VerifyCheck::Roots];
        let run = || {
            super::super::verify_source(
                &remote_source_at(store.clone(), table_prefix),
                None,
                &opts,
                time::OffsetDateTime::now_utc(),
                uuid::Uuid::new_v4().to_string(),
            )
        };
        let err = format!("{:#}", run().unwrap_err());
        assert!(
            err.contains(&format!("s3://bucket/{legacy}"))
                && err.contains(&format!("s3://bucket/{moved}"))
                && err.contains("Move it there"),
            "{chain_prefix:?}: {err}"
        );
        // Moved: the legacy check passes and the run proceeds to the registry.
        let bytes = super::super::block_on_async(async {
            store
                .get(&object_store::path::Path::from(legacy.as_str()))
                .await
                .unwrap()
                .bytes()
                .await
        })
        .unwrap();
        super::super::block_on_async(async {
            store
                .put(
                    &object_store::path::Path::from(moved.as_str()),
                    bytes.into(),
                )
                .await
                .unwrap();
            store
                .delete(&object_store::path::Path::from(legacy.as_str()))
                .await
                .unwrap();
        });
        let err = format!("{:#}", run().unwrap_err());
        assert!(
            err.contains("AWS config required"),
            "{chain_prefix:?}: {err}"
        );
    }
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
    assert!(!root.join("mainnet").join(default_registry()).exists());
}

#[test]
fn the_frontier_is_read_before_the_scanned_listing() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = legacy_fixture(&root);
    let chain_root = root.join("mainnet");
    save_cursor(&chain_root, 4, None);
    // `build` commits blocks 5 and 6 after verify read the frontier but
    // before it listed the files: they are in the scan, so they must be open.
    let (hooked_data, hooked_root) = (data.clone(), chain_root.clone());
    at("after-frontier", move || {
        write_block_nums(&hooked_data.join("date=2024-01-03/part-0.parquet"), &[5, 6]);
        save_cursor(&hooked_root, 6, None);
    });
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert!(
        report.warnings.iter().any(|w| w.contains("is at block 4")),
        "{:?}",
        report.warnings
    );
    assert_eq!(
        partitions(&report, "open"),
        ["date=2024-01-02", "date=2024-01-03"]
    );
    assert_eq!(partitions(&report, "missing_expected"), ["date=2024-01-01"]);
}

#[test]
fn a_merge_that_starts_during_the_scan_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = legacy_fixture(&root);
    let hooked = data.clone();
    after_scan(move || {
        std::fs::write(hooked.join("date=2024-01-01").join(JOURNAL_FILE), b"{}").unwrap()
    });
    let err = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap_err();
    assert!(format!("{err:#}").contains("unfinished merge"), "{err:#}");
    assert!(!root.join("mainnet").join(default_registry()).exists());
}

#[test]
fn files_that_vanish_are_tolerated_only_in_open_partitions() {
    let fixture = |root: &Path| {
        let data = root.join("mainnet/blocks");
        write_block_nums(&data.join("date=2024-01-01/part-0.parquet"), &[1]);
        write_block_nums(&data.join("date=2024-01-01/part-1.parquet"), &[2]);
        write_block_nums(&data.join("date=2024-01-02/part-0.parquet"), &[3, 4]);
        write_block_nums(&data.join("date=2024-01-02/part-1.parquet"), &[5]);
        save_cursor(&root.join("mainnet"), 4, None);
        data
    };
    // A restarted build rolls back an uncommitted part in an open partition
    // while verify is listing: harmless.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = fixture(&root);
    let hooked = data.clone();
    at("before-scan", move || {
        std::fs::remove_file(hooked.join("date=2024-01-02/part-1.parquet")).unwrap()
    });
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(partitions(&report, "open"), ["date=2024-01-02"]);
    assert_eq!(partitions(&report, "missing_expected"), ["date=2024-01-01"]);
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("disappeared") && w.contains("date=2024-01-02/part-1.parquet")),
        "{:?}",
        report.warnings
    );

    // A file of a closed partition that disappears fails the run.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = fixture(&root);
    let hooked = data.clone();
    at("before-scan", move || {
        std::fs::remove_file(hooked.join("date=2024-01-01/part-1.parquet")).unwrap()
    });
    let err = format!(
        "{:#}",
        verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap_err()
    );
    assert!(err.contains("changed while verify was reading"), "{err}");
    assert!(err.contains("date=2024-01-01"), "{err}");
    assert!(!root.join("mainnet").join(default_registry()).exists());
}

#[test]
fn a_reversible_stream_leaves_every_partition_open_while_it_can_grow() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap().join("mainnet");
    let descriptor = protected_root(&root, false);
    commit_windows(&root, &descriptor, &[&[100, 101], &[110]]);
    let report = verify_parquet(root.join("blocks").to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(partitions(&report, "open"), [day(1), day(2)]);
    assert!(report.findings[0]
        .error
        .as_deref()
        .unwrap()
        .contains("reversible"));
    assert!(!report.summary.wrote_registry);
}

#[test]
fn a_protected_dataset_without_a_committed_block_leaves_everything_open() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap().join("mainnet");
    protected_root(&root, true);
    // A first transaction published a part but has not committed.
    let data = root.join("blocks");
    write_block_nums(&data.join(day(1)).join("part-pending.parquet"), &[100]);
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(partitions(&report, "open"), [day(1)]);
    assert!(report.findings[0]
        .error
        .as_deref()
        .unwrap()
        .contains("has no committed block yet"));
}

/// #652: a protected dataset from a pre-release build (semantic mapper epoch
/// `v1`, whose partitions used another layout) cannot tell `verify` which
/// partitions are open, so it is refused with the reason instead of guessed.
#[test]
fn a_pre_release_protected_root_is_refused_with_its_epoch() {
    use crate::durable_state::{ControlKey, LocalStateStore};
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap().join("mainnet");
    protected_dataset(&root, &[&[100, 101]]);
    super::super::block_on_async(async {
        let ownership = DatasetOwnership::acquire(
            "fixture",
            vec![MutationScope::directory(root.to_string_lossy())],
            None,
        )
        .await
        .unwrap();
        let store = LocalStateStore::new(&root, ownership.local().unwrap()).unwrap();
        let record = store
            .load::<AuthorityState>(ControlKey::State)
            .unwrap()
            .unwrap();
        let mut legacy = record.payload.clone();
        legacy.descriptor.mapper_epoch = "fireparq-mapping-v1".into();
        store
            .replace(ControlKey::State, &record.version, &legacy)
            .unwrap();
        ownership.release().await.unwrap();
    });
    let err = format!(
        "{:#}",
        verify_parquet(root.join("blocks").to_str().unwrap(), None, &base_opts()).unwrap_err()
    );
    assert!(
        err.contains("semantic mapper epoch `fireparq-mapping-v1`")
            && err.contains("date=YYYY-MM-DD"),
        "{err}"
    );
    assert!(!root.join(default_registry()).exists());
}

#[test]
fn a_protected_marker_without_authoritative_state_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = legacy_fixture(&root);
    std::fs::create_dir(root.join("mainnet/.fireparq-ingest")).unwrap();
    let err = format!(
        "{:#}",
        verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap_err()
    );
    assert!(err.contains("no authoritative ingestion state"), "{err}");
    assert!(!root.join("mainnet").join(default_registry()).exists());
}

#[test]
fn an_unreadable_legacy_cursor_leaves_every_partition_open() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = legacy_fixture(&root);
    std::fs::write(root.join("mainnet/cursor.parquet"), b"not parquet").unwrap();
    let report = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(
        partitions(&report, "open"),
        ["date=2024-01-01", "date=2024-01-02"]
    );
    assert!(report.warnings.iter().any(|w| w.contains("could not read")));
    assert!(!report.summary.wrote_registry);
    assert!(report.is_valid());
}

#[test]
fn a_published_protocol_report_needs_every_file_it_read() {
    let run = |rewrite: fn(&Path), report: bool| {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let data = legacy_fixture(&root);
        let mut opts = base_opts();
        opts.checks = vec![VerifyCheck::Protocol];
        if report {
            opts.report_json = Some(root.join("report.json"));
        }
        let hooked = data.clone();
        after_scan(move || rewrite(&hooked));
        verify_parquet(data.to_str().unwrap(), None, &opts).map(|_| ())
    };
    let remove: fn(&Path) =
        |data| std::fs::remove_file(data.join("date=2024-01-02/part-0.parquet")).unwrap();
    let add: fn(&Path) =
        |data| write_block_nums(&data.join("date=2024-01-01/part-1.parquet"), &[2]);
    let err = format!("{:#}", run(remove, true).unwrap_err());
    assert!(err.contains("was removed"), "{err}");
    // Added files do not invalidate what was read.
    run(add, true).unwrap();
    // A protocol run without output stays observational.
    run(remove, false).unwrap();
}

#[test]
fn a_partition_directory_or_file_is_recorded_under_its_own_partition() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let data = legacy_fixture(&root);
    let table = verify_parquet(data.to_str().unwrap(), None, &base_opts()).unwrap();
    assert_eq!(
        partitions(&table, "missing_expected"),
        ["date=2024-01-01", "date=2024-01-02"]
    );
    let partition = verify_parquet(
        data.join("date=2024-01-01").to_str().unwrap(),
        None,
        &base_opts(),
    )
    .unwrap();
    assert_eq!(partitions(&partition, "match"), ["date=2024-01-01"]);
    let file = verify_parquet(
        data.join("date=2024-01-02/part-0.parquet")
            .to_str()
            .unwrap(),
        None,
        &base_opts(),
    )
    .unwrap();
    assert_eq!(partitions(&file, "match"), ["date=2024-01-02"]);
    assert_eq!(
        registry_partitions(&root.join("mainnet").join(default_registry())),
        ["date=2024-01-01", "date=2024-01-02"]
    );
}

#[test]
fn artifact_destinations_are_checked_before_any_row_is_read() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap().join("mainnet");
    protected_dataset(&root, &[&[100, 101]]);
    let scanned = std::rc::Rc::new(std::cell::Cell::new(false));
    let flag = scanned.clone();
    at("before-scan", move || flag.set(true));
    let mut opts = base_opts();
    opts.registry_path = Some(root.join("cursor.parquet").display().to_string());
    let err = verify_parquet(root.join("blocks").to_str().unwrap(), None, &opts).unwrap_err();
    assert!(format!("{err:#}").contains("protected recovery metadata"));
    assert!(!scanned.get(), "the destination is refused before the scan");
    super::super::TEST_HOOKS.with(|hooks| hooks.borrow_mut().clear());
}

#[test]
fn remote_reads_are_pinned_to_the_listed_object() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    put_block_nums(
        store.as_ref(),
        "mainnet/blocks/date=2024-01-01/part-0.parquet",
        &[1, 2],
    );
    put_block_nums(
        store.as_ref(),
        "mainnet/blocks/date=2024-01-02/part-0.parquet",
        &[3, 4],
    );

    // An object replaced after the listing is not read: the GET carries the
    // listed ETag, so the new content is never hashed under the old identity.
    let source = remote_source(store.clone());
    let listing = source
        .list(&super::super::ExcludedPaths::default())
        .unwrap();
    put_block_nums(
        store.as_ref(),
        "mainnet/blocks/date=2024-01-01/part-0.parquet",
        &[1, 2, 2],
    );
    let scan = source.scan(&listing, &base_opts(), None).unwrap();
    assert_eq!(
        scan.vanished.keys().collect::<Vec<_>>(),
        ["date=2024-01-01"],
        "the pinned read of the replaced object failed"
    );

    // End to end, the run fails before anything is compared or written.
    let registry = dir.path().join("roots.parquet");
    let hooked = store.clone();
    at("before-scan", move || {
        put_block_nums(
            hooked.as_ref(),
            "mainnet/blocks/date=2024-01-01/part-0.parquet",
            &[1, 2, 2, 2],
        )
    });
    let err = format!(
        "{:#}",
        verify_remote(&store, &roots_opts(&registry)).unwrap_err()
    );
    assert!(err.contains("changed while verify was reading"), "{err}");
    assert!(err.contains("date=2024-01-01"), "{err}");
    assert!(!registry.exists());
}
