//! Bounded concurrent table work inside one transaction (#516 stage A).
use super::*;
use crate::durable_state::CONTROL_DIRECTORY;
use crate::ingest::controller::pipeline::tests::set_fault;
use arrow::array::BinaryArray;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    path::Path as ObjectPath, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use sha2::{Digest as _, Sha256};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

const PREFIX: &str = "dataset";

/// Incompressible, deterministic payloads make encoded sizes predictable.
fn payload(table: &str, row: usize, len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    let mut counter = 0u64;
    while bytes.len() < len {
        let mut hash = Sha256::new();
        hash.update(table.as_bytes());
        hash.update(row.to_le_bytes());
        hash.update(counter.to_le_bytes());
        bytes.extend_from_slice(&hash.finalize());
        counter += 1;
    }
    bytes.truncate(len);
    bytes
}

/// `(table, rows, bytes per row)`, each table with `block_num` and `payload`.
fn batches(tables: &[(&str, usize, usize)]) -> HashMap<String, RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new("payload", DataType::Binary, false),
    ]));
    tables
        .iter()
        .map(|(table, rows, width)| {
            let values: Vec<Vec<u8>> = (0..*rows).map(|row| payload(table, row, *width)).collect();
            let numbers: Vec<u64> = (0..*rows).map(|row| 100 + (row % 2) as u64).collect();
            (
                table.to_string(),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from(numbers)),
                        Arc::new(BinaryArray::from_iter_values(values.iter())),
                    ],
                )
                .unwrap(),
            )
        })
        .collect()
}

fn descriptor_for(
    output: StorageIdentity,
    data: &HashMap<String, RecordBatch>,
) -> StreamDescriptor {
    let mut descriptor = descriptor(RoutingPolicy::DirectV1);
    descriptor.output = output;
    descriptor.partition = PartitionPolicy::None;
    descriptor.tables = data
        .iter()
        .map(|(table, batch)| {
            (
                table.clone(),
                Digest::parse(schema_sha256(batch.schema().as_ref()).unwrap()).unwrap(),
            )
        })
        .collect();
    descriptor
}

fn s3_output() -> StorageIdentity {
    StorageIdentity::S3 {
        service: Digest::hash("service", &"fixture").unwrap(),
        bucket: "bucket".into(),
        prefix: PREFIX.into(),
    }
}

/// In-memory S3 with request latency. For every data PUT it records how many
/// part PUTs run at once and checks that the journal already holds that
/// part's receipt: publication must never precede its durable receipt.
#[derive(Default)]
struct Remote {
    inner: InMemory,
    put_delay_ms: u64,
    /// Benchmark-only model: every request waits this long plus transfer time.
    request_latency_ms: u64,
    bytes_per_ms: u64,
    running: AtomicUsize,
    peak: AtomicUsize,
    part_puts: AtomicUsize,
    journal_puts: AtomicUsize,
    violations: Mutex<Vec<String>>,
    lose_ack_suffix: Mutex<Option<String>>,
}
impl std::fmt::Display for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("concurrency-fixture")
    }
}
impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("concurrency-fixture")
    }
}
impl Remote {
    async fn transfer(&self, bytes: u64) {
        if self.request_latency_ms == 0 && self.bytes_per_ms == 0 {
            return;
        }
        let transfer = bytes.checked_div(self.bytes_per_ms).unwrap_or(0);
        tokio::time::sleep(Duration::from_millis(self.request_latency_ms + transfer)).await;
    }
    async fn receipt_is_durable(&self, key: &str) -> bool {
        let pending = ObjectPath::from(format!("{PREFIX}/{CONTROL_DIRECTORY}/pending.json"));
        let Ok(response) = self.inner.get(&pending).await else {
            return false;
        };
        let record: serde_json::Value =
            serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        record["payload"]["parts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|part| {
                format!("{PREFIX}/{}", part["final_relative_path"].as_str().unwrap()) == key
                    && !part["receipt"].is_null()
            })
    }
    async fn objects(&self) -> BTreeMap<String, Vec<u8>> {
        let mut objects = BTreeMap::new();
        let mut listing = self.inner.list(Some(&ObjectPath::from(PREFIX)));
        while let Some(meta) = futures::StreamExt::next(&mut listing).await {
            let meta = meta.unwrap();
            if meta.location.as_ref().ends_with(".parquet") {
                let bytes = self.inner.get(&meta.location).await.unwrap().bytes().await;
                objects.insert(meta.location.to_string(), bytes.unwrap().to_vec());
            }
        }
        objects
    }
}
#[async_trait]
impl ObjectStore for Remote {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let key = path.to_string();
        if !key.ends_with(".parquet") {
            if key.ends_with("/pending.json") {
                self.journal_puts.fetch_add(1, Ordering::SeqCst);
            }
            self.transfer(payload.content_length() as u64).await;
            return self.inner.put_opts(path, payload, opts).await;
        }
        self.part_puts.fetch_add(1, Ordering::SeqCst);
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        self.transfer(payload.content_length() as u64).await;
        if !self.receipt_is_durable(&key).await {
            self.violations.lock().unwrap().push(key.clone());
        }
        tokio::time::sleep(Duration::from_millis(self.put_delay_ms)).await;
        let result = self.inner.put_opts(path, payload, opts).await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        let lose = self
            .lose_ack_suffix
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|suffix| key.contains(suffix));
        if lose {
            result?;
            return Err(object_store::Error::Generic {
                store: "fixture",
                source: "acknowledgement lost after acceptance".into(),
            });
        }
        result
    }
    async fn get_opts(
        &self,
        path: &ObjectPath,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(path, opts).await?;
        self.transfer(result.range.end - result.range.start).await;
        Ok(result)
    }
    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(path).await
    }
    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
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

struct RemoteRun {
    backend: Arc<Remote>,
    result: Result<CommittedFlush>,
    ordinal: u64,
    mirror: Option<Checkpoint>,
}

/// Commit one transaction of `tables` to a fresh in-memory S3 dataset.
async fn remote_commit(
    tables: &[(&str, usize, usize)],
    concurrency: FlushConcurrency,
    put_delay_ms: u64,
) -> RemoteRun {
    let backend = Arc::new(Remote {
        put_delay_ms,
        ..Default::default()
    });
    let store: Arc<dyn ObjectStore> = backend.clone();
    let owner = S3Ownership::acquire(store, "test-ingest", vec![PREFIX.into()])
        .await
        .unwrap();
    let data = batches(tables);
    let descriptor = descriptor_for(s3_output(), &data);
    let states = TransactionStateStore::s3(PREFIX, &owner).unwrap();
    states
        .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
        .await
        .unwrap();
    let mirror = Mirror::default();
    let mut controller = TransactionController::open(
        states,
        TransactionParts::s3(PREFIX, &owner, "").unwrap(),
        &mirror,
        &descriptor,
    )
    .await
    .unwrap()
    .with_concurrency(concurrency)
    .unwrap();
    let result = controller
        .commit(
            prefix(controller.authority()),
            data,
            metadata(),
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await;
    let ordinal = controller.authority().checkpoint.ordinal;
    drop(controller);
    let mirror = mirror.head.borrow().clone();
    RemoteRun {
        backend,
        result,
        ordinal,
        mirror,
    }
}

const TABLES: &[(&str, usize, usize)] = &[
    ("slow_a", 64, 4096),
    ("slow_b", 32, 2048),
    ("slow_c", 96, 4096),
    ("slow_d", 16, 1024),
    ("slow_e", 80, 3072),
    ("slow_f", 48, 4096),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_commit_publishes_serial_bytes_with_bounded_work() {
    let serial = remote_commit(TABLES, FlushConcurrency::SERIAL, 30).await;
    let committed = serial.result.unwrap();
    assert_eq!(serial.backend.peak.load(Ordering::SeqCst), 1);
    assert_eq!(
        (
            committed.work.peak_encoders,
            committed.work.peak_publications
        ),
        (1, 1)
    );
    let limits = FlushConcurrency {
        encoders: 4,
        publications: 3,
        inflight_bytes: 64 * 1024 * 1024,
    };
    let parallel = remote_commit(TABLES, limits, 30).await;
    let work = parallel.result.unwrap().work;
    // Every receipt was durable before its PUT, in both modes.
    assert!(serial.backend.violations.lock().unwrap().is_empty());
    assert!(parallel.backend.violations.lock().unwrap().is_empty());
    assert_eq!(
        parallel.backend.part_puts.load(Ordering::SeqCst),
        TABLES.len()
    );
    // Independently measured at the store, and inside the executors.
    let store_peak = parallel.backend.peak.load(Ordering::SeqCst);
    assert!((2..=3).contains(&store_peak), "store peak {store_peak}");
    assert!((2..=4).contains(&work.peak_encoders), "{work:?}");
    assert!(work.peak_publications <= 3, "{work:?}");
    assert!(
        work.peak_inflight_bytes <= limits.inflight_bytes,
        "{work:?}"
    );
    assert_eq!((work.reencoded_parts, work.oversized_parts), (0, 0));
    // Identical deterministic names and bytes, and the same checkpoint.
    assert_eq!(
        parallel.backend.objects().await,
        serial.backend.objects().await
    );
    assert_eq!(parallel.backend.objects().await.len(), TABLES.len());
    assert_eq!((serial.ordinal, parallel.ordinal), (2, 2));
    assert_eq!(serial.mirror.unwrap().id, parallel.mirror.unwrap().id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_budget_bounds_inflight_parts_and_reencodes_refused_growth() {
    // Incompressible parts of ~1 MiB each against a 2.5 MiB budget: initial
    // reservations under-estimate, so some growth is refused and those parts
    // are encoded again alone. Reserved bytes never exceed the budget.
    let tables: &[(&str, usize, usize)] = &[
        ("slow_big_a", 256, 4096),
        ("slow_big_b", 256, 4096),
        ("slow_big_c", 256, 4096),
        ("slow_big_d", 256, 4096),
    ];
    let limits = FlushConcurrency {
        encoders: 4,
        publications: 4,
        inflight_bytes: 5 * 512 * 1024,
    };
    let serial = remote_commit(tables, FlushConcurrency::SERIAL, 5).await;
    let bounded = remote_commit(tables, limits, 5).await;
    let work = bounded.result.unwrap().work;
    assert!(
        work.peak_inflight_bytes <= limits.inflight_bytes,
        "{work:?}"
    );
    assert!(work.reencoded_parts >= 1, "{work:?}");
    assert_eq!(work.oversized_parts, 0, "{work:?}");
    assert!(bounded.backend.violations.lock().unwrap().is_empty());
    serial.result.unwrap();
    assert_eq!(
        bounded.backend.objects().await,
        serial.backend.objects().await
    );

    // One part larger than the whole budget is admitted alone, with an
    // explicit overshoot, and the transaction still commits identically.
    let tables: &[(&str, usize, usize)] = &[
        ("slow_huge", 1024, 4096),
        ("slow_small_a", 8, 512),
        ("slow_small_b", 8, 512),
    ];
    let limits = FlushConcurrency {
        encoders: 3,
        publications: 3,
        inflight_bytes: 1024 * 1024,
    };
    let serial = remote_commit(tables, FlushConcurrency::SERIAL, 5).await;
    let bounded = remote_commit(tables, limits, 5).await;
    let work = bounded.result.unwrap().work;
    assert_eq!(work.oversized_parts, 1, "{work:?}");
    assert!(work.peak_inflight_bytes > limits.inflight_bytes, "{work:?}");
    serial.result.unwrap();
    assert_eq!(
        bounded.backend.objects().await,
        serial.backend.objects().await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_lost_acknowledgement_drains_parallel_work_without_advancing_authority() {
    let backend_suffix = "-2.parquet";
    let backend = Arc::new(Remote {
        put_delay_ms: 20,
        lose_ack_suffix: Mutex::new(Some(backend_suffix.into())),
        ..Default::default()
    });
    let store: Arc<dyn ObjectStore> = backend.clone();
    let owner = S3Ownership::acquire(store, "test-ingest", vec![PREFIX.into()])
        .await
        .unwrap();
    let data = batches(TABLES);
    let descriptor = descriptor_for(s3_output(), &data);
    let states = TransactionStateStore::s3(PREFIX, &owner).unwrap();
    states
        .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
        .await
        .unwrap();
    let mirror = Mirror::default();
    let mut controller = TransactionController::open(
        states,
        TransactionParts::s3(PREFIX, &owner, "").unwrap(),
        &mirror,
        &descriptor,
    )
    .await
    .unwrap()
    .with_concurrency(FlushConcurrency {
        encoders: 4,
        publications: 3,
        inflight_bytes: 64 * 1024 * 1024,
    })
    .unwrap();
    let error = controller
        .commit(
            prefix(controller.authority()),
            data,
            metadata(),
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await
        .err()
        .unwrap();
    assert!(!format!("{error:#}").contains("acknowledgement lost after acceptance"));
    assert_eq!(controller.authority().checkpoint.ordinal, 0);
    // The mirror still reflects only the initial authority.
    assert_eq!(mirror.head.borrow().as_ref().unwrap().ordinal, 0);
    // Every started PUT drained before the error returned; no request remains.
    assert_eq!(backend.running.load(Ordering::SeqCst), 0);
    assert!(backend.violations.lock().unwrap().is_empty());
    // The owner stays retained for quiescent recovery and the journal remains.
    assert!(owner.is_mutation_uncertain());
    drop(controller);
    let snapshot = TransactionStateStore::s3(PREFIX, &owner)
        .unwrap()
        .load()
        .await
        .unwrap();
    assert_eq!(
        snapshot.pending.unwrap().payload.phase,
        TransactionPhase::Writing
    );
    assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 0);
    assert!(owner.release().await.is_err());
}

fn local_data_files(root: &Path) -> BTreeMap<String, usize> {
    let mut rows = BTreeMap::new();
    for path in data_files(root) {
        let table = path
            .strip_prefix(root)
            .unwrap()
            .components()
            .next()
            .unwrap()
            .as_os_str()
            .to_string_lossy()
            .into_owned();
        let count: usize = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum();
        *rows.entry(table).or_default() += count;
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_encode_publish_and_lost_ack_failures_recover_rows_exactly_once() {
    let limits = FlushConcurrency {
        encoders: 4,
        publications: 3,
        inflight_bytes: 64 * 1024 * 1024,
    };
    for (kind, target) in [
        ("encode", "fault_encode_target"),
        ("publish", "fault_publish_target"),
        ("lost-ack", "fault_lost_ack_target"),
    ] {
        let tables: Vec<(&str, usize, usize)> = vec![
            ("slow_first", 40, 2048),
            (target, 30, 1024),
            ("slow_last", 50, 2048),
        ];
        let expected: BTreeMap<String, usize> = tables
            .iter()
            .map(|(table, rows, _)| (table.to_string(), *rows))
            .collect();
        let root = tempfile::tempdir().unwrap();
        let data = batches(&tables);
        let descriptor = descriptor_for(
            StorageIdentity::Local {
                canonical_root: fs::canonicalize(root.path())
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .into(),
            },
            &data,
        );
        let mirror = Mirror::default();
        let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
        initialize(root.path(), &owner, &descriptor).await;
        let mut controller = open(root.path(), &owner, &mirror, &descriptor)
            .await
            .unwrap()
            .with_concurrency(limits)
            .unwrap();
        set_fault(Some((kind, target)));
        let error = controller
            .commit(
                prefix(controller.authority()),
                data,
                metadata(),
                Compression::Zstd,
                ParquetFileMetadata::new(),
            )
            .await
            .err()
            .expect("injected fault must fail the transaction");
        set_fault(None);
        assert!(
            format!("{error:#}").contains("injected debug fault"),
            "{kind}: {error:#}"
        );
        assert_eq!(controller.authority().checkpoint.ordinal, 0, "{kind}");
        assert_eq!(mirror.head.borrow().as_ref().unwrap().ordinal, 0, "{kind}");
        assert!(staged_temporaries(root.path()).is_empty(), "{kind}");
        let published = local_data_files(root.path());
        if kind == "lost-ack" {
            // Published, but never acknowledged: owned by the Writing journal.
            assert_eq!(published.get(target), Some(&30), "{kind}");
        } else {
            assert!(!published.contains_key(target), "{kind}");
        }
        assert_eq!(
            TransactionStateStore::local(root.path(), &owner)
                .unwrap()
                .load()
                .await
                .unwrap()
                .pending
                .unwrap()
                .payload
                .phase,
            TransactionPhase::Writing,
            "{kind}"
        );
        drop(controller);
        // Recovery rolls back every owned part before the replayed commit.
        let mut controller = open(root.path(), &owner, &mirror, &descriptor)
            .await
            .unwrap()
            .with_concurrency(limits)
            .unwrap();
        assert!(data_files(root.path()).is_empty(), "{kind}");
        controller
            .commit(
                prefix(controller.authority()),
                batches(&tables),
                metadata(),
                Compression::Zstd,
                ParquetFileMetadata::new(),
            )
            .await
            .unwrap();
        assert_eq!(local_data_files(root.path()), expected, "{kind}");
        assert_eq!(controller.authority().checkpoint.ordinal, 2, "{kind}");
    }
}

/// Replays the transactions of a protected local dataset (for example one
/// written by `blocks/examples/bench_ingestion_concurrency.rs`) into in-memory
/// S3 with modeled request latency and bandwidth, once per concurrency setting.
/// Every setting must produce identical objects. Reports per-commit latency.
///
/// `FIREPARQ_516_DATASET=<root> cargo test -p firehose-parquet --lib --release \
///  s3_replay_benchmark -- --ignored --nocapture`, optionally with
/// `FIREPARQ_516_LATENCY_MS` (default 25), `FIREPARQ_516_MBPS` (default 100) and
/// `FIREPARQ_516_SETTINGS` (default `1:1:1,1:1,2:4,4:4,8:8`, ENCODE:PUBLISH[:BYTES]).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark; requires FIREPARQ_516_DATASET"]
async fn s3_replay_benchmark() {
    let root = PathBuf::from(std::env::var("FIREPARQ_516_DATASET").expect("FIREPARQ_516_DATASET"));
    let latency: u64 =
        std::env::var("FIREPARQ_516_LATENCY_MS").map_or(25, |value| value.parse().unwrap());
    let mbps: u64 = std::env::var("FIREPARQ_516_MBPS").map_or(100, |value| value.parse().unwrap());
    let settings =
        std::env::var("FIREPARQ_516_SETTINGS").unwrap_or_else(|_| "1:1:1,1:1,2:4,4:4,8:8".into());

    // Group parts by transaction: part-v1-<stream>-<first>-<last>-<tx>-<index>.
    let mut transactions: BTreeMap<(u64, u64, String), HashMap<String, RecordBatch>> =
        BTreeMap::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if !path.file_name().unwrap().to_string_lossy().starts_with('.') {
                    stack.push(path);
                }
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let Some(rest) = name
                .strip_prefix("part-v1-")
                .and_then(|rest| rest.strip_suffix(".parquet"))
            else {
                continue;
            };
            let fields: Vec<_> = rest.split('-').collect();
            let (first, last, transaction) = (
                fields[1].parse::<u64>().unwrap(),
                fields[2].parse::<u64>().unwrap(),
                fields[3].to_string(),
            );
            let table = path
                .strip_prefix(&root)
                .unwrap()
                .components()
                .next()
                .unwrap()
                .as_os_str()
                .to_string_lossy()
                .into_owned();
            let batches: Vec<RecordBatch> =
                ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&path).unwrap())
                    .unwrap()
                    .build()
                    .unwrap()
                    .map(|batch| batch.unwrap())
                    .collect();
            // Drop footer keys the reader merges into schema metadata.
            let schema = Arc::new(Schema::new(batches[0].schema().fields().clone()));
            let batch = arrow::compute::concat_batches(
                &schema,
                &batches
                    .iter()
                    .map(|batch| batch.clone().with_schema(schema.clone()).unwrap())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            transactions
                .entry((first, last, transaction))
                .or_default()
                .insert(table, batch);
        }
    }
    assert!(
        !transactions.is_empty(),
        "no protected parts under {root:?}"
    );
    let mut inventory: HashMap<String, RecordBatch> = HashMap::new();
    for batches in transactions.values() {
        for (table, batch) in batches {
            inventory
                .entry(table.clone())
                .or_insert_with(|| batch.slice(0, 0));
        }
    }
    let descriptor = descriptor_for(s3_output(), &inventory);
    let parts: usize = transactions.values().map(HashMap::len).sum();
    let bytes: u64 = transactions
        .values()
        .flat_map(HashMap::values)
        .map(|batch| batch.get_array_memory_size() as u64)
        .sum();
    eprintln!(
        "{}",
        serde_json::json!({"transactions": transactions.len(), "parts": parts,
            "arrow_bytes": bytes, "latency_ms": latency, "mbps": mbps})
    );

    let mut reference: Option<BTreeMap<String, Vec<u8>>> = None;
    for setting in settings.split(',') {
        let values: Vec<u64> = setting.split(':').map(|v| v.parse().unwrap()).collect();
        let limits = FlushConcurrency {
            encoders: values[0] as usize,
            publications: values[1] as usize,
            inflight_bytes: values
                .get(2)
                .copied()
                .unwrap_or(crate::config::DEFAULT_FLUSH_INFLIGHT_BYTES),
        };
        let backend = Arc::new(Remote {
            request_latency_ms: latency,
            bytes_per_ms: mbps * 1_000,
            ..Default::default()
        });
        let store: Arc<dyn ObjectStore> = backend.clone();
        let owner = S3Ownership::acquire(store, "test-ingest", vec![PREFIX.into()])
            .await
            .unwrap();
        let states = TransactionStateStore::s3(PREFIX, &owner).unwrap();
        states
            .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
            .await
            .unwrap();
        let mirror = Mirror::default();
        let mut controller = TransactionController::open(
            states,
            TransactionParts::s3(PREFIX, &owner, "").unwrap(),
            &mirror,
            &descriptor,
        )
        .await
        .unwrap()
        .with_concurrency(limits)
        .unwrap();
        let started = std::time::Instant::now();
        let mut commits = Vec::new();
        let mut peaks = (0, 0, 0u64);
        for ((first, last, _), batches) in &transactions {
            let mut frontier = AcceptedFrontier::resume(&controller.authority().checkpoint);
            assert!(*first > controller.authority().checkpoint.ordinal);
            // Zero-row windows left no part; accept their ordinals here too.
            for ordinal in controller.authority().checkpoint.ordinal + 1..=*last {
                let received = frontier.receive(event(26_049_574 + ordinal, 1)).unwrap();
                frontier
                    .accept(received, routing(RoutingPolicy::DirectV1))
                    .unwrap();
            }
            let committed = controller
                .commit(
                    frontier.snapshot().unwrap().unwrap(),
                    batches.clone(),
                    BlockMetadata {
                        min_block_number: 26_049_574 + first,
                        max_block_number: 26_049_574 + last,
                        min_timestamp: None,
                        max_timestamp: None,
                    },
                    Compression::Zstd,
                    ParquetFileMetadata::new(),
                )
                .await
                .unwrap();
            commits.push(committed.elapsed.as_secs_f64() * 1000.0);
            peaks.0 = peaks.0.max(committed.work.peak_encoders);
            peaks.1 = peaks.1.max(committed.work.peak_publications);
            peaks.2 = peaks.2.max(committed.work.peak_inflight_bytes);
        }
        let total = started.elapsed().as_secs_f64();
        drop(controller);
        let objects = backend.objects().await;
        let identical = match &reference {
            Some(expected) => *expected == objects,
            None => {
                reference = Some(objects);
                true
            }
        };
        assert!(identical, "setting {setting} produced different objects");
        commits.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "{}",
            serde_json::json!({
                "setting": setting,
                "total_seconds": total,
                "commit_ms": commits,
                "commit_ms_p50": commits[commits.len() / 2],
                "part_puts": backend.part_puts.load(Ordering::SeqCst),
                "journal_puts": backend.journal_puts.load(Ordering::SeqCst),
                "peak_concurrent_part_puts": backend.peak.load(Ordering::SeqCst),
                "peak_encoders": peaks.0,
                "peak_publications": peaks.1,
                "peak_inflight_bytes": peaks.2,
                "identical_objects": identical,
            })
        );
        owner.release().await.unwrap();
    }
}
