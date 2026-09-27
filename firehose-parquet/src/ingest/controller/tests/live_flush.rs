//! #658: in-process phase timing of real protected transactions.
//!
//! Replays the committed transactions of a protected local dataset (for
//! example one written by `blocks/examples/bench_live_flush` from Robinhood or
//! Arbitrum blocks) back to back through the real controller, into local disk
//! or into the native loopback S3 fixture with injected per-request latency,
//! and records when each transaction boundary is reached. The cursor mirror is
//! the in-memory test double here; the real-binary benchmark includes it.
//!
//! `FIREPARQ_658_DATASET=<root> cargo test -p firehose-parquet --lib --release \
//!  live_flush_phase_benchmark -- --ignored --nocapture`, optionally with
//! `FIREPARQ_658_STORAGE` (default `local,s3`), `FIREPARQ_658_LATENCY_MS`
//! (default `0,10,30,80`), `FIREPARQ_658_SLOW_EVERY` (0 = off; slow requests
//! take 1 s), `FIREPARQ_658_SETTINGS` (default `2:4,2:8,2:16,4:16`,
//! ENCODE:PUBLISH) and `FIREPARQ_658_LIMIT` (transactions per run).
use super::concurrency::descriptor_for;
use super::*;
use crate::s3::upload::fixture::Server;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Batches of each committed transaction, keyed by `(first, last, id)`.
pub(super) type Transactions = BTreeMap<(u64, u64, String), HashMap<String, RecordBatch>>;

/// Group the parts of a protected dataset by transaction, from their names:
/// `part-v1-<stream>-<first>-<last>-<transaction>-<index>.parquet`.
pub(super) fn protected_transactions(root: &Path) -> Transactions {
    let mut transactions = Transactions::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                if !name.starts_with('.') && name != "_fireparq" {
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
                .strip_prefix(root)
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
    transactions
}

/// One zero-row batch per table ever written, for the stream descriptor.
pub(super) fn inventory(transactions: &Transactions) -> HashMap<String, RecordBatch> {
    let mut inventory = HashMap::new();
    for batches in transactions.values() {
        for (table, batch) in batches {
            inventory
                .entry(table.clone())
                .or_insert_with(|| batch.slice(0, 0));
        }
    }
    inventory
}

/// A block time inside the transaction's `date=` partition, from the `date`
/// column its rows carry (commits route every table by that date).
fn routing_seconds(batches: &HashMap<String, RecordBatch>) -> i64 {
    batches
        .values()
        .filter_map(|batch| batch.column_by_name("date"))
        .filter_map(|column| {
            column
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .filter(|dates| !dates.is_empty())
                .map(|dates| i64::from(dates.value(0)) * 86_400 + 43_200)
        })
        .next()
        .unwrap_or(FIXTURE_SECONDS)
}

fn env_list(name: &str, default: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.into())
        .split(',')
        .map(str::to_owned)
        .collect()
}

/// Nearest-rank p50/p95/p99, mean and max in milliseconds.
fn summary(values: &[f64]) -> serde_json::Value {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rank = |q: f64| {
        let index = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
        (sorted[index] * 1000.0).round() / 1000.0
    };
    serde_json::json!({
        "n": sorted.len(),
        "p50": rank(0.5),
        "p95": rank(0.95),
        "p99": rank(0.99),
        "max": rank(1.0),
        "mean": (sorted.iter().sum::<f64>() / sorted.len() as f64 * 1000.0).round() / 1000.0,
    })
}

/// Sequential phases of one commit (they sum to its wall time) plus the
/// overlapping encode milestones inside table work.
fn phases(
    started: Instant,
    ended: Instant,
    stages: &[(Stage, Instant)],
) -> Vec<(&'static str, f64)> {
    let ms = |from: Instant, to: Instant| to.saturating_duration_since(from).as_secs_f64() * 1000.0;
    let first = |wanted: fn(&Stage) -> bool| {
        stages
            .iter()
            .filter(|(stage, _)| wanted(stage))
            .map(|(_, at)| *at)
            .min()
    };
    let last = |wanted: fn(&Stage) -> bool| {
        stages
            .iter()
            .filter(|(stage, _)| wanted(stage))
            .map(|(_, at)| *at)
            .max()
    };
    let writing = first(|s| *s == Stage::WritingPersisted).unwrap();
    let published = last(|s| matches!(s, Stage::Published(_))).unwrap_or(writing);
    let committed = first(|s| *s == Stage::CommittedPersisted).unwrap();
    let authority = first(|s| *s == Stage::AuthorityAdvanced).unwrap();
    let mirror = first(|s| *s == Stage::MirrorReconciled).unwrap();
    let cleared = first(|s| *s == Stage::PendingCleared).unwrap();
    let mut phases = vec![
        (
            "begin (plan, unoccupied checks, Writing)",
            ms(started, writing),
        ),
        (
            "table_work (encode, stage, receipts, publish)",
            ms(writing, published),
        ),
        ("final_verify + Committed", ms(published, committed)),
        ("authority", ms(committed, authority)),
        ("mirror (test double)", ms(authority, mirror)),
        ("cleanup + clear", ms(mirror, cleared)),
        ("tail", ms(cleared, ended)),
        ("total", ms(started, ended)),
    ];
    if let (Some(first_staged), Some(last_staged)) = (
        first(|s| matches!(s, Stage::Staged(_))),
        last(|s| matches!(s, Stage::Staged(_))),
    ) {
        phases.push((
            "table_work: first part encoded+staged",
            ms(writing, first_staged),
        ));
        phases.push((
            "table_work: all parts encoded+staged",
            ms(writing, last_staged),
        ));
    }
    if let Some(receipt) = first(|s| matches!(s, Stage::ReceiptPersisted(_))) {
        phases.push(("table_work: first receipt durable", ms(writing, receipt)));
    }
    phases
}

enum Backend {
    Local(tempfile::TempDir),
    S3(Server),
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark; requires FIREPARQ_658_DATASET"]
async fn live_flush_phase_benchmark() {
    let root = PathBuf::from(std::env::var("FIREPARQ_658_DATASET").expect("FIREPARQ_658_DATASET"));
    let limit: usize =
        std::env::var("FIREPARQ_658_LIMIT").map_or(usize::MAX, |v| v.parse().unwrap());
    let slow_every: u64 =
        std::env::var("FIREPARQ_658_SLOW_EVERY").map_or(0, |value| value.parse().unwrap());
    let transactions: Vec<_> = protected_transactions(&root)
        .into_iter()
        .take(limit)
        .collect();
    assert!(
        !transactions.is_empty(),
        "no protected parts under {root:?}"
    );
    let inventory = inventory(&transactions.iter().cloned().collect());
    let part_count: usize = transactions.iter().map(|(_, batches)| batches.len()).sum();
    eprintln!(
        "{}",
        serde_json::json!({"dataset": root, "transactions": transactions.len(), "parts": part_count,
            "tables": inventory.len()})
    );
    let mut runs = Vec::new();
    for storage in env_list("FIREPARQ_658_STORAGE", "local,s3") {
        let latencies = if storage == "s3" {
            env_list("FIREPARQ_658_LATENCY_MS", "0,10,30,80")
        } else {
            vec!["0".into()]
        };
        for latency in latencies {
            for setting in env_list("FIREPARQ_658_SETTINGS", "2:4,2:8,2:16,4:16") {
                runs.push((storage.clone(), latency.parse::<u64>().unwrap(), setting));
            }
        }
    }
    for (storage, latency_ms, setting) in runs {
        let values: Vec<usize> = setting.split(':').map(|v| v.parse().unwrap()).collect();
        let limits = FlushConcurrency {
            encoders: values[0],
            publications: values[1],
            inflight_bytes: crate::config::DEFAULT_FLUSH_INFLIGHT_BYTES,
        };
        let backend = match storage.as_str() {
            "local" => Backend::Local(tempfile::tempdir().unwrap()),
            "s3" => Backend::S3(Server::start().await),
            other => panic!("unknown storage {other}"),
        };
        let local_owner = match &backend {
            Backend::Local(dir) => Some(LocalOwnership::acquire(&[dir.path().into()]).unwrap()),
            Backend::S3(_) => None,
        };
        let remote_owner = match &backend {
            Backend::S3(server) => Some(
                S3Ownership::acquire_native(server.client.clone(), "build", vec!["dataset".into()])
                    .await
                    .unwrap(),
            ),
            Backend::Local(_) => None,
        };
        let (states, parts, descriptor) = match (&backend, &local_owner, &remote_owner) {
            (Backend::Local(dir), Some(owner), _) => {
                let descriptor = descriptor_for(
                    StorageIdentity::Local {
                        canonical_root: fs::canonicalize(dir.path())
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .into(),
                    },
                    &inventory,
                );
                (
                    TransactionStateStore::local(dir.path(), owner).unwrap(),
                    TransactionParts::local(dir.path(), owner).unwrap(),
                    descriptor,
                )
            }
            (Backend::S3(_), _, Some(owner)) => {
                let descriptor = descriptor_for(
                    StorageIdentity::S3 {
                        service: Digest::hash("service", &"fixture").unwrap(),
                        bucket: "bucket".into(),
                        prefix: "dataset".into(),
                    },
                    &inventory,
                );
                (
                    TransactionStateStore::s3("dataset", owner).unwrap(),
                    TransactionParts::s3("dataset", owner, "").unwrap(),
                    descriptor,
                )
            }
            _ => unreachable!(),
        };
        states
            .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
            .await
            .unwrap();
        let mirror = Mirror::default();
        let mut controller = TransactionController::open(states, parts, &mirror, &descriptor)
            .await
            .unwrap()
            .with_concurrency(limits)
            .unwrap();
        // Setup ran without latency; every measured request pays it.
        let requests_before = match &backend {
            Backend::S3(server) => {
                let mut state = server.state.lock().unwrap();
                state.latency = Duration::from_millis(latency_ms);
                state.slow_every = slow_every;
                state.slow = Duration::from_secs(1);
                Some(state.requests.len())
            }
            Backend::Local(_) => None,
        };
        let mut per_phase: BTreeMap<&'static str, Vec<f64>> = BTreeMap::new();
        let mut requests = Vec::new();
        let mut methods: BTreeMap<String, usize> = BTreeMap::new();
        let mut previous_requests = requests_before.unwrap_or(0);
        let wall = Instant::now();
        for ((first, last, _), batches) in &transactions {
            let mut frontier = AcceptedFrontier::resume(&controller.authority().checkpoint);
            assert!(*first > controller.authority().checkpoint.ordinal);
            for ordinal in controller.authority().checkpoint.ordinal + 1..=*last {
                let received = frontier.receive(event(1_000_000 + ordinal, 1)).unwrap();
                frontier
                    .accept(received, routing(RoutingPolicy::GenesisLookaheadV1))
                    .unwrap();
            }
            let prefix = frontier.snapshot().unwrap().unwrap();
            let seconds = routing_seconds(batches);
            let batches = batches.clone();
            *STAGES.lock().unwrap() = Some(Vec::new());
            let started = Instant::now();
            controller
                .commit(
                    prefix,
                    batches,
                    BlockMetadata {
                        min_block_number: 1_000_000 + first,
                        max_block_number: 1_000_000 + last,
                        min_timestamp: Some(seconds),
                        max_timestamp: Some(seconds),
                    },
                    Compression::Zstd,
                    ParquetFileMetadata::new(),
                )
                .await
                .unwrap();
            let ended = Instant::now();
            let stages = STAGES.lock().unwrap().take().unwrap();
            for (name, value) in phases(started, ended, &stages) {
                per_phase.entry(name).or_default().push(value);
            }
            if let Backend::S3(server) = &backend {
                let state = server.state.lock().unwrap();
                requests.push((state.requests.len() - previous_requests) as f64);
                for request in &state.requests[previous_requests..] {
                    let class = if request.path.ends_with(".parquet") {
                        format!("{} part", request.method)
                    } else if request.path.contains("/.fireparq-ingest/") {
                        format!("{} control", request.method)
                    } else {
                        format!(
                            "{} {}",
                            request.method,
                            request.path.rsplit('/').next().unwrap()
                        )
                    };
                    *methods.entry(class).or_default() += 1;
                }
                previous_requests = state.requests.len();
            }
        }
        let wall = wall.elapsed().as_secs_f64();
        drop(controller);
        let commits = transactions.len();
        println!(
            "{}",
            serde_json::json!({
                "storage": storage,
                "latency_ms": latency_ms,
                "slow_every": slow_every,
                "setting": setting,
                "transactions": commits,
                "parts": part_count,
                "back_to_back_commits_per_second": commits as f64 / wall,
                "phases_ms": per_phase
                    .iter()
                    .map(|(name, values)| (name.to_string(), summary(values)))
                    .collect::<serde_json::Map<_, _>>(),
                "requests_per_commit": (!requests.is_empty()).then(|| summary(&requests)),
                "requests_per_commit_by_class": methods
                    .iter()
                    .map(|(name, count)| (name.clone(), serde_json::json!(*count as f64 / commits as f64)))
                    .collect::<serde_json::Map<_, _>>(),
            })
        );
        if let Backend::S3(server) = &backend {
            server.state.lock().unwrap().latency = Duration::ZERO;
        }
        if let Some(owner) = remote_owner {
            owner.release().await.unwrap();
        }
    }
    *STAGES.lock().unwrap() = None;
}
