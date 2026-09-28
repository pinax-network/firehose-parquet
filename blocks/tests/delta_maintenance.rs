//! #643 L9: the maintenance job, the `fireparq-maintenance` binary (delta-rs
//! `deltalake-core` 1.0.0), runs over and over beside a real `fireparq build`.
//!
//! A cursor-aware mock Firehose serves final EVM blocks, 10 per UTC day, one
//! transaction per block. It pauses on three blocks until three more
//! maintenance rounds (one of each mode below) have finished, so OPTIMIZE and
//! VACUUM commits land between the writer's commits every run. The build runs
//! to block 130 (three days), then a clean restart continues to 140 (a fourth
//! day) over the compacted and vacuumed tables. The rounds cycle through the
//! job's modes:
//! lite VACUUM with retention 0 (so compacted files are deleted at once), the
//! same with `OPTIMIZE_DATES=all` (compacting the very date being appended,
//! the worst case), and a full VACUUM with the enforced 168 h retention.
//! On local disk and on a loopback HTTPS S3 endpoint the test asserts:
//!
//! - before the writer created any table, a round skips every table (#680)
//!   and succeeds;
//! - the writer never fails, and no round reports an error or a conflict;
//! - OPTIMIZE commits land between writer commits, also on the open date;
//! - delta-rs (the active files of a snapshot) and DuckDB `delta_scan` read
//!   exactly the written rows (each block once, with its rows), including a
//!   pruned closed date;
//! - every table's `txn` version is still the authority's ordinal;
//! - each closed date is one active file, and one data file in storage (the
//!   compacted parts are vacuumed), while the full VACUUM deleted nothing;
//! - on S3, no request of delta-rs's client (the job, and the writer's log
//!   commits) carries `If-Match` (#678: Ceph RGW 19.2 compares it literally).
//!
//! `vacuum_runs_before_the_checkpoint_and_never_deletes_untracked_parts`
//! checks design §4.1: the VACUUM-then-checkpoint order (and that the reverse
//! leaves orphans), that the job never deletes a part fireparq published but
//! has not committed, and that reruns change nothing.
//!
//! The job is the binary, run with a cleared environment
//! (`common::maintenance_job`); without it (locally) these tests are skipped.
//! `maintenance/tests/cli.rs` tests its settings, exit statuses, redaction
//! and skipped tables. Engines: see `common/mod.rs`.
use firehose_protos::{eth, firehose};
use prost::Message;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tonic::codegen::{http, BoxFuture, Service};

mod common;
use common::{number, DuckDb, JobRun};

#[path = "../examples/bench_live_flush/s3.rs"]
#[allow(dead_code)]
mod s3;

const CHAIN: &str = "maintenance-test";
const BUCKET: &str = "delta-lake";
/// Block 100 is at 2023-11-14T00:00:00Z; `BLOCKS_PER_DAY` blocks per UTC day.
const DAY_ZERO: i64 = 1_699_920_000;
const BLOCKS_PER_DAY: u64 = 10;
const FIRST: u64 = 100;
const FIRST_STOP: u64 = 130;
const LAST_STOP: u64 = 140;
/// The stream waits on these blocks for [`MODES`]`.len()` more maintenance
/// rounds.
const GATES: [u64; 3] = [115, 125, 135];
const CLOSED_DAYS: [&str; 3] = ["2023-11-14", "2023-11-15", "2023-11-16"];
const OPEN_DAY: &str = "2023-11-17";
/// Rows of the tables that get rows, per block.
const TABLES: [(&str, u64); 4] = [
    ("blocks", 1),
    ("transactions", 2),
    ("logs", 2),
    ("access_lists", 2),
];

#[derive(Clone)]
struct Info;
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: CHAIN.into(),
                first_streamable_block_num: FIRST,
                ..Default::default()
            }))
        })
    }
}

/// Final blocks from `FIRST`; a request with a cursor resumes after it. Waits
/// on each gate block until `rounds` grew by one per mode.
#[derive(Clone)]
struct Stream {
    rounds: Arc<AtomicU64>,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        assert!(request.final_blocks_only);
        let after = match request.cursor.as_str() {
            "" => FIRST - 1,
            cursor => cursor
                .strip_prefix("block-")
                .and_then(|number| number.parse().ok())
                .unwrap_or_else(|| panic!("unexpected cursor {cursor}")),
        };
        // The request's stop block is inclusive.
        let last = request.stop_block_num;
        let rounds = self.rounds.clone();
        let blocks = futures::stream::unfold(after + 1, move |number| {
            let rounds = rounds.clone();
            async move {
                if number > last {
                    return None;
                }
                if GATES.contains(&number) {
                    let target = rounds.load(Ordering::SeqCst) + MODES.len() as u64;
                    let waited = tokio::time::timeout(Duration::from_secs(90), async {
                        while rounds.load(Ordering::SeqCst) < target {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    })
                    .await;
                    assert!(waited.is_ok(), "no maintenance round finished at {number}");
                } else {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                }
                Some((Ok(response(number)), number + 1))
            }
        });
        Box::pin(async move {
            Ok(tonic::Response::new(
                Box::pin(blocks) as Self::ResponseStream
            ))
        })
    }
}

macro_rules! service {
    ($type:ty, $name:literal, $method:ident) => {
        impl tonic::server::NamedService for $type {
            const NAME: &'static str = $name;
        }
        impl Service<http::Request<tonic::body::Body>> for $type {
            type Response = http::Response<tonic::body::Body>;
            type Error = std::convert::Infallible;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(
                &mut self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
                let service = self.clone();
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    Ok(grpc.$method(service, request).await)
                })
            }
        }
    };
}
service!(Info, "sf.firehose.v2.EndpointInfo", unary);
service!(Stream, "sf.firehose.v2.Stream", server_streaming);

/// A final EVM block with two transactions, each with a log and an access list.
fn response(number: u64) -> firehose::Response {
    let id = number as u8;
    let transaction = |index: u32| eth::TransactionTrace {
        hash: vec![id ^ (index as u8 + 1); 32].into(),
        index,
        status: 1,
        from: vec![0x11; 20].into(),
        to: vec![0x22; 20].into(),
        gas_used: 21_000,
        access_list: vec![eth::AccessTuple {
            address: vec![0x33; 20].into(),
            storage_keys: vec![vec![0x44; 32].into()],
        }],
        receipt: Some(eth::TransactionReceipt {
            logs: vec![eth::Log {
                address: vec![0x66; 20].into(),
                topics: vec![vec![0x77; 32].into()],
                data: vec![1, 2, 3].into(),
                index: 0,
                block_index: index,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    let block = eth::Block {
        number,
        hash: vec![id; 32].into(),
        header: Some(eth::BlockHeader {
            number,
            gas_used: 42_000,
            ..Default::default()
        }),
        transaction_traces: vec![transaction(0), transaction(1)],
        ..Default::default()
    };
    let seconds = DAY_ZERO + ((number - FIRST) * 86_400 / BLOCKS_PER_DAY) as i64;
    firehose::Response {
        block: Some(prost_types::Any {
            type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".into(),
            value: block.encode_to_vec(),
        }),
        step: 3,
        cursor: format!("block-{number}"),
        metadata: Some(firehose::BlockMetadata {
            num: number,
            id: format!("{id:02x}").repeat(32),
            parent_num: number - 1,
            parent_id: format!("{:02x}", id.wrapping_sub(1)).repeat(32),
            lib_num: number,
            time: Some(prost_types::Timestamp { seconds, nanos: 0 }),
            ..Default::default()
        }),
    }
}

/// Where the dataset is written.
enum Storage<'a> {
    Local(PathBuf),
    S3(&'a s3::Server),
}

impl Storage<'_> {
    fn root(&self) -> String {
        match self {
            Storage::Local(root) => root.to_str().unwrap().to_string(),
            Storage::S3(_) => format!("s3://{BUCKET}/{CHAIN}"),
        }
    }

    /// The S3 credentials and endpoint, for `fireparq` and for the job.
    fn s3_env(&self) -> Vec<(&'static str, String)> {
        match self {
            Storage::Local(_) => Vec::new(),
            Storage::S3(server) => vec![
                ("AWS_ACCESS_KEY_ID", "loopback-access-key".into()),
                ("AWS_SECRET_ACCESS_KEY", "loopback-secret-key".into()),
                ("AWS_REGION", "us-east-1".into()),
                ("SSL_CERT_FILE", server.ca_file.to_str().unwrap().into()),
            ],
        }
    }

    /// Every object below the dataset root, by key relative to it.
    fn objects(&self) -> Vec<String> {
        match self {
            Storage::Local(root) if !root.exists() => Vec::new(),
            Storage::Local(root) => {
                let mut pending = vec![root.clone()];
                let mut files = Vec::new();
                while let Some(directory) = pending.pop() {
                    for entry in std::fs::read_dir(&directory).unwrap() {
                        let path = entry.unwrap().path();
                        if path.is_dir() {
                            pending.push(path);
                        } else {
                            let relative = path.strip_prefix(root).unwrap();
                            files.push(relative.to_str().unwrap().to_string());
                        }
                    }
                }
                files.sort();
                files
            }
            Storage::S3(server) => server
                .objects(&format!("{CHAIN}/"))
                .into_iter()
                .map(|(key, _)| key.strip_prefix(&format!("{CHAIN}/")).unwrap().into())
                .collect(),
        }
    }

    /// The dataset as a local directory: the root itself, or a copy of every
    /// object under the S3 prefix.
    fn local_copy(&self, cwd: &Path) -> PathBuf {
        match self {
            Storage::Local(root) => root.clone(),
            Storage::S3(server) => {
                let copy = cwd.join("s3-copy");
                let _ = std::fs::remove_dir_all(&copy);
                for (key, bytes) in server.objects(&format!("{CHAIN}/")) {
                    let path = copy.join(key.strip_prefix(&format!("{CHAIN}/")).unwrap());
                    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                    std::fs::write(path, bytes).unwrap();
                }
                copy
            }
        }
    }
}

/// A mock Firehose; returns its endpoint and server task.
async fn firehose(rounds: Arc<AtomicU64>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Info)
            .add_service(Stream { rounds })
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    (endpoint, server)
}

/// A running `build` to `stop` (exclusive), one transaction per block. Its
/// output goes to a file, so a full pipe never blocks it.
struct Build {
    child: tokio::process::Child,
    log: PathBuf,
}

impl Build {
    fn start(storage: &Storage<'_>, cwd: &Path, endpoint: &str, stop: u64) -> Self {
        let log = cwd.join(format!("build-{stop}.log"));
        let file = std::fs::File::create(&log).unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
        command
            .kill_on_drop(true)
            .env_clear()
            .current_dir(cwd)
            .args(["build", "--endpoint", endpoint, "--block-type", "evm"])
            .args(["--start-block", &FIRST.to_string()])
            .args(["--stop-block", &stop.to_string()])
            .args(["--flush-blocks", "1", "--stream-idle-timeout-secs", "0"])
            .args(["--output", &storage.root()])
            .stdout(file.try_clone().unwrap())
            .stderr(file);
        command.envs(storage.s3_env());
        if let Storage::S3(server) = storage {
            command.env("AWS_ENDPOINT_URL_S3", &server.endpoint);
        }
        Self {
            child: command.spawn().unwrap(),
            log,
        }
    }

    /// Asserts that the build exited successfully.
    fn succeeded(&self, status: std::process::ExitStatus) {
        assert!(
            status.success(),
            "the writer failed beside maintenance: {}",
            std::fs::read_to_string(&self.log).unwrap_or_default()
        );
    }

    async fn wait(mut self) {
        let status = tokio::time::timeout(Duration::from_secs(120), self.child.wait())
            .await
            .expect("fireparq timed out")
            .unwrap();
        self.succeeded(status);
    }
}

/// One run of the maintenance job over `tables` of `storage`'s lake, with
/// extra settings.
async fn maintain(
    job: &Path,
    storage: &Storage<'_>,
    tables: &[String],
    settings: &[(&'static str, &'static str)],
) -> JobRun {
    let mut env: Vec<(&str, String)> = vec![
        ("LAKE_ROOT", storage.root()),
        ("LAKE_TABLES", tables.join(",")),
    ];
    env.extend(storage.s3_env());
    if let Storage::S3(server) = storage {
        env.push(("S3_ENDPOINT", server.endpoint.clone()));
    }
    env.extend(
        settings
            .iter()
            .map(|(key, value)| (*key, value.to_string())),
    );
    common::maintenance_job(job, &env).await
}

/// The job's modes, cycled through while the writer runs.
const MODES: [&[(&str, &str)]; 3] = [
    &[("VACUUM_RETENTION_HOURS", "0")],
    &[("VACUUM_RETENTION_HOURS", "0"), ("OPTIMIZE_DATES", "all")],
    &[("FULL_VACUUM", "1")],
];

/// Runs maintenance rounds until `build` exits; returns the rounds.
async fn rounds_beside(
    job: &Path,
    storage: &Storage<'_>,
    tables: &[String],
    mut build: Build,
    counter: &AtomicU64,
) -> Vec<JobRun> {
    let mut rounds = Vec::new();
    let status = loop {
        if let Some(status) = build.child.try_wait().unwrap() {
            break status;
        }
        let mode = MODES[rounds.len() % MODES.len()];
        rounds.push(maintain(job, storage, tables, mode).await);
        counter.fetch_add(1, Ordering::SeqCst);
        assert!(rounds.len() < 1_000, "the build does not finish");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    build.succeeded(status);
    rounds
}

/// Waits until `blocks` has its first transaction; returns every table.
async fn tables_once_committed(storage: &Storage<'_>) -> Vec<String> {
    let first = "blocks/_delta_log/00000000000000000001.json";
    for _ in 0..600 {
        let objects = storage.objects();
        if objects.iter().any(|key| key == first) {
            let mut tables: Vec<String> = objects
                .iter()
                .filter_map(|key| key.strip_suffix("/_delta_log/00000000000000000000.json"))
                .map(str::to_string)
                .collect();
            tables.sort();
            return tables;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the build committed no transaction");
}

fn authority_ordinal(root: &Path) -> i64 {
    let state: Value =
        serde_json::from_slice(&std::fs::read(root.join(".fireparq-ingest/state.json")).unwrap())
            .unwrap();
    state["payload"]["checkpoint"]["ordinal"].as_i64().unwrap()
}

/// `(version, operation)` of every JSON commit of a local table.
fn operations(table: &Path) -> Vec<(u64, String)> {
    common::delta_log(table)
        .into_iter()
        .map(|(version, actions)| {
            let info = common::action(&actions, "commitInfo").unwrap();
            (version, info["operation"].as_str().unwrap().to_string())
        })
        .collect()
}

async fn maintenance_beside_build(storage: Storage<'_>, cwd: &Path) {
    let Some(job) = common::maintenance_bin() else {
        return;
    };
    let counter = Arc::new(AtomicU64::new(0));
    let (endpoint, server) = firehose(counter.clone()).await;

    // Before the writer created any table, every table is skipped (#680).
    let names: Vec<String> = TABLES.iter().map(|(table, _)| table.to_string()).collect();
    let empty = maintain(&job, &storage, &names, MODES[0]).await;
    empty.assert_clean();
    assert_eq!(empty.done()["skipped"], json!(names), "{empty:?}");
    assert_eq!(empty.tables().count(), 0, "{empty:?}");

    // Three days, then a clean restart over the maintained tables to a fourth.
    let first = Build::start(&storage, cwd, &endpoint, FIRST_STOP);
    let tables = tables_once_committed(&storage).await;
    assert!(tables.len() > TABLES.len(), "{tables:?}");
    let mut rounds = rounds_beside(&job, &storage, &tables, first, &counter).await;
    rounds.push(maintain(&job, &storage, &tables, MODES[0]).await);
    let restart = Build::start(&storage, cwd, &endpoint, LAST_STOP);
    rounds.extend(rounds_beside(&job, &storage, &tables, restart, &counter).await);
    // The writer has stopped: a last round compacts every date, including the
    // last one (`OPTIMIZE_DATES=all`), and vacuums at once.
    rounds.push(maintain(&job, &storage, &tables, MODES[1]).await);
    server.abort();

    let (mut compactions, mut open_compactions, mut compacted, mut vacuumed) = (0, 0, 0, 0);
    for (index, round) in rounds.iter().enumerate() {
        // Every round but the last one ran beside a writer.
        let beside_writer = index + 1 < rounds.len();
        round.assert_clean();
        assert_eq!(round.done()["skipped"], json!([]), "{round:?}");
        for table in round.tables() {
            assert_eq!(table["errors"], json!([]), "{table}");
            assert_eq!(table["conflicts"], json!([]), "{table}");
            for compaction in table["compacted"].as_array().unwrap() {
                compactions += 1;
                compacted += number(&compaction["files_removed"]);
                // The date the writer was still appending to.
                open_compactions +=
                    usize::from(beside_writer && compaction["date"] == table["open_date"]);
            }
            let deleted = number(&table["vacuum"]["files_deleted"]);
            vacuumed += deleted;
            if round.has("FULL_VACUUM", "1") {
                // The enforced 168 h retention: nothing here is that old.
                assert_eq!(deleted, 0, "{table}");
            }
            // A checkpoint after every VACUUM.
            assert_eq!(
                table["checkpoint_version"], table["version_after"],
                "{table}"
            );
        }
    }
    eprintln!(
        "{} maintenance rounds beside the build: {compactions} date compactions \
         ({open_compactions} of the open date beside the writer) removing {compacted} \
         files; {vacuumed} VACUUM deletions (retention 0 repeats earlier ones)",
        rounds.len()
    );
    assert!(
        open_compactions > 0,
        "no OPTIMIZE of the date being appended"
    );

    // Each closed date is one active file and one data file in storage.
    let objects = storage.objects();
    for (table, _) in TABLES {
        for date in CLOSED_DAYS.iter().chain([&OPEN_DAY]) {
            let prefix = format!("{table}/date={date}/");
            let stored = objects
                .iter()
                .filter(|key| key.starts_with(&prefix) && key.ends_with(".parquet"))
                .count();
            assert_eq!(stored, 1, "{prefix}: {stored} data files after VACUUM");
        }
    }
    // #678: delta-rs's client (object_store 0.13: the job, and the writer's
    // log commits) never sends `If-Match`, which Ceph RGW 19.2 compares
    // literally. The writer's own pinned part readbacks (object_store 0.12)
    // do, in the owner's ETag form.
    if let Storage::S3(server) = &storage {
        let log = server.log();
        let delta_rs = |entry: &&s3::Entry| {
            entry
                .user_agent
                .as_deref()
                .is_some_and(|agent| agent.starts_with("object_store/0.13."))
        };
        let delta_requests = log.iter().filter(delta_rs).count();
        let conditional: Vec<String> = log
            .iter()
            .filter(delta_rs)
            .filter(|entry| entry.if_match.is_some())
            .map(|entry| format!("{} {}", entry.method, entry.key))
            .collect();
        assert!(delta_requests > 0, "no delta-rs request was logged");
        assert!(conditional.is_empty(), "If-Match on {conditional:?}");
        eprintln!(
            "{delta_requests} delta-rs requests, none with If-Match; {} pinned writer readbacks",
            log.iter()
                .filter(|entry| entry.if_match.is_some() && !delta_rs(entry))
                .count()
        );
    }

    let root = storage.local_copy(cwd);
    let blocks = LAST_STOP - FIRST;
    assert_eq!(authority_ordinal(&root), blocks as i64);
    let mut interleaved = 0;
    for (table, per_block) in TABLES {
        let delta = common::open_local(&root, table).await;
        let read = common::delta_read(&delta).await;
        let context = format!("delta-rs {table}");
        let block_nums = common::delta_block_nums(&root.join(table), &read);
        let expected: Vec<i64> = (FIRST..LAST_STOP)
            .flat_map(|block| std::iter::repeat_n(block as i64, per_block as usize))
            .collect();
        assert_eq!(
            block_nums, expected,
            "{context}: each block once, with its rows"
        );
        // The `date` filter prunes to the day's one file.
        let day_files = common::delta_day_files(&delta, CLOSED_DAYS[1]).await;
        assert_eq!(day_files.len(), 1, "{context}: pruned");
        let day = read
            .files
            .iter()
            .find(|file| file.date == CLOSED_DAYS[1])
            .unwrap();
        assert_eq!(day.rows, Some(per_block * BLOCKS_PER_DAY), "{context}");
        // The writer's `txn` survives every OPTIMIZE, VACUUM and checkpoint.
        assert_eq!(read.txn, Some(blocks as i64), "{context}");
        let expected: BTreeMap<String, u64> = CLOSED_DAYS
            .iter()
            .chain([&OPEN_DAY])
            .map(|date| (date.to_string(), 1))
            .collect();
        assert_eq!(
            read.files_per_date(),
            expected,
            "{context}: every date compacted"
        );
        let operations = operations(&root.join(table));
        let writes: Vec<u64> = operations
            .iter()
            .filter(|(_, operation)| operation == "WRITE")
            .map(|(version, _)| *version)
            .collect();
        assert_eq!(
            writes.len() as u64,
            blocks,
            "{context}: one commit per block"
        );
        interleaved += operations
            .iter()
            .filter(|(version, operation)| {
                operation == "OPTIMIZE"
                    && writes.first() < Some(version)
                    && Some(version) < writes.last()
            })
            .count();
    }
    assert!(interleaved > 0, "no OPTIMIZE landed between writer commits");

    if let Some(duckdb) = DuckDb::open(cwd) {
        let counts = common::duckdb_counts(&duckdb, &root, &names);
        for (table, per_block) in TABLES {
            assert_eq!(counts[table], per_block * blocks, "duckdb {table}");
        }
        let scan = format!("delta_scan('{}/blocks', filename = true)", root.display());
        let rows = duckdb.query(&format!(
            "SELECT 'blocks' AS q, count(DISTINCT block_num) AS distinct_blocks, \
             count(*) FILTER (WHERE contains(filename, '/_delta_log/') \
               OR contains(filename, '/_fireparq/')) AS hidden, \
             count(*) FILTER (WHERE date = DATE '{}') AS on_day FROM {scan};",
            CLOSED_DAYS[1]
        ));
        let row = &rows["blocks"][0];
        assert_eq!(number(&row["distinct_blocks"]), blocks, "{row}");
        assert_eq!(number(&row["hidden"]), 0, "{row}");
        assert_eq!(number(&row["on_day"]), BLOCKS_PER_DAY, "{row}");
        eprintln!(
            "duckdb {} (delta {}) read the maintained tables exactly",
            duckdb.version, duckdb.delta_version
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_beside_a_local_build_keeps_exact_rows() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    maintenance_beside_build(Storage::Local(cwd.join("dataset")), &cwd).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_beside_an_s3_build_keeps_exact_rows() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let server = s3::Server::start(
        &cwd.join("tls"),
        BUCKET,
        s3::Latency {
            base_ms: 0,
            slow_every: 0,
            slow_ms: 0,
        },
    )
    .await
    .unwrap();
    maintenance_beside_build(Storage::S3(&server), &cwd).await;
}

/// The data files of a local table on disk, relative to it.
fn data_files(table: &Path) -> Vec<String> {
    let mut pending = vec![table.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.ends_with("_delta_log") {
                continue;
            }
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                let relative = path.strip_prefix(table).unwrap();
                files.push(relative.to_str().unwrap().to_string());
            }
        }
    }
    files.sort();
    files
}

/// Data files on disk that no active `add` names.
async fn orphans(table: &Path) -> usize {
    let (root, name) = (table.parent().unwrap(), table.file_name().unwrap());
    let active = common::delta_read(&common::open_local(root, name.to_str().unwrap()).await)
        .await
        .paths();
    data_files(table)
        .iter()
        .filter(|file| !active.contains(file))
        .count()
}

/// Seconds of `delta.deletedFileRetentionDuration` of the scratch tables.
const SCRATCH_RETENTION_SECS: u64 = 4;

/// A Delta table of three one-row appends on 2023-11-14, written by hand
/// (commit 0 with `protocol` and `metaData`, then one `add` per commit), with
/// a retention of [`SCRATCH_RETENTION_SECS`].
fn scratch_table(table: &Path) {
    let log = table.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let schema = json!({"type": "struct", "fields": [
        {"name": "block_num", "type": "long", "nullable": true, "metadata": {}},
        {"name": "date", "type": "date", "nullable": true, "metadata": {}},
    ]});
    let mut commits = vec![vec![
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {
            "id": "00000000-0000-4000-8000-000000000643",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(),
            "partitionColumns": ["date"],
            "configuration": {
                "delta.deletedFileRetentionDuration":
                    format!("interval {SCRATCH_RETENTION_SECS} seconds"),
            },
            "createdTime": now,
        }}),
        json!({"commitInfo": {"timestamp": now, "operation": "CREATE TABLE"}}),
    ]];
    let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("block_num", arrow::datatypes::DataType::Int64, true),
    ]));
    for block in 0..3 {
        let batch = arrow::array::RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from(vec![block]))],
        )
        .unwrap();
        let bytes = firehose_parquet::writer::encode_parquet(
            &batch,
            firehose_parquet::config::Compression::Zstd,
            &firehose_parquet::writer::ParquetFileMetadata::new(),
        )
        .unwrap();
        let path = format!("date=2023-11-14/part-{block}.parquet");
        std::fs::create_dir_all(table.join("date=2023-11-14")).unwrap();
        std::fs::write(table.join(&path), &bytes).unwrap();
        commits.push(vec![
            json!({"commitInfo": {"timestamp": now, "operation": "WRITE"}}),
            json!({"add": {
                "path": path, "partitionValues": {"date": "2023-11-14"},
                "size": bytes.len(), "modificationTime": now, "dataChange": true,
                "stats": json!({"numRecords": 1}).to_string(),
            }}),
        ]);
    }
    for (version, actions) in commits.iter().enumerate() {
        let lines: String = actions.iter().map(|action| format!("{action}\n")).collect();
        std::fs::write(log.join(format!("{version:020}.json")), lines).unwrap();
    }
}

/// The job over one table of a local lake.
async fn job_on(job: &Path, root: &Path, table: &str, settings: &[(&str, &str)]) -> JobRun {
    let mut env = vec![
        ("LAKE_ROOT", root.to_str().unwrap().to_string()),
        ("LAKE_TABLES", table.to_string()),
    ];
    env.extend(
        settings
            .iter()
            .map(|(key, value)| (*key, value.to_string())),
    );
    common::maintenance_job(job, &env).await
}

/// Design §4.1, measured: the job's VACUUM-then-checkpoint order deletes
/// expired tombstones' files, the reverse order leaves them as orphans, and
/// no lite or enforced full VACUUM deletes a part that is not in the log yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vacuum_runs_before_the_checkpoint_and_never_deletes_untracked_parts() {
    let Some(job) = common::maintenance_bin() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();

    // The order: three scratch tables, each compacted by the job (three
    // files tombstoned; its VACUUM finds them too young, its checkpoint
    // keeps them), then left until the tombstones are older than the
    // retention.
    let scratch = cwd.join("vacuum-order");
    for table in ["checkpoint_first", "vacuum_first", "blocks"] {
        scratch_table(&scratch.join(table));
        let compacted = job_on(&job, &scratch, table, &[("OPTIMIZE_DATES", "all")]).await;
        compacted.assert_clean();
        let line = compacted.table(table);
        assert_eq!(
            line["compacted"][0]["files_removed"],
            json!(3),
            "{compacted:?}"
        );
        assert_eq!(line["vacuum"]["files_deleted"], json!(0), "{compacted:?}");
    }
    tokio::time::sleep(Duration::from_secs(SCRATCH_RETENTION_SECS + 1)).await;
    // A checkpoint drops the expired tombstones: the next run's VACUUM (a
    // fresh snapshot) finds nothing to delete, and the files stay orphans.
    // (The job's checkpoint is at the OPTIMIZE version; a later commit gives
    // this one a version of its own, as the next hourly run would.)
    let first = scratch.join("checkpoint_first");
    let next = common::delta_log(&first).keys().max().unwrap() + 1;
    std::fs::write(
        first.join(format!("_delta_log/{next:020}.json")),
        format!(
            "{}\n",
            json!({"commitInfo": {"timestamp": 1_700_000_000_000u64, "operation": "WRITE"}})
        ),
    )
    .unwrap();
    common::delta_checkpoint(&common::open_local(&scratch, "checkpoint_first").await).await;
    let (_, reverse) = common::open_local(&scratch, "checkpoint_first")
        .await
        .vacuum()
        .await
        .unwrap();
    assert_eq!(reverse.files_deleted.len(), 0);
    assert_eq!(orphans(&scratch.join("checkpoint_first")).await, 3);
    let (table, forward) = common::open_local(&scratch, "vacuum_first")
        .await
        .vacuum()
        .await
        .unwrap();
    common::delta_checkpoint(&table).await;
    assert_eq!(forward.files_deleted.len(), 3);
    assert_eq!(orphans(&scratch.join("vacuum_first")).await, 0);
    // The job itself: VACUUM, then its checkpoint.
    let run = job_on(&job, &scratch, "blocks", &[]).await;
    run.assert_clean();
    assert_eq!(
        run.table("blocks")["vacuum"]["files_deleted"],
        json!(3),
        "{run:?}"
    );
    assert_eq!(orphans(&scratch.join("blocks")).await, 0);

    // A fireparq lake of two days, and no gate reached.
    let storage = Storage::Local(cwd.join("dataset"));
    let counter = Arc::new(AtomicU64::new(0));
    let (endpoint, server) = firehose(counter).await;
    Build::start(&storage, &cwd, &endpoint, FIRST + BLOCKS_PER_DAY + 4)
        .wait()
        .await;
    server.abort();
    let root = cwd.join("dataset");
    let tables = common::delta_tables(&root);

    // The default run compacts the closed date of every table with rows (the
    // other date is still open), and a second run changes nothing.
    let first = maintain(&job, &storage, &tables, &[]).await;
    first.assert_clean();
    let compacted: std::collections::BTreeSet<String> = first
        .tables()
        .flat_map(|line| {
            let table = line["table"].as_str().unwrap().to_string();
            line["compacted"]
                .as_array()
                .unwrap()
                .iter()
                .map(move |compaction| format!("{table}/{}", compaction["date"].as_str().unwrap()))
        })
        .collect();
    let closed: std::collections::BTreeSet<String> = TABLES
        .iter()
        .map(|(table, _)| format!("{table}/{}", CLOSED_DAYS[0]))
        .collect();
    assert_eq!(compacted, closed);
    let second = maintain(&job, &storage, &tables, &[]).await;
    second.assert_clean();
    let changed: Vec<&Value> = second
        .tables()
        .filter(|line| line["version_before"] != line["version_after"])
        .map(|line| &line["table"])
        .collect();
    assert!(changed.is_empty(), "{changed:?}");

    // A published but uncommitted part: a copy of a `blocks` data file that
    // no log entry names.
    let blocks = common::delta_read(&common::open_local(&root, "blocks").await).await;
    let source = root.join("blocks").join(&blocks.files[0].path);
    let part = source.with_file_name("part-v1-untracked-copy.parquet");
    std::fs::copy(&source, &part).unwrap();
    let rows_before = common::delta_counts(&root, &tables).await;
    let lite = maintain(&job, &storage, &tables, &[("VACUUM_RETENTION_HOURS", "0")]).await;
    lite.assert_clean();
    assert!(part.exists(), "a lite VACUUM kept the part: {lite:?}");
    let full = maintain(&job, &storage, &tables, &[("FULL_VACUUM", "1")]).await;
    full.assert_clean();
    assert!(
        part.exists(),
        "a full VACUUM of 168 h kept the part: {full:?}"
    );
    let refused = maintain(
        &job,
        &storage,
        &tables,
        &[("FULL_VACUUM", "1"), ("VACUUM_RETENTION_HOURS", "0")],
    )
    .await;
    assert_eq!((refused.status, part.exists()), (2, true), "{refused:?}");
    assert_eq!(refused.lines.len(), 1, "{refused:?}");
    assert_eq!(refused.lines[0]["event"], "config_error");
    // An unguarded full VACUUM (retention 0, not enforced) would delete it.
    let would =
        common::delta_vacuum_now(common::open_local(&root, "blocks").await, true, true).await;
    assert!(
        would
            .iter()
            .any(|path| path.ends_with("part-v1-untracked-copy.parquet")),
        "{would:?}"
    );
    // Once older than the enforced 168 h, the weekly full VACUUM deletes it.
    let eight_days_ago = SystemTime::now() - Duration::from_secs(8 * 86_400);
    std::fs::File::options()
        .write(true)
        .open(&part)
        .unwrap()
        .set_modified(eight_days_ago)
        .unwrap();
    let weekly = maintain(&job, &storage, &tables, &[("FULL_VACUUM", "1")]).await;
    weekly.assert_clean();
    assert!(!part.exists(), "{weekly:?}");
    assert_eq!(
        common::delta_counts(&root, &tables).await,
        rows_before,
        "the committed rows are untouched"
    );
}
