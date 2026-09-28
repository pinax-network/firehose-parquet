//! #643 L9: the reference maintenance job, `scripts/delta_maintenance.py`
//! (`deltalake` 1.6.6), runs over and over beside a real `fireparq build`.
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
//! - the writer never fails, and no round reports an error or a conflict;
//! - OPTIMIZE commits land between writer commits, also on the open date;
//! - DuckDB `delta_scan` and Polars `scan_delta` read exactly the written
//!   rows (each block once, with its rows), including a pruned closed date;
//! - every table's `txn` version is still the authority's ordinal;
//! - each closed date is one active file, and one data file in storage (the
//!   compacted parts are vacuumed), while the full VACUUM deleted nothing.
//!
//! `vacuum_runs_before_the_checkpoint_and_never_deletes_untracked_parts`
//! checks design §4.1: the VACUUM-then-checkpoint order (and that the reverse
//! leaves orphans), and that the job never deletes a part fireparq published
//! but has not committed. Engines: see `common/mod.rs`; the job runs with
//! the Python in `FIREPARQ_POLARS_PYTHON`.
use firehose_protos::{eth, firehose};
use prost::Message;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

mod common;
use common::{number, DuckDb};

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

/// One run of the maintenance job over `tables`, with extra settings.
struct Round {
    settings: Vec<(&'static str, &'static str)>,
    status: i32,
    lines: Vec<Value>,
    stderr: String,
}

impl Round {
    fn done(&self) -> &Value {
        self.lines
            .iter()
            .find(|line| line["event"] == "done")
            .unwrap_or_else(|| panic!("no done line: {self:?}"))
    }

    fn tables(&self) -> impl Iterator<Item = &Value> {
        self.lines.iter().filter(|line| line["event"] == "table")
    }
}

impl std::fmt::Debug for Round {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} exit {}: {:?} {}",
            self.settings, self.status, self.lines, self.stderr
        )
    }
}

fn script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../scripts/delta_maintenance.py")
}

async fn maintain(
    python: &Path,
    storage: &Storage<'_>,
    tables: &[String],
    settings: &[(&'static str, &'static str)],
) -> Round {
    let mut command = tokio::process::Command::new(python);
    command
        .kill_on_drop(true)
        .env_clear()
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env("LAKE_ROOT", storage.root())
        .env("LAKE_TABLES", tables.join(","))
        .envs(storage.s3_env())
        .envs(settings.iter().copied())
        .arg(script());
    if let Storage::S3(server) = storage {
        command.env("S3_ENDPOINT", &server.endpoint);
    }
    let output = tokio::time::timeout(Duration::from_secs(120), command.output())
        .await
        .expect("maintenance timed out")
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    Round {
        settings: settings.to_vec(),
        status: output.status.code().unwrap_or(-1),
        lines: stdout
            .lines()
            .map(|line| serde_json::from_str(line).unwrap_or_else(|e| panic!("{e}: {line}")))
            .collect(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// The job's modes, cycled through while the writer runs.
const MODES: [&[(&str, &str)]; 3] = [
    &[("VACUUM_RETENTION_HOURS", "0")],
    &[("VACUUM_RETENTION_HOURS", "0"), ("OPTIMIZE_DATES", "all")],
    &[("FULL_VACUUM", "1")],
];

/// Runs maintenance rounds until `build` exits; returns the rounds.
async fn rounds_beside(
    python: &Path,
    storage: &Storage<'_>,
    tables: &[String],
    mut build: Build,
    counter: &AtomicU64,
) -> Vec<Round> {
    let mut rounds = Vec::new();
    let status = loop {
        if let Some(status) = build.child.try_wait().unwrap() {
            break status;
        }
        let mode = MODES[rounds.len() % MODES.len()];
        rounds.push(maintain(python, storage, tables, mode).await);
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

async fn maintenance_beside_build(storage: Storage<'_>, cwd: &Path) {
    let Some(python) = common::python() else {
        return;
    };
    let counter = Arc::new(AtomicU64::new(0));
    let (endpoint, server) = firehose(counter.clone()).await;

    // Three days, then a clean restart over the maintained tables to a fourth.
    let first = Build::start(&storage, cwd, &endpoint, FIRST_STOP);
    let tables = tables_once_committed(&storage).await;
    assert!(tables.len() > TABLES.len(), "{tables:?}");
    let mut rounds = rounds_beside(&python, &storage, &tables, first, &counter).await;
    rounds.push(maintain(&python, &storage, &tables, MODES[0]).await);
    let restart = Build::start(&storage, cwd, &endpoint, LAST_STOP);
    rounds.extend(rounds_beside(&python, &storage, &tables, restart, &counter).await);
    // The writer has stopped: a last round compacts every date, including the
    // last one (`OPTIMIZE_DATES=all`), and vacuums at once.
    rounds.push(maintain(&python, &storage, &tables, MODES[1]).await);
    server.abort();

    let (mut compactions, mut open_compactions, mut compacted, mut vacuumed) = (0, 0, 0, 0);
    for (index, round) in rounds.iter().enumerate() {
        // Every round but the last one ran beside a writer.
        let beside_writer = index + 1 < rounds.len();
        assert_eq!(round.status, 0, "{round:?}");
        let done = round.done();
        assert_eq!(
            (done["failed"].clone(), done["conflicts"].clone()),
            (json!([]), json!(0)),
            "{round:?}"
        );
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
            if round.settings.contains(&("FULL_VACUUM", "1")) {
                // The enforced 168 h retention: nothing here is that old.
                assert_eq!(deleted, 0, "{table}");
            }
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

    let root = storage.local_copy(cwd);
    let blocks = LAST_STOP - FIRST;
    assert_eq!(authority_ordinal(&root), blocks as i64);
    let names: Vec<String> = TABLES.iter().map(|(table, _)| table.to_string()).collect();
    let report = common::python_report(
        &python,
        "delta_check.py",
        &json!({"root": root, "tables": names, "day": CLOSED_DAYS[1], "history": true}),
    );
    let mut interleaved = 0;
    for (table, per_block) in TABLES {
        let seen = &report["tables"][table];
        let context = format!("polars {table}");
        assert_eq!(
            number(&seen["rows"]),
            per_block * blocks,
            "{context}: {seen}"
        );
        assert_eq!(
            (
                number(&seen["distinct_blocks"]),
                number(&seen["min_block"]),
                number(&seen["max_block"])
            ),
            (blocks, FIRST, LAST_STOP - 1),
            "{context}"
        );
        assert_eq!(seen["rows_per_block"], json!([per_block]), "{context}");
        assert_eq!(number(&seen["day_rows"]), per_block * BLOCKS_PER_DAY);
        assert_eq!(number(&seen["day_scan_files"]), 1, "{context}: pruned");
        // The writer's `txn` survives every OPTIMIZE, VACUUM and checkpoint.
        assert_eq!(number(&seen["txn"]), blocks, "{context}");
        let expected: BTreeMap<String, Value> = CLOSED_DAYS
            .iter()
            .chain([&OPEN_DAY])
            .map(|date| (date.to_string(), json!(1)))
            .collect();
        assert_eq!(
            seen["active_files_per_date"],
            json!(expected),
            "{context}: every date compacted"
        );
        let operations = seen["operations"].as_array().unwrap();
        let writes: Vec<u64> = operations
            .iter()
            .filter(|entry| entry[1] == "WRITE")
            .map(|entry| number(&entry[0]))
            .collect();
        assert_eq!(
            writes.len() as u64,
            blocks,
            "{context}: one commit per block"
        );
        interleaved += operations
            .iter()
            .filter(|entry| entry[1] == "OPTIMIZE")
            .map(|entry| number(&entry[0]))
            .filter(|version| writes.first() < Some(version) && Some(version) < writes.last())
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

/// Design §4.1, measured: the job's VACUUM-then-checkpoint order deletes
/// expired tombstones' files, the reverse order leaves them as orphans, and
/// no lite or enforced full VACUUM deletes a part that is not in the log yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vacuum_runs_before_the_checkpoint_and_never_deletes_untracked_parts() {
    let Some(python) = common::python() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let storage = Storage::Local(cwd.join("dataset"));
    let counter = Arc::new(AtomicU64::new(0));
    let (endpoint, server) = firehose(counter).await;
    // Two days, and no gate reached.
    Build::start(&storage, &cwd, &endpoint, FIRST + BLOCKS_PER_DAY + 4)
        .wait()
        .await;
    server.abort();
    let report = common::python_report(
        &python,
        "vacuum_check.py",
        &json!({
            "script": script(),
            "lake": storage.root(),
            "tables": common::delta_tables(Path::new(&storage.root())),
            "scratch": cwd.join("vacuum-order"),
        }),
    );
    eprintln!("{report:#}");
    // The reverse order orphans the expired tombstones' files; the job's
    // order and the job itself delete them.
    let order = &report["order"];
    assert_eq!(order["checkpoint_then_vacuum"]["deleted"], json!(0));
    assert_eq!(order["checkpoint_then_vacuum"]["orphans"], json!(3));
    assert_eq!(order["vacuum_then_checkpoint"]["deleted"], json!(3));
    assert_eq!(order["vacuum_then_checkpoint"]["orphans"], json!(0));
    assert_eq!(order["job"]["deleted"], json!(3));
    assert_eq!(order["job"]["orphans"], json!(0));
    // A published but uncommitted part.
    let untracked = &report["untracked"];
    assert_eq!(untracked["lite_retention_0"]["kept"], json!(true));
    assert_eq!(untracked["full_enforced"]["kept"], json!(true));
    assert_eq!(untracked["full_retention_0"]["exit"], json!(2));
    assert_eq!(untracked["full_retention_0"]["kept"], json!(true));
    assert_eq!(untracked["unguarded_full_vacuum_would_delete"], json!(true));
    assert_eq!(
        untracked["full_enforced_after_8_days"]["kept"],
        json!(false)
    );
    assert_eq!(
        untracked["rows_before"], untracked["rows_after"],
        "the committed rows are untouched"
    );
    // The default run compacts the closed date of every table with rows (the
    // other date is still open), and a second run changes nothing.
    let closed: Vec<String> = TABLES
        .iter()
        .map(|(table, _)| format!("{table}/{}", CLOSED_DAYS[0]))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    assert_eq!(report["idempotent"]["first_run_compacted"], json!(closed));
    assert_eq!(report["idempotent"]["versions_changed"], json!([]));
}

/// Settings that would make the job unsafe or incomplete are refused before
/// any request, and credentials never appear in its output, not even in an
/// error from the store (here a refused loopback connection).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_job_refuses_unsafe_settings_and_never_prints_credentials() {
    let Some(python) = common::python() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let local = Storage::Local(dir.path().join("lake"));
    let tables = ["blocks".to_string()];
    let secret = "sekrit-loopback-secret-key";
    let s3 = [
        ("LAKE_ROOT", "s3://delta-lake/nothing-here"),
        ("S3_ENDPOINT", "https://127.0.0.1:9"),
        ("AWS_ACCESS_KEY_ID", "loopback-access-key"),
        ("AWS_SECRET_ACCESS_KEY", secret),
    ];
    let cases: Vec<(&str, Vec<(&'static str, &'static str)>)> = vec![
        ("no root", vec![("LAKE_ROOT", "")]),
        ("two roots", vec![("LAKE_BUCKET", "ethereum-mainnet")]),
        ("no tables", vec![("LAKE_TABLES", "")]),
        ("a path as a table", vec![("LAKE_TABLES", "blocks/../x")]),
        (
            "full VACUUM below 168 h",
            vec![("FULL_VACUUM", "1"), ("VACUUM_RETENTION_HOURS", "167")],
        ),
        ("an unknown flag value", vec![("FULL_VACUUM", "maybe")]),
        ("an unknown date scope", vec![("OPTIMIZE_DATES", "open")]),
        (
            "S3 without credentials",
            vec![
                ("LAKE_ROOT", "s3://delta-lake/x"),
                ("AWS_SECRET_ACCESS_KEY", ""),
            ],
        ),
        (
            "unsafe renames on S3",
            s3.iter()
                .copied()
                .chain([("AWS_S3_ALLOW_UNSAFE_RENAME", "true")])
                .collect(),
        ),
    ];
    for (case, settings) in cases {
        let round = maintain(&python, &local, &tables, &settings).await;
        assert_eq!(round.status, 2, "{case}: {round:?}");
        assert_eq!(round.lines.len(), 1, "{case}: {round:?}");
        assert_eq!(round.lines[0]["event"], "config_error", "{case}: {round:?}");
    }
    // A store error: reported per table, exit 1, the secret redacted.
    let round = maintain(&python, &local, &tables, &s3).await;
    assert_eq!(round.status, 1, "{round:?}");
    assert_eq!(round.done()["failed"], json!(["blocks"]), "{round:?}");
    let output = format!("{:?}{}", round.lines, round.stderr);
    assert!(!output.contains(secret), "{output}");
}

/// `name==version` and the hashes of each package of a hash-pinned
/// requirements file.
fn pins(path: &Path) -> BTreeMap<String, Vec<String>> {
    let mut pins: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut current = None;
    for line in std::fs::read_to_string(path).unwrap().lines() {
        let line = line.trim().trim_end_matches('\\').trim();
        if let Some(hash) = line.strip_prefix("--hash=") {
            let package: &String = current.as_ref().unwrap();
            pins.get_mut(package).unwrap().push(hash.to_string());
        } else if !line.is_empty() && !line.starts_with('#') {
            current = Some(line.to_string());
            pins.insert(line.to_string(), Vec::new());
        }
    }
    pins
}

/// The job runs in CI with the engine tests' `deltalake`, so its own pins
/// (what a CronJob installs) must be the same, and the version the script
/// requires.
#[test]
fn the_job_pins_the_deltalake_that_ci_tests() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let job = pins(&manifest.join("../scripts/delta_maintenance.requirements.txt"));
    let engines = pins(&manifest.join("tests/engines/requirements.txt"));
    for (package, hashes) in &job {
        assert!(!hashes.is_empty(), "{package} is not hash-pinned");
        assert_eq!(engines.get(package), Some(hashes), "{package}");
    }
    let source = std::fs::read_to_string(script()).unwrap();
    let required = source
        .lines()
        .find_map(|line| line.strip_prefix("REQUIRED_DELTALAKE = "))
        .unwrap()
        .trim_matches('"');
    assert!(
        job.contains_key(&format!("deltalake=={required}")),
        "{required}: {:?}",
        job.keys()
    );
}
