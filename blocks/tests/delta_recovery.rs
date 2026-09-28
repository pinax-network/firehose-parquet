//! #643 L4: every crash row of `docs/design/delta-lake.md` §4, recovered by
//! the real `fireparq` binary, and #636: `deltalake` maintenance beside a
//! running `build`.
//!
//! A cursor-aware mock Firehose serves final EVM blocks from 100, two UTC days
//! (100 and 101 on 2023-11-14, the rest on 2023-11-15), each with two
//! transactions, logs and access lists. Every `build` flushes one block per
//! transaction, on local disk and on the loopback HTTPS S3 endpoint of
//! `examples/bench_live_flush/s3.rs`. A crash is a `FIREPARQ_DEBUG_FAULT`
//! (debug builds only): `crash-at:<Stage>` aborts at a transaction boundary,
//! `crash-after-delta-commit:<table>` once a table's commit is durable,
//! `delta-commit-lost-response:<table>` fails after the commit landed as if
//! its response were lost, and `crash-after-delta-create:<table>` aborts once
//! that table is created. With `--flush-publish-concurrency 1` the tables
//! commit in name order, `blocks` last: `access_lists`, `logs`,
//! `transactions`, `blocks`. An aborted S3 build keeps its bucket owner, which
//! the test releases with `recovery release` as an operator would.
//!
//! Each test reads the result through the Delta logs alone (replayed from
//! their JSON commits, which maintenance never cleans up here): the rows of
//! every active file, each block exactly once per table, each table's `txn`
//! increasing by one commit per transaction up to the authority's ordinal,
//! and `blocks` committed last in every transaction.
//!
//! Maintenance is the `deltalake` package run by `tests/engines/delta_maintain.py`
//! in the Python named by `FIREPARQ_POLARS_PYTHON` (`FIREPARQ_REQUIRE_POLARS`
//! in CI makes it mandatory): OPTIMIZE with a lite VACUUM of retention 0 (it
//! deletes compacted parts at once), or a full VACUUM of retention 0 (it
//! deletes untracked parts). Without it, a test deletes exactly the files that
//! VACUUM would, and skips the row reads those files would serve; the
//! maintenance-beside-`build` tests are skipped. Those also read the final
//! tables with Polars `scan_delta` (`tests/engines/delta_check.py`).
use firehose_parquet::writer::read_parquet;
use firehose_protos::{eth, firehose};
use futures::StreamExt;
use prost::Message;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

#[path = "../examples/bench_live_flush/s3.rs"]
#[allow(dead_code)]
mod s3;

const CHAIN: &str = "delta-recovery-chain";
/// 2023-11-15T00:00:00Z: blocks 100 and 101 are on 2023-11-14, later blocks
/// on 2023-11-15.
const MIDNIGHT: i64 = 1_700_006_400;
const BUCKET: &str = "delta-recovery";
/// The tables with rows, and their rows per block.
const ROW_TABLES: [(&str, usize); 4] = [
    ("access_lists", 2),
    ("blocks", 1),
    ("logs", 2),
    ("transactions", 2),
];
/// Serial commits in name order, `blocks` last.
const SERIAL: [&str; 2] = ["--flush-publish-concurrency", "1"];

#[derive(Clone)]
struct Info;
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: CHAIN.into(),
                first_streamable_block_num: 100,
                ..Default::default()
            }))
        })
    }
}

/// Final blocks from 100 up to the request's stop, one every `pace`; a
/// request with a cursor resumes after it.
#[derive(Clone)]
struct Stream {
    pace: Duration,
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
            "" => 99,
            cursor => cursor
                .strip_prefix("block-")
                .and_then(|number| number.parse().ok())
                .unwrap_or_else(|| panic!("unexpected cursor {cursor}")),
        };
        let pace = self.pace;
        let responses = futures::stream::iter(after + 1..=request.stop_block_num).then(
            move |number| async move {
                if !pace.is_zero() {
                    tokio::time::sleep(pace).await;
                }
                Ok(response(number))
            },
        );
        Box::pin(async move {
            Ok(tonic::Response::new(
                Box::pin(responses) as Self::ResponseStream
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
            time: Some(prost_types::Timestamp {
                seconds: MIDNIGHT - 102 + number as i64,
                nanos: 0,
            }),
            ..Default::default()
        }),
    }
}

/// Where a dataset is written.
enum Storage {
    Local(PathBuf),
    S3(s3::Server),
}

impl Storage {
    async fn new(s3: bool, cwd: &Path) -> Self {
        if !s3 {
            return Storage::Local(cwd.join("dataset"));
        }
        let latency = s3::Latency {
            base_ms: 0,
            slow_every: 0,
            slow_ms: 0,
        };
        Storage::S3(
            s3::Server::start(&cwd.join("tls"), BUCKET, latency)
                .await
                .unwrap(),
        )
    }

    fn output(&self) -> String {
        match self {
            Storage::Local(root) => root.to_str().unwrap().to_string(),
            Storage::S3(_) => format!("s3://{BUCKET}/{CHAIN}"),
        }
    }

    /// The dataset as a local directory: the root itself, or a fresh copy of
    /// every object under the S3 prefix.
    fn copy(&self, cwd: &Path) -> PathBuf {
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

    /// Replaces a dataset file (a path relative to the root) with `bytes`.
    fn restore(&self, relative: &str, bytes: Vec<u8>) {
        match self {
            Storage::Local(root) => std::fs::write(root.join(relative), bytes).unwrap(),
            Storage::S3(server) => server.put(&format!("{CHAIN}/{relative}"), bytes.into()),
        }
    }

    /// Deletes dataset files (paths relative to the root), as a cleanup would.
    fn delete(&self, relative: &[String]) {
        for path in relative {
            match self {
                Storage::Local(root) => std::fs::remove_file(root.join(path)).unwrap(),
                Storage::S3(server) => assert!(server.remove(&format!("{CHAIN}/{path}"))),
            }
        }
    }

    /// Every file of the dataset under `prefix` (relative to the root).
    fn remove_tree(&self, prefix: &str) {
        match self {
            Storage::Local(root) => std::fs::remove_dir_all(root.join(prefix)).unwrap(),
            Storage::S3(server) => {
                for (key, _) in server.objects(&format!("{CHAIN}/{prefix}/")) {
                    assert!(server.remove(&key));
                }
            }
        }
    }

    /// The object_store options of `deltalake` for this storage.
    fn storage_options(&self) -> Value {
        match self {
            Storage::Local(_) => Value::Null,
            Storage::S3(server) => json!({
                "AWS_ENDPOINT_URL": server.endpoint,
                "AWS_ACCESS_KEY_ID": "loopback-access-key",
                "AWS_SECRET_ACCESS_KEY": "loopback-secret-key",
                "AWS_REGION": "us-east-1",
                "aws_conditional_put": "etag",
                "allow_invalid_certificates": "true",
            }),
        }
    }

    /// A `fireparq` command in `cwd` with a cleared environment and, for S3,
    /// the loopback endpoint's credentials and CA.
    fn fireparq(&self, cwd: &Path) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
        command.kill_on_drop(true).env_clear().current_dir(cwd);
        if let Storage::S3(server) = self {
            command
                .env("AWS_ACCESS_KEY_ID", "loopback-access-key")
                .env("AWS_SECRET_ACCESS_KEY", "loopback-secret-key")
                .env("AWS_REGION", "us-east-1")
                .env("AWS_ENDPOINT_URL_S3", &server.endpoint)
                .env("SSL_CERT_FILE", &server.ca_file);
        }
        command
    }
}

async fn output(mut command: tokio::process::Command) -> Output {
    tokio::time::timeout(Duration::from_secs(120), command.output())
        .await
        .expect("fireparq timed out")
        .unwrap()
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// One `build` to `stop` (exclusive) from a mock Firehose, with an optional
/// `FIREPARQ_DEBUG_FAULT` and extra arguments.
async fn run(
    storage: &Storage,
    cwd: &Path,
    stop: u64,
    fault: Option<&str>,
    args: &[&str],
    pace: Duration,
) -> Output {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Info)
            .add_service(Stream { pace })
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let mut command = storage.fireparq(cwd);
    command
        .args(["build", "--endpoint", &endpoint, "--block-type", "evm"])
        .args(["--start-block", "100", "--stop-block", &stop.to_string()])
        .args(["--flush-blocks", "1", "--stream-idle-timeout-secs", "0"])
        .args(["--output", &storage.output()])
        .args(args);
    if let Some(fault) = fault {
        command.env("FIREPARQ_DEBUG_FAULT", fault);
    }
    let result = output(command).await;
    server.abort();
    result
}

async fn build(storage: &Storage, cwd: &Path, stop: u64, args: &[&str]) -> Output {
    let result = run(storage, cwd, stop, None, args, Duration::ZERO).await;
    assert!(result.status.success(), "{}", text(&result));
    result
}

/// A `build` that the fault `fault` stops before `stop`.
async fn crash(storage: &Storage, cwd: &Path, stop: u64, fault: &str, args: &[&str]) -> Output {
    let result = run(storage, cwd, stop, Some(fault), args, Duration::ZERO).await;
    assert!(!result.status.success(), "{fault} stops the build");
    result
}

/// After an aborted S3 build: release its retained bucket owner with
/// `recovery release`, as an operator would once the process is gone and
/// every request it sent was answered (true of the loopback endpoint).
async fn release_after_abort(storage: &Storage, cwd: &Path) {
    if !matches!(storage, Storage::S3(_)) {
        return;
    }
    let mut status = storage.fireparq(cwd);
    status.args(["recovery", "status", &storage.output()]);
    let status = output(status).await;
    assert!(status.status.success(), "{}", text(&status));
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(
        status["ownership"], "owned",
        "an aborted build keeps its owner"
    );
    let mut release = storage.fireparq(cwd);
    release
        .args(["recovery", "release", &storage.output()])
        .args([
            "--expected-owner",
            status["owner"]["owner_id"].as_str().unwrap(),
            "--expected-generation",
            &status["owner"]["generation"].to_string(),
            "--stopped-writer-evidence",
            "the test reaped the aborted build process",
            "--provider-quiescence-evidence",
            "the loopback endpoint answered every request before the abort",
        ]);
    let released = output(release).await;
    assert!(released.status.success(), "{}", text(&released));
}

/// The S3 bucket owner's state from `recovery status`: `owned` or `released`.
async fn ownership(storage: &Storage, cwd: &Path) -> String {
    let mut status = storage.fireparq(cwd);
    status.args(["recovery", "status", &storage.output()]);
    let status = output(status).await;
    assert!(status.status.success(), "{}", text(&status));
    serde_json::from_slice::<Value>(&status.stdout).unwrap()["ownership"]
        .as_str()
        .unwrap()
        .to_string()
}

fn control(root: &Path, name: &str) -> Option<Value> {
    let bytes = match std::fs::read(root.join(".fireparq-ingest").join(name)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("{error}"),
    };
    let record: Value = serde_json::from_slice(&bytes).unwrap();
    (record["deleted"] == false).then(|| record["payload"].clone())
}

/// The authority's accepted ordinal.
fn authority(root: &Path) -> u64 {
    control(root, "state.json").unwrap()["checkpoint"]["ordinal"]
        .as_u64()
        .unwrap()
}

/// The pending journal, if any.
fn pending(root: &Path) -> Option<Value> {
    control(root, "pending.json")
}

/// The dataset-relative paths of the pending transaction's parts in `tables`.
fn pending_parts(root: &Path, tables: &[&str]) -> Vec<String> {
    pending(root).unwrap()["parts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|part| tables.contains(&part["table"].as_str().unwrap()))
        .map(|part| part["final_relative_path"].as_str().unwrap().to_string())
        .collect()
}

/// One table's Delta log, replayed from its JSON commits.
struct DeltaLog {
    table: String,
    commits: BTreeMap<u64, Vec<Value>>,
}

impl DeltaLog {
    fn read(root: &Path, table: &str) -> Self {
        let mut commits = BTreeMap::new();
        let log = root.join(table).join("_delta_log");
        for entry in std::fs::read_dir(&log).unwrap_or_else(|error| panic!("{log:?}: {error}")) {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_str().unwrap().to_string();
            let Some(version) = name.strip_suffix(".json") else {
                continue;
            };
            let Ok(version) = version.parse::<u64>() else {
                continue;
            };
            let actions = std::fs::read_to_string(&path)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            commits.insert(version, actions);
        }
        Self {
            table: table.to_string(),
            commits,
        }
    }

    fn version(&self) -> u64 {
        *self.commits.keys().last().unwrap()
    }

    fn actions<'a>(&'a self, kind: &'a str) -> impl Iterator<Item = (u64, &'a Value)> + 'a {
        self.commits.iter().flat_map(move |(version, actions)| {
            actions
                .iter()
                .filter_map(move |action| Some((*version, action.get(kind)?)))
        })
    }

    /// The `txn` versions of the commits that carry one, in log order.
    fn txns(&self) -> Vec<i64> {
        self.actions("txn")
            .map(|(_, txn)| txn["version"].as_i64().unwrap())
            .collect()
    }

    fn txn(&self) -> Option<i64> {
        self.txns().last().copied()
    }

    /// The commit time of the commit with `txn` version `txn`.
    fn committed_at(&self, txn: i64) -> Option<i64> {
        let (version, _) = self
            .actions("txn")
            .find(|(_, action)| action["version"].as_i64() == Some(txn))?;
        self.commits[&version]
            .iter()
            .find_map(|action| action.get("commitInfo")?.get("timestamp")?.as_i64())
    }

    /// The active files: every `add` without a later `remove`. A path that is
    /// added again while it is active is a duplicate.
    fn active(&self) -> BTreeSet<String> {
        let mut files = BTreeSet::new();
        for (_, actions) in &self.commits {
            for action in actions {
                if let Some(add) = action.get("add") {
                    let path = add["path"].as_str().unwrap().to_string();
                    assert!(
                        files.insert(path.clone()),
                        "{}: {path} added twice",
                        self.table
                    );
                }
                if let Some(remove) = action.get("remove") {
                    files.remove(remove["path"].as_str().unwrap());
                }
            }
        }
        files
    }

    /// Commits made by maintenance (`OPTIMIZE`), by version.
    fn optimize_versions(&self) -> Vec<u64> {
        self.actions("commitInfo")
            .filter(|(_, info)| info["operation"] == "OPTIMIZE")
            .map(|(version, _)| version)
            .collect()
    }

    /// The sorted `block_num` of every row the log's active files hold.
    fn block_numbers(&self, root: &Path) -> Vec<i64> {
        let mut numbers = Vec::new();
        for file in self.active() {
            let path = root.join(&self.table).join(&file);
            for batch in read_parquet(&path).unwrap_or_else(|error| panic!("{path:?}: {error}")) {
                let column = batch
                    .column_by_name("block_num")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .clone();
                numbers.extend(column.values().iter().copied());
            }
        }
        numbers.sort_unstable();
        numbers
    }
}

/// Checks a finished dataset of `blocks` through its logs: authority at one
/// ordinal per block and nothing pending; every table exists; each table
/// with rows holds each block exactly once per row (with `rows`), one commit
/// per block with `txn` rising by one up to the authority's ordinal, and
/// `blocks` committed after the other tables of every transaction.
fn check_lake(root: &Path, blocks: Range<u64>, rows: bool) -> BTreeMap<String, DeltaLog> {
    let ordinal = blocks.end - blocks.start;
    assert_eq!(authority(root), ordinal);
    assert!(pending(root).is_none(), "the journal is cleared");
    let mut logs = BTreeMap::new();
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        if !path.is_dir() || name.starts_with('.') || name == "_fireparq" {
            continue;
        }
        logs.insert(name.clone(), DeltaLog::read(root, &name));
    }
    assert!(
        logs.len() > ROW_TABLES.len(),
        "every mapper table: {:?}",
        logs.keys()
    );
    for (table, log) in &logs {
        let expected_txns: Vec<i64> = match ROW_TABLES.iter().find(|(name, _)| name == table) {
            Some(_) => (1..=ordinal as i64).collect(),
            None => Vec::new(),
        };
        assert_eq!(
            log.txns(),
            expected_txns,
            "{table}: one commit per transaction"
        );
    }
    for (table, per_block) in ROW_TABLES {
        if rows {
            let expected: Vec<i64> = blocks
                .clone()
                .flat_map(|block| std::iter::repeat_n(block as i64, per_block))
                .collect();
            assert_eq!(logs[table].block_numbers(root), expected, "{table}");
        }
    }
    for txn in 1..=ordinal as i64 {
        let blocks_at = logs["blocks"].committed_at(txn).unwrap();
        for (table, _) in ROW_TABLES {
            assert!(
                logs[table].committed_at(txn).unwrap() <= blocks_at,
                "{table}: blocks commits last in transaction {txn}"
            );
        }
    }
    logs
}

/// The Python with `deltalake`, or `None` locally when it is missing.
fn python() -> Option<PathBuf> {
    let candidate = std::env::var_os("FIREPARQ_POLARS_PYTHON").map(PathBuf::from);
    let available = candidate.as_ref().is_some_and(|python| {
        std::process::Command::new(python)
            .args(["-c", "import deltalake"])
            .env_clear()
            .output()
            .is_ok_and(|output| output.status.success())
    });
    if available {
        return candidate;
    }
    assert!(
        std::env::var_os("FIREPARQ_REQUIRE_POLARS").is_none(),
        "FIREPARQ_REQUIRE_POLARS is set but FIREPARQ_POLARS_PYTHON ({candidate:?}) cannot import deltalake"
    );
    eprintln!(
        "no deltalake maintenance: set FIREPARQ_POLARS_PYTHON to a Python with deltalake; deleting the files VACUUM would instead"
    );
    None
}

fn maintenance_spec(storage: &Storage, tables: &[&str], mode: &str) -> Value {
    json!({
        "root": match storage {
            Storage::Local(root) => root.to_str().unwrap().to_string(),
            Storage::S3(_) => storage.output(),
        },
        "tables": tables,
        "mode": mode,
        "storage": storage.storage_options(),
    })
}

fn maintain_command(python: &Path, spec: &Value) -> std::process::Command {
    let mut command = std::process::Command::new(python);
    command
        .env_clear()
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/engines/delta_maintain.py"
        ))
        .arg(spec.to_string());
    command
}

/// Runs one maintenance `mode` on `tables` and returns its report, after
/// checking that no call failed.
fn maintain(python: &Path, storage: &Storage, tables: &[&str], mode: &str) -> Value {
    let spec = maintenance_spec(storage, tables, mode);
    let result = maintain_command(python, &spec).output().unwrap();
    assert!(
        result.status.success(),
        "{mode}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    for table in tables {
        assert_eq!(
            report["tables"][table]["errors"],
            json!([]),
            "{mode} {table}: {report}"
        );
    }
    report
}

fn row_tables() -> Vec<&'static str> {
    ROW_TABLES.iter().map(|(table, _)| *table).collect()
}

fn scratch() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    (dir, cwd)
}

/// §4 "`CommittedPersisted`, no Delta commit yet": the restart commits the
/// transaction to every table, `blocks` last, then advances authority.
async fn committed_without_a_delta_commit(s3: bool) {
    let (_dir, cwd) = scratch();
    let storage = Storage::new(s3, &cwd).await;
    crash(&storage, &cwd, 102, "crash-at:CommittedPersisted", &[]).await;
    let root = storage.copy(&cwd);
    assert_eq!(authority(&root), 0);
    assert_eq!(pending(&root).unwrap()["phase"], "committed");
    for (table, _) in ROW_TABLES {
        assert_eq!(
            DeltaLog::read(&root, table).version(),
            0,
            "{table}: no commit yet"
        );
    }
    release_after_abort(&storage, &cwd).await;
    build(&storage, &cwd, 102, &[]).await;
    check_lake(&storage.copy(&cwd), 100..102, true);
}

/// §4 "Between table commits": `access_lists` and `logs` hold the first
/// transaction, `transactions` and `blocks` do not; the restart commits only
/// those two, `blocks` last.
async fn between_table_commits(s3: bool) {
    let (_dir, cwd) = scratch();
    let storage = Storage::new(s3, &cwd).await;
    crash(
        &storage,
        &cwd,
        102,
        "crash-after-delta-commit:logs",
        &SERIAL,
    )
    .await;
    let root = storage.copy(&cwd);
    assert_eq!(authority(&root), 0);
    let txns: BTreeMap<_, _> = row_tables()
        .into_iter()
        .map(|table| (table, DeltaLog::read(&root, table).txn()))
        .collect();
    assert_eq!(
        txns,
        BTreeMap::from([
            ("access_lists", Some(1)),
            ("blocks", None),
            ("logs", Some(1)),
            ("transactions", None),
        ])
    );
    release_after_abort(&storage, &cwd).await;
    build(&storage, &cwd, 102, &SERIAL).await;
    let logs = check_lake(&storage.copy(&cwd), 100..102, true);
    // The committed tables were skipped: one commit each for transaction 1.
    assert_eq!(logs["logs"].version(), 2);
    assert!(logs["transactions"].committed_at(1) >= logs["logs"].committed_at(1));
}

/// §4 "A table's commit PUT is ambiguous": the `transactions` commit lands
/// but its response is lost. The build fails and, on S3, releases its owner
/// anyway: the log resolves the outcome, so the latch stays clear (design
/// §3.5). The restart reads `txn`, skips `transactions` and commits `blocks`.
async fn an_unknown_commit_outcome(s3: bool) {
    let (_dir, cwd) = scratch();
    let storage = Storage::new(s3, &cwd).await;
    let failed = crash(
        &storage,
        &cwd,
        102,
        "delta-commit-lost-response:transactions",
        &SERIAL,
    )
    .await;
    let message = text(&failed);
    assert!(
        message.contains("outcome is unknown") && message.contains("its response was lost"),
        "{message}"
    );
    let root = storage.copy(&cwd);
    assert_eq!(authority(&root), 0);
    assert_eq!(
        DeltaLog::read(&root, "transactions").txn(),
        Some(1),
        "it landed"
    );
    assert_eq!(DeltaLog::read(&root, "blocks").txn(), None);
    if s3 {
        assert_eq!(ownership(&storage, &cwd).await, "released");
    }
    build(&storage, &cwd, 102, &SERIAL).await;
    let logs = check_lake(&storage.copy(&cwd), 100..102, true);
    assert_eq!(
        logs["transactions"].version(),
        2,
        "committed once per transaction"
    );
}

/// §4 "All Delta commits done, before `AuthorityAdvanced`" and
/// "`AuthorityAdvanced`, before mirror or clear": after the crash,
/// maintenance compacts the pending transaction's parts away (OPTIMIZE, then
/// a lite VACUUM of retention 0). The restart reads no part and completes.
async fn committed_parts_compacted_before_the_restart(s3: bool, fault: &str) {
    let (_dir, cwd) = scratch();
    let storage = Storage::new(s3, &cwd).await;
    // Blocks 100..=102, then 103 (the second part of 2023-11-15) crashes.
    build(&storage, &cwd, 103, &[]).await;
    crash(&storage, &cwd, 104, fault, &[]).await;
    let root = storage.copy(&cwd);
    assert_eq!(pending(&root).unwrap()["phase"], "committed", "{fault}");
    let parts = pending_parts(&root, &row_tables());
    assert_eq!(parts.len(), ROW_TABLES.len());
    let rows = match python() {
        Some(python) => {
            let report = maintain(&python, &storage, &row_tables(), "compact");
            for table in row_tables() {
                assert_eq!(report["tables"][table]["optimize_commits"], 1, "{report}");
            }
            true
        }
        None => {
            storage.delete(&parts);
            false
        }
    };
    let root = storage.copy(&cwd);
    for part in &parts {
        assert!(!root.join(part).exists(), "{part} was vacuumed");
    }
    release_after_abort(&storage, &cwd).await;
    build(&storage, &cwd, 104, &[]).await;
    let logs = check_lake(&storage.copy(&cwd), 100..104, rows);
    if rows {
        for table in row_tables() {
            assert_eq!(logs[table].optimize_versions().len(), 1, "{table}");
        }
    }
}

async fn all_delta_commits_done(s3: bool) {
    committed_parts_compacted_before_the_restart(s3, "crash-after-delta-commit:blocks").await;
}

async fn authority_advanced(s3: bool) {
    committed_parts_compacted_before_the_restart(s3, "crash-at:AuthorityAdvanced").await;
}

/// §4 "During table creation at initialization": the first start dies once
/// `logs` is created (tables are created in name order). The restart creates
/// the others, keeps the existing ones as they were, and continues. Later, a
/// table whose log was removed is refused with guidance.
async fn table_creation(s3: bool) {
    let (_dir, cwd) = scratch();
    let storage = Storage::new(s3, &cwd).await;
    crash(
        &storage,
        &cwd,
        102,
        "crash-after-delta-create:logs",
        &SERIAL,
    )
    .await;
    let root = storage.copy(&cwd);
    assert_eq!(authority(&root), 0);
    let created = |root: &Path, table: &str| {
        root.join(table)
            .join("_delta_log/00000000000000000000.json")
            .is_file()
    };
    for table in ["access_lists", "blocks", "logs"] {
        assert!(
            created(&root, table),
            "{table} was created before the crash"
        );
    }
    for table in ["nonce_changes", "transactions", "withdrawals"] {
        assert!(!created(&root, table), "{table} was not created yet");
    }
    let first = std::fs::read(root.join("logs/_delta_log/00000000000000000000.json")).unwrap();
    release_after_abort(&storage, &cwd).await;
    build(&storage, &cwd, 102, &SERIAL).await;
    let root = storage.copy(&cwd);
    check_lake(&root, 100..102, true);
    assert_eq!(
        std::fs::read(root.join("logs/_delta_log/00000000000000000000.json")).unwrap(),
        first,
        "an existing table is validated, not created again"
    );
    // Once the stream has committed, a table without its log is refused.
    storage.remove_tree("withdrawals/_delta_log");
    let refused = run(&storage, &cwd, 104, None, &[], Duration::ZERO).await;
    assert!(!refused.status.success());
    let message = text(&refused);
    assert!(
        message.contains("table `withdrawals` has no Delta log: its rows are unreachable")
            && message.contains("build into a new, empty output root"),
        "{message}"
    );
    assert_eq!(authority(&storage.copy(&cwd)), 2);
}

/// §4 "`txn` > L": the authority is restored from an older copy while the
/// logs hold later transactions. The start refuses before any change.
async fn log_ahead_of_authority(s3: bool) {
    let (_dir, cwd) = scratch();
    let storage = Storage::new(s3, &cwd).await;
    build(&storage, &cwd, 102, &[]).await;
    let older = std::fs::read(storage.copy(&cwd).join(".fireparq-ingest/state.json")).unwrap();
    build(&storage, &cwd, 104, &[]).await;
    storage.restore(".fireparq-ingest/state.json", older);
    let before: Vec<u64> = row_tables()
        .into_iter()
        .map(|table| DeltaLog::read(&storage.copy(&cwd), table).version())
        .collect();
    let refused = run(&storage, &cwd, 105, None, &[], Duration::ZERO).await;
    assert!(!refused.status.success());
    let message = text(&refused);
    assert!(
        message.contains("is ahead of this dataset's authority")
            && message.contains("holds transaction 4")
            && message.contains("up to ordinal 2")
            && message.contains("restored from an older copy"),
        "{message}"
    );
    let root = storage.copy(&cwd);
    assert_eq!(authority(&root), 2);
    let after: Vec<u64> = row_tables()
        .into_iter()
        .map(|table| DeltaLog::read(&root, table).version())
        .collect();
    assert_eq!(before, after, "nothing was committed");
    if s3 {
        assert_eq!(ownership(&storage, &cwd).await, "released");
    }
}

/// §4 last row and §4.1: after a crash between table commits, a full VACUUM
/// of retention 0 (never run by the deployed job) deletes the uncommitted
/// parts of `transactions` and `blocks`. The restart fails closed: it names
/// the parts and the tables that hold the transaction, and keeps the journal.
async fn a_vacuumed_uncommitted_part(s3: bool) {
    let (_dir, cwd) = scratch();
    let storage = Storage::new(s3, &cwd).await;
    crash(
        &storage,
        &cwd,
        102,
        "crash-after-delta-commit:logs",
        &SERIAL,
    )
    .await;
    let root = storage.copy(&cwd);
    let untracked = pending_parts(&root, &["blocks", "transactions"]);
    let committed = pending_parts(&root, &["access_lists", "logs"]);
    match python() {
        Some(python) => {
            let report = maintain(&python, &storage, &row_tables(), "full-vacuum");
            for table in ["blocks", "transactions"] {
                assert_eq!(report["tables"][table]["vacuumed"], 1, "{report}");
            }
        }
        None => storage.delete(&untracked),
    }
    let root = storage.copy(&cwd);
    assert!(untracked.iter().all(|part| !root.join(part).exists()));
    assert!(committed.iter().all(|part| root.join(part).exists()));
    release_after_abort(&storage, &cwd).await;
    let refused = run(&storage, &cwd, 102, None, &SERIAL, Duration::ZERO).await;
    assert!(!refused.status.success());
    let message = text(&refused);
    assert!(
        message.contains("cannot roll the Committed transaction of ordinals 1..=1")
            && message.contains("forward into the Delta tables blocks, transactions:")
            && message.contains("Tables that already hold the transaction: access_lists, logs.")
            && message.contains("§4.1")
            && message.contains(&format!("({}) is missing", untracked[0])),
        "{message}"
    );
    let root = storage.copy(&cwd);
    assert_eq!(authority(&root), 0);
    assert_eq!(
        pending(&root).unwrap()["phase"],
        "committed",
        "evidence kept"
    );
    assert_eq!(DeltaLog::read(&root, "logs").txn(), Some(1));
    assert_eq!(DeltaLog::read(&root, "blocks").txn(), None);
    if s3 {
        assert_eq!(ownership(&storage, &cwd).await, "released");
    }
}

/// `recovery recover` rolls a Committed transaction forward like a `build`
/// start does, and the next `build` continues after it.
async fn recovery_recover(s3: bool) {
    let (_dir, cwd) = scratch();
    let storage = Storage::new(s3, &cwd).await;
    crash(
        &storage,
        &cwd,
        102,
        "crash-after-delta-commit:logs",
        &SERIAL,
    )
    .await;
    release_after_abort(&storage, &cwd).await;
    let mut recover = storage.fireparq(&cwd);
    recover.args(["recovery", "recover", &storage.output()]);
    let recovered = output(recover).await;
    assert!(recovered.status.success(), "{}", text(&recovered));
    assert_eq!(
        serde_json::from_slice::<Value>(&recovered.stdout).unwrap(),
        json!({"recovered_protected_roots": 1})
    );
    check_lake(&storage.copy(&cwd), 100..101, true);
    build(&storage, &cwd, 102, &[]).await;
    check_lake(&storage.copy(&cwd), 100..102, true);
}

/// #636: `deltalake` maintenance runs beside `build` while it catches up
/// (block times from 2023) and across a restart, rewriting and vacuuming the
/// very dates being appended to. Every flush commits, maintenance never
/// fails, OPTIMIZE lands between fireparq's commits, and the tables end with
/// each block exactly once.
async fn maintenance_beside_build(s3: bool) {
    let (_dir, cwd) = scratch();
    let storage = Storage::new(s3, &cwd).await;
    let Some(python) = python() else {
        return;
    };
    // The tables exist once a first run has started.
    build(&storage, &cwd, 101, &[]).await;
    let stop_file = cwd.join("stop-maintenance");
    let mut spec = maintenance_spec(&storage, &row_tables(), "loop");
    spec["stop_file"] = json!(stop_file);
    spec["min_rounds"] = json!(2);
    let maintenance = maintain_command(&python, &spec)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let pace = Duration::from_millis(60);
    for stop in [124, 140] {
        let result = run(&storage, &cwd, stop, None, &[], pace).await;
        assert!(result.status.success(), "{}", text(&result));
    }
    std::fs::write(&stop_file, b"stop").unwrap();
    let finished = maintenance.wait_with_output().unwrap();
    assert!(
        finished.status.success(),
        "{}",
        String::from_utf8_lossy(&finished.stderr)
    );
    let report: Value = serde_json::from_slice(&finished.stdout).unwrap();
    for table in row_tables() {
        assert_eq!(report["tables"][table]["errors"], json!([]), "{report}");
    }
    // A last round, then the restart after maintenance continues.
    maintain(&python, &storage, &row_tables(), "compact");
    build(&storage, &cwd, 142, &[]).await;
    let root = storage.copy(&cwd);
    let logs = check_lake(&root, 100..142, true);
    // Polars `scan_delta` reads the same rows through the compacted,
    // vacuumed and checkpointed logs.
    let spec = json!({"root": root, "tables": row_tables(), "day": "2023-11-15", "decimals": {}});
    let read = std::process::Command::new(&python)
        .env_clear()
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/engines/delta_check.py"
        ))
        .arg(spec.to_string())
        .output()
        .unwrap();
    assert!(
        read.status.success(),
        "polars: {}",
        String::from_utf8_lossy(&read.stderr)
    );
    let read: Value = serde_json::from_slice(&read.stdout).unwrap();
    for (table, per_block) in ROW_TABLES {
        let expected: Vec<u64> = (100..142)
            .flat_map(|block| std::iter::repeat_n(block, per_block))
            .collect();
        assert_eq!(
            read["tables"][table]["block_nums"],
            json!(expected),
            "polars {table}"
        );
    }
    let mut interleaved = 0;
    for table in row_tables() {
        let log = &logs[table];
        let fireparq: Vec<u64> = log.actions("txn").map(|(version, _)| version).collect();
        interleaved += log
            .optimize_versions()
            .iter()
            .filter(|version| {
                fireparq.first().is_some_and(|first| first < version)
                    && fireparq.last().is_some_and(|last| last > version)
            })
            .count();
    }
    assert!(
        interleaved > 0,
        "OPTIMIZE ran between fireparq's commits: {report}"
    );
    eprintln!(
        "maintenance beside build ({}): {} rounds, {interleaved} OPTIMIZE commits between fireparq commits",
        if s3 { "s3" } else { "local" },
        report["rounds"]
    );
}

macro_rules! on_both_stores {
    ($($scenario:ident),* $(,)?) => {
        mod on_local {
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn $scenario() {
                    super::$scenario(false).await;
                }
            )*
        }
        mod on_s3 {
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn $scenario() {
                    super::$scenario(true).await;
                }
            )*
        }
    };
}

on_both_stores!(
    committed_without_a_delta_commit,
    between_table_commits,
    an_unknown_commit_outcome,
    all_delta_commits_done,
    authority_advanced,
    table_creation,
    log_ahead_of_authority,
    a_vacuumed_uncommitted_part,
    recovery_recover,
    maintenance_beside_build,
);
