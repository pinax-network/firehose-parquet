//! Exercise reversible CLI selection, append-only events and the bounded warning,
//! the durable per-row `stream_ordinal`, the README canonical live view, and a
//! live build whose expired committed parts disappear under it.
use arrow::array::{StringArray, UInt64Array};
use firehose_parquet::{cursor::load_cursor_parquet, writer::read_parquet};
use firehose_protos::{eth, firehose};
use prost::Message;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tonic::codegen::{http, BoxFuture, Service};

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

#[derive(Clone)]
struct Stream {
    final_only: bool,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        assert_eq!(request.final_blocks_only, self.final_only);
        assert_eq!(
            (request.start_block_num, request.stop_block_num),
            (100, 101)
        );
        assert!(request.cursor.is_empty());
        assert_eq!(
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            0
        );
        let events = if self.final_only {
            vec![(100, 0xaa, 3), (101, 0xcc, 3)]
        } else {
            // Two histories can have equal unordered event sets but different
            // current state. Preserve recurrence rather than collapsing identity.
            vec![
                (100, 0xaa, 1),
                (100, 0xaa, 2),
                (100, 0xbb, 1),
                (100, 0xbb, 2),
                (100, 0xaa, 1),
                (101, 0xcc, 1),
            ]
        };
        let lib_num = if self.final_only { 101 } else { 99 };
        let responses: Vec<_> = events
            .into_iter()
            .enumerate()
            .map(|(event, (number, hash, step))| {
                Ok(firehose::Response {
                    block: Some(prost_types::Any {
                        type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".into(),
                        value: eth::Block {
                            number,
                            hash: vec![hash; 32].into(),
                            ..Default::default()
                        }
                        .encode_to_vec(),
                    }),
                    step,
                    cursor: format!("event-{event}"),
                    metadata: Some(firehose::BlockMetadata {
                        num: number,
                        id: format!("{hash:02x}").repeat(32),
                        parent_num: number - 1,
                        parent_id: "11".repeat(32),
                        lib_num,
                        time: Some(prost_types::Timestamp {
                            seconds: 1_700_000_000 + number as i64,
                            nanos: 0,
                        }),
                        ..Default::default()
                    }),
                })
            })
            .collect();
        Box::pin(async move {
            Ok(tonic::Response::new(
                Box::pin(futures::stream::iter(responses)) as Self::ResponseStream,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_false_reaches_rpc_preserves_recurrence_and_warns_only_non_final() {
    for final_only in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stream = Stream {
            final_only,
            calls: calls.clone(),
        };
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(Info)
                .add_service(stream)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output");
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
        child
            .kill_on_drop(true)
            .env_clear()
            .current_dir(dir.path())
            .args([
                "build",
                "--endpoint",
                &endpoint,
                "--block-type",
                "evm",
                "--start-block",
                "100",
                "--stop-block",
                "102",
                "--partition",
                "none",
                "--flush-blocks",
                "2",
                "--output",
            ])
            .arg(&output)
            .arg(format!("--final-blocks-only={final_only}"));
        let result = tokio::time::timeout(Duration::from_secs(15), child.output())
            .await
            .unwrap()
            .unwrap();
        let log = format!(
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(result.status.success(), "{log}");
        assert_eq!(
            log.contains("does not prove its tail is final"),
            !final_only,
            "{log}"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        // `--output` is the dataset root.
        let root = output.clone();
        let mut values = BTreeMap::new();
        for entry in std::fs::read_dir(root.join("blocks")).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|ext| ext != "parquet") {
                continue;
            }
            for batch in read_parquet(&path).unwrap() {
                let numbers = batch
                    .column_by_name("block_num")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let ids = batch
                    .column_by_name("block_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let steps = batch.column_by_name("fork_step");
                assert_eq!(steps.is_none(), final_only);
                for row in 0..batch.num_rows() {
                    let step = steps
                        .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap().value(row))
                        .unwrap_or("FINAL");
                    *values
                        .entry((
                            numbers.value(row),
                            ids.value(row).to_string(),
                            step.to_string(),
                        ))
                        .or_insert(0) += 1;
                }
            }
        }
        assert_eq!(
            values.values().sum::<usize>(),
            if final_only { 2 } else { 6 }
        );
        if !final_only {
            assert_eq!(
                values.get(&(100, format!("0x{}", "aa".repeat(32)), "NEW".into())),
                Some(&2)
            );
            assert_eq!(values.values().filter(|&&count| count == 1).count(), 4);
        }
        let cursor = load_cursor_parquet(&root.join("_fireparq/cursor.parquet"))
            .unwrap()
            .unwrap();
        assert_eq!(cursor.last_block_num, 101);
        assert_eq!(cursor.final_blocks_only, final_only);
        server.abort();
    }
}

// ---------------------------------------------------------------------------
// Cursor-aware replay: `stream_ordinal`, the README live view and expired parts
// ---------------------------------------------------------------------------

const CHAIN: &str = "nonfinal-test";
/// 2023-11-14 22:00:00 UTC, the first second of an hour partition.
const HOUR: i64 = 1_699_999_200;
const NEW: i32 = 1;
const UNDO: i32 = 2;
const FINAL: i32 = 3;

/// One Firehose envelope. Each block carries one transaction whose hash is
/// derived from the block id, so child rows can be told apart per block.
#[derive(Clone, Copy)]
struct Envelope {
    num: u64,
    id: u8,
    step: i32,
    time: i64,
}
const fn envelope(num: u64, id: u8, step: i32, time: i64) -> Envelope {
    Envelope {
        num,
        id,
        step,
        time,
    }
}
fn block_id(id: u8) -> String {
    format!("0x{}", format!("{id:02x}").repeat(32))
}
fn tx_hash(id: u8) -> String {
    format!("0x{}", format!("{:02x}", id ^ 0x0f).repeat(32))
}
fn responses(envelopes: &[Envelope]) -> Vec<firehose::Response> {
    envelopes
        .iter()
        .enumerate()
        .map(|(index, e)| firehose::Response {
            block: Some(prost_types::Any {
                type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".into(),
                value: eth::Block {
                    number: e.num,
                    hash: vec![e.id; 32].into(),
                    transaction_traces: vec![eth::TransactionTrace {
                        hash: vec![e.id ^ 0x0f; 32].into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }
                .encode_to_vec(),
            }),
            step: e.step,
            cursor: format!("event-{index}"),
            metadata: Some(firehose::BlockMetadata {
                num: e.num,
                id: format!("{:02x}", e.id).repeat(32),
                parent_num: e.num - 1,
                parent_id: "11".repeat(32),
                // A FINAL event is at or below the last irreversible block.
                lib_num: if e.step == FINAL { e.num } else { 99 },
                time: Some(prost_types::Timestamp {
                    seconds: e.time,
                    nanos: 0,
                }),
                ..Default::default()
            }),
        })
        .collect()
}

/// One expected Blocks request and how to answer it.
struct Plan {
    cursor: &'static str,
    final_only: bool,
    /// The inclusive `stop_block_num` the request must carry.
    stop: u64,
    /// Fail the stream with a retryable status after this many envelopes.
    fail_after: Option<usize>,
    /// Hold the stream open after this many envelopes until the test adds a
    /// permit to the gate.
    pause_after: Option<usize>,
}
impl Plan {
    fn new(cursor: &'static str, final_only: bool, stop: u64) -> Self {
        Self {
            cursor,
            final_only,
            stop,
            fail_after: None,
            pause_after: None,
        }
    }
}

/// Replays a fixed envelope log from the position after the request cursor.
#[derive(Clone)]
struct Replay {
    events: Arc<Vec<firehose::Response>>,
    plans: Arc<Mutex<VecDeque<Plan>>>,
    cursors: Arc<Mutex<Vec<String>>>,
    gate: Arc<tokio::sync::Semaphore>,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Replay {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        self.cursors.lock().unwrap().push(request.cursor.clone());
        let Some(plan) = self.plans.lock().unwrap().pop_front() else {
            return Box::pin(async {
                Err(tonic::Status::invalid_argument("unexpected Blocks RPC"))
            });
        };
        assert_eq!(
            (
                request.cursor.as_str(),
                request.final_blocks_only,
                request.start_block_num,
                request.stop_block_num
            ),
            (plan.cursor, plan.final_only, 100, plan.stop)
        );
        let from = if request.cursor.is_empty() {
            0
        } else {
            self.events
                .iter()
                .position(|event| event.cursor == request.cursor)
                .expect("unknown fixture cursor")
                + 1
        };
        let replies: Vec<_> = self.events[from..]
            .iter()
            .take_while(|event| event.metadata.as_ref().unwrap().num <= plan.stop)
            .cloned()
            .collect();
        let gate = self.gate.clone();
        let stream = futures::stream::unfold(
            (replies.into_iter(), 0_usize, Some(plan)),
            move |(mut replies, sent, plan)| {
                let gate = gate.clone();
                async move {
                    let plan = plan?;
                    if plan.fail_after == Some(sent) {
                        let status = tonic::Status::unavailable("injected disconnect");
                        return Some((Err(status), (replies, sent, None)));
                    }
                    if plan.pause_after == Some(sent) {
                        gate.acquire().await.unwrap().forget();
                    }
                    let next = replies.next()?;
                    Some((Ok(next), (replies, sent + 1, Some(plan))))
                }
            },
        );
        Box::pin(async move {
            Ok(tonic::Response::new(
                Box::pin(stream) as Self::ResponseStream
            ))
        })
    }
}
service!(Replay, "sf.firehose.v2.Stream", server_streaming);

struct ReplayServer {
    endpoint: String,
    cursors: Arc<Mutex<Vec<String>>>,
    plans: Arc<Mutex<VecDeque<Plan>>>,
    gate: Arc<tokio::sync::Semaphore>,
    task: tokio::task::JoinHandle<()>,
}
impl ReplayServer {
    async fn start(envelopes: &[Envelope], plans: Vec<Plan>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let replay = Replay {
            events: Arc::new(responses(envelopes)),
            plans: Arc::new(Mutex::new(VecDeque::from(plans))),
            cursors: Arc::default(),
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
        };
        let (cursors, plans, gate) = (
            replay.cursors.clone(),
            replay.plans.clone(),
            replay.gate.clone(),
        );
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(Info)
                .add_service(replay)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        Self {
            endpoint,
            cursors,
            plans,
            gate,
            task,
        }
    }
    fn cursors(&self) -> Vec<String> {
        self.cursors.lock().unwrap().clone()
    }
    fn assert_drained(&self) {
        assert!(self.plans.lock().unwrap().is_empty(), "unused Blocks plans");
    }
}
impl Drop for ReplayServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `fireparq <args>` from `cwd` with a cleared environment: no `S3_BUCKET`,
/// `AWS_*` or `.env` from the repository can reach it.
fn fireparq(cwd: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    command.kill_on_drop(true).env_clear().current_dir(cwd);
    command
}
async fn finish(command: &mut tokio::process::Command) -> (bool, String, Vec<u8>) {
    let output = tokio::time::timeout(Duration::from_secs(60), command.output())
        .await
        .expect("fireparq timed out")
        .unwrap();
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), log, output.stdout)
}
/// A bounded `build` of the mock chain into `output`, hour partitions.
fn build(
    server: &ReplayServer,
    cwd: &Path,
    output: &Path,
    final_only: bool,
    stop: u64,
    flush_blocks: u64,
) -> tokio::process::Command {
    let mut command = fireparq(cwd);
    command
        .args(["build", "--endpoint", &server.endpoint])
        .args(["--block-type", "evm", "--start-block", "100"])
        .args(["--stop-block", &stop.to_string(), "--partition", "hour"])
        .args(["--flush-blocks", &flush_blocks.to_string(), "--output"])
        .arg(output)
        .arg(format!("--final-blocks-only={final_only}"));
    command
}
async fn succeed(mut command: tokio::process::Command) -> String {
    let (success, log, _) = finish(&mut command).await;
    assert!(success, "{log}");
    log
}

/// Every `.parquet` file of `table` under `chain_root`, at any partition depth.
fn table_files(chain_root: &Path, table: &str) -> Vec<PathBuf> {
    let mut pending = vec![chain_root.join(table)];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// One non-final row, with the transaction hash for the `transactions` table.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EventRow {
    ordinal: u64,
    num: u64,
    id: String,
    step: String,
    tx_hash: Option<String>,
}
fn string_column<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing {name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{name} is not Utf8"))
}
fn u64_column<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a UInt64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing {name}"))
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap_or_else(|| panic!("{name} is not UInt64"))
}
/// The rows of one table, sorted by `stream_ordinal`. Each part's rows must
/// lie inside the accepted-event window its deterministic name records
/// (`part-v1-<stream>-<first>-<last>-<transaction>-<index>.parquet`).
fn event_rows(chain_root: &Path, table: &str) -> Vec<EventRow> {
    let mut rows = Vec::new();
    for path in table_files(chain_root, table) {
        let name = path.file_name().unwrap().to_str().unwrap();
        let window: Vec<u64> = name
            .strip_prefix("part-v1-")
            .unwrap_or_else(|| panic!("unexpected part name {name}"))
            .split('-')
            .skip(1)
            .take(2)
            .map(|ordinal| ordinal.parse().unwrap())
            .collect();
        for batch in read_parquet(&path).unwrap() {
            let schema = batch.schema();
            assert_eq!(
                schema.index_of("stream_ordinal").unwrap(),
                schema.index_of("fork_step").unwrap() + 1,
                "{name}: stream_ordinal directly follows fork_step"
            );
            let ordinals = u64_column(&batch, "stream_ordinal");
            for row in 0..batch.num_rows() {
                let ordinal = ordinals.value(row);
                assert!(
                    (window[0]..=window[1]).contains(&ordinal),
                    "{name}: ordinal {ordinal} outside its part window {window:?}"
                );
                rows.push(EventRow {
                    ordinal,
                    num: u64_column(&batch, "block_num").value(row),
                    id: string_column(&batch, "block_id").value(row).to_string(),
                    step: string_column(&batch, "fork_step").value(row).to_string(),
                    tx_hash: (table == "transactions")
                        .then(|| string_column(&batch, "hash").value(row).to_string()),
                });
            }
        }
    }
    rows.sort();
    rows
}

/// The DuckDB CLI for the README query check: `FIREPARQ_DUCKDB`, else
/// `duckdb` on `PATH`. CI installs a pinned CLI and sets
/// `FIREPARQ_REQUIRE_DUCKDB`, so the check can only be skipped locally.
fn duckdb() -> Option<PathBuf> {
    // Resolve to an absolute path: the query runs with a cleared environment.
    let candidate = std::env::var_os("FIREPARQ_DUCKDB")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::split_paths(&std::env::var_os("PATH")?)
                .map(|directory| directory.join("duckdb"))
                .find(|path| path.is_file())
        })
        .unwrap_or_else(|| PathBuf::from("duckdb"));
    let available = std::process::Command::new(&candidate)
        .arg("-version")
        .output()
        .is_ok_and(|output| output.status.success());
    if available {
        return Some(candidate);
    }
    assert!(
        std::env::var_os("FIREPARQ_REQUIRE_DUCKDB").is_none(),
        "FIREPARQ_REQUIRE_DUCKDB is set but the DuckDB CLI {candidate:?} is unavailable"
    );
    eprintln!("skipping the README DuckDB query check: no DuckDB CLI ({candidate:?})");
    None
}
/// Run `sql` in a fresh in-memory DuckDB, without any `~/.duckdbrc`, and
/// return the last statement's rows.
fn duckdb_rows(duckdb: &Path, cwd: &Path, sql: &str) -> Vec<Value> {
    let init = cwd.join("empty.duckdbrc");
    std::fs::write(&init, "").unwrap();
    let output = std::process::Command::new(duckdb)
        .env_clear()
        .current_dir(cwd)
        .arg("-init")
        .arg(&init)
        .args(["-json", "-c", sql])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{sql}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    if stdout.trim().is_empty() {
        return Vec::new();
    }
    let rows: Vec<Value> =
        serde_json::from_str(stdout.trim()).unwrap_or_else(|error| panic!("{error}: {stdout}"));
    rows.iter().map(scalars_as_text).collect()
}
/// DuckDB versions differ in whether JSON output quotes `UBIGINT` values, so
/// numbers and strings are compared as text.
fn scalars_as_text(value: &Value) -> Value {
    match value {
        Value::Number(number) => Value::String(number.to_string()),
        Value::Array(items) => Value::Array(items.iter().map(scalars_as_text).collect()),
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(name, field)| (name.clone(), scalars_as_text(field)))
                .collect(),
        ),
        other => other.clone(),
    }
}
/// The SQL blocks of the README "Non-final streams and reorgs" section: the
/// live view first, then the two-bucket union.
fn readme_sql() -> Vec<String> {
    let readme =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../README.md")).unwrap();
    let section = readme
        .split("### Non-final streams and reorgs\n")
        .nth(1)
        .expect("README non-final section")
        .split("\n### ")
        .next()
        .unwrap();
    section
        .split("```sql\n")
        .skip(1)
        .map(|block| block.split("\n```").next().unwrap().to_string())
        .collect()
}

/// A reorg-heavy non-final history:
/// NEW(A), UNDO(A), NEW(B) at 100; NEW(C), UNDO(C), NEW(C) at 101 (the next
/// hour); NEW(D), UNDO(D) at 102, an unreplaced UNDO at the tip.
const HISTORY: [Envelope; 8] = [
    envelope(100, 0xaa, NEW, HOUR + 3_598),
    envelope(100, 0xaa, UNDO, HOUR + 3_598),
    envelope(100, 0xbb, NEW, HOUR + 3_599),
    envelope(101, 0xcc, NEW, HOUR + 3_600),
    envelope(101, 0xcc, UNDO, HOUR + 3_600),
    envelope(101, 0xcc, NEW, HOUR + 3_600),
    envelope(102, 0xdd, NEW, HOUR + 3_601),
    envelope(102, 0xdd, UNDO, HOUR + 3_601),
];

/// `stream_ordinal` is the accepted-event ordinal: one per delivered envelope,
/// strictly increasing in delivery order, continued across an in-process
/// reconnect and a restarted build, identical for the rows of one envelope in
/// every table and inside its part's recorded window. The README live view
/// (and its two-bucket union with a final-only dataset) then selects exactly
/// the canonical head: B at 100, C at 101 once (the later NEW), nothing at 102.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_ordinals_are_durable_and_the_readme_live_view_selects_the_canonical_head() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("live");
    // Run 1 (stop 102) is cut after NEW(B) and reconnects from its cursor; run 2
    // (stop 103) is a new process resuming from authority.
    let server = ReplayServer::start(
        &HISTORY,
        vec![
            Plan {
                fail_after: Some(3),
                ..Plan::new("", false, 101)
            },
            Plan::new("event-2", false, 101),
            Plan::new("event-5", false, 102),
        ],
    )
    .await;
    let log = succeed(build(&server, dir.path(), &live, false, 102, 2)).await;
    assert!(log.contains("will reconnect"), "{log}");
    succeed(build(&server, dir.path(), &live, false, 103, 2)).await;
    assert_eq!(server.cursors(), ["", "event-2", "event-5"]);
    server.assert_drained();

    let live_root = live.clone();
    let blocks = event_rows(&live_root, "blocks");
    let expected: Vec<_> = HISTORY
        .iter()
        .enumerate()
        .map(|(index, e)| {
            (
                index as u64 + 1,
                e.num,
                block_id(e.id),
                if e.step == NEW { "NEW" } else { "UNDO" }.to_string(),
            )
        })
        .collect();
    assert_eq!(
        blocks
            .iter()
            .map(|row| (row.ordinal, row.num, row.id.clone(), row.step.clone()))
            .collect::<Vec<_>>(),
        expected,
        "one row per envelope, numbered in delivery order across the reconnect and the restart"
    );
    // The child table's rows carry exactly their envelope's ordinal.
    let transactions = event_rows(&live_root, "transactions");
    assert_eq!(transactions.len(), HISTORY.len());
    for (tx, block) in transactions.iter().zip(&blocks) {
        assert_eq!(
            (tx.ordinal, tx.num, &tx.id, &tx.step),
            (block.ordinal, block.num, &block.id, &block.step)
        );
        assert_eq!(
            tx.tx_hash.as_deref(),
            Some(tx_hash(HISTORY[tx.ordinal as usize - 1].id).as_str())
        );
    }

    // The documented rule, evaluated directly: the latest event per height.
    let mut latest: BTreeMap<u64, &EventRow> = BTreeMap::new();
    for row in &blocks {
        let entry = latest.entry(row.num).or_insert(row);
        if row.ordinal > entry.ordinal {
            *entry = row;
        }
    }
    let head: Vec<(u64, String, u64)> = latest
        .values()
        .filter(|row| matches!(row.step.as_str(), "NEW" | "FINAL"))
        .map(|row| (row.num, row.id.clone(), row.ordinal))
        .collect();
    assert_eq!(head, [(100, block_id(0xbb), 3), (101, block_id(0xcc), 6)]);
    let live_transactions: Vec<_> = transactions
        .iter()
        .filter(|tx| head.contains(&(tx.num, tx.id.clone(), tx.ordinal)))
        .map(|tx| (tx.num, tx.tx_hash.clone().unwrap()))
        .collect();
    assert_eq!(
        live_transactions,
        [(100, tx_hash(0xbb)), (101, tx_hash(0xcc))]
    );

    // A final-only dataset of the same chain up to 100: the final frontier.
    let finalized = dir.path().join("final");
    let final_server = ReplayServer::start(
        &[envelope(100, 0xbb, FINAL, HOUR + 3_599)],
        vec![Plan::new("", true, 100)],
    )
    .await;
    succeed(build(&final_server, dir.path(), &finalized, true, 101, 1)).await;
    final_server.assert_drained();

    let Some(duckdb) = duckdb() else {
        return;
    };
    let sql = readme_sql();
    assert!(sql.len() >= 2, "README live view and union: {sql:?}");
    let live_prefix = live_root.to_str().unwrap();
    let final_prefix = finalized.clone();
    let views = format!("{};\n{}", sql[0], sql[1])
        .replace("live/mainnet", live_prefix)
        .replace("final/mainnet", final_prefix.to_str().unwrap());
    let check = |select: &str, expected: Vec<Value>| {
        let rows = duckdb_rows(&duckdb, dir.path(), &format!("{views};\n{select};"));
        let expected: Vec<Value> = expected.iter().map(scalars_as_text).collect();
        assert_eq!(rows, expected, "{select}");
    };
    check(
        "SELECT block_num, block_id, stream_ordinal FROM live_head ORDER BY block_num",
        vec![
            json!({"block_num": 100, "block_id": block_id(0xbb), "stream_ordinal": 3}),
            json!({"block_num": 101, "block_id": block_id(0xcc), "stream_ordinal": 6}),
        ],
    );
    check(
        "SELECT block_num, block_id, fork_step, stream_ordinal FROM live_blocks \
         ORDER BY block_num",
        vec![
            json!({"block_num": 100, "block_id": block_id(0xbb), "fork_step": "NEW", "stream_ordinal": 3}),
            json!({"block_num": 101, "block_id": block_id(0xcc), "fork_step": "NEW", "stream_ordinal": 6}),
        ],
    );
    // C was delivered as NEW twice; only the rows of the later NEW remain.
    check(
        "SELECT block_num, hash, stream_ordinal FROM live_transactions ORDER BY block_num",
        vec![
            json!({"block_num": 100, "hash": tx_hash(0xbb), "stream_ordinal": 3}),
            json!({"block_num": 101, "hash": tx_hash(0xcc), "stream_ordinal": 6}),
        ],
    );
    // Two-bucket union: the final dataset up to its frontier (100, whose FINAL
    // row reports lib_num 100), the live view above it, in the final schema.
    check(
        "SELECT block_num, block_id, lib_num FROM canonical_blocks ORDER BY block_num",
        vec![
            json!({"block_num": 100, "block_id": block_id(0xbb), "lib_num": 100}),
            json!({"block_num": 101, "block_id": block_id(0xcc), "lib_num": 99}),
        ],
    );
    check(
        "SELECT block_num, hash, lib_num FROM canonical_transactions ORDER BY block_num",
        vec![
            json!({"block_num": 100, "hash": tx_hash(0xbb), "lib_num": 100}),
            json!({"block_num": 101, "hash": tx_hash(0xcc), "lib_num": 99}),
        ],
    );
    check(
        "SELECT count(*) AS n FROM (DESCRIBE canonical_transactions) \
         WHERE column_name IN ('fork_step', 'stream_ordinal')",
        vec![json!({"n": 0})],
    );
}

/// A live bucket's lifecycle rule deletes committed parts older than its
/// window, outside fireparq's ownership. A running build (its next flushes and
/// completion), `recovery status`, `recovery recover` and a restarted build
/// (resume from authority, the next flushes) never read those parts, so they
/// are unaffected when every part of an earlier hour disappears. Control state
/// under `.fireparq-ingest/` and the cursor mirror are left in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_committed_parts_do_not_affect_a_running_or_restarted_live_build() {
    let history: Vec<Envelope> = (0..8)
        .map(|offset| {
            let time = if offset < 4 {
                HOUR + 3_596 + offset as i64
            } else {
                HOUR + 3_600 + offset as i64
            };
            envelope(100 + offset, 0xa0 + offset as u8, NEW, time)
        })
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("live");
    let root = output.clone();
    let server = ReplayServer::start(
        &history,
        vec![
            Plan {
                pause_after: Some(4),
                ..Plan::new("", false, 105)
            },
            Plan::new("event-5", false, 107),
        ],
    )
    .await;
    let running = build(&server, dir.path(), &output, false, 106, 1)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    // Wait until the four blocks of the first hour are committed and mirrored.
    let cursor = root.join("_fireparq/cursor.parquet");
    let committed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(Some(state)) = load_cursor_parquet(&cursor) {
                if state.last_block_num == 103 {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(committed.is_ok(), "first hour was never committed");
    let hour_dirs: Vec<PathBuf> = ["blocks", "transactions"]
        .iter()
        .map(|table| {
            let files = table_files(&root, table);
            assert_eq!(files.len(), 4, "{table}: {files:?}");
            let hour = files[0].parent().unwrap().to_path_buf();
            assert!(hour.ends_with("hour=22"), "{hour:?}");
            assert!(files.iter().all(|file| file.parent().unwrap() == hour));
            hour
        })
        .collect();
    // The lifecycle rule expires the whole first hour of every table.
    for hour in &hour_dirs {
        std::fs::remove_dir_all(hour).unwrap();
    }
    server.gate.add_permits(1);
    let output_log = running.wait_with_output();
    let result = tokio::time::timeout(Duration::from_secs(60), output_log)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.status.success(),
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(hour_dirs.iter().all(|hour| !hour.exists()));
    let ordinals = |table| {
        event_rows(&root, table)
            .into_iter()
            .map(|row| row.ordinal)
            .collect::<Vec<_>>()
    };
    assert_eq!(ordinals("blocks"), [5, 6]);
    assert_eq!(ordinals("transactions"), [5, 6]);

    // Read-only status and offline recovery see clean, unaffected authority.
    let root_arg = root.to_str().unwrap();
    let (success, log, stdout) =
        finish(fireparq(dir.path()).args(["recovery", "status", root_arg])).await;
    assert!(success, "{log}");
    let status: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(status["state"]["present"], json!(true), "{status}");
    assert_eq!(status["pending"]["present"], json!(false), "{status}");
    let (success, log, _) =
        finish(fireparq(dir.path()).args(["recovery", "recover", root_arg])).await;
    assert!(success, "{log}");

    // Restarted: resume from authority and keep numbering after the expiry.
    succeed(build(&server, dir.path(), &output, false, 108, 1)).await;
    assert_eq!(server.cursors(), ["", "event-5"]);
    server.assert_drained();
    assert_eq!(ordinals("blocks"), [5, 6, 7, 8]);
    assert_eq!(ordinals("transactions"), [5, 6, 7, 8]);
    assert!(hour_dirs.iter().all(|hour| !hour.exists()));
    assert_eq!(
        load_cursor_parquet(&cursor)
            .unwrap()
            .unwrap()
            .last_block_num,
        107
    );
}
