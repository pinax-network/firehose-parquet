//! Protected ingestion through the actual CLI and a cursor-aware local Firehose.
//! These tests never seed authority: every protected checkpoint is made by a CLI run.
use arrow::array::{StringArray, TimestampSecondArray, UInt64Array};
use arrow::datatypes::{DataType, TimeUnit};
use firehose_parquet::{
    cursor::{load_cursor_parquet, save_cursor_parquet, CursorState},
    durable_state::{ControlKey, CONTROL_DIRECTORY},
    writer::read_parquet,
};
use firehose_protos::{eth, firehose};
use futures::StreamExt;
use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::codegen::{http, BoxFuture, Service};

const CHAIN: &str = "ingestion-test";

#[derive(Clone)]
struct Info {
    first: u64,
}
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        let first = self.first;
        Box::pin(async move {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: CHAIN.into(),
                first_streamable_block_num: first,
                ..Default::default()
            }))
        })
    }
}

struct Plan {
    cursor: &'static str,
    origin: i64,
    stop: u64,
    // A bounded delivery limit models a disconnect/paused source, not a cursor.
    limit: Option<usize>,
    keep_open: bool,
}
impl Plan {
    fn complete(cursor: &'static str, origin: i64, stop: u64) -> Self {
        Self {
            cursor,
            origin,
            stop,
            limit: None,
            keep_open: false,
        }
    }
}

#[derive(Clone)]
struct Stream {
    events: Arc<Vec<firehose::Response>>,
    plans: Arc<Mutex<VecDeque<Plan>>>,
    requests: Arc<Mutex<Vec<firehose::Request>>>,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        self.requests.lock().unwrap().push(request.clone());
        let planned = self.plans.lock().unwrap().pop_front();
        let Some(plan) = planned else {
            return Box::pin(async {
                Err(tonic::Status::invalid_argument("unexpected Blocks RPC"))
            });
        };
        assert_eq!(request.cursor, plan.cursor);
        assert_eq!(request.start_block_num, plan.origin);
        assert_eq!(request.stop_block_num, plan.stop);
        assert!(request.final_blocks_only);
        // Resolve the actual opaque cursor into a source position. A bad resume
        // cannot accidentally pass just because the fixture starts after it.
        let from = if request.cursor.is_empty() {
            self.events
                .iter()
                .position(|event| {
                    event.metadata.as_ref().unwrap().num >= request.start_block_num as u64
                })
                .unwrap_or(self.events.len())
        } else {
            self.events
                .iter()
                .position(|event| event.cursor == request.cursor)
                .expect("unknown fixture cursor")
                + 1
        };
        let replies: Vec<_> = self.events[from..]
            .iter()
            .take_while(|event| event.metadata.as_ref().unwrap().num <= request.stop_block_num)
            .take(plan.limit.unwrap_or(usize::MAX))
            .cloned()
            .map(Ok)
            .collect();
        Box::pin(async move {
            let stream: Self::ResponseStream = if plan.keep_open {
                Box::pin(futures::stream::iter(replies).chain(futures::stream::pending()))
            } else {
                Box::pin(futures::stream::iter(replies))
            };
            Ok(tonic::Response::new(stream))
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

struct MockFirehose {
    endpoint: String,
    requests: Arc<Mutex<Vec<firehose::Request>>>,
    plans: Arc<Mutex<VecDeque<Plan>>>,
    task: tokio::task::JoinHandle<()>,
}
impl MockFirehose {
    async fn start(events: Vec<firehose::Response>, plans: Vec<Plan>) -> Self {
        let first = events
            .first()
            .and_then(|e| e.metadata.as_ref())
            .map_or(0, |m| m.num);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let requests = Arc::new(Mutex::new(Vec::new()));
        let plans = Arc::new(Mutex::new(VecDeque::from(plans)));
        let stream = Stream {
            events: Arc::new(events),
            plans: plans.clone(),
            requests: requests.clone(),
        };
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(Info { first })
                .add_service(stream)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        Self {
            endpoint,
            requests,
            plans,
            task,
        }
    }
    fn calls(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
    fn assert_drained(&self) {
        assert!(self.plans.lock().unwrap().is_empty());
    }
}
impl Drop for MockFirehose {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn response(number: u64, step: i32) -> firehose::Response {
    firehose::Response {
        block: Some(prost_types::Any {
            type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".into(),
            value: eth::Block {
                number,
                ..Default::default()
            }
            .encode_to_vec(),
        }),
        step,
        cursor: format!("fixture-{number}"),
        metadata: Some(firehose::BlockMetadata {
            num: number,
            id: format!("{number:064x}"),
            parent_num: number.saturating_sub(1),
            parent_id: format!("{:064x}", number.saturating_sub(1)),
            lib_num: number,
            time: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Default::default()
        }),
    }
}

fn command(server: &MockFirehose, dir: &Path, origin: u64, stop: u64) -> tokio::process::Command {
    command_with_flush(server, dir, origin, stop, 1_000_000)
}
fn command_with_flush(
    server: &MockFirehose,
    dir: &Path,
    origin: u64,
    stop: u64,
    flush_blocks: u64,
) -> tokio::process::Command {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    child
        .kill_on_drop(true)
        .env_clear()
        .current_dir(dir)
        .args([
            "build",
            "--endpoint",
            &server.endpoint,
            "--block-type",
            "evm",
            "--start-block",
            &origin.to_string(),
            "--stop-block",
            &stop.to_string(),
            "--partition",
            "none",
            "--flush-blocks",
            &flush_blocks.to_string(),
            "--flush-bytes",
            "1000000000",
            "--flush-interval-secs",
            "1000000000",
            "--stream-idle-timeout-secs",
            "0",
            "--output",
        ])
        .arg(dir.join("output"));
    child
}

fn logs(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
async fn run(mut command: tokio::process::Command) -> Output {
    tokio::time::timeout(Duration::from_secs(15), command.output())
        .await
        .expect("CLI exceeded bounded test deadline")
        .unwrap()
}
async fn success(command: tokio::process::Command) -> Output {
    let output = run(command).await;
    assert!(output.status.success(), "{}", logs(&output));
    output
}

async fn inspect_footer(path: &Path) -> String {
    // Read actual Parquet footer keys through the public CLI. Arrow's batch
    // schema metadata does not expose these separate transaction receipts.
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    command
        .kill_on_drop(true)
        .env_clear()
        .current_dir(path.parent().unwrap())
        .arg("inspect")
        .arg(path);
    let output = success(command).await;
    String::from_utf8(output.stdout).unwrap()
}
fn footer_value<'a>(footer: &'a str, key: &str) -> &'a str {
    footer
        .lines()
        .find_map(|line| line.trim_start().strip_prefix(key).map(str::trim))
        .unwrap_or_else(|| panic!("footer lacks {key}"))
}
fn root(dir: &Path) -> PathBuf {
    dir.join("output").join(CHAIN)
}
fn authority(root: &Path) -> Value {
    let bytes = std::fs::read(
        root.join(CONTROL_DIRECTORY)
            .join(ControlKey::State.filename()),
    )
    .unwrap();
    let record: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(record["deleted"], false);
    record["payload"].clone()
}
fn parts(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut result = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet")
                && path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("part-")
            {
                result.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    Sha256::digest(std::fs::read(&path).unwrap()).to_vec(),
                );
            }
        }
    }
    result
}
fn block_numbers(root: &Path) -> Vec<u64> {
    let mut numbers = Vec::new();
    for path in parts(root).keys().filter(|path| path.starts_with("blocks")) {
        for batch in read_parquet(&root.join(path)).unwrap() {
            let column = batch
                .column_by_name("block_num")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            numbers.extend(column.values().iter().copied());
        }
    }
    numbers.sort_unstable();
    numbers
}
fn assert_checkpoint(root: &Path, ordinal: u64, number: u64, stop: u64) {
    let state = authority(root);
    assert_eq!(state["checkpoint"]["ordinal"], ordinal);
    assert_eq!(state["checkpoint"]["event"]["block_num"], number);
    assert_eq!(
        state["checkpoint"]["event"]["cursor"],
        format!("fixture-{number}")
    );
    assert_eq!(state["checkpoint"]["completed_stop"], stop);
    let mirror = load_cursor_parquet(&root.join("cursor.parquet"))
        .unwrap()
        .unwrap();
    assert_eq!(mirror.last_block_num, number);
    assert_eq!(mirror.cursor, format!("fixture-{number}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_completed_range_is_noop_and_deleted_mirror_is_repaired_before_extension() {
    let server = MockFirehose::start(
        (100..103).map(|n| response(n, 3)).collect(),
        vec![
            Plan::complete("", 100, 101),
            Plan::complete("fixture-101", 100, 102),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let root = root(dir.path());
    success(command(&server, dir.path(), 100, 102)).await;
    assert_eq!(block_numbers(&root), [100, 101]);
    assert_checkpoint(&root, 2, 101, 102);
    let initial_parts = parts(&root);
    assert_eq!(initial_parts.len(), 1);
    let initial_state = authority(&root);

    let repeated = success(command(&server, dir.path(), 100, 102)).await;
    assert!(logs(&repeated).contains("without opening Blocks"));
    assert_eq!(server.calls(), 1);
    assert_eq!(parts(&root), initial_parts);
    assert_eq!(authority(&root), initial_state);

    std::fs::remove_file(root.join("cursor.parquet")).unwrap();
    success(command(&server, dir.path(), 100, 102)).await;
    assert_eq!(server.calls(), 1);
    assert_checkpoint(&root, 2, 101, 102);
    assert_eq!(parts(&root), initial_parts);
    assert_eq!(authority(&root), initial_state);

    // The extension must also work with no compatibility mirror; only authority
    // can select fixture-101. The fake serves source events after that cursor.
    std::fs::remove_file(root.join("cursor.parquet")).unwrap();
    success(command(&server, dir.path(), 100, 103)).await;
    assert_eq!(server.calls(), 2);
    assert_checkpoint(&root, 3, 102, 103);
    assert_eq!(block_numbers(&root), [100, 101, 102]);
    let extended_parts = parts(&root);
    assert_eq!(extended_parts.len(), 2);
    for (path, digest) in initial_parts {
        assert_eq!(extended_parts.get(&path), Some(&digest));
    }
    server.assert_drained();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protected_origin_mode_and_override_changes_are_refused_before_blocks() {
    let server =
        MockFirehose::start(vec![response(100, 3)], vec![Plan::complete("", 100, 100)]).await;
    let dir = tempfile::tempdir().unwrap();
    success(command(&server, dir.path(), 100, 101)).await;
    let root = root(dir.path());
    let before = (authority(&root), parts(&root));
    for (origin, extra, expected_error) in [
        (99, None, "explicit start differs"),
        (101, None, "explicit start differs"),
        (
            100,
            Some("--cursor-override"),
            "cannot rewind protected output",
        ),
        (
            100,
            Some("--final-blocks-only=false"),
            "authoritative stream differs",
        ),
    ] {
        let mut request = command(&server, dir.path(), origin, 103);
        if let Some(extra) = extra {
            request.arg(extra);
        }
        let output = run(request).await;
        assert!(!output.status.success(), "{}", logs(&output));
        assert!(logs(&output).contains(expected_error), "{}", logs(&output));
        assert_eq!(server.calls(), 1);
        assert_eq!((authority(&root), parts(&root)), before);
    }
    server.assert_drained();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_data_and_cursors_cannot_initialize_authority_even_with_override() {
    let server = MockFirehose::start(vec![response(100, 3)], vec![]).await;
    for cursor_only in [false, true] {
        for override_cursor in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = root(dir.path());
            std::fs::create_dir_all(&root).unwrap();
            let existing = if cursor_only {
                let path = root.join("cursor.parquet");
                save_cursor_parquet(
                    &path,
                    &CursorState {
                        cursor: "fixture-99".into(),
                        last_block_num: 99,
                        start_block: Some(100),
                        ..Default::default()
                    },
                )
                .unwrap();
                path
            } else {
                std::fs::create_dir_all(root.join("blocks")).unwrap();
                let path = root.join("blocks").join("part-legacy.parquet");
                // Eligibility must reject existing files without parsing/adopting them.
                std::fs::write(&path, b"legacy-data").unwrap();
                path
            };
            let before = std::fs::read(&existing).unwrap();
            let mut request = command(&server, dir.path(), 100, 101);
            if override_cursor {
                request.arg("--cursor-override");
            }
            let output = run(request).await;
            assert!(!output.status.success(), "{}", logs(&output));
            assert!(logs(&output).contains("legacy"), "{}", logs(&output));
            assert_eq!(server.calls(), 0);
            assert_eq!(std::fs::read(existing).unwrap(), before);
            assert!(!root
                .join(CONTROL_DIRECTORY)
                .join(ControlKey::State.filename())
                .exists());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filtered_undo_is_a_zero_row_accepted_completion_and_repeat_is_noop() {
    let server =
        MockFirehose::start(vec![response(100, 2)], vec![Plan::complete("", 100, 100)]).await;
    let dir = tempfile::tempdir().unwrap();
    success(command(&server, dir.path(), 100, 101)).await;
    let root = root(dir.path());
    assert_checkpoint(&root, 1, 100, 101);
    assert_eq!(authority(&root)["checkpoint"]["event"]["fork_step"], 2);
    assert!(parts(&root).is_empty());
    success(command(&server, dir.path(), 100, 101)).await;
    assert_eq!(server.calls(), 1);
    assert!(parts(&root).is_empty());
    server.assert_drained();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eof_without_boundary_retains_prefix_but_cannot_claim_completed_range() {
    let server = MockFirehose::start(
        vec![response(100, 3)],
        vec![
            Plan::complete("", 100, 101),
            Plan::complete("fixture-100", 100, 101),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let output = run(command(&server, dir.path(), 100, 102)).await;
    assert!(!output.status.success(), "{}", logs(&output));
    assert!(
        logs(&output).contains("does not prove requested stop"),
        "{}",
        logs(&output)
    );
    let root = root(dir.path());
    let state = authority(&root);
    assert_eq!(state["checkpoint"]["ordinal"], 1);
    assert_eq!(state["checkpoint"]["event"]["cursor"], "fixture-100");
    assert!(state["checkpoint"]["completed_stop"].is_null());
    assert_eq!(block_numbers(&root), [100]);
    assert_eq!(server.calls(), 2);
    server.assert_drained();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genesis_bootstrap_keeps_zero_height_filtered_ordinals_and_lookahead_provenance() {
    for malformed_anchor in [false, true] {
        let mut genesis = response(0, 1);
        genesis
            .metadata
            .as_mut()
            .unwrap()
            .time
            .as_mut()
            .unwrap()
            .seconds = 0;
        let mut undo = genesis.clone();
        undo.step = 2;
        undo.cursor = "fixture-0-undo".into();
        let mut anchor = response(1, 1);
        if malformed_anchor {
            // Its validated metadata supplies a real future timestamp. Decoding
            // the anchor's payload fails only after the buffered genesis flush,
            // letting this CLI test inspect persisted lookahead provenance.
            anchor.block.as_mut().unwrap().value = vec![0xff];
        }
        let server =
            MockFirehose::start(vec![genesis, undo, anchor], vec![Plan::complete("", 0, 1)]).await;
        let dir = tempfile::tempdir().unwrap();
        let output = run(command_with_flush(&server, dir.path(), 0, 2, 1)).await;
        assert_eq!(
            output.status.success(),
            !malformed_anchor,
            "{}",
            logs(&output)
        );
        let root = root(dir.path());
        let state = authority(&root);
        assert_eq!(state["descriptor"]["origin_start"], 0);
        let expected: &[u64] = if malformed_anchor { &[0] } else { &[0, 1] };
        assert_eq!(block_numbers(&root), expected);
        assert_eq!(parts(&root).len(), expected.len());
        for path in parts(&root).keys() {
            let footer = inspect_footer(&root.join(path)).await;
            for batch in read_parquet(&root.join(path)).unwrap() {
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
                let timestamps = arrow::compute::cast(
                    batch.column_by_name("timestamp").unwrap(),
                    &DataType::Timestamp(TimeUnit::Second, None),
                )
                .unwrap();
                let timestamps = timestamps
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    assert_eq!(ids.value(row), format!("0x{:064x}", numbers.value(row)));
                    // This is the existing bootstrap output timestamp, while
                    // authority below keeps the original absent source time.
                    assert_eq!(timestamps.value(row), 1_700_000_000);
                }
                let (first, last) = if numbers.value(0) == 0 {
                    ("1", "2")
                } else {
                    ("3", "3")
                };
                assert_eq!(
                    footer_value(&footer, "fireparq.ingest.first_ordinal"),
                    first
                );
                assert_eq!(footer_value(&footer, "fireparq.ingest.last_ordinal"), last);
            }
        }
        if malformed_anchor {
            let checkpoint = &state["checkpoint"];
            assert_eq!(checkpoint["ordinal"], 2);
            assert!(checkpoint["completed_stop"].is_null());
            assert_eq!(checkpoint["event"]["cursor"], "fixture-0-undo");
            assert_eq!(checkpoint["event"]["block_num"], 0);
            assert_eq!(checkpoint["event"]["fork_step"], 2);
            assert!(checkpoint["event"]["source_timestamp"].is_null());
            let anchor = &checkpoint["routing"]["anchor"];
            assert_eq!(anchor["provenance"], "lookahead");
            assert_eq!(anchor["source_ordinal"], 3);
            assert_eq!(anchor["source_block_num"], 1);
            assert_eq!(anchor["source_block_id"], format!("{:064x}", 1));
            assert_eq!(anchor["seconds"], 1_700_000_000);
            let mirror = load_cursor_parquet(&root.join("cursor.parquet"))
                .unwrap()
                .unwrap();
            assert_eq!(mirror.last_block_num, 0);
            assert_eq!(mirror.cursor, "fixture-0-undo");
        } else {
            assert_checkpoint(&root, 3, 1, 2);
            assert_eq!(
                state["checkpoint"]["event"]["source_timestamp"],
                1_700_000_000
            );
            assert!(state["checkpoint"]["routing"]["anchor"].is_null());
        }
        assert_eq!(server.calls(), 1);
        server.assert_drained();
    }
}

async fn wait_for_buffer(port: u16) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(mut stream) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
                stream
                    .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .await
                    .unwrap();
                let mut response = String::new();
                stream.read_to_string(&mut response).await.unwrap();
                // The processed counter increments after accept_mapped, whereas
                // the mapper row gauge alone can be seen just before acceptance.
                if response.contains("firehose_parquet_mapper_buffer_rows 1\n")
                    && response.contains("firehose_parquet_blocks_processed_total 1\n")
                    && response.contains("firehose_parquet_cursor_last_block_num 99\n")
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("CLI did not accept the unflushed event");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abrupt_restart_replays_only_accepted_unflushed_events_without_duplicate_parts() {
    let server = MockFirehose::start(
        (99..102).map(|n| response(n, 3)).collect(),
        vec![
            Plan::complete("", 99, 99),
            Plan {
                cursor: "fixture-99",
                origin: 99,
                stop: 101,
                limit: Some(1),
                keep_open: true,
            },
            Plan::complete("fixture-99", 99, 101),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    success(command(&server, dir.path(), 99, 100)).await;
    let root = root(dir.path());
    let before = (authority(&root), parts(&root));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut child = command(&server, dir.path(), 99, 102)
        .args(["--metrics-port", &port.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_buffer(port).await;
    assert_eq!((authority(&root), parts(&root)), before);
    // Child::start_kill sends SIGKILL on Unix: normal drain/shutdown cannot run.
    child.start_kill().unwrap();
    let killed = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(!killed.status.success());
    assert_eq!((authority(&root), parts(&root)), before);

    success(command(&server, dir.path(), 99, 102)).await;
    assert_checkpoint(&root, 3, 101, 102);
    assert_eq!(block_numbers(&root), [99, 100, 101]);
    let recovered = parts(&root);
    assert_eq!(recovered.len(), 2);
    for (path, digest) in before.1 {
        assert_eq!(recovered.get(&path), Some(&digest));
    }
    assert_eq!(server.calls(), 3);
    server.assert_drained();
}
