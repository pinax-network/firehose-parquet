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
    command_with_limits(
        server,
        dir,
        origin,
        stop,
        flush_blocks,
        1_000_000_000,
        268_435_456,
    )
}
fn command_with_limits(
    server: &MockFirehose,
    dir: &Path,
    origin: u64,
    stop: u64,
    flush_blocks: u64,
    flush_bytes: u64,
    flush_memory_bytes: u64,
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
            &flush_bytes.to_string(),
            "--flush-memory-bytes",
            &flush_memory_bytes.to_string(),
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
            // The override is refused before eligibility is even inspected.
            let expected = if override_cursor {
                "--cursor-override is only valid with --dry-run"
            } else {
                "legacy"
            };
            assert!(logs(&output).contains(expected), "{}", logs(&output));
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
async fn cursor_override_is_refused_at_a_new_root_but_still_serves_dry_runs() {
    let server =
        MockFirehose::start(vec![response(100, 3)], vec![Plan::complete("", 100, 100)]).await;
    let dir = tempfile::tempdir().unwrap();
    let mut request = command(&server, dir.path(), 100, 101);
    request.arg("--cursor-override");
    let output = run(request).await;
    assert!(!output.status.success(), "{}", logs(&output));
    assert!(
        logs(&output).contains("--cursor-override is only valid with --dry-run"),
        "{}",
        logs(&output)
    );
    // Refused before endpoint startup: no Blocks request and no output root.
    assert_eq!(server.calls(), 0);
    assert!(!dir.path().join("output").exists());

    // The documented read-only use is unchanged.
    let mut request = command(&server, dir.path(), 100, 101);
    request.args(["--cursor-override", "--dry-run"]);
    success(request).await;
    assert_eq!(server.calls(), 1);
    let output_dir = dir.path().join("output");
    assert!(!output_dir.exists() || parts(&output_dir).is_empty());
    assert!(!root(dir.path()).join(CONTROL_DIRECTORY).exists());
    server.assert_drained();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_none_keeps_mandatory_authority_without_a_mirror_and_binds_that_choice() {
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
    let without_mirror = |origin, stop| {
        let mut request = command(&server, dir.path(), origin, stop);
        request.args(["--cursor", "NONE"]);
        request
    };
    success(without_mirror(100, 102)).await;
    assert_eq!(block_numbers(&root), [100, 101]);
    let state = authority(&root);
    assert_eq!(state["descriptor"]["mirror"]["kind"], "disabled");
    assert_eq!(state["checkpoint"]["ordinal"], 2);
    assert_eq!(state["checkpoint"]["event"]["cursor"], "fixture-101");
    assert_eq!(state["checkpoint"]["completed_stop"], 102);
    assert!(!root.join("cursor.parquet").exists());

    // Same-bound completion is still an authority-backed no-op.
    let repeated = success(without_mirror(100, 102)).await;
    assert!(logs(&repeated).contains("without opening Blocks"));
    assert_eq!(server.calls(), 1);

    // The disabled mirror is part of the stream identity: omitting --cursor
    // none would add a mirror, which is refused before Blocks and before any
    // cursor file is created.
    let before = (authority(&root), parts(&root));
    let output = run(command(&server, dir.path(), 100, 103)).await;
    assert!(!output.status.success(), "{}", logs(&output));
    assert!(
        logs(&output).contains("created without a cursor mirror; rerun with --cursor none"),
        "{}",
        logs(&output)
    );
    assert_eq!(server.calls(), 1);
    assert_eq!((authority(&root), parts(&root)), before);
    assert!(!root.join("cursor.parquet").exists());

    // Extension resumes from the authoritative cursor alone.
    success(without_mirror(100, 103)).await;
    assert_eq!(server.calls(), 2);
    assert_eq!(block_numbers(&root), [100, 101, 102]);
    let state = authority(&root);
    assert_eq!(state["checkpoint"]["ordinal"], 3);
    assert_eq!(state["checkpoint"]["event"]["cursor"], "fixture-102");
    assert_eq!(state["checkpoint"]["completed_stop"], 103);
    assert!(!root.join("cursor.parquet").exists());
    server.assert_drained();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_none_cannot_drop_an_existing_bound_mirror() {
    let server =
        MockFirehose::start(vec![response(100, 3)], vec![Plan::complete("", 100, 100)]).await;
    let dir = tempfile::tempdir().unwrap();
    let root = root(dir.path());
    success(command(&server, dir.path(), 100, 101)).await;
    assert!(root.join("cursor.parquet").exists());
    let before = (authority(&root), parts(&root));
    let mut request = command(&server, dir.path(), 100, 102);
    request.args(["--cursor", "none"]);
    let output = run(request).await;
    assert!(!output.status.success(), "{}", logs(&output));
    assert!(
        logs(&output).contains("--cursor none cannot disable it"),
        "{}",
        logs(&output)
    );
    assert_eq!(server.calls(), 1);
    assert_eq!((authority(&root), parts(&root)), before);
    server.assert_drained();
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

fn response_with_sizing_payload(number: u64, two_tables: bool) -> firehose::Response {
    let mut event = response(number, firehose::ForkStep::StepNew as i32);
    let block = eth::Block {
        number,
        header: Some(eth::BlockHeader {
            extra_data: vec![0x71; 16_384].into(),
            ..Default::default()
        }),
        transaction_traces: if two_tables {
            vec![eth::TransactionTrace {
                input: vec![0x82; 16_384].into(),
                ..Default::default()
            }]
        } else {
            vec![]
        },
        ..Default::default()
    };
    event.block.as_mut().unwrap().value = block.encode_to_vec().into();
    event
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compressed_receipts_train_later_cli_flush_windows() {
    let events = (100..130)
        .map(|height| response_with_sizing_payload(height, false))
        .collect();
    let server = MockFirehose::start(events, vec![Plan::complete("", 100, 129)]).await;
    let dir = tempfile::tempdir().unwrap();
    let output = success(command_with_limits(
        &server,
        dir.path(),
        100,
        130,
        1_000_000,
        65_536,
        2_097_152,
    ))
    .await;
    let root = root(dir.path());
    let mut windows: Vec<_> = parts(&root)
        .keys()
        .filter(|path| path.starts_with("blocks"))
        .map(|path| {
            let batches = read_parquet(&root.join(path)).unwrap();
            let first = batches[0]
                .column_by_name("block_num")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0);
            (
                first,
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            )
        })
        .collect();
    windows.sort_unstable();
    assert!(windows.len() >= 2, "{windows:?}");
    assert!(
        windows[1].1 > windows[0].1,
        "successful receipt must expand the next window: {windows:?}"
    );
    assert_eq!(windows.iter().map(|(_, rows)| rows).sum::<usize>(), 30);
    assert_eq!(block_numbers(&root), (100..130).collect::<Vec<_>>());
    assert_checkpoint(&root, 30, 129, 130);
    assert!(logs(&output).contains("committed flush size observation"));
    server.assert_drained();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn summed_memory_flushes_cli_when_each_table_is_below_the_limit() {
    use firehose_parquet::{
        encode::EncodeBytes,
        traits::{BlockIdentity, BlockMapper},
    };
    let events: Vec<_> = (100..103)
        .map(|height| response_with_sizing_payload(height, true))
        .collect();
    let mut mapper = blocks::evm::mapper::EvmBlockMapper::new(false, false, EncodeBytes::Hex, true);
    mapper
        .map_block(
            &events[0].block.as_ref().unwrap().value,
            &BlockIdentity {
                block_num: 100,
                timestamp: 1_700_000_000,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    let sizes = mapper.table_estimates();
    assert!(sizes.iter().all(|(_, bytes)| *bytes < 49_152));
    assert!(sizes.iter().map(|(_, bytes)| bytes).sum::<usize>() > 49_152);
    let server = MockFirehose::start(events, vec![Plan::complete("", 100, 102)]).await;
    let dir = tempfile::tempdir().unwrap();
    let output = success(command_with_limits(
        &server,
        dir.path(),
        100,
        103,
        1_000_000,
        1_000_000_000,
        49_152,
    ))
    .await;
    let root = root(dir.path());
    assert_eq!(
        parts(&root)
            .keys()
            .filter(|path| path.starts_with("blocks"))
            .count(),
        3
    );
    assert_eq!(
        parts(&root)
            .keys()
            .filter(|path| path.starts_with("transactions"))
            .count(),
        3
    );
    assert_eq!(block_numbers(&root), vec![100, 101, 102]);
    assert_checkpoint(&root, 3, 102, 103);
    assert!(logs(&output).contains("memory"));
    server.assert_drained();
}

/// Opt-in cross-binary qualification: both runs start from the identical durable
/// prefix at the same canonical output path. Source IDs, transaction IDs, footer
/// metadata and file boundaries must therefore agree, including physical bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires FIREPARQ_BASELINE_BIN from the same dependency/schema base"]
async fn retained_evm_replay_matches_baseline_bytes_authority_and_mirror() {
    fn copy_tree(source: &Path, destination: &Path) {
        std::fs::create_dir_all(destination).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let file_type = entry.file_type().unwrap();
            assert!(
                !file_type.is_symlink(),
                "qualification roots must not contain aliases"
            );
            let target = destination.join(entry.file_name());
            if file_type.is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    fn baseline_command(
        binary: &Path,
        server: &MockFirehose,
        dir: &Path,
        origin: u64,
        stop: u64,
    ) -> tokio::process::Command {
        let candidate = command_with_flush(server, dir, origin, stop, 1);
        let mut command = tokio::process::Command::new(binary);
        command
            .kill_on_drop(true)
            .env_clear()
            .current_dir(dir)
            .args(candidate.as_std().get_args());
        command
    }

    let baseline = PathBuf::from(
        std::env::var_os("FIREPARQ_BASELINE_BIN")
            .expect("set FIREPARQ_BASELINE_BIN to a built, matching-main fireparq"),
    );
    assert!(baseline.is_absolute() && baseline.is_file());
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/evm-mainnet");
    let metadata: Value =
        serde_json::from_slice(&std::fs::read(fixture_dir.join("metadata.json")).unwrap()).unwrap();
    let bytes = std::fs::read(fixture_dir.join("block.pb")).unwrap();
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        metadata["sha256"].as_str().unwrap()
    );
    let identity = &metadata["identity"];
    let number = identity["block_num"].as_u64().unwrap();
    let origin = number - 1;
    let mut seed = response(origin, 3);
    let seed_metadata = seed.metadata.as_mut().unwrap();
    seed_metadata.id = identity["parent_id"].as_str().unwrap().into();
    seed_metadata.lib_num = identity["lib_num"].as_u64().unwrap();
    seed_metadata.time.as_mut().unwrap().seconds = identity["timestamp"].as_i64().unwrap() - 12;
    let retained = firehose::Response {
        block: Some(prost_types::Any {
            type_url: metadata["protobuf_type"].as_str().unwrap().into(),
            value: bytes,
        }),
        step: 3,
        cursor: format!("fixture-{number}"),
        metadata: Some(firehose::BlockMetadata {
            num: number,
            id: identity["block_id"].as_str().unwrap().into(),
            parent_num: identity["parent_num"].as_u64().unwrap(),
            parent_id: identity["parent_id"].as_str().unwrap().into(),
            lib_num: identity["lib_num"].as_u64().unwrap(),
            time: Some(prost_types::Timestamp {
                seconds: identity["timestamp"].as_i64().unwrap(),
                nanos: identity["timestamp_nanos"].as_i64().unwrap() as i32,
            }),
            ..Default::default()
        }),
    };
    // Fixed public fixture cursor; never capture or print a provider cursor.
    assert_eq!(origin, 26_049_574);
    let server = MockFirehose::start(
        vec![seed, retained],
        vec![
            Plan::complete("", origin as i64, origin),
            Plan::complete("fixture-26049574", origin as i64, number),
            Plan::complete("fixture-26049574", origin as i64, number),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let output = root(directory.path());
    success(baseline_command(
        &baseline,
        &server,
        directory.path(),
        origin,
        number,
    ))
    .await;
    let saved_prefix = directory.path().join("saved-prefix");
    copy_tree(&output, &saved_prefix);
    let prefix = authority(&output);

    success(baseline_command(
        &baseline,
        &server,
        directory.path(),
        origin,
        number + 1,
    ))
    .await;
    let expected_authority = authority(&output);
    let state_path = output
        .join(CONTROL_DIRECTORY)
        .join(ControlKey::State.filename());
    let expected_state_record = std::fs::read(&state_path).unwrap();
    assert!(!output
        .join(CONTROL_DIRECTORY)
        .join(ControlKey::Pending.filename())
        .exists());
    let expected_paths = parts(&output);
    let expected_bytes: BTreeMap<_, _> = expected_paths
        .keys()
        .map(|path| (path.clone(), std::fs::read(output.join(path)).unwrap()))
        .collect();
    let expected_batches: BTreeMap<_, _> = expected_paths
        .keys()
        .map(|path| (path.clone(), read_parquet(&output.join(path)).unwrap()))
        .collect();
    let expected_mirror = read_parquet(&output.join("cursor.parquet")).unwrap();

    // Every child has exited via output(). No process can retain the old inode
    // ownership when the disposable qualification root is restored in place.
    std::fs::remove_dir_all(&output).unwrap();
    copy_tree(&saved_prefix, &output);
    assert_eq!(authority(&output), prefix);
    success(command_with_flush(
        &server,
        directory.path(),
        origin,
        number + 1,
        1,
    ))
    .await;
    assert_eq!(
        parts(&output),
        expected_paths,
        "part paths and physical hashes"
    );
    let mut rows = 0;
    for (path, bytes) in expected_bytes {
        assert_eq!(
            std::fs::read(output.join(&path)).unwrap(),
            bytes,
            "part bytes: {path:?}"
        );
        let actual = read_parquet(&output.join(&path)).unwrap();
        assert_eq!(
            actual, expected_batches[&path],
            "complete schema and typed rows: {path:?}"
        );
        rows += actual.iter().map(|batch| batch.num_rows()).sum::<usize>();
    }
    assert_eq!(
        authority(&output),
        expected_authority,
        "all authoritative payload fields"
    );
    // The restored incarnation is the same, so record equality also checks the
    // number of authority revisions instead of hiding redundant checkpoint writes.
    assert_eq!(std::fs::read(&state_path).unwrap(), expected_state_record);
    assert!(!output
        .join(CONTROL_DIRECTORY)
        .join(ControlKey::Pending.filename())
        .exists());
    assert_checkpoint(&output, 2, number, number + 1);
    let actual_mirror = read_parquet(&output.join("cursor.parquet")).unwrap();
    assert_eq!(actual_mirror.len(), expected_mirror.len());
    for (actual, expected) in actual_mirror.iter().zip(&expected_mirror) {
        assert_eq!(actual.schema().fields(), expected.schema().fields());
        for (index, field) in actual.schema().fields().iter().enumerate() {
            if field.name() != "updated_at" {
                assert_eq!(
                    actual.column(index),
                    expected.column(index),
                    "mirror column {}",
                    field.name()
                );
            }
        }
    }
    let repeated = success(command_with_flush(
        &server,
        directory.path(),
        origin,
        number + 1,
        1,
    ))
    .await;
    assert!(logs(&repeated).contains("without opening Blocks"));
    assert_eq!(server.calls(), 3);
    assert_eq!(authority(&output), expected_authority);
    server.assert_drained();
    assert_eq!(
        rows, 5_050,
        "one seed row plus the retained 5,049-row block"
    );
    eprintln!("exact baseline parity: {} parts, {rows} rows; complete authority and stable mirror columns", expected_paths.len());
}
