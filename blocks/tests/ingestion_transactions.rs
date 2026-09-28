//! Protected ingestion through the actual CLI and a cursor-aware local Firehose.
//! These tests never seed authority: every protected checkpoint is made by a CLI run.
use arrow::array::{Int64Array, StringArray, TimestampSecondArray};
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
/// The default `--cursor` mirror, relative to the dataset root.
const MIRROR: &str = "_fireparq/cursor.parquet";

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
    // A start above LIB is served from LIB+1: deliver earlier blocks than asked.
    serve_from: Option<u64>,
}
impl Plan {
    fn complete(cursor: &'static str, origin: i64, stop: u64) -> Self {
        Self {
            cursor,
            origin,
            stop,
            limit: None,
            keep_open: false,
            serve_from: None,
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
            let first = plan.serve_from.unwrap_or(request.start_block_num as u64);
            self.events
                .iter()
                .position(|event| event.metadata.as_ref().unwrap().num >= first)
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
/// Logs without terminal styling, for matching structured `key=value` fields.
fn plain_logs(output: &Output) -> String {
    let logs = logs(output);
    let mut plain = String::with_capacity(logs.len());
    let mut chars = logs.chars();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            plain.push(ch);
        }
    }
    plain
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
/// The dataset root of [`command`]: `--output` itself.
fn root(dir: &Path) -> PathBuf {
    dir.join("output")
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
    table_block_numbers(root, "blocks")
}
fn table_block_numbers(root: &Path, table: &str) -> Vec<u64> {
    let mut numbers = Vec::new();
    for path in parts(root).keys().filter(|path| path.starts_with(table)) {
        for batch in read_parquet(&root.join(path)).unwrap() {
            let column = batch
                .column_by_name("block_num")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            numbers.extend(column.values().iter().map(|n| u64::try_from(*n).unwrap()));
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
    let mirror = load_cursor_parquet(&root.join(MIRROR)).unwrap().unwrap();
    assert_eq!(mirror.last_block_num, number);
    assert_eq!(mirror.cursor, format!("fixture-{number}"));
}

/// Build with a relative `--output` (or none) from `cwd`, clearing the process
/// environment so only an env file can supply settings.
fn relative_output_build(
    server: &MockFirehose,
    cwd: &Path,
    output: Option<&str>,
    extra: &[&str],
) -> tokio::process::Command {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    child
        .kill_on_drop(true)
        .env_clear()
        .current_dir(cwd)
        .args(extra)
        .args([
            "build",
            "--endpoint",
            &server.endpoint,
            "--block-type",
            "evm",
            "--start-block",
            "100",
            "--stop-block",
            "102",
            "--stream-idle-timeout-secs",
            "0",
        ]);
    if let Some(output) = output {
        child.args(["--output", output]);
    }
    child
}

/// #617: a `.env` in a parent directory is never loaded, an inherited
/// `S3_BUCKET` never turns a relative output into an S3 write, and startup logs
/// name the env file (never values) and the absolute write destination.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parent_env_file_is_ignored_and_bucket_never_redirects_relative_output() {
    let server = MockFirehose::start(
        (100..102).map(|n| response(n, 3)).collect(),
        vec![Plan::complete("", 100, 101), Plan::complete("", 100, 101)],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    // A checkout holding production settings, with a worktree below it.
    let production_env = "S3_BUCKET=production-bucket\n\
        AWS_ACCESS_KEY_ID=parent-key-id\n\
        AWS_SECRET_ACCESS_KEY=parent-secret-value\n\
        AWS_ENDPOINT_URL_S3=http://127.0.0.1:9\n";
    std::fs::write(dir.path().join(".env"), production_env).unwrap();
    let worktree = dir.path().join("worktree");
    std::fs::create_dir(&worktree).unwrap();
    // The child's working directory is the canonical path (macOS /private/var).
    let worktree_abs = worktree.canonicalize().unwrap();

    // 1. The parent .env is ignored: the relative output stays local.
    let output = success(relative_output_build(
        &server,
        &worktree,
        Some("target/out"),
        &[],
    ))
    .await;
    let logs = plain_logs(&output);
    assert!(logs.contains("no env file loaded"), "{logs}");
    let local_root = worktree_abs.join("target/out");
    assert!(
        logs.contains(&format!("output={}", local_root.display())),
        "{logs}"
    );
    assert!(
        logs.contains(&format!("cursor={}", local_root.join(MIRROR).display())),
        "{logs}"
    );
    assert!(logs.contains("resolved write destinations"), "{logs}");
    assert_eq!(block_numbers(&local_root), [100, 101]);
    assert!(!logs.contains("production-bucket"), "{logs}");
    assert!(!logs.contains("parent-secret-value"), "{logs}");

    // 2. The same file in the current directory is loaded and named, with
    // variable names only, and the relative output is refused before any write.
    std::fs::write(worktree.join(".env"), production_env).unwrap();
    let output = run(relative_output_build(
        &server,
        &worktree,
        Some("target/refused"),
        &[],
    ))
    .await;
    assert!(!output.status.success());
    let logs = plain_logs(&output);
    assert!(logs.contains("loaded env file"), "{logs}");
    assert!(
        logs.contains(&worktree_abs.join(".env").display().to_string()),
        "{logs}"
    );
    assert!(logs.contains("S3_BUCKET"), "{logs}");
    assert!(logs.contains("AWS_SECRET_ACCESS_KEY"), "{logs}");
    assert!(!logs.contains("parent-secret-value"), "{logs}");
    assert!(!logs.contains("parent-key-id"), "{logs}");
    assert!(logs.contains("relative path"), "{logs}");
    assert!(
        logs.contains("s3://production-bucket/target/refused"),
        "{logs}"
    );
    assert!(!worktree.join("target/refused").exists());

    // 3. --env-file selects the only file to load, so ./.env is ignored.
    let explicit = dir.path().join("explicit.env");
    std::fs::write(&explicit, "OUTPUT=./target/explicit/{chain}\n").unwrap();
    let output = success(relative_output_build(
        &server,
        &worktree,
        None,
        &["--env-file", explicit.to_str().unwrap()],
    ))
    .await;
    let logs = plain_logs(&output);
    assert!(logs.contains("loaded env file"), "{logs}");
    assert!(
        logs.contains(&explicit.canonicalize().unwrap().display().to_string())
            || logs.contains(&explicit.display().to_string()),
        "{logs}"
    );
    assert!(logs.contains("supplied=OUTPUT"), "{logs}");
    assert!(!logs.contains("S3_BUCKET"), "{logs}");
    assert_eq!(
        block_numbers(&worktree_abs.join("target/explicit").join(CHAIN)),
        [100, 101]
    );
    server.assert_drained();
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

    std::fs::remove_file(root.join(MIRROR)).unwrap();
    success(command(&server, dir.path(), 100, 102)).await;
    assert_eq!(server.calls(), 1);
    assert_checkpoint(&root, 2, 101, 102);
    assert_eq!(parts(&root), initial_parts);
    assert_eq!(authority(&root), initial_state);

    // The extension must also work with no compatibility mirror; only authority
    // can select fixture-101. The fake serves source events after that cursor.
    std::fs::remove_file(root.join(MIRROR)).unwrap();
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

/// #652: `build` writes only `<table>/date=YYYY-MM-DD/`. A root holding
/// partitions of an older layout (v0.x `year=/month=/date=DD`, pre-release
/// `year=/month=/day=`, `block_range=`) is refused before any Blocks request,
/// and nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roots_with_older_partition_layouts_are_refused_before_blocks() {
    let server = MockFirehose::start(vec![response(100, 3)], vec![]).await;
    for legacy in [
        "blocks/year=2024/month=01/date=15/part-000001.parquet",
        "blocks/year=2023/month=11/day=14/hour=22/part-v1-a.parquet",
        "blocks/block_range=100-200/part-v1-a.parquet",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = root(dir.path());
        let path = root.join(legacy);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"old layout").unwrap();
        let before = tree_digests(&root);
        let output = run(command(&server, dir.path(), 100, 101)).await;
        assert!(!output.status.success(), "{}", logs(&output));
        assert!(logs(&output).contains("legacy data"), "{}", logs(&output));
        assert_eq!(server.calls(), 0, "{legacy}");
        assert_eq!(tree_digests(&root), before, "{legacy}");
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
    assert!(!root.join(MIRROR).exists());

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
    assert!(!root.join(MIRROR).exists());

    // Extension resumes from the authoritative cursor alone.
    success(without_mirror(100, 103)).await;
    assert_eq!(server.calls(), 2);
    assert_eq!(block_numbers(&root), [100, 101, 102]);
    let state = authority(&root);
    assert_eq!(state["checkpoint"]["ordinal"], 3);
    assert_eq!(state["checkpoint"]["event"]["cursor"], "fixture-102");
    assert_eq!(state["checkpoint"]["completed_stop"], 103);
    assert!(!root.join(MIRROR).exists());
    server.assert_drained();
}

/// Ownership and authority must resolve a `--cursor` mirror outside the
/// output root to the same file; otherwise the mirror write is refused as
/// outside held ownership. Braces in the path are literal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_cursor_is_owned_and_bound_as_one_mirror() {
    let server =
        MockFirehose::start(vec![response(100, 3)], vec![Plan::complete("", 100, 100)]).await;
    let dir = tempfile::tempdir().unwrap();
    let mirror = dir.path().join("state").join("worker-{a}.parquet");
    let mut request = command(&server, dir.path(), 100, 101);
    request.arg("--cursor").arg(&mirror);
    success(request).await;
    let root = root(dir.path());
    let descriptor = &authority(&root)["descriptor"]["mirror"];
    assert_eq!(descriptor["kind"], "local");
    assert_eq!(descriptor["absolute_path"], mirror.to_str().unwrap());
    let saved = load_cursor_parquet(&mirror).unwrap().unwrap();
    assert_eq!(saved.last_block_num, 100);
    assert_eq!(saved.cursor, "fixture-100");
    assert!(!root.join(MIRROR).exists());
    server.assert_drained();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_none_cannot_drop_an_existing_bound_mirror() {
    let server =
        MockFirehose::start(vec![response(100, 3)], vec![Plan::complete("", 100, 100)]).await;
    let dir = tempfile::tempdir().unwrap();
    let root = root(dir.path());
    success(command(&server, dir.path(), 100, 101)).await;
    assert!(root.join(MIRROR).exists());
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

/// #647 moved the default mirror to `_fireparq/cursor.parquet`, and the mirror
/// location is bound when a dataset is created. A dataset whose mirror was
/// bound at the old default `<root>/cursor.parquet` is refused before Blocks
/// when rerun with the new default, with the exact flag that resumes it, and
/// nothing moves; `--cursor cursor.parquet` then resumes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mirror_bound_at_the_pre_v1_default_resumes_only_with_that_cursor() {
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
    let legacy = |origin, stop| {
        let mut request = command(&server, dir.path(), origin, stop);
        request.args(["--cursor", "cursor.parquet"]);
        request
    };
    success(legacy(100, 102)).await;
    let legacy_mirror = root.join("cursor.parquet");
    assert_eq!(
        authority(&root)["descriptor"]["mirror"]["absolute_path"],
        legacy_mirror.to_str().unwrap()
    );
    assert!(!root.join("_fireparq").exists());

    let before = tree_digests(&root);
    let output = run(command(&server, dir.path(), 100, 103)).await;
    assert!(!output.status.success(), "{}", logs(&output));
    assert!(
        logs(&output).contains("pre-v1.0.0 default cursor mirror")
            && logs(&output).contains("--cursor cursor.parquet"),
        "{}",
        logs(&output)
    );
    assert_eq!(server.calls(), 1);
    assert_eq!(tree_digests(&root), before);

    success(legacy(100, 103)).await;
    assert_eq!(server.calls(), 2);
    assert_eq!(block_numbers(&root), [100, 101, 102]);
    let mirror = load_cursor_parquet(&legacy_mirror).unwrap().unwrap();
    assert_eq!(mirror.last_block_num, 102);
    assert!(!root.join(MIRROR).exists());
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

/// A dry run predicts the real bounded-completion rule: the same exhausted
/// sparse tail fails in both, without writing anything in the dry run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dry_run_refuses_the_same_unproven_sparse_tail_as_a_real_build() {
    let server = MockFirehose::start(
        vec![response(100, 3)],
        vec![
            Plan::complete("", 100, 101),
            Plan::complete("fixture-100", 100, 101),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let mut request = command(&server, dir.path(), 100, 102);
    request.arg("--dry-run");
    let output = run(request).await;
    assert!(!output.status.success(), "{}", logs(&output));
    assert!(
        logs(&output).contains("before the last requested block 101"),
        "{}",
        logs(&output)
    );
    assert!(
        logs(&output).contains("on every chain"),
        "{}",
        logs(&output)
    );
    assert!(!root(dir.path()).join(CONTROL_DIRECTORY).exists());
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
                    .downcast_ref::<Int64Array>()
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
            let mirror = load_cursor_parquet(&root.join(MIRROR)).unwrap().unwrap();
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

fn pending(root: &Path) -> Option<Value> {
    let path = root
        .join(CONTROL_DIRECTORY)
        .join(ControlKey::Pending.filename());
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("{error}"),
    };
    let record: Value = serde_json::from_slice(&bytes).unwrap();
    (record["deleted"] == false).then(|| record["payload"].clone())
}
fn staged_temporaries(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".fireparq-txn-")
            {
                found.push(path);
            }
        }
    }
    found
}

/// Make `directory` read-only. Returns false when this user bypasses directory
/// permissions (for example root), so the failure cannot be modeled.
#[cfg(unix)]
fn deny_writes(directory: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o555)).unwrap();
    let probe = directory.join(".permission-probe");
    if std::fs::write(&probe, b"").is_ok() {
        std::fs::remove_file(probe).unwrap();
        allow_writes(directory);
        eprintln!("skipping: directory permissions are not enforced for this user");
        return false;
    }
    true
}
#[cfg(unix)]
fn allow_writes(directory: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// #464 on the real path: a table write failing inside an all-table flush
/// (after an earlier table's part was already published) exits nonzero,
/// leaves authority and the mirror unchanged, and the next run rolls back the
/// failed transaction before replaying, so every row appears exactly once.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_failure_during_flush_keeps_authority_and_rerun_recovers_rows_once() {
    let server = MockFirehose::start(
        (100..102)
            .map(|height| response_with_sizing_payload(height, true))
            .collect(),
        vec![
            Plan::complete("", 100, 100),
            Plan::complete("fixture-100", 100, 101),
            Plan::complete("fixture-100", 100, 101),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let root = root(dir.path());
    success(command(&server, dir.path(), 100, 101)).await;
    assert_eq!(table_block_numbers(&root, "blocks"), [100]);
    assert_eq!(table_block_numbers(&root, "transactions"), [100]);
    let before_state = authority(&root);
    let before_mirror = std::fs::read(root.join(MIRROR)).unwrap();
    let before_parts = parts(&root);

    // The next block lands in the same `date=` partition directory.
    let blocked = before_parts
        .keys()
        .find(|path| path.starts_with("transactions"))
        .map(|path| root.join(path).parent().unwrap().to_path_buf())
        .unwrap();
    if !deny_writes(&blocked) {
        return;
    }
    // A one-byte in-flight budget admits each part only after the previous
    // one is published, so `blocks` deterministically publishes before the
    // `transactions` staging failure. Parallel orderings are covered below.
    let mut serial = command(&server, dir.path(), 100, 102);
    serial.args(["--flush-inflight-bytes", "1"]);
    let output = run(serial).await;
    allow_writes(&blocked);
    assert!(!output.status.success(), "{}", logs(&output));
    assert!(
        logs(&output).contains("creating planned staging file"),
        "{}",
        logs(&output)
    );
    assert_eq!(authority(&root), before_state);
    assert_eq!(std::fs::read(root.join(MIRROR)).unwrap(), before_mirror);
    // `blocks` sorts before `transactions`, so its part was published by the
    // failed transaction. The Writing journal owns it; no temp survives.
    let failed_parts = parts(&root);
    assert_eq!(failed_parts.len(), before_parts.len() + 1);
    let orphan: Vec<_> = failed_parts
        .keys()
        .filter(|path| !before_parts.contains_key(*path))
        .collect();
    assert!(orphan[0].starts_with("blocks"), "{orphan:?}");
    assert_eq!(pending(&root).unwrap()["phase"], "writing");
    assert!(staged_temporaries(&root).is_empty());

    success(command(&server, dir.path(), 100, 102)).await;
    assert_eq!(table_block_numbers(&root, "blocks"), [100, 101]);
    assert_eq!(table_block_numbers(&root, "transactions"), [100, 101]);
    assert_checkpoint(&root, 2, 101, 102);
    assert!(pending(&root).is_none());
    let recovered = parts(&root);
    assert_eq!(recovered.len(), 4);
    // Recovery removed the Writing part before replay; deterministic transaction
    // names let the replay publish the identical part again, never a second copy.
    assert_eq!(recovered.get(orphan[0]), failed_parts.get(orphan[0]));
    for (path, digest) in before_parts {
        assert_eq!(recovered.get(&path), Some(&digest));
    }
    assert_eq!(server.calls(), 3);
    server.assert_drained();
}

/// #469 on the real path: a persistently failing mirror save is fatal and
/// reported, although the all-table commit and authority already advanced.
/// The next run repairs the mirror from authority without replaying rows.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_mirror_save_failure_exits_nonzero_and_rerun_repairs_the_mirror() {
    let server = MockFirehose::start(
        (100..102).map(|n| response(n, 3)).collect(),
        vec![
            Plan::complete("", 100, 101),
            // The resumed stream has nothing left; it is resumed once more to
            // confirm the range is exhausted, then completion is proven.
            Plan::complete("fixture-101", 100, 101),
            Plan::complete("fixture-101", 100, 101),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let root = root(dir.path());
    let state = dir.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let mirror = state.join("cursor.parquet");
    let with_mirror = || {
        let mut request = command(&server, dir.path(), 100, 102);
        request.args(["--cursor", mirror.to_str().unwrap()]);
        request
    };
    if !deny_writes(&state) {
        return;
    }
    let output = run(with_mirror()).await;
    allow_writes(&state);
    assert!(!output.status.success(), "{}", logs(&output));
    assert!(
        logs(&output).contains("protected mirror persistence failed after three local attempts"),
        "{}",
        logs(&output)
    );
    assert!(!mirror.exists());
    assert_eq!(block_numbers(&root), [100, 101]);
    let committed = authority(&root);
    assert_eq!(committed["checkpoint"]["ordinal"], 2);
    assert!(committed["checkpoint"]["completed_stop"].is_null());
    assert_eq!(pending(&root).unwrap()["phase"], "committed");
    let committed_parts = parts(&root);

    success(with_mirror()).await;
    assert!(pending(&root).is_none());
    let state = authority(&root);
    assert_eq!(state["checkpoint"]["ordinal"], 2);
    assert_eq!(state["checkpoint"]["completed_stop"], 102);
    let saved = load_cursor_parquet(&mirror).unwrap().unwrap();
    assert_eq!(saved.last_block_num, 101);
    assert_eq!(saved.cursor, "fixture-101");
    assert_eq!(parts(&root), committed_parts);
    assert_eq!(block_numbers(&root), [100, 101]);
    assert_eq!(server.calls(), 3);
    server.assert_drained();
}

/// #572 on the real path: when the last mapper window is flushed only by the
/// stream-end drain, that commit and the completion checkpoint both land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_end_mapper_flush_commits_the_final_checkpoint() {
    let server = MockFirehose::start(
        (100..105).map(|n| response(n, 3)).collect(),
        vec![Plan::complete("", 100, 104)],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    // Two block-count flushes ([100,101], [102,103]); 104 remains buffered
    // until the clean end of stream.
    let output = success(command_with_flush(&server, dir.path(), 100, 105, 2)).await;
    let root = root(dir.path());
    assert!(
        plain_logs(&output).contains("committed flush size observation trigger=\"stream_end\""),
        "{}",
        plain_logs(&output)
    );
    assert_eq!(block_numbers(&root), [100, 101, 102, 103, 104]);
    let blocks: Vec<_> = parts(&root)
        .into_keys()
        .filter(|path| path.starts_with("blocks"))
        .collect();
    assert_eq!(blocks.len(), 3);
    let mut final_window = None;
    for path in &blocks {
        let footer = inspect_footer(&root.join(path)).await;
        if footer_value(&footer, "fireparq.ingest.first_ordinal") == "5" {
            assert_eq!(footer_value(&footer, "fireparq.ingest.last_ordinal"), "5");
            final_window = Some(path.clone());
        }
    }
    assert!(
        final_window.is_some(),
        "no part holds the stream-end window"
    );
    assert_checkpoint(&root, 5, 104, 105);
    assert!(pending(&root).is_none());
    server.assert_drained();
}

/// #466 on the real path: Firehose serves a start above LIB from LIB+1, so
/// blocks below `--start-block` arrive on a non-dry run. They are counted as
/// accepted zero-row events and never written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocks_served_below_start_are_skipped_and_never_written() {
    let server = MockFirehose::start(
        (100..104).map(|n| response(n, 3)).collect(),
        vec![Plan {
            serve_from: Some(100),
            ..Plan::complete("", 102, 103)
        }],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let output = success(command(&server, dir.path(), 102, 104)).await;
    let root = root(dir.path());
    assert!(
        plain_logs(&output).contains("blocks_skipped_below_start=2"),
        "{}",
        plain_logs(&output)
    );
    assert_eq!(block_numbers(&root), [102, 103]);
    for table in ["blocks", "transactions", "calls", "logs"] {
        assert!(
            table_block_numbers(&root, table)
                .iter()
                .all(|number| *number >= 102),
            "{table}"
        );
    }
    let state = authority(&root);
    assert_eq!(state["descriptor"]["origin_start"], 102);
    // Every received envelope is ordered, including the two filtered ones.
    assert_checkpoint(&root, 4, 103, 104);
    server.assert_drained();
}

/// Replaces a unit test that routed through the former `OutputWriter`: a
/// Solana payload without `block_time` keeps a null row timestamp, while the
/// protected commit routes it by the received Firehose source time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn solana_null_block_time_routes_by_source_metadata_on_the_real_path() {
    let mut event = response(42, 3);
    event.block = Some(prost_types::Any {
        type_url: "type.googleapis.com/sf.solana.type.v1.Block".into(),
        value: firehose_protos::sf::solana::r#type::v1::Block {
            slot: 42,
            parent_slot: 41,
            block_time: None,
            ..Default::default()
        }
        .encode_to_vec()
        .into(),
    });
    let server = MockFirehose::start(vec![event], vec![Plan::complete("", 42, 42)]).await;
    let dir = tempfile::tempdir().unwrap();
    let mut request = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    request
        .kill_on_drop(true)
        .env_clear()
        .current_dir(dir.path())
        .args([
            "build",
            "--endpoint",
            &server.endpoint,
            "--block-type",
            "solana",
            "--start-block",
            "42",
            "--stop-block",
            "43",
            "--stream-idle-timeout-secs",
            "0",
            "--output",
        ])
        .arg(dir.path().join("output"));
    success(request).await;
    let root = root(dir.path());
    let blocks: Vec<_> = parts(&root)
        .into_keys()
        .filter(|path| path.starts_with("blocks"))
        .collect();
    assert_eq!(blocks.len(), 1);
    // 1_700_000_000 is 2023-11-14 UTC.
    assert!(
        blocks[0].starts_with("blocks/date=2023-11-14/"),
        "{blocks:?}"
    );
    let batches = read_parquet(&root.join(&blocks[0])).unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    assert_eq!(
        batches[0].column_by_name("timestamp").unwrap().null_count(),
        1
    );
    assert_checkpoint(&root, 1, 42, 43);
    server.assert_drained();
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
                serve_from: None,
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
                .downcast_ref::<Int64Array>()
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
        traits::{BlockIdentity, BlockMapper, StreamEvent},
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
            StreamEvent::default(),
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

/// The retained real EVM block (5,049 rows over many tables) and a synthetic
/// parent seed at `origin = number - 1`, with fixed public fixture cursors.
fn retained_evm_fixture() -> (u64, u64, firehose::Response, firehose::Response) {
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
    (origin, number, seed, retained)
}

/// `--flush-inflight-bytes 1` admits each part only after the previous one is
/// published: one table at a time, as before #516.
const STRICT_SERIAL: [&str; 6] = [
    "--flush-encode-concurrency",
    "1",
    "--flush-publish-concurrency",
    "1",
    "--flush-inflight-bytes",
    "1",
];
const PARALLEL: [&str; 4] = [
    "--flush-encode-concurrency",
    "4",
    "--flush-publish-concurrency",
    "4",
];

fn table_rows(root: &Path) -> BTreeMap<String, usize> {
    let mut rows = BTreeMap::new();
    for path in parts(root).keys() {
        let table = path.components().next().unwrap().as_os_str();
        let count: usize = read_parquet(&root.join(path))
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        *rows
            .entry(table.to_string_lossy().into_owned())
            .or_default() += count;
    }
    rows
}

/// Parse `key=value` from the committed flush log line.
fn committed_field(logs: &str, key: &str) -> Vec<u64> {
    logs.lines()
        .filter(|line| line.contains("committed flush size observation"))
        .filter_map(|line| {
            let start = line.find(&format!(" {key}="))? + key.len() + 2;
            line[start..].split_whitespace().next()?.parse().ok()
        })
        .collect()
}

/// Parallel table work is byte-for-byte the strict serial output: the same
/// deterministic part names and bytes, authority and mirror row, on the real
/// retained EVM block. The logged work peaks respect every limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_flush_matches_strict_serial_bytes_on_the_retained_evm_block() {
    let (origin, number, seed, retained) = retained_evm_fixture();
    let server = MockFirehose::start(
        vec![seed, retained],
        (0..4)
            .map(|_| Plan::complete("", origin as i64, number))
            .collect(),
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let output = root(directory.path());
    let run = |extra: &'static [&'static str]| {
        let mut request = command_with_flush(&server, directory.path(), origin, number + 1, 1);
        request.args(extra);
        request
    };

    success(run(&STRICT_SERIAL)).await;
    let expected_paths = parts(&output);
    assert!(expected_paths.len() >= 10, "{}", expected_paths.len());
    let expected_authority = authority(&output);
    let expected_mirror = read_parquet(&output.join(MIRROR)).unwrap();

    // Same canonical root, so the stream identity and names are identical.
    for (extra, limits) in [
        (&PARALLEL[..], Some((4, 4, None))),
        (
            &[
                "--flush-encode-concurrency",
                "3",
                "--flush-publish-concurrency",
                "2",
                "--flush-inflight-bytes",
                "600000",
            ][..],
            Some((3, 2, Some(600_000))),
        ),
        (&[][..], None),
    ] {
        std::fs::remove_dir_all(&output).unwrap();
        let logged = plain_logs(&success(run(extra)).await);
        assert_eq!(parts(&output), expected_paths, "{extra:?}");
        assert_eq!(authority(&output), expected_authority, "{extra:?}");
        let mirror = read_parquet(&output.join(MIRROR)).unwrap();
        for (actual, expected) in mirror.iter().zip(&expected_mirror) {
            for (index, field) in actual.schema().fields().iter().enumerate() {
                if field.name() != "updated_at" {
                    assert_eq!(actual.column(index), expected.column(index), "{extra:?}");
                }
            }
        }
        if let Some((encoders, publications, bytes)) = limits {
            let peaks = committed_field(&logged, "peak_encoders");
            assert!(!peaks.is_empty() && peaks.iter().all(|peak| *peak <= encoders));
            assert!(peaks.iter().any(|peak| *peak > 1), "no overlap: {peaks:?}");
            assert!(committed_field(&logged, "peak_publications")
                .iter()
                .all(|peak| *peak <= publications));
            if let Some(bytes) = bytes {
                // Local output keeps staged parts reserved until publication;
                // no part of this block is larger than the budget.
                assert!(committed_field(&logged, "peak_inflight_bytes")
                    .iter()
                    .all(|peak| *peak <= bytes));
            }
        }
    }
    assert_eq!(server.calls(), 4);
    server.assert_drained();
}

/// Faults injected into bounded concurrent table work of the real binary
/// (debug builds only): a failed encoder, a failed publication, a lost
/// acknowledgement after publication, and an abrupt process death between
/// parts. Each leaves authority and the mirror at the previous checkpoint, and
/// the next ordinary run recovers every row exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_flush_faults_recover_every_row_exactly_once() {
    let (origin, number, seed, retained) = retained_evm_fixture();
    // Clean reference rows for the same range.
    let reference = {
        let server = MockFirehose::start(
            vec![seed.clone(), retained.clone()],
            vec![Plan::complete("", origin as i64, number)],
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        success(command_with_flush(
            &server,
            directory.path(),
            origin,
            number + 1,
            1,
        ))
        .await;
        table_rows(&root(directory.path()))
    };
    assert!(reference.len() >= 10, "{reference:?}");
    for fault in [
        "encode:transactions",
        "publish:logs",
        "lost-ack:calls",
        "crash-after-publish:balance_changes",
    ] {
        let server = MockFirehose::start(
            vec![seed.clone(), retained.clone()],
            vec![
                Plan::complete("", origin as i64, origin),
                Plan::complete("fixture-26049574", origin as i64, number),
                Plan::complete("fixture-26049574", origin as i64, number),
            ],
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        let output = root(directory.path());
        success(command_with_flush(
            &server,
            directory.path(),
            origin,
            number,
            1,
        ))
        .await;
        let before = (authority(&output), parts(&output));
        let mirror_before = std::fs::read(output.join(MIRROR)).unwrap();

        let mut faulted = command_with_flush(&server, directory.path(), origin, number + 1, 1);
        faulted.args(PARALLEL).env("FIREPARQ_DEBUG_FAULT", fault);
        let failed = run(faulted).await;
        assert!(!failed.status.success(), "{fault}: {}", logs(&failed));
        let (kind, table) = fault.split_once(':').unwrap();
        if kind != "crash-after-publish" {
            assert!(
                logs(&failed).contains("injected debug fault"),
                "{fault}: {}",
                logs(&failed)
            );
            // Ordinary errors remove this transaction's staging names.
            assert!(staged_temporaries(&output).is_empty(), "{fault}");
        }
        assert_eq!(authority(&output), before.0, "{fault}");
        assert_eq!(
            std::fs::read(output.join(MIRROR)).unwrap(),
            mirror_before,
            "{fault}"
        );
        assert_eq!(pending(&output).unwrap()["phase"], "writing", "{fault}");
        let during = parts(&output);
        for (path, digest) in &before.1 {
            assert_eq!(during.get(path), Some(digest), "{fault}");
        }
        if matches!(kind, "lost-ack" | "crash-after-publish") {
            // Published by the failed transaction, owned by its journal.
            assert!(
                during
                    .keys()
                    .any(|path| !before.1.contains_key(path) && path.starts_with(table)),
                "{fault}"
            );
        }

        success(command_with_flush(
            &server,
            directory.path(),
            origin,
            number + 1,
            1,
        ))
        .await;
        assert!(pending(&output).is_none(), "{fault}");
        assert!(staged_temporaries(&output).is_empty(), "{fault}");
        assert_eq!(table_rows(&output), reference, "{fault}");
        let state = authority(&output);
        assert_eq!(state["checkpoint"]["ordinal"], 2, "{fault}");
        assert_eq!(state["checkpoint"]["completed_stop"], number + 1, "{fault}");
        assert_eq!(server.calls(), 3, "{fault}");
        server.assert_drained();
    }
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
    let (origin, number, seed, retained) = retained_evm_fixture();
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
    let expected_mirror = read_parquet(&output.join(MIRROR)).unwrap();

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
    let actual_mirror = read_parquet(&output.join(MIRROR)).unwrap();
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

/// A final fixture block whose time is `seconds` (the default fixture uses one time).
fn response_at(number: u64, seconds: i64) -> firehose::Response {
    let mut response = response(number, 3);
    response.metadata.as_mut().unwrap().time = Some(prost_types::Timestamp { seconds, nanos: 0 });
    response
}

/// The `fireparq` binary with a cleared environment, run from `dir`.
fn fireparq(dir: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    command.kill_on_drop(true).env_clear().current_dir(dir);
    command
}

/// A bounded build from block 100 into `<dir>/output` (date partitions).
fn daily_build(server: &MockFirehose, dir: &Path, stop: u64) -> tokio::process::Command {
    let mut command = fireparq(dir);
    command
        .args([
            "build",
            "--endpoint",
            &server.endpoint,
            "--block-type",
            "evm",
            "--start-block",
            "100",
            "--stop-block",
            &stop.to_string(),
            "--flush-interval-secs",
            "1000000000",
            "--stream-idle-timeout-secs",
            "0",
            "--output",
        ])
        .arg(dir.join("output"));
    command
}

/// Every file below `root` (relative, `/`-separated) with its bytes digest.
fn tree_digests(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                    Sha256::digest(std::fs::read(&path).unwrap()).to_vec(),
                );
            }
        }
    }
    files
}

/// Every `.parquet` file below `dir`, like a `<dir>/**/*.parquet` glob. With
/// `skip_hidden`, paths with a component starting with `_` or `.` are left
/// out, the Hadoop/Hive convention Spark, Trino, Hive and Delta follow.
fn glob_parquet(dir: &Path, skip_hidden: bool) -> std::collections::BTreeSet<PathBuf> {
    let mut files = std::collections::BTreeSet::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_str().unwrap();
            if skip_hidden && (name.starts_with('_') || name.starts_with('.')) {
                continue;
            }
            if path.is_dir() {
                stack.push(path);
            } else if name.ends_with(".parquet") {
                files.insert(path);
            }
        }
    }
    files
}

/// #647: a dataset root holds only its table directories, fireparq's
/// `_fireparq/` artifact directory and dot-prefixed control state. A
/// DuckDB-style per-table glob `<root>/<table>/**/*.parquet` reads only that
/// table's parts, and a dataset-wide read that follows the `_`/`.` hidden-path
/// convention reads exactly the union of the table globs. Only a naive
/// dataset-wide glob would also pick up the artifacts.
fn assert_dataset_root_layout(root: &Path, tables: &[&str], artifacts: &[&str]) {
    let names = |dir: &Path| -> std::collections::BTreeSet<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    };
    for name in names(root) {
        assert!(
            name == "_fireparq" || name.starts_with('.') || tables.contains(&name.as_str()),
            "unexpected root entry {name} in {root:?}"
        );
        if !name.starts_with('.') || name == ".fireparq-ingest" {
            assert!(root.join(&name).is_dir(), "{name} is a directory");
        }
    }
    let in_artifacts = names(&root.join("_fireparq"));
    for artifact in artifacts {
        assert!(
            in_artifacts.contains(*artifact),
            "{artifact}: {in_artifacts:?}"
        );
    }
    let mut union = std::collections::BTreeSet::new();
    let mut with_rows = 0;
    for table in tables {
        // Every table is a Delta table from the first start (#643), whose log
        // holds no Parquet file; a table without rows holds only its log.
        assert!(root.join(table).join("_delta_log").is_dir(), "{table}");
        let files = glob_parquet(&root.join(table), false);
        if files.is_empty() {
            assert_eq!(names(&root.join(table)), ["_delta_log".to_string()].into());
            continue;
        }
        with_rows += 1;
        let mut schema = None;
        for file in &files {
            let name = file.file_name().unwrap().to_str().unwrap();
            assert!(name.starts_with("part-"), "{table}: {file:?}");
            let batches = read_parquet(file).unwrap();
            let file_schema = batches[0].schema();
            assert_eq!(
                schema.get_or_insert_with(|| file_schema.clone()),
                &file_schema,
                "{file:?}"
            );
        }
        union.extend(files);
    }
    assert!(with_rows > 0, "{tables:?}");
    assert_eq!(glob_parquet(root, true), union);
    assert!(glob_parquet(root, false)
        .iter()
        .any(|file| file.starts_with(root.join("_fireparq"))));
}

/// The DuckDB CLI: `FIREPARQ_DUCKDB`, else `duckdb` on `PATH`. CI installs a
/// pinned CLI and sets `FIREPARQ_REQUIRE_DUCKDB`, so the check can only be
/// skipped locally (same contract as `non_final_stream.rs`).
fn duckdb() -> Option<PathBuf> {
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
    eprintln!("skipping the DuckDB table-glob check: no DuckDB CLI ({candidate:?})");
    None
}

/// Runs `sql` in a fresh in-memory DuckDB without `~/.duckdbrc` and returns
/// the rows of the last statement, numbers compared as text.
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
    let rows: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    rows.into_iter()
        .map(|row| {
            Value::Object(
                row.as_object()
                    .unwrap()
                    .iter()
                    .map(|(name, value)| {
                        let text = match value {
                            Value::String(text) => text.clone(),
                            other => other.to_string(),
                        };
                        (name.clone(), Value::String(text))
                    })
                    .collect(),
            )
        })
        .collect()
}

fn json_output(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{error}: {}", logs(output)))
}

/// `fireparq <args>` from `dir` with a cleared environment; its output.
async fn fireparq_output(dir: &Path, args: &[&str]) -> Output {
    let mut command = fireparq(dir);
    command.args(args);
    run(command).await
}

/// `--output` is the dataset root (one bucket or directory per network): no
/// `<chain_name>` directory is appended. The chain name is still required and
/// recorded, and every downstream command works on that root.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_is_the_dataset_root_and_every_command_follows_it() {
    // 2023-11-14 22:13:20 UTC. Blocks 100 to 102 share 2023-11-14, so the two
    // runs leave two parts there; 103 starts 2023-11-15.
    const T: i64 = 1_700_000_000;
    let server = MockFirehose::start(
        vec![
            response_at(100, T),
            response_at(101, T + 3_600),
            response_at(102, T + 3_660),
            response_at(103, T + 7_200),
        ],
        vec![
            Plan::complete("", 100, 101),
            Plan::complete("fixture-101", 100, 103),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("output");
    let (root_arg, blocks_arg) = (
        root.to_str().unwrap().to_string(),
        root.join("blocks").to_str().unwrap().to_string(),
    );

    // build: data and authority sit at the root, the mirror in `_fireparq/`.
    let logs_first = plain_logs(&success(daily_build(&server, dir.path(), 102)).await);
    assert!(
        logs_first.contains(&format!(
            "resolved write destinations output={} cursor={}",
            root.display(),
            root.join(MIRROR).display()
        )),
        "{logs_first}"
    );
    assert!(!root.join(CHAIN).exists());
    let state = authority(&root);
    assert_eq!(state["descriptor"]["chain"], CHAIN);
    assert_eq!(
        state["descriptor"]["output"]["canonical_root"],
        std::fs::canonicalize(&root).unwrap().to_str().unwrap()
    );
    assert_eq!(
        state["descriptor"]["mirror"]["absolute_path"],
        root.join(MIRROR).to_str().unwrap()
    );
    assert_checkpoint(&root, 2, 101, 102);
    assert_eq!(block_numbers(&root), [100, 101]);
    let first_parts = parts(&root);
    // <table>/date=YYYY-MM-DD/<part>, with no chain directory above.
    assert!(first_parts.keys().all(|path| path.components().count() == 3
        && path
            .iter()
            .nth(1)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("date=")));
    let blocks_part = first_parts
        .keys()
        .find(|path| path.starts_with("blocks/date=2023-11-14"))
        .unwrap();
    let footer = inspect_footer(&root.join(blocks_part)).await;
    assert_eq!(footer_value(&footer, "firehose-parquet.chain_name"), CHAIN);

    // The same command resumes the root from its authority.
    success(daily_build(&server, dir.path(), 104)).await;
    assert_eq!(server.calls(), 2);
    assert_checkpoint(&root, 4, 103, 104);
    assert_eq!(block_numbers(&root), [100, 101, 102, 103]);
    let day_14 = Path::new("blocks/date=2023-11-14");
    assert_eq!(
        parts(&root)
            .keys()
            .filter(|path| path.starts_with(day_14))
            .count(),
        2
    );

    let canonical = std::fs::canonicalize(&root).unwrap();

    // Read-only commands take the table directory under the root: `validate`
    // reads the three active files of the blocks table's latest snapshot.
    let validated = fireparq_output(dir.path(), &["validate", &blocks_arg]).await;
    assert!(validated.status.success(), "{}", logs(&validated));
    assert!(String::from_utf8_lossy(&validated.stdout).contains("Files scanned:     3"));
    let inspected = fireparq_output(
        dir.path(),
        &["inspect", root.join(MIRROR).to_str().unwrap()],
    )
    .await;
    assert!(inspected.status.success(), "{}", logs(&inspected));

    // The root holds table directories, `_fireparq/` (the mirror) and
    // dot-prefixed control state only, and a per-table glob is unaffected by
    // the artifacts.
    assert_eq!(MIRROR, firehose_parquet::artifacts::DEFAULT_CURSOR_MIRROR);
    let tables: Vec<String> = std::fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| !name.starts_with('_') && !name.starts_with('.'))
        .collect();
    assert!(tables.iter().any(|table| table == "blocks"), "{tables:?}");
    assert_dataset_root_layout(
        &root,
        &tables.iter().map(String::as_str).collect::<Vec<_>>(),
        &["cursor.parquet"],
    );
    // The same with DuckDB: a per-table glob of the data files reads every
    // block row and no artifact. DuckDB does not skip `_` paths, so a
    // dataset-wide glob would also match `_fireparq/`.
    if let Some(duckdb) = duckdb() {
        let blocks_files = parts(&root)
            .keys()
            .filter(|path| path.starts_with("blocks"))
            .count();
        let root_sql = canonical.to_str().unwrap();
        let rows = duckdb_rows(
            &duckdb,
            dir.path(),
            &format!(
                "SELECT count(*) AS n, count(DISTINCT filename) AS files, \
                 count(*) FILTER (WHERE filename LIKE '%/_fireparq/%') AS artifacts \
                 FROM read_parquet('{root_sql}/blocks/**/*.parquet', filename = true)"
            ),
        );
        assert_eq!(
            rows,
            [serde_json::json!({
                "n": "4",
                "files": blocks_files.to_string(),
                "artifacts": "0",
            })]
        );
        let rows = duckdb_rows(
            &duckdb,
            dir.path(),
            &format!(
                "SELECT count(*) AS artifacts FROM glob('{root_sql}/**/*.parquet') \
                 WHERE file LIKE '%/_fireparq/%'"
            ),
        );
        assert_ne!(rows, [serde_json::json!({ "artifacts": "0" })]);
    }

    // recovery reads the same root.
    let status =
        json_output(&fireparq_output(dir.path(), &["recovery", "status", &root_arg]).await);
    assert_eq!(status["state"]["present"], true);
    assert_eq!(status["pending"]["present"], false);
    let output = fireparq_output(dir.path(), &["recovery", "recover", &root_arg]).await;
    assert!(output.status.success(), "{}", logs(&output));

    // A completed range at the root is still an authority-backed no-op.
    let repeated = daily_build(&server, dir.path(), 104);
    assert!(logs(&success(repeated).await).contains("without opening Blocks"));
    assert_eq!(server.calls(), 2);
    assert!(!root.join(CHAIN).exists());
    server.assert_drained();
}

/// `build` with `--output <output>` (or `OUTPUT=<output>`), relative to `dir`,
/// instead of [`command`]'s `<dir>/output`.
fn command_at(
    server: &MockFirehose,
    dir: &Path,
    origin: u64,
    stop: u64,
    output: &str,
    from_env: bool,
) -> tokio::process::Command {
    let mut child = fireparq(dir);
    child.args([
        "build",
        "--endpoint",
        &server.endpoint,
        "--block-type",
        "evm",
        "--start-block",
        &origin.to_string(),
        "--stop-block",
        &stop.to_string(),
        "--flush-interval-secs",
        "1000000000",
        "--stream-idle-timeout-secs",
        "0",
    ]);
    if from_env {
        child.env("OUTPUT", output);
    } else {
        child.args(["--output", output]);
    }
    child
}

/// `--output` is used exactly as given, and `{chain}` is the opt-in chain
/// directory. The protected descriptor binds the resolved root, so a changed
/// template that resolves to another root is refused before any Blocks
/// request and changes no byte: a dataset created at `output` cannot be
/// resumed as `output/{chain}` (nested in it), and one created at
/// `output/{chain}` cannot be resumed as `output` (enclosing it). Spellings
/// of the same root (`output/`, the expanded chain name) resume it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_output_template_is_refused_before_blocks() {
    let server = MockFirehose::start(
        (100..103).map(|n| response(n, 3)).collect(),
        vec![Plan::complete("", 100, 101), Plan::complete("", 100, 101)],
    )
    .await;

    // 1. Created at `output`: the tables sit directly in it.
    let plain = tempfile::tempdir().unwrap();
    let root = plain.path().join("output");
    success(command_at(&server, plain.path(), 100, 102, "output", false)).await;
    assert_eq!(server.calls(), 1);
    assert_checkpoint(&root, 2, 101, 102);
    assert!(!root.join(CHAIN).exists());
    assert_eq!(authority(&root)["descriptor"]["chain"], CHAIN);
    let before = tree_digests(plain.path());
    for from_env in [false, true] {
        let output = run(command_at(
            &server,
            plain.path(),
            100,
            102,
            "output/{chain}",
            from_env,
        ))
        .await;
        assert!(!output.status.success(), "{}", logs(&output));
        assert!(
            logs(&output).contains("overlaps another protected root")
                && logs(&output).contains("--output"),
            "{}",
            logs(&output)
        );
        assert_eq!(server.calls(), 1);
        assert_eq!(tree_digests(plain.path()), before);
        assert!(!root.join(CHAIN).exists());
    }
    for same_root in ["output/", "./output"] {
        let output = success(command_at(
            &server,
            plain.path(),
            100,
            102,
            same_root,
            false,
        ))
        .await;
        assert!(logs(&output).contains("without opening Blocks"));
    }
    assert_eq!(server.calls(), 1);

    // 2. Created at `output/{chain}`: the v0.7.x layout, by opt-in.
    let templated = tempfile::tempdir().unwrap();
    let parent = templated.path().join("output");
    let chain_root = parent.join(CHAIN);
    let first = success(command_at(
        &server,
        templated.path(),
        100,
        102,
        "output/{chain}",
        true,
    ))
    .await;
    assert!(
        plain_logs(&first).contains(&format!(
            "resolved --output template output_template=output/{{chain}} output=output/{CHAIN}"
        )),
        "{}",
        logs(&first)
    );
    assert!(
        plain_logs(&first).contains(&format!(
            "resolved write destinations output={} cursor={}",
            templated
                .path()
                .canonicalize()
                .unwrap()
                .join("output")
                .join(CHAIN)
                .display(),
            templated
                .path()
                .canonicalize()
                .unwrap()
                .join("output")
                .join(CHAIN)
                .join(MIRROR)
                .display()
        )),
        "{}",
        logs(&first)
    );
    assert_eq!(server.calls(), 2);
    assert_checkpoint(&chain_root, 2, 101, 102);
    assert_eq!(
        std::fs::read_dir(&parent)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>(),
        [std::ffi::OsString::from(CHAIN)]
    );
    let before = tree_digests(templated.path());
    for from_env in [false, true] {
        let output = run(command_at(
            &server,
            templated.path(),
            100,
            102,
            "output",
            from_env,
        ))
        .await;
        assert!(!output.status.success(), "{}", logs(&output));
        assert!(
            logs(&output).contains("overlaps another protected root"),
            "{}",
            logs(&output)
        );
        assert_eq!(server.calls(), 2);
        assert_eq!(tree_digests(templated.path()), before);
        assert!(!parent.join(CONTROL_DIRECTORY).exists());
    }
    let literal = format!("output/{CHAIN}");
    for same_root in ["output/{chain}", literal.as_str()] {
        let output = success(command_at(
            &server,
            templated.path(),
            100,
            102,
            same_root,
            false,
        ))
        .await;
        assert!(logs(&output).contains("without opening Blocks"));
    }
    assert_eq!(server.calls(), 2);
    server.assert_drained();
}

/// Template errors (unknown variable, unterminated or unmatched brace, a
/// placeholder in the S3 bucket name) are refused while parsing the
/// configuration, before the endpoint is contacted: the endpoint here is a
/// closed port, and nothing is created.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_output_templates_are_refused_before_the_endpoint_is_contacted() {
    let dir = tempfile::tempdir().unwrap();
    for (output, expected) in [
        ("output/{network}", "unknown --output variable {network}"),
        ("output/{chain", "unterminated --output variable"),
        ("output/chain}", "unmatched } in --output"),
        ("s3://{chain}/datasets", "S3 bucket name"),
    ] {
        {
            let mut command = fireparq(dir.path());
            command
                .arg("build")
                .args(["--endpoint", "http://127.0.0.1:9", "--stop-block", "200"])
                .args(["--output", output])
                .env("AWS_ACCESS_KEY_ID", "test")
                .env("AWS_SECRET_ACCESS_KEY", "test");
            let result = run(command).await;
            assert!(!result.status.success(), "{}", logs(&result));
            assert!(
                logs(&result).contains(expected),
                "{output}: {}",
                logs(&result)
            );
            assert!(!logs(&result).contains("unavailable"), "{}", logs(&result));
        }
    }
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

/// Without `--output` the dataset root is the working directory itself (the
/// default `.`, used as given). A working directory that already holds
/// unrelated files is refused before any Blocks request and gains no entry;
/// an empty one becomes the dataset root.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_output_is_the_working_directory_itself() {
    let server = MockFirehose::start(
        (100..102).map(|n| response(n, 3)).collect(),
        vec![Plan::complete("", 100, 101)],
    )
    .await;
    let default_build = |dir: &Path| {
        let mut command = fireparq(dir);
        command.args([
            "build",
            "--endpoint",
            &server.endpoint,
            "--block-type",
            "evm",
            "--start-block",
            "100",
            "--stop-block",
            "102",
            "--flush-interval-secs",
            "1000000000",
            "--stream-idle-timeout-secs",
            "0",
        ]);
        command
    };
    let entries = |dir: &Path| -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    };

    let busy = tempfile::tempdir().unwrap();
    std::fs::write(busy.path().join("Cargo.toml"), b"[workspace]\n").unwrap();
    let output = run(default_build(busy.path())).await;
    assert!(!output.status.success(), "{}", logs(&output));
    assert!(
        logs(&output).contains("unrelated files"),
        "{}",
        logs(&output)
    );
    assert_eq!(server.calls(), 0);
    assert_eq!(entries(busy.path()), ["Cargo.toml"]);

    let empty = tempfile::tempdir().unwrap();
    success(default_build(empty.path())).await;
    assert_eq!(server.calls(), 1);
    assert_checkpoint(empty.path(), 2, 101, 102);
    assert_eq!(block_numbers(empty.path()), [100, 101]);
    assert!(!empty.path().join(CHAIN).exists());
    server.assert_drained();
}
