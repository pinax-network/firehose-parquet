//! The real CLI publishes only exact finalized snapshots and preserves previous
//! index/cursor bytes when probes, ancestry or legacy metadata are invalid.
use firehose_parquet::cli::{
    read_verified_partitions_index, write_partitions_index, PartitionBuildRow,
};
use firehose_protos::firehose;
use futures::StreamExt;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

const A: i64 = 1_700_000_000;
const B: i64 = A + 3_600;
/// A fault injected into the next range-scan Stream request.
#[derive(Clone, Copy, Debug)]
enum ScanFault {
    /// The request fails with a transient `Unavailable` status.
    Unavailable,
    /// Response headers arrive but no message is ever sent.
    Stall,
}
#[derive(Clone)]
struct Fixture {
    blocks: Arc<BTreeMap<u64, firehose::BlockMetadata>>,
    requests: Arc<Mutex<Vec<firehose::Request>>>,
    finalized: u64,
    /// Consumed by range scans only; finality proofs are never faulted.
    scan_faults: Arc<Mutex<VecDeque<ScanFault>>>,
    /// Number of upcoming Fetch requests answered with `Unavailable`.
    fetch_failures: Arc<AtomicUsize>,
}
fn metadata(num: u64, parent: u64, timestamp: i64) -> firehose::BlockMetadata {
    firehose::BlockMetadata {
        num,
        id: format!("id-{num}"),
        parent_num: parent,
        parent_id: format!("id-{parent}"),
        lib_num: 20,
        time: Some(prost_types::Timestamp {
            seconds: timestamp,
            nanos: 0,
        }),
        ..Default::default()
    }
}
fn alternating(n: u64) -> i64 {
    if n.is_multiple_of(2) {
        A
    } else {
        B
    }
}
/// Blocks `8..=max` with timestamps from `ts` and a finalized head at `finalized`.
fn fixture_with(max: u64, finalized: u64, ts: impl Fn(u64) -> i64) -> Fixture {
    Fixture {
        blocks: Arc::new((8..=max).map(|n| (n, metadata(n, n - 1, ts(n)))).collect()),
        requests: Arc::new(Mutex::new(Vec::new())),
        finalized,
        scan_faults: Arc::new(Mutex::new(VecDeque::new())),
        fetch_failures: Arc::new(AtomicUsize::new(0)),
    }
}
impl Fixture {
    fn regular() -> Self {
        fixture_with(20, 20, alternating)
    }
}
#[derive(Clone)]
struct Info;
#[derive(Clone)]
struct Stream(Fixture);
#[derive(Clone)]
struct Fetch(Fixture);
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "test-chain".into(),
                first_streamable_block_num: 10,
                ..Default::default()
            }))
        })
    }
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream =
        futures::stream::BoxStream<'static, Result<Self::Response, tonic::Status>>;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        self.0.requests.lock().unwrap().push(request.clone());
        let finality_proof = request.start_block_num >= 0
            && request.start_block_num as u64 == self.0.finalized
            && request.stop_block_num == self.0.finalized;
        let fault = if request.start_block_num >= 0 && !finality_proof {
            self.0.scan_faults.lock().unwrap().pop_front()
        } else {
            None
        };
        let blocks = if request.start_block_num < 0 {
            vec![firehose::Response {
                metadata: Some(firehose::BlockMetadata {
                    lib_num: self.0.finalized,
                    ..metadata(30, 29, A)
                }),
                block: Some(prost_types::Any::default()),
                step: 1,
                ..Default::default()
            }]
        } else {
            self.0
                .blocks
                .range(request.start_block_num as u64..=request.stop_block_num)
                .map(|(_, value)| firehose::Response {
                    metadata: Some(value.clone()),
                    block: Some(prost_types::Any::default()),
                    step: 3,
                    ..Default::default()
                })
                .collect()
        };
        Box::pin(async move {
            match fault {
                Some(ScanFault::Unavailable) => {
                    Err(tonic::Status::unavailable("injected scan failure"))
                }
                Some(ScanFault::Stall) => {
                    Ok(tonic::Response::new(futures::stream::pending().boxed()))
                }
                None => Ok(tonic::Response::new(
                    futures::stream::iter(blocks.into_iter().map(Ok)).boxed(),
                )),
            }
        })
    }
}
impl tonic::server::UnaryService<firehose::SingleBlockRequest> for Fetch {
    type Response = firehose::SingleBlockResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::SingleBlockRequest>) -> Self::Future {
        let Some(firehose::single_block_request::Reference::BlockNumber(number)) =
            request.into_inner().reference
        else {
            panic!("expected number reference")
        };
        let block = self.0.blocks.get(&number.num).cloned();
        let fail = self
            .0
            .fetch_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        Box::pin(async move {
            if fail {
                return Err(tonic::Status::unavailable("injected fetch failure"));
            }
            Ok(tonic::Response::new(firehose::SingleBlockResponse {
                metadata: Some(block.ok_or_else(|| tonic::Status::not_found("missing block"))?),
                block: None,
            }))
        })
    }
}
macro_rules! service {
    ($name:ident, $route:literal, $method:ident) => {
        impl tonic::server::NamedService for $name {
            const NAME: &'static str = $route;
        }
        impl Service<http::Request<tonic::body::Body>> for $name {
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
                    Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .$method(service, request)
                        .await)
                })
            }
        }
    };
}
service!(Info, "sf.firehose.v2.EndpointInfo", unary);
service!(Stream, "sf.firehose.v2.Stream", server_streaming);
service!(Fetch, "sf.firehose.v2.Fetch", unary);
struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn spawn_server(fixture: Fixture) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Info)
            .add_service(Stream(fixture.clone()))
            .add_service(Fetch(fixture))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    Server { url, task }
}
fn command(root: &std::path::Path, endpoint: &str, extra: &[&str]) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    command
        .kill_on_drop(true)
        .env_clear()
        .current_dir(root)
        .args([
            "--log-level",
            "error",
            "partitions",
            "build",
            "--endpoint",
            endpoint,
            "--output",
            "output",
            "--json",
        ])
        .args(extra);
    command
}
async fn run(root: &std::path::Path, endpoint: &str, extra: &[&str]) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(10),
        command(root, endpoint, extra).output(),
    )
    .await
    .unwrap()
    .unwrap()
}
fn index_path(root: &std::path::Path) -> std::path::PathBuf {
    root.join("output/test-chain/partitions.parquet")
}
fn assert_ok(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_records_all_backward_runs_and_resumes_by_source_frontier() {
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::regular();
    let server = spawn_server(fixture.clone()).await;
    let output = run(
        root.path(),
        &server.url,
        &[
            "--partition",
            "hour",
            "--start-block",
            "10",
            "--stop-block",
            "13",
        ],
    )
    .await;
    assert_ok(&output);
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["coverage"]["start_block"], 10);
    assert_eq!(json["coverage"]["stop_block"], 13);
    let path = index_path(root.path());
    let first = read_verified_partitions_index(path.to_str().unwrap(), None).unwrap();
    assert_eq!(first.spans.len(), 3);
    assert!(first.spans.iter().all(|span| span.proof.complete()));
    assert_eq!(
        first.spans[0].row.partition_value,
        first.spans[2].row.partition_value
    );
    let output = run(
        root.path(),
        &server.url,
        &["--partition", "hour", "--resume", "--stop-block", "16"],
    )
    .await;
    assert_ok(&output);
    let next = read_verified_partitions_index(path.to_str().unwrap(), None).unwrap();
    assert_eq!(next.coverage.start_block, 10);
    assert_eq!(next.coverage.stop_block, 16);
    assert_eq!(next.spans.len(), 6);
    assert_eq!(next.spans[..3], first.spans);
    assert!(fixture
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|request| request.start_block_num == 13 && request.stop_block_num == 15));
    assert!(!root
        .path()
        .join("output/test-chain/cursor.parquet")
        .exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_future_stop_or_omitted_block_preserves_index_and_cursor() {
    for missing in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut fixture = Fixture::regular();
        if missing {
            Arc::make_mut(&mut fixture.blocks).remove(&11);
        }
        let server = spawn_server(fixture.clone()).await;
        let path = index_path(root.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"previous-index").unwrap();
        let cursor = path.parent().unwrap().join("cursor.parquet");
        std::fs::write(&cursor, b"private-checkpoint").unwrap();
        let output = run(
            root.path(),
            &server.url,
            &[
                "--partition",
                "hour",
                "--start-block",
                "10",
                "--stop-block",
                if missing { "13" } else { "22" },
                "--overwrite",
            ],
        )
        .await;
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains(if missing {
                "omitted"
            } else {
                "proven finalized"
            }),
            "{error}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"previous-index");
        assert_eq!(std::fs::read(&cursor).unwrap(), b"private-checkpoint");
        if !missing {
            assert_eq!(fixture.requests.lock().unwrap().len(), 2);
        }
    }
    let root = tempfile::tempdir().unwrap();
    let server = spawn_server(Fixture::regular()).await;
    let result = run(
        root.path(),
        &server.url,
        &[
            "--partition",
            "block_range",
            "--block-range-size",
            "1",
            "--start-block",
            "10",
            "--stop-block",
            "22",
        ],
    )
    .await;
    assert!(!result.status.success());
    assert!(!root.path().join("output").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_legacy_resume_fails_closed_and_bounded_gap_keeps_clipped_edge() {
    let root = tempfile::tempdir().unwrap();
    let path = index_path(root.path());
    let row = PartitionBuildRow {
        partition_type: "hour".into(),
        partition_interval_seconds: 3_600,
        partition_start_ts: "2023-11-14 22:00:00".into(),
        partition_value: "2023-11-14 22:00:00".into(),
        start_block: 10,
        stop_block: 13,
        start_time: None,
        end_time: None,
        chain: Some("test-chain".into()),
    };
    write_partitions_index(path.to_str().unwrap(), &[row], None).unwrap();
    let old = std::fs::read(&path).unwrap();
    let server = spawn_server(Fixture::regular()).await;
    let result = run(
        root.path(),
        &server.url,
        &["--partition", "hour", "--resume", "--stop-block", "16"],
    )
    .await;
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("rebuild"));
    assert_eq!(std::fs::read(&path).unwrap(), old);
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::regular();
    Arc::make_mut(&mut fixture.blocks).remove(&13);
    Arc::make_mut(&mut fixture.blocks)
        .get_mut(&14)
        .unwrap()
        .parent_num = 12;
    Arc::make_mut(&mut fixture.blocks)
        .get_mut(&14)
        .unwrap()
        .parent_id = "id-12".into();
    let server = spawn_server(fixture).await;
    let result = run(
        root.path(),
        &server.url,
        &[
            "--partition",
            "hour",
            "--start-block",
            "10",
            "--stop-block",
            "13",
        ],
    )
    .await;
    assert_ok(&result);
    let index =
        read_verified_partitions_index(index_path(root.path()).to_str().unwrap(), None).unwrap();
    assert_eq!(index.coverage.stop_block, 13);
    assert_eq!(index.coverage.next_observed.unwrap().block_num, 14);
    assert!(!index.spans.last().unwrap().proof.end_complete);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_live_snapshots_keep_the_head_span_incomplete_on_shutdown() {
    for partition in ["hour", "block_range"] {
        let root = tempfile::tempdir().unwrap();
        let server = spawn_server(Fixture::regular()).await;
        let mut args = vec![
            "--partition",
            partition,
            "--start-block",
            "8",
            "--live",
            "--poll-interval-secs",
            "1",
        ];
        if partition == "block_range" {
            args.extend(["--block-range-size", "4"]);
        }
        live_until_published_then_term(root.path(), &server.url, &args, 21, 10).await;
        let path = index_path(root.path());
        let index = read_verified_partitions_index(path.to_str().unwrap(), None).unwrap();
        assert_eq!(index.coverage.stop_block, 21);
        assert!(!index.spans.last().unwrap().proof.end_complete);
        assert!(!index.spans.last().unwrap().proof.complete());
    }
}

async fn utility(root: &std::path::Path, subcommand: &str, extra: &[&str]) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"))
            .kill_on_drop(true)
            .env_clear()
            .current_dir(root)
            .args([
                "--log-level",
                "error",
                "partitions",
                subcommand,
                "--partitions-index",
            ])
            .arg(index_path(root))
            .args(extra)
            .output(),
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_consumers_expose_coverage_and_preserve_disjoint_runs() {
    let root = tempfile::tempdir().unwrap();
    let server = spawn_server(Fixture::regular()).await;
    assert_ok(
        &run(
            root.path(),
            &server.url,
            &[
                "--partition",
                "hour",
                "--start-block",
                "10",
                "--stop-block",
                "13",
            ],
        )
        .await,
    );
    let select = [
        "--partition-type",
        "hour",
        "--partition-value",
        "2023-11-14 22:00:00",
    ];
    let ambiguous = utility(root.path(), "resolve", &select).await;
    assert!(!ambiguous.status.success());
    assert!(String::from_utf8_lossy(&ambiguous.stderr).contains("ambiguous"));
    let mut all = select.to_vec();
    all.push("--all-spans");
    let parser = utility(root.path(), "resolve", &all).await;
    assert!(!parser.status.success());
    assert!(String::from_utf8_lossy(&parser.stderr).contains("--json"));
    all.push("--json");
    let result = utility(root.path(), "resolve", &all).await;
    assert_ok(&result);
    let json: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(json["coverage"]["start_block"], 10);
    assert_eq!(json["coverage"]["stop_block"], 13);
    assert!(json.get("start_block").is_none());
    assert!(json.get("stop_block").is_none());
    let spans = json["spans"].as_array().unwrap();
    assert_eq!(spans.len(), 2);
    assert_eq!(
        (
            spans[0]["start_block"].as_u64(),
            spans[0]["stop_block"].as_u64()
        ),
        (Some(10), Some(11))
    );
    assert_eq!(
        (
            spans[1]["start_block"].as_u64(),
            spans[1]["stop_block"].as_u64()
        ),
        (Some(12), Some(13))
    );
    let single = utility(
        root.path(),
        "resolve",
        &[
            "--partition-type",
            "hour",
            "--partition-value",
            "2023-11-14 23:00:00",
        ],
    )
    .await;
    assert_ok(&single);
    let text = String::from_utf8_lossy(&single.stdout);
    assert!(text.contains("[10, 13) (finalized snapshot)"));
    assert!(text.contains("start_block:      11"));
    let list = utility(root.path(), "ls", &[]).await;
    assert_ok(&list);
    assert!(String::from_utf8_lossy(&list.stdout).contains("complete"));
    let report = utility(root.path(), "validate", &["--json"]).await;
    assert_ok(&report);
    let json: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    assert_eq!(json["valid"], true);
    assert_eq!(json["incomplete_spans"], 0);

    // A legacy artifact remains inspectable but cannot silently produce a range.
    let old =
        read_verified_partitions_index(index_path(root.path()).to_str().unwrap(), None).unwrap();
    write_partitions_index(
        index_path(root.path()).to_str().unwrap(),
        &old.spans
            .into_iter()
            .map(|span| span.row)
            .collect::<Vec<_>>(),
        None,
    )
    .unwrap();
    let result = utility(root.path(), "resolve", &all).await;
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("rebuild"));
    let list = utility(root.path(), "ls", &[]).await;
    assert_ok(&list);
    let text = String::from_utf8_lossy(&list.stdout);
    assert!(text.contains("unknown (legacy index"));
    assert!(!root
        .path()
        .join("output/test-chain/cursor.parquet")
        .exists());
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// `(partition_value, start_block, stop_block, start_complete, end_complete)` per span.
fn spans(root: &std::path::Path) -> Vec<(String, u64, u64, bool, bool)> {
    read_verified_partitions_index(index_path(root).to_str().unwrap(), None)
        .unwrap()
        .spans
        .into_iter()
        .map(|span| {
            (
                span.row.partition_value,
                span.row.start_block,
                span.row.stop_block,
                span.proof.start_complete,
                span.proof.end_complete,
            )
        })
        .collect()
}

/// Run a live build until it publishes coverage reaching `stop_block`, then send
/// SIGTERM and require a clean exit. The live command must not exit on its own.
#[cfg(unix)]
async fn live_until_published_then_term(
    root: &std::path::Path,
    url: &str,
    args: &[&str],
    stop_block: u64,
    deadline_secs: u64,
) {
    let mut child = command(root, url, args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let path = index_path(root);
    let published = tokio::time::timeout(Duration::from_secs(deadline_secs), async {
        loop {
            if read_verified_partitions_index(path.to_str().unwrap(), None)
                .is_ok_and(|index| index.coverage.stop_block == stop_block)
            {
                return true;
            }
            if child.try_wait().unwrap().is_some() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("live command did not publish the expected snapshot in time");
    if !published {
        let output = child.wait_with_output().await.unwrap();
        panic!(
            "live command exited early: {}: {}",
            output.status,
            stderr(&output)
        );
    }
    assert!(tokio::process::Command::new("/bin/kill")
        .args(["-TERM", &child.id().unwrap().to_string()])
        .status()
        .await
        .unwrap()
        .success());
    let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert_ok(&output);
}

/// #483: a bounded build over an existing index needs --resume or --overwrite and
/// leaves the file bytes unchanged, an explicit start past the frontier is refused,
/// resume starts at the frontier, a covered re-run is a no-op and --overwrite
/// replaces the index. Checked for a time-based and a block_range index.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_existing_index_requires_resume_or_overwrite_and_resumes_at_frontier() {
    for (partition, extra) in [
        ("hour", Vec::<&str>::new()),
        ("block_range", vec!["--block-range-size", "2"]),
    ] {
        let root = tempfile::tempdir().unwrap();
        let server = spawn_server(Fixture::regular()).await;
        let args = |more: &[&'static str]| {
            let mut all = vec!["--partition", partition];
            all.extend(extra.iter().copied());
            all.extend(more.iter().copied());
            all
        };
        let bounded = args(&["--start-block", "10", "--stop-block", "14"]);
        assert_ok(&run(root.path(), &server.url, &bounded).await);
        let path = index_path(root.path());
        let original = std::fs::read(&path).unwrap();
        let original_spans = spans(root.path());

        // A later range without --resume/--overwrite is refused; the index is unchanged.
        let later = args(&["--start-block", "14", "--stop-block", "18"]);
        let out = run(root.path(), &server.url, &later).await;
        assert!(!out.status.success(), "{partition}: later range accepted");
        assert!(
            stderr(&out).contains("already exists"),
            "{partition}: {}",
            stderr(&out)
        );
        assert!(
            stderr(&out).contains("--resume"),
            "{partition}: {}",
            stderr(&out)
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            original,
            "{partition}: index modified"
        );

        // An overlapping range is refused too.
        let overlapping = args(&["--start-block", "12", "--stop-block", "16"]);
        let out = run(root.path(), &server.url, &overlapping).await;
        assert!(
            !out.status.success(),
            "{partition}: overlapping range accepted"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            original,
            "{partition}: index modified"
        );

        // --resume with an explicit start past the stored frontier is refused.
        let past = args(&["--resume", "--start-block", "16", "--stop-block", "18"]);
        let out = run(root.path(), &server.url, &past).await;
        assert!(
            !out.status.success(),
            "{partition}: start past frontier accepted"
        );
        assert!(
            stderr(&out).contains("is past the existing"),
            "{partition}: {}",
            stderr(&out)
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            original,
            "{partition}: index modified"
        );

        // --resume with an explicit start before the frontier resumes at the frontier.
        let resume = args(&["--resume", "--start-block", "10", "--stop-block", "18"]);
        let out = run(root.path(), &server.url, &resume).await;
        assert_ok(&out);
        let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(json["resumed_from_block"], 14, "{partition}");
        let resumed_spans = spans(root.path());
        assert_eq!(
            resumed_spans[..original_spans.len()],
            original_spans[..],
            "{partition}: prior spans changed"
        );
        assert_eq!(resumed_spans.first().unwrap().1, 10, "{partition}");
        assert_eq!(resumed_spans.last().unwrap().2, 18, "{partition}");
        for pair in resumed_spans.windows(2) {
            assert_eq!(
                pair[0].2, pair[1].1,
                "{partition}: gap/overlap {resumed_spans:?}"
            );
        }

        // A re-run already covered by the index is a no-op: bytes are unchanged.
        let resumed = std::fs::read(&path).unwrap();
        let covered = args(&["--resume", "--stop-block", "16"]);
        assert_ok(&run(root.path(), &server.url, &covered).await);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            resumed,
            "{partition}: no-op rewrote"
        );

        // --overwrite replaces the index with the new range only.
        let overwrite = args(&["--start-block", "16", "--stop-block", "20", "--overwrite"]);
        assert_ok(&run(root.path(), &server.url, &overwrite).await);
        let replaced = spans(root.path());
        assert_eq!(replaced.first().unwrap().1, 16, "{partition}");
        assert_eq!(replaced.last().unwrap().2, 20, "{partition}");
    }
}

/// #483/#486: a live block_range snapshot leaves an incomplete head span that
/// resolve and shard refuse; a later bounded --resume completes that span in place
/// instead of duplicating it.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_live_block_range_head_is_refused_then_resumed_without_duplicates() {
    let root = tempfile::tempdir().unwrap();
    let server = spawn_server(Fixture::regular()).await;
    let live = [
        "--partition",
        "block_range",
        "--block-range-size",
        "4",
        "--start-block",
        "8",
        "--live",
        "--poll-interval-secs",
        "1",
    ];
    live_until_published_then_term(root.path(), &server.url, &live, 21, 10).await;
    let head = spans(root.path());
    let last = head.last().unwrap();
    assert_eq!((last.1, last.2, last.4), (20, 21, false), "{head:?}");

    // The incomplete head span is neither resolvable nor shardable.
    let select = ["--partition-type", "block_range", "--partition-value", "20"];
    let out = utility(root.path(), "resolve", &select).await;
    assert!(!out.status.success());
    assert!(stderr(&out).contains("incomplete"), "{}", stderr(&out));
    let out = utility(
        root.path(),
        "shard",
        &["--shard-count", "1", "--shard-index", "0"],
    )
    .await;
    assert!(!out.status.success(), "shard accepted an incomplete span");
    assert!(stderr(&out).contains("incomplete"), "{}", stderr(&out));
    let complete = ["--partition-type", "block_range", "--partition-value", "16"];
    assert_ok(&utility(root.path(), "resolve", &complete).await);

    drop(server);
    let server = spawn_server(fixture_with(30, 28, alternating)).await;
    let resume = [
        "--partition",
        "block_range",
        "--block-range-size",
        "4",
        "--resume",
        "--stop-block",
        "28",
    ];
    assert_ok(&run(root.path(), &server.url, &resume).await);
    let resumed = spans(root.path());
    let ranges = resumed
        .iter()
        .map(|span| (span.1, span.2))
        .collect::<Vec<_>>();
    assert_eq!(ranges, [(8, 12), (12, 16), (16, 20), (20, 24), (24, 28)]);
    assert!(resumed.iter().all(|span| span.3 && span.4), "{resumed:?}");
}

/// #483/#486: a time index whose terminal span ends at the finalized head is
/// extended to its real boundary on --resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_time_head_span_extends_to_its_real_boundary_on_resume() {
    let root = tempfile::tempdir().unwrap();
    let server = spawn_server(Fixture::regular()).await;
    // Stop 21 is the finalized head plus one: no right-edge witness, so the head
    // span is incomplete.
    let bounded = [
        "--partition",
        "hour",
        "--start-block",
        "16",
        "--stop-block",
        "21",
    ];
    assert_ok(&run(root.path(), &server.url, &bounded).await);
    let first = spans(root.path());
    let head = first.last().unwrap().clone();
    assert!(!head.4, "{first:?}");
    let select = [
        "--partition-type",
        "hour",
        "--partition-value",
        head.0.as_str(),
    ];
    assert!(!utility(root.path(), "resolve", &select)
        .await
        .status
        .success());

    drop(server);
    // Blocks 21..=23 share block 20's hour (24 is even), so the head span
    // continues to 25.
    let fixture = fixture_with(30, 28, |n| {
        if (21..=23).contains(&n) {
            A
        } else {
            alternating(n)
        }
    });
    let server = spawn_server(fixture).await;
    let resume = ["--partition", "hour", "--resume", "--stop-block", "26"];
    assert_ok(&run(root.path(), &server.url, &resume).await);
    let next = spans(root.path());
    let ranges = next.iter().map(|span| (span.1, span.2)).collect::<Vec<_>>();
    assert_eq!(
        ranges,
        [(16, 17), (17, 18), (18, 19), (19, 20), (20, 25), (25, 26)]
    );
    assert!(next[4].3 && next[4].4, "{next:?}");
}

/// #482 end to end on a v2 index built by the real CLI: block ranges crossing a
/// digit boundary (8, 9, 10, 11) list, filter, shard and resolve numerically.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_block_range_queries_order_numerically_across_digit_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let server = spawn_server(Fixture::regular()).await;
    let bounded = [
        "--partition",
        "block_range",
        "--block-range-size",
        "1",
        "--start-block",
        "8",
        "--stop-block",
        "12",
    ];
    assert_ok(&run(root.path(), &server.url, &bounded).await);
    let starts = |out: &std::process::Output| -> Vec<u64> {
        let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        json["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["start_block"].as_u64().unwrap())
            .collect()
    };
    let out = utility(root.path(), "ls", &["--limit", "2", "--json"]).await;
    assert_ok(&out);
    assert_eq!(starts(&out), [8, 9]);
    let out = utility(root.path(), "ls", &["--from", "9", "--to", "10", "--json"]).await;
    assert_ok(&out);
    assert_eq!(starts(&out), [9, 10]);
    let shard = [
        "--shard-count",
        "2",
        "--shard-index",
        "0",
        "--strategy",
        "ordinal",
        "--json",
    ];
    let out = utility(root.path(), "shard", &shard).await;
    assert_ok(&out);
    assert_eq!(starts(&out), [8, 10]);
    let out = utility(root.path(), "validate", &["--json"]).await;
    assert_ok(&out);
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["valid"], true);
    let select = [
        "--partition-type",
        "block_range",
        "--partition-value",
        "10",
        "--json",
    ];
    let out = utility(root.path(), "resolve", &select).await;
    assert_ok(&out);
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        (json["start_block"].as_u64(), json["stop_block"].as_u64()),
        (Some(10), Some(11))
    );
}

/// A stalled traversal message (5 s deadline) or exhausted boundary probes end a
/// bounded build, but a live build keeps its last verified snapshot, backs off and
/// retries from the stored frontier until it publishes.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_live_retries_transient_scan_failures_and_keeps_the_snapshot() {
    // Time index: a verified snapshot exists, then the live resume scan stalls.
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::regular();
    let server = spawn_server(fixture.clone()).await;
    let bounded = [
        "--partition",
        "hour",
        "--start-block",
        "10",
        "--stop-block",
        "14",
    ];
    assert_ok(&run(root.path(), &server.url, &bounded).await);
    let path = index_path(root.path());
    let original = std::fs::read(&path).unwrap();
    let original_spans = spans(root.path());

    // Bounded mode is unchanged: a transient scan failure fails the command and
    // leaves the index bytes untouched.
    fixture
        .scan_faults
        .lock()
        .unwrap()
        .push_back(ScanFault::Unavailable);
    let resume = ["--partition", "hour", "--resume", "--stop-block", "18"];
    let out = run(root.path(), &server.url, &resume).await;
    assert!(!out.status.success(), "bounded run ignored a scan failure");
    assert!(
        stderr(&out).contains("injected scan failure"),
        "{}",
        stderr(&out)
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);

    fixture.requests.lock().unwrap().clear();
    fixture
        .scan_faults
        .lock()
        .unwrap()
        .extend([ScanFault::Stall, ScanFault::Unavailable]);
    let live = ["--partition", "hour", "--live", "--poll-interval-secs", "1"];
    live_until_published_then_term(root.path(), &server.url, &live, 21, 30).await;
    let extended = spans(root.path());
    assert_eq!(extended[..original_spans.len()], original_spans[..]);
    let scans_from_frontier = fixture
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.start_block_num == 14)
        .count();
    assert_eq!(scans_from_frontier, 3, "two failed scans then one success");
    assert!(fixture.scan_faults.lock().unwrap().is_empty());

    // Block-range index: every boundary probe attempt fails once in a row (four
    // attempts), which previously ended the live run before its first snapshot.
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::regular();
    fixture.fetch_failures.store(4, Ordering::SeqCst);
    let server = spawn_server(fixture.clone()).await;
    let live = [
        "--partition",
        "block_range",
        "--block-range-size",
        "4",
        "--start-block",
        "8",
        "--live",
        "--poll-interval-secs",
        "1",
    ];
    live_until_published_then_term(root.path(), &server.url, &live, 21, 30).await;
    assert_eq!(fixture.fetch_failures.load(Ordering::SeqCst), 0);
    let ranges = spans(root.path())
        .iter()
        .map(|span| (span.1, span.2))
        .collect::<Vec<_>>();
    assert_eq!(ranges, [(8, 12), (12, 16), (16, 20), (20, 21)]);
}
