//! The real CLI publishes only exact finalized snapshots and preserves previous
//! index/cursor bytes when probes, ancestry or legacy metadata are invalid.
use firehose_parquet::cli::{
    read_verified_partitions_index, write_partitions_index, PartitionBuildRow,
};
use firehose_protos::firehose;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

const A: i64 = 1_700_000_000;
const B: i64 = A + 3_600;
#[derive(Clone)]
struct Fixture {
    blocks: Arc<BTreeMap<u64, firehose::BlockMetadata>>,
    requests: Arc<Mutex<Vec<firehose::Request>>>,
    finalized: u64,
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
impl Fixture {
    fn regular() -> Self {
        Self {
            blocks: Arc::new(
                (8..=20)
                    .map(|n| (n, metadata(n, n - 1, if n % 2 == 0 { A } else { B })))
                    .collect(),
            ),
            requests: Arc::new(Mutex::new(Vec::new())),
            finalized: 20,
        }
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
            Ok(tonic::Response::new(
                futures::stream::iter(blocks.into_iter().map(Ok)).boxed(),
            ))
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
        Box::pin(async move {
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
        let mut child = command(root.path(), &server.url, &args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let path = index_path(root.path());
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if read_verified_partitions_index(path.to_str().unwrap(), None).is_ok() {
                    break;
                }
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("live command exited early: {status}");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
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
        let index = read_verified_partitions_index(path.to_str().unwrap(), None).unwrap();
        assert_eq!(index.coverage.stop_block, 21);
        assert!(!index.spans.last().unwrap().proof.end_complete);
        assert!(!index.spans.last().unwrap().proof.complete());
    }
}
