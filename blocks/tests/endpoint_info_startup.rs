//! Startup must not derive new destinations when a reachable server lacks
//! Info, and a network's data origin bounds the start that Info resolves.
use firehose_protos::{firehose, hypercore};
use prost::Message;
use std::convert::Infallible;
use std::future::{ready, Ready};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

#[derive(Clone)]
struct MissingInfo(Arc<AtomicUsize>);

impl Service<http::Request<tonic::body::Body>> for MissingInfo {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        assert_eq!(request.uri().path(), "/sf.firehose.v2.EndpointInfo/Info");
        self.0.fetch_add(1, Ordering::SeqCst);
        ready(Ok(http::Response::builder()
            .header("content-type", "application/grpc")
            .header("grpc-status", "12")
            .body(tonic::body::Body::empty())
            .unwrap()))
    }
}

impl tonic::server::NamedService for MissingInfo {
    const NAME: &'static str = "sf.firehose.v2.EndpointInfo";
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_info_never_creates_output_or_changes_an_existing_cursor() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let requests = Arc::new(AtomicUsize::new(0));
    let service = MissingInfo(Arc::clone(&requests));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("existing.parquet");
    let sentinel = b"existing checkpoint must not even be opened on Info failure";
    std::fs::write(&cursor, sentinel).unwrap();
    for (index, arguments) in [
        vec!["build", "--endpoint", &endpoint, "--block-type", "evm"],
        vec!["build", "--network", "mainnet"],
    ]
    .into_iter()
    .enumerate()
    {
        let output = dir.path().join(format!("output-{index}"));
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
        command
            .kill_on_drop(true)
            .env_clear()
            .current_dir(dir.path())
            .env("FIREHOSE_ENDPOINT_MAINNET", &endpoint)
            .args(&arguments)
            .args(["--start-block", "100", "--stop-block", "102", "--output"])
            .arg(&output)
            .arg("--cursor")
            .arg(&cursor);
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), command.output())
            .await
            .expect("startup must fail promptly")
            .unwrap();
        assert!(!result.status.success());
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(stderr.contains("EndpointInfo"), "{stderr}");
        assert!(
            !output.exists(),
            "startup created output despite missing Info"
        );
        assert_eq!(std::fs::read(&cursor).unwrap(), sentinel);
    }
    assert_eq!(
        requests.load(Ordering::SeqCst),
        2,
        "both reached Info after healthcheck"
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// What the HyperCore endpoint advertises, and the data origin fireparq uses.
const FIRST_STREAMABLE: u64 = 846_000_000;
const ORIGIN: u64 = 846_903_317;

/// EndpointInfo of the HyperCore endpoint: `hypercore`, first streamable
/// block 846000000.
#[derive(Clone)]
struct HypercoreInfo;
impl tonic::server::UnaryService<firehose::InfoRequest> for HypercoreInfo {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "hypercore".into(),
                first_streamable_block_num: FIRST_STREAMABLE,
                ..Default::default()
            }))
        })
    }
}

/// Records each Blocks request's start block and serves block 846903317.
#[derive(Clone)]
struct OriginStream(Arc<Mutex<Vec<i64>>>);
impl tonic::server::ServerStreamingService<firehose::Request> for OriginStream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        assert!(request.cursor.is_empty(), "every run here is a new stream");
        self.0.lock().unwrap().push(request.start_block_num);
        Box::pin(async {
            Ok(tonic::Response::new(
                Box::pin(futures::stream::iter([Ok(origin_block())])) as Self::ResponseStream,
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
            type Error = Infallible;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
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
service!(HypercoreInfo, "sf.firehose.v2.EndpointInfo", unary);
service!(OriginStream, "sf.firehose.v2.Stream", server_streaming);

/// A real HyperCore block (a small fixture) with its header rewritten to the
/// origin, 846903317 at 2026-01-01T00:00:00.063Z (the mapper refuses a
/// header that differs from the Firehose identity).
fn origin_block() -> firehose::Response {
    let mut block =
        hypercore::Block::decode(include_bytes!("fixtures/hypercore/1127672017.pb").as_slice())
            .unwrap();
    let time = prost_types::Timestamp {
        seconds: 1_767_225_600,
        nanos: 63_000_000,
    };
    block.block_header = Some(hypercore::BlockHeader {
        block_number: ORIGIN,
        block_time: Some(time),
    });
    firehose::Response {
        block: Some(prost_types::Any {
            type_url: "type.googleapis.com/pinax.hypercore.v1.Block".into(),
            value: block.encode_to_vec(),
        }),
        step: 3,
        cursor: format!("cursor-{ORIGIN}"),
        metadata: Some(firehose::BlockMetadata {
            num: ORIGIN,
            id: ORIGIN.to_string(),
            parent_num: ORIGIN - 1,
            parent_id: (ORIGIN - 1).to_string(),
            lib_num: ORIGIN - 1,
            time: Some(time),
            ..Default::default()
        }),
    }
}

/// `fireparq` with `arguments` against `endpoint` (also `--network
/// hypercore`'s override), bounded to the origin block unless `arguments` set
/// `--stop-block`.
async fn fireparq(cwd: &Path, endpoint: &str, arguments: &[&str]) -> (bool, String) {
    let stop_block = (ORIGIN + 1).to_string();
    let bound: &[&str] = if arguments.contains(&"--stop-block") {
        &[]
    } else {
        &["--stop-block", &stop_block]
    };
    let output = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"))
            .kill_on_drop(true)
            .env_clear()
            .current_dir(cwd)
            .env("FIREHOSE_ENDPOINT_HYPERCORE", endpoint)
            .arg("build")
            .args(arguments)
            .args(bound)
            .output(),
    )
    .await
    .expect("fireparq timed out")
    .unwrap();
    (
        output.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

/// HyperCore data is known from 2026-01-01 (block 846903317), but the
/// endpoint advertises 846000000. Keyed by the EndpointInfo chain name, a new
/// stream starts at the origin by default (dry run and real build), and an
/// explicit earlier start is refused before any Blocks request, leaving the
/// root usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hypercore_data_origin_is_the_default_start_and_earlier_starts_are_refused() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let requests = Arc::new(Mutex::new(Vec::new()));
    let stream = OriginStream(Arc::clone(&requests));
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(HypercoreInfo)
            .add_service(stream)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    let root = cwd.join("hypercore");
    let root_arg = root.to_str().unwrap();
    let take = || std::mem::take(&mut *requests.lock().unwrap());
    let origin = ORIGIN as i64;
    let default_start = "no --start-block: starting at the network's data origin";

    // Default start, dry run, through the alias.
    let (ok, logs) = fireparq(cwd, &endpoint, &["--network", "hypercore", "--dry-run"]).await;
    assert!(ok, "{logs}");
    assert!(logs.contains(default_start), "{logs}");
    assert_eq!(take(), [origin]);

    // Refused before streaming: dry runs and real builds, through the alias
    // or a plain --endpoint (the EndpointInfo chain name is the key).
    for (start, arguments) in [
        ("846903300", vec!["--network", "hypercore", "--dry-run"]),
        ("846000000", vec!["--endpoint", &endpoint, "--dry-run"]),
        (
            "846903316",
            vec!["--network", "hypercore", "--output", root_arg],
        ),
        (
            "846903313",
            vec!["--endpoint", &endpoint, "--output", root_arg],
        ),
    ] {
        let mut arguments = arguments.clone();
        arguments.extend(["--start-block", start]);
        let (ok, logs) = fireparq(cwd, &endpoint, &arguments).await;
        assert!(!ok, "{arguments:?}: {logs}");
        for expected in [
            format!("--start-block {start} is before the data origin of network `hypercore`, block 846903317"),
            "HyperCore data is known from 2026-01-01".to_string(),
            "lacks 846903300-846903312".to_string(),
            "Use --start-block 846903317 or later".to_string(),
        ] {
            assert!(logs.contains(&expected), "{arguments:?}: {expected}: {logs}");
        }
        assert_eq!(
            take(),
            [],
            "{arguments:?}: refused before any Blocks request"
        );
    }
    // A stop block before the origin is checked against the adjusted start,
    // and the error says where that start came from.
    let (ok, logs) = fireparq(
        cwd,
        &endpoint,
        &[
            "--network",
            "hypercore",
            "--dry-run",
            "--stop-block",
            "846903300",
        ],
    )
    .await;
    assert!(!ok, "{logs}");
    assert!(
        logs.contains(
            "--stop-block (846903300) must be greater than the start block (846903317); --stop-block is exclusive (the start block is the data origin of network `hypercore`; --stop-block must be after 846903317)"
        ),
        "{logs}"
    );
    assert_eq!(take(), [], "refused before any Blocks request");

    assert!(
        !root.join("blocks").exists(),
        "a refused build wrote no table"
    );

    // An explicit start at the origin is accepted as given.
    let (ok, logs) = fireparq(
        cwd,
        &endpoint,
        &[
            "--network",
            "hypercore",
            "--dry-run",
            "--start-block",
            "846903317",
        ],
    )
    .await;
    assert!(ok, "{logs}");
    assert!(!logs.contains(default_start), "{logs}");
    assert_eq!(take(), [origin]);

    // A real build into the root the refusals left behind starts at the
    // origin and commits the block.
    let (ok, logs) = fireparq(
        cwd,
        &endpoint,
        &["--network", "hypercore", "--output", root_arg],
    )
    .await;
    assert!(ok, "{logs}");
    assert!(logs.contains(default_start), "{logs}");
    assert_eq!(take(), [origin]);
    assert!(root.join("blocks/_delta_log").is_dir(), "{logs}");

    server.abort();
}
