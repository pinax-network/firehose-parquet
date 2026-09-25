//! Startup must not derive new destinations when a reachable server lacks Info.
use std::convert::Infallible;
use std::future::{ready, Ready};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::codegen::{http, Service};

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
        vec![
            "partitions",
            "build",
            "--endpoint",
            &endpoint,
            "--partition",
            "date",
        ],
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
            .arg(&output);
        if arguments[0] == "build" {
            command.arg("--cursor").arg(&cursor);
        }
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
        3,
        "all three reached Info after healthcheck"
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}
