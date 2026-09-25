//! Real commands must share ownership before they read checkpoints or publish data.
use firehose_parquet::dataset_lock::LocalOwnership;
use firehose_protos::firehose;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tonic::codegen::{http, BoxFuture, Service};

#[derive(Clone)]
struct Info(Arc<AtomicUsize>);
impl tonic::server::NamedService for Info {
    const NAME: &'static str = "sf.firehose.v2.EndpointInfo";
}
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "test-chain".into(),
                first_streamable_block_num: 100,
                ..Default::default()
            }))
        })
    }
}
impl Service<http::Request<tonic::body::Body>> for Info {
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
            Ok(grpc.unary(service, request).await)
        })
    }
}

async fn run(cwd: &std::path::Path, args: &[&str]) -> std::process::Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    command
        .kill_on_drop(true)
        .env_clear()
        .current_dir(cwd)
        .args(args);
    tokio::time::timeout(std::time::Duration::from_secs(10), command.output())
        .await
        .unwrap()
        .unwrap()
}

fn assert_ownership_conflict(result: std::process::Output) {
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("dataset ownership"),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_partition_and_maintenance_commands_conflict_with_a_descendant_owner() {
    let temp = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let info = Info(calls.clone());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(info)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let root = temp.path().join("output/test-chain");
    let table = root.join("blocks");
    let owner = LocalOwnership::acquire(&[table]).unwrap();
    let cursor = root.join("cursor.parquet");
    std::fs::write(&cursor, b"unchanged-private-checkpoint").unwrap();
    let output = temp.path().join("output");
    let output = output.to_str().unwrap();
    let root_str = root.to_str().unwrap();
    let rolled = temp.path().join("rolled");
    for args in [
        vec![
            "build",
            "--endpoint",
            &endpoint,
            "--start-block",
            "100",
            "--stop-block",
            "102",
            "--output",
            output,
        ],
        vec![
            "partitions",
            "build",
            "--endpoint",
            &endpoint,
            "--start-block",
            "100",
            "--stop-block",
            "102",
            "--partition",
            "block_range",
            "--block-range-size",
            "2",
            "--output",
            output,
        ],
        vec!["merge", root_str],
        vec!["truncate", root_str, "--yes"],
        vec![
            "rollup",
            root_str,
            "--output",
            rolled.to_str().unwrap(),
            "--partition",
            "date",
        ],
    ] {
        assert_ownership_conflict(run(temp.path(), &args).await);
        assert_eq!(
            std::fs::read(&cursor).unwrap(),
            b"unchanged-private-checkpoint"
        );
        assert!(!rolled.exists());
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "build commands must resolve chain identity before acquiring its output root"
    );
    drop(owner);

    // An independently placed cursor participates in the same acquisition. A
    // failed acquisition must not create the new output or read private bytes.
    let external = temp.path().join("external");
    let cursor_owner = LocalOwnership::acquire(&[external.clone()]).unwrap();
    let cursor = external.join("cursor.parquet");
    std::fs::write(&cursor, b"external-private-checkpoint").unwrap();
    let fresh = temp.path().join("fresh");
    assert_ownership_conflict(
        run(
            temp.path(),
            &[
                "build",
                "--endpoint",
                &endpoint,
                "--start-block",
                "100",
                "--stop-block",
                "102",
                "--output",
                fresh.to_str().unwrap(),
                "--cursor",
                cursor.to_str().unwrap(),
            ],
        )
        .await,
    );
    assert!(!fresh.exists());
    assert_eq!(
        std::fs::read(&cursor).unwrap(),
        b"external-private-checkpoint"
    );
    drop(cursor_owner);
    server.abort();
}
