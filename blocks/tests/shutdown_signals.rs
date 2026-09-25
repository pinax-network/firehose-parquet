//! Real process signals must interrupt endpoint startup without touching output.
#![cfg(unix)]

use std::convert::Infallible;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::codegen::{http, Service};

#[derive(Clone)]
struct HangingInfo(Arc<tokio::sync::Notify>);

impl Service<http::Request<tonic::body::Body>> for HangingInfo {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        assert_eq!(request.uri().path(), "/sf.firehose.v2.EndpointInfo/Info");
        self.0.notify_one();
        Box::pin(std::future::pending())
    }
}

impl tonic::server::NamedService for HangingInfo {
    const NAME: &'static str = "sf.firehose.v2.EndpointInfo";
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_signals_interrupt_info_without_output_or_cursor_changes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let info_called = Arc::new(tokio::sync::Notify::new());
    let service = HangingInfo(Arc::clone(&info_called));
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("existing.parquet");
    let sentinel = b"existing checkpoint must not be opened during cancelled Info";
    std::fs::write(&cursor, sentinel).unwrap();
    for signal in ["-TERM", "-INT"] {
        let output = dir.path().join(format!("output{signal}"));
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"))
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
                "--output",
            ])
            .arg(&output)
            .arg("--cursor")
            .arg(&cursor)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), info_called.notified())
            .await
            .expect("child must reach the pending Info request");
        let status = tokio::process::Command::new("/bin/kill")
            .args([signal, &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap();
        assert!(status.success());
        let result = tokio::time::timeout(Duration::from_secs(2), child.wait_with_output())
            .await
            .expect("the real signal must interrupt the pending Info request promptly")
            .unwrap();
        let logs = format!(
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(result.status.success(), "{signal}: {logs}");
        assert!(logs.contains("shutdown requested during startup"), "{logs}");
        assert!(!output.exists());
        assert_eq!(std::fs::read(&cursor).unwrap(), sentinel);
    }
    server.abort();
    let _ = server.await;
}
