//! Exercise metrics through the real CLI, mapper, writer, cursor and gRPC stream.
use firehose_parquet::cursor::load_cursor_parquet;
use firehose_protos::{firehose, sf::ethereum::r#type::v2 as eth};
use prost::Message;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::codegen::{http, BoxFuture, Service};

#[derive(Clone)]
struct Info;
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "metrics-test".into(),
                first_streamable_block_num: 99,
                ..Default::default()
            }))
        })
    }
}

#[derive(Clone)]
struct Stream {
    receiver: Arc<Mutex<Option<tokio::sync::mpsc::Receiver<firehose::Response>>>>,
    calls: Arc<AtomicUsize>,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        assert_eq!(request.start_block_num, 99);
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            // Seed an actual protected authority through a completed CLI run;
            // a hand-written compatibility cursor is no longer a resume authority.
            assert!(request.cursor.is_empty());
            assert_eq!(request.stop_block_num, 99);
            return Box::pin(async {
                Ok(tonic::Response::new(
                    Box::pin(futures::stream::iter([Ok(response(99))])) as Self::ResponseStream,
                ))
            });
        }
        assert_eq!(call, 1, "unexpected reconnect");
        assert_eq!(request.stop_block_num, 101);
        assert_eq!(request.cursor, "resume-99");
        let receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .expect("unexpected reconnect");
        Box::pin(async move {
            let stream = futures::stream::unfold(receiver, |mut receiver| async {
                receiver
                    .recv()
                    .await
                    .map(|response| (Ok(response), receiver))
            });
            Ok(tonic::Response::new(
                Box::pin(stream) as Self::ResponseStream
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

async fn get(port: u16, path: &str) -> std::io::Result<String> {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
        .await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response)
}

async fn wait_for_metric(port: u16, expected: &str) -> String {
    wait_for_metrics(port, &[expected]).await
}

async fn wait_for_metrics(port: u16, expected: &[&str]) -> String {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = get(port, "/metrics").await {
                if expected.iter().all(|metric| response.contains(metric)) {
                    return response;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("metrics did not reach {expected:?}"))
}

fn response(number: u64) -> firehose::Response {
    firehose::Response {
        block: Some(prost_types::Any {
            type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".into(),
            value: eth::Block {
                number,
                ..Default::default()
            }
            .encode_to_vec(),
        }),
        step: 3,
        cursor: format!("resume-{number}"),
        metadata: Some(firehose::BlockMetadata {
            num: number,
            id: format!("{number:064x}"),
            parent_num: number - 1,
            parent_id: format!("{:064x}", number - 1),
            time: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Default::default()
        }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_cli_reports_freshness_actual_buffers_and_saved_cursor() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let (send, receive) = tokio::sync::mpsc::channel(4);
    let calls = Arc::new(AtomicUsize::new(0));
    let service = Stream {
        receiver: Arc::new(Mutex::new(Some(receive))),
        calls: calls.clone(),
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Info)
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("cursor.parquet");
    let seed = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"))
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
                "99",
                "--stop-block",
                "100",
                "--partition",
                "none",
                "--flush-blocks",
                "2",
                "--flush-bytes",
                "1000000000",
                "--flush-interval-secs",
                "1000000000",
                "--output",
            ])
            .arg(dir.path().join("output"))
            .arg("--cursor")
            .arg(&cursor)
            .arg("--cursor-template")
            .arg(&cursor)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        seed.status.success(),
        "{}{}",
        String::from_utf8_lossy(&seed.stdout),
        String::from_utf8_lossy(&seed.stderr)
    );
    assert_eq!(
        load_cursor_parquet(&cursor)
            .unwrap()
            .unwrap()
            .last_block_num,
        99
    );
    let metrics_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = metrics_listener.local_addr().unwrap().port();
    drop(metrics_listener);
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
            "99",
            "--stop-block",
            "102",
            "--partition",
            "none",
            "--flush-blocks",
            "2",
            "--flush-bytes",
            "1000000000",
            "--flush-interval-secs",
            "1000000000",
            "--metrics-stale-after-secs",
            "1",
            "--stream-idle-timeout-secs",
            "0",
            "--metrics-port",
            &port.to_string(),
            "--output",
        ])
        .arg(dir.path().join("output"))
        .arg("--cursor")
        .arg(&cursor)
        .arg("--cursor-template")
        .arg(&cursor)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    let initial = wait_for_metric(port, "firehose_parquet_cursor_last_block_num 99\n").await;
    assert!(initial.contains("firehose_parquet_cursor_saves_total 0\n"));
    assert!(get(port, "/ready")
        .await
        .unwrap()
        .starts_with("HTTP/1.1 503"));
    send.send(response(100)).await.unwrap();
    let buffered = wait_for_metric(port, "firehose_parquet_mapper_buffer_rows 1\n").await;
    assert!(buffered.contains("firehose_parquet_buffer_estimated_bytes 0\n"));
    assert!(buffered.contains("firehose_parquet_cursor_last_block_num 99\n"));
    assert!(get(port, "/ready")
        .await
        .unwrap()
        .starts_with("HTTP/1.1 200"));
    // A historical block's age does not make an actively receiving backfill unready.
    assert!(buffered.contains("firehose_parquet_last_block_timestamp_seconds 1700000000"));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(get(port, "/ready")
        .await
        .unwrap()
        .starts_with("HTTP/1.1 503"));
    assert!(get(port, "/health")
        .await
        .unwrap()
        .starts_with("HTTP/1.1 200"));
    send.send(response(101)).await.unwrap();
    // Saving the mirror and releasing flush buffers update separate gauges.
    // Require the stable post-flush state instead of assuming one atomic scrape.
    let flushed = wait_for_metrics(
        port,
        &[
            "firehose_parquet_cursor_last_block_num 101\n",
            "firehose_parquet_mapper_buffer_rows 0\n",
            "firehose_parquet_buffer_estimated_bytes 0\n",
            "firehose_parquet_files_written_total{table=\"blocks\"} 1\n",
        ],
    )
    .await;
    assert!(flushed.contains("firehose_parquet_mapper_buffer_rows 0\n"));
    assert!(flushed.contains("firehose_parquet_buffer_estimated_bytes 0\n"));
    assert!(flushed.contains("firehose_parquet_files_written_total{table=\"blocks\"} 1\n"));
    assert!(!flushed.contains("_total_total"));
    assert_eq!(
        load_cursor_parquet(&cursor)
            .unwrap()
            .unwrap()
            .last_block_num,
        101
    );
    drop(send);
    let result = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.abort();
}
