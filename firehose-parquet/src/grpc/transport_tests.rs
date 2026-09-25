//! Local protocol checks and an opt-in, bounded receive-throughput benchmark.
use super::*;
use futures::StreamExt;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::net::{TcpListener, TcpStream};
use tonic::codec::CompressionEncoding;

#[derive(Clone)]
struct Fixture {
    payload: Arc<Vec<u8>>,
    compression: Option<CompressionEncoding>,
    calls: Arc<Mutex<Vec<String>>>,
}
impl Fixture {
    fn response(&self, num: u64) -> firehose::Response {
        firehose::Response {
            block: Some(prost_types::Any {
                type_url: "test.Block".into(),
                value: self.payload.as_ref().clone(),
            }),
            step: 3,
            cursor: format!("cursor-{num}"),
            metadata: Some(firehose::BlockMetadata {
                num,
                id: format!("block-{num}"),
                parent_num: num.saturating_sub(1),
                parent_id: format!("block-{}", num.saturating_sub(1)),
                lib_num: 8,
                time: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                ..Default::default()
            }),
        }
    }
}
#[derive(Clone)]
struct StreamService(Fixture);
impl tonic::server::ServerStreamingService<firehose::Request> for StreamService {
    type Response = firehose::Response;
    type ResponseStream =
        futures::stream::BoxStream<'static, Result<Self::Response, tonic::Status>>;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let fixture = self.0.clone();
        let request = request.into_inner();
        Box::pin(async move {
            let (start, stop) = if request.start_block_num < 0 {
                (10, 10)
            } else {
                (request.start_block_num as u64, request.stop_block_num)
            };
            assert!(
                stop >= start && stop - start < 128,
                "bounded fixture requests only"
            );
            Ok(tonic::Response::new(
                futures::stream::iter(start..=stop)
                    .map(move |num| Ok(fixture.response(num)))
                    .boxed(),
            ))
        })
    }
}
#[derive(Clone)]
struct FetchService(Fixture);
impl tonic::server::UnaryService<firehose::SingleBlockRequest> for FetchService {
    type Response = firehose::SingleBlockResponse;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::SingleBlockRequest>) -> Self::Future {
        let response = self.0.response(100);
        Box::pin(async move {
            Ok(tonic::Response::new(firehose::SingleBlockResponse {
                block: response.block,
                metadata: response.metadata,
            }))
        })
    }
}
#[derive(Clone)]
struct InfoService(Fixture);
impl tonic::server::UnaryService<firehose::InfoRequest> for InfoService {
    type Response = firehose::InfoResponse;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        let size = self.0.payload.len();
        Box::pin(async move {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "x".repeat(size),
                ..Default::default()
            }))
        })
    }
}
macro_rules! transport_service {
    ($name:ident, $path:literal, $method:ident) => {
        impl tonic::server::NamedService for $name {
            const NAME: &'static str = $path;
        }
        impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for $name {
            type Response = tonic::codegen::http::Response<tonic::body::Body>;
            type Error = std::convert::Infallible;
            type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(
                &mut self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn call(
                &mut self,
                mut request: tonic::codegen::http::Request<tonic::body::Body>,
            ) -> Self::Future {
                let service = self.clone();
                let accepted = request
                    .headers()
                    .get("grpc-accept-encoding")
                    .unwrap()
                    .to_str()
                    .unwrap();
                assert!(accepted.split(',').any(|value| value == "gzip"));
                assert!(accepted.split(',').any(|value| value == "zstd"));
                assert!(
                    !request.headers().contains_key("grpc-encoding"),
                    "request compression unchanged"
                );
                service
                    .0
                    .calls
                    .lock()
                    .unwrap()
                    .push(request.uri().path().into());
                // tonic 0.14.5's server selects the first compiled codec in the
                // accepted list even if another codec alone was enabled. Narrow
                // only this fixture's copy after checking the actual wire header
                // to emulate a server offering exactly the requested algorithm.
                if let Some(encoding) = service.0.compression {
                    request.headers_mut().insert(
                        "grpc-accept-encoding",
                        encoding.to_string().parse().unwrap(),
                    );
                }
                Box::pin(async move {
                    let encoding = service.0.compression;
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    if let Some(encoding) = encoding {
                        grpc = grpc.send_compressed(encoding);
                    }
                    let response = grpc.$method(service, request).await;
                    let actual = response
                        .headers()
                        .get("grpc-encoding")
                        .map(|value| value.to_str().unwrap());
                    assert_eq!(
                        actual,
                        encoding.map(|encoding| match encoding {
                            CompressionEncoding::Gzip => "gzip",
                            CompressionEncoding::Zstd => "zstd",
                            _ => unreachable!(),
                        })
                    );
                    Ok(response)
                })
            }
        }
    };
}
transport_service!(StreamService, "sf.firehose.v2.Stream", server_streaming);
transport_service!(FetchService, "sf.firehose.v2.Fetch", unary);
transport_service!(InfoService, "sf.firehose.v2.EndpointInfo", unary);

struct Server {
    endpoint: String,
    calls: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new(bytes: usize, compression: Option<CompressionEncoding>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let fixture = Fixture {
            payload: Arc::new(vec![7; bytes]),
            compression,
            calls: calls.clone(),
        };
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(StreamService(fixture.clone()))
                .add_service(FetchService(fixture.clone()))
                .add_service(InfoService(fixture))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        Self {
            endpoint,
            calls,
            task,
        }
    }
    fn client(&self, max_message_bytes: u32) -> FirehoseClient {
        FirehoseClient::new(Config {
            endpoint: self.endpoint.clone(),
            start_block: Some(100),
            stop_block: Some(101),
            grpc: crate::config::GrpcConfig {
                max_message_bytes,
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap()
    }
}

#[tokio::test]
async fn all_rpc_paths_accept_plain_gzip_zstd_and_preserve_payloads() {
    for encoding in [
        None,
        Some(CompressionEncoding::Gzip),
        Some(CompressionEncoding::Zstd),
    ] {
        let server = Server::new(4096, encoding).await;
        let client = server.client(8192);
        assert_eq!(client.info().await.unwrap().chain_name.len(), 4096);
        assert_eq!(
            client
                .fetch_block_identity(100, None)
                .await
                .unwrap()
                .unwrap()
                .block_num,
            100
        );
        let shutdown = CancellationToken::new();
        let mut seen = 0;
        client
            .stream_blocks(
                None,
                &shutdown,
                |payload, type_url, cursor, identity, step| {
                    assert_eq!(payload, vec![7; 4096]);
                    assert_eq!(cursor, "cursor-100");
                    assert_eq!(type_url, "test.Block");
                    assert_eq!(identity.block_num, 100);
                    assert_eq!(step, 3);
                    seen += 1;
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(seen, 1);
        assert_eq!(
            client
                .finalized_anchor(Duration::from_secs(2), &shutdown)
                .await
                .unwrap()
                .block_num,
            8
        );
        let mut traversal = client
            .finalized_metadata_stream(100, 100, Duration::from_secs(2), &shutdown)
            .await
            .unwrap();
        assert_eq!(traversal.next().await.unwrap().unwrap().block_num, 100);
        assert!(traversal.next().await.unwrap().is_none());
        assert_eq!(server.calls.lock().unwrap().len(), 6);
    }
}

#[tokio::test]
async fn limits_reject_large_plain_or_decompressed_responses_on_every_rpc_path() {
    for encoding in [
        None,
        Some(CompressionEncoding::Gzip),
        Some(CompressionEncoding::Zstd),
    ] {
        let server = Server::new(4096, encoding).await;
        let client = server.client(1024);
        let shutdown = CancellationToken::new();
        let mut errors = Vec::new();
        errors.push(client.info().await.unwrap_err());
        errors.push(client.fetch_block_identity(100, None).await.unwrap_err());
        errors.push(
            tokio::time::timeout(
                Duration::from_secs(2),
                client.stream_blocks(None, &shutdown, |_, _, _, _, _| {
                    panic!("oversized payload reached callback")
                }),
            )
            .await
            .unwrap()
            .unwrap_err(),
        );
        errors.push(
            client
                .finalized_anchor(Duration::from_secs(2), &shutdown)
                .await
                .unwrap_err(),
        );
        match client
            .finalized_metadata_stream(100, 100, Duration::from_secs(2), &shutdown)
            .await
        {
            Ok(mut stream) => errors.push(stream.next().await.unwrap_err()),
            Err(error) => errors.push(error),
        }
        for error in errors {
            assert!(format!("{error:#}").contains("1024"), "{error:#}");
        }
        assert_eq!(
            server.calls.lock().unwrap().len(),
            5,
            "oversized messages must not retry"
        );
    }
}

#[test]
fn transport_validation_and_local_limit_classification_do_not_change_quota_policy() {
    let mut config = Config::default();
    config.grpc.max_message_bytes = 0;
    assert!(FirehoseClient::new(config)
        .err()
        .unwrap()
        .to_string()
        .contains("greater than zero"));
    for bytes in [0, i32::MAX as u32 + 1] {
        let mut config = Config::default();
        config.grpc.initial_window_bytes = Some(bytes);
        assert!(FirehoseClient::new(config)
            .err()
            .unwrap()
            .to_string()
            .contains("initial_window_bytes"));
    }
    for message in [
        "temporarily exhausted",
        "Error decompressing: other failure",
        "size limit exceeded",
    ] {
        let status = tonic::Status::resource_exhausted(message);
        assert!(fatal_status_error(&status).is_none());
        assert_eq!(
            classify_fetch_error(&status.into()),
            FetchErrorKind::Transient
        );
    }
    let status = tonic::Status::resource_exhausted(
        "Error decompressing: size limit, of 1024 bytes, exceeded while decompressing message",
    );
    assert!(fatal_status_error(&status)
        .unwrap()
        .to_string()
        .contains("grpc-max-message-bytes=1024"));
    assert_eq!(classify_fetch_error(&status.into()), FetchErrorKind::Fatal);
    assert!(fatal_status_error(&tonic::Status::resource_exhausted(
        "billable egress bytes quota exceeded"
    ))
    .is_some());
}

// Fixed propagation delay: reading and scheduling run independently of writing.
// Unlike sleeping after every chunk, this does not impose an artificial bandwidth
// cap. Queue capacity is 128 MiB/direction, above the entire 64 MiB test stream.
async fn forward(
    reader: tokio::net::tcp::OwnedReadHalf,
    writer: tokio::net::tcp::OwnedWriteHalf,
    delay: Duration,
    bytes: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2048);
    let read = async move {
        let mut reader = reader;
        loop {
            let mut data = vec![0; 64 * 1024];
            let len = reader.read(&mut data).await?;
            if len == 0 {
                break;
            }
            data.truncate(len);
            // There is only one sender, so available capacity cannot shrink
            // before send(). Fail the benchmark if this relay would throttle.
            assert!(
                sender.capacity() > 0,
                "relay queue filled; timing is invalid"
            );
            if sender
                .send((tokio::time::Instant::now() + delay, data))
                .await
                .is_err()
            {
                break;
            }
        }
        Ok::<_, std::io::Error>(())
    };
    let write = async move {
        let mut writer = writer;
        while let Some((ready, data)) = receiver.recv().await {
            tokio::time::sleep_until(ready).await;
            writer.write_all(&data).await?;
            bytes.fetch_add(data.len(), Ordering::Relaxed);
        }
        writer.shutdown().await
    };
    tokio::try_join!(read, write)?;
    Ok(())
}

struct Relay {
    endpoint: String,
    bytes: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Relay {
    async fn new(upstream: &str, delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = upstream.strip_prefix("http://").unwrap().to_string();
        let bytes = Arc::new(AtomicUsize::new(0));
        let count = bytes.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (incoming, _) = listener.accept().await.unwrap();
                let outgoing = TcpStream::connect(&upstream).await.unwrap();
                incoming.set_nodelay(true).unwrap();
                outgoing.set_nodelay(true).unwrap();
                let count = count.clone();
                connections.spawn(async move {
                    let (ir, iw) = incoming.into_split();
                    let (or, ow) = outgoing.into_split();
                    let _ = tokio::try_join!(
                        forward(ir, ow, delay, Arc::new(AtomicUsize::new(0))),
                        forward(or, iw, delay, count)
                    );
                });
                while connections.try_join_next().is_some() {}
            }
        });
        Self {
            endpoint,
            bytes,
            task,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bounded loopback throughput benchmark; run serially in release mode"]
async fn benchmark_receive_windows() {
    for one_way_ms in [0, 25] {
        for repetition in 0..3 {
            let mut order = [
                ("library-fixed", false, None),
                ("adaptive", true, None),
                ("fixed-16mib", false, Some(16 * 1024 * 1024)),
            ];
            order.rotate_left(repetition);
            for (mode, adaptive, initial_window_bytes) in order {
                let server = Server::new(1024 * 1024, None).await;
                let relay = Relay::new(&server.endpoint, Duration::from_millis(one_way_ms)).await;
                let client = FirehoseClient::new(Config {
                    endpoint: relay.endpoint.clone(),
                    start_block: Some(0),
                    stop_block: Some(64),
                    grpc: crate::config::GrpcConfig {
                        adaptive_window: adaptive,
                        initial_window_bytes,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .unwrap();
                let mut count = 0usize;
                let start = Instant::now();
                tokio::time::timeout(
                    Duration::from_secs(30),
                    client.stream_blocks(
                        None,
                        &CancellationToken::new(),
                        |payload, _, _, identity, _| {
                            assert_eq!(identity.block_num as usize, count);
                            assert_eq!(payload.len(), 1024 * 1024);
                            assert_eq!((payload[0], payload[payload.len() - 1]), (7, 7));
                            count += 1;
                            Ok(())
                        },
                    ),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(count, 64);
                println!(
                    "GRPC_BENCH {}",
                    serde_json::json!({
                        "one_way_delay_ms": one_way_ms, "mode": mode, "adaptive": adaptive, "repetition": repetition,
                        "payload_bytes": count * 1024 * 1024, "server_wire_bytes": relay.bytes.load(Ordering::Relaxed),
                        "elapsed_seconds": start.elapsed().as_secs_f64(),
                    })
                );
            }
        }
    }
}
