//! Retained Fetch failures must not become nullable block-range boundaries.
//! Exact time traversal and ancestry failures are covered in partition_coverage.rs.
use firehose_protos::firehose;
use futures::StreamExt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use tonic::codegen::{http, BoxFuture, Service};

#[derive(Clone)]
struct Info;

impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "test-chain".into(),
                first_streamable_block_num: 100,
                ..Default::default()
            }))
        })
    }
}

#[derive(Clone)]
struct Fetch {
    mode: Arc<AtomicU8>,
    requested: Arc<Mutex<Vec<u64>>>,
}

impl tonic::server::UnaryService<firehose::SingleBlockRequest> for Fetch {
    type Response = firehose::SingleBlockResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::SingleBlockRequest>) -> Self::Future {
        let mode = self.mode.load(Ordering::SeqCst);
        let Some(firehose::single_block_request::Reference::BlockNumber(number)) =
            request.into_inner().reference
        else {
            panic!("expected number reference")
        };
        self.requested.lock().unwrap().push(number.num);
        Box::pin(async move {
            match mode {
                1 => Err(tonic::Status::deadline_exceeded("upstream timed out")),
                2 => Err(tonic::Status::internal(
                    "block index not found; storage failure",
                )),
                3 => Err(tonic::Status::unauthenticated(
                    "block access token not found",
                )),
                4 => Ok(tonic::Response::new(
                    firehose::SingleBlockResponse::default(),
                )),
                _ => Ok(tonic::Response::new(firehose::SingleBlockResponse {
                    block: None,
                    metadata: Some(firehose::BlockMetadata {
                        num: number.num + u64::from(mode == 5),
                        id: format!("block-{}", number.num),
                        time: Some(prost_types::Timestamp {
                            seconds: 1_700_000_000,
                            nanos: 0,
                        }),
                        ..Default::default()
                    }),
                })),
            }
        })
    }
}

// The production command now proves finality before its optional boundary Fetches.
// Keep that independent RPC valid so each injected Fetch failure is still exercised.
#[derive(Clone)]
struct Finality;
impl tonic::server::ServerStreamingService<firehose::Request> for Finality {
    type Response = firehose::Response;
    type ResponseStream =
        futures::stream::BoxStream<'static, Result<Self::Response, tonic::Status>>;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        let number = if request.start_block_num == -1 {
            assert!(!request.final_blocks_only);
            200
        } else {
            assert_eq!(
                (request.start_block_num, request.stop_block_num),
                (180, 180)
            );
            assert!(request.final_blocks_only);
            180
        };
        let response = firehose::Response {
            step: if number == 200 { 1 } else { 3 },
            block: Some(prost_types::Any::default()),
            metadata: Some(firehose::BlockMetadata {
                num: number,
                id: format!("block-{number}"),
                lib_num: 180,
                time: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        Box::pin(async move {
            Ok(tonic::Response::new(
                futures::stream::iter([Ok(response)]).boxed(),
            ))
        })
    }
}

macro_rules! rpc_service {
    ($service:ty, $name:literal, $method:ident) => {
        impl tonic::server::NamedService for $service {
            const NAME: &'static str = $name;
        }
        impl Service<http::Request<tonic::body::Body>> for $service {
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
                Box::pin(async {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    Ok(grpc.$method(service, request).await)
                })
            }
        }
    };
}
rpc_service!(Info, "sf.firehose.v2.EndpointInfo", unary);
rpc_service!(Fetch, "sf.firehose.v2.Fetch", unary);
rpc_service!(Finality, "sf.firehose.v2.Stream", server_streaming);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_block_range_probes_cannot_create_or_replace_partition_output() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let mode = Arc::new(AtomicU8::new(0));
    let requested = Arc::new(Mutex::new(Vec::new()));
    let service = Fetch {
        mode: Arc::clone(&mode),
        requested: Arc::clone(&requested),
    };
    let server = tokio::spawn(async {
        tonic::transport::Server::builder()
            .add_service(Info)
            .add_service(Finality)
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let existing = dir.path().join("existing");
    let run = |output: std::path::PathBuf, partition: &'static str| {
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
        child
            .kill_on_drop(true)
            .env_clear()
            .current_dir(dir.path())
            .args([
                "partitions",
                "build",
                "--endpoint",
                &endpoint,
                "--start-block",
                "100",
                "--stop-block",
                "102",
                "--partition",
                partition,
                "--overwrite",
                "--output",
            ])
            .arg(output);
        if partition == "block_range" {
            child.args(["--block-range-size", "2"]);
        }
        async move {
            tokio::time::timeout(std::time::Duration::from_secs(10), child.output())
                .await
                .unwrap()
                .unwrap()
        }
    };
    let seeded = run(existing.clone(), "block_range").await;
    assert!(
        seeded.status.success(),
        "{}",
        String::from_utf8_lossy(&seeded.stderr)
    );
    let index = existing.join("test-chain/partitions.parquet");
    let original = std::fs::read(&index).unwrap();
    for failure_mode in 1..=5 {
        mode.store(failure_mode, Ordering::SeqCst);
        for partition in ["block_range"] {
            for output in [
                dir.path().join(format!("fresh-{failure_mode}-{partition}")),
                existing.clone(),
            ] {
                requested.lock().unwrap().clear();
                let result = run(output.clone(), partition).await;
                assert!(
                    !result.status.success(),
                    "mode {failure_mode}, partition {partition} unexpectedly succeeded"
                );
                let seen = requested.lock().unwrap();
                assert!(!seen.is_empty(), "fixture must reach Fetch");
                assert!(
                    seen.iter().all(|number| *number == 100),
                    "invalid block must not be skipped: {seen:?}"
                );
                assert_eq!(seen.len(), if failure_mode == 3 { 1 } else { 4 });
                if output != existing {
                    assert!(!output.exists(), "invalid probe created output");
                }
                assert_eq!(
                    std::fs::read(&index).unwrap(),
                    original,
                    "invalid probe replaced an existing index"
                );
            }
        }
    }
    server.abort();
}
