//! Exercise reversible CLI selection, append-only events and the bounded warning.
use arrow::array::{StringArray, UInt64Array};
use firehose_parquet::{cursor::load_cursor_parquet, writer::read_parquet};
use firehose_protos::{eth, firehose};
use prost::Message;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tonic::codegen::{http, BoxFuture, Service};

#[derive(Clone)]
struct Info;
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "nonfinal-test".into(),
                first_streamable_block_num: 100,
                ..Default::default()
            }))
        })
    }
}

#[derive(Clone)]
struct Stream {
    final_only: bool,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        assert_eq!(request.final_blocks_only, self.final_only);
        assert_eq!(
            (request.start_block_num, request.stop_block_num),
            (100, 101)
        );
        assert!(request.cursor.is_empty());
        assert_eq!(
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            0
        );
        let events = if self.final_only {
            vec![(100, 0xaa, 3), (101, 0xcc, 3)]
        } else {
            // Two histories can have equal unordered event sets but different
            // current state. Preserve recurrence rather than collapsing identity.
            vec![
                (100, 0xaa, 1),
                (100, 0xaa, 2),
                (100, 0xbb, 1),
                (100, 0xbb, 2),
                (100, 0xaa, 1),
                (101, 0xcc, 1),
            ]
        };
        let lib_num = if self.final_only { 101 } else { 99 };
        let responses: Vec<_> = events
            .into_iter()
            .enumerate()
            .map(|(event, (number, hash, step))| {
                Ok(firehose::Response {
                    block: Some(prost_types::Any {
                        type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".into(),
                        value: eth::Block {
                            number,
                            hash: vec![hash; 32],
                            ..Default::default()
                        }
                        .encode_to_vec(),
                    }),
                    step,
                    cursor: format!("event-{event}"),
                    metadata: Some(firehose::BlockMetadata {
                        num: number,
                        id: format!("{hash:02x}").repeat(32),
                        parent_num: number - 1,
                        parent_id: "11".repeat(32),
                        lib_num,
                        time: Some(prost_types::Timestamp {
                            seconds: 1_700_000_000 + number as i64,
                            nanos: 0,
                        }),
                        ..Default::default()
                    }),
                })
            })
            .collect();
        Box::pin(async move {
            Ok(tonic::Response::new(
                Box::pin(futures::stream::iter(responses)) as Self::ResponseStream,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_false_reaches_rpc_preserves_recurrence_and_warns_only_non_final() {
    for final_only in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stream = Stream {
            final_only,
            calls: calls.clone(),
        };
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(Info)
                .add_service(stream)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output");
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
        child
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
                "--flush-blocks",
                "2",
                "--output",
            ])
            .arg(&output)
            .arg(format!("--final-blocks-only={final_only}"));
        let result = tokio::time::timeout(Duration::from_secs(15), child.output())
            .await
            .unwrap()
            .unwrap();
        let log = format!(
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(result.status.success(), "{log}");
        assert_eq!(
            log.contains("does not prove its tail is final"),
            !final_only,
            "{log}"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let root = output.join("nonfinal-test");
        let mut values = BTreeMap::new();
        for entry in std::fs::read_dir(root.join("blocks")).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|ext| ext != "parquet") {
                continue;
            }
            for batch in read_parquet(&path).unwrap() {
                let numbers = batch
                    .column_by_name("block_num")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let ids = batch
                    .column_by_name("block_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let steps = batch.column_by_name("fork_step");
                assert_eq!(steps.is_none(), final_only);
                for row in 0..batch.num_rows() {
                    let step = steps
                        .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap().value(row))
                        .unwrap_or("FINAL");
                    *values
                        .entry((
                            numbers.value(row),
                            ids.value(row).to_string(),
                            step.to_string(),
                        ))
                        .or_insert(0) += 1;
                }
            }
        }
        assert_eq!(
            values.values().sum::<usize>(),
            if final_only { 2 } else { 6 }
        );
        if !final_only {
            assert_eq!(
                values.get(&(100, format!("0x{}", "aa".repeat(32)), "NEW".into())),
                Some(&2)
            );
            assert_eq!(values.values().filter(|&&count| count == 1).count(), 4);
        }
        let cursor = load_cursor_parquet(&root.join("cursor.parquet"))
            .unwrap()
            .unwrap();
        assert_eq!(cursor.last_block_num, 101);
        assert_eq!(cursor.final_blocks_only, final_only);
        server.abort();
    }
}
