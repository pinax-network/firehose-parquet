//! Authentication is attached by the client, including retries and cached channels.
use super::*;
use futures::StreamExt;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::net::TcpListener;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Headers {
    key: Option<String>,
    bearer: Option<String>,
}
#[derive(Clone, Default)]
struct Fixture {
    headers: Arc<Mutex<Vec<(String, Headers)>>>,
    requests: Arc<Mutex<Vec<firehose::Request>>>,
    info_calls: Arc<AtomicUsize>,
    retry: bool,
}
fn response(num: u64) -> firehose::Response {
    firehose::Response {
        block: Some(prost_types::Any {
            type_url: "test.Block".into(),
            value: vec![7],
        }),
        cursor: format!("cursor-{num}"),
        step: 3,
        metadata: Some(firehose::BlockMetadata {
            num,
            id: format!("block-{num}"),
            lib_num: 8,
            ..Default::default()
        }),
    }
}
#[derive(Clone)]
struct Info(Fixture);
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        let fail = self.0.retry && self.0.info_calls.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            if fail {
                return Err(tonic::Status::unavailable("retry Info"));
            }
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "fixture".into(),
                ..Default::default()
            }))
        })
    }
}
#[derive(Clone)]
struct Fetch(Fixture);
impl tonic::server::UnaryService<firehose::SingleBlockRequest> for Fetch {
    type Response = firehose::SingleBlockResponse;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::SingleBlockRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::SingleBlockResponse {
                metadata: response(100).metadata,
                block: None,
            }))
        })
    }
}
#[derive(Clone)]
struct Stream(Fixture);
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream =
        futures::stream::BoxStream<'static, Result<Self::Response, tonic::Status>>;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        let mut requests = self.0.requests.lock().unwrap();
        let attempt = requests.len();
        requests.push(request.clone());
        let retry = self.0.retry;
        Box::pin(async move {
            let messages = if retry {
                match attempt {
                    0 => return Err(tonic::Status::unavailable("retry Blocks headers")),
                    1 => vec![
                        Ok(response(100)),
                        Err(tonic::Status::unavailable("retry Blocks body")),
                    ],
                    2 => vec![Ok(response(101))],
                    _ => panic!("unexpected retry"),
                }
            } else {
                vec![Ok(response(if request.start_block_num < 0 {
                    10
                } else {
                    request.start_block_num as u64
                }))]
            };
            Ok(tonic::Response::new(
                futures::stream::iter(messages).boxed(),
            ))
        })
    }
}
macro_rules! service {
    ($service:ident, $name:literal, $method:ident) => {
        impl tonic::server::NamedService for $service {
            const NAME: &'static str = $name;
        }
        impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>>
            for $service
        {
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
                request: tonic::codegen::http::Request<tonic::body::Body>,
            ) -> Self::Future {
                let value = |name| {
                    request
                        .headers()
                        .get(name)
                        .map(|v| v.to_str().unwrap().to_owned())
                };
                self.0.headers.lock().unwrap().push((
                    request.uri().path().into(),
                    Headers {
                        key: value("x-api-key"),
                        bearer: value("authorization"),
                    },
                ));
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
service!(Fetch, "sf.firehose.v2.Fetch", unary);
service!(Stream, "sf.firehose.v2.Stream", server_streaming);
struct Server {
    endpoint: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(fixture: Fixture) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(s, _)| s), listener))
    });
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Info(fixture.clone()))
            .add_service(Fetch(fixture.clone()))
            .add_service(Stream(fixture))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    Server { endpoint, task }
}

#[tokio::test]
async fn every_rpc_path_automatically_attaches_only_configured_credentials() {
    for (key, token) in [
        (None, None),
        (Some(" key\n"), None),
        (None, Some(" token\r\n")),
        (Some("key"), Some("token")),
        (Some(" \n"), Some("\r\n")),
    ] {
        let fixture = Fixture::default();
        let server = server(fixture.clone()).await;
        let client = FirehoseClient::new(Config {
            endpoint: server.endpoint.clone(),
            api_key: key.map(str::to_owned),
            jwt_token: token.map(str::to_owned),
            start_block: Some(100),
            stop_block: Some(101),
            ..Default::default()
        })
        .unwrap();
        let shutdown = CancellationToken::new();
        client.info().await.unwrap();
        // Repeat Fetch to exercise the cached channel and new intercepted clients.
        for _ in 0..2 {
            assert_eq!(
                client
                    .fetch_block_identity(100, None)
                    .await
                    .unwrap()
                    .unwrap()
                    .block_num,
                100
            );
        }
        client
            .stream_blocks(None, &shutdown, |_, _, _, _, _| Ok(()))
            .await
            .unwrap();
        assert_eq!(
            client
                .finalized_anchor(Duration::from_secs(2), &shutdown)
                .await
                .unwrap()
                .block_num,
            8
        );
        let mut stream = client
            .finalized_metadata_stream(100, 100, Duration::from_secs(2), &shutdown)
            .await
            .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().block_num, 100);
        assert!(stream.next().await.unwrap().is_none());
        let expected = Headers {
            key: key
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
            bearer: token
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| format!("Bearer {s}")),
        };
        let headers = fixture.headers.lock().unwrap();
        assert_eq!(headers.len(), 7);
        for (route, actual) in headers.iter() {
            assert_eq!(actual, &expected, "{route}");
        }
    }
}

#[tokio::test]
async fn retries_preserve_auth_and_resume_cursor_with_exact_reconnect_metrics() {
    let fixture = Fixture {
        retry: true,
        ..Default::default()
    };
    let server = server(fixture.clone()).await;
    let mut client = FirehoseClient::new(Config {
        endpoint: server.endpoint.clone(),
        api_key: Some("key".into()),
        jwt_token: Some("token".into()),
        start_block: Some(100),
        stop_block: Some(102),
        ..Default::default()
    })
    .unwrap();
    let (_, metrics) = crate::metrics::init();
    client.set_metrics(metrics.clone());
    client.info().await.unwrap();
    let mut blocks = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        client.stream_blocks(
            Some("original".into()),
            &CancellationToken::new(),
            |_, _, _, identity, _| {
                blocks.push(identity.block_num);
                Ok(())
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(blocks, vec![100, 101]);
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.cursor.as_str())
            .collect::<Vec<_>>(),
        vec!["original", "original", "cursor-100"]
    );
    for request in requests.iter() {
        assert_eq!(
            (
                request.start_block_num,
                request.stop_block_num,
                request.final_blocks_only
            ),
            (100, 101, true)
        );
    }
    assert_eq!(metrics.grpc_reconnects_total.get(), 2);
    assert_eq!(
        metrics
            .errors_total
            .get_or_create(&crate::metrics::ErrorLabels {
                kind: "grpc_reconnect".into()
            })
            .get(),
        2
    );
    assert_eq!(
        metrics
            .errors_total
            .get_or_create(&crate::metrics::ErrorLabels {
                kind: "grpc_fatal".into()
            })
            .get(),
        0
    );
    let headers = fixture.headers.lock().unwrap();
    assert_eq!(headers.len(), 5); // Two Info calls and three Blocks calls.
    for (_, actual) in headers.iter() {
        assert_eq!(
            actual,
            &Headers {
                key: Some("key".into()),
                bearer: Some("Bearer token".into())
            }
        );
    }
}

#[test]
fn interceptor_preserves_unrelated_metadata_and_extensions() {
    let mut auth = AuthMetadata::from_config(&Config {
        api_key: Some("key".into()),
        jwt_token: Some("token".into()),
        ..Default::default()
    })
    .unwrap();
    let mut request = tonic::Request::new(());
    request
        .metadata_mut()
        .insert("x-request-id", "test-request".parse().unwrap());
    request
        .metadata_mut()
        .insert_bin("trace-bin", MetadataValue::from_bytes(&[0, 255]));
    request.set_timeout(Duration::from_secs(3));
    request.extensions_mut().insert(42_u32);
    request
        .metadata_mut()
        .insert("x-api-key", "old".parse().unwrap());
    let before = request.metadata().get("grpc-timeout").cloned();
    let request = auth.call(request).unwrap();
    assert_eq!(
        request.metadata().get("x-request-id").unwrap(),
        "test-request"
    );
    assert_eq!(
        request
            .metadata()
            .get_bin("trace-bin")
            .unwrap()
            .to_bytes()
            .unwrap()
            .as_ref(),
        &[0, 255]
    );
    assert_eq!(request.metadata().get("grpc-timeout"), before.as_ref());
    assert_eq!(request.extensions().get::<u32>(), Some(&42));
    assert_eq!(request.metadata().get("x-api-key").unwrap(), "key");
}
