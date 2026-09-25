//! Real AmazonS3 HTTP requests, with only HTTPS/timeout settings adapted for
//! a loopback test server. Production credential, bucket and retry builders run.
use super::*;
use crate::cli::AwsConfig;
use crate::cursor::{CursorLocation, CursorState};
use bytes::Bytes;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Copy)]
enum LostResponse {
    Close,
    Timeout,
    ServerError,
}

#[derive(Default)]
struct Observed {
    requests: Vec<String>,
    body: Vec<u8>,
}

struct Server {
    endpoint: String,
    observed: Arc<Mutex<Observed>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(loss: LostResponse) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let observed = Arc::new(Mutex::new(Observed::default()));
        let state = observed.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut requests = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                requests.spawn(handle(socket, state.clone(), loss));
            }
            // In timeout tests, observe the known server task finish; elapsed
            // time alone is not a provider-quiescence guarantee.
            while let Some(result) = requests.join_next().await {
                result.unwrap();
            }
        });
        Self {
            endpoint,
            observed,
            stop: Some(stop),
            task,
        }
    }

    fn builder(&self, maintenance: bool, mutation: bool) -> AmazonS3Builder {
        let config = Config {
            aws_access_key_id: Some("synthetic-key".into()),
            aws_secret_access_key: Some("synthetic-secret".into()),
            aws_region: Some("us-east-1".into()),
            aws_endpoint_url: Some(self.endpoint.clone()),
            ..Default::default()
        };
        let builder = if maintenance {
            AwsConfig {
                aws_access_key_id: config.aws_access_key_id,
                aws_secret_access_key: config.aws_secret_access_key,
                aws_session_token: None,
                aws_region: config.aws_region,
                aws_endpoint_url: config.aws_endpoint_url,
            }
            .s3_client_builder("mutation-tests", mutation)
            .unwrap()
        } else {
            assert!(mutation);
            mutation_builder(&config, "mutation-tests").unwrap()
        };
        builder.with_client_options(
            object_store::ClientOptions::new()
                .with_allow_http(true)
                .with_timeout(Duration::from_millis(250)),
        )
    }

    async fn finish(mut self) -> Vec<String> {
        self.stop.take().unwrap().send(()).unwrap();
        (&mut self.task).await.unwrap();
        self.observed.lock().unwrap().requests.clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle(
    mut socket: tokio::net::TcpStream,
    observed: Arc<Mutex<Observed>>,
    loss: LostResponse,
) {
    let mut input = Vec::new();
    let header_end = loop {
        let mut buffer = [0_u8; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        if count == 0 {
            return;
        }
        input.extend_from_slice(&buffer[..count]);
        assert!(input.len() < 64 * 1024);
        if let Some(index) = input.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8(input[..header_end].to_vec()).unwrap();
    let method = headers.split_whitespace().next().unwrap().to_string();
    let length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, length)| length.trim().parse::<usize>().unwrap());
    assert!(length < 64 * 1024);
    while input.len() < header_end + length {
        let mut buffer = [0_u8; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        input.extend_from_slice(&buffer[..count]);
    }
    let first = {
        let mut state = observed.lock().unwrap();
        state.requests.push(method.clone());
        if method == "PUT" {
            state.body = input[header_end..header_end + length].to_vec();
        }
        state.requests.len() == 1
    };
    // The server has already accepted the full mutation. Any accidental retry
    // receives success, reproducing the false recovery that must be prevented.
    if first {
        match loss {
            LostResponse::Close => {
                let _ = socket.shutdown().await;
                return;
            }
            LostResponse::Timeout => tokio::time::sleep(Duration::from_millis(750)).await,
            LostResponse::ServerError => {
                let _ = socket.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
                return;
            }
        }
    }
    let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"synthetic-etag\"\r\nLast-Modified: Fri, 25 Sep 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n").await;
}

#[tokio::test]
async fn data_mutation_builders_do_not_retry_accepted_put_or_delete() {
    for maintenance in [false, true] {
        for loss in [
            LostResponse::Close,
            LostResponse::Timeout,
            LostResponse::ServerError,
        ] {
            for method in ["PUT", "DELETE"] {
                let server = Server::start(loss).await;
                let client = server.builder(maintenance, true).build().unwrap();
                let path = object_store::path::Path::from("part.parquet");
                let result = tokio::time::timeout(Duration::from_secs(5), async {
                    if method == "PUT" {
                        client
                            .put(&path, Bytes::from_static(b"synthetic-part").into())
                            .await
                            .map(|_| ())
                    } else {
                        client.delete(&path).await
                    }
                })
                .await
                .unwrap();
                assert!(result.is_err(), "an ambiguous mutation must stop");
                if method == "PUT" {
                    assert_eq!(server.observed.lock().unwrap().body, b"synthetic-part");
                }
                assert_eq!(server.finish().await, [method]);
            }
        }
    }
}

#[tokio::test]
async fn cursor_save_never_retries_ambiguous_s3_publication() {
    for maintenance in [false, true] {
        for loss in [
            LostResponse::Close,
            LostResponse::Timeout,
            LostResponse::ServerError,
        ] {
            let server = Server::start(loss).await;
            let location = CursorLocation::S3 {
                client: Arc::new(server.builder(maintenance, true).build().unwrap()),
                key: "cursor.parquet".into(),
            };
            let (_, metrics) = crate::metrics::init();
            metrics.cursor_last_block_num.set(99);
            metrics.cursor_last_success_timestamp_seconds.set(123);
            let state = CursorState {
                cursor: "synthetic-cursor".into(),
                last_block_num: 100,
                ..Default::default()
            };
            let error = tokio::time::timeout(
                Duration::from_secs(5),
                location.save_with_retry(&state, &metrics, &AtomicBool::new(false)),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert!(error.to_string().contains("failed after 1 attempt"));
            assert_eq!(metrics.cursor_save_failures_total.get(), 1);
            assert_eq!(metrics.cursor_saves_total.get(), 0);
            assert_eq!(metrics.cursor_last_block_num.get(), 99);
            assert_eq!(metrics.cursor_last_success_timestamp_seconds.get(), 123);
            assert_eq!(
                metrics
                    .errors_total
                    .get_or_create(&crate::metrics::ErrorLabels {
                        kind: "cursor_save".into()
                    })
                    .get(),
                1
            );
            let published = server.observed.lock().unwrap().body.clone();
            assert_eq!(
                crate::cursor::parse_cursor(Bytes::from(published))
                    .unwrap()
                    .unwrap()
                    .last_block_num,
                100
            );
            assert_eq!(server.finish().await, ["PUT"]);
        }
    }
}

#[tokio::test]
async fn read_builder_retains_read_retry_policy() {
    let server = Server::start(LostResponse::Close).await;
    let client = server.builder(true, false).build().unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        client.get(&object_store::path::Path::from("part.parquet")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(server.finish().await, ["GET", "GET"]);
}
