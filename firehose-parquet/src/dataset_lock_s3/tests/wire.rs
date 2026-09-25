//! Exercise the real AmazonS3 adapter against a loopback-only conditional store.
use super::*;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
struct Stored {
    bytes: Vec<u8>,
    etag: String,
    version: String,
}
struct RequestSummary {
    method: String,
    path: String,
    if_match: Option<String>,
    if_none_match: Option<String>,
    cache_control: Option<String>,
    signed: bool,
}
#[derive(Default)]
struct ServerState {
    objects: HashMap<String, Stored>,
    revision: u64,
    ignore_conditions: bool,
    lose_owner_put_responses: usize,
    requests: Vec<RequestSummary>,
}
struct Server {
    endpoint: String,
    state: Arc<Mutex<ServerState>>,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(ServerState::default()));
        let server_state = state.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                handle(stream, server_state.clone()).await;
            }
        });
        Self {
            endpoint,
            state,
            stop,
            task,
        }
    }
    fn client(&self) -> Arc<dyn ObjectStore> {
        Arc::new(
            AmazonS3Builder::new()
                .with_bucket_name("owner-tests")
                .with_region("us-east-1")
                .with_access_key_id("synthetic-key")
                .with_secret_access_key("synthetic-secret")
                .with_endpoint(&self.endpoint)
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false)
                .with_conditional_put(S3ConditionalPut::ETagMatch)
                .with_retry(object_store::RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
                .build()
                .unwrap(),
        )
    }
    async fn shutdown(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap();
    }
}

async fn handle(mut stream: tokio::net::TcpStream, state: Arc<Mutex<ServerState>>) {
    let mut input = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).await.unwrap();
        if read == 0 {
            return;
        }
        input.extend_from_slice(&chunk[..read]);
        assert!(input.len() < 128 * 1024);
        if let Some(index) = input.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8(input[..header_end].to_vec()).unwrap();
    let mut lines = headers.split("\r\n");
    let request = lines.next().unwrap().split_whitespace().collect::<Vec<_>>();
    let method = request[0].to_string();
    let path = request[1].split('?').next().unwrap().to_string();
    let headers: HashMap<String, String> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    if headers.contains_key("expect") {
        stream
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .await
            .unwrap();
    }
    let length = headers
        .get("content-length")
        .map_or(0, |value| value.parse::<usize>().unwrap());
    assert!(length < 128 * 1024);
    while input.len() < header_end + length {
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0);
        input.extend_from_slice(&chunk[..read]);
    }
    let payload = input[header_end..header_end + length].to_vec();
    let (status, body, stored, lose_response) = {
        let mut state = state.lock().unwrap();
        state.requests.push(RequestSummary {
            method: method.clone(),
            path: path.clone(),
            if_match: headers.get("if-match").cloned(),
            if_none_match: headers.get("if-none-match").cloned(),
            cache_control: headers.get("cache-control").cloned(),
            signed: headers
                .get("authorization")
                .is_some_and(|value| value.starts_with("AWS4-HMAC-SHA256 ")),
        });
        match method.as_str() {
            "GET" => match state.objects.get(&path).cloned() {
                Some(object) => (200, object.bytes.clone(), Some(object), false),
                None => (404, xml_error("NoSuchKey"), None, false),
            },
            "PUT" => {
                let exists = state.objects.get(&path);
                let denied = !state.ignore_conditions
                    && (headers
                        .get("if-none-match")
                        .is_some_and(|value| value == "*" && exists.is_some())
                        || headers.get("if-match").is_some_and(|value| {
                            exists.is_none_or(|object| &object.etag != value)
                        }));
                if denied {
                    (412, xml_error("PreconditionFailed"), None, false)
                } else {
                    state.revision += 1;
                    let object = Stored {
                        bytes: payload,
                        etag: format!("\"etag-{}\"", state.revision),
                        version: format!("version-{}", state.revision),
                    };
                    state.objects.insert(path.clone(), object.clone());
                    let lose = path == format!("/owner-tests/{OWNER_KEY}")
                        && state.lose_owner_put_responses > 0;
                    if lose {
                        state.lose_owner_put_responses -= 1;
                    }
                    (200, Vec::new(), Some(object), lose)
                }
            }
            "DELETE" => {
                assert_ne!(
                    path,
                    format!("/owner-tests/{OWNER_KEY}"),
                    "owner key must never be deleted"
                );
                state.objects.remove(&path);
                (204, Vec::new(), None, false)
            }
            _ => panic!("unexpected HTTP method"),
        }
    };
    if lose_response {
        // Remote publication succeeded, but the client never receives its result.
        stream.shutdown().await.unwrap();
        return;
    }
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        404 => "Not Found",
        412 => "Precondition Failed",
        _ => unreachable!(),
    };
    let mut response = format!("HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\nLast-Modified: Fri, 25 Sep 2026 00:00:00 GMT\r\nContent-Type: application/xml\r\n", body.len());
    if let Some(object) = stored {
        response.push_str(&format!(
            "ETag: {}\r\nx-amz-version-id: {}\r\n",
            object.etag, object.version
        ));
    }
    response.push_str("\r\n");
    stream.write_all(response.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    stream.shutdown().await.unwrap();
}
fn xml_error(code: &str) -> Vec<u8> {
    format!("<Error><Code>{code}</Code><Message>synthetic failure</Message></Error>").into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_s3_headers_and_lost_responses_preserve_exact_ownership() {
    let server = Server::start().await;
    let store = server.client();
    server.state.lock().unwrap().lose_owner_put_responses = 1;
    let guard = acquire(&store).await;
    let first_id = guard.record().owner_id().to_string();
    server.state.lock().unwrap().lose_owner_put_responses = 1;
    guard.release().await.unwrap();
    let next = acquire(&store).await;
    assert_eq!(next.record().generation(), 2);
    assert_ne!(next.record().owner_id(), first_id);
    next.release().await.unwrap();
    {
        let state = server.state.lock().unwrap();
        let writes = state
            .requests
            .iter()
            .filter(|request| request.method == "PUT")
            .collect::<Vec<_>>();
        assert!(!writes.is_empty());
        assert!(writes.iter().all(|request| request.signed
            && request.cache_control.as_deref() == Some("no-store, no-cache, max-age=0")));
        let owner = writes
            .into_iter()
            .filter(|request| request.path == format!("/owner-tests/{OWNER_KEY}"))
            .collect::<Vec<_>>();
        assert_eq!(owner.len(), 4);
        assert_eq!(owner[0].if_none_match.as_deref(), Some("*"));
        assert!(owner[1..].iter().all(|request| request
            .if_match
            .as_ref()
            .is_some_and(|value| value.starts_with("\"etag-"))));
        assert!(state
            .objects
            .keys()
            .all(|path| path == &format!("/owner-tests/{OWNER_KEY}")));
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_s3_adapter_refuses_a_server_ignoring_conditions() {
    let server = Server::start().await;
    server.state.lock().unwrap().ignore_conditions = true;
    let store = server.client();
    assert_eq!(
        S3Ownership::acquire(store, "ingest", vec!["mainnet".into()])
            .await
            .unwrap_err(),
        OwnershipError::ConditionalWritesUnproven
    );
    assert!(server
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .all(|request| request.method != "PUT"
            || request.path != format!("/owner-tests/{OWNER_KEY}")));
    server.shutdown().await;
}
