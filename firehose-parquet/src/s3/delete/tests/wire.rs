//! Real AmazonS3 requests to a hermetic HTTP endpoint; no external bucket access.
use super::*;
use futures::TryStreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
struct State {
    objects: HashSet<Path>,
    requests: Vec<(String, String)>,
    active: usize,
    maximum: usize,
    lose: Option<Path>,
    deny: Option<Path>,
    bulk_body: String,
}
struct Server {
    endpoint: String,
    state: Arc<Mutex<State>>,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn start(count: usize) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State {
            objects: keys(count).into_iter().collect(),
            bulk_body: "<DeleteResult/>".into(),
            ..Default::default()
        }));
        let server_state = state.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut requests = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        requests.spawn(handle(stream, server_state.clone()));
                    }
                }
            }
            while let Some(result) = requests.join_next().await {
                result.unwrap();
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
        let config = crate::cli::AwsConfig {
            aws_access_key_id: Some("synthetic-key".into()),
            aws_secret_access_key: Some("synthetic-secret".into()),
            aws_session_token: None,
            aws_region: Some("us-east-1".into()),
            aws_endpoint_url: Some(self.endpoint.clone()),
        };
        // Same builder used by production maintenance; HTTP enabled only here.
        Arc::new(
            config
                .s3_client_builder("bucket", true)
                .unwrap()
                .with_allow_http(true)
                .build()
                .unwrap(),
        )
    }
    async fn shutdown(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap();
    }
}
async fn handle(mut stream: tokio::net::TcpStream, state: Arc<Mutex<State>>) {
    let mut request = Vec::new();
    let header_end = loop {
        let mut bytes = [0; 4096];
        let count = stream.read(&mut bytes).await.unwrap();
        if count == 0 {
            return;
        }
        request.extend_from_slice(&bytes[..count]);
        assert!(request.len() < 128 * 1024);
        if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let header = String::from_utf8(request[..header_end].to_vec()).unwrap();
    let mut lines = header.split("\r\n");
    let first = lines.next().unwrap().split_whitespace().collect::<Vec<_>>();
    let method = first[0].to_string();
    let uri = first[1].to_string();
    let headers: std::collections::HashMap<_, _> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    assert!(headers
        .get("authorization")
        .unwrap()
        .starts_with("AWS4-HMAC-SHA256 "));
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
    while request.len() < header_end + length {
        let mut bytes = [0; 4096];
        let count = stream.read(&mut bytes).await.unwrap();
        assert_ne!(count, 0);
        request.extend_from_slice(&bytes[..count]);
    }
    let (key, failure, lose) = {
        let mut state = state.lock().unwrap();
        state.requests.push((method.clone(), uri.clone()));
        state.active += 1;
        state.maximum = state.maximum.max(state.active);
        if method == "DELETE" {
            let key = Path::from_url_path(uri.strip_prefix("/bucket/").unwrap()).unwrap();
            let deny = state.deny.as_ref() == Some(&key);
            let lose = state.lose.as_ref() == Some(&key);
            (Some(key), deny, lose)
        } else {
            assert_eq!(method, "POST");
            assert!(uri.ends_with("?delete"));
            assert!(headers.contains_key("content-md5"));
            (None, false, false)
        }
    };
    tokio::time::sleep(Duration::from_millis(if failure || lose { 3 } else { 25 })).await;
    let (status, body) = {
        let mut state = state.lock().unwrap();
        state.active -= 1;
        match key {
            Some(key) if !failure => {
                state.objects.remove(&key);
                (204, String::new())
            }
            Some(_) => (
                503,
                "<Error><Code>ServiceUnavailable</Code><Message>synthetic</Message></Error>".into(),
            ),
            None => (200, state.bulk_body.clone()),
        }
    };
    if lose {
        stream.shutdown().await.unwrap();
        return;
    }
    let reason = match status {
        204 => "No Content",
        503 => "Service Unavailable",
        _ => "OK",
    };
    let response = format!("HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: application/xml\r\n\r\n{body}", body.len());
    stream.write_all(response.as_bytes()).await.unwrap();
    stream.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_client_uses_bounded_individual_deletes_and_no_bulk_fabrication() {
    let server = Server::start(31).await;
    delete_objects_once(&server.client(), &keys(31))
        .await
        .unwrap();
    {
        let state = server.state.lock().unwrap();
        assert!(state.objects.is_empty());
        assert_eq!(state.requests.len(), 31);
        assert!(state.requests.iter().all(|(method, _)| method == "DELETE"));
        assert!(state.maximum > 1 && state.maximum <= 10);
        assert_eq!(state.active, 0);
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_lost_response_and_503_stop_without_retry_or_later_dispatch() {
    for lose in [false, true] {
        let server = Server::start(100).await;
        if lose {
            server.state.lock().unwrap().lose = Some(keys(1)[0].clone());
        } else {
            server.state.lock().unwrap().deny = Some(keys(1)[0].clone());
        }
        assert!(delete_objects_once(&server.client(), &keys(100))
            .await
            .is_err());
        {
            let state = server.state.lock().unwrap();
            assert!(state.requests.len() <= 10);
            assert_eq!(
                state
                    .requests
                    .iter()
                    .filter(|(_, key)| key.ends_with("part-000000.parquet"))
                    .count(),
                1
            );
            assert_eq!(state.active, 0, "started responses must be drained");
            assert_eq!(state.objects.contains(&keys(1)[0]), !lose);
            assert!(state.objects.contains(&keys(100)[99]));
        }
        server.shutdown().await;
    }
}

#[tokio::test]
async fn pinned_bulk_adapter_synthesizes_success_from_incomplete_wire_responses() {
    // This documents why production deliberately does not use delete_stream.
    // The request omits Quiet, yet the adapter ignores missing
    // and unrelated Deleted entries and invents Ok for every requested key.
    for (body, fabricated_success) in [
        ("<DeleteResult/>", false),
        (
            "<DeleteResult><Deleted><Key>blocks/part-000000.parquet</Key></Deleted></DeleteResult>",
            true,
        ),
        (
            "<DeleteResult><Deleted><Key>unrequested.parquet</Key></Deleted></DeleteResult>",
            true,
        ),
    ] {
        let server = Server::start(2).await;
        server.state.lock().unwrap().bulk_body = body.into();
        let client = server.client();
        let results = client
            .delete_stream(futures::stream::iter(keys(2).into_iter().map(Ok)).boxed())
            .try_collect::<Vec<_>>()
            .await;
        if fabricated_success {
            assert_eq!(results.unwrap(), keys(2));
        } else {
            assert!(
                results.is_err(),
                "an entirely empty response is rejected by the XML decoder"
            );
        }
        assert_eq!(
            server.state.lock().unwrap().objects.len(),
            2,
            "the fake server deleted nothing"
        );
        server.shutdown().await;
    }
}
