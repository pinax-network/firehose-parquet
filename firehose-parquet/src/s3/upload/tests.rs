use super::*;
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Copy)]
enum Reply {
    Ok,
    Lost,
    Delayed,
    Status(u16),
    Duplicate,
    Missing,
    Wildcard,
    NullVersion,
    ChangedVersion,
}
#[derive(Default)]
struct Observed {
    count: AtomicUsize,
    size: Mutex<usize>,
    digest: Mutex<Vec<u8>>,
    headers: Mutex<String>,
}
struct Server {
    endpoint: String,
    observed: Arc<Observed>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn start(reply: Reply) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let observed = Arc::new(Observed::default());
        let state = observed.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let state = state.clone();
                tokio::spawn(async move {
                    let mut input = Vec::new();
                    let end = loop {
                        let mut buffer = [0u8; 4096];
                        let n = socket.read(&mut buffer).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        input.extend_from_slice(&buffer[..n]);
                        assert!(input.len() < 32 * 1024);
                        if let Some(i) = input.windows(4).position(|s| s == b"\r\n\r\n") {
                            break i + 4;
                        }
                    };
                    let headers = String::from_utf8(input[..end].to_vec()).unwrap();
                    let length: usize = headers
                        .lines()
                        .filter_map(|l| l.split_once(':'))
                        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                        .unwrap()
                        .1
                        .trim()
                        .parse()
                        .unwrap();
                    let mut hash = Sha256::new();
                    hash.update(&input[end..]);
                    let mut size = input.len() - end;
                    while size < length {
                        let mut buffer = [0u8; IO_BUFFER_BYTES];
                        let n = socket.read(&mut buffer).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        size += n;
                        hash.update(&buffer[..n]);
                    }
                    assert_eq!(size, length);
                    *state.headers.lock().unwrap() = headers;
                    *state.size.lock().unwrap() = size;
                    *state.digest.lock().unwrap() = hash.finalize().to_vec();
                    let first = state.count.fetch_add(1, Ordering::SeqCst) == 0;
                    let (status, extra) = if first {
                        match reply {
                            Reply::Lost => { let _ = socket.shutdown().await; return; },
                            Reply::Delayed => { tokio::time::sleep(Duration::from_millis(200)).await; (200, "ETag: \"fixture\"\r\n") },
                            Reply::Status(n) => (n, "ETag: \"fixture\"\r\nLocation: http://127.0.0.1:1/forbidden\r\n"),
                            Reply::Duplicate => (200, "ETag: \"fixture\"\r\nETag: \"other\"\r\n"),
                            Reply::Missing => (200, ""),
                            Reply::Wildcard => (200, "ETag: *\r\n"),
                            Reply::NullVersion => (200, "x-amz-version-id: null\r\n"),
                            Reply::ChangedVersion => (200, "ETag: \"fixture\"\r\nx-amz-version-id: first\r\nx-amz-version-id: second\r\n"),
                            Reply::Ok => (200, "ETag: \"fixture\"\r\nx-amz-version-id: v1\r\n"),
                        }
                    } else {
                        (200, "ETag: \"fixture\"\r\n")
                    };
                    let response = format!("HTTP/1.1 {status} Response\r\n{extra}Content-Length: 0\r\nConnection: close\r\n\r\n");
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            endpoint,
            observed,
            task,
        }
    }
    fn client(&self, timeout: Duration) -> NativeS3Upload {
        let config = AwsConfig {
            aws_access_key_id: Some("fixture-key".into()),
            aws_secret_access_key: Some("fixture-secret".into()),
            aws_session_token: Some("fixture-token".into()),
            aws_region: Some("us-east-1".into()),
            aws_endpoint_url: Some(self.endpoint.clone()),
        };
        let store = store_builder(
            &config,
            "bucket",
            S3Operation::Mutation,
            CredentialPolicy::ProviderChain,
        )
        .unwrap()
        .with_allow_http(true)
        .build()
        .unwrap();
        NativeS3Upload::from_store(store, timeout, true).unwrap()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn spool() -> (File, Vec<u8>) {
    let bytes = vec![53u8; 256 * 1024];
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&bytes).unwrap();
    (file, bytes)
}

#[tokio::test]
async fn native_upload_has_exact_body_headers_and_same_store() {
    let server = Server::start(Reply::Ok).await;
    let client = server.client(Duration::from_secs(2));
    assert!(Arc::ptr_eq(&client.object_store(), &client.object_store()));
    let (file, bytes) = spool();
    let result = client
        .prepare(
            &Path::from("data/a space%/λ.parquet"),
            file,
            bytes.len() as u64,
            "private, max-age=60",
        )
        .await
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(
        result,
        UpdateVersion {
            e_tag: Some("\"fixture\"".into()),
            version: Some("v1".into())
        }
    );
    assert_eq!(server.observed.count.load(Ordering::SeqCst), 1);
    assert_eq!(*server.observed.size.lock().unwrap(), bytes.len());
    assert_eq!(
        *server.observed.digest.lock().unwrap(),
        Sha256::digest(&bytes).to_vec()
    );
    let headers = server.observed.headers.lock().unwrap();
    let first = headers.lines().next().unwrap();
    assert_eq!(
        first.split('?').next().unwrap(),
        "PUT /bucket/data/a%20space%2525/%25CE%25BB.parquet"
    );
    let url = reqwest::Url::parse(&format!(
        "{}{}",
        server.endpoint,
        first.split_whitespace().nth(1).unwrap()
    ))
    .unwrap();
    let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
    assert_eq!(query.get("X-Amz-SignedHeaders").unwrap(), "host");
    assert_eq!(query.get("X-Amz-Expires").unwrap(), "1200");
    assert_eq!(query.get("X-Amz-Security-Token").unwrap(), "fixture-token");
    let headers = headers.to_ascii_lowercase();
    assert!(headers.contains("if-none-match: *\r\n"));
    assert!(headers.contains("content-type: application/vnd.apache.parquet\r\n"));
    assert!(headers.contains("cache-control: private, max-age=60\r\n"));
    assert!(!headers.contains("\r\nx-amz-"));
}

#[tokio::test]
async fn errors_and_invalid_acknowledgements_never_retry_or_expose_presigned_url() {
    for reply in [
        Reply::Lost,
        Reply::Delayed,
        Reply::Status(307),
        Reply::Status(409),
        Reply::Status(412),
        Reply::Status(500),
        Reply::Duplicate,
        Reply::Missing,
        Reply::Wildcard,
        Reply::NullVersion,
        Reply::ChangedVersion,
    ] {
        let server = Server::start(reply).await;
        let client = server.client(Duration::from_millis(100));
        let (file, bytes) = spool();
        let result = client
            .prepare(
                &Path::from("data/part.parquet"),
                file,
                bytes.len() as u64,
                "",
            )
            .await
            .unwrap()
            .send()
            .await
            .unwrap_err();
        let error = format!("{result:#?}");
        for forbidden in [
            "fixture-key",
            "fixture-secret",
            "fixture-token",
            "X-Amz",
            "http://",
            "part.parquet",
        ] {
            assert!(
                !error.contains(forbidden),
                "upload errors must stay sanitized"
            );
        }
        assert_eq!(server.observed.count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn invalid_spool_or_headers_fail_before_data_request() {
    let server = Server::start(Reply::Ok).await;
    let client = server.client(Duration::from_secs(2));
    for (size, cache) in [
        (MAX_PART_BYTES + 1, ""),
        (0, ""),
        (17, ""),
        (256 * 1024, "bad\r\nvalue"),
    ] {
        let (file, _) = spool();
        assert!(client
            .prepare(&Path::from("part.parquet"), file, size, cache)
            .await
            .is_err());
    }
    assert_eq!(server.observed.count.load(Ordering::SeqCst), 0);
}

#[test]
fn native_capability_refuses_missing_credentials_before_any_lookup() {
    let config = AwsConfig {
        aws_access_key_id: None,
        aws_secret_access_key: None,
        aws_session_token: None,
        aws_region: None,
        aws_endpoint_url: None,
    };
    assert!(NativeS3Upload::new(&config, "fixture").is_err());
}
