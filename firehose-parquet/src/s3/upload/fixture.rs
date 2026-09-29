//! Stateful loopback provider for native owner/controller tests. Never contacts S3.
use super::*;
use sha2::{Digest as _, Sha256};
use std::{collections::HashMap, sync::Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Copy, Default)]
pub(crate) enum Fault {
    #[default]
    None,
    LostPartAck,
    DelayedPartAck,
    DuplicateEtag,
    DuplicateVersion,
    WildcardEtag,
    ListEtag,
    NullOnlyVersion,
    ControlVersion,
    ChangedVersion,
    CorruptBody,
    TruncatedBody,
    OversizeHeaders,
    /// Refuse every part PUT with HTTP 403 without storing it.
    ForbiddenPart,
    /// Refuse every control-record PUT with HTTP 403 without storing it.
    ForbiddenControl,
    /// Drop the connection of a part PUT without storing it: the request was
    /// cut before the provider applied it (#646).
    ResetPartUnstored,
    /// Drop the connection of a part PUT, then apply the PUT after this delay,
    /// as a provider still processing it would (#646).
    LatePart(Duration),
    /// The same for every control-record PUT (#646).
    LateControl(Duration),
}
#[derive(Clone)]
pub(crate) struct Stored {
    pub bytes: Vec<u8>,
    pub etag: String,
    pub version: String,
}
pub(crate) struct Request {
    pub method: String,
    pub path: String,
    pub if_match: Option<String>,
    pub version: Option<String>,
    pub conditional: bool,
    pub query_signed: bool,
    pub receipt_verified: bool,
}
#[derive(Default)]
pub(crate) struct State {
    pub objects: HashMap<String, Stored>,
    pub requests: Vec<Request>,
    pub fault: Fault,
    revision: u64,
    /// Injected round trip per request (#658 benchmark): half before the
    /// request is applied and half before the response is written.
    pub latency: Duration,
    /// Every `slow_every`th request (by arrival) takes `slow` instead.
    pub slow_every: u64,
    pub slow: Duration,
    arrivals: u64,
}
pub(crate) struct Server {
    pub state: Arc<Mutex<State>>,
    pub client: NativeS3Upload,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = AwsConfig {
            aws_access_key_id: Some("fixture-key".into()),
            aws_secret_access_key: Some("fixture-secret".into()),
            aws_session_token: Some("fixture-token".into()),
            aws_region: Some("us-east-1".into()),
            aws_endpoint_url: Some(format!("http://{}", listener.local_addr().unwrap())),
        };
        let client = NativeS3Upload::configured(&config, "bucket", DATA_TIMEOUT, true).unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let observed = state.clone();
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        handlers.spawn(handle(socket, observed.clone()));
                    }
                    result = handlers.join_next(), if !handlers.is_empty() => { result.unwrap().unwrap(); }
                }
            }
        });
        Self {
            state,
            client,
            task,
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle(mut socket: tokio::net::TcpStream, state: Arc<Mutex<State>>) {
    let mut input = Vec::new();
    let end = loop {
        let mut buffer = [0; 4096];
        let n = socket.read(&mut buffer).await.unwrap();
        if n == 0 {
            return;
        }
        input.extend_from_slice(&buffer[..n]);
        assert!(input.len() < 128 * 1024);
        if let Some(i) = input.windows(4).position(|s| s == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let text = String::from_utf8(input[..end].to_vec()).unwrap();
    let first = text
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .collect::<Vec<_>>();
    let method = first[0];
    let url = reqwest::Url::parse(&format!("http://fixture{}", first[1])).unwrap();
    let path = url.path().to_string();
    let query: HashMap<_, _> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let headers: HashMap<String, String> = text
        .lines()
        .filter_map(|s| s.split_once(':'))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().into()))
        .collect();
    let size: usize = headers
        .get("content-length")
        .map_or(0, |s| s.parse().unwrap());
    assert!(
        size <= 8 * 1024 * 1024,
        "small controller fixture must stay bounded"
    );
    while input.len() - end < size {
        let mut buffer = [0; 4096];
        let n = socket.read(&mut buffer).await.unwrap();
        if n == 0 {
            return;
        }
        input.extend_from_slice(&buffer[..n]);
    }
    let payload = input[end..end + size].to_vec();
    let arrived = tokio::time::Instant::now();
    let delay = {
        let mut state = state.lock().unwrap();
        state.arrivals += 1;
        if state.slow_every > 0 && state.arrivals % state.slow_every == 0 {
            state.slow
        } else {
            state.latency
        }
    };
    tokio::time::sleep_until(arrived + delay / 2).await;
    let is_part = path.ends_with(".parquet");
    let listing = query.get("list-type").is_some_and(|s| s == "2");
    let late_control = match state.lock().unwrap().fault {
        Fault::LateControl(after) if method == "PUT" && path.contains("/.fireparq-ingest/") => {
            Some(after)
        }
        _ => None,
    };
    let (status, mut body, stored, fault) = {
        let mut state = state.lock().unwrap();
        let receipt_verified = if is_part && method == "PUT" {
            state
                .objects
                .iter()
                .find(|(key, _)| key.ends_with("/pending.json"))
                .is_some_and(|(_, record)| {
                    let json: serde_json::Value = serde_json::from_slice(&record.bytes).unwrap();
                    let entries = json["payload"]["parts"].as_array().unwrap();
                    // Request paths percent-encode `=` (`date=YYYY-MM-DD`).
                    let decoded = path.replace("%3D", "=");
                    entries.iter().any(|part| {
                        decoded.ends_with(part["final_relative_path"].as_str().unwrap())
                            && part["receipt"]["byte_size"].as_u64() == Some(size as u64)
                            && part["receipt"]["sha256"].as_str()
                                == Some(hex::encode(Sha256::digest(&payload)).as_str())
                    })
                })
        } else {
            false
        };
        state.requests.push(Request {
            method: method.into(),
            path: path.clone(),
            if_match: headers.get("if-match").cloned(),
            version: query.get("versionId").cloned(),
            conditional: headers.get("if-none-match").is_some_and(|s| s == "*"),
            query_signed: query.contains_key("X-Amz-Signature"),
            receipt_verified,
        });
        let fault = if is_part { state.fault } else { Fault::None };
        match method {
            "GET" if listing => (200, b"<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>".to_vec(), None, Fault::None),
            "GET" | "HEAD" => match state.objects.get(&path).cloned() {
                Some(stored) if headers.get("if-match").is_none_or(|s|s==&stored.etag)
                    && query.get("versionId").is_none_or(|s|s==&stored.version) => (200, stored.bytes.clone(), Some(stored), fault),
                Some(_) => (412, error("PreconditionFailed"), None, Fault::None),
                None => (404, error("NoSuchKey"), None, Fault::None),
            },
            "PUT" => {
                let forbidden = match state.fault {
                    Fault::ForbiddenPart => is_part,
                    Fault::ForbiddenControl => path.contains("/.fireparq-ingest/"),
                    _ => false,
                };
                if forbidden {
                    (403, error("AccessDenied"), None, Fault::None)
                } else if late_control.is_some()
                    || (is_part && matches!(fault, Fault::ResetPartUnstored | Fault::LatePart(_)))
                {
                    // Applied later, or never, after the connection is dropped below.
                    (0, Vec::new(), None, fault)
                } else {
                    match apply_put(&mut state, &path, &headers, payload.clone()) {
                        Some(object) => (200, Vec::new(), Some(object), fault),
                        None => (412, error("PreconditionFailed"), None, Fault::None),
                    }
                }
            }
            "DELETE" => { state.objects.remove(&path); (204, Vec::new(), None, Fault::None) }
            _ => panic!("unexpected fixture request"),
        }
    };
    if let Some(after) = late_control {
        let _ = socket.shutdown().await;
        tokio::time::sleep(after).await;
        let mut state = state.lock().unwrap();
        let _ = apply_put(&mut state, &path, &headers, payload);
        return;
    }
    if method == "PUT" && matches!(fault, Fault::ResetPartUnstored) {
        let _ = socket.shutdown().await;
        return;
    }
    if let (true, Fault::LatePart(after)) = (method == "PUT", fault) {
        let _ = socket.shutdown().await;
        tokio::time::sleep(after).await;
        let mut state = state.lock().unwrap();
        let _ = apply_put(&mut state, &path, &headers, payload);
        return;
    }
    if method == "PUT" && matches!(fault, Fault::DelayedPartAck) {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if method == "PUT" && matches!(fault, Fault::LostPartAck) {
        let _ = socket.shutdown().await;
        return;
    }
    tokio::time::sleep_until(arrived + delay).await;
    let actual_size = body.len();
    let mut extra = String::new();
    if let Some(stored) = stored {
        if method == "GET" {
            match fault {
                Fault::DuplicateEtag => extra.push_str("ETag: \"duplicate\"\r\n"),
                Fault::DuplicateVersion => extra.push_str("x-amz-version-id: duplicate\r\n"),
                Fault::CorruptBody => {
                    body[0] ^= 1;
                }
                Fault::TruncatedBody => {
                    body.pop();
                }
                Fault::OversizeHeaders => {
                    extra.push_str(&format!("x-fixture-large: {}\r\n", "x".repeat(40 * 1024)));
                }
                _ => {}
            }
        }
        let (etag, version) = if method == "GET" {
            match fault {
                Fault::WildcardEtag => ("*".into(), stored.version),
                Fault::ListEtag => ("\"one\",\"two\"".into(), stored.version),
                Fault::NullOnlyVersion => (String::new(), "null".into()),
                Fault::ControlVersion => (stored.etag, "bad\tvalue".into()),
                Fault::ChangedVersion => (stored.etag, "changed".into()),
                _ => (stored.etag, stored.version),
            }
        } else {
            (stored.etag, stored.version)
        };
        if !etag.is_empty() {
            extra.push_str(&format!("ETag: {etag}\r\n"));
        }
        extra.push_str(&format!("x-amz-version-id: {version}\r\n"));
    }
    let response = format!("HTTP/1.1 {status} Response\r\n{extra}Content-Length: {actual_size}\r\nConnection: close\r\nLast-Modified: Fri, 25 Sep 2026 00:00:00 GMT\r\n\r\n");
    if socket.write_all(response.as_bytes()).await.is_ok() && method != "HEAD" {
        let _ = socket.write_all(&body).await;
    }
    let _ = socket.shutdown().await;
}
fn error(code: &str) -> Vec<u8> {
    format!("<Error><Code>{code}</Code><Message>fixture</Message></Error>").into_bytes()
}

/// Apply one PUT with its S3 preconditions: `None` when refused (412).
fn apply_put(
    state: &mut State,
    path: &str,
    headers: &HashMap<String, String>,
    payload: Vec<u8>,
) -> Option<Stored> {
    let existing = state.objects.get(path);
    if headers
        .get("if-none-match")
        .is_some_and(|s| s == "*" && existing.is_some())
        || headers
            .get("if-match")
            .is_some_and(|s| existing.is_none_or(|o| s != &o.etag))
    {
        return None;
    }
    state.revision += 1;
    let object = Stored {
        bytes: payload,
        etag: format!("\"etag-{}\"", state.revision),
        version: format!("v{}", state.revision),
    };
    state.objects.insert(path.to_owned(), object.clone());
    Some(object)
}
