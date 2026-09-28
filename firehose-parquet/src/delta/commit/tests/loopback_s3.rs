//! A loopback S3 endpoint on `127.0.0.1` for the Delta log store tests. It
//! never contacts S3: objects live in memory.
//!
//! It serves what delta-rs and object_store's S3 client use for a log:
//! path-style GET (with byte ranges), HEAD, PUT with `If-None-Match: *` and
//! `If-Match`, DELETE and ListObjectsV2 (`prefix`, `delimiter`, `start-after`,
//! `continuation-token`, `max-keys`) over HTTP/1.1 keep-alive connections.
//! A conditional PUT that loses is a 412, counted in [`Server::lost_puts`].
//! [`Server::lose_put_responses`] stores matching PUTs and then drops the
//! connection instead of answering: a write with no definite outcome.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) const BUCKET: &str = "delta-test";

struct Object {
    bytes: Vec<u8>,
    etag: String,
}

#[derive(Default)]
struct State {
    objects: Mutex<(BTreeMap<String, Object>, u64)>,
    lost_puts: AtomicU64,
    /// PUTs of keys ending with this are stored, then left unanswered.
    lose_responses: Mutex<Option<String>>,
}

pub(crate) struct Server {
    pub endpoint: String,
    state: Arc<State>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(State::default());
        let served = Arc::clone(&state);
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    continue;
                };
                tokio::spawn(connection(socket, Arc::clone(&served)));
            }
        });
        Self {
            endpoint,
            state,
            task,
        }
    }

    /// Store every later PUT of a key ending with `suffix`, then drop its
    /// connection without a response.
    pub fn lose_put_responses(&self, suffix: &str) {
        *self.state.lose_responses.lock().unwrap() = Some(suffix.to_string());
    }

    /// Conditional PUTs that found their key taken.
    pub fn lost_puts(&self) -> u64 {
        self.state.lost_puts.load(Ordering::SeqCst)
    }

    /// The exact bytes of an object.
    pub fn object(&self, key: &str) -> Option<Vec<u8>> {
        let objects = self.state.objects.lock().unwrap();
        objects.0.get(key).map(|object| object.bytes.clone())
    }

    /// The keys under `prefix`, sorted.
    pub fn keys(&self, prefix: &str) -> Vec<String> {
        let objects = self.state.objects.lock().unwrap();
        objects
            .0
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect()
    }
}

async fn connection(mut socket: tokio::net::TcpStream, state: Arc<State>) {
    let mut input = Vec::new();
    loop {
        // One request: headers, then a Content-Length body.
        let end = loop {
            if let Some(index) = input.windows(4).position(|w| w == b"\r\n\r\n") {
                break index + 4;
            }
            let mut buffer = [0; 8192];
            match socket.read(&mut buffer).await {
                Ok(0) | Err(_) => return,
                Ok(n) => input.extend_from_slice(&buffer[..n]),
            }
        };
        let head = String::from_utf8_lossy(&input[..end]).into_owned();
        let mut lines = head.lines();
        let request_line: Vec<&str> = lines.next().unwrap_or_default().split(' ').collect();
        let headers: BTreeMap<String, String> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
            .collect();
        let length: usize = headers
            .get("content-length")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        while input.len() < end + length {
            let mut buffer = [0; 8192];
            match socket.read(&mut buffer).await {
                Ok(0) | Err(_) => return,
                Ok(n) => input.extend_from_slice(&buffer[..n]),
            }
        }
        let body = input[end..end + length].to_vec();
        input.drain(..end + length);
        let (method, target) = (request_line[0], request_line.get(1).copied().unwrap_or("/"));
        let lose = method == "PUT"
            && state
                .lose_responses
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|suffix| {
                    decode(target.split('?').next().unwrap_or("")).ends_with(suffix)
                });
        let (status, extra, payload) = handle(&state, method, target, &headers, body);
        if lose {
            let _ = socket.shutdown().await;
            return;
        }
        let mut response = format!("HTTP/1.1 {status} Loopback\r\n");
        let mut has_length = false;
        for (name, value) in &extra {
            has_length |= name == "content-length";
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        if !has_length {
            response.push_str(&format!("content-length: {}\r\n", payload.len()));
        }
        response.push_str("\r\n");
        if socket.write_all(response.as_bytes()).await.is_err() {
            return;
        }
        if method != "HEAD" && socket.write_all(&payload).await.is_err() {
            return;
        }
    }
}

type Response = (u16, Vec<(String, String)>, Vec<u8>);

fn error(status: u16, code: &str) -> Response {
    (
        status,
        vec![("content-type".into(), "application/xml".into())],
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>{code}</Message></Error>").into_bytes(),
    )
}

fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Some(value) = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
            {
                out.push(value);
                i += 3;
                continue;
            }
        }
        out.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

const LAST_MODIFIED: &str = "Sun, 27 Sep 2026 00:00:00 GMT";

fn handle(
    state: &State,
    method: &str,
    target: &str,
    headers: &BTreeMap<String, String>,
    body: Vec<u8>,
) -> Response {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let query: BTreeMap<String, String> = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(name), decode(value))
        })
        .collect();
    let path = decode(path);
    let trimmed = path.trim_start_matches('/');
    let (bucket, key) = trimmed.split_once('/').unwrap_or((trimmed, ""));
    if bucket != BUCKET {
        return error(404, "NoSuchBucket");
    }
    if key.is_empty() {
        return if method == "GET" && query.get("list-type").map(String::as_str) == Some("2") {
            list(state, &query)
        } else {
            error(501, "NotImplemented")
        };
    }
    let mut objects = state.objects.lock().unwrap();
    match method {
        "GET" | "HEAD" => {
            let Some(object) = objects.0.get(key) else {
                return error(404, "NoSuchKey");
            };
            if headers
                .get("if-match")
                .is_some_and(|etag| *etag != object.etag && etag != "*")
            {
                return error(412, "PreconditionFailed");
            }
            let size = object.bytes.len();
            let mut extra = vec![
                ("etag".to_string(), object.etag.clone()),
                ("last-modified".to_string(), LAST_MODIFIED.to_string()),
                ("accept-ranges".to_string(), "bytes".to_string()),
                (
                    "content-type".to_string(),
                    "application/octet-stream".to_string(),
                ),
            ];
            let range = headers.get("range").and_then(|range| {
                let (first, last) = range.strip_prefix("bytes=")?.split_once('-')?;
                if first.is_empty() {
                    let suffix: usize = last.parse().ok()?;
                    Some((size.saturating_sub(suffix), size.saturating_sub(1)))
                } else {
                    let first: usize = first.parse().ok()?;
                    let last = match last {
                        "" => size.saturating_sub(1),
                        last => last.parse::<usize>().ok()?.min(size.saturating_sub(1)),
                    };
                    Some((first, last))
                }
            });
            match range {
                Some((first, last)) if size > 0 && first <= last => {
                    extra.push((
                        "content-range".to_string(),
                        format!("bytes {first}-{last}/{size}"),
                    ));
                    let slice = object.bytes[first..=last].to_vec();
                    if method == "HEAD" {
                        extra.push(("content-length".to_string(), slice.len().to_string()));
                    }
                    (206, extra, slice)
                }
                Some(_) => error(416, "InvalidRange"),
                None => {
                    if method == "HEAD" {
                        extra.push(("content-length".to_string(), size.to_string()));
                    }
                    (200, extra, object.bytes.clone())
                }
            }
        }
        "PUT" => {
            let existing = objects.0.get(key);
            let taken = headers
                .get("if-none-match")
                .is_some_and(|value| value == "*")
                && existing.is_some();
            let stale = headers
                .get("if-match")
                .is_some_and(|etag| existing.is_none_or(|object| object.etag != *etag));
            if taken || stale {
                state.lost_puts.fetch_add(1, Ordering::SeqCst);
                return error(412, "PreconditionFailed");
            }
            objects.1 += 1;
            let etag = format!("\"etag-{}\"", objects.1);
            objects.0.insert(
                key.to_string(),
                Object {
                    bytes: body,
                    etag: etag.clone(),
                },
            );
            (200, vec![("etag".to_string(), etag)], Vec::new())
        }
        "DELETE" => {
            objects.0.remove(key);
            (204, Vec::new(), Vec::new())
        }
        _ => error(501, "NotImplemented"),
    }
}

fn list(state: &State, query: &BTreeMap<String, String>) -> Response {
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let delimiter = query.get("delimiter").filter(|d| !d.is_empty()).cloned();
    let max_keys: usize = query
        .get("max-keys")
        .and_then(|value| value.parse().ok())
        .unwrap_or(1000)
        .clamp(1, 1000);
    let after = query
        .get("continuation-token")
        .or_else(|| query.get("start-after"))
        .cloned()
        .unwrap_or_default();
    let objects = state.objects.lock().unwrap();
    let mut contents = Vec::new();
    let mut prefixes: Vec<String> = Vec::new();
    let mut last = None;
    let mut truncated = false;
    for (key, object) in objects.0.range(prefix.clone()..) {
        if !key.starts_with(&prefix) {
            break;
        }
        if key.as_str() <= after.as_str() {
            continue;
        }
        let common = delimiter.as_ref().and_then(|delimiter| {
            key[prefix.len()..]
                .find(delimiter.as_str())
                .map(|index| key[..prefix.len() + index + delimiter.len()].to_string())
        });
        if common.is_some() && prefixes.last() == common.as_ref() {
            continue;
        }
        if contents.len() + prefixes.len() == max_keys {
            truncated = true;
            break;
        }
        match common {
            Some(common) => {
                last = Some(format!("{common}\u{10ffff}"));
                prefixes.push(common);
            }
            None => {
                last = Some(key.clone());
                contents.push(format!(
                    "<Contents><Key>{}</Key><LastModified>2026-09-27T00:00:00.000Z</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                    escape(key),
                    escape(&object.etag),
                    object.bytes.len()
                ));
            }
        }
    }
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{BUCKET}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{max_keys}</MaxKeys><IsTruncated>{truncated}</IsTruncated>",
        escape(&prefix),
        contents.len() + prefixes.len(),
    );
    if let (true, Some(last)) = (truncated, last) {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape(&last)
        ));
    }
    for entry in contents {
        xml.push_str(&entry);
    }
    for common in prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape(&common)
        ));
    }
    xml.push_str("</ListBucketResult>");
    (
        200,
        vec![("content-type".into(), "application/xml".into())],
        xml.into_bytes(),
    )
}
