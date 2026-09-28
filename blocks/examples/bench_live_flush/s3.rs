//! Loopback HTTPS S3 endpoint for the real `fireparq` binary. The native
//! ingestion transport refuses plain HTTP, so this serves TLS with a throwaway
//! CA that the child trusts through `SSL_CERT_FILE`. Objects live in memory.
//!
//! Semantics are the subset protected ingestion uses: path-style GET/HEAD/PUT/
//! DELETE, `If-None-Match: *` and `If-Match` conditions, pinned `versionId`
//! reads, byte ranges and ListObjectsV2. Every request waits half its injected
//! latency before it is applied and half after, like a symmetric round trip;
//! with `slow_every = N`, every Nth request (by arrival) waits `slow` instead.
//! Each request is logged with wall-clock start/end times for phase analysis.
use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, Request, Response, StatusCode};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Latency {
    pub base_ms: u64,
    /// Zero disables the slow requests.
    pub slow_every: u64,
    pub slow_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Entry {
    pub seq: u64,
    /// Seconds since the Unix epoch, comparable with the child's log times.
    pub start: f64,
    pub end: f64,
    pub method: String,
    pub key: String,
    pub status: u16,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub delay_ms: u64,
    /// GET/HEAD carried `If-Match` or `versionId`.
    pub pinned: bool,
    /// For control-slot PUTs: `writing:<receipts>`, `committed`, `tombstone`.
    pub note: Option<String>,
}

struct Stored {
    bytes: Bytes,
    etag: String,
    version: String,
}

struct State {
    bucket: String,
    latency: Latency,
    arrivals: AtomicU64,
    objects: Mutex<(BTreeMap<String, Stored>, u64)>,
    log: Mutex<Vec<Entry>>,
}

pub struct Server {
    pub endpoint: String,
    pub ca_file: PathBuf,
    state: Arc<State>,
    task: tokio::task::JoinHandle<()>,
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// A throwaway CA plus a `127.0.0.1` leaf, generated with the `openssl` CLI.
fn certificates(dir: &Path) -> Result<(PathBuf, PathBuf, PathBuf)> {
    std::fs::create_dir_all(dir)?;
    let run = |args: &[&str]| -> Result<()> {
        let output = std::process::Command::new("openssl")
            .args(args)
            .current_dir(dir)
            .output()
            .context("running openssl (required to create the loopback TLS certificate)")?;
        ensure!(
            output.status.success(),
            "openssl {:?} failed: {}",
            args.first(),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    };
    let ec = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1"];
    run(&[
        &["req", "-x509"],
        &ec[..],
        &[
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "2",
            "-subj",
            "/CN=fireparq-658-bench-ca",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ],
    ]
    .concat())?;
    run(&[
        &["req"],
        &ec[..],
        &[
            "-nodes",
            "-keyout",
            "leaf.key",
            "-out",
            "leaf.csr",
            "-subj",
            "/CN=127.0.0.1",
        ],
    ]
    .concat())?;
    std::fs::write(
        dir.join("leaf.ext"),
        "subjectAltName=IP:127.0.0.1,DNS:localhost\nbasicConstraints=critical,CA:FALSE\n\
         keyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n",
    )?;
    run(&[
        "x509",
        "-req",
        "-in",
        "leaf.csr",
        "-CA",
        "ca.pem",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-out",
        "leaf.pem",
        "-days",
        "2",
        "-extfile",
        "leaf.ext",
    ])?;
    Ok((
        dir.join("ca.pem"),
        dir.join("leaf.pem"),
        dir.join("leaf.key"),
    ))
}

impl Server {
    pub async fn start(dir: &Path, bucket: &str, latency: Latency) -> Result<Self> {
        let (ca_file, leaf, key) = certificates(dir)?;
        let chain = CertificateDer::pem_file_iter(&leaf)?.collect::<Result<Vec<_>, _>>()?;
        let key = PrivateKeyDer::from_pem_file(&key)?;
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(chain, key)?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://{}", listener.local_addr()?);
        let state = Arc::new(State {
            bucket: bucket.into(),
            latency,
            arrivals: AtomicU64::new(0),
            objects: Mutex::new((BTreeMap::new(), 0)),
            log: Mutex::new(Vec::new()),
        });
        let served = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    continue;
                };
                let _ = socket.set_nodelay(true);
                let (acceptor, state) = (acceptor.clone(), served.clone());
                tokio::spawn(async move {
                    let Ok(tls) = acceptor.accept(socket).await else {
                        return;
                    };
                    let service = hyper::service::service_fn(move |request| {
                        let state = state.clone();
                        async move { Ok::<_, Infallible>(handle(state, request).await) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .keep_alive(true)
                        .serve_connection(hyper_util::rt::TokioIo::new(tls), service)
                        .await;
                });
            }
        });
        Ok(Self {
            endpoint,
            ca_file,
            state,
            task,
        })
    }

    pub fn log(&self) -> Vec<Entry> {
        self.state.log.lock().unwrap().clone()
    }

    /// Every stored object under `prefix`, with its exact bytes, sorted by
    /// key (`blocks/tests/delta_tables.rs` copies a written dataset out).
    pub fn objects(&self, prefix: &str) -> Vec<(String, Bytes)> {
        let objects = self.state.objects.lock().unwrap();
        objects
            .0
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, stored)| (key.clone(), stored.bytes.clone()))
            .collect()
    }

    /// `(objects, bytes)` of data parts (`*.parquet` outside `_fireparq/`).
    pub fn data_objects(&self) -> (usize, u64) {
        let objects = self.state.objects.lock().unwrap();
        objects
            .0
            .iter()
            .filter(|(key, _)| is_data_part(key))
            .fold((0, 0), |(count, bytes), (_, stored)| {
                (count + 1, bytes + stored.bytes.len() as u64)
            })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub fn is_data_part(key: &str) -> bool {
    key.ends_with(".parquet") && !key.split('/').any(|part| part == "_fireparq")
}

fn percent_decode(text: &str) -> String {
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
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn error(status: StatusCode, code: &str) -> (StatusCode, Vec<(&'static str, String)>, Bytes) {
    (
        status,
        vec![("content-type", "application/xml".into())],
        Bytes::from(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>{code}</Message></Error>"
        )),
    )
}

const LAST_MODIFIED: &str = "Sun, 27 Sep 2026 00:00:00 GMT";

fn control_note(key: &str, body: &[u8]) -> Option<String> {
    if !key.ends_with("/.fireparq-ingest/pending.json") {
        return None;
    }
    let record: serde_json::Value = serde_json::from_slice(body).ok()?;
    if record["deleted"].as_bool() == Some(true) {
        return Some("tombstone".into());
    }
    let payload = &record["payload"];
    match payload["phase"].as_str()? {
        "committed" => Some("committed".into()),
        phase => {
            let receipts = payload["parts"].as_array().map_or(0, |parts| {
                parts.iter().filter(|p| !p["receipt"].is_null()).count()
            });
            Some(format!("{phase}:{receipts}"))
        }
    }
}

async fn handle(state: Arc<State>, request: Request<Incoming>) -> Response<Full<Bytes>> {
    let start = now();
    let arrived = tokio::time::Instant::now();
    let seq = state.arrivals.fetch_add(1, Ordering::SeqCst) + 1;
    let method = request.method().as_str().to_string();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => Bytes::new(),
    };
    let latency = state.latency;
    let delay_ms = if latency.slow_every > 0 && seq % latency.slow_every == 0 {
        latency.slow_ms
    } else {
        latency.base_ms
    };
    // Both waits are anchored to the arrival, so timer rounding adds at most
    // one tick to the whole request instead of one per wait.
    let half = Duration::from_micros(delay_ms * 500);
    tokio::time::sleep_until(arrived + half).await;

    let path = percent_decode(uri.path());
    let mut query = BTreeMap::new();
    for pair in uri
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|p| !p.is_empty())
    {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        query.insert(percent_decode(name), percent_decode(value));
    }
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    let trimmed = path.trim_start_matches('/');
    let (bucket, key) = trimmed.split_once('/').unwrap_or((trimmed, ""));
    let pinned = header("if-match").is_some() || query.contains_key("versionId");
    let note = (method == "PUT")
        .then(|| control_note(key, &body))
        .flatten();

    let (status, extra, payload) = if bucket != state.bucket {
        error(StatusCode::NOT_FOUND, "NoSuchBucket")
    } else if key.is_empty() {
        if method == "GET" && query.get("list-type").map(String::as_str) == Some("2") {
            list(&state, &query)
        } else if method == "HEAD" {
            (StatusCode::OK, Vec::new(), Bytes::new())
        } else {
            error(StatusCode::NOT_IMPLEMENTED, "NotImplemented")
        }
    } else {
        object(&state, &method, key, &query, &header, body.clone())
    };

    tokio::time::sleep_until(arrived + Duration::from_millis(delay_ms)).await;
    let mut response = Response::builder().status(status);
    let response_bytes = payload.len();
    let mut has_length = false;
    for (name, value) in extra {
        has_length |= name == "content-length";
        response = response.header(name, value);
    }
    if !has_length {
        response = response.header("content-length", payload.len().to_string());
    }
    let response = response
        .body(Full::new(if method == "HEAD" {
            Bytes::new()
        } else {
            payload
        }))
        .unwrap();
    state.log.lock().unwrap().push(Entry {
        seq,
        start,
        end: now(),
        method,
        key: key.to_string(),
        status: status.as_u16(),
        request_bytes: body.len(),
        response_bytes,
        delay_ms,
        pinned,
        note,
    });
    response
}

fn list(
    state: &State,
    query: &BTreeMap<String, String>,
) -> (StatusCode, Vec<(&'static str, String)>, Bytes) {
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
    for (key, stored) in objects.0.range(prefix.clone()..) {
        if !key.starts_with(&prefix) {
            break;
        }
        if key.as_str() <= after.as_str() {
            continue;
        }
        let entry = match &delimiter {
            Some(delimiter) => key[prefix.len()..]
                .find(delimiter.as_str())
                .map(|index| key[..prefix.len() + index + delimiter.len()].to_string()),
            None => None,
        };
        if let Some(common) = &entry {
            if prefixes.last() == Some(common) {
                continue;
            }
        }
        if contents.len() + prefixes.len() == max_keys {
            truncated = true;
            break;
        }
        match entry {
            Some(common) => {
                // Skip every later key under this common prefix.
                last = Some(format!("{common}\u{10ffff}"));
                prefixes.push(common);
            }
            None => {
                last = Some(key.clone());
                contents.push(format!(
                    "<Contents><Key>{}</Key><LastModified>2026-09-27T00:00:00.000Z</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                    xml_escape(key),
                    xml_escape(&stored.etag),
                    stored.bytes.len()
                ));
            }
        }
    }
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{max_keys}</MaxKeys><IsTruncated>{truncated}</IsTruncated>",
        xml_escape(&state.bucket),
        xml_escape(&prefix),
        contents.len() + prefixes.len(),
    );
    if truncated {
        if let Some(last) = last {
            xml.push_str(&format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                xml_escape(&last)
            ));
        }
    }
    for entry in contents {
        xml.push_str(&entry);
    }
    for common in prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(&common)
        ));
    }
    xml.push_str("</ListBucketResult>");
    (
        StatusCode::OK,
        vec![("content-type", "application/xml".into())],
        Bytes::from(xml),
    )
}

fn object(
    state: &State,
    method: &str,
    key: &str,
    query: &BTreeMap<String, String>,
    header: &dyn Fn(&str) -> Option<String>,
    body: Bytes,
) -> (StatusCode, Vec<(&'static str, String)>, Bytes) {
    let mut objects = state.objects.lock().unwrap();
    match method {
        "GET" | "HEAD" => {
            let Some(stored) = objects.0.get(key) else {
                return error(StatusCode::NOT_FOUND, "NoSuchKey");
            };
            if query
                .get("versionId")
                .is_some_and(|version| *version != stored.version)
            {
                return error(StatusCode::NOT_FOUND, "NoSuchVersion");
            }
            if header("if-match").is_some_and(|etag| etag != stored.etag && etag != "*") {
                return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
            }
            if header("if-none-match").is_some_and(|etag| etag == stored.etag || etag == "*") {
                return (StatusCode::NOT_MODIFIED, Vec::new(), Bytes::new());
            }
            let size = stored.bytes.len();
            let mut headers = vec![
                ("etag", stored.etag.clone()),
                ("x-amz-version-id", stored.version.clone()),
                ("last-modified", LAST_MODIFIED.into()),
                ("accept-ranges", "bytes".into()),
                ("content-type", "application/octet-stream".into()),
            ];
            let range = header("range").and_then(|range| {
                let spec = range.strip_prefix("bytes=")?;
                let (first, last) = spec.split_once('-')?;
                if first.is_empty() {
                    let suffix: usize = last.parse().ok()?;
                    Some((size.saturating_sub(suffix), size.saturating_sub(1)))
                } else {
                    let first: usize = first.parse().ok()?;
                    let last = if last.is_empty() {
                        size.saturating_sub(1)
                    } else {
                        last.parse::<usize>().ok()?.min(size.saturating_sub(1))
                    };
                    Some((first, last))
                }
            });
            match range {
                Some((first, last)) if size > 0 && first <= last && last < size => {
                    headers.push(("content-range", format!("bytes {first}-{last}/{size}")));
                    if method == "HEAD" {
                        headers.push(("content-length", (last + 1 - first).to_string()));
                    }
                    (
                        StatusCode::PARTIAL_CONTENT,
                        headers,
                        stored.bytes.slice(first..=last),
                    )
                }
                Some(_) => error(StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange"),
                None => {
                    if method == "HEAD" {
                        headers.push(("content-length", size.to_string()));
                    }
                    (StatusCode::OK, headers, stored.bytes.clone())
                }
            }
        }
        "PUT" => {
            if header("x-amz-copy-source").is_some() {
                return error(StatusCode::NOT_IMPLEMENTED, "NotImplemented");
            }
            let existing = objects.0.get(key);
            if header("if-none-match").is_some_and(|value| value == "*") && existing.is_some() {
                return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
            }
            if let Some(expected) = header("if-match") {
                if existing.is_none_or(|stored| stored.etag != expected) {
                    return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
                }
            }
            objects.1 += 1;
            let digest = Sha256::digest(&body);
            let etag = format!(
                "\"{}\"",
                digest[..16]
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
            let version = format!("v{:010}", objects.1);
            let headers = vec![
                ("etag", etag.clone()),
                ("x-amz-version-id", version.clone()),
            ];
            objects.0.insert(
                key.to_string(),
                Stored {
                    bytes: body,
                    etag,
                    version,
                },
            );
            (StatusCode::OK, headers, Bytes::new())
        }
        "DELETE" => {
            objects.0.remove(key);
            (StatusCode::NO_CONTENT, Vec::new(), Bytes::new())
        }
        _ => error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
    }
}
