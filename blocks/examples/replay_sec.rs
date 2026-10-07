//! Replay a firesec `.fire` file through the real `fireparq build --block-type
//! sec` into a fresh local Delta root: the replay acceptance and memory load
//! test of the SEC block type.
//!
//! ```sh
//! cargo build --release -p blocks --bin fireparq --example replay_sec
//! target/release/examples/replay_sec --fire 2026-08-14.fire --output /tmp/replay/2026-08-14 \
//!     -- --grpc-max-message-bytes 268435456
//! ```
//!
//! The example serves the file's `FIRE BLOCK` lines (`num id parent_num
//! parent_id lib_num timestamp_nanos base64(pinax.sec.v1.Block)`) as FINAL
//! responses from a loopback mock Firehose, one window at a time with
//! back-pressure, and runs the `fireparq` binary against it at its default
//! settings (anything after `--` is appended to `fireparq build`; deadline days
//! need the larger gRPC message limit of `docs/chains/sec.md`). It samples
//! the RSS of `fireparq` (and of its child processes, so a `/usr/bin/time -l`
//! wrapper still counts) with `ps`, then prints one JSON summary: wall time,
//! peak RSS, and the Delta commits, files and rows of every table read from
//! the `_delta_log`.
//!
//! Entirely offline (loopback only); it refuses an existing output directory.
//! The sample files are large (2 GB in all) and stay out of CI.
use anyhow::{bail, ensure, Context, Result};
use blocks::sec::{proto::sec, schema::TABLE_NAMES};
use clap::Parser;
use firehose_protos::firehose;
use prost::Message;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tonic::codegen::{http, BoxFuture, Service};

const TYPE_URL: &str = "type.googleapis.com/pinax.sec.v1.Block";
/// firesec's first streamable window (2003-06-30).
const FIRST_STREAMABLE: u64 = 1_761_552;

#[derive(Parser)]
struct Args {
    /// firesec `.fire` output (`FIRE INIT` then one `FIRE BLOCK` line per window).
    #[arg(long)]
    fire: PathBuf,
    /// The Delta dataset root to create (must not exist).
    #[arg(long)]
    output: PathBuf,
    /// The `fireparq` binary (default: `../fireparq` next to this example).
    #[arg(long)]
    fireparq: Option<PathBuf>,
    /// RSS sampling period.
    #[arg(long, default_value_t = 100)]
    sample_ms: u64,
    /// Extra `fireparq build` arguments (after `--`).
    #[arg(last = true)]
    extra: Vec<String>,
}

/// One `FIRE BLOCK` line, its payload still base64.
struct FireLine {
    num: u64,
    id: String,
    parent_num: u64,
    parent_id: String,
    lib_num: u64,
    timestamp_nanos: i128,
    payload: Vec<u8>,
}

fn parse_line(line: &[u8]) -> Result<Option<FireLine>> {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if line.starts_with(b"FIRE INIT ") || line.is_empty() {
        return Ok(None);
    }
    let mut fields = line.splitn(9, |byte| *byte == b' ');
    let mut text = || -> Result<&str> {
        Ok(std::str::from_utf8(
            fields.next().context("truncated FIRE BLOCK line")?,
        )?)
    };
    ensure!(
        text()? == "FIRE" && text()? == "BLOCK",
        "not a FIRE BLOCK line"
    );
    let num = text()?.parse()?;
    let id = text()?.to_string();
    let parent_num = text()?.parse()?;
    let parent_id = text()?.to_string();
    let lib_num = text()?.parse()?;
    let timestamp_nanos = text()?.parse()?;
    let payload = fields
        .next()
        .context("FIRE BLOCK line without payload")?
        .to_vec();
    Ok(Some(FireLine {
        num,
        id,
        parent_num,
        parent_id,
        lib_num,
        timestamp_nanos,
        payload,
    }))
}

/// Standard base64 with padding (no new dependency for one example).
fn base64_decode(input: &[u8]) -> Result<Vec<u8>> {
    fn value(byte: u8) -> Result<u32> {
        Ok(match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => bail!("invalid base64 byte {byte:#04x}"),
        } as u32)
    }
    ensure!(
        input.len().is_multiple_of(4),
        "base64 length is not a multiple of 4"
    );
    let mut output = Vec::with_capacity(input.len() / 4 * 3);
    for (index, chunk) in input.as_chunks::<4>().0.iter().enumerate() {
        let last = index + 1 == input.len() / 4;
        let pad = chunk.iter().rev().take_while(|byte| **byte == b'=').count();
        ensure!(pad <= 2 && (pad == 0 || last), "misplaced base64 padding");
        let mut word = 0u32;
        for byte in &chunk[..4 - pad] {
            word = (word << 6) | value(*byte)?;
        }
        word <<= 6 * pad as u32;
        output.extend_from_slice(&word.to_be_bytes()[1..4 - pad]);
    }
    Ok(output)
}

/// Only the header of a `pinax.sec.v1.Block`: prost skips the filings.
#[derive(Clone, PartialEq, prost::Message)]
struct HeaderOnly {
    #[prost(message, optional, tag = "1")]
    header: Option<sec::BlockHeader>,
}

/// The block numbers of the file, in order (a cheap first pass).
fn scan(path: &Path) -> Result<Vec<u64>> {
    let mut reader = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut numbers = Vec::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if line.starts_with(b"FIRE BLOCK ") {
            let number = line
                .split(|byte| *byte == b' ')
                .nth(2)
                .context("truncated FIRE BLOCK line")?;
            numbers.push(std::str::from_utf8(number)?.parse()?);
        }
    }
    ensure!(!numbers.is_empty(), "no FIRE BLOCK line");
    ensure!(
        numbers.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "the windows are not contiguous"
    );
    Ok(numbers)
}

/// One FINAL Firehose response of a `FIRE BLOCK` line, checked against its
/// payload header.
fn response(line: FireLine, ordinal: u64) -> Result<firehose::Response> {
    let payload = base64_decode(&line.payload)?;
    let header = HeaderOnly::decode(payload.as_slice())?
        .header
        .context("block without header")?;
    let seconds = i64::try_from(line.timestamp_nanos.div_euclid(1_000_000_000))?;
    let nanos = i32::try_from(line.timestamp_nanos.rem_euclid(1_000_000_000))?;
    let time = header.block_time.context("header without block_time")?;
    ensure!(
        header.block_number == line.num && time.seconds == seconds && time.nanos == nanos,
        "block {}: the payload header disagrees with the FIRE line",
        line.num
    );
    Ok(firehose::Response {
        block: Some(prost_types::Any {
            type_url: TYPE_URL.into(),
            value: payload,
        }),
        step: 3,
        cursor: format!("replay-{ordinal}"),
        metadata: Some(firehose::BlockMetadata {
            num: line.num,
            id: line.id,
            parent_num: line.parent_num,
            parent_id: line.parent_id,
            lib_num: line.lib_num,
            time: Some(prost_types::Timestamp { seconds, nanos }),
        }),
    })
}

#[derive(Clone)]
struct Info;
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "sec".into(),
                first_streamable_block_num: FIRST_STREAMABLE,
                ..Default::default()
            }))
        })
    }
}

/// Serves the file once, a window at a time: a reader thread decodes the next
/// line while the previous one is in flight (channel depth 1).
#[derive(Clone)]
struct Stream {
    fire: Arc<PathBuf>,
    first: u64,
    served: Arc<AtomicU64>,
    requests: Arc<AtomicU64>,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let this = self.clone();
        Box::pin(async move {
            this.requests.fetch_add(1, Ordering::SeqCst);
            if request.start_block_num != this.first as i64
                || !request.final_blocks_only
                || !request.cursor.is_empty()
            {
                return Err(tonic::Status::invalid_argument(format!(
                    "the replay serves one fresh final stream from {}, got start {} final {} cursor {:?}",
                    this.first, request.start_block_num, request.final_blocks_only, request.cursor
                )));
            }
            std::thread::spawn(move || {
                let result = (|| -> Result<()> {
                    let mut reader = BufReader::with_capacity(1 << 20, File::open(&*this.fire)?);
                    let mut line = Vec::new();
                    let mut ordinal = 0;
                    loop {
                        line.clear();
                        if reader.read_until(b'\n', &mut line)? == 0 {
                            return Ok(());
                        }
                        let Some(parsed) = parse_line(&line)? else {
                            continue;
                        };
                        let response = response(parsed, ordinal)?;
                        ordinal += 1;
                        if sender.blocking_send(Ok(response)).is_err() {
                            return Ok(());
                        }
                        this.served.fetch_add(1, Ordering::SeqCst);
                    }
                })();
                if let Err(error) = result {
                    let _ =
                        sender.blocking_send(Err(tonic::Status::internal(format!("{error:#}"))));
                }
            });
            let stream = futures::stream::unfold(receiver, |mut receiver| async move {
                receiver.recv().await.map(|item| (item, receiver))
            });
            Ok(tonic::Response::new(
                Box::pin(stream) as Self::ResponseStream
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
                    // The server encodes whatever the file holds; only the
                    // client (`--grpc-max-message-bytes`) limits a window.
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .max_encoding_message_size(usize::MAX)
                        .max_decoding_message_size(usize::MAX);
                    Ok(grpc.$method(service, request).await)
                })
            }
        }
    };
}
service!(Info, "sf.firehose.v2.EndpointInfo", unary);
service!(Stream, "sf.firehose.v2.Stream", server_streaming);

/// Resident KiB of `pid` and its direct children (`ps`, macOS and Linux).
fn tree_rss_kib(pid: u32) -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-A", "-o", "pid=,ppid=,rss="])
        .output()
        .ok()?;
    let mut total = None;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let fields: Vec<u64> = line
            .split_whitespace()
            .filter_map(|field| field.parse().ok())
            .collect();
        if let [process, parent, rss] = fields[..] {
            if process == u64::from(pid) || parent == u64::from(pid) {
                *total.get_or_insert(0) += rss;
            }
        }
    }
    total
}

/// Commits, data files and rows of one Delta table, from its JSON log.
fn delta_table(root: &Path) -> Result<Value> {
    let mut commits = 0u64;
    let mut files = 0u64;
    let mut rows = 0u64;
    let mut bytes = 0u64;
    let mut largest = 0u64;
    for entry in std::fs::read_dir(root.join("_delta_log"))? {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "json") {
            continue;
        }
        commits += 1;
        for line in std::fs::read_to_string(&path)?.lines() {
            let action: Value = serde_json::from_str(line)?;
            if let Some(add) = action.get("add") {
                files += 1;
                let size = add["size"].as_u64().context("add without size")?;
                bytes += size;
                largest = largest.max(size);
                let stats: Value =
                    serde_json::from_str(add["stats"].as_str().context("add without stats")?)?;
                rows += stats["numRecords"].as_u64().context("stats without rows")?;
            }
        }
    }
    // Version 0 creates the table; every later commit adds one flush's part.
    Ok(
        json!({"commits": commits, "files": files, "rows": rows, "bytes": bytes, "largest_file_bytes": largest}),
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        !args.output.try_exists()?,
        "output must be a fresh directory"
    );
    let fireparq = match args.fireparq {
        Some(path) => path,
        None => std::env::current_exe()?
            .parent()
            .and_then(Path::parent)
            .context("no parent directory")?
            .join("fireparq"),
    };
    ensure!(
        fireparq.is_file(),
        "{} is not built (cargo build --release -p blocks --bin fireparq)",
        fireparq.display()
    );
    // It runs in an empty working directory.
    let fireparq = fireparq.canonicalize()?;
    let output = std::path::absolute(&args.output)?;
    let scan_started = Instant::now();
    let numbers = scan(&args.fire)?;
    let (first, last) = (numbers[0], *numbers.last().unwrap());
    eprintln!(
        "{}: windows {first}..={last} ({} blocks), scanned in {:.1}s",
        args.fire.display(),
        numbers.len(),
        scan_started.elapsed().as_secs_f64()
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let served = Arc::new(AtomicU64::new(0));
    let requests = Arc::new(AtomicU64::new(0));
    let stream = Stream {
        fire: Arc::new(args.fire.clone()),
        first,
        served: served.clone(),
        requests: requests.clone(),
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Info)
            .add_service(stream)
            .serve_with_incoming(incoming)
            .await
    });

    // An empty working directory: no `.env` is loaded.
    let cwd = tempfile::tempdir()?;
    let started = Instant::now();
    let mut child = tokio::process::Command::new(&fireparq)
        .kill_on_drop(true)
        .env_clear()
        .current_dir(cwd.path())
        .args(["build", "--endpoint", &endpoint, "--block-type", "sec"])
        .args(["--start-block", &first.to_string()])
        .args(["--stop-block", &(last + 1).to_string()])
        .arg("--output")
        .arg(&output)
        .args(&args.extra)
        // Its logs go to stderr: stdout is this example's JSON summary.
        .stdout(std::io::stderr())
        .spawn()?;
    let pid = child.id().context("fireparq exited at once")?;
    let done = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(Mutex::new(Vec::<(f64, u64)>::new()));
    let sampler = {
        let (done, samples) = (done.clone(), samples.clone());
        let period = Duration::from_millis(args.sample_ms);
        std::thread::spawn(move || {
            while !done.load(Ordering::SeqCst) {
                if let Some(rss) = tree_rss_kib(pid) {
                    samples
                        .lock()
                        .unwrap()
                        .push((started.elapsed().as_secs_f64(), rss));
                }
                std::thread::sleep(period);
            }
        })
    };
    let status = child.wait().await?;
    let wall = started.elapsed().as_secs_f64();
    done.store(true, Ordering::SeqCst);
    sampler.join().unwrap();
    server.abort();

    let samples = samples.lock().unwrap().clone();
    let (peak_at, peak_kib) = samples
        .iter()
        .copied()
        .max_by_key(|(_, rss)| *rss)
        .unwrap_or_default();
    // A coarse RSS profile: the peak of each tenth of the run.
    let profile: Vec<u64> = (0..10)
        .map(|tenth| {
            samples
                .iter()
                .filter(|(at, _)| ((at / wall * 10.0) as usize).min(9) == tenth)
                .map(|(_, rss)| rss / 1024)
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut tables = BTreeMap::new();
    if status.success() {
        for table in TABLE_NAMES {
            let root = output.join(table);
            if root.join("_delta_log").is_dir() {
                tables.insert(table, delta_table(&root)?);
            }
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "fire": args.fire,
            "output": output,
            "first_block": first,
            "last_block": last,
            "blocks_in_file": numbers.len(),
            "blocks_served": served.load(Ordering::SeqCst),
            "stream_requests": requests.load(Ordering::SeqCst),
            "extra_args": args.extra,
            "exit": status.code(),
            "wall_secs": wall,
            "rss_samples": samples.len(),
            "peak_rss_mib": peak_kib as f64 / 1024.0,
            "peak_rss_at_secs": peak_at,
            "rss_profile_mib_by_tenth": profile,
            "tables": tables,
        }))?
    );
    ensure!(status.success(), "fireparq failed: {status}");
    Ok(())
}
