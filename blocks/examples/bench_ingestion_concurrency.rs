//! Offline end-to-end benchmark for bounded flush concurrency (#516).
//!
//! `capture` records a fixed range of finalized raw Firehose responses once.
//! `replay` serves that file from a loopback Firehose and runs real `fireparq
//! build` binaries for each labeled run, always into the same fresh local root
//! so part names and bytes are comparable across runs and binaries. It reports
//! wall time and per-flush commit latency (from log timestamps, so binaries
//! without concurrency flags are measured the same way). Replay makes no network
//! request beyond loopback and never loads a dotenv file.
//!
//! Captured cursors are opaque provider values: keep capture files private and
//! out of the repository.
use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand};
use firehose_parquet::{
    auth::resolve_credentials,
    config::Config,
    grpc::{CancellationToken, FirehoseClient},
};
use firehose_protos::firehose;
use prost::Message;
use serde_json::json;
use std::{
    fs,
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tonic::codegen::{http, BoxFuture, Service};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Capture `[start, start + count)` finalized blocks into a new file.
    Capture {
        #[arg(long)]
        start: u64,
        #[arg(long)]
        count: u64,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "https://eth.firehose.pinax.network:443")]
        endpoint: String,
        #[arg(long)]
        api_key_envvar: Option<String>,
        #[arg(long)]
        api_token_envvar: Option<String>,
    },
    /// Replay a capture through each `LABEL=BINARY[@ENCODE:PUBLISH[:BYTES]]` run.
    Replay {
        #[arg(long)]
        input: PathBuf,
        /// e.g. `base=/abs/old-fireparq` or `par=/abs/fireparq@4:4`.
        #[arg(long = "run", required = true)]
        runs: Vec<String>,
        /// New directory; the shared output root is recreated below it.
        #[arg(long)]
        work: PathBuf,
        #[arg(long, default_value_t = 3)]
        repeat: usize,
        /// Extra `fireparq build` arguments, e.g. `--flush-blocks=20`.
        #[arg(long, allow_hyphen_values = true, num_args = 0..)]
        build_arg: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Args::parse().command {
        Command::Capture {
            start,
            count,
            output,
            endpoint,
            api_key_envvar,
            api_token_envvar,
        } => {
            capture(
                start,
                count,
                &output,
                &endpoint,
                api_key_envvar.as_deref(),
                api_token_envvar.as_deref(),
            )
            .await
        }
        Command::Replay {
            input,
            runs,
            work,
            repeat,
            build_arg,
        } => replay(&input, &runs, &work, repeat, &build_arg).await,
    }
}

async fn capture(
    start: u64,
    count: u64,
    output: &Path,
    endpoint: &str,
    api_key_envvar: Option<&str>,
    api_token_envvar: Option<&str>,
) -> Result<()> {
    ensure!(!output.try_exists()?, "output already exists");
    ensure!(count > 0 && count <= 1_000, "capture 1..=1000 blocks");
    let stop = start.checked_add(count).context("stop overflow")?;
    let credentials = resolve_credentials(endpoint, api_key_envvar, api_token_envvar)?;
    let client = FirehoseClient::new(Config {
        endpoint: endpoint.into(),
        api_key: credentials.api_key,
        jwt_token: credentials.jwt_token,
        start_block: Some(start),
        stop_block: Some(stop),
        final_blocks_only: true,
        stream_idle_timeout_secs: Some(30),
        reconnect_stall_timeout_secs: Some(60),
        ..Default::default()
    })?;
    let mut file = BufWriter::new(
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)?,
    );
    let mut written = 0u64;
    let mut bytes_total = 0u64;
    let mut next = start;
    tokio::time::timeout(Duration::from_secs(900), async {
        client
            .stream_blocks(
                None,
                &CancellationToken::new(),
                |bytes, type_url, cursor, identity, step| {
                    ensure!(
                        identity.block_num == next && step == 3,
                        "expected consecutive finalized blocks"
                    );
                    next += 1;
                    bytes_total += bytes.len() as u64;
                    let response = firehose::Response {
                        block: Some(prost_types::Any {
                            type_url,
                            value: bytes.into(),
                        }),
                        step,
                        cursor,
                        metadata: Some(firehose::BlockMetadata {
                            num: identity.block_num,
                            id: identity.block_id.clone(),
                            parent_num: identity.parent_num,
                            parent_id: identity.parent_id.clone(),
                            lib_num: identity.lib_num,
                            time: Some(prost_types::Timestamp {
                                seconds: identity.timestamp,
                                nanos: identity.timestamp_nanos,
                            }),
                            ..Default::default()
                        }),
                    };
                    file.write_all(&response.encode_length_delimited_to_vec())?;
                    written += 1;
                    Ok(())
                },
            )
            .await
    })
    .await
    .context("capture exceeded its deadline")??;
    file.flush()?;
    ensure!(written == count, "captured {written} of {count} blocks");
    println!(
        "{}",
        json!({"blocks": written, "start": start, "stop": stop, "raw_payload_bytes": bytes_total})
    );
    Ok(())
}

fn load(input: &Path) -> Result<Vec<firehose::Response>> {
    let mut bytes = Vec::new();
    fs::File::open(input)?.read_to_end(&mut bytes)?;
    let mut buffer = bytes.as_slice();
    let mut responses = Vec::new();
    while !buffer.is_empty() {
        responses.push(firehose::Response::decode_length_delimited(&mut buffer)?);
    }
    ensure!(!responses.is_empty(), "empty capture");
    Ok(responses)
}

#[derive(Clone)]
struct Info {
    first: u64,
}
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        let first = self.first;
        Box::pin(async move {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "bench-mainnet".into(),
                first_streamable_block_num: first,
                ..Default::default()
            }))
        })
    }
}

#[derive(Clone)]
struct Stream {
    events: Arc<Vec<firehose::Response>>,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        let events = self.events.clone();
        Box::pin(async move {
            let from = if request.cursor.is_empty() {
                events
                    .iter()
                    .position(|event| {
                        event.metadata.as_ref().unwrap().num >= request.start_block_num as u64
                    })
                    .unwrap_or(events.len())
            } else {
                events
                    .iter()
                    .position(|event| event.cursor == request.cursor)
                    .map_or(events.len(), |index| index + 1)
            };
            let replies: Vec<_> = events[from..]
                .iter()
                .take_while(|event| event.metadata.as_ref().unwrap().num <= request.stop_block_num)
                .cloned()
                .map(Ok)
                .collect();
            let stream: Self::ResponseStream = Box::pin(futures::stream::iter(replies));
            Ok(tonic::Response::new(stream))
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

fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            plain.push(ch);
        }
    }
    plain
}

/// Seconds since the Unix epoch of a `YYYY-MM-DDTHH:MM:SS.ffffffZ` log prefix.
fn log_seconds(line: &str) -> Option<f64> {
    let stamp = line.split_whitespace().next()?;
    let (date, time) = stamp.strip_suffix('Z')?.split_once('T')?;
    let mut date = date.split('-').map(|part| part.parse::<i64>());
    let (year, month, day) = (date.next()?.ok()?, date.next()?.ok()?, date.next()?.ok()?);
    let mut clock = time.split(':');
    let hours: f64 = clock.next()?.parse().ok()?;
    let minutes: f64 = clock.next()?.parse().ok()?;
    let seconds: f64 = clock.next()?.parse().ok()?;
    // Days from civil (Howard Hinnant), proleptic Gregorian.
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let month_index = (month + 9) % 12;
    let doy = (153 * month_index + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days as f64 * 86_400.0 + hours * 3_600.0 + minutes * 60.0 + seconds)
}

/// Flush latency: from the last flush trigger (`mapper flush emitted record
/// batches`, or clean stream end for the final drain) to its committed log.
fn flush_latencies(logs: &str) -> Vec<f64> {
    let mut latencies = Vec::new();
    let mut started = None;
    for line in logs.lines() {
        if line.contains("mapper flush emitted record batches")
            || line.contains("stream ended (stop block reached)")
        {
            started = log_seconds(line);
        } else if line.contains("committed flush size observation") {
            if let (Some(start), Some(end)) = (started.take(), log_seconds(line)) {
                latencies.push((end - start) * 1000.0);
            }
        }
    }
    latencies
}

fn percentile(values: &mut [f64], quantile: f64) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    values[index]
}

fn parts_digest(root: &Path) -> Result<(usize, u64, String)> {
    use sha2::{Digest, Sha256};
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet")
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("part-"))
            {
                files.push(path);
            }
        }
    }
    files.sort();
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    for path in &files {
        let data = fs::read(path)?;
        bytes += data.len() as u64;
        digest.update(path.strip_prefix(root)?.to_string_lossy().as_bytes());
        digest.update(Sha256::digest(&data));
    }
    Ok((files.len(), bytes, format!("{:x}", digest.finalize())))
}

#[derive(Default)]
struct Usage {
    user: Option<f64>,
    system: Option<f64>,
    max_rss_bytes: Option<u64>,
}

/// Parse `/usr/bin/time -l` (macOS) or `-v` (GNU) output.
fn resource_usage(logs: &str) -> Usage {
    let mut usage = Usage::default();
    for line in logs.lines() {
        let words: Vec<_> = line.split_whitespace().collect();
        if words.len() == 6 && words[1] == "real" && words[3] == "user" && words[5] == "sys" {
            usage.user = words[2].parse().ok();
            usage.system = words[4].parse().ok();
        } else if line.trim_end().ends_with("maximum resident set size") {
            usage.max_rss_bytes = words.first().and_then(|value| value.parse().ok());
        } else if let Some(value) = line.trim().strip_prefix("User time (seconds): ") {
            usage.user = value.parse().ok();
        } else if let Some(value) = line.trim().strip_prefix("System time (seconds): ") {
            usage.system = value.parse().ok();
        } else if let Some(value) = line
            .trim()
            .strip_prefix("Maximum resident set size (kbytes): ")
        {
            usage.max_rss_bytes = value.parse::<u64>().ok().map(|kib| kib * 1024);
        }
    }
    usage
}

/// One-minute load average, recorded because shared machines add noise.
fn load_average() -> Option<f64> {
    let output = std::process::Command::new("uptime").output().ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let tail = text.rsplit_once("load average")?.1;
    tail.trim_start_matches(['s', ':', ' '])
        .split([',', ' '])
        .find(|part| !part.is_empty())?
        .parse()
        .ok()
}

struct RunSpec {
    label: String,
    binary: PathBuf,
    flags: Vec<String>,
}

fn parse_run(spec: &str) -> Result<RunSpec> {
    let (label, rest) = spec.split_once('=').context("runs use LABEL=BINARY")?;
    let (binary, setting) = match rest.split_once('@') {
        Some((binary, setting)) => (binary, Some(setting)),
        None => (rest, None),
    };
    let binary = PathBuf::from(binary);
    ensure!(binary.is_absolute(), "binary paths must be absolute");
    let mut flags = Vec::new();
    if let Some(setting) = setting {
        let values: Vec<_> = setting.split(':').collect();
        ensure!(
            (2..=3).contains(&values.len()),
            "settings use ENCODE:PUBLISH[:BYTES]"
        );
        flags.extend([
            "--flush-encode-concurrency".to_string(),
            values[0].to_string(),
            "--flush-publish-concurrency".to_string(),
            values[1].to_string(),
        ]);
        if let Some(bytes) = values.get(2) {
            flags.extend(["--flush-inflight-bytes".to_string(), bytes.to_string()]);
        }
    }
    Ok(RunSpec {
        label: label.to_string(),
        binary,
        flags,
    })
}

async fn replay(
    input: &Path,
    runs: &[String],
    work: &Path,
    repeat: usize,
    build_args: &[String],
) -> Result<()> {
    ensure!(work.is_absolute(), "use an absolute work directory");
    ensure!(!work.try_exists()?, "work directory already exists");
    fs::create_dir_all(work)?;
    let specs = runs
        .iter()
        .map(|spec| parse_run(spec))
        .collect::<Result<Vec<_>>>()?;
    let events = load(input)?;
    let start = events[0].metadata.as_ref().unwrap().num;
    let stop = events.last().unwrap().metadata.as_ref().unwrap().num + 1;
    let raw_bytes: usize = events
        .iter()
        .map(|event| event.block.as_ref().unwrap().value.len())
        .sum();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(Info { first: start })
            .add_service(Stream {
                events: Arc::new(events),
            })
            .serve_with_incoming(incoming),
    );
    // One output path for every run: the stream identity (and so every part
    // name and footer) depends on the canonical output root.
    let dir = work.join("run");
    let mut results = Vec::new();
    let mut reference: Option<String> = None;
    for round in 0..repeat {
        for spec in &specs {
            if dir.exists() {
                fs::remove_dir_all(&dir)?;
            }
            fs::create_dir(&dir)?;
            // `/usr/bin/time` reports the child's own CPU time and peak RSS.
            let mut command = tokio::process::Command::new("/usr/bin/time");
            command
                .arg(if cfg!(target_os = "macos") {
                    "-l"
                } else {
                    "-v"
                })
                .arg(&spec.binary)
                .kill_on_drop(true)
                .env_clear()
                .current_dir(&dir)
                .args([
                    "build",
                    "--endpoint",
                    &endpoint,
                    "--block-type",
                    "evm",
                    "--start-block",
                    &start.to_string(),
                    "--stop-block",
                    &stop.to_string(),
                    "--stream-idle-timeout-secs",
                    "0",
                ])
                .args(&spec.flags)
                .args(build_args)
                .arg("--output")
                .arg(dir.join("output"));
            let started = Instant::now();
            let output = command.output().await?;
            let wall = started.elapsed().as_secs_f64();
            let logs = strip_ansi(&format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
            fs::write(work.join(format!("{}-{round}.log", spec.label)), &logs)?;
            if !output.status.success() {
                bail!("run {} failed:\n{logs}", spec.label);
            }
            let mut latencies = flush_latencies(&logs);
            ensure!(!latencies.is_empty(), "no flush timings in logs:\n{logs}");
            let total: f64 = latencies.iter().sum();
            let usage = resource_usage(&logs);
            let (files, bytes, digest) = parts_digest(&dir.join("output").join("bench-mainnet"))?;
            let identical = match &reference {
                Some(expected) => *expected == digest,
                None => {
                    reference = Some(digest.clone());
                    true
                }
            };
            let run = json!({
                "round": round,
                "label": spec.label,
                "flags": spec.flags,
                "wall_seconds": wall,
                "blocks_per_second": (stop - start) as f64 / wall,
                "flushes": latencies.len(),
                "flush_ms_total": total,
                "flush_ms_p50": percentile(&mut latencies, 0.5),
                "flush_ms_max": percentile(&mut latencies, 1.0),
                "user_seconds": usage.user,
                "system_seconds": usage.system,
                "max_rss_bytes": usage.max_rss_bytes,
                "load_average_1m": load_average(),
                "parts": files,
                "part_bytes": bytes,
                "parts_identical_to_first_run": identical,
            });
            eprintln!("{run}");
            ensure!(
                identical,
                "run {} produced different part names or bytes",
                spec.label
            );
            results.push(run);
        }
    }
    server.abort();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "blocks": stop - start,
            "range": [start, stop],
            "raw_payload_bytes": raw_bytes,
            "build_args": build_args,
            "parts_sha256": reference,
            "runs": results,
        }))?
    );
    Ok(())
}
