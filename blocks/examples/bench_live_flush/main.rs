//! Writer throughput benchmark (#658): how fast does `fireparq build` catch up
//! on fast chains (Robinhood 10 blocks/s, Arbitrum One 4 blocks/s), how much
//! margin does it keep at the head, and where does each commit's time go?
//!
//! `capture` records at most 200 finalized blocks plus the `EndpointInfo` of a
//! Pinax Firehose into a private zstd fixture (no provider cursors). `run` and
//! `matrix` replay a fixture in a loop from a loopback Firehose, at the chain
//! rate (steady state) or as fast as it is read (catch-up), into the real
//! release `fireparq build`, writing to local disk or to a loopback HTTPS S3
//! with injected per-request latency. Each scenario prints one JSON object:
//! commit latency, the stream callback's wait on each commit, blocks/s,
//! commits/s, objects and bytes, block-to-durable lag at steady state, and for
//! S3 the critical-path phases of every transaction from the endpoint's
//! request log. No request leaves loopback except `capture`, which only talks
//! to `*.firehose.pinax.network:443`. Results are recorded in
//! `docs/audit/658-live-flush-benchmark.md`.
//!
//! ```sh
//! cargo build --release --locked -p blocks --bin fireparq --example bench_live_flush
//! B=target/release/examples/bench_live_flush
//! $B capture --endpoint https://robinhood.firehose.pinax.network:443 \
//!   --start 50000000 --count 200 --output /tmp/…/robinhood-50000000.fixture
//! $B run --fixture /tmp/…/robinhood-50000000.fixture --chain robinhood --chain-rate 10 \
//!   --binary "$PWD/target/release/fireparq" --work /tmp/…/run1 \
//!   --storage s3 --latency-ms 30 --flush-bytes 33554432 --duration-secs 45
//! $B matrix --preset final --robinhood … --arbitrum … --binary "$PWD/target/release/fireparq" \
//!   --work /tmp/…/matrix --results /tmp/…/final.jsonl   # then --preset steady, tuning
//! ```
mod analysis;
mod firehose;
mod fixture;
mod s3;

use analysis::{parse_log, phases, round, summary, Flush};
use anyhow::{bail, ensure, Context, Result};
use clap::{Args as ClapArgs, Parser, Subcommand};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Capture 2..=200 finalized blocks from a Pinax Firehose into a new fixture.
    Capture {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        start: u64,
        #[arg(long)]
        count: u64,
        #[arg(long)]
        output: PathBuf,
        /// Shell variable holding the Pinax API key (never printed).
        #[arg(long, default_value = "SUBSTREAMS_API_KEY")]
        api_key_envvar: String,
    },
    /// Summarize a fixture.
    Info {
        #[arg(long)]
        fixture: PathBuf,
    },
    /// Run one scenario and print its JSON result.
    Run(RunArgs),
    /// Run a preset list of scenarios, appending one JSON line per result.
    Matrix(MatrixArgs),
}

#[derive(ClapArgs)]
struct RunArgs {
    #[arg(long)]
    fixture: PathBuf,
    /// Label of the replayed chain, used in the output root and results.
    #[arg(long)]
    chain: String,
    /// Nominal blocks per second of the chain, for reporting.
    #[arg(long)]
    chain_rate: f64,
    /// Absolute path of the `fireparq` binary under test.
    #[arg(long)]
    binary: PathBuf,
    /// New directory for this run's output, logs and certificates.
    #[arg(long)]
    work: PathBuf,
    #[arg(long, value_parser = ["local", "s3"], default_value = "local")]
    storage: String,
    #[arg(long, default_value_t = 0)]
    latency_ms: u64,
    #[arg(long, default_value_t = 0)]
    slow_every: u64,
    #[arg(long, default_value_t = 1000)]
    slow_ms: u64,
    /// Source blocks per second; 0 releases blocks as fast as they are read.
    #[arg(long, default_value_t = 0.0)]
    rate: f64,
    /// Measurement length after the first commit.
    #[arg(long, default_value_t = 60.0)]
    duration_secs: f64,
    #[arg(long, conflicts_with_all = ["flush_blocks", "flush_bytes"])]
    interval_secs: Option<u64>,
    #[arg(long, conflicts_with = "flush_bytes")]
    flush_blocks: Option<u64>,
    /// Size-only flushing: this `--flush-bytes` target and no interval.
    #[arg(long)]
    flush_bytes: Option<u64>,
    /// Passed as `--flush-memory-bytes` (the binary's default otherwise).
    #[arg(long)]
    flush_memory_bytes: Option<u64>,
    #[arg(long)]
    encode: Option<usize>,
    #[arg(long)]
    publish: Option<usize>,
    /// Stream NEW blocks with `--final-blocks-only=false` instead of the
    /// final-only stream the primary writer uses.
    #[arg(long, default_value_t = false)]
    non_final: bool,
    /// Run with `--cursor none` (no cursor mirror).
    #[arg(long, default_value_t = false)]
    cursor_none: bool,
    #[arg(long)]
    label: Option<String>,
}

#[derive(ClapArgs)]
struct MatrixArgs {
    #[arg(long, value_parser = ["final", "steady", "tuning", "smoke"])]
    preset: String,
    #[arg(long)]
    robinhood: PathBuf,
    #[arg(long)]
    arbitrum: PathBuf,
    #[arg(long)]
    binary: PathBuf,
    /// New or existing directory; each scenario gets its own subdirectory.
    #[arg(long)]
    work: PathBuf,
    /// JSON lines; scenarios whose label is already present are skipped.
    #[arg(long)]
    results: PathBuf,
    /// Run only scenarios whose label contains this text.
    #[arg(long)]
    only: Option<String>,
    /// Multiply every measurement duration (e.g. 0.25 for a quick pass).
    #[arg(long, default_value_t = 1.0)]
    scale: f64,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Storage {
    Local,
    S3 {
        latency_ms: u64,
        slow_every: u64,
        slow_ms: u64,
    },
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Trigger {
    Interval { secs: u64 },
    Blocks { blocks: u64 },
    Size { flush_bytes: u64 },
}

#[derive(Clone, Debug, Serialize)]
struct Scenario {
    label: String,
    chain: String,
    #[serde(skip)]
    fixture: PathBuf,
    chain_rate: f64,
    storage: Storage,
    trigger: Trigger,
    /// Source blocks per second; zero is as fast as the writer reads.
    rate: f64,
    duration_secs: f64,
    flush_memory_bytes: Option<u64>,
    encode: Option<usize>,
    publish: Option<usize>,
    cursor_none: bool,
    final_only: bool,
}

/// Blocks behind the head that the synthetic LIB trails in NEW streams:
/// 0.4 min on Robinhood and 1.0 min on Arbitrum One are both about 240 blocks.
const LIB_LAG_BLOCKS: u64 = 240;
const BUCKET: &str = "bench";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    match Args::parse().command {
        Command::Capture {
            endpoint,
            start,
            count,
            output,
            api_key_envvar,
        } => fixture::capture(&endpoint, start, count, &output, &api_key_envvar).await,
        Command::Info { fixture } => {
            let fixture = fixture::Fixture::read(&fixture)?;
            let first = fixture::metadata(&fixture.blocks[0]);
            println!(
                "{}",
                json!({
                    "chain_name": fixture.info.chain_name,
                    "block_features": fixture.info.block_features,
                    "first_block": first.num,
                    "blocks": fixture.blocks.len(),
                    "raw_payload_bytes": fixture.payload_bytes(),
                    "block_interval_ms": fixture.block_interval_ns() as f64 / 1e6,
                })
            );
            Ok(())
        }
        Command::Run(args) => {
            let scenario = Scenario {
                label: args.label.clone().unwrap_or_else(|| "run".into()),
                chain: args.chain.clone(),
                fixture: args.fixture.clone(),
                chain_rate: args.chain_rate,
                storage: match args.storage.as_str() {
                    "s3" => Storage::S3 {
                        latency_ms: args.latency_ms,
                        slow_every: args.slow_every,
                        slow_ms: args.slow_ms,
                    },
                    _ => Storage::Local,
                },
                trigger: match (args.interval_secs, args.flush_blocks, args.flush_bytes) {
                    (Some(secs), None, None) => Trigger::Interval { secs },
                    (None, Some(blocks), None) => Trigger::Blocks { blocks },
                    (None, None, Some(flush_bytes)) => Trigger::Size { flush_bytes },
                    _ => bail!("choose one of --interval-secs, --flush-blocks or --flush-bytes"),
                },
                rate: args.rate,
                duration_secs: args.duration_secs,
                flush_memory_bytes: args.flush_memory_bytes,
                encode: args.encode,
                publish: args.publish,
                cursor_none: args.cursor_none,
                final_only: !args.non_final,
            };
            let result = run_scenario(&scenario, &args.binary, &args.work).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        Command::Matrix(args) => matrix(args).await,
    }
}

/// One benchmark mode: its trigger, source rate and measurement length.
#[derive(Clone)]
struct Mode {
    name: &'static str,
    trigger: fn(u64) -> Trigger,
    /// Source rate as a multiple of the chain rate; zero is unlimited.
    rate: f64,
    duration: f64,
    /// `--flush-memory-bytes`; the binary's 256 MiB default otherwise.
    memory: Option<u64>,
    /// `--cursor none`: no cursor mirror write per commit.
    cursor_none: bool,
}

const MIB: u64 = 1024 * 1024;
const SIZE_32M: u64 = 32 * MIB;

fn mode(name: &'static str, trigger: fn(u64) -> Trigger, rate: f64, duration: f64) -> Mode {
    Mode {
        name,
        trigger,
        rate,
        duration,
        memory: None,
        cursor_none: false,
    }
}

/// Backfill as #659 plans it: size-only flushes, source as fast as it is read.
fn catchup() -> Mode {
    mode(
        "catchup-size32m",
        |_| Trigger::Size {
            flush_bytes: SIZE_32M,
        },
        0.0,
        45.0,
    )
}

/// Back-to-back commits of windows holding `seconds` of chain time, with room
/// in the memory trigger so the window, not the 256 MiB default, decides.
fn windows(name: &'static str, trigger: fn(u64) -> Trigger) -> Mode {
    Mode {
        memory: Some(1024 * MIB),
        ..mode(name, trigger, 0.0, 45.0)
    }
}

fn scenarios(args: &MatrixArgs) -> Vec<Scenario> {
    let robinhood = ("robinhood", &args.robinhood, 10.0);
    let arbitrum = ("arbitrum", &args.arbitrum, 4.0);
    let s3 = |latency_ms, slow_every, slow_ms| Storage::S3 {
        latency_ms,
        slow_every,
        slow_ms,
    };
    let local = ("local", Storage::Local);
    let s3_30 = ("s3-30ms", s3(30, 0, 0));
    let s3_80 = ("s3-80ms", s3(80, 0, 0));
    let storages = vec![
        local,
        ("s3-0ms", s3(0, 0, 0)),
        ("s3-10ms", s3(10, 0, 0)),
        s3_30,
        s3_80,
        ("s3-30ms-slow200", s3(30, 200, 1000)),
    ];
    let windows_60s = windows("windows-60s", |per_second| Trigger::Blocks {
        blocks: 60 * per_second,
    });
    let bigger = Mode {
        name: "catchup-size32m-mem1g",
        memory: Some(1024 * MIB),
        duration: 60.0,
        ..catchup()
    };
    type Variant = Option<(usize, usize)>;
    type Chain<'a> = (&'a str, &'a PathBuf, f64);
    // (chains, storages, modes, concurrency variants)
    let plan: Vec<(Vec<Chain>, Vec<(&str, Storage)>, Vec<Mode>, Vec<Variant>)> =
        match args.preset.as_str() {
            "smoke" => vec![(
                vec![robinhood, arbitrum],
                vec![local, s3_30],
                vec![Mode {
                    duration: 20.0,
                    ..catchup()
                }],
                vec![None],
            )],
            // Catch-up throughput, commit phases and 60 s / 300 s windows.
            "final" => vec![
                (
                    vec![robinhood, arbitrum],
                    storages.clone(),
                    vec![catchup(), windows_60s.clone()],
                    vec![None],
                ),
                (
                    vec![robinhood, arbitrum],
                    vec![local, s3_30, s3_80],
                    vec![bigger.clone()],
                    vec![None],
                ),
                // 300 s of Robinhood (3,000 blocks) does not fit the default
                // triggers at all; Arbitrum's 1,200 blocks fit in 1 GiB.
                (
                    vec![arbitrum],
                    vec![local, s3_30, s3_80],
                    vec![Mode {
                        duration: 60.0,
                        ..windows("windows-300s", |per_second| Trigger::Blocks {
                            blocks: 300 * per_second,
                        })
                    }],
                    vec![None],
                ),
            ],
            // Source at the chain rate with the v1.0.0 head interval: which
            // trigger fires, and how much of each period the commit uses.
            // The head cadence (60-120 s commits): 120 s windows back to back,
            // then the source at the chain rate with default triggers.
            "steady" => vec![
                (
                    vec![robinhood, arbitrum],
                    vec![local, s3_30, s3_80],
                    vec![Mode {
                        // 1,200 Robinhood blocks need about 1.4 GB of mapper estimate.
                        memory: Some(2048 * MIB),
                        ..windows("windows-120s", |per_second| Trigger::Blocks {
                            blocks: 120 * per_second,
                        })
                    }],
                    vec![None],
                ),
                (
                    vec![robinhood],
                    vec![local, s3_30, s3_80],
                    vec![mode(
                        "steady-120s",
                        |_| Trigger::Interval { secs: 120 },
                        1.0,
                        110.0,
                    )],
                    vec![None],
                ),
                (
                    vec![arbitrum],
                    vec![local, s3_30, s3_80],
                    vec![mode(
                        "steady-60s",
                        |_| Trigger::Interval { secs: 60 },
                        1.0,
                        190.0,
                    )],
                    vec![None],
                ),
            ],
            "tuning" => vec![
                (
                    vec![robinhood, arbitrum],
                    vec![s3_30, s3_80],
                    vec![catchup(), windows_60s.clone()],
                    vec![Some((2, 8)), Some((2, 16)), Some((4, 16))],
                ),
                (
                    vec![robinhood, arbitrum],
                    vec![s3_30, s3_80],
                    vec![Mode {
                        name: "catchup-size32m-nocursor",
                        cursor_none: true,
                        ..catchup()
                    }],
                    vec![None],
                ),
            ],
            _ => unreachable!(),
        };
    let mut list = Vec::new();
    for (chains, storages, modes, variants) in &plan {
        for chain in chains {
            for storage in storages {
                for mode in modes {
                    for concurrency in variants {
                        let suffix =
                            concurrency.map_or(String::new(), |(e, p)| format!("/e{e}p{p}"));
                        list.push(Scenario {
                            label: format!("{}/{}/{}{suffix}", chain.0, storage.0, mode.name),
                            chain: chain.0.into(),
                            fixture: chain.1.clone(),
                            chain_rate: chain.2,
                            storage: storage.1,
                            trigger: (mode.trigger)(chain.2 as u64),
                            rate: mode.rate * chain.2,
                            duration_secs: mode.duration * args.scale,
                            flush_memory_bytes: mode.memory,
                            encode: concurrency.map(|(e, _)| e),
                            publish: concurrency.map(|(_, p)| p),
                            cursor_none: mode.cursor_none,
                            final_only: true,
                        });
                    }
                }
            }
        }
    }
    list
}

async fn matrix(args: MatrixArgs) -> Result<()> {
    ensure!(args.work.is_absolute(), "use an absolute work directory");
    fs::create_dir_all(&args.work)?;
    let done: Vec<String> = match fs::read_to_string(&args.results) {
        Ok(text) => text
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter_map(|value| value["scenario"]["label"].as_str().map(str::to_owned))
            .collect(),
        Err(_) => Vec::new(),
    };
    let list: Vec<Scenario> = scenarios(&args)
        .into_iter()
        .filter(|s| {
            args.only
                .as_ref()
                .is_none_or(|only| s.label.contains(only.as_str()))
        })
        .filter(|s| !done.contains(&s.label))
        .collect();
    eprintln!("{} scenarios to run", list.len());
    for (index, scenario) in list.iter().enumerate() {
        let dir = args.work.join(scenario.label.replace('/', "_"));
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        eprintln!("[{}/{}] {}", index + 1, list.len(), scenario.label);
        let result = match run_scenario(scenario, &args.binary, &dir).await {
            Ok(result) => result,
            Err(error) => json!({
                "scenario": scenario,
                "error": format!("{error:#}"),
            }),
        };
        eprintln!("{}", headline(&result));
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&args.results)?;
        writeln!(file, "{}", serde_json::to_string(&result)?)?;
        // Keep logs and request logs; drop bulky output data.
        let _ = fs::remove_dir_all(dir.join("out"));
    }
    Ok(())
}

fn headline(result: &Value) -> String {
    if let Some(error) = result.get("error") {
        return format!("  error: {error}");
    }
    format!(
        "  commits {} blocks/s {} commits/s {} commit_ms p50 {} p99 {} stall {}% lag_p99_ms {}",
        result["commits"],
        result["blocks_per_second"],
        result["commits_per_second"],
        result["commit_ms"]["p50"],
        result["commit_ms"]["p99"],
        result["stall_percent"],
        result["lag_ms"]["p99"],
    )
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

/// Sum of every sample of each metric family, ignoring labels.
fn scrape(port: u16) -> Option<BTreeMap<String, f64>> {
    let mut stream = std::net::TcpStream::connect_timeout(
        &([127, 0, 0, 1], port).into(),
        Duration::from_millis(200),
    )
    .ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .ok()?;
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")
        .ok()?;
    let mut text = String::new();
    stream.read_to_string(&mut text).ok()?;
    let body = text.split_once("\r\n\r\n")?.1;
    let mut values = BTreeMap::new();
    for line in body.lines().filter(|line| !line.starts_with('#')) {
        let Some((name, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let name = name.split('{').next().unwrap_or(name);
        if let Ok(value) = value.parse::<f64>() {
            *values.entry(name.to_string()).or_insert(0.0) += value;
        }
    }
    Some(values)
}

#[derive(Clone, Debug)]
struct Sample {
    time: f64,
    current_block: f64,
    durable_block: f64,
    files: f64,
    file_bytes: f64,
}

/// Cumulative CPU seconds and resident KiB of a process, from `ps`.
fn process_usage(pid: u32) -> Option<(f64, u64)> {
    let output = std::process::Command::new("ps")
        .args(["-o", "time=,rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let mut words = text.split_whitespace();
    let time = words.next()?;
    let rss: u64 = words.next()?.parse().ok()?;
    let mut seconds = 0.0;
    for part in time.split(':') {
        seconds = seconds * 60.0 + part.parse::<f64>().ok()?;
    }
    Some((seconds, rss))
}

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

async fn run_scenario(scenario: &Scenario, binary: &Path, work: &Path) -> Result<Value> {
    ensure!(binary.is_absolute(), "--binary must be an absolute path");
    ensure!(work.is_absolute(), "--work must be an absolute path");
    ensure!(!work.try_exists()?, "work directory already exists");
    fs::create_dir_all(work)?;
    let fixture = fixture::Fixture::read(&scenario.fixture)?;
    let timeline = Arc::new(firehose::Timeline::new(
        &fixture,
        scenario.final_only,
        LIB_LAG_BLOCKS,
        true,
    ));
    let base = timeline.base();
    let streams = Arc::new(Mutex::new(Vec::new()));
    let firehose = firehose::Server::start(firehose::Mock {
        timeline,
        info: fixture.info.clone(),
        rate: scenario.rate,
        streams: streams.clone(),
    })
    .await?;
    let s3 = match scenario.storage {
        Storage::Local => None,
        Storage::S3 {
            latency_ms,
            slow_every,
            slow_ms,
        } => Some(
            s3::Server::start(
                &work.join("tls"),
                BUCKET,
                s3::Latency {
                    base_ms: latency_ms,
                    slow_every,
                    slow_ms,
                },
            )
            .await?,
        ),
    };
    let output = match &s3 {
        None => work.join("out").to_string_lossy().into_owned(),
        Some(_) => format!("s3://{BUCKET}/{}", scenario.chain),
    };
    let metrics_port = free_port()?;
    let log_path = work.join("fireparq.log");
    let log = fs::File::create(&log_path)?;
    let mut command = tokio::process::Command::new(binary);
    command
        .kill_on_drop(true)
        .env_clear()
        .env("NO_COLOR", "1")
        .current_dir(work)
        .stdout(log.try_clone()?)
        .stderr(log)
        .args([
            "build",
            "--endpoint",
            &firehose.endpoint,
            "--block-type",
            "evm",
            "--start-block",
            &base.to_string(),
            &format!("--final-blocks-only={}", scenario.final_only),
            "--stream-idle-timeout-secs",
            "0",
            "--metrics-port",
            &metrics_port.to_string(),
            "--output",
            &output,
        ]);
    match scenario.trigger {
        Trigger::Interval { secs } => command.args(["--flush-interval-secs", &secs.to_string()]),
        Trigger::Blocks { blocks } => command.args(["--flush-blocks", &blocks.to_string()]),
        Trigger::Size { flush_bytes } => command.args(["--flush-bytes", &flush_bytes.to_string()]),
    };
    if scenario.cursor_none {
        command.args(["--cursor", "none"]);
    }
    if let Some(bytes) = scenario.flush_memory_bytes {
        command.args(["--flush-memory-bytes", &bytes.to_string()]);
    }
    if let Some(encode) = scenario.encode {
        command.args(["--flush-encode-concurrency", &encode.to_string()]);
    }
    if let Some(publish) = scenario.publish {
        command.args(["--flush-publish-concurrency", &publish.to_string()]);
    }
    if let Some(s3) = &s3 {
        command
            .env("AWS_ACCESS_KEY_ID", "bench-access-key")
            .env("AWS_SECRET_ACCESS_KEY", "bench-secret-key")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_ENDPOINT_URL_S3", &s3.endpoint)
            .env("SSL_CERT_FILE", &s3.ca_file);
    }
    let load_before = load_average();
    let mut child = command.spawn()?;
    let pid = child.id().context("child pid")?;
    let spawned = unix_now();
    let mut samples: Vec<Sample> = Vec::new();
    let mut usage = (0.0f64, 0u64);
    let mut last_usage = Instant::now() - Duration::from_secs(2);
    let mut first_commit: Option<f64> = None;
    let mut interrupted: Option<f64> = None;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        let now = unix_now();
        if let Some(values) = tokio::task::spawn_blocking(move || scrape(metrics_port)).await? {
            let get = |name: &str| values.get(name).copied().unwrap_or(0.0);
            let sample = Sample {
                time: now,
                current_block: get("firehose_parquet_current_block_number"),
                durable_block: get("firehose_parquet_cursor_last_block_num"),
                files: get("firehose_parquet_files_written_total"),
                file_bytes: get("firehose_parquet_file_bytes_total"),
            };
            // `--cursor none` never sets the mirror gauge; files count too.
            if first_commit.is_none() && (sample.durable_block > 0.0 || sample.files > 0.0) {
                first_commit = Some(now);
            }
            samples.push(sample);
        }
        if last_usage.elapsed() >= Duration::from_secs(1) {
            if let Some((cpu, rss)) = process_usage(pid) {
                usage = (cpu, usage.1.max(rss));
            }
            last_usage = Instant::now();
        }
        match (first_commit, interrupted) {
            (None, None) if now - spawned > 180.0 => {
                interrupted = Some(now);
                let _ = std::process::Command::new("kill")
                    .args(["-INT", &pid.to_string()])
                    .status();
            }
            (Some(first), None) if now - first >= scenario.duration_secs => {
                interrupted = Some(now);
                let _ = std::process::Command::new("kill")
                    .args(["-INT", &pid.to_string()])
                    .status();
            }
            (_, Some(at)) if now - at > 180.0 => {
                let _ = child.start_kill();
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let load_after = load_average();
    let text = analysis::strip_ansi(&fs::read_to_string(&log_path)?);
    let parsed = parse_log(&text);
    let requests = s3.as_ref().map(|s3| s3.log()).unwrap_or_default();
    if s3.is_some() {
        let mut file = fs::File::create(work.join("s3-requests.jsonl"))?;
        for entry in &requests {
            writeln!(file, "{}", serde_json::to_string(entry)?)?;
        }
    }
    let interrupted = interrupted.context("the binary exited before the measurement ended")?;
    ensure!(
        parsed.flushes.len() >= 3,
        "only {} commits; see {}",
        parsed.flushes.len(),
        log_path.display()
    );
    let streams = streams.lock().unwrap().clone();
    let mut result = analyze(
        scenario,
        &parsed.flushes,
        &samples,
        &requests,
        &streams,
        interrupted,
    );
    let map = result.as_object_mut().unwrap();
    map.insert("scenario".into(), serde_json::to_value(scenario)?);
    map.insert(
        "fixture".into(),
        json!({
            "chain_name": fixture.info.chain_name,
            "first_block": base,
            "blocks": fixture.blocks.len(),
            "raw_payload_bytes": fixture.payload_bytes(),
            "block_interval_ms": fixture.block_interval_ns() as f64 / 1e6,
        }),
    );
    map.insert("exit_success".into(), json!(status.success()));
    map.insert(
        "startup_to_first_commit_s".into(),
        json!(parsed
            .started
            .map(|started| round(parsed.flushes[0].committed - started))),
    );
    map.insert("cpu_seconds".into(), json!(round(usage.0)));
    map.insert("max_rss_mib".into(), json!(round(usage.1 as f64 / 1024.0)));
    map.insert("load_average_1m".into(), json!([load_before, load_after]));
    map.insert("log_warnings".into(), json!(parsed.errors));
    if let Some(s3) = &s3 {
        let (objects, bytes) = s3.data_objects();
        map.insert(
            "s3_data_objects".into(),
            json!({"objects": objects, "bytes": bytes}),
        );
    }
    Ok(result)
}

/// The durable block number at each change of the mirror gauge, as sampled.
fn durable_changes(samples: &[Sample]) -> Vec<(f64, f64)> {
    let mut changes: Vec<(f64, f64)> = Vec::new();
    for sample in samples {
        if changes
            .last()
            .is_none_or(|(_, block)| sample.durable_block > *block)
            && sample.durable_block > 0.0
        {
            changes.push((sample.time, sample.durable_block));
        }
    }
    changes
}

fn analyze(
    scenario: &Scenario,
    flushes: &[Flush],
    samples: &[Sample],
    requests: &[s3::Entry],
    streams: &[firehose::StreamStart],
    interrupted: f64,
) -> Value {
    // The first commit carries startup; later commits are measured.
    let measured: Vec<&Flush> = flushes
        .iter()
        .skip(1)
        .filter(|flush| flush.committed <= interrupted)
        .collect();
    let first = &flushes[0];
    let last = measured.last().copied().unwrap_or(first);
    let window = (last.committed - first.committed).max(1e-9);
    let mut changes = durable_changes(samples);
    if changes.is_empty() {
        // `--cursor none` never sets the mirror gauge. While the callback waits
        // on a commit, the last mapped block is the last block of that window.
        for flush in flushes {
            let block = samples
                .iter()
                .filter(|s| s.time >= flush.emitted && s.time <= flush.committed)
                .map(|s| s.current_block)
                .fold(0.0, f64::max);
            if block > 0.0 {
                changes.push((flush.committed, block));
            }
        }
    }
    let durable_at = |time: f64| {
        changes
            .iter()
            .take_while(|(at, _)| *at <= time + 0.2)
            .last()
            .map_or(0.0, |(_, block)| *block)
    };
    // Blocks made durable between the first and last measured commit.
    let blocks = durable_at(last.committed) - durable_at(first.committed);
    let sample_at = |time: f64| {
        samples
            .iter()
            .find(|sample| sample.time >= time)
            .or(samples.last())
            .cloned()
    };
    let (files, bytes) = match (
        sample_at(first.committed + 0.05),
        sample_at(last.committed + 0.05),
    ) {
        (Some(a), Some(b)) => (b.files - a.files, b.file_bytes - a.file_bytes),
        _ => (0.0, 0.0),
    };
    let commit_ms: Vec<f64> = measured.iter().map(|f| f.commit_ms).collect();
    let stall_ms: Vec<f64> = measured
        .iter()
        .map(|f| (f.committed - f.emitted) * 1000.0)
        .collect();
    let period: Vec<f64> = measured
        .iter()
        .zip(flushes.iter())
        .map(|(later, earlier)| later.committed - earlier.committed)
        .collect();
    let mut triggers: BTreeMap<String, u64> = BTreeMap::new();
    for flush in &measured {
        *triggers.entry(flush.trigger.clone()).or_default() += 1;
    }
    let files_per_commit: Vec<f64> = measured.iter().map(|f| f.files as f64).collect();
    let rows_per_commit: Vec<f64> = measured.iter().map(|f| f.rows as f64).collect();

    let mut result = Map::new();
    result.insert("commits".into(), json!(measured.len()));
    result.insert("window_secs".into(), json!(round(window)));
    result.insert("blocks".into(), json!(blocks));
    result.insert("blocks_per_second".into(), json!(round(blocks / window)));
    result.insert(
        "chain_rate_multiple".into(),
        json!(round(blocks / window / scenario.chain_rate)),
    );
    result.insert(
        "commits_per_second".into(),
        json!(round(measured.len() as f64 / window)),
    );
    result.insert(
        "blocks_per_commit".into(),
        json!(round(blocks / measured.len().max(1) as f64)),
    );
    result.insert("commit_ms".into(), summary(&commit_ms));
    result.insert("receive_stall_ms".into(), summary(&stall_ms));
    result.insert(
        "stall_percent".into(),
        json!(round(stall_ms.iter().sum::<f64>() / 10.0 / window)),
    );
    result.insert("flush_period_s".into(), summary(&period));
    // Callback not blocked on a commit: receiving and mapping the next window.
    let between: Vec<f64> = measured
        .iter()
        .zip(flushes.iter())
        .map(|(later, earlier)| (later.emitted - earlier.committed) * 1000.0)
        .collect();
    result.insert("between_commits_ms".into(), summary(&between));
    result.insert("triggers".into(), json!(triggers));
    result.insert("files_per_commit".into(), summary(&files_per_commit));
    result.insert("rows_per_commit".into(), summary(&rows_per_commit));
    result.insert("objects".into(), json!(files));
    result.insert("object_bytes_total".into(), json!(bytes));
    result.insert(
        "bytes_per_object".into(),
        json!(if files > 0.0 {
            round(bytes / files)
        } else {
            0.0
        }),
    );
    let largest: Vec<f64> = measured
        .iter()
        .map(|f| f.largest_file_bytes as f64)
        .collect();
    result.insert("largest_file_bytes".into(), summary(&largest));
    result.insert(
        "peak_publications".into(),
        json!(measured.iter().map(|f| f.peak_publications).max()),
    );
    result.insert(
        "peak_encoders".into(),
        json!(measured.iter().map(|f| f.peak_encoders).max()),
    );

    // Steady state: time from a block's scheduled release to its durable commit.
    if scenario.rate > 0.0 {
        if let Some(stream) = streams.last() {
            let released = stream
                .requested
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64();
            let scheduled = |block: f64| released + (block - stream.start as f64) / scenario.rate;
            let mut lags = Vec::new();
            let mut halves = (Vec::new(), Vec::new());
            let mut previous = durable_at(first.committed);
            let midpoint = first.committed + window / 2.0;
            for (time, block) in &changes {
                if *time <= first.committed + 0.2 || *time > last.committed + 0.2 {
                    continue;
                }
                let mut next = previous + 1.0;
                while next <= *block {
                    let lag = (time - scheduled(next)) * 1000.0;
                    lags.push(lag);
                    if *time < midpoint {
                        halves.0.push(lag);
                    } else {
                        halves.1.push(lag);
                    }
                    next += 1.0;
                }
                previous = *block;
            }
            let mean = |values: &Vec<f64>| {
                (!values.is_empty())
                    .then(|| round(values.iter().sum::<f64>() / values.len() as f64))
            };
            let head = stream.start as f64 + (interrupted - released) * scenario.rate;
            result.insert("lag_ms".into(), summary(&lags));
            result.insert(
                "lag_trend_ms".into(),
                json!({"first_half_mean": mean(&halves.0), "second_half_mean": mean(&halves.1)}),
            );
            result.insert(
                "backlog_at_end_blocks".into(),
                json!(round(head - durable_at(interrupted))),
            );
            let mapped_behind: Vec<f64> = samples
                .iter()
                .filter(|s| {
                    s.time >= first.committed && s.time <= last.committed && s.current_block > 0.0
                })
                .map(|s| {
                    (stream.start as f64 + (s.time - released) * scenario.rate) - s.current_block
                })
                .collect();
            result.insert("mapped_behind_head_blocks".into(), summary(&mapped_behind));
        }
    }

    // Loopback S3: per-transaction critical-path phases from the request log.
    if !requests.is_empty() {
        let mut per_phase: BTreeMap<&'static str, Vec<f64>> = BTreeMap::new();
        let mut request_counts: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        let mut totals = Vec::new();
        let mut request_ms: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        let mut attributed = 0;
        for flush in &measured {
            let Some((phases, requests)) = phases(flush, requests) else {
                continue;
            };
            attributed += 1;
            for (name, value) in phases {
                per_phase.entry(name).or_default().push(value);
            }
            totals.push(requests["total"].as_f64().unwrap_or(0.0));
            if let Some(counts) = requests["by_class"].as_object() {
                for (name, count) in counts {
                    request_counts
                        .entry(name.clone())
                        .or_default()
                        .push(count.as_f64().unwrap_or(0.0));
                }
            }
            if let Some(classes) = requests["request_ms_by_class"].as_object() {
                for (name, stats) in classes {
                    if let Some(mean) = stats["mean"].as_f64() {
                        request_ms.entry(name.clone()).or_default().push(mean);
                    }
                }
            }
        }
        result.insert("phase_attributed_commits".into(), json!(attributed));
        result.insert(
            "phases_ms".into(),
            json!(per_phase
                .into_iter()
                .map(|(name, values)| (name.to_string(), summary(&values)))
                .collect::<Map<_, _>>()),
        );
        result.insert("requests_per_commit".into(), summary(&totals));
        result.insert(
            "requests_per_commit_by_class".into(),
            json!(request_counts
                .into_iter()
                .map(|(name, values)| (
                    name,
                    json!(round(values.iter().sum::<f64>() / values.len() as f64))
                ))
                .collect::<Map<_, _>>()),
        );
        result.insert(
            "request_ms_by_class_mean".into(),
            json!(request_ms
                .into_iter()
                .map(|(name, values)| (
                    name,
                    json!(round(values.iter().sum::<f64>() / values.len() as f64))
                ))
                .collect::<Map<_, _>>()),
        );
        let slow = requests
            .iter()
            .filter(|entry| entry.delay_ms >= 500)
            .count();
        result.insert("slow_requests".into(), json!(slow));
        result.insert("total_requests".into(), json!(requests.len()));
    }
    Value::Object(result)
}
