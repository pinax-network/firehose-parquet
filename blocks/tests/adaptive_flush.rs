//! #659: `--flush-interval-secs` applies only while `build` follows the chain
//! head. The real binary reads a paced local Firehose: each block is delivered
//! at a scheduled wall-clock offset with its own block time, so a replay can
//! run faster than real time or at it. Debug builds shorten the pace windows
//! with `FIREPARQ_DEBUG_PACE_SAMPLE_MS` (50 ms samples: catching up after
//! 300 ms of fast evidence, caught up after 200 ms of real-time evidence), so
//! each run takes a few seconds. Phases leave room for local commits of up to
//! about a second, which pause the stream.
use arrow::array::UInt64Array;
use firehose_parquet::writer::read_parquet;
use firehose_protos::{eth, firehose};
use prost::Message;
use std::{
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::codegen::{http, BoxFuture, Service};

const CHAIN: &str = "adaptive-flush-test";
const FIRST: u64 = 100;
/// 2023-11-14T22:13:20Z: every run below stays within one UTC day.
const GENESIS_MILLIS: i64 = 1_700_000_000_000;
const SAMPLE_MS: &str = "50";

#[derive(Clone)]
struct Info;
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: CHAIN.into(),
                first_streamable_block_num: FIRST,
                ..Default::default()
            }))
        })
    }
}

/// Serves one Blocks request: each response at its offset from the request.
#[derive(Clone)]
struct Stream {
    final_only: bool,
    schedule: Arc<Vec<(firehose::Response, Duration)>>,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        assert_eq!(request.final_blocks_only, self.final_only);
        assert_eq!(request.start_block_num, FIRST as i64);
        assert!(request.cursor.is_empty());
        let schedule = self.schedule.clone();
        let started = tokio::time::Instant::now();
        let stream = futures::stream::unfold(0, move |index| {
            let schedule = schedule.clone();
            async move {
                let (response, at) = schedule.get(index)?.clone();
                tokio::time::sleep_until(started + at).await;
                Some((Ok(response), index + 1))
            }
        });
        Box::pin(async move {
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
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    Ok(grpc.$method(service, request).await)
                })
            }
        }
    };
}
service!(Info, "sf.firehose.v2.EndpointInfo", unary);
service!(Stream, "sf.firehose.v2.Stream", server_streaming);

/// A delivery schedule built phase by phase.
struct Schedule {
    final_only: bool,
    responses: Vec<(firehose::Response, Duration)>,
    wall: Duration,
    block_millis: i64,
}

impl Schedule {
    fn new(final_only: bool) -> Self {
        Self {
            final_only,
            responses: Vec::new(),
            wall: Duration::ZERO,
            block_millis: GENESIS_MILLIS,
        }
    }
    /// `count` blocks, each `wall_step` after the previous delivery and
    /// `block_step` milliseconds of block time after the previous block.
    fn blocks(mut self, count: u64, wall_step: Duration, block_step: i64) -> Self {
        for _ in 0..count {
            self.block_millis += block_step;
            let number = FIRST + self.responses.len() as u64;
            let response = response(number, self.block_millis, self.final_only);
            self.responses.push((response, self.wall));
            self.wall += wall_step;
        }
        self
    }
    /// No delivery for `duration`, while the chain keeps its pace.
    fn stall(mut self, duration: Duration) -> Self {
        self.wall += duration;
        self
    }
    fn stop(&self) -> u64 {
        FIRST + self.responses.len() as u64
    }
}

fn response(number: u64, block_millis: i64, final_only: bool) -> firehose::Response {
    firehose::Response {
        block: Some(prost_types::Any {
            type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".into(),
            value: eth::Block {
                number,
                ..Default::default()
            }
            .encode_to_vec(),
        }),
        // FINAL in a final-only stream, NEW in a reversible one.
        step: if final_only { 3 } else { 1 },
        cursor: format!("paced-{number}"),
        metadata: Some(firehose::BlockMetadata {
            num: number,
            id: format!("{number:064x}"),
            parent_num: number - 1,
            parent_id: format!("{:064x}", number - 1),
            lib_num: if final_only { number } else { number - 5 },
            time: Some(prost_types::Timestamp {
                seconds: block_millis.div_euclid(1_000),
                nanos: (block_millis.rem_euclid(1_000) * 1_000_000) as i32,
            }),
            ..Default::default()
        }),
    }
}

struct MockFirehose {
    endpoint: String,
    task: tokio::task::JoinHandle<()>,
}
impl MockFirehose {
    async fn start(schedule: &Schedule) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let stream = Stream {
            final_only: schedule.final_only,
            schedule: Arc::new(schedule.responses.clone()),
        };
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(Info)
                .add_service(stream)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        Self { endpoint, task }
    }
}
impl Drop for MockFirehose {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A bounded build of the whole schedule with a one-second interval and size
/// targets far above what the fixture blocks reach.
fn build(
    server: &MockFirehose,
    schedule: &Schedule,
    dir: &Path,
    port: u16,
) -> tokio::process::Command {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    child
        .kill_on_drop(true)
        .env_clear()
        .env("FIREPARQ_DEBUG_PACE_SAMPLE_MS", SAMPLE_MS)
        .current_dir(dir)
        .args([
            "build",
            "--endpoint",
            &server.endpoint,
            "--block-type",
            "evm",
            "--start-block",
            &FIRST.to_string(),
            "--stop-block",
            &schedule.stop().to_string(),
            "--flush-interval-secs",
            "1",
            "--flush-bytes",
            "1000000000",
            "--flush-memory-bytes",
            "1000000000",
            "--stream-idle-timeout-secs",
            "0",
            "--metrics-port",
            &port.to_string(),
            &format!("--final-blocks-only={}", schedule.final_only),
            "--output",
        ])
        .arg(dir.join("output"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    child
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn metrics(port: u16) -> std::io::Result<String> {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response)
}

/// Run the build to completion while polling `/metrics` for `expected`;
/// returns the output and whether every expected line was seen together.
async fn run(mut command: tokio::process::Command, port: u16, expected: &[&str]) -> (Output, bool) {
    let child = command.spawn().unwrap();
    let poll = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while tokio::time::Instant::now() < deadline {
            if let Ok(body) = metrics(port).await {
                if expected.iter().all(|line| body.contains(line)) {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    };
    let (output, seen) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(child.wait_with_output(), poll)
    })
    .await
    .expect("CLI exceeded bounded test deadline");
    let output = output.unwrap();
    assert!(output.status.success(), "{}", plain_logs(&output));
    (output, seen)
}

/// Logs without terminal styling, for matching structured `key=value` fields.
fn plain_logs(output: &Output) -> String {
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let mut plain = String::with_capacity(logs.len());
    let mut chars = logs.chars();
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

/// A pace switch or a committed flush, in log order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Pace(&'static str),
    Flush { trigger: String, pace: String },
}

fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let start = line.find(&format!(" {key}="))? + key.len() + 2;
    Some(line[start..].split_whitespace().next()?.trim_matches('"'))
}

fn events(logs: &str) -> Vec<Event> {
    logs.lines()
        .filter_map(|line| {
            if line.contains("catching up: block time advances") {
                Some(Event::Pace("catching_up"))
            } else if line.contains("caught up: block time advances")
                || line.contains("caught up: no block timestamp")
            {
                Some(Event::Pace("caught_up"))
            } else if line.contains("committed flush size observation") {
                Some(Event::Flush {
                    trigger: field(line, "trigger")?.to_string(),
                    pace: field(line, "pace")?.to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}

fn switches(events: &[Event]) -> Vec<&'static str> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Pace(pace) => Some(*pace),
            Event::Flush { .. } => None,
        })
        .collect()
}

fn flushes<'a>(events: &'a [Event], trigger: &str) -> Vec<&'a str> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Flush { trigger: t, pace } if t == trigger => Some(pace.as_str()),
            _ => None,
        })
        .collect()
}

/// Every interval flush happens while caught up, and none between a switch to
/// catching up and the next switch back.
fn assert_no_interval_flush_while_catching_up(events: &[Event], logs: &str) {
    let mut catching_up = false;
    for event in events {
        match event {
            Event::Pace(pace) => catching_up = *pace == "catching_up",
            Event::Flush { trigger, pace } if trigger == "interval" => {
                assert!(!catching_up && pace == "caught_up", "{events:?}\n{logs}");
            }
            Event::Flush { .. } => {}
        }
    }
}

/// Row counts of each `blocks` part, and every block number written.
fn blocks_parts(root: &Path) -> (Vec<usize>, Vec<u64>) {
    let mut parts: Vec<PathBuf> = Vec::new();
    let mut stack = vec![root.join("blocks")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                parts.push(path);
            }
        }
    }
    parts.sort();
    let mut rows = Vec::new();
    let mut numbers = Vec::new();
    for part in &parts {
        let mut count = 0;
        for batch in read_parquet(part).unwrap() {
            count += batch.num_rows();
            let column = batch
                .column_by_name("block_num")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            numbers.extend(column.values().iter().copied());
        }
        rows.push(count);
    }
    numbers.sort_unstable();
    (rows, numbers)
}

/// A replay 600x faster than real time with a one-second interval writes
/// fewer, larger parts than the interval would: after the switch to catching
/// up, only the end of the stream flushes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fast_replay_suspends_the_interval_and_flushes_at_the_end() {
    // 150 Ethereum-like blocks (12 s) delivered every 20 ms: 3 s of wall time.
    let schedule = Schedule::new(true).blocks(150, Duration::from_millis(20), 12_000);
    let server = MockFirehose::start(&schedule).await;
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let (output, seen) = run(
        build(&server, &schedule, dir.path(), port),
        port,
        &["firehose_parquet_catching_up 1\n"],
    )
    .await;
    let logs = plain_logs(&output);
    assert!(seen, "the catching_up gauge never read 1\n{logs}");
    let events = events(&logs);
    assert_eq!(switches(&events), ["catching_up"], "{logs}");
    assert!(
        logs.contains("--flush-interval-secs is suspended") && logs.contains("block_time_ratio="),
        "{logs}"
    );
    assert_no_interval_flush_while_catching_up(&events, &logs);
    // A loaded machine may reach the one-second interval before the switch;
    // after it, only the end of the stream flushes.
    assert!(flushes(&events, "interval").len() <= 1, "{events:?}");
    assert_eq!(
        flushes(&events, "stream_end"),
        ["catching_up"],
        "{events:?}"
    );
    let (rows, numbers) = blocks_parts(&dir.path().join("output"));
    assert_eq!(numbers, (FIRST..schedule.stop()).collect::<Vec<_>>());
    assert!(rows.len() <= 2, "{rows:?}");
    assert!(rows.iter().max().unwrap() >= &100, "{rows:?}");
}

/// A stream at the real rate stays caught up and flushes on the interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_real_time_stream_flushes_on_the_interval() {
    // 125 blocks of 40 ms block time delivered every 40 ms: 5 s at 1x.
    let schedule = Schedule::new(true).blocks(125, Duration::from_millis(40), 40);
    let server = MockFirehose::start(&schedule).await;
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let (output, seen) = run(
        build(&server, &schedule, dir.path(), port),
        port,
        &[
            "firehose_parquet_catching_up 0\n",
            "firehose_parquet_flushes_total{trigger=\"interval\",pace=\"caught_up\"} 2\n",
        ],
    )
    .await;
    let logs = plain_logs(&output);
    assert!(seen, "the interval flush counter never read 2\n{logs}");
    let events = events(&logs);
    assert!(switches(&events).is_empty(), "{logs}");
    let interval = flushes(&events, "interval");
    assert!(interval.len() >= 3, "{events:?}");
    assert!(interval.iter().all(|pace| *pace == "caught_up"));
    let (rows, numbers) = blocks_parts(&dir.path().join("output"));
    assert_eq!(numbers, (FIRST..schedule.stop()).collect::<Vec<_>>());
    assert!(rows.len() >= 4, "{rows:?}");
    assert!(rows.iter().max().unwrap() < &60, "{rows:?}");
}

/// Fast, stalled, fast again, then real time: the pace switches both ways
/// twice, the stall is treated as caught up (its first block flushes the old
/// window on the interval), and the interval flushes again at the head.
async fn switches_both_ways(final_only: bool) {
    let schedule = Schedule::new(final_only)
        // 100x: 1 s blocks every 10 ms, for 1 s.
        .blocks(100, Duration::from_millis(10), 1_000)
        // The chain waits one block time: the next block is 1 s later.
        .stall(Duration::from_secs(1))
        // 100x again for 2.5 s, past the interval after the stall's flush.
        .blocks(250, Duration::from_millis(10), 1_000)
        // Real time: 50 ms blocks every 50 ms for 3.5 s.
        .blocks(70, Duration::from_millis(50), 50);
    let server = MockFirehose::start(&schedule).await;
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let (output, _) = run(build(&server, &schedule, dir.path(), port), port, &[]).await;
    let logs = plain_logs(&output);
    let events = events(&logs);
    assert_eq!(
        switches(&events),
        ["catching_up", "caught_up", "catching_up", "caught_up"],
        "{logs}"
    );
    assert_no_interval_flush_while_catching_up(&events, &logs);
    // The first block after the stall flushes the window the replay kept open.
    let second = events
        .iter()
        .position(|event| *event == Event::Pace("caught_up"))
        .unwrap();
    assert!(
        matches!(&events[second + 1], Event::Flush { trigger, pace } if trigger == "interval" && pace == "caught_up"),
        "{events:?}"
    );
    // Back at the head, the interval flushes again.
    let head = events
        .iter()
        .rposition(|event| *event == Event::Pace("caught_up"))
        .unwrap();
    assert!(
        flushes(&events[head..], "interval").len() >= 2,
        "{events:?}"
    );
    let (_, numbers) = blocks_parts(&dir.path().join("output"));
    assert_eq!(numbers, (FIRST..schedule.stop()).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_final_only_stream_switches_both_ways() {
    switches_both_ways(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_non_final_stream_switches_both_ways() {
    switches_both_ways(false).await;
}
