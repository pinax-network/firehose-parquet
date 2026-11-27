//! #725: a parallel backfill fills one dataset with several `build`s at once.
//!
//! A mock Firehose serves final EVM blocks 100..=111 over two UTC days, and a
//! loopback HTTPS S3 endpoint holds the dataset. The test opens a backfill of
//! `[100, 108)` in lanes of three blocks, runs the lanes (two at the same
//! time, one crashed and recovered), seals, and extends the sealed dataset
//! with an ordinary `build`. It checks that:
//!
//! - only planned lanes may write before the seal, and none can run twice at
//!   once;
//! - every lane commits to the same tables under its own `txn` application
//!   id, and the tables carry one `fireparq.descriptor`, the dataset's;
//! - a lane commits its flushes to the Delta logs together, not one commit
//!   per flush, and a lane that died inside such a commit finishes it (the
//!   third test);
//! - a lane stopped by SIGTERM commits the flushes it accepted, publishes
//!   nothing it had only buffered and releases its owner (the fourth test);
//! - `seal` refuses until every lane is complete;
//! - a lane that does not receive its first block fails;
//! - a lane that died between two tables' commits recovers after the
//!   maintenance job compacted, vacuumed and checkpointed the tables, with
//!   the parts no log holds yet still in place (the second test);
//! - the sealed dataset holds every block exactly once, `validate` passes
//!   across the lanes, and a `build` continues from the stored cursor under
//!   the dataset's own application id, refusing any other start.
use firehose_protos::{eth, firehose};
use prost::Message;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

mod common;

#[path = "../examples/bench_live_flush/s3.rs"]
#[allow(dead_code)]
mod s3;

const CHAIN: &str = "backfill-test-chain";
const BUCKET: &str = "backfill-lake";
/// 2023-11-15T00:00:00Z: blocks 100 and 101 are on 2023-11-14, the rest on
/// 2023-11-15, so the first lane spans two dates and all lanes share one.
const MIDNIGHT: i64 = 1_700_006_400;
const FIRST: u64 = 100;
const LAST: u64 = 111;
/// The backfilled range and its lanes: `[100,103)`, `[103,106)`, `[106,108)`.
const STOP: u64 = 108;
const LANES: [(u64, u64); 3] = [(100, 103), (103, 106), (106, 108)];

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

/// Final blocks from the request's start block, or after its cursor, up to
/// its inclusive stop block.
#[derive(Clone)]
struct Stream {
    /// When not 0, the last block served: the stream then stays open and
    /// sends nothing more.
    stall_after: Arc<AtomicU64>,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        assert!(request.final_blocks_only);
        let first = match request.cursor.as_str() {
            "" => u64::try_from(request.start_block_num).unwrap(),
            cursor => {
                cursor
                    .strip_prefix("block-")
                    .and_then(|number| number.parse::<u64>().ok())
                    .unwrap_or_else(|| panic!("unexpected cursor {cursor}"))
                    + 1
            }
        };
        // The chain begins at FIRST, whatever start is requested.
        let stall_after = self.stall_after.load(Ordering::SeqCst);
        let last = match stall_after {
            0 => request.stop_block_num.min(LAST),
            stall_after => request.stop_block_num.min(LAST).min(stall_after),
        };
        let responses: Vec<_> = (first.max(FIRST)..=last)
            .map(|number| Ok(response(number)))
            .collect();
        Box::pin(async move {
            let blocks = futures::stream::iter(responses);
            Ok(tonic::Response::new(match stall_after {
                0 => Box::pin(blocks) as Self::ResponseStream,
                _ => Box::pin(futures::StreamExt::chain(
                    blocks,
                    futures::stream::pending(),
                )),
            }))
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

/// A final EVM block with one transaction, linked to its parent.
fn response(number: u64) -> firehose::Response {
    let id = number as u8;
    let block = eth::Block {
        number,
        hash: vec![id; 32].into(),
        header: Some(eth::BlockHeader {
            number,
            gas_used: 21_000,
            ..Default::default()
        }),
        transaction_traces: vec![eth::TransactionTrace {
            hash: vec![id ^ 0xff; 32].into(),
            status: 1,
            from: vec![0x11; 20].into(),
            to: vec![0x22; 20].into(),
            gas_used: 21_000,
            ..Default::default()
        }],
        ..Default::default()
    };
    firehose::Response {
        block: Some(prost_types::Any {
            type_url: "type.googleapis.com/sf.ethereum.type.v2.Block".into(),
            value: block.encode_to_vec(),
        }),
        step: 3,
        cursor: format!("block-{number}"),
        metadata: Some(firehose::BlockMetadata {
            num: number,
            id: format!("{id:02x}").repeat(32),
            parent_num: number - 1,
            parent_id: format!("{:02x}", id.wrapping_sub(1)).repeat(32),
            lib_num: number,
            time: Some(prost_types::Timestamp {
                seconds: MIDNIGHT - 102 + number as i64,
                nanos: 0,
            }),
        }),
    }
}

struct Fixture {
    cwd: PathBuf,
    endpoint: String,
    s3: s3::Server,
    /// The mock Firehose's [`Stream::stall_after`].
    stall_after: Arc<AtomicU64>,
}

impl Fixture {
    /// The mock Firehose (its server task is returned) and the loopback S3.
    async fn start(dir: &Path) -> (Self, tokio::task::JoinHandle<()>) {
        let cwd = std::fs::canonicalize(dir).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let stall_after = Arc::new(AtomicU64::new(0));
        let stream = Stream {
            stall_after: stall_after.clone(),
        };
        let firehose = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(Info)
                .add_service(stream)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let s3 = s3::Server::start(
            &cwd.join("tls"),
            BUCKET,
            s3::Latency {
                base_ms: 0,
                slow_every: 0,
                slow_ms: 0,
            },
        )
        .await
        .unwrap();
        (
            Self {
                cwd,
                endpoint,
                s3,
                stall_after,
            },
            firehose,
        )
    }

    /// One run of the maintenance job over `tables`; no table may fail.
    async fn maintain(&self, tables: &[String], settings: &[(&str, &str)]) -> common::JobRun {
        let mut env: Vec<(&str, String)> = vec![
            ("LAKE_ROOT", self.output()),
            ("LAKE_TABLES", tables.join(",")),
            ("S3_ENDPOINT", self.s3.endpoint.clone()),
            ("AWS_ACCESS_KEY_ID", "loopback-access-key".into()),
            ("AWS_SECRET_ACCESS_KEY", "loopback-secret-key".into()),
            ("AWS_REGION", "us-east-1".into()),
            ("SSL_CERT_FILE", self.s3.ca_file.to_str().unwrap().into()),
        ];
        env.extend(
            settings
                .iter()
                .map(|(key, value)| (*key, value.to_string())),
        );
        let run = common::maintenance_job(Path::new(env!("CARGO_BIN_EXE_fireparq")), &env).await;
        assert_eq!(run.status, 0, "{run:?}");
        assert_eq!(run.done()["failed"], serde_json::json!([]), "{run:?}");
        for table in run.tables() {
            assert_eq!(table["errors"], serde_json::json!([]), "{table}");
        }
        run
    }

    fn output(&self) -> String {
        format!("s3://{BUCKET}/{CHAIN}")
    }

    fn fireparq(&self) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
        command
            .kill_on_drop(true)
            .env_clear()
            .current_dir(&self.cwd)
            .env("AWS_ACCESS_KEY_ID", "loopback-access-key")
            .env("AWS_SECRET_ACCESS_KEY", "loopback-secret-key")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_ENDPOINT_URL_S3", &self.s3.endpoint)
            .env("SSL_CERT_FILE", &self.s3.ca_file);
        command
    }

    /// `build` (or `backfill open`) of `[start, stop)`, one transaction per block.
    fn ingest(&self, command: &[&str], range: Option<(u64, u64)>) -> tokio::process::Command {
        self.ingest_into(&self.output(), command, range)
    }

    fn ingest_into(
        &self,
        output: &str,
        command: &[&str],
        range: Option<(u64, u64)>,
    ) -> tokio::process::Command {
        let mut fireparq = self.fireparq();
        fireparq
            .args(command)
            .args(["--endpoint", &self.endpoint, "--block-type", "evm"])
            .args(["--flush-blocks", "1", "--stream-idle-timeout-secs", "0"])
            .args(["--output", output]);
        if let Some((start, stop)) = range {
            fireparq
                .args(["--start-block", &start.to_string()])
                .args(["--stop-block", &stop.to_string()]);
        }
        fireparq
    }

    fn lane(&self, (start, stop): (u64, u64)) -> tokio::process::Command {
        self.ingest(&["build"], Some((start, stop)))
    }

    fn open(&self, lane_blocks: u64) -> tokio::process::Command {
        let mut open = self.ingest(&["backfill", "open"], Some((FIRST, STOP)));
        open.args(["--lane-blocks", &lane_blocks.to_string()]);
        open
    }

    fn command(&self, args: &[&str]) -> tokio::process::Command {
        let mut fireparq = self.fireparq();
        fireparq.args(args);
        fireparq
    }

    async fn json(&self, args: &[&str]) -> Value {
        let output = succeed(self.command(args)).await;
        serde_json::from_slice(&output.stdout).unwrap_or_else(|_| panic!("{}", text(&output)))
    }

    async fn status(&self) -> Value {
        self.json(&["backfill", "status", &self.output()]).await
    }

    /// The dataset as a local directory: a copy of every object under it.
    fn local_copy(&self) -> PathBuf {
        self.copy_of(CHAIN)
    }

    /// A local copy of every object under `prefix`.
    fn copy_of(&self, prefix: &str) -> PathBuf {
        let copy = self.cwd.join(format!("s3-copy-{prefix}"));
        let _ = std::fs::remove_dir_all(&copy);
        for (key, bytes) in self.s3.objects(&format!("{prefix}/")) {
            let path = copy.join(key.strip_prefix(&format!("{prefix}/")).unwrap());
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
        copy
    }

    /// After an aborted lane: release its retained owner as an operator would.
    async fn release_lane(&self, (start, stop): (u64, u64)) {
        let lane = format!("{start}-{stop}");
        let status = self
            .json(&["recovery", "status", &self.output(), "--lane", &lane])
            .await;
        assert_eq!(
            status["ownership"], "owned",
            "an aborted lane keeps its owner"
        );
        succeed(self.command(&[
            "recovery",
            "release",
            &self.output(),
            "--lane",
            &lane,
            "--expected-owner",
            status["owner"]["owner_id"].as_str().unwrap(),
            "--expected-generation",
            &status["owner"]["generation"].to_string(),
            "--stopped-writer-evidence",
            "the test reaped the aborted lane process",
            "--provider-quiescence-evidence",
            "the loopback endpoint answered every request before the abort",
        ]))
        .await;
    }
}

async fn output(mut command: tokio::process::Command) -> Output {
    tokio::time::timeout(Duration::from_secs(120), command.output())
        .await
        .expect("fireparq timed out")
        .unwrap()
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

async fn succeed(command: tokio::process::Command) -> Output {
    let output = output(command).await;
    assert!(output.status.success(), "{}", text(&output));
    output
}

/// The command fails and says `expected`.
async fn refused(command: tokio::process::Command, expected: &str) {
    let output = output(command).await;
    let text = text(&output);
    assert!(!output.status.success(), "expected a refusal: {text}");
    assert!(text.contains(expected), "expected `{expected}` in: {text}");
}

/// Every `block_num` of `blocks`, sorted, and the `txn` application ids of
/// its log.
async fn blocks_and_writers(root: &Path) -> (Vec<i64>, BTreeSet<String>) {
    let read = common::delta_read(&common::open_local(root, "blocks").await).await;
    let blocks = common::delta_block_nums(&root.join("blocks"), &read);
    let writers = common::delta_log(&root.join("blocks"))
        .values()
        .filter_map(|actions| common::action(actions, "txn"))
        .map(|txn| txn["appId"].as_str().unwrap().to_owned())
        .collect();
    (blocks, writers)
}

/// The commits of `table`'s log that lanes or writers made: those with a
/// `txn`.
fn data_commits(root: &Path, table: &str) -> usize {
    common::delta_log(&root.join(table))
        .values()
        .filter(|actions| common::action(actions, "txn").is_some())
        .count()
}

/// The lane starting at `start` in a `backfill status` report.
fn lane_report(status: &Value, start: u64) -> &Value {
    status["incomplete"]
        .as_array()
        .unwrap()
        .iter()
        .find(|lane| lane["start"] == start)
        .unwrap_or_else(|| panic!("{status}"))
}

/// The one `fireparq.descriptor` every table of the dataset records.
fn dataset_descriptor(root: &Path) -> String {
    let descriptors: BTreeSet<String> = common::delta_tables(root)
        .iter()
        .map(|table| {
            let commits = common::delta_log(&root.join(table));
            let metadata = common::action(&commits[&0], "metaData").unwrap();
            metadata["configuration"]["fireparq.descriptor"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        descriptors.len(),
        1,
        "one dataset identity: {descriptors:?}"
    );
    descriptors.into_iter().next().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lanes_fill_one_dataset_in_parallel_and_seal_into_an_ordinary_one() {
    let dir = tempfile::tempdir().unwrap();
    let (fixture, firehose) = Fixture::start(dir.path()).await;
    let root = fixture.output();

    // Before a backfill is open, a lane-shaped build is just a new dataset;
    // status has nothing to report.
    refused(
        fixture.command(&["backfill", "status", &root]),
        "no parallel backfill was opened",
    )
    .await;

    // Open: only the plan is written. Opening the same backfill again changes
    // nothing; a different one is refused.
    succeed(fixture.open(3)).await;
    succeed(fixture.open(3)).await;
    refused(fixture.open(4), "another backfill is already open").await;
    let status = fixture.status().await;
    assert_eq!(status["sealed"], false);
    assert_eq!(
        (
            &status["lanes"],
            &status["not_started"],
            &status["complete"]
        ),
        (&3.into(), &3.into(), &0.into()),
        "{status}"
    );

    // Until the seal only planned lanes write: no live writer, no other
    // range, and no seal of an incomplete backfill.
    refused(fixture.ingest(&["build"], None), "not sealed").await;
    refused(fixture.lane((FIRST, STOP)), "not a lane").await;
    refused(fixture.lane((101, 104)), "not a lane").await;
    refused(
        fixture.command(&["backfill", "seal", &root]),
        "has not started",
    )
    .await;
    // The plan binds the default, without the opt-in EVM tables: a lane with
    // one of their flags is refused, and the refusal names the flag.
    for flag in [
        "--with-gas-changes",
        "--with-account-creations",
        "--with-system-nonce-changes",
    ] {
        let mut lane = fixture.lane(LANES[1]);
        lane.arg(flag);
        refused(lane, &format!("run it without {flag}")).await;
    }

    // The last two lanes run at the same time, into the same tables.
    let (second, third) = tokio::join!(
        output(fixture.lane(LANES[1])),
        output(fixture.lane(LANES[2]))
    );
    assert!(second.status.success(), "{}", text(&second));
    assert!(third.status.success(), "{}", text(&third));

    // The first lane dies after journaling its first transaction as
    // Committed, before any Delta commit. Its owner is kept: the lane cannot
    // run again until it is released, while the dataset is otherwise untouched.
    let mut crashed = fixture.lane(LANES[0]);
    crashed.env("FIREPARQ_DEBUG_FAULT", "crash-at:CommittedPersisted");
    assert!(!output(crashed).await.status.success());
    let status = fixture.status().await;
    assert_eq!(
        (&status["complete"], &status["running"]),
        (&2.into(), &1.into()),
        "{status}"
    );
    refused(fixture.lane(LANES[0]), "owned by another worker").await;
    refused(
        fixture.command(&["backfill", "seal", &root]),
        "100-103 is running",
    )
    .await;
    fixture.release_lane(LANES[0]).await;
    let status = fixture.status().await;
    assert_eq!(status["stopped"], 1, "{status}");

    // Rerunning the lane rolls its journal forward and finishes it; running a
    // complete lane again is a no-op.
    succeed(fixture.lane(LANES[0])).await;
    succeed(fixture.lane(LANES[0])).await;
    let status = fixture.status().await;
    assert_eq!(status["complete"], 3, "{status}");
    assert_eq!(status["sealed"], false);

    // Every lane committed under its own application id to tables that carry
    // one identity, and the lanes' blocks are all there exactly once.
    let copy = fixture.local_copy();
    let descriptor = dataset_descriptor(&copy);
    let (blocks, writers) = blocks_and_writers(&copy).await;
    assert_eq!(blocks, (FIRST as i64..STOP as i64).collect::<Vec<_>>());
    assert_eq!(
        writers.len(),
        LANES.len(),
        "one `txn` appId per lane: {writers:?}"
    );
    assert!(!writers.contains(&format!("fireparq:{descriptor}")));
    // A lane commits its flushes together: one commit per lane here, where
    // every block is a flush, and one more by the restart of the crashed
    // lane for the flush it had journaled.
    assert_eq!(data_commits(&copy, "blocks"), LANES.len() + 1);
    assert_eq!(data_commits(&copy, "transactions"), LANES.len() + 1);
    assert!(
        !copy.join(".fireparq-ingest/state.json").exists(),
        "an unsealed dataset has no authority"
    );
    assert!(
        !copy.join("_fireparq").exists(),
        "lanes write no cursor mirror"
    );

    // The seal reads `blocks` itself: with one block's file holding another
    // block, the range is not all there and nothing is sealed.
    let files: Vec<_> = fixture
        .s3
        .objects(&format!("{CHAIN}/blocks/"))
        .into_iter()
        .filter(|(key, _)| key.ends_with(".parquet") && !key.contains("_delta_log"))
        .collect();
    assert_eq!(files.len() as u64, STOP - FIRST);
    fixture.s3.put(&files[0].0, files[1].1.clone());
    let seal = output(fixture.command(&["backfill", "seal", &root])).await;
    assert!(!seal.status.success());
    for expected in ["the dataset was not sealed", "stored more than once"] {
        assert!(text(&seal).contains(expected), "{}", text(&seal));
    }
    assert_eq!(fixture.status().await["sealed"], false);
    fixture.s3.put(&files[0].0, files[0].1.clone());

    // Seal: checks `blocks` against the range and writes the authority.
    let sealed = fixture.json(&["backfill", "seal", &root]).await;
    assert_eq!(sealed["sealed_now"], true, "{sealed}");
    assert_eq!(sealed["last_block"], STOP - 1);
    let again = fixture.json(&["backfill", "seal", &root]).await;
    assert_eq!(again["sealed_now"], false);
    assert_eq!(fixture.status().await["sealed"], true);
    let bucket = fixture.json(&["recovery", "status", &root]).await;
    assert_eq!(bucket["ownership"], "released", "{bucket}");
    assert_eq!(bucket["state"]["present"], true);

    // The sealed dataset is an ordinary one: its start is the backfill's, a
    // lane or any other start is refused, and the same range is complete.
    refused(fixture.lane(LANES[1]), "original start").await;
    let mut gas_changes = fixture.lane((FIRST, STOP));
    gas_changes.arg("--with-gas-changes");
    refused(gas_changes, "run it without --with-gas-changes").await;
    succeed(fixture.lane((FIRST, STOP))).await;

    // An ordinary build extends it from the stored cursor, under the
    // dataset's own application id.
    succeed(fixture.lane((FIRST, LAST + 1))).await;
    let copy = fixture.local_copy();
    let (blocks, writers) = blocks_and_writers(&copy).await;
    assert_eq!(blocks, (FIRST as i64..=LAST as i64).collect::<Vec<_>>());
    assert_eq!(writers.len(), LANES.len() + 1, "{writers:?}");
    assert!(writers.contains(&format!("fireparq:{descriptor}")));
    assert_eq!(dataset_descriptor(&copy), descriptor);
    assert!(copy.join("_fireparq/cursor.parquet").exists());
    let state: Value =
        serde_json::from_slice(&std::fs::read(copy.join(".fireparq-ingest/state.json")).unwrap())
            .unwrap();
    let checkpoint = &state["payload"]["checkpoint"];
    assert_eq!(checkpoint["event"]["block_num"], LAST);
    assert_eq!(checkpoint["completed_stop"], LAST + 1);
    assert_eq!(state["payload"]["descriptor"]["origin_start"], FIRST);

    // It holds exactly the rows one sequential writer puts in every table.
    let single = format!("s3://{BUCKET}/single-writer");
    succeed(fixture.ingest_into(&single, &["build"], Some((FIRST, LAST + 1)))).await;
    let single = fixture.copy_of("single-writer");
    let tables = common::delta_tables(&copy);
    assert_eq!(tables, common::delta_tables(&single));
    let rows = common::delta_counts(&copy, &tables).await;
    assert_eq!(rows, common::delta_counts(&single, &tables).await);
    assert_eq!(rows["blocks"], LAST - FIRST + 1);
    assert_eq!(rows["transactions"], LAST - FIRST + 1);

    // The whole table is continuous, parent links included.
    succeed(fixture.command(&["validate", &format!("{root}/blocks"), "--cross-partition"])).await;

    // A backfill opened below the chain's first block: its first lane fails
    // at the first block it receives, long before any seal.
    let early = format!("s3://{BUCKET}/early-start");
    let range = Some((FIRST - 2, FIRST + 2));
    let mut open = fixture.ingest_into(&early, &["backfill", "open"], range);
    open.args(["--lane-blocks", "4"]);
    succeed(open).await;
    refused(
        fixture.ingest_into(&early, &["build"], range),
        &format!("expected block {} but received block {FIRST}", FIRST - 2),
    )
    .await;
    firehose.abort();
}

/// The maintenance job's modes: a lite VACUUM that deletes replaced files at
/// once, the same compacting every date, and the weekly full VACUUM.
const MAINTENANCE: [&[(&str, &str)]; 3] = [
    &[("VACUUM_RETENTION_HOURS", "0")],
    &[("VACUUM_RETENTION_HOURS", "0"), ("OPTIMIZE_DATES", "all")],
    &[("FULL_VACUUM", "1")],
];

/// A lane dies between two tables' commits of its flushes. Before it is
/// rerun, the maintenance job compacts, vacuums and checkpoints the shared
/// tables in every mode, also while another lane writes. The lane's recovery
/// must find its own `txn` in the rewritten logs: commit the tables that
/// lack its parts, and no table twice. The parts no log holds yet must
/// survive every VACUUM meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_lane_recovers_over_tables_the_maintenance_job_rewrote() {
    let dir = tempfile::tempdir().unwrap();
    let (fixture, firehose) = Fixture::start(dir.path()).await;
    let root = fixture.output();
    succeed(fixture.open(3)).await;

    // The last lane fills 2023-11-15 with blocks 106 and 107. The middle
    // lane then accepts its three blocks of the same date and dies while
    // committing them, once `transactions` has its commit: `blocks`, which
    // commits last, has not.
    succeed(fixture.lane(LANES[2])).await;
    let mut crashed = fixture.lane(LANES[1]);
    crashed.env(
        "FIREPARQ_DEBUG_FAULT",
        "crash-after-delta-commit:transactions",
    );
    assert!(!output(crashed).await.status.success());
    let copy = fixture.local_copy();
    let tables = common::delta_tables(&copy);
    let block_nums = |root: PathBuf, table: &'static str| async move {
        let read = common::delta_read(&common::open_local(&root, table).await).await;
        common::delta_block_nums(&root.join(table), &read)
    };
    assert_eq!(
        block_nums(copy.clone(), "transactions").await,
        [103, 104, 105, 106, 107]
    );
    assert_eq!(block_nums(copy, "blocks").await, [106, 107]);
    // The lane accepted every block and still owes them to the logs.
    let status = fixture.status().await;
    let lane = lane_report(&status, LANES[1].0);
    assert_eq!(lane["last_block"], LANES[1].1 - 1, "{status}");
    assert_eq!(lane["uncommitted_flushes"], 3, "{status}");
    refused(
        fixture.command(&["backfill", "seal", &root]),
        "103-106 is running",
    )
    .await;

    // Maintenance in every mode: `OPTIMIZE_DATES=all` compacts the date all
    // lanes share, the crashed lane's committed part with it, and the lite
    // VACUUM deletes the replaced files at once.
    let mut compacted = 0;
    for mode in MAINTENANCE {
        let run = fixture.maintain(&tables, mode).await;
        assert_eq!(run.done()["conflicts"], 0, "{run:?}");
        compacted += run.table("transactions")["compacted"]
            .as_array()
            .unwrap()
            .len();
    }
    assert!(compacted > 0, "the crashed lane's part was not compacted");

    // The first lane writes while the job runs again.
    let first = tokio::spawn(output(fixture.lane(LANES[0])));
    for mode in MAINTENANCE {
        fixture.maintain(&tables, mode).await;
    }
    let first = first.await.unwrap();
    assert!(first.status.success(), "{}", text(&first));

    // The crashed lane is released and rerun: it commits its parts to the
    // tables that lack them, compacted meanwhile, and finishes, beside the
    // job.
    fixture.release_lane(LANES[1]).await;
    let rerun = tokio::spawn(output(fixture.lane(LANES[1])));
    for mode in MAINTENANCE {
        fixture.maintain(&tables, mode).await;
    }
    let rerun = rerun.await.unwrap();
    assert!(rerun.status.success(), "{}", text(&rerun));
    for mode in MAINTENANCE {
        let run = fixture.maintain(&tables, mode).await;
        assert_eq!(run.done()["conflicts"], 0, "{run:?}");
    }

    // The seal's own check passes, and every table holds each block's rows
    // exactly once: the rows of one sequential writer.
    let sealed = fixture.json(&["backfill", "seal", &root]).await;
    assert_eq!(sealed["sealed_now"], true, "{sealed}");
    let copy = fixture.local_copy();
    let (blocks, writers) = blocks_and_writers(&copy).await;
    assert_eq!(blocks, (FIRST as i64..STOP as i64).collect::<Vec<_>>());
    assert_eq!(writers.len(), LANES.len(), "{writers:?}");
    let single = format!("s3://{BUCKET}/single-writer");
    succeed(fixture.ingest_into(&single, &["build"], Some((FIRST, STOP)))).await;
    let single = fixture.copy_of("single-writer");
    assert_eq!(tables, common::delta_tables(&single));
    let rows = common::delta_counts(&copy, &tables).await;
    assert_eq!(rows, common::delta_counts(&single, &tables).await);
    assert_eq!(rows["blocks"], STOP - FIRST);
    for table in ["blocks", "transactions"] {
        let read = common::delta_read(&common::open_local(&copy, table).await).await;
        let mut stored = common::delta_block_nums(&copy.join(table), &read);
        stored.sort_unstable();
        assert_eq!(stored, blocks, "{table}");
    }
    succeed(fixture.command(&["validate", &format!("{root}/blocks"), "--cross-partition"])).await;
    firehose.abort();
}

/// A lane of eight flushes that commits every three of them. Its first run
/// dies inside the first batch, between two tables. The rerun finishes that
/// batch from each table's `txn` and goes on: every block is in every table
/// once, in three commits per table instead of eight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lane_commits_its_flushes_in_batches_and_finishes_one_it_died_in() {
    let dir = tempfile::tempdir().unwrap();
    let (fixture, firehose) = Fixture::start(dir.path()).await;
    let root = fixture.output();
    let lane = (FIRST, STOP);
    succeed(fixture.open(STOP - FIRST)).await;
    let batched = || {
        let mut build = fixture.lane(lane);
        build.args(["--lane-commit-flushes", "3"]);
        build
    };

    let mut crashed = batched();
    crashed.env(
        "FIREPARQ_DEBUG_FAULT",
        "crash-after-delta-commit:transactions",
    );
    assert!(!output(crashed).await.status.success());
    let copy = fixture.local_copy();
    let tables = common::delta_tables(&copy);
    assert_eq!(
        common::delta_counts(&copy, &tables).await["transactions"],
        3
    );
    assert_eq!(common::delta_counts(&copy, &tables).await["blocks"], 0);
    let status = fixture.status().await;
    let report = lane_report(&status, FIRST);
    assert_eq!(report["last_block"], FIRST + 2, "{status}");
    assert_eq!(report["uncommitted_flushes"], 3, "{status}");

    // A release that commits every flush cannot read this lane's state, so
    // it cannot drop the parts the lane owes. Here: the lane's own rerun.
    fixture.release_lane(lane).await;
    succeed(batched()).await;
    let status = fixture.status().await;
    assert_eq!(status["complete"], 1, "{status}");

    let copy = fixture.local_copy();
    let (blocks, writers) = blocks_and_writers(&copy).await;
    assert_eq!(blocks, (FIRST as i64..STOP as i64).collect::<Vec<_>>());
    assert_eq!(writers.len(), 1, "{writers:?}");
    let rows = common::delta_counts(&copy, &tables).await;
    assert_eq!(rows["blocks"], STOP - FIRST);
    assert_eq!(rows["transactions"], STOP - FIRST);
    // Blocks 100..=102, then 103..=105, then 106 and 107 at the lane's end.
    for table in ["blocks", "transactions"] {
        assert_eq!(data_commits(&copy, table), 3, "{table}");
    }
    let state: Value = serde_json::from_slice(
        &std::fs::read(copy.join(format!(
            ".fireparq-ingest/lanes/{}-{}/state.json",
            lane.0, lane.1
        )))
        .unwrap(),
    )
    .unwrap();
    assert!(
        state["payload"].get("deferred").is_none(),
        "a complete lane owes nothing: {state}"
    );

    let sealed = fixture.json(&["backfill", "seal", &root]).await;
    assert_eq!(sealed["sealed_now"], true, "{sealed}");
    succeed(fixture.command(&["validate", &format!("{root}/blocks"), "--cross-partition"])).await;
    firehose.abort();
}

/// A lane is stopped by SIGTERM with two flushes accepted and a fifth block
/// only buffered. It commits the two flushes before it exits, publishes
/// nothing of the buffered block, releases its owner and is not complete.
/// The rerun continues after the last accepted block.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lane_stopped_by_sigterm_commits_what_it_accepted_and_is_released() {
    let dir = tempfile::tempdir().unwrap();
    let (fixture, firehose) = Fixture::start(dir.path()).await;
    let root = fixture.output();
    let lane = (FIRST, STOP);
    let lane_state = format!(
        "{CHAIN}/.fireparq-ingest/lanes/{}-{}/state.json",
        lane.0, lane.1
    );
    succeed(fixture.open(STOP - FIRST)).await;
    // Two blocks a flush: 100 and 101, then 102 and 103. Block 104 arrives
    // and stays in memory, since the stream then goes quiet.
    let build = || {
        let mut build = fixture.fireparq();
        build
            .args(["build", "--endpoint", &fixture.endpoint])
            .args(["--block-type", "evm", "--flush-blocks", "2"])
            .args(["--stream-idle-timeout-secs", "0", "--output", &root])
            .args(["--start-block", &lane.0.to_string()])
            .args(["--stop-block", &lane.1.to_string()]);
        build
    };
    fixture.stall_after.store(FIRST + 4, Ordering::SeqCst);
    let child = build()
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let state = || -> Option<Value> {
        let (_, bytes) = fixture
            .s3
            .objects(&lane_state)
            .into_iter()
            .find(|(key, _)| *key == lane_state)?;
        serde_json::from_slice(&bytes).ok()
    };
    let accepted = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if let Some(state) = state() {
                if state["payload"]["checkpoint"]["event"]["block_num"] == FIRST + 3 {
                    break state;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the lane did not accept its second flush");
    // Both flushes are accepted and owed; no log has them.
    assert_eq!(
        accepted["payload"]["deferred"].as_array().unwrap().len(),
        2,
        "{accepted}"
    );
    let status = fixture.status().await;
    let report = lane_report(&status, FIRST);
    assert_eq!(report["state"], "running", "{status}");
    assert_eq!(report["uncommitted_flushes"], 2, "{status}");
    assert_eq!(data_commits(&fixture.local_copy(), "blocks"), 0);

    let pid = child.id().unwrap().to_string();
    assert!(std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap()
        .success());
    let stopped = tokio::time::timeout(Duration::from_secs(120), child.wait_with_output())
        .await
        .expect("the lane did not stop on SIGTERM")
        .unwrap();
    assert!(stopped.status.success(), "{}", text(&stopped));

    // What it accepted is in every table's log, in one commit, and nothing
    // is owed. The buffered block is nowhere: not in a log, not as a file,
    // not in a journal.
    let copy = fixture.local_copy();
    let (blocks, _) = blocks_and_writers(&copy).await;
    assert_eq!(blocks, (FIRST as i64..FIRST as i64 + 4).collect::<Vec<_>>());
    for table in ["blocks", "transactions"] {
        assert_eq!(data_commits(&copy, table), 1, "{table}");
        let files = fixture
            .s3
            .objects(&format!("{CHAIN}/{table}/"))
            .into_iter()
            .filter(|(key, _)| key.ends_with(".parquet") && !key.contains("_delta_log"))
            .count();
        assert_eq!(files, 2, "{table}: one part per accepted flush");
    }
    let settled = state().unwrap();
    assert!(settled["payload"].get("deferred").is_none(), "{settled}");
    let checkpoint = &settled["payload"]["checkpoint"];
    assert_eq!(checkpoint["event"]["block_num"], FIRST + 3);
    assert!(checkpoint["completed_stop"].is_null(), "{settled}");
    // A cleared journal slot is kept as a tombstone.
    let journal: Value = serde_json::from_slice(
        &std::fs::read(copy.join(format!(
            ".fireparq-ingest/lanes/{}-{}/pending.json",
            lane.0, lane.1
        )))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(journal["deleted"], true, "a clean stop leaves no journal");

    // The lane is released and not complete: it can be rerun at once, and
    // the backfill cannot be sealed.
    let status = fixture.status().await;
    assert_eq!(
        (&status["stopped"], &status["complete"]),
        (&1.into(), &0.into())
    );
    let report = lane_report(&status, FIRST);
    assert_eq!(report["owner"], "released", "{status}");
    assert_eq!(report["last_block"], FIRST + 3, "{status}");
    assert!(report.get("uncommitted_flushes").is_none(), "{status}");
    let owner = fixture
        .json(&["recovery", "status", &root, "--lane", "100-108"])
        .await;
    assert_eq!(owner["ownership"], "released", "{owner}");
    refused(
        fixture.command(&["backfill", "seal", &root]),
        "100-108 stopped before its end",
    )
    .await;

    // The rerun continues from block 104 and completes.
    fixture.stall_after.store(0, Ordering::SeqCst);
    succeed(build()).await;
    let copy = fixture.local_copy();
    let (blocks, writers) = blocks_and_writers(&copy).await;
    assert_eq!(blocks, (FIRST as i64..STOP as i64).collect::<Vec<_>>());
    assert_eq!(writers.len(), 1, "{writers:?}");
    assert_eq!(data_commits(&copy, "blocks"), 2);
    let sealed = fixture.json(&["backfill", "seal", &root]).await;
    assert_eq!(sealed["sealed_now"], true, "{sealed}");
    succeed(fixture.command(&["validate", &format!("{root}/blocks"), "--cross-partition"])).await;
    firehose.abort();
}
