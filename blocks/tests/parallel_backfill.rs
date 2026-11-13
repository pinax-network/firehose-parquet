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
//! - `seal` refuses until every lane is complete;
//! - a lane that does not receive its first block fails;
//! - the sealed dataset holds every block exactly once, `validate` passes
//!   across the lanes, and a `build` continues from the stored cursor under
//!   the dataset's own application id, refusing any other start.
use firehose_protos::{eth, firehose};
use prost::Message;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Output;
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
struct Stream;
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
        let responses: Vec<_> = (first.max(FIRST)..=request.stop_block_num.min(LAST))
            .map(|number| Ok(response(number)))
            .collect();
        Box::pin(async move {
            Ok(tonic::Response::new(
                Box::pin(futures::stream::iter(responses)) as Self::ResponseStream,
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
}

impl Fixture {
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
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let firehose = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Info)
            .add_service(Stream)
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
    let fixture = Fixture { cwd, endpoint, s3 };
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
