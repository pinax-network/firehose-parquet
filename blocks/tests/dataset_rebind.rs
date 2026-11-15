//! #635: `fireparq recovery rebind` relocates a stopped dataset, run by the
//! real `fireparq` binary on local disk and on the loopback HTTPS S3 endpoint
//! of `examples/bench_live_flush/s3.rs`.
//!
//! A mock Firehose serves final EVM blocks from 100. `build` writes blocks
//! 100..=102 (cursor mirror at its default, `_fireparq/cursor.parquet`) into
//! `chain`, which is then copied file by file to `chain-v1` (`cp -R`, or each
//! object of the prefix, as `aws s3 sync` would; the loopback endpoint serves
//! one bucket). The test checks that:
//!
//! - `build` refuses the copy and names the rebind command;
//! - `recovery rebind chain-v1 --from chain` rebinds every table, `blocks`
//!   last, and the mirror, and a rerun changes nothing;
//! - the copy then resumes from the stored cursor: `build` adds blocks
//!   103..=104 with continuous ordinals, `recovery recover` and the
//!   maintenance job (`LAKE_ROOT` at the copy) run clean, and its Delta
//!   tables hold every block once, read by delta-rs and, when available,
//!   DuckDB's `delta_scan`;
//! - the root it was copied from is unchanged, and still resumes on its own.
use firehose_protos::{eth, firehose};
use prost::Message;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

#[path = "../examples/bench_live_flush/s3.rs"]
#[allow(dead_code)]
mod s3;

mod common;

const CHAIN: &str = "rebind-chain";
const BUCKET: &str = "rebind-lake";
/// 2023-11-15T00:00:00Z: blocks 100 and 101 are on 2023-11-14, later blocks
/// on 2023-11-15.
const MIDNIGHT: i64 = 1_700_006_400;
/// The tables with rows, and their rows per block.
const ROW_TABLES: [(&str, u64); 4] = [
    ("access_lists", 2),
    ("blocks", 1),
    ("logs", 2),
    ("transactions", 2),
];

#[derive(Clone)]
struct Info;
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: CHAIN.into(),
                first_streamable_block_num: 100,
                ..Default::default()
            }))
        })
    }
}

/// Final blocks from 100 up to the request's stop; a request with a cursor
/// resumes after it.
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
        let after = match request.cursor.as_str() {
            "" => 99,
            cursor => cursor
                .strip_prefix("block-")
                .and_then(|number| number.parse().ok())
                .unwrap_or_else(|| panic!("unexpected cursor {cursor}")),
        };
        let responses =
            futures::stream::iter((after + 1..=request.stop_block_num).map(|n| Ok(response(n))));
        Box::pin(async move {
            Ok(tonic::Response::new(
                Box::pin(responses) as Self::ResponseStream
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

/// A final EVM block with two transactions, each with a log and an access list.
fn response(number: u64) -> firehose::Response {
    let id = number as u8;
    let transaction = |index: u32| eth::TransactionTrace {
        hash: vec![id ^ (index as u8 + 1); 32].into(),
        index,
        status: 1,
        from: vec![0x11; 20].into(),
        to: vec![0x22; 20].into(),
        gas_used: 21_000,
        access_list: vec![eth::AccessTuple {
            address: vec![0x33; 20].into(),
            storage_keys: vec![vec![0x44; 32].into()],
        }],
        receipt: Some(eth::TransactionReceipt {
            logs: vec![eth::Log {
                address: vec![0x66; 20].into(),
                topics: vec![vec![0x77; 32].into()],
                data: vec![1, 2, 3].into(),
                index: 0,
                block_index: index,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    let block = eth::Block {
        number,
        hash: vec![id; 32].into(),
        header: Some(eth::BlockHeader {
            number,
            gas_used: 42_000,
            ..Default::default()
        }),
        transaction_traces: vec![transaction(0), transaction(1)],
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

/// Where the datasets are: a local directory of roots, or the loopback
/// bucket, whose roots are prefixes.
enum Lake {
    Local(PathBuf),
    S3(s3::Server),
}

impl Lake {
    async fn new(s3: bool, cwd: &Path) -> Self {
        if !s3 {
            return Lake::Local(cwd.join("lake"));
        }
        let latency = s3::Latency {
            base_ms: 0,
            slow_every: 0,
            slow_ms: 0,
        };
        Lake::S3(
            s3::Server::start(&cwd.join("tls"), BUCKET, latency)
                .await
                .unwrap(),
        )
    }

    fn output(&self, root: &str) -> String {
        match self {
            Lake::Local(lake) => lake.join(root).to_str().unwrap().to_string(),
            Lake::S3(_) => format!("s3://{BUCKET}/{root}"),
        }
    }

    /// Every file of `root`, by path relative to it.
    fn files(&self, root: &str) -> BTreeMap<String, Vec<u8>> {
        match self {
            Lake::Local(lake) => {
                fn walk(root: &Path, dir: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
                    for entry in std::fs::read_dir(dir).unwrap() {
                        let path = entry.unwrap().path();
                        if path.is_dir() {
                            walk(root, &path, files);
                        } else {
                            let relative = path.strip_prefix(root).unwrap();
                            let relative = relative.to_str().unwrap().to_string();
                            files.insert(relative, std::fs::read(&path).unwrap());
                        }
                    }
                }
                let mut files = BTreeMap::new();
                walk(&lake.join(root), &lake.join(root), &mut files);
                files
            }
            Lake::S3(server) => server
                .objects(&format!("{root}/"))
                .into_iter()
                .map(|(key, bytes)| {
                    let relative = key.strip_prefix(&format!("{root}/")).unwrap();
                    (relative.to_string(), bytes.to_vec())
                })
                .collect(),
        }
    }

    /// Copies `from` to `to`, file by file.
    fn copy(&self, from: &str, to: &str) {
        for (relative, bytes) in self.files(from) {
            match self {
                Lake::Local(lake) => {
                    let path = lake.join(to).join(relative);
                    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                    std::fs::write(path, bytes).unwrap();
                }
                Lake::S3(server) => server.put(&format!("{to}/{relative}"), bytes.into()),
            }
        }
    }

    /// `root` as a local directory: the root itself, or a fresh copy of every
    /// object under its prefix.
    fn local(&self, root: &str, cwd: &Path) -> PathBuf {
        match self {
            Lake::Local(lake) => lake.join(root),
            Lake::S3(_) => {
                let copy = cwd.join("s3-copy").join(root);
                let _ = std::fs::remove_dir_all(&copy);
                for (relative, bytes) in self.files(root) {
                    let path = copy.join(relative);
                    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                    std::fs::write(path, bytes).unwrap();
                }
                copy
            }
        }
    }

    /// A `fireparq` command in `cwd` with a cleared environment and, for S3,
    /// the loopback endpoint's credentials and CA.
    fn fireparq(&self, cwd: &Path) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
        command.kill_on_drop(true).env_clear().current_dir(cwd);
        if let Lake::S3(server) = self {
            command
                .env("AWS_ACCESS_KEY_ID", "loopback-access-key")
                .env("AWS_SECRET_ACCESS_KEY", "loopback-secret-key")
                .env("AWS_REGION", "us-east-1")
                .env("AWS_ENDPOINT_URL_S3", &server.endpoint)
                .env("SSL_CERT_FILE", &server.ca_file);
        }
        command
    }

    /// The maintenance job's environment for `root`, as a CronJob would set
    /// it.
    fn maintenance_env(&self, root: &str, tables: &[String]) -> Vec<(&'static str, String)> {
        let mut env = vec![
            ("LAKE_ROOT", self.output(root)),
            ("LAKE_TABLES", tables.join(",")),
        ];
        if let Lake::S3(server) = self {
            env.extend([
                ("S3_ENDPOINT", server.endpoint.clone()),
                ("AWS_ACCESS_KEY_ID", "loopback-access-key".into()),
                ("AWS_SECRET_ACCESS_KEY", "loopback-secret-key".into()),
                ("AWS_REGION", "us-east-1".into()),
                ("SSL_CERT_FILE", server.ca_file.to_str().unwrap().into()),
            ]);
        }
        env
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

/// One `build` of `root` to `stop` (exclusive) from a mock Firehose.
async fn run(lake: &Lake, cwd: &Path, root: &str, stop: u64) -> Output {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Info)
            .add_service(Stream)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let mut command = lake.fireparq(cwd);
    command
        .args(["build", "--endpoint", &endpoint, "--block-type", "evm"])
        .args(["--start-block", "100", "--stop-block", &stop.to_string()])
        .args(["--flush-blocks", "1", "--stream-idle-timeout-secs", "0"])
        .args(["--output", &lake.output(root)]);
    let result = output(command).await;
    server.abort();
    result
}

async fn build(lake: &Lake, cwd: &Path, root: &str, stop: u64) {
    let result = run(lake, cwd, root, stop).await;
    assert!(result.status.success(), "{}", text(&result));
}

/// `fireparq recovery <args>`, which must succeed, and its JSON report.
async fn recovery(lake: &Lake, cwd: &Path, args: &[&str]) -> Value {
    let mut command = lake.fireparq(cwd);
    command.arg("recovery").args(args);
    let result = output(command).await;
    assert!(result.status.success(), "{}", text(&result));
    serde_json::from_slice(&result.stdout).unwrap_or_else(|_| panic!("{}", text(&result)))
}

/// A control record's payload, unless it is absent or deleted.
fn control(root: &Path, name: &str) -> Option<Value> {
    let bytes = std::fs::read(root.join(".fireparq-ingest").join(name)).ok()?;
    let record: Value = serde_json::from_slice(&bytes).unwrap();
    (record["deleted"] == false).then(|| record["payload"].clone())
}

/// The last value of `kind` in the `kind` actions of `table`'s log, in log
/// order, and the `txn` versions of `app_id`.
fn log_summary(root: &Path, table: &str, app_id: &str) -> (Value, Vec<i64>) {
    let log = common::delta_log(&root.join(table));
    let mut metadata = Value::Null;
    let mut txns = Vec::new();
    for actions in log.values() {
        if let Some(found) = common::action(actions, "metaData") {
            metadata = found.clone();
        }
        if let Some(txn) = common::action(actions, "txn") {
            if txn["appId"] == app_id {
                txns.push(txn["version"].as_i64().unwrap());
            }
        }
    }
    (metadata, txns)
}

fn scratch() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    (dir, cwd)
}

async fn a_copied_dataset_resumes_at_its_new_root_after_a_rebind(s3: bool) {
    let (_dir, cwd) = scratch();
    let lake = Lake::new(s3, &cwd).await;
    build(&lake, &cwd, "chain", 103).await;
    lake.copy("chain", "chain-v1");
    let original = lake.files("chain");
    let (copy, source) = (lake.output("chain-v1"), lake.output("chain"));

    // The copy is bound to `chain`: refused, naming the command.
    let refused = run(&lake, &cwd, "chain-v1", 105).await;
    assert!(!refused.status.success());
    let hint = format!("fireparq recovery rebind {copy} --from {source}");
    assert!(text(&refused).contains(&hint), "{}", text(&refused));

    let rebound = recovery(&lake, &cwd, &["rebind", &copy, "--from", &source]).await;
    assert_eq!(rebound["rebound_now"], true, "{rebound}");
    assert_eq!(rebound["ordinal"], 3, "{rebound}");
    assert_eq!(rebound["mirror"], "repaired", "{rebound}");
    let tables: Vec<String> = rebound["tables_updated"]
        .as_array()
        .unwrap()
        .iter()
        .map(|table| table.as_str().unwrap().to_string())
        .collect();
    assert_eq!(tables.last().map(String::as_str), Some("blocks"));
    let mut sorted = tables.clone();
    sorted.sort();
    assert_eq!(sorted, common::delta_tables(&lake.local("chain-v1", &cwd)));
    let again = recovery(&lake, &cwd, &["rebind", &copy, "--from", &source]).await;
    assert_eq!(again["rebound_now"], false, "{again}");
    assert_eq!(again["tables_updated"], serde_json::json!([]), "{again}");
    assert_eq!(again["mirror"], "unchanged", "{again}");

    // The copy resumes from its stored cursor, recovers and is maintained.
    build(&lake, &cwd, "chain-v1", 105).await;
    recovery(&lake, &cwd, &["recover", &copy]).await;
    let run = common::maintenance_job(
        &common::maintenance_bin(),
        &lake.maintenance_env("chain-v1", &sorted),
    )
    .await;
    run.assert_clean();

    let root = lake.local("chain-v1", &cwd);
    let state = control(&root, "state.json").unwrap();
    assert_eq!(state["checkpoint"]["ordinal"], 5);
    assert_eq!(state["checkpoint"]["event"]["block_num"], 104);
    assert!(control(&root, "pending.json").is_none());
    let stream = rebound["stream_id"].as_str().unwrap();
    let app_id = format!("fireparq:{stream}");
    for table in &sorted {
        let (metadata, txns) = log_summary(&root, table, &app_id);
        assert_eq!(
            metadata["configuration"]["fireparq.descriptor"], stream,
            "{table}"
        );
        // The rebind's baseline, then one commit per transaction with rows.
        assert_eq!(txns.first(), Some(&3), "{table}");
        if ROW_TABLES.iter().any(|(name, _)| name == table) {
            assert_eq!(txns, [3, 4, 5], "{table}");
        }
    }
    let counts = common::delta_counts(&root, &sorted).await;
    for (table, per_block) in ROW_TABLES {
        assert_eq!(counts[table], 5 * per_block, "{table}");
    }
    let blocks = common::delta_read(&common::open_local(&root, "blocks").await).await;
    let mut numbers = common::delta_block_nums(&root.join("blocks"), &blocks);
    numbers.sort_unstable();
    assert_eq!(numbers, [100, 101, 102, 103, 104]);
    match common::DuckDb::open(&cwd) {
        Some(duckdb) => assert_eq!(common::duckdb_counts(&duckdb, &root, &sorted), counts),
        None => assert!(
            std::env::var_os("FIREPARQ_REQUIRE_DUCKDB").is_none(),
            "DuckDB is required"
        ),
    }

    // The source was only read, and resumes at its own root.
    assert_eq!(lake.files("chain"), original);
    build(&lake, &cwd, "chain", 104).await;
    let state = control(&lake.local("chain", &cwd), "state.json").unwrap();
    assert_eq!(state["checkpoint"]["ordinal"], 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copied_local_dataset_resumes_at_its_new_root_after_a_rebind() {
    a_copied_dataset_resumes_at_its_new_root_after_a_rebind(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dataset_copied_to_a_new_s3_prefix_resumes_there_after_a_rebind() {
    a_copied_dataset_resumes_at_its_new_root_after_a_rebind(true).await;
}
