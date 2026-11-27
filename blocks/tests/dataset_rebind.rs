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
//!
//! A dataset renamed in place instead (`mv chain chain-v1`, or the loopback
//! bucket renamed, as `radosgw-admin bucket link --bucket-new-name` does) is
//! refused by `build` and by a rebind without `--renamed`, both naming the
//! flag; `recovery rebind --renamed` rebinds it, and it resumes, recovers and
//! is maintained the same way, while nothing is written at the old name.
//!
//! A Tron dataset (#747) on the loopback bucket is refused by name by
//! `build`, `recovery recover` and `recovery rebind`, and each refusal leaves
//! the bucket's owner record released and the dataset unchanged.
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
            Lake::S3(server) => format!("s3://{}/{root}", server.bucket()),
        }
    }

    /// Renames the dataset at `root` in place, and returns the root it is
    /// at now: `mv <root> <root>-v1`, or the bucket renamed `<bucket>-v1`,
    /// which keeps the prefix.
    fn rename(&self, root: &str) -> String {
        match self {
            Lake::Local(lake) => {
                let renamed = format!("{root}-v1");
                std::fs::rename(lake.join(root), lake.join(&renamed)).unwrap();
                renamed
            }
            Lake::S3(server) => {
                server.rename_bucket(&format!("{}-v1", server.bucket()));
                root.to_string()
            }
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

/// `fireparq recovery <args>`, which must fail, and its output.
async fn refused_recovery(lake: &Lake, cwd: &Path, args: &[&str]) -> String {
    let mut command = lake.fireparq(cwd);
    command.arg("recovery").args(args);
    let result = output(command).await;
    assert!(!result.status.success(), "{}", text(&result));
    text(&result)
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

/// The tables a first rebind reports, `blocks` last, sorted: every Delta
/// table of the dataset at `root`.
fn rebound_tables(lake: &Lake, cwd: &Path, root: &str, rebound: &Value) -> Vec<String> {
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
    let mut sorted = tables;
    sorted.sort();
    assert_eq!(sorted, common::delta_tables(&lake.local(root, cwd)));
    sorted
}

/// A rerun of a complete rebind changes nothing.
fn unchanged(again: &Value) {
    assert_eq!(again["rebound_now"], false, "{again}");
    assert_eq!(again["tables_updated"], serde_json::json!([]), "{again}");
    assert_eq!(again["mirror"], "unchanged", "{again}");
}

/// The dataset rebound at `root` (blocks 100..=102, three transactions)
/// resumes from its stored cursor, recovers and is maintained: `build` adds
/// blocks 103..=104 with continuous ordinals, and every Delta table holds
/// each block once under the stream `rebound` names.
async fn resumes_after_the_rebind(
    lake: &Lake,
    cwd: &Path,
    root: &str,
    rebound: &Value,
    tables: &[String],
) {
    build(lake, cwd, root, 105).await;
    recovery(lake, cwd, &["recover", &lake.output(root)]).await;
    let run = common::maintenance_job(
        &common::maintenance_bin(),
        &lake.maintenance_env(root, tables),
    )
    .await;
    run.assert_clean();

    let local = lake.local(root, cwd);
    let state = control(&local, "state.json").unwrap();
    assert_eq!(state["checkpoint"]["ordinal"], 5);
    assert_eq!(state["checkpoint"]["event"]["block_num"], 104);
    assert!(control(&local, "pending.json").is_none());
    let stream = rebound["stream_id"].as_str().unwrap();
    let app_id = format!("fireparq:{stream}");
    for table in tables {
        let (metadata, txns) = log_summary(&local, table, &app_id);
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
    let counts = common::delta_counts(&local, tables).await;
    for (table, per_block) in ROW_TABLES {
        assert_eq!(counts[table], 5 * per_block, "{table}");
    }
    let blocks = common::delta_read(&common::open_local(&local, "blocks").await).await;
    let mut numbers = common::delta_block_nums(&local.join("blocks"), &blocks);
    numbers.sort_unstable();
    assert_eq!(numbers, [100, 101, 102, 103, 104]);
    match common::DuckDb::open(cwd) {
        Some(duckdb) => assert_eq!(common::duckdb_counts(&duckdb, &local, tables), counts),
        None => assert!(
            std::env::var_os("FIREPARQ_REQUIRE_DUCKDB").is_none(),
            "DuckDB is required"
        ),
    }
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

    // A copy is not a rename: `chain` still holds the dataset (on S3, the
    // copy is also in the same bucket, which no bucket rename reaches).
    let refused = refused_recovery(
        &lake,
        &cwd,
        &["rebind", &copy, "--from", &source, "--renamed"],
    )
    .await;
    let reason = match &lake {
        Lake::Local(_) => "was not renamed",
        Lake::S3(_) => "with its bucket renamed",
    };
    assert!(refused.contains(reason), "{refused}");

    let rebound = recovery(&lake, &cwd, &["rebind", &copy, "--from", &source]).await;
    let tables = rebound_tables(&lake, &cwd, "chain-v1", &rebound);
    unchanged(&recovery(&lake, &cwd, &["rebind", &copy, "--from", &source]).await);
    resumes_after_the_rebind(&lake, &cwd, "chain-v1", &rebound, &tables).await;

    // The source was only read, and resumes at its own root.
    assert_eq!(lake.files("chain"), original);
    build(&lake, &cwd, "chain", 104).await;
    let state = control(&lake.local("chain", &cwd), "state.json").unwrap();
    assert_eq!(state["checkpoint"]["ordinal"], 4);
}

async fn a_renamed_dataset_resumes_under_its_new_name_after_a_rebind(s3: bool) {
    let (_dir, cwd) = scratch();
    let lake = Lake::new(s3, &cwd).await;
    build(&lake, &cwd, "chain", 103).await;
    let original = lake.files("chain");
    let source = lake.output("chain");

    // Renamed in place: the same files under the new name.
    let root = lake.rename("chain");
    let renamed = lake.output(&root);
    assert_ne!(renamed, source);
    assert_eq!(lake.files(&root), original);
    let since = match &lake {
        Lake::S3(server) => server.log().len(),
        Lake::Local(_) => 0,
    };

    // `build` refuses it, naming the rebind and --renamed.
    let refused = run(&lake, &cwd, &root, 105).await;
    assert!(!refused.status.success());
    let hint = format!("fireparq recovery rebind {renamed} --from {source}");
    assert!(
        text(&refused).contains(&hint) && text(&refused).contains("--renamed"),
        "{}",
        text(&refused)
    );

    // Without --renamed there is nothing at --from to compare with.
    let refused = refused_recovery(&lake, &cwd, &["rebind", &renamed, "--from", &source]).await;
    assert!(
        refused.contains("holds no ingestion state") && refused.contains("--renamed"),
        "{refused}"
    );

    let rebind = ["rebind", &renamed, "--from", &source, "--renamed"];
    let rebound = recovery(&lake, &cwd, &rebind).await;
    let tables = rebound_tables(&lake, &cwd, &root, &rebound);
    unchanged(&recovery(&lake, &cwd, &rebind).await);
    resumes_after_the_rebind(&lake, &cwd, &root, &rebound, &tables).await;

    // Nothing was written at the old name: no local root, and no write to
    // the old bucket, where every request answers `NoSuchBucket` (404).
    match &lake {
        Lake::Local(lake) => assert!(!lake.join("chain").exists()),
        Lake::S3(server) => {
            let writes: Vec<_> = server.log()[since..]
                .iter()
                .filter(|entry| entry.method != "GET" && entry.method != "HEAD")
                .filter(|entry| entry.status == 404)
                .map(|entry| format!("{} {}", entry.method, entry.key))
                .collect();
            assert!(writes.is_empty(), "{writes:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copied_local_dataset_resumes_at_its_new_root_after_a_rebind() {
    a_copied_dataset_resumes_at_its_new_root_after_a_rebind(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dataset_copied_to_a_new_s3_prefix_resumes_there_after_a_rebind() {
    a_copied_dataset_resumes_at_its_new_root_after_a_rebind(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_dataset_moved_with_mv_resumes_there_after_a_rebind_with_renamed() {
    a_renamed_dataset_resumes_under_its_new_name_after_a_rebind(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dataset_whose_bucket_was_renamed_resumes_there_after_a_rebind_with_renamed() {
    a_renamed_dataset_resumes_under_its_new_name_after_a_rebind(true).await;
}

/// The bucket's owner record, as stored: one per bucket, at its root.
fn owner(server: &s3::Server) -> Value {
    let (_, bytes) = server.objects(".fireparq-owner-v1.json").pop().unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Rewrites `root`'s stored state as a Tron dataset's, as fireparq 1.7.0 or
/// earlier wrote it from `--network tron`: `family` and `bytes_encoding` of
/// its stream descriptor, with the record's checksum recomputed, so only the
/// Tron check can refuse it.
fn mark_as_tron(server: &s3::Server, root: &str) {
    use sha2::{Digest, Sha256};
    let key = format!("{root}/.fireparq-ingest/state.json");
    let (_, bytes) = server.objects(&key).pop().unwrap();
    let mut record: Value = serde_json::from_slice(&bytes).unwrap();
    let descriptor = &mut record["payload"]["descriptor"];
    descriptor["family"] = "tron".into();
    descriptor["bytes_encoding"] = "tron_base58".into();
    let checked = (
        &record["format_version"],
        &record["incarnation"],
        &record["revision"],
        &record["deleted"],
        &record["payload"],
    );
    let digest = Sha256::digest(serde_json::to_vec(&checked).unwrap());
    record["sha256"] = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        .into();
    server.put(&key, serde_json::to_vec(&record).unwrap().into());
}

/// #747: a Tron dataset on S3 is refused by name by `build`, `recovery
/// recover` and `recovery rebind`, and none of them keeps the bucket's owner
/// record. `build` acquires it before it loads the state, so the refusal
/// goes through the failure path, which releases an owner whose requests all
/// had a definite outcome. Nothing under the dataset is written, and
/// `recovery status` still reads it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tron_dataset_on_s3_is_refused_and_its_owner_record_released() {
    let (_dir, cwd) = scratch();
    let lake = Lake::new(true, &cwd).await;
    let Lake::S3(server) = &lake else {
        unreachable!()
    };
    build(&lake, &cwd, "chain", 103).await;
    mark_as_tron(server, "chain");
    lake.copy("chain", "chain-v1");
    let (files, copy) = (lake.files("chain"), lake.files("chain-v1"));
    let before = owner(server);
    assert_eq!(before["state"], "released");

    let refusal = "written from Tron, which fireparq no longer supports";
    let refused = run(&lake, &cwd, "chain", 105).await;
    assert!(!refused.status.success(), "{}", text(&refused));
    assert!(text(&refused).contains(refusal), "{}", text(&refused));
    assert!(
        !text(&refused).contains("fireparq recovery release"),
        "{}",
        text(&refused)
    );
    // `build` took the record and released it.
    let after = owner(server);
    assert_eq!(after["state"], "released", "{after}");
    assert!(
        after["generation"].as_u64() > before["generation"].as_u64(),
        "{before} {after}"
    );

    let (source, target) = (lake.output("chain"), lake.output("chain-v1"));
    for args in [
        vec!["recover", source.as_str()],
        vec!["rebind", target.as_str(), "--from", source.as_str()],
    ] {
        let refused = refused_recovery(&lake, &cwd, &args).await;
        assert!(refused.contains(refusal), "{args:?}: {refused}");
        assert_eq!(owner(server)["state"], "released", "{args:?}: {refused}");
    }
    recovery(&lake, &cwd, &["status", &source]).await;
    assert_eq!(owner(server)["state"], "released");
    assert_eq!(lake.files("chain"), files);
    assert_eq!(lake.files("chain-v1"), copy);
}
