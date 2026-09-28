//! #643 L3: every `fireparq build` writes Delta tables, and DuckDB
//! (`delta_scan`) and Polars (`scan_delta`) read them through the log with
//! exactly the rows the parts hold.
//!
//! A cursor-aware mock Firehose serves four final EVM blocks over two UTC
//! days. `build` runs twice, `[100, 102)` then a clean restart to `[100,
//! 104)`, with one transaction per block, on local disk and on a loopback
//! HTTPS S3 endpoint (`examples/bench_live_flush/s3.rs`, trusted through
//! `SSL_CERT_FILE`). The test then checks, from the logs:
//!
//! - one Delta table per mapper table under `<root>/<table>/`, created at the
//!   first start with reader 1 / writer 2, `date` as the only partition column
//!   and the `fireparq.*` identity, including tables that never get rows;
//! - one commit per table per transaction with rows: the part's `add` (the
//!   published file, byte for byte in size, with `numRecords` and bounds) and
//!   `txn {appId: fireparq:<descriptor>, version: last ordinal}`, `blocks` last;
//! - after the restart, each table's `txn` equals the authority's ordinal and
//!   its version counts exactly one commit per transaction;
//! - after the restart, DuckDB `delta_scan` and Polars `scan_delta` read
//!   exactly the rows the logs add, and a table that never had rows as empty.
//!   Types, the `date` partition and pruning are `engine_compat.rs`'s (#643
//!   L8).
//!
//! The S3 dataset is copied out of the loopback server and read locally.
//! Engines: see `common/mod.rs`.
use firehose_parquet::writer::read_parquet;
use firehose_protos::{eth, firehose};
use prost::Message;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

mod common;
use common::DuckDb;

#[path = "../examples/bench_live_flush/s3.rs"]
#[allow(dead_code)]
mod s3;

const CHAIN: &str = "delta-test-chain";
/// 2023-11-15T00:00:00Z: blocks 100 and 101 are on 2023-11-14, 102 and 103
/// on 2023-11-15.
const MIDNIGHT: i64 = 1_700_006_400;
const BUCKET: &str = "delta-lake";

/// Rows of each checked table per block, and its partition-free columns.
const TABLES: [(&str, u64); 4] = [
    ("blocks", 1),
    ("transactions", 2),
    ("logs", 2),
    ("access_lists", 2),
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

/// Final blocks 100..=103; a request with a cursor resumes after it.
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
        let responses: Vec<_> = (after + 1..=request.stop_block_num.min(103))
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

fn seconds(number: u64) -> i64 {
    MIDNIGHT - 102 + number as i64
}

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
        // A PoW nonce above i64::MAX: `blocks.nonce` is decimal(20,0).
        header: Some(eth::BlockHeader {
            number,
            nonce: u64::MAX,
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
                seconds: seconds(number),
                nanos: 250_000_000,
            }),
            ..Default::default()
        }),
    }
}

/// Where a dataset is written.
enum Storage<'a> {
    Local(PathBuf),
    S3(&'a s3::Server),
}

impl Storage<'_> {
    fn output(&self) -> String {
        match self {
            Storage::Local(root) => root.to_str().unwrap().to_string(),
            Storage::S3(_) => format!("s3://{BUCKET}/{CHAIN}"),
        }
    }

    /// The dataset as a local directory: the root itself, or a copy of every
    /// object under the S3 prefix.
    fn local_copy(&self, cwd: &Path) -> PathBuf {
        match self {
            Storage::Local(root) => root.clone(),
            Storage::S3(server) => {
                let copy = cwd.join("s3-copy");
                let _ = std::fs::remove_dir_all(&copy);
                for (key, bytes) in server.objects(&format!("{CHAIN}/")) {
                    let path = copy.join(key.strip_prefix(&format!("{CHAIN}/")).unwrap());
                    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                    std::fs::write(path, bytes).unwrap();
                }
                copy
            }
        }
    }
}

/// `build` to `stop` (exclusive) from a mock Firehose, one transaction per block.
async fn build(storage: &Storage<'_>, cwd: &Path, stop: u64) {
    let output = run(storage, cwd, stop, None).await;
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// One `build` run, with an optional `FIREPARQ_DEBUG_FAULT`.
async fn run(
    storage: &Storage<'_>,
    cwd: &Path,
    stop: u64,
    fault: Option<&str>,
) -> std::process::Output {
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
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    command
        .kill_on_drop(true)
        .env_clear()
        .current_dir(cwd)
        .args(["build", "--endpoint", &endpoint, "--block-type", "evm"])
        .args(["--start-block", "100", "--stop-block", &stop.to_string()])
        .args(["--flush-blocks", "1", "--stream-idle-timeout-secs", "0"])
        .args(["--output", &storage.output()]);
    if let Storage::S3(s3) = storage {
        command
            .env("AWS_ACCESS_KEY_ID", "loopback-access-key")
            .env("AWS_SECRET_ACCESS_KEY", "loopback-secret-key")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_ENDPOINT_URL_S3", &s3.endpoint)
            .env("SSL_CERT_FILE", &s3.ca_file);
    }
    if let Some(fault) = fault {
        command.env("FIREPARQ_DEBUG_FAULT", fault);
    }
    let output = tokio::time::timeout(Duration::from_secs(120), command.output())
        .await
        .expect("fireparq timed out")
        .unwrap();
    server.abort();
    output
}

/// The authority's accepted ordinal, from the local control state or its
/// copy.
fn authority_ordinal(root: &Path) -> i64 {
    let state: Value =
        serde_json::from_slice(&std::fs::read(root.join(".fireparq-ingest/state.json")).unwrap())
            .unwrap();
    state["payload"]["checkpoint"]["ordinal"].as_i64().unwrap()
}

/// Checks the logs of every table against the parts they add; returns the
/// rows per checked table.
fn check_logs(root: &Path, transactions: u64) -> BTreeMap<String, u64> {
    let ordinal = authority_ordinal(root);
    assert_eq!(ordinal, transactions as i64);
    let tables: Vec<PathBuf> = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            path.is_dir() && !name.starts_with('.') && name != "_fireparq"
        })
        .collect();
    assert!(
        tables.len() > TABLES.len(),
        "every mapper table: {tables:?}"
    );
    let mut descriptor = None;
    let mut rows = BTreeMap::new();
    let mut blocks_commits: Vec<i64> = Vec::new();
    let mut other_commits: BTreeMap<i64, i64> = BTreeMap::new();
    for table in &tables {
        let name = table.file_name().unwrap().to_str().unwrap().to_string();
        let commits = common::delta_log(table);
        // Commit 0 creates the table: protocol, metadata and identity only.
        let create = &commits[&0];
        let protocol = common::action(create, "protocol").unwrap();
        assert_eq!(
            (&protocol["minReaderVersion"], &protocol["minWriterVersion"]),
            (&json!(1), &json!(2)),
            "{name}"
        );
        assert!(
            protocol.get("readerFeatures").is_none() && protocol.get("writerFeatures").is_none()
        );
        let metadata = common::action(create, "metaData").unwrap();
        assert_eq!(metadata["partitionColumns"], json!(["date"]), "{name}");
        let configuration = &metadata["configuration"];
        assert_eq!(configuration["delta.appendOnly"], json!("true"), "{name}");
        assert_eq!(
            configuration["delta.dataSkippingStatsColumns"],
            json!("block_num,timestamp")
        );
        assert_eq!(configuration["fireparq.chain"], json!(CHAIN), "{name}");
        assert_eq!(configuration["fireparq.blockType"], json!("evm"), "{name}");
        assert!(configuration
            .get("delta.setTransactionRetentionDuration")
            .is_none());
        let identity = configuration["fireparq.descriptor"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(identity.len(), 64);
        assert_eq!(
            descriptor.get_or_insert(identity.clone()),
            &identity,
            "{name}"
        );
        assert!(common::action(create, "add").is_none(), "{name}");

        let expected = TABLES.iter().find(|(table, _)| *table == name);
        let versions: Vec<u64> = commits.keys().copied().collect();
        let per_block = expected.map_or(0, |(_, rows)| *rows);
        let expected_commits = if per_block == 0 && name != "blocks" {
            0
        } else {
            transactions
        };
        if expected.is_none() {
            // Tables without rows in this fixture exist but are never committed to.
            if versions.len() == 1 {
                continue;
            }
        }
        assert_eq!(
            versions,
            (0..=expected_commits).collect::<Vec<_>>(),
            "{name}"
        );
        let mut table_rows = 0;
        let mut last_txn = None;
        for version in 1..=expected_commits {
            let actions = &commits[&version];
            assert_eq!(
                actions.len(),
                3,
                "{name} v{version}: commitInfo, add, txn: {actions:?}"
            );
            let add = common::action(actions, "add").unwrap();
            let path = table.join(add["path"].as_str().unwrap());
            assert!(
                add["path"].as_str().unwrap().contains("/part-v1-"),
                "{name}: {add}"
            );
            // The published part is the data file, unchanged in size.
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                add["size"].as_u64().unwrap()
            );
            let date = add["partitionValues"]["date"].as_str().unwrap();
            assert!(add["path"]
                .as_str()
                .unwrap()
                .starts_with(&format!("date={date}/")));
            let stats: Value = serde_json::from_str(add["stats"].as_str().unwrap()).unwrap();
            let file_rows: u64 = read_parquet(&path)
                .unwrap()
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum();
            assert_eq!(stats["numRecords"].as_u64(), Some(file_rows), "{name}");
            assert_eq!(stats["nullCount"]["block_num"], json!(0));
            assert!(stats["minValues"]["timestamp"]
                .as_str()
                .unwrap()
                .ends_with(".250Z"));
            table_rows += file_rows;
            let txn = common::action(actions, "txn").unwrap();
            assert_eq!(
                txn["appId"],
                json!(format!("fireparq:{identity}")),
                "{name}"
            );
            let version_txn = txn["version"].as_i64().unwrap();
            assert!(
                last_txn.is_none_or(|last| version_txn > last),
                "{name}: txn increases"
            );
            last_txn = Some(version_txn);
            let committed_at = common::action(actions, "commitInfo").unwrap()["timestamp"]
                .as_i64()
                .unwrap();
            if name == "blocks" {
                blocks_commits.push(committed_at);
                assert_eq!(version_txn, version as i64, "one ordinal per block");
            } else {
                let latest = other_commits.entry(version_txn).or_insert(committed_at);
                *latest = (*latest).max(committed_at);
            }
        }
        assert_eq!(
            last_txn,
            Some(ordinal),
            "{name}: txn is the authority's ordinal"
        );
        rows.insert(name, table_rows);
    }
    // `blocks` commits last in each transaction.
    for (index, blocks_at) in blocks_commits.iter().enumerate() {
        let ordinal = index as i64 + 1;
        assert!(
            other_commits
                .get(&ordinal)
                .is_none_or(|other| other <= blocks_at),
            "blocks commits after the other tables of transaction {ordinal}"
        );
    }
    for (table, per_block) in TABLES {
        assert_eq!(rows[table], per_block * transactions, "{table}");
    }
    rows
}

/// DuckDB `delta_scan` and Polars `scan_delta` read exactly the rows the logs
/// add, and an empty table (one that never had rows) as empty.
/// `engine_compat.rs` checks types, partitions and pruning in detail.
fn read_with_engines(cwd: &Path, root: &Path, rows: &BTreeMap<String, u64>) {
    let mut tables: Vec<String> = rows.keys().cloned().collect();
    tables.push("withdrawals".into());
    let mut expected = rows.clone();
    expected.insert("withdrawals".into(), 0);
    if let Some(duckdb) = DuckDb::open(cwd) {
        assert_eq!(
            common::duckdb_counts(&duckdb, root, &tables),
            expected,
            "duckdb {}",
            duckdb.version
        );
        eprintln!(
            "duckdb {} (delta {}) read {} Delta tables with exact rows",
            duckdb.version,
            duckdb.delta_version,
            tables.len()
        );
    }
    if let Some(python) = common::python() {
        assert_eq!(
            common::polars_counts(&python, root, &tables),
            expected,
            "polars"
        );
        eprintln!("polars read {} Delta tables with exact rows", tables.len());
    }
}

async fn build_restart_and_read(storage: Storage<'_>, cwd: &Path) {
    // A first run to 102, then a clean restart that continues to 104.
    build(&storage, cwd, 102).await;
    let first = storage.local_copy(cwd);
    let rows = check_logs(&first, 2);
    assert_eq!(rows["blocks"], 2);
    build(&storage, cwd, 104).await;
    let root = storage.local_copy(cwd);
    let rows = check_logs(&root, 4);

    read_with_engines(cwd, &root, &rows);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_build_writes_delta_tables_that_duckdb_and_polars_read_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    build_restart_and_read(Storage::Local(cwd.join("dataset")), &cwd).await;
}

async fn s3_server(cwd: &Path) -> s3::Server {
    s3::Server::start(
        &cwd.join("tls"),
        BUCKET,
        s3::Latency {
            base_ms: 0,
            slow_every: 0,
            slow_ms: 0,
        },
    )
    .await
    .unwrap()
}

fn text(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

const UNQUOTED_CHOICE: &str =
    "s3 conditional writes: If-Match ETags sent unquoted (provider compares them literally)";

/// `(If-Match, status)` of every conditional request, per canary probe key in
/// order of creation, and of every request outside the probes.
type Conditions = Vec<(String, u16)>;
fn if_match_requests(server: &s3::Server) -> (Vec<Conditions>, Conditions) {
    let mut probes: Vec<(String, Conditions)> = Vec::new();
    let mut others = Vec::new();
    for entry in server.log() {
        let Some(value) = entry.if_match else {
            continue;
        };
        if !entry.key.starts_with(".fireparq-owner-probes-v1/") {
            others.push((value, entry.status));
            continue;
        }
        let value = if value.contains("never-match-") {
            "wrong".to_string()
        } else {
            value
        };
        match probes.iter_mut().find(|(key, _)| *key == entry.key) {
            Some((_, conditions)) => conditions.push((value, entry.status)),
            None => probes.push((entry.key, vec![(value, entry.status)])),
        }
    }
    (probes.into_iter().map(|(_, c)| c).collect(), others)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_build_writes_delta_tables_with_conditional_log_commits() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let server = s3_server(&cwd).await;
    build_restart_and_read(Storage::S3(&server), &cwd).await;
    // RFC 9110 `If-Match` (AWS S3, MinIO): the ETags are sent as returned,
    // quoted, after one canary run per start (#678).
    let (probes, others) = if_match_requests(&server);
    assert_eq!(probes.len(), 2, "{probes:?}");
    assert!(!others.is_empty());
    for (value, status) in others {
        assert!(value.starts_with('"') && status == 200, "{value} {status}");
    }
    // Every log commit was one PUT: no commit was sent twice.
    let log_puts: Vec<_> = server
        .log()
        .into_iter()
        .filter(|entry| entry.method == "PUT" && entry.key.contains("/_delta_log/"))
        .collect();
    let mut keys: Vec<&str> = log_puts.iter().map(|entry| entry.key.as_str()).collect();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(keys.len(), log_puts.len(), "{log_puts:?}");
    assert!(log_puts.iter().all(|entry| entry.status == 200));
}

/// #678: Ceph RGW 19.2 compares `If-Match` literally with the stored ETag
/// without its quotes. Each start's canary sees its quoted correct version
/// refused, qualifies the unquoted form on a fresh probe (wrong and stale
/// versions still refused), logs the choice once, and then every
/// conditional request of fireparq's own state carries the unquoted ETag.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_build_on_rgw_19_sends_unquoted_if_match_etags() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let server = s3_server(&cwd).await;
    server.set_if_match(s3::IfMatch::Rgw19);
    let storage = Storage::S3(&server);
    for stop in [102, 104] {
        let output = run(&storage, &cwd, stop, None).await;
        let text = text(&output);
        assert!(output.status.success(), "{text}");
        assert_eq!(text.matches(UNQUOTED_CHOICE).count(), 1, "{text}");
    }
    let root = storage.local_copy(&cwd);
    assert_eq!(check_logs(&root, 4)["blocks"], 4);
    let (probes, others) = if_match_requests(&server);
    assert_eq!(probes.len(), 4, "two canary runs per start: {probes:?}");
    for pair in probes.chunks(2) {
        // As returned: the wrong version, then the quoted correct one, refused.
        assert!(
            matches!(&pair[0][..], [(wrong, 412), (quoted, 412)]
                if wrong == "wrong" && quoted.starts_with('"')),
            "{pair:?}"
        );
        // Unquoted: wrong refused, pinned GET and CAS applied, stale refused.
        assert!(
            matches!(&pair[1][..], [(wrong, 412), (get, 200), (cas, 200), (stale, 412)]
                if wrong == "wrong" && !get.starts_with('"') && get == cas && cas == stale),
            "{pair:?}"
        );
    }
    assert!(!others.is_empty());
    for (value, status) in others {
        assert!(!value.contains('"') && status == 200, "{value} {status}");
    }
    // Each kind of conditional request ran in that form: the owner record's
    // CAS, the control state's CAS, and the GET pinning an uploaded part.
    let log = server.log();
    let unquoted = |method: &str, key: &dyn Fn(&str) -> bool| {
        log.iter().any(|entry| {
            entry.method == method
                && key(&entry.key)
                && entry.status == 200
                && entry
                    .if_match
                    .as_deref()
                    .is_some_and(|value| !value.contains('"'))
        })
    };
    assert!(unquoted("PUT", &|key| key == ".fireparq-owner-v1.json"));
    assert!(unquoted("PUT", &|key| key.contains("/.fireparq-ingest/")));
    assert!(unquoted("GET", &s3::is_data_part));
}

/// #678: a provider on which neither ETag form matches fails closed, as
/// before: no owner record, no data, and both probes deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_build_fails_closed_when_no_if_match_form_matches() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let server = s3_server(&cwd).await;
    server.set_if_match(s3::IfMatch::RefuseAll);
    let output = run(&Storage::S3(&server), &cwd, 102, None).await;
    let text = text(&output);
    assert!(!output.status.success(), "{text}");
    assert!(
        text.contains(
            "conditional-write capability could not be proven; ownership was not acquired"
        ),
        "{text}"
    );
    assert!(!text.contains(UNQUOTED_CHOICE), "{text}");
    let (probes, others) = if_match_requests(&server);
    assert_eq!(probes.len(), 2, "{probes:?}");
    assert!(others.is_empty(), "{others:?}");
    assert!(server.objects("").is_empty());
}

/// The crash row the commit layer already recovers without a roll-forward
/// (`docs/design/delta-lake.md` §4): every Delta commit of a transaction is
/// durable, `blocks` last, and the process dies before authority advances.
/// The restart advances authority and continues; each table holds each
/// transaction exactly once. (Every row of the crash matrix, with
/// maintenance between the crash and the restart, is in
/// `delta_recovery.rs`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_after_every_delta_commit_restarts_without_a_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let storage = Storage::Local(cwd.join("dataset"));
    let crashed = run(&storage, &cwd, 102, Some("crash-after-delta-commit:blocks")).await;
    assert!(
        !crashed.status.success(),
        "the injected abort stops the build"
    );
    let root = storage.local_copy(&cwd);
    assert_eq!(authority_ordinal(&root), 0);
    let pending: Value =
        serde_json::from_slice(&std::fs::read(root.join(".fireparq-ingest/pending.json")).unwrap())
            .unwrap();
    assert_eq!(pending["payload"]["phase"], json!("committed"));
    for (table, _) in TABLES {
        assert_eq!(
            common::delta_log(&root.join(table)).len(),
            2,
            "{table}: create and transaction 1"
        );
    }
    build(&storage, &cwd, 102).await;
    check_logs(&root, 2);
}

/// The tracing lines of `text` logged at ERROR (ANSI colors removed).
fn error_lines(text: &str) -> Vec<String> {
    text.lines()
        .map(|line| {
            let mut plain = String::with_capacity(line.len());
            let mut chars = line.chars();
            while let Some(c) = chars.next() {
                if c == '\u{1b}' {
                    // Skip a CSI sequence: ESC '[' ... final letter.
                    for c in chars.by_ref() {
                        if c.is_ascii_alphabetic() {
                            break;
                        }
                    }
                } else {
                    plain.push(c);
                }
            }
            plain
        })
        .filter(|line| line.split_whitespace().nth(1) == Some("ERROR"))
        .collect()
}

/// #680: a clean first start creates every table without an ERROR line. On
/// S3, delta-rs's kernel logged `Generic delta kernel error: No files in log
/// segment` at ERROR for each table fireparq opened before it existed;
/// fireparq now lists the log first while tables may be created.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_first_start_logs_no_error_line() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let server = s3_server(&cwd).await;
    for storage in [Storage::Local(cwd.join("dataset")), Storage::S3(&server)] {
        let output = run(&storage, &cwd, 102, None).await;
        let text = text(&output);
        assert!(output.status.success(), "{text}");
        assert!(text.contains("INFO"), "logs at the default level: {text}");
        assert_eq!(error_lines(&text), Vec::<String>::new(), "{text}");
        assert!(!text.contains("No files in log segment"), "{text}");
        let root = storage.local_copy(&cwd);
        assert_eq!(check_logs(&root, 2)["blocks"], 2);
    }
}

const READ_RETRY: &str = "retrying an idempotent Delta log read after a transient error";

fn log_key(table: &str, object: &str) -> String {
    format!("{CHAIN}/{table}/_delta_log/{object}")
}

fn commit_key(table: &str, version: u64) -> String {
    log_key(table, &format!("{version:020}.json"))
}

/// #680: an idempotent Delta log read that meets a dropped connection, a 5xx
/// or a 429 is sent again (logged at WARN), and `build` goes on: on a first
/// start (the reads that create and load each table) and on a restart (the
/// reads that load each snapshot).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_log_reads_retry_transient_failures() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let server = s3_server(&cwd).await;
    let storage = Storage::S3(&server);
    let checkpoint = log_key("blocks", "_last_checkpoint");

    // First start: the first GET of `blocks`' checkpoint hint loses its
    // connection, and the first listing of `transactions`' log answers 503.
    server.inject("GET", &checkpoint, s3::Fault::Reset, 1);
    server.inject_list(&log_key("transactions", ""), s3::Fault::Status(503), 1);
    let output = run(&storage, &cwd, 102, None).await;
    let first = text(&output);
    assert!(output.status.success(), "{first}");
    assert_eq!(first.matches(READ_RETRY).count(), 2, "{first}");
    assert!(first.contains("_last_checkpoint"), "{first}");
    assert_eq!(error_lines(&first), Vec::<String>::new(), "{first}");

    // Restart: loading each snapshot lists and reads the log again. The
    // first listing of `blocks`' log loses its connection, the first read of
    // a `logs` commit answers 500 and the next one 502, and the checkpoint
    // hint of `blocks` answers 429.
    server.inject_list(&log_key("blocks", ""), s3::Fault::Reset, 1);
    server.inject("GET", &commit_key("logs", 1), s3::Fault::Status(500), 1);
    server.inject("GET", &commit_key("logs", 1), s3::Fault::Status(502), 1);
    server.inject("GET", &checkpoint, s3::Fault::Status(429), 1);
    let output = run(&storage, &cwd, 104, None).await;
    let restart = text(&output);
    assert!(output.status.success(), "{restart}");
    assert_eq!(restart.matches(READ_RETRY).count(), 4, "{restart}");
    assert_eq!(error_lines(&restart), Vec::<String>::new(), "{restart}");
    for line in first.lines().chain(restart.lines()) {
        if line.contains(READ_RETRY) {
            eprintln!("{line}");
        }
    }

    // Every injected fault was met, and each faulted read was answered by a
    // later attempt.
    let log = server.log();
    let faulted: Vec<_> = log.iter().filter(|entry| entry.fault.is_some()).collect();
    assert_eq!(faulted.len(), 6, "{faulted:?}");
    for entry in &faulted {
        assert_eq!(entry.method, "GET");
        let answered = log.iter().any(|later| {
            later.seq > entry.seq
                && later.fault.is_none()
                && later.method == "GET"
                && later.key == entry.key
                && later.prefix == entry.prefix
                && matches!(later.status, 200 | 206 | 404)
        });
        assert!(answered, "{entry:?}");
    }
    let root = storage.local_copy(&cwd);
    assert_eq!(check_logs(&root, 4)["blocks"], 4);
    // No log write was sent twice.
    let log_puts: Vec<&str> = log
        .iter()
        .filter(|entry| entry.method == "PUT" && entry.key.contains("/_delta_log/"))
        .map(|entry| entry.key.as_str())
        .collect();
    let mut unique = log_puts.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), log_puts.len(), "{log_puts:?}");
}

/// #680: a Delta log commit stays a single conditional PUT. A commit that
/// meets a 503, or lands and loses its answer, is never sent again: the run
/// fails with an unknown outcome, the S3 owner is released (#675), and the
/// next start reads the table's `txn` to commit only what did not land, so
/// each table holds each transaction once (design §3.5).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_log_commits_are_sent_once_and_resolve_from_the_txn() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let server = s3_server(&cwd).await;
    let storage = Storage::S3(&server);
    let puts = |from: usize, key: &str| -> Vec<(u16, Option<s3::Fault>)> {
        server.log()[from..]
            .iter()
            .filter(|entry| entry.method == "PUT" && entry.key == key)
            .map(|entry| (entry.status, entry.fault))
            .collect()
    };

    for (fault, version, stop, landed) in [
        // Answered 503 without being applied: nothing landed.
        (s3::Fault::Status(503), 1, 102, false),
        // Applied, then the connection dropped: it landed, unknown to fireparq.
        (s3::Fault::ApplyThenReset, 3, 104, true),
    ] {
        let commit = commit_key("blocks", version);
        let next = commit_key("blocks", version + 1);
        server.inject("PUT", &commit, fault, 1);
        let from = server.log().len();
        let output = run(&storage, &cwd, stop, None).await;
        let failed = text(&output);
        assert!(!output.status.success(), "{failed}");
        assert!(
            failed.contains("a Delta commit's outcome is unknown"),
            "{fault:?}: {failed}"
        );
        assert!(!failed.contains(READ_RETRY), "{failed}");
        // One PUT, never sent again, and no commit at a later version.
        let status = if landed { 200 } else { 503 };
        assert_eq!(
            puts(from, &commit),
            vec![(status, Some(fault))],
            "{fault:?}"
        );
        assert!(puts(from, &next).is_empty(), "{fault:?}");
        assert_eq!(server.objects(&commit).len(), usize::from(landed));

        // The restart needs no `recovery release`, and commits to `blocks`
        // only when its log lacks the transaction.
        let from = server.log().len();
        let output = run(&storage, &cwd, stop, None).await;
        let restart = text(&output);
        assert!(output.status.success(), "{restart}");
        let resent = puts(from, &commit);
        if landed {
            assert!(resent.is_empty(), "{resent:?}");
        } else {
            assert_eq!(resent, vec![(200, None)]);
        }
        let root = storage.local_copy(&cwd);
        check_logs(&root, stop - 100);
    }
}
