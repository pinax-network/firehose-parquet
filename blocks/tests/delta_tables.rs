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
//! - DuckDB and Polars read exact row counts, the `date` partition, Delta
//!   types (`BIGINT`/`Int64`, `DECIMAL(20,0)` for a `u64::MAX` nonce,
//!   microsecond UTC timestamps) and prune by `date`.
//!
//! The S3 dataset is copied out of the loopback server and read locally.
//! Engines: the DuckDB CLI from `FIREPARQ_DUCKDB` (else `duckdb` on `PATH`),
//! which installs its `delta` extension into a temporary directory (or
//! `FIREPARQ_DUCKDB_EXTENSION_DIR`), and the Python in
//! `FIREPARQ_POLARS_PYTHON` with `polars` and `deltalake`. With
//! `FIREPARQ_REQUIRE_DUCKDB` / `FIREPARQ_REQUIRE_POLARS` (CI) a missing
//! engine fails instead of skipping.
use firehose_parquet::writer::read_parquet;
use firehose_protos::{eth, firehose};
use prost::Message;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

#[path = "../examples/bench_live_flush/s3.rs"]
#[allow(dead_code)]
mod s3;

const CHAIN: &str = "delta-test-chain";
/// 2023-11-15T00:00:00Z: blocks 100 and 101 are on 2023-11-14, 102 and 103
/// on 2023-11-15.
const MIDNIGHT: i64 = 1_700_006_400;
const DAY: &str = "2023-11-15";
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

/// The actions of every commit of a local table, by version.
fn log(table: &Path) -> BTreeMap<u64, Vec<Value>> {
    let mut commits = BTreeMap::new();
    for entry in std::fs::read_dir(table.join("_delta_log")).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        let Some(version) = name.strip_suffix(".json") else {
            continue;
        };
        let actions = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        commits.insert(version.parse().unwrap(), actions);
    }
    commits
}

fn action<'a>(actions: &'a [Value], kind: &str) -> Option<&'a Value> {
    actions.iter().find_map(|action| action.get(kind))
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
        let commits = log(table);
        // Commit 0 creates the table: protocol, metadata and identity only.
        let create = &commits[&0];
        let protocol = action(create, "protocol").unwrap();
        assert_eq!(
            (&protocol["minReaderVersion"], &protocol["minWriterVersion"]),
            (&json!(1), &json!(2)),
            "{name}"
        );
        assert!(
            protocol.get("readerFeatures").is_none() && protocol.get("writerFeatures").is_none()
        );
        let metadata = action(create, "metaData").unwrap();
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
        assert!(action(create, "add").is_none(), "{name}");

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
            let add = action(actions, "add").unwrap();
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
            let txn = action(actions, "txn").unwrap();
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
            let committed_at = action(actions, "commitInfo").unwrap()["timestamp"]
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

/// The DuckDB CLI, or `None` locally when it is missing.
fn duckdb() -> Option<PathBuf> {
    let candidate = std::env::var_os("FIREPARQ_DUCKDB")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::split_paths(&std::env::var_os("PATH")?)
                .map(|directory| directory.join("duckdb"))
                .find(|path| path.is_file())
        })
        .unwrap_or_else(|| PathBuf::from("duckdb"));
    let available = std::process::Command::new(&candidate)
        .arg("-version")
        .output()
        .is_ok_and(|output| output.status.success());
    if available {
        return Some(candidate);
    }
    assert!(
        std::env::var_os("FIREPARQ_REQUIRE_DUCKDB").is_none(),
        "FIREPARQ_REQUIRE_DUCKDB is set but the DuckDB CLI {candidate:?} is unavailable"
    );
    eprintln!("skipping the DuckDB Delta check: no DuckDB CLI ({candidate:?})");
    None
}

/// The Python with Polars and `deltalake`, or `None` locally when missing.
fn polars() -> Option<PathBuf> {
    let candidate = std::env::var_os("FIREPARQ_POLARS_PYTHON").map(PathBuf::from);
    let available = candidate.as_ref().is_some_and(|python| {
        std::process::Command::new(python)
            .args(["-c", "import polars, deltalake"])
            .env_clear()
            .output()
            .is_ok_and(|output| output.status.success())
    });
    if available {
        return candidate;
    }
    assert!(
        std::env::var_os("FIREPARQ_REQUIRE_POLARS").is_none(),
        "FIREPARQ_REQUIRE_POLARS is set but FIREPARQ_POLARS_PYTHON ({candidate:?}) cannot import polars and deltalake"
    );
    eprintln!(
        "skipping the Polars Delta check: set FIREPARQ_POLARS_PYTHON to a Python with polars and deltalake"
    );
    None
}

/// Runs `sql` after loading the `delta` extension in a fresh in-memory
/// DuckDB, and returns the rows of the last statement.
fn duckdb_rows(duckdb: &Path, cwd: &Path, extensions: &Path, sql: &str) -> Vec<Value> {
    let init = cwd.join("empty.duckdbrc");
    std::fs::write(&init, "").unwrap();
    let setup = format!(
        "SET extension_directory = '{}'; LOAD delta;",
        extensions.display()
    );
    let output = std::process::Command::new(duckdb)
        .env_clear()
        .current_dir(cwd)
        .arg("-init")
        .arg(&init)
        .args(["-json", "-c", &format!("{setup} {sql}")])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{sql}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let last = stdout
        .trim()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if last.is_empty() {
        return Vec::new();
    }
    serde_json::from_str(&last).unwrap_or_else(|error| panic!("{error}: {stdout}"))
}

/// Installs DuckDB's `delta` extension, retrying transient download errors.
fn install_delta(duckdb: &Path, cwd: &Path, extensions: &Path) {
    let sql = format!(
        "SET extension_directory = '{}'; INSTALL delta; LOAD delta;",
        extensions.display()
    );
    for attempt in 1..=3 {
        let output = std::process::Command::new(duckdb)
            .env_clear()
            .current_dir(cwd)
            .args(["-c", &sql])
            .output()
            .unwrap();
        if output.status.success() {
            return;
        }
        assert!(
            attempt < 3,
            "cannot install the DuckDB delta extension: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_secs(5));
    }
}

fn number(value: &Value) -> u64 {
    match value {
        Value::Number(number) => number.as_u64().unwrap(),
        Value::String(text) => text.parse().unwrap(),
        other => panic!("not a number: {other}"),
    }
}

fn check_duckdb(
    duckdb: &Path,
    cwd: &Path,
    extensions: &Path,
    root: &Path,
    rows: &BTreeMap<String, u64>,
) {
    let version = duckdb_rows(duckdb, cwd, extensions, "SELECT version() AS v")[0]["v"].clone();
    for (table, expected) in rows {
        let scan = format!("delta_scan('{}/{table}')", root.display());
        let row = &duckdb_rows(
            duckdb,
            cwd,
            extensions,
            &format!(
                "SELECT count(*) AS n, count(DISTINCT block_num) AS blocks, \
                 min(block_num) AS first, max(block_num) AS last, \
                 (SELECT count(*) FROM {scan} WHERE date = DATE '{DAY}') AS on_day, \
                 epoch_us(max(timestamp)) % 1000000 AS micros \
                 FROM {scan}"
            ),
        )[0];
        let context = format!("duckdb {version} {table}");
        assert_eq!(number(&row["n"]), *expected, "{context}: {row}");
        assert_eq!(
            (
                number(&row["blocks"]),
                number(&row["first"]),
                number(&row["last"])
            ),
            (4, 100, 103),
            "{context}: {row}"
        );
        assert_eq!(number(&row["on_day"]), expected / 2, "{context}: {row}");
        assert_eq!(number(&row["micros"]), 250_000, "{context}: {row}");
        let types: BTreeMap<String, String> = duckdb_rows(
            duckdb,
            cwd,
            extensions,
            &format!("DESCRIBE SELECT * FROM {scan}"),
        )
        .into_iter()
        .map(|row| {
            (
                row["column_name"].as_str().unwrap().to_string(),
                row["column_type"].as_str().unwrap().to_string(),
            )
        })
        .collect();
        assert_eq!(types["block_num"], "BIGINT", "{context}");
        assert_eq!(types["timestamp"], "TIMESTAMP WITH TIME ZONE", "{context}");
        assert_eq!(types["date"], "DATE", "{context}");
        if table == "blocks" {
            assert_eq!(types["nonce"], "DECIMAL(20,0)", "{context}");
            let nonce = &duckdb_rows(
                duckdb,
                cwd,
                extensions,
                &format!("SELECT CAST(min(nonce) AS VARCHAR) AS nonce FROM {scan}"),
            )[0];
            assert_eq!(nonce["nonce"], json!("18446744073709551615"), "{context}");
        }
    }
    // A table that never had rows reads as empty.
    let empty = &duckdb_rows(
        duckdb,
        cwd,
        extensions,
        &format!(
            "SELECT count(*) AS n FROM delta_scan('{}/withdrawals')",
            root.display()
        ),
    )[0];
    assert_eq!(number(&empty["n"]), 0);
    eprintln!(
        "duckdb {version} read {} Delta tables with exact rows",
        rows.len()
    );
}

fn check_polars(python: &Path, root: &Path, rows: &BTreeMap<String, u64>) {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/engines/delta_check.py");
    let mut tables: Vec<&str> = rows.keys().map(String::as_str).collect();
    tables.push("withdrawals");
    let spec = json!({
        "root": root,
        "tables": tables,
        "day": DAY,
        "decimals": {"blocks": ["nonce"]},
    });
    let output = std::process::Command::new(python)
        .env_clear()
        .arg(script)
        .arg(spec.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "polars: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    for (table, expected) in rows {
        let seen = &report["tables"][table];
        let context = format!("polars {} {table}", report["polars"]);
        assert_eq!(number(&seen["rows"]), *expected, "{context}: {seen}");
        assert_eq!(number(&seen["rows_on_day"]), expected / 2, "{context}");
        let blocks: Vec<u64> = seen["block_nums"]
            .as_array()
            .unwrap()
            .iter()
            .map(number)
            .collect();
        let per_block = (*expected / 4) as usize;
        let expected_blocks: Vec<u64> = (100..104)
            .flat_map(|block| std::iter::repeat_n(block, per_block))
            .collect();
        assert_eq!(blocks, expected_blocks, "{context}");
        assert_eq!(seen["schema"]["block_num"], json!("Int64"), "{context}");
        assert_eq!(
            seen["schema"]["timestamp"],
            json!("Datetime(time_unit='us', time_zone='UTC')"),
            "{context}"
        );
        assert_eq!(seen["schema"]["date"], json!("Date"), "{context}");
        assert_eq!(
            number(&seen["version"]),
            4,
            "{context}: one commit per block"
        );
    }
    let blocks = &report["tables"]["blocks"];
    assert_eq!(
        blocks["schema"]["nonce"],
        json!("Decimal(precision=20, scale=0)")
    );
    assert_eq!(blocks["minimums"]["nonce"], json!("18446744073709551615"));
    let empty = &report["tables"]["withdrawals"];
    assert_eq!((number(&empty["rows"]), number(&empty["version"])), (0, 0));
    eprintln!(
        "polars {} with deltalake {} read {} Delta tables with exact rows",
        report["polars"],
        report["deltalake"],
        rows.len()
    );
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

    let extensions = std::env::var_os("FIREPARQ_DUCKDB_EXTENSION_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| cwd.join("duckdb-extensions"));
    if let Some(duckdb) = duckdb() {
        install_delta(&duckdb, cwd, &extensions);
        check_duckdb(&duckdb, cwd, &extensions, &root, &rows);
    }
    if let Some(python) = polars() {
        check_polars(&python, &root, &rows);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_build_writes_delta_tables_that_duckdb_and_polars_read_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    build_restart_and_read(Storage::Local(cwd.join("dataset")), &cwd).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_build_writes_delta_tables_with_conditional_log_commits() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let server = s3::Server::start(
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
    build_restart_and_read(Storage::S3(&server), &cwd).await;
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

/// The crash row the commit layer already recovers without a roll-forward
/// (`docs/design/delta-lake.md` §4): every Delta commit of a transaction is
/// durable, `blocks` last, and the process dies before authority advances.
/// The restart advances authority and continues; each table holds each
/// transaction exactly once. (A crash between table commits needs the
/// `txn`-gated roll-forward of #643 L4.)
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
            log(&root.join(table)).len(),
            2,
            "{table}: create and transaction 1"
        );
    }
    build(&storage, &cwd, 102).await;
    check_logs(&root, 2);
}
