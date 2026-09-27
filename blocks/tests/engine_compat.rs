//! #652: real `fireparq build` output read by the supported engines, DuckDB
//! and Polars, with Hive partitioning over the `date=YYYY-MM-DD` directories.
//!
//! A mock Firehose serves two UTC days of blocks. `build` writes EVM (final
//! and non-final) and Solana (list, binary and dictionary columns) datasets,
//! and each engine must read, for every checked table:
//!
//! - `date` as a date, equal in every file to its directory, and a `date`
//!   filter that returns exactly that day's rows and files;
//! - unsigned integers (UInt64, UInt32, List<UInt8>) as unsigned types;
//! - `timestamp` as a UTC timestamp in milliseconds;
//! - dictionary-encoded, list and binary columns;
//! - `stream_ordinal` in non-final output only;
//! - no file under `_fireparq/` or a dot-prefixed path;
//! - the row count the files hold, the same in both engines.
//!
//! The DuckDB CLI comes from `FIREPARQ_DUCKDB` (else `duckdb` on `PATH`) and
//! Polars from the interpreter in `FIREPARQ_POLARS_PYTHON`. CI installs both
//! at pinned versions and sets `FIREPARQ_REQUIRE_DUCKDB` and
//! `FIREPARQ_REQUIRE_POLARS`, so neither check can be skipped there; locally
//! a missing engine is skipped with a message.
use firehose_parquet::writer::read_parquet;
use firehose_protos::{eth, firehose, solana};
use prost::Message;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

const CHAIN: &str = "engine-test";
/// 2023-11-15T00:00:00Z: blocks 100 and 101 are on 2023-11-14, 102 and 103
/// on 2023-11-15.
const MIDNIGHT: i64 = 1_700_006_400;
const DAY: &str = "2023-11-15";
/// Sub-second part of every Firehose block time, so a seconds-only reader
/// would show. EVM rows keep it; Solana rows take the payload's whole-second
/// `block_time`.
const NANOS: i32 = 250_000_000;

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

/// Serves its fixed responses once, to a request for `[100, 104)`.
#[derive(Clone)]
struct Stream {
    responses: Arc<Vec<firehose::Response>>,
    final_only: bool,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        assert_eq!(
            (
                request.start_block_num,
                request.stop_block_num,
                request.final_blocks_only
            ),
            (100, 103, self.final_only)
        );
        assert!(request.cursor.is_empty());
        let responses: Vec<_> = self.responses.iter().cloned().map(Ok).collect();
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

/// Block time of the fixture block `number`, in whole seconds.
fn seconds(number: u64) -> i64 {
    MIDNIGHT - 102 + number as i64
}

/// One stream event: `(number, id byte, fork step)`.
type Event = (u64, u8, i32);

fn response(type_url: &str, payload: Vec<u8>, event: Event, ordinal: usize) -> firehose::Response {
    let (number, id, step) = event;
    firehose::Response {
        block: Some(prost_types::Any {
            type_url: type_url.into(),
            value: payload,
        }),
        step,
        cursor: format!("event-{ordinal}"),
        metadata: Some(firehose::BlockMetadata {
            num: number,
            id: format!("{id:02x}").repeat(32),
            parent_num: number - 1,
            parent_id: "11".repeat(32),
            // A FINAL event is at or below the last irreversible block.
            lib_num: if step == 3 { number } else { 99 },
            time: Some(prost_types::Timestamp {
                seconds: seconds(number),
                nanos: NANOS,
            }),
            ..Default::default()
        }),
    }
}

/// An EVM block with two transactions, each with a log and an access list.
fn evm_block(number: u64, id: u8) -> Vec<u8> {
    let transaction = |index: u32| eth::TransactionTrace {
        hash: vec![id ^ (index as u8 + 1); 32].into(),
        index,
        status: 1,
        from: vec![0x11; 20].into(),
        to: vec![0x22; 20].into(),
        gas_used: 21_000,
        access_list: vec![eth::AccessTuple {
            address: vec![0x33; 20].into(),
            storage_keys: vec![vec![0x44; 32].into(), vec![0x55; 32].into()],
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
    eth::Block {
        number,
        hash: vec![id; 32].into(),
        transaction_traces: vec![transaction(0), transaction(1)],
        ..Default::default()
    }
    .encode_to_vec()
}

/// A Solana block with one transaction, its instructions and a reward.
fn solana_block(slot: u64, id: u8) -> Vec<u8> {
    let hash = |byte: u8| firehose_parquet::encode::encode_base58(&[byte; 32]);
    solana::Block {
        slot,
        parent_slot: slot - 1,
        blockhash: hash(id),
        previous_blockhash: hash(0x02),
        block_height: Some(solana::BlockHeight { block_height: slot }),
        block_time: Some(solana::UnixTimestamp {
            timestamp: seconds(slot),
        }),
        transactions: vec![solana::ConfirmedTransaction {
            transaction: Some(solana::Transaction {
                signatures: vec![vec![id; 64].into()],
                message: Some(solana::Message {
                    header: Some(solana::MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 1,
                    }),
                    account_keys: vec![vec![2u8; 32].into(), vec![3u8; 32].into()],
                    recent_blockhash: vec![4u8; 32].into(),
                    instructions: vec![solana::CompiledInstruction {
                        program_id_index: 1,
                        accounts: vec![0, 1].into(),
                        data: vec![5, 6, 7].into(),
                    }],
                    ..Default::default()
                }),
            }),
            meta: Some(solana::TransactionStatusMeta {
                fee: 5_000,
                pre_balances: vec![100_000, 0],
                post_balances: vec![95_000, 0],
                log_messages: vec!["Program log: engine test".into()],
                return_data: Some(solana::ReturnData {
                    program_id: vec![3u8; 32].into(),
                    data: vec![42, 43].into(),
                }),
                compute_units_consumed: Some(1_234),
                ..Default::default()
            }),
        }],
        rewards: vec![solana::Reward {
            pubkey: "RewardPubkey".into(),
            lamports: 42,
            post_balance: 999,
            reward_type: 1,
            commission: String::new(),
        }],
    }
    .encode_to_vec()
}

/// One dataset written by `build`.
struct Dataset {
    name: &'static str,
    block_type: &'static str,
    final_only: bool,
    /// Milliseconds of the latest row `timestamp` within its second.
    millis: u64,
    events: Vec<Event>,
    tables: Vec<Table>,
}

/// A checked table and the engine types of some of its columns.
struct Table {
    name: &'static str,
    /// `(column, DuckDB type, Polars type)`. Dictionary-encoded enum strings
    /// read as `VARCHAR` in DuckDB and `Categorical` in Polars.
    columns: Vec<(&'static str, &'static str, &'static str)>,
}

/// Columns every table has: the canonical identity and the date key.
const CANONICAL: [(&str, &str, &str); 3] = [
    ("block_num", "UBIGINT", "UInt64"),
    (
        "timestamp",
        "TIMESTAMP WITH TIME ZONE",
        "Datetime(time_unit='ms', time_zone='UTC')",
    ),
    ("date", "DATE", "Date"),
];

fn datasets() -> Vec<Dataset> {
    let final_events: Vec<Event> = (100..104).map(|n| (n, 0xa0 + (n - 100) as u8, 3)).collect();
    let evm_tables = || {
        vec![
            Table {
                name: "blocks",
                columns: vec![
                    ("num_transactions", "UINTEGER", "UInt32"),
                    ("detail_level", "VARCHAR", "Categorical"),
                ],
            },
            Table {
                name: "transactions",
                columns: vec![
                    ("index", "UINTEGER", "UInt32"),
                    ("type", "VARCHAR", "Categorical"),
                    ("status", "VARCHAR", "Categorical"),
                ],
            },
            Table {
                name: "logs",
                columns: vec![("log_index", "UINTEGER", "UInt32")],
            },
            Table {
                name: "access_lists",
                columns: vec![("storage_keys", "VARCHAR[]", "List(String)")],
            },
        ]
    };
    vec![
        Dataset {
            name: "evm-final",
            block_type: "evm",
            final_only: true,
            millis: 250,
            events: final_events.clone(),
            tables: evm_tables(),
        },
        Dataset {
            name: "evm-live",
            block_type: "evm",
            final_only: false,
            millis: 250,
            // A reorg at the tip: NEW(103), UNDO(103), NEW(103').
            events: vec![
                (100, 0xa0, 1),
                (101, 0xa1, 1),
                (102, 0xa2, 1),
                (103, 0xa3, 1),
                (103, 0xa3, 2),
                (103, 0xb3, 1),
            ],
            tables: evm_tables(),
        },
        Dataset {
            name: "solana-final",
            block_type: "solana",
            final_only: true,
            millis: 0,
            events: final_events,
            tables: vec![
                Table {
                    name: "transactions",
                    columns: vec![
                        ("transaction_index", "UINTEGER", "UInt32"),
                        ("pre_balances", "UBIGINT[]", "List(UInt64)"),
                        ("return_data", "BLOB", "Binary"),
                    ],
                },
                Table {
                    name: "instructions",
                    columns: vec![
                        ("accounts", "UTINYINT[]", "List(UInt8)"),
                        ("data", "BLOB", "Binary"),
                    ],
                },
                Table {
                    name: "rewards",
                    columns: vec![("reward_type", "VARCHAR", "Categorical")],
                },
            ],
        },
    ]
}

impl Dataset {
    fn responses(&self) -> Vec<firehose::Response> {
        let (type_url, block): (&str, fn(u64, u8) -> Vec<u8>) = match self.block_type {
            "evm" => ("type.googleapis.com/sf.ethereum.type.v2.Block", evm_block),
            _ => ("type.googleapis.com/sf.solana.type.v1.Block", solana_block),
        };
        self.events
            .iter()
            .enumerate()
            .map(|(ordinal, event)| response(type_url, block(event.0, event.1), *event, ordinal))
            .collect()
    }
}

/// `build` of `dataset` from a mock Firehose into `root`, one part per block.
async fn build(dataset: &Dataset, cwd: &Path, root: &Path) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let stream = Stream {
        responses: Arc::new(dataset.responses()),
        final_only: dataset.final_only,
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Info)
            .add_service(stream)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let output = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"))
            .kill_on_drop(true)
            .env_clear()
            .current_dir(cwd)
            .args(["build", "--endpoint", &endpoint])
            .args(["--block-type", dataset.block_type])
            .args(["--start-block", "100", "--stop-block", "104"])
            .args(["--flush-blocks", "1", "--stream-idle-timeout-secs", "0"])
            .arg(format!("--final-blocks-only={}", dataset.final_only))
            .arg("--output")
            .arg(root)
            .output(),
    )
    .await
    .expect("fireparq timed out")
    .unwrap();
    server.abort();
    assert!(
        output.status.success(),
        "{}: {}{}",
        dataset.name,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Every `.parquet` file below `dir`.
fn parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut pending = vec![dir.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// What the files of one table hold, read directly: rows in total and on
/// [`DAY`], and the files of that day.
struct Expected {
    rows: u64,
    rows_on_day: u64,
    files_on_day: Vec<String>,
}

fn expected(root: &Path, table: &str) -> Expected {
    let mut expected = Expected {
        rows: 0,
        rows_on_day: 0,
        files_on_day: Vec::new(),
    };
    for file in parquet_files(&root.join(table)) {
        let partition = file
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            partition.starts_with("date=2023-11-1"),
            "{table}: {file:?} is not in a date partition"
        );
        let rows: u64 = read_parquet(&file)
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows() as u64)
            .sum();
        expected.rows += rows;
        if partition == format!("date={DAY}") {
            expected.rows_on_day += rows;
            expected
                .files_on_day
                .push(file.to_str().unwrap().to_string());
        }
    }
    assert!(expected.rows > expected.rows_on_day && expected.rows_on_day > 0);
    expected
}

/// The DuckDB CLI, or `None` locally when it is missing (see the module docs).
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
    eprintln!("skipping the DuckDB engine check: no DuckDB CLI ({candidate:?})");
    None
}

/// The Polars interpreter, or `None` locally when it is missing.
fn polars() -> Option<PathBuf> {
    let candidate = std::env::var_os("FIREPARQ_POLARS_PYTHON").map(PathBuf::from);
    let available = candidate.as_ref().is_some_and(|python| {
        std::process::Command::new(python)
            .args(["-c", "import polars"])
            .env_clear()
            .output()
            .is_ok_and(|output| output.status.success())
    });
    if available {
        return candidate;
    }
    assert!(
        std::env::var_os("FIREPARQ_REQUIRE_POLARS").is_none(),
        "FIREPARQ_REQUIRE_POLARS is set but FIREPARQ_POLARS_PYTHON ({candidate:?}) cannot import polars"
    );
    eprintln!(
        "skipping the Polars engine check: set FIREPARQ_POLARS_PYTHON to a Python with polars"
    );
    None
}

/// Runs `sql` in a fresh in-memory DuckDB without `~/.duckdbrc` and returns
/// the rows of its last statement.
fn duckdb_rows(duckdb: &Path, cwd: &Path, sql: &str) -> Vec<Value> {
    let init = cwd.join("empty.duckdbrc");
    std::fs::write(&init, "").unwrap();
    let output = std::process::Command::new(duckdb)
        .env_clear()
        .current_dir(cwd)
        .arg("-init")
        .arg(&init)
        .args(["-json", "-c", sql])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{sql}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    if stdout.trim().is_empty() {
        return Vec::new();
    }
    serde_json::from_str(stdout.trim()).unwrap_or_else(|error| panic!("{error}: {stdout}"))
}

/// A JSON number or numeric string (DuckDB versions differ) as `u64`.
fn number(value: &Value) -> u64 {
    match value {
        Value::Number(number) => number.as_u64().unwrap(),
        Value::String(text) => text.parse().unwrap(),
        other => panic!("not a number: {other}"),
    }
}

/// DuckDB's view of one table; returns its row count.
fn check_duckdb(duckdb: &Path, cwd: &Path, root: &str, dataset: &Dataset, table: &Table) -> u64 {
    let context = format!("duckdb {} {}", dataset.name, table.name);
    let glob = format!("{root}/{}/**/*.parquet", table.name);
    let expected = expected(Path::new(root), table.name);
    let types: BTreeMap<String, String> = duckdb_rows(
        duckdb,
        cwd,
        &format!("DESCRIBE SELECT * FROM read_parquet('{glob}', hive_partitioning = true)"),
    )
    .into_iter()
    .map(|row| {
        (
            row["column_name"].as_str().unwrap().to_string(),
            row["column_type"].as_str().unwrap().to_string(),
        )
    })
    .collect();
    for (column, duck, _) in CANONICAL.iter().chain(&table.columns) {
        assert_eq!(
            types.get(*column).map(String::as_str),
            Some(*duck),
            "{context}: {column} in {types:?}"
        );
    }
    assert_eq!(
        types.get("stream_ordinal").map(String::as_str),
        (!dataset.final_only).then_some("UBIGINT"),
        "{context}"
    );
    // The `date=` directory is the only partition column.
    assert!(
        !types.contains_key("year") && !types.contains_key("day"),
        "{context}"
    );

    let row = &duckdb_rows(
        duckdb,
        cwd,
        &format!(
            "SELECT count(*) AS n, \
             epoch_ms(max(timestamp)) % 1000 AS millis, \
             count(*) FILTER (WHERE regexp_matches(replace(filename, '{root}/', ''), '(^|/)[._]')) \
               AS hidden, \
             (SELECT count(*) FROM read_parquet('{glob}', hive_partitioning = true) \
               WHERE date = DATE '{DAY}') AS on_day, \
             (SELECT count(DISTINCT date) FROM read_parquet('{glob}', hive_partitioning = true) \
               WHERE date = DATE '{DAY}') AS days_on_day, \
             (SELECT string_agg(DISTINCT filename, ',' ORDER BY filename) \
               FROM read_parquet('{glob}', hive_partitioning = true, filename = true) \
               WHERE date = DATE '{DAY}') AS files_on_day, \
             (SELECT count(*) FROM read_parquet('{glob}', hive_partitioning = false, filename = true) \
               WHERE date IS DISTINCT FROM \
                 CAST(regexp_extract(filename, 'date=([0-9-]{{10}})/[^/]+$', 1) AS DATE)) \
               AS stored_mismatches \
             FROM read_parquet('{glob}', hive_partitioning = true, filename = true)"
        ),
    )[0];
    assert_eq!(number(&row["n"]), expected.rows, "{context}: {row}");
    assert_eq!(number(&row["millis"]), dataset.millis, "{context}: {row}");
    assert_eq!(number(&row["hidden"]), 0, "{context}: {row}");
    assert_eq!(
        number(&row["on_day"]),
        expected.rows_on_day,
        "{context}: {row}"
    );
    assert_eq!(number(&row["days_on_day"]), 1, "{context}: {row}");
    assert_eq!(
        row["files_on_day"]
            .as_str()
            .unwrap()
            .split(',')
            .collect::<Vec<_>>(),
        expected.files_on_day,
        "{context}"
    );
    assert_eq!(number(&row["stored_mismatches"]), 0, "{context}: {row}");

    // The Parquet logical type is TIMESTAMP(MILLIS, UTC).
    let schema = duckdb_rows(
        duckdb,
        cwd,
        &format!(
            "SELECT DISTINCT converted_type, CAST(logical_type AS VARCHAR) AS logical \
             FROM parquet_schema('{glob}') WHERE name = 'timestamp'"
        ),
    );
    assert_eq!(schema.len(), 1, "{context}: {schema:?}");
    let logical = schema[0]["logical"]
        .as_str()
        .unwrap_or_default()
        .to_uppercase();
    assert!(
        schema[0]["converted_type"] == json!("TIMESTAMP_MILLIS")
            || (logical.contains("MILLIS") && logical.contains("UTC=1")),
        "{context}: {schema:?}"
    );
    expected.rows
}

/// Polars' view of every table of `dataset`; returns the row counts.
fn check_polars(python: &Path, root: &str, dataset: &Dataset) -> BTreeMap<String, u64> {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/engines/polars_check.py");
    let spec = json!({
        "root": root,
        "tables": dataset.tables.iter().map(|table| table.name).collect::<Vec<_>>(),
        "day": DAY,
    });
    let output = std::process::Command::new(python)
        .env_clear()
        .arg(script)
        .arg(spec.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "polars {}: {}",
        dataset.name,
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    eprintln!("polars {} read {}", report["polars"], dataset.name);
    let mut rows = BTreeMap::new();
    for table in &dataset.tables {
        let context = format!("polars {} {}", dataset.name, table.name);
        let seen = &report["tables"][table.name];
        let expected = expected(Path::new(root), table.name);
        let schema = &seen["schema"];
        for (column, _, polars) in CANONICAL.iter().chain(&table.columns) {
            assert_eq!(
                schema[*column].as_str(),
                Some(*polars),
                "{context}: {column} in {schema}"
            );
        }
        assert_eq!(
            schema["stream_ordinal"].as_str(),
            (!dataset.final_only).then_some("UInt64"),
            "{context}"
        );
        assert_eq!(number(&seen["rows"]), expected.rows, "{context}");
        assert_eq!(
            number(&seen["rows_on_day"]),
            expected.rows_on_day,
            "{context}"
        );
        assert_eq!(seen["days_on_day"], json!([DAY]), "{context}");
        assert_eq!(
            seen["paths_on_day"],
            json!(expected.files_on_day),
            "{context}"
        );
        assert_eq!(number(&seen["stored_date_mismatches"]), 0, "{context}");
        assert_eq!(
            number(&seen["max_timestamp_ms"]) % 1000,
            dataset.millis,
            "{context}"
        );
        for path in seen["paths"].as_array().unwrap() {
            let relative = path.as_str().unwrap().strip_prefix(root).unwrap();
            assert!(
                !relative
                    .split('/')
                    .any(|part| part.starts_with('_') || part.starts_with('.')),
                "{context}: {relative}"
            );
        }
        rows.insert(table.name.to_string(), number(&seen["rows"]));
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_and_polars_read_date_partitioned_output() {
    let duckdb = duckdb();
    let polars = polars();
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    for dataset in datasets() {
        let root = cwd.join(dataset.name);
        build(&dataset, &cwd, &root).await;
        // The dataset root holds the tables, `_fireparq/` (with a Parquet
        // cursor mirror no table read may pick up) and dot-prefixed state.
        assert!(root.join("_fireparq/cursor.parquet").is_file());
        assert!(root.join(".fireparq-ingest").is_dir());
        let root_str = root.to_str().unwrap();
        let duck_rows: Option<BTreeMap<String, u64>> = duckdb.as_ref().map(|duckdb| {
            dataset
                .tables
                .iter()
                .map(|table| {
                    let rows = check_duckdb(duckdb, &cwd, root_str, &dataset, table);
                    (table.name.to_string(), rows)
                })
                .collect()
        });
        let polars_rows = polars
            .as_ref()
            .map(|python| check_polars(python, root_str, &dataset));
        if let (Some(duck), Some(polars)) = (&duck_rows, &polars_rows) {
            assert_eq!(
                duck, polars,
                "{}: row counts differ across engines",
                dataset.name
            );
        }
    }
}
