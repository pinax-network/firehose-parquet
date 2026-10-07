//! #652, #643 L8: real `fireparq build` output read by the target engines,
//! DuckDB (`delta_scan`) and delta-rs (a snapshot's schema, active files and
//! partition pruning, then those files' rows: how Polars' `scan_delta` reads
//! through delta-rs), through each table's Delta log.
//!
//! A mock Firehose serves two UTC days of blocks. `build` writes EVM (final
//! and non-final), Solana (list, binary, decimal and enum columns) and SEC
//! (the real fixture filings: `decimal(38,s)`, data `date`, `array<struct>`,
//! `array<date>`, `array<integer>` and `binary` columns) datasets. delta-rs first writes a checkpoint of every table, so each
//! `_delta_log/` also holds Parquet, next to the Parquet cursor mirror in
//! `_fireparq/`. Then both engines must read, for **every** table of every
//! dataset (tables that never get rows included):
//!
//! - exactly the rows the log's `add` actions count, the same in both engines;
//! - only the log's data files (`<table>/date=YYYY-MM-DD/part-v1-*.parquet`),
//!   nothing under `_delta_log/`, `_fireparq/` or a dot-prefixed path;
//! - `date` as the partition column: a `date` filter returns exactly that
//!   day's rows, and both engines prune to that day's files; the data files
//!   themselves hold no `date` column;
//! - `block_num` as `BIGINT`/`Int64` and `timestamp` as a UTC timestamp stored
//!   in microseconds (Parquet `TIMESTAMP(MICROS, UTC)`), holding whole
//!   milliseconds;
//! - `stream_ordinal` in non-final output only;
//!
//! and, for the listed columns, their Delta types: signed integers (`long`,
//! `array<short>`) for the mapper's unsigned ones, `decimal(20,0)` for the
//! chain's decimal columns (an EVM block nonce of `u64::MAX`, exactly),
//! `string` for enums, lists and binary columns, and SEC's native types
//! passed through (`decimal(38,s)`, `date`, `array<struct<…>>`, `binary`).
//!
//! `anonymous_reads_of_a_public_deployment_bucket` is an opt-in check against
//! a deployment's public-read bucket (RGW), off unless `FIREPARQ_RGW_ENDPOINT`
//! and `FIREPARQ_RGW_BUCKET` are set; see its docs. Engines: see
//! `common/mod.rs`.
use firehose_parquet::delta::store::DeltaStore;
use firehose_protos::{eth, firehose, solana};
use object_store_delta::ObjectStoreExt as _;
use prost::Message;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

mod common;
mod sec_fixture;
use blocks::sec::proto::sec;
use common::{number, DuckDb};

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
        // A PoW nonce above i64::MAX: `blocks.nonce` is decimal(20,0).
        header: Some(eth::BlockHeader {
            number,
            nonce: u64::MAX,
            gas_used: 42_000,
            ..Default::default()
        }),
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

/// A SEC window. Windows 100 and 102, one on each UTC day, hold every real
/// fixture filing (`tests/fixtures/sec-v013/`) re-homed into the window
/// (header number and time are the stream's, as the mapper requires), so
/// every SEC table has rows on both days; 101 and 103 are empty windows. The
/// first filing also carries `raw_xml` (the `filing_raw_xml.raw_xml` binary
/// column).
fn sec_block(number: u64, _id: u8) -> Vec<u8> {
    let mut filings: Vec<sec::Filing> = Vec::new();
    if number.is_multiple_of(2) {
        filings = sec_fixture::blocks()
            .iter()
            .flat_map(|block| {
                <sec::Block as Message>::decode(block.payload.as_slice())
                    .unwrap()
                    .filings
            })
            .enumerate()
            .map(|(position, filing)| sec::Filing {
                ordinal: position as u64,
                ..filing
            })
            .collect();
        filings[0].raw_xml = b"<edgarSubmission>engine test</edgarSubmission>"
            .to_vec()
            .into();
    }
    let day = seconds(number).div_euclid(86_400);
    sec::Block {
        header: Some(sec::BlockHeader {
            block_number: number,
            block_time: Some(prost_types::Timestamp {
                seconds: seconds(number),
                nanos: 0,
            }),
            feed_date: sec_fixture::iso_date(day as i32),
        }),
        filings,
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
    /// Tables whose listed columns are checked, besides the checks every
    /// table gets. Each must have rows.
    tables: Vec<Table>,
}

/// A table with the engine types of some of its columns.
struct Table {
    name: &'static str,
    /// `(column, DuckDB type, delta-rs Arrow type)`. Enum columns are plain
    /// strings in the files (their pages are still dictionary-encoded).
    columns: Vec<(&'static str, &'static str, &'static str)>,
    /// `(column, exact minimum)`: both engines' `min`, as text.
    minimums: Vec<(&'static str, &'static str)>,
}

/// Columns every table has: the canonical identity and the date key.
const CANONICAL: [(&str, &str, &str); 3] = [
    ("block_num", "BIGINT", "Int64"),
    (
        "timestamp",
        "TIMESTAMP WITH TIME ZONE",
        "Timestamp(µs, \"UTC\")",
    ),
    ("date", "DATE", "Date32"),
];

/// Arrow's spelling of a Delta `decimal(20,0)`, as delta-rs reads it.
const DELTA_DECIMAL: &str = "Decimal128(20, 0)";

fn datasets() -> Vec<Dataset> {
    let final_events: Vec<Event> = (100..104).map(|n| (n, 0xa0 + (n - 100) as u8, 3)).collect();
    let evm_tables = || {
        vec![
            Table {
                name: "blocks",
                columns: vec![
                    ("num_transactions", "BIGINT", "Int64"),
                    ("gas_used", "BIGINT", "Int64"),
                    ("nonce", "DECIMAL(20,0)", DELTA_DECIMAL),
                    ("detail_level", "VARCHAR", "Utf8"),
                ],
                minimums: vec![("nonce", "18446744073709551615"), ("gas_used", "42000")],
            },
            Table {
                name: "transactions",
                columns: vec![
                    ("index", "BIGINT", "Int64"),
                    ("type", "VARCHAR", "Utf8"),
                    ("status", "VARCHAR", "Utf8"),
                ],
                minimums: vec![("gas_used", "21000")],
            },
            Table {
                name: "logs",
                columns: vec![("log_index", "BIGINT", "Int64")],
                minimums: vec![],
            },
            Table {
                name: "access_lists",
                columns: vec![("storage_keys", "VARCHAR[]", "List(Utf8, field: 'element')")],
                minimums: vec![],
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
                        ("transaction_index", "BIGINT", "Int64"),
                        ("fee", "DECIMAL(20,0)", DELTA_DECIMAL),
                        (
                            "pre_balances",
                            "DECIMAL(20,0)[]",
                            "List(Decimal128(20, 0), field: 'element')",
                        ),
                        ("compute_units_consumed", "BIGINT", "Int64"),
                        ("return_data", "BLOB", "Binary"),
                    ],
                    minimums: vec![("fee", "5000"), ("compute_units_consumed", "1234")],
                },
                Table {
                    name: "instructions",
                    columns: vec![
                        (
                            "accounts",
                            "SMALLINT[]",
                            "List(non-null Int16, field: 'element')",
                        ),
                        ("data", "BLOB", "Binary"),
                    ],
                    minimums: vec![],
                },
                Table {
                    name: "rewards",
                    columns: vec![
                        ("reward_type", "VARCHAR", "Utf8"),
                        ("post_balance", "DECIMAL(20,0)", DELTA_DECIMAL),
                    ],
                    minimums: vec![("post_balance", "999")],
                },
            ],
        },
        Dataset {
            name: "sec-final",
            block_type: "sec",
            final_only: true,
            millis: 250,
            events: (100..104).map(|n| (n, 0xa0 + (n - 100) as u8, 3)).collect(),
            tables: sec_tables(),
        },
    ]
}

/// The SEC columns whose types no other chain has: `decimal(38,s)` in the
/// five scale families, data `date` columns next to the `date` partition,
/// `array<struct<…>>`, `array<date>`, `array<integer>` and `binary`.
fn sec_tables() -> Vec<Table> {
    vec![
        Table {
            name: "filings",
            columns: vec![
                ("filing_index", "BIGINT", "Int64"),
                ("filing_date", "DATE", "Date32"),
                (
                    "acceptance_datetime",
                    "TIMESTAMP WITH TIME ZONE",
                    "Timestamp(µs, \"UTC\")",
                ),
                ("dissemination_lag_days", "INTEGER", "Int32"),
                ("body_kind", "VARCHAR", "Utf8"),
                (
                    "dissemination_flags",
                    "VARCHAR[]",
                    "List(Utf8, field: 'element')",
                ),
            ],
            minimums: vec![("filing_date", "2004-06-21")],
        },
        Table {
            name: "filing_raw_xml",
            columns: vec![("raw_xml", "BLOB", "Binary")],
            minimums: vec![],
        },
        Table {
            name: "filing_parties",
            columns: vec![(
                "former_names",
                "STRUCT(\"name\" VARCHAR, date_changed DATE)[]",
                "List(Struct(\"name\": Utf8, \"date_changed\": Date32), field: 'element')",
            )],
            minimums: vec![],
        },
        Table {
            name: "ownership_transactions",
            columns: vec![
                ("shares", "DECIMAL(38,6)", "Decimal128(38, 6)"),
                ("value_usd", "DECIMAL(38,6)", "Decimal128(38, 6)"),
                ("transaction_date", "DATE", "Date32"),
            ],
            minimums: vec![("shares", "86.000000")],
        },
        Table {
            name: "form13f_holdings",
            columns: vec![
                ("value", "BIGINT", "Int64"),
                (
                    "other_manager_sequence_numbers",
                    "INTEGER[]",
                    "List(Int32, field: 'element')",
                ),
            ],
            minimums: vec![],
        },
        Table {
            name: "form144_notices",
            columns: vec![
                (
                    "plan_adoption_dates",
                    "DATE[]",
                    "List(Date32, field: 'element')",
                ),
                (
                    "total_aggregate_market_value",
                    "DECIMAL(38,2)",
                    "Decimal128(38, 2)",
                ),
            ],
            minimums: vec![],
        },
        Table {
            name: "nport_holdings",
            columns: vec![
                ("balance", "DECIMAL(38,10)", "Decimal128(38, 10)"),
                ("pct_value", "DECIMAL(38,12)", "Decimal128(38, 12)"),
            ],
            minimums: vec![("pct_value", "-18.124339407100")],
        },
        Table {
            name: "npx_vote_records",
            columns: vec![("shares_voted", "DECIMAL(38,16)", "Decimal128(38, 16)")],
            minimums: vec![],
        },
        Table {
            name: "parse_issues",
            columns: vec![("issue", "VARCHAR", "Utf8"), ("index_1", "BIGINT", "Int64")],
            minimums: vec![],
        },
    ]
}

impl Dataset {
    fn responses(&self) -> Vec<firehose::Response> {
        let (type_url, block): (&str, fn(u64, u8) -> Vec<u8>) = match self.block_type {
            "evm" => ("type.googleapis.com/sf.ethereum.type.v2.Block", evm_block),
            "sec" => ("type.googleapis.com/pinax.sec.v1.Block", sec_block),
            _ => ("type.googleapis.com/sf.solana.type.v1.Block", solana_block),
        };
        self.events
            .iter()
            .enumerate()
            .map(|(ordinal, event)| response(type_url, block(event.0, event.1), *event, ordinal))
            .collect()
    }

    fn checked(&self, table: &str) -> Option<&Table> {
        self.tables.iter().find(|checked| checked.name == table)
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

/// What one table's Delta log adds: rows in total and on [`DAY`], and the
/// data files (paths relative to the table).
#[derive(Default)]
struct Expected {
    rows: u64,
    rows_on_day: u64,
    files: Vec<String>,
    files_on_day: Vec<String>,
}

/// Reads the JSON commits of a table that nothing but fireparq wrote (only
/// `add` actions, no `remove`).
fn expected(table: &Path) -> Expected {
    let mut expected = Expected::default();
    for actions in common::delta_log(table).values() {
        assert!(common::action(actions, "remove").is_none());
        for add in actions.iter().filter_map(|action| action.get("add")) {
            let path = add["path"].as_str().unwrap().to_string();
            let date = add["partitionValues"]["date"].as_str().unwrap();
            assert!(path.starts_with(&format!("date={date}/part-v1-")), "{path}");
            let stats: Value = serde_json::from_str(add["stats"].as_str().unwrap()).unwrap();
            let rows = stats["numRecords"].as_u64().unwrap();
            expected.rows += rows;
            if date == DAY {
                expected.rows_on_day += rows;
                expected.files_on_day.push(path.clone());
            }
            expected.files.push(path);
        }
    }
    expected.files.sort();
    expected.files_on_day.sort();
    expected
}

/// The `Scanning Files` value (`read/total`) of the Delta scan in a DuckDB
/// JSON profile.
fn scanning_files(profile: &Value) -> Option<String> {
    if let Some(files) = profile["extra_info"]["Scanning Files"].as_str() {
        return Some(files.to_string());
    }
    profile["children"]
        .as_array()?
        .iter()
        .find_map(scanning_files)
}

/// DuckDB's view of one table; returns its row count.
fn check_duckdb(
    duckdb: &DuckDb,
    cwd: &Path,
    root: &Path,
    dataset: &Dataset,
    table: &str,
    expected: &Expected,
) -> u64 {
    let context = format!("duckdb {} {} {}", duckdb.version, dataset.name, table);
    let location = root.join(table);
    let location = location.to_str().unwrap();
    // The file path under a name no table uses (SEC's `filing_documents` has
    // a `filename` column).
    let scan = format!("delta_scan('{location}', filename = 'fireparq_data_file')");
    let checked = dataset.checked(table);
    let minimums: String = checked
        .map(|checked| {
            checked
                .minimums
                .iter()
                .map(|(column, _)| format!(", CAST(min({column}) AS VARCHAR) AS min_{column}"))
                .collect()
        })
        .unwrap_or_default();
    let profile = cwd.join(format!("profile-{}-{table}.json", dataset.name));
    let mut sql = format!(
        "SELECT 'types' AS q, column_name, column_type \
           FROM (DESCRIBE SELECT * FROM delta_scan('{location}')); \
         SELECT 'stats' AS q, count(*) AS n, \
           epoch_ms(max(timestamp)) % 1000 AS millis, \
           epoch_us(max(timestamp)) % 1000 AS micros, \
           count(*) FILTER (WHERE NOT (starts_with(fireparq_data_file, '{location}/date=') \
             AND regexp_matches(fireparq_data_file, '/date=[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}/part-v1-[^/]+[.]parquet$'))) \
             AS foreign_files, \
           count(*) FILTER (WHERE date = DATE '{DAY}') AS on_day, \
           count(DISTINCT date) FILTER (WHERE date = DATE '{DAY}') AS days_on_day, \
           string_agg(DISTINCT replace(fireparq_data_file, '{location}/', ''), ',' ORDER BY replace(fireparq_data_file, '{location}/', '')) \
             FILTER (WHERE date = DATE '{DAY}') AS files_on_day, \
           count(*) FILTER (WHERE date IS DISTINCT FROM \
             CAST(regexp_extract(fireparq_data_file, 'date=([0-9-]{{10}})/[^/]+$', 1) AS DATE)) \
             AS partition_mismatches{minimums} \
           FROM {scan}; \
         PRAGMA enable_profiling = 'json'; SET profiling_output = '{}'; \
         SELECT 'pruned' AS q, count(*) AS n FROM delta_scan('{location}') WHERE date = DATE '{DAY}'; \
         PRAGMA disable_profiling;",
        profile.display()
    );
    if !expected.files.is_empty() {
        let files = expected
            .files
            .iter()
            .map(|file| format!("'{location}/{file}'"))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(
            " SELECT 'stored' AS q, column_name \
                FROM (DESCRIBE SELECT * FROM read_parquet([{files}], hive_partitioning = false)); \
              SELECT DISTINCT 'timestamp' AS q, converted_type, CAST(logical_type AS VARCHAR) AS logical \
                FROM parquet_schema([{files}]) WHERE name = 'timestamp';"
        ));
    }
    let rows = duckdb.query(&sql);
    let types: BTreeMap<String, String> = rows["types"]
        .iter()
        .map(|row| {
            (
                row["column_name"].as_str().unwrap().to_string(),
                row["column_type"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let columns = checked
        .map(|checked| checked.columns.as_slice())
        .unwrap_or_default();
    for (column, duck, _) in CANONICAL.iter().chain(columns) {
        assert_eq!(
            types.get(*column).map(String::as_str),
            Some(*duck),
            "{context}: {column} in {types:?}"
        );
    }
    assert_eq!(
        types.get("stream_ordinal").map(String::as_str),
        (!dataset.final_only).then_some("BIGINT"),
        "{context}"
    );
    // `date=` is the only partition column.
    assert!(
        !types.contains_key("year") && !types.contains_key("day"),
        "{context}"
    );

    let stats = &rows["stats"][0];
    assert_eq!(number(&stats["n"]), expected.rows, "{context}: {stats}");
    assert_eq!(number(&stats["foreign_files"]), 0, "{context}: {stats}");
    assert_eq!(
        number(&stats["on_day"]),
        expected.rows_on_day,
        "{context}: {stats}"
    );
    assert_eq!(
        number(&stats["partition_mismatches"]),
        0,
        "{context}: {stats}"
    );
    if let Some(checked) = checked {
        for (column, minimum) in &checked.minimums {
            assert_eq!(
                stats[format!("min_{column}")],
                json!(minimum),
                "{context}: min({column})"
            );
        }
    }
    let pruned = &rows["pruned"][0];
    assert_eq!(number(&pruned["n"]), expected.rows_on_day, "{context}");
    let profile: Value = serde_json::from_slice(&std::fs::read(&profile).unwrap()).unwrap();
    if expected.rows > 0 {
        assert_eq!(
            number(&stats["millis"]),
            dataset.millis,
            "{context}: {stats}"
        );
        assert_eq!(
            number(&stats["micros"]),
            0,
            "{context}: whole milliseconds: {stats}"
        );
        assert_eq!(number(&stats["days_on_day"]), 1, "{context}: {stats}");
        assert_eq!(
            stats["files_on_day"]
                .as_str()
                .unwrap()
                .split(',')
                .collect::<Vec<_>>(),
            expected.files_on_day,
            "{context}"
        );
        // The `date` filter prunes to that day's files.
        assert_eq!(
            scanning_files(&profile),
            Some(format!(
                "{}/{}",
                expected.files_on_day.len(),
                expected.files.len()
            )),
            "{context}: {profile}"
        );
        // The data files hold no `date` column: it is the partition value only.
        let stored: Vec<&str> = rows["stored"]
            .iter()
            .map(|row| row["column_name"].as_str().unwrap())
            .collect();
        assert!(
            !stored.contains(&"date") && stored.contains(&"block_num"),
            "{context}: {stored:?}"
        );
        // The Parquet logical type is TIMESTAMP(MICROS, UTC), which Polars
        // requires under a Delta `timestamp` column.
        let schema = &rows["timestamp"];
        assert_eq!(schema.len(), 1, "{context}: {schema:?}");
        let logical = schema[0]["logical"]
            .as_str()
            .unwrap_or_default()
            .to_uppercase();
        assert!(
            schema[0]["converted_type"] == json!("TIMESTAMP_MICROS")
                || (logical.contains("MICROS") && logical.contains("UTC=1")),
            "{context}: {schema:?}"
        );
    }
    expected.rows
}

/// delta-rs's view of every table of `dataset`, after a checkpoint of each:
/// the snapshot's schema and active files, its partition pruning, and the
/// rows of those files (the way Polars' `scan_delta` reads through delta-rs);
/// returns the row counts.
async fn check_delta_rs(
    root: &Path,
    dataset: &Dataset,
    tables: &[String],
    expected: &BTreeMap<String, Expected>,
) -> BTreeMap<String, u64> {
    let mut rows = BTreeMap::new();
    for table in tables {
        let context = format!("delta-rs {} {table}", dataset.name);
        let expected = &expected[table];
        // A checkpoint first, so the log also holds Parquet that no read may
        // pick up; the tables are then read through it.
        common::delta_checkpoint(&common::open_local(root, table).await).await;
        let delta = common::open_local(root, table).await;
        let read = common::delta_read(&delta).await;
        let checked = dataset.checked(table);
        let columns = checked
            .map(|checked| checked.columns.as_slice())
            .unwrap_or_default();
        for (column, _, arrow) in CANONICAL.iter().chain(columns) {
            assert_eq!(
                read.types.get(*column).map(String::as_str),
                Some(*arrow),
                "{context}: {column} in {:?}",
                read.types
            );
        }
        assert_eq!(
            read.types.get("stream_ordinal").map(String::as_str),
            (!dataset.final_only).then_some("Int64"),
            "{context}"
        );
        // Only the log's data files, and the `date` filter prunes to the day's.
        let sorted = |files: &[String]| {
            let mut files = files.to_vec();
            files.sort();
            files
        };
        assert_eq!(read.paths(), sorted(&expected.files), "{context}");
        assert_eq!(
            common::delta_day_files(&delta, DAY).await,
            sorted(&expected.files_on_day),
            "{context}"
        );
        let batches = common::delta_batches(&root.join(table), &read);
        let count: u64 = batches
            .iter()
            .map(|(_, batch)| batch.num_rows() as u64)
            .sum();
        let on_day: u64 = batches
            .iter()
            .filter(|(date, _)| date == DAY)
            .map(|(_, batch)| batch.num_rows() as u64)
            .sum();
        assert_eq!(count, expected.rows, "{context}");
        assert_eq!(on_day, expected.rows_on_day, "{context}");
        if let Some(checked) = checked {
            for (column, minimum) in &checked.minimums {
                assert_eq!(
                    common::minimum(&batches, column).as_deref(),
                    Some(*minimum),
                    "{context}: min({column})"
                );
            }
        }
        if let Some((_, batch)) = batches.first() {
            // The data files hold no `date` column: it is the partition value.
            let schema = batch.schema();
            let stored: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert!(
                !stored.contains(&"date") && stored.contains(&"block_num"),
                "{context}: the data files hold no date column: {stored:?}"
            );
            // Microseconds that hold whole milliseconds.
            let micros = common::max_timestamp_micros(&batches).unwrap() as u64;
            assert_eq!(micros % 1000, 0, "{context}");
            assert_eq!(micros / 1000 % 1000, dataset.millis, "{context}");
        }
        rows.insert(table.clone(), count);
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_and_delta_rs_read_every_delta_table() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let duckdb = DuckDb::open(&cwd);
    for dataset in datasets() {
        let root = cwd.join(dataset.name);
        build(&dataset, &cwd, &root).await;
        // The dataset root holds the tables, `_fireparq/` (with a Parquet
        // cursor mirror no table read may pick up) and dot-prefixed state.
        assert!(root.join("_fireparq/cursor.parquet").is_file());
        assert!(root.join(".fireparq-ingest").is_dir());
        let tables = common::delta_tables(&root);
        let expected: BTreeMap<String, Expected> = tables
            .iter()
            .map(|table| (table.clone(), expected(&root.join(table))))
            .collect();
        for checked in &dataset.tables {
            let rows = expected[checked.name].rows;
            assert!(rows > 0, "{} {}", dataset.name, checked.name);
        }
        let with_rows = expected.values().filter(|table| table.rows > 0).count();
        eprintln!(
            "{}: {} Delta tables, {with_rows} with rows",
            dataset.name,
            tables.len()
        );
        let delta_rows = check_delta_rs(&root, &dataset, &tables, &expected).await;
        // Every `_delta_log/` now holds a Parquet checkpoint too.
        for table in &tables {
            assert!(
                root.join(table)
                    .join("_delta_log/_last_checkpoint")
                    .is_file(),
                "{table}"
            );
        }
        if let Some(duckdb) = &duckdb {
            let duck_rows: BTreeMap<String, u64> = tables
                .iter()
                .map(|table| {
                    let rows = check_duckdb(duckdb, &cwd, &root, &dataset, table, &expected[table]);
                    (table.clone(), rows)
                })
                .collect();
            assert_eq!(
                duck_rows, delta_rows,
                "{}: row counts differ across engines",
                dataset.name
            );
        }
    }
    if let Some(duckdb) = &duckdb {
        eprintln!(
            "duckdb {} (delta {}) read every table",
            duckdb.version, duckdb.delta_version
        );
    }
}

/// The "Engine compatibility" section of docs/reading-tables.md states what
/// this test checks: it names every family of [`datasets`], and its type table
/// has a DuckDB row for every type pinned above and for each list's element
/// type (`DECIMAL(38,s)` stands for the five SEC scales, `STRUCT(…)` for any
/// struct, the `T[]` row for every list).
#[test]
fn reading_tables_documents_every_checked_family_and_type() {
    let docs = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/reading-tables.md"),
    )
    .unwrap();
    let section = docs
        .split("\n## Engine compatibility\n")
        .nth(1)
        .and_then(|rest| rest.split("\n## ").next())
        .expect("docs/reading-tables.md has an Engine compatibility section");
    let ci_sentence = section
        .split("CI builds real ")
        .nth(1)
        .and_then(|rest| rest.split(" output").next())
        .expect("the section says which output CI builds")
        .replace('\n', " ");
    for dataset in datasets() {
        let family = match dataset.block_type {
            "evm" => "EVM",
            "solana" => "Solana",
            "sec" => "SEC",
            other => panic!("name the {other} family here"),
        };
        assert!(ci_sentence.contains(family), "{family}: {ci_sentence}");
    }
    // The DuckDB column of the type table.
    let duckdb_cells: Vec<&str> = section
        .lines()
        .filter(|line| line.starts_with("| `"))
        .filter_map(|line| line.split(" | ").nth(1))
        .collect();
    let documented = |spelling: &str| {
        duckdb_cells
            .iter()
            .any(|cell| cell.contains(&format!("`{spelling}")))
    };
    let pinned = datasets().into_iter().flat_map(|dataset| {
        dataset
            .tables
            .into_iter()
            .flat_map(|table| table.columns.into_iter().map(|(_, duckdb, _)| duckdb))
    });
    for duckdb in CANONICAL.iter().map(|(_, duckdb, _)| *duckdb).chain(pinned) {
        let element = duckdb.trim_end_matches("[]");
        let element = if element.starts_with("DECIMAL(38,") {
            "DECIMAL(38,s)"
        } else if element.starts_with("STRUCT(") {
            "STRUCT("
        } else {
            element
        };
        assert!(documented(element), "{duckdb}: no row for {element}");
        if duckdb.ends_with("[]") {
            assert!(documented("T[]"), "{duckdb}: no array row");
        }
    }
}

/// Opt-in, off in CI: anonymous reads of a deployment's public-read bucket
/// (the S3 API of Ceph RGW or another S3-compatible service), which the
/// spike could only check against moto. Operators run it against a live
/// lake:
///
/// ```sh
/// FIREPARQ_RGW_ENDPOINT=https://rgw.example.org FIREPARQ_RGW_BUCKET=ethereum-mainnet \
/// FIREPARQ_DUCKDB=/path/to/duckdb-1.5.5 \
/// cargo test -p blocks --test engine_compat anonymous -- --nocapture
/// ```
///
/// Optional: `FIREPARQ_RGW_PREFIX` (a dataset below the bucket root),
/// `FIREPARQ_RGW_REGION` (default `us-east-1`) and `FIREPARQ_RGW_TABLE`, the
/// child table of the consistent cut (default `transactions`). Every request
/// is unsigned: no credential is read or sent. DuckDB and delta-rs must read
/// the newest closed day of `blocks` (found from its log) and the child
/// table: the same rows and block range, pruned to that day's files, with the
/// canonical types, and the frontier cut of docs/reading-tables.md.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anonymous_reads_of_a_public_deployment_bucket() {
    let (Ok(endpoint), Ok(bucket)) = (
        std::env::var("FIREPARQ_RGW_ENDPOINT"),
        std::env::var("FIREPARQ_RGW_BUCKET"),
    ) else {
        eprintln!(
            "skipping the anonymous deployment read: set FIREPARQ_RGW_ENDPOINT and FIREPARQ_RGW_BUCKET"
        );
        return;
    };
    let region = std::env::var("FIREPARQ_RGW_REGION").unwrap_or_else(|_| "us-east-1".into());
    let child = std::env::var("FIREPARQ_RGW_TABLE").unwrap_or_else(|_| "transactions".into());
    let prefix = std::env::var("FIREPARQ_RGW_PREFIX")
        .map(|prefix| prefix.trim_matches('/').to_string())
        .unwrap_or_default();
    let root = match prefix.as_str() {
        "" => format!("s3://{bucket}"),
        prefix => format!("s3://{bucket}/{prefix}"),
    };
    let (use_ssl, host) = if let Some(host) = endpoint.strip_prefix("https://") {
        (true, host.trim_end_matches('/'))
    } else if let Some(host) = endpoint.strip_prefix("http://") {
        (false, host.trim_end_matches('/'))
    } else {
        panic!("FIREPARQ_RGW_ENDPOINT must start with https:// or http://");
    };
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let duckdb = DuckDb::open(&cwd).expect("the anonymous check needs the DuckDB CLI");
    let client = object_store_delta::aws::AmazonS3Builder::new()
        .with_bucket_name(&bucket)
        .with_region(&region)
        .with_endpoint(&endpoint)
        .with_allow_http(!use_ssl)
        .with_skip_signature(true)
        .build()
        .unwrap();
    let client: Arc<dyn object_store_delta::ObjectStore> = Arc::new(client);
    let store = DeltaStore::s3(&bucket, &prefix, Arc::clone(&client)).unwrap();

    // The newest closed day, from `blocks`' log alone.
    let blocks = common::open_table(&store, "blocks").await;
    let blocks_read = common::delta_read(&blocks).await;
    let day = blocks_read
        .files_per_date()
        .into_keys()
        .rev()
        .nth(1)
        .expect("blocks has no closed day yet");
    let mut seen = BTreeMap::new();
    for table in ["blocks", child.as_str()] {
        // The rows of that day's files (partition pruning), read unsigned.
        let delta = common::open_table(&store, table).await;
        let mut block_nums = Vec::new();
        for file in common::delta_day_files(&delta, &day).await {
            let key = match prefix.as_str() {
                "" => format!("{table}/{file}"),
                prefix => format!("{prefix}/{table}/{file}"),
            };
            let bytes = client
                .get(&object_store_delta::path::Path::from(key))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
                    .unwrap()
                    .build()
                    .unwrap();
            for batch in reader {
                block_nums.extend(
                    common::int64(&batch.unwrap(), "block_num")
                        .values()
                        .to_vec(),
                );
            }
        }
        seen.insert(
            table.to_string(),
            (
                block_nums.len() as u64,
                block_nums.iter().min().copied(),
                block_nums.iter().max().copied(),
            ),
        );
    }
    let day_files = common::delta_day_files(&blocks, &day).await.len();
    let secret = format!(
        "CREATE SECRET lake (TYPE s3, KEY_ID '', SECRET '', REGION '{region}', \
         ENDPOINT '{host}', URL_STYLE 'path', USE_SSL {use_ssl});"
    );
    let profile = cwd.join("profile-anonymous.json");
    let rows = duckdb.query(&format!(
        "{secret} \
         SELECT 'types' AS q, column_name, column_type \
           FROM (DESCRIBE SELECT * FROM delta_scan('{root}/blocks')); \
         PRAGMA enable_profiling = 'json'; SET profiling_output = '{}'; \
         SELECT 'blocks' AS q, count(*) AS n, min(block_num) AS first, max(block_num) AS last \
           FROM delta_scan('{root}/blocks') WHERE date = DATE '{day}'; \
         PRAGMA disable_profiling; \
         WITH f AS (SELECT max(block_num) AS b FROM delta_scan('{root}/blocks') \
                    WHERE date = DATE '{day}') \
         SELECT 'child' AS q, count(*) AS n, min(block_num) AS first, max(block_num) AS last \
           FROM delta_scan('{root}/{child}'), f WHERE date = DATE '{day}' AND block_num <= f.b;",
        profile.display()
    ));
    let types: BTreeMap<&str, &str> = rows["types"]
        .iter()
        .map(|row| {
            (
                row["column_name"].as_str().unwrap(),
                row["column_type"].as_str().unwrap(),
            )
        })
        .collect();
    for (column, duck, arrow) in CANONICAL {
        assert_eq!(types.get(column), Some(&duck), "duckdb {column}");
        assert_eq!(
            blocks_read.types.get(column).map(String::as_str),
            Some(arrow),
            "delta-rs {column}"
        );
    }
    for (table, tag) in [("blocks", "blocks"), (child.as_str(), "child")] {
        let duck = &rows[tag][0];
        let (n, first, last) = seen[table];
        eprintln!(
            "{table} on {day}: duckdb {duck}, delta-rs {:?}",
            seen[table]
        );
        assert!(number(&duck["n"]) > 0, "{table}: no rows on {day}");
        assert_eq!(
            (
                number(&duck["n"]),
                Some(number(&duck["first"]) as i64),
                Some(number(&duck["last"]) as i64)
            ),
            (n, first, last),
            "{table}"
        );
    }
    let profile: Value = serde_json::from_slice(&std::fs::read(&profile).unwrap()).unwrap();
    let files = scanning_files(&profile).expect("a Delta scan in the profile");
    let (read, total) = files.split_once('/').unwrap();
    assert_eq!(
        read.parse::<usize>().unwrap(),
        day_files,
        "both engines prune to the day's files: {files}"
    );
    eprintln!(
        "anonymous reads of {root}: duckdb {} (delta {}) scanned {read} of {total} blocks files for {day}",
        duckdb.version, duckdb.delta_version
    );
}
