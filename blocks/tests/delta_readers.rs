//! #643 L5b/L7: the read-only commands over real Delta tables. `validate`
//! reads the active files of a pinned snapshot, never a directory listing, so
//! a file that OPTIMIZE replaced (tombstoned but still on disk) and the log's
//! checkpoint Parquet files are not read as data. `inspect` reads one file.
//! The table summary from the Delta log of docs/reading-tables.md, which
//! replaced `scan`, runs against the same table.
//!
//! A cursor-aware mock Firehose serves four final EVM blocks over two UTC
//! days, and `build` writes them with one transaction per block, locally and
//! to the loopback HTTPS S3 endpoint of `examples/bench_live_flush/s3.rs`.
//! OPTIMIZE is simulated with a commit that removes a day's files and adds
//! their rows rewritten as one file (`docs/design/delta-lake.md` §7.3), and
//! run for real by the maintenance job (the `fireparq-maintenance` binary,
//! optional locally, required in CI) together with a checkpoint.
use arrow::compute::concat_batches;
use deltalake_core::DeltaTable;
use firehose_parquet::config::Compression;
use firehose_parquet::writer::{encode_parquet, read_parquet, ParquetFileMetadata};
use firehose_protos::{eth, firehose};
use prost::Message;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Output;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::codegen::{http, BoxFuture, Service};

#[path = "../examples/bench_live_flush/s3.rs"]
#[allow(dead_code)]
mod s3;

mod common;

const CHAIN: &str = "delta-readers-chain";
/// 2023-11-15T00:00:00Z: blocks 100 and 101 are on 2023-11-14, 102 and 103
/// on 2023-11-15.
const MIDNIGHT: i64 = 1_700_006_400;
const FIRST_DAY: &str = "2023-11-14";
const SECOND_DAY: &str = "2023-11-15";
const BUCKET: &str = "delta-readers";

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

/// A final EVM block without transactions; block ids chain through parents.
fn response(number: u64) -> firehose::Response {
    let id = number as u8;
    let block = eth::Block {
        number,
        hash: vec![id; 32].into(),
        header: Some(eth::BlockHeader {
            number,
            ..Default::default()
        }),
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
                nanos: 250_000_000,
            }),
            ..Default::default()
        }),
    }
}

/// The loopback S3 endpoint's credentials, endpoint and CA for the binary.
fn s3_env(server: &s3::Server) -> Vec<(&'static str, String)> {
    vec![
        ("AWS_ACCESS_KEY_ID", "loopback-access-key".into()),
        ("AWS_SECRET_ACCESS_KEY", "loopback-secret-key".into()),
        ("AWS_REGION", "us-east-1".into()),
        ("AWS_ENDPOINT_URL_S3", server.endpoint.clone()),
        ("SSL_CERT_FILE", server.ca_file.to_str().unwrap().into()),
    ]
}

/// `build [100, 104)` into `output`, one transaction per block.
async fn build(cwd: &Path, output: &str, env: &[(&str, String)]) {
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
    let output = fireparq(
        cwd,
        &[
            "build",
            "--endpoint",
            &endpoint,
            "--block-type",
            "evm",
            "--start-block",
            "100",
            "--stop-block",
            "104",
            "--flush-blocks",
            "1",
            "--stream-idle-timeout-secs",
            "0",
            "--output",
            output,
        ],
        env,
    )
    .await;
    server.abort();
    assert!(output.status.success(), "{}", text(&output));
}

/// Runs the real binary with a cleared environment.
async fn fireparq(cwd: &Path, args: &[&str], env: &[(&str, String)]) -> Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"));
    command
        .kill_on_drop(true)
        .env_clear()
        .current_dir(cwd)
        .args(args)
        .envs(env.iter().map(|(key, value)| (key, value)));
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

/// What one `validate` run reported: the pinned version, its summary lines
/// (`Files scanned`, `Total blocks`, `Duplicates`, ...) and whether it passed.
#[derive(Debug)]
struct Validated {
    success: bool,
    version: u64,
    summary: BTreeMap<String, String>,
    output: String,
}

impl Validated {
    fn count(&self, key: &str) -> u64 {
        self.summary[key]
            .parse()
            .unwrap_or_else(|_| panic!("{key}: {}", self.output))
    }
}

async fn validate(cwd: &Path, table: &str, env: &[(&str, String)]) -> Validated {
    let output = fireparq(cwd, &["validate", table, "--cross-partition"], env).await;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let version = stdout
        .lines()
        .find_map(|line| line.split(" at version ").nth(1))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|version| version.parse().ok())
        .unwrap_or_else(|| panic!("no pinned version: {}", text(&output)));
    let summary = stdout
        .lines()
        .filter_map(|line| {
            let (key, value) = line.trim().split_once(':')?;
            Some((key.to_string(), value.trim().to_string()))
        })
        .collect();
    Validated {
        success: output.status.success(),
        version,
        summary,
        output: text(&output),
    }
}

/// Every commit of a local table's log, by version.
fn commits(table: &Path) -> BTreeMap<u64, Vec<Value>> {
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

/// The active files of a local table's latest version, with their `date`,
/// from replaying every JSON commit.
fn active_files(table: &Path) -> BTreeMap<String, String> {
    let mut active = BTreeMap::new();
    for actions in commits(table).values() {
        for action in actions {
            if let Some(add) = action.get("add") {
                active.insert(
                    add["path"].as_str().unwrap().to_string(),
                    add["partitionValues"]["date"].as_str().unwrap().to_string(),
                );
            }
            if let Some(remove) = action.get("remove") {
                active.remove(remove["path"].as_str().unwrap());
            }
        }
    }
    active
}

/// Writes `actions` as the table's next commit.
fn commit(table: &Path, actions: &[Value]) {
    let version = commits(table).keys().last().unwrap() + 1;
    let body: String = actions.iter().map(|action| format!("{action}\n")).collect();
    std::fs::write(
        table.join("_delta_log").join(format!("{version:020}.json")),
        body,
    )
    .unwrap();
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// What OPTIMIZE commits for one day (design §1.7): its active files are
/// `remove`d with `dataChange: false`, tombstoned but left on disk for
/// readers of older snapshots, and their rows `add`ed back as one new file.
/// Returns the number of files it replaced.
fn simulate_optimize(table: &Path, day: &str) -> usize {
    let replaced: Vec<String> = active_files(table)
        .into_iter()
        .filter(|(_, date)| date == day)
        .map(|(path, _)| path)
        .collect();
    let batches: Vec<_> = replaced
        .iter()
        .flat_map(|path| read_parquet(&table.join(path)).unwrap())
        .collect();
    let rows = concat_batches(&batches[0].schema(), &batches).unwrap();
    let bytes = encode_parquet(&rows, Compression::Zstd, &ParquetFileMetadata::new()).unwrap();
    let path = format!("date={day}/part-00000-simulated-optimize.zstd.parquet");
    std::fs::write(table.join(&path), &bytes).unwrap();
    let now = now_millis();
    let mut actions = vec![json!({"commitInfo": {
        "timestamp": now, "operation": "OPTIMIZE", "operationParameters": {},
        "isBlindAppend": false,
    }})];
    for old in &replaced {
        actions.push(json!({"remove": {
            "path": old, "deletionTimestamp": now, "dataChange": false,
            "extendedFileMetadata": true, "partitionValues": {"date": day},
            "size": std::fs::metadata(table.join(old)).unwrap().len(),
        }}));
    }
    actions.push(json!({"add": {
        "path": path, "partitionValues": {"date": day}, "size": bytes.len(),
        "modificationTime": now, "dataChange": false,
        "stats": json!({"numRecords": rows.num_rows()}).to_string(),
    }}));
    commit(table, &actions);
    replaced.len()
}

/// `block_num` rows of every `.parquet` file below the table's `date=`
/// directories: what a directory walk would read.
fn walked_block_rows(table: &Path) -> usize {
    let mut rows = 0;
    for day in std::fs::read_dir(table).unwrap() {
        let day = day.unwrap().path();
        if !day
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("date=")
        {
            continue;
        }
        for file in std::fs::read_dir(&day).unwrap() {
            let file = file.unwrap().path();
            if file.extension().is_some_and(|ext| ext == "parquet") {
                rows += read_parquet(&file)
                    .unwrap()
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>();
            }
        }
    }
    rows
}

/// The "Table summary from the Delta log" of docs/reading-tables.md (files,
/// rows, bytes and days of the active files), from the delta-rs snapshot. The
/// documented Polars program reads the same `add` actions through delta-rs.
fn readme_summary(table: &DeltaTable) -> Value {
    let snapshot = table.snapshot().unwrap();
    let files: Vec<_> = snapshot.log_data().iter().collect();
    let days: Vec<String> = files
        .iter()
        .map(|file| file.partition_values_map()["date"].clone().unwrap())
        .collect();
    json!({
        "version": table.version(),
        "files": files.len(),
        "rows": files.iter().map(|file| file.num_records().unwrap()).sum::<usize>(),
        "bytes": files.iter().map(|file| file.size() as u64).sum::<u64>(),
        "first_day": days.iter().min(),
        "last_day": days.iter().max(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validate_reads_a_pinned_snapshot_through_optimize_and_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let root = cwd.join("dataset");
    build(&cwd, root.to_str().unwrap(), &[]).await;
    let table = root.join("blocks");
    let table_arg = table.to_str().unwrap();

    // Version 0 creates the table; each of the four blocks is one commit.
    let first = validate(&cwd, table_arg, &[]).await;
    assert!(first.success, "{}", first.output);
    assert_eq!(first.version, 4, "{}", first.output);
    assert_eq!(first.count("Files scanned"), 4);
    assert_eq!(first.count("Total blocks"), 4);
    assert_eq!(first.summary["Block range"], "100 — 103");
    assert_eq!(first.count("Duplicates"), 0);
    assert_eq!(first.count("Gaps"), 0);
    assert_eq!(first.count("Parent mismatches"), 0);
    assert_eq!(first.count("Timestamp reversals"), 0);
    assert_eq!(
        first.summary["Partitions"],
        "2 total, 2 valid, 0 with issues"
    );
    assert!(
        first.output.contains("✓ All blocks valid"),
        "{}",
        first.output
    );

    // OPTIMIZE of the closed day: its two parts stay on disk, tombstoned, next
    // to the file that replaced them. A directory walk would read blocks 100
    // and 101 twice; the snapshot reads each once.
    assert_eq!(simulate_optimize(&table, FIRST_DAY), 2);
    assert_eq!(walked_block_rows(&table), 6);
    let optimized = validate(&cwd, table_arg, &[]).await;
    assert!(optimized.success, "{}", optimized.output);
    assert_eq!(optimized.version, 5);
    assert_eq!(optimized.count("Files scanned"), 3);
    assert_eq!(optimized.count("Total blocks"), 4);
    assert_eq!(optimized.count("Duplicates"), 0);
    assert_eq!(optimized.summary["Partitions"], first.summary["Partitions"]);

    // The real maintenance job (the `fireparq-maintenance` binary): OPTIMIZE
    // of the other day, which alone has two files, and a checkpoint, whose
    // Parquet file sits in `_delta_log/`.
    if let Some(job) = common::maintenance_bin() {
        let run = common::maintenance_job(
            &job,
            &[
                ("LAKE_ROOT", root.to_str().unwrap().to_string()),
                ("LAKE_TABLES", "blocks".into()),
                ("OPTIMIZE_DATES", "all".into()),
            ],
        )
        .await;
        run.assert_clean();
        let line = run.table("blocks");
        assert_eq!(
            (
                &line["compacted"],
                &line["checkpoint_version"],
                &line["version_after"]
            ),
            (
                // footer_keys: the writer's 8 dataset-level keys and the job's.
                &json!([{"date": SECOND_DAY, "files_removed": 2, "files_added": 1, "footer_keys": 9}]),
                &json!(6),
                &json!(6)
            ),
            "{run:?}"
        );
        let checkpoint = table.join("_delta_log/00000000000000000006.checkpoint.parquet");
        assert!(checkpoint.is_file());
        assert_eq!(walked_block_rows(&table), 8);
        let compacted = validate(&cwd, table_arg, &[]).await;
        assert!(compacted.success, "{}", compacted.output);
        assert_eq!(compacted.version, 6);
        assert_eq!(compacted.count("Files scanned"), 2);
        assert_eq!(compacted.count("Total blocks"), 4);
        assert_eq!(compacted.count("Duplicates"), 0);
        assert_eq!(compacted.summary["Block range"], "100 — 103");
        assert_eq!(compacted.count("Timestamp reversals"), 0);

        // `inspect` reads one Parquet file, whatever it is: a checkpoint too.
        let inspected = fireparq(&cwd, &["inspect", checkpoint.to_str().unwrap()], &[]).await;
        assert!(inspected.status.success(), "{}", text(&inspected));

        // The documented summary, from the log alone, replaces `scan`.
        let summary = readme_summary(&common::open_local(&root, "blocks").await);
        let sizes: u64 = active_files(&table)
            .keys()
            .map(|path| std::fs::metadata(table.join(path)).unwrap().len())
            .sum();
        assert_eq!(
            summary,
            json!({
                "version": 6, "files": 2, "rows": 4, "bytes": sizes,
                "first_day": FIRST_DAY, "last_day": SECOND_DAY,
            })
        );
    }

    // The checks still find what the snapshot holds: an added copy of an
    // active file duplicates its blocks.
    let (active, date) = active_files(&table)
        .into_iter()
        .find(|(_, date)| date == FIRST_DAY)
        .unwrap();
    let copy = format!("date={date}/part-duplicate.parquet");
    std::fs::copy(table.join(&active), table.join(&copy)).unwrap();
    commit(
        &table,
        &[
            json!({"commitInfo": {"timestamp": now_millis(), "operation": "WRITE"}}),
            json!({"add": {
                "path": copy, "partitionValues": {"date": date},
                "size": std::fs::metadata(table.join(&copy)).unwrap().len(),
                "modificationTime": now_millis(), "dataChange": true,
            }}),
        ],
    );
    let duplicated = validate(&cwd, table_arg, &[]).await;
    assert!(!duplicated.success, "{}", duplicated.output);
    assert_eq!(duplicated.count("Duplicates"), 2, "{}", duplicated.output);
    assert!(duplicated
        .output
        .contains("duplicate block 100 (2 occurrences)"));
    assert!(duplicated.output.contains("✗ Validation failed"));

    // An active file that is gone (a VACUUM after the snapshot was read)
    // fails the run instead of being skipped.
    std::fs::remove_file(table.join(&copy)).unwrap();
    let missing = fireparq(&cwd, &["validate", table_arg], &[]).await;
    assert!(!missing.status.success());
    let message = text(&missing);
    assert!(message.contains("part-duplicate.parquet"), "{message}");
    assert!(message.contains("is missing"), "{message}");

    // A directory without a log is not a table, and `inspect` reads files only.
    let refused = fireparq(&cwd, &["validate", root.to_str().unwrap()], &[]).await;
    assert!(!refused.status.success());
    assert!(
        text(&refused).contains("is not a Delta table"),
        "{}",
        text(&refused)
    );
    let refused = fireparq(&cwd, &["inspect", table_arg], &[]).await;
    assert!(!refused.status.success());
    assert!(
        text(&refused).contains("is a directory"),
        "{}",
        text(&refused)
    );
    let part = table.join(&active);
    let inspected = fireparq(&cwd, &["inspect", part.to_str().unwrap()], &[]).await;
    assert!(inspected.status.success(), "{}", text(&inspected));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validate_reads_an_s3_table_through_its_log() {
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
    let env = s3_env(&server);
    build(&cwd, &format!("s3://{BUCKET}/{CHAIN}"), &env).await;
    let before = server.log().len();
    let validated = validate(&cwd, &format!("s3://{BUCKET}/{CHAIN}/blocks/"), &env).await;
    assert!(validated.success, "{}", validated.output);
    assert_eq!(validated.version, 4);
    assert_eq!(validated.count("Files scanned"), 4);
    assert_eq!(validated.count("Total blocks"), 4);
    assert_eq!(validated.count("Duplicates"), 0);
    // The log named the data files: each active part was read once, and
    // nothing else below the table's `date=` prefixes.
    let mut reads: Vec<String> = server.log()[before..]
        .iter()
        .filter(|entry| entry.method == "GET" && entry.key.contains("/blocks/date="))
        .map(|entry| entry.key.clone())
        .collect();
    reads.sort();
    let parts = reads.len();
    reads.dedup();
    assert_eq!((parts, reads.len()), (4, 4), "{reads:?}");
    assert!(
        reads.iter().all(|key| key.contains("/part-v1-")),
        "{reads:?}"
    );
    let refused = fireparq(
        &cwd,
        &["validate", &format!("s3://{BUCKET}/{CHAIN}/missing")],
        &env,
    )
    .await;
    assert!(!refused.status.success(), "{}", text(&refused));
}
