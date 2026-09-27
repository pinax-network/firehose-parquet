//! `delta-lake-spike`: a fireparq-style appender for the concurrency and reader
//! checks driven by `spikes/delta-lake/run.sh`.
//!
//! ```text
//! delta-lake-spike write   --lake <dir|s3:PREFIX> --transactions N [--first N] [--interval-ms MS]
//!                          [--date D] [--blocks B] [--variant standard|physical-date|millis]
//! delta-lake-spike summary --lake <dir|s3:PREFIX> --stream S
//! ```
//!
//! `s3:PREFIX` uses `DELTA_SPIKE_S3_ENDPOINT` (loopback only) and
//! `DELTA_SPIKE_S3_BUCKET`. Output is one JSON object on stdout.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use delta_lake_spike::mapping::Fixture;
use delta_lake_spike::storage::{Lake, S3Settings};
use delta_lake_spike::{
    app_id, delta, open_or_create, write_transaction, SpikeTransaction, Variant,
};
use serde_json::json;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn number(args: &[String], name: &str, default: u64) -> u64 {
    arg(args, name).map_or(default, |v| v.parse().expect("numeric argument"))
}

fn lake(args: &[String]) -> Lake {
    let spec = arg(args, "--lake").expect("--lake <dir|s3:PREFIX>");
    match spec.strip_prefix("s3:") {
        Some(prefix) => Lake::s3(
            S3Settings::from_env().expect("DELTA_SPIKE_S3_ENDPOINT for an s3: lake"),
            prefix,
        ),
        None => Lake::local(std::path::Path::new(&spec)),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(String::as_str).unwrap_or("");
    let stream = arg(&args, "--stream").unwrap_or_else(|| "s0".into());
    match command {
        "write" => {
            let lake = lake(&args);
            let transactions = number(&args, "--transactions", 10);
            let first = number(&args, "--first", 1);
            let interval = Duration::from_millis(number(&args, "--interval-ms", 0));
            let date = number(&args, "--date", 20_721) as i32; // 2026-09-25
            let blocks = number(&args, "--blocks", 20);
            let variant: Variant = arg(&args, "--variant")
                .unwrap_or_else(|| "standard".into())
                .parse()
                .expect("--variant standard|physical-date|millis");
            let mut tables = open_or_create(&lake).await.expect("open tables");
            let started = Instant::now();
            let mut retries = 0;
            let mut versions = BTreeMap::new();
            for i in 0..transactions {
                let ordinal = first + i;
                let txn = SpikeTransaction {
                    stream: stream.clone(),
                    first_ordinal: ordinal,
                    last_ordinal: ordinal,
                    fixture: Fixture {
                        date,
                        first_block: (ordinal - 1) * blocks,
                        blocks,
                        txs_per_block: 3,
                    },
                    variant,
                };
                let commits = write_transaction(&lake, &mut tables, &txn)
                    .await
                    .unwrap_or_else(|e| panic!("transaction {ordinal} failed: {e}"));
                for ((name, _), commit) in tables.iter().zip(&commits) {
                    retries += commit.retries;
                    versions.insert(name.clone(), commit.version);
                }
                if !interval.is_zero() {
                    tokio::time::sleep(interval).await;
                }
            }
            println!(
                "{}",
                json!({
                    "transactions": transactions,
                    "commit_retries": retries,
                    "last_versions": versions,
                    "elapsed_ms": started.elapsed().as_millis() as u64,
                })
            );
        }
        "summary" => {
            let lake = lake(&args);
            let tables = open_or_create(&lake).await.expect("open tables");
            let mut out = BTreeMap::new();
            for (name, table) in &tables {
                let (files, rows, _) = delta::active_files(table).expect("active files");
                let txn = delta::txn_version(table, &app_id(&stream))
                    .await
                    .expect("txn version");
                out.insert(
                    name.clone(),
                    json!({
                        "version": table.version(),
                        "txn_version": txn,
                        "active_files": files,
                        "rows": rows,
                    }),
                );
            }
            println!("{}", json!(out));
        }
        "bench-load" => {
            // Resume cost (#655): how long does a writer take to open a table
            // with many active files and read its txn version? The adds point at
            // one real part; no data file is read or listed.
            let lake = lake(&args);
            let files = number(&args, "--files", 50_000);
            let per_commit = number(&args, "--per-commit", 5_000);
            if arg(&args, "--open-only").is_none() {
                let mut tables = open_or_create(&lake).await.expect("open tables");
                let (_, table) = tables.iter_mut().find(|(n, _)| n == "blocks").unwrap();
                let txn = SpikeTransaction {
                    stream: stream.clone(),
                    first_ordinal: 1,
                    last_ordinal: 1,
                    fixture: Fixture {
                        date: 20_721,
                        first_block: 0,
                        blocks: 20,
                        txs_per_block: 3,
                    },
                    variant: Variant::Standard,
                };
                let (template, _) = txn.publish(&lake, "blocks", 0).await;
                let mut written = 0;
                let mut version = 0;
                while written < files {
                    let n = per_commit.min(files - written);
                    let adds: Vec<delta::PartAdd> = (0..n)
                        .map(|i| delta::PartAdd {
                            path: format!("date=2026-09-25/bench-{}.parquet", written + i),
                            ..template.clone()
                        })
                        .collect();
                    version += 1;
                    delta::commit_parts(table, &adds, &app_id(&stream), version, &[])
                        .await
                        .expect("bench commit");
                    written += n;
                }
            }
            let started = Instant::now();
            let table = delta::open_table(lake.log_store("blocks"))
                .await
                .expect("open");
            let opened = started.elapsed();
            let txn = delta::txn_version(&table, &app_id(&stream))
                .await
                .expect("txn");
            let total = started.elapsed();
            let (active, _, _) = delta::active_files(&table).expect("files");
            println!(
                "{}",
                json!({
                    "active_files": active,
                    "log_version": table.version(),
                    "txn_version": txn,
                    "open_ms": opened.as_millis() as u64,
                    "open_and_txn_ms": total.as_millis() as u64,
                })
            );
        }
        _ => {
            eprintln!(
                "usage: delta-lake-spike write|summary|bench-load --lake <dir|s3:PREFIX> ..."
            );
            std::process::exit(2);
        }
    }
}
