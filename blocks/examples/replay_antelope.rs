//! Replay saved Antelope Firehose blocks through the mapper and the part encoder,
//! one `<case>/<table>.parquet` file per table with rows.
//! This example is entirely offline and refuses an existing output directory.
//!
//! `--capture` is a directory with `manifest.json` (a list of Firehose block
//! identities: block_num, block_id, parent_num, parent_id, lib_num, timestamp,
//! timestamp_nanos, sha256) and one `<block_num>.pb` payload per entry.
use anyhow::{ensure, Context, Result};
use blocks::antelope::mapper::AntelopeBlockMapper;
use clap::Parser;
use firehose_parquet::{
    config::Compression,
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper, StreamEvent},
    writer::{encode_parquet, ParquetFileMetadata},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    capture: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Deserialize)]
struct Captured {
    block_num: u64,
    block_id: String,
    parent_num: u64,
    parent_id: String,
    lib_num: u64,
    timestamp: i64,
    timestamp_nanos: i32,
    sha256: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(!args.output.exists(), "output must be a fresh directory");
    let manifest: Vec<Captured> =
        serde_json::from_slice(&fs::read(args.capture.join("manifest.json"))?)?;
    ensure!(!manifest.is_empty(), "empty capture");
    let mut blocks = Vec::new();
    for entry in &manifest {
        let bytes = fs::read(args.capture.join(format!("{}.pb", entry.block_num)))
            .with_context(|| format!("block {}", entry.block_num))?;
        ensure!(
            format!("{:x}", Sha256::digest(&bytes)) == entry.sha256,
            "block {} does not match its recorded SHA-256",
            entry.block_num
        );
        let identity = BlockIdentity {
            block_num: entry.block_num,
            block_id: entry.block_id.clone(),
            parent_num: entry.parent_num,
            parent_id: entry.parent_id.clone(),
            lib_num: entry.lib_num,
            timestamp: entry.timestamp,
            timestamp_nanos: entry.timestamp_nanos,
            ..Default::default()
        };
        blocks.push((bytes, identity));
    }
    fs::create_dir_all(&args.output)?;
    let mut cases = BTreeMap::new();
    for (name, encoding) in [
        ("binary", EncodeBytes::Binary),
        ("base58", EncodeBytes::Base58),
        ("hex", EncodeBytes::Hex),
        ("hex_no_prefix", EncodeBytes::HexNoPrefix),
        ("tron_base58", EncodeBytes::TronBase58),
    ] {
        for include_failed in [false, true] {
            for fork in [false, true] {
                let case = format!("{name}-failed{include_failed}-fork{fork}");
                let mut mapper = AntelopeBlockMapper::new(fork, encoding.clone(), include_failed);
                for (bytes, identity) in &blocks {
                    mapper.map_block_bytes(
                        bytes.clone().into(),
                        identity,
                        StreamEvent::new(Some("FINAL"), 1),
                    )?;
                }
                let mut rows = BTreeMap::new();
                let mut schemas = BTreeMap::new();
                for (table, batch) in mapper.flush()? {
                    rows.insert(table.clone(), batch.num_rows());
                    schemas.insert(
                        table.clone(),
                        serde_json::to_value(batch.schema().as_ref())?,
                    );
                    if batch.num_rows() > 0 {
                        let path = args.output.join(&case).join(format!("{table}.parquet"));
                        fs::create_dir_all(path.parent().unwrap())?;
                        let part =
                            encode_parquet(&batch, Compression::Zstd, &ParquetFileMetadata::new())?;
                        fs::write(path, part)?;
                    }
                }
                ensure!(
                    mapper.flush()?.values().all(|batch| batch.num_rows() == 0),
                    "flush retained rows"
                );
                cases.insert(
                    case,
                    serde_json::json!({
                        "encoding": name, "include_failed": include_failed, "fork": fork,
                        "rows": rows, "schemas": schemas,
                    }),
                );
            }
        }
    }
    fs::write(
        args.output.join("manifest.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "blocks": manifest.iter().map(|b| serde_json::json!({
                "block_num": b.block_num, "sha256": b.sha256,
            })).collect::<Vec<_>>(),
            "writer": "encode_parquet", "owned": true, "cases": cases,
        }))?,
    )?;
    println!(
        "replayed {} Antelope blocks across {} cases",
        blocks.len(),
        cases.len()
    );
    Ok(())
}
