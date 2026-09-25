//! Replay a saved producer-compatible Tron block through the mapper/local writer.
//! This example is entirely offline and refuses an existing output directory.
use anyhow::{ensure, Context, Result};
use blocks::tron::{mapper::TronBlockMapper, proto::tron};
use clap::Parser;
use firehose_parquet::{
    config::{BlockMetadata, Compression, Partition},
    encode::{encode_hex_no_prefix, EncodeBytes},
    traits::{BlockIdentity, BlockMapper},
    writer::ParquetTableWriter,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    block: PathBuf,
    #[arg(long)]
    output: PathBuf,
}
fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(!args.output.exists(), "output must be a fresh directory");
    let bytes = fs::read(&args.block)?;
    let block = tron::Block::decode(bytes.as_slice())?;
    let header = block.header.as_ref().context("missing block header")?;
    ensure!(
        header.number >= 20 && header.timestamp > 0,
        "invalid source identity"
    );
    let identity = BlockIdentity {
        block_num: header.number,
        block_id: encode_hex_no_prefix(&block.id),
        parent_num: header.parent_number,
        parent_id: encode_hex_no_prefix(&header.parent_hash),
        timestamp: header.timestamp / 1000,
        timestamp_nanos: ((header.timestamp % 1000) * 1_000_000) as i32,
        // Exact pinned producer compatibility rule, not a separate finality proof.
        lib_num: header.number - 20,
        ..Default::default()
    };
    let metadata = BlockMetadata {
        min_block_number: identity.block_num,
        max_block_number: identity.block_num,
        min_timestamp: Some(identity.timestamp),
        max_timestamp: Some(identity.timestamp),
    };
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
                let mut mapper = TronBlockMapper::new(fork, encoding.clone(), include_failed);
                let mut writer = ParquetTableWriter::new(
                    args.output.join(&case),
                    Partition::None,
                    Compression::Zstd,
                );
                mapper.map_block_bytes(bytes.clone().into(), &identity, Some("FINAL"))?;
                let mut rows = BTreeMap::new();
                let mut schemas = BTreeMap::new();
                for (table, batch) in mapper.flush()? {
                    rows.insert(table.clone(), batch.num_rows());
                    schemas.insert(
                        table.clone(),
                        serde_json::to_value(batch.schema().as_ref())?,
                    );
                    writer.write_batch(&table, &batch, &metadata)?;
                }
                ensure!(
                    mapper.flush()?.values().all(|batch| batch.num_rows() == 0),
                    "flush retained rows"
                );
                cases.insert(case, serde_json::json!({"encoding": name, "include_failed": include_failed, "fork": fork, "rows": rows, "schemas": schemas}));
            }
        }
    }
    fs::write(
        args.output.join("manifest.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "block_sha256": format!("{:x}", Sha256::digest(&bytes)), "block_num": identity.block_num,
            "writer": "ParquetTableWriter", "owned": true, "cases": cases,
        }))?,
    )?;
    println!("replayed one local Tron block across {} cases", cases.len());
    Ok(())
}
