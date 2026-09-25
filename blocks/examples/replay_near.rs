//! Replay a saved producer-compatible NEAR block through the mapper/local writer.
//! This example is entirely offline and refuses an existing output directory.
use anyhow::{ensure, Context, Result};
use blocks::near::{mapper::NearBlockMapper, proto::near};
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
    let block = near::Block::decode(bytes.as_slice())?;
    let header = block.header.as_ref().context("missing block header")?;
    ensure!(
        header.height > header.prev_height && header.timestamp_nanosec > 0,
        "invalid source identity"
    );
    let hash = &header.hash.as_ref().context("missing hash")?.bytes;
    let parent = &header
        .prev_hash
        .as_ref()
        .context("missing parent hash")?
        .bytes;
    ensure!(
        hash.len() == 32 && parent.len() == 32,
        "invalid hash lengths"
    );
    ensure!(
        header.last_final_block_height < header.height,
        "invalid LIB height"
    );
    let identity = BlockIdentity {
        block_num: header.height,
        block_id: encode_hex_no_prefix(hash),
        parent_num: header.prev_height,
        parent_id: encode_hex_no_prefix(parent),
        timestamp: i64::try_from(header.timestamp_nanosec / 1_000_000_000)?,
        timestamp_nanos: (header.timestamp_nanosec % 1_000_000_000) as i32,
        // Exact older-hash RPC lookup performed by the qualification adapter.
        lib_num: header.last_final_block_height,
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
                let mut mapper = NearBlockMapper::new(fork, encoding.clone(), include_failed);
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
    println!("replayed one local NEAR block across {} cases", cases.len());
    Ok(())
}
