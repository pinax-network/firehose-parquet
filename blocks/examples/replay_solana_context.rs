//! Offline Solana mapper qualification: local protobuf inputs to fresh Parquet.
//! Exercises encoding/filter/vote/flush combinations; never connects to a provider.
use anyhow::{ensure, Result};
use blocks::solana::{mapper::SolanaBlockMapper, proto::solana};
use clap::Parser;
use firehose_parquet::{
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper},
};
use parquet::arrow::ArrowWriter;
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::PathBuf,
};

#[derive(Parser)]
struct Args {
    #[arg(long, required = true)]
    raw: Vec<PathBuf>,
    #[arg(long)]
    output: PathBuf,
    /// Exercise the owned protobuf container used by ingestion.
    #[arg(long)]
    owned: bool,
}
fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        !args.output.exists(),
        "qualification output must be a fresh directory"
    );
    let mut inputs = Vec::new();
    let mut hashes = BTreeMap::new();
    for path in args.raw {
        let bytes = fs::read(&path)?;
        let block = solana::Block::decode(bytes.as_slice())?;
        ensure!(
            hashes
                .insert(block.slot, format!("{:x}", Sha256::digest(&bytes)))
                .is_none(),
            "duplicate input slot"
        );
        let identity = BlockIdentity {
            block_num: block.slot,
            block_id: block.blockhash,
            parent_num: block.parent_slot,
            parent_id: block.previous_blockhash,
            timestamp: block.block_time.map_or(0, |time| time.timestamp),
            ..Default::default()
        };
        inputs.push((bytes, identity));
    }
    inputs.sort_by_key(|(_, identity)| identity.block_num);
    fs::create_dir_all(&args.output)?;
    let mut manifest = BTreeMap::new();
    for (name, encoding) in [
        ("binary", EncodeBytes::Binary),
        ("base58", EncodeBytes::Base58),
        ("hex", EncodeBytes::Hex),
        ("hex_no_prefix", EncodeBytes::HexNoPrefix),
        ("tron_base58", EncodeBytes::TronBase58),
    ] {
        for include_failed in [false, true] {
            for with_votes in [false, true] {
                for flush_each in [false, true] {
                    let case =
                        format!("{name}-failed{include_failed}-votes{with_votes}-each{flush_each}");
                    let mut mapper = SolanaBlockMapper::new(
                        with_votes,
                        true,
                        encoding.clone(),
                        false,
                        include_failed,
                    );
                    let mut totals = BTreeMap::<String, usize>::new();
                    let mut schemas = BTreeMap::new();
                    for (index, (bytes, identity)) in inputs.iter().enumerate() {
                        if args.owned {
                            mapper.map_block_bytes(
                                bytes.clone().into(),
                                identity,
                                Some("FINAL"),
                            )?;
                        } else {
                            mapper.map_block(bytes, identity, Some("FINAL"))?;
                        }
                        if flush_each || index + 1 == inputs.len() {
                            for (table, batch) in mapper.flush()? {
                                *totals.entry(table.clone()).or_default() += batch.num_rows();
                                schemas.insert(
                                    table.clone(),
                                    serde_json::to_value(batch.schema().as_ref())?,
                                );
                                let dir = args.output.join(&case).join(table);
                                fs::create_dir_all(&dir)?;
                                let file =
                                    File::create_new(dir.join(format!("{index:04}.parquet")))?;
                                let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
                                writer.write(&batch)?;
                                writer.close()?;
                            }
                        }
                    }
                    ensure!(
                        mapper.flush()?.values().all(|batch| batch.num_rows() == 0),
                        "flush retained rows"
                    );
                    manifest.insert(case,serde_json::json!({"encoding":name,"include_failed":include_failed,"with_votes":with_votes,"flush_each":flush_each,"rows":totals,"schemas":schemas}));
                }
            }
        }
    }
    let output = serde_json::json!({"raw_sha256":hashes,"cases":manifest,"owned":args.owned});
    fs::write(
        args.output.join("manifest.json"),
        serde_json::to_vec_pretty(&output)?,
    )?;
    println!(
        "replayed {} raw slots across {} cases",
        inputs.len(),
        manifest.len()
    );
    Ok(())
}
