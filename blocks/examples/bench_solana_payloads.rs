//! Offline mapping + Arrow flush benchmark using a caller-supplied raw block.
//! File reading, mapper construction, and Parquet/storage writes are not timed.
use anyhow::{ensure, Result};
use blocks::solana::{mapper::SolanaBlockMapper, proto::solana};
use clap::Parser;
use firehose_parquet::{
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper},
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{hint::black_box, path::PathBuf, time::Instant};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    block: PathBuf,
    #[arg(long, default_value_t = 20)]
    iterations: u32,
    #[arg(long, default_value_t = 7)]
    samples: u32,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.iterations > 0 && args.samples > 0,
        "positive iterations and samples required"
    );
    let bytes = std::fs::read(args.block)?;
    let block = solana::Block::decode(bytes.as_slice())?;
    let identity = BlockIdentity {
        block_num: block.slot,
        block_id: block.blockhash.clone(),
        parent_num: block.parent_slot,
        parent_id: block.previous_blockhash.clone(),
        timestamp: block.block_time.as_ref().map_or(0, |t| t.timestamp),
        ..Default::default()
    };
    for encoding in [EncodeBytes::Base58, EncodeBytes::Binary] {
        let mut mapper = SolanaBlockMapper::new(true, false, encoding.clone(), false, true);
        let mut run = || -> Result<(usize, usize)> {
            mapper.map_block(black_box(&bytes), &identity, None)?;
            let batches = mapper.flush()?;
            let rows = batches.values().map(|b| b.num_rows()).sum();
            let allocated = batches.values().map(|b| b.get_array_memory_size()).sum();
            black_box(batches);
            Ok((rows, allocated))
        };
        for _ in 0..3 {
            run()?;
        }
        let mut measurements = Vec::new();
        let mut counts = (0, 0);
        for _ in 0..args.samples {
            let start = Instant::now();
            for _ in 0..args.iterations {
                counts = run()?;
            }
            measurements.push(start.elapsed().as_secs_f64() * 1000.0 / f64::from(args.iterations));
        }
        let mut sorted = measurements.clone();
        sorted.sort_by(f64::total_cmp);
        println!(
            "{}",
            serde_json::json!({
                "slot": block.slot, "raw_bytes": bytes.len(), "raw_sha256": format!("{:x}", Sha256::digest(&bytes)),
                "source_transactions": block.transactions.len(), "encoding": format!("{encoding:?}"),
                "iterations_per_sample": args.iterations, "samples_ms_per_block": measurements,
                "median_ms_per_block": sorted[sorted.len() / 2], "rows": counts.0,
                "arrow_allocated_bytes_last_flush": counts.1,
            })
        );
    }
    Ok(())
}
