//! Offline replay of a caller-supplied sf.cosmos.type.v2.Block into a new directory.
//! No network access. RPC-derived inputs are not evidence of Firehose transport coverage.
use anyhow::{ensure, Context, Result};
use blocks::cosmos::{mapper::CosmosBlockMapper, proto::cosmos};
use clap::Parser;
use firehose_parquet::{
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper},
};
use parquet::arrow::ArrowWriter;
use prost::Message;
use std::{fs, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    block: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        !args.output.try_exists()?,
        "output directory already exists"
    );
    let raw = fs::read(args.block)?;
    let block = cosmos::Block::decode(raw.as_slice())?;
    let height = u64::try_from(block.height).context("negative height")?;
    let timestamp = block.time.as_ref().context("missing timestamp")?;
    let identity = BlockIdentity {
        block_num: height,
        parent_num: height.saturating_sub(1),
        timestamp: timestamp.seconds,
        timestamp_nanos: timestamp.nanos,
        ..Default::default()
    };
    let mut mapper = CosmosBlockMapper::new(false, EncodeBytes::Binary, true);
    mapper.map_block(&raw, &identity, None)?;
    let batches = mapper.flush()?;
    fs::create_dir(&args.output)?;
    let mut counts = std::collections::BTreeMap::new();
    for (table, batch) in batches {
        let file = fs::File::create_new(args.output.join(format!("{table}.parquet")))?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        counts.insert(table, batch.num_rows());
    }
    println!("{}", serde_json::to_string(&counts)?);
    Ok(())
}
