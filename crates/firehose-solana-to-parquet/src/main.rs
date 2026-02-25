use anyhow::Result;
use clap::Parser;
use firehose_parquet::config::{Compression, Config, Partition};
use firehose_parquet::grpc::FirehoseClient;
use firehose_parquet::mapper::BlockMapper;
use firehose_parquet::writer::OutputWriter;
use std::path::PathBuf;
use tracing::info;

/// CLI that consumes a StreamingFast Firehose gRPC stream of Solana blocks
/// and writes Apache Parquet files.
#[derive(Parser, Debug)]
#[command(name = "firehose-solana-to-parquet", version, about)]
struct Cli {
    /// Firehose gRPC endpoint URL (e.g. https://mainnet.sol.streamingfast.io:443)
    #[arg(long)]
    endpoint: String,

    /// Optional bearer token for authentication.
    #[arg(long)]
    api_token: Option<String>,

    /// Start block number (inclusive).
    #[arg(long)]
    start_block: Option<u64>,

    /// Stop block number (inclusive). 0 means stream forever.
    #[arg(long)]
    stop_block: Option<u64>,

    /// Resume cursor from a previous session.
    #[arg(long)]
    cursor: Option<String>,

    /// Output directory for Parquet files.
    #[arg(long, default_value = "output")]
    output: PathBuf,

    /// Partitioning strategy: none | block_range
    #[arg(long, default_value = "none")]
    partition: String,

    /// Block range size when --partition=block_range
    #[arg(long, default_value = "10000")]
    block_range_size: u64,

    /// Flush after this many rows in the largest table.
    #[arg(long, default_value = "50000")]
    flush_rows: u32,

    /// Flush after approximate byte size (bytes) of buffered data.
    #[arg(long, default_value = "134217728")]
    flush_bytes: u64,

    /// Parquet compression: zstd | snappy | gzip | none
    #[arg(long, default_value = "zstd")]
    compression: String,

    /// Log level: info | debug | trace
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Dry-run mode: decode and map but do not write Parquet files.
    #[arg(long, default_value = "false")]
    dry_run: bool,

    /// Only stream final (irreversible) blocks.
    #[arg(long, default_value = "true")]
    final_blocks_only: bool,
}

fn parse_compression(s: &str) -> Compression {
    match s.to_lowercase().as_str() {
        "snappy" => Compression::Snappy,
        "gzip" => Compression::Gzip,
        "none" => Compression::None,
        _ => Compression::Zstd,
    }
}

fn parse_partition(s: &str, block_range_size: u64) -> Partition {
    match s.to_lowercase().as_str() {
        "block_range" => Partition::BlockRange(block_range_size),
        _ => Partition::None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    let filter = tracing_subscriber::EnvFilter::try_new(&cli.log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = Config {
        endpoint: cli.endpoint,
        api_token: cli.api_token,
        start_block: cli.start_block,
        stop_block: cli.stop_block,
        cursor: cli.cursor,
        output: cli.output.clone(),
        partition: parse_partition(&cli.partition, cli.block_range_size),
        flush_rows: cli.flush_rows,
        flush_bytes: cli.flush_bytes,
        compression: parse_compression(&cli.compression),
        final_blocks_only: cli.final_blocks_only,
        dry_run: cli.dry_run,
    };

    info!(?config, "starting pipeline");

    let mut mapper = BlockMapper::new();
    let mut writer = OutputWriter::new(
        &config.output,
        config.partition.clone(),
        config.compression,
    );
    let flush_rows = config.flush_rows as usize;
    let dry_run = config.dry_run;

    let mut blocks_processed: u64 = 0;
    let mut min_slot: Option<u64> = None;
    let mut max_slot: Option<u64> = None;

    let client = FirehoseClient::new(config);

    client
        .stream_blocks(|block, _cursor| {
            let slot = block.slot;
            min_slot = Some(min_slot.map_or(slot, |s: u64| s.min(slot)));
            max_slot = Some(max_slot.map_or(slot, |s: u64| s.max(slot)));

            mapper.map_block(&block);
            blocks_processed += 1;

            if blocks_processed.is_multiple_of(100) {
                info!(
                    blocks_processed,
                    slot,
                    buffered_rows = mapper.max_table_rows(),
                    "progress"
                );
            }

            // Check flush threshold
            if mapper.max_table_rows() >= flush_rows {
                let batches = mapper.flush()?;
                if !dry_run {
                    let slot_range = min_slot.zip(max_slot);
                    writer.write_all(&batches, slot_range)?;
                }
                min_slot = None;
                max_slot = None;
            }

            Ok(())
        })
        .await?;

    // Final flush
    if mapper.max_table_rows() > 0 {
        let batches = mapper.flush()?;
        if !dry_run {
            let slot_range = min_slot.zip(max_slot);
            writer.write_all(&batches, slot_range)?;
        }
    }

    info!(blocks_processed, "pipeline finished");
    Ok(())
}
