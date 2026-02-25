mod mapper;
mod proto;
mod schema;

use anyhow::Result;
use clap::Parser;
use firehose_parquet::config::{Compression, Config, Partition};
use firehose_parquet::grpc::FirehoseClient;
use firehose_parquet::traits::BlockMapper;
use firehose_parquet::writer::OutputWriter;
use mapper::EvmBlockMapper;
use std::path::PathBuf;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "firehose-evm-to-parquet", version, about = "Convert Firehose EVM gRPC stream to Apache Parquet")]
struct Cli {
    /// Firehose gRPC endpoint URL
    #[arg(long)]
    endpoint: String,

    #[arg(long, env = "FIREHOSE_API_KEY")]
    api_key: Option<String>,

    #[arg(long, env = "SUBSTREAMS_API_TOKEN")]
    jwt_token: Option<String>,

    #[arg(long)]
    start_block: Option<u64>,

    #[arg(long)]
    stop_block: Option<u64>,

    #[arg(long)]
    cursor: Option<String>,

    #[arg(long, default_value = "output")]
    output: PathBuf,

    #[arg(long, default_value = "none")]
    partition: String,

    #[arg(long, default_value = "10000")]
    block_range_size: u64,

    #[arg(long, default_value = "50000")]
    flush_rows: u32,

    #[arg(long, default_value = "134217728")]
    flush_bytes: u64,

    #[arg(long, default_value = "zstd")]
    compression: String,

    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, default_value = "false")]
    dry_run: bool,

    #[arg(long, default_value = "true")]
    final_blocks_only: bool,

    /// Enable extended detail level (calls, balance_changes, etc.)
    #[arg(long, default_value = "false")]
    extended: bool,
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

    let filter = tracing_subscriber::EnvFilter::try_new(&cli.log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let extended = cli.extended;
    let config = Config {
        endpoint: cli.endpoint,
        api_key: cli.api_key,
        jwt_token: cli.jwt_token,
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

    info!(?config, extended, "starting EVM pipeline");

    let mut mapper = EvmBlockMapper::new(extended);
    let mut writer = OutputWriter::new(&config.output, config.partition.clone(), config.compression);
    let flush_rows = config.flush_rows as usize;
    let dry_run = config.dry_run;

    let mut blocks_processed: u64 = 0;
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;

    let client = FirehoseClient::new(config);

    client
        .stream_blocks(|block_bytes, _cursor| {
            // Quick decode just block number for tracking
            let block_number = prost::Message::decode(block_bytes.as_slice())
                .map(|b: proto::eth::Block| b.number)
                .unwrap_or(0);
            min_block = Some(min_block.map_or(block_number, |s: u64| s.min(block_number)));
            max_block = Some(max_block.map_or(block_number, |s: u64| s.max(block_number)));

            mapper.map_block(&block_bytes)?;
            blocks_processed += 1;

            if blocks_processed % 100 == 0 {
                info!(blocks_processed, block_number, buffered_rows = mapper.max_table_rows(), "progress");
            }

            if mapper.max_table_rows() >= flush_rows {
                let batches = mapper.flush()?;
                if !dry_run {
                    let block_range = min_block.zip(max_block);
                    writer.write_all(&batches, block_range)?;
                }
                min_block = None;
                max_block = None;
            }

            Ok(())
        })
        .await?;

    if mapper.max_table_rows() > 0 {
        let batches = mapper.flush()?;
        if !dry_run {
            let block_range = min_block.zip(max_block);
            writer.write_all(&batches, block_range)?;
        }
    }

    info!(blocks_processed, "EVM pipeline finished");
    Ok(())
}
