use anyhow::Result;
use clap::Parser;
use firehose_parquet::config::{BlockMetadata, Compression, Config, Partition};
use firehose_parquet::encode::{parse_encode_bytes, EncodeBytes};
use firehose_parquet::grpc::FirehoseClient;
use firehose_parquet::traits::{fork_step_name, BlockIdentity, BlockMapper};
use firehose_parquet::writer::OutputWriter;
use blocks::solana::mapper::SolanaBlockMapper;
use std::path::PathBuf;
use std::time::Instant;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "firehose-solana-to-parquet", version, about)]
struct Cli {
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

    #[arg(long)]
    flush_interval_secs: Option<u64>,

    #[arg(long, default_value = "zstd")]
    compression: String,

    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, default_value = "false")]
    dry_run: bool,

    #[arg(long, default_value = "true")]
    final_blocks_only: bool,

    /// Byte encoding for binary fields (hashes, keys, etc.)
    /// Options: binary (default raw bytes), hex, base58, tron_base58, auto (chain-appropriate = base58 for Solana)
    #[arg(long, default_value = "binary")]
    encode_bytes: String,
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
        "date" => Partition::Date,
        "hour" => Partition::Hour,
        _ => Partition::None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let filter = tracing_subscriber::EnvFilter::try_new(&cli.log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

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
        flush_interval_secs: cli.flush_interval_secs,
        compression: parse_compression(&cli.compression),
        final_blocks_only: cli.final_blocks_only,
        dry_run: cli.dry_run,
    };

    info!(?config, "starting Solana pipeline");

    let include_fork_step = !config.final_blocks_only;
    let final_blocks_only = config.final_blocks_only;
    let encode_bytes = parse_encode_bytes(&cli.encode_bytes)
        .unwrap_or(EncodeBytes::Base58); // "auto" → base58 for Solana
    let mut mapper = SolanaBlockMapper::new(include_fork_step, encode_bytes);
    let mut writer = OutputWriter::new(&config.output, config.partition.clone(), config.compression);
    let flush_rows = config.flush_rows as usize;
    let flush_interval_secs = config.flush_interval_secs;
    let dry_run = config.dry_run;

    let mut blocks_processed: u64 = 0;
    let mut min_slot: Option<u64> = None;
    let mut max_slot: Option<u64> = None;
    let mut min_timestamp: Option<i64> = None;
    let mut max_timestamp: Option<i64> = None;
    let mut last_flush_time = Instant::now();

    let client = FirehoseClient::new(config);

    client
        .stream_blocks(|block_bytes, _cursor, identity: BlockIdentity, step: i32| {
            let fork_step_str = fork_step_name(step);
            if final_blocks_only && step == 2 {
                return Ok(());
            }

            let slot = identity.block_num;
            let ts = identity.timestamp;
            min_slot = Some(min_slot.map_or(slot, |s: u64| s.min(slot)));
            max_slot = Some(max_slot.map_or(slot, |s: u64| s.max(slot)));
            if let Some(t) = ts {
                min_timestamp = Some(min_timestamp.map_or(t, |s: i64| s.min(t)));
                max_timestamp = Some(max_timestamp.map_or(t, |s: i64| s.max(t)));
            }

            mapper.map_block(&block_bytes, &identity, fork_step_str)?;
            blocks_processed += 1;

            if blocks_processed % 100 == 0 {
                info!(blocks_processed, slot, buffered_rows = mapper.max_table_rows(), "progress");
            }

            let time_to_flush = flush_interval_secs
                .map(|secs| last_flush_time.elapsed().as_secs() >= secs)
                .unwrap_or(false);

            if mapper.max_table_rows() >= flush_rows || time_to_flush {
                let batches = mapper.flush()?;
                if !dry_run {
                    let metadata = BlockMetadata {
                        min_block_number: min_slot.unwrap_or(0),
                        max_block_number: max_slot.unwrap_or(0),
                        min_timestamp,
                        max_timestamp,
                    };
                    writer.write_all(&batches, &metadata)?;
                }
                min_slot = None;
                max_slot = None;
                min_timestamp = None;
                max_timestamp = None;
                last_flush_time = Instant::now();
            }

            Ok(())
        })
        .await?;

    if mapper.max_table_rows() > 0 {
        let batches = mapper.flush()?;
        if !dry_run {
            let metadata = BlockMetadata {
                min_block_number: min_slot.unwrap_or(0),
                max_block_number: max_slot.unwrap_or(0),
                min_timestamp,
                max_timestamp,
            };
            writer.write_all(&batches, &metadata)?;
        }
    }

    info!(blocks_processed, "pipeline finished");
    Ok(())
}
