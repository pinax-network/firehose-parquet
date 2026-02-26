use anyhow::Result;
use clap::Parser;
use firehose_parquet::cli::{build_config, init_tracing, Commands, CommonArgs};
use firehose_parquet::config::BlockMetadata;
use firehose_parquet::encode::{parse_encode_bytes, EncodeBytes};
use firehose_parquet::grpc::FirehoseClient;
use firehose_parquet::traits::{fork_step_name, BlockIdentity, BlockMapper};
use firehose_parquet::writer::OutputWriter;
use blocks::evm::mapper::EvmBlockMapper;
use std::time::Instant;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "firehose-evm-to-parquet", version, about = "Convert Firehose EVM gRPC stream to Apache Parquet")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    common: CommonArgs,

    /// Enable extended detail level (calls, balance_changes, etc.)
    #[arg(long, default_value = "false")]
    extended: bool,

    /// Byte encoding strategy for binary fields (hashes, addresses, etc.)
    /// Options: binary (raw bytes), hex (default, 0x-prefixed), base58, tron_base58, auto (chain-appropriate = hex for EVM)
    #[arg(long, default_value = "hex")]
    encode_bytes: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(Commands::Completions { shell }) = cli.command {
        firehose_parquet::cli::generate_completions::<Cli>(shell);
        return Ok(());
    }

    init_tracing(&cli.common.log_level);

    let extended = cli.extended;
    let config = build_config(&cli.common)?;

    info!(?config, extended, "starting EVM pipeline");

    let final_blocks_only = config.final_blocks_only;
    let include_fork_step = !final_blocks_only;
    let encode_bytes = parse_encode_bytes(&cli.encode_bytes)
        .unwrap_or(EncodeBytes::Hex); // "auto" → hex for EVM
    let mut mapper = EvmBlockMapper::new(extended, include_fork_step, encode_bytes);
    let mut writer = OutputWriter::new(&config.output, config.partition.clone(), config.compression);
    let flush_rows = config.flush_rows as usize;
    let flush_interval_secs = config.flush_interval_secs;
    let dry_run = config.dry_run;

    let mut blocks_processed: u64 = 0;
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;
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

            let block_number = identity.block_num;
            let ts = identity.timestamp;
            min_block = Some(min_block.map_or(block_number, |s: u64| s.min(block_number)));
            max_block = Some(max_block.map_or(block_number, |s: u64| s.max(block_number)));
            if let Some(t) = ts {
                min_timestamp = Some(min_timestamp.map_or(t, |s: i64| s.min(t)));
                max_timestamp = Some(max_timestamp.map_or(t, |s: i64| s.max(t)));
            }

            mapper.map_block(&block_bytes, &identity, fork_step_str)?;
            blocks_processed += 1;

            if blocks_processed % 100 == 0 {
                info!(blocks_processed, block_number, buffered_rows = mapper.max_table_rows(), "progress");
            }

            let time_to_flush = flush_interval_secs
                .map(|secs| last_flush_time.elapsed().as_secs() >= secs)
                .unwrap_or(false);

            if mapper.max_table_rows() >= flush_rows || time_to_flush {
                let batches = mapper.flush()?;
                if !dry_run {
                    let metadata = BlockMetadata {
                        min_block_number: min_block.unwrap_or(0),
                        max_block_number: max_block.unwrap_or(0),
                        min_timestamp,
                        max_timestamp,
                    };
                    writer.write_all(&batches, &metadata)?;
                }
                min_block = None;
                max_block = None;
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
                min_block_number: min_block.unwrap_or(0),
                max_block_number: max_block.unwrap_or(0),
                min_timestamp,
                max_timestamp,
            };
            writer.write_all(&batches, &metadata)?;
        }
    }

    info!(blocks_processed, "EVM pipeline finished");
    Ok(())
}
