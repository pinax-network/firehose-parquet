use anyhow::{anyhow, Result};
use clap::Parser;
use firehose_parquet::cli::{build_config, init_tracing, load_dotenv, Commands, CommonArgs};
use firehose_parquet::config::BlockMetadata;
use firehose_parquet::cursor::save_cursor;
use firehose_parquet::encode::{parse_encode_bytes, EncodeBytes};
use firehose_parquet::grpc::{EndpointInfo, FirehoseClient};
use firehose_parquet::traits::{fork_step_name, BlockIdentity, BlockMapper};
use firehose_parquet::writer::OutputWriter;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

use blocks::antelope::mapper::AntelopeBlockMapper;
use blocks::beacon::mapper::BeaconBlockMapper;
use blocks::bitcoin::mapper::BitcoinBlockMapper;
use blocks::cosmos::mapper::CosmosBlockMapper;
use blocks::evm::mapper::EvmBlockMapper;
use blocks::near::mapper::NearBlockMapper;
use blocks::solana::mapper::SolanaBlockMapper;
use blocks::tron::mapper::TronBlockMapper;

/// Supported block types.
const BLOCK_TYPES: &[&str] = &["auto", "evm", "bitcoin", "solana", "near", "antelope", "cosmos", "tron", "beacon"];

#[derive(Parser, Debug)]
#[command(name = "firehose-to-parquet", version, about = "Convert Firehose gRPC stream to Apache Parquet")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    common: CommonArgs,

    /// Block type to process.
    /// Use "auto" to detect from the Firehose stream.
    /// Options: auto, evm, bitcoin, solana, near, antelope, cosmos, tron, beacon
    #[arg(long, env = "BLOCK_TYPE", default_value = "auto", hide_env_values = true, help_heading = "Chain")]
    block_type: String,

    /// Enable extended detail level (extra tables: EVM calls/balance_changes/etc., Antelope db_ops, Solana vote_transactions)
    #[arg(long, env = "EXTENDED", default_value = "false", hide_env_values = true, help_heading = "Chain")]
    extended: bool,

    /// Byte encoding strategy for binary fields (hashes, addresses, etc.)
    /// Options: binary (raw bytes), hex (0x-prefixed), base58, tron_base58, auto (chain-appropriate)
    #[arg(long, env = "BYTES_ENCODING", default_value = "auto", hide_env_values = true, help_heading = "Chain")]
    bytes_encoding: String,
}

/// Detect block type from a protobuf `Any.type_url`.
fn detect_block_type(type_url: &str) -> Result<String> {
    if type_url.contains("ethereum") {
        Ok("evm".to_string())
    } else if type_url.contains("bitcoin") {
        Ok("bitcoin".to_string())
    } else if type_url.contains("solana") {
        Ok("solana".to_string())
    } else if type_url.contains("near") {
        Ok("near".to_string())
    } else if type_url.contains("antelope") {
        Ok("antelope".to_string())
    } else if type_url.contains("cosmos") {
        Ok("cosmos".to_string())
    } else if type_url.contains("tron") {
        Ok("tron".to_string())
    } else if type_url.contains("beacon") {
        Ok("beacon".to_string())
    } else {
        Err(anyhow!("unable to auto-detect block type from type_url: {type_url}"))
    }
}

/// Resolve the default `EncodeBytes` for a chain when the user specified "auto".
fn default_encode_bytes(block_type: &str) -> EncodeBytes {
    match block_type {
        "solana" => EncodeBytes::Base58,
        "tron" => EncodeBytes::TronBase58,
        _ => EncodeBytes::Hex,
    }
}

/// Resolve `EncodeBytes` from the endpoint info `block_id_encoding` field.
///
/// Encoding values (from `InfoResponse.BlockIdEncoding`):
///   0 = UNSET, 1 = HEX, 2 = 0X_HEX, 3 = BASE58
fn encode_bytes_from_block_id_encoding(encoding: i32) -> Option<EncodeBytes> {
    match encoding {
        1 => Some(EncodeBytes::Hex),       // BLOCK_ID_ENCODING_HEX
        2 => Some(EncodeBytes::Hex),       // BLOCK_ID_ENCODING_0X_HEX
        3 => Some(EncodeBytes::Base58),    // BLOCK_ID_ENCODING_BASE58
        _ => None,                         // UNSET or unknown
    }
}

/// Resolve the output directory, prepending `chain_name` when available.
fn resolve_output(base: &PathBuf, endpoint_info: &Option<EndpointInfo>) -> PathBuf {
    if let Some(ref ei) = endpoint_info {
        if !ei.chain_name.is_empty() {
            return base.join(&ei.chain_name);
        }
    }
    base.clone()
}

/// Check if the endpoint supports `extended` block features.
fn supports_extended(endpoint_info: &Option<EndpointInfo>) -> bool {
    endpoint_info.as_ref().map_or(false, |ei| {
        ei.block_features.iter().any(|f| f == "extended")
    })
}

/// Create a `Box<dyn BlockMapper>` for the given block type.
fn create_mapper(
    block_type: &str,
    extended: bool,
    include_fork_step: bool,
    encode_bytes: EncodeBytes,
) -> Result<Box<dyn BlockMapper>> {
    match block_type {
        "evm" => Ok(Box::new(EvmBlockMapper::new(extended, include_fork_step, encode_bytes))),
        "bitcoin" => Ok(Box::new(BitcoinBlockMapper::new(include_fork_step))),
        "solana" => Ok(Box::new(SolanaBlockMapper::new(extended, include_fork_step, encode_bytes))),
        "near" => Ok(Box::new(NearBlockMapper::new(include_fork_step, encode_bytes))),
        "antelope" => Ok(Box::new(AntelopeBlockMapper::new(extended, include_fork_step, encode_bytes))),
        "cosmos" => Ok(Box::new(CosmosBlockMapper::new(include_fork_step, encode_bytes))),
        "tron" => Ok(Box::new(TronBlockMapper::new(include_fork_step, encode_bytes))),
        "beacon" => Ok(Box::new(BeaconBlockMapper::new(include_fork_step, encode_bytes))),
        other => Err(anyhow!("unsupported block type: {other}. Supported: {}", BLOCK_TYPES.join(", "))),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    load_dotenv();
    let cli = Cli::parse();

    if let Some(ref cmd) = cli.command {
        match cmd {
            Commands::Completions { shell } => {
                firehose_parquet::cli::generate_completions::<Cli>(*shell);
                return Ok(());
            }
            Commands::Scan {
                path, rows, schema_only,
                aws_access_key_id, aws_secret_access_key, aws_session_token,
                aws_region, aws_endpoint_url,
            } => {
                let aws = firehose_parquet::cli::AwsConfig {
                    aws_access_key_id: aws_access_key_id.clone(),
                    aws_secret_access_key: aws_secret_access_key.clone(),
                    aws_session_token: aws_session_token.clone(),
                    aws_region: aws_region.clone(),
                    aws_endpoint_url: aws_endpoint_url.clone(),
                };
                firehose_parquet::cli::scan_parquet(path, *rows, *schema_only, Some(&aws))?;
                return Ok(());
            }
            Commands::Validate {
                path, cross_partition,
                aws_access_key_id, aws_secret_access_key, aws_session_token,
                aws_region, aws_endpoint_url,
            } => {
                let aws = firehose_parquet::cli::AwsConfig {
                    aws_access_key_id: aws_access_key_id.clone(),
                    aws_secret_access_key: aws_secret_access_key.clone(),
                    aws_session_token: aws_session_token.clone(),
                    aws_region: aws_region.clone(),
                    aws_endpoint_url: aws_endpoint_url.clone(),
                };
                let opts = firehose_parquet::cli::ValidateOptions {
                    cross_partition: *cross_partition,
                };
                let result = firehose_parquet::cli::validate_parquet(path, Some(&aws), &opts)?;
                result.print(path);
                if !result.is_valid() {
                    std::process::exit(1);
                }
                return Ok(());
            }
            Commands::Rollup {
                source, output, target_partition, compression, flush_bytes, delete_source,
                aws_access_key_id, aws_secret_access_key, aws_session_token,
                aws_region, aws_endpoint_url,
            } => {
                init_tracing(&cli.common.log_level);
                let target = firehose_parquet::rollup::parse_rollup_target(target_partition)?;
                let compression = firehose_parquet::cli::parse_compression(compression)?;
                let output_path = output.clone().unwrap_or_else(|| source.clone());
                let aws = Some(firehose_parquet::cli::AwsConfig {
                    aws_access_key_id: aws_access_key_id.clone(),
                    aws_secret_access_key: aws_secret_access_key.clone(),
                    aws_session_token: aws_session_token.clone(),
                    aws_region: aws_region.clone(),
                    aws_endpoint_url: aws_endpoint_url.clone(),
                });
                let rollup_config = firehose_parquet::rollup::RollupConfig {
                    source: source.clone(),
                    output: output_path,
                    target,
                    compression,
                    flush_bytes: *flush_bytes,
                    delete_source: *delete_source,
                    aws,
                };
                firehose_parquet::rollup::run_rollup(&rollup_config)?;
                return Ok(());
            }
        }
    }

    init_tracing(&cli.common.log_level);

    // Install graceful shutdown handler for SIGINT (Ctrl-C) and SIGTERM.
    // When a signal is received, the flag is set and the streaming loop
    // will break after the current block.  Partial (incomplete partition)
    // buffers are discarded so that only fully-written partitions survive
    // on disk, keeping file creation deterministic.
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            let ctrl_c = tokio::signal::ctrl_c();

            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate())
                    .expect("failed to install SIGTERM handler");
                tokio::select! {
                    _ = ctrl_c => {}
                    _ = sigterm.recv() => {}
                }
            }

            #[cfg(not(unix))]
            {
                ctrl_c.await.ok();
            }

            info!("shutdown signal received, finishing current block...");
            shutdown.store(true, Ordering::SeqCst);
        });
    }

    let block_type = cli.block_type.to_lowercase();
    if block_type != "auto" && !BLOCK_TYPES.contains(&block_type.as_str()) {
        return Err(anyhow!("unsupported block type: {block_type}. Supported: {}", BLOCK_TYPES.join(", ")));
    }

    let mut extended = cli.extended;
    let bytes_encoding_str = cli.bytes_encoding.clone();
    let mut config = build_config(&cli.common)?;

    // Fetch endpoint info for auto-detection of encoding, extended features,
    // and chain_name-based output directory.
    let client = FirehoseClient::new(config.clone());
    let endpoint_info = client.info().await;

    // Use chain_name as a subdirectory under the output path.
    config.output = resolve_output(&config.output, &endpoint_info);

    // Auto-detect extended block features if not explicitly set by user.
    if !extended && supports_extended(&endpoint_info) {
        info!("auto-detected extended block features from endpoint info");
        extended = true;
    }

    info!(block_type, extended, "starting pipeline\n{config}");

    let final_blocks_only = config.final_blocks_only;
    let include_fork_step = !final_blocks_only;
    let flush_rows = config.flush_rows.map(|r| r as usize);
    let flush_bytes = config.flush_bytes;
    let flush_interval_secs = config.flush_interval_secs;
    let dry_run = config.dry_run;

    let mut writer = if firehose_parquet::writer::is_s3_output(&config.output) {
        OutputWriter::new_s3(
            &config.output.to_string_lossy(),
            config.partition.clone(),
            config.compression,
            &config,
            flush_bytes,
        )?
    } else {
        OutputWriter::new(&config.output, config.partition.clone(), config.compression, flush_bytes)
    };

    // If block type is known upfront, resolve encode_bytes and create mapper immediately.
    // If "auto", defer until first block arrives.
    let mut mapper: Option<Box<dyn BlockMapper>> = if block_type != "auto" {
        let encode_bytes = parse_encode_bytes(&bytes_encoding_str)
            .or_else(|| endpoint_info.as_ref().and_then(|ei| encode_bytes_from_block_id_encoding(ei.block_id_encoding)))
            .unwrap_or_else(|| default_encode_bytes(&block_type));
        Some(create_mapper(&block_type, extended, include_fork_step, encode_bytes)?)
    } else {
        None
    };

    let cursor_path = config.cursor_path.clone();
    let mut blocks_processed: u64 = 0;
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;
    let mut min_timestamp: Option<i64> = None;
    let mut max_timestamp: Option<i64> = None;
    // Global accumulators (not reset on flush) for final summary.
    let mut global_min_block: Option<u64> = None;
    let mut global_max_block: Option<u64> = None;
    let mut last_flush_time = Instant::now();
    let mut last_cursor: Option<String> = None;
    let mut bytes_read: u64 = 0;
    let progress_start = Instant::now();

    let stream_result = client
        .stream_blocks(|block_bytes, type_url, cursor_str, identity: BlockIdentity, step: i32| {
            let fork_step_str = fork_step_name(step);
            if final_blocks_only && step == 2 {
                return Ok(());
            }

            // Lazy mapper creation for "auto" mode.
            if mapper.is_none() {
                let detected = detect_block_type(&type_url)?;
                info!(detected_type = %detected, type_url = %type_url, "auto-detected block type");
                let encode_bytes = parse_encode_bytes(&bytes_encoding_str)
                    .or_else(|| endpoint_info.as_ref().and_then(|ei| encode_bytes_from_block_id_encoding(ei.block_id_encoding)))
                    .unwrap_or_else(|| default_encode_bytes(&detected));
                mapper = Some(create_mapper(&detected, extended, include_fork_step, encode_bytes)?);
            }

            let m = mapper.as_mut().unwrap();

            let block_number = identity.block_num;
            let ts = identity.timestamp;
            min_block = Some(min_block.map_or(block_number, |s: u64| s.min(block_number)));
            max_block = Some(max_block.map_or(block_number, |s: u64| s.max(block_number)));
            global_min_block = Some(global_min_block.map_or(block_number, |s: u64| s.min(block_number)));
            global_max_block = Some(global_max_block.map_or(block_number, |s: u64| s.max(block_number)));
            min_timestamp = Some(min_timestamp.map_or(ts, |s: i64| s.min(ts)));
            max_timestamp = Some(max_timestamp.map_or(ts, |s: i64| s.max(ts)));

            m.map_block(&block_bytes, &identity, fork_step_str)?;
            blocks_processed += 1;
            bytes_read += block_bytes.len() as u64;
            last_cursor = Some(cursor_str);

            // Check for graceful shutdown after processing the current block.
            if shutdown.load(Ordering::SeqCst) {
                info!(blocks_processed, block_number, "shutdown requested, breaking out of stream");
                return Err(anyhow!("__shutdown__"));
            }

            if blocks_processed % 100 == 0 {
                let elapsed_secs = progress_start.elapsed().as_secs_f64();
                let speed_per_sec = if elapsed_secs > 0.0 {
                    bytes_read as f64 / elapsed_secs
                } else {
                    0.0
                };
                let blocks_per_sec = if elapsed_secs > 0.0 {
                    blocks_processed as f64 / elapsed_secs
                } else {
                    0.0
                };
                info!(
                    blocks_processed,
                    block_number,
                    total_rows = m.total_rows(),
                    bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                    speed = format!("{}/s", firehose_parquet::cli::format_bytes(speed_per_sec as u64)),
                    blocks_per_sec = format!("{:.1}", blocks_per_sec),
                    "progress"
                );
            }

            let time_to_flush = flush_interval_secs
                .map(|secs| last_flush_time.elapsed().as_secs() >= secs)
                .unwrap_or(false);

            let rows_to_flush = flush_rows
                .map(|limit| m.max_table_rows() >= limit)
                .unwrap_or(false);

            let bytes_to_flush = m.estimated_bytes() as u64 >= flush_bytes;

            if rows_to_flush || time_to_flush || bytes_to_flush {
                let batches = m.flush()?;
                if !dry_run {
                    let metadata = BlockMetadata {
                        min_block_number: min_block.unwrap_or(0),
                        max_block_number: max_block.unwrap_or(0),
                        min_timestamp,
                        max_timestamp,
                    };
                    let wrote = writer.write_all(&batches, &metadata)?;

                    // Only update cursor after all tables have been written.
                    if wrote {
                        if let (Some(ref path), Some(ref cursor)) = (&cursor_path, &last_cursor) {
                            if let Err(e) = save_cursor(path, cursor) {
                                warn!(error = %e, path = %path.display(), "failed to save cursor");
                            }
                        }
                    }
                }
                min_block = None;
                max_block = None;
                min_timestamp = None;
                max_timestamp = None;
                last_flush_time = Instant::now();
            }

            Ok(())
        })
        .await;

    // Distinguish graceful shutdown from real errors.
    let is_shutdown = match &stream_result {
        Err(e) if format!("{e}").contains("__shutdown__") => {
            info!("graceful shutdown initiated");
            true
        }
        Err(e) => {
            warn!(error = %e, "stream ended with error, flushing buffered data before exit");
            false
        }
        Ok(()) => false,
    };

    // On graceful shutdown, do not write partial buffers — this avoids
    // non-deterministic extra part files.  Only complete partitions that
    // were already flushed during normal processing are preserved.  On
    // restart the stream will resume from the last saved cursor, which
    // corresponds to the last fully-written partition.
    if is_shutdown {
        info!("skipping partial buffer flush to preserve partition determinism");
    } else {
        if let Some(m) = mapper.as_mut() {
            if m.max_table_rows() > 0 {
                let batches = m.flush()?;
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
        }

        // Flush any remaining buffered data in the writer.
        if !dry_run {
            let wrote = writer.flush_remaining()?;

            // Save cursor after final flush.
            if wrote {
                if let (Some(ref path), Some(ref cursor)) = (&cursor_path, &last_cursor) {
                    if let Err(e) = save_cursor(path, cursor) {
                        warn!(error = %e, path = %path.display(), "failed to save cursor");
                    }
                }
            }
        }
    }

    // Final metrics.
    let elapsed = progress_start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let blocks_per_sec = if elapsed_secs > 0.0 { blocks_processed as f64 / elapsed_secs } else { 0.0 };
    let speed_per_sec = if elapsed_secs > 0.0 { bytes_read as f64 / elapsed_secs } else { 0.0 };

    // Format elapsed as human-readable duration.
    let elapsed_display = {
        let total_secs = elapsed.as_secs();
        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let secs = total_secs % 60;
        if hours > 0 {
            format!("{}h{}m{}s", hours, minutes, secs)
        } else if minutes > 0 {
            format!("{}m{}s", minutes, secs)
        } else {
            format!("{}.{}s", secs, (elapsed.subsec_millis() / 100))
        }
    };

    // Format block range.
    let block_range = match (global_min_block, global_max_block) {
        (Some(min), Some(max)) => format!("{} — {}", min, max),
        _ => "N/A".to_string(),
    };

    info!(
        blocks_processed,
        block_range = %block_range,
        elapsed = %elapsed_display,
        bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
        speed = format!("{}/s | {:.0} blocks/s", firehose_parquet::cli::format_bytes(speed_per_sec as u64), blocks_per_sec),
        "pipeline finished",
    );

    // Propagate real (non-shutdown) errors after flushing.
    if let Err(e) = stream_result {
        if !is_shutdown {
            return Err(e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_block_type_evm() {
        assert_eq!(detect_block_type("type.googleapis.com/sf.ethereum.type.v2.Block").unwrap(), "evm");
    }

    #[test]
    fn test_detect_block_type_bitcoin() {
        assert_eq!(detect_block_type("type.googleapis.com/sf.bitcoin.type.v1.Block").unwrap(), "bitcoin");
    }

    #[test]
    fn test_detect_block_type_solana() {
        assert_eq!(detect_block_type("type.googleapis.com/sf.solana.type.v1.Block").unwrap(), "solana");
    }

    #[test]
    fn test_detect_block_type_near() {
        assert_eq!(detect_block_type("type.googleapis.com/sf.near.type.v1.Block").unwrap(), "near");
    }

    #[test]
    fn test_detect_block_type_antelope() {
        assert_eq!(detect_block_type("type.googleapis.com/sf.antelope.type.v1.Block").unwrap(), "antelope");
    }

    #[test]
    fn test_detect_block_type_cosmos() {
        assert_eq!(detect_block_type("type.googleapis.com/sf.cosmos.type.v2.Block").unwrap(), "cosmos");
    }

    #[test]
    fn test_detect_block_type_tron() {
        assert_eq!(detect_block_type("type.googleapis.com/sf.tron.type.v1.Block").unwrap(), "tron");
    }

    #[test]
    fn test_detect_block_type_beacon() {
        assert_eq!(detect_block_type("type.googleapis.com/sf.beacon.type.v1.Block").unwrap(), "beacon");
    }

    #[test]
    fn test_detect_block_type_unknown() {
        assert!(detect_block_type("type.googleapis.com/sf.unknown.type.v1.Block").is_err());
    }

    #[test]
    fn test_default_encode_bytes() {
        assert_eq!(default_encode_bytes("evm"), EncodeBytes::Hex);
        assert_eq!(default_encode_bytes("bitcoin"), EncodeBytes::Hex);
        assert_eq!(default_encode_bytes("solana"), EncodeBytes::Base58);
        assert_eq!(default_encode_bytes("tron"), EncodeBytes::TronBase58);
        assert_eq!(default_encode_bytes("near"), EncodeBytes::Hex);
        assert_eq!(default_encode_bytes("antelope"), EncodeBytes::Hex);
        assert_eq!(default_encode_bytes("cosmos"), EncodeBytes::Hex);
        assert_eq!(default_encode_bytes("beacon"), EncodeBytes::Hex);
    }

    #[test]
    fn test_create_mapper_all_types() {
        for block_type in &["evm", "bitcoin", "solana", "near", "antelope", "cosmos", "tron", "beacon"] {
            let encode_bytes = default_encode_bytes(block_type);
            let mapper = create_mapper(block_type, false, false, encode_bytes);
            assert!(mapper.is_ok(), "create_mapper failed for block_type: {block_type}");
        }
    }

    #[test]
    fn test_create_mapper_invalid_type() {
        assert!(create_mapper("unknown", false, false, EncodeBytes::Hex).is_err());
    }

    #[test]
    fn test_block_types_constant() {
        assert!(BLOCK_TYPES.contains(&"auto"));
        assert!(BLOCK_TYPES.contains(&"evm"));
        assert!(BLOCK_TYPES.contains(&"bitcoin"));
        assert!(BLOCK_TYPES.contains(&"solana"));
        assert!(BLOCK_TYPES.contains(&"near"));
        assert!(BLOCK_TYPES.contains(&"antelope"));
        assert!(BLOCK_TYPES.contains(&"cosmos"));
        assert!(BLOCK_TYPES.contains(&"tron"));
        assert!(BLOCK_TYPES.contains(&"beacon"));
        assert_eq!(BLOCK_TYPES.len(), 9); // auto + 8 chains
    }

    #[test]
    fn test_encode_bytes_from_block_id_encoding_hex() {
        assert_eq!(encode_bytes_from_block_id_encoding(1), Some(EncodeBytes::Hex));
    }

    #[test]
    fn test_encode_bytes_from_block_id_encoding_0x_hex() {
        assert_eq!(encode_bytes_from_block_id_encoding(2), Some(EncodeBytes::Hex));
    }

    #[test]
    fn test_encode_bytes_from_block_id_encoding_base58() {
        assert_eq!(encode_bytes_from_block_id_encoding(3), Some(EncodeBytes::Base58));
    }

    #[test]
    fn test_encode_bytes_from_block_id_encoding_unset() {
        assert_eq!(encode_bytes_from_block_id_encoding(0), None);
    }

    #[test]
    fn test_encode_bytes_from_block_id_encoding_unknown() {
        assert_eq!(encode_bytes_from_block_id_encoding(99), None);
    }

    #[test]
    fn test_resolve_output_with_chain_name() {
        let base = PathBuf::from(".");
        let ei = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        assert_eq!(resolve_output(&base, &ei), PathBuf::from("./mainnet"));
    }

    #[test]
    fn test_resolve_output_without_endpoint_info() {
        let base = PathBuf::from(".");
        assert_eq!(resolve_output(&base, &None), PathBuf::from("."));
    }

    #[test]
    fn test_resolve_output_empty_chain_name() {
        let base = PathBuf::from(".");
        let ei = Some(EndpointInfo {
            chain_name: String::new(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        assert_eq!(resolve_output(&base, &ei), PathBuf::from("."));
    }

    #[test]
    fn test_supports_extended_true() {
        let ei = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 1,
            block_features: vec!["extended".to_string()],
        });
        assert!(supports_extended(&ei));
    }

    #[test]
    fn test_supports_extended_false() {
        let ei = Some(EndpointInfo {
            chain_name: "solana-mainnet-beta".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 3,
            block_features: vec![],
        });
        assert!(!supports_extended(&ei));
    }

    #[test]
    fn test_supports_extended_none() {
        assert!(!supports_extended(&None));
    }
}
