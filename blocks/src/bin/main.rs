use anyhow::{anyhow, Result};
use clap::Parser;
use firehose_parquet::cli::{build_config, init_tracing, load_dotenv, Commands, CommonArgs};
use firehose_parquet::config::BlockMetadata;
use firehose_parquet::encode::{parse_encode_bytes, EncodeBytes};
use firehose_parquet::grpc::{EndpointInfo, FirehoseClient};
use firehose_parquet::traits::{fork_step_name, BlockIdentity, BlockMapper};
use firehose_parquet::writer::OutputWriter;
use std::path::PathBuf;
use std::time::Instant;
use tracing::info;

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
    #[arg(long, env = "BLOCK_TYPE", default_value = "auto")]
    block_type: String,

    /// Enable extended detail level (EVM only: calls, balance_changes, etc.)
    #[arg(long, env = "EXTENDED", default_value = "false")]
    extended: bool,

    /// Byte encoding strategy for binary fields (hashes, addresses, etc.)
    /// Options: binary (raw bytes), hex (0x-prefixed), base58, tron_base58, auto (chain-appropriate)
    #[arg(long, env = "BYTES_ENCODING", default_value = "auto")]
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
        "solana" => Ok(Box::new(SolanaBlockMapper::new(include_fork_step, encode_bytes))),
        "near" => Ok(Box::new(NearBlockMapper::new(include_fork_step, encode_bytes))),
        "antelope" => Ok(Box::new(AntelopeBlockMapper::new(include_fork_step, encode_bytes))),
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

    if let Some(Commands::Completions { shell }) = cli.command {
        firehose_parquet::cli::generate_completions::<Cli>(shell);
        return Ok(());
    }

    init_tracing(&cli.common.log_level);

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
        )?
    } else {
        OutputWriter::new(&config.output, config.partition.clone(), config.compression)
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

    let mut blocks_processed: u64 = 0;
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;
    let mut min_timestamp: Option<i64> = None;
    let mut max_timestamp: Option<i64> = None;
    let mut last_flush_time = Instant::now();

    client
        .stream_blocks(|block_bytes, type_url, _cursor, identity: BlockIdentity, step: i32| {
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
            if let Some(t) = ts {
                min_timestamp = Some(min_timestamp.map_or(t, |s: i64| s.min(t)));
                max_timestamp = Some(max_timestamp.map_or(t, |s: i64| s.max(t)));
            }

            m.map_block(&block_bytes, &identity, fork_step_str)?;
            blocks_processed += 1;

            if blocks_processed % 100 == 0 {
                info!(blocks_processed, block_number, buffered_rows = m.max_table_rows(), buffered_bytes = m.estimated_bytes(), "progress");
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

    info!(blocks_processed, "pipeline finished");
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
        let base = PathBuf::from("output");
        let ei = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        assert_eq!(resolve_output(&base, &ei), PathBuf::from("output/mainnet"));
    }

    #[test]
    fn test_resolve_output_without_endpoint_info() {
        let base = PathBuf::from("output");
        assert_eq!(resolve_output(&base, &None), PathBuf::from("output"));
    }

    #[test]
    fn test_resolve_output_empty_chain_name() {
        let base = PathBuf::from("output");
        let ei = Some(EndpointInfo {
            chain_name: String::new(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        assert_eq!(resolve_output(&base, &ei), PathBuf::from("output"));
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
