use anyhow::{anyhow, Result};
use clap::builder::PossibleValuesParser;
use clap::Parser;
use firehose_parquet::cli::{
    build_config, build_partitions_index_path, build_partitions_output_root,
    cursor_template_context_from_selection, init_tracing, list_partitions_from_index, load_dotenv,
    parse_partition_build_types, parse_partition_selection_request, parse_partition_shard_strategy,
    read_partitions_build_rows, resolve_cursor_template, resolve_partition_bounds_from_index,
    resolve_partition_command, resolve_partition_window_bounds_from_index, resolve_s3_output_root,
    shard_partitions_from_index, validate_partitions_index, write_partitions_index_strict,
    AwsConfig, Commands, CommonArgs, PartitionBoundsRequest, PartitionBuildResult,
    PartitionBuildRow, PartitionBuildType, PartitionIndexBuilder, PartitionListRequest,
    PartitionResolveOptions, PartitionSelectionRequest, PartitionShardRequest,
    PartitionValidateRequest, PartitionsCommands,
};
use firehose_parquet::config::{BlockMetadata, Compression, Config, Partition};
use firehose_parquet::cursor::{CursorLocation, CursorState};
use firehose_parquet::encode::{parse_encode_bytes, EncodeBytes};
use firehose_parquet::grpc::{EndpointInfo, FirehoseClient};
use firehose_parquet::metrics;
use firehose_parquet::networks::{resolve_network_endpoint, EndpointSource, KNOWN_NETWORK_NAMES};
use firehose_parquet::traits::{decode_id_bytes, fork_step_name, BlockIdentity, BlockMapper};
use firehose_parquet::writer::{OutputWriter, ParquetFileMetadata};
use object_store::ObjectStore;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
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
const BLOCK_TYPES: &[&str] = &[
    "auto", "evm", "bitcoin", "solana", "near", "antelope", "cosmos", "tron", "beacon",
];

#[derive(Parser, Debug)]
#[command(
    name = "fireparq",
    version,
    about = "Build Apache Parquet datasets from Firehose gRPC streams",
    after_long_help = "\
Default action:
  Invoke `fireparq` with stream/output flags to run the ingestion build pipeline.
  Utility workflows live under subcommands such as `partitions`, `scan`, `inspect`, `validate`, and `verify`.

Examples:
  # Resolve a built-in network alias to its default endpoint
  fireparq --network eth --start-block 20000000 --stop-block 20001000

  # Override a network alias with an env var
  FIREHOSE_ENDPOINT_SOLANA=https://solana.internal.example.com:443 \\
    fireparq --network solana --start-block 250000000 --stop-block 250100000

  # Stream EVM blocks to local Parquet (auto-detect chain)
  fireparq --network mainnet \\
    --start-block 20000000 --stop-block 20001000

  # Stream Solana with date partitioning to S3
  fireparq --network solana-mainnet-beta \\
    --start-block 250000000 --stop-block 250100000 \\
    --partition date --s3-bucket my-bucket

  # Stream with hex encoding and extended tables
  fireparq --network mainnet \\
    --start-block 20000000 --bytes-encoding hex --extended

  # Resume from cursor
  fireparq --network mainnet \\
    --cursor cursor.txt --partition date

  # Resolve range from local partitions index (no explicit start/stop)
  fireparq --network mainnet \\
    --partitions-index ./output/eth-mainnet/partitions.parquet \\
    --partition-type hour \\
    --partition-value '2015-07-30 15:00:00' \\
    --partition-chain eth-mainnet

  # Resolve range from S3 partitions index
  fireparq --network mainnet \\
    --partitions-index s3://my-bucket/eth-mainnet/partitions.parquet \\
    --partition-type date \\
    --partition-value '2015-07-30 00:00:00' \\
    --partition-chain eth-mainnet

  # Resolve range from global S3 index shared across chains
  fireparq --network mainnet \\
    --partitions-index s3://my-bucket/partitions.parquet \\
    --partition-type hour \\
    --partition-value '2015-07-30 15:00:00' \\
    --partition-chain eth-mainnet

  # Resolve an inclusive/exclusive partition window [from, to)
  fireparq --network mainnet \\
    --partitions-index ./output/eth-mainnet/partitions.parquet \\
    --partition-type hour \\
    --partition-from '2015-07-30 14:00:00' \\
    --partition-to '2015-07-30 18:00:00' \\
    --partition-chain eth-mainnet
"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    common: CommonArgs,

    /// Firehose network `chainName`.
    ///
    /// When set, resolves a known network `chainName` to a default endpoint.
    /// The canonical `chainName` remains the final resolved output. `--endpoint`
    /// or `ENDPOINT` takes precedence if already set. Supports per-network env
    /// overrides such as `FIREHOSE_ENDPOINT_MAINNET` or
    /// `FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA`.
    #[arg(
        long,
        env = "NETWORK",
        hide_env_values = true,
        value_parser = PossibleValuesParser::new(KNOWN_NETWORK_NAMES),
        help_heading = "Connection"
    )]
    network: Option<String>,

    /// Block type to process.
    /// Use "auto" to detect from the Firehose stream.
    /// Options: auto, evm, bitcoin, solana, near, antelope, cosmos, tron, beacon
    #[arg(
        long,
        env = "BLOCK_TYPE",
        default_value = "auto",
        hide_env_values = true,
        help_heading = "Chain"
    )]
    block_type: String,

    /// Enable extended detail level (extra tables: EVM calls/balance_changes/etc., Antelope db_ops, Solana vote_transactions)
    #[arg(
        long,
        env = "EXTENDED",
        default_value = "false",
        hide_env_values = true,
        help_heading = "Chain"
    )]
    extended: bool,

    /// Byte encoding strategy for binary fields (hashes, addresses, etc.)
    /// Options: binary (raw bytes), hex (0x-prefixed), hex_no_prefix, base58, tron_base58, auto (chain-appropriate)
    #[arg(
        long,
        env = "BYTES_ENCODING",
        default_value = "auto",
        hide_env_values = true,
        help_heading = "Chain"
    )]
    bytes_encoding: String,

    /// Include failed/reverted transactions in output (default: false)
    #[arg(
        long,
        env = "INCLUDE_FAILED_TRANSACTIONS",
        default_value = "false",
        hide_env_values = true,
        help_heading = "Chain"
    )]
    include_failed_transactions: bool,

    /// Override cursor parameter validation. When a cursor file exists and its
    /// stored parameters differ from the current CLI arguments, the pipeline
    /// normally exits with an error. This flag suppresses that check and
    /// resumes with the current parameters.
    #[arg(
        long,
        env = "CURSOR_OVERRIDE",
        default_value = "false",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    cursor_override: bool,
}

/// Detect block type from a protobuf `Any.type_url`.
/// Build Parquet file-level metadata with pipeline context.
fn log_file_metadata(meta: &ParquetFileMetadata) {
    for (key, value) in &meta.entries {
        info!(key = %key, value = %value, "parquet metadata");
    }
}

/// Convert `InfoResponse.BlockIdEncoding` integer to a human-readable label.
fn block_id_encoding_label(encoding: i32) -> &'static str {
    match encoding {
        1 => "hex",
        2 => "hex_0x",
        3 => "base58",
        4 => "base64",
        5 => "base64url",
        _ => "0",
    }
}

fn build_file_metadata(
    block_type: &str,
    encoding: &firehose_parquet::encode::EncodeBytes,
    endpoint: &str,
    endpoint_info: &Option<EndpointInfo>,
) -> ParquetFileMetadata {
    let mut meta = ParquetFileMetadata::new();
    meta.add("firehose-parquet.version", env!("CARGO_PKG_VERSION"));
    meta.add("firehose-parquet.block_type", block_type);
    meta.add(
        "firehose-parquet.bytes_encoding",
        format!("{:?}", encoding).to_lowercase(),
    );
    meta.add("firehose-parquet.endpoint", endpoint);
    if let Some(ref ei) = endpoint_info {
        if !ei.chain_name.is_empty() {
            meta.add("firehose-parquet.chain_name", &ei.chain_name);
        }
        if !ei.chain_name_aliases.is_empty() {
            meta.add(
                "firehose-parquet.chain_name_aliases",
                ei.chain_name_aliases.join(","),
            );
        }
        if !ei.first_streamable_block_id.is_empty() {
            meta.add(
                "firehose-parquet.first_streamable_block_id",
                &ei.first_streamable_block_id,
            );
            // When first_streamable_block_id is present, always write
            // first_streamable_block_num (even when 0) to confirm the
            // endpoint explicitly provided genesis block info.
            meta.add(
                "firehose-parquet.first_streamable_block_num",
                ei.first_streamable_block_num.to_string(),
            );
        } else if ei.first_streamable_block_num > 0 {
            meta.add(
                "firehose-parquet.first_streamable_block_num",
                ei.first_streamable_block_num.to_string(),
            );
        }
        if ei.block_id_encoding > 0 {
            meta.add(
                "firehose-parquet.block_id_encoding",
                block_id_encoding_label(ei.block_id_encoding),
            );
        }
        if !ei.block_features.is_empty() {
            meta.add(
                "firehose-parquet.block_features",
                ei.block_features.join(","),
            );
        }
    }
    meta
}

fn infer_partitions_block_type(
    chain: &str,
    endpoint_info: &Option<EndpointInfo>,
) -> Option<&'static str> {
    let mut candidates = vec![chain.to_ascii_lowercase()];
    if let Some(info) = endpoint_info {
        if !info.chain_name.is_empty() {
            candidates.push(info.chain_name.to_ascii_lowercase());
        }
        candidates.extend(
            info.chain_name_aliases
                .iter()
                .map(|alias| alias.to_ascii_lowercase()),
        );
    }

    for candidate in candidates {
        if candidate.contains("beacon") {
            return Some("beacon");
        }
        if candidate.contains("solana") {
            return Some("solana");
        }
        if candidate.contains("bitcoin") {
            return Some("bitcoin");
        }
        if candidate.contains("near") {
            return Some("near");
        }
        if candidate.contains("antelope") || candidate.contains("eos") {
            return Some("antelope");
        }
        if candidate.contains("cosmos") {
            return Some("cosmos");
        }
        if candidate.contains("tron") {
            return Some("tron");
        }
        if candidate.contains("ethereum") || candidate.contains("evm") || candidate == "mainnet" {
            return Some("evm");
        }
    }

    None
}

/// Validate that existing partitions rows match current build parameters.
/// Prevents mixing chains, partition types, or block range sizes.
fn validate_existing_partitions_params(
    existing_rows: &[PartitionBuildRow],
    chain: &str,
    partition_type: PartitionBuildType,
    block_range_size: Option<u64>,
) -> Result<()> {
    // Validate chain consistency
    for row in existing_rows {
        if let Some(ref existing_chain) = row.chain {
            if existing_chain != chain {
                return Err(anyhow!(
                    "existing partitions.parquet was built for chain '{}' but current --chain is '{}'; \
                     cannot mix chains in the same partitions file",
                    existing_chain,
                    chain
                ));
            }
        }
    }

    // Validate partition type consistency
    let current_pt = partition_type.as_str();
    for row in existing_rows {
        if row.partition_type != current_pt {
            return Err(anyhow!(
                "existing partitions.parquet uses partition type '{}' but current --partition is '{}'; \
                 cannot mix partition types in the same file",
                row.partition_type,
                current_pt
            ));
        }
    }

    // Validate block_range_size consistency (when block_range)
    if let Some(brs) = block_range_size {
        for row in existing_rows {
            if row.partition_interval_seconds > 0 && row.partition_interval_seconds != brs as i64 {
                return Err(anyhow!(
                    "existing partitions.parquet uses block_range_size={} but current --block-range-size is {}; \
                     cannot change block range size for an existing partitions file",
                    row.partition_interval_seconds,
                    brs
                ));
            }
        }
    }

    Ok(())
}

fn build_partitions_file_metadata(
    endpoint: &str,
    chain: &str,
    partition: &str,
    compression: Compression,
    endpoint_info: &Option<EndpointInfo>,
    block_range_size: Option<u64>,
    strict_timestamps: bool,
) -> ParquetFileMetadata {
    let inferred_block_type = infer_partitions_block_type(chain, endpoint_info);
    let encoding = inferred_block_type
        .map(default_encode_bytes)
        .or_else(|| {
            endpoint_info
                .as_ref()
                .and_then(|info| encode_bytes_from_block_id_encoding(info.block_id_encoding))
        })
        .unwrap_or(EncodeBytes::Hex);

    let mut meta = ParquetFileMetadata::new();
    meta.add("firehose-parquet.version", env!("CARGO_PKG_VERSION"));
    if let Some(block_type) = inferred_block_type {
        meta.add("firehose-parquet.block_type", block_type);
    }
    meta.add(
        "firehose-parquet.bytes_encoding",
        format!("{:?}", encoding).to_lowercase(),
    );
    meta.add("firehose-parquet.endpoint", endpoint);
    if let Some(info) = endpoint_info {
        if !info.chain_name.is_empty() {
            meta.add("firehose-parquet.chain_name", &info.chain_name);
        } else {
            meta.add("firehose-parquet.chain_name", chain);
        }
        if !info.chain_name_aliases.is_empty() {
            meta.add(
                "firehose-parquet.chain_name_aliases",
                info.chain_name_aliases.join(","),
            );
        }
        if !info.first_streamable_block_id.is_empty() {
            meta.add(
                "firehose-parquet.first_streamable_block_id",
                &info.first_streamable_block_id,
            );
            meta.add(
                "firehose-parquet.first_streamable_block_num",
                info.first_streamable_block_num.to_string(),
            );
        } else if info.first_streamable_block_num > 0 {
            meta.add(
                "firehose-parquet.first_streamable_block_num",
                info.first_streamable_block_num.to_string(),
            );
        }
        if info.block_id_encoding > 0 {
            meta.add(
                "firehose-parquet.block_id_encoding",
                block_id_encoding_label(info.block_id_encoding),
            );
        }
        if !info.block_features.is_empty() {
            meta.add(
                "firehose-parquet.block_features",
                info.block_features.join(","),
            );
        }
    } else {
        meta.add("firehose-parquet.chain_name", chain);
    }
    meta.add("firehose-parquet.partition", partition);
    meta.add(
        "firehose-parquet.block_range_size",
        block_range_size.unwrap_or(0).to_string(),
    );
    meta.add(
        "firehose-parquet.strict_timestamps",
        strict_timestamps.to_string(),
    );
    meta.add("firehose-parquet.compression", compression.to_string());
    meta
}

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
        Err(anyhow!(
            "unable to auto-detect block type from type_url: {type_url}"
        ))
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
        1 => Some(EncodeBytes::Hex),    // BLOCK_ID_ENCODING_HEX
        2 => Some(EncodeBytes::Hex),    // BLOCK_ID_ENCODING_0X_HEX
        3 => Some(EncodeBytes::Base58), // BLOCK_ID_ENCODING_BASE58
        _ => None,                      // UNSET or unknown
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

fn read_optional_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

async fn run_partitions_build(
    endpoint: &str,
    api_key_envvar: &str,
    api_token_envvar: &str,
    chain_override: Option<&str>,
    start_block: Option<u64>,
    stop_block: Option<u64>,
    live: bool,
    poll_interval_secs: u64,
    skip_missing_blocks: bool,
    partition_types_spec: &str,
    block_range_size: Option<u64>,
    strict_timestamps: bool,
    compression: Compression,
    output: Option<&str>,
    s3_bucket: Option<&str>,
    resume: bool,
    aws: &AwsConfig,
) -> Result<PartitionBuildResult> {
    const LIVE_PARTITIONS_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);
    const PARTITIONS_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);
    const PARTITIONS_CHECKPOINT_ROLLOVERS: usize = 16;
    const PARTITIONS_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
    const PROBE_TIMESTAMP_SCAN_LIMIT: u64 = 16;
    if !live && stop_block.is_none() {
        return Err(anyhow!("--stop-block is required unless --live is set"));
    }
    if live && stop_block.is_some() {
        return Err(anyhow!("--stop-block is incompatible with --live"));
    }
    if live && poll_interval_secs == 0 {
        return Err(anyhow!(
            "--poll-interval-secs must be greater than 0 when --live is set"
        ));
    }

    let partition_types = parse_partition_build_types(partition_types_spec)?;
    let partition_type = partition_types[0];
    let partition_label = partition_type.to_string();

    // Validate block_range_size requirement
    if partition_type == PartitionBuildType::BlockRange && block_range_size.is_none() {
        return Err(anyhow!(
            "--block-range-size is required when --partition block_range"
        ));
    }
    if partition_type != PartitionBuildType::BlockRange && block_range_size.is_some() {
        return Err(anyhow!(
            "--block-range-size is only valid when --partition block_range"
        ));
    }
    validate_block_range_bounds(partition_type, start_block, stop_block, block_range_size)?;

    let output_root = resolve_s3_output_root(output, s3_bucket)?;

    let base_config = Config {
        endpoint: endpoint.to_string(),
        api_key: read_optional_env(api_key_envvar),
        jwt_token: read_optional_env(api_token_envvar),
        start_block,
        stop_block,
        cursor_path: None,
        output: PathBuf::from(&output_root),
        partition: Partition::None,
        flush_rows: None,
        flush_bytes: 0,
        flush_interval_secs: None,
        compression,
        final_blocks_only: true,
        dry_run: false,
        aws_access_key_id: aws.aws_access_key_id.clone(),
        aws_secret_access_key: aws.aws_secret_access_key.clone(),
        aws_session_token: aws.aws_session_token.clone(),
        aws_region: aws.aws_region.clone(),
        aws_endpoint_url: aws.aws_endpoint_url.clone(),
        s3_bucket: s3_bucket.map(str::to_string),
        cache_control: None,
        metrics_port: None,
        stream_idle_timeout_secs: None,
        reconnect_stall_timeout_secs: None,
    };

    let info_client = FirehoseClient::new(base_config.clone());
    let endpoint_info = info_client.info().await;
    let chain = chain_override
        .map(str::to_string)
        .or_else(|| {
            endpoint_info
                .as_ref()
                .map(|info| info.chain_name.clone())
                .filter(|name| !name.is_empty())
        })
        .ok_or_else(|| {
            anyhow!("--chain is required when the endpoint does not expose chain_name")
        })?;

    let partitions_file_metadata = build_partitions_file_metadata(
        endpoint,
        &chain,
        &partition_label,
        compression,
        &endpoint_info,
        block_range_size,
        strict_timestamps,
    );
    let partitions_index = build_partitions_index_path(&output_root, &chain);
    let chain_output_root = build_partitions_output_root(&output_root, &chain);

    let existing_rows = match read_partitions_build_rows(&partitions_index, Some(aws)) {
        Ok(rows) => rows,
        Err(err) if err.to_string().contains("No such file or directory") => Vec::new(),
        Err(err) if err.to_string().contains("not found") => Vec::new(),
        Err(err) => return Err(err),
    };

    // Validate existing file metadata matches current parameters (prevent mixing)
    if !existing_rows.is_empty() {
        validate_existing_partitions_params(
            &existing_rows,
            &chain,
            partition_type,
            block_range_size,
        )?;
    }

    let existing_resume_block = existing_rows.iter().map(|row| row.end_block).max();

    let cursor_location = if !live && chain_output_root.starts_with("s3://") {
        let (bucket, _) = firehose_parquet::writer::parse_s3_url(&chain_output_root)?;
        let s3_client = Arc::new(aws.build_s3_client(&bucket)?) as Arc<dyn ObjectStore>;
        Some(CursorLocation::resolve(
            &chain_output_root,
            firehose_parquet::cursor::CURSOR_PARQUET_FILENAME,
            Some(s3_client),
        )?)
    } else if !live {
        Some(CursorLocation::resolve(
            &chain_output_root,
            firehose_parquet::cursor::CURSOR_PARQUET_FILENAME,
            None,
        )?)
    } else {
        None
    };

    let inferred_start_block = if live {
        if let Some(resume_block) = existing_resume_block {
            if let Some(explicit_start_block) = start_block {
                if explicit_start_block != resume_block {
                    return Err(anyhow!(
                        "--live resumes from existing partitions.parquet frontier {}; explicit --start-block {} does not match",
                        resume_block,
                        explicit_start_block
                    ));
                }
            }
            resume_block
        } else if let Some(start_block) = start_block {
            start_block
        } else if let Some(first_streamable) = endpoint_info
            .as_ref()
            .map(|info| info.first_streamable_block_num)
        {
            first_streamable
        } else {
            return Err(anyhow!(
                "--start-block is required when --live has no existing partitions.parquet frontier and the endpoint does not expose first_streamable_block_num"
            ));
        }
    } else if let Some(start_block) = start_block {
        start_block
    } else if let Some(cursor_state) = cursor_location.as_ref().and_then(CursorLocation::load) {
        cursor_state.last_block_num.saturating_add(1)
    } else if let Some(first_streamable) = endpoint_info
        .as_ref()
        .map(|info| info.first_streamable_block_num)
    {
        first_streamable
    } else {
        return Err(anyhow!(
            "--start-block is required when no sibling cursor.parquet exists and the endpoint does not expose first_streamable_block_num"
        ));
    };

    let should_resume_from_existing = live || resume;
    let (mut builder, effective_start_block, resumed_from_block) =
        if should_resume_from_existing && !existing_rows.is_empty() {
            let (mut builder, resume_start_block) = PartitionIndexBuilder::resume_from_existing(
                chain.clone(),
                partition_types.clone(),
                existing_rows.clone(),
            )?;
            if let Some(brs) = block_range_size {
                builder = builder.with_block_range_size(brs);
            }
            builder = builder.with_strict_timestamps(strict_timestamps);
            let effective_start_block = if live {
                resume_start_block
            } else {
                inferred_start_block.max(resume_start_block)
            };
            (builder, effective_start_block, Some(resume_start_block))
        } else {
            let mut builder = PartitionIndexBuilder::new(chain.clone(), partition_types.clone())?;
            if let Some(brs) = block_range_size {
                builder = builder.with_block_range_size(brs);
            }
            builder = builder.with_strict_timestamps(strict_timestamps);
            (builder, inferred_start_block, None)
        };

    if let Some(stop_block) = stop_block {
        if stop_block <= inferred_start_block {
            return Err(anyhow!(
                "--stop-block must be greater than the effective start block, got {stop_block} <= {inferred_start_block}"
            ));
        }

        if effective_start_block > stop_block {
            return Err(anyhow!(
                "effective start block {} is past --stop-block {}",
                effective_start_block,
                stop_block
            ));
        }

        if effective_start_block == stop_block {
            let rows = if should_resume_from_existing {
                existing_rows.clone()
            } else {
                Vec::new()
            };
            return Ok(PartitionBuildResult {
                partitions_index,
                chain,
                partition: partition_label.clone(),
                row_count: rows.len(),
                start_block: rows
                    .iter()
                    .map(|row| row.start_block)
                    .min()
                    .unwrap_or(inferred_start_block),
                stop_block: rows
                    .iter()
                    .map(|row| row.end_block)
                    .max()
                    .unwrap_or(stop_block),
                resumed: should_resume_from_existing && resumed_from_block.is_some(),
                resumed_from_block,
            });
        }
    }

    let stream_client = FirehoseClient::new(base_config.clone());

    info!(
        version = env!("CARGO_PKG_VERSION"),
        endpoint = %endpoint,
        chain = %chain,
        mode = if live { "live" } else { "bounded" },
        partition = %partition_label,
        "starting partitions build"
    );
    info!(
        requested_start_block = start_block,
        effective_start_block,
        requested_stop_block = stop_block,
        poll_interval_secs,
        resume_requested = resume,
        resumed = should_resume_from_existing && resumed_from_block.is_some(),
        resumed_from_block = resumed_from_block,
        existing_rows = existing_rows.len(),
        "resolved partitions build range"
    );
    info!(
        output_root = %output_root,
        chain_output_root = %chain_output_root,
        partitions_index = %partitions_index,
        "resolved partitions build paths"
    );
    log_file_metadata(&partitions_file_metadata);

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_notify = Arc::new(Notify::new());
    if live {
        let shutdown = Arc::clone(&shutdown);
        let shutdown_notify = Arc::clone(&shutdown_notify);
        tokio::spawn(async move {
            let ctrl_c = tokio::signal::ctrl_c();

            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm =
                    signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
                tokio::select! {
                    _ = ctrl_c => {}
                    _ = sigterm.recv() => {}
                }
            }

            #[cfg(not(unix))]
            {
                ctrl_c.await.ok();
            }

            info!("shutdown signal received, checkpointing live partitions build...");
            shutdown.store(true, Ordering::SeqCst);
            shutdown_notify.notify_waiters();
        });
    }

    let probe_counter = AtomicU64::new(0);

    let rows = if live {
        let poll_interval = Duration::from_secs(poll_interval_secs);
        let mut checkpoint_state = PartitionsCheckpointState::default();

        'live: loop {
            if shutdown.load(Ordering::SeqCst) {
                break;
            }

            let frontier = builder.current_frontier().unwrap_or(effective_start_block);
            let Some(maybe_frontier_block) = await_live_interruptible(
                &shutdown,
                &shutdown_notify,
                fetch_optional_probe_block_identity(
                    &stream_client,
                    frontier,
                    Some(poll_interval),
                    "polling live frontier",
                    PROBE_TIMESTAMP_SCAN_LIMIT,
                    skip_missing_blocks,
                    &probe_counter,
                ),
            )
            .await?
            else {
                info!(
                    frontier,
                    "live partitions build interrupted during frontier probe"
                );
                break;
            };

            let Some(mut current_block) = maybe_frontier_block else {
                info!(
                    frontier,
                    frontier_partition_start_ts = builder
                        .active_partition_value()
                        .unwrap_or_else(|| "<none>".to_string()),
                    poll_interval_secs,
                    "no new finalized blocks available yet; polling again"
                );

                if builder.has_rows()
                    && checkpoint_state.should_checkpoint(
                        &builder,
                        LIVE_PARTITIONS_CHECKPOINT_INTERVAL,
                        PARTITIONS_CHECKPOINT_ROLLOVERS,
                    )
                {
                    checkpoint_partitions_builder(
                        &builder,
                        &partitions_index,
                        compression,
                        Some(aws),
                        &partitions_file_metadata,
                        &mut checkpoint_state,
                        &probe_counter,
                        !strict_timestamps,
                    )?;
                }
                continue;
            };

            info!(
                frontier = current_block.block_num,
                timestamp = current_block.timestamp,
                partition_start_ts = %block_partition_start_label(partition_type, &current_block)?,
                "processing finalized blocks via sparse probes"
            );

            loop {
                if shutdown.load(Ordering::SeqCst) {
                    break 'live;
                }
                builder.observe_block(&current_block)?;
                if checkpoint_state.should_checkpoint(
                    &builder,
                    LIVE_PARTITIONS_CHECKPOINT_INTERVAL,
                    PARTITIONS_CHECKPOINT_ROLLOVERS,
                ) {
                    checkpoint_partitions_builder(
                        &builder,
                        &partitions_index,
                        compression,
                        Some(aws),
                        &partitions_file_metadata,
                        &mut checkpoint_state,
                        &probe_counter,
                        !strict_timestamps,
                    )?;
                }
                let Some(span) = await_live_interruptible(
                    &shutdown,
                    &shutdown_notify,
                    locate_live_partition_span(
                        &stream_client,
                        partition_type,
                        &current_block,
                        PARTITIONS_PROBE_TIMEOUT,
                        skip_missing_blocks,
                        &probe_counter,
                    ),
                )
                .await?
                else {
                    info!(
                        frontier = current_block.block_num,
                        "live partitions build interrupted during boundary search"
                    );
                    break 'live;
                };

                if span.last_same.block_num > current_block.block_num {
                    builder.observe_block(&span.last_same)?;
                }

                if let Some(next_boundary) = span.next_boundary {
                    info!(
                        partition = %block_partition_date_label(partition_type, &current_block)?,
                        range = %format!("[{}, {})", current_block.block_num, next_boundary.block_num),
                        next = next_boundary.block_num,
                        "detected partition rollover"
                    );
                    builder.observe_block(&next_boundary)?;
                    checkpoint_state.record_rollover();
                    current_block = next_boundary;
                    continue;
                }

                break;
            }

            if checkpoint_state.should_checkpoint(
                &builder,
                LIVE_PARTITIONS_CHECKPOINT_INTERVAL,
                PARTITIONS_CHECKPOINT_ROLLOVERS,
            ) {
                checkpoint_partitions_builder(
                    &builder,
                    &partitions_index,
                    compression,
                    Some(aws),
                    &partitions_file_metadata,
                    &mut checkpoint_state,
                    &probe_counter,
                    !strict_timestamps,
                )?;
            }
        }

        if builder.has_rows() {
            let rows = checkpoint_partitions_builder(
                &builder,
                &partitions_index,
                compression,
                Some(aws),
                &partitions_file_metadata,
                &mut checkpoint_state,
                &probe_counter,
                !strict_timestamps,
            )?;
            rows
        } else if !existing_rows.is_empty() {
            existing_rows.clone()
        } else {
            Vec::new()
        }
    } else if partition_type == PartitionBuildType::BlockRange {
        // ── Block-range build: deterministic boundaries, one probe per boundary ──
        let stop_block = stop_block.expect("validated above");
        let block_range_size = block_range_size.expect("validated above");
        let checkpoint_state_started = Instant::now();
        let mut checkpoint_state = PartitionsCheckpointState::default();

        // Align start to block_range_size boundary
        let aligned_start = (effective_start_block / block_range_size) * block_range_size;
        // Align stop to the next boundary (exclusive)
        let aligned_stop =
            ((stop_block + block_range_size - 1) / block_range_size) * block_range_size;

        info!(
            effective_start_block,
            aligned_start,
            stop_block,
            aligned_stop,
            block_range_size,
            partitions = (aligned_stop - aligned_start) / block_range_size,
            "starting block-range partitions build"
        );

        let mut rows = existing_rows.clone();
        let existing_row_count = rows.len();
        let mut boundary = aligned_start;

        while boundary < aligned_stop {
            let partition_end = (boundary + block_range_size).min(aligned_stop);

            // Probe the first block of this partition for start_time (best-effort)
            let start_time = match stream_client
                .fetch_block_identity(boundary, Some(PARTITIONS_PROBE_TIMEOUT))
                .await
            {
                Ok(Some(block)) if block.timestamp != 0 => {
                    probe_counter.fetch_add(1, Ordering::Relaxed);
                    Some(block.timestamp)
                }
                Ok(Some(_)) => {
                    probe_counter.fetch_add(1, Ordering::Relaxed);
                    None // block exists but no timestamp
                }
                _ => {
                    probe_counter.fetch_add(1, Ordering::Relaxed);
                    if skip_missing_blocks {
                        None
                    } else {
                        // Try scanning forward a small window
                        let mut found = None;
                        for offset in 1..=PROBE_TIMESTAMP_SCAN_LIMIT {
                            if let Ok(Some(block)) = stream_client
                                .fetch_block_identity(
                                    boundary + offset,
                                    Some(PARTITIONS_PROBE_TIMEOUT),
                                )
                                .await
                            {
                                probe_counter.fetch_add(1, Ordering::Relaxed);
                                if block.timestamp != 0 {
                                    found = Some(block.timestamp);
                                    break;
                                }
                            } else {
                                probe_counter.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        found
                    }
                }
            };

            // Probe the last block of this partition for end_time (best-effort)
            let end_time = if partition_end > boundary + 1 {
                match stream_client
                    .fetch_block_identity(
                        partition_end.saturating_sub(1),
                        Some(PARTITIONS_PROBE_TIMEOUT),
                    )
                    .await
                {
                    Ok(Some(block)) if block.timestamp != 0 => {
                        probe_counter.fetch_add(1, Ordering::Relaxed);
                        Some(block.timestamp)
                    }
                    Ok(Some(_)) => {
                        probe_counter.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                    _ => {
                        probe_counter.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                }
            } else {
                start_time
            };

            // Enforce strict timestamps if enabled
            if strict_timestamps && (start_time.is_none() || end_time.is_none()) {
                return Err(anyhow!(
                    "block-range partition [{}, {}) has no timestamp and --strict-timestamps is enabled; \
                     use --strict-timestamps false for chains with missing blocks",
                    boundary,
                    partition_end
                ));
            }

            let start_time_str = start_time.map(format_probe_timestamp).transpose()?;
            let end_time_str = end_time.map(format_probe_timestamp).transpose()?;

            rows.push(PartitionBuildRow {
                partition_type: "block_range".to_string(),
                partition_interval_seconds: block_range_size as i64,
                partition_start_ts: boundary.to_string(),
                partition_value: boundary.to_string(),
                start_block: boundary,
                end_block: partition_end,
                start_time: start_time_str,
                end_time: end_time_str,
                chain: Some(chain.clone()),
            });

            info!(
                partition = %format!("[{}, {})", boundary, partition_end),
                start_time = start_time.map(|t| t.to_string()).unwrap_or_else(|| "null".to_string()),
                end_time = end_time.map(|t| t.to_string()).unwrap_or_else(|| "null".to_string()),
                "built block-range partition"
            );

            checkpoint_state.record_rollover();
            let frontier_advanced =
                checkpoint_state.last_checkpoint_frontier != Some(partition_end);
            let interval_elapsed = checkpoint_state
                .last_checkpoint_at
                .map(|at| at.elapsed() >= PARTITIONS_CHECKPOINT_INTERVAL)
                .unwrap_or(true);
            let enough_rollovers =
                checkpoint_state.rollovers_since_checkpoint >= PARTITIONS_CHECKPOINT_ROLLOVERS;
            if frontier_advanced
                && (checkpoint_state.last_checkpoint_frontier.is_none()
                    || interval_elapsed
                    || enough_rollovers)
            {
                checkpoint_partitions_rows(
                    &rows,
                    &partitions_index,
                    compression,
                    Some(aws),
                    &partitions_file_metadata,
                    &mut checkpoint_state,
                    &probe_counter,
                    !strict_timestamps,
                )?;
            }

            boundary = partition_end;
        }

        if rows.len() > existing_row_count {
            write_partitions_index_strict(
                &partitions_index,
                &rows,
                compression,
                Some(aws),
                Some(&partitions_file_metadata),
                !strict_timestamps, // nullable when strict is off
            )?;
        }
        let total_probes = probe_counter.load(Ordering::Relaxed);
        let elapsed_secs = checkpoint_state_started.elapsed().as_secs();
        info!(
            stop_block = aligned_stop,
            partitions = rows.len(),
            probes = total_probes,
            elapsed = format_elapsed_human(elapsed_secs),
            "completed block-range partitions build"
        );
        rows
    } else {
        // ── Time-based build: sparse probing with exponential/binary search ──
        let stop_block = stop_block.expect("validated above");
        let mut checkpoint_state = PartitionsCheckpointState::default();
        let seed_block = fetch_required_block_identity(
            &stream_client,
            effective_start_block,
            Some(PARTITIONS_PROBE_TIMEOUT),
            "starting partitions build",
            PROBE_TIMESTAMP_SCAN_LIMIT,
            skip_missing_blocks,
            &probe_counter,
        )
        .await?;
        let lower_bound = endpoint_info
            .as_ref()
            .map(|info| info.first_streamable_block_num)
            .unwrap_or(0);
        let mut current_block = locate_partition_start(
            &stream_client,
            partition_type,
            &seed_block,
            lower_bound,
            PARTITIONS_PROBE_TIMEOUT,
            skip_missing_blocks,
            &probe_counter,
        )
        .await?;
        if current_block.block_num != seed_block.block_num {
            info!(
                requested_start_block = effective_start_block,
                partition_start_block = current_block.block_num,
                partition_start_ts = %block_partition_start_label(partition_type, &current_block)?,
                "expanded bounded build start to the enclosing partition boundary"
            );
        }

        let final_end_block = loop {
            builder.observe_block(&current_block)?;
            if checkpoint_state.should_checkpoint(
                &builder,
                PARTITIONS_CHECKPOINT_INTERVAL,
                PARTITIONS_CHECKPOINT_ROLLOVERS,
            ) {
                checkpoint_partitions_builder(
                    &builder,
                    &partitions_index,
                    compression,
                    Some(aws),
                    &partitions_file_metadata,
                    &mut checkpoint_state,
                    &probe_counter,
                    !strict_timestamps,
                )?;
            }
            let span = locate_live_partition_span(
                &stream_client,
                partition_type,
                &current_block,
                PARTITIONS_PROBE_TIMEOUT,
                skip_missing_blocks,
                &probe_counter,
            )
            .await?;

            if span.last_same.block_num > current_block.block_num {
                builder.observe_block(&span.last_same)?;
            }

            match span.next_boundary {
                Some(next_boundary) if next_boundary.block_num < stop_block => {
                    info!(
                        partition = %block_partition_date_label(partition_type, &current_block)?,
                        range = %format!("[{}, {})", current_block.block_num, next_boundary.block_num),
                        next = next_boundary.block_num,
                        "finalized sparse partition span"
                    );
                    builder.observe_block(&next_boundary)?;
                    checkpoint_state.record_rollover();
                    if checkpoint_state.should_checkpoint(
                        &builder,
                        PARTITIONS_CHECKPOINT_INTERVAL,
                        PARTITIONS_CHECKPOINT_ROLLOVERS,
                    ) {
                        checkpoint_partitions_builder(
                            &builder,
                            &partitions_index,
                            compression,
                            Some(aws),
                            &partitions_file_metadata,
                            &mut checkpoint_state,
                            &probe_counter,
                            !strict_timestamps,
                        )?;
                    }
                    current_block = next_boundary;
                }
                Some(next_boundary) => {
                    info!(
                        requested_stop_block = stop_block,
                        partition = %block_partition_date_label(partition_type, &current_block)?,
                        range = %format!("[{}, {})", current_block.block_num, next_boundary.block_num),
                        next = next_boundary.block_num,
                        "expanded bounded build stop to the enclosing partition boundary"
                    );
                    break next_boundary.block_num;
                }
                None => {
                    return Err(anyhow!(
                        "bounded partitions build could not determine the exact closing boundary for the partition containing block {}; wait for the next partition to begin or use --live",
                        current_block.block_num
                    ));
                }
            }
        };

        let rows = builder.finish(final_end_block)?;
        write_partitions_index_strict(
            &partitions_index,
            &rows,
            compression,
            Some(aws),
            Some(&partitions_file_metadata),
            !strict_timestamps,
        )?;
        let total_probes = probe_counter.load(Ordering::Relaxed);
        info!(
            stop_block = final_end_block,
            partitions = format!(
                "{} ({:.1}/h)",
                checkpoint_state.total_rollovers,
                checkpoint_state.partitions_per_hour()
            ),
            probes = format!(
                "{} ({:.1}/m)",
                total_probes,
                checkpoint_state.probes_per_min(total_probes)
            ),
            elapsed = format_elapsed_human(checkpoint_state.started_at.elapsed().as_secs()),
            "completed bounded sparse partitions build"
        );
        rows
    };

    let result_stop_block = rows
        .iter()
        .map(|row| row.end_block)
        .max()
        .unwrap_or_else(|| stop_block.unwrap_or(inferred_start_block));

    Ok(PartitionBuildResult {
        partitions_index,
        chain,
        partition: partition_label,
        row_count: rows.len(),
        start_block: rows
            .iter()
            .map(|row| row.start_block)
            .min()
            .unwrap_or(inferred_start_block),
        stop_block: result_stop_block,
        resumed: should_resume_from_existing && resumed_from_block.is_some(),
        resumed_from_block,
    })
}

fn validate_block_range_bounds(
    partition_type: PartitionBuildType,
    start_block: Option<u64>,
    stop_block: Option<u64>,
    block_range_size: Option<u64>,
) -> Result<()> {
    if partition_type != PartitionBuildType::BlockRange {
        return Ok(());
    }

    let block_range_size = block_range_size.expect("validated by caller");
    for (flag, block_num) in [("start-block", start_block), ("stop-block", stop_block)] {
        if let Some(block_num) = block_num {
            if block_num % block_range_size != 0 {
                return Err(anyhow!(
                    "--{flag} must align to --block-range-size ({block_range_size}) when --partition block_range; got {block_num}"
                ));
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct PartitionProbeSpan {
    last_same: BlockIdentity,
    next_boundary: Option<BlockIdentity>,
}

#[derive(Debug, Clone)]
struct PartitionsCheckpointState {
    started_at: Instant,
    last_checkpoint_at: Option<Instant>,
    last_checkpoint_frontier: Option<u64>,
    rollovers_since_checkpoint: usize,
    total_rollovers: usize,
}

impl Default for PartitionsCheckpointState {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            last_checkpoint_at: None,
            last_checkpoint_frontier: None,
            rollovers_since_checkpoint: 0,
            total_rollovers: 0,
        }
    }
}

impl PartitionsCheckpointState {
    fn record_rollover(&mut self) {
        self.rollovers_since_checkpoint = self.rollovers_since_checkpoint.saturating_add(1);
        self.total_rollovers = self.total_rollovers.saturating_add(1);
    }

    fn partitions_per_hour(&self) -> f64 {
        let elapsed_secs = self.started_at.elapsed().as_secs_f64();
        if elapsed_secs < 1.0 {
            return 0.0;
        }
        (self.total_rollovers as f64) / (elapsed_secs / 3600.0)
    }

    fn probes_per_min(&self, total_probes: u64) -> f64 {
        let elapsed_secs = self.started_at.elapsed().as_secs_f64();
        if elapsed_secs < 1.0 {
            return 0.0;
        }
        (total_probes as f64) / (elapsed_secs / 60.0)
    }

    fn should_checkpoint(
        &self,
        builder: &PartitionIndexBuilder,
        interval: Duration,
        max_rollovers: usize,
    ) -> bool {
        if !builder.has_rows() {
            return false;
        }

        let frontier = match builder.current_frontier() {
            Some(frontier) => frontier,
            None => return false,
        };

        if self.last_checkpoint_frontier.is_none() {
            return true;
        }

        let frontier_advanced = self.last_checkpoint_frontier != Some(frontier);
        let interval_elapsed = self
            .last_checkpoint_at
            .map(|at| at.elapsed() >= interval)
            .unwrap_or(true);
        let enough_rollovers = self.rollovers_since_checkpoint >= max_rollovers;

        frontier_advanced && (interval_elapsed || enough_rollovers)
    }

    fn record_checkpoint(&mut self, frontier: u64) {
        self.last_checkpoint_frontier = Some(frontier);
        self.last_checkpoint_at = Some(Instant::now());
        self.rollovers_since_checkpoint = 0;
    }
}

fn checkpoint_partitions_builder(
    builder: &PartitionIndexBuilder,
    partitions_index: &str,
    compression: Compression,
    aws: Option<&AwsConfig>,
    file_metadata: &ParquetFileMetadata,
    checkpoint_state: &mut PartitionsCheckpointState,
    probe_counter: &AtomicU64,
    nullable_timestamps: bool,
) -> Result<Vec<firehose_parquet::cli::PartitionBuildRow>> {
    let frontier = builder
        .current_frontier()
        .ok_or_else(|| anyhow!("partition build is missing a checkpoint frontier"))?;
    let rows = builder.snapshot(frontier)?;
    write_partitions_index_strict(
        partitions_index,
        &rows,
        compression,
        aws,
        Some(file_metadata),
        nullable_timestamps,
    )?;
    let total_probes = probe_counter.load(Ordering::Relaxed);
    info!(
        partitions = format!(
            "{} ({:.1}/h)",
            checkpoint_state.total_rollovers,
            checkpoint_state.partitions_per_hour()
        ),
        probes = format!(
            "{} ({:.1}/m)",
            total_probes,
            checkpoint_state.probes_per_min(total_probes)
        ),
        elapsed = format_elapsed_human(checkpoint_state.started_at.elapsed().as_secs()),
        "checkpoint"
    );
    checkpoint_state.record_checkpoint(frontier);
    Ok(rows)
}

fn checkpoint_partitions_rows(
    rows: &[firehose_parquet::cli::PartitionBuildRow],
    partitions_index: &str,
    compression: Compression,
    aws: Option<&AwsConfig>,
    file_metadata: &ParquetFileMetadata,
    checkpoint_state: &mut PartitionsCheckpointState,
    probe_counter: &AtomicU64,
    nullable_timestamps: bool,
) -> Result<Vec<firehose_parquet::cli::PartitionBuildRow>> {
    let frontier = rows
        .iter()
        .map(|row| row.end_block)
        .max()
        .ok_or_else(|| anyhow!("partition build is missing a checkpoint frontier"))?;
    write_partitions_index_strict(
        partitions_index,
        rows,
        compression,
        aws,
        Some(file_metadata),
        nullable_timestamps,
    )?;
    let total_probes = probe_counter.load(Ordering::Relaxed);
    info!(
        partitions = format!(
            "{} ({:.1}/h)",
            checkpoint_state.total_rollovers,
            checkpoint_state.partitions_per_hour()
        ),
        probes = format!(
            "{} ({:.1}/m)",
            total_probes,
            checkpoint_state.probes_per_min(total_probes)
        ),
        elapsed = format_elapsed_human(checkpoint_state.started_at.elapsed().as_secs()),
        "checkpoint"
    );
    checkpoint_state.record_checkpoint(frontier);
    Ok(rows.to_vec())
}

const PARTITIONS_PROBE_FETCH_MAX_ATTEMPTS: usize = 4;
const PARTITIONS_PROBE_FETCH_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const PARTITIONS_PROBE_SKIP_MISSING_BLOCK_SCAN_LIMIT: u64 = 16;

fn is_missing_probe_block_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("not found") && message.contains("block")
}

async fn scan_forward_for_available_block<T, Op, Fut>(
    start_block_num: u64,
    max_skip_blocks: u64,
    mut op: Op,
) -> Result<Option<(u64, T)>>
where
    Op: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
    for skipped_blocks in 0..=max_skip_blocks {
        let candidate_block_num = start_block_num.saturating_add(skipped_blocks);
        if let Some(value) = op(candidate_block_num).await? {
            return Ok(Some((candidate_block_num, value)));
        }
    }

    Ok(None)
}

async fn find_timestamp_borrow_probe<T, Fetch, Fut, GetTimestamp>(
    block_num: u64,
    timestamp_scan_limit: u64,
    max_skip_blocks: u64,
    mut fetch: Fetch,
    get_timestamp: GetTimestamp,
) -> Result<Option<(u64, T)>>
where
    Fetch: FnMut(u64, u64) -> Fut,
    Fut: std::future::Future<Output = Result<Option<(u64, T)>>>,
    GetTimestamp: Fn(&T) -> i64 + Copy,
{
    let phase1_end = block_num.saturating_add(timestamp_scan_limit);
    let mut search_start = block_num.saturating_add(1);

    while search_start <= phase1_end {
        let allowed_skip = max_skip_blocks.min(phase1_end.saturating_sub(search_start));
        let Some((resolved_block_num, probe)) = fetch(search_start, allowed_skip).await? else {
            break;
        };
        if get_timestamp(&probe) > 0 {
            return Ok(Some((resolved_block_num, probe)));
        }
        search_start = resolved_block_num.saturating_add(1);
    }

    let mut jump = timestamp_scan_limit.saturating_mul(2).max(32);
    loop {
        let probe_num = block_num.saturating_add(jump);
        let Some((resolved_block_num, probe)) = fetch(probe_num, max_skip_blocks).await? else {
            if max_skip_blocks == 0 {
                break;
            }
            jump = match jump.checked_mul(2) {
                Some(next) => next,
                None => break,
            };
            continue;
        };
        if get_timestamp(&probe) > 0 {
            return Ok(Some((resolved_block_num, probe)));
        }
        jump = match jump.checked_mul(2) {
            Some(next) => next,
            None => break,
        };
    }

    Ok(None)
}

async fn retry_probe_fetch_with_policy<T, Op, Fut>(
    block_num: u64,
    context: &str,
    max_attempts: usize,
    initial_backoff: Duration,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
    mut op: Op,
) -> Result<Option<T>>
where
    Op: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
    let attempts = max_attempts.max(1);
    let mut attempt = 1usize;
    let mut backoff = initial_backoff;

    loop {
        probe_counter.fetch_add(1, Ordering::Relaxed);
        match op().await {
            Ok(Some(value)) => return Ok(Some(value)),
            Ok(None) if attempt >= attempts => {
                warn!(
                    block_num,
                    context,
                    attempts = attempt,
                    "probe returned no block after retries"
                );
                return Ok(None);
            }
            Ok(None) => {
                warn!(
                    block_num,
                    context,
                    attempt,
                    max_attempts = attempts,
                    retry_backoff_ms = backoff.as_millis() as u64,
                    "probe returned no block; retrying"
                );
            }
            Err(error) if attempt >= attempts => {
                if skip_missing_blocks && is_missing_probe_block_error(&error) {
                    warn!(
                        block_num,
                        context,
                        attempts = attempt,
                        error = %error,
                        "probe fetch exhausted retries for a missing block; treating it as skipped"
                    );
                    return Ok(None);
                }
                return Err(anyhow!(
                    "{context}: probe fetch for block {block_num} failed after {attempt} attempts: {error}"
                ));
            }
            Err(error) => {
                warn!(
                    block_num,
                    context,
                    attempt,
                    max_attempts = attempts,
                    retry_backoff_ms = backoff.as_millis() as u64,
                    error = %error,
                    "probe fetch failed; retrying"
                );
            }
        }

        tokio::time::sleep(backoff).await;
        attempt = attempt.saturating_add(1);
        backoff = backoff.saturating_mul(2);
    }
}

async fn fetch_required_block_identity(
    client: &FirehoseClient,
    block_num: u64,
    wait_timeout: Option<Duration>,
    context: &str,
    timestamp_scan_limit: u64,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<BlockIdentity> {
    fetch_optional_probe_block_identity(
        client,
        block_num,
        wait_timeout,
        context,
        timestamp_scan_limit,
        skip_missing_blocks,
        probe_counter,
    )
    .await?
        .ok_or_else(|| {
            anyhow!(
                "{context}: block {block_num} is not currently available after probe retries; lower the range or use --live"
            )
        })
}

async fn await_live_interruptible<T, F>(
    shutdown: &Arc<AtomicBool>,
    shutdown_notify: &Arc<Notify>,
    future: F,
) -> Result<Option<T>>
where
    F: std::future::Future<Output = Result<T>>,
{
    if shutdown.load(Ordering::SeqCst) {
        return Ok(None);
    }

    tokio::select! {
        result = future => result.map(Some),
        _ = shutdown_notify.notified() => Ok(None),
    }
}

async fn fetch_optional_probe_block_identity(
    client: &FirehoseClient,
    block_num: u64,
    wait_timeout: Option<Duration>,
    context: &str,
    timestamp_scan_limit: u64,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<Option<BlockIdentity>> {
    let max_skip_blocks = if skip_missing_blocks {
        PARTITIONS_PROBE_SKIP_MISSING_BLOCK_SCAN_LIMIT
    } else {
        0
    };

    let Some((resolved_block_num, block)) = fetch_probe_block_identity_raw(
        client,
        block_num,
        wait_timeout,
        context,
        max_skip_blocks,
        skip_missing_blocks,
        probe_counter,
    )
    .await?
    else {
        return Ok(None);
    };

    if resolved_block_num > block_num {
        warn!(
            requested_block_num = block_num,
            resolved_block_num,
            skipped_missing_blocks = resolved_block_num.saturating_sub(block_num),
            context,
            "skipping missing blocks after probe retries"
        );
    }

    Ok(Some(
        normalize_probe_block_identity(
            client,
            block,
            wait_timeout,
            context,
            timestamp_scan_limit,
            skip_missing_blocks,
            probe_counter,
        )
        .await?,
    ))
}

async fn fetch_probe_block_identity_raw(
    client: &FirehoseClient,
    block_num: u64,
    wait_timeout: Option<Duration>,
    context: &str,
    max_skip_blocks: u64,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<Option<(u64, BlockIdentity)>> {
    scan_forward_for_available_block(block_num, max_skip_blocks, |candidate_block_num| {
        retry_probe_fetch_with_policy(
            candidate_block_num,
            context,
            PARTITIONS_PROBE_FETCH_MAX_ATTEMPTS,
            PARTITIONS_PROBE_FETCH_INITIAL_BACKOFF,
            skip_missing_blocks,
            probe_counter,
            move || client.fetch_block_identity(candidate_block_num, wait_timeout),
        )
    })
    .await
}

async fn normalize_probe_block_identity(
    client: &FirehoseClient,
    mut block: BlockIdentity,
    wait_timeout: Option<Duration>,
    context: &str,
    timestamp_scan_limit: u64,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<BlockIdentity> {
    if block.timestamp > 0 {
        return Ok(block);
    }

    let max_skip_blocks = if skip_missing_blocks {
        PARTITIONS_PROBE_SKIP_MISSING_BLOCK_SCAN_LIMIT
    } else {
        0
    };

    if let Some((borrowed_block_num, probe)) = find_timestamp_borrow_probe(
        block.block_num,
        timestamp_scan_limit,
        max_skip_blocks,
        |candidate_block_num, allowed_skip| {
            fetch_probe_block_identity_raw(
                client,
                candidate_block_num,
                wait_timeout,
                context,
                allowed_skip,
                skip_missing_blocks,
                probe_counter,
            )
        },
        |probe: &BlockIdentity| probe.timestamp,
    )
    .await?
    {
        if borrowed_block_num <= block.block_num.saturating_add(timestamp_scan_limit) {
            warn!(
                block_num = block.block_num,
                borrowed_timestamp_block = probe.block_num,
                borrowed_timestamp = probe.timestamp,
                context,
                "probe block timestamp missing; borrowing a subsequent finalized block timestamp"
            );
        } else {
            warn!(
                block_num = block.block_num,
                borrowed_timestamp_block = probe.block_num,
                borrowed_timestamp = probe.timestamp,
                scanned_offset = borrowed_block_num.saturating_sub(block.block_num),
                context,
                "probe block timestamp missing; borrowing a distant finalized block timestamp via exponential search"
            );
        }
        block.timestamp = probe.timestamp;
        return Ok(block);
    }

    Err(anyhow!(
        "{context}: block {} is missing timestamp metadata and no finalized block with a timestamp was found in any reachable subsequent block",
        block.block_num,
    ))
}

fn block_partition_start(partition_type: PartitionBuildType, block: &BlockIdentity) -> Result<i64> {
    partition_type.round_timestamp(block.timestamp)
}

fn format_probe_timestamp(timestamp: i64) -> Result<String> {
    let dt = time::OffsetDateTime::from_unix_timestamp(timestamp)
        .map_err(|err| anyhow!("invalid unix timestamp {timestamp}: {err}"))?;
    Ok(format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        dt.year(),
        dt.month() as u8,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    ))
}

fn block_partition_start_label(
    partition_type: PartitionBuildType,
    block: &BlockIdentity,
) -> Result<String> {
    format_probe_timestamp(block_partition_start(partition_type, block)?)
}

fn block_partition_date_label(
    partition_type: PartitionBuildType,
    block: &BlockIdentity,
) -> Result<String> {
    let ts = block_partition_start(partition_type, block)?;
    let dt = time::OffsetDateTime::from_unix_timestamp(ts)
        .map_err(|err| anyhow!("invalid unix timestamp {ts}: {err}"))?;
    Ok(format!(
        "{:04}-{:02}-{:02}",
        dt.year(),
        dt.month() as u8,
        dt.day(),
    ))
}

fn format_elapsed_human(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 && m > 0 {
        format!("{h}h{m}m")
    } else if h > 0 {
        format!("{h}h")
    } else if m > 0 && s > 0 {
        format!("{m}m{s}s")
    } else if m > 0 {
        format!("{m}m")
    } else {
        format!("{s}s")
    }
}

async fn find_first_different_block(
    client: &FirehoseClient,
    partition_type: PartitionBuildType,
    partition_start_ts: i64,
    mut low_same: BlockIdentity,
    mut high_different: BlockIdentity,
    probe_timeout: Duration,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<(BlockIdentity, BlockIdentity)> {
    while low_same.block_num.saturating_add(1) < high_different.block_num {
        let mid = low_same.block_num + (high_different.block_num - low_same.block_num) / 2;
        let probe = fetch_required_block_identity(
            client,
            mid,
            Some(probe_timeout),
            "probing partition boundary",
            16,
            skip_missing_blocks,
            probe_counter,
        )
        .await?;

        if probe.block_num >= high_different.block_num {
            break;
        } else if block_partition_start(partition_type, &probe)? == partition_start_ts {
            low_same = probe;
        } else {
            high_different = probe;
        }
    }

    Ok((low_same, high_different))
}

async fn locate_partition_start(
    client: &FirehoseClient,
    partition_type: PartitionBuildType,
    anchor_block: &BlockIdentity,
    lower_bound: u64,
    probe_timeout: Duration,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<BlockIdentity> {
    let partition_start_ts = block_partition_start(partition_type, anchor_block)?;
    let mut high_same = anchor_block.clone();
    let mut step = 1u64;

    let mut low_exclusive = loop {
        if high_same.block_num <= lower_bound {
            return Ok(high_same);
        }

        let candidate = high_same.block_num.saturating_sub(step).max(lower_bound);
        match fetch_optional_probe_block_identity(
            client,
            candidate,
            Some(probe_timeout),
            "backtracking partition start",
            16,
            skip_missing_blocks,
            probe_counter,
        )
        .await?
        {
            Some(probe) if block_partition_start(partition_type, &probe)? == partition_start_ts => {
                high_same = probe;
                if candidate == lower_bound {
                    return Ok(high_same);
                }
                step = step.saturating_mul(2).max(1);
            }
            Some(probe) => {
                let probe = normalize_probe_block_identity(
                    client,
                    probe,
                    Some(probe_timeout),
                    "backtracking partition start",
                    16,
                    skip_missing_blocks,
                    probe_counter,
                )
                .await?;
                break probe.block_num;
            }
            None => {
                break candidate;
            }
        }
    };

    while low_exclusive.saturating_add(1) < high_same.block_num {
        let mid = low_exclusive + (high_same.block_num - low_exclusive) / 2;
        match fetch_optional_probe_block_identity(
            client,
            mid,
            Some(probe_timeout),
            "refining partition start",
            16,
            skip_missing_blocks,
            probe_counter,
        )
        .await?
        {
            Some(probe) => {
                if probe.block_num >= high_same.block_num {
                    low_exclusive = mid;
                } else if block_partition_start(partition_type, &probe)? == partition_start_ts {
                    high_same = probe;
                } else {
                    low_exclusive = probe.block_num;
                }
            }
            _ => {
                low_exclusive = mid;
            }
        }
    }

    Ok(high_same)
}

async fn find_latest_available_block(
    client: &FirehoseClient,
    mut low_available: BlockIdentity,
    mut high_unavailable: u64,
    probe_timeout: Duration,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<BlockIdentity> {
    while low_available.block_num.saturating_add(1) < high_unavailable {
        let mid = low_available.block_num + (high_unavailable - low_available.block_num) / 2;
        match fetch_optional_probe_block_identity(
            client,
            mid,
            Some(probe_timeout),
            "finding latest available block",
            16,
            skip_missing_blocks,
            probe_counter,
        )
        .await?
        {
            Some(probe) => low_available = probe,
            None => high_unavailable = mid,
        }
    }

    Ok(low_available)
}

async fn locate_live_partition_span(
    client: &FirehoseClient,
    partition_type: PartitionBuildType,
    start_block: &BlockIdentity,
    probe_timeout: Duration,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<PartitionProbeSpan> {
    let partition_start_ts = block_partition_start(partition_type, start_block)?;
    let mut low_same = start_block.clone();
    let mut step = 1u64;

    loop {
        let candidate = low_same.block_num.saturating_add(step);
        match fetch_optional_probe_block_identity(
            client,
            candidate,
            Some(probe_timeout),
            "probing live partition span",
            16,
            skip_missing_blocks,
            probe_counter,
        )
        .await?
        {
            Some(probe) => {
                if block_partition_start(partition_type, &probe)? == partition_start_ts {
                    low_same = probe;
                    step = step.saturating_mul(2).max(1);
                    continue;
                }

                let (last_same, next_boundary) = find_first_different_block(
                    client,
                    partition_type,
                    partition_start_ts,
                    low_same,
                    probe,
                    probe_timeout,
                    skip_missing_blocks,
                    probe_counter,
                )
                .await?;

                return Ok(PartitionProbeSpan {
                    last_same,
                    next_boundary: Some(next_boundary),
                });
            }
            None => {
                let latest_available = find_latest_available_block(
                    client,
                    low_same.clone(),
                    candidate,
                    probe_timeout,
                    skip_missing_blocks,
                    probe_counter,
                )
                .await?;

                if latest_available.block_num == low_same.block_num {
                    return Ok(PartitionProbeSpan {
                        last_same: low_same,
                        next_boundary: None,
                    });
                }

                if block_partition_start(partition_type, &latest_available)? == partition_start_ts {
                    return Ok(PartitionProbeSpan {
                        last_same: latest_available,
                        next_boundary: None,
                    });
                }

                let (last_same, next_boundary) = find_first_different_block(
                    client,
                    partition_type,
                    partition_start_ts,
                    low_same,
                    latest_available,
                    probe_timeout,
                    skip_missing_blocks,
                    probe_counter,
                )
                .await?;

                return Ok(PartitionProbeSpan {
                    last_same,
                    next_boundary: Some(next_boundary),
                });
            }
        }
    }
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
    include_failed_transactions: bool,
) -> Result<Box<dyn BlockMapper>> {
    match block_type {
        "evm" => Ok(Box::new(EvmBlockMapper::new(
            extended,
            include_fork_step,
            encode_bytes,
            include_failed_transactions,
        ))),
        "bitcoin" => Ok(Box::new(BitcoinBlockMapper::new(
            include_fork_step,
            encode_bytes.clone(),
        ))),
        "solana" => Ok(Box::new(SolanaBlockMapper::new(
            extended,
            include_fork_step,
            encode_bytes,
            include_failed_transactions,
        ))),
        "near" => Ok(Box::new(NearBlockMapper::new(
            include_fork_step,
            encode_bytes,
            include_failed_transactions,
        ))),
        "antelope" => Ok(Box::new(AntelopeBlockMapper::new(
            extended,
            include_fork_step,
            encode_bytes,
            include_failed_transactions,
        ))),
        "cosmos" => Ok(Box::new(CosmosBlockMapper::new(
            include_fork_step,
            encode_bytes,
            include_failed_transactions,
        ))),
        "tron" => Ok(Box::new(TronBlockMapper::new(
            include_fork_step,
            encode_bytes,
            include_failed_transactions,
        ))),
        "beacon" => Ok(Box::new(BeaconBlockMapper::new(
            include_fork_step,
            encode_bytes,
        ))),
        other => Err(anyhow!(
            "unsupported block type: {other}. Supported: {}",
            BLOCK_TYPES.join(", ")
        )),
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
            Commands::Partitions(subcommand) => match subcommand {
                PartitionsCommands::Build {
                    endpoint,
                    network,
                    api_key_envvar,
                    api_token_envvar,
                    chain,
                    start_block,
                    stop_block,
                    live,
                    poll_interval_secs,
                    skip_missing_blocks,
                    partition,
                    block_range_size,
                    strict_timestamps,
                    compression,
                    output,
                    s3_bucket,
                    resume,
                    json,
                    aws_access_key_id,
                    aws_secret_access_key,
                    aws_session_token,
                    aws_region,
                    aws_endpoint_url,
                } => {
                    init_tracing(&cli.common.log_level);
                    let compression = firehose_parquet::cli::parse_compression(compression)?;
                    let aws = AwsConfig {
                        aws_access_key_id: aws_access_key_id.clone(),
                        aws_secret_access_key: aws_secret_access_key.clone(),
                        aws_session_token: aws_session_token.clone(),
                        aws_region: aws_region.clone(),
                        aws_endpoint_url: aws_endpoint_url.clone(),
                    };
                    let resolved_endpoint = if let Some(endpoint) = endpoint.as_deref() {
                        endpoint.to_string()
                    } else if let Some(network) = network.as_deref() {
                        let resolved = resolve_network_endpoint(network)?;
                        resolved.endpoint
                    } else {
                        return Err(anyhow!("either --endpoint or --network is required"));
                    };

                    let result = run_partitions_build(
                        &resolved_endpoint,
                        api_key_envvar,
                        api_token_envvar,
                        chain.as_deref(),
                        *start_block,
                        *stop_block,
                        *live,
                        *poll_interval_secs,
                        *skip_missing_blocks,
                        partition,
                        *block_range_size,
                        *strict_timestamps,
                        compression,
                        output.as_deref(),
                        s3_bucket.as_deref(),
                        *resume,
                        &aws,
                    )
                    .await?;

                    if *json {
                        println!("{}", serde_json::to_string_pretty(&result)?);
                    } else {
                        println!("partitions_index: {}", result.partitions_index);
                        println!("chain:            {}", result.chain);
                        println!("partition:        {}", result.partition);
                        println!("row_count:        {}", result.row_count);
                        println!("start_block:      {}", result.start_block);
                        println!("stop_block:       {}", result.stop_block);
                        println!("resumed:          {}", result.resumed);
                        if let Some(resumed_from_block) = result.resumed_from_block {
                            println!("resumed_from:     {}", resumed_from_block);
                        }
                    }

                    return Ok(());
                }
                PartitionsCommands::Validate {
                    partitions_index,
                    partition_type,
                    partition_chain,
                    allow_gaps,
                    json,
                    aws_access_key_id,
                    aws_secret_access_key,
                    aws_session_token,
                    aws_region,
                    aws_endpoint_url,
                } => {
                    let request = PartitionValidateRequest {
                        list: PartitionListRequest {
                            index_path: partitions_index.clone(),
                            partition_type: partition_type.clone(),
                            chain: partition_chain.clone(),
                            from: None,
                            to: None,
                            limit: usize::MAX,
                        },
                        allow_gaps: *allow_gaps,
                    };
                    let aws = AwsConfig {
                        aws_access_key_id: aws_access_key_id.clone(),
                        aws_secret_access_key: aws_secret_access_key.clone(),
                        aws_session_token: aws_session_token.clone(),
                        aws_region: aws_region.clone(),
                        aws_endpoint_url: aws_endpoint_url.clone(),
                    };
                    let result = validate_partitions_index(&request, Some(&aws))?;

                    if *json {
                        println!("{}", serde_json::to_string_pretty(&result)?);
                    } else {
                        println!("partitions_index: {}", result.partitions_index);
                        println!("total_rows:       {}", result.total_rows);
                        println!("issue_count:      {}", result.issue_count);
                        println!("valid:            {}", result.valid);
                        for issue in &result.issues {
                            let chain = issue.chain.as_deref().unwrap_or("<none>");
                            println!(
                                "- {:?} chain={} partition_type={} partition_value={} {}",
                                issue.kind,
                                chain,
                                issue.partition_type,
                                issue.partition_value,
                                issue.message
                            );
                        }
                    }

                    if !result.valid {
                        std::process::exit(1);
                    }
                    return Ok(());
                }
                PartitionsCommands::Shard {
                    partitions_index,
                    partition_type,
                    partition_chain,
                    from,
                    to,
                    shard_count,
                    shard_index,
                    strategy,
                    json,
                    aws_access_key_id,
                    aws_secret_access_key,
                    aws_session_token,
                    aws_region,
                    aws_endpoint_url,
                } => {
                    let list = PartitionListRequest {
                        index_path: partitions_index.clone(),
                        partition_type: partition_type.clone(),
                        chain: partition_chain.clone(),
                        from: from.clone(),
                        to: to.clone(),
                        limit: usize::MAX,
                    };
                    let request = PartitionShardRequest {
                        list,
                        shard_count: *shard_count,
                        shard_index: *shard_index,
                        strategy: parse_partition_shard_strategy(strategy)?,
                    };
                    let aws = AwsConfig {
                        aws_access_key_id: aws_access_key_id.clone(),
                        aws_secret_access_key: aws_secret_access_key.clone(),
                        aws_session_token: aws_session_token.clone(),
                        aws_region: aws_region.clone(),
                        aws_endpoint_url: aws_endpoint_url.clone(),
                    };
                    let result = shard_partitions_from_index(&request, Some(&aws))?;

                    if *json {
                        println!("{}", serde_json::to_string_pretty(&result)?);
                    } else {
                        println!("partitions_index: {}", result.partitions_index);
                        println!("shard_count:      {}", result.shard_count);
                        println!("shard_index:      {}", result.shard_index);
                        println!("strategy:         {:?}", result.strategy);
                        println!("total_matches:    {}", result.total_matches);
                        println!("returned_rows:    {}", result.returned_rows);
                        if !result.rows.is_empty() {
                            println!();
                            println!(
                                "{:<15} {:<19} {:<19} {:>12} {:>12} {}",
                                "partition_type",
                                "partition_value",
                                "partition_start_ts",
                                "start_block",
                                "end_block",
                                "chain"
                            );
                            for row in result.rows {
                                println!(
                                    "{:<15} {:<19} {:<19} {:>12} {:>12} {}",
                                    row.partition_type,
                                    row.partition_value,
                                    row.partition_start_ts,
                                    row.start_block,
                                    row.end_block,
                                    row.chain.unwrap_or_default()
                                );
                            }
                        }
                    }

                    return Ok(());
                }
                PartitionsCommands::Ls {
                    partitions_index,
                    partition_type,
                    partition_chain,
                    from,
                    to,
                    limit,
                    json,
                    aws_access_key_id,
                    aws_secret_access_key,
                    aws_session_token,
                    aws_region,
                    aws_endpoint_url,
                } => {
                    let request = PartitionListRequest {
                        index_path: partitions_index.clone(),
                        partition_type: partition_type.clone(),
                        chain: partition_chain.clone(),
                        from: from.clone(),
                        to: to.clone(),
                        limit: *limit,
                    };
                    let aws = AwsConfig {
                        aws_access_key_id: aws_access_key_id.clone(),
                        aws_secret_access_key: aws_secret_access_key.clone(),
                        aws_session_token: aws_session_token.clone(),
                        aws_region: aws_region.clone(),
                        aws_endpoint_url: aws_endpoint_url.clone(),
                    };
                    let result = list_partitions_from_index(&request, Some(&aws))?;

                    if *json {
                        println!("{}", serde_json::to_string_pretty(&result)?);
                    } else {
                        println!("partitions_index: {}", result.partitions_index);
                        println!("limit:            {}", result.limit);
                        println!("total_matches:    {}", result.total_matches);
                        println!("returned_rows:    {}", result.returned_rows);
                        if !result.rows.is_empty() {
                            println!();
                            println!(
                                "{:<15} {:<19} {:<19} {:>12} {:>12} {}",
                                "partition_type",
                                "partition_value",
                                "partition_start_ts",
                                "start_block",
                                "end_block",
                                "chain"
                            );
                            for row in result.rows {
                                println!(
                                    "{:<15} {:<19} {:<19} {:>12} {:>12} {}",
                                    row.partition_type,
                                    row.partition_value,
                                    row.partition_start_ts,
                                    row.start_block,
                                    row.end_block,
                                    row.chain.unwrap_or_default()
                                );
                            }
                        }
                    }

                    return Ok(());
                }
                PartitionsCommands::Resolve {
                    partitions_index,
                    partition_type,
                    partition_value,
                    partition_chain,
                    strict_single_chain,
                    json,
                    aws_access_key_id,
                    aws_secret_access_key,
                    aws_session_token,
                    aws_region,
                    aws_endpoint_url,
                } => {
                    let request = PartitionBoundsRequest {
                        index_path: partitions_index.clone(),
                        partition_type: partition_type.clone(),
                        partition_value: partition_value.clone(),
                        chain: partition_chain.clone(),
                    };
                    let aws = AwsConfig {
                        aws_access_key_id: aws_access_key_id.clone(),
                        aws_secret_access_key: aws_secret_access_key.clone(),
                        aws_session_token: aws_session_token.clone(),
                        aws_region: aws_region.clone(),
                        aws_endpoint_url: aws_endpoint_url.clone(),
                    };
                    let result = resolve_partition_command(
                        request,
                        Some(&aws),
                        &PartitionResolveOptions {
                            strict_single_chain: *strict_single_chain,
                        },
                    )?;

                    if *json {
                        println!("{}", serde_json::to_string_pretty(&result)?);
                    } else {
                        println!("partitions_index: {}", result.partitions_index);
                        println!("partition_type:   {}", result.partition_type);
                        println!("partition_value:  {}", result.partition_value);
                        if let Some(chain) = result.partition_chain {
                            println!("partition_chain:  {chain}");
                        }
                        println!("start_block:      {}", result.start_block);
                        println!("stop_block:       {}", result.stop_block);
                    }

                    return Ok(());
                }
            },
            Commands::Scan {
                path,
                limit,
                offset,
                schema_only,
                vertical,
                json,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
                aws_region,
                aws_endpoint_url,
            } => {
                let aws = firehose_parquet::cli::AwsConfig {
                    aws_access_key_id: aws_access_key_id.clone(),
                    aws_secret_access_key: aws_secret_access_key.clone(),
                    aws_session_token: aws_session_token.clone(),
                    aws_region: aws_region.clone(),
                    aws_endpoint_url: aws_endpoint_url.clone(),
                };
                firehose_parquet::cli::scan_parquet(
                    path,
                    *limit,
                    *offset,
                    *schema_only,
                    *vertical,
                    *json,
                    Some(&aws),
                )?;
                return Ok(());
            }
            Commands::Inspect {
                path,
                schema_only,
                json,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
                aws_region,
                aws_endpoint_url,
            } => {
                let aws = firehose_parquet::cli::AwsConfig {
                    aws_access_key_id: aws_access_key_id.clone(),
                    aws_secret_access_key: aws_secret_access_key.clone(),
                    aws_session_token: aws_session_token.clone(),
                    aws_region: aws_region.clone(),
                    aws_endpoint_url: aws_endpoint_url.clone(),
                };
                firehose_parquet::cli::inspect_parquet(path, *schema_only, *json, Some(&aws))?;
                return Ok(());
            }
            Commands::Validate {
                path,
                cross_partition,
                allow_gaps,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
                aws_region,
                aws_endpoint_url,
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
                    allow_gaps: *allow_gaps,
                };
                let result = firehose_parquet::cli::validate_parquet(path, Some(&aws), &opts)?;
                result.print(path);
                if !result.is_valid() {
                    std::process::exit(1);
                }
                return Ok(());
            }
            Commands::Rollup {
                source,
                output,
                partition,
                compression,
                flush_bytes,
                delete_source,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
                aws_region,
                aws_endpoint_url,
                cache_control,
            } => {
                init_tracing(&cli.common.log_level);
                let target = firehose_parquet::rollup::parse_rollup_target(partition)?;
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
                    cache_control: cache_control.clone(),
                };
                firehose_parquet::rollup::run_rollup(&rollup_config)?;
                return Ok(());
            }
            Commands::Verify {
                path,
                chain,
                table,
                hash_strategy,
                checks,
                profile,
                scope,
                no_fail_fast,
                report_json,
                publish_report,
                publish_report_path,
                registry_path,
                update_registry,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
                aws_region,
                aws_endpoint_url,
            } => {
                let aws = firehose_parquet::cli::AwsConfig {
                    aws_access_key_id: aws_access_key_id.clone(),
                    aws_secret_access_key: aws_secret_access_key.clone(),
                    aws_session_token: aws_session_token.clone(),
                    aws_region: aws_region.clone(),
                    aws_endpoint_url: aws_endpoint_url.clone(),
                };
                let opts = firehose_parquet::verify::VerifyOptions {
                    chain: chain.clone(),
                    table: table.clone(),
                    hash_strategy: Some(hash_strategy.clone()),
                    checks: checks.clone(),
                    profile: *profile,
                    scope: *scope,
                    no_fail_fast: *no_fail_fast,
                    report_json: report_json.clone(),
                    publish_report: *publish_report,
                    publish_report_path: publish_report_path.clone(),
                    registry_path: registry_path.clone(),
                    update_registry: *update_registry,
                };
                let report = firehose_parquet::verify::verify_parquet(path, Some(&aws), &opts)?;
                report.print();
                if !report.is_valid() {
                    std::process::exit(1);
                }
                return Ok(());
            }
            Commands::Merge {
                path,
                compression,
                flush_bytes,
                dry_run,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
                aws_region,
                aws_endpoint_url,
                cache_control,
            } => {
                init_tracing(&cli.common.log_level);
                let compression = firehose_parquet::cli::parse_compression(compression)?;
                let aws = Some(firehose_parquet::cli::AwsConfig {
                    aws_access_key_id: aws_access_key_id.clone(),
                    aws_secret_access_key: aws_secret_access_key.clone(),
                    aws_session_token: aws_session_token.clone(),
                    aws_region: aws_region.clone(),
                    aws_endpoint_url: aws_endpoint_url.clone(),
                });
                let merge_config = firehose_parquet::merge::MergeConfig {
                    path: path.clone(),
                    compression,
                    flush_bytes: *flush_bytes,
                    dry_run: *dry_run,
                    aws,
                    cache_control: cache_control.clone(),
                };
                let result = firehose_parquet::merge::run_merge(&merge_config)?;
                result.print();
                return Ok(());
            }
            Commands::Truncate {
                path,
                partition,
                dry_run,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
                aws_region,
                aws_endpoint_url,
            } => {
                init_tracing(&cli.common.log_level);
                let aws = Some(firehose_parquet::cli::AwsConfig {
                    aws_access_key_id: aws_access_key_id.clone(),
                    aws_secret_access_key: aws_secret_access_key.clone(),
                    aws_session_token: aws_session_token.clone(),
                    aws_region: aws_region.clone(),
                    aws_endpoint_url: aws_endpoint_url.clone(),
                });
                let truncate_config = firehose_parquet::truncate::TruncateConfig {
                    path: path.clone(),
                    partitions: partition.clone(),
                    dry_run: *dry_run,
                    aws,
                };
                let result = firehose_parquet::truncate::run_truncate(&truncate_config)?;
                result.print(path, *dry_run);
                return Ok(());
            }
        }
    }

    init_tracing(&cli.common.log_level);

    info!(version = env!("CARGO_PKG_VERSION"), "fireparq starting");

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
                let mut sigterm =
                    signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
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
        return Err(anyhow!(
            "unsupported block type: {block_type}. Supported: {}",
            BLOCK_TYPES.join(", ")
        ));
    }

    let mut extended = cli.extended;
    let bytes_encoding_str = cli.bytes_encoding.clone();
    let mut common = cli.common.clone();
    if common.endpoint.is_none() {
        if let Some(network) = cli.network.as_deref() {
            let resolved = resolve_network_endpoint(network)?;
            match &resolved.source {
                EndpointSource::Builtin => info!(
                    network = %resolved.requested,
                    chain_name = resolved.chain_name,
                    endpoint = %resolved.endpoint,
                    "resolved built-in network endpoint"
                ),
                EndpointSource::EnvOverride { env_var } => info!(
                    network = %resolved.requested,
                    chain_name = resolved.chain_name,
                    endpoint = %resolved.endpoint,
                    env_var = %env_var,
                    "resolved network endpoint from environment override"
                ),
            }
            common.endpoint = Some(resolved.endpoint);
        }
    } else if let Some(network) = cli.network.as_deref() {
        info!(
            endpoint = %common.endpoint.as_deref().unwrap_or_default(),
            network,
            "ignoring --network because --endpoint or ENDPOINT is already set"
        );
    }

    let mut config = build_config(&common)?;

    let partition_selection_request = parse_partition_selection_request(&cli.common)?;
    let has_explicit_range = cli.common.start_block.is_some() || cli.common.stop_block.is_some();
    if partition_selection_request.is_some() && has_explicit_range {
        warn!("partition selection flags ignored because --start-block/--stop-block was provided");
    }
    if let Some(ref request) = partition_selection_request {
        if !has_explicit_range {
            warn!(
                "partition range flags are deprecated for direct ingestion; prefer `fireparq partitions resolve ...`"
                );
            let aws = AwsConfig {
                aws_access_key_id: config.aws_access_key_id.clone(),
                aws_secret_access_key: config.aws_secret_access_key.clone(),
                aws_session_token: config.aws_session_token.clone(),
                aws_region: config.aws_region.clone(),
                aws_endpoint_url: config.aws_endpoint_url.clone(),
            };
            match request {
                PartitionSelectionRequest::Single(single_request) => {
                    let bounds = resolve_partition_bounds_from_index(single_request, Some(&aws))?;
                    config.start_block = Some(bounds.start_block);
                    config.stop_block = Some(bounds.stop_block);
                    info!(
                        partitions_index = %single_request.index_path,
                        partition_type = %single_request.partition_type,
                        partition_value = %single_request.partition_value,
                        start_block = bounds.start_block,
                        stop_block = bounds.stop_block,
                        "resolved block range from partitions index"
                    );
                }
                PartitionSelectionRequest::Window(window_request) => {
                    let bounds =
                        resolve_partition_window_bounds_from_index(window_request, Some(&aws))?;
                    config.start_block = Some(bounds.start_block);
                    config.stop_block = Some(bounds.stop_block);
                    info!(
                        partitions_index = %window_request.index_path,
                        partition_type = %window_request.partition_type,
                        partition_from = %window_request.partition_from,
                        partition_to = %window_request.partition_to,
                        partitions_count = bounds.partitions_count,
                        start_block = bounds.start_block,
                        stop_block = bounds.stop_block,
                        "resolved block range from partition window"
                    );
                }
            }
        }
    }

    if let Some(template) = cli.common.cursor_template.as_deref() {
        let selection_context = if has_explicit_range {
            None
        } else {
            partition_selection_request.as_ref()
        };
        let context = cursor_template_context_from_selection(selection_context);
        let resolved_cursor_path = resolve_cursor_template(template, &context)?;
        config.cursor_path = Some(resolved_cursor_path.clone());
        info!(
            cursor_template = %template,
            cursor_path = %resolved_cursor_path,
            "resolved partition-aware cursor path"
        );
    }

    // Fetch endpoint info for auto-detection of encoding, extended features,
    // and chain_name-based output directory.
    let mut client = FirehoseClient::new(config.clone());
    let endpoint_info = client.info().await;

    // Use chain_name as a subdirectory under the output path.
    config.output = resolve_output(&config.output, &endpoint_info);

    // Auto-detect extended block features if not explicitly set by user.
    if !extended && supports_extended(&endpoint_info) {
        info!("auto-detected extended block features from endpoint info");
        extended = true;
    }

    let include_failed_transactions = cli.include_failed_transactions;

    info!(block_type, extended, bytes_encoding = %bytes_encoding_str, include_failed_transactions, "starting pipeline\n{config}");

    // Initialize Prometheus metrics if --metrics-port is set.
    let (mut metrics_registry, pipeline_metrics) = metrics::init();

    // Register the info metric with endpoint metadata labels.
    {
        let mut labels = vec![
            ("endpoint".to_string(), config.endpoint.clone()),
            ("partition".to_string(), config.partition.to_string()),
            ("compression".to_string(), config.compression.to_string()),
            ("bytes_encoding".to_string(), bytes_encoding_str.clone()),
            ("extended".to_string(), extended.to_string()),
            (
                "final_blocks_only".to_string(),
                config.final_blocks_only.to_string(),
            ),
            ("version".to_string(), env!("CARGO_PKG_VERSION").to_string()),
        ];
        if let Some(ref ei) = endpoint_info {
            labels.push(("chain_name".to_string(), ei.chain_name.clone()));
            if !ei.chain_name_aliases.is_empty() {
                labels.push((
                    "chain_name_aliases".to_string(),
                    ei.chain_name_aliases.join(","),
                ));
            }
            labels.push((
                "first_streamable_block_num".to_string(),
                ei.first_streamable_block_num.to_string(),
            ));
            if !ei.first_streamable_block_id.is_empty() {
                labels.push((
                    "first_streamable_block_id".to_string(),
                    ei.first_streamable_block_id.clone(),
                ));
            }
            if ei.block_id_encoding > 0 {
                labels.push((
                    "block_id_encoding".to_string(),
                    block_id_encoding_label(ei.block_id_encoding).to_string(),
                ));
            }
            if !ei.block_features.is_empty() {
                labels.push(("block_features".to_string(), ei.block_features.join(",")));
            }
        }
        metrics::register_info_metric(&mut metrics_registry, labels);
    }

    // Spawn the metrics HTTP server if a port was provided.
    let metrics_registry = Arc::new(metrics_registry);
    if let Some(port) = config.metrics_port {
        metrics::serve(Arc::clone(&metrics_registry), port);
    }

    // Pass metrics to the gRPC client for reconnect tracking.
    client.set_metrics(pipeline_metrics.clone());

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
        OutputWriter::new(
            &config.output,
            config.partition.clone(),
            config.compression,
            flush_bytes,
        )
    };

    // Pass metrics to the writer for file/byte/row tracking.
    writer.set_metrics(pipeline_metrics.clone());

    // If block type is known upfront, resolve encode_bytes and create mapper immediately.
    // If "auto", defer until first block arrives.
    let mut mapper: Option<Box<dyn BlockMapper>> = if block_type != "auto" {
        let encode_bytes = parse_encode_bytes(&bytes_encoding_str)
            .or_else(|| {
                endpoint_info
                    .as_ref()
                    .and_then(|ei| encode_bytes_from_block_id_encoding(ei.block_id_encoding))
            })
            .unwrap_or_else(|| default_encode_bytes(&block_type));
        let meta =
            build_file_metadata(&block_type, &encode_bytes, &config.endpoint, &endpoint_info);
        log_file_metadata(&meta);
        writer.inner.set_file_metadata(meta);
        Some(create_mapper(
            &block_type,
            extended,
            include_fork_step,
            encode_bytes,
            include_failed_transactions,
        )?)
    } else {
        None
    };

    // Resolve cursor location (local or S3) based on output path.
    let cursor_location: Option<CursorLocation> = if let Some(ref cp) = config.cursor_path {
        let output_str = config.output.to_string_lossy().to_string();
        let s3_client = if firehose_parquet::writer::is_s3_output(&config.output) {
            Some(firehose_parquet::s3::build_s3_client(&config)?)
        } else {
            None
        };
        Some(CursorLocation::resolve(&output_str, cp, s3_client)?)
    } else {
        None
    };
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
    let mut last_block_num: u64 = 0;
    let mut last_block_id: String = String::new();
    let mut bytes_read: u64 = 0;
    let progress_start = Instant::now();
    let mut current_partition_key: Option<String> = None;
    let partition_config = config.partition.clone();

    // Build file-level metadata for the cursor (same `firehose-parquet.*`
    // namespace as table files). Includes version, endpoint, chain info, and
    // pipeline parameters.
    let cursor_file_metadata = {
        let mut meta = ParquetFileMetadata::new();
        meta.add("firehose-parquet.version", env!("CARGO_PKG_VERSION"));
        if block_type != "auto" {
            meta.add("firehose-parquet.block_type", &block_type);
        }
        meta.add("firehose-parquet.bytes_encoding", &bytes_encoding_str);
        meta.add("firehose-parquet.endpoint", &config.endpoint);
        if let Some(ref ei) = endpoint_info {
            if !ei.chain_name.is_empty() {
                meta.add("firehose-parquet.chain_name", &ei.chain_name);
            }
            if !ei.chain_name_aliases.is_empty() {
                meta.add(
                    "firehose-parquet.chain_name_aliases",
                    ei.chain_name_aliases.join(","),
                );
            }
            if !ei.first_streamable_block_id.is_empty() {
                meta.add(
                    "firehose-parquet.first_streamable_block_id",
                    &ei.first_streamable_block_id,
                );
                meta.add(
                    "firehose-parquet.first_streamable_block_num",
                    ei.first_streamable_block_num.to_string(),
                );
            } else if ei.first_streamable_block_num > 0 {
                meta.add(
                    "firehose-parquet.first_streamable_block_num",
                    ei.first_streamable_block_num.to_string(),
                );
            }
            if ei.block_id_encoding > 0 {
                meta.add(
                    "firehose-parquet.block_id_encoding",
                    block_id_encoding_label(ei.block_id_encoding),
                );
            }
            if !ei.block_features.is_empty() {
                meta.add(
                    "firehose-parquet.block_features",
                    ei.block_features.join(","),
                );
            }
        }
        meta.add("firehose-parquet.partition", config.partition.to_string());
        meta.add(
            "firehose-parquet.block_range_size",
            match &config.partition {
                firehose_parquet::config::Partition::BlockRange(size) => size.to_string(),
                _ => "0".to_string(),
            },
        );
        meta.add(
            "firehose-parquet.compression",
            config.compression.to_string(),
        );
        meta
    };

    // Build a template CursorState with pipeline parameters that stay constant.
    let cursor_state_template = CursorState {
        start_block: config.start_block,
        stop_block: config.stop_block,
        extended,
        final_blocks_only: config.final_blocks_only,
        include_failed_transactions,
        file_metadata: cursor_file_metadata,
        ..CursorState::default()
    };

    // Validate cursor parameters against current CLI arguments.
    if let Some(ref loc) = cursor_location {
        if let Some(loaded) = loc.load() {
            let mismatches = loaded.validate_params(&cursor_state_template);
            if !mismatches.is_empty() {
                if cli.cursor_override {
                    warn!(
                        "cursor parameter mismatch detected (overridden via --cursor-override):\n  {}",
                        mismatches.join("\n  ")
                    );
                } else {
                    return Err(anyhow!(
                        "cursor parameter mismatch detected:\n  {}\n\nUse --cursor-override to force resume with current parameters.",
                        mismatches.join("\n  ")
                    ));
                }
            }
        }
    }

    let stream_result = client
        .stream_blocks(cursor_location.as_ref(), |block_bytes, type_url, cursor_str, identity: BlockIdentity, step: i32| {
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
                let meta = build_file_metadata(&detected, &encode_bytes, &config.endpoint, &endpoint_info);
                log_file_metadata(&meta);
                writer.inner.set_file_metadata(meta);
                mapper = Some(create_mapper(&detected, extended, include_fork_step, encode_bytes, include_failed_transactions)?);
            }

            let m = mapper.as_mut().unwrap();

            let block_number = identity.block_num;
            let ts = identity.timestamp;

            // Flush the mapper at partition boundaries to ensure each flush
            // produces batches belonging to exactly one partition.
            // See: https://github.com/pinax-network/firehose-parquet/issues/110
            let new_partition_key = partition_config.partition_key(block_number, ts);
            if let Some(ref new_key) = new_partition_key {
                let partition_changed = current_partition_key
                    .as_ref()
                    .map_or(false, |cur| cur != new_key);
                if partition_changed && m.max_table_rows() > 0 {
                    info!(
                        old_partition = %current_partition_key.as_deref().unwrap_or("?"),
                        new_partition = %new_key,
                        block_number,
                        "partition boundary detected, flushing mapper"
                    );
                    let batches = m.flush()?;
                    if !dry_run {
                        let metadata = BlockMetadata {
                            min_block_number: min_block.unwrap_or(0),
                            max_block_number: max_block.unwrap_or(0),
                            min_timestamp,
                            max_timestamp,
                        };
                        let wrote = writer.write_all(&batches, &metadata)?;
                        if wrote {
                            pipeline_metrics.flushes_total.get_or_create(&metrics::FlushLabels { trigger: "partition_boundary".to_string() }).inc();
                            if let (Some(ref loc), Some(ref cursor)) = (&cursor_location, &last_cursor) {
                                let mut state = cursor_state_template.clone();
                                state.cursor = cursor.clone();
                                state.last_block_num = last_block_num;
                                state.last_block_id = decode_id_bytes(&last_block_id);
                                state.updated_at = time::OffsetDateTime::now_utc()
                                    .format(&time::format_description::well_known::Rfc3339)
                                    .unwrap_or_default();
                                if let Err(e) = loc.save(&state) {
                                    warn!(error = %e, "failed to save cursor.parquet");
                                    pipeline_metrics.errors_total.get_or_create(&metrics::ErrorLabels { kind: "cursor_save".to_string() }).inc();
                                } else {
                                    pipeline_metrics.cursor_saves_total.inc();
                                    pipeline_metrics.cursor_last_block_num.set(last_block_num as i64);
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
            }
            current_partition_key = new_partition_key;

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
            last_block_num = block_number;
            last_block_id = identity.block_id.clone();

            // Update Prometheus metrics.
            pipeline_metrics.blocks_processed_total.inc();
            pipeline_metrics.bytes_read_total.inc_by(block_bytes.len() as u64);
            pipeline_metrics.current_block_number.set(block_number as i64);
            if global_min_block.map_or(true, |g| block_number <= g) {
                pipeline_metrics.min_block_number.set(block_number as i64);
            }
            if global_max_block.map_or(true, |g| block_number >= g) {
                pipeline_metrics.max_block_number.set(block_number as i64);
            }

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
                    speed = format!("{}/s | {:.0} blocks/s", firehose_parquet::cli::format_bytes(speed_per_sec as u64), blocks_per_sec),
                    "progress"
                );

                // Update rolling throughput gauges.
                pipeline_metrics.blocks_per_second.set(blocks_per_sec);
                pipeline_metrics.bytes_per_second.set(speed_per_sec);
                pipeline_metrics.elapsed_seconds.set(elapsed_secs);
            }

            let time_to_flush = flush_interval_secs
                .map(|secs| last_flush_time.elapsed().as_secs() >= secs)
                .unwrap_or(false);

            let rows_to_flush = flush_rows
                .map(|limit| m.max_table_rows() >= limit)
                .unwrap_or(false);

            let bytes_to_flush = m.estimated_bytes() as u64 >= flush_bytes;

            if rows_to_flush || time_to_flush || bytes_to_flush {
                let flush_trigger = if bytes_to_flush { "bytes" } else if rows_to_flush { "rows" } else { "interval" };
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
                        pipeline_metrics.flushes_total.get_or_create(&metrics::FlushLabels { trigger: flush_trigger.to_string() }).inc();
                        if let (Some(ref loc), Some(ref cursor)) = (&cursor_location, &last_cursor) {
                            let mut state = cursor_state_template.clone();
                            state.cursor = cursor.clone();
                            state.last_block_num = last_block_num;
                            state.last_block_id = decode_id_bytes(&last_block_id);
                            state.updated_at = time::OffsetDateTime::now_utc()
                                .format(&time::format_description::well_known::Rfc3339)
                                .unwrap_or_default();
                            if let Err(e) = loc.save(&state) {
                                warn!(error = %e, "failed to save cursor.parquet");
                                pipeline_metrics.errors_total.get_or_create(&metrics::ErrorLabels { kind: "cursor_save".to_string() }).inc();
                            } else {
                                pipeline_metrics.cursor_saves_total.inc();
                                pipeline_metrics.cursor_last_block_num.set(last_block_num as i64);
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
                pipeline_metrics
                    .flushes_total
                    .get_or_create(&metrics::FlushLabels {
                        trigger: "shutdown".to_string(),
                    })
                    .inc();
                if let (Some(ref loc), Some(ref cursor)) = (&cursor_location, &last_cursor) {
                    let mut state = cursor_state_template.clone();
                    state.cursor = cursor.clone();
                    state.last_block_num = last_block_num;
                    state.last_block_id = decode_id_bytes(&last_block_id);
                    state.updated_at = time::OffsetDateTime::now_utc()
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_default();
                    if let Err(e) = loc.save(&state) {
                        warn!(error = %e, "failed to save cursor.parquet");
                        pipeline_metrics
                            .errors_total
                            .get_or_create(&metrics::ErrorLabels {
                                kind: "cursor_save".to_string(),
                            })
                            .inc();
                    } else {
                        pipeline_metrics.cursor_saves_total.inc();
                        pipeline_metrics
                            .cursor_last_block_num
                            .set(last_block_num as i64);
                    }
                }
            }
        }
    }

    // Final metrics.
    let elapsed = progress_start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let blocks_per_sec = if elapsed_secs > 0.0 {
        blocks_processed as f64 / elapsed_secs
    } else {
        0.0
    };
    let speed_per_sec = if elapsed_secs > 0.0 {
        bytes_read as f64 / elapsed_secs
    } else {
        0.0
    };

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
    use clap::CommandFactory;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[test]
    fn test_cli_name_is_fireparq() {
        let cmd = Cli::command();
        assert_eq!(cmd.get_name(), "fireparq");
    }

    #[test]
    fn test_cli_long_help_mentions_default_action() {
        let mut cmd = Cli::command();
        let help = cmd.render_long_help().to_string();
        assert!(help.contains("Default action:"));
        assert!(help.contains("fireparq"));
        assert!(help.contains("ingestion build pipeline"));
    }

    #[test]
    fn test_cli_help_mentions_network() {
        let mut cmd = Cli::command();
        let help = cmd.render_long_help().to_string();
        assert!(help.contains("--network <NETWORK>"));
        assert!(help.contains("FIREHOSE_ENDPOINT_MAINNET"));
    }

    #[test]
    fn test_cli_parses_network_flag() {
        let cli = Cli::parse_from(["fireparq", "--network", "mainnet", "--start-block", "100"]);
        assert_eq!(cli.network.as_deref(), Some("mainnet"));
        assert_eq!(cli.common.start_block, Some(100));
    }

    #[test]
    fn test_cli_rejects_unknown_network_flag() {
        let err = Cli::try_parse_from(["fireparq", "--network", "unknown"])
            .expect_err("unknown network should fail clap parsing");
        let rendered = err.to_string();
        assert!(rendered.contains("invalid value 'unknown'"));
        assert!(rendered.contains("solana-mainnet-beta"));
    }

    #[test]
    fn test_validate_block_range_bounds_accepts_aligned_values() {
        validate_block_range_bounds(
            PartitionBuildType::BlockRange,
            Some(0),
            Some(30_000_000),
            Some(10_000_000),
        )
        .expect("aligned block-range bounds should be accepted");
    }

    #[test]
    fn test_validate_block_range_bounds_rejects_misaligned_start_block() {
        let err = validate_block_range_bounds(
            PartitionBuildType::BlockRange,
            Some(1),
            Some(30_000_000),
            Some(10_000_000),
        )
        .expect_err("misaligned start block should fail");

        assert_eq!(
            err.to_string(),
            "--start-block must align to --block-range-size (10000000) when --partition block_range; got 1"
        );
    }

    #[test]
    fn test_validate_block_range_bounds_rejects_misaligned_stop_block() {
        let err = validate_block_range_bounds(
            PartitionBuildType::BlockRange,
            Some(0),
            Some(30_000_001),
            Some(10_000_000),
        )
        .expect_err("misaligned stop block should fail");

        assert_eq!(
            err.to_string(),
            "--stop-block must align to --block-range-size (10000000) when --partition block_range; got 30000001"
        );
    }

    #[test]
    fn test_validate_block_range_bounds_ignores_non_block_range_partitions() {
        validate_block_range_bounds(PartitionBuildType::Date, Some(1), Some(2), None)
            .expect("non block-range partitions should not enforce alignment");
    }

    #[test]
    fn test_detect_block_type_evm() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.ethereum.type.v2.Block").unwrap(),
            "evm"
        );
    }

    #[test]
    fn test_detect_block_type_bitcoin() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.bitcoin.type.v1.Block").unwrap(),
            "bitcoin"
        );
    }

    #[test]
    fn test_detect_block_type_solana() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.solana.type.v1.Block").unwrap(),
            "solana"
        );
    }

    #[test]
    fn test_detect_block_type_near() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.near.type.v1.Block").unwrap(),
            "near"
        );
    }

    #[test]
    fn test_detect_block_type_antelope() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.antelope.type.v1.Block").unwrap(),
            "antelope"
        );
    }

    #[test]
    fn test_detect_block_type_cosmos() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.cosmos.type.v2.Block").unwrap(),
            "cosmos"
        );
    }

    #[test]
    fn test_detect_block_type_tron() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.tron.type.v1.Block").unwrap(),
            "tron"
        );
    }

    #[test]
    fn test_detect_block_type_beacon() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.beacon.type.v1.Block").unwrap(),
            "beacon"
        );
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
        for block_type in &[
            "evm", "bitcoin", "solana", "near", "antelope", "cosmos", "tron", "beacon",
        ] {
            let encode_bytes = default_encode_bytes(block_type);
            let mapper = create_mapper(block_type, false, false, encode_bytes, false);
            assert!(
                mapper.is_ok(),
                "create_mapper failed for block_type: {block_type}"
            );
        }
    }

    #[test]
    fn test_create_mapper_invalid_type() {
        assert!(create_mapper("unknown", false, false, EncodeBytes::Hex, false).is_err());
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
        assert_eq!(
            encode_bytes_from_block_id_encoding(1),
            Some(EncodeBytes::Hex)
        );
    }

    #[test]
    fn test_encode_bytes_from_block_id_encoding_0x_hex() {
        assert_eq!(
            encode_bytes_from_block_id_encoding(2),
            Some(EncodeBytes::Hex)
        );
    }

    #[test]
    fn test_encode_bytes_from_block_id_encoding_base58() {
        assert_eq!(
            encode_bytes_from_block_id_encoding(3),
            Some(EncodeBytes::Base58)
        );
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

    // -- block_id_encoding_label tests --

    #[test]
    fn test_block_id_encoding_label() {
        assert_eq!(block_id_encoding_label(0), "0");
        assert_eq!(block_id_encoding_label(1), "hex");
        assert_eq!(block_id_encoding_label(2), "hex_0x");
        assert_eq!(block_id_encoding_label(3), "base58");
        assert_eq!(block_id_encoding_label(4), "base64");
        assert_eq!(block_id_encoding_label(5), "base64url");
        assert_eq!(block_id_encoding_label(99), "0");
    }

    // -- build_file_metadata tests --

    fn find_meta<'a>(meta: &'a ParquetFileMetadata, key: &str) -> Option<&'a str> {
        meta.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn test_build_file_metadata_full_endpoint_info() {
        let ei = Some(EndpointInfo {
            chain_name: "matic".to_string(),
            chain_name_aliases: vec!["polygon".to_string(), "matic".to_string()],
            first_streamable_block_num: 100,
            first_streamable_block_id: "0xabc".to_string(),
            block_id_encoding: 2,
            block_features: vec!["base".to_string(), "extended".to_string()],
        });
        let meta = build_file_metadata("evm", &EncodeBytes::Hex, "https://example.com", &ei);

        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name"),
            Some("matic")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name_aliases"),
            Some("polygon,matic")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_id"),
            Some("0xabc")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_num"),
            Some("100")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_id_encoding"),
            Some("hex_0x")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_features"),
            Some("base,extended")
        );
    }

    #[test]
    fn test_build_file_metadata_genesis_block_zero() {
        // When first_streamable_block_id is present, first_streamable_block_num
        // should be written even when it is 0.
        let ei = Some(EndpointInfo {
            chain_name: "eth".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: "0xd4e56740".to_string(),
            block_id_encoding: 1,
            block_features: vec![],
        });
        let meta = build_file_metadata("evm", &EncodeBytes::Hex, "https://example.com", &ei);

        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_id"),
            Some("0xd4e56740")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_num"),
            Some("0")
        );
    }

    #[test]
    fn test_build_file_metadata_no_block_id_skips_zero_block_num() {
        // When first_streamable_block_id is empty and block_num is 0, neither
        // should be written (proto default ambiguity).
        let ei = Some(EndpointInfo {
            chain_name: "eth".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        let meta = build_file_metadata("evm", &EncodeBytes::Hex, "https://example.com", &ei);

        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_id"),
            None
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_num"),
            None
        );
    }

    #[test]
    fn test_build_file_metadata_block_num_without_block_id() {
        // When block_num > 0 but block_id is empty, still write block_num.
        let ei = Some(EndpointInfo {
            chain_name: "eth".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 42,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        let meta = build_file_metadata("evm", &EncodeBytes::Hex, "https://example.com", &ei);

        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_id"),
            None
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_num"),
            Some("42")
        );
    }

    #[test]
    fn test_build_file_metadata_no_endpoint_info() {
        let meta = build_file_metadata("evm", &EncodeBytes::Hex, "https://example.com", &None);

        assert_eq!(find_meta(&meta, "firehose-parquet.chain_name"), None);
        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name_aliases"),
            None
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_id"),
            None
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_num"),
            None
        );
        assert_eq!(find_meta(&meta, "firehose-parquet.block_id_encoding"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.block_features"), None);
    }

    #[test]
    fn test_build_file_metadata_empty_optional_fields() {
        // Empty aliases, empty block_id, unset encoding, empty features → none written.
        let ei = Some(EndpointInfo {
            chain_name: "eth".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        let meta = build_file_metadata("evm", &EncodeBytes::Hex, "https://example.com", &ei);

        assert_eq!(find_meta(&meta, "firehose-parquet.chain_name"), Some("eth"));
        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name_aliases"),
            None
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_id"),
            None
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_num"),
            None
        );
        assert_eq!(find_meta(&meta, "firehose-parquet.block_id_encoding"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.block_features"), None);
    }

    #[tokio::test]
    async fn test_retry_probe_fetch_with_policy_retries_none_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let probe_counter = AtomicU64::new(0);
        let result = retry_probe_fetch_with_policy(
            42,
            "testing probe retry",
            3,
            Duration::from_millis(0),
            false,
            &probe_counter,
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        let current = attempts.fetch_add(1, AtomicOrdering::SeqCst);
                        if current < 2 {
                            Ok(None)
                        } else {
                            Ok(Some(99_u64))
                        }
                    }
                }
            },
        )
        .await
        .expect("retry should succeed");

        assert_eq!(result, Some(99));
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(probe_counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_probe_fetch_with_policy_retries_error_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let probe_counter = AtomicU64::new(0);
        let result = retry_probe_fetch_with_policy(
            42,
            "testing probe retry",
            3,
            Duration::from_millis(0),
            false,
            &probe_counter,
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        let current = attempts.fetch_add(1, AtomicOrdering::SeqCst);
                        if current < 2 {
                            Err(anyhow!("transient failure"))
                        } else {
                            Ok(Some(77_u64))
                        }
                    }
                }
            },
        )
        .await
        .expect("retry should succeed");

        assert_eq!(result, Some(77));
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(probe_counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_probe_fetch_with_policy_exhausts_errors() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let probe_counter = AtomicU64::new(0);
        let err = retry_probe_fetch_with_policy(
            42,
            "testing probe retry",
            3,
            Duration::from_millis(0),
            false,
            &probe_counter,
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        attempts.fetch_add(1, AtomicOrdering::SeqCst);
                        Err::<Option<u64>, _>(anyhow!("still failing"))
                    }
                }
            },
        )
        .await
        .expect_err("retries should exhaust");

        assert!(err.to_string().contains("failed after 3 attempts"));
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(probe_counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_probe_fetch_with_policy_skips_missing_block_errors() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let probe_counter = AtomicU64::new(0);
        let result = retry_probe_fetch_with_policy(
            42,
            "testing probe retry",
            3,
            Duration::from_millis(0),
            true,
            &probe_counter,
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        attempts.fetch_add(1, AtomicOrdering::SeqCst);
                        Err::<Option<u64>, _>(anyhow!(
                            "rpc error: code = NotFound desc = block not found in files"
                        ))
                    }
                }
            },
        )
        .await
        .expect("missing block errors should be skippable");

        assert_eq!(result, None);
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(probe_counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_scan_forward_for_available_block_returns_first_available_candidate() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let result = scan_forward_for_available_block(10, 3, {
            let attempts = Arc::clone(&attempts);
            move |candidate| {
                let attempts = Arc::clone(&attempts);
                async move {
                    attempts.fetch_add(1, AtomicOrdering::SeqCst);
                    match candidate {
                        10 | 11 => Ok(None),
                        12 => Ok(Some(candidate * 2)),
                        _ => Ok(None),
                    }
                }
            }
        })
        .await
        .expect("scan should succeed");

        assert_eq!(result, Some((12, 24)));
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_find_timestamp_borrow_probe_skips_missing_exponential_probe() {
        #[derive(Clone, Debug)]
        struct Probe {
            timestamp: i64,
        }

        let attempts = Arc::new(AtomicUsize::new(0));
        let result = find_timestamp_borrow_probe(
            0,
            16,
            16,
            {
                let attempts = Arc::clone(&attempts);
                move |candidate, allowed_skip| {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        attempts.fetch_add(1, AtomicOrdering::SeqCst);
                        match (candidate, allowed_skip) {
                            (1..=16, _) => Ok(None),
                            (32, 16) => Ok(Some((
                                33,
                                Probe {
                                    timestamp: 1_700_000_000,
                                },
                            ))),
                            _ => Ok(None),
                        }
                    }
                }
            },
            |probe: &Probe| probe.timestamp,
        )
        .await
        .expect("timestamp scan should succeed");

        assert_eq!(
            result.map(|(block_num, probe)| (block_num, probe.timestamp)),
            Some((33, 1_700_000_000))
        );
        assert!(attempts.load(AtomicOrdering::SeqCst) >= 2);
    }

    // -- build_partitions_file_metadata tests --

    #[test]
    fn test_build_partitions_file_metadata_with_endpoint_info() {
        let ei = Some(EndpointInfo {
            chain_name: "eth-mainnet".to_string(),
            chain_name_aliases: vec!["eth".to_string(), "mainnet".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: "0xd4e56740".to_string(),
            block_id_encoding: 2,
            block_features: vec!["base".to_string(), "extended".to_string()],
        });
        let meta = build_partitions_file_metadata(
            "https://example.com",
            "eth-mainnet",
            "date",
            Compression::Zstd,
            &ei,
            None,
            true,
        );

        assert_eq!(
            find_meta(&meta, "firehose-parquet.version"),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(find_meta(&meta, "firehose-parquet.partition"), Some("date"));
        assert_eq!(
            find_meta(&meta, "firehose-parquet.endpoint"),
            Some("https://example.com")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name"),
            Some("eth-mainnet")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name_aliases"),
            Some("eth,mainnet")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_id"),
            Some("0xd4e56740")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_features"),
            Some("base,extended")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.compression"),
            Some("zstd")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_range_size"),
            Some("0")
        );
        assert_eq!(find_meta(&meta, "partition_type"), None);
    }

    #[test]
    fn test_validate_existing_partitions_params_rejects_block_range_size_change() {
        let existing_rows = vec![PartitionBuildRow {
            partition_type: "block_range".to_string(),
            partition_interval_seconds: 1_000_000,
            partition_start_ts: "0".to_string(),
            partition_value: "0".to_string(),
            start_block: 0,
            end_block: 1_000_000,
            start_time: None,
            end_time: None,
            chain: Some("solana-mainnet-beta".to_string()),
        }];

        let err = validate_existing_partitions_params(
            &existing_rows,
            "solana-mainnet-beta",
            PartitionBuildType::BlockRange,
            Some(10_000_000),
        )
        .expect_err("block range size changes should be rejected");

        assert!(err
            .to_string()
            .contains("cannot change block range size for an existing partitions file"));
    }

    #[test]
    fn test_checkpoint_partitions_rows_preserves_existing_block_range_rows() {
        let dir = std::env::temp_dir().join(format!(
            "fireparq-block-range-checkpoint-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("partitions.parquet");
        let rows = vec![
            PartitionBuildRow {
                partition_type: "block_range".to_string(),
                partition_interval_seconds: 10,
                partition_start_ts: "0".to_string(),
                partition_value: "0".to_string(),
                start_block: 0,
                end_block: 10,
                start_time: None,
                end_time: None,
                chain: Some("solana-mainnet-beta".to_string()),
            },
            PartitionBuildRow {
                partition_type: "block_range".to_string(),
                partition_interval_seconds: 10,
                partition_start_ts: "10".to_string(),
                partition_value: "10".to_string(),
                start_block: 10,
                end_block: 20,
                start_time: None,
                end_time: None,
                chain: Some("solana-mainnet-beta".to_string()),
            },
        ];
        let metadata = build_partitions_file_metadata(
            "https://example.com",
            "solana-mainnet-beta",
            "block_range",
            Compression::Zstd,
            &None,
            Some(10),
            false,
        );
        let mut checkpoint_state = PartitionsCheckpointState::default();
        let probe_counter = std::sync::atomic::AtomicU64::new(0);

        checkpoint_partitions_rows(
            &rows,
            &path.to_string_lossy(),
            Compression::Zstd,
            None,
            &metadata,
            &mut checkpoint_state,
            &probe_counter,
            true,
        )
        .expect("checkpoint rows");

        let persisted =
            read_partitions_build_rows(path.to_str().expect("utf8 path"), None).expect("read back");
        assert_eq!(persisted.len(), 2);
        assert_eq!(persisted[0].start_block, 0);
        assert_eq!(persisted[0].end_block, 10);
        assert_eq!(persisted[1].start_block, 10);
        assert_eq!(persisted[1].end_block, 20);
        assert_eq!(checkpoint_state.last_checkpoint_frontier, Some(20));
        std::fs::remove_file(&path).expect("remove parquet");
        std::fs::remove_dir(&dir).expect("remove temp dir");
    }

    #[test]
    fn test_build_partitions_file_metadata_no_endpoint_info() {
        let meta = build_partitions_file_metadata(
            "https://example.com",
            "solana-mainnet",
            "hour",
            Compression::Zstd,
            &None,
            None,
            true,
        );

        assert_eq!(
            find_meta(&meta, "firehose-parquet.version"),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(find_meta(&meta, "firehose-parquet.partition"), Some("hour"));
        assert_eq!(
            find_meta(&meta, "firehose-parquet.endpoint"),
            Some("https://example.com")
        );
        // Falls back to the chain_override value when no endpoint_info
        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name"),
            Some("solana-mainnet")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name_aliases"),
            None
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.first_streamable_block_id"),
            None
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.compression"),
            Some("zstd")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_range_size"),
            Some("0")
        );
    }

    #[test]
    fn test_build_partitions_file_metadata_chain_name_from_endpoint_info_overrides_arg() {
        // endpoint_info.chain_name takes precedence over the chain arg
        let ei = Some(EndpointInfo {
            chain_name: "polygon".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        let meta = build_partitions_file_metadata(
            "https://example.com",
            "polygon-override",
            "date",
            Compression::Zstd,
            &ei,
            None,
            true,
        );

        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name"),
            Some("polygon")
        );
    }

    #[test]
    fn test_build_partitions_file_metadata_empty_chain_name_falls_back_to_arg() {
        // When endpoint_info.chain_name is empty, fall back to the chain arg
        let ei = Some(EndpointInfo {
            chain_name: String::new(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        let meta = build_partitions_file_metadata(
            "https://example.com",
            "eth-mainnet",
            "date",
            Compression::Zstd,
            &ei,
            None,
            true,
        );

        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name"),
            Some("eth-mainnet")
        );
    }

    #[test]
    fn test_build_partitions_file_metadata_honors_requested_compression() {
        let meta = build_partitions_file_metadata(
            "https://example.com",
            "eth-mainnet",
            "date",
            Compression::Snappy,
            &None,
            None,
            true,
        );

        assert_eq!(
            find_meta(&meta, "firehose-parquet.compression"),
            Some("snappy")
        );
    }
}
