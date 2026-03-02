use anyhow::{anyhow, Result};
use clap::Parser;
use firehose_parquet::cli::{build_config, init_tracing, load_dotenv, Commands, CommonArgs};
use firehose_parquet::config::BlockMetadata;
use firehose_parquet::cursor::{save_cursor_parquet, CursorState};
use firehose_parquet::encode::{parse_encode_bytes, EncodeBytes};
use firehose_parquet::grpc::{EndpointInfo, FirehoseClient};
use firehose_parquet::metrics;
use firehose_parquet::traits::{fork_step_name, BlockIdentity, BlockMapper};
use firehose_parquet::writer::{OutputWriter, ParquetFileMetadata};
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
#[command(name = "firehose-parquet", version, about = "Convert Firehose gRPC stream to Apache Parquet", after_long_help = "\
Examples:
  # Stream EVM blocks to local Parquet (auto-detect chain)
  firehose-parquet --endpoint https://eth.firehose.pinax.network:443 \\
    --start-block 20000000 --stop-block 20001000

  # Stream Solana with date partitioning to S3
  firehose-parquet --endpoint https://solana.firehose.pinax.network:443 \\
    --start-block 250000000 --stop-block 250100000 \\
    --partition date --s3-bucket my-bucket

  # Stream with hex encoding and extended tables
  firehose-parquet --endpoint https://eth.firehose.pinax.network:443 \\
    --start-block 20000000 --bytes-encoding hex --extended

  # Resume from cursor
  firehose-parquet --endpoint https://eth.firehose.pinax.network:443 \\
    --cursor cursor.txt --partition date
")]
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
    /// Options: binary (raw bytes), hex (0x-prefixed), hex_no_prefix, base58, tron_base58, auto (chain-appropriate)
    #[arg(long, env = "BYTES_ENCODING", default_value = "auto", hide_env_values = true, help_heading = "Chain")]
    bytes_encoding: String,

    /// Include failed/reverted transactions in output (default: false)
    #[arg(long, env = "INCLUDE_FAILED_TRANSACTIONS", default_value = "false", hide_env_values = true, help_heading = "Chain")]
    include_failed_transactions: bool,
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
    meta.add("firehose-parquet.bytes_encoding", format!("{:?}", encoding).to_lowercase());
    meta.add("firehose-parquet.endpoint", endpoint);
    if let Some(ref ei) = endpoint_info {
        if !ei.chain_name.is_empty() {
            meta.add("firehose-parquet.chain_name", &ei.chain_name);
        }
        if !ei.chain_name_aliases.is_empty() {
            meta.add("firehose-parquet.chain_name_aliases", ei.chain_name_aliases.join(","));
        }
        if !ei.first_streamable_block_id.is_empty() {
            meta.add("firehose-parquet.first_streamable_block_id", &ei.first_streamable_block_id);
            // When first_streamable_block_id is present, always write
            // first_streamable_block_num (even when 0) to confirm the
            // endpoint explicitly provided genesis block info.
            meta.add("firehose-parquet.first_streamable_block_num", ei.first_streamable_block_num.to_string());
        } else if ei.first_streamable_block_num > 0 {
            meta.add("firehose-parquet.first_streamable_block_num", ei.first_streamable_block_num.to_string());
        }
        if ei.block_id_encoding > 0 {
            meta.add("firehose-parquet.block_id_encoding", block_id_encoding_label(ei.block_id_encoding));
        }
        if !ei.block_features.is_empty() {
            meta.add("firehose-parquet.block_features", ei.block_features.join(","));
        }
    }
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
    include_failed_transactions: bool,
) -> Result<Box<dyn BlockMapper>> {
    match block_type {
        "evm" => Ok(Box::new(EvmBlockMapper::new(extended, include_fork_step, encode_bytes, include_failed_transactions))),
        "bitcoin" => Ok(Box::new(BitcoinBlockMapper::new(include_fork_step, encode_bytes.clone()))),
        "solana" => Ok(Box::new(SolanaBlockMapper::new(extended, include_fork_step, encode_bytes, include_failed_transactions))),
        "near" => Ok(Box::new(NearBlockMapper::new(include_fork_step, encode_bytes, include_failed_transactions))),
        "antelope" => Ok(Box::new(AntelopeBlockMapper::new(extended, include_fork_step, encode_bytes, include_failed_transactions))),
        "cosmos" => Ok(Box::new(CosmosBlockMapper::new(include_fork_step, encode_bytes, include_failed_transactions))),
        "tron" => Ok(Box::new(TronBlockMapper::new(include_fork_step, encode_bytes, include_failed_transactions))),
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
                path, cross_partition, allow_gaps,
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
                source, output, target_partition, compression, flush_bytes, delete_source,
                aws_access_key_id, aws_secret_access_key, aws_session_token,
                aws_region, aws_endpoint_url, cache_control,
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
                    cache_control: cache_control.clone(),
                };
                firehose_parquet::rollup::run_rollup(&rollup_config)?;
                return Ok(());
            }
            Commands::Merge {
                path, compression, flush_bytes, dry_run,
                aws_access_key_id, aws_secret_access_key, aws_session_token,
                aws_region, aws_endpoint_url, cache_control,
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
                path, partition, dry_run,
                aws_access_key_id, aws_secret_access_key, aws_session_token,
                aws_region, aws_endpoint_url,
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

    info!(version = env!("CARGO_PKG_VERSION"), "firehose-parquet starting");

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
            ("final_blocks_only".to_string(), config.final_blocks_only.to_string()),
            ("version".to_string(), env!("CARGO_PKG_VERSION").to_string()),
        ];
        if let Some(ref ei) = endpoint_info {
            labels.push(("chain_name".to_string(), ei.chain_name.clone()));
            if !ei.chain_name_aliases.is_empty() {
                labels.push(("chain_name_aliases".to_string(), ei.chain_name_aliases.join(",")));
            }
            labels.push(("first_streamable_block_num".to_string(), ei.first_streamable_block_num.to_string()));
            if !ei.first_streamable_block_id.is_empty() {
                labels.push(("first_streamable_block_id".to_string(), ei.first_streamable_block_id.clone()));
            }
            if ei.block_id_encoding > 0 {
                labels.push(("block_id_encoding".to_string(), block_id_encoding_label(ei.block_id_encoding).to_string()));
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
        OutputWriter::new(&config.output, config.partition.clone(), config.compression, flush_bytes)
    };

    // Pass metrics to the writer for file/byte/row tracking.
    writer.set_metrics(pipeline_metrics.clone());

    // If block type is known upfront, resolve encode_bytes and create mapper immediately.
    // If "auto", defer until first block arrives.
    let mut mapper: Option<Box<dyn BlockMapper>> = if block_type != "auto" {
        let encode_bytes = parse_encode_bytes(&bytes_encoding_str)
            .or_else(|| endpoint_info.as_ref().and_then(|ei| encode_bytes_from_block_id_encoding(ei.block_id_encoding)))
            .unwrap_or_else(|| default_encode_bytes(&block_type));
        let meta = build_file_metadata(&block_type, &encode_bytes, &config.endpoint, &endpoint_info);
        log_file_metadata(&meta);
        writer.inner.set_file_metadata(meta);
        Some(create_mapper(&block_type, extended, include_fork_step, encode_bytes, include_failed_transactions)?)
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
    let mut last_block_num: u64 = 0;
    let mut last_block_id: String = String::new();
    let mut bytes_read: u64 = 0;
    let progress_start = Instant::now();
    let mut current_partition_key: Option<String> = None;
    let partition_config = config.partition.clone();

    // Build a template CursorState with pipeline parameters that stay constant.
    let cursor_state_template = CursorState {
        endpoint: config.endpoint.clone(),
        start_block: config.start_block,
        stop_block: config.stop_block,
        partition: config.partition.to_string(),
        block_range_size: match &config.partition {
            firehose_parquet::config::Partition::BlockRange(size) => *size,
            _ => 0,
        },
        compression: config.compression.to_string(),
        flush_bytes: config.flush_bytes,
        flush_rows: config.flush_rows.map(|r| r as u64),
        bytes_encoding: bytes_encoding_str.clone(),
        extended,
        final_blocks_only: config.final_blocks_only,
        chain_name: endpoint_info.as_ref().map_or_else(String::new, |ei| ei.chain_name.clone()),
        chain_name_aliases: endpoint_info.as_ref().map_or_else(String::new, |ei| ei.chain_name_aliases.join(",")),
        first_streamable_block_num: endpoint_info.as_ref().map_or(0, |ei| ei.first_streamable_block_num),
        first_streamable_block_id: endpoint_info.as_ref().map_or_else(String::new, |ei| ei.first_streamable_block_id.clone()),
        block_id_encoding: endpoint_info.as_ref().map_or_else(String::new, |ei| ei.block_id_encoding.to_string()),
        block_features: endpoint_info.as_ref().map_or_else(String::new, |ei| ei.block_features.join(",")),
        firehose_parquet_version: env!("CARGO_PKG_VERSION").to_string(),
        ..CursorState::default()
    };

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
                            if let (Some(ref pq_path), Some(ref cursor)) = (&cursor_path, &last_cursor) {
                                let mut state = cursor_state_template.clone();
                                state.cursor = cursor.clone();
                                state.last_block_num = last_block_num;
                                state.last_block_id = last_block_id.clone();
                                state.updated_at = time::OffsetDateTime::now_utc()
                                    .format(&time::format_description::well_known::Rfc3339)
                                    .unwrap_or_default();
                                if let Err(e) = save_cursor_parquet(pq_path, &state) {
                                    warn!(error = %e, path = %pq_path.display(), "failed to save cursor.parquet");
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
                        if let (Some(ref pq_path), Some(ref cursor)) = (&cursor_path, &last_cursor) {
                            let mut state = cursor_state_template.clone();
                            state.cursor = cursor.clone();
                            state.last_block_num = last_block_num;
                            state.last_block_id = last_block_id.clone();
                            state.updated_at = time::OffsetDateTime::now_utc()
                                .format(&time::format_description::well_known::Rfc3339)
                                .unwrap_or_default();
                            if let Err(e) = save_cursor_parquet(pq_path, &state) {
                                warn!(error = %e, path = %pq_path.display(), "failed to save cursor.parquet");
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
                pipeline_metrics.flushes_total.get_or_create(&metrics::FlushLabels { trigger: "shutdown".to_string() }).inc();
                if let (Some(ref pq_path), Some(ref cursor)) = (&cursor_path, &last_cursor) {
                    let mut state = cursor_state_template.clone();
                    state.cursor = cursor.clone();
                    state.last_block_num = last_block_num;
                    state.last_block_id = last_block_id.clone();
                    state.updated_at = time::OffsetDateTime::now_utc()
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_default();
                    if let Err(e) = save_cursor_parquet(pq_path, &state) {
                        warn!(error = %e, path = %pq_path.display(), "failed to save cursor.parquet");
                        pipeline_metrics.errors_total.get_or_create(&metrics::ErrorLabels { kind: "cursor_save".to_string() }).inc();
                    } else {
                        pipeline_metrics.cursor_saves_total.inc();
                        pipeline_metrics.cursor_last_block_num.set(last_block_num as i64);
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
            let mapper = create_mapper(block_type, false, false, encode_bytes, false);
            assert!(mapper.is_ok(), "create_mapper failed for block_type: {block_type}");
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
        meta.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
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

        assert_eq!(find_meta(&meta, "firehose-parquet.chain_name"), Some("matic"));
        assert_eq!(find_meta(&meta, "firehose-parquet.chain_name_aliases"), Some("polygon,matic"));
        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_id"), Some("0xabc"));
        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_num"), Some("100"));
        assert_eq!(find_meta(&meta, "firehose-parquet.block_id_encoding"), Some("hex_0x"));
        assert_eq!(find_meta(&meta, "firehose-parquet.block_features"), Some("base,extended"));
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

        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_id"), Some("0xd4e56740"));
        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_num"), Some("0"));
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

        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_id"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_num"), None);
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

        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_id"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_num"), Some("42"));
    }

    #[test]
    fn test_build_file_metadata_no_endpoint_info() {
        let meta = build_file_metadata("evm", &EncodeBytes::Hex, "https://example.com", &None);

        assert_eq!(find_meta(&meta, "firehose-parquet.chain_name"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.chain_name_aliases"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_id"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_num"), None);
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
        assert_eq!(find_meta(&meta, "firehose-parquet.chain_name_aliases"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_id"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.first_streamable_block_num"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.block_id_encoding"), None);
        assert_eq!(find_meta(&meta, "firehose-parquet.block_features"), None);
    }
}
