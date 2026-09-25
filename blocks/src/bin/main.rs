use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use clap::{Args, Parser};
use firehose_parquet::cli::{
    build_config, build_partitions_index_path, build_partitions_output_root, init_tracing,
    list_partitions_from_index, load_dotenv, parse_partition_build_types,
    parse_partition_shard_strategy, read_verified_partitions_index, resolve_cursor_template,
    resolve_partition_command, resolve_s3_output_root, shard_partitions_from_index,
    validate_partitions_index, validate_s3_output_credentials, write_verified_partitions_index,
    AwsConfig, BuildArgs, Commands, CursorTemplateContext, PartitionBoundsRequest,
    PartitionBuildResult, PartitionBuildRow, PartitionBuildType, PartitionListRequest,
    PartitionResolveOptions, PartitionShardRequest, PartitionValidateRequest, PartitionsCommands,
};
use firehose_parquet::config::{BlockMetadata, Compression, Config, Partition};
use firehose_parquet::cursor::{CursorLocation, CursorState};
use firehose_parquet::dataset_lock::{DatasetOwnership, MutationScope};
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::flush::{FlushSizing, MapperBufferEstimate, SizeFlushTrigger};
use firehose_parquet::grpc::{
    classify_fetch_error, is_shutdown_error, unless_shutdown, CancellationToken, EndpointInfo,
    FetchErrorKind, FirehoseClient, ShutdownRequested,
};
use firehose_parquet::ingest::{
    declare_inventory, load_authoritative_resume, prepare_partitions_index_write, BlockFamily,
    IngestionSession, MapperSemantics,
};
use firehose_parquet::metrics;
use firehose_parquet::networks::{resolve_network_endpoint, EndpointSource};
use firehose_parquet::partition_index::{
    append_verified_extension, scan_time_index, IndexRoutingPolicy, PartitionCoverage,
    PartitionSpanProof, RoutingWitness, VerifiedPartitionIndex, VerifiedPartitionSpan,
    INDEX_FORMAT_VERSION,
};
use firehose_parquet::traits::{fork_step_name, BlockIdentity, BlockMapper};
#[cfg(test)]
use firehose_parquet::writer::OutputWriter;
use firehose_parquet::writer::{ParquetFileMetadata, WriterBufferStats};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use blocks::antelope::mapper::AntelopeBlockMapper;
use blocks::beacon::mapper::BeaconBlockMapper;
use blocks::bitcoin::mapper::BitcoinBlockMapper;
use blocks::cosmos::mapper::CosmosBlockMapper;
use blocks::evm::mapper::EvmBlockMapper;
use blocks::near::mapper::NearBlockMapper;
use blocks::solana::mapper::SolanaBlockMapper;
use blocks::tron::mapper::TronBlockMapper;

fn print_partition_coverage(coverage: Option<&PartitionCoverage>) {
    match coverage {
        Some(coverage) => {
            println!(
                "coverage:         [{}, {}) (finalized snapshot)",
                coverage.start_block, coverage.stop_block
            );
            println!("finalized_block:  {}", coverage.finalized.block_num);
            println!("finalized_id:     {}", coverage.finalized.block_id);
            println!("routing_policy:   {:?}", coverage.routing_policy);
        }
        None => println!("coverage:         unknown (legacy index; rebuild before resolving)"),
    }
}

fn partition_completeness(complete: Option<bool>) -> &'static str {
    match complete {
        Some(true) => "complete",
        Some(false) => "incomplete",
        None => "unknown",
    }
}

/// Supported block types.
const BLOCK_TYPES: &[&str] = &[
    "auto", "evm", "bitcoin", "solana", "near", "antelope", "cosmos", "tron", "beacon",
];
const DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES: u64 = 134_217_728;
const WITHOUT_EXTENDED_WARNING: &str =
    "--without-extended had no effect because extended output is not supported for this chain";
const WITHOUT_VOTES_NON_SOLANA_WARNING: &str =
    "--without-votes had no effect because vote_transactions are only available for Solana";

#[derive(Parser, Debug)]
#[command(
    name = "fireparq",
    version,
    subcommand_required = true,
    arg_required_else_help = true,
    about = "Build Apache Parquet datasets from Firehose gRPC streams",
    after_long_help = "\
Primary workflow:
  Use `fireparq build` to run the ingestion pipeline.
  Utility workflows live under subcommands such as `partitions`, `scan`, `inspect`, `validate`, and `verify`.

Examples:
  # Run a bounded historical ingestion
  fireparq build --network mainnet \\
    --start-block 20000000 --stop-block 20001000

  # Backfill from a block and keep following finalized blocks
  fireparq build --network solana-mainnet-beta \\
    --start-block 250000000

  # Resume from cursor.parquet, or fall back to the endpoint's
  # first streamable block when no cursor exists
  fireparq build --network mainnet

  # See all build options
  fireparq build --help
"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    global: GlobalArgs,
}

#[derive(Args, Debug)]
struct GlobalArgs {
    /// Log level: trace, debug, info, warn, error
    #[arg(
        long,
        env = "LOG_LEVEL",
        default_value = "info",
        hide_env_values = true,
        global = true,
        help_heading = "Runtime / Logging"
    )]
    log_level: String,

    /// Enable verbose operational logs for debugging without making normal runs noisy
    #[arg(
        long,
        env = "VERBOSE",
        default_value = "false",
        hide_env_values = true,
        global = true,
        help_heading = "Runtime / Logging"
    )]
    verbose: bool,
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

fn encode_bytes_label(encoding: &EncodeBytes) -> &'static str {
    match encoding {
        EncodeBytes::Binary => "binary",
        EncodeBytes::Hex => "hex",
        EncodeBytes::HexNoPrefix => "hex_no_prefix",
        EncodeBytes::Base58 => "base58",
        EncodeBytes::TronBase58 => "tron_base58",
    }
}

#[derive(Debug, Clone, Copy)]
struct WriterFlushOutcome {
    materialized: bool,
    buffered: WriterBufferStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MapperFlushTrigger {
    Memory,
    Bytes,
    Blocks,
    Rows,
    Interval,
}

impl MapperFlushTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Bytes => "bytes",
            Self::Blocks => "blocks",
            Self::Rows => "rows",
            Self::Interval => "interval",
        }
    }
}

fn next_mapper_flush_trigger(
    flush_rows: Option<usize>,
    max_table_rows: usize,
    flush_blocks: Option<u64>,
    blocks_since_flush: u64,
    flush_interval_secs: Option<u64>,
    last_flush_time: Instant,
    sizing: &FlushSizing,
    estimate: MapperBufferEstimate,
) -> Option<MapperFlushTrigger> {
    let time_to_flush = flush_interval_secs
        .map(|secs| last_flush_time.elapsed().as_secs() >= secs)
        .unwrap_or(false);

    let rows_to_flush = flush_rows
        .map(|limit| max_table_rows >= limit)
        .unwrap_or(false);

    let blocks_to_flush = flush_blocks
        .map(|limit| blocks_since_flush >= limit)
        .unwrap_or(false);

    if let Some(trigger) = sizing.trigger(estimate) {
        Some(match trigger {
            SizeFlushTrigger::Memory => MapperFlushTrigger::Memory,
            SizeFlushTrigger::Bytes => MapperFlushTrigger::Bytes,
        })
    } else if blocks_to_flush {
        Some(MapperFlushTrigger::Blocks)
    } else if rows_to_flush {
        Some(MapperFlushTrigger::Rows)
    } else if time_to_flush {
        Some(MapperFlushTrigger::Interval)
    } else {
        None
    }
}

#[cfg(test)]
fn write_mapper_flush(
    writer: &mut OutputWriter,
    batches: &HashMap<String, RecordBatch>,
    metadata: &BlockMetadata,
) -> Result<WriterFlushOutcome> {
    let materialized = writer.write_all(batches, metadata)?;

    Ok(WriterFlushOutcome {
        materialized,
        buffered: writer.buffered_stats(),
    })
}

fn log_writer_flush_outcome(
    trigger: &str,
    tables: usize,
    rows: usize,
    outcome: WriterFlushOutcome,
) {
    if outcome.materialized {
        info!(
            trigger,
            tables,
            rows,
            buffered_tables = outcome.buffered.tables,
            buffered_rows = outcome.buffered.rows,
            buffered_estimated_bytes =
                firehose_parquet::cli::format_bytes(outcome.buffered.estimated_compressed_bytes),
            "writer materialized parquet output for mapper flush"
        );
    } else {
        info!(
            trigger,
            tables,
            rows,
            buffered_tables = outcome.buffered.tables,
            buffered_rows = outcome.buffered.rows,
            buffered_estimated_bytes =
                firehose_parquet::cli::format_bytes(outcome.buffered.estimated_compressed_bytes),
            "mapper flush contained no nonempty table output"
        );
    }
}

/// How the Firehose stream ended, which decides whether buffered output may be
/// materialized and the cursor committed on exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamExit {
    /// The stream ended cleanly (stop block reached or server closed it).
    Completed,
    /// SIGINT/SIGTERM was received.
    Shutdown,
    /// A mapper, writer or stream error ended the run.
    Failed,
}

/// SIGINT (Ctrl-C) and, on Unix, SIGTERM.
struct ShutdownSignals {
    #[cfg(unix)]
    sigterm: tokio::signal::unix::Signal,
    #[cfg(unix)]
    sigint: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn install() -> Self {
        Self {
            #[cfg(unix)]
            sigterm: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler"),
            #[cfg(unix)]
            sigint: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .expect("failed to install SIGINT handler"),
        }
    }

    /// Wait for the next shutdown signal.
    async fn next(&mut self) {
        #[cfg(unix)]
        tokio::select! {
            _ = self.sigint.recv() => {}
            _ = self.sigterm.recv() => {}
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }
    }
}

fn spawn_ingestion_shutdown_handler(shutdown: CancellationToken, cursor_shutdown: Arc<AtomicBool>) {
    // Install both Unix signals before yielding to another task. Otherwise an
    // early SIGINT could arrive before a lazily polled ctrl_c future registers.
    let mut signals = ShutdownSignals::install();
    tokio::spawn(async move {
        signals.next().await;
        info!("shutdown signal received, stopping after the current block (send it again to exit immediately)");
        cursor_shutdown.store(true, Ordering::SeqCst);
        shutdown.cancel();

        signals.next().await;
        warn!("second shutdown signal received, exiting immediately; in-flight writes may be interrupted");
        std::process::exit(130);
    });
}

impl StreamExit {
    fn from_result(result: &Result<()>) -> Self {
        match result {
            Ok(()) => Self::Completed,
            Err(e) if is_shutdown_error(e) => Self::Shutdown,
            Err(_) => Self::Failed,
        }
    }

    /// Only a completed stream flushes partial buffers and commits the cursor.
    /// After a shutdown or a failure the buffers are discarded and the cursor
    /// stays at the last committed flush, so the next run replays that window.
    /// Flushing after a failure could save the cursor past rows whose write
    /// had failed.
    fn materializes_buffers(self) -> bool {
        matches!(self, Self::Completed)
    }
}

/// Final writer flush when the stream ends. Remaining buffers are written, and
/// `commit_cursor` runs, only when the stream completed and either its final
/// mapper write or this remaining-buffer flush materialized data. Earlier
/// normal-loop writes have already been checkpointed and do not count here.
/// Returns whether the cursor commit ran.
#[cfg(test)]
fn flush_writer_on_exit(
    exit: StreamExit,
    writer: &mut OutputWriter,
    final_mapper_materialized: bool,
    pipeline_metrics: &metrics::PipelineMetrics,
    commit_cursor: impl FnOnce() -> Result<()>,
) -> Result<bool> {
    if !exit.materializes_buffers() {
        return Ok(false);
    }

    // Always drain before committing, even if a final write already materialized
    // data. Any retained table write must succeed before the cursor can advance.
    let wrote_remaining = writer.flush_remaining()?;
    if !final_mapper_materialized && !wrote_remaining {
        info!(
            trigger = "shutdown",
            "no writer-buffered parquet data remained to materialize before exit"
        );
        return Ok(false);
    }

    let writer_buffered = writer.buffered_stats();
    info!(
        trigger = "shutdown",
        buffered_tables = writer_buffered.tables,
        buffered_rows = writer_buffered.rows,
        buffered_estimated_bytes =
            firehose_parquet::cli::format_bytes(writer_buffered.estimated_compressed_bytes),
        "writer materialized final parquet output before exit"
    );
    pipeline_metrics
        .flushes_total
        .get_or_create(&metrics::FlushLabels {
            trigger: "shutdown".to_string(),
        })
        .inc();
    commit_cursor()?;
    Ok(true)
}

fn output_block_id_encoding_label(encoding: &EncodeBytes) -> Option<&'static str> {
    match encoding {
        EncodeBytes::Binary => None,
        EncodeBytes::Hex => Some("hex_0x"),
        EncodeBytes::HexNoPrefix => Some("hex_no_prefix"),
        EncodeBytes::Base58 => Some("base58"),
        EncodeBytes::TronBase58 => Some("hex_no_prefix"),
    }
}

fn is_tron_style_chain_name(chain_name: &str) -> bool {
    chain_name.eq_ignore_ascii_case("tron") || chain_name.eq_ignore_ascii_case("tron-evm")
}

fn endpoint_uses_tron_style_evm_profile(endpoint_info: &Option<EndpointInfo>) -> bool {
    endpoint_info.as_ref().map_or(false, |ei| {
        is_tron_style_chain_name(&ei.chain_name)
            || ei
                .chain_name_aliases
                .iter()
                .any(|alias| is_tron_style_chain_name(alias))
    })
}

fn chain_uses_tron_style_evm_profile(chain: &str, endpoint_info: &Option<EndpointInfo>) -> bool {
    is_tron_style_chain_name(chain) || endpoint_uses_tron_style_evm_profile(endpoint_info)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OutputEncodingPolicy {
    bytes_encoding: EncodeBytes,
    block_id_encoding: &'static str,
    allow_endpoint_block_id_hint: bool,
}

fn output_encoding_policy(
    block_type: &str,
    tron_style_evm_profile: bool,
) -> Option<OutputEncodingPolicy> {
    match block_type {
        "evm" if tron_style_evm_profile => Some(OutputEncodingPolicy {
            bytes_encoding: EncodeBytes::TronBase58,
            block_id_encoding: "hex_no_prefix",
            allow_endpoint_block_id_hint: false,
        }),
        "evm" | "bitcoin" | "cosmos" | "beacon" => Some(OutputEncodingPolicy {
            bytes_encoding: EncodeBytes::Hex,
            block_id_encoding: "hex_0x",
            allow_endpoint_block_id_hint: false,
        }),
        "antelope" => Some(OutputEncodingPolicy {
            bytes_encoding: EncodeBytes::HexNoPrefix,
            block_id_encoding: "hex_no_prefix",
            allow_endpoint_block_id_hint: false,
        }),
        "solana" | "near" => Some(OutputEncodingPolicy {
            bytes_encoding: EncodeBytes::Base58,
            block_id_encoding: "base58",
            allow_endpoint_block_id_hint: false,
        }),
        "tron" => Some(OutputEncodingPolicy {
            bytes_encoding: EncodeBytes::TronBase58,
            block_id_encoding: "hex_no_prefix",
            allow_endpoint_block_id_hint: false,
        }),
        _ => None,
    }
}

fn resolve_auto_encode_bytes(
    block_type: Option<&str>,
    endpoint_info: &Option<EndpointInfo>,
    tron_style_evm_profile: bool,
) -> EncodeBytes {
    if let Some(block_type) = block_type {
        if let Some(policy) = output_encoding_policy(block_type, tron_style_evm_profile) {
            if policy.allow_endpoint_block_id_hint {
                return endpoint_info
                    .as_ref()
                    .and_then(|ei| encode_bytes_from_block_id_encoding(ei.block_id_encoding))
                    .unwrap_or(policy.bytes_encoding);
            }

            return policy.bytes_encoding;
        }
    }

    endpoint_info
        .as_ref()
        .and_then(|ei| encode_bytes_from_block_id_encoding(ei.block_id_encoding))
        .unwrap_or(EncodeBytes::Hex)
}

fn inferred_block_type_from_endpoint_info(
    endpoint_info: &Option<EndpointInfo>,
) -> Option<&'static str> {
    let info = endpoint_info.as_ref()?;

    if !info.chain_name.is_empty() {
        return infer_partitions_block_type(&info.chain_name, endpoint_info);
    }

    info.chain_name_aliases
        .iter()
        .find_map(|alias| infer_partitions_block_type(alias, endpoint_info))
}

fn add_common_file_metadata(
    meta: &mut ParquetFileMetadata,
    block_type: Option<&str>,
    encoding: Option<&EncodeBytes>,
    endpoint: &str,
    endpoint_info: &Option<EndpointInfo>,
) {
    meta.add("firehose-parquet.version", env!("CARGO_PKG_VERSION"));
    if let Some(block_type) = block_type {
        meta.add("firehose-parquet.block_type", block_type);
    }
    if let Some(encoding) = encoding {
        meta.add(
            "firehose-parquet.bytes_encoding",
            encode_bytes_label(encoding),
        );
    }
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
        if !ei.block_features.is_empty() {
            meta.add(
                "firehose-parquet.block_features",
                ei.block_features.join(","),
            );
        }
    }
    if let Some(encoding) = encoding {
        if let Some(block_id_encoding) = output_block_id_encoding_label(encoding) {
            meta.add("firehose-parquet.block_id_encoding", block_id_encoding);
        }
    } else if let Some(ref ei) = endpoint_info {
        if ei.block_id_encoding > 0 {
            meta.add(
                "firehose-parquet.block_id_encoding",
                block_id_encoding_label(ei.block_id_encoding),
            );
        }
    }
}

fn build_file_metadata(
    block_type: &str,
    encoding: &firehose_parquet::encode::EncodeBytes,
    endpoint: &str,
    compression: Compression,
    endpoint_info: &Option<EndpointInfo>,
) -> ParquetFileMetadata {
    let mut meta = ParquetFileMetadata::new();
    add_common_file_metadata(
        &mut meta,
        Some(block_type),
        Some(encoding),
        endpoint,
        endpoint_info,
    );
    meta.add("firehose-parquet.compression", compression.to_string());
    meta
}

fn maybe_add_synthetic_timestamp_metadata(
    meta: &mut ParquetFileMetadata,
    block_type: &str,
    synthetic_partition_routing: bool,
) {
    if block_type_has_nullable_timestamps(block_type) && synthetic_partition_routing {
        meta.add("firehose-parquet.synthetic_timestamps", "true");
        meta.add(
            "firehose-parquet.synthetic_timestamp_policy",
            "last_known_partition_routing",
        );
    }
}

/// Resolve whether failed/reverted transactions are written (#494).
///
/// - `--exclude-failed-transactions` always drops them.
/// - EVM writes them by default, with only their persistent state changes.
///   `--include-failed-transactions` is a deprecated no-op there. A resumed EVM
///   cursor that was written with failed transactions excluded (the default
///   before #494) keeps excluding them, so one output does not mix both modes;
///   `--cursor-override` opts out of that.
/// - Other chains exclude them unless `--include-failed-transactions` is set.
///
/// Returns the effective value and the warnings to log.
fn resolve_include_failed_transactions(
    block_type: Option<&str>,
    include_flag: bool,
    exclude_flag: bool,
    cursor_state: Option<&CursorState>,
    cursor_override: bool,
) -> (bool, Vec<String>) {
    let mut warnings = Vec::new();
    if exclude_flag {
        if include_flag {
            warnings.push(
                "--include-failed-transactions is ignored because --exclude-failed-transactions is set"
                    .to_string(),
            );
        }
        return (false, warnings);
    }
    if block_type != Some("evm") {
        return (include_flag, warnings);
    }
    if include_flag {
        warnings.push(
            "--include-failed-transactions is deprecated and has no effect on EVM: failed transactions are included by default; use --exclude-failed-transactions to drop them"
                .to_string(),
        );
    }
    if let Some(cursor_state) = cursor_state.filter(|_| !cursor_override) {
        if !cursor_state.include_failed_transactions {
            warnings.push(
                "cursor.parquet was written with failed transactions excluded (the EVM default before #494); still excluding them so this output stays consistent. Pass --exclude-failed-transactions to keep this and silence the warning, or --cursor-override with --start-block to switch this output to the new default"
                    .to_string(),
            );
            return (false, warnings);
        }
    }
    (true, warnings)
}

fn add_cursor_compatibility_metadata(
    meta: &mut ParquetFileMetadata,
    extended: bool,
    final_blocks_only: bool,
    include_failed_transactions: bool,
) {
    meta.add("firehose-parquet.extended", extended.to_string());
    meta.add(
        "firehose-parquet.final_blocks_only",
        final_blocks_only.to_string(),
    );
    meta.add(
        "firehose-parquet.include_failed_transactions",
        include_failed_transactions.to_string(),
    );
}

fn build_cursor_file_metadata(
    block_type: Option<&str>,
    encoding: Option<&EncodeBytes>,
    endpoint: &str,
    compression: Compression,
    partition: &firehose_parquet::config::Partition,
    endpoint_info: &Option<EndpointInfo>,
    extended: bool,
    final_blocks_only: bool,
    include_failed_transactions: bool,
) -> ParquetFileMetadata {
    let mut meta = ParquetFileMetadata::new();
    add_common_file_metadata(&mut meta, block_type, encoding, endpoint, endpoint_info);
    meta.add("firehose-parquet.compression", compression.to_string());
    meta.add("firehose-parquet.partition", partition.to_string());
    meta.add(
        "firehose-parquet.block_range_size",
        match partition {
            firehose_parquet::config::Partition::BlockRange { size, .. } => size.to_string(),
            _ => "0".to_string(),
        },
    );
    add_cursor_compatibility_metadata(
        &mut meta,
        extended,
        final_blocks_only,
        include_failed_transactions,
    );
    meta
}

fn partition_requires_timestamp(partition: &Partition) -> bool {
    matches!(
        partition,
        Partition::Date | Partition::Hour | Partition::Minute | Partition::Second
    )
}

fn block_type_has_nullable_timestamps(block_type: &str) -> bool {
    block_type == "solana"
}

use firehose_parquet::partition_index::SOLANA_GENESIS_TIMESTAMP;

fn use_last_known_timestamp_partition_routing(block_type: &str, partition: &Partition) -> bool {
    block_type_has_nullable_timestamps(block_type) && partition_requires_timestamp(partition)
}

fn validate_block_timestamp(block_num: u64, timestamp: i64, partition: &Partition) -> Result<()> {
    firehose_parquet::traits::checked_timestamp(timestamp)?;
    if timestamp != 0 {
        return Ok(());
    }

    if partition_requires_timestamp(partition) {
        return Err(anyhow!(
            "block {block_num} is missing timestamp metadata; time-based partitioning requires timestamps"
        ));
    }

    Err(anyhow!("block {block_num} is missing timestamp metadata"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenesisTimestampBootstrapAction {
    None,
    Buffer,
    Anchored {
        anchor_block: u64,
        buffered_blocks: u64,
        first_buffered_block: u64,
    },
}

#[derive(Debug, Clone)]
struct GenesisTimestampBootstrap {
    enabled: bool,
    requested_start_block: Option<u64>,
    first_buffered_block: Option<u64>,
    buffered_blocks: u64,
}

#[derive(Debug, Clone)]
struct BufferedBootstrapBlock {
    received_ordinal: u64,
    block_bytes: Vec<u8>,
    cursor: String,
    fork_step: Option<String>,
    identity: BlockIdentity,
}

impl GenesisTimestampBootstrap {
    fn new(requested_start_block: Option<u64>) -> Self {
        Self {
            enabled: true,
            requested_start_block,
            first_buffered_block: None,
            buffered_blocks: 0,
        }
    }

    fn observe_block(
        &mut self,
        blocks_processed: u64,
        block_number: u64,
        timestamp: i64,
    ) -> GenesisTimestampBootstrapAction {
        if !self.enabled {
            return GenesisTimestampBootstrapAction::None;
        }

        if blocks_processed > 0 {
            self.enabled = false;
            return GenesisTimestampBootstrapAction::None;
        }

        if let Some(first_buffered_block) = self.first_buffered_block {
            if timestamp == 0 {
                self.buffered_blocks += 1;
                return GenesisTimestampBootstrapAction::Buffer;
            }

            self.enabled = false;
            return GenesisTimestampBootstrapAction::Anchored {
                anchor_block: block_number,
                buffered_blocks: self.buffered_blocks,
                first_buffered_block,
            };
        }

        if self.requested_start_block != Some(block_number) || timestamp != 0 {
            self.enabled = false;
            return GenesisTimestampBootstrapAction::None;
        }

        self.first_buffered_block = Some(block_number);
        self.buffered_blocks = 1;
        GenesisTimestampBootstrapAction::Buffer
    }
}

fn missing_genesis_timestamp_bootstrap_error(
    first_buffered_block: u64,
    buffered_blocks: u64,
) -> anyhow::Error {
    anyhow!(
        "buffered {buffered_blocks} timestamp-less bootstrap block(s) starting at block {first_buffered_block}, but no later block with timestamp metadata was found before the stream ended"
    )
}

fn take_anchored_bootstrap_blocks(
    buffered_blocks: &mut Vec<BufferedBootstrapBlock>,
    anchor_timestamp: i64,
) -> Vec<BufferedBootstrapBlock> {
    buffered_blocks
        .drain(..)
        .map(|mut block| {
            block.identity.timestamp = anchor_timestamp;
            block
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimestampAnchor {
    block_num: u64,
    timestamp: i64,
}

#[derive(Debug, Clone, Default)]
struct TimestampBackfill {
    enabled: bool,
    last_anchor: Option<TimestampAnchor>,
}

impl TimestampBackfill {
    fn new(enabled: bool, _max_buffered_bytes: u64) -> Self {
        Self {
            enabled,
            last_anchor: enabled.then_some(TimestampAnchor {
                block_num: 0,
                timestamp: SOLANA_GENESIS_TIMESTAMP,
            }),
        }
    }

    fn restore_anchor(&mut self, block_num: u64, timestamp: i64) {
        if self.enabled {
            self.last_anchor = Some(TimestampAnchor {
                block_num,
                timestamp,
            });
        }
    }

    fn buffered_blocks_len(&self) -> usize {
        0
    }

    fn buffered_bytes(&self) -> u64 {
        0
    }

    fn observe_block(
        &mut self,
        block_bytes: Vec<u8>,
        cursor: String,
        fork_step: Option<String>,
        identity: BlockIdentity,
    ) -> anyhow::Result<Vec<BufferedBootstrapBlock>> {
        let current = BufferedBootstrapBlock {
            received_ordinal: 0,
            block_bytes,
            cursor,
            fork_step,
            identity,
        };

        if !self.enabled {
            if current.identity.timestamp != 0 {
                self.last_anchor = Some(TimestampAnchor {
                    block_num: current.identity.block_num,
                    timestamp: current.identity.timestamp,
                });
            }
            return Ok(vec![current]);
        }

        if current.identity.timestamp == 0 {
            let mut current = current;
            let anchor = self.last_anchor.ok_or_else(|| {
                anyhow!(
                    "nullable-timestamp partition routing requires a last-known timestamp anchor"
                )
            })?;
            current.identity.timestamp = anchor.timestamp;
            return Ok(vec![current]);
        }

        let anchor = TimestampAnchor {
            block_num: current.identity.block_num,
            timestamp: current.identity.timestamp,
        };
        self.last_anchor = Some(anchor);
        Ok(vec![current])
    }

    fn drain_open_span(&mut self) -> anyhow::Result<Vec<BufferedBootstrapBlock>> {
        Ok(Vec::new())
    }
}

fn restore_sparse_routing_cursor_anchor(
    timestamp_backfill: &mut TimestampBackfill,
    cursor_state: Option<&CursorState>,
    cursor_override: bool,
    block_type: &str,
    partition: &Partition,
) {
    if cursor_override || !use_last_known_timestamp_partition_routing(block_type, partition) {
        return;
    }

    let Some(cursor_state) = cursor_state else {
        return;
    };

    if let Some(last_timestamp) = cursor_state.last_timestamp {
        timestamp_backfill.restore_anchor(cursor_state.last_block_num, last_timestamp);
        info!(
            stored_cursor_last_block_num = cursor_state.last_block_num,
            last_timestamp, "restored sparse-routing timestamp anchor from cursor.parquet"
        );
    } else {
        warn!(
            stored_cursor_last_block_num = cursor_state.last_block_num,
            "legacy cursor.parquet is missing last_timestamp; resume timestamp anchoring may be less precise until a new timestamped block arrives"
        );
    }
}

fn update_bootstrap_buffer_metrics(
    pipeline_metrics: &metrics::PipelineMetrics,
    buffered: &[BufferedBootstrapBlock],
) {
    pipeline_metrics
        .bootstrap_buffered_bytes
        .set(buffered.iter().fold(0_i64, |sum, block| {
            sum.saturating_add(i64::try_from(block.block_bytes.len()).unwrap_or(i64::MAX))
        }));
    pipeline_metrics
        .bootstrap_buffered_blocks
        .set(i64::try_from(buffered.len()).unwrap_or(i64::MAX));
}

fn update_mapper_buffer_metrics(
    metrics: &metrics::PipelineMetrics,
    mapper: &mut dyn BlockMapper,
) -> MapperBufferEstimate {
    let rows = mapper.total_rows();
    let estimates = MapperBufferEstimate::from_table_sizes(
        mapper.table_estimates().into_iter().map(|(_, bytes)| bytes),
    );
    metrics.record_mapper_buffer(
        rows,
        usize::try_from(estimates.largest_table_bytes).unwrap_or(usize::MAX),
    );
    metrics
        .mapper_buffer_estimated_bytes
        .set(i64::try_from(estimates.total_bytes).unwrap_or(i64::MAX));
    estimates
}

fn should_emit_progress_log(counter: u64) -> bool {
    counter > 0 && counter % 100 == 0
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
        if candidate.eq_ignore_ascii_case("tron-evm") {
            return Some("evm");
        }
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
                    "existing partitions.parquet was built for chain '{}' but current resolved chain is '{}'; \
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
) -> ParquetFileMetadata {
    let inferred_block_type = infer_partitions_block_type(chain, endpoint_info);
    let tron_style_evm_profile = chain_uses_tron_style_evm_profile(chain, endpoint_info);
    let encoding =
        resolve_auto_encode_bytes(inferred_block_type, endpoint_info, tron_style_evm_profile);

    let mut meta = ParquetFileMetadata::new();
    add_common_file_metadata(
        &mut meta,
        inferred_block_type,
        Some(&encoding),
        endpoint,
        endpoint_info,
    );
    if let Some(info) = endpoint_info {
        if !info.chain_name.is_empty() {
            // already set by `add_common_file_metadata`
        } else {
            meta.add("firehose-parquet.chain_name", chain);
        }
    } else {
        meta.add("firehose-parquet.chain_name", chain);
    }
    meta.add("firehose-parquet.partition", partition);
    meta.add(
        "firehose-parquet.block_range_size",
        block_range_size.unwrap_or(0).to_string(),
    );
    meta.add("firehose-parquet.compression", compression.to_string());
    meta
}

fn log_existing_partitions_index_state(
    partitions_index: &str,
    existing_rows: &[PartitionBuildRow],
    overwrite: bool,
) {
    if overwrite {
        info!(
            partitions_index = %partitions_index,
            "overwrite requested; ignoring any existing partitions index until the next successful write"
        );
    } else if let Some(existing_frontier) = existing_rows.iter().map(|row| row.stop_block).max() {
        info!(
            partitions_index = %partitions_index,
            existing_rows = existing_rows.len(),
            existing_frontier,
            "loaded existing partitions index"
        );
    } else {
        info!(
            partitions_index = %partitions_index,
            "no existing partitions index found; starting with a fresh canonical index"
        );
    }
}

fn protected_block_family(label: &str) -> Result<BlockFamily> {
    Ok(match label {
        "evm" => BlockFamily::Evm,
        "bitcoin" => BlockFamily::Bitcoin,
        "solana" => BlockFamily::Solana,
        "near" => BlockFamily::Near,
        "antelope" => BlockFamily::Antelope,
        "cosmos" => BlockFamily::Cosmos,
        "tron" => BlockFamily::Tron,
        "beacon" => BlockFamily::Beacon,
        _ => return Err(anyhow!("unsupported resolved mapper family")),
    })
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

fn resolve_output_bytes_encoding(
    block_type: Option<&str>,
    endpoint_info: &Option<EndpointInfo>,
    tron_style_evm_profile: bool,
) -> EncodeBytes {
    resolve_auto_encode_bytes(block_type, endpoint_info, tron_style_evm_profile)
}

/// Resolve the output only after a successful metadata lookup. Neither a
/// network alias nor a block family proves the endpoint's canonical suffix.
fn resolve_output(base: &PathBuf, endpoint_info: &Option<EndpointInfo>) -> Result<PathBuf> {
    let info = endpoint_info.as_ref().filter(|info| !info.chain_name.trim().is_empty())
        .ok_or_else(|| anyhow!("EndpointInfo with a nonempty chain_name is required before resolving output and cursor paths"))?;
    Ok(base.join(&info.chain_name))
}

async fn ensure_endpoint_available(
    client: &FirehoseClient,
    endpoint: &str,
    network: Option<&str>,
) -> Result<()> {
    client.healthcheck().await.map_err(|err| {
        if let Some(network) = network {
            anyhow!(
                "resolved endpoint for --network `{network}` is unavailable or unhealthy: {endpoint}. verify the network is still supported or provide --endpoint explicitly. root cause: {err}"
            )
        } else {
            anyhow!(
                "Firehose endpoint `{endpoint}` is unavailable or unhealthy; verify --endpoint/ENDPOINT and try again. root cause: {err}"
            )
        }
    })
}

fn infer_ingestion_live_mode(stop_block: Option<u64>) -> bool {
    stop_block.is_none()
}

fn resolve_ingestion_start_block(
    start_block: Option<u64>,
    cursor_state: Option<&CursorState>,
    endpoint_info: &Option<EndpointInfo>,
    cursor_override: bool,
) -> Result<Option<u64>> {
    if !cursor_override {
        if let Some(cursor_start_block) = cursor_state.and_then(|state| state.start_block) {
            return Ok(Some(cursor_start_block));
        }
    }

    if let Some(start_block) = start_block {
        return Ok(Some(start_block));
    }

    endpoint_info
        .as_ref()
        .map(|info| info.first_streamable_block_num)
        .map(Some)
        .ok_or_else(|| {
            anyhow!(
                "--start-block is required when neither an existing cursor nor the endpoint exposes first_streamable_block_num"
            )
        })
}

fn resolve_ingestion_stop_block(stop_block: Option<u64>) -> Result<Option<u64>> {
    if let Some(stop_block) = stop_block {
        return Ok(Some(stop_block));
    }

    Ok(None)
}

fn stream_resume_cursor(
    cursor_state: Option<&CursorState>,
    cursor_override: bool,
) -> Option<String> {
    if cursor_override {
        None
    } else {
        cursor_state.map(|state| state.cursor.clone())
    }
}

/// Drops blocks below the effective start block before they are mapped.
///
/// Without a resume cursor, a Firehose request whose start block is above the
/// last irreversible block (LIB) is served from LIB+1, so the first blocks can
/// be below `--start-block`. With a cursor the server resumes after the cursor,
/// which is never below the run's start block, so the filter is only a guard.
#[derive(Debug)]
struct StartBlockFilter {
    start_block: Option<u64>,
    skipped: u64,
}

impl StartBlockFilter {
    fn new(start_block: Option<u64>) -> Self {
        Self {
            start_block,
            skipped: 0,
        }
    }

    /// Returns `true` when the block should be mapped. Blocks below the start
    /// block are counted and rejected; the first one is logged.
    fn admit(&mut self, block_num: u64) -> bool {
        match self.start_block {
            Some(start_block) if block_num < start_block => {
                if self.skipped == 0 {
                    warn!(
                        block_num,
                        start_block,
                        "Firehose streamed a block below --start-block (a start above the last irreversible block is served from LIB+1); skipping blocks below the start block"
                    );
                }
                self.skipped += 1;
                false
            }
            _ => true,
        }
    }
}

/// Chains whose block numbers can legitimately have gaps (skipped slots or
/// heights), so a bounded range may end below `stop_block - 1`.
fn block_type_allows_block_number_gaps(block_type: &str) -> bool {
    matches!(block_type, "solana" | "near" | "beacon")
}

/// Check that a bounded stream which ended cleanly reached its last requested
/// block (`stop_block - 1`). `last_block_num` is the highest block processed
/// by this run or recorded in the cursor it resumed from.
///
/// On chains without block-number gaps an early end is an error, so a bounded
/// run never exits 0 with part of its range missing. The output and cursor
/// already cover the blocks received, so a rerun resumes after them.
fn ensure_bounded_stream_reached_stop(
    stop_block: u64,
    last_block_num: Option<u64>,
    block_number_gaps_allowed: bool,
) -> Result<()> {
    let last_requested_block = stop_block.saturating_sub(1);
    if last_block_num.is_some_and(|block| block >= last_requested_block) {
        return Ok(());
    }
    if block_number_gaps_allowed {
        warn!(
            last_block_num = ?last_block_num,
            last_requested_block,
            "stream ended below the last requested block; the server has no more blocks in the range (skipped slots)"
        );
        return Ok(());
    }
    let reached =
        last_block_num.map_or_else(|| "no block".to_string(), |block| format!("block {block}"));
    Err(anyhow!(
        "Firehose stream ended at {reached}, before the last requested block {last_requested_block} (--stop-block {stop_block} is exclusive). The output and cursor cover the blocks received; rerun to resume"
    ))
}

fn validate_block_range_alignment(
    explicit_start_block: Option<u64>,
    effective_start_block: Option<u64>,
    stop_block: Option<u64>,
    block_range_size: u64,
) -> Result<()> {
    if let Some(start_block) = explicit_start_block {
        if start_block % block_range_size != 0 {
            return Err(anyhow!(
                "--start-block must align to --block-range-size ({block_range_size}) when --partition block_range; got {start_block}"
            ));
        }
    }

    if let Some(stop_block) = stop_block {
        let effective_start_block = effective_start_block.expect("validated by caller");
        if stop_block < effective_start_block
            || stop_block
                .saturating_sub(effective_start_block)
                .rem_euclid(block_range_size)
                != 0
        {
            return Err(anyhow!(
                "--stop-block must align to the effective start block ({effective_start_block}) in --block-range-size ({block_range_size}) increments when --partition block_range; got {stop_block}"
            ));
        }
    }

    Ok(())
}

/// Validate the effective cursor after any template expansion, before startup I/O.
fn validate_cursor_storage(config: &Config) -> Result<()> {
    firehose_parquet::s3::validate_output_bucket(
        config.output.to_string_lossy().as_ref(),
        config.s3_bucket.as_deref(),
    )?;
    for path in std::iter::once(config.output.to_string_lossy().into_owned())
        .chain(config.cursor_path.iter().cloned())
        .filter(|path| path.starts_with("s3://"))
    {
        validate_s3_output_credentials(
            &path,
            config.aws_access_key_id.as_deref(),
            config.aws_secret_access_key.as_deref(),
        )?;
        let (bucket, _) = firehose_parquet::writer::parse_s3_url(&path)?;
        if let Some(endpoint) = config.aws_endpoint_url.as_deref() {
            firehose_parquet::s3::endpoint_is_bucket_bound(endpoint, &bucket)?;
        }
    }
    Ok(())
}

fn resolve_cursor_location(
    config: &firehose_parquet::config::Config,
) -> Result<Option<CursorLocation>> {
    validate_cursor_storage(config)?;
    if let Some(ref cp) = config.cursor_path {
        let output_str = config.output.to_string_lossy().to_string();
        Ok(Some(CursorLocation::resolve(&output_str, cp, |bucket| {
            firehose_parquet::s3::build_s3_client(config, bucket)
        })?))
    } else {
        Ok(None)
    }
}

/// Load the stored cursor for `fireparq build`.
///
/// A cursor that exists but cannot be read (permission denied, S3 5xx/403,
/// truncated or corrupt file) is a hard error: silently starting fresh would
/// re-ingest from `--start-block` and overwrite the resume point on the first
/// flush. With `--cursor-override` the unreadable cursor is ignored, since the
/// run restarts from the CLI bounds anyway.
fn load_existing_cursor(
    cursor_location: Option<&CursorLocation>,
    cursor_override: bool,
) -> Result<Option<CursorState>> {
    let Some(cursor_location) = cursor_location else {
        return Ok(None);
    };
    match cursor_location.load() {
        Ok(state) => Ok(state),
        Err(error) if cursor_override => {
            warn!(
                error = %format!("{error:#}"),
                "ignoring unreadable cursor because --cursor-override is set; restarting from CLI-provided/default bounds"
            );
            Ok(None)
        }
        Err(error) => Err(error.context(
            "stored cursor exists but could not be loaded; refusing to start fresh. Fix access to the cursor, or pass --cursor-override to ignore it and restart from --start-block (the cursor is overwritten on the next flush)",
        )),
    }
}

async fn run_partitions_build(
    endpoint: &str,
    network: Option<&str>,
    api_key_envvar: Option<&str>,
    api_token_envvar: Option<&str>,
    start_block: Option<u64>,
    stop_block: Option<u64>,
    live: bool,
    poll_interval_secs: u64,
    partition_types_spec: &str,
    block_range_size: Option<u64>,
    compression: Compression,
    output: Option<&str>,
    s3_bucket: Option<&str>,
    resume: bool,
    overwrite: bool,
    aws: &AwsConfig,
) -> Result<PartitionBuildResult> {
    const PARTITIONS_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
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
    validate_s3_output_credentials(
        &output_root,
        aws.aws_access_key_id.as_deref(),
        aws.aws_secret_access_key.as_deref(),
    )?;

    let credentials =
        firehose_parquet::auth::resolve_credentials(endpoint, api_key_envvar, api_token_envvar)?;
    let base_config = Config {
        endpoint: endpoint.to_string(),
        api_key: credentials.api_key,
        jwt_token: credentials.jwt_token,
        start_block,
        stop_block,
        skip_missing_blocks: true,
        cursor_path: None,
        output: PathBuf::from(&output_root),
        partition: Partition::None,
        flush_rows: None,
        flush_blocks: None,
        flush_bytes: 0,
        flush_memory_bytes: firehose_parquet::config::DEFAULT_FLUSH_MEMORY_BYTES,
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

    let shutdown = CancellationToken::new();
    let _signal_task = spawn_partitions_shutdown_handler(shutdown.clone());
    let info_client = FirehoseClient::new(base_config.clone())?;
    unless_shutdown(
        &shutdown,
        ensure_endpoint_available(&info_client, endpoint, network),
    )
    .await??;
    let endpoint_info = Some(unless_shutdown(&shutdown, info_client.info()).await??);
    let chain = endpoint_info
        .as_ref()
        .map(|info| info.chain_name.clone())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "partitions build requires endpoint info with chain_name; the legacy --chain override has been removed"
            )
        })?;

    let partitions_file_metadata = build_partitions_file_metadata(
        endpoint,
        &chain,
        &partition_label,
        compression,
        &endpoint_info,
        block_range_size,
    );
    let partitions_index = build_partitions_index_path(&output_root, &chain);
    let chain_output_root = build_partitions_output_root(&output_root, &chain);

    // Prove the requested bound before acquiring remote ownership or reading
    // an existing index. No failed probe can create or replace an index.
    let initial_finalized = info_client
        .finalized_anchor(PARTITIONS_PROBE_TIMEOUT, &shutdown)
        .await?;
    if let Some(stop) = stop_block {
        anyhow::ensure!(
            stop <= initial_finalized.exclusive_stop()?,
            "--stop-block {stop} exceeds proven finalized coverage ending at {}",
            initial_finalized.exclusive_stop()?
        );
    }
    let ownership =
        prepare_partitions_index_write(&chain_output_root, &partitions_index, aws).await?;
    let mut snapshot = load_existing_verified_partitions_index(&partitions_index, aws, overwrite)?;
    let existing_rows = snapshot
        .as_ref()
        .map(|index| {
            index
                .spans
                .iter()
                .map(|span| span.row.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    log_existing_partitions_index_state(&partitions_index, &existing_rows, overwrite);
    ensure_existing_partitions_index_mode(
        &partitions_index,
        &existing_rows,
        live,
        resume,
        overwrite,
    )?;
    if !existing_rows.is_empty() {
        validate_existing_partitions_params(
            &existing_rows,
            &chain,
            partition_type,
            block_range_size,
        )?;
    }
    let policy = if partition_type == PartitionBuildType::BlockRange {
        IndexRoutingPolicy::BlockNumber
    } else if endpoint_chain_is_solana(&endpoint_info) {
        IndexRoutingPolicy::SolanaPriorTimestamp
    } else {
        IndexRoutingPolicy::CanonicalTimestamp
    };
    if let Some(index) = &snapshot {
        anyhow::ensure!(
            index.coverage.routing_policy == policy,
            "existing index routing policy differs from this endpoint"
        );
    }
    let resumed_from_block = snapshot.as_ref().map(|index| index.coverage.stop_block);
    let effective_start_block = resolve_partitions_build_start_block(
        resumed_from_block,
        start_block,
        live,
        || {
            let cursor = CursorLocation::resolve(
                &chain_output_root,
                firehose_parquet::cursor::CURSOR_PARQUET_FILENAME,
                |bucket| Ok(Arc::new(aws.build_s3_client(bucket)?)),
            )?;
            Ok(cursor
                .load()
                .context("reading sibling cursor.parquet to infer --start-block")?
                .map(|cursor| {
                    cursor
                        .last_block_num
                        .checked_add(1)
                        .context("cursor frontier overflows block range")
                })
                .transpose()?)
        },
        endpoint_info
            .as_ref()
            .map(|info| info.first_streamable_block_num),
    )?;
    if snapshot.is_none() {
        if let Some(stop) = stop_block {
            anyhow::ensure!(
                effective_start_block < stop,
                "--stop-block must exceed the effective start block"
            );
        }
    }
    info!(chain = %chain, partition = %partition_label, effective_start_block, requested_stop_block = stop_block,
        live, resumed = snapshot.is_some(), "building exact finalized partition coverage");
    let probe_counter = AtomicU64::new(0);
    let mut initial_finalized = Some(initial_finalized);
    loop {
        if shutdown.is_cancelled() {
            if !live {
                return Err(ShutdownRequested.into());
            }
            break;
        }
        let finalized = match initial_finalized.take() {
            Some(anchor) => anchor,
            None => match info_client
                .finalized_anchor(PARTITIONS_PROBE_TIMEOUT, &shutdown)
                .await
            {
                Ok(anchor) => anchor,
                Err(error) if is_shutdown_error(&error) => break,
                Err(error)
                    if live
                        && matches!(
                            classify_fetch_error(&error),
                            FetchErrorKind::Timeout | FetchErrorKind::Transient
                        ) =>
                {
                    warn!(error = %error, "finalized head check failed; retaining the previous verified snapshot");
                    if unless_shutdown(
                        &shutdown,
                        tokio::time::sleep(Duration::from_secs(poll_interval_secs)),
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                    continue;
                }
                Err(error) => return Err(error),
            },
        };
        let frontier = snapshot
            .as_ref()
            .map(|index| index.coverage.stop_block)
            .unwrap_or(effective_start_block);
        let stop = stop_block.unwrap_or(finalized.exclusive_stop()?);
        anyhow::ensure!(
            stop <= finalized.exclusive_stop()?,
            "requested stop exceeds the proven finalized range"
        );
        if let Some(previous) = &snapshot {
            anyhow::ensure!(
                finalized.block_num >= previous.coverage.finalized.block_num,
                "endpoint finalized anchor moved behind the existing verified snapshot"
            );
            anyhow::ensure!(
                finalized.block_num != previous.coverage.finalized.block_num
                    || finalized.block_id == previous.coverage.finalized.block_id,
                "endpoint contradicts the stored finalized anchor identity"
            );
        }
        if frontier < stop {
            let extension = if partition_type == PartitionBuildType::BlockRange {
                unless_shutdown(
                    &shutdown,
                    build_verified_block_range_index(
                        &info_client,
                        &chain,
                        frontier,
                        stop,
                        block_range_size.expect("validated above"),
                        finalized,
                        PARTITIONS_PROBE_TIMEOUT,
                        &probe_counter,
                    ),
                )
                .await
                .and_then(|result| result)
            } else {
                let parent = snapshot.as_ref().map(|index| RoutingWitness {
                    block: index
                        .coverage
                        .last_observed
                        .clone()
                        .expect("validated time index"),
                    timestamp: index
                        .coverage
                        .last_routing_timestamp
                        .expect("validated routing timestamp"),
                });
                scan_time_index(
                    &info_client,
                    chain.clone(),
                    partition_type,
                    frontier,
                    stop,
                    finalized,
                    policy.clone(),
                    parent,
                    PARTITIONS_PROBE_TIMEOUT,
                    &shutdown,
                )
                .await
            };
            let extension = match extension {
                Ok(value) => value,
                Err(error) if live && is_shutdown_error(&error) => break,
                Err(error) => return Err(error),
            };
            let next = match snapshot.take() {
                Some(previous) => append_verified_extension(previous, extension)?,
                None => extension,
            };
            ownership.revalidate_local_paths()?;
            write_verified_partitions_index(
                &partitions_index,
                &next,
                compression,
                Some(aws),
                Some(&partitions_file_metadata),
            )?;
            info!(
                start_block = next.coverage.start_block,
                stop_block = next.coverage.stop_block,
                finalized_block = next.coverage.finalized.block_num,
                spans = next.spans.len(),
                incomplete_spans = next
                    .spans
                    .iter()
                    .filter(|span| !span.proof.complete())
                    .count(),
                "published verified partition snapshot"
            );
            snapshot = Some(next);
        }
        if !live {
            break;
        }
        if unless_shutdown(
            &shutdown,
            tokio::time::sleep(Duration::from_secs(poll_interval_secs)),
        )
        .await
        .is_err()
        {
            break;
        }
    }
    // Reads may be cancelled without an in-flight publication: writes above
    // finish synchronously before this release and retain errors as fatal.
    ownership.release().await?;
    Ok(PartitionBuildResult {
        partitions_index,
        chain,
        partition: partition_label,
        row_count: snapshot.as_ref().map_or(0, |index| index.spans.len()),
        start_block: snapshot
            .as_ref()
            .map_or(effective_start_block, |index| index.coverage.start_block),
        stop_block: snapshot
            .as_ref()
            .map_or(effective_start_block, |index| index.coverage.stop_block),
        resumed: resumed_from_block.is_some(),
        resumed_from_block,
        coverage: snapshot.map(|index| index.coverage),
    })
}

struct PartitionsSignalTask(tokio::task::JoinHandle<()>);
impl Drop for PartitionsSignalTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn spawn_partitions_shutdown_handler(shutdown: CancellationToken) -> PartitionsSignalTask {
    let mut signals = ShutdownSignals::install();
    PartitionsSignalTask(tokio::spawn(async move {
        signals.next().await;
        info!("shutdown requested; retaining the last verified partition snapshot");
        shutdown.cancel();
        signals.next().await;
        warn!("second shutdown signal received; an in-flight index write may be interrupted");
        std::process::exit(130);
    }))
}

fn load_existing_verified_partitions_index(
    path: &str,
    aws: &AwsConfig,
    overwrite: bool,
) -> Result<Option<VerifiedPartitionIndex>> {
    if overwrite {
        return Ok(None);
    }
    match read_verified_partitions_index(path, Some(aws)) {
        Ok(index) => Ok(Some(index)),
        Err(error)
            if error.chain().any(|cause| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                    || cause
                        .downcast_ref::<object_store::Error>()
                        .is_some_and(|error| matches!(error, object_store::Error::NotFound { .. }))
            }) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
async fn build_verified_block_range_index(
    client: &FirehoseClient,
    chain: &str,
    start: u64,
    stop: u64,
    size: u64,
    finalized: firehose_parquet::grpc::FinalizedAnchor,
    timeout: Duration,
    probes: &AtomicU64,
) -> Result<VerifiedPartitionIndex> {
    anyhow::ensure!(
        size > 0 && size <= i64::MAX as u64,
        "invalid block-range size"
    );
    anyhow::ensure!(
        start < stop && stop <= finalized.exclusive_stop()?,
        "block range exceeds finalized coverage"
    );
    let mut spans = Vec::new();
    let mut current = start;
    while current < stop {
        let aligned = (current / size) * size;
        let natural_stop = aligned
            .checked_add(size)
            .context("block-range boundary overflow")?;
        let end = natural_stop.min(stop);
        let mut row = build_block_range_partition_row(
            client, chain, current, end, size, timeout, true, probes,
        )
        .await?;
        row.partition_value = aligned.to_string();
        row.partition_start_ts = aligned.to_string();
        spans.push(VerifiedPartitionSpan {
            row,
            proof: PartitionSpanProof {
                start_complete: current == aligned,
                end_complete: end == natural_stop,
                first_block: None,
                routing_start_timestamp: None,
            },
        });
        current = end;
    }
    let index = VerifiedPartitionIndex {
        coverage: PartitionCoverage {
            version: INDEX_FORMAT_VERSION,
            start_block: start,
            stop_block: stop,
            finalized,
            routing_policy: IndexRoutingPolicy::BlockNumber,
            first_observed: None,
            last_observed: None,
            next_observed: None,
            last_routing_timestamp: None,
        },
        spans,
    };
    index.validate()?;
    Ok(index)
}

/// Refuse to modify an existing canonical index unless the caller chose how to treat it.
///
/// Without `--resume`, a bounded build would replace `partitions.parquet` with only the new
/// range (time-based) or append rows that overlap the stored ones (block_range). Live mode
/// always continues from the stored frontier, and `--overwrite` replaces the index.
fn ensure_existing_partitions_index_mode(
    partitions_index: &str,
    existing_rows: &[PartitionBuildRow],
    live: bool,
    resume: bool,
    overwrite: bool,
) -> Result<()> {
    if live || resume || overwrite || existing_rows.is_empty() {
        return Ok(());
    }

    let start_block = existing_rows
        .iter()
        .map(|row| row.start_block)
        .min()
        .unwrap_or_default();
    let frontier = existing_rows
        .iter()
        .map(|row| row.stop_block)
        .max()
        .unwrap_or_default();
    Err(anyhow!(
        "{partitions_index} already exists with {} rows covering blocks [{start_block}, {frontier}); pass --resume to extend it from block {frontier}, or --overwrite to replace it",
        existing_rows.len()
    ))
}

/// Resolve the first block a partitions build probes.
///
/// When an existing index is resumed (`--live`, or bounded `--resume`), its stored frontier
/// is the only valid start: the sibling cursor and endpoint metadata are not consulted, and
/// an explicit `--start-block` past the frontier is rejected because the terminal row would
/// otherwise be stretched across the unprobed blocks in between. A new index starts at the
/// explicit `--start-block`, then the sibling cursor (bounded mode only), then the endpoint's
/// first streamable block.
fn resolve_partitions_build_start_block(
    resume_frontier: Option<u64>,
    explicit_start_block: Option<u64>,
    live: bool,
    cursor_start_block: impl FnOnce() -> Result<Option<u64>>,
    first_streamable_block: Option<u64>,
) -> Result<u64> {
    if let Some(frontier) = resume_frontier {
        return match explicit_start_block {
            Some(start_block) if live && start_block != frontier => Err(anyhow!(
                "--live resumes from existing partitions.parquet frontier {frontier}; explicit --start-block {start_block} does not match"
            )),
            Some(start_block) if start_block > frontier => Err(anyhow!(
                "--start-block {start_block} is past the existing partitions.parquet frontier {frontier}; resuming would leave blocks [{frontier}, {start_block}) unindexed. Omit --start-block to resume from {frontier}, or pass --overwrite to rebuild the index from {start_block}"
            )),
            _ => Ok(frontier),
        };
    }

    if let Some(start_block) = explicit_start_block {
        return Ok(start_block);
    }
    if !live {
        if let Some(start_block) = cursor_start_block()? {
            return Ok(start_block);
        }
    }
    first_streamable_block.ok_or_else(|| {
        if live {
            anyhow!(
                "--start-block is required when --live has no existing partitions.parquet frontier and the endpoint does not expose first_streamable_block_num"
            )
        } else {
            anyhow!(
                "--start-block is required when no sibling cursor.parquet exists and the endpoint does not expose first_streamable_block_num"
            )
        }
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
    validate_block_range_alignment(start_block, Some(0), stop_block, block_range_size)
}

const PARTITIONS_PROBE_FETCH_MAX_ATTEMPTS: usize = 4;
const PARTITIONS_PROBE_FETCH_INITIAL_BACKOFF: Duration = Duration::from_millis(250);

/// Retry settings for one probe fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProbeRetryPolicy {
    max_attempts: usize,
    /// Attempts before a "not found" answer is accepted as a missing block.
    missing_attempts: usize,
    initial_backoff: Duration,
}

impl ProbeRetryPolicy {
    /// Default policy. Without `confirm_missing`, a "not found" answer is accepted at once,
    /// which is used once a run of missing blocks is already established.
    fn new(confirm_missing: bool) -> Self {
        Self {
            max_attempts: PARTITIONS_PROBE_FETCH_MAX_ATTEMPTS,
            missing_attempts: if confirm_missing {
                PARTITIONS_PROBE_FETCH_MAX_ATTEMPTS
            } else {
                1
            },
            initial_backoff: PARTITIONS_PROBE_FETCH_INITIAL_BACKOFF,
        }
    }
}

/// Outcome of probing one block number.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ProbeFetch<T> {
    /// The endpoint returned the requested block.
    Found(T),
    /// The endpoint has no block at this number, e.g. a skipped Solana slot.
    Missing,
    /// The number is past the chain head: the endpoint answered with an earlier block.
    PastHead,
}

/// Probe one block number, retrying failures with exponential backoff.
///
/// Errors are classified with [`classify_fetch_error`]:
/// - timeouts and transient errors are retried up to `policy.max_attempts`, then returned; they
///   never count as a missing block, so a slow endpoint cannot make probing skip real blocks
/// - "not found" is retried up to `policy.missing_attempts`, then reported as
///   [`ProbeFetch::Missing`] when `skip_missing_blocks` is set (returned as an error otherwise)
/// - fatal errors (authentication, permissions) are returned immediately
async fn retry_probe_fetch_with_policy<T, Op, Fut>(
    block_num: u64,
    context: &str,
    policy: ProbeRetryPolicy,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
    mut op: Op,
) -> Result<ProbeFetch<T>>
where
    Op: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<ProbeFetch<T>>>,
{
    let attempts = policy.max_attempts.max(1);
    let missing_attempts = policy.missing_attempts.clamp(1, attempts);
    let mut attempt = 1usize;
    let mut backoff = policy.initial_backoff;

    loop {
        probe_counter.fetch_add(1, Ordering::Relaxed);
        let error = match op().await {
            Ok(outcome) => return Ok(outcome),
            Err(error) => error,
        };

        let kind = classify_fetch_error(&error);
        match kind {
            FetchErrorKind::NotFound if skip_missing_blocks && attempt >= missing_attempts => {
                debug!(
                    block_num,
                    context,
                    attempts = attempt,
                    error = %format!("{error:#}"),
                    "probe found no block at this number; treating it as skipped"
                );
                return Ok(ProbeFetch::Missing);
            }
            FetchErrorKind::Fatal => {
                return Err(error.context(format!(
                    "{context}: probe fetch for block {block_num} failed"
                )));
            }
            _ if attempt >= attempts => {
                return Err(error.context(format!(
                    "{context}: probe fetch for block {block_num} failed after {attempt} attempts"
                )));
            }
            _ => {
                warn!(
                    block_num,
                    context,
                    attempt,
                    max_attempts = attempts,
                    kind = ?kind,
                    retry_backoff_ms = backoff.as_millis() as u64,
                    error = %format!("{error:#}"),
                    "probe fetch failed; retrying"
                );
            }
        }

        tokio::time::sleep(backoff).await;
        attempt = attempt.saturating_add(1);
        backoff = backoff.saturating_mul(2);
    }
}

/// Fetch one block number, mapping an answer for an earlier block to [`ProbeFetch::PastHead`].
async fn probe_fetch_block(
    client: &FirehoseClient,
    block_num: u64,
    wait_timeout: Option<Duration>,
) -> Result<ProbeFetch<BlockIdentity>> {
    match client.fetch_block_identity(block_num, wait_timeout).await? {
        Some(block) if block.block_num < block_num => Ok(ProbeFetch::PastHead),
        Some(block) if block.block_num == block_num => Ok(ProbeFetch::Found(block)),
        Some(block) => Err(anyhow!(
            "fetch for block {block_num} returned unexpected later block {}",
            block.block_num
        )),
        None => Err(anyhow!(
            "fetch for block {block_num} returned no block metadata"
        )),
    }
}

async fn probe_block_range_boundary_timestamp(
    client: &FirehoseClient,
    block_num: u64,
    probe_timeout: Duration,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<Option<i64>> {
    // A missing boundary may have a nullable timestamp. Endpoint failures are
    // not missing metadata and must never produce an apparently valid row.
    let outcome = retry_probe_fetch_with_policy(
        block_num,
        "probing block-range boundary timestamp",
        ProbeRetryPolicy::new(true),
        skip_missing_blocks,
        probe_counter,
        || probe_fetch_block(client, block_num, Some(probe_timeout)),
    )
    .await?;
    Ok(match outcome {
        ProbeFetch::Found(block) if block.timestamp != 0 => Some(block.timestamp),
        ProbeFetch::Found(_) | ProbeFetch::Missing | ProbeFetch::PastHead => None,
    })
}

async fn build_block_range_partition_row(
    client: &FirehoseClient,
    chain: &str,
    boundary: u64,
    partition_end: u64,
    block_range_size: u64,
    probe_timeout: Duration,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<PartitionBuildRow> {
    let start_time = probe_block_range_boundary_timestamp(
        client,
        boundary,
        probe_timeout,
        skip_missing_blocks,
        probe_counter,
    )
    .await?;
    let end_time = if partition_end > boundary + 1 {
        probe_block_range_boundary_timestamp(
            client,
            partition_end.saturating_sub(1),
            probe_timeout,
            skip_missing_blocks,
            probe_counter,
        )
        .await?
    } else {
        start_time
    };

    Ok(PartitionBuildRow {
        partition_type: "block_range".to_string(),
        partition_interval_seconds: block_range_size as i64,
        partition_start_ts: boundary.to_string(),
        partition_value: boundary.to_string(),
        start_block: boundary,
        stop_block: partition_end,
        start_time: start_time.map(format_probe_timestamp).transpose()?,
        end_time: end_time.map(format_probe_timestamp).transpose()?,
        chain: Some(chain.to_string()),
    })
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

// Progress logging must never turn an otherwise successful operation into an error.
fn format_optional_probe_timestamp(timestamp: i64) -> Option<String> {
    (timestamp != 0).then(|| {
        format_probe_timestamp(timestamp)
            .unwrap_or_else(|_| format!("invalid unix timestamp {timestamp}"))
    })
}

/// Check if the endpoint supports `extended` block features.
fn supports_extended(endpoint_info: &Option<EndpointInfo>) -> bool {
    endpoint_info.as_ref().map_or(false, |ei| {
        ei.block_features.iter().any(|f| f == "extended")
    })
}

fn chain_name_is_solana(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    normalized == "solana" || normalized.starts_with("solana-")
}

fn chain_name_is_antelope(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    normalized == "antelope" || normalized.starts_with("antelope-") || normalized == "eos"
}

fn endpoint_chain_is_solana(endpoint_info: &Option<EndpointInfo>) -> bool {
    endpoint_info.as_ref().is_some_and(|ei| {
        chain_name_is_solana(&ei.chain_name)
            || ei
                .chain_name_aliases
                .iter()
                .any(|alias| chain_name_is_solana(alias))
    })
}

fn endpoint_chain_is_antelope(endpoint_info: &Option<EndpointInfo>) -> bool {
    endpoint_info.as_ref().is_some_and(|ei| {
        chain_name_is_antelope(&ei.chain_name)
            || ei
                .chain_name_aliases
                .iter()
                .any(|alias| chain_name_is_antelope(alias))
    })
}

fn cursor_metadata_block_type<'a>(cursor_state: Option<&'a CursorState>) -> Option<&'a str> {
    cursor_state.and_then(|state| state.get_metadata("firehose-parquet.block_type"))
}

fn cursor_chain_is_solana(cursor_state: Option<&CursorState>) -> bool {
    cursor_metadata_block_type(cursor_state).is_some_and(chain_name_is_solana)
        || cursor_state.is_some_and(|state| {
            state
                .get_metadata("firehose-parquet.chain_name")
                .is_some_and(chain_name_is_solana)
                || state
                    .get_metadata("firehose-parquet.chain_name_aliases")
                    .is_some_and(|aliases| aliases.split(',').any(chain_name_is_solana))
        })
}

fn cursor_chain_is_antelope(cursor_state: Option<&CursorState>) -> bool {
    cursor_metadata_block_type(cursor_state).is_some_and(chain_name_is_antelope)
        || cursor_state.is_some_and(|state| {
            state
                .get_metadata("firehose-parquet.chain_name")
                .is_some_and(chain_name_is_antelope)
                || state
                    .get_metadata("firehose-parquet.chain_name_aliases")
                    .is_some_and(|aliases| aliases.split(',').any(chain_name_is_antelope))
        })
}

fn chain_is_solana(
    requested_block_type: &str,
    endpoint_info: &Option<EndpointInfo>,
    cursor_state: Option<&CursorState>,
) -> bool {
    requested_block_type == "solana"
        || (requested_block_type == "auto"
            && (endpoint_chain_is_solana(endpoint_info) || cursor_chain_is_solana(cursor_state)))
}

fn chain_is_antelope(
    requested_block_type: &str,
    endpoint_info: &Option<EndpointInfo>,
    cursor_state: Option<&CursorState>,
) -> bool {
    requested_block_type == "antelope"
        || (requested_block_type == "auto"
            && (endpoint_chain_is_antelope(endpoint_info)
                || cursor_chain_is_antelope(cursor_state)))
}

fn chain_is_known_non_solana(
    requested_block_type: &str,
    endpoint_info: &Option<EndpointInfo>,
    cursor_state: Option<&CursorState>,
) -> bool {
    match requested_block_type {
        "auto" => {
            if endpoint_info.is_some() {
                !endpoint_chain_is_solana(endpoint_info)
            } else {
                cursor_metadata_block_type(cursor_state)
                    .is_some_and(|block_type| !chain_name_is_solana(block_type))
            }
        }
        "solana" => false,
        _ => true,
    }
}

fn unsupported_chain_feature_flag_warnings(
    requested_block_type: &str,
    endpoint_info: &Option<EndpointInfo>,
    cursor_state: Option<&CursorState>,
    without_extended: bool,
    without_votes: bool,
) -> Vec<&'static str> {
    let mut warnings = Vec::new();
    let solana_chain = chain_is_solana(requested_block_type, endpoint_info, cursor_state);
    let antelope_chain = chain_is_antelope(requested_block_type, endpoint_info, cursor_state);
    let known_non_solana_chain =
        chain_is_known_non_solana(requested_block_type, endpoint_info, cursor_state);

    if without_extended && (solana_chain || antelope_chain) {
        warnings.push(without_extended_warning_message());
    }
    if without_votes && known_non_solana_chain {
        warnings.push(without_votes_warning_message());
    }

    warnings
}

fn without_extended_warning_message() -> &'static str {
    WITHOUT_EXTENDED_WARNING
}

fn without_votes_warning_message() -> &'static str {
    WITHOUT_VOTES_NON_SOLANA_WARNING
}

fn log_solana_vote_mode(with_votes: bool) {
    if with_votes {
        info!("Solana vote_transactions enabled");
    } else {
        info!("Solana vote_transactions disabled via --without-votes");
    }
}

fn maybe_add_solana_with_votes_metadata(
    meta: &mut ParquetFileMetadata,
    block_type: Option<&str>,
    with_votes: bool,
) {
    if block_type == Some("solana") {
        meta.add("firehose-parquet.with_votes", with_votes.to_string());
    }
}

fn parse_metadata_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn solana_with_votes_from_cursor(cursor_state: &CursorState) -> Option<bool> {
    cursor_state
        .get_metadata("firehose-parquet.with_votes")
        .and_then(parse_metadata_bool)
}

fn apply_solana_cursor_feature_validation(
    mismatches: &mut Vec<String>,
    cursor_state: &CursorState,
    with_votes: bool,
) {
    if let Some(stored_with_votes) = solana_with_votes_from_cursor(cursor_state) {
        if stored_with_votes != with_votes {
            mismatches.push(format!(
                "with_votes: cursor={} vs current={}",
                stored_with_votes, with_votes
            ));
        }
    } else if with_votes
        && !mismatches
            .iter()
            .any(|mismatch| mismatch.starts_with("extended:"))
    {
        mismatches.push(format!(
            "with_votes: cursor=unknown vs current={}",
            with_votes
        ));
    }
}

fn apply_antelope_cursor_feature_validation(mismatches: &mut Vec<String>) {
    mismatches.retain(|mismatch| !mismatch.starts_with("extended:"));
}

/// Keep extended output enabled by default while logging endpoint capability
/// when available.
fn resolve_extended_mode(
    extended_enabled: bool,
    without_extended: bool,
    endpoint_info: &Option<EndpointInfo>,
) -> bool {
    let endpoint_supports_extended = supports_extended(endpoint_info);

    if endpoint_supports_extended {
        if extended_enabled {
            info!("endpoint advertises extended block features; extended output enabled");
        } else {
            info!(
                "endpoint advertises extended block features; extended output disabled via --without-extended"
            );
        }
    } else if without_extended {
        warn!("{}", without_extended_warning_message());
    }

    extended_enabled
}

/// Create a `Box<dyn BlockMapper>` for the given block type.
fn create_mapper(
    block_type: &str,
    extended: bool,
    with_votes: bool,
    include_fork_step: bool,
    encode_bytes: EncodeBytes,
    synthetic_partition_routing: bool,
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
            with_votes,
            include_fork_step,
            encode_bytes,
            synthetic_partition_routing,
            include_failed_transactions,
        ))),
        "near" => Ok(Box::new(NearBlockMapper::new(
            include_fork_step,
            encode_bytes,
            include_failed_transactions,
        ))),
        "antelope" => Ok(Box::new(AntelopeBlockMapper::new(
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
            Commands::Recovery(command) => {
                firehose_parquet::recovery::run_recovery(command).await?;
                return Ok(());
            }
            Commands::Completions { shell } => {
                firehose_parquet::cli::generate_completions::<Cli>(*shell);
                return Ok(());
            }
            Commands::Build(build_args) => {
                run_ingestion(build_args, &cli.global).await?;
                return Ok(());
            }
            Commands::Partitions(subcommand) => match subcommand {
                PartitionsCommands::Build {
                    endpoint,
                    network,
                    api_key_envvar,
                    api_token_envvar,
                    start_block,
                    stop_block,
                    live,
                    poll_interval_secs,
                    partition,
                    block_range_size,
                    compression,
                    output,
                    s3_bucket,
                    resume,
                    overwrite,
                    json,
                    aws_access_key_id,
                    aws_secret_access_key,
                    aws_session_token,
                    aws_region,
                    aws_endpoint_url,
                } => {
                    init_tracing(&cli.global.log_level, cli.global.verbose);
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
                        network.as_deref(),
                        api_key_envvar.as_deref(),
                        api_token_envvar.as_deref(),
                        *start_block,
                        *stop_block,
                        *live,
                        *poll_interval_secs,
                        partition,
                        *block_range_size,
                        compression,
                        output.as_deref(),
                        s3_bucket.as_deref(),
                        *resume,
                        *overwrite,
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
                        print_partition_coverage(result.coverage.as_ref());
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
                        println!("incomplete_spans: {}", result.incomplete_spans);
                        println!("unknown_spans:    {}", result.unknown_spans);
                        print_partition_coverage(result.coverage.as_ref());
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
                        print_partition_coverage(Some(&result.coverage));
                        if !result.rows.is_empty() {
                            println!();
                            println!(
                                "{:<15} {:<19} {:<19} {:>12} {:>12} {:<10} {:<16} {}",
                                "partition_type",
                                "partition_value",
                                "partition_start_ts",
                                "start_block",
                                "stop_block",
                                "span",
                                "prior_context",
                                "chain"
                            );
                            for row in result.rows {
                                println!(
                                    "{:<15} {:<19} {:<19} {:>12} {:>12} {:<10} {:<16} {}",
                                    row.partition_type,
                                    row.partition_value,
                                    row.partition_start_ts,
                                    row.start_block,
                                    row.stop_block,
                                    partition_completeness(row.complete),
                                    match row.routing_context_required {
                                        Some(true) => "required",
                                        Some(false) => "independent",
                                        None => "unknown",
                                    },
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
                        print_partition_coverage(result.coverage.as_ref());
                        if !result.rows.is_empty() {
                            println!();
                            println!(
                                "{:<15} {:<19} {:<19} {:>12} {:>12} {:<10} {:<16} {}",
                                "partition_type",
                                "partition_value",
                                "partition_start_ts",
                                "start_block",
                                "stop_block",
                                "span",
                                "prior_context",
                                "chain"
                            );
                            for row in result.rows {
                                println!(
                                    "{:<15} {:<19} {:<19} {:>12} {:>12} {:<10} {:<16} {}",
                                    row.partition_type,
                                    row.partition_value,
                                    row.partition_start_ts,
                                    row.start_block,
                                    row.stop_block,
                                    partition_completeness(row.complete),
                                    match row.routing_context_required {
                                        Some(true) => "required",
                                        Some(false) => "independent",
                                        None => "unknown",
                                    },
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
                    all_spans,
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
                            all_spans: *all_spans,
                        },
                    )?;

                    if *json {
                        println!("{}", serde_json::to_string_pretty(&result)?);
                    } else {
                        println!("partitions_index: {}", result.partitions_index);
                        println!("partition_type:   {}", result.partition_type);
                        println!("partition_value:  {}", result.partition_value);
                        print_partition_coverage(Some(&result.coverage));
                        if let Some(chain) = result.partition_chain {
                            println!("partition_chain:  {chain}");
                        }
                        println!(
                            "start_block:      {}",
                            result.start_block.expect("single span")
                        );
                        println!(
                            "stop_block:       {}",
                            result.stop_block.expect("single span")
                        );
                    }

                    return Ok(());
                }
            },
            Commands::Scan {
                path,
                limit,
                offset,
                order,
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
                    *order,
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
                init_tracing(&cli.global.log_level, cli.global.verbose);
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
                init_tracing(&cli.global.log_level, cli.global.verbose);
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
                flush_rows,
                flush_bytes,
                dry_run,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
                aws_region,
                aws_endpoint_url,
                cache_control,
            } => {
                init_tracing(&cli.global.log_level, cli.global.verbose);
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
                    flush_rows: *flush_rows,
                    flush_bytes: *flush_bytes,
                    dry_run: *dry_run,
                    verbose: cli.global.verbose,
                    aws,
                    cache_control: cache_control.clone(),
                };
                let result = firehose_parquet::merge::run_merge(&merge_config)?;
                result.print();
                if !result.schema_mismatches.is_empty() {
                    anyhow::bail!(
                        "{} partition(s) were not merged because their parts have different \
                         schemas; nothing was written or deleted in them",
                        result.schema_mismatches.len()
                    );
                }
                return Ok(());
            }
            Commands::Truncate {
                path,
                partition,
                dry_run,
                yes,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
                aws_region,
                aws_endpoint_url,
            } => {
                init_tracing(&cli.global.log_level, cli.global.verbose);
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
                    yes: *yes,
                    aws,
                };
                let result = firehose_parquet::truncate::run_truncate(&truncate_config)?;
                result.print(path, *dry_run);
                return Ok(());
            }
        }
    }

    unreachable!("clap enforces a subcommand")
}

/// Collect output and the fully resolved cursor before taking any ownership.
fn ingestion_mutation_scopes(
    config: &Config,
    cursor: Option<&CursorLocation>,
) -> Result<Vec<MutationScope>> {
    let output = config.output.to_string_lossy().into_owned();
    let mut scopes = vec![MutationScope::directory(output.clone())];
    match cursor {
        Some(CursorLocation::Local(path)) => {
            scopes.push(MutationScope::file(path.to_string_lossy()));
        }
        Some(CursorLocation::S3 { key, .. }) => {
            let explicit = config.cursor_path.as_deref().unwrap_or_default();
            let bucket_source = if explicit.starts_with("s3://") {
                explicit
            } else {
                &output
            };
            let (bucket, _) = firehose_parquet::writer::parse_s3_url(bucket_source)?;
            scopes.push(MutationScope::file(format!("s3://{bucket}/{key}")));
        }
        None => {}
    }
    Ok(scopes)
}

fn commit_ingestion_flush(
    session: &mut IngestionSession<'_>,
    batches: HashMap<String, RecordBatch>,
    metadata: BlockMetadata,
    compression: Compression,
    file_metadata: ParquetFileMetadata,
    metrics: &metrics::PipelineMetrics,
    sizing: &mut FlushSizing,
    estimate: MapperBufferEstimate,
    trigger: &str,
) -> Result<WriterFlushOutcome> {
    let committed = session.flush_blocking(batches, metadata, compression, file_metadata)?;
    if let Some(flush) = &committed {
        record_committed_flush_sizing(sizing, estimate, flush, trigger);
        metrics
            .flushes_total
            .get_or_create(&metrics::FlushLabels {
                trigger: trigger.into(),
            })
            .inc();
    }
    Ok(WriterFlushOutcome {
        materialized: committed.is_some_and(|flush| flush.files > 0),
        buffered: WriterBufferStats::default(),
    })
}

fn record_committed_flush_sizing(
    sizing: &mut FlushSizing,
    estimate: MapperBufferEstimate,
    committed: &firehose_parquet::ingest::CommittedFlush,
    trigger: &str,
) {
    let largest_file_bytes = committed
        .tables
        .iter()
        .map(|table| table.bytes)
        .max()
        .unwrap_or(0);
    sizing.observe_committed(estimate.largest_table_bytes, largest_file_bytes);
    info!(
        trigger,
        largest_file_bytes,
        largest_mapper_estimated_bytes = estimate.largest_table_bytes,
        total_mapper_estimated_bytes = estimate.total_bytes,
        compressed_to_mapper_ratio = sizing.ratio(),
        "committed flush size observation"
    );
}

async fn run_ingestion(args: &BuildArgs, global: &GlobalArgs) -> Result<()> {
    init_tracing(
        &args.common.log_level,
        args.common.verbose || global.verbose,
    );

    info!(version = env!("CARGO_PKG_VERSION"), "fireparq starting");

    // Install graceful shutdown handler for SIGINT (Ctrl-C) and SIGTERM.
    // The first signal cancels endpoint waits and lets current block work
    // finish. Unflushed buffers are discarded and previously completed flushes
    // are preserved. A second signal forces exit and may interrupt writes.
    let shutdown = CancellationToken::new();
    // Cursor retries retain their durable-write contract: finish in-flight I/O,
    // then interrupt retry backoff on the same shutdown signal.
    let cursor_shutdown = Arc::new(AtomicBool::new(false));
    spawn_ingestion_shutdown_handler(shutdown.clone(), Arc::clone(&cursor_shutdown));

    let mut block_type = args.block_type.to_lowercase();
    if block_type != "auto" && !BLOCK_TYPES.contains(&block_type.as_str()) {
        return Err(anyhow!(
            "unsupported block type: {block_type}. Supported: {}",
            BLOCK_TYPES.join(", ")
        ));
    }

    let mut extended = !args.without_extended;
    let with_votes = !args.without_votes;
    let mut common = args.common.clone();
    let mut resolved_network_name: Option<String> = None;
    if common.endpoint.is_none() {
        if let Some(network) = args.network.as_deref() {
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
            resolved_network_name = Some(resolved.requested.clone());
            common.endpoint = Some(resolved.endpoint);
        }
    } else if let Some(network) = args.network.as_deref() {
        info!(
            endpoint = %common.endpoint.as_deref().unwrap_or_default(),
            network,
            "ignoring --network because --endpoint or ENDPOINT is already set"
        );
    }

    let mut config = build_config(&common)?;

    if let Some(template) = args.common.cursor_template.as_deref() {
        let context = CursorTemplateContext {
            chain: None,
            partition_type: None,
            partition_value: None,
            partition_from: None,
            partition_to: None,
        };
        let resolved_cursor_path = resolve_cursor_template(template, &context)?;
        config.cursor_path = Some(resolved_cursor_path.clone());
        info!(
            cursor_template = %template,
            cursor_path = %resolved_cursor_path,
            "resolved cursor path"
        );
    }

    validate_cursor_storage(&config)?;

    // Fetch endpoint info for auto-detection of encoding, chain_name-based
    // output directory, and feature capability logging.
    let mut client = FirehoseClient::new(config.clone())?;
    // A signal during endpoint startup stops before anything is written.
    // Preserve the required Info result; cancellation does not restore fallback
    // output identity when Info is unavailable.
    let startup = async {
        ensure_endpoint_available(&client, &config.endpoint, resolved_network_name.as_deref())
            .await?;
        client.info().await
    };
    let endpoint_info = match unless_shutdown(&shutdown, startup).await {
        Ok(endpoint_info) => Some(endpoint_info?),
        Err(error) if is_shutdown_error(&error) => {
            info!("shutdown requested during startup, exiting");
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    debug!(endpoint_info = ?endpoint_info, "fetched endpoint metadata");

    // Use chain_name as a subdirectory under the output path.
    config.output = resolve_output(&config.output, &endpoint_info)?;

    // Protected authority must bind the actual mapper before opening Blocks.
    // Unknown custom endpoint metadata therefore requires an explicit family.
    if !config.dry_run && block_type == "auto" {
        block_type = inferred_block_type_from_endpoint_info(&endpoint_info)
            .context("cannot resolve the mapper from EndpointInfo before protected recovery; provide --block-type explicitly")?.to_owned();
    }
    let cursor_location = resolve_cursor_location(&config)?;
    let mut ownership = if config.dry_run {
        None
    } else {
        let aws = AwsConfig {
            aws_access_key_id: config.aws_access_key_id.clone(),
            aws_secret_access_key: config.aws_secret_access_key.clone(),
            aws_session_token: config.aws_session_token.clone(),
            aws_region: config.aws_region.clone(),
            aws_endpoint_url: config.aws_endpoint_url.clone(),
        };
        Some(
            DatasetOwnership::acquire(
                "build",
                ingestion_mutation_scopes(&config, cursor_location.as_ref())?,
                Some(&aws),
            )
            .await?,
        )
    };
    let existing_cursor_state = if let Some(ownership) = &ownership {
        load_authoritative_resume(&config, ownership, args.cursor_override).await?
    } else {
        load_existing_cursor(cursor_location.as_ref(), args.cursor_override)?
    };
    let solana_chain = chain_is_solana(&block_type, &endpoint_info, existing_cursor_state.as_ref());
    let antelope_chain =
        chain_is_antelope(&block_type, &endpoint_info, existing_cursor_state.as_ref());
    let known_non_solana_chain =
        chain_is_known_non_solana(&block_type, &endpoint_info, existing_cursor_state.as_ref());

    for warning in unsupported_chain_feature_flag_warnings(
        &block_type,
        &endpoint_info,
        existing_cursor_state.as_ref(),
        args.without_extended,
        args.without_votes,
    ) {
        warn!("{}", warning);
    }

    let live = infer_ingestion_live_mode(config.stop_block);
    config.start_block = resolve_ingestion_start_block(
        config.start_block,
        existing_cursor_state.as_ref(),
        &endpoint_info,
        args.cursor_override,
    )?;
    config.stop_block = resolve_ingestion_stop_block(config.stop_block)?;
    firehose_parquet::cli::validate_stop_block_after_start(config.start_block, config.stop_block)?;
    debug!(
        requested_start_block = ?args.common.start_block,
        resolved_start_block = ?config.start_block,
        resolved_stop_block = ?config.stop_block,
        cursor_override = args.cursor_override,
        has_cursor = existing_cursor_state.is_some(),
        "resolved ingestion bounds"
    );
    if let firehose_parquet::config::Partition::BlockRange { size, .. } = &mut config.partition {
        let explicit_start_block = args.common.start_block;
        let effective_start_block = config.start_block;
        validate_block_range_alignment(
            explicit_start_block,
            effective_start_block,
            config.stop_block,
            *size,
        )?;
        config
            .partition
            .set_block_range_start(effective_start_block);
    }

    if solana_chain {
        extended = false;
        log_solana_vote_mode(with_votes);
    } else if antelope_chain {
        extended = false;
    } else if known_non_solana_chain {
        extended = resolve_extended_mode(extended, args.without_extended, &endpoint_info);
    }

    if !config.dry_run && block_type != "evm" {
        extended = false;
    }

    let tron_style_evm_profile = endpoint_uses_tron_style_evm_profile(&endpoint_info);
    let initial_block_type = if block_type != "auto" {
        Some(block_type.as_str())
    } else {
        inferred_block_type_from_endpoint_info(&endpoint_info)
    };
    // Failed-transaction handling depends on the chain, so resolve it from the
    // best block type known before streaming; auto-detection re-resolves it.
    let failed_transactions_block_type = initial_block_type
        .map(str::to_string)
        .or_else(|| cursor_metadata_block_type(existing_cursor_state.as_ref()).map(str::to_string));
    let (mut include_failed_transactions, failed_transactions_warnings) =
        resolve_include_failed_transactions(
            failed_transactions_block_type.as_deref(),
            args.include_failed_transactions,
            args.exclude_failed_transactions,
            existing_cursor_state.as_ref(),
            args.cursor_override,
        );
    for warning in failed_transactions_warnings {
        warn!("{}", warning);
    }
    let initial_bytes_encoding =
        resolve_output_bytes_encoding(initial_block_type, &endpoint_info, tron_style_evm_profile);
    let initial_bytes_encoding_label = encode_bytes_label(&initial_bytes_encoding).to_string();

    info!(
        block_type,
        extended,
        with_votes,
        bytes_encoding = %initial_bytes_encoding_label,
        include_failed_transactions,
        "starting pipeline\n{config}"
    );

    if let Some(cursor_state) = existing_cursor_state.as_ref() {
        if args.cursor_override {
            info!(
                stored_cursor_last_block_num = cursor_state.last_block_num,
                requested_start_block = ?config.start_block,
                requested_stop_block = ?config.stop_block,
                live,
                "cursor override enabled, restarting from CLI-provided/default bounds"
            );
        } else {
            info!(
                stored_cursor_last_block_num = cursor_state.last_block_num,
                requested_start_block = ?config.start_block,
                requested_stop_block = ?config.stop_block,
                live,
                "resuming from stored cursor"
            );
        }
    } else {
        info!(
            requested_start_block = ?config.start_block,
            requested_stop_block = ?config.stop_block,
            live,
            "starting without stored cursor"
        );
    }

    // Initialize Prometheus metrics if --metrics-port is set.
    let (mut metrics_registry, pipeline_metrics) = metrics::init();
    let _pipeline_activity = pipeline_metrics.begin_pipeline();
    pipeline_metrics
        .set_readiness_timeout(Duration::from_secs(args.common.metrics_stale_after_secs));
    if let Some(cursor) = existing_cursor_state.as_ref() {
        pipeline_metrics
            .cursor_last_block_num
            .set(i64::try_from(cursor.last_block_num).unwrap_or(i64::MAX));
    }

    // Register the info metric with endpoint metadata labels.
    {
        let mut labels = vec![
            ("endpoint".to_string(), config.endpoint.clone()),
            ("partition".to_string(), config.partition.to_string()),
            ("compression".to_string(), config.compression.to_string()),
            (
                "bytes_encoding".to_string(),
                initial_bytes_encoding_label.clone(),
            ),
            ("extended".to_string(), extended.to_string()),
            ("with_votes".to_string(), with_votes.to_string()),
            (
                "final_blocks_only".to_string(),
                config.final_blocks_only.to_string(),
            ),
            ("version".to_string(), env!("CARGO_PKG_VERSION").to_string()),
            (
                "block_start".to_string(),
                config
                    .start_block
                    .map_or("N/A".to_string(), |n| n.to_string()),
            ),
            (
                "block_end".to_string(),
                config
                    .stop_block
                    .map_or("N/A".to_string(), |n| n.to_string()),
            ),
            (
                "block_restarted_at".to_string(),
                existing_cursor_state
                    .as_ref()
                    .map_or("N/A".to_string(), |cs| cs.last_block_num.to_string()),
            ),
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
        metrics::serve(
            Arc::clone(&metrics_registry),
            pipeline_metrics.clone(),
            port,
        );
    }

    // Pass metrics to the gRPC client for reconnect tracking.
    // Metadata/default resolution may have supplied an original start that was
    // absent when the startup Info client was built. Blocks uses the resolved request.
    client = FirehoseClient::new(config.clone())?;
    client.set_metrics(pipeline_metrics.clone());

    let final_blocks_only = config.final_blocks_only;
    let include_fork_step = !final_blocks_only;
    let flush_rows = config.flush_rows.map(|r| r as usize);
    let flush_blocks = config.flush_blocks;
    let mut flush_sizing = FlushSizing::new(config.flush_bytes, config.flush_memory_bytes)?;
    let flush_interval_secs = config.flush_interval_secs;
    let dry_run = config.dry_run;
    let mut is_solana = block_type == "solana";
    let partition_config = config.partition.clone();
    let mut use_synthetic_partition_routing =
        use_last_known_timestamp_partition_routing(&block_type, &partition_config);
    let mut genesis_timestamp_bootstrap = GenesisTimestampBootstrap::new(config.start_block);
    let mut timestamp_backfill = TimestampBackfill::new(
        use_synthetic_partition_routing,
        DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES,
    );
    if block_type != "auto" {
        restore_sparse_routing_cursor_anchor(
            &mut timestamp_backfill,
            existing_cursor_state.as_ref(),
            args.cursor_override,
            &block_type,
            &partition_config,
        );
    }

    // If block type is known upfront, resolve encode_bytes and create mapper immediately.
    // If "auto", defer until first block arrives.
    let mut current_file_metadata = ParquetFileMetadata::new();
    let mut mapper: Option<Box<dyn BlockMapper>> = if block_type != "auto" {
        let encode_bytes = initial_bytes_encoding.clone();
        let meta = build_file_metadata(
            &block_type,
            &encode_bytes,
            &config.endpoint,
            config.compression,
            &endpoint_info,
        );
        let mut meta = meta;
        maybe_add_solana_with_votes_metadata(&mut meta, Some(&block_type), with_votes);
        maybe_add_synthetic_timestamp_metadata(
            &mut meta,
            &block_type,
            use_synthetic_partition_routing,
        );
        log_file_metadata(&meta);
        current_file_metadata = meta;
        Some(create_mapper(
            &block_type,
            extended,
            with_votes,
            include_fork_step,
            encode_bytes,
            use_synthetic_partition_routing,
            include_failed_transactions,
        )?)
    } else {
        None
    };

    let mut session = if let Some(owner) = &ownership {
        let mapper = mapper
            .as_mut()
            .context("protected ingestion requires a mapper before recovery")?;
        let empty_batches = mapper.flush()?;
        let tables = declare_inventory(&empty_batches, &mapper.table_names())?;
        let semantics = MapperSemantics {
            chain: endpoint_info
                .as_ref()
                .context("endpoint identity is required")?
                .chain_name
                .clone(),
            family: protected_block_family(&block_type)?,
            bytes_encoding: initial_bytes_encoding_label.clone(),
            extended,
            with_votes,
            include_failed_transactions,
            tables,
        };
        Some(
            IngestionSession::open(
                &config,
                semantics,
                owner,
                Some(&pipeline_metrics),
                Some(&cursor_shutdown),
            )
            .await?,
        )
    } else {
        None
    };
    if let Some(session) = &session {
        if let Some((source_block, seconds)) = session.routing_anchor_source() {
            timestamp_backfill.restore_anchor(source_block, seconds);
        }
    }
    let already_complete = match (&session, config.stop_block) {
        (Some(session), Some(stop)) => session.request_already_complete(stop)?,
        _ => false,
    };
    if already_complete {
        info!(stop_block=?config.stop_block, "requested range is already complete; recovered authority and cursor mirror without opening Blocks");
        drop(session);
        if let Some(owner) = ownership.take() {
            owner.release().await?;
        }
        return Ok(());
    }

    let mut blocks_observed: u64 = 0;
    let mut blocks_processed: u64 = 0;
    let mut transactions_processed: u64 = 0;
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;
    let mut min_timestamp: Option<i64> = None;
    let mut max_timestamp: Option<i64> = None;
    // Global accumulators (not reset on flush) for final summary.
    let mut global_min_block: Option<u64> = None;
    let mut global_max_block: Option<u64> = None;
    let mut blocks_since_flush: u64 = 0;
    let mut last_flush_time = Instant::now();
    let mut bytes_read: u64 = 0;
    let mut buffered_bootstrap_blocks: Vec<BufferedBootstrapBlock> = Vec::new();
    let progress_start = Instant::now();
    let mut current_partition_key: Option<String> = None;
    let mut start_block_filter = StartBlockFilter::new(config.start_block);
    // Chain resolved for this run (set once the mapper exists in auto mode).
    let mut resolved_block_type: Option<String> =
        (block_type != "auto").then(|| block_type.clone());

    // Build file-level metadata for the cursor (same `firehose-parquet.*`
    // namespace as table files). Includes version, endpoint, chain info, and
    // pipeline parameters.
    let initial_cursor_encoding = Some(initial_bytes_encoding.clone());
    let cursor_file_metadata = build_cursor_file_metadata(
        if block_type != "auto" {
            Some(block_type.as_str())
        } else {
            initial_block_type
        },
        initial_cursor_encoding.as_ref(),
        &config.endpoint,
        config.compression,
        &config.partition,
        &endpoint_info,
        extended,
        config.final_blocks_only,
        include_failed_transactions,
    );
    let mut cursor_file_metadata = cursor_file_metadata;
    if solana_chain {
        maybe_add_solana_with_votes_metadata(&mut cursor_file_metadata, Some("solana"), with_votes);
    }

    // Build a template CursorState with pipeline parameters that stay constant.
    let mut cursor_state_template = CursorState {
        start_block: config.start_block,
        stop_block: config.stop_block,
        extended,
        final_blocks_only: config.final_blocks_only,
        include_failed_transactions,
        file_metadata: cursor_file_metadata,
        ..CursorState::default()
    };

    // Validate cursor parameters against current CLI arguments.
    if let Some(loaded) = existing_cursor_state.as_ref().filter(|_| dry_run) {
        let mut mismatches = loaded.validate_params(&cursor_state_template);
        mismatches.retain(|mismatch| !mismatch.starts_with("stop_block:"));
        if solana_chain {
            apply_solana_cursor_feature_validation(&mut mismatches, loaded, with_votes);
        } else if antelope_chain {
            apply_antelope_cursor_feature_validation(&mut mismatches);
        }
        if !mismatches.is_empty() {
            if args.cursor_override {
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

    let resume_cursor = match &session {
        Some(session) => session.resume_cursor().map(str::to_owned),
        None => stream_resume_cursor(existing_cursor_state.as_ref(), args.cursor_override),
    };
    let stream_result = client
        .stream_blocks(resume_cursor, &shutdown, |block_bytes, type_url, cursor_str, identity: BlockIdentity, step: i32| {
            let received_ordinal = if let Some(session) = session.as_mut() {
                let family = protected_block_family(&detect_block_type(&type_url)?)?;
                session.receive(cursor_str.clone(), &identity, step, family)?
            } else { 0 };
            let fork_step_str = fork_step_name(step);
            if final_blocks_only && step == 2 {
                if let Some(session) = session.as_mut() { session.accept_filtered(received_ordinal)?; }
                return Ok(());
            }
            if !start_block_filter.admit(identity.block_num) {
                pipeline_metrics.blocks_skipped_below_start_total.inc();
                if let Some(session) = session.as_mut() { session.accept_filtered(received_ordinal)?; }
                return Ok(());
            }

            // Lazy mapper creation for "auto" mode.
            if mapper.is_none() {
                let detected = detect_block_type(&type_url)?;
                info!(detected_type = %detected, type_url = %type_url, "auto-detected block type");
                for warning in unsupported_chain_feature_flag_warnings(
                    &detected,
                    &endpoint_info,
                    existing_cursor_state.as_ref(),
                    args.without_extended,
                    args.without_votes,
                ) {
                    warn!("{}", warning);
                }
                if detected == "solana" {
                    extended = false;
                    log_solana_vote_mode(with_votes);
                } else if detected == "antelope" {
                    extended = false;
                } else {
                    extended = resolve_extended_mode(extended, args.without_extended, &endpoint_info);
                }
                if failed_transactions_block_type.as_deref() != Some(detected.as_str()) {
                    let (resolved, warnings) = resolve_include_failed_transactions(
                        Some(&detected),
                        args.include_failed_transactions,
                        args.exclude_failed_transactions,
                        existing_cursor_state.as_ref(),
                        args.cursor_override,
                    );
                    for warning in warnings {
                        warn!("{}", warning);
                    }
                    include_failed_transactions = resolved;
                    cursor_state_template.include_failed_transactions = resolved;
                }
                let encode_bytes = resolve_output_bytes_encoding(
                    Some(&detected),
                    &endpoint_info,
                    tron_style_evm_profile,
                );
                let meta = build_file_metadata(
                    &detected,
                    &encode_bytes,
                    &config.endpoint,
                    config.compression,
                    &endpoint_info,
                );
                let mut meta = meta;
                maybe_add_solana_with_votes_metadata(&mut meta, Some(&detected), with_votes);
                let detected_uses_synthetic_partition_routing =
                    use_last_known_timestamp_partition_routing(&detected, &partition_config);
                maybe_add_synthetic_timestamp_metadata(
                    &mut meta,
                    &detected,
                    detected_uses_synthetic_partition_routing,
                );
                log_file_metadata(&meta);
                current_file_metadata = meta;
                let mut cursor_meta = build_cursor_file_metadata(
                    Some(&detected),
                    Some(&encode_bytes),
                    &config.endpoint,
                    config.compression,
                    &config.partition,
                    &endpoint_info,
                    extended,
                    config.final_blocks_only,
                    include_failed_transactions,
                );
                maybe_add_solana_with_votes_metadata(&mut cursor_meta, Some(&detected), with_votes);
                cursor_state_template.file_metadata = cursor_meta;
                cursor_state_template.extended = extended;
                is_solana = detected == "solana";
                resolved_block_type = Some(detected.clone());
                use_synthetic_partition_routing = detected_uses_synthetic_partition_routing;
                timestamp_backfill = TimestampBackfill::new(
                    use_synthetic_partition_routing,
                    DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES,
                );
                restore_sparse_routing_cursor_anchor(
                    &mut timestamp_backfill,
                    existing_cursor_state.as_ref(),
                    args.cursor_override,
                    &detected,
                    &partition_config,
                );
                mapper = Some(create_mapper(
                    &detected,
                    extended,
                    with_votes,
                    include_fork_step,
                    encode_bytes,
                    use_synthetic_partition_routing,
                    include_failed_transactions,
                )?);
            }

            let m = mapper.as_mut().unwrap();

            let block_number = identity.block_num;
            let ts = identity.timestamp;
            blocks_observed += 1;
            let fork_step_owned = fork_step_str.map(str::to_owned);
            let mut ready_solana_blocks = if is_solana && use_synthetic_partition_routing {
                timestamp_backfill.observe_block(
                    block_bytes,
                    cursor_str,
                    fork_step_owned,
                    identity,
                )?
            } else {
                vec![BufferedBootstrapBlock {
                    received_ordinal: 0,
                    block_bytes,
                    cursor: cursor_str,
                    fork_step: fork_step_owned,
                    identity,
                }]
            };
            let resumed_bootstrap_timestamp = (!is_solana && ts == 0)
                .then(|| session.as_ref().and_then(IngestionSession::routing_timestamp_hint))
                .flatten();
            for block in &mut ready_solana_blocks {
                block.received_ordinal = received_ordinal;
                if let Some(seconds) = resumed_bootstrap_timestamp { block.identity.timestamp = seconds; }
            }
            let current_anchor_timestamp = ready_solana_blocks
                .last()
                .map(|block| block.identity.timestamp)
                .unwrap_or(ts);
            if ready_solana_blocks.is_empty() {
                if should_emit_progress_log(blocks_observed) {
                    let elapsed_secs = progress_start.elapsed().as_secs_f64();
                    let observed_blocks_per_sec = if elapsed_secs > 0.0 {
                        blocks_observed as f64 / elapsed_secs
                    } else {
                        0.0
                    };
                    let timestamp = format_optional_probe_timestamp(current_anchor_timestamp);
                    match timestamp.as_deref() {
                        Some(timestamp) => info!(
                            blocks_observed,
                            blocks = blocks_processed,
                            block_num = block_number,
                            timestamp,
                            buffered_blocks = timestamp_backfill.buffered_blocks_len(),
                            buffered_bytes = firehose_parquet::cli::format_bytes(timestamp_backfill.buffered_bytes()),
                            blocks_per_sec = format!("{:.0}", observed_blocks_per_sec),
                            "progress (buffering timestamps)"
                        ),
                        None => info!(
                            blocks_observed,
                            blocks = blocks_processed,
                            block_num = block_number,
                            buffered_blocks = timestamp_backfill.buffered_blocks_len(),
                            buffered_bytes = firehose_parquet::cli::format_bytes(timestamp_backfill.buffered_bytes()),
                            blocks_per_sec = format!("{:.0}", observed_blocks_per_sec),
                            "progress (buffering timestamps)"
                        ),
                    }
                }
                return Ok(());
            }
            // For Solana, blocks may legitimately lack timestamps — skip the
            // genesis bootstrap and timestamp validation entirely.
            if !is_solana && resumed_bootstrap_timestamp.is_none() {
                match genesis_timestamp_bootstrap.observe_block(blocks_processed, block_number, ts) {
                    GenesisTimestampBootstrapAction::Buffer => {
                        if genesis_timestamp_bootstrap.buffered_blocks == 1 {
                            warn!(
                                requested_start_block = ?config.start_block,
                                block_number,
                                "buffering first streamable block because it lacks timestamp metadata; its timestamp will be synthesized from the first later timestamped block automatically"
                            );
                        }
                        buffered_bootstrap_blocks.push(BufferedBootstrapBlock {
                            received_ordinal,
                            block_bytes: ready_solana_blocks[0].block_bytes.clone(),
                            cursor: ready_solana_blocks[0].cursor.clone(),
                            fork_step: ready_solana_blocks[0].fork_step.clone(),
                            identity: ready_solana_blocks[0].identity.clone(),
                        });
                        update_bootstrap_buffer_metrics(&pipeline_metrics, &buffered_bootstrap_blocks);
                        return Ok(());
                    }
                    GenesisTimestampBootstrapAction::Anchored {
                        anchor_block,
                        buffered_blocks,
                        first_buffered_block,
                    } => {
                        info!(
                            first_buffered_block,
                            anchor_block,
                            buffered_blocks,
                            "preserving buffered bootstrap block(s) with a synthesized timestamp from the first later timestamped block"
                        );
                    }
                    GenesisTimestampBootstrapAction::None => {}
                }
            } // end if !is_solana
            let mut process_block = |block_bytes: &[u8],
                                     identity: &BlockIdentity,
                                     fork_step: Option<&str>,
                                     _cursor: &str,
                                     received_ordinal: u64,
                                     lookahead_ordinal: Option<u64>|
             -> Result<()> {
                let block_number = identity.block_num;
                let ts = identity.timestamp;
                // Solana blocks may have no timestamp; skip validation for Solana.
                if !is_solana {
                    validate_block_timestamp(block_number, ts, &partition_config)?;
                }
                let has_timestamp = ts != 0;

                // Flush the mapper at partition boundaries to ensure each flush
                // produces batches belonging to exactly one partition.
                // See: https://github.com/pinax-network/firehose-parquet/issues/110
                let new_partition_key = partition_config.partition_key(block_number, ts)?;
                if let Some(ref new_key) = new_partition_key {
                    let partition_changed = current_partition_key
                        .as_ref()
                        .map_or(false, |cur| cur != new_key);
                    if partition_changed && (m.max_table_rows() > 0 || session.as_ref().map(IngestionSession::has_accepted).transpose()?.unwrap_or(false)) {
                        info!(
                            old_partition = %current_partition_key.as_deref().unwrap_or("?"),
                            new_partition = %new_key,
                            block_number,
                            "partition boundary detected, flushing mapper"
                        );
                        let preflush_estimate = update_mapper_buffer_metrics(&pipeline_metrics, m.as_mut());
                        let batches = m.flush()?;
                        update_mapper_buffer_metrics(&pipeline_metrics, m.as_mut());
                        let flushed_tables = batches.len();
                        let flushed_rows: usize = batches.values().map(|batch| batch.num_rows()).sum();
                        info!(
                            trigger = "partition_boundary",
                            old_partition = %current_partition_key.as_deref().unwrap_or("?"),
                            new_partition = %new_key,
                            block_number,
                            tables = flushed_tables,
                            rows = flushed_rows,
                            "mapper flush emitted record batches"
                        );
                        if !dry_run {
                            let metadata = BlockMetadata {
                                min_block_number: min_block.unwrap_or(0), max_block_number: max_block.unwrap_or(0), min_timestamp, max_timestamp,
                            };
                            let outcome = commit_ingestion_flush(session.as_mut().context("protected session is required")?, batches, metadata, config.compression, current_file_metadata.clone(), &pipeline_metrics, &mut flush_sizing, preflush_estimate, "partition_boundary")?;
                            log_writer_flush_outcome("partition_boundary", flushed_tables, flushed_rows, outcome);
                        } else {
                            info!(
                                trigger = "partition_boundary",
                                tables = flushed_tables,
                                rows = flushed_rows,
                                "dry run mapper flush skipped parquet writes"
                            );
                        }
                        min_block = None;
                        max_block = None;
                        min_timestamp = None;
                        max_timestamp = None;
                        blocks_since_flush = 0;
                        last_flush_time = Instant::now();
                    }
                }
                current_partition_key = new_partition_key;

                let mapped = m.map_block(block_bytes, identity, fork_step);
                let mapper_estimate = update_mapper_buffer_metrics(&pipeline_metrics, m.as_mut());
                transactions_processed += mapped?;
                if let Some(session) = session.as_mut() {
                    session.accept_mapped(received_ordinal, (ts != 0).then_some(ts), lookahead_ordinal)?;
                }

                // Only count the block in the file metadata once it is mapped.
                min_block = Some(min_block.map_or(block_number, |s: u64| s.min(block_number)));
                max_block = Some(max_block.map_or(block_number, |s: u64| s.max(block_number)));
                global_min_block = Some(global_min_block.map_or(block_number, |s: u64| s.min(block_number)));
                global_max_block = Some(global_max_block.map_or(block_number, |s: u64| s.max(block_number)));
                if has_timestamp {
                    min_timestamp = Some(min_timestamp.map_or(ts, |s: i64| s.min(ts)));
                    max_timestamp = Some(max_timestamp.map_or(ts, |s: i64| s.max(ts)));
                }

                blocks_processed += 1;
                blocks_since_flush += 1;
                bytes_read += block_bytes.len() as u64;

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
                if shutdown.is_cancelled() {
                    info!(blocks_processed, block_number, "shutdown requested, breaking out of stream");
                    return Err(ShutdownRequested.into());
                }

                if should_emit_progress_log(blocks_processed) {
                    let elapsed_secs = progress_start.elapsed().as_secs_f64();
                    let blocks_per_sec = if elapsed_secs > 0.0 {
                        blocks_processed as f64 / elapsed_secs
                    } else {
                        0.0
                    };
                    let timestamp = format_optional_probe_timestamp(ts);
                    match (timestamp.as_deref(), current_partition_key.as_deref()) {
                        (Some(timestamp), Some(partition)) => info!(
                            blocks = blocks_processed,
                            transactions = transactions_processed,
                            block_num = block_number,
                            timestamp,
                            partition,
                            blocks_per_sec = format!("{:.0}", blocks_per_sec),
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            "progress"
                        ),
                        (Some(timestamp), None) => info!(
                            blocks = blocks_processed,
                            transactions = transactions_processed,
                            block_num = block_number,
                            timestamp,
                            blocks_per_sec = format!("{:.0}", blocks_per_sec),
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            "progress"
                        ),
                        (None, Some(partition)) => info!(
                            blocks = blocks_processed,
                            transactions = transactions_processed,
                            block_num = block_number,
                            partition,
                            blocks_per_sec = format!("{:.0}", blocks_per_sec),
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            "progress"
                        ),
                        (None, None) => info!(
                            blocks = blocks_processed,
                            transactions = transactions_processed,
                            block_num = block_number,
                            blocks_per_sec = format!("{:.0}", blocks_per_sec),
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            "progress"
                        ),
                    }

                }

                if let Some(flush_trigger) = next_mapper_flush_trigger(
                    flush_rows,
                    m.max_table_rows(),
                    flush_blocks,
                    blocks_since_flush,
                    flush_interval_secs,
                    last_flush_time,
                    &flush_sizing,
                    mapper_estimate,
                ) {
                    let flush_trigger = flush_trigger.as_str();
                    let batches = m.flush()?;
                    update_mapper_buffer_metrics(&pipeline_metrics, m.as_mut());
                    let flushed_tables = batches.len();
                    let flushed_rows: usize = batches.values().map(|batch| batch.num_rows()).sum();
                    info!(
                        trigger = flush_trigger,
                        tables = flushed_tables,
                        rows = flushed_rows,
                        "mapper flush emitted record batches"
                    );
                    if !dry_run {
                        let metadata = BlockMetadata {
                            min_block_number: min_block.unwrap_or(0), max_block_number: max_block.unwrap_or(0), min_timestamp, max_timestamp,
                        };
                        let outcome = commit_ingestion_flush(session.as_mut().context("protected session is required")?, batches, metadata, config.compression, current_file_metadata.clone(), &pipeline_metrics, &mut flush_sizing, mapper_estimate, flush_trigger)?;
                        log_writer_flush_outcome(flush_trigger, flushed_tables, flushed_rows, outcome);
                    } else {
                        info!(
                            trigger = flush_trigger,
                            tables = flushed_tables,
                            rows = flushed_rows,
                            "dry run mapper flush skipped parquet writes"
                        );
                    }
                    min_block = None;
                    max_block = None;
                    min_timestamp = None;
                    max_timestamp = None;
                    blocks_since_flush = 0;
                    last_flush_time = Instant::now();
                }

                Ok(())
            };

            let anchored_blocks = take_anchored_bootstrap_blocks(
                &mut buffered_bootstrap_blocks,
                current_anchor_timestamp,
            );
            update_bootstrap_buffer_metrics(&pipeline_metrics, &buffered_bootstrap_blocks);
            for buffered_block in anchored_blocks {
                process_block(
                    &buffered_block.block_bytes,
                    &buffered_block.identity,
                    buffered_block.fork_step.as_deref(),
                    &buffered_block.cursor,
                    buffered_block.received_ordinal,
                    Some(received_ordinal),
                )?;
            }

            for ready_block in ready_solana_blocks {
                process_block(
                    &ready_block.block_bytes,
                    &ready_block.identity,
                    ready_block.fork_step.as_deref(),
                    &ready_block.cursor,
                    ready_block.received_ordinal,
                    None,
                )?;
            }

            Ok(())
        })
        .await;

    // Distinguish graceful shutdown from real errors.
    let exit = StreamExit::from_result(&stream_result);
    match &stream_result {
        Err(e) if exit == StreamExit::Failed => {
            warn!(error = %e, "stream ended with error, discarding buffered data without advancing the cursor");
        }
        Err(_) => info!("graceful shutdown initiated"),
        Ok(()) => {}
    }

    // Only a completed stream writes partial buffers. On graceful shutdown
    // they are discarded to avoid non-deterministic extra part files. On an
    // error they are discarded so the cursor is never saved past rows whose
    // write (or mapping) failed. Either way only complete partitions that were
    // already flushed during normal processing are preserved, and on restart
    // the stream resumes from the last saved cursor, which corresponds to the
    // last fully-written flush.
    if !exit.materializes_buffers() {
        info!(
            mapper_buffered_rows = mapper
                .as_ref()
                .map(|mapper| mapper.total_rows())
                .unwrap_or(0),
            "stream stopped; discarded uncommitted buffers and retained the authoritative prefix"
        );
    } else if let Some(mapper) = mapper.as_mut() {
        anyhow::ensure!(
            timestamp_backfill.drain_open_span()?.is_empty(),
            "unresolved timestamp routing remains at clean EOF"
        );
        if mapper.max_table_rows() > 0
            || session
                .as_ref()
                .map(IngestionSession::has_accepted)
                .transpose()?
                .unwrap_or(false)
        {
            let preflush_estimate =
                update_mapper_buffer_metrics(&pipeline_metrics, mapper.as_mut());
            let batches = mapper.flush()?;
            update_mapper_buffer_metrics(&pipeline_metrics, mapper.as_mut());
            if let Some(session) = session.as_mut() {
                let metadata = BlockMetadata {
                    min_block_number: min_block.unwrap_or(0),
                    max_block_number: max_block.unwrap_or(0),
                    min_timestamp,
                    max_timestamp,
                };
                if let Some(committed) = session
                    .flush(
                        batches,
                        metadata,
                        config.compression,
                        current_file_metadata.clone(),
                    )
                    .await?
                {
                    record_committed_flush_sizing(
                        &mut flush_sizing,
                        preflush_estimate,
                        &committed,
                        "stream_end",
                    );
                    pipeline_metrics
                        .flushes_total
                        .get_or_create(&metrics::FlushLabels {
                            trigger: "stream_end".into(),
                        })
                        .inc();
                }
            }
        }
    }

    if exit == StreamExit::Completed && genesis_timestamp_bootstrap.enabled {
        if let Some(first_buffered_block) = genesis_timestamp_bootstrap.first_buffered_block {
            return Err(missing_genesis_timestamp_bootstrap_error(
                first_buffered_block,
                genesis_timestamp_bootstrap.buffered_blocks,
            ));
        }
    }

    if let (StreamExit::Completed, Some(stop)) = (exit, config.stop_block) {
        if let Some(session) = session.as_mut() {
            session.complete_request(stop, true).await?;
        } else {
            let resumed_block_num =
                stream_resume_cursor(existing_cursor_state.as_ref(), args.cursor_override)
                    .and(existing_cursor_state.as_ref())
                    .map(|state| state.last_block_num);
            let gaps_allowed = resolved_block_type
                .as_deref()
                .or(initial_block_type)
                .is_some_and(block_type_allows_block_number_gaps);
            ensure_bounded_stream_reached_stop(
                stop,
                global_max_block.max(resumed_block_num),
                gaps_allowed,
            )?;
        }
    }

    if exit == StreamExit::Completed && !dry_run {
        if let Some(message) = firehose_parquet::cli::non_final_bounded_warning(
            config.final_blocks_only,
            config.stop_block,
        ) {
            warn!("{message}");
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
        blocks_skipped_below_start = start_block_filter.skipped,
        block_range = %block_range,
        elapsed = %elapsed_display,
        bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
        speed = format!("{}/s | {:.0} blocks/s", firehose_parquet::cli::format_bytes(speed_per_sec as u64), blocks_per_sec),
        "pipeline finished",
    );

    // Propagate real (non-shutdown) errors so the process exits non-zero.
    if exit == StreamExit::Failed {
        return stream_result;
    }

    drop(session);

    // All synchronous writes have resolved by this point. Any earlier error or
    // cancellation of this future drops the guard and retains remote ownership.
    if let Some(ownership) = ownership {
        ownership.release().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Builder;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use clap::CommandFactory;
    use firehose_parquet::cursor::CursorLocation;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn protected_inventory_declares_every_effective_mapper_schema_before_receipt() {
        for family in BLOCK_TYPES
            .iter()
            .copied()
            .filter(|family| *family != "auto")
        {
            for encoding in [
                EncodeBytes::Binary,
                EncodeBytes::Hex,
                EncodeBytes::HexNoPrefix,
                EncodeBytes::Base58,
                EncodeBytes::TronBase58,
            ] {
                for fork_steps in [false, true] {
                    for feature in [false, true] {
                        let mut mapper = create_mapper(
                            family,
                            feature,
                            feature,
                            fork_steps,
                            encoding.clone(),
                            family == "solana" && feature,
                            feature,
                        )
                        .unwrap();
                        let first = mapper.flush().unwrap();
                        let inventory = declare_inventory(&first, &mapper.table_names())
                            .unwrap_or_else(|error| panic!("{family} empty inventory: {error}"));
                        assert!(!inventory.is_empty());
                        let second = mapper.flush().unwrap();
                        assert_eq!(
                            inventory,
                            declare_inventory(&second, &mapper.table_names()).unwrap(),
                            "{family} empty schema changes between flushes"
                        );
                    }
                }
            }
        }
    }

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new(
                "timestamp",
                firehose_parquet::traits::timestamp_millis_utc_type(),
                false,
            ),
        ]));
        let mut builder = UInt64Builder::new();
        builder.append_value(42);
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(builder.finish()),
                Arc::new(
                    arrow::array::TimestampMillisecondArray::from(vec![1_705_320_000_000])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap()
    }

    fn make_test_batches() -> HashMap<String, RecordBatch> {
        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), make_test_batch());
        batches
    }

    fn make_temp_output_dir() -> PathBuf {
        // SystemTime has microsecond resolution on macOS, so tests starting in
        // the same microsecond got the same path; the counter keeps them apart.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fireparq-partition-boundary-test-{}-{}-{}",
            std::process::id(),
            unique,
            sequence
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn test_empty_mapper_flush_does_not_materialize() {
        let dir = make_temp_output_dir();
        let mut writer = OutputWriter::new(&dir, Partition::Date, Compression::None, u64::MAX);
        let outcome = write_mapper_flush(
            &mut writer,
            &HashMap::new(),
            &BlockMetadata {
                min_block_number: 0,
                max_block_number: 0,
                min_timestamp: None,
                max_timestamp: None,
            },
        )
        .unwrap();
        assert!(!outcome.materialized);
        assert_eq!(outcome.buffered, WriterBufferStats::default());
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_write_mapper_flush_materializes_partition_boundary() {
        let dir = make_temp_output_dir();
        let batches = make_test_batches();
        let metadata = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705320000),
            max_timestamp: Some(1705320000),
        };
        let mut writer = OutputWriter::new(&dir, Partition::Date, Compression::None, 1_000_000);

        let outcome = write_mapper_flush(&mut writer, &batches, &metadata).unwrap();

        assert!(outcome.materialized);
        assert_eq!(outcome.buffered, WriterBufferStats::default());
        assert!(
            dir.join("blocks/year=2024/month=01/day=15").exists(),
            "partition-boundary materialization should write the old partition immediately"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_signal_child_fixture() {
        if std::env::var("FIREPARQ_SHUTDOWN_TEST_CHILD").as_deref() != Ok("1") {
            return;
        }
        let shutdown = CancellationToken::new();
        let cursor_shutdown = Arc::new(AtomicBool::new(false));
        spawn_ingestion_shutdown_handler(shutdown.clone(), Arc::clone(&cursor_shutdown));
        println!("signal-handlers-ready");
        shutdown.cancelled().await;
        assert!(cursor_shutdown.load(Ordering::SeqCst));
        println!("first-signal-cancelled-both-waits");
        // Model a current operation still finishing after the first signal.
        std::future::pending::<()>().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_second_shutdown_signal_force_exits_after_first_cancellation() {
        use tokio::io::AsyncBufReadExt;
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .kill_on_drop(true)
            .env_clear()
            .env("FIREPARQ_SHUTDOWN_TEST_CHILD", "1")
            .args([
                "--exact",
                "tests::shutdown_signal_child_fixture",
                "--nocapture",
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap();
        let pid = child.id().unwrap().to_string();
        let mut lines = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
        for (marker, signal) in [
            ("signal-handlers-ready", "-TERM"),
            ("first-signal-cancelled-both-waits", "-INT"),
        ] {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let line = lines
                        .next_line()
                        .await
                        .unwrap()
                        .expect("child stopped before signal marker");
                    if line.contains(marker) {
                        break;
                    }
                }
            })
            .await
            .expect("signal handler child must reach the marker");
            assert!(tokio::process::Command::new("/bin/kill")
                .args([signal, &pid])
                .status()
                .await
                .unwrap()
                .success());
        }
        let status = tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .expect("second signal must force exit promptly")
            .unwrap();
        assert_eq!(status.code(), Some(130));
    }

    #[test]
    fn test_stream_exit_only_materializes_buffers_after_completed_stream() {
        assert_eq!(StreamExit::from_result(&Ok(())), StreamExit::Completed);
        assert_eq!(
            StreamExit::from_result(&Err(ShutdownRequested.into())),
            StreamExit::Shutdown
        );
        assert_eq!(
            StreamExit::from_result(&Err(anyhow!("storage path contains __shutdown__"))),
            StreamExit::Failed,
        );
        assert_eq!(
            StreamExit::from_result(&Err(anyhow!("uploading to S3: logs/part.parquet"))),
            StreamExit::Failed
        );

        assert!(StreamExit::Completed.materializes_buffers());
        assert!(!StreamExit::Shutdown.materializes_buffers());
        assert!(!StreamExit::Failed.materializes_buffers());
    }

    #[test]
    fn test_failed_table_write_does_not_save_cursor_on_exit() {
        let dir = make_temp_output_dir();
        let output = dir.join("output");
        let cursor_path = dir.join("cursor.parquet");
        let cursor_location = CursorLocation::Local(cursor_path.clone());
        let (_registry, pipeline_metrics) = metrics::init();
        let metadata = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705320000),
            max_timestamp: Some(1705320000),
        };
        let mut batches = make_test_batches();
        batches.insert("logs".to_string(), make_test_batch());
        let mut writer = OutputWriter::new(&output, Partition::None, Compression::None, 0);

        // A mapper flush whose `logs` write fails ends the stream with an error.
        std::fs::create_dir_all(&output).unwrap();
        std::fs::write(output.join("logs"), b"").unwrap();
        let stream_result = write_mapper_flush(&mut writer, &batches, &metadata).map(|_| ());
        let exit = StreamExit::from_result(&stream_result);
        assert_eq!(exit, StreamExit::Failed);

        let save_cursor = || {
            cursor_location.save(&CursorState {
                cursor: "cursor-at-block-200".to_string(),
                last_block_num: 200,
                ..CursorState::default()
            })
        };
        let committed =
            flush_writer_on_exit(exit, &mut writer, false, &pipeline_metrics, save_cursor).unwrap();
        assert!(!committed);
        assert!(
            !cursor_path.exists(),
            "the error exit path must not save the cursor past the failed rows"
        );
        assert!(
            writer.buffered_stats().rows >= 1,
            "the failed table's rows stay buffered instead of being dropped"
        );

        // The same final flush on a completed stream writes and commits.
        std::fs::remove_file(output.join("logs")).unwrap();
        let committed = flush_writer_on_exit(
            StreamExit::Completed,
            &mut writer,
            false,
            &pipeline_metrics,
            save_cursor,
        )
        .unwrap();
        assert!(committed);
        assert!(cursor_path.exists());
        assert!(output.join("logs").is_dir());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_final_cursor_failure_is_not_a_successful_completion() {
        // Exercise both a buffered batch and one already materialized by the
        // final mapper write. Either must surface an exhausted cursor save.
        for materialize_on_write in [false, true] {
            let dir = make_temp_output_dir();
            let mut writer = OutputWriter::new(&dir, Partition::None, Compression::None, u64::MAX);
            let metadata = BlockMetadata {
                min_block_number: 100,
                max_block_number: 200,
                min_timestamp: Some(1705320000),
                max_timestamp: Some(1705320000),
            };
            let final_mapper_materialized = if materialize_on_write {
                writer.write_all(&make_test_batches(), &metadata).unwrap()
            } else {
                // A known pre-publication failure retains a batch for the drain.
                let blocker = dir.join("blocks");
                std::fs::write(&blocker, b"not-a-directory").unwrap();
                assert!(writer.write_all(&make_test_batches(), &metadata).is_err());
                std::fs::remove_file(blocker).unwrap();
                false
            };
            assert_eq!(final_mapper_materialized, materialize_on_write);
            let invalid_parent = dir.join("not-a-directory");
            std::fs::write(&invalid_parent, b"file").unwrap();
            let location = CursorLocation::Local(invalid_parent.join("cursor.parquet"));
            let (_, metrics) = metrics::init();
            let error = flush_writer_on_exit(
                StreamExit::Completed,
                &mut writer,
                final_mapper_materialized,
                &metrics,
                || {
                    location.save_with_retry_blocking(
                        &CursorState {
                            last_block_num: 200,
                            ..CursorState::default()
                        },
                        &metrics,
                        &AtomicBool::new(false),
                    )
                },
            )
            .unwrap_err();
            assert!(error
                .to_string()
                .contains("cursor persistence failed after 3 attempts"));
            assert_eq!(StreamExit::from_result(&Err(error)), StreamExit::Failed);
            assert_eq!(metrics.cursor_save_failures_total.get(), 3);
            assert_eq!(metrics.cursor_saves_total.get(), 0);
            assert_eq!(metrics.cursor_last_success_timestamp_seconds.get(), 0);
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_final_mapper_write_commits_cursor_with_empty_writer_buffers() {
        let dir = make_temp_output_dir();
        let output = dir.join("output");
        let location = CursorLocation::Local(dir.join("cursor.parquet"));
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        let block = firehose_protos::sf::solana::r#type::v1::Block {
            slot: 42,
            parent_slot: 41,
            block_time: Some(firehose_protos::sf::solana::r#type::v1::UnixTimestamp {
                timestamp: 1_705_320_000,
            }),
            ..Default::default()
        };
        let identity = BlockIdentity {
            block_num: 42,
            timestamp: 1_705_320_000,
            ..Default::default()
        };
        mapper
            .map_block(&prost::Message::encode_to_vec(&block), &identity, None)
            .unwrap();
        let flush_bytes = 4_096;
        let mapper_estimate = mapper.estimated_bytes() as u64;
        assert!(next_mapper_flush_trigger(
            None,
            mapper.max_table_rows(),
            None,
            1,
            None,
            Instant::now(),
            &FlushSizing::new(flush_bytes, u64::MAX).unwrap(),
            MapperBufferEstimate {
                largest_table_bytes: mapper_estimate,
                total_bytes: mapper_estimate
            },
        )
        .is_none());

        let batches = mapper.flush().unwrap();
        let mut writer =
            OutputWriter::new(&output, Partition::None, Compression::Zstd, flush_bytes);
        let metadata = BlockMetadata {
            min_block_number: 42,
            max_block_number: 42,
            min_timestamp: Some(identity.timestamp),
            max_timestamp: Some(identity.timestamp),
        };
        // The real mapper did not trigger a normal loop flush. Its final write
        // now always materializes, so completion must retain that result even
        // though there is nothing left for the final drain.
        let materialized = writer.write_all(&batches, &metadata).unwrap();
        assert!(materialized);
        assert_eq!(writer.buffered_stats(), WriterBufferStats::default());
        assert_eq!(std::fs::read_dir(output.join("blocks")).unwrap().count(), 1);
        let (_, metrics) = metrics::init();
        let committed = flush_writer_on_exit(
            StreamExit::Completed,
            &mut writer,
            materialized,
            &metrics,
            || {
                location.save_with_retry_blocking(
                    &CursorState {
                        cursor: "cursor-42".to_string(),
                        last_block_num: 42,
                        ..Default::default()
                    },
                    &metrics,
                    &AtomicBool::new(false),
                )
            },
        )
        .unwrap();
        assert!(committed);
        let saved = location.load().unwrap().unwrap();
        assert_eq!(saved.cursor, "cursor-42");
        assert_eq!(saved.last_block_num, 42);
        assert_eq!(metrics.cursor_saves_total.get(), 1);
        assert_eq!(metrics.cursor_save_failures_total.get(), 0);
        assert!(metrics.cursor_last_success_timestamp_seconds.get() > 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_real_solana_null_time_uses_metadata_partition_anchor() {
        let dir = make_temp_output_dir();
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, true, false);
        let block = firehose_protos::sf::solana::r#type::v1::Block {
            slot: 42,
            parent_slot: 41,
            block_time: None,
            ..Default::default()
        };
        let identity = BlockIdentity {
            block_num: 42,
            timestamp: 1_705_320_000,
            ..Default::default()
        };
        mapper
            .map_block(&prost::Message::encode_to_vec(&block), &identity, None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(
            batches["blocks"]
                .column_by_name("timestamp")
                .unwrap()
                .null_count(),
            1
        );
        let mut writer = OutputWriter::new(&dir, Partition::Date, Compression::Zstd, 0);
        let outcome = write_mapper_flush(
            &mut writer,
            &batches,
            &BlockMetadata {
                min_block_number: 42,
                max_block_number: 42,
                min_timestamp: Some(identity.timestamp),
                max_timestamp: Some(identity.timestamp),
            },
        )
        .unwrap();
        assert!(outcome.materialized);
        assert_eq!(outcome.buffered, WriterBufferStats::default());
        assert_eq!(
            std::fs::read_dir(dir.join("blocks/year=2024/month=01/day=15"))
                .unwrap()
                .count(),
            1
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // Preserve the completion-drain invariant with an explicit pre-publication
    // table failure. The normal ingestion path stops on this error; these tests
    // exercise recovery and ensure a materialization flag never skips the drain.
    fn writer_with_failed_completion_table(output: &std::path::Path) -> (OutputWriter, PathBuf) {
        let mut writer = OutputWriter::new(output, Partition::None, Compression::None, u64::MAX);
        let mut batches = make_test_batches();
        batches.insert("logs".into(), make_test_batch());
        let pending_path = output.join("logs");
        std::fs::write(&pending_path, b"not-a-directory").unwrap();
        let metadata = BlockMetadata {
            min_block_number: 42,
            max_block_number: 42,
            min_timestamp: Some(1_705_320_000),
            max_timestamp: Some(1_705_320_000),
        };
        assert!(writer.write_all(&batches, &metadata).is_err());
        assert_eq!(writer.buffered_stats().rows, 1);
        assert_eq!(std::fs::read_dir(output.join("blocks")).unwrap().count(), 1);
        std::fs::remove_file(&pending_path).unwrap();
        (writer, pending_path)
    }

    #[test]
    fn test_completion_drains_retained_table_before_one_checkpoint() {
        let dir = make_temp_output_dir();
        let (mut writer, pending_path) = writer_with_failed_completion_table(&dir);
        let (_, metrics) = metrics::init();
        let mut commits = 0;
        let committed =
            flush_writer_on_exit(StreamExit::Completed, &mut writer, true, &metrics, || {
                assert_eq!(std::fs::read_dir(&pending_path).unwrap().count(), 1);
                commits += 1;
                Ok(())
            })
            .unwrap();
        assert!(committed);
        assert_eq!(commits, 1);
        assert_eq!(writer.buffered_stats(), WriterBufferStats::default());
        assert_eq!(
            metrics
                .flushes_total
                .get_or_create(&metrics::FlushLabels {
                    trigger: "shutdown".into()
                })
                .get(),
            1
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_completion_drain_failure_prevents_checkpoint_after_materialization() {
        let dir = make_temp_output_dir();
        let (mut writer, pending_path) = writer_with_failed_completion_table(&dir);
        std::fs::write(&pending_path, b"not-a-directory").unwrap();
        let (_, metrics) = metrics::init();
        let error =
            flush_writer_on_exit(StreamExit::Completed, &mut writer, true, &metrics, || {
                panic!("a failed retained table write must prevent checkpointing");
            })
            .unwrap_err();
        assert_eq!(StreamExit::from_result(&Err(error)), StreamExit::Failed);
        assert_eq!(writer.buffered_stats().rows, 1);
        assert_eq!(
            metrics
                .flushes_total
                .get_or_create(&metrics::FlushLabels {
                    trigger: "shutdown".into()
                })
                .get(),
            0
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_completion_without_new_output_does_not_checkpoint() {
        for previously_materialized in [false, true] {
            let dir = make_temp_output_dir();
            let mut writer = OutputWriter::new(&dir, Partition::None, Compression::None, u64::MAX);
            if previously_materialized {
                let metadata = BlockMetadata {
                    min_block_number: 42,
                    max_block_number: 42,
                    min_timestamp: None,
                    max_timestamp: None,
                };
                assert!(
                    write_mapper_flush(&mut writer, &make_test_batches(), &metadata)
                        .unwrap()
                        .materialized
                );
            }
            let (_, metrics) = metrics::init();
            let committed =
                flush_writer_on_exit(StreamExit::Completed, &mut writer, false, &metrics, || {
                    panic!("earlier normal-loop output must not trigger another final checkpoint");
                })
                .unwrap();
            assert!(!committed);
            assert_eq!(
                metrics
                    .flushes_total
                    .get_or_create(&metrics::FlushLabels {
                        trigger: "shutdown".into()
                    })
                    .get(),
                0
            );
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    #[test]
    fn test_interrupted_exit_does_not_drain_or_checkpoint_after_materialization() {
        for exit in [StreamExit::Failed, StreamExit::Shutdown] {
            let dir = make_temp_output_dir();
            let (mut writer, pending_path) = writer_with_failed_completion_table(&dir);
            let (_, metrics) = metrics::init();
            let committed = flush_writer_on_exit(exit, &mut writer, true, &metrics, || {
                panic!("failed or shutdown exits must not checkpoint");
            })
            .unwrap();
            assert!(!committed);
            assert!(!pending_path.exists());
            assert_eq!(writer.buffered_stats().rows, 1);
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    #[test]
    fn test_next_mapper_flush_trigger_prefers_blocks_when_configured_limit_is_reached() {
        let trigger = next_mapper_flush_trigger(
            Some(10),
            10,
            Some(3),
            3,
            Some(60),
            Instant::now(),
            &FlushSizing::new(1_000_000, u64::MAX).unwrap(),
            MapperBufferEstimate {
                largest_table_bytes: 128,
                total_bytes: 128,
            },
        );

        assert_eq!(trigger, Some(MapperFlushTrigger::Blocks));
    }

    #[test]
    fn test_next_mapper_flush_trigger_ignores_blocks_when_flag_is_omitted() {
        let trigger = next_mapper_flush_trigger(
            None,
            0,
            None,
            3,
            None,
            Instant::now(),
            &FlushSizing::new(1_000_000, u64::MAX).unwrap(),
            MapperBufferEstimate {
                largest_table_bytes: 128,
                total_bytes: 128,
            },
        );

        assert_eq!(trigger, None);
    }

    #[test]
    fn test_next_mapper_flush_trigger_flush_bytes_zero_is_disabled() {
        // `--flush-bytes 0` used to flush after every block (`estimated >= 0`).
        for estimated_bytes in [0, 128, u64::MAX - 1] {
            let trigger = next_mapper_flush_trigger(
                None,
                0,
                None,
                1,
                None,
                Instant::now(),
                &FlushSizing::new(0, u64::MAX).unwrap(),
                MapperBufferEstimate {
                    largest_table_bytes: estimated_bytes,
                    total_bytes: estimated_bytes,
                },
            );
            assert_eq!(trigger, None);
        }

        // Other triggers still apply.
        let trigger = next_mapper_flush_trigger(
            None,
            0,
            Some(1),
            1,
            None,
            Instant::now(),
            &FlushSizing::new(0, u64::MAX).unwrap(),
            MapperBufferEstimate {
                largest_table_bytes: 128,
                total_bytes: 128,
            },
        );
        assert_eq!(trigger, Some(MapperFlushTrigger::Blocks));
    }

    #[test]
    fn test_build_subcommand_rejects_zero_block_range_size_and_stop_block() {
        for (flag, value) in [("--block-range-size", "0"), ("--stop-block", "0")] {
            let error =
                Cli::try_parse_from(["fireparq", "build", "--network", "mainnet", flag, value])
                    .expect_err("zero should be rejected");
            assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
            assert!(error.to_string().contains(flag), "{error}");
        }
    }

    #[test]
    fn test_cli_name_is_fireparq() {
        let cmd = Cli::command();
        assert_eq!(cmd.get_name(), "fireparq");
    }

    #[test]
    fn test_cli_long_help_mentions_build_subcommand() {
        let mut cmd = Cli::command();
        let help = cmd.render_long_help().to_string();
        assert!(help.contains("fireparq build"));
        assert!(help.contains("fireparq"));
    }

    fn subcommand_help(name: &str) -> String {
        let cmd = Cli::command();
        let subcommand = cmd
            .get_subcommands()
            .find(|sc| sc.get_name() == name)
            .unwrap_or_else(|| panic!("subcommand `{name}` should exist"));
        subcommand.clone().render_long_help().to_string()
    }

    fn command_help(args: &[&str]) -> String {
        let err = Cli::try_parse_from(args).expect_err("`--help` should short-circuit parsing");
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
        err.to_string()
    }

    #[test]
    fn test_build_subcommand_parses_network_flag() {
        let cli = Cli::parse_from([
            "fireparq",
            "build",
            "--network",
            "mainnet",
            "--start-block",
            "100",
            "--stop-block",
            "200",
        ]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert_eq!(build_args.network.as_deref(), Some("mainnet"));
            assert_eq!(build_args.common.start_block, Some(100));
            assert_eq!(build_args.common.stop_block, Some(200));
        } else {
            panic!("expected Commands::Build");
        }
    }

    #[test]
    fn test_global_verbose_flag_parses_after_build_subcommand() {
        let cli = Cli::parse_from([
            "fireparq",
            "build",
            "--network",
            "mainnet",
            "--start-block",
            "100",
            "--stop-block",
            "200",
            "--verbose",
        ]);

        assert!(cli.global.verbose);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert_eq!(build_args.network.as_deref(), Some("mainnet"));
        } else {
            panic!("expected Commands::Build");
        }
    }

    #[test]
    fn test_merge_help_mentions_shared_verbose_flag() {
        let help = command_help(&["fireparq", "merge", "--help"]);
        assert!(help.contains("--verbose"));
        assert!(help.contains("verbose operational logs"));
    }

    #[test]
    fn test_build_subcommand_parses_extended_and_block_type() {
        let cli = Cli::parse_from([
            "fireparq",
            "build",
            "--network",
            "mainnet",
            "--start-block",
            "100",
            "--stop-block",
            "200",
            "--block-type",
            "evm",
        ]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert!(!build_args.without_extended);
            assert!(!build_args.without_votes);
            assert_eq!(build_args.block_type, "evm");
        } else {
            panic!("expected Commands::Build");
        }
    }

    #[test]
    fn test_build_subcommand_accepts_disable_feature_flags() {
        let cli = Cli::parse_from([
            "fireparq",
            "build",
            "--network",
            "solana-mainnet-beta",
            "--start-block",
            "100",
            "--stop-block",
            "200",
            "--without-extended",
            "--without-votes",
        ]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert!(build_args.without_extended);
            assert!(build_args.without_votes);
        } else {
            panic!("expected Commands::Build");
        }
    }

    #[test]
    fn test_build_subcommand_parses_with_votes_for_solana() {
        let cli = Cli::parse_from([
            "fireparq",
            "build",
            "--network",
            "solana-mainnet-beta",
            "--start-block",
            "100",
            "--stop-block",
            "200",
        ]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert!(!build_args.without_votes);
            assert!(!build_args.without_extended);
            assert_eq!(build_args.network.as_deref(), Some("solana-mainnet-beta"));
        } else {
            panic!("expected Commands::Build");
        }
    }

    #[test]
    fn test_build_subcommand_rejects_removed_feature_toggle_flags() {
        for args in [
            vec![
                "fireparq",
                "build",
                "--network",
                "mainnet",
                "--start-block",
                "100",
                "--stop-block",
                "200",
                "--extended",
                "false",
            ],
            vec![
                "fireparq",
                "build",
                "--network",
                "solana-mainnet-beta",
                "--start-block",
                "100",
                "--stop-block",
                "200",
                "--with-votes",
                "false",
            ],
        ] {
            let err = Cli::try_parse_from(args).expect_err("removed feature toggle should fail");
            assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
            assert!(err.to_string().contains("unexpected argument"));
        }
    }

    #[test]
    fn test_build_subcommand_rejects_removed_backfill_missing_timestamps_flag() {
        let err = Cli::try_parse_from([
            "fireparq",
            "build",
            "--network",
            "solana-mainnet-beta",
            "--start-block",
            "100",
            "--stop-block",
            "200",
            "--backfill-missing-timestamps",
        ])
        .expect_err("removed backfill flag should fail clap parsing");

        let rendered = err.to_string();
        assert!(rendered.contains("--backfill-missing-timestamps"));
        assert!(rendered.contains("unexpected argument"));
    }

    #[test]
    fn test_build_subcommand_rejects_removed_backfill_missing_timestamps_buffer_limit_flag() {
        let err = Cli::try_parse_from([
            "fireparq",
            "build",
            "--network",
            "solana-mainnet-beta",
            "--start-block",
            "100",
            "--stop-block",
            "200",
            "--backfill-missing-timestamps-buffer-bytes",
            "2048",
        ])
        .expect_err("removed backfill buffer flag should fail clap parsing");

        let rendered = err.to_string();
        assert!(rendered.contains("--backfill-missing-timestamps-buffer-bytes"));
        assert!(rendered.contains("unexpected argument"));
    }

    #[test]
    fn test_effective_s3_cursor_template_requires_complete_credentials() {
        let cursor = resolve_cursor_template(
            "s3://state/worker.parquet",
            &CursorTemplateContext {
                chain: None,
                partition_type: None,
                partition_value: None,
                partition_from: None,
                partition_to: None,
            },
        )
        .unwrap();
        for (key, secret) in [(None, None), (Some("key"), None), (None, Some("secret"))] {
            let config = Config {
                output: "./local".into(),
                cursor_path: Some(cursor.clone()),
                aws_access_key_id: key.map(str::to_string),
                aws_secret_access_key: secret.map(str::to_string),
                ..Default::default()
            };
            let error = resolve_cursor_location(&config).unwrap_err();
            assert!(error
                .to_string()
                .contains("refusing to fall back silently to metadata providers"));
        }
    }

    #[test]
    fn test_effective_cursor_rejects_bucket_bound_endpoint_mismatch() {
        let config = Config {
            output: "s3://data/mainnet".into(),
            cursor_path: Some("s3://state/c.parquet".into()),
            aws_access_key_id: Some("test-key".into()),
            aws_secret_access_key: Some("test-secret".into()),
            aws_endpoint_url: Some("https://data.s3.us-east-1.amazonaws.com".into()),
            ..Default::default()
        };
        let error = validate_cursor_storage(&config).unwrap_err();
        assert!(error.to_string().contains("requested bucket is `state`"));
    }

    #[test]
    fn test_resolve_cursor_location_builds_store_for_cursor_bucket() {
        for (output, cursor, bucket, key) in [
            (
                "s3://data/mainnet",
                "cursor.parquet",
                "data",
                "mainnet/cursor.parquet",
            ),
            (
                "s3://data/mainnet",
                "workers/a.parquet",
                "data",
                "mainnet/workers/a.parquet",
            ),
            ("s3://data/mainnet", "s3://x/c.parquet", "x", "c.parquet"),
            ("s3://data/mainnet", "s3://y/c.parquet", "y", "c.parquet"),
            ("./output/mainnet", "s3://x/c.parquet", "x", "c.parquet"),
        ] {
            let config = Config {
                output: output.into(),
                cursor_path: Some(cursor.into()),
                s3_bucket: Some("data".into()),
                aws_access_key_id: Some("test-key".into()),
                aws_secret_access_key: Some("test-secret".into()),
                aws_region: Some("us-east-1".into()),
                ..Config::default()
            };
            match resolve_cursor_location(&config).unwrap().unwrap() {
                CursorLocation::S3 {
                    client,
                    key: actual_key,
                } => {
                    assert_eq!(client.to_string(), format!("AmazonS3({bucket})"));
                    assert_eq!(actual_key, key);
                }
                other => panic!("expected S3 cursor, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_resolve_cursor_location_rejects_conflicting_output_bucket() {
        let config = Config {
            output: "s3://data/mainnet".into(),
            cursor_path: Some("s3://independent/cursor.parquet".into()),
            s3_bucket: Some("wrong-bucket".into()),
            ..Config::default()
        };
        let error = resolve_cursor_location(&config).unwrap_err();
        assert!(error
            .to_string()
            .contains("S3 output bucket `data` disagrees"));
    }

    #[test]
    fn test_resolve_cursor_location_places_default_local_cursor_under_chain_output_root() {
        let config = Config {
            output: std::path::PathBuf::from("./output/mainnet"),
            cursor_path: Some("cursor.parquet".to_string()),
            s3_bucket: Some("my-bucket".to_string()),
            aws_access_key_id: Some("AKID123".to_string()),
            aws_secret_access_key: Some("secret456".to_string()),
            aws_region: Some("auto".to_string()),
            aws_endpoint_url: Some("https://storage.example.com".to_string()),
            ..Config::default()
        };

        let cursor_location = resolve_cursor_location(&config).expect("cursor location");
        match cursor_location {
            Some(CursorLocation::Local(path)) => {
                assert_eq!(
                    path,
                    std::path::PathBuf::from("./output/mainnet").join("cursor.parquet")
                );
            }
            other => panic!("expected local cursor location, got {other:?}"),
        }
    }

    #[test]
    fn test_load_existing_cursor_fails_on_corrupt_cursor_unless_overridden() {
        let dir = make_temp_output_dir();
        let cursor_path = dir.join("cursor.parquet");
        let location = CursorLocation::Local(cursor_path.clone());

        // No cursor yet: start fresh.
        assert!(load_existing_cursor(Some(&location), false)
            .unwrap()
            .is_none());
        assert!(load_existing_cursor(None, false).unwrap().is_none());

        // A truncated cursor is a hard error that points at the remediation.
        location
            .save(&CursorState {
                cursor: "cursor-at-block-200".to_string(),
                last_block_num: 200,
                ..CursorState::default()
            })
            .unwrap();
        let bytes = std::fs::read(&cursor_path).unwrap();
        std::fs::write(&cursor_path, &bytes[..bytes.len() / 2]).unwrap();
        let error = load_existing_cursor(Some(&location), false).unwrap_err();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("--cursor-override"), "{rendered}");
        assert!(
            rendered.contains(&cursor_path.display().to_string()),
            "{rendered}"
        );

        // --cursor-override explicitly ignores the unreadable cursor.
        assert!(load_existing_cursor(Some(&location), true)
            .unwrap()
            .is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_build_subcommand_infers_live_mode_when_stop_block_is_omitted() {
        let cli = Cli::parse_from(["fireparq", "build", "--network", "mainnet"]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert!(infer_ingestion_live_mode(build_args.common.stop_block));
        } else {
            panic!("expected Commands::Build");
        }
    }

    #[test]
    fn test_build_subcommand_infers_live_mode_without_stop_block_when_flush_blocks_is_set() {
        let cli = Cli::parse_from([
            "fireparq",
            "build",
            "--network",
            "mainnet",
            "--flush-blocks",
            "100000",
        ]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert_eq!(build_args.common.flush_blocks, Some(100000));
            assert!(infer_ingestion_live_mode(build_args.common.stop_block));
        } else {
            panic!("expected Commands::Build");
        }
    }

    #[test]
    fn test_build_subcommand_rejects_removed_live_flag() {
        let err = Cli::try_parse_from(["fireparq", "build", "--network", "mainnet", "--live"])
            .expect_err("--live should no longer be accepted");
        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn test_build_subcommand_cursor_override_default_false() {
        let cli = Cli::parse_from([
            "fireparq",
            "build",
            "--network",
            "mainnet",
            "--start-block",
            "100",
            "--stop-block",
            "200",
        ]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert!(!build_args.cursor_override);
        } else {
            panic!("expected Commands::Build");
        }
    }

    #[test]
    fn test_build_subcommand_failed_transaction_flags() {
        let parse = |extra: &[&str]| {
            let mut argv = vec!["fireparq", "build", "--network", "mainnet"];
            argv.extend_from_slice(extra);
            match Cli::parse_from(argv).command {
                Some(Commands::Build(build_args)) => (
                    build_args.include_failed_transactions,
                    build_args.exclude_failed_transactions,
                ),
                _ => panic!("expected Commands::Build"),
            }
        };
        assert_eq!(parse(&[]), (false, false));
        assert_eq!(parse(&["--exclude-failed-transactions"]), (false, true));
        assert_eq!(parse(&["--include-failed-transactions"]), (true, false));
    }

    #[test]
    fn test_resolve_include_failed_transactions_evm_defaults_to_include() {
        assert_eq!(
            resolve_include_failed_transactions(Some("evm"), false, false, None, false),
            (true, vec![])
        );
        let (include, warnings) =
            resolve_include_failed_transactions(Some("evm"), false, true, None, false);
        assert!(!include);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_resolve_include_failed_transactions_evm_include_flag_is_deprecated_no_op() {
        let (include, warnings) =
            resolve_include_failed_transactions(Some("evm"), true, false, None, false);
        assert!(include);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("deprecated"));
    }

    #[test]
    fn test_resolve_include_failed_transactions_exclude_wins_over_include() {
        for block_type in [Some("evm"), Some("solana"), None] {
            let (include, warnings) =
                resolve_include_failed_transactions(block_type, true, true, None, false);
            assert!(!include);
            assert_eq!(warnings.len(), 1);
            assert!(warnings[0].contains("ignored"));
        }
    }

    #[test]
    fn test_resolve_include_failed_transactions_non_evm_unchanged() {
        for block_type in [
            Some("solana"),
            Some("tron"),
            Some("near"),
            Some("antelope"),
            Some("cosmos"),
            None,
        ] {
            assert_eq!(
                resolve_include_failed_transactions(block_type, false, false, None, false),
                (false, vec![])
            );
            assert_eq!(
                resolve_include_failed_transactions(block_type, true, false, None, false),
                (true, vec![])
            );
        }
    }

    #[test]
    fn test_resolve_include_failed_transactions_evm_resume_keeps_legacy_exclusion() {
        let legacy_cursor = CursorState {
            include_failed_transactions: false,
            ..CursorState::default()
        };
        let (include, warnings) = resolve_include_failed_transactions(
            Some("evm"),
            false,
            false,
            Some(&legacy_cursor),
            false,
        );
        assert!(!include, "resume keeps the stored exclusion");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("still excluding"));
        let mut current = CursorState {
            include_failed_transactions: include,
            ..CursorState::default()
        };
        current.file_metadata.add(
            "firehose-parquet.include_failed_transactions",
            include.to_string(),
        );
        assert!(legacy_cursor.validate_params(&current).is_empty());

        // An explicit --exclude-failed-transactions matches silently.
        assert_eq!(
            resolve_include_failed_transactions(
                Some("evm"),
                false,
                true,
                Some(&legacy_cursor),
                false
            ),
            (false, vec![])
        );
        // --cursor-override switches the output to the new default.
        assert_eq!(
            resolve_include_failed_transactions(
                Some("evm"),
                false,
                false,
                Some(&legacy_cursor),
                true
            ),
            (true, vec![])
        );
        // A cursor written with failed transactions included keeps including them.
        let included_cursor = CursorState {
            include_failed_transactions: true,
            ..CursorState::default()
        };
        assert_eq!(
            resolve_include_failed_transactions(
                Some("evm"),
                false,
                false,
                Some(&included_cursor),
                false
            ),
            (true, vec![])
        );
    }

    #[test]
    fn test_build_subcommand_rejects_removed_bootstrap_missing_genesis_timestamp_flag() {
        let err = Cli::try_parse_from([
            "fireparq",
            "build",
            "--network",
            "mainnet",
            "--start-block",
            "0",
            "--stop-block",
            "200",
            "--bootstrap-missing-genesis-timestamp",
        ])
        .expect_err("removed bootstrap flag should fail clap parsing");

        let rendered = err.to_string();
        assert!(rendered.contains("--bootstrap-missing-genesis-timestamp"));
        assert!(rendered.contains("unexpected argument"));
    }

    #[test]
    fn test_build_subcommand_rejects_unknown_network() {
        let err = Cli::try_parse_from(["fireparq", "build", "--network", "unknown"])
            .expect_err("unknown network should fail clap parsing");
        let rendered = err.to_string();
        assert!(rendered.contains("invalid value 'unknown'"));
        assert!(rendered.contains("solana-mainnet-beta"));
    }

    #[test]
    fn test_build_subcommand_rejects_removed_builtin_network() {
        let err = Cli::try_parse_from(["fireparq", "build", "--network", "arbitrum-nova"])
            .expect_err("removed built-in network should fail clap parsing");
        let rendered = err.to_string();
        assert!(rendered.contains("invalid value 'arbitrum-nova'"));
        assert!(rendered.contains("arbitrum-one"));
    }

    #[test]
    fn test_build_subcommand_help_contains_examples() {
        let cmd = Cli::command();
        // Find the build subcommand
        let build_subcmd = cmd
            .get_subcommands()
            .find(|sc| sc.get_name() == "build")
            .expect("build subcommand should exist");
        let help = build_subcmd.clone().render_long_help().to_string();
        assert!(help.contains("fireparq build --network"));
        assert!(help.contains("--without-votes"));
        assert!(help.contains("--start-block"));
        assert!(!help.contains("--live"));
        assert!(!help.contains("--partitions-index"));
        assert!(!help.contains("--partition-from"));
        assert!(!help.contains("--partition-to"));
        assert!(!help.contains("--strict-timestamps"));
        assert!(!help.contains("--with-votes [<WITH_VOTES>]"));
    }

    #[test]
    fn test_cli_help_mentions_network() {
        let cmd = Cli::command();
        let build_subcmd = cmd
            .get_subcommands()
            .find(|sc| sc.get_name() == "build")
            .expect("build subcommand should exist");
        let help = build_subcmd.clone().render_long_help().to_string();
        assert!(help.contains("--network <NETWORK>"));
        assert!(help.contains("FIREHOSE_ENDPOINT_MAINNET"));
        assert!(!help.contains("--live"));
        assert!(help.contains("existing cursor"));
        assert!(help.contains("first streamable block"));
        assert!(help.contains("When omitted, the build runs in live mode"));
        assert!(help.contains("Missing blocks are skipped automatically"));
        assert!(!help.contains("--bootstrap-missing-genesis-timestamp"));
        assert!(!help.contains("--backfill-missing-timestamps"));
        assert!(!help.contains("--backfill-missing-timestamps-buffer-bytes"));
        assert!(!help.contains("--strict-timestamps"));
        assert!(!help.contains("--skip-missing-blocks"));
    }

    #[test]
    fn test_build_subcommand_rejects_removed_strict_timestamps_flag() {
        let err = Cli::try_parse_from([
            "fireparq",
            "build",
            "--network",
            "solana-mainnet-beta",
            "--start-block",
            "0",
            "--stop-block",
            "200",
            "--strict-timestamps",
            "false",
        ])
        .expect_err("removed strict timestamp flag should fail clap parsing");

        let rendered = err.to_string();
        assert!(rendered.contains("--strict-timestamps"));
        assert!(rendered.contains("unexpected argument"));
    }

    #[test]
    fn test_build_help_mentions_tron_encoding_behavior() {
        let help = subcommand_help("build");
        assert!(!help.contains("--bytes-encoding"));
    }

    #[test]
    fn test_build_subcommand_rejects_removed_skip_missing_blocks_flag() {
        let err = Cli::try_parse_from([
            "fireparq",
            "build",
            "--network",
            "solana-mainnet-beta",
            "--start-block",
            "100",
            "--stop-block",
            "200",
            "--skip-missing-blocks",
        ])
        .expect_err("removed skip-missing-blocks flag should fail clap parsing");

        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
        let rendered = err.to_string();
        assert!(rendered.contains("--skip-missing-blocks"));
        assert!(rendered.contains("unexpected argument"));
    }

    #[test]
    fn test_partitions_build_help_omits_chain_override_flag() {
        let help = command_help(&["fireparq", "partitions", "build", "--help"]);
        assert!(!help.contains("--chain"));
        assert!(help.contains("chainName"));
        assert!(help.contains("Time spans traverse exact finalized ancestry"));
        assert!(!help.contains("--skip-missing-blocks"));
    }

    #[test]
    fn test_utility_subcommand_help_uses_grouped_headings() {
        for (name, headings, snippets) in [
            (
                "scan",
                vec!["Selection:", "Display:", "AWS / S3:", "Runtime / Logging:"],
                vec![
                    "<PATH>",
                    "--limit",
                    "--vertical",
                    "--aws-region",
                    "--log-level",
                ],
            ),
            (
                "validate",
                vec![
                    "Selection:",
                    "Validation:",
                    "AWS / S3:",
                    "Runtime / Logging:",
                ],
                vec![
                    "<PATH>",
                    "--cross-partition",
                    "--allow-gaps",
                    "--aws-region",
                    "--log-level",
                ],
            ),
            (
                "inspect",
                vec!["Selection:", "Display:", "AWS / S3:", "Runtime / Logging:"],
                vec![
                    "<PATH>",
                    "--schema-only",
                    "--json",
                    "--aws-region",
                    "--log-level",
                ],
            ),
            (
                "truncate",
                vec![
                    "Selection:",
                    "Execution:",
                    "AWS / S3:",
                    "Runtime / Logging:",
                ],
                vec![
                    "<PATH>",
                    "--partition",
                    "--dry-run",
                    "--yes",
                    "--aws-region",
                    "--log-level",
                ],
            ),
            (
                "merge",
                vec![
                    "Selection:",
                    "Output:",
                    "Execution:",
                    "AWS / S3:",
                    "Runtime / Logging:",
                ],
                vec![
                    "<PATH>",
                    "--compression",
                    "--dry-run",
                    "--cache-control",
                    "--log-level",
                ],
            ),
            (
                "rollup",
                vec![
                    "Selection:",
                    "Output:",
                    "Execution:",
                    "AWS / S3:",
                    "Runtime / Logging:",
                ],
                vec![
                    "<SOURCE>",
                    "--output",
                    "--delete-source",
                    "--cache-control",
                    "--log-level",
                ],
            ),
            (
                "verify",
                vec![
                    "Selection:",
                    "Verification:",
                    "Reporting:",
                    "Registry:",
                    "AWS / S3:",
                    "Runtime / Logging:",
                ],
                vec![
                    "<PATH>",
                    "--chain",
                    "--report-json",
                    "--registry-path",
                    "--log-level",
                ],
            ),
        ] {
            let help = command_help(&["fireparq", name, "--help"]);

            for heading in headings {
                assert!(
                    help.contains(heading),
                    "expected `{name}` help to contain heading `{heading}`\n{help}"
                );
            }

            for snippet in snippets {
                assert!(
                    help.contains(snippet),
                    "expected `{name}` help to mention `{snippet}`\n{help}"
                );
            }
        }
    }

    #[test]
    fn test_build_subcommand_rejects_removed_partition_index_flags() {
        for (flag, value) in [
            ("--partitions-index", "./partitions.parquet"),
            ("--partition-from", "2015-07-30 14:00:00"),
            ("--partition-to", "2015-07-30 18:00:00"),
        ] {
            let err = Cli::try_parse_from([
                "fireparq",
                "build",
                "--network",
                "mainnet",
                "--start-block",
                "100",
                "--stop-block",
                "200",
                flag,
                value,
            ])
            .expect_err("removed build flag must be rejected");
            assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
            assert!(err.to_string().contains(flag));
        }
    }

    #[test]
    fn test_cli_requires_subcommand() {
        let err = Cli::try_parse_from(["fireparq"]).expect_err("subcommand should be required");
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn test_infer_ingestion_live_mode_is_false_when_stop_block_is_present() {
        assert!(!infer_ingestion_live_mode(Some(200)));
    }

    #[test]
    fn test_infer_ingestion_live_mode_is_true_when_stop_block_is_omitted() {
        assert!(infer_ingestion_live_mode(None));
    }

    #[test]
    fn test_infer_ingestion_live_mode_ignores_cursor_stop_block() {
        let cursor_state = CursorState {
            stop_block: Some(200),
            ..CursorState::default()
        };

        assert!(infer_ingestion_live_mode(None));
        assert_eq!(cursor_state.stop_block, Some(200));
    }

    #[test]
    fn test_resolve_ingestion_start_block_prefers_cursor_start_block() {
        let cursor_state = CursorState {
            start_block: Some(21),
            ..CursorState::default()
        };
        let endpoint_info = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 42,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });

        let start_block =
            resolve_ingestion_start_block(None, Some(&cursor_state), &endpoint_info, false)
                .expect("ingestion should prefer an existing cursor");

        assert_eq!(start_block, Some(21));
    }

    #[test]
    fn test_resolve_ingestion_start_block_uses_explicit_start_without_cursor() {
        let start_block = resolve_ingestion_start_block(Some(21), None, &None, false)
            .expect("ingestion should use an explicit start block when no cursor exists");

        assert_eq!(start_block, Some(21));
    }

    #[test]
    fn test_resolve_ingestion_start_block_ignores_cursor_start_when_override_is_enabled() {
        let cursor_state = CursorState {
            start_block: Some(21),
            ..CursorState::default()
        };
        let endpoint_info = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 42,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });

        let start_block =
            resolve_ingestion_start_block(Some(7), Some(&cursor_state), &endpoint_info, true)
                .expect("cursor override should use the requested start block");

        assert_eq!(start_block, Some(7));
    }

    #[test]
    fn test_resolve_ingestion_start_block_uses_endpoint_first_streamable_block() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 42,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });

        let start_block = resolve_ingestion_start_block(None, None, &endpoint_info, false)
            .expect("ingestion should use endpoint first streamable block");

        assert_eq!(start_block, Some(42));
    }

    #[test]
    fn test_resolve_ingestion_start_block_uses_endpoint_first_streamable_block_when_override_is_enabled(
    ) {
        let cursor_state = CursorState {
            start_block: Some(21),
            ..CursorState::default()
        };
        let endpoint_info = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 42,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });

        let start_block =
            resolve_ingestion_start_block(None, Some(&cursor_state), &endpoint_info, true)
                .expect("cursor override should fall back to endpoint metadata");

        assert_eq!(start_block, Some(42));
    }

    #[test]
    fn test_resolve_ingestion_start_block_rejects_missing_first_streamable_metadata() {
        let err = resolve_ingestion_start_block(None, None, &None, false)
            .expect_err("ingestion should require an explicit start, cursor, or endpoint metadata");

        assert_eq!(
            err.to_string(),
            "--start-block is required when neither an existing cursor nor the endpoint exposes first_streamable_block_num"
        );
    }

    #[test]
    fn test_resolve_ingestion_stop_block_prefers_explicit_stop_block() {
        let stop_block = resolve_ingestion_stop_block(Some(300))
            .expect("ingestion should accept an explicit stop block");

        assert_eq!(stop_block, Some(300));
    }

    #[test]
    fn test_resolve_ingestion_stop_block_keeps_live_stream_open_when_omitted() {
        let stop_block =
            resolve_ingestion_stop_block(None).expect("omitting stop block should keep streaming");

        assert_eq!(stop_block, None);
    }

    #[test]
    fn test_stream_resume_cursor_ignores_stored_cursor_when_override_is_enabled() {
        let cursor_state = CursorState {
            cursor: "cursor-123".to_string(),
            ..CursorState::default()
        };

        assert_eq!(stream_resume_cursor(Some(&cursor_state), true), None);
    }

    #[test]
    fn test_stream_resume_cursor_uses_stored_cursor_without_override() {
        let cursor_state = CursorState {
            cursor: "cursor-123".to_string(),
            ..CursorState::default()
        };

        assert_eq!(
            stream_resume_cursor(Some(&cursor_state), false),
            Some("cursor-123".to_string())
        );
    }

    #[test]
    fn test_start_block_filter_skips_blocks_below_start_before_mapping() {
        // Start 26049673 above LIB 26049592: the server streams from LIB+1.
        let mut filter = StartBlockFilter::new(Some(26_049_673));
        let mapped: Vec<u64> = (26_049_593..=26_049_675)
            .filter(|&block_num| filter.admit(block_num))
            .collect();

        assert_eq!(mapped, vec![26_049_673, 26_049_674, 26_049_675]);
        assert_eq!(filter.skipped, 80);
    }

    #[test]
    fn test_start_block_filter_without_start_block_admits_everything() {
        let mut filter = StartBlockFilter::new(None);
        assert!(filter.admit(0));
        assert!(filter.admit(42));
        assert_eq!(filter.skipped, 0);
    }

    #[test]
    fn test_bounded_stream_ending_early_is_an_error() {
        let error = ensure_bounded_stream_reached_stop(200, Some(150), false).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("block 150"), "{message}");
        assert!(message.contains("last requested block 199"), "{message}");

        let error = ensure_bounded_stream_reached_stop(200, None, false).unwrap_err();
        assert!(error.to_string().contains("no block"), "{error}");
    }

    #[test]
    fn test_bounded_stream_reaching_last_requested_block_is_complete() {
        assert!(ensure_bounded_stream_reached_stop(200, Some(199), false).is_ok());
        assert!(ensure_bounded_stream_reached_stop(200, Some(250), false).is_ok());
    }

    #[test]
    fn test_bounded_stream_on_sparse_chain_may_end_below_last_requested_block() {
        assert!(block_type_allows_block_number_gaps("solana"));
        assert!(block_type_allows_block_number_gaps("near"));
        assert!(block_type_allows_block_number_gaps("beacon"));
        assert!(!block_type_allows_block_number_gaps("evm"));
        assert!(!block_type_allows_block_number_gaps("bitcoin"));

        assert!(ensure_bounded_stream_reached_stop(200, Some(197), true).is_ok());
    }

    #[test]
    fn test_validate_block_range_alignment_accepts_aligned_values() {
        validate_block_range_alignment(Some(0), Some(0), Some(30_000_000), 10_000_000)
            .expect("aligned block-range bounds should be accepted");
    }

    #[test]
    fn test_validate_block_range_alignment_rejects_misaligned_stop_block() {
        let err = validate_block_range_alignment(Some(0), Some(0), Some(30_000_001), 10_000_000)
            .expect_err("misaligned stop block should fail");

        assert_eq!(
            err.to_string(),
            "--stop-block must align to the effective start block (0) in --block-range-size (10000000) increments when --partition block_range; got 30000001"
        );
    }

    #[test]
    fn test_validate_block_range_alignment_accepts_implicit_start_block_anchor() {
        validate_block_range_alignment(None, Some(9_820_210), Some(9_820_510), 100)
            .expect("implicit start block should anchor block-range alignment");
    }

    #[test]
    fn test_validate_block_range_alignment_rejects_misaligned_relative_stop_block() {
        let err = validate_block_range_alignment(None, Some(9_820_210), Some(9_820_500), 100)
            .expect_err("stop block misaligned to implicit anchor should fail");

        assert_eq!(
            err.to_string(),
            "--stop-block must align to the effective start block (9820210) in --block-range-size (100) increments when --partition block_range; got 9820500"
        );
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
            "--stop-block must align to the effective start block (0) in --block-range-size (10000000) increments when --partition block_range; got 30000001"
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
    fn test_output_encoding_policy_defaults() {
        assert_eq!(
            output_encoding_policy("evm", false).map(|policy| policy.bytes_encoding),
            Some(EncodeBytes::Hex)
        );
        assert_eq!(
            output_encoding_policy("evm", true).map(|policy| policy.bytes_encoding),
            Some(EncodeBytes::TronBase58)
        );
        assert_eq!(
            output_encoding_policy("bitcoin", false).map(|policy| policy.bytes_encoding),
            Some(EncodeBytes::Hex)
        );
        assert_eq!(
            output_encoding_policy("solana", false).map(|policy| policy.bytes_encoding),
            Some(EncodeBytes::Base58)
        );
        assert_eq!(
            output_encoding_policy("tron", false).map(|policy| policy.bytes_encoding),
            Some(EncodeBytes::TronBase58)
        );
        assert_eq!(
            output_encoding_policy("near", false).map(|policy| policy.bytes_encoding),
            Some(EncodeBytes::Base58)
        );
        assert_eq!(
            output_encoding_policy("antelope", false).map(|policy| policy.bytes_encoding),
            Some(EncodeBytes::HexNoPrefix)
        );
        assert_eq!(
            output_encoding_policy("cosmos", false).map(|policy| policy.bytes_encoding),
            Some(EncodeBytes::Hex)
        );
        assert_eq!(
            output_encoding_policy("beacon", false).map(|policy| policy.bytes_encoding),
            Some(EncodeBytes::Hex)
        );
    }

    #[test]
    fn test_default_block_id_encoding_contract() {
        assert_eq!(
            output_encoding_policy("evm", false).map(|policy| policy.block_id_encoding),
            Some("hex_0x")
        );
        assert_eq!(
            output_encoding_policy("evm", true).map(|policy| policy.block_id_encoding),
            Some("hex_no_prefix")
        );
        assert_eq!(
            output_encoding_policy("bitcoin", false).map(|policy| policy.block_id_encoding),
            Some("hex_0x")
        );
        assert_eq!(
            output_encoding_policy("solana", false).map(|policy| policy.block_id_encoding),
            Some("base58")
        );
        assert_eq!(
            output_encoding_policy("tron", false).map(|policy| policy.block_id_encoding),
            Some("hex_no_prefix")
        );
        assert_eq!(
            output_encoding_policy("near", false).map(|policy| policy.block_id_encoding),
            Some("base58")
        );
        assert_eq!(
            output_encoding_policy("antelope", false).map(|policy| policy.block_id_encoding),
            Some("hex_no_prefix")
        );
        assert_eq!(
            output_encoding_policy("cosmos", false).map(|policy| policy.block_id_encoding),
            Some("hex_0x")
        );
        assert_eq!(
            output_encoding_policy("beacon", false).map(|policy| policy.block_id_encoding),
            Some("hex_0x")
        );
    }

    #[test]
    fn test_endpoint_uses_tron_style_evm_profile() {
        let ei = Some(EndpointInfo {
            chain_name: "tron-evm".to_string(),
            chain_name_aliases: vec!["tron-mainnet".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        assert!(endpoint_uses_tron_style_evm_profile(&ei));
        assert!(chain_uses_tron_style_evm_profile("eth-mainnet", &ei));
        assert!(chain_uses_tron_style_evm_profile("tron-evm", &None));

        let tron = Some(EndpointInfo {
            chain_name: "tron".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        assert!(endpoint_uses_tron_style_evm_profile(&tron));
        assert!(chain_uses_tron_style_evm_profile("tron", &None));
    }

    #[test]
    fn test_create_mapper_all_types() {
        for block_type in &[
            "evm", "bitcoin", "solana", "near", "antelope", "cosmos", "tron", "beacon",
        ] {
            let encode_bytes = output_encoding_policy(block_type, false)
                .map(|policy| policy.bytes_encoding)
                .unwrap_or(EncodeBytes::Hex);
            let mapper = create_mapper(block_type, false, false, false, encode_bytes, false, false);
            assert!(
                mapper.is_ok(),
                "create_mapper failed for block_type: {block_type}"
            );
        }
    }

    #[test]
    fn test_create_mapper_invalid_type() {
        assert!(create_mapper(
            "unknown",
            false,
            false,
            false,
            EncodeBytes::Hex,
            false,
            false
        )
        .is_err());
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
    fn test_resolve_output_bytes_encoding_uses_tron_style_profile_for_tron_chain_name() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "tron".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });

        let resolved = resolve_output_bytes_encoding(Some("evm"), &endpoint_info, true);

        assert_eq!(resolved, EncodeBytes::TronBase58);
    }

    #[test]
    fn test_resolve_output_bytes_encoding_near_prefers_output_contract_over_endpoint_hint() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "near-mainnet".to_string(),
            chain_name_aliases: vec!["near".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 2,
            block_features: vec![],
        });

        let resolved = resolve_output_bytes_encoding(Some("near"), &endpoint_info, false);

        assert_eq!(resolved, EncodeBytes::Base58);
    }

    #[test]
    fn test_resolve_output_bytes_encoding_supported_contracts_override_endpoint_hints() {
        let cases = [
            ("evm", false, 3, EncodeBytes::Hex),
            ("bitcoin", false, 3, EncodeBytes::Hex),
            ("solana", false, 2, EncodeBytes::Base58),
            ("near", false, 2, EncodeBytes::Base58),
            ("antelope", false, 3, EncodeBytes::HexNoPrefix),
            ("cosmos", false, 3, EncodeBytes::Hex),
            ("tron", false, 2, EncodeBytes::TronBase58),
            ("beacon", false, 3, EncodeBytes::Hex),
            ("evm", true, 2, EncodeBytes::TronBase58),
        ];

        for (block_type, tron_style_evm_profile, endpoint_block_id_encoding, expected) in cases {
            let endpoint_info = Some(EndpointInfo {
                chain_name: format!("{block_type}-mainnet"),
                chain_name_aliases: vec![],
                first_streamable_block_num: 0,
                first_streamable_block_id: String::new(),
                block_id_encoding: endpoint_block_id_encoding,
                block_features: vec![],
            });

            let resolved = resolve_output_bytes_encoding(
                Some(block_type),
                &endpoint_info,
                tron_style_evm_profile,
            );

            assert_eq!(
                resolved, expected,
                "expected explicit output contract for block_type={block_type} tron_style_evm_profile={tron_style_evm_profile}"
            );
        }
    }

    #[test]
    fn test_resolve_output_bytes_encoding_tron_style_contract_overrides_endpoint_hint() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "tron-evm".to_string(),
            chain_name_aliases: vec!["tron".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 2,
            block_features: vec![],
        });

        let resolved = resolve_output_bytes_encoding(Some("evm"), &endpoint_info, true);

        assert_eq!(resolved, EncodeBytes::TronBase58);
    }

    #[test]
    fn test_resolve_auto_encode_bytes_unknown_block_type_uses_endpoint_hint_then_generic_default() {
        let base58_endpoint = Some(EndpointInfo {
            chain_name: "mystery-chain".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 3,
            block_features: vec![],
        });

        assert_eq!(
            resolve_auto_encode_bytes(None, &base58_endpoint, false),
            EncodeBytes::Base58
        );
        assert_eq!(
            resolve_auto_encode_bytes(Some("unknown"), &base58_endpoint, false),
            EncodeBytes::Base58
        );
        assert_eq!(
            resolve_auto_encode_bytes(None, &None, false),
            EncodeBytes::Hex
        );
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
        assert_eq!(
            resolve_output(&base, &ei).unwrap(),
            PathBuf::from("./mainnet")
        );
    }

    #[test]
    fn test_resolve_output_without_endpoint_info() {
        let base = PathBuf::from(".");
        assert!(resolve_output(&base, &None).is_err());
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
        assert!(resolve_output(&base, &ei).is_err());
    }

    #[test]
    fn test_validate_block_timestamp_missing_errors() {
        let err = validate_block_timestamp(42, 0, &Partition::None)
            .expect_err("missing timestamp should error");
        assert!(err.to_string().contains("missing timestamp metadata"));
    }

    #[test]
    fn test_validate_block_timestamp_missing_in_first_streamable_block_is_generic() {
        let err = validate_block_timestamp(0, 0, &Partition::None)
            .expect_err("missing timestamp should still report a missing timestamp");
        let message = err.to_string();

        assert!(message.contains("missing timestamp metadata"));
        assert!(!message.contains("--bootstrap-missing-genesis-timestamp"));
    }

    #[test]
    fn test_validate_block_timestamp_missing_time_partition_errors() {
        let err = validate_block_timestamp(42, 0, &Partition::Date)
            .expect_err("time-based partitioning requires a timestamp");
        assert!(err
            .to_string()
            .contains("time-based partitioning requires timestamps"));
    }

    #[test]
    fn test_genesis_timestamp_bootstrap_buffers_until_first_timestamped_block() {
        let mut bootstrap = GenesisTimestampBootstrap::new(Some(0));

        assert_eq!(
            bootstrap.observe_block(0, 0, 0),
            GenesisTimestampBootstrapAction::Buffer
        );
        assert_eq!(
            bootstrap.observe_block(0, 1, 0),
            GenesisTimestampBootstrapAction::Buffer
        );
        assert_eq!(
            bootstrap.observe_block(0, 2, 1_700_000_000),
            GenesisTimestampBootstrapAction::Anchored {
                anchor_block: 2,
                buffered_blocks: 2,
                first_buffered_block: 0,
            }
        );
    }

    #[test]
    fn test_genesis_timestamp_bootstrap_is_inactive_after_a_nonmatching_first_block() {
        let mut bootstrap = GenesisTimestampBootstrap::new(Some(0));

        assert_eq!(
            bootstrap.observe_block(0, 1, 0),
            GenesisTimestampBootstrapAction::None
        );
        assert_eq!(
            bootstrap.observe_block(0, 0, 0),
            GenesisTimestampBootstrapAction::None
        );
    }

    #[test]
    fn test_missing_genesis_timestamp_bootstrap_error_mentions_missing_anchor() {
        let err = missing_genesis_timestamp_bootstrap_error(0, 3);
        let message = err.to_string();

        assert!(message.contains("no later block with timestamp metadata was found"));
        assert!(message.contains("block 0"));
    }

    #[test]
    fn test_take_anchored_bootstrap_blocks_preserves_block_numbers() {
        let mut buffered_blocks = vec![
            BufferedBootstrapBlock {
                received_ordinal: 0,
                block_bytes: vec![0x01],
                cursor: "cursor-0".to_string(),
                fork_step: None,
                identity: BlockIdentity {
                    block_num: 0,
                    block_id: "block-0".to_string(),
                    timestamp: 0,
                    ..BlockIdentity::default()
                },
            },
            BufferedBootstrapBlock {
                received_ordinal: 0,
                block_bytes: vec![0x02],
                cursor: "cursor-1".to_string(),
                fork_step: Some("STEP_NEW".to_string()),
                identity: BlockIdentity {
                    block_num: 1,
                    block_id: "block-1".to_string(),
                    timestamp: 0,
                    ..BlockIdentity::default()
                },
            },
        ];

        let anchored = take_anchored_bootstrap_blocks(&mut buffered_blocks, 1_700_000_000);

        assert!(buffered_blocks.is_empty());
        assert_eq!(anchored.len(), 2);
        assert_eq!(anchored[0].identity.block_num, 0);
        assert_eq!(anchored[1].identity.block_num, 1);
        assert_eq!(anchored[0].identity.timestamp, 1_700_000_000);
        assert_eq!(anchored[1].identity.timestamp, 1_700_000_000);
        assert_eq!(anchored[0].cursor, "cursor-0");
        assert_eq!(anchored[1].fork_step.as_deref(), Some("STEP_NEW"));
    }

    #[test]
    fn test_last_known_timestamp_partition_routing_is_automatic_for_solana_time_partitions() {
        for partition in [
            Partition::Date,
            Partition::Hour,
            Partition::Minute,
            Partition::Second,
        ] {
            assert!(use_last_known_timestamp_partition_routing(
                "solana", &partition
            ));
        }

        assert!(!use_last_known_timestamp_partition_routing(
            "solana",
            &Partition::None
        ));
        assert!(!use_last_known_timestamp_partition_routing(
            "evm",
            &Partition::Date
        ));
    }

    #[test]
    fn test_should_emit_progress_log_every_hundred_blocks() {
        assert!(!should_emit_progress_log(0));
        assert!(!should_emit_progress_log(99));
        assert!(should_emit_progress_log(100));
        assert!(!should_emit_progress_log(101));
        assert!(should_emit_progress_log(200));
    }

    #[test]
    fn test_timestamp_backfill_uses_last_known_anchor_without_interpolation() {
        let mut backfill =
            TimestampBackfill::new(true, DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES);
        let first = backfill
            .observe_block(
                vec![0x01],
                "cursor-10".to_string(),
                None,
                BlockIdentity {
                    block_num: 10,
                    timestamp: 1_000,
                    ..BlockIdentity::default()
                },
            )
            .expect("first anchor should process immediately");
        assert_eq!(first.len(), 1);

        let ready = backfill
            .observe_block(
                vec![0x02],
                "cursor-15".to_string(),
                None,
                BlockIdentity {
                    block_num: 15,
                    timestamp: 0,
                    ..BlockIdentity::default()
                },
            )
            .expect("missing block should reuse the prior anchor immediately");
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].identity.block_num, 15);
        assert_eq!(ready[0].identity.timestamp, 1_000);
        assert_eq!(backfill.buffered_bytes(), 0);

        let next = backfill
            .observe_block(
                vec![0x03],
                "cursor-20".to_string(),
                None,
                BlockIdentity {
                    block_num: 20,
                    timestamp: 1_100,
                    ..BlockIdentity::default()
                },
            )
            .expect("later anchor should update the last-known routing timestamp");
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].identity.block_num, 20);
        assert_eq!(next[0].identity.timestamp, 1_100);
        assert_eq!(backfill.buffered_bytes(), 0);
    }

    #[test]
    fn test_timestamp_backfill_seeds_genesis_anchor_for_first_missing_block() {
        let mut backfill =
            TimestampBackfill::new(true, DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES);
        let ready = backfill
            .observe_block(
                vec![0x01],
                "cursor-0".to_string(),
                None,
                BlockIdentity {
                    block_num: 0,
                    timestamp: 0,
                    ..BlockIdentity::default()
                },
            )
            .expect("genesis anchor should route the first missing-timestamp block");
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].identity.block_num, 0);
        assert_eq!(ready[0].identity.timestamp, SOLANA_GENESIS_TIMESTAMP);
    }

    #[test]
    fn test_timestamp_backfill_drain_is_empty_with_last_known_routing() {
        let mut backfill =
            TimestampBackfill::new(true, DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES);
        backfill
            .observe_block(
                vec![0x01],
                "cursor-100".to_string(),
                None,
                BlockIdentity {
                    block_num: 100,
                    timestamp: 1_700_000_000,
                    ..BlockIdentity::default()
                },
            )
            .expect("anchor should process immediately");
        let routed = backfill
            .observe_block(
                vec![0x02],
                "cursor-101".to_string(),
                None,
                BlockIdentity {
                    block_num: 101,
                    timestamp: 0,
                    ..BlockIdentity::default()
                },
            )
            .expect("missing block should reuse the prior anchor immediately");
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].identity.timestamp, 1_700_000_000);

        let drained = backfill
            .drain_open_span()
            .expect("no buffered span should remain to drain");
        assert!(drained.is_empty());
        assert_eq!(backfill.buffered_bytes(), 0);
    }

    #[test]
    fn test_timestamp_backfill_routes_time_partitions_from_last_known_timestamp() {
        let mut backfill =
            TimestampBackfill::new(true, DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES);
        backfill
            .observe_block(
                vec![0x01],
                "cursor-100".to_string(),
                None,
                BlockIdentity {
                    block_num: 100,
                    timestamp: 1_700_000_000,
                    ..BlockIdentity::default()
                },
            )
            .expect("known timestamp should update the last-known anchor");
        let routed = backfill
            .observe_block(
                vec![0x02],
                "cursor-101".to_string(),
                None,
                BlockIdentity {
                    block_num: 101,
                    timestamp: 0,
                    ..BlockIdentity::default()
                },
            )
            .expect("missing block should reuse the last-known timestamp");
        let routing_timestamp = routed[0].identity.timestamp;

        for partition in [
            Partition::Date,
            Partition::Hour,
            Partition::Minute,
            Partition::Second,
        ] {
            let anchor_key = partition.partition_key(100, 1_700_000_000).unwrap();
            let routed_key = partition.partition_key(101, routing_timestamp).unwrap();
            assert_eq!(routed_key, anchor_key);
        }
    }

    #[test]
    fn test_timestamp_backfill_routes_first_missing_block_from_genesis_anchor() {
        let mut backfill =
            TimestampBackfill::new(true, DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES);
        let routed = backfill
            .observe_block(
                vec![0x01],
                "cursor-0".to_string(),
                None,
                BlockIdentity {
                    block_num: 0,
                    timestamp: 0,
                    ..BlockIdentity::default()
                },
            )
            .expect("genesis anchor should route the first missing-timestamp block");
        let routing_timestamp = routed[0].identity.timestamp;

        for partition in [
            Partition::Date,
            Partition::Hour,
            Partition::Minute,
            Partition::Second,
        ] {
            let expected = partition
                .partition_key(0, SOLANA_GENESIS_TIMESTAMP)
                .unwrap();
            let routed_key = partition.partition_key(0, routing_timestamp).unwrap();
            assert_eq!(routed_key, expected);
        }
    }

    #[test]
    fn test_timestamp_backfill_routes_from_restored_cursor_anchor() {
        let mut backfill =
            TimestampBackfill::new(true, DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES);
        let cursor_state = CursorState {
            last_block_num: 99,
            last_timestamp: Some(1_700_000_000),
            ..CursorState::default()
        };

        restore_sparse_routing_cursor_anchor(
            &mut backfill,
            Some(&cursor_state),
            false,
            "solana",
            &Partition::Hour,
        );

        let routed = backfill
            .observe_block(
                vec![0x01],
                "cursor-100".to_string(),
                None,
                BlockIdentity {
                    block_num: 100,
                    timestamp: 0,
                    ..BlockIdentity::default()
                },
            )
            .expect("restored cursor anchor should route the first sparse block after resume");

        assert_eq!(routed[0].identity.timestamp, 1_700_000_000);
    }

    #[test]
    fn test_build_file_metadata_includes_block_type() {
        let metadata = build_file_metadata(
            "solana",
            &EncodeBytes::Base58,
            "https://example.com:443",
            Compression::Zstd,
            &None,
        );

        assert!(metadata
            .entries
            .iter()
            .any(|(key, value)| { key == "firehose-parquet.block_type" && value == "solana" }));
        // strict_timestamps is no longer recorded in metadata
        assert!(!metadata
            .entries
            .iter()
            .any(|(key, _)| { key == "firehose-parquet.strict_timestamps" }));
    }

    #[test]
    fn test_synthetic_timestamp_metadata_added_for_solana_backfill() {
        let mut metadata = build_file_metadata(
            "solana",
            &EncodeBytes::Base58,
            "https://example.com:443",
            Compression::Zstd,
            &None,
        );
        maybe_add_synthetic_timestamp_metadata(&mut metadata, "solana", true);

        assert_eq!(
            find_meta(&metadata, "firehose-parquet.synthetic_timestamps"),
            Some("true")
        );
        assert_eq!(
            find_meta(&metadata, "firehose-parquet.synthetic_timestamp_policy"),
            Some("last_known_partition_routing")
        );
    }

    #[test]
    fn test_synthetic_timestamp_metadata_not_added_when_backfill_disabled() {
        let mut metadata = build_file_metadata(
            "solana",
            &EncodeBytes::Base58,
            "https://example.com:443",
            Compression::Zstd,
            &None,
        );
        maybe_add_synthetic_timestamp_metadata(&mut metadata, "solana", false);

        assert_eq!(
            find_meta(&metadata, "firehose-parquet.synthetic_timestamps"),
            None
        );
        assert_eq!(
            find_meta(&metadata, "firehose-parquet.synthetic_timestamp_policy"),
            None
        );
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

    #[test]
    fn test_chain_name_is_solana_matches_expected_aliases() {
        assert!(chain_name_is_solana("solana"));
        assert!(chain_name_is_solana("solana-mainnet-beta"));
        assert!(!chain_name_is_solana("mainnet"));
    }

    #[test]
    fn test_chain_name_is_antelope_matches_expected_aliases() {
        assert!(chain_name_is_antelope("antelope"));
        assert!(chain_name_is_antelope("antelope-mainnet"));
        assert!(chain_name_is_antelope("eos"));
        assert!(!chain_name_is_antelope("mainnet"));
    }

    #[test]
    fn test_endpoint_chain_is_solana_matches_expected_aliases() {
        let ei = Some(EndpointInfo {
            chain_name: "solana-mainnet-beta".to_string(),
            chain_name_aliases: vec!["solana".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 3,
            block_features: vec![],
        });

        assert!(endpoint_chain_is_solana(&ei));
    }

    #[test]
    fn test_endpoint_chain_is_antelope_matches_expected_aliases() {
        let ei = Some(EndpointInfo {
            chain_name: "eos".to_string(),
            chain_name_aliases: vec!["antelope-mainnet".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 3,
            block_features: vec![],
        });

        assert!(endpoint_chain_is_antelope(&ei));
    }

    #[test]
    fn test_extended_warning_message_stays_generic() {
        let ei = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec!["eth".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 1,
            block_features: vec![],
        });

        assert!(!endpoint_chain_is_solana(&ei));
        assert_eq!(
            without_extended_warning_message(),
            "--without-extended had no effect because extended output is not supported for this chain"
        );
    }

    #[test]
    fn test_resolve_extended_mode_does_not_auto_enable_from_endpoint_info() {
        let ei = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 1,
            block_features: vec!["extended".to_string()],
        });

        assert!(!resolve_extended_mode(false, true, &ei));
    }

    #[test]
    fn test_resolve_extended_mode_honors_cli_flag_when_endpoint_supports_extended() {
        let ei = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 1,
            block_features: vec!["base".to_string(), "extended".to_string()],
        });

        assert!(resolve_extended_mode(true, false, &ei));
    }

    #[test]
    fn test_unsupported_chain_feature_flag_warnings_warn_for_without_extended_on_solana() {
        assert_eq!(
            unsupported_chain_feature_flag_warnings("solana", &None, None, true, false),
            vec![WITHOUT_EXTENDED_WARNING]
        );
    }

    #[test]
    fn test_unsupported_chain_feature_flag_warnings_warn_for_without_votes_on_non_solana() {
        assert_eq!(
            unsupported_chain_feature_flag_warnings("evm", &None, None, false, true),
            vec![WITHOUT_VOTES_NON_SOLANA_WARNING]
        );
    }

    #[test]
    fn test_unsupported_chain_feature_flag_warnings_warn_for_without_extended_on_antelope() {
        assert_eq!(
            unsupported_chain_feature_flag_warnings("antelope", &None, None, true, false),
            vec![WITHOUT_EXTENDED_WARNING]
        );
    }

    #[test]
    fn test_unsupported_chain_feature_flag_warnings_are_empty_when_flags_are_applicable() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 1,
            block_features: vec!["base".to_string(), "extended".to_string()],
        });

        assert!(
            unsupported_chain_feature_flag_warnings("evm", &endpoint_info, None, true, false)
                .is_empty()
        );
        assert!(
            unsupported_chain_feature_flag_warnings("solana", &None, None, false, true).is_empty()
        );
    }

    #[test]
    fn test_antelope_cursor_feature_validation_ignores_extended_mismatch() {
        let mut mismatches = vec![
            "extended: cursor=true vs current=false".to_string(),
            "include_failed_transactions: cursor=false vs current=true".to_string(),
        ];

        apply_antelope_cursor_feature_validation(&mut mismatches);

        assert_eq!(
            mismatches,
            vec!["include_failed_transactions: cursor=false vs current=true".to_string()]
        );
    }

    #[test]
    fn test_solana_cursor_feature_validation_preserves_legacy_extended_mismatch() {
        let mut mismatches = vec!["extended: cursor=true vs current=false".to_string()];
        let cursor_state = CursorState {
            extended: true,
            ..CursorState::default()
        };

        apply_solana_cursor_feature_validation(&mut mismatches, &cursor_state, true);

        assert_eq!(mismatches, vec!["extended: cursor=true vs current=false"]);
    }

    #[test]
    fn test_solana_cursor_feature_validation_reports_with_votes_mismatch_from_metadata() {
        let mut mismatches = Vec::new();
        let mut file_metadata = ParquetFileMetadata::new();
        file_metadata.add("firehose-parquet.with_votes", "true");
        let cursor_state = CursorState {
            file_metadata,
            ..CursorState::default()
        };

        apply_solana_cursor_feature_validation(&mut mismatches, &cursor_state, false);

        assert_eq!(mismatches, vec!["with_votes: cursor=true vs current=false"]);
    }

    #[test]
    fn test_solana_cursor_feature_validation_reports_unknown_with_votes_for_legacy_cursor() {
        let mut mismatches = Vec::new();
        let cursor_state = CursorState::default();

        apply_solana_cursor_feature_validation(&mut mismatches, &cursor_state, true);

        assert_eq!(
            mismatches,
            vec!["with_votes: cursor=unknown vs current=true"]
        );
    }

    #[test]
    fn test_maybe_add_solana_with_votes_metadata_only_for_solana() {
        let mut solana_meta = ParquetFileMetadata::new();
        maybe_add_solana_with_votes_metadata(&mut solana_meta, Some("solana"), true);
        assert_eq!(
            find_meta(&solana_meta, "firehose-parquet.with_votes"),
            Some("true")
        );

        let mut evm_meta = ParquetFileMetadata::new();
        maybe_add_solana_with_votes_metadata(&mut evm_meta, Some("evm"), true);
        assert_eq!(find_meta(&evm_meta, "firehose-parquet.with_votes"), None);
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
        let meta = build_file_metadata(
            "evm",
            &EncodeBytes::Hex,
            "https://example.com",
            Compression::Zstd,
            &ei,
        );

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
    fn test_build_file_metadata_tron_contract_overrides_endpoint_block_id_encoding() {
        let ei = Some(EndpointInfo {
            chain_name: "tron-evm".to_string(),
            chain_name_aliases: vec!["tron-mainnet".to_string()],
            first_streamable_block_num: 100,
            first_streamable_block_id: "0xabc".to_string(),
            block_id_encoding: 2,
            block_features: vec!["base".to_string()],
        });
        let meta = build_file_metadata(
            "evm",
            &EncodeBytes::TronBase58,
            "https://example.com",
            Compression::Zstd,
            &ei,
        );

        assert_eq!(
            find_meta(&meta, "firehose-parquet.bytes_encoding"),
            Some("tron_base58")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_id_encoding"),
            Some("hex_no_prefix")
        );
    }

    #[test]
    fn test_build_file_metadata_near_base58_overrides_endpoint_block_id_encoding() {
        let ei = Some(EndpointInfo {
            chain_name: "near-mainnet".to_string(),
            chain_name_aliases: vec!["near".to_string()],
            first_streamable_block_num: 100,
            first_streamable_block_id: "0xabc".to_string(),
            block_id_encoding: 2,
            block_features: vec!["base".to_string()],
        });
        let meta = build_file_metadata(
            "near",
            &EncodeBytes::Base58,
            "https://example.com",
            Compression::Zstd,
            &ei,
        );

        assert_eq!(
            find_meta(&meta, "firehose-parquet.bytes_encoding"),
            Some("base58")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_id_encoding"),
            Some("base58")
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
        let meta = build_file_metadata(
            "evm",
            &EncodeBytes::Hex,
            "https://example.com",
            Compression::Zstd,
            &ei,
        );

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
        let meta = build_file_metadata(
            "evm",
            &EncodeBytes::Hex,
            "https://example.com",
            Compression::Zstd,
            &ei,
        );

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
        let meta = build_file_metadata(
            "evm",
            &EncodeBytes::Hex,
            "https://example.com",
            Compression::Zstd,
            &ei,
        );

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
        let meta = build_file_metadata(
            "evm",
            &EncodeBytes::Hex,
            "https://example.com",
            Compression::Zstd,
            &None,
        );

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
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_id_encoding"),
            Some("hex_0x")
        );
        assert_eq!(find_meta(&meta, "firehose-parquet.block_features"), None);
        assert_eq!(
            find_meta(&meta, "firehose-parquet.compression"),
            Some("zstd")
        );
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
        let meta = build_file_metadata(
            "evm",
            &EncodeBytes::Hex,
            "https://example.com",
            Compression::Zstd,
            &ei,
        );

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
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_id_encoding"),
            Some("hex_0x")
        );
        assert_eq!(find_meta(&meta, "firehose-parquet.block_features"), None);
    }

    #[test]
    fn test_build_file_metadata_honors_requested_compression() {
        let meta = build_file_metadata(
            "evm",
            &EncodeBytes::Hex,
            "https://example.com",
            Compression::Snappy,
            &None,
        );

        assert_eq!(
            find_meta(&meta, "firehose-parquet.compression"),
            Some("snappy")
        );
    }

    #[test]
    fn test_build_cursor_file_metadata_tron_contract() {
        let ei = Some(EndpointInfo {
            chain_name: "tron-evm".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: "0xabc".to_string(),
            block_id_encoding: 2,
            block_features: vec!["base".to_string()],
        });
        let meta = build_cursor_file_metadata(
            Some("evm"),
            Some(&EncodeBytes::TronBase58),
            "https://example.com",
            Compression::Zstd,
            &firehose_parquet::config::Partition::Date,
            &ei,
            true,
            false,
            true,
        );

        assert_eq!(
            find_meta(&meta, "firehose-parquet.bytes_encoding"),
            Some("tron_base58")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_id_encoding"),
            Some("hex_no_prefix")
        );
        assert_eq!(find_meta(&meta, "firehose-parquet.partition"), Some("date"));
        assert_eq!(find_meta(&meta, "firehose-parquet.extended"), Some("true"));
        assert_eq!(
            find_meta(&meta, "firehose-parquet.final_blocks_only"),
            Some("false")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.include_failed_transactions"),
            Some("true")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.synthetic_timestamps"),
            None
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.synthetic_timestamp_policy"),
            None
        );
    }

    fn probe_timeout_error(block_num: u64) -> anyhow::Error {
        firehose_parquet::grpc::FetchTimeoutError {
            block_num,
            timeout: Duration::from_secs(5),
        }
        .into()
    }

    #[tokio::test]
    async fn test_retry_probe_fetch_with_policy_retries_timeouts_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let probe_counter = AtomicU64::new(0);
        let result = retry_probe_fetch_with_policy(
            42,
            "testing probe retry",
            ProbeRetryPolicy {
                max_attempts: 3,
                missing_attempts: 3,
                initial_backoff: Duration::from_millis(0),
            },
            true,
            &probe_counter,
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        let current = attempts.fetch_add(1, AtomicOrdering::SeqCst);
                        if current < 2 {
                            Err(probe_timeout_error(42))
                        } else {
                            Ok(ProbeFetch::Found(99_u64))
                        }
                    }
                }
            },
        )
        .await
        .expect("retry should succeed");

        assert_eq!(result, ProbeFetch::Found(99));
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
            ProbeRetryPolicy {
                max_attempts: 3,
                missing_attempts: 3,
                initial_backoff: Duration::from_millis(0),
            },
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
                            Ok(ProbeFetch::Found(77_u64))
                        }
                    }
                }
            },
        )
        .await
        .expect("retry should succeed");

        assert_eq!(result, ProbeFetch::Found(77));
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
            ProbeRetryPolicy {
                max_attempts: 3,
                missing_attempts: 3,
                initial_backoff: Duration::from_millis(0),
            },
            false,
            &probe_counter,
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        attempts.fetch_add(1, AtomicOrdering::SeqCst);
                        Err::<ProbeFetch<u64>, _>(anyhow!("still failing"))
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
    async fn test_retry_probe_fetch_with_policy_fails_fast_on_fatal_errors() {
        let probe_counter = AtomicU64::new(0);
        let err = retry_probe_fetch_with_policy(
            42,
            "testing probe retry",
            ProbeRetryPolicy {
                max_attempts: 3,
                missing_attempts: 3,
                initial_backoff: Duration::from_millis(0),
            },
            true,
            &probe_counter,
            || async {
                Err::<ProbeFetch<u64>, _>(tonic::Status::unauthenticated("bad token").into())
            },
        )
        .await
        .expect_err("authentication errors cannot be retried away");

        assert_eq!(classify_fetch_error(&err), FetchErrorKind::Fatal);
        assert_eq!(probe_counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_retry_probe_fetch_with_policy_skips_missing_block_errors() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let probe_counter = AtomicU64::new(0);
        let result = retry_probe_fetch_with_policy(
            42,
            "testing probe retry",
            ProbeRetryPolicy {
                max_attempts: 3,
                missing_attempts: 3,
                initial_backoff: Duration::from_millis(0),
            },
            true,
            &probe_counter,
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        attempts.fetch_add(1, AtomicOrdering::SeqCst);
                        Err::<ProbeFetch<u64>, _>(
                            tonic::Status::unknown(
                                "rpc error: code = NotFound desc = block not found in files",
                            )
                            .into(),
                        )
                    }
                }
            },
        )
        .await
        .expect("missing block errors should be skippable");

        assert_eq!(result, ProbeFetch::Missing);
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(probe_counter.load(Ordering::Relaxed), 3);

        // Once a run of missing blocks is established, each further block is fetched once.
        let probe_counter = AtomicU64::new(0);
        let result = retry_probe_fetch_with_policy(
            43,
            "testing probe retry",
            ProbeRetryPolicy {
                max_attempts: 3,
                missing_attempts: 1,
                initial_backoff: Duration::from_millis(0),
            },
            true,
            &probe_counter,
            || async {
                Err::<ProbeFetch<u64>, _>(tonic::Status::not_found("block 43 not found").into())
            },
        )
        .await
        .expect("missing block errors should be skippable");
        assert_eq!(result, ProbeFetch::Missing);
        assert_eq!(probe_counter.load(Ordering::Relaxed), 1);
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
        assert_eq!(
            find_meta(&meta, "firehose-parquet.bytes_encoding"),
            Some("hex")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_id_encoding"),
            Some("hex_0x")
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
            stop_block: 1_000_000,
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
    fn test_format_optional_probe_timestamp_handles_missing_timestamp() {
        assert_eq!(
            format_optional_probe_timestamp(-1).as_deref(),
            Some("1969-12-31 23:59:59")
        );
        for timestamp in [i64::MIN, i64::MAX, 1_700_000_000_000] {
            assert_eq!(
                format_optional_probe_timestamp(timestamp),
                Some(format!("invalid unix timestamp {timestamp}"))
            );
        }
        assert_eq!(format_optional_probe_timestamp(0), None);
        assert_eq!(
            format_optional_probe_timestamp(1_690_815_590).as_deref(),
            Some("2023-07-31 14:59:50")
        );
    }

    fn partitions_test_row(
        partition_type: &str,
        partition_value: &str,
        start_block: u64,
        stop_block: u64,
    ) -> PartitionBuildRow {
        PartitionBuildRow {
            partition_type: partition_type.to_string(),
            partition_interval_seconds: if partition_type == "block_range" {
                (stop_block - start_block) as i64
            } else {
                PartitionBuildType::from_cli_value(partition_type)
                    .expect("partition type")
                    .interval_seconds()
            },
            partition_start_ts: partition_value.to_string(),
            partition_value: partition_value.to_string(),
            start_block,
            stop_block,
            start_time: None,
            end_time: None,
            chain: Some("mainnet".to_string()),
        }
    }

    fn unused_cursor_start() -> Result<Option<u64>> {
        panic!("the sibling cursor must not be consulted")
    }

    #[test]
    fn test_resolve_partitions_build_start_block_resumes_from_frontier_not_cursor() {
        // Audit C2 reproduction: a sibling cursor far past the stored frontier used to become
        // the resumed start, stretching the terminal row across the unprobed gap.
        let start = resolve_partitions_build_start_block(
            Some(14_000),
            None,
            false,
            unused_cursor_start,
            Some(0),
        )
        .expect("resume start");
        assert_eq!(start, 14_000);

        for explicit in [100, 14_000] {
            let start = resolve_partitions_build_start_block(
                Some(14_000),
                Some(explicit),
                false,
                unused_cursor_start,
                Some(0),
            )
            .expect("explicit start at or before the frontier");
            assert_eq!(start, 14_000, "explicit --start-block {explicit}");
        }
    }

    #[test]
    fn test_resolve_partitions_build_start_block_rejects_start_past_resume_frontier() {
        let err = resolve_partitions_build_start_block(
            Some(14_000),
            Some(50_000),
            false,
            unused_cursor_start,
            Some(0),
        )
        .expect_err("a start past the frontier would leave a gap");
        let message = err.to_string();
        assert!(
            message.contains("past the existing partitions.parquet frontier 14000"),
            "{message}"
        );
        assert!(message.contains("[14000, 50000)"), "{message}");

        let err = resolve_partitions_build_start_block(
            Some(14_000),
            Some(100),
            true,
            unused_cursor_start,
            Some(0),
        )
        .expect_err("live mode requires an explicit start to match the frontier");
        assert!(err.to_string().contains("does not match"), "{err}");
        let start = resolve_partitions_build_start_block(
            Some(14_000),
            Some(14_000),
            true,
            unused_cursor_start,
            Some(0),
        )
        .expect("live start at the frontier");
        assert_eq!(start, 14_000);
    }

    #[test]
    fn test_resolve_partitions_build_start_block_for_new_index() {
        let explicit =
            resolve_partitions_build_start_block(None, Some(500), false, unused_cursor_start, None)
                .expect("explicit start");
        assert_eq!(explicit, 500);

        let from_cursor =
            resolve_partitions_build_start_block(None, None, false, || Ok(Some(900)), Some(0))
                .expect("cursor start");
        assert_eq!(from_cursor, 900);

        let from_endpoint =
            resolve_partitions_build_start_block(None, None, false, || Ok(None), Some(42))
                .expect("first streamable start");
        assert_eq!(from_endpoint, 42);

        let live =
            resolve_partitions_build_start_block(None, None, true, unused_cursor_start, Some(42))
                .expect("live ignores the cursor");
        assert_eq!(live, 42);

        let err = resolve_partitions_build_start_block(
            None,
            None,
            false,
            || Err(anyhow!("cursor unreadable")),
            Some(42),
        )
        .expect_err("an unreadable cursor is an error");
        assert!(err.to_string().contains("cursor unreadable"), "{err}");

        let err = resolve_partitions_build_start_block(None, None, false, || Ok(None), None)
            .expect_err("no start source");
        assert!(
            err.to_string().contains("--start-block is required"),
            "{err}"
        );
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
        // Falls back to the inferred chain name when no endpoint_info is available
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
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_id_encoding"),
            Some("base58")
        );
    }

    #[test]
    fn test_build_partitions_file_metadata_unknown_chain_uses_endpoint_hint_fallback() {
        let ei = Some(EndpointInfo {
            chain_name: "mystery-mainnet".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 3,
            block_features: vec![],
        });
        let meta = build_partitions_file_metadata(
            "https://example.com",
            "mystery-mainnet",
            "hour",
            Compression::Zstd,
            &ei,
            None,
        );

        assert_eq!(find_meta(&meta, "firehose-parquet.block_type"), None);
        assert_eq!(
            find_meta(&meta, "firehose-parquet.bytes_encoding"),
            Some("base58")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_id_encoding"),
            Some("base58")
        );
    }

    #[test]
    fn test_build_partitions_file_metadata_tron_evm_uses_tron_encoding_contract() {
        let meta = build_partitions_file_metadata(
            "https://example.com",
            "tron-evm",
            "hour",
            Compression::Zstd,
            &None,
            None,
        );

        assert_eq!(find_meta(&meta, "firehose-parquet.block_type"), Some("evm"));
        assert_eq!(
            find_meta(&meta, "firehose-parquet.bytes_encoding"),
            Some("tron_base58")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.block_id_encoding"),
            Some("hex_no_prefix")
        );
        assert_eq!(
            find_meta(&meta, "firehose-parquet.chain_name"),
            Some("tron-evm")
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
        );

        assert_eq!(
            find_meta(&meta, "firehose-parquet.compression"),
            Some("snappy")
        );
    }
    #[tokio::test]
    async fn test_retry_probe_fetch_with_policy_never_treats_timeouts_as_missing_blocks() {
        let probe_counter = AtomicU64::new(0);
        let err = retry_probe_fetch_with_policy(
            42,
            "testing probe retry",
            ProbeRetryPolicy {
                max_attempts: 3,
                missing_attempts: 3,
                initial_backoff: Duration::from_millis(0),
            },
            true,
            &probe_counter,
            || async { Err::<ProbeFetch<u64>, _>(probe_timeout_error(42)) },
        )
        .await
        .expect_err("exhausted timeouts must surface as an error, not a skipped block");

        assert!(
            err.to_string().contains("failed after 3 attempts"),
            "{err:#}"
        );
        assert_eq!(classify_fetch_error(&err), FetchErrorKind::Timeout);
        assert_eq!(probe_counter.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn verified_resume_refuses_legacy_and_malformed_files_but_overwrite_can_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partitions.parquet");
        let aws = AwsConfig {
            aws_access_key_id: None,
            aws_secret_access_key: None,
            aws_session_token: None,
            aws_region: None,
            aws_endpoint_url: None,
        };
        assert!(
            load_existing_verified_partitions_index(path.to_str().unwrap(), &aws, false)
                .unwrap()
                .is_none()
        );
        let rows = vec![partitions_test_row("hour", "2024-01-01 00:00:00", 100, 200)];
        firehose_parquet::cli::write_partitions_index(path.to_str().unwrap(), &rows, None).unwrap();
        assert!(
            load_existing_verified_partitions_index(path.to_str().unwrap(), &aws, false)
                .unwrap_err()
                .to_string()
                .contains("rebuild")
        );
        assert!(
            load_existing_verified_partitions_index(path.to_str().unwrap(), &aws, true)
                .unwrap()
                .is_none()
        );
        std::fs::write(&path, b"corrupt index: not found is not an IO error").unwrap();
        assert!(
            load_existing_verified_partitions_index(path.to_str().unwrap(), &aws, false).is_err()
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"corrupt index: not found is not an IO error"
        );
        assert!(
            ensure_existing_partitions_index_mode("index", &rows, false, false, false).is_err()
        );
        ensure_existing_partitions_index_mode("index", &rows, false, true, false).unwrap();
        ensure_existing_partitions_index_mode("index", &rows, true, false, false).unwrap();
        ensure_existing_partitions_index_mode("index", &[], false, false, true).unwrap();
    }
}
