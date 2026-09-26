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
    declare_inventory, load_authoritative_resume, prepare_partitions_index_write, IngestionSession,
    MapperSemantics, CURSOR_OVERRIDE_REFUSED,
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

use blocks::chain::{ChainKind, ChainProfile, ExtendedOutput, MapperOptions};

#[cfg(test)]
mod chain_profile_tests;
mod ingestion;
use ingestion::run_ingestion;

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

/// A known family's output encoding contract wins over the endpoint's
/// block-id encoding hint; an unknown family uses the hint, then hex.
fn resolve_auto_encode_bytes(
    block_type: Option<ChainKind>,
    endpoint_info: &Option<EndpointInfo>,
    tron_style_evm_profile: bool,
) -> EncodeBytes {
    if let Some(kind) = block_type {
        return kind.default_bytes_encoding(tron_style_evm_profile);
    }

    endpoint_info
        .as_ref()
        .and_then(|ei| encode_bytes_from_block_id_encoding(ei.block_id_encoding))
        .unwrap_or(EncodeBytes::Hex)
}

/// Endpoint chain names in inference order: the nonempty `chain_name`, then
/// its aliases.
fn endpoint_chain_names(endpoint_info: &Option<EndpointInfo>) -> impl Iterator<Item = &str> {
    endpoint_info.iter().flat_map(|info| {
        (!info.chain_name.is_empty())
            .then_some(info.chain_name.as_str())
            .into_iter()
            .chain(info.chain_name_aliases.iter().map(String::as_str))
    })
}

fn inferred_block_type_from_endpoint_info(
    endpoint_info: &Option<EndpointInfo>,
) -> Option<ChainKind> {
    ChainKind::infer_from_chain_names(endpoint_chain_names(endpoint_info))
}

/// The best family known before streaming (`--block-type`, else endpoint
/// inference), and the family that decides failed-transaction defaults, which
/// falls back to the cursor's `block_type` label. An unknown cursor label has
/// no family and gets the non-EVM default, as before.
fn pre_stream_block_types(
    block_type: Option<ChainKind>,
    endpoint_info: &Option<EndpointInfo>,
    cursor_state: Option<&CursorState>,
) -> (Option<ChainKind>, Option<ChainKind>) {
    let initial = block_type.or_else(|| inferred_block_type_from_endpoint_info(endpoint_info));
    let failed_transactions = initial
        .or_else(|| cursor_metadata_block_type(cursor_state).and_then(ChainKind::from_label));
    (initial, failed_transactions)
}

fn add_common_file_metadata(
    meta: &mut ParquetFileMetadata,
    block_type: Option<ChainKind>,
    encoding: Option<&EncodeBytes>,
    endpoint: &str,
    endpoint_info: &Option<EndpointInfo>,
) {
    meta.add("firehose-parquet.version", env!("CARGO_PKG_VERSION"));
    if let Some(kind) = block_type {
        meta.add("firehose-parquet.block_type", kind.label());
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
    block_type: ChainKind,
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
    block_type: ChainKind,
    synthetic_partition_routing: bool,
) {
    if block_type.profile().nullable_timestamps && synthetic_partition_routing {
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
/// - EVM (`ChainProfile::failed_transactions_by_default`) writes them by
///   default, with only their persistent state changes.
///   `--include-failed-transactions` is a deprecated no-op there. Resumed EVM
///   state (protected authority, or a legacy cursor in a dry run) that records
///   failed transactions as excluded keeps excluding them, so one output does
///   not mix both modes. Only a read-only dry run's `--cursor-override` ignores
///   that state; a protected build must use a new empty output root instead.
/// - Other chains exclude them unless `--include-failed-transactions` is set.
///
/// Returns the effective value and the warnings to log.
fn resolve_include_failed_transactions(
    block_type: Option<ChainKind>,
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
    if !block_type.is_some_and(|kind| kind.profile().failed_transactions_by_default) {
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
                "this output's resume state records failed transactions as excluded (the EVM default before #494); still excluding them so this output stays consistent. Pass --exclude-failed-transactions to keep this and silence the warning. To switch to the new default, rebuild into a new empty output root with an absent cursor mirror; --cursor-override cannot change protected output"
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
    block_type: Option<ChainKind>,
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

use firehose_parquet::partition_index::SOLANA_GENESIS_TIMESTAMP;

fn use_last_known_timestamp_partition_routing(
    block_type: ChainKind,
    partition: &Partition,
) -> bool {
    block_type.profile().nullable_timestamps && partition_requires_timestamp(partition)
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
    block_bytes: prost::bytes::Bytes,
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
struct TimestampRouting {
    enabled: bool,
    last_anchor: Option<TimestampAnchor>,
}

impl TimestampRouting {
    fn new(enabled: bool) -> Self {
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

    fn route_block(
        &mut self,
        block_bytes: impl Into<prost::bytes::Bytes>,
        cursor: String,
        fork_step: Option<String>,
        identity: BlockIdentity,
    ) -> anyhow::Result<BufferedBootstrapBlock> {
        let current = BufferedBootstrapBlock {
            received_ordinal: 0,
            block_bytes: block_bytes.into(),
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
            return Ok(current);
        }

        if current.identity.timestamp == 0 {
            let mut current = current;
            let anchor = self.last_anchor.ok_or_else(|| {
                anyhow!(
                    "nullable-timestamp partition routing requires a last-known timestamp anchor"
                )
            })?;
            current.identity.timestamp = anchor.timestamp;
            return Ok(current);
        }

        let anchor = TimestampAnchor {
            block_num: current.identity.block_num,
            timestamp: current.identity.timestamp,
        };
        self.last_anchor = Some(anchor);
        Ok(current)
    }
}

fn restore_sparse_routing_cursor_anchor(
    timestamp_routing: &mut TimestampRouting,
    cursor_state: Option<&CursorState>,
    cursor_override: bool,
    block_type: ChainKind,
    partition: &Partition,
) {
    if cursor_override || !use_last_known_timestamp_partition_routing(block_type, partition) {
        return;
    }

    let Some(cursor_state) = cursor_state else {
        return;
    };

    if let Some(last_timestamp) = cursor_state.last_timestamp {
        timestamp_routing.restore_anchor(cursor_state.last_block_num, last_timestamp);
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
    // Empty builders can retain offset-array headers/capacity; these gauges
    // describe populated logical buffers, not allocator reserve.
    let estimates = if rows == 0 {
        MapperBufferEstimate::default()
    } else {
        MapperBufferEstimate::from_table_sizes(
            mapper.table_estimates().into_iter().map(|(_, bytes)| bytes),
        )
    };
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
) -> Option<ChainKind> {
    ChainKind::infer_from_chain_names(
        std::iter::once(chain).chain(endpoint_chain_names(endpoint_info)),
    )
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

/// Parse `--block-type`; `None` is `auto`.
fn parse_requested_block_type(block_type: &str) -> Result<Option<ChainKind>> {
    let block_type = block_type.to_lowercase();
    if block_type == "auto" {
        return Ok(None);
    }
    ChainKind::from_label(&block_type).map(Some).ok_or_else(|| {
        anyhow!(
            "unsupported block type: {block_type}. Supported: {}",
            BLOCK_TYPES.join(", ")
        )
    })
}

fn detect_block_type(type_url: &str) -> Result<ChainKind> {
    ChainKind::from_type_url(type_url)
        .ok_or_else(|| anyhow!("unable to auto-detect block type from type_url: {type_url}"))
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

/// Load a legacy cursor for a read-only `fireparq build --dry-run`.
///
/// Protected builds never call this: they resume only from output authority.
/// A cursor that exists but cannot be read (permission denied, S3 5xx/403,
/// truncated or corrupt file) is a hard error rather than a silent fresh
/// start. With `--cursor-override` the dry run ignores the unreadable cursor
/// and previews the CLI bounds instead.
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
            "stored cursor exists but could not be loaded; this read-only dry run refuses to guess a resume point. Fix access to the cursor, or pass --cursor-override to ignore it for this dry run only. A real build never resumes from this file: it reads output authority, repairs a missing mirror and refuses a corrupt one",
        )),
    }
}

async fn run_partitions_build(
    endpoint: &str,
    grpc: firehose_parquet::config::GrpcConfig,
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
        grpc,
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
    } else if endpoint_chain_has(&endpoint_info, has_nullable_timestamps) {
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

/// A [`ChainProfile`] property that decides chain-specific flag handling.
type ProfileProperty = fn(&ChainProfile) -> bool;

fn has_vote_transactions(profile: &ChainProfile) -> bool {
    profile.vote_transactions
}

fn has_unsupported_extended_output(profile: &ChainProfile) -> bool {
    profile.extended == ExtendedOutput::Unsupported
}

fn has_nullable_timestamps(profile: &ChainProfile) -> bool {
    profile.nullable_timestamps
}

/// Whether `name` strictly identifies a family with `property` (see
/// `ChainProfile::strict_chain_names`).
fn chain_name_has(name: &str, property: ProfileProperty) -> bool {
    ChainKind::ALL
        .into_iter()
        .any(|kind| property(kind.profile()) && kind.matches_chain_name(name))
}

fn endpoint_chain_has(endpoint_info: &Option<EndpointInfo>, property: ProfileProperty) -> bool {
    endpoint_info.as_ref().is_some_and(|ei| {
        chain_name_has(&ei.chain_name, property)
            || ei
                .chain_name_aliases
                .iter()
                .any(|alias| chain_name_has(alias, property))
    })
}

fn cursor_metadata_block_type<'a>(cursor_state: Option<&'a CursorState>) -> Option<&'a str> {
    cursor_state.and_then(|state| state.get_metadata("firehose-parquet.block_type"))
}

fn cursor_chain_has(cursor_state: Option<&CursorState>, property: ProfileProperty) -> bool {
    cursor_metadata_block_type(cursor_state).is_some_and(|name| chain_name_has(name, property))
        || cursor_state.is_some_and(|state| {
            state
                .get_metadata("firehose-parquet.chain_name")
                .is_some_and(|name| chain_name_has(name, property))
                || state
                    .get_metadata("firehose-parquet.chain_name_aliases")
                    .is_some_and(|aliases| {
                        aliases
                            .split(',')
                            .any(|alias| chain_name_has(alias, property))
                    })
        })
}

/// Whether the chain has `property` before the first block: from an explicit
/// `--block-type`, or for `auto` (`None`) from a strict chain-name match in the
/// endpoint or cursor metadata.
fn chain_has(
    requested_block_type: Option<ChainKind>,
    endpoint_info: &Option<EndpointInfo>,
    cursor_state: Option<&CursorState>,
    property: ProfileProperty,
) -> bool {
    match requested_block_type {
        Some(kind) => property(kind.profile()),
        None => {
            endpoint_chain_has(endpoint_info, property) || cursor_chain_has(cursor_state, property)
        }
    }
}

/// Whether the chain is known to lack `property` before the first block. For
/// `auto`, endpoint metadata decides when present; otherwise only a cursor
/// `block_type` makes the chain known.
fn chain_known_without(
    requested_block_type: Option<ChainKind>,
    endpoint_info: &Option<EndpointInfo>,
    cursor_state: Option<&CursorState>,
    property: ProfileProperty,
) -> bool {
    match requested_block_type {
        Some(kind) => !property(kind.profile()),
        None => {
            if endpoint_info.is_some() {
                !endpoint_chain_has(endpoint_info, property)
            } else {
                cursor_metadata_block_type(cursor_state)
                    .is_some_and(|block_type| !chain_name_has(block_type, property))
            }
        }
    }
}

/// Chain-specific feature handling that is resolved before the first block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreStreamChainFeatures {
    /// The chain writes `vote_transactions`; `--without-votes` applies.
    vote_transactions: bool,
    /// Extended output is statically unsupported and always disabled.
    extended_unsupported: bool,
    /// The chain is known and has no `vote_transactions`: `--without-votes`
    /// warns, and extended output follows the endpoint capability.
    known_without_votes: bool,
}

impl PreStreamChainFeatures {
    fn resolve(
        requested_block_type: Option<ChainKind>,
        endpoint_info: &Option<EndpointInfo>,
        cursor_state: Option<&CursorState>,
    ) -> Self {
        Self {
            vote_transactions: chain_has(
                requested_block_type,
                endpoint_info,
                cursor_state,
                has_vote_transactions,
            ),
            extended_unsupported: chain_has(
                requested_block_type,
                endpoint_info,
                cursor_state,
                has_unsupported_extended_output,
            ),
            known_without_votes: chain_known_without(
                requested_block_type,
                endpoint_info,
                cursor_state,
                has_vote_transactions,
            ),
        }
    }
}

fn unsupported_chain_feature_flag_warnings(
    requested_block_type: Option<ChainKind>,
    endpoint_info: &Option<EndpointInfo>,
    cursor_state: Option<&CursorState>,
    without_extended: bool,
    without_votes: bool,
) -> Vec<&'static str> {
    let mut warnings = Vec::new();
    let features =
        PreStreamChainFeatures::resolve(requested_block_type, endpoint_info, cursor_state);

    if without_extended && features.extended_unsupported {
        warnings.push(without_extended_warning_message());
    }
    if without_votes && features.known_without_votes {
        warnings.push(without_votes_warning_message());
    }

    warnings
}

/// Extended output before the first block. Statically unsupported chains
/// disable it; other known chains log the endpoint capability. Protected runs
/// keep it only for a family that maps extended tables.
fn resolve_pre_stream_extended(
    features: PreStreamChainFeatures,
    block_type: Option<ChainKind>,
    extended_enabled: bool,
    without_extended: bool,
    endpoint_info: &Option<EndpointInfo>,
    dry_run: bool,
) -> bool {
    let mut extended = extended_enabled;
    if features.extended_unsupported {
        extended = false;
    } else if features.known_without_votes {
        extended = resolve_extended_mode(extended, without_extended, endpoint_info);
    }
    let maps_extended_tables =
        block_type.is_some_and(|kind| kind.profile().extended == ExtendedOutput::Supported);
    if !dry_run && !maps_extended_tables {
        extended = false;
    }
    extended
}

/// Extended output for a family detected from the first payload (dry-run `auto`).
fn resolve_detected_extended(
    detected: ChainKind,
    extended_enabled: bool,
    without_extended: bool,
    endpoint_info: &Option<EndpointInfo>,
) -> bool {
    if has_unsupported_extended_output(detected.profile()) {
        false
    } else {
        resolve_extended_mode(extended_enabled, without_extended, endpoint_info)
    }
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

fn add_with_votes_metadata(meta: &mut ParquetFileMetadata, with_votes: bool) {
    meta.add("firehose-parquet.with_votes", with_votes.to_string());
}

fn maybe_add_with_votes_metadata(
    meta: &mut ParquetFileMetadata,
    block_type: ChainKind,
    with_votes: bool,
) {
    if block_type.profile().vote_transactions {
        add_with_votes_metadata(meta, with_votes);
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
                    grpc,
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
                    aws,
                } => {
                    init_tracing(&cli.global.log_level, cli.global.verbose);
                    let compression = firehose_parquet::cli::parse_compression(compression)?;
                    let aws = AwsConfig::from(aws);
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
                        grpc.config(),
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
                    aws,
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
                    let aws = AwsConfig::from(aws);
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
                    aws,
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
                    let aws = AwsConfig::from(aws);
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
                    aws,
                } => {
                    let request = PartitionListRequest {
                        index_path: partitions_index.clone(),
                        partition_type: partition_type.clone(),
                        chain: partition_chain.clone(),
                        from: from.clone(),
                        to: to.clone(),
                        limit: *limit,
                    };
                    let aws = AwsConfig::from(aws);
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
                    aws,
                } => {
                    let request = PartitionBoundsRequest {
                        index_path: partitions_index.clone(),
                        partition_type: partition_type.clone(),
                        partition_value: partition_value.clone(),
                        chain: partition_chain.clone(),
                    };
                    let aws = AwsConfig::from(aws);
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
                aws,
            } => {
                let aws = AwsConfig::from(aws);
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
                aws,
            } => {
                let aws = AwsConfig::from(aws);
                firehose_parquet::cli::inspect_parquet(path, *schema_only, *json, Some(&aws))?;
                return Ok(());
            }
            Commands::Validate {
                path,
                cross_partition,
                allow_gaps,
                aws,
            } => {
                let aws = AwsConfig::from(aws);
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
                aws,
                cache_control,
            } => {
                init_tracing(&cli.global.log_level, cli.global.verbose);
                let target = firehose_parquet::rollup::parse_rollup_target(partition)?;
                let compression = firehose_parquet::cli::parse_compression(compression)?;
                let output_path = output.clone().unwrap_or_else(|| source.clone());
                let aws = Some(AwsConfig::from(aws));
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
                aws,
            } => {
                init_tracing(&cli.global.log_level, cli.global.verbose);
                let aws = AwsConfig::from(aws);
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
                aws,
                cache_control,
            } => {
                init_tracing(&cli.global.log_level, cli.global.verbose);
                let compression = firehose_parquet::cli::parse_compression(compression)?;
                let aws = Some(AwsConfig::from(aws));
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
                aws,
            } => {
                init_tracing(&cli.global.log_level, cli.global.verbose);
                let aws = Some(AwsConfig::from(aws));
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Builder;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use blocks::evm::mapper::EvmBlockMapper;
    use blocks::solana::mapper::SolanaBlockMapper;
    use clap::CommandFactory;
    use firehose_parquet::cursor::CursorLocation;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn protected_inventory_declares_every_effective_mapper_schema_before_receipt() {
        for family in ChainKind::ALL {
            for encoding in [
                EncodeBytes::Binary,
                EncodeBytes::Hex,
                EncodeBytes::HexNoPrefix,
                EncodeBytes::Base58,
                EncodeBytes::TronBase58,
            ] {
                for fork_steps in [false, true] {
                    for feature in [false, true] {
                        let mut mapper = family.create_mapper(MapperOptions {
                            extended: feature,
                            with_votes: feature,
                            include_fork_step: fork_steps,
                            encode_bytes: encoding.clone(),
                            synthetic_partition_routing: family == ChainKind::Solana && feature,
                            include_failed_transactions: feature,
                        });
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
    fn mapper_memory_metrics_reset_after_flush() {
        let mut mapper = EvmBlockMapper::new(true, false, EncodeBytes::Hex, true);
        let (_, metrics) = metrics::init();
        assert_eq!(
            update_mapper_buffer_metrics(&metrics, &mut mapper),
            MapperBufferEstimate::default()
        );
        let block = firehose_protos::eth::Block {
            number: 42,
            ..Default::default()
        };
        mapper
            .map_block(
                &prost::Message::encode_to_vec(&block),
                &BlockIdentity {
                    block_num: 42,
                    timestamp: 1_700_000_000,
                    ..Default::default()
                },
                None,
            )
            .unwrap();
        let buffered = update_mapper_buffer_metrics(&metrics, &mut mapper);
        assert!(buffered.total_bytes > 0);
        assert!(buffered.total_bytes >= buffered.largest_table_bytes);
        assert_eq!(
            metrics.mapper_buffer_estimated_bytes.get(),
            buffered.total_bytes as i64
        );
        mapper.flush().unwrap();
        assert_eq!(
            update_mapper_buffer_metrics(&metrics, &mut mapper),
            MapperBufferEstimate::default()
        );
        assert_eq!(metrics.mapper_buffer_rows.get(), 0);
        assert_eq!(metrics.mapper_largest_table_estimated_bytes.get(), 0);
        assert_eq!(metrics.mapper_buffer_estimated_bytes.get(), 0);
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
        assert!(rendered.contains("for this dry run only"), "{rendered}");
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
            resolve_include_failed_transactions(Some(ChainKind::Evm), false, false, None, false),
            (true, vec![])
        );
        let (include, warnings) =
            resolve_include_failed_transactions(Some(ChainKind::Evm), false, true, None, false);
        assert!(!include);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_resolve_include_failed_transactions_evm_include_flag_is_deprecated_no_op() {
        let (include, warnings) =
            resolve_include_failed_transactions(Some(ChainKind::Evm), true, false, None, false);
        assert!(include);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("deprecated"));
    }

    #[test]
    fn test_resolve_include_failed_transactions_exclude_wins_over_include() {
        for block_type in [Some(ChainKind::Evm), Some(ChainKind::Solana), None] {
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
            Some(ChainKind::Solana),
            Some(ChainKind::Tron),
            Some(ChainKind::Near),
            Some(ChainKind::Antelope),
            Some(ChainKind::Cosmos),
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
            Some(ChainKind::Evm),
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
                Some(ChainKind::Evm),
                false,
                true,
                Some(&legacy_cursor),
                false
            ),
            (false, vec![])
        );
        // A read-only dry run's --cursor-override previews the new default.
        assert_eq!(
            resolve_include_failed_transactions(
                Some(ChainKind::Evm),
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
                Some(ChainKind::Evm),
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
        assert!(ChainKind::Solana.profile().block_number_gaps);
        assert!(ChainKind::Near.profile().block_number_gaps);
        assert!(ChainKind::Beacon.profile().block_number_gaps);
        assert!(!ChainKind::Evm.profile().block_number_gaps);
        assert!(!ChainKind::Bitcoin.profile().block_number_gaps);

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
            ChainKind::Evm
        );
    }

    #[test]
    fn test_detect_block_type_bitcoin() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.bitcoin.type.v1.Block").unwrap(),
            ChainKind::Bitcoin
        );
    }

    #[test]
    fn test_detect_block_type_solana() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.solana.type.v1.Block").unwrap(),
            ChainKind::Solana
        );
    }

    #[test]
    fn test_detect_block_type_near() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.near.type.v1.Block").unwrap(),
            ChainKind::Near
        );
    }

    #[test]
    fn test_detect_block_type_antelope() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.antelope.type.v1.Block").unwrap(),
            ChainKind::Antelope
        );
    }

    #[test]
    fn test_detect_block_type_cosmos() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.cosmos.type.v2.Block").unwrap(),
            ChainKind::Cosmos
        );
    }

    #[test]
    fn test_detect_block_type_tron() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.tron.type.v1.Block").unwrap(),
            ChainKind::Tron
        );
    }

    #[test]
    fn test_detect_block_type_beacon() {
        assert_eq!(
            detect_block_type("type.googleapis.com/sf.beacon.type.v1.Block").unwrap(),
            ChainKind::Beacon
        );
    }

    #[test]
    fn test_detect_block_type_unknown() {
        assert!(detect_block_type("type.googleapis.com/sf.unknown.type.v1.Block").is_err());
    }

    #[test]
    fn test_output_encoding_policy_defaults() {
        assert_eq!(
            Some(ChainKind::Evm.default_bytes_encoding(false)),
            Some(EncodeBytes::Hex)
        );
        assert_eq!(
            Some(ChainKind::Evm.default_bytes_encoding(true)),
            Some(EncodeBytes::TronBase58)
        );
        assert_eq!(
            Some(ChainKind::Bitcoin.default_bytes_encoding(false)),
            Some(EncodeBytes::Hex)
        );
        assert_eq!(
            Some(ChainKind::Solana.default_bytes_encoding(false)),
            Some(EncodeBytes::Base58)
        );
        assert_eq!(
            Some(ChainKind::Tron.default_bytes_encoding(false)),
            Some(EncodeBytes::TronBase58)
        );
        assert_eq!(
            Some(ChainKind::Near.default_bytes_encoding(false)),
            Some(EncodeBytes::Base58)
        );
        assert_eq!(
            Some(ChainKind::Antelope.default_bytes_encoding(false)),
            Some(EncodeBytes::HexNoPrefix)
        );
        assert_eq!(
            Some(ChainKind::Cosmos.default_bytes_encoding(false)),
            Some(EncodeBytes::Hex)
        );
        assert_eq!(
            Some(ChainKind::Beacon.default_bytes_encoding(false)),
            Some(EncodeBytes::Hex)
        );
    }

    #[test]
    fn test_default_block_id_encoding_contract() {
        assert_eq!(
            output_block_id_encoding_label(&ChainKind::Evm.default_bytes_encoding(false)),
            Some("hex_0x")
        );
        assert_eq!(
            output_block_id_encoding_label(&ChainKind::Evm.default_bytes_encoding(true)),
            Some("hex_no_prefix")
        );
        assert_eq!(
            output_block_id_encoding_label(&ChainKind::Bitcoin.default_bytes_encoding(false)),
            Some("hex_0x")
        );
        assert_eq!(
            output_block_id_encoding_label(&ChainKind::Solana.default_bytes_encoding(false)),
            Some("base58")
        );
        assert_eq!(
            output_block_id_encoding_label(&ChainKind::Tron.default_bytes_encoding(false)),
            Some("hex_no_prefix")
        );
        assert_eq!(
            output_block_id_encoding_label(&ChainKind::Near.default_bytes_encoding(false)),
            Some("base58")
        );
        assert_eq!(
            output_block_id_encoding_label(&ChainKind::Antelope.default_bytes_encoding(false)),
            Some("hex_no_prefix")
        );
        assert_eq!(
            output_block_id_encoding_label(&ChainKind::Cosmos.default_bytes_encoding(false)),
            Some("hex_0x")
        );
        assert_eq!(
            output_block_id_encoding_label(&ChainKind::Beacon.default_bytes_encoding(false)),
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
        for kind in ChainKind::ALL {
            let mut mapper = kind.create_mapper(MapperOptions {
                extended: false,
                with_votes: false,
                include_fork_step: false,
                encode_bytes: kind.default_bytes_encoding(false),
                synthetic_partition_routing: false,
                include_failed_transactions: false,
            });
            assert!(!mapper.table_names().is_empty(), "{kind}");
            assert!(mapper.flush().is_ok(), "{kind}");
        }
    }

    #[test]
    fn test_parse_requested_block_type_rejects_unknown_types() {
        assert_eq!(parse_requested_block_type("auto").unwrap(), None);
        assert_eq!(parse_requested_block_type("AUTO").unwrap(), None);
        assert_eq!(
            parse_requested_block_type("Solana").unwrap(),
            Some(ChainKind::Solana)
        );
        let error = parse_requested_block_type("Unknown")
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "unsupported block type: unknown. Supported: auto, evm, bitcoin, solana, near, antelope, cosmos, tron, beacon"
        );
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

        // `--block-type` help and errors list every profile, in profile order.
        assert_eq!(BLOCK_TYPES[0], "auto");
        assert!(BLOCK_TYPES[1..]
            .iter()
            .copied()
            .eq(ChainKind::ALL.map(ChainKind::label)));
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
    fn test_resolve_auto_encode_bytes_uses_tron_style_profile_for_tron_chain_name() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "tron".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });

        let resolved = resolve_auto_encode_bytes(Some(ChainKind::Evm), &endpoint_info, true);

        assert_eq!(resolved, EncodeBytes::TronBase58);
    }

    #[test]
    fn test_resolve_auto_encode_bytes_near_prefers_output_contract_over_endpoint_hint() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "near-mainnet".to_string(),
            chain_name_aliases: vec!["near".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 2,
            block_features: vec![],
        });

        let resolved = resolve_auto_encode_bytes(Some(ChainKind::Near), &endpoint_info, false);

        assert_eq!(resolved, EncodeBytes::Base58);
    }

    #[test]
    fn test_resolve_auto_encode_bytes_supported_contracts_override_endpoint_hints() {
        let cases = [
            (ChainKind::Evm, false, 3, EncodeBytes::Hex),
            (ChainKind::Bitcoin, false, 3, EncodeBytes::Hex),
            (ChainKind::Solana, false, 2, EncodeBytes::Base58),
            (ChainKind::Near, false, 2, EncodeBytes::Base58),
            (ChainKind::Antelope, false, 3, EncodeBytes::HexNoPrefix),
            (ChainKind::Cosmos, false, 3, EncodeBytes::Hex),
            (ChainKind::Tron, false, 2, EncodeBytes::TronBase58),
            (ChainKind::Beacon, false, 3, EncodeBytes::Hex),
            (ChainKind::Evm, true, 2, EncodeBytes::TronBase58),
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

            let resolved =
                resolve_auto_encode_bytes(Some(block_type), &endpoint_info, tron_style_evm_profile);

            assert_eq!(
                resolved, expected,
                "expected explicit output contract for block_type={block_type} tron_style_evm_profile={tron_style_evm_profile}"
            );
        }
    }

    #[test]
    fn test_resolve_auto_encode_bytes_tron_style_contract_overrides_endpoint_hint() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "tron-evm".to_string(),
            chain_name_aliases: vec!["tron".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 2,
            block_features: vec![],
        });

        let resolved = resolve_auto_encode_bytes(Some(ChainKind::Evm), &endpoint_info, true);

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
                block_bytes: vec![0x01].into(),
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
                block_bytes: vec![0x02].into(),
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
                ChainKind::Solana,
                &partition
            ));
        }

        assert!(!use_last_known_timestamp_partition_routing(
            ChainKind::Solana,
            &Partition::None
        ));
        assert!(!use_last_known_timestamp_partition_routing(
            ChainKind::Evm,
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
    fn test_timestamp_routing_uses_last_known_anchor_without_interpolation() {
        let mut backfill = TimestampRouting::new(true);
        let first = backfill
            .route_block(
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
        assert_eq!(first.identity.timestamp, 1_000);

        let ready = backfill
            .route_block(
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
        assert_eq!(ready.identity.block_num, 15);
        assert_eq!(ready.identity.timestamp, 1_000);

        let next = backfill
            .route_block(
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
        assert_eq!(next.identity.block_num, 20);
        assert_eq!(next.identity.timestamp, 1_100);
    }

    #[test]
    fn test_timestamp_routing_seeds_genesis_anchor_for_first_missing_block() {
        let mut backfill = TimestampRouting::new(true);
        let ready = backfill
            .route_block(
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
        assert_eq!(ready.identity.block_num, 0);
        assert_eq!(ready.identity.timestamp, SOLANA_GENESIS_TIMESTAMP);
    }

    #[test]
    fn test_timestamp_routing_routes_time_partitions_from_last_known_timestamp() {
        let mut backfill = TimestampRouting::new(true);
        backfill
            .route_block(
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
            .route_block(
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
        let routing_timestamp = routed.identity.timestamp;

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
    fn test_timestamp_routing_routes_first_missing_block_from_genesis_anchor() {
        let mut backfill = TimestampRouting::new(true);
        let routed = backfill
            .route_block(
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
        let routing_timestamp = routed.identity.timestamp;

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
    fn test_timestamp_routing_routes_from_restored_cursor_anchor() {
        let mut backfill = TimestampRouting::new(true);
        let cursor_state = CursorState {
            last_block_num: 99,
            last_timestamp: Some(1_700_000_000),
            ..CursorState::default()
        };

        restore_sparse_routing_cursor_anchor(
            &mut backfill,
            Some(&cursor_state),
            false,
            ChainKind::Solana,
            &Partition::Hour,
        );

        let routed = backfill
            .route_block(
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

        assert_eq!(routed.identity.timestamp, 1_700_000_000);
    }

    #[test]
    fn test_build_file_metadata_includes_block_type() {
        let metadata = build_file_metadata(
            ChainKind::Solana,
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
            ChainKind::Solana,
            &EncodeBytes::Base58,
            "https://example.com:443",
            Compression::Zstd,
            &None,
        );
        maybe_add_synthetic_timestamp_metadata(&mut metadata, ChainKind::Solana, true);

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
            ChainKind::Solana,
            &EncodeBytes::Base58,
            "https://example.com:443",
            Compression::Zstd,
            &None,
        );
        maybe_add_synthetic_timestamp_metadata(&mut metadata, ChainKind::Solana, false);

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
        assert!(ChainKind::Solana.matches_chain_name("solana"));
        assert!(ChainKind::Solana.matches_chain_name("solana-mainnet-beta"));
        assert!(!ChainKind::Solana.matches_chain_name("mainnet"));
    }

    #[test]
    fn test_chain_name_is_antelope_matches_expected_aliases() {
        assert!(ChainKind::Antelope.matches_chain_name("antelope"));
        assert!(ChainKind::Antelope.matches_chain_name("antelope-mainnet"));
        assert!(ChainKind::Antelope.matches_chain_name("eos"));
        assert!(!ChainKind::Antelope.matches_chain_name("mainnet"));
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

        assert!(endpoint_chain_has(&ei, has_vote_transactions));
        assert!(endpoint_chain_has(&ei, has_nullable_timestamps));
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

        assert!(endpoint_chain_has(&ei, has_unsupported_extended_output));
        assert!(!endpoint_chain_has(&ei, has_vote_transactions));
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

        assert!(!endpoint_chain_has(&ei, has_vote_transactions));
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
            unsupported_chain_feature_flag_warnings(
                Some(ChainKind::Solana),
                &None,
                None,
                true,
                false
            ),
            vec![WITHOUT_EXTENDED_WARNING]
        );
    }

    #[test]
    fn test_unsupported_chain_feature_flag_warnings_warn_for_without_votes_on_non_solana() {
        assert_eq!(
            unsupported_chain_feature_flag_warnings(Some(ChainKind::Evm), &None, None, false, true),
            vec![WITHOUT_VOTES_NON_SOLANA_WARNING]
        );
    }

    #[test]
    fn test_unsupported_chain_feature_flag_warnings_warn_for_without_extended_on_antelope() {
        assert_eq!(
            unsupported_chain_feature_flag_warnings(
                Some(ChainKind::Antelope),
                &None,
                None,
                true,
                false
            ),
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

        assert!(unsupported_chain_feature_flag_warnings(
            Some(ChainKind::Evm),
            &endpoint_info,
            None,
            true,
            false
        )
        .is_empty());
        assert!(unsupported_chain_feature_flag_warnings(
            Some(ChainKind::Solana),
            &None,
            None,
            false,
            true
        )
        .is_empty());
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
        maybe_add_with_votes_metadata(&mut solana_meta, ChainKind::Solana, true);
        assert_eq!(
            find_meta(&solana_meta, "firehose-parquet.with_votes"),
            Some("true")
        );

        let mut evm_meta = ParquetFileMetadata::new();
        maybe_add_with_votes_metadata(&mut evm_meta, ChainKind::Evm, true);
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
            ChainKind::Evm,
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
            ChainKind::Evm,
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
            ChainKind::Near,
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
            ChainKind::Evm,
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
            ChainKind::Evm,
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
            ChainKind::Evm,
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
            ChainKind::Evm,
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
            ChainKind::Evm,
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
            ChainKind::Evm,
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
            Some(ChainKind::Evm),
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
