use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use clap::{Args, Parser};
use firehose_parquet::cli::{
    build_config, init_tracing, load_env_file, resolve_output_root, validate_s3_output_credentials,
    AwsConfig, BuildArgs, Commands,
};
use firehose_parquet::config::{BlockMetadata, Compression, Config};
use firehose_parquet::cursor::{CursorLocation, CursorState};
use firehose_parquet::dataset_lock::DatasetOwnership;
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::flush::{
    FlushSizing, MapperBufferEstimate, PaceCause, PaceDetector, PaceTiming, PaceTransition,
    SizeFlushTrigger, StreamPace,
};
use firehose_parquet::grpc::{
    is_shutdown_error, unless_shutdown, CancellationToken, EndpointInfo, FirehoseClient,
    ShutdownRequested,
};
use firehose_parquet::ingest::{
    declare_inventory, ingestion_mutation_scopes, load_authoritative_resume, IngestionSession,
    MapperSemantics, CURSOR_OVERRIDE_REFUSED, SOLANA_GENESIS_ROUTING_SECONDS,
};
use firehose_parquet::metrics;
use firehose_parquet::networks::{resolve_network_endpoint, EndpointSource};
use firehose_parquet::traits::{fork_step_name, BlockIdentity, BlockMapper, StreamEvent};
use firehose_parquet::writer::ParquetFileMetadata;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use blocks::chain::{ChainKind, ChainProfile, ExtendedOutput, MapperOptions};

#[cfg(test)]
mod chain_profile_tests;
mod ingestion;
use ingestion::run_ingestion;

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
  Utility workflows live under subcommands such as `scan`, `inspect`, `validate`, and `verify`.

Examples:
  # Run a bounded historical ingestion
  fireparq build --network mainnet \\
    --start-block 20000000 --stop-block 20001000

  # Backfill from a block and keep following finalized blocks
  fireparq build --network solana-mainnet-beta \\
    --start-block 250000000

  # Resume an existing output from its authoritative .fireparq-ingest/
  # state, or start a new one at the endpoint's first streamable block
  # (_fireparq/cursor.parquet is only an optional mirror; --cursor none disables it)
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

    /// Load settings from this env file instead of ./.env (parent directories are never searched)
    #[arg(
        long,
        env = "FIREPARQ_ENV_FILE",
        value_name = "PATH",
        hide_env_values = true,
        global = true,
        help_heading = "Runtime / Logging"
    )]
    env_file: Option<PathBuf>,
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

/// The trigger that flushes the open window after a block, if any. Pure: the
/// caller passes the window's age and the stream pace, so tests inject both.
fn next_mapper_flush_trigger(
    flush_rows: Option<usize>,
    max_table_rows: usize,
    flush_blocks: Option<u64>,
    blocks_since_flush: u64,
    flush_interval_secs: Option<u64>,
    window_age: Duration,
    pace: StreamPace,
    sizing: &FlushSizing,
    estimate: MapperBufferEstimate,
) -> Option<MapperFlushTrigger> {
    // Zero disables the row and interval triggers, like `--flush-bytes 0`;
    // `rows >= 0` or `elapsed >= 0` would otherwise flush after every block.
    // The interval bounds how long rows wait at the chain head; while the
    // stream replays history only the size, row and block triggers apply (#659).
    let time_to_flush = !pace.is_catching_up()
        && flush_interval_secs
            .filter(|secs| *secs > 0)
            .is_some_and(|secs| window_age >= Duration::from_secs(secs));

    let rows_to_flush = flush_rows
        .filter(|limit| *limit > 0)
        .is_some_and(|limit| max_table_rows >= limit);

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

/// Log one pace switch (#659) with the evidence that decided it.
fn log_pace_transition(
    transition: &PaceTransition,
    block_num: u64,
    flush_interval_secs: Option<u64>,
) {
    let pace = transition.pace.as_str();
    let ratio = transition
        .ratio
        .map_or_else(|| "unknown".to_string(), |ratio| format!("{ratio:.1}"));
    let blocks_per_sec = format!("{:.1}", transition.blocks_per_sec);
    let evidence_secs = format!("{:.1}", transition.evidence.as_secs_f64());
    let interval = flush_interval_secs.filter(|secs| *secs > 0);
    match (transition.pace, transition.cause, interval) {
        (StreamPace::CatchingUp, _, Some(flush_interval_secs)) => info!(
            pace, block_num, block_time_ratio = %ratio, blocks_per_sec = %blocks_per_sec,
            evidence_secs = %evidence_secs, flush_interval_secs,
            "catching up: block time advances faster than wall-clock time, so --flush-interval-secs is suspended and the size, row and block triggers flush"
        ),
        (StreamPace::CatchingUp, _, None) => info!(
            pace, block_num, block_time_ratio = %ratio, blocks_per_sec = %blocks_per_sec,
            evidence_secs = %evidence_secs,
            "catching up: block time advances faster than wall-clock time"
        ),
        (StreamPace::CaughtUp, PaceCause::MissingTimestamps, _) => {
            info!(
                pace, block_num, block_time_ratio = %ratio, blocks_per_sec = %blocks_per_sec,
                evidence_secs = %evidence_secs, flush_interval_secs = ?interval,
                "caught up: no block timestamp to measure the pace, so the stream is treated as following the chain head"
            )
        }
        (StreamPace::CaughtUp, _, Some(flush_interval_secs)) => info!(
            pace, block_num, block_time_ratio = %ratio, blocks_per_sec = %blocks_per_sec,
            evidence_secs = %evidence_secs, flush_interval_secs,
            "caught up: block time advances at about wall-clock speed, so --flush-interval-secs applies again"
        ),
        (StreamPace::CaughtUp, _, None) => info!(
            pace, block_num, block_time_ratio = %ratio, blocks_per_sec = %blocks_per_sec,
            evidence_secs = %evidence_secs,
            "caught up: block time advances at about wall-clock speed"
        ),
    }
}

/// Protected transactions publish every table of a flush or none, so no writer
/// buffer is retained between flushes and there are no buffered stats to log.
fn log_writer_flush_outcome(trigger: &str, tables: usize, rows: usize, materialized: bool) {
    if materialized {
        info!(
            trigger,
            tables, rows, "writer materialized parquet output for mapper flush"
        );
    } else {
        info!(
            trigger,
            tables, rows, "mapper flush contained no nonempty table output"
        );
    }
}

/// How the Firehose stream ended, which decides whether the remaining mapper
/// window is committed through the protected session on exit.
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
        // No destructor or release runs: an interrupted request may still take
        // effect, so any S3 bucket ownership stays held for operator recovery.
        warn!("second shutdown signal received, exiting immediately; in-flight writes may be interrupted and any S3 bucket ownership is retained (inspect it with `fireparq recovery status <output>`)");
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

    /// Only a completed stream commits its partial mapper window (one all-table
    /// transaction that also advances authority and the mirror). After a
    /// shutdown or a failure the window is discarded and authority stays at the
    /// last committed flush, so the next run replays it; committing after a
    /// failure could advance authority past rows whose mapping or write failed.
    fn materializes_buffers(self) -> bool {
        matches!(self, Self::Completed)
    }
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

/// Logged when resumed EVM state keeps excluding failed transactions.
const EVM_FAILED_TRANSACTIONS_EXCLUDED_WARNING: &str = "this output's resume state records failed transactions as excluded (--exclude-failed-transactions, or the EVM default before #494); still excluding them so this output stays consistent. Pass --exclude-failed-transactions to keep this and silence the warning. To switch to the new default, rebuild into a new empty output root with an absent cursor mirror; --cursor-override cannot change protected output";

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
            warnings.push(EVM_FAILED_TRANSACTIONS_EXCLUDED_WARNING.to_string());
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
    endpoint_info: &Option<EndpointInfo>,
    extended: bool,
    final_blocks_only: bool,
    include_failed_transactions: bool,
) -> ParquetFileMetadata {
    let mut meta = ParquetFileMetadata::new();
    add_common_file_metadata(&mut meta, block_type, encoding, endpoint, endpoint_info);
    meta.add("firehose-parquet.compression", compression.to_string());
    meta.add("firehose-parquet.partition", "date");
    add_cursor_compatibility_metadata(
        &mut meta,
        extended,
        final_blocks_only,
        include_failed_transactions,
    );
    meta
}

/// Whether blocks of this family route by the last known block time: every
/// table is partitioned by date, and these families may omit block times.
fn use_last_known_timestamp_partition_routing(block_type: ChainKind) -> bool {
    block_type.profile().nullable_timestamps
}

fn validate_block_timestamp(block_num: u64, timestamp: i64) -> Result<()> {
    firehose_parquet::traits::checked_timestamp(timestamp)?;
    if timestamp != 0 {
        return Ok(());
    }
    Err(anyhow!(
        "block {block_num} is missing timestamp metadata; date partitioning requires timestamps"
    ))
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
                timestamp: SOLANA_GENESIS_ROUTING_SECONDS,
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
) {
    if cursor_override || !use_last_known_timestamp_partition_routing(block_type) {
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
///
/// The dataset root is `--output` as given, with `{chain}` expanded to the
/// endpoint's `chain_name` ([`resolve_output_root`]). A nonempty `chain_name`
/// is required either way: it stays in file metadata and in the protected
/// dataset identity.
fn resolve_output(output: &Path, endpoint_info: &Option<EndpointInfo>) -> Result<PathBuf> {
    let chain_name = endpoint_info
        .as_ref()
        .map_or("", |info| info.chain_name.as_str());
    let output = output.to_str().context("--output must be valid UTF-8")?;
    Ok(PathBuf::from(resolve_output_root(output, chain_name)?))
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

/// Dry-run check that a bounded stream which ended cleanly reached its last
/// requested block (`stop_block - 1`). `last_block_num` is the highest block
/// processed by this run or recorded in the legacy cursor it previewed.
///
/// This mirrors the protected completion rule (`Checkpoint::complete_request`)
/// on every chain: a sparse or empty tail cannot prove the bound, even where
/// block numbers legitimately skip (Solana, NEAR, Beacon). A dry run therefore
/// fails exactly where the real build would retain its prefix and exit nonzero.
fn ensure_bounded_stream_reached_stop(stop_block: u64, last_block_num: Option<u64>) -> Result<()> {
    let last_requested_block = stop_block.saturating_sub(1);
    if last_block_num.is_some_and(|block| block >= last_requested_block) {
        return Ok(());
    }
    let reached =
        last_block_num.map_or_else(|| "no block".to_string(), |block| format!("block {block}"));
    Err(anyhow!(
        "Firehose stream ended at {reached}, before the last requested block {last_requested_block} (--stop-block {stop_block} is exclusive). A real build would keep the accepted prefix but exit nonzero, on every chain, because a sparse or empty tail cannot prove the bound; choose a stop bound ending at an observed block"
    ))
}

/// Validate the effective cursor before startup I/O.
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
            firehose_parquet::s3::build_ingestion_mutation_client(config, bucket)
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

fn format_block_timestamp(timestamp: i64) -> Result<String> {
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
fn format_optional_block_timestamp(timestamp: i64) -> Option<String> {
    (timestamp != 0).then(|| {
        format_block_timestamp(timestamp)
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

/// Whether to report the loaded env file on stderr. Commands that install the
/// tracing subscriber log it there instead (`init_tracing`); completions stay
/// silent because their output is a script.
fn env_file_notice_on_stderr(command: Option<&Commands>) -> bool {
    !matches!(
        command,
        Some(
            Commands::Build(_)
                | Commands::Verify { .. }
                | Commands::Merge { .. }
                | Commands::Truncate { .. }
                | Commands::Completions { .. }
        )
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    // Only ./.env or an explicit --env-file / FIREPARQ_ENV_FILE; never parents (#617).
    let env_file = load_env_file(std::env::args_os())?;
    let cli = Cli::parse();
    if env_file_notice_on_stderr(cli.command.as_ref()) {
        // Commands without a log subscriber still say which file supplied settings.
        if let Some(loaded) = env_file.as_ref() {
            eprintln!("{}", loaded.summary());
        }
    }

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
    use blocks::evm::mapper::EvmBlockMapper;
    use clap::CommandFactory;
    use firehose_parquet::cursor::CursorLocation;
    use firehose_parquet::date_partition::DatePartition;
    use std::path::PathBuf;
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
                        let types = family.profile().delta_types();
                        let first = mapper.flush().unwrap();
                        let inventory = declare_inventory(&first, &mapper.table_names(), &types)
                            .unwrap_or_else(|error| panic!("{family} empty inventory: {error}"));
                        assert!(!inventory.is_empty());
                        let second = mapper.flush().unwrap();
                        assert_eq!(
                            inventory,
                            declare_inventory(&second, &mapper.table_names(), &types).unwrap(),
                            "{family} empty schema changes between flushes"
                        );
                    }
                }
            }
        }
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
                StreamEvent::default(),
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
            Duration::ZERO,
            StreamPace::CaughtUp,
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
            Duration::ZERO,
            StreamPace::CaughtUp,
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
                Duration::ZERO,
                StreamPace::CaughtUp,
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
            Duration::ZERO,
            StreamPace::CaughtUp,
            &FlushSizing::new(0, u64::MAX).unwrap(),
            MapperBufferEstimate {
                largest_table_bytes: 128,
                total_bytes: 128,
            },
        );
        assert_eq!(trigger, Some(MapperFlushTrigger::Blocks));
    }

    #[test]
    fn test_next_mapper_flush_trigger_zero_rows_and_interval_are_disabled() {
        // `rows >= 0` and `elapsed >= 0` used to flush after every block.
        let long_ago = Duration::from_secs(3_600);
        let caught_up = StreamPace::CaughtUp;
        for max_table_rows in [0, 1, usize::MAX] {
            let trigger = next_mapper_flush_trigger(
                Some(0),
                max_table_rows,
                None,
                1,
                Some(0),
                long_ago,
                caught_up,
                &FlushSizing::new(0, u64::MAX).unwrap(),
                MapperBufferEstimate {
                    largest_table_bytes: 128,
                    total_bytes: 128,
                },
            );
            assert_eq!(trigger, None, "max_table_rows={max_table_rows}");
        }

        // Positive limits still fire.
        let estimate = MapperBufferEstimate {
            largest_table_bytes: 128,
            total_bytes: 128,
        };
        let sizing = FlushSizing::new(0, u64::MAX).unwrap();
        assert_eq!(
            next_mapper_flush_trigger(
                Some(1),
                1,
                None,
                1,
                None,
                long_ago,
                caught_up,
                &sizing,
                estimate
            ),
            Some(MapperFlushTrigger::Rows)
        );
        assert_eq!(
            next_mapper_flush_trigger(
                None,
                1,
                None,
                1,
                Some(1),
                long_ago,
                caught_up,
                &sizing,
                estimate
            ),
            Some(MapperFlushTrigger::Interval)
        );
        assert_eq!(
            next_mapper_flush_trigger(
                None,
                1,
                None,
                1,
                Some(60),
                Duration::ZERO,
                caught_up,
                &sizing,
                estimate
            ),
            None
        );
    }

    /// #659: the interval applies only while caught up; every other trigger
    /// keeps working while the stream catches up.
    #[test]
    fn test_next_mapper_flush_trigger_suspends_only_the_interval_while_catching_up() {
        let small = MapperBufferEstimate {
            largest_table_bytes: 128,
            total_bytes: 128,
        };
        let sizing = FlushSizing::new(1_000_000, 1_000_000_000).unwrap();
        let trigger = |rows, blocks, interval, age: u64, pace, estimate| {
            next_mapper_flush_trigger(
                rows,
                10,
                blocks,
                10,
                interval,
                Duration::from_secs(age),
                pace,
                &sizing,
                estimate,
            )
        };
        let (catching_up, caught_up) = (StreamPace::CatchingUp, StreamPace::CaughtUp);
        // The window is an hour older than the one-second interval.
        assert_eq!(
            trigger(None, None, Some(1), 3_600, caught_up, small),
            Some(MapperFlushTrigger::Interval)
        );
        assert_eq!(
            trigger(None, None, Some(1), 3_600, catching_up, small),
            None
        );
        // The interval boundary is inclusive at the head.
        assert_eq!(
            trigger(None, None, Some(60), 60, caught_up, small),
            Some(MapperFlushTrigger::Interval)
        );
        assert_eq!(trigger(None, None, Some(60), 59, caught_up, small), None);
        // Size, memory, rows and blocks still flush while catching up.
        let target = MapperBufferEstimate {
            largest_table_bytes: 1_000_000,
            total_bytes: 1_000_000,
        };
        let memory = MapperBufferEstimate {
            largest_table_bytes: 1,
            total_bytes: 1_000_000_000,
        };
        assert_eq!(
            trigger(None, None, Some(1), 3_600, catching_up, target),
            Some(MapperFlushTrigger::Bytes)
        );
        assert_eq!(
            trigger(None, None, Some(1), 3_600, catching_up, memory),
            Some(MapperFlushTrigger::Memory)
        );
        assert_eq!(
            trigger(Some(10), None, Some(1), 3_600, catching_up, small),
            Some(MapperFlushTrigger::Rows)
        );
        assert_eq!(
            trigger(None, Some(10), Some(1), 3_600, catching_up, small),
            Some(MapperFlushTrigger::Blocks)
        );
    }

    #[test]
    fn test_build_subcommand_rejects_zero_stop_block_and_has_no_partition_mode() {
        let error = Cli::try_parse_from([
            "fireparq",
            "build",
            "--network",
            "mainnet",
            "--stop-block",
            "0",
        ])
        .expect_err("zero should be rejected");
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(error.to_string().contains("--stop-block"), "{error}");
        // Every table is written as <table>/date=YYYY-MM-DD/ (#652).
        for (flag, value) in [("--partition", "date"), ("--block-range-size", "100")] {
            let error =
                Cli::try_parse_from(["fireparq", "build", "--network", "mainnet", flag, value])
                    .expect_err("output partition flags are removed");
            assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn test_env_example_matches_cli_environment_variables() {
        use std::collections::BTreeSet;
        fn collect(command: &clap::Command, names: &mut BTreeSet<String>) {
            for arg in command.get_arguments() {
                if let Some(env) = arg.get_env() {
                    names.insert(env.to_string_lossy().into_owned());
                }
            }
            for subcommand in command.get_subcommands() {
                collect(subcommand, names);
            }
        }
        let mut cli_names = BTreeSet::new();
        collect(&Cli::command(), &mut cli_names);
        assert!(cli_names.contains("FLUSH_MEMORY_BYTES"));

        let example = include_str!("../../../.env.example");
        let listed: BTreeSet<String> = example
            .lines()
            .filter_map(|line| {
                let (name, _) = line.trim_start_matches('#').trim().split_once('=')?;
                (!name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
                .then(|| name.to_string())
            })
            .collect();
        let missing: Vec<_> = cli_names.difference(&listed).collect();
        assert!(
            missing.is_empty(),
            ".env.example is missing CLI variables: {missing:?}"
        );

        let credentials: BTreeSet<&str> = firehose_parquet::auth::PINAX_API_KEY_ENV_VARS
            .iter()
            .chain(firehose_parquet::auth::PINAX_API_TOKEN_ENV_VARS)
            .chain(firehose_parquet::auth::STREAMINGFAST_API_KEY_ENV_VARS)
            .chain(firehose_parquet::auth::STREAMINGFAST_API_TOKEN_ENV_VARS)
            .copied()
            .collect();
        for name in &credentials {
            assert!(listed.contains(*name), ".env.example is missing {name}");
        }
        let unknown: Vec<_> = listed
            .iter()
            .filter(|name| {
                !cli_names.contains(*name)
                    && !credentials.contains(name.as_str())
                    && !name.starts_with("FIREHOSE_ENDPOINT_")
            })
            .collect();
        assert!(
            unknown.is_empty(),
            ".env.example lists variables the CLI does not read: {unknown:?}"
        );
        assert!(!example.contains("FLUSH_BYTES=134217728"));
        assert!(example.contains(&format!(
            "FLUSH_BYTES={}",
            firehose_parquet::config::DEFAULT_FLUSH_BYTES
        )));
        assert!(example.contains(&format!(
            "FLUSH_MEMORY_BYTES={}",
            firehose_parquet::config::DEFAULT_FLUSH_MEMORY_BYTES
        )));
        assert!(example.contains(&format!(
            "GRPC_WINDOW_BYTES={}",
            firehose_parquet::config::DEFAULT_GRPC_WINDOW_BYTES
        )));
        assert!(example.contains(&format!(
            "GRPC_MAX_MESSAGE_BYTES={}",
            firehose_parquet::config::DEFAULT_GRPC_MAX_MESSAGE_BYTES
        )));
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

    /// `build` resumes from `.fireparq-ingest/` authority, not from the optional
    /// `cursor.parquet` mirror, and never probes missing blocks.
    #[test]
    fn test_root_and_build_help_describe_authoritative_resume() {
        let root = Cli::command().render_long_help().to_string();
        let build = command_help(&["fireparq", "build", "--help"]);
        for help in [&root, &build] {
            assert!(help.contains(".fireparq-ingest/"), "{help}");
            assert!(help.contains("--cursor none"), "{help}");
            assert!(!help.contains("Resume from cursor"), "{help}");
            assert!(!help.contains("probe retries"), "{help}");
        }
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
    fn test_effective_s3_cursor_requires_complete_credentials() {
        let cursor = "s3://state/worker.parquet".to_string();
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
                firehose_parquet::artifacts::DEFAULT_CURSOR_MIRROR,
                "data",
                "mainnet/_fireparq/cursor.parquet",
            ),
            (
                "s3://data",
                firehose_parquet::artifacts::DEFAULT_CURSOR_MIRROR,
                "data",
                "_fireparq/cursor.parquet",
            ),
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

    /// The default mirror follows the resolved dataset root: `--output` as
    /// given, or with `{chain}` expanded.
    #[test]
    fn test_resolve_cursor_location_places_default_local_cursor_under_the_dataset_root() {
        for (output, cursor) in [
            ("./output", "./output/_fireparq/cursor.parquet"),
            (
                "./output/{chain}",
                "./output/mainnet/_fireparq/cursor.parquet",
            ),
        ] {
            let config = Config {
                output: resolve_output(Path::new(output), &endpoint_info_named("mainnet")).unwrap(),
                cursor_path: Some(firehose_parquet::artifacts::DEFAULT_CURSOR_MIRROR.to_string()),
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
                    assert_eq!(path, std::path::PathBuf::from(cursor));
                }
                other => panic!("expected local cursor location, got {other:?}"),
            }
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
        firehose_parquet::cursor::save_cursor_parquet(
            &cursor_path,
            &CursorState {
                cursor: "cursor-at-block-200".to_string(),
                last_block_num: 200,
                ..CursorState::default()
            },
        )
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
        assert!(help.contains("authoritative state"));
        assert!(help.contains("first streamable block"));
        assert!(help.contains("When omitted, the build runs in live mode"));
        // `build` never probes missing blocks.
        assert!(!help.contains("Missing blocks are skipped automatically"));
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

    /// `--output` is the only layout control of `build`: it has no
    /// chain-directory flag or env var, and its help documents the opt-in
    /// `{chain}` placeholder with an example.
    #[test]
    fn test_output_is_the_only_layout_control_and_help_documents_chain() {
        let command = Cli::command();
        let build = command.find_subcommand("build").unwrap();
        for argument in build.get_arguments() {
            let long = argument.get_long().unwrap_or_default();
            let env = argument
                .get_env()
                .map(|env| env.to_string_lossy().into_owned())
                .unwrap_or_default();
            assert!(
                !long.contains("chain-dir") && !env.contains("CHAIN_DIR"),
                "{long} {env}"
            );
        }
        let build = command_help(&["fireparq", "build", "--help"]);
        for snippet in [
            "Used exactly as given",
            "`{chain}` expands to the endpoint's chain_name",
            "--output 's3://datasets/v1/{chain}'",
            "--output s3://ethereum-mainnet",
        ] {
            assert!(build.contains(snippet), "{snippet}: {build}");
        }
    }

    /// `partitions` and all of its subcommands are removed (#653).
    #[test]
    fn test_partitions_subcommand_is_removed() {
        for subcommand in ["build", "ls", "validate", "resolve", "shard"] {
            let err = Cli::try_parse_from(["fireparq", "partitions", subcommand])
                .expect_err("partitions is not a subcommand");
            assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
        }
        assert!(Cli::command().find_subcommand("partitions").is_none());
        assert!(!command_help(&["fireparq", "--help"]).contains("partitions"));
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
            ("--cursor-template", "cursor/{chain}.parquet"),
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
        let error = ensure_bounded_stream_reached_stop(200, Some(150)).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("block 150"), "{message}");
        assert!(message.contains("last requested block 199"), "{message}");

        let error = ensure_bounded_stream_reached_stop(200, None).unwrap_err();
        assert!(error.to_string().contains("no block"), "{error}");
    }

    #[test]
    fn test_bounded_stream_reaching_last_requested_block_is_complete() {
        assert!(ensure_bounded_stream_reached_stop(200, Some(199)).is_ok());
        assert!(ensure_bounded_stream_reached_stop(200, Some(250)).is_ok());
    }

    /// Dry runs used to accept a sparse tail on Solana, NEAR and Beacon while
    /// the real protected build refused it. Both now apply one rule.
    #[test]
    fn test_bounded_dry_run_on_sparse_chain_matches_protected_completion() {
        // Skipped heights remain a chain fact, but they no longer relax the
        // completion proof for any family.
        assert!(ChainKind::Solana.profile().block_number_gaps);
        assert!(ChainKind::Near.profile().block_number_gaps);
        assert!(ChainKind::Beacon.profile().block_number_gaps);
        assert!(!ChainKind::Evm.profile().block_number_gaps);
        assert!(!ChainKind::Bitcoin.profile().block_number_gaps);
        let error = ensure_bounded_stream_reached_stop(200, Some(197)).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("block 197"), "{message}");
        assert!(message.contains("on every chain"), "{message}");
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

        let tron = Some(EndpointInfo {
            chain_name: "tron".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });
        assert!(endpoint_uses_tron_style_evm_profile(&tron));
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

    fn endpoint_info_named(chain_name: &str) -> Option<EndpointInfo> {
        Some(EndpointInfo {
            chain_name: chain_name.to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        })
    }

    /// The dataset root is `--output` byte for byte, for local paths and S3
    /// URIs alike (only S3 trailing `/` separators are dropped): no
    /// `<chain_name>` directory is appended.
    #[test]
    fn test_resolve_output_uses_the_output_exactly_as_given() {
        let ei = endpoint_info_named("mainnet");
        for base in [
            ".",
            "./output",
            "./output/",
            "/data/output",
            "s3://ethereum-mainnet",
            "s3://bucket/v1",
        ] {
            let resolved = resolve_output(&PathBuf::from(base), &ei).unwrap();
            assert_eq!(resolved.to_str(), Some(base), "{base}");
            assert_eq!(resolved, PathBuf::from(base), "{base}");
        }
        for (base, expected) in [
            ("s3://ethereum-mainnet/", "s3://ethereum-mainnet"),
            ("s3://bucket/v1/", "s3://bucket/v1"),
        ] {
            let resolved = resolve_output(&PathBuf::from(base), &ei).unwrap();
            assert_eq!(resolved.to_str(), Some(expected), "{base}");
        }
    }

    /// `{chain}` is the opt-in way to name a directory after the endpoint's
    /// canonical chain name; v0.7.x's `<output>/<chain_name>` layout is
    /// `<output>/{chain}`.
    #[test]
    fn test_resolve_output_expands_the_chain_placeholder() {
        let ei = endpoint_info_named("mainnet");
        for (base, expected) in [
            ("./{chain}", "./mainnet"),
            ("./output/{chain}", "./output/mainnet"),
            ("/data/{chain}/raw", "/data/mainnet/raw"),
            ("s3://datasets/{chain}", "s3://datasets/mainnet"),
            (
                "s3://datasets/v1/{chain}/raw",
                "s3://datasets/v1/mainnet/raw",
            ),
            ("s3://datasets/{chain}/", "s3://datasets/mainnet"),
        ] {
            let resolved = resolve_output(&PathBuf::from(base), &ei).unwrap();
            assert_eq!(resolved.to_str(), Some(expected), "{base}");
        }
        let error = resolve_output(&PathBuf::from("s3://{chain}/raw"), &ei)
            .unwrap_err()
            .to_string();
        assert!(error.contains("S3 bucket name"), "{error}");
    }

    #[test]
    fn test_resolve_output_without_endpoint_info() {
        for base in [".", "./output/{chain}"] {
            let error = resolve_output(&PathBuf::from(base), &None)
                .unwrap_err()
                .to_string();
            assert!(error.contains("nonempty chain_name"), "{error}");
        }
    }

    /// EndpointInfo with a nonempty chain name stays mandatory even when
    /// `--output` does not use `{chain}`: the name is still recorded in file
    /// metadata and in the protected dataset identity.
    #[test]
    fn test_resolve_output_empty_chain_name() {
        for base in [".", "./output/{chain}", "s3://bucket"] {
            for chain_name in ["", "   "] {
                let error = resolve_output(&PathBuf::from(base), &endpoint_info_named(chain_name))
                    .unwrap_err()
                    .to_string();
                assert!(error.contains("nonempty chain_name"), "{error}");
            }
        }
    }

    #[test]
    fn test_validate_block_timestamp_missing_errors() {
        let err = validate_block_timestamp(42, 0).expect_err("missing timestamp should error");
        assert!(err.to_string().contains("missing timestamp metadata"));
        assert!(err
            .to_string()
            .contains("date partitioning requires timestamps"));
        validate_block_timestamp(42, 1_700_000_000).unwrap();
    }

    #[test]
    fn test_validate_block_timestamp_missing_in_first_streamable_block_is_generic() {
        let err = validate_block_timestamp(0, 0)
            .expect_err("missing timestamp should still report a missing timestamp");
        let message = err.to_string();

        assert!(message.contains("missing timestamp metadata"));
        assert!(!message.contains("--bootstrap-missing-genesis-timestamp"));
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
    fn test_last_known_timestamp_partition_routing_is_automatic_for_solana() {
        assert!(use_last_known_timestamp_partition_routing(
            ChainKind::Solana
        ));
        assert!(!use_last_known_timestamp_partition_routing(ChainKind::Evm));
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
        assert_eq!(ready.identity.timestamp, SOLANA_GENESIS_ROUTING_SECONDS);
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
        assert_eq!(
            DatePartition::from_timestamp(routing_timestamp).unwrap(),
            DatePartition::from_timestamp(1_700_000_000).unwrap()
        );
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
        assert_eq!(routed.identity.timestamp, SOLANA_GENESIS_ROUTING_SECONDS);
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

    #[test]
    fn test_format_optional_block_timestamp_handles_missing_timestamp() {
        assert_eq!(
            format_optional_block_timestamp(-1).as_deref(),
            Some("1969-12-31 23:59:59")
        );
        for timestamp in [i64::MIN, i64::MAX, 1_700_000_000_000] {
            assert_eq!(
                format_optional_block_timestamp(timestamp),
                Some(format!("invalid unix timestamp {timestamp}"))
            );
        }
        assert_eq!(format_optional_block_timestamp(0), None);
        assert_eq!(
            format_optional_block_timestamp(1_690_815_590).as_deref(),
            Some("2023-07-31 14:59:50")
        );
    }
}
