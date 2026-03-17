use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use clap::{Args, Parser};
use firehose_parquet::cli::{
    build_config, build_partitions_index_path, build_partitions_output_root, init_tracing,
    list_partitions_from_index, load_dotenv, parse_partition_build_types,
    parse_partition_shard_strategy, read_partitions_build_rows, resolve_cursor_template,
    resolve_partition_command, resolve_s3_output_root, shard_partitions_from_index,
    validate_partitions_index, validate_s3_output_credentials, write_partitions_index_strict,
    AwsConfig, BuildArgs, Commands, CursorTemplateContext, PartitionBoundsRequest,
    PartitionBuildResult, PartitionBuildRow, PartitionBuildType, PartitionIndexBuilder,
    PartitionListRequest, PartitionResolveOptions, PartitionShardRequest, PartitionValidateRequest,
    PartitionsCommands,
};
use firehose_parquet::config::{BlockMetadata, Compression, Config, Partition};
use firehose_parquet::cursor::{CursorLocation, CursorState};
use firehose_parquet::encode::{parse_encode_bytes, EncodeBytes};
use firehose_parquet::grpc::{EndpointInfo, FirehoseClient};
use firehose_parquet::metrics;
use firehose_parquet::networks::{resolve_network_endpoint, EndpointSource};
use firehose_parquet::traits::{decode_id_bytes, fork_step_name, BlockIdentity, BlockMapper};
use firehose_parquet::writer::{OutputWriter, ParquetFileMetadata, WriterBufferStats};
use object_store::ObjectStore;
use std::collections::HashMap;
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
const DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES: u64 = 134_217_728;
const SOLANA_EXTENDED_ERROR: &str =
    "--extended is not supported for Solana; use --with-votes to emit vote_transactions";
const ANTELOPE_EXTENDED_ERROR: &str = "--extended is not supported for Antelope";
const WITH_VOTES_NON_SOLANA_ERROR: &str = "--with-votes is only supported for Solana";

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
    --start-block 250000000 --live

  # Resume live mode from cursor.parquet, or fall back to the endpoint's
  # first streamable block when no cursor exists
  fireparq build --network mainnet --live

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

fn write_mapper_flush(
    writer: &mut OutputWriter,
    batches: &HashMap<String, RecordBatch>,
    metadata: &BlockMetadata,
    force_materialize: bool,
) -> Result<WriterFlushOutcome> {
    let mut materialized = writer.write_all(batches, metadata)?;
    if force_materialize && !materialized {
        if writer.flush_remaining()? {
            materialized = true;
        }
    }

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
            "writer buffered mapper flush; no parquet files materialized yet"
        );
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

fn add_common_file_metadata(
    meta: &mut ParquetFileMetadata,
    block_type: Option<&str>,
    encoding: Option<&EncodeBytes>,
    bytes_encoding_fallback: Option<&str>,
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
    } else if let Some(bytes_encoding_fallback) = bytes_encoding_fallback {
        meta.add("firehose-parquet.bytes_encoding", bytes_encoding_fallback);
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
        None,
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

fn build_cursor_file_metadata(
    block_type: Option<&str>,
    encoding: Option<&EncodeBytes>,
    bytes_encoding_fallback: &str,
    endpoint: &str,
    compression: Compression,
    partition: &firehose_parquet::config::Partition,
    endpoint_info: &Option<EndpointInfo>,
) -> ParquetFileMetadata {
    let mut meta = ParquetFileMetadata::new();
    add_common_file_metadata(
        &mut meta,
        block_type,
        encoding,
        Some(bytes_encoding_fallback),
        endpoint,
        endpoint_info,
    );
    meta.add("firehose-parquet.compression", compression.to_string());
    meta.add("firehose-parquet.partition", partition.to_string());
    meta.add(
        "firehose-parquet.block_range_size",
        match partition {
            firehose_parquet::config::Partition::BlockRange { size, .. } => size.to_string(),
            _ => "0".to_string(),
        },
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

const SOLANA_GENESIS_TIMESTAMP: i64 = 1_584_368_940;

fn use_last_known_timestamp_partition_routing(block_type: &str, partition: &Partition) -> bool {
    block_type_has_nullable_timestamps(block_type) && partition_requires_timestamp(partition)
}

fn validate_block_timestamp(
    block_num: u64,
    timestamp: i64,
    partition: &Partition,
    start_block: Option<u64>,
) -> Result<()> {
    if timestamp != 0 {
        return Ok(());
    }

    if start_block == Some(block_num) {
        return Err(anyhow!(
            "block {block_num} is missing timestamp metadata; this can happen for genesis / first-streamable blocks. Rerun with --bootstrap-missing-genesis-timestamp to start from the next timestamped block"
        ));
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
    block_bytes: Vec<u8>,
    cursor: String,
    fork_step: Option<String>,
    identity: BlockIdentity,
}

impl GenesisTimestampBootstrap {
    fn new(enabled: bool, requested_start_block: Option<u64>) -> Self {
        Self {
            enabled,
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
        "buffered {buffered_blocks} timestamp-less bootstrap block(s) starting at block {first_buffered_block} because --bootstrap-missing-genesis-timestamp is enabled, but no later block with timestamp metadata was found before the stream ended"
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

fn update_timestamp_backfill_metrics(
    pipeline_metrics: &metrics::PipelineMetrics,
    timestamp_backfill: &TimestampBackfill,
) {
    pipeline_metrics
        .backfill_buffer_estimated_bytes
        .set(i64::try_from(timestamp_backfill.buffered_bytes()).unwrap_or(i64::MAX));
    pipeline_metrics
        .backfill_buffered_blocks
        .set(i64::try_from(timestamp_backfill.buffered_blocks_len()).unwrap_or(i64::MAX));
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
        None,
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

fn load_existing_partitions_build_rows(
    partitions_index: &str,
    aws: &AwsConfig,
    overwrite: bool,
) -> Result<Vec<PartitionBuildRow>> {
    if overwrite {
        return Ok(Vec::new());
    }

    match read_partitions_build_rows(partitions_index, Some(aws)) {
        Ok(rows) => Ok(rows),
        Err(err) if err.to_string().contains("No such file or directory") => Ok(Vec::new()),
        Err(err) if err.to_string().contains("not found") => Ok(Vec::new()),
        Err(err) => Err(err),
    }
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

fn log_overwrite_completed(partitions_index: &str, row_count: usize, frontier: u64) {
    info!(
        partitions_index = %partitions_index,
        row_count,
        frontier,
        "overwrite completed; replaced canonical partitions index"
    );
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

fn resolve_encode_bytes(
    block_type: &str,
    bytes_encoding: &str,
    endpoint_info: &Option<EndpointInfo>,
    tron_style_evm_profile: bool,
) -> EncodeBytes {
    parse_encode_bytes(bytes_encoding).unwrap_or_else(|| {
        resolve_auto_encode_bytes(Some(block_type), endpoint_info, tron_style_evm_profile)
    })
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

fn validate_ingestion_block_range(
    live: bool,
    stop_block: Option<u64>,
    cursor_state: Option<&CursorState>,
) -> Result<()> {
    if !live && stop_block.is_none() && cursor_state.and_then(|state| state.stop_block).is_none() {
        return Err(anyhow!(
            "--stop-block is required unless --live is set or an existing cursor provides one"
        ));
    }
    Ok(())
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

fn resolve_ingestion_stop_block(
    live: bool,
    stop_block: Option<u64>,
    cursor_state: Option<&CursorState>,
    cursor_override: bool,
) -> Result<Option<u64>> {
    if let Some(stop_block) = stop_block {
        return Ok(Some(stop_block));
    }

    if live {
        return Ok(None);
    }

    if cursor_override {
        return Err(anyhow!(
            "--stop-block is required unless --live is set or an existing cursor provides one"
        ));
    }

    cursor_state
        .and_then(|state| state.stop_block)
        .map(Some)
        .ok_or_else(|| {
            anyhow!(
                "--stop-block is required unless --live is set or an existing cursor provides one"
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

fn resolve_cursor_location(
    config: &firehose_parquet::config::Config,
) -> Result<Option<CursorLocation>> {
    if let Some(ref cp) = config.cursor_path {
        let output_str = config.output.to_string_lossy().to_string();
        let s3_client = if firehose_parquet::writer::is_s3_output(&config.output) {
            Some(firehose_parquet::s3::build_s3_client(config)?)
        } else {
            None
        };
        Ok(Some(CursorLocation::resolve(&output_str, cp, s3_client)?))
    } else {
        Ok(None)
    }
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
    compression: Compression,
    output: Option<&str>,
    s3_bucket: Option<&str>,
    resume: bool,
    overwrite: bool,
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
    validate_s3_output_credentials(
        &output_root,
        aws.aws_access_key_id.as_deref(),
        aws.aws_secret_access_key.as_deref(),
    )?;

    let base_config = Config {
        endpoint: endpoint.to_string(),
        api_key: read_optional_env(api_key_envvar),
        jwt_token: read_optional_env(api_token_envvar),
        start_block,
        stop_block,
        skip_missing_blocks,
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
    );
    let partitions_index = build_partitions_index_path(&output_root, &chain);
    let chain_output_root = build_partitions_output_root(&output_root, &chain);

    let existing_rows = load_existing_partitions_build_rows(&partitions_index, aws, overwrite)?;
    log_existing_partitions_index_state(&partitions_index, &existing_rows, overwrite);

    // Validate existing file metadata matches current parameters (prevent mixing)
    if !existing_rows.is_empty() {
        validate_existing_partitions_params(
            &existing_rows,
            &chain,
            partition_type,
            block_range_size,
        )?;
    }

    let existing_resume_block = existing_rows.iter().map(|row| row.stop_block).max();

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

    let should_resume_from_existing = (live || resume) && !overwrite;
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
                    .map(|row| row.stop_block)
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
        overwrite_requested = overwrite,
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

    let rows = if live && partition_type == PartitionBuildType::BlockRange {
        let poll_interval = Duration::from_secs(poll_interval_secs);
        let block_range_size = block_range_size.expect("validated above");
        let mut checkpoint_state = PartitionsCheckpointState::default();
        let mut rows = existing_rows.clone();

        info!(
            effective_start_block,
            block_range_size,
            existing_rows = rows.len(),
            "starting live block-range partitions build"
        );

        'live: loop {
            if shutdown.load(Ordering::SeqCst) {
                break;
            }

            let frontier = rows
                .iter()
                .map(|row| row.stop_block)
                .max()
                .unwrap_or(effective_start_block);
            let frontier_context =
                live_block_range_frontier_log_context(&rows, frontier, block_range_size);
            let Some(maybe_latest_available) = await_live_interruptible(
                &shutdown,
                &shutdown_notify,
                locate_live_block_range_latest_available_block(
                    &stream_client,
                    frontier,
                    poll_interval,
                    skip_missing_blocks,
                    &probe_counter,
                ),
            )
            .await?
            else {
                info!(
                    frontier,
                    next_partition_start = frontier_context.next_partition_start,
                    next_partition = %frontier_context.next_partition,
                    latest_completed_partition = frontier_context
                        .latest_completed_partition
                        .as_deref()
                        .unwrap_or("none"),
                    latest_completed_start_time = frontier_context
                        .latest_completed_start_time
                        .as_deref()
                        .unwrap_or("unavailable"),
                    latest_completed_end_time = frontier_context
                        .latest_completed_end_time
                        .as_deref()
                        .unwrap_or("unavailable"),
                    "live block-range build interrupted during frontier probe"
                );
                break;
            };

            let Some(latest_available) = maybe_latest_available else {
                info!(
                    frontier,
                    poll_interval_secs,
                    next_partition_start = frontier_context.next_partition_start,
                    next_partition = %frontier_context.next_partition,
                    latest_completed_partition = frontier_context
                        .latest_completed_partition
                        .as_deref()
                        .unwrap_or("none"),
                    latest_completed_start_time = frontier_context
                        .latest_completed_start_time
                        .as_deref()
                        .unwrap_or("unavailable"),
                    latest_completed_end_time = frontier_context
                        .latest_completed_end_time
                        .as_deref()
                        .unwrap_or("unavailable"),
                    "no new finalized blocks available yet for block-range build; polling again"
                );
                continue;
            };

            let latest_finalized_block_time =
                format_optional_probe_timestamp(latest_available.timestamp)?;
            let completed_frontier =
                completed_block_range_frontier(latest_available.block_num, block_range_size);
            if completed_frontier <= frontier {
                info!(
                    frontier,
                    next_partition_start = frontier_context.next_partition_start,
                    next_partition = %frontier_context.next_partition,
                    latest_completed_partition = frontier_context
                        .latest_completed_partition
                        .as_deref()
                        .unwrap_or("none"),
                    latest_completed_start_time = frontier_context
                        .latest_completed_start_time
                        .as_deref()
                        .unwrap_or("unavailable"),
                    latest_completed_end_time = frontier_context
                        .latest_completed_end_time
                        .as_deref()
                        .unwrap_or("unavailable"),
                    latest_finalized_block = latest_available.block_num,
                    latest_finalized_block_time =
                        latest_finalized_block_time.as_deref().unwrap_or("unavailable"),
                    next_partition_end = frontier.saturating_add(block_range_size),
                    "latest finalized block has not completed the next block-range partition yet"
                );
                continue;
            }

            info!(
                frontier,
                next_partition_start = frontier_context.next_partition_start,
                next_partition = %frontier_context.next_partition,
                latest_finalized_block = latest_available.block_num,
                latest_finalized_block_time =
                    latest_finalized_block_time.as_deref().unwrap_or("unavailable"),
                completed_frontier,
                block_range_size,
                "processing live block-range partitions"
            );

            let mut boundary = frontier;
            while boundary < completed_frontier {
                if shutdown.load(Ordering::SeqCst) {
                    break 'live;
                }

                let partition_end = (boundary + block_range_size).min(completed_frontier);
                let row = build_block_range_partition_row(
                    &stream_client,
                    &chain,
                    boundary,
                    partition_end,
                    block_range_size,
                    PARTITIONS_PROBE_TIMEOUT,
                    skip_missing_blocks,
                    &probe_counter,
                )
                .await?;

                info!(
                    partition = %format!("[{}, {})", boundary, partition_end),
                    start_time = row.start_time.as_deref().unwrap_or("null"),
                    end_time = row.end_time.as_deref().unwrap_or("null"),
                    "built live block-range partition"
                );

                rows.push(row);
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
                        overwrite,
                    )?;
                }

                boundary = partition_end;
            }
        }

        if !rows.is_empty() {
            checkpoint_partitions_rows(
                &rows,
                &partitions_index,
                compression,
                Some(aws),
                &partitions_file_metadata,
                &mut checkpoint_state,
                &probe_counter,
                overwrite,
            )?
        } else if !existing_rows.is_empty() {
            existing_rows.clone()
        } else {
            Vec::new()
        }
    } else if live {
        let poll_interval = Duration::from_secs(poll_interval_secs);
        let mut checkpoint_state = PartitionsCheckpointState::default();

        'live: loop {
            if shutdown.load(Ordering::SeqCst) {
                break;
            }

            let frontier = builder.current_frontier().unwrap_or(effective_start_block);
            let maybe_frontier_block = match await_live_probe_or_backoff(
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
                frontier,
                poll_interval,
                "polling live frontier",
            )
            .await?
            {
                LiveProbeOutcome::Ready(value) => value,
                LiveProbeOutcome::Interrupted => {
                    info!(
                        frontier,
                        "live partitions build interrupted during frontier probe"
                    );
                    break;
                }
                LiveProbeOutcome::RetryAfterBackoff => continue,
            };

            let Some(mut current_block) = maybe_frontier_block else {
                info!(
                    frontier,
                    frontier_partition_start = builder
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
                        overwrite,
                    )?;
                }
                continue;
            };

            info!(
                frontier = current_block.block_num,
                timestamp = current_block.timestamp,
                partition_start = %block_partition_start_label(partition_type, &current_block, block_range_size)?,
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
                        overwrite,
                    )?;
                }
                let span = match await_live_probe_or_backoff(
                    &shutdown,
                    &shutdown_notify,
                    locate_live_partition_span(
                        &stream_client,
                        partition_type,
                        &current_block,
                        PARTITIONS_PROBE_TIMEOUT,
                        true,
                        skip_missing_blocks,
                        &probe_counter,
                    ),
                    current_block.block_num,
                    poll_interval,
                    "probing live partition span",
                )
                .await?
                {
                    LiveProbeOutcome::Ready(span) => span,
                    LiveProbeOutcome::Interrupted => {
                        info!(
                            frontier = current_block.block_num,
                            "live partitions build interrupted during boundary search"
                        );
                        break 'live;
                    }
                    LiveProbeOutcome::RetryAfterBackoff => continue,
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
                    overwrite,
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
                overwrite,
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

            let row = build_block_range_partition_row(
                &stream_client,
                &chain,
                boundary,
                partition_end,
                block_range_size,
                PARTITIONS_PROBE_TIMEOUT,
                skip_missing_blocks,
                &probe_counter,
            )
            .await?;

            info!(
                partition = %format!("[{}, {})", boundary, partition_end),
                start_time = row.start_time.as_deref().unwrap_or("null"),
                end_time = row.end_time.as_deref().unwrap_or("null"),
                "built block-range partition"
            );

            rows.push(row);

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
                    overwrite,
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
            )?;
            if overwrite && checkpoint_state.last_checkpoint_frontier.is_none() {
                let frontier = rows
                    .iter()
                    .map(|row| row.stop_block)
                    .max()
                    .unwrap_or(aligned_start);
                log_overwrite_completed(&partitions_index, rows.len(), frontier);
            }
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
                partition_start = %block_partition_start_label(
                    partition_type,
                    &current_block,
                    block_range_size
                )?,
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
                    overwrite,
                )?;
            }
            let span = locate_live_partition_span(
                &stream_client,
                partition_type,
                &current_block,
                PARTITIONS_PROBE_TIMEOUT,
                false,
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
                            overwrite,
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
        )?;
        if overwrite && checkpoint_state.last_checkpoint_frontier.is_none() {
            log_overwrite_completed(&partitions_index, rows.len(), final_end_block);
        }
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
        .map(|row| row.stop_block)
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
    validate_block_range_alignment(start_block, Some(0), stop_block, block_range_size)
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
    overwrite_requested: bool,
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
    )?;
    if overwrite_requested && checkpoint_state.last_checkpoint_frontier.is_none() {
        log_overwrite_completed(partitions_index, rows.len(), frontier);
    }
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
    overwrite_requested: bool,
) -> Result<Vec<firehose_parquet::cli::PartitionBuildRow>> {
    let frontier = rows
        .iter()
        .map(|row| row.stop_block)
        .max()
        .ok_or_else(|| anyhow!("partition build is missing a checkpoint frontier"))?;
    write_partitions_index_strict(
        partitions_index,
        rows,
        compression,
        aws,
        Some(file_metadata),
    )?;
    if overwrite_requested && checkpoint_state.last_checkpoint_frontier.is_none() {
        log_overwrite_completed(partitions_index, rows.len(), frontier);
    }
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

fn completed_block_range_frontier(latest_available_block_num: u64, block_range_size: u64) -> u64 {
    latest_available_block_num
        .saturating_add(1)
        .checked_div(block_range_size)
        .unwrap_or(0)
        .saturating_mul(block_range_size)
}

const PARTITIONS_PROBE_FETCH_MAX_ATTEMPTS: usize = 4;
const PARTITIONS_PROBE_FETCH_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const PARTITIONS_PROBE_SKIP_MISSING_BLOCK_SCAN_LIMIT: u64 = 16;
const PARTITIONS_PROBE_TIMESTAMP_EXPONENTIAL_MAX_JUMP: u64 = 65_536;

enum LiveProbeOutcome<T> {
    Ready(T),
    Interrupted,
    RetryAfterBackoff,
}

fn is_missing_probe_block_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("not found") && message.contains("block")
}

fn is_transient_live_probe_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    [
        "service is currently unavailable",
        "message: \"unavailable\"",
        "code = unavailable",
        "code: unavailable",
        "deadline exceeded",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "connection reset",
        "connection refused",
        "broken pipe",
        "transport error",
    ]
    .iter()
    .any(|pattern| message.contains(pattern))
}

async fn await_live_probe_or_backoff<T, F>(
    shutdown: &Arc<AtomicBool>,
    shutdown_notify: &Arc<Notify>,
    future: F,
    frontier: u64,
    poll_interval: Duration,
    context: &str,
) -> Result<LiveProbeOutcome<T>>
where
    F: std::future::Future<Output = Result<T>>,
{
    match await_live_interruptible(shutdown, shutdown_notify, future).await {
        Ok(Some(value)) => Ok(LiveProbeOutcome::Ready(value)),
        Ok(None) => Ok(LiveProbeOutcome::Interrupted),
        Err(error) if is_transient_live_probe_error(&error) => {
            warn!(
                frontier,
                context,
                poll_interval_secs = poll_interval.as_secs(),
                error = %error,
                "transient live probe failed after retries; backing off before polling again"
            );

            match await_live_interruptible(shutdown, shutdown_notify, async move {
                tokio::time::sleep(poll_interval).await;
                Ok::<(), anyhow::Error>(())
            })
            .await?
            {
                Some(()) => Ok(LiveProbeOutcome::RetryAfterBackoff),
                None => Ok(LiveProbeOutcome::Interrupted),
            }
        }
        Err(error) => Err(error),
    }
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
        if jump > PARTITIONS_PROBE_TIMESTAMP_EXPONENTIAL_MAX_JUMP {
            break;
        }
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

async fn fetch_optional_raw_probe_block_identity(
    client: &FirehoseClient,
    block_num: u64,
    wait_timeout: Option<Duration>,
    context: &str,
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

    Ok(Some(block))
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

    Err(anyhow!(format_missing_timestamp_probe_error(
        context,
        block.block_num,
        timestamp_scan_limit,
        PARTITIONS_PROBE_TIMESTAMP_EXPONENTIAL_MAX_JUMP,
    )))
}

async fn probe_block_range_boundary_timestamp(
    client: &FirehoseClient,
    block_num: u64,
    probe_timeout: Duration,
    scan_forward_on_missing: bool,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Option<i64> {
    match client
        .fetch_block_identity(block_num, Some(probe_timeout))
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
        _ if skip_missing_blocks || !scan_forward_on_missing => {
            probe_counter.fetch_add(1, Ordering::Relaxed);
            None
        }
        _ => {
            probe_counter.fetch_add(1, Ordering::Relaxed);
            let mut found = None;
            for offset in 1..=16 {
                if let Ok(Some(block)) = client
                    .fetch_block_identity(block_num + offset, Some(probe_timeout))
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
        true,
        skip_missing_blocks,
        probe_counter,
    )
    .await;
    let end_time = if partition_end > boundary + 1 {
        probe_block_range_boundary_timestamp(
            client,
            partition_end.saturating_sub(1),
            probe_timeout,
            false,
            skip_missing_blocks,
            probe_counter,
        )
        .await
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

async fn find_latest_available_block_raw(
    client: &FirehoseClient,
    mut low_available: BlockIdentity,
    mut high_unavailable: u64,
    probe_timeout: Duration,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<BlockIdentity> {
    while low_available.block_num.saturating_add(1) < high_unavailable {
        let mid = low_available.block_num + (high_unavailable - low_available.block_num) / 2;
        match fetch_optional_raw_probe_block_identity(
            client,
            mid,
            Some(probe_timeout),
            "finding latest available block-range block",
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

async fn locate_live_block_range_latest_available_block(
    client: &FirehoseClient,
    frontier: u64,
    probe_timeout: Duration,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<Option<BlockIdentity>> {
    let Some(mut low_available) = fetch_optional_raw_probe_block_identity(
        client,
        frontier,
        Some(probe_timeout),
        "polling live frontier",
        skip_missing_blocks,
        probe_counter,
    )
    .await?
    else {
        return Ok(None);
    };

    let mut step = 1u64;
    loop {
        let Some(candidate) = next_live_partition_probe_candidate(low_available.block_num, step)
        else {
            return Ok(Some(low_available));
        };

        match fetch_optional_raw_probe_block_identity(
            client,
            candidate,
            Some(probe_timeout),
            "probing live block-range availability",
            skip_missing_blocks,
            probe_counter,
        )
        .await?
        {
            Some(probe) => {
                low_available = probe;
                step = step.saturating_mul(2).max(1);
            }
            None => {
                return Ok(Some(
                    find_latest_available_block_raw(
                        client,
                        low_available,
                        candidate,
                        probe_timeout,
                        skip_missing_blocks,
                        probe_counter,
                    )
                    .await?,
                ));
            }
        }
    }
}

fn format_missing_timestamp_probe_error(
    context: &str,
    block_num: u64,
    timestamp_scan_limit: u64,
    max_jump: u64,
) -> String {
    format!(
        "{context}: block {block_num} is missing timestamp metadata and no finalized block with a timestamp was found within {timestamp_scan_limit} sequential probe blocks or the bounded exponential probe window (max jump {max_jump}); for legacy ranges on chains like Solana, rerun with --partition block_range --block-range-size <N>"
    )
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

fn format_optional_probe_timestamp(timestamp: i64) -> Result<Option<String>> {
    if timestamp > 0 {
        Ok(Some(format_probe_timestamp(timestamp)?))
    } else {
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveBlockRangeFrontierLogContext {
    next_partition_start: u64,
    next_partition: String,
    latest_completed_partition: Option<String>,
    latest_completed_start_time: Option<String>,
    latest_completed_end_time: Option<String>,
}

fn live_block_range_frontier_log_context(
    rows: &[PartitionBuildRow],
    frontier: u64,
    block_range_size: u64,
) -> LiveBlockRangeFrontierLogContext {
    let latest_completed = rows
        .iter()
        .filter(|row| row.stop_block == frontier)
        .max_by_key(|row| row.start_block)
        .or_else(|| rows.iter().max_by_key(|row| row.stop_block));

    LiveBlockRangeFrontierLogContext {
        next_partition_start: frontier,
        next_partition: format!("{}-{}", frontier, frontier.saturating_add(block_range_size)),
        latest_completed_partition: latest_completed.map(|row| row.partition_value.clone()),
        latest_completed_start_time: latest_completed.and_then(|row| row.start_time.clone()),
        latest_completed_end_time: latest_completed.and_then(|row| row.end_time.clone()),
    }
}

fn block_partition_start_label(
    partition_type: PartitionBuildType,
    block: &BlockIdentity,
    block_range_size: Option<u64>,
) -> Result<String> {
    match partition_type {
        PartitionBuildType::BlockRange => {
            let block_range_size = block_range_size.ok_or_else(|| {
                anyhow!("block_range logging requires --block-range-size to be resolved")
            })?;
            let partition_start = (block.block_num / block_range_size) * block_range_size;
            Ok(partition_start.to_string())
        }
        _ => format_probe_timestamp(block_partition_start(partition_type, block)?),
    }
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
    low_same: BlockIdentity,
    high_different: BlockIdentity,
    probe_timeout: Duration,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<(BlockIdentity, BlockIdentity)> {
    let span = find_first_different_block_with_fetch(
        partition_type,
        partition_start_ts,
        low_same,
        high_different,
        |mid| async move {
            fetch_required_block_identity(
                client,
                mid,
                Some(probe_timeout),
                "probing partition boundary",
                16,
                skip_missing_blocks,
                probe_counter,
            )
            .await
            .map(Some)
        },
    )
    .await?;

    Ok((
        span.last_same,
        span.next_boundary
            .expect("bounded partition boundary search should resolve a next boundary"),
    ))
}

async fn find_first_different_block_with_fetch<F, Fut>(
    partition_type: PartitionBuildType,
    partition_start_ts: i64,
    mut low_same: BlockIdentity,
    mut high_different: BlockIdentity,
    mut fetch_probe: F,
) -> Result<PartitionProbeSpan>
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<Option<BlockIdentity>>>,
{
    while low_same.block_num.saturating_add(1) < high_different.block_num {
        let mid = low_same.block_num + (high_different.block_num - low_same.block_num) / 2;
        let Some(probe) = fetch_probe(mid).await? else {
            return Ok(PartitionProbeSpan {
                last_same: low_same,
                next_boundary: None,
            });
        };

        if probe.block_num >= high_different.block_num {
            break;
        } else if block_partition_start(partition_type, &probe)? == partition_start_ts {
            low_same = probe;
        } else {
            high_different = probe;
        }
    }

    Ok(PartitionProbeSpan {
        last_same: low_same,
        next_boundary: Some(high_different),
    })
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

/// Avoid probing the `u64::MAX` sentinel, which can pin live sparse probing to
/// a non-progressing "latest block" fetch instead of a concrete block number.
fn next_live_partition_probe_candidate(block_num: u64, step: u64) -> Option<u64> {
    block_num
        .checked_add(step)
        .filter(|candidate| *candidate != u64::MAX)
}

async fn locate_live_partition_span(
    client: &FirehoseClient,
    partition_type: PartitionBuildType,
    start_block: &BlockIdentity,
    probe_timeout: Duration,
    allow_incomplete_boundary: bool,
    skip_missing_blocks: bool,
    probe_counter: &AtomicU64,
) -> Result<PartitionProbeSpan> {
    let partition_start_ts = block_partition_start(partition_type, start_block)?;
    let mut low_same = start_block.clone();
    let mut step = 1u64;

    loop {
        let Some(candidate) = next_live_partition_probe_candidate(low_same.block_num, step) else {
            return Ok(PartitionProbeSpan {
                last_same: low_same,
                next_boundary: None,
            });
        };
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

                if allow_incomplete_boundary {
                    return find_first_different_block_with_fetch(
                        partition_type,
                        partition_start_ts,
                        low_same,
                        probe,
                        |mid| async move {
                            fetch_optional_probe_block_identity(
                                client,
                                mid,
                                Some(probe_timeout),
                                "probing partition boundary",
                                16,
                                skip_missing_blocks,
                                probe_counter,
                            )
                            .await
                        },
                    )
                    .await;
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

                if allow_incomplete_boundary {
                    return find_first_different_block_with_fetch(
                        partition_type,
                        partition_start_ts,
                        low_same,
                        latest_available,
                        |mid| async move {
                            fetch_optional_probe_block_identity(
                                client,
                                mid,
                                Some(probe_timeout),
                                "probing partition boundary",
                                16,
                                skip_missing_blocks,
                                probe_counter,
                            )
                            .await
                        },
                    )
                    .await;
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

fn validate_chain_feature_flags(
    requested_block_type: &str,
    endpoint_info: &Option<EndpointInfo>,
    cursor_state: Option<&CursorState>,
    extended_requested: bool,
    extended_explicitly_requested: bool,
    with_votes_requested: bool,
) -> Result<()> {
    if chain_is_solana(requested_block_type, endpoint_info, cursor_state)
        && extended_explicitly_requested
        && extended_requested
    {
        return Err(anyhow!(SOLANA_EXTENDED_ERROR));
    }

    if chain_is_antelope(requested_block_type, endpoint_info, cursor_state)
        && extended_explicitly_requested
        && extended_requested
    {
        return Err(anyhow!(ANTELOPE_EXTENDED_ERROR));
    }

    if chain_is_known_non_solana(requested_block_type, endpoint_info, cursor_state)
        && with_votes_requested
    {
        return Err(anyhow!(WITH_VOTES_NON_SOLANA_ERROR));
    }

    Ok(())
}

fn extended_warning_message() -> &'static str {
    "--extended requested but endpoint did not advertise extended block features"
}

fn log_solana_vote_mode(with_votes: bool) {
    if with_votes {
        info!("Solana vote_transactions enabled");
    } else {
        info!("Solana vote_transactions disabled via --with-votes false");
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

/// Keep `--extended` as the generic extended-output control while logging
/// endpoint capability when available.
fn resolve_extended_mode(extended_requested: bool, endpoint_info: &Option<EndpointInfo>) -> bool {
    let endpoint_supports_extended = supports_extended(endpoint_info);

    if endpoint_supports_extended {
        if extended_requested {
            info!("endpoint advertises extended block features; extended output enabled");
        } else {
            info!(
                "endpoint advertises extended block features; extended output disabled via --extended false"
            );
        }
    } else if extended_requested {
        warn!("{}", extended_warning_message());
    }

    extended_requested
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
            Commands::Completions { shell } => {
                firehose_parquet::cli::generate_completions::<Cli>(*shell);
                return Ok(());
            }
            Commands::Build(build_args) => {
                run_ingestion(build_args).await?;
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
                    init_tracing(&cli.global.log_level);
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
                                "stop_block",
                                "chain"
                            );
                            for row in result.rows {
                                println!(
                                    "{:<15} {:<19} {:<19} {:>12} {:>12} {}",
                                    row.partition_type,
                                    row.partition_value,
                                    row.partition_start_ts,
                                    row.start_block,
                                    row.stop_block,
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
                                "stop_block",
                                "chain"
                            );
                            for row in result.rows {
                                println!(
                                    "{:<15} {:<19} {:<19} {:>12} {:>12} {}",
                                    row.partition_type,
                                    row.partition_value,
                                    row.partition_start_ts,
                                    row.start_block,
                                    row.stop_block,
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
                init_tracing(&cli.global.log_level);
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
                init_tracing(&cli.global.log_level);
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
                init_tracing(&cli.global.log_level);
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

    unreachable!("clap enforces a subcommand")
}

async fn run_ingestion(args: &BuildArgs) -> Result<()> {
    init_tracing(&args.common.log_level);

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

    let block_type = args.block_type.to_lowercase();
    if block_type != "auto" && !BLOCK_TYPES.contains(&block_type.as_str()) {
        return Err(anyhow!(
            "unsupported block type: {block_type}. Supported: {}",
            BLOCK_TYPES.join(", ")
        ));
    }

    let mut extended = args.extended.unwrap_or(true);
    let extended_explicitly_requested = args.extended.is_some();
    let with_votes = args.with_votes;
    let bytes_encoding_str = args.bytes_encoding.clone();
    let mut common = args.common.clone();
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

    // Fetch endpoint info for auto-detection of encoding, chain_name-based
    // output directory, and feature capability logging.
    let mut client = FirehoseClient::new(config.clone());
    let endpoint_info = client.info().await;

    // Use chain_name as a subdirectory under the output path.
    config.output = resolve_output(&config.output, &endpoint_info);

    let cursor_location = resolve_cursor_location(&config)?;
    let existing_cursor_state = cursor_location.as_ref().and_then(CursorLocation::load);
    let resume_cursor_state = existing_cursor_state
        .as_ref()
        .filter(|_| !args.cursor_override);

    validate_chain_feature_flags(
        &block_type,
        &endpoint_info,
        existing_cursor_state.as_ref(),
        extended,
        extended_explicitly_requested,
        with_votes,
    )?;
    let solana_chain = chain_is_solana(&block_type, &endpoint_info, existing_cursor_state.as_ref());
    let antelope_chain =
        chain_is_antelope(&block_type, &endpoint_info, existing_cursor_state.as_ref());
    let known_non_solana_chain =
        chain_is_known_non_solana(&block_type, &endpoint_info, existing_cursor_state.as_ref());

    validate_ingestion_block_range(args.common.live, config.stop_block, resume_cursor_state)?;
    config.start_block = resolve_ingestion_start_block(
        config.start_block,
        existing_cursor_state.as_ref(),
        &endpoint_info,
        args.cursor_override,
    )?;
    config.stop_block = resolve_ingestion_stop_block(
        args.common.live,
        config.stop_block,
        resume_cursor_state,
        args.cursor_override,
    )?;
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
        extended = resolve_extended_mode(extended, &endpoint_info);
    }

    let include_failed_transactions = args.include_failed_transactions;

    info!(
        block_type,
        extended,
        with_votes,
        bytes_encoding = %bytes_encoding_str,
        include_failed_transactions,
        "starting pipeline\n{config}"
    );

    if let Some(cursor_state) = existing_cursor_state.as_ref() {
        if args.cursor_override {
            info!(
                stored_cursor_last_block_num = cursor_state.last_block_num,
                requested_start_block = ?config.start_block,
                requested_stop_block = ?config.stop_block,
                live = args.common.live,
                "cursor override enabled, restarting from CLI-provided/default bounds"
            );
        } else {
            info!(
                stored_cursor_last_block_num = cursor_state.last_block_num,
                requested_start_block = ?config.start_block,
                requested_stop_block = ?config.stop_block,
                live = args.common.live,
                "resuming from stored cursor"
            );
        }
    } else {
        info!(
            requested_start_block = ?config.start_block,
            requested_stop_block = ?config.stop_block,
            live = args.common.live,
            "starting without stored cursor"
        );
    }

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
            ("with_votes".to_string(), with_votes.to_string()),
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
    let tron_style_evm_profile = endpoint_uses_tron_style_evm_profile(&endpoint_info);
    let mut is_solana = block_type == "solana";
    let partition_config = config.partition.clone();
    let mut use_synthetic_partition_routing =
        use_last_known_timestamp_partition_routing(&block_type, &partition_config);
    let mut genesis_timestamp_bootstrap = GenesisTimestampBootstrap::new(
        args.bootstrap_missing_genesis_timestamp,
        config.start_block,
    );
    let mut timestamp_backfill = TimestampBackfill::new(
        use_synthetic_partition_routing,
        DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES,
    );
    update_timestamp_backfill_metrics(&pipeline_metrics, &timestamp_backfill);

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
        let encode_bytes = resolve_encode_bytes(
            &block_type,
            &bytes_encoding_str,
            &endpoint_info,
            tron_style_evm_profile,
        );
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
        writer.inner.set_file_metadata(meta);
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

    let mut blocks_observed: u64 = 0;
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
    let mut buffered_bootstrap_blocks: Vec<BufferedBootstrapBlock> = Vec::new();
    let progress_start = Instant::now();
    let mut current_partition_key: Option<String> = None;

    // Build file-level metadata for the cursor (same `firehose-parquet.*`
    // namespace as table files). Includes version, endpoint, chain info, and
    // pipeline parameters.
    let initial_cursor_encoding = if block_type != "auto" {
        Some(resolve_encode_bytes(
            &block_type,
            &bytes_encoding_str,
            &endpoint_info,
            tron_style_evm_profile,
        ))
    } else {
        None
    };
    let cursor_file_metadata = build_cursor_file_metadata(
        if block_type != "auto" {
            Some(block_type.as_str())
        } else {
            None
        },
        initial_cursor_encoding.as_ref(),
        &bytes_encoding_str,
        &config.endpoint,
        config.compression,
        &config.partition,
        &endpoint_info,
    );
    let mut cursor_file_metadata = cursor_file_metadata;
    if solana_chain {
        maybe_add_solana_with_votes_metadata(&mut cursor_file_metadata, Some("solana"), with_votes);
    }
    if block_type != "auto" {
        maybe_add_synthetic_timestamp_metadata(
            &mut cursor_file_metadata,
            &block_type,
            use_synthetic_partition_routing,
        );
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
    if let Some(loaded) = existing_cursor_state.as_ref() {
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

    let stream_result = client
        .stream_blocks(stream_resume_cursor(existing_cursor_state.as_ref(), args.cursor_override), |block_bytes, type_url, cursor_str, identity: BlockIdentity, step: i32| {
            let fork_step_str = fork_step_name(step);
            if final_blocks_only && step == 2 {
                return Ok(());
            }

            // Lazy mapper creation for "auto" mode.
            if mapper.is_none() {
                let detected = detect_block_type(&type_url)?;
                info!(detected_type = %detected, type_url = %type_url, "auto-detected block type");
                validate_chain_feature_flags(
                    &detected,
                    &endpoint_info,
                    existing_cursor_state.as_ref(),
                    extended,
                    extended_explicitly_requested,
                    with_votes,
                )?;
                if detected == "solana" {
                    extended = false;
                    log_solana_vote_mode(with_votes);
                } else if detected == "antelope" {
                    extended = false;
                } else {
                    extended = resolve_extended_mode(extended, &endpoint_info);
                }
                let encode_bytes = resolve_encode_bytes(
                    &detected,
                    &bytes_encoding_str,
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
                writer.inner.set_file_metadata(meta);
                let mut cursor_meta = build_cursor_file_metadata(
                    Some(&detected),
                    Some(&encode_bytes),
                    &bytes_encoding_str,
                    &config.endpoint,
                    config.compression,
                    &config.partition,
                    &endpoint_info,
                );
                maybe_add_solana_with_votes_metadata(&mut cursor_meta, Some(&detected), with_votes);
                maybe_add_synthetic_timestamp_metadata(
                    &mut cursor_meta,
                    &detected,
                    detected_uses_synthetic_partition_routing,
                );
                cursor_state_template.file_metadata = cursor_meta;
                cursor_state_template.extended = extended;
                is_solana = detected == "solana";
                use_synthetic_partition_routing = detected_uses_synthetic_partition_routing;
                timestamp_backfill = TimestampBackfill::new(
                    use_synthetic_partition_routing,
                    DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES,
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
            let ready_solana_blocks = if is_solana && use_synthetic_partition_routing {
                timestamp_backfill.observe_block(
                    block_bytes,
                    cursor_str,
                    fork_step_owned,
                    identity,
                )?
            } else {
                vec![BufferedBootstrapBlock {
                    block_bytes,
                    cursor: cursor_str,
                    fork_step: fork_step_owned,
                    identity,
                }]
            };
            update_timestamp_backfill_metrics(&pipeline_metrics, &timestamp_backfill);
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
                    let block_timestamp = format_optional_probe_timestamp(current_anchor_timestamp)?;
                    match block_timestamp.as_deref() {
                        Some(block_timestamp) => info!(
                            blocks_observed,
                            blocks_processed,
                            block_number,
                            block_timestamp,
                            buffered_blocks = timestamp_backfill.buffered_blocks_len(),
                            buffered_bytes = firehose_parquet::cli::format_bytes(timestamp_backfill.buffered_bytes()),
                            speed = format!("{:.0} observed blocks/s", observed_blocks_per_sec),
                            "progress (buffering timestamps)"
                        ),
                        None => info!(
                            blocks_observed,
                            blocks_processed,
                            block_number,
                            buffered_blocks = timestamp_backfill.buffered_blocks_len(),
                            buffered_bytes = firehose_parquet::cli::format_bytes(timestamp_backfill.buffered_bytes()),
                            speed = format!("{:.0} observed blocks/s", observed_blocks_per_sec),
                            "progress (buffering timestamps)"
                        ),
                    }
                }
                return Ok(());
            }
            // For Solana, blocks may legitimately lack timestamps — skip the
            // genesis bootstrap and timestamp validation entirely.
            if !is_solana {
            match genesis_timestamp_bootstrap.observe_block(blocks_processed, block_number, ts) {
                GenesisTimestampBootstrapAction::Buffer => {
                    if genesis_timestamp_bootstrap.buffered_blocks == 1 {
                        warn!(
                            requested_start_block = ?config.start_block,
                            block_number,
                            "buffering first streamable block because it lacks timestamp metadata; its timestamp will be synthesized from the first later timestamped block because --bootstrap-missing-genesis-timestamp is enabled"
                        );
                    }
                    buffered_bootstrap_blocks.push(BufferedBootstrapBlock {
                        block_bytes: ready_solana_blocks[0].block_bytes.clone(),
                        cursor: ready_solana_blocks[0].cursor.clone(),
                        fork_step: ready_solana_blocks[0].fork_step.clone(),
                        identity: ready_solana_blocks[0].identity.clone(),
                    });
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
                                     cursor: &str|
             -> Result<()> {
                let block_number = identity.block_num;
                let ts = identity.timestamp;
                // Solana blocks may have no timestamp; skip validation for Solana.
                if !is_solana {
                    validate_block_timestamp(
                        block_number,
                        ts,
                        &partition_config,
                        config.start_block,
                    )?;
                }
                let has_timestamp = ts != 0;

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
                                min_block_number: min_block.unwrap_or(0),
                                max_block_number: max_block.unwrap_or(0),
                                min_timestamp,
                                max_timestamp,
                            };
                            let outcome = write_mapper_flush(&mut writer, &batches, &metadata, true)?;
                            log_writer_flush_outcome(
                                "partition_boundary",
                                flushed_tables,
                                flushed_rows,
                                outcome,
                            );
                            if outcome.materialized {
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
                        last_flush_time = Instant::now();
                    }
                }
                current_partition_key = new_partition_key;

                min_block = Some(min_block.map_or(block_number, |s: u64| s.min(block_number)));
                max_block = Some(max_block.map_or(block_number, |s: u64| s.max(block_number)));
                global_min_block = Some(global_min_block.map_or(block_number, |s: u64| s.min(block_number)));
                global_max_block = Some(global_max_block.map_or(block_number, |s: u64| s.max(block_number)));
                if has_timestamp {
                    min_timestamp = Some(min_timestamp.map_or(ts, |s: i64| s.min(ts)));
                    max_timestamp = Some(max_timestamp.map_or(ts, |s: i64| s.max(ts)));
                }

                m.map_block(block_bytes, identity, fork_step)?;
                blocks_processed += 1;
                bytes_read += block_bytes.len() as u64;
                last_cursor = Some(cursor.to_owned());
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

                if should_emit_progress_log(blocks_processed) {
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
                    let block_timestamp = format_optional_probe_timestamp(ts)?;
                    match (block_timestamp.as_deref(), current_partition_key.as_deref()) {
                        (Some(block_timestamp), Some(partition)) => info!(
                            blocks_processed,
                            block_number,
                            block_timestamp,
                            partition,
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            speed = format!("{}/s | {:.0} blocks/s", firehose_parquet::cli::format_bytes(speed_per_sec as u64), blocks_per_sec),
                            "progress"
                        ),
                        (Some(block_timestamp), None) => info!(
                            blocks_processed,
                            block_number,
                            block_timestamp,
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            speed = format!("{}/s | {:.0} blocks/s", firehose_parquet::cli::format_bytes(speed_per_sec as u64), blocks_per_sec),
                            "progress"
                        ),
                        (None, Some(partition)) => info!(
                            blocks_processed,
                            block_number,
                            partition,
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            speed = format!("{}/s | {:.0} blocks/s", firehose_parquet::cli::format_bytes(speed_per_sec as u64), blocks_per_sec),
                            "progress"
                        ),
                        (None, None) => info!(
                            blocks_processed,
                            block_number,
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            speed = format!("{}/s | {:.0} blocks/s", firehose_parquet::cli::format_bytes(speed_per_sec as u64), blocks_per_sec),
                            "progress"
                        ),
                    }

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
                    let flush_trigger = if bytes_to_flush {
                        "bytes"
                    } else if rows_to_flush {
                        "rows"
                    } else {
                        "interval"
                    };
                    let batches = m.flush()?;
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
                            min_block_number: min_block.unwrap_or(0),
                            max_block_number: max_block.unwrap_or(0),
                            min_timestamp,
                            max_timestamp,
                        };
                        let outcome = write_mapper_flush(&mut writer, &batches, &metadata, true)?;
                        log_writer_flush_outcome(flush_trigger, flushed_tables, flushed_rows, outcome);

                        // Only update cursor after all tables have been written.
                        if outcome.materialized {
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
                    last_flush_time = Instant::now();
                }

                Ok(())
            };

            for buffered_block in take_anchored_bootstrap_blocks(
                &mut buffered_bootstrap_blocks,
                current_anchor_timestamp,
            ) {
                process_block(
                    &buffered_block.block_bytes,
                    &buffered_block.identity,
                    buffered_block.fork_step.as_deref(),
                    &buffered_block.cursor,
                )?;
            }

            for ready_block in ready_solana_blocks {
                process_block(
                    &ready_block.block_bytes,
                    &ready_block.identity,
                    ready_block.fork_step.as_deref(),
                    &ready_block.cursor,
                )?;
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
        let mapper_buffered_rows = mapper.as_ref().map(|m| m.total_rows()).unwrap_or(0);
        let writer_buffered = writer.buffered_stats();
        info!(
            mapper_buffered_rows,
            writer_buffered_tables = writer_buffered.tables,
            writer_buffered_rows = writer_buffered.rows,
            writer_buffered_estimated_bytes = firehose_parquet::cli::format_bytes(
                writer_buffered.estimated_compressed_bytes
            ),
            timestamp_backfill_buffered_blocks = timestamp_backfill.buffered_blocks_len(),
            timestamp_backfill_buffered_bytes = firehose_parquet::cli::format_bytes(
                timestamp_backfill.buffered_bytes()
            ),
            "graceful shutdown skipped partial flushes; buffered data was not materialized to storage"
        );
    } else {
        let trailing_timestamp_backfill_blocks = timestamp_backfill.drain_open_span()?;
        update_timestamp_backfill_metrics(&pipeline_metrics, &timestamp_backfill);
        if let Some(m) = mapper.as_mut() {
            for buffered_block in trailing_timestamp_backfill_blocks {
                let block_number = buffered_block.identity.block_num;
                let ts = buffered_block.identity.timestamp;
                min_block = Some(min_block.map_or(block_number, |s: u64| s.min(block_number)));
                max_block = Some(max_block.map_or(block_number, |s: u64| s.max(block_number)));
                global_min_block =
                    Some(global_min_block.map_or(block_number, |s: u64| s.min(block_number)));
                global_max_block =
                    Some(global_max_block.map_or(block_number, |s: u64| s.max(block_number)));
                min_timestamp = Some(min_timestamp.map_or(ts, |s: i64| s.min(ts)));
                max_timestamp = Some(max_timestamp.map_or(ts, |s: i64| s.max(ts)));

                m.map_block(
                    &buffered_block.block_bytes,
                    &buffered_block.identity,
                    buffered_block.fork_step.as_deref(),
                )?;
                blocks_processed += 1;
                bytes_read += buffered_block.block_bytes.len() as u64;
                last_cursor = Some(buffered_block.cursor);
                last_block_num = block_number;
                last_block_id = buffered_block.identity.block_id.clone();
                pipeline_metrics.blocks_processed_total.inc();
                pipeline_metrics
                    .bytes_read_total
                    .inc_by(buffered_block.block_bytes.len() as u64);
                pipeline_metrics
                    .current_block_number
                    .set(block_number as i64);
            }
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
            let writer_buffered = writer.buffered_stats();
            if wrote {
                info!(
                    trigger = "shutdown",
                    buffered_tables = writer_buffered.tables,
                    buffered_rows = writer_buffered.rows,
                    buffered_estimated_bytes = firehose_parquet::cli::format_bytes(
                        writer_buffered.estimated_compressed_bytes
                    ),
                    "writer materialized remaining parquet output before exit"
                );
            } else {
                info!(
                    trigger = "shutdown",
                    "no writer-buffered parquet data remained to materialize before exit"
                );
            }

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

    if !is_shutdown && genesis_timestamp_bootstrap.enabled {
        if let Some(first_buffered_block) = genesis_timestamp_bootstrap.first_buffered_block {
            return Err(missing_genesis_timestamp_bootstrap_error(
                first_buffered_block,
                genesis_timestamp_bootstrap.buffered_blocks,
            ));
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

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_number",
            DataType::UInt64,
            false,
        )]));
        let mut builder = UInt64Builder::new();
        builder.append_value(42);
        RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap()
    }

    fn make_test_batches() -> HashMap<String, RecordBatch> {
        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), make_test_batch());
        batches
    }

    fn make_temp_output_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "fireparq-partition-boundary-test-{}-{}",
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn test_write_mapper_flush_can_leave_batches_buffered() {
        let dir = make_temp_output_dir();
        let batches = make_test_batches();
        let metadata = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705320000),
            max_timestamp: Some(1705320000),
        };
        let mut writer = OutputWriter::new(&dir, Partition::Date, Compression::None, 1_000_000);

        let outcome = write_mapper_flush(&mut writer, &batches, &metadata, false).unwrap();

        assert!(!outcome.materialized);
        assert_eq!(outcome.buffered.tables, 1);
        assert_eq!(outcome.buffered.batches, 1);
        assert_eq!(outcome.buffered.rows, 1);
        assert!(
            outcome.buffered.estimated_arrow_bytes > 0,
            "buffered stats should report in-memory data"
        );
        assert!(
            !dir.join("blocks/year=2024/month=01/date=15").exists(),
            "without forced materialization the partition should stay buffered"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_write_mapper_flush_forces_partition_boundary_materialization() {
        let dir = make_temp_output_dir();
        let batches = make_test_batches();
        let metadata = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1705320000),
            max_timestamp: Some(1705320000),
        };
        let mut writer = OutputWriter::new(&dir, Partition::Date, Compression::None, 1_000_000);

        let outcome = write_mapper_flush(&mut writer, &batches, &metadata, true).unwrap();

        assert!(outcome.materialized);
        assert_eq!(outcome.buffered, WriterBufferStats::default());
        assert!(
            dir.join("blocks/year=2024/month=01/date=15").exists(),
            "forced partition-boundary materialization should write the old partition immediately"
        );
        std::fs::remove_dir_all(&dir).unwrap();
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
            "--bytes-encoding",
            "hex",
        ]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert_eq!(build_args.extended, None);
            assert_eq!(build_args.block_type, "evm");
            assert_eq!(build_args.bytes_encoding, "hex");
        } else {
            panic!("expected Commands::Build");
        }
    }

    #[test]
    fn test_build_subcommand_accepts_explicit_false_feature_flags() {
        let cli = Cli::parse_from([
            "fireparq",
            "build",
            "--network",
            "solana-mainnet-beta",
            "--start-block",
            "100",
            "--stop-block",
            "200",
            "--extended",
            "false",
            "--with-votes",
            "false",
        ]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert_eq!(build_args.extended, Some(false));
            assert!(!build_args.with_votes);
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
            assert!(build_args.with_votes);
            assert_eq!(build_args.extended, None);
            assert_eq!(build_args.network.as_deref(), Some("solana-mainnet-beta"));
        } else {
            panic!("expected Commands::Build");
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
    fn test_build_subcommand_parses_live_flag() {
        let cli = Cli::parse_from(["fireparq", "build", "--network", "mainnet", "--live"]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert!(build_args.common.live);
        } else {
            panic!("expected Commands::Build");
        }
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
    fn test_build_subcommand_bootstrap_missing_genesis_timestamp_default_false() {
        let cli = Cli::parse_from([
            "fireparq",
            "build",
            "--network",
            "mainnet",
            "--start-block",
            "0",
            "--stop-block",
            "200",
        ]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert!(!build_args.bootstrap_missing_genesis_timestamp);
        } else {
            panic!("expected Commands::Build");
        }
    }

    #[test]
    fn test_build_subcommand_parses_bootstrap_missing_genesis_timestamp_flag() {
        let cli = Cli::parse_from([
            "fireparq",
            "build",
            "--network",
            "mainnet",
            "--start-block",
            "0",
            "--stop-block",
            "200",
            "--bootstrap-missing-genesis-timestamp",
        ]);
        if let Some(Commands::Build(build_args)) = cli.command {
            assert!(build_args.bootstrap_missing_genesis_timestamp);
        } else {
            panic!("expected Commands::Build");
        }
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
    fn test_build_subcommand_help_contains_examples() {
        let cmd = Cli::command();
        // Find the build subcommand
        let build_subcmd = cmd
            .get_subcommands()
            .find(|sc| sc.get_name() == "build")
            .expect("build subcommand should exist");
        let help = build_subcmd.clone().render_long_help().to_string();
        assert!(help.contains("fireparq build --network"));
        assert!(help.contains("--with-votes"));
        assert!(help.contains("--start-block"));
        assert!(help.contains("--live"));
        assert!(!help.contains("--partitions-index"));
        assert!(!help.contains("--partition-from"));
        assert!(!help.contains("--partition-to"));
        assert!(!help.contains("--strict-timestamps"));
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
        assert!(help.contains("--live"));
        assert!(help.contains("existing cursor"));
        assert!(help.contains("first streamable block"));
        assert!(help.contains("--skip-missing-blocks"));
        assert!(help.contains("--bootstrap-missing-genesis-timestamp"));
        assert!(help.contains("synthesize their timestamp"));
        assert!(!help.contains("--backfill-missing-timestamps"));
        assert!(!help.contains("--backfill-missing-timestamps-buffer-bytes"));
        assert!(!help.contains("--strict-timestamps"));
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
    fn test_build_help_mentions_tron_bytes_encoding_behavior() {
        let help = subcommand_help("build");
        assert!(help.contains("For Tron, `auto` resolves to `tron_base58`"));
        assert!(help.contains("hashes and topics stay raw hex without `0x`"));
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
    fn test_validate_ingestion_block_range_accepts_bounded_mode() {
        validate_ingestion_block_range(false, Some(200), None)
            .expect("bounded mode should accept --stop-block");
    }

    #[test]
    fn test_validate_ingestion_block_range_accepts_cursor_stop_without_live() {
        let cursor_state = CursorState {
            stop_block: Some(200),
            ..CursorState::default()
        };

        validate_ingestion_block_range(false, None, Some(&cursor_state))
            .expect("bounded mode should accept a stop block from the cursor");
    }

    #[test]
    fn test_validate_ingestion_block_range_rejects_missing_stop_without_live_or_cursor() {
        let err = validate_ingestion_block_range(false, None, None)
            .expect_err("missing --stop-block should require --live or cursor state");
        assert_eq!(
            err.to_string(),
            "--stop-block is required unless --live is set or an existing cursor provides one"
        );
    }

    #[test]
    fn test_validate_ingestion_block_range_accepts_stop_block_in_live_mode() {
        validate_ingestion_block_range(true, Some(200), None)
            .expect("live mode should allow an explicit --stop-block");
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
        let cursor_state = CursorState {
            stop_block: Some(200),
            ..CursorState::default()
        };

        let stop_block = resolve_ingestion_stop_block(false, Some(300), Some(&cursor_state), false)
            .expect("ingestion should accept an explicit stop block");

        assert_eq!(stop_block, Some(300));
    }

    #[test]
    fn test_resolve_ingestion_stop_block_uses_cursor_stop_for_bounded_resume() {
        let cursor_state = CursorState {
            stop_block: Some(200),
            ..CursorState::default()
        };

        let stop_block = resolve_ingestion_stop_block(false, None, Some(&cursor_state), false)
            .expect("bounded resume should use the cursor stop block");

        assert_eq!(stop_block, Some(200));
    }

    #[test]
    fn test_resolve_ingestion_stop_block_keeps_live_stream_open_when_omitted() {
        let cursor_state = CursorState {
            stop_block: Some(200),
            ..CursorState::default()
        };

        let stop_block = resolve_ingestion_stop_block(true, None, Some(&cursor_state), false)
            .expect("live mode should keep streaming when stop block is omitted");

        assert_eq!(stop_block, None);
    }

    #[test]
    fn test_resolve_ingestion_stop_block_rejects_cursor_stop_when_override_is_enabled() {
        let cursor_state = CursorState {
            stop_block: Some(200),
            ..CursorState::default()
        };

        let err = resolve_ingestion_stop_block(false, None, Some(&cursor_state), true)
            .expect_err("cursor override should not inherit the cursor stop block");

        assert_eq!(
            err.to_string(),
            "--stop-block is required unless --live is set or an existing cursor provides one"
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
    fn test_completed_block_range_frontier_requires_full_partition() {
        assert_eq!(completed_block_range_frontier(0, 100_000), 0);
        assert_eq!(completed_block_range_frontier(99_998, 100_000), 0);
        assert_eq!(completed_block_range_frontier(99_999, 100_000), 100_000);
        assert_eq!(completed_block_range_frontier(250_123, 100_000), 200_000);
    }

    #[test]
    fn test_completed_block_range_frontier_handles_u64_max() {
        assert_eq!(
            completed_block_range_frontier(u64::MAX, 100_000),
            (u64::MAX / 100_000).saturating_mul(100_000)
        );
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
    fn test_resolve_encode_bytes_uses_tron_style_profile_for_tron_chain_name() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "tron".to_string(),
            chain_name_aliases: vec![],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 0,
            block_features: vec![],
        });

        let resolved = resolve_encode_bytes("evm", "auto", &endpoint_info, true);

        assert_eq!(resolved, EncodeBytes::TronBase58);
    }

    #[test]
    fn test_resolve_encode_bytes_near_prefers_output_contract_over_endpoint_hint() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "near-mainnet".to_string(),
            chain_name_aliases: vec!["near".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 2,
            block_features: vec![],
        });

        let resolved = resolve_encode_bytes("near", "auto", &endpoint_info, false);

        assert_eq!(resolved, EncodeBytes::Base58);
    }

    #[test]
    fn test_resolve_encode_bytes_explicit_cli_override_wins_for_near() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "near-mainnet".to_string(),
            chain_name_aliases: vec!["near".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 3,
            block_features: vec![],
        });

        let resolved = resolve_encode_bytes("near", "hex", &endpoint_info, false);

        assert_eq!(resolved, EncodeBytes::Hex);
    }

    #[test]
    fn test_resolve_encode_bytes_supported_contracts_override_endpoint_hints() {
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

            let resolved =
                resolve_encode_bytes(block_type, "auto", &endpoint_info, tron_style_evm_profile);

            assert_eq!(
                resolved, expected,
                "expected explicit output contract for block_type={block_type} tron_style_evm_profile={tron_style_evm_profile}"
            );
        }
    }

    #[test]
    fn test_resolve_encode_bytes_tron_style_contract_overrides_endpoint_hint() {
        let endpoint_info = Some(EndpointInfo {
            chain_name: "tron-evm".to_string(),
            chain_name_aliases: vec!["tron".to_string()],
            first_streamable_block_num: 0,
            first_streamable_block_id: String::new(),
            block_id_encoding: 2,
            block_features: vec![],
        });

        let resolved = resolve_encode_bytes("evm", "auto", &endpoint_info, true);

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
    fn test_validate_block_timestamp_missing_errors() {
        let err = validate_block_timestamp(42, 0, &Partition::None, Some(0))
            .expect_err("missing timestamp should error");
        assert!(err.to_string().contains("missing timestamp metadata"));
    }

    #[test]
    fn test_validate_block_timestamp_missing_in_first_streamable_block_suggests_bootstrap() {
        let err = validate_block_timestamp(0, 0, &Partition::None, Some(0)).expect_err(
            "missing timestamp should suggest bootstrap for the first streamable block",
        );
        let message = err.to_string();

        assert!(message.contains("genesis / first-streamable blocks"));
        assert!(message.contains("--bootstrap-missing-genesis-timestamp"));
    }

    #[test]
    fn test_validate_block_timestamp_missing_time_partition_errors() {
        let err = validate_block_timestamp(42, 0, &Partition::Date, Some(0))
            .expect_err("time-based partitioning requires a timestamp");
        assert!(err
            .to_string()
            .contains("time-based partitioning requires timestamps"));
    }

    #[test]
    fn test_genesis_timestamp_bootstrap_buffers_until_first_timestamped_block() {
        let mut bootstrap = GenesisTimestampBootstrap::new(true, Some(0));

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
    fn test_genesis_timestamp_bootstrap_disabled_when_flag_not_set() {
        let mut bootstrap = GenesisTimestampBootstrap::new(false, Some(0));

        assert_eq!(
            bootstrap.observe_block(0, 0, 0),
            GenesisTimestampBootstrapAction::None
        );
        let err = validate_block_timestamp(0, 0, &Partition::None, Some(0))
            .expect_err("missing timestamps should still fail without bootstrap flag");
        let message = err.to_string();
        assert!(message.contains("missing timestamp metadata"));
        assert!(message.contains("--bootstrap-missing-genesis-timestamp"));
    }

    #[test]
    fn test_missing_genesis_timestamp_bootstrap_error_mentions_missing_anchor() {
        let err = missing_genesis_timestamp_bootstrap_error(0, 3);
        let message = err.to_string();

        assert!(message.contains("--bootstrap-missing-genesis-timestamp"));
        assert!(message.contains("no later block with timestamp metadata was found"));
        assert!(message.contains("block 0"));
    }

    #[test]
    fn test_take_anchored_bootstrap_blocks_preserves_block_numbers() {
        let mut buffered_blocks = vec![
            BufferedBootstrapBlock {
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
            let anchor_key = partition.partition_key(100, 1_700_000_000);
            let routed_key = partition.partition_key(101, routing_timestamp);
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
            let expected = partition.partition_key(0, SOLANA_GENESIS_TIMESTAMP);
            let routed_key = partition.partition_key(0, routing_timestamp);
            assert_eq!(routed_key, expected);
        }
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
            extended_warning_message(),
            "--extended requested but endpoint did not advertise extended block features"
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

        assert!(!resolve_extended_mode(false, &ei));
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

        assert!(resolve_extended_mode(true, &ei));
    }

    #[test]
    fn test_validate_chain_feature_flags_rejects_extended_for_solana() {
        let err = validate_chain_feature_flags("solana", &None, None, true, true, false)
            .expect_err("Solana should reject --extended");
        assert!(err.to_string().contains("--with-votes"));
    }

    #[test]
    fn test_validate_chain_feature_flags_allows_implicit_extended_default_for_solana() {
        validate_chain_feature_flags("solana", &None, None, true, false, false)
            .expect("implicit default should not reject for Solana");
    }

    #[test]
    fn test_validate_chain_feature_flags_rejects_with_votes_for_non_solana() {
        let err = validate_chain_feature_flags("evm", &None, None, false, false, true)
            .expect_err("non-Solana chains should reject --with-votes");
        assert_eq!(err.to_string(), WITH_VOTES_NON_SOLANA_ERROR);
    }

    #[test]
    fn test_validate_chain_feature_flags_rejects_extended_for_antelope() {
        let err = validate_chain_feature_flags("antelope", &None, None, true, true, false)
            .expect_err("Antelope should reject --extended");
        assert_eq!(err.to_string(), ANTELOPE_EXTENDED_ERROR);
    }

    #[test]
    fn test_validate_chain_feature_flags_allows_implicit_extended_default_for_antelope() {
        validate_chain_feature_flags("antelope", &None, None, true, false, false)
            .expect("implicit default should not reject for Antelope");
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
            "auto",
            "https://example.com",
            Compression::Zstd,
            &firehose_parquet::config::Partition::Date,
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
        assert_eq!(find_meta(&meta, "firehose-parquet.partition"), Some("date"));
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

    #[test]
    fn test_is_transient_live_probe_error_matches_service_unavailable() {
        let err = anyhow!("code: 'The service is currently unavailable', message: \"unavailable\"");
        assert!(is_transient_live_probe_error(&err));
    }

    #[tokio::test]
    async fn test_await_live_probe_or_backoff_retries_transient_sparse_probe_failures() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(Notify::new());

        let outcome = await_live_probe_or_backoff(
            &shutdown,
            &shutdown_notify,
            async {
                Err::<u64, _>(anyhow!(
                    "code: 'The service is currently unavailable', message: \"unavailable\""
                ))
            },
            274_902_564_975,
            Duration::from_millis(0),
            "probing live partition span",
        )
        .await
        .expect("transient live probe failures should back off instead of exiting");

        assert!(matches!(outcome, LiveProbeOutcome::RetryAfterBackoff));
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

    #[test]
    fn test_next_live_partition_probe_candidate_avoids_u64_max_sentinel_duplicate_guard() {
        assert_eq!(next_live_partition_probe_candidate(10, 5), Some(15));
        assert_eq!(next_live_partition_probe_candidate(u64::MAX - 1, 1), None);
        assert_eq!(next_live_partition_probe_candidate(u64::MAX - 5, 10), None);
    }

    #[tokio::test]
    async fn test_find_first_different_block_with_fetch_returns_incomplete_boundary_on_missing_probe(
    ) {
        let low_same = BlockIdentity {
            block_num: 100,
            timestamp: 1_700_000_000,
            ..Default::default()
        };
        let high_different = BlockIdentity {
            block_num: 110,
            timestamp: 1_700_086_400,
            ..Default::default()
        };
        let same_ts = low_same.timestamp;

        let span = find_first_different_block_with_fetch(
            PartitionBuildType::Date,
            block_partition_start(PartitionBuildType::Date, &low_same)
                .expect("partition start timestamp"),
            low_same.clone(),
            high_different,
            |mid| async move {
                match mid {
                    105 => Ok(Some(BlockIdentity {
                        block_num: 105,
                        timestamp: same_ts,
                        ..Default::default()
                    })),
                    107 => Ok(None),
                    other => panic!("unexpected probe block {other}"),
                }
            },
        )
        .await
        .expect("boundary search should succeed");

        assert_eq!(span.last_same.block_num, 105);
        assert!(span.next_boundary.is_none());
    }

    #[tokio::test]
    async fn test_find_first_different_block_with_fetch_resolves_boundary_when_probes_available() {
        let low_same = BlockIdentity {
            block_num: 100,
            timestamp: 1_700_000_000,
            ..Default::default()
        };
        let high_different = BlockIdentity {
            block_num: 110,
            timestamp: 1_700_086_400,
            ..Default::default()
        };
        let same_ts = low_same.timestamp;
        let next_ts = high_different.timestamp;

        let span = find_first_different_block_with_fetch(
            PartitionBuildType::Date,
            block_partition_start(PartitionBuildType::Date, &low_same)
                .expect("partition start timestamp"),
            low_same.clone(),
            high_different,
            |mid| async move {
                match mid {
                    105 => Ok(Some(BlockIdentity {
                        block_num: 105,
                        timestamp: same_ts,
                        ..Default::default()
                    })),
                    107 => Ok(Some(BlockIdentity {
                        block_num: 107,
                        timestamp: next_ts,
                        ..Default::default()
                    })),
                    106 => Ok(Some(BlockIdentity {
                        block_num: 106,
                        timestamp: next_ts,
                        ..Default::default()
                    })),
                    other => panic!("unexpected probe block {other}"),
                }
            },
        )
        .await
        .expect("boundary search should succeed");

        assert_eq!(span.last_same.block_num, 105);
        assert_eq!(
            span.next_boundary
                .expect("boundary should resolve")
                .block_num,
            106
        );
    }

    #[tokio::test]
    async fn test_find_timestamp_borrow_probe_bounds_exponential_search() {
        #[derive(Clone, Debug)]
        struct Probe {
            timestamp: i64,
        }

        let probed_candidates = Arc::new(std::sync::Mutex::new(Vec::new()));
        let result = find_timestamp_borrow_probe(
            0,
            16,
            0,
            {
                let probed_candidates = Arc::clone(&probed_candidates);
                move |candidate, allowed_skip| {
                    let probed_candidates = Arc::clone(&probed_candidates);
                    async move {
                        probed_candidates
                            .lock()
                            .expect("lock probed candidates")
                            .push((candidate, allowed_skip));
                        Ok(Some((candidate, Probe { timestamp: 0 })))
                    }
                }
            },
            |probe: &Probe| probe.timestamp,
        )
        .await
        .expect("bounded timestamp scan should succeed");

        assert!(result.is_none());

        let probed_candidates = probed_candidates
            .lock()
            .expect("lock probed candidates")
            .clone();
        let exponential_candidates: Vec<u64> = probed_candidates
            .iter()
            .filter_map(|(candidate, _)| (*candidate > 16).then_some(*candidate))
            .collect();

        assert_eq!(
            exponential_candidates.last().copied(),
            Some(PARTITIONS_PROBE_TIMESTAMP_EXPONENTIAL_MAX_JUMP)
        );
        assert!(exponential_candidates
            .iter()
            .all(|candidate| *candidate <= PARTITIONS_PROBE_TIMESTAMP_EXPONENTIAL_MAX_JUMP))
    }

    #[test]
    fn test_format_missing_timestamp_probe_error_suggests_block_range() {
        let message = format_missing_timestamp_probe_error(
            "polling live frontier",
            4_420_838,
            16,
            PARTITIONS_PROBE_TIMESTAMP_EXPONENTIAL_MAX_JUMP,
        );

        assert!(
            message.contains("polling live frontier: block 4420838 is missing timestamp metadata")
        );
        assert!(message.contains("--partition block_range"));
        assert!(message.contains("--block-range-size <N>"));
        assert!(!message.contains("--strict-timestamps"));
    }

    #[test]
    fn test_next_live_partition_probe_candidate_avoids_u64_max_sentinel() {
        assert_eq!(next_live_partition_probe_candidate(10, 5), Some(15));
        assert_eq!(next_live_partition_probe_candidate(u64::MAX - 1, 1), None);
        assert_eq!(next_live_partition_probe_candidate(u64::MAX - 5, 10), None);
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
    fn test_load_existing_partitions_build_rows_reads_existing_index() {
        let dir = std::env::temp_dir().join(format!(
            "fireparq-load-existing-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("partitions.parquet");
        let rows = vec![PartitionBuildRow {
            partition_type: "hour".to_string(),
            partition_interval_seconds: 3_600,
            partition_start_ts: "2023-07-31 14:00:00".to_string(),
            partition_value: "2023-07-31 14:00:00".to_string(),
            start_block: 100,
            stop_block: 200,
            start_time: Some("2023-07-31 14:00:01".to_string()),
            end_time: Some("2023-07-31 14:59:59".to_string()),
            chain: Some("eth-mainnet".to_string()),
        }];

        firehose_parquet::cli::write_partitions_index(&path.to_string_lossy(), &rows, None)
            .expect("write partitions index");

        let loaded = load_existing_partitions_build_rows(
            &path.to_string_lossy(),
            &AwsConfig {
                aws_access_key_id: None,
                aws_secret_access_key: None,
                aws_session_token: None,
                aws_region: None,
                aws_endpoint_url: None,
            },
            false,
        )
        .expect("load existing rows");

        assert_eq!(loaded, rows);
        std::fs::remove_file(&path).expect("remove parquet");
        std::fs::remove_dir(&dir).expect("remove temp dir");
    }

    #[test]
    fn test_load_existing_partitions_build_rows_skips_existing_index_when_overwrite() {
        let dir = std::env::temp_dir().join(format!(
            "fireparq-load-overwrite-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("partitions.parquet");
        let rows = vec![PartitionBuildRow {
            partition_type: "hour".to_string(),
            partition_interval_seconds: 3_600,
            partition_start_ts: "2023-07-31 14:00:00".to_string(),
            partition_value: "2023-07-31 14:00:00".to_string(),
            start_block: 100,
            stop_block: 200,
            start_time: Some("2023-07-31 14:00:01".to_string()),
            end_time: Some("2023-07-31 14:59:59".to_string()),
            chain: Some("eth-mainnet".to_string()),
        }];

        firehose_parquet::cli::write_partitions_index(&path.to_string_lossy(), &rows, None)
            .expect("write partitions index");

        let loaded = load_existing_partitions_build_rows(
            &path.to_string_lossy(),
            &AwsConfig {
                aws_access_key_id: None,
                aws_secret_access_key: None,
                aws_session_token: None,
                aws_region: None,
                aws_endpoint_url: None,
            },
            true,
        )
        .expect("skip existing rows");

        assert!(loaded.is_empty());
        std::fs::remove_file(&path).expect("remove parquet");
        std::fs::remove_dir(&dir).expect("remove temp dir");
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
                stop_block: 10,
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
                stop_block: 20,
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
            false,
        )
        .expect("checkpoint rows");

        let persisted =
            read_partitions_build_rows(path.to_str().expect("utf8 path"), None).expect("read back");
        assert_eq!(persisted.len(), 2);
        assert_eq!(persisted[0].start_block, 0);
        assert_eq!(persisted[0].stop_block, 10);
        assert_eq!(persisted[1].start_block, 10);
        assert_eq!(persisted[1].stop_block, 20);
        assert_eq!(checkpoint_state.last_checkpoint_frontier, Some(20));
        std::fs::remove_file(&path).expect("remove parquet");
        std::fs::remove_dir(&dir).expect("remove temp dir");
    }

    #[test]
    fn test_block_partition_start_label_for_block_range_uses_partition_boundary() {
        let block = BlockIdentity {
            block_num: 167_772_19,
            timestamp: 1_614_498_520,
            ..Default::default()
        };

        let label =
            block_partition_start_label(PartitionBuildType::BlockRange, &block, Some(100_000))
                .expect("block range label");

        assert_eq!(label, "16700000");
    }

    #[test]
    fn test_block_partition_start_label_for_hour_formats_timestamp() {
        let block = BlockIdentity {
            block_num: 42,
            timestamp: 1_690_815_590,
            ..Default::default()
        };

        let label = block_partition_start_label(PartitionBuildType::Hour, &block, None)
            .expect("hour label");

        assert_eq!(label, "2023-07-31 14:00:00");
    }

    #[test]
    fn test_format_optional_probe_timestamp_handles_missing_timestamp() {
        assert_eq!(
            format_optional_probe_timestamp(0).expect("missing timestamp should be allowed"),
            None
        );
        assert_eq!(
            format_optional_probe_timestamp(1_690_815_590)
                .expect("valid timestamp")
                .as_deref(),
            Some("2023-07-31 14:59:50")
        );
    }

    #[test]
    fn test_live_block_range_frontier_log_context_uses_latest_completed_partition() {
        let rows = vec![
            PartitionBuildRow {
                partition_type: "block_range".to_string(),
                partition_interval_seconds: 100_000,
                partition_start_ts: "406200000".to_string(),
                partition_value: "406200000".to_string(),
                start_block: 406_200_000,
                stop_block: 406_300_000,
                start_time: Some("2026-03-14 16:00:00".to_string()),
                end_time: Some("2026-03-14 16:59:59".to_string()),
                chain: Some("solana-mainnet-beta".to_string()),
            },
            PartitionBuildRow {
                partition_type: "block_range".to_string(),
                partition_interval_seconds: 100_000,
                partition_start_ts: "406300000".to_string(),
                partition_value: "406300000".to_string(),
                start_block: 406_300_000,
                stop_block: 406_400_000,
                start_time: Some("2026-03-14 17:00:00".to_string()),
                end_time: Some("2026-03-14 17:59:59".to_string()),
                chain: Some("solana-mainnet-beta".to_string()),
            },
        ];

        let context = live_block_range_frontier_log_context(&rows, 406_400_000, 100_000);

        assert_eq!(context.next_partition_start, 406_400_000);
        assert_eq!(context.next_partition, "406400000-406500000");
        assert_eq!(
            context.latest_completed_partition.as_deref(),
            Some("406300000")
        );
        assert_eq!(
            context.latest_completed_start_time.as_deref(),
            Some("2026-03-14 17:00:00")
        );
        assert_eq!(
            context.latest_completed_end_time.as_deref(),
            Some("2026-03-14 17:59:59")
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
}
