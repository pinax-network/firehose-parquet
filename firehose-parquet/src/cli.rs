//! Command-line declarations with stable re-exports of command operations.
mod configuration;
mod inspect;
mod partitions;
mod paths;
mod validate;
// Keep the established crate::cli API while implementations live in focused modules.
// Restricted helpers stay visible only inside cli and are not made public by re-export.
pub use configuration::*;
pub use inspect::*;
pub use partitions::*;
pub use paths::*;
pub use validate::*;

use crate::config::{Compression, Config, Partition};
use crate::networks::KNOWN_NETWORK_NAMES;
use crate::partition_index::{
    PartitionCoverage, PartitionSpanProof, VerifiedPartitionIndex, VerifiedPartitionSpan,
    INDEX_COVERAGE_METADATA,
};
use clap::builder::PossibleValuesParser;
use clap::Args;
use clap_complete::{generate, Shell};
#[cfg(test)]
use parquet::basic::Compression as PqCompression;
use std::io;
use std::path::{Component, Path, PathBuf};

pub use crate::config::DEFAULT_FLUSH_BYTES;
use crate::config::{
    DEFAULT_FLUSH_MEMORY_BYTES, DEFAULT_GRPC_MAX_MESSAGE_BYTES, DEFAULT_GRPC_WINDOW_BYTES,
};

/// Transport options shared by ingestion and partition index construction.
#[derive(Args, Debug, Clone)]
pub struct GrpcArgs {
    /// Adapt HTTP/2 receive windows to measured bandwidth/latency, overriding --grpc-window-bytes
    #[arg(long = "grpc-adaptive-window", env = "GRPC_ADAPTIVE_WINDOW", default_value = "false",
        action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true",
        require_equals = true, hide_env_values = true, help_heading = "Connection")]
    pub adaptive_window: bool,

    /// Initial HTTP/2 stream and connection receive window bytes (0 uses library defaults; adaptive mode overrides)
    #[arg(long = "grpc-window-bytes", env = "GRPC_WINDOW_BYTES", default_value_t = DEFAULT_GRPC_WINDOW_BYTES,
        value_parser = clap::value_parser!(u32).range(0..=2147483647), hide_env_values = true,
        help_heading = "Connection")]
    pub window_bytes: u32,

    /// Maximum encoded or decompressed gRPC response bytes (128 MiB by default)
    #[arg(long = "grpc-max-message-bytes", env = "GRPC_MAX_MESSAGE_BYTES",
        default_value_t = DEFAULT_GRPC_MAX_MESSAGE_BYTES, value_parser = clap::value_parser!(u32).range(1..),
        hide_env_values = true, help_heading = "Connection")]
    pub max_message_bytes: u32,
}

impl GrpcArgs {
    pub fn config(&self) -> crate::config::GrpcConfig {
        crate::config::GrpcConfig {
            adaptive_window: self.adaptive_window,
            initial_window_bytes: (self.window_bytes != 0).then_some(self.window_bytes),
            max_message_bytes: self.max_message_bytes,
        }
    }
}

/// AWS options shared by every command; credentials are never displayed in help.
#[derive(Args, Debug, Clone, Default)]
pub struct AwsArgs {
    /// AWS access key ID (for S3 access)
    #[arg(
        long,
        env = "AWS_ACCESS_KEY_ID",
        hide_env_values = true,
        help_heading = "AWS / S3"
    )]
    pub aws_access_key_id: Option<String>,

    /// AWS secret access key (for S3 access)
    #[arg(
        long,
        env = "AWS_SECRET_ACCESS_KEY",
        hide_env_values = true,
        help_heading = "AWS / S3"
    )]
    pub aws_secret_access_key: Option<String>,

    /// AWS session token (for S3 access)
    #[arg(
        long,
        env = "AWS_SESSION_TOKEN",
        hide_env_values = true,
        help_heading = "AWS / S3"
    )]
    pub aws_session_token: Option<String>,

    /// AWS region (for S3 access)
    #[arg(
        long,
        env = "AWS_REGION",
        hide_env_values = true,
        help_heading = "AWS / S3"
    )]
    pub aws_region: Option<String>,

    /// AWS endpoint URL (for S3-compatible services)
    #[arg(
        long,
        env = "AWS_ENDPOINT_URL_S3",
        hide_env_values = true,
        help_heading = "AWS / S3"
    )]
    pub aws_endpoint_url: Option<String>,
}

impl From<&AwsArgs> for AwsConfig {
    fn from(args: &AwsArgs) -> Self {
        Self {
            aws_access_key_id: args.aws_access_key_id.clone(),
            aws_secret_access_key: args.aws_secret_access_key.clone(),
            aws_session_token: args.aws_session_token.clone(),
            aws_region: args.aws_region.clone(),
            aws_endpoint_url: args.aws_endpoint_url.clone(),
        }
    }
}

// Shared CLI arguments for all fireparq binaries.
//
// Embed in a per-chain `#[derive(Parser)]` struct with `#[command(flatten)]`.
#[derive(Args, Debug, Clone)]
pub struct CommonArgs {
    #[command(flatten)]
    pub grpc: GrpcArgs,
    /// Firehose gRPC endpoint URL
    #[arg(
        short = 'e',
        long,
        env = "ENDPOINT",
        hide_env_values = true,
        help_heading = "Connection"
    )]
    pub endpoint: Option<String>,

    /// Explicit API key env var for this endpoint (default: provider-scoped credentials)
    #[arg(
        long,
        env = "API_KEY_ENVVAR",
        hide_env_values = true,
        help_heading = "Connection"
    )]
    pub api_key_envvar: Option<String>,

    /// Explicit bearer token env var for this endpoint (default: provider-scoped credentials)
    #[arg(
        long,
        env = "API_TOKEN_ENVVAR",
        hide_env_values = true,
        help_heading = "Connection"
    )]
    pub api_token_envvar: Option<String>,

    /// Start block number (inclusive).
    ///
    /// When `--stop-block` is omitted, omitting this resumes from an existing
    /// cursor when available, otherwise starts from the endpoint's first
    /// streamable block.
    #[arg(
        short = 's',
        long,
        env = "START_BLOCK",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub start_block: Option<u64>,

    /// Stop block number (exclusive); must be greater than the start block.
    ///
    /// When omitted, the build runs in live mode and keeps following finalized
    /// blocks.
    #[arg(
        short = 't',
        long,
        env = "STOP_BLOCK",
        hide_env_values = true,
        help_heading = "Block Range",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub stop_block: Option<u64>,

    /// Path to cursor parquet file for resuming a previous session (must end in .parquet)
    #[arg(
        short = 'c',
        long,
        env = "CURSOR",
        default_value = "cursor.parquet",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub cursor: PathBuf,

    /// Optional template used to derive the cursor path.
    ///
    /// Literal paths are supported directly. Use `{{` and `}}` to escape braces.
    #[arg(
        long,
        env = "CURSOR_TEMPLATE",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub cursor_template: Option<String>,

    /// Only process finalized blocks; =false appends NEW/UNDO rows with fork_step
    #[arg(
        long,
        env = "FINAL_BLOCKS_ONLY",
        default_value = "true",
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = true,
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub final_blocks_only: bool,

    /// Output directory
    #[arg(
        long,
        env = "OUTPUT",
        default_value = ".",
        hide_env_values = true,
        help_heading = "Output"
    )]
    pub output: PathBuf,

    /// Partitioning mode: none, block_range, date, hour, minute, second
    #[arg(
        long,
        env = "PARTITION",
        default_value = "none",
        hide_env_values = true,
        help_heading = "Output"
    )]
    pub partition: String,

    /// Block range size when partition=block_range (must be at least 1)
    #[arg(
        long,
        env = "BLOCK_RANGE_SIZE",
        default_value = "10000",
        hide_env_values = true,
        help_heading = "Output",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub block_range_size: u64,

    /// Compression codec: zstd (level 3), zstd:<level>, snappy, gzip, none
    #[arg(
        long,
        env = "COMPRESSION",
        default_value = "zstd",
        hide_env_values = true,
        help_heading = "Output"
    )]
    pub compression: String,

    /// Flush mapper state and write Parquet after this many rows in the largest table (0 or unset disables)
    #[arg(
        long,
        env = "FLUSH_ROWS",
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_rows: Option<u32>,

    /// Flush written files after this many processed blocks (disabled by default)
    #[arg(
        long,
        env = "FLUSH_BLOCKS",
        hide_env_values = true,
        help_heading = "Flush",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub flush_blocks: Option<u64>,

    /// Target compressed bytes in the largest table file, learned from committed files (0 disables this target; other triggers can write smaller files)
    #[arg(
        long,
        env = "FLUSH_BYTES",
        default_value_t = DEFAULT_FLUSH_BYTES,
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_bytes: u64,

    /// Flush at this summed mapper byte estimate, even below the file target (positive; not RSS, excludes decoder/encoder/allocator overhead; one block can overshoot)
    #[arg(
        long,
        env = "FLUSH_MEMORY_BYTES",
        default_value_t = DEFAULT_FLUSH_MEMORY_BYTES,
        value_parser = clap::value_parser!(u64).range(1..),
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_memory_bytes: u64,

    /// Flush mapper state and write Parquet every N seconds (0 or unset disables)
    #[arg(
        long,
        env = "FLUSH_INTERVAL_SECS",
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_interval_secs: Option<u64>,

    /// Log level: trace, debug, info, warn, error
    #[arg(
        long,
        env = "LOG_LEVEL",
        default_value = "info",
        hide_env_values = true
    )]
    pub log_level: String,

    /// Enable verbose operational logs for debugging without changing normal default output
    #[arg(long, env = "VERBOSE", default_value = "false", hide_env_values = true)]
    pub verbose: bool,

    /// Decode and map but don't write files
    #[arg(long, env = "DRY_RUN", default_value = "false", hide_env_values = true)]
    pub dry_run: bool,

    /// Prometheus /metrics HTTP port. When set, an HTTP server binds to 0.0.0.0:<PORT>/metrics.
    #[arg(long, env = "METRICS_PORT", hide_env_values = true)]
    pub metrics_port: Option<u16>,

    /// Return /ready 503 after N seconds without a valid stream message
    #[arg(long, env = "METRICS_STALE_AFTER_SECS", default_value = "120", value_parser = clap::value_parser!(u64).range(1..), hide_env_values = true)]
    pub metrics_stale_after_secs: u64,

    /// Force a reconnect if no stream message is received for N seconds (0 disables)
    #[arg(
        long,
        env = "STREAM_IDLE_TIMEOUT_SECS",
        default_value = "120",
        hide_env_values = true,
        help_heading = "Connection"
    )]
    pub stream_idle_timeout_secs: Option<u64>,

    /// Exit with an error if reconnecting continuously for N seconds (0 disables)
    #[arg(
        long,
        env = "RECONNECT_STALL_TIMEOUT_SECS",
        default_value = "900",
        hide_env_values = true,
        help_heading = "Connection"
    )]
    pub reconnect_stall_timeout_secs: Option<u64>,

    #[command(flatten)]
    pub aws: AwsArgs,

    /// S3 bucket for relative output paths; must match an explicit s3:// output URI
    #[arg(
        long,
        env = "S3_BUCKET",
        hide_env_values = true,
        help_heading = "AWS / S3"
    )]
    pub s3_bucket: Option<String>,

    /// Cache-Control header for S3 uploads (empty string = no header)
    #[arg(
        long,
        env = "CACHE_CONTROL",
        default_value = "public, max-age=31536000, immutable",
        hide_env_values = true,
        help_heading = "AWS / S3"
    )]
    pub cache_control: String,
}

/// Arguments for the `build` subcommand — main Firehose ingestion pipeline.
///
/// Streams blocks from a Firehose gRPC endpoint and writes Apache Parquet
/// datasets partitioned by block range, date, hour, minute, or second.
/// Missing blocks are skipped automatically after probe retries are exhausted.
#[derive(clap::Args, Debug, Clone)]
#[command(after_long_help = "\
Examples:
  # Run a bounded historical ingestion
  fireparq build --network mainnet \\
    --start-block 20000000 --stop-block 20001000

  # Backfill from a block and keep following finalized blocks
  fireparq build --network solana-mainnet-beta \\
    --start-block 250000000

  # Start live mode from the endpoint's first streamable block
  fireparq build --network mainnet

  # Override a network alias with an env var
  FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA=https://solana.internal.example.com:443 \\
    fireparq build --network solana-mainnet-beta --start-block 250000000 --stop-block 250100000

  # Disable Solana vote transactions explicitly
  fireparq build --network solana-mainnet-beta \\
    --start-block 250000000 --stop-block 250001000 \\
    --without-votes

  # Disable extended EVM tables explicitly
  fireparq build --network mainnet \\
    --start-block 20000000 --stop-block 20001000 \\
    --without-extended

  # Stream Antelope blocks
  fireparq build --block-type antelope \\
    --endpoint https://eos.firehose.pinax.network:443 \\
    --start-block 1000000 --stop-block 1001000

  # Resume from cursor
  fireparq build --network mainnet \\
    --cursor cursor.parquet --partition date
")]
pub struct BuildArgs {
    #[command(flatten)]
    pub common: CommonArgs,

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
    pub network: Option<String>,

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
    pub block_type: String,

    /// Disable extended detail tables for chains that support them.
    #[arg(
        long,
        env = "WITHOUT_EXTENDED",
        hide_env_values = true,
        help_heading = "Chain"
    )]
    pub without_extended: bool,

    /// Disable Solana `vote_transactions` output.
    #[arg(
        long,
        env = "WITHOUT_VOTES",
        hide_env_values = true,
        help_heading = "Chain"
    )]
    pub without_votes: bool,

    /// Include failed/reverted transactions on non-EVM chains (default: false).
    /// Deprecated for EVM, which includes them by default; it has no effect there.
    #[arg(
        long,
        env = "INCLUDE_FAILED_TRANSACTIONS",
        default_value = "false",
        hide_env_values = true,
        help_heading = "Chain"
    )]
    pub include_failed_transactions: bool,

    /// Drop failed/reverted transactions. EVM includes them by default with
    /// only their persistent state changes (gas/fee balance changes, the
    /// sender's nonce, EIP-7702 authorizations). Takes precedence over
    /// --include-failed-transactions.
    #[arg(
        long,
        env = "EXCLUDE_FAILED_TRANSACTIONS",
        default_value = "false",
        hide_env_values = true,
        help_heading = "Chain"
    )]
    pub exclude_failed_transactions: bool,

    /// Ignore legacy cursor defaults during a read-only dry run. Protected
    /// ingestion refuses cursor overrides; use a new empty output root and an
    /// absent mirror to change the original range or mapper semantics.
    #[arg(
        long,
        env = "CURSOR_OVERRIDE",
        default_value = "false",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub cursor_override: bool,
}

/// Subcommands shared by all binaries.
#[derive(clap::Subcommand, Debug)]
pub enum Commands {
    /// Inspect recovery state or explicitly release a provider-quiescent S3 owner.
    #[command(subcommand)]
    Recovery(crate::recovery::RecoveryCommands),
    /// Generate shell completions for the given shell
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
    },
    /// Stream blocks from a Firehose gRPC endpoint and write Apache Parquet datasets.
    ///
    /// This is the primary ingestion workflow. Partitions output by block range,
    /// date, hour, minute, or second. Supports live mode, cursor-based resume,
    /// and S3 output. Missing blocks are skipped automatically after probe retries
    /// are exhausted.
    Build(BuildArgs),
    /// Partition index utilities (`partitions.parquet` workflows).
    #[command(subcommand)]
    Partitions(PartitionsCommands),
    /// Read and inspect Parquet files (schema, row counts, sample rows).
    /// Supports local paths, shorthand S3 keys via `S3_BUCKET`, and `s3://bucket/prefix` URIs.
    #[command(after_long_help = "\
Examples:
  # Inspect a local parquet file
  fireparq scan ./output/blocks/part-000001.parquet

  # Scan all files in a directory (up to 20 sample rows total)
  fireparq scan ./output/blocks/

  # Schema only, no data preview
  fireparq scan ./output/blocks/ --schema-only

  # Scan S3 files
  fireparq scan s3://bucket/eth-mainnet/blocks/

  # Resolve a shorthand key via S3_BUCKET when no local match exists
  S3_BUCKET=my-bucket fireparq scan eth-mainnet/partitions.parquet

  # Scan a single S3 parquet file
  fireparq scan s3://bucket/eth-mainnet/partitions.parquet

  # Use row-by-row vertical output
  fireparq scan ./output/blocks/part-000001.parquet --vertical

  # Emit machine-readable JSON
  fireparq scan ./output/blocks/part-000001.parquet --json

  # Show up to 50 sample rows total across the scan
  fireparq scan ./output/blocks/ --limit 50

  # Paginate: skip first 20 rows, show next 20
  fireparq scan ./output/blocks/ --offset 20 --limit 20

  # Show the latest 20 rows first
  fireparq scan ./output/blocks/part-000001.parquet --order desc --limit 20

  # Skip the latest 20 rows, then show the previous 20
  fireparq scan ./output/blocks/part-000001.parquet --order desc --offset 20 --limit 20

Lookup order:
  1. Explicit s3://bucket/... URIs are used as-is.
  2. Non-URI paths use the local filesystem when the path exists.
  3. Otherwise, if S3_BUCKET is set, relative paths fall back to s3://<bucket>/<path>.
")]
    Scan {
        /// Path to a .parquet file or directory, a shorthand S3 key/prefix via S3_BUCKET, or an S3 URI
        #[arg(help_heading = "Selection")]
        path: String,
        /// Number of sample rows to display across the full scan (0 = schema only)
        #[arg(
            short = 'n',
            long = "limit",
            default_value = "20",
            help_heading = "Selection"
        )]
        limit: usize,
        /// Number of rows to skip before displaying (for pagination across the full scan)
        #[arg(long, default_value = "0", help_heading = "Selection")]
        offset: usize,
        /// Row display order for pagination and previews
        #[arg(long, value_enum, default_value = "asc", help_heading = "Selection")]
        order: ScanOrder,
        /// Only show file metadata (schema, row count, size) without data
        #[arg(long, default_value = "false", help_heading = "Selection")]
        schema_only: bool,
        /// Use row-by-row vertical display instead of boxed table output
        #[arg(
            long,
            default_value = "false",
            conflicts_with = "json",
            help_heading = "Display"
        )]
        vertical: bool,
        /// Emit machine-readable JSON including file info, schema, and sampled rows
        #[arg(
            long,
            default_value = "false",
            conflicts_with = "vertical",
            help_heading = "Display"
        )]
        json: bool,
        #[command(flatten)]
        aws: AwsArgs,
    },
    /// Validate block sequence integrity of Parquet files.
    ///
    /// Checks for gaps, parent hash chain, ordering, timestamps, schema
    /// consistency, empty partitions, and cross-partition continuity.
    #[command(after_long_help = "\
Examples:
  # Validate local blocks directory
  fireparq validate ./output/blocks/

  # Validate shorthand S3 path when no local match exists
  S3_BUCKET=my-bucket fireparq validate eth-mainnet/blocks/

  # Validate S3 path
  fireparq validate s3://bucket/eth-mainnet/blocks/

  # Check continuity across partition boundaries
  fireparq validate ./output/blocks/ --cross-partition

Lookup order:
  1. Explicit s3://bucket/... URIs are used as-is.
  2. Non-URI paths use the local filesystem when the path exists.
  3. Otherwise, if S3_BUCKET is set, relative paths fall back to s3://<bucket>/<path>.
")]
    Validate {
        /// Path to a directory of .parquet files, a shorthand S3 key/prefix via S3_BUCKET, or an S3 URI
        #[arg(help_heading = "Selection")]
        path: String,
        /// Check continuity across partition boundaries
        #[arg(long, default_value = "false", help_heading = "Validation")]
        cross_partition: bool,
        /// Allow gaps in block numbers (e.g. Solana skipped slots)
        #[arg(long, default_value = "false", help_heading = "Validation")]
        allow_gaps: bool,
        #[command(flatten)]
        aws: AwsArgs,
    },
    /// Verify deterministic partition merkle roots for table parquet data.
    ///
    /// Reads one table directory of `build` output (`<output>/<chain_name>/<table>`),
    /// computes partition-level `merkle_v2` roots, compares them to
    /// `<output>/<chain_name>/merkle_roots.parquet`, and optionally writes
    /// missing/updated entries. The chain and table are inferred from the file
    /// metadata and the directory layout.
    #[command(after_long_help = "\
Examples:
  # Verify ETH mainnet blocks (chain and table are inferred) and fill missing registry roots
  fireparq verify ./output/mainnet/blocks

  # Quick profile (roots only)
  fireparq verify ./output/mainnet/blocks --profile quick

  # Explicitly select checks regardless of profile
  fireparq verify ./output/mainnet/blocks --checks roots,protocol

  # Continue scanning all partitions (no fail-fast) and emit JSON report
  fireparq verify ./output/mainnet/blocks --no-fail-fast --report-json verify-report.json

  # Publish the report to <output>/<chain_name>/verify_runs/<run_id>/report.json
  fireparq verify ./output/mainnet/blocks --publish-report

  # Publish the report to an explicit S3 location
  fireparq verify s3://bucket/mainnet/blocks \
    --publish-report-path s3://bucket/mainnet/verify_runs/custom-run/report.json

  # Resolve a shorthand S3 data path when no local match exists
  S3_BUCKET=my-bucket fireparq verify mainnet/blocks

  # Verify S3 parquet data with an explicit registry (rows are keyed by network)
  fireparq verify s3://bucket/mainnet/blocks \
    --registry-path s3://bucket/mainnet/merkle_roots.parquet

  # Data without firehose-parquet.block_type metadata: name the chain explicitly
  fireparq verify ./output/btc/blocks --chain bitcoin

  # Override hash strategy (default: auto from chain)
  fireparq verify ./output/btc/blocks --hash-strategy sha256

  # Update mismatched registry roots (default behavior only fills missing roots)
  fireparq verify ./output/mainnet/blocks --update-registry

  # Rebuild a registry written with an older Merkle version (e.g. legacy merkle_v1 roots);
  # replaced rows are reported as `updated` and the run exits 0 once the registry is written
  fireparq verify ./output/mainnet/blocks --update-registry

Lookup order for the data path:
  1. Explicit s3://bucket/... URIs are used as-is.
  2. Non-URI paths use the local filesystem when the path exists.
  3. Otherwise, if S3_BUCKET is set, relative paths fall back to s3://<bucket>/<path>.
")]
    Verify {
        /// Path to a directory of .parquet files, a single parquet file, a shorthand S3 key/prefix via S3_BUCKET, or an S3 URI
        #[arg(help_heading = "Selection")]
        path: String,
        /// Chain family (evm, bitcoin, solana, ...) [default: firehose-parquet.block_type file metadata; a different value is an error]
        #[arg(long, help_heading = "Selection")]
        chain: Option<String>,
        /// Table name [default: the table directory in <chain_root>/<table>/...; a different value is an error]
        #[arg(long, help_heading = "Selection")]
        table: Option<String>,
        /// Hash strategy used for leaf+merkle hashing (auto, keccak256, sha256)
        #[arg(long, default_value = "auto", help_heading = "Verification")]
        hash_strategy: String,
        /// Check families to run (`roots`, `protocol`, `continuity`, `completeness`)
        #[arg(long, value_enum, value_delimiter = ',', help_heading = "Verification")]
        checks: Vec<crate::verify::VerifyCheck>,
        /// Check profile (`quick`, `standard`, `deep`) used when --checks is not set
        #[arg(
            long,
            value_enum,
            default_value = "standard",
            help_heading = "Verification"
        )]
        profile: crate::verify::VerifyProfile,
        /// Report scope tag for metadata (`chain`, `table`, `partition`, `run`)
        #[arg(
            long,
            value_enum,
            default_value = "table",
            help_heading = "Verification"
        )]
        scope: crate::verify::VerifyScope,
        /// Continue scanning and aggregate findings instead of failing on first mismatch
        #[arg(long, default_value = "false", help_heading = "Verification")]
        no_fail_fast: bool,
        /// Optional path to write a JSON verification report
        #[arg(long, help_heading = "Reporting")]
        report_json: Option<PathBuf>,
        /// Publish the JSON report to the suggested verify artifact path
        #[arg(long, default_value = "false", help_heading = "Reporting")]
        publish_report: bool,
        /// Explicit path to publish the JSON report (local or s3://)
        #[arg(long, help_heading = "Reporting")]
        publish_report_path: Option<String>,
        /// Explicit merkle roots registry path (local or s3://) [default: <chain_root>/merkle_roots.parquet]
        #[arg(long, help_heading = "Registry")]
        registry_path: Option<String>,
        /// Accept the current data: replace differing roots (including roots from an older Merkle version) with computed values; replaced rows are reported as `updated` and the run passes once the registry is written
        #[arg(long, default_value = "false", help_heading = "Registry")]
        update_registry: bool,
        #[command(flatten)]
        aws: AwsArgs,
    },
    /// Roll up fine-grained partitioned Parquet files into coarser intervals.
    ///
    /// Reads minute/hour-partitioned files and merges them into hourly or daily
    /// partitions, streaming one output part at a time with a --flush-bytes target.
    #[command(after_long_help = "\
Examples:
  # Roll up minute partitions into daily, replacing the minute files (in-place)
  fireparq rollup ./output/blocks/ --delete-source

  # Roll up to hourly partitions with a separate output, keeping the source files
  fireparq rollup ./output/blocks/ -o ./merged/ -p hour

  # Roll up S3 data, delete source files after
  fireparq rollup s3://bucket/blocks/ --delete-source

  # Custom file size limit (256 MB)
  fireparq rollup ./output/blocks/ -o ./daily/blocks/ --flush-bytes 268435456

Only part-*.parquet files below a partition finer than --partition are read.
Files already at the target granularity and root artifacts (cursor.parquet,
partitions.parquet, merkle_roots.parquet, verify_runs/) are left untouched, so
re-running a rollup is safe. Without --delete-source, each re-run replaces the
part-rollup-*.parquet files it wrote earlier in the target partitions it rolls up.

A target partition whose source files have different columns (names, types,
nullability, or order) is left untouched, and rollup exits non-zero.

The source path must exist locally or be an explicit s3://bucket/... URI. Unlike
scan and inspect, rollup never falls back to s3://$S3_BUCKET/<path> for a missing
local path.
")]
    Rollup {
        /// Source path containing partitioned Parquet files (existing local directory or s3:// URI)
        #[arg(help_heading = "Selection")]
        source: String,
        /// Output path (local directory or S3 URI). Defaults to source (in-place rollup, which requires --delete-source).
        #[arg(short = 'o', long, help_heading = "Selection")]
        output: Option<String>,
        /// Target partition interval: hour or date
        #[arg(
            short = 'p',
            long = "partition",
            default_value = "date",
            help_heading = "Selection"
        )]
        partition: String,
        /// Compression codec: zstd (level 3), zstd:<level>, snappy, gzip, none
        #[arg(long, default_value = "zstd", help_heading = "Output")]
        compression: String,
        /// Target compressed bytes per part, with batch/codec overhead (0 = unlimited output size)
        #[arg(long, default_value_t = DEFAULT_FLUSH_BYTES, help_heading = "Output")]
        flush_bytes: u64,
        /// Delete each source file once its target partition is written (required for in-place rollup)
        #[arg(long, default_value = "false", help_heading = "Execution")]
        delete_source: bool,
        #[command(flatten)]
        aws: AwsArgs,
        /// Cache-Control header for S3 uploads (empty string = no header)
        #[arg(
            long,
            env = "CACHE_CONTROL",
            default_value = "public, max-age=31536000, immutable",
            help_heading = "AWS / S3"
        )]
        cache_control: String,
    },
    /// Merge small parquet part files within each partition into larger files.
    ///
    /// Unlike rollup (which changes partition granularity), merge consolidates
    /// multiple small parts within each existing partition directory into fewer,
    /// larger files. Source parts are deleted after successful merge. Use
    /// --flush-bytes and/or --flush-rows to cap merged output size.
    #[command(after_long_help = "\
Examples:
  # Merge parts within each partition (local)
  fireparq merge ./output/blocks/

  # Merge S3 data
  fireparq merge s3://bucket/eth-mainnet/blocks/

  # Custom target file size (512 MB)
  fireparq merge ./output/blocks/ --flush-bytes 536870912

  # Cap merged output by rows
  fireparq merge ./output/blocks/ --flush-rows 100000

  # Preview what would be merged
  fireparq merge ./output/blocks/ --dry-run

  # Log uploaded and deleted files during merge
  fireparq merge s3://bucket/eth-mainnet/blocks/ --verbose

  # Use snappy compression
  fireparq merge ./output/blocks/ --compression snappy

Root artifacts (cursor.parquet, partitions.parquet, merkle_roots.parquet,
verify_runs/) are skipped. A partition whose parts have different columns
(names, types, nullability, or order) is left untouched and listed in the
summary, and merge exits non-zero.

Each partition merge is journaled in _fireparq_merge.json. Local interrupted
merges recover under the common directory guard. S3 mutations hold a persistent
bucket-wide owner with no expiry or automatic takeover. After a remote error,
recovery requires provider-confirmed request quiescence and explicit release of
the exact owner. Legacy S3 journals using the former expiring lock are refused.
Use recovery status to inspect ownership; process exit alone is not remote drain.

The path must exist locally or be an explicit s3://bucket/... URI. Unlike scan and
inspect, merge never falls back to s3://$S3_BUCKET/<path> for a missing local path.
")]
    Merge {
        /// Path to a directory of partitioned .parquet files (existing local directory or s3:// URI)
        #[arg(help_heading = "Selection")]
        path: String,
        /// Compression codec: zstd (level 3), zstd:<level>, snappy, gzip, none
        #[arg(long, default_value = "zstd", help_heading = "Output")]
        compression: String,
        /// Flush merged output after this many rows (0 or unset disables)
        #[arg(long, help_heading = "Flush")]
        flush_rows: Option<u32>,
        /// Start a new merged file once its encoded (compressed) bytes reach this target, checked between batches (0 = unlimited)
        #[arg(long, default_value_t = DEFAULT_FLUSH_BYTES, help_heading = "Flush")]
        flush_bytes: u64,
        /// Show what would be merged without writing
        #[arg(long, default_value = "false", help_heading = "Execution")]
        dry_run: bool,
        #[command(flatten)]
        aws: AwsArgs,
        /// Cache-Control header for S3 uploads
        #[arg(
            long,
            env = "CACHE_CONTROL",
            default_value = "public, max-age=31536000, immutable",
            help_heading = "AWS / S3"
        )]
        cache_control: String,
    },
    /// Inspect a single Parquet file's metadata: file-level key-value pairs,
    /// schema, row group details, and column chunk info.
    /// Supports local paths, shorthand S3 keys via `S3_BUCKET`, and `s3://bucket/key.parquet` URIs.
    #[command(after_long_help = "\
Examples:
  # Inspect a local parquet file
  fireparq inspect ./output/blocks/part-000001.parquet

  # Inspect an S3 parquet file
  fireparq inspect s3://bucket/eth-mainnet/blocks/part-000001.parquet

  # Resolve a shorthand key via S3_BUCKET when no local match exists
  S3_BUCKET=my-bucket fireparq inspect eth-mainnet/partitions.parquet

  # Show only the schema with explicit nullability
  fireparq inspect s3://bucket/eth-mainnet/partitions.parquet --schema-only

  # Emit machine-readable schema details
  fireparq inspect s3://bucket/eth-mainnet/partitions.parquet --schema-only --json

Lookup order:
  1. Explicit s3://bucket/... URIs are used as-is.
  2. Non-URI paths use the local filesystem when the path exists.
  3. Otherwise, if S3_BUCKET is set, relative paths fall back to s3://<bucket>/<path>.
")]
    Inspect {
        /// Path to a single .parquet file (local path, shorthand key via S3_BUCKET, or s3:// URI)
        #[arg(help_heading = "Selection")]
        path: String,
        /// Only show the schema, including explicit nullability
        #[arg(long, default_value = "false", help_heading = "Display")]
        schema_only: bool,
        /// Emit machine-readable JSON output
        #[arg(long, default_value = "false", help_heading = "Display")]
        json: bool,
        #[command(flatten)]
        aws: AwsArgs,
    },
    /// Delete parquet files from local filesystem or S3, with optional partition filtering.
    ///
    /// Deletes only .parquet files. Never deletes buckets or non-parquet files.
    /// Truncating a network root includes root-level parquet artifacts like
    /// partitions.parquet and cursor.parquet, and --dry-run lists each matched file.
    /// Use --partition to target specific partitions. Nothing is deleted without --yes:
    /// without it, truncate prints a summary of what matched and exits non-zero.
    #[command(after_long_help = "\
Examples:
  # Preview what would be deleted
  fireparq truncate ./output/blocks/ --dry-run

  # Delete all parquet files under a path
  fireparq truncate ./output/blocks/ --yes

  # Delete a single parquet file directly
  fireparq truncate ./output/mainnet/partitions.parquet --yes

  # Delete one day (also matches legacy date=15 directories)
  fireparq truncate ./output/blocks/ -p \"year=2026/month=01/day=15\" --yes

  # Delete one day in every table of a network root
  fireparq truncate ./output/mainnet/ -p \"year=2026/month=01/day=15\" --yes

  # Delete January 2026 on S3 (filters on different keys must all match)
  fireparq truncate s3://bucket/eth-mainnet/blocks/ -p year=2026 -p month=01 --yes

  # Delete two days (filters on the same key match either value)
  fireparq truncate ./output/blocks/ -p year=2026/month=01/day=01 -p year=2026/month=01/day=02 --yes

  # Delete all minute-level partitions (key-only filter)
  fireparq truncate ./output/blocks/ -p minute --yes

Partition filters:
  key=value       matches files under a directory with that segment, e.g. month=01
                  (every January of every year, unless combined with -p year=...)
  key             matches every value of the key, e.g. minute
  a/b/c           a partition path, e.g. year=2026/month=01/day=15: matches files whose
                  partition directories start with exactly these segments
  Each segment may contain one * glob (day=0*). day= also matches legacy date= directories.
  Filters on different keys must all match; filters on the same key (and path filters)
  match if any of them does.

The path must exist locally or be an explicit s3://bucket/... URI. Unlike scan and
inspect, truncate never falls back to s3://$S3_BUCKET/<path> for a missing local path.
")]
    Truncate {
        /// Path to a .parquet file or a directory containing .parquet files (existing local path or s3:// URI)
        #[arg(help_heading = "Selection")]
        path: String,
        /// Partition filter(s) — only delete files matching these partitions. A key=value
        /// segment with an optional glob (e.g. "month=01", "day=0*"), a key name for all its
        /// values (e.g. "minute"), or a partition path (e.g. "year=2026/month=01/day=15").
        /// Filters on different keys must all match; filters on the same key match either.
        /// `day` also matches the legacy `date=DD` day directories. Repeatable.
        #[arg(long, short = 'p', help_heading = "Selection")]
        partition: Vec<String>,
        /// Show what would be deleted without actually deleting
        #[arg(long, default_value = "false", help_heading = "Execution")]
        dry_run: bool,
        /// Delete the matched files. Without --yes, truncate prints a summary and exits non-zero
        #[arg(long, short = 'y', default_value = "false", help_heading = "Execution")]
        yes: bool,
        #[command(flatten)]
        aws: AwsArgs,
    },
}

/// Subcommands under `fireparq partitions`.
#[derive(clap::Subcommand, Debug)]
pub enum PartitionsCommands {
    /// Build `partitions.parquet` directly from Firehose block timestamps.
    /// Time spans traverse exact finalized ancestry; clipped spans stay incomplete.
    #[command(after_long_help = "\
Examples:
  # Build a local date index for one chain
  fireparq partitions build \\
    --network mainnet \\
    --stop-block 10010000 \\
    --partition date \\
    --output ./output

  # Build to S3 with JSON output
  fireparq partitions build \\
    --network mainnet \\
    --stop-block 10010000 \\
    --partition hour \\
    --s3-bucket my-bucket \\
    --json

  # Let start block fall back to a sibling cursor or endpoint metadata
  fireparq partitions build \\
    --network mainnet \\
    --stop-block 10010000 \\
    --partition date \\
    --output ./output

  # Continue maintaining the canonical index in live mode
  fireparq partitions build \\
    --network mainnet \\
    --partition date \\
    --output ./output \\
    --live

  # Poll for new finalized blocks every 15s in live mode
  fireparq partitions build \\
    --network mainnet \\
    --partition date \\
    --output ./output \\
    --live \\
    --poll-interval-secs 15

  # Override the default zstd compression
  fireparq partitions build \\
    --network mainnet \\
    --stop-block 10010000 \\
    --partition date \\
    --compression snappy \\
    --output ./output

  # Build block-range partitions for Solana (1M blocks each)
  fireparq partitions build \\
    --network solana-mainnet-beta \\
    --start-block 0 \\
    --stop-block 300000000 \\
    --partition block_range \\
    --block-range-size 1000000 \\
    --output ./output
")]
    Build {
        #[command(flatten)]
        grpc: GrpcArgs,
        /// Firehose gRPC endpoint URL
        #[arg(
            long,
            env = "ENDPOINT",
            hide_env_values = true,
            help_heading = "Connection"
        )]
        endpoint: Option<String>,
        /// Firehose network `chainName`
        #[arg(
            long,
            env = "NETWORK",
            hide_env_values = true,
            help_heading = "Connection"
        )]
        network: Option<String>,
        /// Explicit API key env var for this endpoint (default: provider-scoped credentials)
        #[arg(
            long,
            env = "API_KEY_ENVVAR",
            hide_env_values = true,
            help_heading = "Connection"
        )]
        api_key_envvar: Option<String>,
        /// Explicit bearer token env var for this endpoint (default: provider-scoped credentials)
        #[arg(
            long,
            env = "API_TOKEN_ENVVAR",
            hide_env_values = true,
            help_heading = "Connection"
        )]
        api_token_envvar: Option<String>,
        /// Start block number (inclusive).
        ///
        /// When omitted in bounded mode, falls back to a sibling `cursor.parquet`
        /// if present, then to the endpoint's first streamable block.
        ///
        /// When omitted in `--live` mode, existing `partitions.parquet` rows take
        /// precedence as the restart anchor.
        ///
        /// With `--resume` (or `--live`) and an existing index, the build continues
        /// from the index frontier; an explicit value past that frontier is rejected
        /// because it would leave the blocks in between unindexed.
        ///
        /// Use `--overwrite` to ignore any existing canonical index and rebuild it
        /// from the requested start point instead.
        ///
        /// When `--partition block_range` is used, explicit values must align to
        /// `--block-range-size`.
        #[arg(long, help_heading = "Block Range")]
        start_block: Option<u64>,
        /// Stop block number (exclusive).
        ///
        /// Required for bounded builds and incompatible with `--live`.
        /// When `--partition block_range` is used, explicit values must align to
        /// `--block-range-size`.
        #[arg(long, conflicts_with = "live", help_heading = "Block Range")]
        stop_block: Option<u64>,
        /// Keep extending `partitions.parquet` from its latest covered frontier.
        #[arg(long, default_value = "false", help_heading = "Block Range")]
        live: bool,
        /// Poll interval used by `--live` finalized-head checks while waiting for new blocks.
        #[arg(long, default_value_t = 30, help_heading = "Runtime / Logging")]
        poll_interval_secs: u64,
        /// Partition to build: date, hour, minute, second, or block_range
        #[arg(long = "partition", help_heading = "Partitioning")]
        partition: String,
        /// Block range size (required when --partition block_range).
        /// Natural partitions have this width; inferred/live edge spans can be clipped.
        #[arg(
            long,
            help_heading = "Partitioning",
            value_parser = clap::value_parser!(u64).range(1..)
        )]
        block_range_size: Option<u64>,
        /// Compression codec for the written `partitions.parquet`: zstd (level 3), zstd:<level>, snappy, gzip, none
        #[arg(long, default_value = "zstd", help_heading = "Output")]
        compression: String,
        /// Output root path (local directory or s3:// URI prefix).
        ///
        /// When omitted, `--s3-bucket` or `S3_BUCKET` is required and the
        /// output root becomes `s3://<bucket>`.
        #[arg(long, help_heading = "Output")]
        output: Option<String>,
        /// S3 bucket name used when `--output` is omitted or should be prefixed.
        #[arg(
            long,
            env = "S3_BUCKET",
            hide_env_values = true,
            help_heading = "Output"
        )]
        s3_bucket: Option<String>,
        /// Resume from an existing canonical index under the resolved output path,
        /// continuing from its stored frontier. Bounded builds require `--resume` or
        /// `--overwrite` when an index already exists.
        #[arg(
            long,
            default_value = "false",
            conflicts_with = "overwrite",
            help_heading = "Output"
        )]
        resume: bool,
        /// Ignore and replace any existing canonical index instead of reading it
        #[arg(
            long,
            default_value = "false",
            conflicts_with = "resume",
            help_heading = "Output"
        )]
        overwrite: bool,
        /// Emit machine-readable JSON output
        #[arg(long, default_value = "false", help_heading = "Runtime / Logging")]
        json: bool,
        #[command(flatten)]
        aws: AwsArgs,
    },
    /// Validate continuity and invariants in `partitions.parquet`.
    #[command(after_long_help = "\
Examples:
  # Validate all rows in a local index
  fireparq partitions validate \\
    --partitions-index ./output/eth-mainnet/partitions.parquet

  # Validate one chain/type and allow gaps
  fireparq partitions validate \\
    --partitions-index s3://my-bucket/partitions.parquet \\
    --partition-type date \\
    --partition-chain eth-mainnet \\
    --allow-gaps \\
    --json
")]
    Validate {
        /// Path to partitions index parquet file (local path, shorthand S3 key via S3_BUCKET, or s3:// URI)
        #[arg(long)]
        partitions_index: String,
        /// Optional partition type filter (e.g. hour, date)
        #[arg(long)]
        partition_type: Option<String>,
        /// Optional chain filter (matches the index chain scope)
        #[arg(long)]
        partition_chain: Option<String>,
        /// Allow gaps between adjacent partitions in the same chain/type
        #[arg(long, default_value = "false")]
        allow_gaps: bool,
        /// Emit machine-readable JSON output
        #[arg(long, default_value = "false")]
        json: bool,
        #[command(flatten)]
        aws: AwsArgs,
    },
    /// Deterministically assign partitions to one shard.
    #[command(after_long_help = "\
Examples:
  # Select shard 1 of 4 using ordinal assignment
  fireparq partitions shard \\
    --partitions-index ./output/eth-mainnet/partitions.parquet \\
    --partition-type hour \\
    --shard-count 4 \\
    --shard-index 1

  # Select shard 0 of 8 using hash assignment and emit JSON
  fireparq partitions shard \\
    --partitions-index s3://my-bucket/eth-mainnet/partitions.parquet \\
    --partition-type date \\
    --partition-chain eth-mainnet \\
    --from '2015-07-29 00:00:00' \\
    --to '2015-07-31 00:00:00' \\
    --shard-count 8 \\
    --shard-index 0 \\
    --strategy hash \\
    --json
")]
    Shard {
        /// Path to partitions index parquet file (local path, shorthand S3 key via S3_BUCKET, or s3:// URI)
        #[arg(long)]
        partitions_index: String,
        /// Optional partition type filter (e.g. hour, date)
        #[arg(long)]
        partition_type: Option<String>,
        /// Optional chain filter (matches the index chain scope)
        #[arg(long)]
        partition_chain: Option<String>,
        /// Lower bound (inclusive) on the partition value (`YYYY-MM-DD HH:MM:SS`, or a start block for block_range)
        #[arg(long)]
        from: Option<String>,
        /// Upper bound (inclusive) on the partition value (`YYYY-MM-DD HH:MM:SS`, or a start block for block_range)
        #[arg(long)]
        to: Option<String>,
        /// Total number of shards
        #[arg(long)]
        shard_count: usize,
        /// Zero-based shard index to select
        #[arg(long)]
        shard_index: usize,
        /// Assignment strategy: ordinal or hash
        #[arg(long, default_value = "ordinal")]
        strategy: String,
        /// Emit machine-readable JSON output
        #[arg(long, default_value = "false")]
        json: bool,
        #[command(flatten)]
        aws: AwsArgs,
    },
    /// List/query partition rows from `partitions.parquet`.
    #[command(after_long_help = "\
Examples:
  # List hour partitions from local index
  fireparq partitions ls \\
    --partitions-index ./output/eth-mainnet/partitions.parquet \\
    --partition-type hour

  # Filter by chain + time window and emit JSON
  fireparq partitions ls \\
    --partitions-index s3://my-bucket/eth-mainnet/partitions.parquet \\
    --partition-type date \\
    --partition-chain eth-mainnet \\
    --from '2015-07-29 00:00:00' \\
    --to '2015-07-31 00:00:00' \\
    --limit 200 \\
    --json
")]
    Ls {
        /// Path to partitions index parquet file (local path, shorthand S3 key via S3_BUCKET, or s3:// URI)
        #[arg(long)]
        partitions_index: String,
        /// Optional partition type filter (e.g. hour, date)
        #[arg(long)]
        partition_type: Option<String>,
        /// Optional chain filter (matches the index chain scope)
        #[arg(long)]
        partition_chain: Option<String>,
        /// Lower bound (inclusive) on the partition value (`YYYY-MM-DD HH:MM:SS`, or a start block for block_range)
        #[arg(long)]
        from: Option<String>,
        /// Upper bound (inclusive) on the partition value (`YYYY-MM-DD HH:MM:SS`, or a start block for block_range)
        #[arg(long)]
        to: Option<String>,
        /// Maximum rows to return (sorted ascending by numeric partition value)
        #[arg(long, default_value = "100")]
        limit: usize,
        /// Emit machine-readable JSON output
        #[arg(long, default_value = "false")]
        json: bool,
        #[command(flatten)]
        aws: AwsArgs,
    },
    /// Resolve one complete span within declared finalized coverage.
    #[command(after_long_help = "\
Examples:
  # Resolve from local index
  fireparq partitions resolve \\
    --partitions-index ./output/eth-mainnet/partitions.parquet \\
    --partition-type hour \\
    --partition-value '2015-07-30 15:00:00' \\
    --partition-chain eth-mainnet

  # Resolve from S3 index and emit JSON
  fireparq partitions resolve \\
    --partitions-index s3://my-bucket/eth-mainnet/partitions.parquet \\
    --partition-type date \\
    --partition-value '2015-07-30 00:00:00' \\
    --partition-chain eth-mainnet \\
    --json
")]
    Resolve {
        /// Path to partitions index parquet file (local path, shorthand S3 key via S3_BUCKET, or s3:// URI)
        #[arg(long)]
        partitions_index: String,
        /// Partition type to resolve (e.g. hour, date)
        #[arg(long)]
        partition_type: String,
        /// Partition value to resolve (e.g. "2015-07-30 15:00:00", or a start block for block_range)
        #[arg(long)]
        partition_value: String,
        /// Optional chain filter (matches the index chain scope)
        #[arg(long)]
        partition_chain: Option<String>,
        /// Require that the partition resolves to exactly one chain when `--partition-chain` is omitted
        #[arg(long, default_value = "false")]
        strict_single_chain: bool,
        /// Return every matching complete span in source order, with finalized coverage.
        #[arg(long, requires = "json", default_value = "false")]
        all_spans: bool,
        /// Emit machine-readable JSON output
        #[arg(long, default_value = "false")]
        json: bool,
        #[command(flatten)]
        aws: AwsArgs,
    },
}

pub use crate::s3::AwsConfig;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "cli/validate_tests.rs"]
mod validate_tests;
