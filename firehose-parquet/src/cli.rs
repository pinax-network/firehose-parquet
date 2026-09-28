//! Command-line declarations with stable re-exports of command operations.
mod configuration;
mod inspect;
mod paths;
mod validate;
// Keep the established crate::cli API while implementations live in focused modules.
// Restricted helpers stay visible only inside cli and are not made public by re-export.
pub use configuration::*;
pub use inspect::*;
pub use paths::*;
pub use validate::*;

use crate::config::{Compression, Config};
use crate::networks::KNOWN_NETWORK_NAMES;
use clap::builder::PossibleValuesParser;
use clap::Args;
use clap_complete::{generate, Shell};
use std::io;
use std::path::{Component, Path, PathBuf};

pub use crate::config::DEFAULT_FLUSH_BYTES;
use crate::config::{
    DEFAULT_FLUSH_ENCODE_CONCURRENCY, DEFAULT_FLUSH_INFLIGHT_BYTES, DEFAULT_FLUSH_MEMORY_BYTES,
    DEFAULT_FLUSH_PUBLISH_CONCURRENCY, DEFAULT_GRPC_MAX_MESSAGE_BYTES, DEFAULT_GRPC_WINDOW_BYTES,
};

/// Transport options of the Firehose client used by `build`.
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
    /// When omitted, an existing output resumes from its authoritative state
    /// under `.fireparq-ingest/` (never from the optional
    /// `_fireparq/cursor.parquet` mirror), and a new output starts from the
    /// endpoint's first streamable block.
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

    /// Optional cursor mirror (must end in .parquet), or `none` to disable it.
    ///
    /// `build` always resumes from the output's mandatory authority under
    /// `.fireparq-ingest/`; this file is a derived compatibility copy. A
    /// relative path resolves against the dataset root (`--output`, with any
    /// `{chain}` expanded), so the default is
    /// `<dataset root>/_fireparq/cursor.parquet`; an absolute local path or an
    /// `s3://bucket/key` URI is used as given. The choice is bound when a
    /// dataset is created: later runs must pass the same value, including `none`.
    #[arg(
        short = 'c',
        long,
        env = "CURSOR",
        default_value = crate::artifacts::DEFAULT_CURSOR_MIRROR,
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub cursor: PathBuf,

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

    /// Dataset root: a directory, or an explicit s3://bucket/prefix URI for S3 output.
    ///
    /// Used exactly as given: the table directories, `_fireparq/` and the
    /// `.fireparq-ingest/` state sit directly in it. `{chain}` expands to the
    /// endpoint's chain_name anywhere in the path or S3 key prefix (not in the
    /// bucket name), for example `s3://datasets/{chain}`; write `{{` and `}}`
    /// for literal braces. The resolved root is bound when a dataset is
    /// created, so later runs must resolve to the same root.
    ///
    /// Every table is written as `<table>/date=YYYY-MM-DD/part-*.parquet`.
    #[arg(
        long,
        env = "OUTPUT",
        default_value = ".",
        hide_env_values = true,
        help_heading = "Output"
    )]
    pub output: PathBuf,

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

    /// Flush mapper state and write Parquet every N seconds while caught up with the chain
    /// head; suspended while the stream catches up, when only the size, row and block
    /// triggers flush (0 or unset disables)
    #[arg(
        long,
        env = "FLUSH_INTERVAL_SECS",
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_interval_secs: Option<u64>,

    /// Parquet encoders running at once within one flush (1-64; each also holds
    /// its table's encoder working memory)
    #[arg(
        long,
        env = "FLUSH_ENCODE_CONCURRENCY",
        default_value_t = DEFAULT_FLUSH_ENCODE_CONCURRENCY,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=64),
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_encode_concurrency: usize,

    /// Table parts published at once within one flush (1-64); for local output
    /// also the threads that stage, publish and verify files. Each part still
    /// publishes only after its receipt is journaled
    #[arg(
        long,
        env = "FLUSH_PUBLISH_CONCURRENCY",
        default_value_t = DEFAULT_FLUSH_PUBLISH_CONCURRENCY,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=64),
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_publish_concurrency: usize,

    /// Budget for encoded parts in flight within one flush (memory, or private
    /// disk spool for S3 output; at least 1). One larger part runs alone
    #[arg(
        long,
        env = "FLUSH_INFLIGHT_BYTES",
        default_value_t = DEFAULT_FLUSH_INFLIGHT_BYTES,
        value_parser = clap::value_parser!(u64).range(1..),
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_inflight_bytes: u64,

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

    /// Optional check that an explicit s3:// output uses this bucket; never expands a relative OUTPUT (then rejected)
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
/// datasets, one `date=YYYY-MM-DD` partition per UTC day in every table.
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

  # Resume: rerun the same command. Progress comes from the output's
  # .fireparq-ingest/ state; _fireparq/cursor.parquet is only an optional mirror
  fireparq build --network mainnet

  # Create an output without the _fireparq/cursor.parquet mirror (bound at creation)
  fireparq build --network mainnet --cursor none

  # One bucket per network: --output is the dataset root, here the bucket root
  fireparq build --network mainnet \\
    --output s3://ethereum-mainnet

  # Several networks in one bucket: {chain} expands to the endpoint's
  # chain_name, here s3://datasets/v1/mainnet (bound at creation)
  fireparq build --network mainnet \\
    --output 's3://datasets/v1/{chain}'
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

    /// Only with --dry-run: ignore legacy cursor defaults or an unreadable cursor.
    ///
    /// A real `build` rejects this flag, even at a new output root: protected
    /// output never rewinds or resets. Omit it to resume from authority, or use
    /// a new empty output root (and absent mirror) to change the original range
    /// or mapper semantics.
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
    /// This is the primary ingestion workflow. Writes every table as
    /// `<table>/date=YYYY-MM-DD/part-*.parquet`. Supports live mode and S3 output. A rerun
    /// resumes from the output's authoritative state under `.fireparq-ingest/`;
    /// `_fireparq/cursor.parquet` is only an optional mirror (`--cursor none`
    /// disables it).
    Build(BuildArgs),
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
  S3_BUCKET=my-bucket fireparq scan eth-mainnet/_fireparq/cursor.parquet

  # Scan a single S3 parquet file
  fireparq scan s3://bucket/eth-mainnet/_fireparq/cursor.parquet

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
  S3_BUCKET=my-bucket fireparq inspect eth-mainnet/_fireparq/cursor.parquet

  # Show only the schema with explicit nullability
  fireparq inspect s3://bucket/eth-mainnet/_fireparq/cursor.parquet --schema-only

  # Emit machine-readable schema details
  fireparq inspect s3://bucket/eth-mainnet/_fireparq/cursor.parquet --schema-only --json

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
