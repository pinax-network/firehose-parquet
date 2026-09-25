use crate::config::{Compression, Config, Partition};
use crate::networks::KNOWN_NETWORK_NAMES;
use clap::builder::PossibleValuesParser;
use clap::Args;
use clap_complete::{generate, Shell};
use std::io;
use std::path::{Component, Path, PathBuf};

/// Default max unresolved timestamp-backfill buffer size in bytes.
pub const DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES: u64 = 134_217_728;
/// Shared 32 MiB default flush target for build/merge byte-based flushing.
pub const DEFAULT_FLUSH_BYTES: u64 = 33_554_432;

/// Load environment variables from `.env` file (if present).
///
/// Call this **before** [`clap::Parser::parse`] so that `env` attributes
/// on CLI arguments pick up the values.
pub fn load_dotenv() {
    dotenvy::dotenv().ok();
}

/// Run a future to completion, working both inside and outside of a tokio runtime.
///
/// When called from within `#[tokio::main]` (or any active runtime), uses
/// `block_in_place` + the current runtime handle.  When no runtime is active,
/// spins up a lightweight current-thread runtime.
pub fn block_on_async<F: std::future::Future>(f: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(f)),
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");
            rt.block_on(f)
        }
    }
}

// Shared CLI arguments for all fireparq binaries.
//
// Embed in a per-chain `#[derive(Parser)]` struct with `#[command(flatten)]`.
#[derive(Args, Debug, Clone)]
pub struct CommonArgs {
    /// Firehose gRPC endpoint URL
    #[arg(
        short = 'e',
        long,
        env = "ENDPOINT",
        hide_env_values = true,
        help_heading = "Connection"
    )]
    pub endpoint: Option<String>,

    /// Name of environment variable containing the API key for authentication
    #[arg(
        long,
        env = "API_KEY_ENVVAR",
        default_value = "SUBSTREAMS_API_KEY",
        hide_env_values = true,
        help_heading = "Connection"
    )]
    pub api_key_envvar: String,

    /// Name of environment variable containing the JWT bearer token for authentication
    #[arg(
        long,
        env = "API_TOKEN_ENVVAR",
        default_value = "SUBSTREAMS_API_TOKEN",
        hide_env_values = true,
        help_heading = "Connection"
    )]
    pub api_token_envvar: String,

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

    /// Only process finalized blocks (when false, adds fork_step column)
    #[arg(
        long,
        env = "FINAL_BLOCKS_ONLY",
        default_value = "true",
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

    /// Compression codec: zstd, snappy, gzip, none
    #[arg(
        long,
        env = "COMPRESSION",
        default_value = "zstd",
        hide_env_values = true,
        help_heading = "Output"
    )]
    pub compression: String,

    /// Flush mapper state after this many rows; does not guarantee parquet files are materialized (disabled by default)
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

    /// Flush mapper state at this many in-memory bytes and target roughly this many compressed bytes per parquet file (0 disables byte-based flushing)
    #[arg(
        long,
        env = "FLUSH_BYTES",
        default_value_t = DEFAULT_FLUSH_BYTES,
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_bytes: u64,

    /// Flush mapper state every N seconds; does not guarantee parquet files are materialized (disabled by default)
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

    /// AWS access key ID (for S3 output)
    #[arg(
        long,
        env = "AWS_ACCESS_KEY_ID",
        hide_env_values = true,
        help_heading = "AWS / S3"
    )]
    pub aws_access_key_id: Option<String>,

    /// AWS secret access key (for S3 output)
    #[arg(
        long,
        env = "AWS_SECRET_ACCESS_KEY",
        hide_env_values = true,
        help_heading = "AWS / S3"
    )]
    pub aws_secret_access_key: Option<String>,

    /// AWS session token (for S3 output)
    #[arg(
        long,
        env = "AWS_SESSION_TOKEN",
        hide_env_values = true,
        help_heading = "AWS / S3"
    )]
    pub aws_session_token: Option<String>,

    /// AWS region (for S3 output)
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

    /// S3 bucket name (when set, output is written to s3://<bucket>/<output>)
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

    /// Override cursor parameter validation and restart from the current CLI
    /// range. When a cursor file exists and its stored parameters differ from
    /// the current CLI arguments, the pipeline normally exits with an error.
    /// This flag suppresses that check and ignores the stored resume position
    /// for start/stop/mode resolution.
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
        /// AWS access key ID (for S3 paths)
        #[arg(
            long,
            env = "AWS_ACCESS_KEY_ID",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(
            long,
            env = "AWS_SECRET_ACCESS_KEY",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(
            long,
            env = "AWS_SESSION_TOKEN",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(
            long,
            env = "AWS_REGION",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(
            long,
            env = "AWS_ENDPOINT_URL_S3",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_endpoint_url: Option<String>,
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
        /// AWS access key ID (for S3 paths)
        #[arg(
            long,
            env = "AWS_ACCESS_KEY_ID",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(
            long,
            env = "AWS_SECRET_ACCESS_KEY",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(
            long,
            env = "AWS_SESSION_TOKEN",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(
            long,
            env = "AWS_REGION",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(
            long,
            env = "AWS_ENDPOINT_URL_S3",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_endpoint_url: Option<String>,
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
        /// AWS access key ID (for S3 paths)
        #[arg(
            long,
            env = "AWS_ACCESS_KEY_ID",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(
            long,
            env = "AWS_SECRET_ACCESS_KEY",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(
            long,
            env = "AWS_SESSION_TOKEN",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(
            long,
            env = "AWS_REGION",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(
            long,
            env = "AWS_ENDPOINT_URL_S3",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_endpoint_url: Option<String>,
    },
    /// Roll up fine-grained partitioned Parquet files into coarser intervals.
    ///
    /// Reads minute/hour-partitioned files and merges them into hourly or daily
    /// partitions, respecting --flush-bytes for file size limits.
    #[command(after_long_help = "\
Examples:
  # Roll up minute partitions into daily, replacing the minute files (in-place)
  fireparq rollup ./output/blocks/ --delete-source

  # Roll up to hourly partitions with a separate output, keeping the source files
  fireparq rollup ./output/blocks/ -o ./merged/ -p hour

  # Roll up S3 data, delete source files after
  fireparq rollup s3://bucket/blocks/ --delete-source

  # Resolve a shorthand S3 source path in-place when no local match exists
  S3_BUCKET=my-bucket fireparq rollup eth-mainnet/blocks/ --delete-source

  # Custom file size limit (256 MB)
  fireparq rollup ./output/blocks/ -o ./daily/blocks/ --flush-bytes 268435456

Only part-*.parquet files below a partition finer than --partition are read.
Files already at the target granularity and root artifacts (cursor.parquet,
partitions.parquet, merkle_roots.parquet, verify_runs/) are left untouched, so
re-running a rollup is safe. Without --delete-source, each re-run replaces the
part-rollup-*.parquet files it wrote earlier in the target partitions it rolls up.

A target partition whose source files have different columns (names, types,
nullability, or order) is left untouched, and rollup exits non-zero.

Lookup order for the source path:
  1. Explicit s3://bucket/... URIs are used as-is.
  2. Non-URI paths use the local filesystem when the path exists.
  3. Otherwise, if S3_BUCKET is set, relative paths fall back to s3://<bucket>/<path>.
")]
    Rollup {
        /// Source path containing partitioned Parquet files (local directory, shorthand S3 key/prefix via S3_BUCKET, or S3 URI)
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
        /// Compression codec: zstd, snappy, gzip, none
        #[arg(long, default_value = "zstd", help_heading = "Output")]
        compression: String,
        /// Max compressed bytes per output file (0 = no limit)
        #[arg(long, default_value = "134217728", help_heading = "Output")]
        flush_bytes: u64,
        /// Delete each source file once its target partition is written (required for in-place rollup)
        #[arg(long, default_value = "false", help_heading = "Execution")]
        delete_source: bool,
        /// AWS access key ID (for S3 paths)
        #[arg(
            long,
            env = "AWS_ACCESS_KEY_ID",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(
            long,
            env = "AWS_SECRET_ACCESS_KEY",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(
            long,
            env = "AWS_SESSION_TOKEN",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(
            long,
            env = "AWS_REGION",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(
            long,
            env = "AWS_ENDPOINT_URL_S3",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_endpoint_url: Option<String>,
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

  # Resolve a shorthand S3 path when no local match exists
  S3_BUCKET=my-bucket fireparq merge eth-mainnet/blocks/

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

Lookup order:
  1. Explicit s3://bucket/... URIs are used as-is.
  2. Non-URI paths use the local filesystem when the path exists.
  3. Otherwise, if S3_BUCKET is set, relative paths fall back to s3://<bucket>/<path>.
")]
    Merge {
        /// Path to a directory of partitioned .parquet files, a shorthand S3 key/prefix via S3_BUCKET, or an S3 URI
        #[arg(help_heading = "Selection")]
        path: String,
        /// Compression codec: zstd, snappy, gzip, none
        #[arg(long, default_value = "zstd", help_heading = "Output")]
        compression: String,
        /// Flush merged output after this many rows (disabled by default)
        #[arg(long, help_heading = "Flush")]
        flush_rows: Option<u32>,
        /// Flush merged output at this many in-memory bytes and target roughly this many compressed bytes per parquet file
        #[arg(long, default_value_t = DEFAULT_FLUSH_BYTES, help_heading = "Flush")]
        flush_bytes: u64,
        /// Show what would be merged without writing
        #[arg(long, default_value = "false", help_heading = "Execution")]
        dry_run: bool,
        /// AWS access key ID (for S3 paths)
        #[arg(
            long,
            env = "AWS_ACCESS_KEY_ID",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(
            long,
            env = "AWS_SECRET_ACCESS_KEY",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(
            long,
            env = "AWS_SESSION_TOKEN",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(
            long,
            env = "AWS_REGION",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(
            long,
            env = "AWS_ENDPOINT_URL_S3",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_endpoint_url: Option<String>,
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
        /// AWS access key ID (for S3 paths)
        #[arg(
            long,
            env = "AWS_ACCESS_KEY_ID",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(
            long,
            env = "AWS_SECRET_ACCESS_KEY",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(
            long,
            env = "AWS_SESSION_TOKEN",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(
            long,
            env = "AWS_REGION",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(
            long,
            env = "AWS_ENDPOINT_URL_S3",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_endpoint_url: Option<String>,
    },
    /// Delete parquet files from local filesystem or S3, with optional partition filtering.
    ///
    /// Deletes only .parquet files. Never deletes buckets or non-parquet files.
    /// Truncating a network root includes root-level parquet artifacts like
    /// partitions.parquet and cursor.parquet, and --dry-run lists each matched file.
    /// Use --partition to target specific partitions (supports glob patterns).
    #[command(after_long_help = "\
Examples:
  # Delete all parquet files under a path
  fireparq truncate ./output/blocks/

  # Delete all parquet files under a network root, including root-level artifacts
  fireparq truncate ./output/mainnet/ --dry-run

  # Delete a single parquet file directly
  fireparq truncate ./output/mainnet/partitions.parquet

  # Delete only day-of-month 01 partitions (also matches legacy date=01 directories)
  fireparq truncate ./output/blocks/ -p \"day=01\"

  # Delete with glob pattern (all of January)
  fireparq truncate s3://bucket/blocks/ -p \"month=01\"

  # Resolve a shorthand S3 path when no local match exists
  S3_BUCKET=my-bucket fireparq truncate eth-mainnet/blocks/ -p \"month=01\"

  # Delete a specific year
  fireparq truncate ./output/ -p \"year=2026\"

  # Delete all minute-level partitions (key-only filter)
  fireparq truncate ./output/blocks/ -p minute

  # Preview what would be deleted
  fireparq truncate ./output/blocks/ --dry-run

Lookup order:
  1. Explicit s3://bucket/... URIs are used as-is.
  2. Non-URI paths use the local filesystem when the path exists.
  3. Otherwise, if S3_BUCKET is set, relative paths fall back to s3://<bucket>/<path>.
")]
    Truncate {
        /// Path to a .parquet file, a directory containing .parquet files, a shorthand S3 key/prefix via S3_BUCKET, or an S3 URI
        #[arg(help_heading = "Selection")]
        path: String,
        /// Partition filter(s) — only delete files matching these partition segments.
        /// Use a key name to match all values (e.g. "minute" matches all minute=* partitions),
        /// or a key=value with optional glob (e.g. "day=0*"). `day` also matches the legacy
        /// `date=DD` day directories written by earlier releases. Repeatable.
        #[arg(long, short = 'p', help_heading = "Selection")]
        partition: Vec<String>,
        /// Show what would be deleted without actually deleting
        #[arg(long, default_value = "false", help_heading = "Execution")]
        dry_run: bool,
        /// AWS access key ID (for S3 paths)
        #[arg(
            long,
            env = "AWS_ACCESS_KEY_ID",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(
            long,
            env = "AWS_SECRET_ACCESS_KEY",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(
            long,
            env = "AWS_SESSION_TOKEN",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(
            long,
            env = "AWS_REGION",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(
            long,
            env = "AWS_ENDPOINT_URL_S3",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_endpoint_url: Option<String>,
    },
}

/// Subcommands under `fireparq partitions`.
#[derive(clap::Subcommand, Debug)]
pub enum PartitionsCommands {
    /// Build `partitions.parquet` directly from Firehose block timestamps.
    /// Missing blocks are skipped automatically after probe retries are exhausted.
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
        /// Name of environment variable containing the API key for authentication
        #[arg(
            long,
            env = "API_KEY_ENVVAR",
            default_value = "SUBSTREAMS_API_KEY",
            hide_env_values = true,
            help_heading = "Connection"
        )]
        api_key_envvar: String,
        /// Name of environment variable containing the JWT bearer token for authentication
        #[arg(
            long,
            env = "API_TOKEN_ENVVAR",
            default_value = "SUBSTREAMS_API_TOKEN",
            hide_env_values = true,
            help_heading = "Connection"
        )]
        api_token_envvar: String,
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
        /// Poll interval used by `--live` sparse probes while waiting for new blocks.
        #[arg(long, default_value_t = 30, help_heading = "Runtime / Logging")]
        poll_interval_secs: u64,
        /// Partition to build: date, hour, minute, second, or block_range
        #[arg(long = "partition", help_heading = "Partitioning")]
        partition: String,
        /// Block range size (required when --partition block_range).
        /// Each partition covers exactly this many blocks (e.g. 1000000).
        #[arg(
            long,
            help_heading = "Partitioning",
            value_parser = clap::value_parser!(u64).range(1..)
        )]
        block_range_size: Option<u64>,
        /// Compression codec for the written `partitions.parquet`: zstd, snappy, gzip, none
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
        /// AWS access key ID (for S3 paths)
        #[arg(
            long,
            env = "AWS_ACCESS_KEY_ID",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(
            long,
            env = "AWS_SECRET_ACCESS_KEY",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(
            long,
            env = "AWS_SESSION_TOKEN",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(
            long,
            env = "AWS_REGION",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(
            long,
            env = "AWS_ENDPOINT_URL_S3",
            hide_env_values = true,
            help_heading = "AWS / S3"
        )]
        aws_endpoint_url: Option<String>,
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
        /// Optional chain filter (matches `chain` column)
        #[arg(long)]
        partition_chain: Option<String>,
        /// Allow gaps between adjacent partitions in the same chain/type
        #[arg(long, default_value = "false")]
        allow_gaps: bool,
        /// Emit machine-readable JSON output
        #[arg(long, default_value = "false")]
        json: bool,
        /// AWS access key ID (for S3 paths)
        #[arg(long, env = "AWS_ACCESS_KEY_ID", hide_env_values = true)]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(long, env = "AWS_SECRET_ACCESS_KEY", hide_env_values = true)]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(long, env = "AWS_SESSION_TOKEN", hide_env_values = true)]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(long, env = "AWS_REGION", hide_env_values = true)]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(long, env = "AWS_ENDPOINT_URL_S3", hide_env_values = true)]
        aws_endpoint_url: Option<String>,
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
        /// Optional chain filter (matches `chain` column)
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
        /// AWS access key ID (for S3 paths)
        #[arg(long, env = "AWS_ACCESS_KEY_ID", hide_env_values = true)]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(long, env = "AWS_SECRET_ACCESS_KEY", hide_env_values = true)]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(long, env = "AWS_SESSION_TOKEN", hide_env_values = true)]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(long, env = "AWS_REGION", hide_env_values = true)]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(long, env = "AWS_ENDPOINT_URL_S3", hide_env_values = true)]
        aws_endpoint_url: Option<String>,
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
        /// Optional chain filter (matches `chain` column)
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
        /// AWS access key ID (for S3 paths)
        #[arg(long, env = "AWS_ACCESS_KEY_ID", hide_env_values = true)]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(long, env = "AWS_SECRET_ACCESS_KEY", hide_env_values = true)]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(long, env = "AWS_SESSION_TOKEN", hide_env_values = true)]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(long, env = "AWS_REGION", hide_env_values = true)]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(long, env = "AWS_ENDPOINT_URL_S3", hide_env_values = true)]
        aws_endpoint_url: Option<String>,
    },
    /// Resolve exact [start_block, stop_block) for one partition.
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
        /// Optional chain filter (matches `chain` column)
        #[arg(long)]
        partition_chain: Option<String>,
        /// Require that the partition resolves to exactly one chain when `--partition-chain` is omitted
        #[arg(long, default_value = "false")]
        strict_single_chain: bool,
        /// Emit machine-readable JSON output
        #[arg(long, default_value = "false")]
        json: bool,
        /// AWS access key ID (for S3 paths)
        #[arg(long, env = "AWS_ACCESS_KEY_ID", hide_env_values = true)]
        aws_access_key_id: Option<String>,
        /// AWS secret access key (for S3 paths)
        #[arg(long, env = "AWS_SECRET_ACCESS_KEY", hide_env_values = true)]
        aws_secret_access_key: Option<String>,
        /// AWS session token (for S3 paths)
        #[arg(long, env = "AWS_SESSION_TOKEN", hide_env_values = true)]
        aws_session_token: Option<String>,
        /// AWS region (for S3 paths)
        #[arg(long, env = "AWS_REGION", hide_env_values = true)]
        aws_region: Option<String>,
        /// AWS endpoint URL (for S3-compatible services)
        #[arg(long, env = "AWS_ENDPOINT_URL_S3", hide_env_values = true)]
        aws_endpoint_url: Option<String>,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PartitionResolveResult {
    pub partitions_index: String,
    pub partition_type: String,
    pub partition_value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_chain: Option<String>,
    pub start_block: u64,
    pub stop_block: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionResolveOptions {
    pub strict_single_chain: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionBuildType {
    Date,
    Hour,
    Minute,
    Second,
    BlockRange,
}

impl PartitionBuildType {
    pub fn from_cli_value(value: &str) -> anyhow::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "day" | "date" => Ok(Self::Date),
            "hour" => Ok(Self::Hour),
            "minute" => Ok(Self::Minute),
            "second" => Ok(Self::Second),
            "block_range" | "block-range" | "blocks" => Ok(Self::BlockRange),
            other => anyhow::bail!(
                "invalid partition type '{other}': expected one of date, hour, minute, second, block_range"
            ),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
            Self::BlockRange => "block_range",
        }
    }

    /// Returns true if this is a time-based partition type.
    pub fn is_time_based(&self) -> bool {
        !matches!(self, Self::BlockRange)
    }

    /// For time-based types, returns the interval in seconds.
    /// For block_range, returns 0 (use `block_range_size` instead).
    pub fn interval_seconds(&self) -> i64 {
        match self {
            Self::Date => 86_400,
            Self::Hour => 3_600,
            Self::Minute => 60,
            Self::Second => 1,
            Self::BlockRange => 0,
        }
    }

    pub fn round_timestamp(&self, timestamp: i64) -> anyhow::Result<i64> {
        use time::OffsetDateTime;

        if matches!(self, Self::BlockRange) {
            anyhow::bail!("round_timestamp is not applicable for block_range partitions");
        }

        let dt = OffsetDateTime::from_unix_timestamp(timestamp)
            .map_err(|e| anyhow::anyhow!("invalid unix timestamp {timestamp}: {e}"))?;
        let rounded = match self {
            Self::Date => dt.replace_time(time::Time::MIDNIGHT),
            Self::Hour => dt.replace_minute(0)?.replace_second(0)?,
            Self::Minute => dt.replace_second(0)?,
            Self::Second => dt,
            Self::BlockRange => unreachable!(),
        };
        Ok(rounded.unix_timestamp())
    }
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanOrder {
    Asc,
    Desc,
}

impl std::fmt::Display for PartitionBuildType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

fn canonical_partition_type_label(value: &str) -> anyhow::Result<String> {
    Ok(PartitionBuildType::from_cli_value(value)?.to_string())
}

fn normalize_partition_bounds_request(
    mut request: PartitionBoundsRequest,
) -> anyhow::Result<PartitionBoundsRequest> {
    request.partition_type = canonical_partition_type_label(&request.partition_type)?;
    Ok(request)
}

fn normalize_partition_window_request(
    mut request: PartitionWindowRequest,
) -> anyhow::Result<PartitionWindowRequest> {
    request.partition_type = canonical_partition_type_label(&request.partition_type)?;
    Ok(request)
}

fn normalize_partition_list_request(
    mut request: PartitionListRequest,
) -> anyhow::Result<PartitionListRequest> {
    request.partition_type = request
        .partition_type
        .as_deref()
        .map(canonical_partition_type_label)
        .transpose()?;
    Ok(request)
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionBuildRow {
    pub partition_type: String,
    pub partition_interval_seconds: i64,
    /// For time-based: epoch seconds formatted as "YYYY-MM-DD HH:MM:SS".
    /// For block_range: the start block number formatted as a string.
    pub partition_start_ts: String,
    /// For time-based: same as partition_start_ts.
    /// For block_range: the start block number as a string.
    pub partition_value: String,
    pub start_block: u64,
    pub stop_block: u64,
    /// Nullable — populated best-effort, None when block is missing/has no timestamp.
    pub start_time: Option<String>,
    /// Nullable — populated best-effort, None when block is missing/has no timestamp.
    pub end_time: Option<String>,
    pub chain: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionBuildResult {
    pub partitions_index: String,
    pub chain: String,
    pub partition: String,
    pub row_count: usize,
    pub start_block: u64,
    pub stop_block: u64,
    pub resumed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_from_block: Option<u64>,
}

#[derive(Debug, Clone)]
struct ActivePartitionBuildRow {
    partition_start_ts: i64,
    partition_value: String,
    start_block: u64,
    start_time: Option<i64>,
    last_block: u64,
    last_time: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct PartitionIndexBuilder {
    chain: String,
    partition_types: Vec<PartitionBuildType>,
    active: std::collections::BTreeMap<PartitionBuildType, ActivePartitionBuildRow>,
    rows: Vec<PartitionBuildRow>,
    first_seen_block: Option<u64>,
    last_seen_block: Option<u64>,
    /// Block range size for block_range partition type. Required when partition type is BlockRange.
    block_range_size: Option<u64>,
}

impl PartitionIndexBuilder {
    pub fn new(
        chain: impl Into<String>,
        partition_types: Vec<PartitionBuildType>,
    ) -> anyhow::Result<Self> {
        if partition_types.is_empty() {
            anyhow::bail!("at least one partition type is required");
        }

        Ok(Self {
            chain: chain.into(),
            partition_types,
            active: std::collections::BTreeMap::new(),
            rows: Vec::new(),
            first_seen_block: None,
            last_seen_block: None,
            block_range_size: None,
        })
    }

    pub fn with_block_range_size(mut self, size: u64) -> Self {
        self.block_range_size = Some(size);
        self
    }

    pub fn observe_block(&mut self, block: &crate::traits::BlockIdentity) -> anyhow::Result<()> {
        if let Some(last_seen_block) = self.last_seen_block {
            if block.block_num < last_seen_block {
                anyhow::bail!(
                    "partition build requires non-decreasing block numbers, saw {} after {}",
                    block.block_num,
                    last_seen_block
                );
            }
        }

        self.first_seen_block.get_or_insert(block.block_num);
        self.last_seen_block = Some(block.block_num);

        let timestamp = if block.timestamp != 0 {
            Some(block.timestamp)
        } else {
            None
        };

        for partition_type in self.partition_types.clone() {
            if partition_type == PartitionBuildType::BlockRange {
                // Block-range partitions don't use observe_block — they are built deterministically.
                // But if called, we can accumulate timestamps for best-effort time columns.
                let block_range_size = self.block_range_size.ok_or_else(|| {
                    anyhow::anyhow!("block_range_size is required for block_range partition type")
                })?;
                let partition_start_block = (block.block_num / block_range_size) * block_range_size;
                let partition_start_ts = partition_start_block as i64;
                let partition_value = partition_start_block.to_string();

                match self.active.get_mut(&partition_type) {
                    Some(active) if active.partition_start_ts == partition_start_ts => {
                        active.last_block = block.block_num;
                        active.last_time = timestamp;
                    }
                    Some(active) => {
                        let finalized = build_partition_row(
                            &self.chain,
                            partition_type,
                            active.clone(),
                            block.block_num,
                            self.block_range_size,
                        )?;
                        self.rows.push(finalized);
                        *active = ActivePartitionBuildRow {
                            partition_start_ts,
                            partition_value,
                            start_block: block.block_num,
                            start_time: timestamp,
                            last_block: block.block_num,
                            last_time: timestamp,
                        };
                    }
                    None => {
                        self.active.insert(
                            partition_type,
                            ActivePartitionBuildRow {
                                partition_start_ts,
                                partition_value,
                                start_block: block.block_num,
                                start_time: timestamp,
                                last_block: block.block_num,
                                last_time: timestamp,
                            },
                        );
                    }
                }
                continue;
            }

            // Time-based partition types: missing timestamps are allowed (e.g. Solana).
            let ts = timestamp.unwrap_or(0);
            let partition_start_ts = partition_type.round_timestamp(ts)?;
            let partition_value = format_partition_timestamp(partition_start_ts)?;

            match self.active.get_mut(&partition_type) {
                Some(active) if active.partition_start_ts == partition_start_ts => {
                    active.last_block = block.block_num;
                    active.last_time = timestamp;
                }
                Some(active) => {
                    let finalized = build_partition_row(
                        &self.chain,
                        partition_type,
                        active.clone(),
                        block.block_num,
                        self.block_range_size,
                    )?;
                    self.rows.push(finalized);
                    *active = ActivePartitionBuildRow {
                        partition_start_ts,
                        partition_value,
                        start_block: block.block_num,
                        start_time: timestamp,
                        last_block: block.block_num,
                        last_time: timestamp,
                    };
                }
                None => {
                    self.active.insert(
                        partition_type,
                        ActivePartitionBuildRow {
                            partition_start_ts,
                            partition_value,
                            start_block: block.block_num,
                            start_time: timestamp,
                            last_block: block.block_num,
                            last_time: timestamp,
                        },
                    );
                }
            }
        }

        Ok(())
    }

    pub fn finish(mut self, stop_block: u64) -> anyhow::Result<Vec<PartitionBuildRow>> {
        if self.first_seen_block.is_none() {
            anyhow::bail!("partition build produced no rows because the stream returned no blocks");
        }
        if stop_block == 0 {
            anyhow::bail!("partition build requires a finite non-zero stop block");
        }

        for partition_type in self.partition_types.clone() {
            if let Some(active) = self.active.remove(&partition_type) {
                self.rows.push(build_partition_row(
                    &self.chain,
                    partition_type,
                    active,
                    stop_block,
                    self.block_range_size,
                )?);
            }
        }

        sort_partition_build_rows(self.rows)
    }

    pub fn snapshot(&self, stop_block: u64) -> anyhow::Result<Vec<PartitionBuildRow>> {
        if self.first_seen_block.is_none() {
            anyhow::bail!("partition build produced no rows because the stream returned no blocks");
        }
        if stop_block == 0 {
            anyhow::bail!("partition build snapshot requires a finite non-zero stop block");
        }

        let mut rows = self.rows.clone();
        for partition_type in self.partition_types.clone() {
            if let Some(active) = self.active.get(&partition_type) {
                rows.push(build_partition_row(
                    &self.chain,
                    partition_type,
                    active.clone(),
                    stop_block,
                    self.block_range_size,
                )?);
            }
        }

        sort_partition_build_rows(rows)
    }

    pub fn current_frontier(&self) -> Option<u64> {
        self.last_seen_block.map(|block| block.saturating_add(1))
    }

    pub fn active_partition_value(&self) -> Option<String> {
        self.partition_types.first().and_then(|partition_type| {
            self.active
                .get(partition_type)
                .map(|active| active.partition_value.clone())
        })
    }

    pub fn has_rows(&self) -> bool {
        self.first_seen_block.is_some()
    }

    pub fn finalized_row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn resume_from_existing(
        chain: impl Into<String>,
        partition_types: Vec<PartitionBuildType>,
        mut existing_rows: Vec<PartitionBuildRow>,
    ) -> anyhow::Result<(Self, u64)> {
        if existing_rows.is_empty() {
            anyhow::bail!("cannot resume partition build without existing rows");
        }

        let chain = chain.into();
        let mut active = std::collections::BTreeMap::new();
        let mut retained_rows = Vec::new();
        let mut resume_block = None;

        for partition_type in partition_types.iter().copied() {
            let mut matching = existing_rows
                .iter()
                .enumerate()
                .filter(|(_, row)| row.partition_type == partition_type.as_str())
                .map(|(index, row)| Ok((row.partition_key()?, index, row)))
                .collect::<anyhow::Result<Vec<_>>>()?;
            sort_resume_rows(partition_type, &mut matching);

            let Some((_, last_index, last_row)) = matching.pop() else {
                anyhow::bail!(
                    "cannot resume partition build for chain {}: missing existing rows for partition type {}",
                    chain,
                    partition_type
                );
            };

            let partition_resume_block = if partition_type == PartitionBuildType::BlockRange {
                validate_block_range_resume_rows(&matching, last_row)?
            } else {
                last_row.stop_block
            };
            let common_resume_block = resume_block.get_or_insert(partition_resume_block);
            if partition_resume_block != *common_resume_block {
                anyhow::bail!(
                    "cannot resume partition build: partition type {} ends at {}, expected common frontier {}",
                    partition_type,
                    partition_resume_block,
                    common_resume_block
                );
            }

            let start_time = last_row
                .start_time
                .as_deref()
                .map(parse_partition_timestamp)
                .transpose()?;
            let last_time = last_row
                .end_time
                .as_deref()
                .map(parse_partition_timestamp)
                .transpose()?;

            let partition_start_ts = if partition_type == PartitionBuildType::BlockRange {
                last_row
                    .partition_start_ts
                    .parse::<i64>()
                    .unwrap_or(last_row.start_block as i64)
            } else {
                parse_partition_timestamp(&last_row.partition_start_ts)?
            };

            active.insert(
                partition_type,
                ActivePartitionBuildRow {
                    partition_start_ts,
                    partition_value: last_row.partition_value.clone(),
                    start_block: last_row.start_block,
                    start_time,
                    last_block: last_row.stop_block.saturating_sub(1),
                    last_time,
                },
            );

            existing_rows.remove(last_index);
        }

        retained_rows.extend(existing_rows);
        let resume_block = resume_block
            .ok_or_else(|| anyhow::anyhow!("missing existing stop_block for resume"))?;

        Ok((
            Self {
                chain,
                partition_types,
                active,
                rows: retained_rows,
                first_seen_block: Some(resume_block),
                last_seen_block: resume_block.checked_sub(1),
                block_range_size: None,
            },
            resume_block,
        ))
    }
}

/// Sort `(partition_key, index, row)` resume candidates so the terminal row is last.
fn sort_resume_rows(
    partition_type: PartitionBuildType,
    matching: &mut [(u64, usize, &PartitionBuildRow)],
) {
    matching.sort_by(
        |(left_key, _, left), (right_key, _, right)| match partition_type {
            PartitionBuildType::BlockRange => left
                .start_block
                .cmp(&right.start_block)
                .then_with(|| left.stop_block.cmp(&right.stop_block))
                .then_with(|| left_key.cmp(right_key)),
            _ => left_key
                .cmp(right_key)
                .then_with(|| left.start_block.cmp(&right.start_block))
                .then_with(|| left.stop_block.cmp(&right.stop_block)),
        },
    );
}

fn validate_block_range_resume_rows(
    completed_rows: &[(u64, usize, &PartitionBuildRow)],
    terminal_row: &PartitionBuildRow,
) -> anyhow::Result<u64> {
    let block_range_size = u64::try_from(terminal_row.partition_interval_seconds)
        .ok()
        .filter(|size| *size > 0)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cannot resume partition build: partition type block_range has invalid block range size {}",
                terminal_row.partition_interval_seconds
            )
        })?;

    let mut previous: Option<&PartitionBuildRow> = None;
    for row in completed_rows
        .iter()
        .map(|(_, _, row)| *row)
        .chain(std::iter::once(terminal_row))
    {
        if row.start_block >= row.stop_block {
            anyhow::bail!(
                "cannot resume partition build: partition type block_range has invalid range {}..{}",
                row.start_block,
                row.stop_block
            );
        }
        if row.partition_interval_seconds != block_range_size as i64 {
            anyhow::bail!(
                "cannot resume partition build: partition type block_range mixes block range sizes {} and {}",
                block_range_size,
                row.partition_interval_seconds
            );
        }
        if row.start_block % block_range_size != 0 {
            anyhow::bail!(
                "cannot resume partition build: partition type block_range starts at misaligned block {}, expected multiple of {}",
                row.start_block,
                block_range_size
            );
        }

        let row_size = row.stop_block - row.start_block;
        if row_size > block_range_size {
            anyhow::bail!(
                "cannot resume partition build: partition type block_range row {}..{} exceeds block range size {}",
                row.start_block,
                row.stop_block,
                block_range_size
            );
        }

        if let Some(previous_row) = previous {
            if previous_row.stop_block != row.start_block {
                let relation = if previous_row.stop_block < row.start_block {
                    "gap"
                } else {
                    "overlap"
                };
                anyhow::bail!(
                    "cannot resume partition build: partition type block_range has {} between {} and {}",
                    relation,
                    previous_row.stop_block,
                    row.start_block
                );
            }
            if previous_row.stop_block - previous_row.start_block != block_range_size {
                anyhow::bail!(
                    "cannot resume partition build: partition type block_range row {}..{} is incomplete before the final frontier {}",
                    previous_row.start_block,
                    previous_row.stop_block,
                    terminal_row.stop_block
                );
            }
        }

        previous = Some(row);
    }

    Ok(terminal_row.stop_block)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionListRequest {
    pub index_path: String,
    pub partition_type: Option<String>,
    pub chain: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionShardStrategy {
    Ordinal,
    Hash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionShardRequest {
    pub list: PartitionListRequest,
    pub shard_count: usize,
    pub shard_index: usize,
    pub strategy: PartitionShardStrategy,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionListRow {
    pub partition_type: String,
    pub partition_value: String,
    pub partition_start_ts: String,
    pub start_block: u64,
    pub stop_block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionListResult {
    pub partitions_index: String,
    pub limit: usize,
    pub total_matches: usize,
    pub returned_rows: usize,
    pub rows: Vec<PartitionListRow>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionShardResult {
    pub partitions_index: String,
    pub shard_count: usize,
    pub shard_index: usize,
    pub strategy: PartitionShardStrategy,
    pub total_matches: usize,
    pub returned_rows: usize,
    pub rows: Vec<PartitionListRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionValidateRequest {
    pub list: PartitionListRequest,
    pub allow_gaps: bool,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionValidationIssueKind {
    InvalidRange,
    Gap,
    Overlap,
    OutOfOrder,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionValidationIssue {
    pub kind: PartitionValidationIssueKind,
    pub partition_type: String,
    pub partition_value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct PartitionValidateResult {
    pub partitions_index: String,
    pub total_rows: usize,
    pub issue_count: usize,
    pub valid: bool,
    pub issues: Vec<PartitionValidationIssue>,
}

pub fn parse_partition_build_types(spec: &str) -> anyhow::Result<Vec<PartitionBuildType>> {
    let value = spec.trim();
    if value.is_empty() {
        anyhow::bail!("--partition is required");
    }
    if value.contains(',') {
        anyhow::bail!(
            "--partition accepts exactly one value per run; got `{}`",
            spec.trim()
        );
    }

    Ok(vec![PartitionBuildType::from_cli_value(value)?])
}

pub fn resolve_s3_output_root(
    output: Option<&str>,
    s3_bucket: Option<&str>,
) -> anyhow::Result<String> {
    match (
        output.map(str::trim).filter(|value| !value.is_empty()),
        s3_bucket.map(str::trim).filter(|value| !value.is_empty()),
    ) {
        (Some(output), Some(bucket))
            if !output.starts_with("s3://") && !is_explicit_local_output_path(output) =>
        {
            if output == "." {
                return Ok(format!("s3://{bucket}"));
            }
            let normalized = output.trim_start_matches("./").trim_start_matches('/');
            Ok(format!("s3://{bucket}/{normalized}"))
        }
        (Some(output), _) => Ok(output.to_string()),
        (None, Some(bucket)) => Ok(format!("s3://{bucket}")),
        (None, None) => {
            anyhow::bail!("--output is required unless --s3-bucket or S3_BUCKET is set")
        }
    }
}

fn is_explicit_local_output_path(output: &str) -> bool {
    let output = output.trim();
    if output.is_empty() || output == "." {
        return false;
    }

    let path = Path::new(output);
    path.is_absolute()
        || output.starts_with("./")
        || output.starts_with("../")
        || output.starts_with(".\\")
        || output.starts_with("..\\")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParquetInputPath {
    Local(PathBuf),
    S3(String),
}

fn configured_s3_bucket() -> Option<String> {
    std::env::var("S3_BUCKET")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn shorthand_s3_key(path: &str) -> Option<String> {
    let mut parts = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    if parts.is_empty() {
        return None;
    }

    Some(parts.join("/"))
}

fn resolve_parquet_input_path(path: &str) -> ParquetInputPath {
    let input_path = Path::new(path);
    if path.starts_with("s3://") {
        return ParquetInputPath::S3(path.to_string());
    }

    if input_path.exists() {
        let local_path = input_path.to_path_buf();
        return ParquetInputPath::Local(local_path);
    }

    if !input_path.is_absolute() {
        if let (Some(bucket), Some(key)) = (configured_s3_bucket(), shorthand_s3_key(path)) {
            return ParquetInputPath::S3(format!("s3://{bucket}/{key}"));
        }
    }

    ParquetInputPath::Local(input_path.to_path_buf())
}

pub fn resolve_parquet_input_path_string(path: &str) -> String {
    match resolve_parquet_input_path(path) {
        ParquetInputPath::S3(path) => path,
        ParquetInputPath::Local(path) => path.to_string_lossy().into_owned(),
    }
}

/// Reject S3 output when explicit AWS credentials were not resolved by the CLI/config layer.
pub fn validate_s3_output_credentials(
    output: &str,
    aws_access_key_id: Option<&str>,
    aws_secret_access_key: Option<&str>,
) -> anyhow::Result<()> {
    if !output.starts_with("s3://") {
        return Ok(());
    }

    let has_access_key_id = aws_access_key_id
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    let has_secret_access_key = aws_secret_access_key
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());

    if has_access_key_id && has_secret_access_key {
        return Ok(());
    }

    anyhow::bail!(
        "S3 output requested but explicit AWS credentials were not fully resolved from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY; refusing to fall back silently to metadata providers"
    );
}

pub fn build_partitions_output_root(output_root: &str, chain: &str) -> String {
    let normalized_root = output_root.trim_end_matches('/');
    if normalized_root.starts_with("s3://") {
        format!("{normalized_root}/{chain}")
    } else {
        std::path::PathBuf::from(normalized_root)
            .join(chain)
            .to_string_lossy()
            .into_owned()
    }
}

pub fn build_partitions_index_path(output_root: &str, chain: &str) -> String {
    let chain_root = build_partitions_output_root(output_root, chain);
    if chain_root.starts_with("s3://") {
        format!("{chain_root}/partitions.parquet")
    } else {
        std::path::PathBuf::from(chain_root)
            .join("partitions.parquet")
            .to_string_lossy()
            .into_owned()
    }
}

pub fn build_partitions_cursor_path(output_root: &str, chain: &str) -> String {
    let chain_root = build_partitions_output_root(output_root, chain);
    if chain_root.starts_with("s3://") {
        format!("{chain_root}/cursor.parquet")
    } else {
        std::path::PathBuf::from(chain_root)
            .join("cursor.parquet")
            .to_string_lossy()
            .into_owned()
    }
}

pub fn build_partition_rows_from_blocks(
    chain: &str,
    partition_types: Vec<PartitionBuildType>,
    blocks: &[crate::traits::BlockIdentity],
    stop_block: u64,
) -> anyhow::Result<Vec<PartitionBuildRow>> {
    let mut builder = PartitionIndexBuilder::new(chain, partition_types)?;
    for block in blocks {
        builder.observe_block(block)?;
    }
    builder.finish(stop_block)
}

fn build_partition_row(
    chain: &str,
    partition_type: PartitionBuildType,
    active: ActivePartitionBuildRow,
    stop_block: u64,
    block_range_size: Option<u64>,
) -> anyhow::Result<PartitionBuildRow> {
    if active.start_block >= stop_block {
        anyhow::bail!(
            "invalid partition row for {} {}: start_block {} must be < stop_block {}",
            partition_type,
            active.partition_value,
            active.start_block,
            stop_block
        );
    }

    let partition_start_ts = if partition_type == PartitionBuildType::BlockRange {
        active.partition_start_ts.to_string()
    } else {
        format_partition_timestamp(active.partition_start_ts)?
    };

    let start_time = active
        .start_time
        .map(format_partition_timestamp)
        .transpose()?;
    let end_time = active
        .last_time
        .map(format_partition_timestamp)
        .transpose()?;

    let interval = if partition_type == PartitionBuildType::BlockRange {
        block_range_size.unwrap_or(0) as i64
    } else {
        partition_type.interval_seconds()
    };

    Ok(PartitionBuildRow {
        partition_type: partition_type.to_string(),
        partition_interval_seconds: interval,
        partition_start_ts,
        partition_value: active.partition_value,
        start_block: active.start_block,
        stop_block,
        start_time,
        end_time,
        chain: Some(chain.to_string()),
    })
}

fn format_partition_timestamp(timestamp: i64) -> anyhow::Result<String> {
    use time::OffsetDateTime;

    let dt = OffsetDateTime::from_unix_timestamp(timestamp)
        .map_err(|e| anyhow::anyhow!("invalid unix timestamp {timestamp}: {e}"))?;
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

fn parse_partition_timestamp(value: &str) -> anyhow::Result<i64> {
    use time::{Date, Month, PrimitiveDateTime, Time};

    let (date_part, time_part) = value.split_once(' ').ok_or_else(|| {
        anyhow::anyhow!("invalid partition timestamp '{value}': expected YYYY-MM-DD HH:MM:SS")
    })?;
    let mut date_iter = date_part.split('-');
    let year: i32 = date_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing year in '{value}'"))?
        .parse()?;
    let month: u8 = date_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing month in '{value}'"))?
        .parse()?;
    let day: u8 = date_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing day in '{value}'"))?
        .parse()?;

    let mut time_iter = time_part.split(':');
    let hour: u8 = time_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing hour in '{value}'"))?
        .parse()?;
    let minute: u8 = time_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing minute in '{value}'"))?
        .parse()?;
    let second: u8 = time_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing second in '{value}'"))?
        .parse()?;

    let month = Month::try_from(month)?;
    let date = Date::from_calendar_date(year, month, day)?;
    let time = Time::from_hms(hour, minute, second)?;
    Ok(PrimitiveDateTime::new(date, time)
        .assume_utc()
        .unix_timestamp())
}

/// Numeric key of a partition value, matching the canonical `partition` column: the
/// start block for `block_range` partitions and UTC epoch seconds for time-based
/// partitions (`YYYY-MM-DD HH:MM:SS`).
///
/// Partition rows must be ordered and range-filtered on this key rather than on the
/// rendered string, because block numbers do not sort lexicographically
/// (`"10000000" < "8000000"`).
pub fn partition_value_key(partition_type: &str, partition_value: &str) -> anyhow::Result<u64> {
    if PartitionBuildType::from_cli_value(partition_type)? == PartitionBuildType::BlockRange {
        return partition_value.parse::<u64>().map_err(|error| {
            anyhow::anyhow!(
                "'{partition_value}' is not a valid block_range partition value: expected a start block number ({error})"
            )
        });
    }

    let timestamp = parse_partition_timestamp(partition_value).map_err(|error| {
        anyhow::anyhow!(
            "'{partition_value}' is not a valid {partition_type} partition value: expected YYYY-MM-DD HH:MM:SS ({error})"
        )
    })?;
    u64::try_from(timestamp).map_err(|_| {
        anyhow::anyhow!(
            "'{partition_value}' is not a valid {partition_type} partition value: timestamps before 1970-01-01 00:00:00 are not supported"
        )
    })
}

impl PartitionBuildRow {
    /// Numeric partition key used for ordering and range filters (see [`partition_value_key`]).
    pub fn partition_key(&self) -> anyhow::Result<u64> {
        partition_value_key(&self.partition_type, &self.partition_value)
    }
}

/// Order rows by partition type, then numeric partition key, then block bounds.
fn sort_keyed_partition_rows(rows: &mut [(u64, PartitionBuildRow)]) {
    rows.sort_by(|(left_key, left), (right_key, right)| {
        left.partition_type
            .cmp(&right.partition_type)
            .then_with(|| left_key.cmp(right_key))
            .then_with(|| left.start_block.cmp(&right.start_block))
            .then_with(|| left.stop_block.cmp(&right.stop_block))
    });
}

fn sort_partition_build_rows(
    rows: Vec<PartitionBuildRow>,
) -> anyhow::Result<Vec<PartitionBuildRow>> {
    let mut keyed = rows
        .into_iter()
        .map(|row| Ok((row.partition_key()?, row)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    sort_keyed_partition_rows(&mut keyed);
    Ok(keyed.into_iter().map(|(_, row)| row).collect())
}

/// Parse a user-supplied partition bound (for example `--from`) against a partition type.
fn parse_partition_bound(flag: &str, partition_type: &str, value: &str) -> anyhow::Result<u64> {
    partition_value_key(partition_type, value)
        .map_err(|error| anyhow::anyhow!("invalid {flag}: {error}"))
}

pub fn read_partitions_build_rows(
    path: &str,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<Vec<PartitionBuildRow>> {
    Ok(read_keyed_partitions_build_rows(path, aws)?
        .into_iter()
        .map(|(_, row)| row)
        .collect())
}

/// Read `partitions.parquet` rows paired with their numeric `partition` column value,
/// sorted by partition type, partition key, and block bounds.
fn read_keyed_partitions_build_rows(
    path: &str,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<Vec<(u64, PartitionBuildRow)>> {
    let path = resolve_parquet_input_path_string(path);
    use arrow::array::{
        Array, Int32Array, Int64Array, LargeStringArray, StringArray, TimestampSecondArray,
        UInt32Array, UInt64Array,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn read_utf8_value(column: &dyn Array, row: usize) -> anyhow::Result<Option<String>> {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
            return Ok(Some(arr.value(row).to_string()));
        }
        if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(Some(arr.value(row).to_string()));
        }
        anyhow::bail!("expected utf8 column, found {}", column.data_type())
    }

    fn read_timestamp_as_string(column: &dyn Array, row: usize) -> anyhow::Result<Option<String>> {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
            return Ok(Some(arr.value(row).to_string()));
        }
        if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(Some(arr.value(row).to_string()));
        }
        if let Some(arr) = column.as_any().downcast_ref::<TimestampSecondArray>() {
            return Ok(Some(format_partition_timestamp(arr.value(row))?));
        }
        anyhow::bail!(
            "expected utf8 or timestamp(second, UTC) column, found {}",
            column.data_type()
        )
    }

    fn read_i64_value(column: &dyn Array, row: usize) -> anyhow::Result<Option<i64>> {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
            return Ok(Some(arr.value(row)));
        }
        if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
            return Ok(Some(arr.value(row) as i64));
        }
        if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
            return Ok(Some(arr.value(row) as i64));
        }
        if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
            return Ok(Some(arr.value(row) as i64));
        }
        anyhow::bail!("expected integer column, found {}", column.data_type())
    }

    fn read_u64_value(column: &dyn Array, row: usize) -> anyhow::Result<Option<u64>> {
        read_i64_value(column, row)?.map_or(Ok(None), |value| {
            if value < 0 {
                anyhow::bail!("negative integer value: {value}");
            }
            Ok(Some(value as u64))
        })
    }

    /// File-level metadata carried by the canonical partitions index schema.
    #[derive(Debug, Clone, Default)]
    struct PartitionsFileContext {
        chain: Option<String>,
        partition_type: Option<String>,
        interval: Option<i64>,
    }

    fn collect_rows(
        batch: &arrow::record_batch::RecordBatch,
        rows: &mut Vec<(u64, PartitionBuildRow)>,
        read_utf8_value: &impl Fn(&dyn Array, usize) -> anyhow::Result<Option<String>>,
        read_u64_value: &impl Fn(&dyn Array, usize) -> anyhow::Result<Option<u64>>,
        file_ctx: &PartitionsFileContext,
    ) -> anyhow::Result<()> {
        let schema = batch.schema();
        let partition_type = file_ctx
            .partition_type
            .as_ref()
            .ok_or_else(|| {
                anyhow::anyhow!("missing required metadata: firehose-parquet.partition")
            })?
            .clone();
        let partition_value_idx = schema
            .index_of("partition")
            .map_err(|_| anyhow::anyhow!("missing required column: partition"))?;
        let start_block_idx = schema
            .index_of("start_block")
            .map_err(|_| anyhow::anyhow!("missing required column: start_block"))?;
        let stop_block_idx = schema
            .index_of("stop_block")
            .map_err(|_| anyhow::anyhow!("missing required column: stop_block"))?;
        let chain_idx = schema.index_of("chain").ok();
        let start_time_idx = schema.index_of("start_time").ok();
        let end_time_idx = schema.index_of("end_time").ok();

        for row_index in 0..batch.num_rows() {
            let partition_column = batch.column(partition_value_idx).as_ref();
            let partition_raw = read_u64_value(partition_column, row_index)?
                .ok_or_else(|| anyhow::anyhow!("partition cannot be null"))?;
            let partition_value = if partition_type == "block_range" {
                partition_raw.to_string()
            } else {
                format_partition_timestamp(partition_raw as i64)?
            };
            let partition_start_ts = partition_value.clone();
            let partition_interval_seconds = file_ctx.interval.unwrap_or_else(|| {
                PartitionBuildType::from_cli_value(&partition_type)
                    .map(|kind| kind.interval_seconds())
                    .unwrap_or_default()
            });
            let start_block = read_u64_value(batch.column(start_block_idx).as_ref(), row_index)?
                .ok_or_else(|| anyhow::anyhow!("start_block cannot be null"))?;
            let stop_block = read_u64_value(batch.column(stop_block_idx).as_ref(), row_index)?
                .ok_or_else(|| anyhow::anyhow!("stop_block cannot be null"))?;

            let chain = if let Some(idx) = chain_idx {
                read_utf8_value(batch.column(idx).as_ref(), row_index)?
            } else {
                file_ctx.chain.clone()
            };

            // start_time and end_time are now nullable
            let start_time = start_time_idx.and_then(|idx| {
                read_timestamp_as_string(batch.column(idx).as_ref(), row_index)
                    .ok()
                    .flatten()
            });
            let end_time = end_time_idx.and_then(|idx| {
                read_timestamp_as_string(batch.column(idx).as_ref(), row_index)
                    .ok()
                    .flatten()
            });

            rows.push((
                partition_raw,
                PartitionBuildRow {
                    partition_type: partition_type.clone(),
                    partition_interval_seconds,
                    partition_start_ts,
                    partition_value,
                    start_block,
                    stop_block,
                    start_time,
                    end_time,
                    chain,
                },
            ));
        }

        Ok(())
    }

    let mut rows = Vec::new();
    /// Extract canonical chain/type/interval from Parquet file-level metadata.
    fn extract_file_context(
        file_metadata: &parquet::file::metadata::FileMetaData,
    ) -> anyhow::Result<PartitionsFileContext> {
        let kv = file_metadata.key_value_metadata();
        let find = |key: &str| -> Option<String> {
            kv.as_ref().and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == key)
                    .and_then(|kv| kv.value.clone())
            })
        };
        let chain = find("firehose-parquet.chain_name");
        let partition_type = find("firehose-parquet.partition")
            .and_then(|pt| canonical_partition_type_label(&pt).ok());
        let interval = find("firehose-parquet.block_range_size")
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .or_else(|| {
                partition_type.as_ref().and_then(|pt| {
                    PartitionBuildType::from_cli_value(pt)
                        .ok()
                        .map(|kind| kind.interval_seconds())
                })
            });
        Ok(PartitionsFileContext {
            chain,
            partition_type,
            interval,
        })
    }

    if path.starts_with("s3://") {
        use crate::writer::parse_s3_url;
        use object_store::ObjectStore;

        let aws = aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?;
        let (bucket, key) = parse_s3_url(&path)?;
        let client = aws.build_s3_client(&bucket)?;
        let object_path = object_store::path::Path::from(key.as_str());
        let data = block_on_async(async { client.get(&object_path).await?.bytes().await })
            .map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        let schema = builder.schema();
        validate_partitions_schema(&schema)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let file_ctx = extract_file_context(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            collect_rows(
                &batch?,
                &mut rows,
                &read_utf8_value,
                &read_u64_value,
                &file_ctx,
            )?;
        }
    } else {
        let file =
            std::fs::File::open(&path).map_err(|e| anyhow::anyhow!("opening {path}: {e}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema();
        validate_partitions_schema(&schema)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let file_ctx = extract_file_context(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            collect_rows(
                &batch?,
                &mut rows,
                &read_utf8_value,
                &read_u64_value,
                &file_ctx,
            )?;
        }
    }

    sort_keyed_partition_rows(&mut rows);
    Ok(rows)
}

pub fn write_partitions_index(
    path: &str,
    rows: &[PartitionBuildRow],
    aws: Option<&AwsConfig>,
) -> anyhow::Result<()> {
    write_partitions_index_with_metadata(path, rows, Compression::Zstd, aws, None)
}

pub fn write_partitions_index_with_metadata(
    path: &str,
    rows: &[PartitionBuildRow],
    compression: Compression,
    aws: Option<&AwsConfig>,
    file_metadata: Option<&crate::writer::ParquetFileMetadata>,
) -> anyhow::Result<()> {
    write_partitions_index_impl(path, rows, compression, aws, file_metadata)
}

/// Write partitions index.
pub fn write_partitions_index_strict(
    path: &str,
    rows: &[PartitionBuildRow],
    compression: Compression,
    aws: Option<&AwsConfig>,
    file_metadata: Option<&crate::writer::ParquetFileMetadata>,
) -> anyhow::Result<()> {
    write_partitions_index_impl(path, rows, compression, aws, file_metadata)
}

fn write_partitions_index_impl(
    path: &str,
    rows: &[PartitionBuildRow],
    compression: Compression,
    aws: Option<&AwsConfig>,
    file_metadata: Option<&crate::writer::ParquetFileMetadata>,
) -> anyhow::Result<()> {
    use arrow::array::{TimestampSecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression as PqCompression;
    use parquet::basic::ZstdLevel;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    if rows.is_empty() {
        anyhow::bail!("cannot write an empty partitions index");
    }

    // Build minimal metadata from rows when no external metadata is provided.
    // This ensures chain_name and partition type are always in file metadata.
    let auto_metadata = if file_metadata.is_none() {
        let mut meta = crate::writer::ParquetFileMetadata::new();
        if let Some(chain) = rows.first().and_then(|r| r.chain.as_deref()) {
            meta.add("firehose-parquet.chain_name", chain);
        }
        if let Some(row) = rows.first() {
            meta.add("firehose-parquet.partition", &row.partition_type);
            meta.add(
                "firehose-parquet.block_range_size",
                if row.partition_type == "block_range" {
                    row.partition_interval_seconds.to_string()
                } else {
                    "0".to_string()
                },
            );
        }
        Some(meta)
    } else {
        None
    };
    let effective_metadata = file_metadata.or(auto_metadata.as_ref());

    // Lean schema: chain, type, interval live in file-level metadata only
    let schema = Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt64, false),
        Field::new("start_block", DataType::UInt64, false),
        Field::new("stop_block", DataType::UInt64, false),
        Field::new(
            "start_time",
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
            true,
        ),
        Field::new(
            "end_time",
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
            true,
        ),
    ]));

    // Build partition column: UInt64 — epoch seconds for time-based, start block for block_range
    let partition_values = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            row.partition_key().map_err(|error| {
                anyhow::anyhow!("invalid partition for partition row at index {index}: {error}")
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // Build nullable start_time / end_time columns
    let start_time_values: Vec<Option<i64>> = rows
        .iter()
        .map(|row| {
            row.start_time
                .as_deref()
                .map(parse_partition_timestamp)
                .transpose()
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let end_time_values: Vec<Option<i64>> = rows
        .iter()
        .map(|row| {
            row.end_time
                .as_deref()
                .map(parse_partition_timestamp)
                .transpose()
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(partition_values)),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.start_block).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.stop_block).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampSecondArray::from(start_time_values).with_timezone("UTC")),
            Arc::new(TimestampSecondArray::from(end_time_values).with_timezone("UTC")),
        ],
    )?;

    let pq_compression = match compression {
        Compression::None => PqCompression::UNCOMPRESSED,
        Compression::Snappy => PqCompression::SNAPPY,
        Compression::Gzip => PqCompression::GZIP(Default::default()),
        Compression::Zstd => PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap()),
    };
    let mut props_builder = WriterProperties::builder().set_compression(pq_compression);
    let mut kvs = Vec::new();
    if let Some(meta) = effective_metadata {
        kvs.extend(
            meta.entries
                .iter()
                .map(|(key, value)| KeyValue::new(key.clone(), value.clone())),
        );
    }
    if !kvs.is_empty() {
        props_builder = props_builder.set_key_value_metadata(Some(kvs));
    }
    let props = props_builder.build();

    if path.starts_with("s3://") {
        use crate::writer::parse_s3_url;
        use bytes::Bytes;
        use object_store::aws::AmazonS3Builder;
        use object_store::ObjectStore;

        let aws = aws
            .ok_or_else(|| anyhow::anyhow!("AWS config required for S3 partitions index output"))?;
        let (bucket, key) = parse_s3_url(path)?;
        let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);
        if let Some(ref value) = aws.aws_access_key_id {
            builder = builder.with_access_key_id(value);
        }
        if let Some(ref value) = aws.aws_secret_access_key {
            builder = builder.with_secret_access_key(value);
        }
        if let Some(ref value) = aws.aws_session_token {
            builder = builder.with_token(value);
        }
        if let Some(ref value) = aws.aws_region {
            builder = builder.with_region(value);
        }
        if let Some(ref value) = aws.aws_endpoint_url {
            builder = builder.with_endpoint(value);
        }

        let client = builder
            .build()
            .map_err(|e| anyhow::anyhow!("building S3 client for bucket {bucket}: {e}"))?;
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
        }

        let object_path = object_store::path::Path::from(key.as_str());
        block_on_async(async {
            client
                .put(
                    &object_path,
                    object_store::PutPayload::from(Bytes::from(buf)),
                )
                .await
        })
        .map_err(|e| anyhow::anyhow!("writing {path}: {e}"))?;
    } else {
        let output_path = std::path::Path::new(path);
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file_name = output_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid output file name for {path}"))?;
        let temp_path = output_path.with_file_name(format!(".{file_name}.tmp"));
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        std::fs::rename(&temp_path, output_path)?;
    }

    Ok(())
}

fn is_utf8_like(data_type: &arrow::datatypes::DataType) -> bool {
    matches!(
        data_type,
        arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8
    )
}

fn is_integer_like(data_type: &arrow::datatypes::DataType) -> bool {
    matches!(
        data_type,
        arrow::datatypes::DataType::UInt64
            | arrow::datatypes::DataType::UInt32
            | arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::Int32
    )
}

fn is_timestamp_second_utc(data_type: &arrow::datatypes::DataType) -> bool {
    matches!(
        data_type,
        arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Second,
            Some(timezone)
        )
            if timezone.as_ref() == "UTC"
    )
}

fn validate_partitions_schema(schema: &arrow::datatypes::Schema) -> anyhow::Result<()> {
    if let Ok(chain) = schema.field_with_name("chain") {
        if !is_utf8_like(chain.data_type()) {
            anyhow::bail!(
                "invalid partitions.parquet column type for chain: expected Utf8/LargeUtf8, got {}",
                chain.data_type()
            );
        }
        if chain.is_nullable() {
            anyhow::bail!("invalid partitions.parquet schema: chain must be non-nullable");
        }
    }

    let partition_value = schema
        .field_with_name("partition")
        .map_err(|_| anyhow::anyhow!("missing required column: partition"))?;
    if !is_integer_like(partition_value.data_type()) {
        anyhow::bail!(
            "invalid partitions.parquet column type for partition: expected integer, got {}",
            partition_value.data_type()
        );
    }

    let start_block = schema
        .field_with_name("start_block")
        .map_err(|_| anyhow::anyhow!("missing required column: start_block"))?;
    if !is_integer_like(start_block.data_type()) {
        anyhow::bail!(
            "invalid partitions.parquet column type for start_block: expected integer, got {}",
            start_block.data_type()
        );
    }

    let stop_block = schema
        .field_with_name("stop_block")
        .map_err(|_| anyhow::anyhow!("missing required column: stop_block"))?;
    if !is_integer_like(stop_block.data_type()) {
        anyhow::bail!(
            "invalid partitions.parquet column type for stop_block: expected integer, got {}",
            stop_block.data_type()
        );
    }
    for (name, field) in [
        ("start_time", schema.field_with_name("start_time")),
        ("end_time", schema.field_with_name("end_time")),
    ] {
        if let Ok(field) = field {
            if !(is_utf8_like(field.data_type()) || is_timestamp_second_utc(field.data_type())) {
                anyhow::bail!(
                    "invalid partitions.parquet column type for {name}: expected Utf8/LargeUtf8 or Timestamp(Second, UTC), got {}",
                    field.data_type()
                );
            }
        }
    }

    Ok(())
}

fn validate_partitions_metadata(
    file_meta: &parquet::file::metadata::FileMetaData,
) -> anyhow::Result<()> {
    let kvs = file_meta
        .key_value_metadata()
        .ok_or_else(|| anyhow::anyhow!("missing required file metadata"))?;
    let find = |key: &str| -> Option<&str> {
        kvs.iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.as_deref())
    };

    let partition_type = find("firehose-parquet.partition")
        .ok_or_else(|| anyhow::anyhow!("missing required metadata: firehose-parquet.partition"))?;
    let partition_type = canonical_partition_type_label(partition_type)?;
    if partition_type == "block_range" {
        let block_range_size = find("firehose-parquet.block_range_size").ok_or_else(|| {
            anyhow::anyhow!("missing required metadata: firehose-parquet.block_range_size")
        })?;
        let parsed = block_range_size.parse::<u64>().map_err(|error| {
            anyhow::anyhow!(
                "invalid firehose-parquet.block_range_size metadata value `{block_range_size}`: {error}"
            )
        })?;
        if parsed == 0 {
            anyhow::bail!("firehose-parquet.block_range_size must be greater than 0");
        }
    }
    Ok(())
}

/// Resolve partition bounds and return a response payload suitable for CLI output.
fn resolve_partition_chains(
    request: &PartitionBoundsRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<Vec<Option<String>>> {
    let request = normalize_partition_bounds_request(request.clone())?;
    use std::collections::BTreeSet;

    let partition_key = parse_partition_bound(
        "--partition-value",
        &request.partition_type,
        &request.partition_value,
    )?;
    let mut chains = BTreeSet::new();
    for (key, row) in read_keyed_partitions_build_rows(&request.index_path, aws)? {
        if row.partition_type != request.partition_type || key != partition_key {
            continue;
        }
        chains.insert(row.chain);
    }

    Ok(chains.into_iter().collect())
}

pub fn resolve_partition_command(
    mut request: PartitionBoundsRequest,
    aws: Option<&AwsConfig>,
    options: &PartitionResolveOptions,
) -> anyhow::Result<PartitionResolveResult> {
    request = normalize_partition_bounds_request(request)?;
    if options.strict_single_chain && request.chain.is_none() {
        let chains = resolve_partition_chains(&request, aws)?;
        match chains.as_slice() {
            [] => {}
            [Some(chain)] => {
                request.chain = Some(chain.clone());
            }
            [None] => {}
            _ => {
                let labels = chains
                    .into_iter()
                    .map(|chain| chain.unwrap_or_else(|| "<null>".to_string()))
                    .collect::<Vec<_>>();
                anyhow::bail!(
                    "partition resolves to multiple chains in {} for partition_type={}, partition_value={}: {}. Re-run with --partition-chain to disambiguate",
                    request.index_path,
                    request.partition_type,
                    request.partition_value,
                    labels.join(", ")
                );
            }
        }
    }

    let bounds = resolve_partition_bounds_from_index(&request, aws)?;
    Ok(PartitionResolveResult {
        partitions_index: request.index_path,
        partition_type: request.partition_type,
        partition_value: request.partition_value,
        partition_chain: request.chain,
        start_block: bounds.start_block,
        stop_block: bounds.stop_block,
    })
}

/// List partition rows from a `partitions.parquet` index with optional filters.
pub fn list_partitions_from_index(
    request: &PartitionListRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionListResult> {
    let request = normalize_partition_list_request(request.clone())?;

    if request.limit == 0 {
        anyhow::bail!("--limit must be greater than 0");
    }

    // `--from`/`--to` are start blocks for block_range rows and timestamps otherwise,
    // so they are parsed (once) against each row's partition type.
    let mut bounds_by_type =
        std::collections::BTreeMap::<String, (Option<u64>, Option<u64>)>::new();
    let mut rows = Vec::new();
    for (key, row) in read_keyed_partitions_build_rows(&request.index_path, aws)? {
        let type_matches = request
            .partition_type
            .as_deref()
            .map(|value| row.partition_type.eq_ignore_ascii_case(value))
            .unwrap_or(true);
        let chain_matches = request
            .chain
            .as_deref()
            .map(|value| row.chain.as_deref() == Some(value))
            .unwrap_or(true);
        if !type_matches || !chain_matches {
            continue;
        }

        let (from, to) = match bounds_by_type.get(&row.partition_type) {
            Some(bounds) => *bounds,
            None => {
                let parse = |flag: &str, value: &Option<String>| {
                    value
                        .as_deref()
                        .map(|value| parse_partition_bound(flag, &row.partition_type, value))
                        .transpose()
                };
                let bounds = (parse("--from", &request.from)?, parse("--to", &request.to)?);
                bounds_by_type.insert(row.partition_type.clone(), bounds);
                bounds
            }
        };
        if from.is_some_and(|from| key < from) || to.is_some_and(|to| key > to) {
            continue;
        }

        rows.push((
            key,
            PartitionListRow {
                partition_type: row.partition_type,
                partition_value: row.partition_value,
                partition_start_ts: row.partition_start_ts,
                start_block: row.start_block,
                stop_block: row.stop_block,
                chain: row.chain,
            },
        ));
    }
    let total_matches = rows.len();
    rows.sort_by(|(left_key, left), (right_key, right)| {
        left_key
            .cmp(right_key)
            .then_with(|| left.partition_type.cmp(&right.partition_type))
            .then_with(|| left.chain.cmp(&right.chain))
            .then_with(|| left.start_block.cmp(&right.start_block))
            .then_with(|| left.stop_block.cmp(&right.stop_block))
    });
    rows.truncate(request.limit);
    let rows = rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>();

    Ok(PartitionListResult {
        partitions_index: request.index_path.clone(),
        limit: request.limit,
        total_matches,
        returned_rows: rows.len(),
        rows,
    })
}

pub fn parse_partition_shard_strategy(value: &str) -> anyhow::Result<PartitionShardStrategy> {
    match value.to_ascii_lowercase().as_str() {
        "ordinal" => Ok(PartitionShardStrategy::Ordinal),
        "hash" => Ok(PartitionShardStrategy::Hash),
        other => anyhow::bail!("invalid --strategy '{other}': expected one of: ordinal, hash"),
    }
}

fn partition_shard_key(row: &PartitionListRow) -> String {
    format!(
        "{}|{}|{}",
        row.chain.as_deref().unwrap_or_default(),
        row.partition_type,
        row.partition_value
    )
}

fn assign_partition_shard(
    row: &PartitionListRow,
    ordinal: usize,
    shard_count: usize,
    strategy: PartitionShardStrategy,
) -> usize {
    match strategy {
        PartitionShardStrategy::Ordinal => ordinal % shard_count,
        PartitionShardStrategy::Hash => {
            use sha2::{Digest, Sha256};

            let digest = Sha256::digest(partition_shard_key(row).as_bytes());
            let value = u64::from_be_bytes(digest[..8].try_into().expect("digest slice"));
            (value as usize) % shard_count
        }
    }
}

pub fn shard_partitions_from_index(
    request: &PartitionShardRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionShardResult> {
    if request.shard_count == 0 {
        anyhow::bail!("--shard-count must be greater than 0");
    }
    if request.shard_index >= request.shard_count {
        anyhow::bail!(
            "--shard-index must be less than --shard-count (got {} >= {})",
            request.shard_index,
            request.shard_count
        );
    }

    let mut list_request = request.list.clone();
    list_request.limit = usize::MAX;
    let list_result = list_partitions_from_index(&list_request, aws)?;

    let rows = list_result
        .rows
        .into_iter()
        .enumerate()
        .filter_map(|(ordinal, row)| {
            let shard =
                assign_partition_shard(&row, ordinal, request.shard_count, request.strategy);
            (shard == request.shard_index).then_some(row)
        })
        .collect::<Vec<_>>();

    Ok(PartitionShardResult {
        partitions_index: request.list.index_path.clone(),
        shard_count: request.shard_count,
        shard_index: request.shard_index,
        strategy: request.strategy,
        total_matches: list_result.total_matches,
        returned_rows: rows.len(),
        rows,
    })
}

pub fn validate_partitions_index(
    request: &PartitionValidateRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionValidateResult> {
    let mut list_request = request.list.clone();
    list_request.limit = usize::MAX;
    let list_result = list_partitions_from_index(&list_request, aws)?;

    let mut issues = Vec::new();

    for row in &list_result.rows {
        if row.start_block >= row.stop_block {
            issues.push(PartitionValidationIssue {
                kind: PartitionValidationIssueKind::InvalidRange,
                partition_type: row.partition_type.clone(),
                partition_value: row.partition_value.clone(),
                chain: row.chain.clone(),
                message: format!(
                    "invalid range: start_block={} stop_block={}",
                    row.start_block, row.stop_block
                ),
            });
        }
    }

    use std::collections::BTreeMap;
    let mut groups: BTreeMap<(Option<String>, String), Vec<&PartitionListRow>> = BTreeMap::new();
    for row in &list_result.rows {
        groups
            .entry((row.chain.clone(), row.partition_type.clone()))
            .or_default()
            .push(row);
    }

    for ((_chain, _ptype), rows) in groups {
        for pair in rows.windows(2) {
            let current = pair[0];
            let next = pair[1];

            if partition_value_key(&current.partition_type, &current.partition_value)?
                > partition_value_key(&next.partition_type, &next.partition_value)?
            {
                issues.push(PartitionValidationIssue {
                    kind: PartitionValidationIssueKind::OutOfOrder,
                    partition_type: next.partition_type.clone(),
                    partition_value: next.partition_value.clone(),
                    chain: next.chain.clone(),
                    message: format!(
                        "out of order: previous partition_start_ts={} next partition_start_ts={}",
                        current.partition_start_ts, next.partition_start_ts
                    ),
                });
            }

            if current.stop_block < next.start_block {
                if !request.allow_gaps {
                    issues.push(PartitionValidationIssue {
                        kind: PartitionValidationIssueKind::Gap,
                        partition_type: next.partition_type.clone(),
                        partition_value: next.partition_value.clone(),
                        chain: next.chain.clone(),
                        message: format!(
                            "gap detected: previous stop_block={} next start_block={}",
                            current.stop_block, next.start_block
                        ),
                    });
                }
            } else if current.stop_block > next.start_block {
                issues.push(PartitionValidationIssue {
                    kind: PartitionValidationIssueKind::Overlap,
                    partition_type: next.partition_type.clone(),
                    partition_value: next.partition_value.clone(),
                    chain: next.chain.clone(),
                    message: format!(
                        "overlap detected: previous stop_block={} next start_block={}",
                        current.stop_block, next.start_block
                    ),
                });
            }
        }
    }

    Ok(PartitionValidateResult {
        partitions_index: request.list.index_path.clone(),
        total_rows: list_result.total_matches,
        issue_count: issues.len(),
        valid: issues.is_empty(),
        issues,
    })
}

/// Parse a compression string into a [`Compression`] variant.
pub fn parse_compression(s: &str) -> anyhow::Result<Compression> {
    match s.to_lowercase().as_str() {
        "zstd" => Ok(Compression::Zstd),
        "snappy" => Ok(Compression::Snappy),
        "gzip" => Ok(Compression::Gzip),
        "none" => Ok(Compression::None),
        other => anyhow::bail!(
            "invalid --compression '{other}': expected one of: zstd, snappy, gzip, none"
        ),
    }
}

/// Parse a partition string into a [`Partition`] variant.
pub fn parse_partition(s: &str, block_range_size: u64) -> anyhow::Result<Partition> {
    match s.to_lowercase().as_str() {
        "none" => Ok(Partition::None),
        "block_range" if block_range_size == 0 => {
            anyhow::bail!("--block-range-size must be at least 1 when --partition block_range")
        }
        "block_range" => Ok(Partition::block_range(block_range_size)),
        "date" => Ok(Partition::Date),
        "hour" => Ok(Partition::Hour),
        "minute" => Ok(Partition::Minute),
        "second" => Ok(Partition::Second),
        other => anyhow::bail!("invalid --partition '{other}': expected one of: none, block_range, date, hour, minute, second"),
    }
}

/// Check that an exclusive stop block leaves a non-empty range after the
/// start block. Either bound may be unknown (live mode, or a start block
/// resolved later from a cursor or the endpoint).
pub fn validate_stop_block_after_start(
    start_block: Option<u64>,
    stop_block: Option<u64>,
) -> anyhow::Result<()> {
    if let (Some(start_block), Some(stop_block)) = (start_block, stop_block) {
        if stop_block <= start_block {
            anyhow::bail!(
                "--stop-block ({stop_block}) must be greater than the start block ({start_block}); --stop-block is exclusive"
            );
        }
    }
    Ok(())
}

/// Read a credential from the environment variable `name`.
///
/// Surrounding whitespace is trimmed (a secret mounted from a file often ends
/// with a newline) and blank values count as unset.
pub fn read_credential_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Build a [`Config`] from [`CommonArgs`].
///
/// Returns an error if `--endpoint` was not provided (required for pipeline execution).
pub fn build_config(args: &CommonArgs) -> anyhow::Result<Config> {
    let endpoint = args
        .endpoint
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--endpoint is required"))?;

    // Resolve the actual API key / JWT token by reading the environment variable
    // whose *name* is given by `--api-key-envvar` / `--api-token-envvar`.
    let api_key = read_credential_env(&args.api_key_envvar);
    let jwt_token = read_credential_env(&args.api_token_envvar);

    validate_stop_block_after_start(args.start_block, args.stop_block)?;

    // Only reinterpret output as S3 when it is not an explicit local path.
    let output = PathBuf::from(resolve_s3_output_root(
        Some(args.output.to_string_lossy().as_ref()),
        args.s3_bucket.as_deref(),
    )?);

    validate_s3_output_credentials(
        output.to_string_lossy().as_ref(),
        args.aws_access_key_id.as_deref(),
        args.aws_secret_access_key.as_deref(),
    )?;

    // Validate that the cursor path has a .parquet extension.
    let cursor_str = args.cursor.to_string_lossy();
    if !cursor_str.ends_with(".parquet") {
        return Err(anyhow::anyhow!(
            "--cursor path must end in .parquet, got: {cursor_str}"
        ));
    }
    if let Some(template) = normalize_opt_string(&args.cursor_template) {
        if !template.ends_with(".parquet") {
            return Err(anyhow::anyhow!(
                "--cursor-template must end in .parquet, got: {template}"
            ));
        }
    }

    Ok(Config {
        endpoint,
        api_key,
        jwt_token,
        start_block: args.start_block,
        stop_block: args.stop_block,
        skip_missing_blocks: true,
        cursor_path: Some(args.cursor.to_string_lossy().to_string()),
        output,
        partition: parse_partition(&args.partition, args.block_range_size)?,
        flush_rows: args.flush_rows,
        flush_blocks: args.flush_blocks,
        flush_bytes: args.flush_bytes,
        flush_interval_secs: args.flush_interval_secs,
        compression: parse_compression(&args.compression)?,
        final_blocks_only: args.final_blocks_only,
        dry_run: args.dry_run,
        aws_access_key_id: args.aws_access_key_id.clone(),
        aws_secret_access_key: args.aws_secret_access_key.clone(),
        aws_session_token: args.aws_session_token.clone(),
        aws_region: args.aws_region.clone(),
        aws_endpoint_url: args.aws_endpoint_url.clone(),
        s3_bucket: args.s3_bucket.clone(),
        cache_control: if args.cache_control.is_empty() {
            None
        } else {
            Some(args.cache_control.clone())
        },
        metrics_port: args.metrics_port,
        // 0 disables either timeout.
        stream_idle_timeout_secs: args.stream_idle_timeout_secs.filter(|secs| *secs > 0),
        reconnect_stall_timeout_secs: args.reconnect_stall_timeout_secs.filter(|secs| *secs > 0),
    })
}

/// Return the tracing level to use for the current CLI settings.
///
/// Verbose mode promotes the normal default `info` level to `debug` so operators
/// can opt into richer logs without overriding an explicitly chosen level such as
/// `warn`, `error`, or `trace`. For example, `--verbose --log-level warn`
/// remains `warn`, while `--verbose` on its own uses `debug`.
pub fn effective_log_level(log_level: &str, verbose: bool) -> &str {
    if verbose && log_level.eq_ignore_ascii_case("info") {
        "debug"
    } else {
        log_level
    }
}

/// Initialize tracing subscriber with the given log level.
pub fn init_tracing(log_level: &str, verbose: bool) {
    let requested_level = effective_log_level(log_level, verbose);
    let fallback_level = if verbose { "debug" } else { "info" };
    let filter = tracing_subscriber::EnvFilter::try_new(requested_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(fallback_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

/// Generate shell completions for the given command and write to stdout.
pub fn generate_completions<C: clap::CommandFactory>(shell: Shell) {
    let mut cmd = C::command();
    let name = cmd.get_name().to_string();
    generate(shell, &mut cmd, name, &mut io::stdout());
}

/// AWS credentials for building an S3 client.
#[derive(Debug, Clone)]
pub struct AwsConfig {
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub aws_region: Option<String>,
    pub aws_endpoint_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionBoundsRequest {
    pub index_path: String,
    pub partition_type: String,
    pub partition_value: String,
    pub chain: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionWindowRequest {
    pub index_path: String,
    pub partition_type: String,
    pub partition_from: String,
    pub partition_to: String,
    pub chain: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionBounds {
    pub start_block: u64,
    pub stop_block: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionWindowBounds {
    pub start_block: u64,
    pub stop_block: u64,
    pub partitions_count: usize,
    pub partition_from: String,
    pub partition_to: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorTemplateContext {
    pub chain: Option<String>,
    pub partition_type: Option<String>,
    pub partition_value: Option<String>,
    pub partition_from: Option<String>,
    pub partition_to: Option<String>,
}

fn normalize_opt_string(value: &Option<String>) -> Option<String> {
    value
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn sanitize_cursor_template_value(value: &str) -> String {
    value.replace(['/', '\\'], "_")
}

fn cursor_template_value<'a>(
    key: &str,
    context: &'a CursorTemplateContext,
) -> anyhow::Result<&'a str> {
    match key {
        "chain" => context.chain.as_deref(),
        "partition_type" => context.partition_type.as_deref(),
        "partition_value" => context.partition_value.as_deref(),
        "partition_from" => context.partition_from.as_deref(),
        "partition_to" => context.partition_to.as_deref(),
        other => anyhow::bail!(
            "unknown --cursor-template variable {{{other}}}; supported: {{chain}}, {{partition_type}}, {{partition_value}}, {{partition_from}}, {{partition_to}}"
        ),
    }
    .ok_or_else(|| anyhow::anyhow!("--cursor-template variable {{{key}}} requires partition selection context"))
}

pub fn resolve_cursor_template(
    template: &str,
    context: &CursorTemplateContext,
) -> anyhow::Result<String> {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '{' => {
                if matches!(chars.peek(), Some('{')) {
                    chars.next();
                    out.push('{');
                    continue;
                }

                let mut key = String::new();
                let mut found_close = false;
                for next in chars.by_ref() {
                    if next == '}' {
                        found_close = true;
                        break;
                    }
                    key.push(next);
                }
                if !found_close {
                    anyhow::bail!("unterminated --cursor-template variable");
                }
                let value = cursor_template_value(&key, context)?;
                out.push_str(&sanitize_cursor_template_value(value));
            }
            '}' => {
                if matches!(chars.peek(), Some('}')) {
                    chars.next();
                    out.push('}');
                } else {
                    anyhow::bail!("unmatched }} in --cursor-template");
                }
            }
            other => out.push(other),
        }
    }

    Ok(out)
}

/// Resolve `[start_block, stop_block)` from a canonical `partitions.parquet` index file.
pub fn resolve_partition_bounds_from_index(
    request: &PartitionBoundsRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionBounds> {
    let request = normalize_partition_bounds_request(request.clone())?;
    let partition_key = parse_partition_bound(
        "--partition-value",
        &request.partition_type,
        &request.partition_value,
    )?;
    let matches = read_keyed_partitions_build_rows(&request.index_path, aws)?
        .into_iter()
        .filter(|(key, row)| {
            row.partition_type == request.partition_type
                && *key == partition_key
                && request
                    .chain
                    .as_deref()
                    .map(|chain| row.chain.as_deref() == Some(chain))
                    .unwrap_or(true)
        })
        .map(|(_, row)| (row.start_block, row.stop_block))
        .collect::<Vec<_>>();

    if matches.is_empty() {
        let chain_filter = request
            .chain
            .as_ref()
            .map(|c| format!(", chain={c}"))
            .unwrap_or_default();
        anyhow::bail!(
            "no partition row found in {} for partition_type={}, partition_value={}{}",
            request.index_path,
            request.partition_type,
            request.partition_value,
            chain_filter
        );
    }

    if matches.len() > 1 {
        anyhow::bail!(
            "partition lookup is ambiguous in {}: found {} rows for partition_type={}, partition_value={}{}",
            request.index_path,
            matches.len(),
            request.partition_type,
            request.partition_value,
            request
                .chain
                .as_ref()
                .map(|c| format!(", chain={c}"))
                .unwrap_or_default()
        );
    }

    let (start_block, stop_block) = matches[0];
    if stop_block <= start_block {
        anyhow::bail!(
            "invalid partition bounds in {}: start_block={} stop_block={}",
            request.index_path,
            start_block,
            stop_block
        );
    }

    Ok(PartitionBounds {
        start_block,
        stop_block,
    })
}

/// Resolve a partition window `[partition_from, partition_to)` from `partitions.parquet`.
///
/// All matching rows must be contiguous and non-overlapping by block bounds.
pub fn resolve_partition_window_bounds_from_index(
    request: &PartitionWindowRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionWindowBounds> {
    let request = normalize_partition_window_request(request.clone())?;
    let partition_from = parse_partition_bound(
        "--partition-from",
        &request.partition_type,
        &request.partition_from,
    )?;
    let partition_to = parse_partition_bound(
        "--partition-to",
        &request.partition_type,
        &request.partition_to,
    )?;
    let mut matches = read_keyed_partitions_build_rows(&request.index_path, aws)?
        .into_iter()
        .filter(|(key, row)| {
            row.partition_type == request.partition_type
                && (partition_from..partition_to).contains(key)
                && request
                    .chain
                    .as_deref()
                    .map(|chain| row.chain.as_deref() == Some(chain))
                    .unwrap_or(true)
        })
        .map(|(key, row)| (key, row.partition_value, row.start_block, row.stop_block))
        .collect::<Vec<_>>();

    if matches.is_empty() {
        anyhow::bail!(
            "no partition rows found in {} for partition_type={} in [{}, {}){}",
            request.index_path,
            request.partition_type,
            request.partition_from,
            request.partition_to,
            request
                .chain
                .as_ref()
                .map(|chain| format!(", chain={chain}"))
                .unwrap_or_default()
        );
    }

    matches.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.2.cmp(&right.2)));

    for (idx, window) in matches.windows(2).enumerate() {
        let (current_key, current_value, _, current_stop) = &window[0];
        let (next_key, next_value, next_start, _) = &window[1];
        if current_key == next_key {
            anyhow::bail!(
                "partition window is ambiguous in {}: multiple rows for partition_value={} (rows {} and {})",
                request.index_path,
                current_value,
                idx,
                idx + 1
            );
        }
        if current_stop != next_start {
            anyhow::bail!(
                "partition window has non-contiguous bounds in {} between {} and {}: stop_block={} next_start_block={}",
                request.index_path,
                current_value,
                next_value,
                current_stop,
                next_start
            );
        }
    }

    let start_block = matches.first().expect("non-empty checked above").2;
    let stop_block = matches.last().expect("non-empty checked above").3;
    if stop_block <= start_block {
        anyhow::bail!(
            "invalid partition window bounds in {}: start_block={} stop_block={}",
            request.index_path,
            start_block,
            stop_block
        );
    }

    Ok(PartitionWindowBounds {
        start_block,
        stop_block,
        partitions_count: matches.len(),
        partition_from: request.partition_from.clone(),
        partition_to: request.partition_to.clone(),
    })
}

impl AwsConfig {
    /// Build an `AmazonS3` client for the given bucket.
    ///
    /// When no access key is provided, enables anonymous (unsigned) requests
    /// via `with_skip_signature(true)` so that public buckets can be accessed
    /// without credentials.
    pub fn build_s3_client(&self, bucket: &str) -> anyhow::Result<object_store::aws::AmazonS3> {
        use object_store::aws::AmazonS3Builder;

        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
        if let Some(ref key) = self.aws_access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(ref secret) = self.aws_secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(ref token) = self.aws_session_token {
            builder = builder.with_token(token);
        }
        if let Some(ref region) = self.aws_region {
            builder = builder.with_region(region);
        }
        if let Some(ref endpoint_url) = self.aws_endpoint_url {
            builder = builder.with_endpoint(endpoint_url);
            // Enable virtual-hosted-style requests when the endpoint contains
            // the bucket name as a subdomain (e.g. bucket.fly.storage.tigris.dev).
            // This is required by providers like Tigris that don't support
            // path-style access.
            if endpoint_url.contains(&format!("{}.", bucket)) {
                builder = builder.with_virtual_hosted_style_request(true);
            }
        }
        // When no credentials are provided, use anonymous (unsigned) requests
        // so public buckets are accessible without IMDS/IAM lookup.
        if self.aws_access_key_id.is_none() {
            builder = builder.with_skip_signature(true);
        }
        builder
            .build()
            .map_err(|e| anyhow::anyhow!("building S3 client for bucket {bucket}: {e}"))
    }
}

/// Scan and display parquet files at the given path.
///
/// Supports local filesystem paths and S3 URIs (`s3://bucket/prefix`).
/// Non-URI relative paths resolve locally first; when no local path exists and
/// `S3_BUCKET` is configured, they fall back to `s3://<bucket>/<path>`.
/// If `path` is a file, inspects that single file.
/// If `path` is a directory, recursively finds all `.parquet` files.
pub fn scan_parquet(
    path: &str,
    rows: usize,
    offset: usize,
    order: ScanOrder,
    schema_only: bool,
    vertical: bool,
    json: bool,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<()> {
    let resolved_path = resolve_parquet_input_path(path);
    let files = match &resolved_path {
        ParquetInputPath::S3(path) => collect_scan_parquet_s3(
            path,
            rows,
            offset,
            order,
            schema_only,
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?,
        )?,
        ParquetInputPath::Local(path) => {
            collect_scan_parquet_local(path, rows, offset, order, schema_only)?
        }
    };

    if files.is_empty() {
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&ScanJsonOutput {
                    files_scanned: 0,
                    files: Vec::new(),
                })?
            );
        } else {
            println!("No .parquet files found in {path}");
        }
        return Ok(());
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&ScanJsonOutput {
                files_scanned: files.len(),
                files,
            })?
        );
        return Ok(());
    }

    let row_mode = if vertical {
        ScanRowDisplayMode::Vertical
    } else {
        ScanRowDisplayMode::Table
    };
    render_scan_results(&files, row_mode, !schema_only && rows > 0);
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanRowDisplayMode {
    Table,
    Vertical,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ScanSchemaColumn {
    name: String,
    data_type: String,
    nullable: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ScanRowCell {
    name: String,
    value: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ScanRow {
    row_number: usize,
    cells: Vec<ScanRowCell>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ScanFileResult {
    path: String,
    total_rows: i64,
    row_groups: usize,
    columns: usize,
    size_bytes: u64,
    size_human: String,
    schema: Vec<ScanSchemaColumn>,
    sample_rows: Vec<ScanRow>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ScanJsonOutput {
    files_scanned: usize,
    files: Vec<ScanFileResult>,
}

/// Scan parquet files from the local filesystem.
fn collect_scan_parquet_local(
    path: &std::path::Path,
    rows: usize,
    offset: usize,
    order: ScanOrder,
    schema_only: bool,
) -> anyhow::Result<Vec<ScanFileResult>> {
    let mut files: Vec<PathBuf> = Vec::new();
    let single_file = if path.is_file() {
        files.push(path.to_path_buf());
        true
    } else if path.is_dir() {
        collect_parquet_files(&path.to_path_buf(), &mut files)?;
        files.sort();
        false
    } else {
        anyhow::bail!("path does not exist: {}", path.display());
    };

    let mut results = Vec::with_capacity(files.len());
    let mut remaining_rows = rows;
    let mut remaining_offset = offset;
    for file_path in &files {
        let display_path = if single_file {
            file_path.display().to_string()
        } else {
            file_path
                .strip_prefix(path)
                .unwrap_or(file_path)
                .display()
                .to_string()
        };
        let result = build_scan_file_result_from_local(
            file_path,
            display_path,
            remaining_rows,
            remaining_offset,
            order,
            schema_only,
        )?;
        update_scan_progress(
            &mut remaining_rows,
            &mut remaining_offset,
            result.total_rows,
            result.sample_rows.len(),
            rows,
            schema_only,
        )?;
        results.push(result);
        if !schema_only && remaining_rows == 0 {
            break;
        }
    }
    Ok(results)
}

fn build_scan_file_result_from_local(
    file_path: &std::path::Path,
    display_path: String,
    rows: usize,
    offset: usize,
    order: ScanOrder,
    schema_only: bool,
) -> anyhow::Result<ScanFileResult> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs;

    let file = fs::File::open(file_path)?;
    let file_size = file.metadata()?.len();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();
    let total_rows: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();
    let row_groups = metadata.num_row_groups();
    let columns = metadata.file_metadata().schema().get_fields().len();
    let schema = builder.schema().clone();
    let sample_rows = if schema_only || rows == 0 {
        Vec::new()
    } else {
        let file = fs::File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        let total_rows_for_sampling = scan_total_rows_for_sampling(total_rows)?;
        collect_sample_rows(
            &schema,
            reader,
            total_rows_for_sampling,
            rows,
            offset,
            order,
        )
    };

    Ok(ScanFileResult {
        path: display_path,
        total_rows,
        row_groups,
        columns,
        size_bytes: file_size,
        size_human: format_bytes(file_size),
        schema: build_scan_schema(&schema),
        sample_rows,
    })
}

/// Scan parquet files from an S3 bucket.
fn collect_scan_parquet_s3(
    path: &str,
    rows: usize,
    offset: usize,
    order: ScanOrder,
    schema_only: bool,
    aws: &AwsConfig,
) -> anyhow::Result<Vec<ScanFileResult>> {
    use crate::writer::parse_s3_url;
    use object_store::ObjectStore;

    let (bucket, prefix) = parse_s3_url(path)?;
    let client = aws.build_s3_client(&bucket)?;
    let (parquet_objects, exact_object_path) =
        block_on_async(collect_scan_s3_parquet_objects(&client, &prefix))
            .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let mut results = Vec::with_capacity(parquet_objects.len());
    let mut remaining_rows = rows;
    let mut remaining_offset = offset;
    for obj in &parquet_objects {
        let data = block_on_async(async { client.get(&obj.location).await?.bytes().await })
            .map_err(|e| anyhow::anyhow!("reading s3://{bucket}/{}: {e}", obj.location))?;
        let display_key = scan_s3_display_key(obj.location.as_ref(), &prefix, exact_object_path);
        let result = build_scan_file_result_from_bytes(
            data,
            display_key,
            remaining_rows,
            remaining_offset,
            order,
            schema_only,
        )?;
        update_scan_progress(
            &mut remaining_rows,
            &mut remaining_offset,
            result.total_rows,
            result.sample_rows.len(),
            rows,
            schema_only,
        )?;
        results.push(result);
        if !schema_only && remaining_rows == 0 {
            break;
        }
    }

    Ok(results)
}

fn build_scan_file_result_from_bytes(
    data: bytes::Bytes,
    display_path: String,
    rows: usize,
    offset: usize,
    order: ScanOrder,
    schema_only: bool,
) -> anyhow::Result<ScanFileResult> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file_size = data.len() as u64;
    let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())?;
    let metadata = builder.metadata();
    let total_rows: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();
    let row_groups = metadata.num_row_groups();
    let columns = metadata.file_metadata().schema().get_fields().len();
    let schema = builder.schema().clone();
    let sample_rows = if schema_only || rows == 0 {
        Vec::new()
    } else {
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        let reader = builder.build()?;
        let total_rows_for_sampling = scan_total_rows_for_sampling(total_rows)?;
        collect_sample_rows(
            &schema,
            reader,
            total_rows_for_sampling,
            rows,
            offset,
            order,
        )
    };

    Ok(ScanFileResult {
        path: display_path,
        total_rows,
        row_groups,
        columns,
        size_bytes: file_size,
        size_human: format_bytes(file_size),
        schema: build_scan_schema(&schema),
        sample_rows,
    })
}

async fn collect_scan_s3_parquet_objects(
    store: &dyn object_store::ObjectStore,
    prefix: &str,
) -> anyhow::Result<(Vec<object_store::ObjectMeta>, bool)> {
    use futures::TryStreamExt;

    if prefix.ends_with(".parquet") && !prefix.is_empty() {
        let object_path = object_store::path::Path::from(prefix);
        match store.head(&object_path).await {
            Ok(meta) => return Ok((vec![meta], true)),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "reading object metadata for {prefix}: {err}"
                ))
            }
        }
    }

    let list_prefix = if prefix.is_empty() {
        None
    } else {
        Some(object_store::path::Path::from(prefix))
    };
    let objects: Vec<object_store::ObjectMeta> =
        store.list(list_prefix.as_ref()).try_collect().await?;
    let mut parquet_objects: Vec<_> = objects
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .collect();
    parquet_objects.sort_by(|a, b| a.location.cmp(&b.location));
    Ok((parquet_objects, false))
}

fn scan_s3_display_key(location: &str, prefix: &str, exact_object_path: bool) -> String {
    if exact_object_path {
        return location.to_string();
    }
    location
        .strip_prefix(prefix)
        .map(|s| s.trim_start_matches('/').to_string())
        .unwrap_or_else(|| location.to_string())
}

fn build_scan_schema(schema: &arrow::datatypes::SchemaRef) -> Vec<ScanSchemaColumn> {
    schema
        .fields()
        .iter()
        .map(|field| ScanSchemaColumn {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
        })
        .collect()
}

fn collect_sample_rows(
    schema: &arrow::datatypes::SchemaRef,
    reader: impl Iterator<Item = Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>>,
    total_rows: usize,
    rows: usize,
    offset: usize,
    order: ScanOrder,
) -> Vec<ScanRow> {
    let Some((start_row, end_row)) = scan_sample_row_bounds(total_rows, rows, offset, order) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut absolute_row = 0usize;

    'outer: for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                eprintln!("  error reading batch: {e}");
                break;
            }
        };
        let batch_end_row = absolute_row.saturating_add(batch.num_rows());
        if batch_end_row < start_row {
            absolute_row = batch_end_row;
            continue;
        }
        for row_idx in 0..batch.num_rows() {
            absolute_row += 1;
            if absolute_row < start_row {
                continue;
            }
            if absolute_row > end_row {
                break 'outer;
            }
            out.push(ScanRow {
                row_number: absolute_row,
                cells: schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(col_idx, field)| ScanRowCell {
                        name: field.name().clone(),
                        value: format_array_value(batch.column(col_idx).as_ref(), row_idx),
                    })
                    .collect(),
            });
        }
    }

    if matches!(order, ScanOrder::Desc) {
        out.reverse();
    }

    out
}

fn scan_total_rows_for_sampling(total_rows: i64) -> anyhow::Result<usize> {
    usize::try_from(total_rows).map_err(|_| {
        anyhow::anyhow!(
            "parquet row count {total_rows} exceeds supported scan preview size on this platform"
        )
    })
}

fn update_scan_progress(
    remaining_rows: &mut usize,
    remaining_offset: &mut usize,
    total_rows: i64,
    sampled_rows: usize,
    requested_rows: usize,
    schema_only: bool,
) -> anyhow::Result<()> {
    if schema_only || requested_rows == 0 {
        return Ok(());
    }

    *remaining_offset = remaining_offset.saturating_sub(scan_total_rows_for_sampling(total_rows)?);
    *remaining_rows = remaining_rows.saturating_sub(sampled_rows);
    Ok(())
}

/// Compute the 1-based inclusive absolute row bounds to sample for `scan`.
///
/// `offset` and `rows` are interpreted relative to the requested display
/// `order`: ascending starts from the beginning of the file, while descending
/// starts from the end of the file. Returns `None` when the requested window
/// falls outside the available rows or when there are no rows to display.
fn scan_sample_row_bounds(
    total_rows: usize,
    rows: usize,
    offset: usize,
    order: ScanOrder,
) -> Option<(usize, usize)> {
    if total_rows == 0 || rows == 0 || offset >= total_rows {
        return None;
    }

    match order {
        ScanOrder::Asc => {
            let start_row = offset.saturating_add(1);
            let end_row = total_rows.min(offset.saturating_add(rows));
            (start_row <= end_row).then_some((start_row, end_row))
        }
        ScanOrder::Desc => {
            let end_row = total_rows.saturating_sub(offset);
            // For descending order, start from the last visible row and walk
            // backward `rows - 1` positions, clamping to the first row.
            let start_row = end_row.saturating_sub(rows.saturating_sub(1)).max(1);
            (start_row <= end_row).then_some((start_row, end_row))
        }
    }
}

fn render_scan_results(files: &[ScanFileResult], row_mode: ScanRowDisplayMode, show_rows: bool) {
    for file in files {
        println!("\n{}", "═".repeat(72));
        println!("  {}", file.path);
        println!("{}", "─".repeat(72));
        println!(
            "  rows: {}  row_groups: {}  columns: {}  size: {}",
            file.total_rows, file.row_groups, file.columns, file.size_human
        );
        println!("{}", "─".repeat(72));

        for field in &file.schema {
            println!(
                "  {:30} {:20} {}",
                field.name,
                field.data_type,
                if field.nullable {
                    "nullable"
                } else {
                    "not null"
                }
            );
        }

        if show_rows {
            render_scan_rows(file, row_mode);
        }
    }

    if files.len() > 1 {
        println!("\n{}", "═".repeat(72));
        println!("  {} parquet files scanned", files.len());
    }
}

fn render_scan_rows(file: &ScanFileResult, row_mode: ScanRowDisplayMode) {
    if file.sample_rows.is_empty() {
        println!("\n  (empty)");
        return;
    }

    let rendered = match row_mode {
        ScanRowDisplayMode::Table => format_scan_rows_table(file),
        ScanRowDisplayMode::Vertical => format_scan_rows_vertical(file),
    };
    println!("\n{rendered}");

    let shown = file.sample_rows.len();
    if file.total_rows > shown as i64 {
        println!("\n  {shown} rows shown of {} total.", file.total_rows);
    } else {
        println!("\n  {shown} rows in set.");
    }
}

fn format_scan_rows_vertical(file: &ScanFileResult) -> String {
    let max_name_len = file
        .schema
        .iter()
        .map(|field| field.name.len())
        .max()
        .unwrap_or(0);
    let mut out = String::new();

    for row in &file.sample_rows {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("Row {}:\n", row.row_number));
        out.push_str("──────");
        for cell in &row.cells {
            out.push('\n');
            out.push_str(&format!(
                "  {:width$}  {}",
                cell.name,
                cell.value,
                width = max_name_len
            ));
        }
        out.push('\n');
    }

    out.trim_end().to_string()
}

fn format_scan_rows_table(file: &ScanFileResult) -> String {
    let headers = file
        .schema
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    let mut widths = headers
        .iter()
        .map(|header| header.chars().count())
        .collect::<Vec<_>>();
    for row in &file.sample_rows {
        for (index, cell) in row.cells.iter().enumerate() {
            widths[index] = widths[index].max(cell.value.chars().count());
        }
    }

    let row_number_width = file
        .sample_rows
        .last()
        .map(|row| row.row_number.to_string().len())
        .unwrap_or(1);
    let table_indent = " ".repeat(row_number_width + 2);
    let mut out = String::new();

    out.push_str(&scan_table_border('┌', '┬', '┐', &widths, &table_indent));
    out.push('\n');
    out.push_str(&table_indent);
    out.push_str(&scan_table_row(
        &headers.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        &widths,
    ));
    out.push('\n');
    out.push_str(&scan_table_border('├', '┼', '┤', &widths, &table_indent));

    for row in &file.sample_rows {
        out.push('\n');
        out.push_str(&format!(
            "{:>width$}. {}",
            row.row_number,
            scan_table_row(
                &row.cells
                    .iter()
                    .map(|cell| cell.value.as_str())
                    .collect::<Vec<_>>(),
                &widths
            ),
            width = row_number_width
        ));
    }

    out.push('\n');
    out.push_str(&scan_table_border('└', '┴', '┘', &widths, &table_indent));
    out
}

fn scan_table_border(
    left: char,
    middle: char,
    right: char,
    widths: &[usize],
    indent: &str,
) -> String {
    let mut out = indent.to_string();
    out.push(left);
    for (index, width) in widths.iter().enumerate() {
        if index > 0 {
            out.push(middle);
        }
        out.push_str(&"─".repeat(*width + 2));
    }
    out.push(right);
    out
}

fn scan_table_row(values: &[&str], widths: &[usize]) -> String {
    let mut out = String::from("│");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push('│');
        }
        out.push(' ');
        out.push_str(value);
        let padding = widths[index].saturating_sub(value.chars().count());
        out.push_str(&" ".repeat(padding + 1));
    }
    out.push('│');
    out
}

/// Format a single cell value from an Arrow array for vertical display.
fn format_array_value(array: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};

    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::UInt64 => {
            let v = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row);
            format_number_with_hint(v as i128)
        }
        DataType::UInt32 => {
            let v = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row);
            v.to_string()
        }
        DataType::Int64 => {
            let v = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row);
            format_number_with_hint(v as i128)
        }
        DataType::Int32 => {
            let v = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row);
            v.to_string()
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            format!("{v}")
        }
        DataType::Boolean => {
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row);
            v.to_string()
        }
        DataType::Utf8 => {
            let v = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row);
            truncate_str(v, 80)
        }
        DataType::LargeUtf8 => {
            let v = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row);
            truncate_str(v, 80)
        }
        DataType::Binary => {
            let v = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row);
            truncate_str(&format!("0x{}", hex::encode(v)), 80)
        }
        DataType::LargeBinary => {
            let v = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(row);
            truncate_str(&format!("0x{}", hex::encode(v)), 80)
        }
        DataType::Timestamp(unit, timezone) => {
            let timezone_label = timezone.as_deref().filter(|tz| !tz.is_empty());
            match unit {
                TimeUnit::Second => format_timestamp_value(
                    array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(row),
                    0,
                    timezone_label,
                    1_000_000_000,
                ),
                TimeUnit::Millisecond => format_timestamp_value(
                    array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(row),
                    3,
                    timezone_label,
                    1_000_000,
                ),
                TimeUnit::Microsecond => format_timestamp_value(
                    array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(row),
                    6,
                    timezone_label,
                    1_000,
                ),
                TimeUnit::Nanosecond => format_timestamp_value(
                    array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(row),
                    9,
                    timezone_label,
                    1,
                ),
            }
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let inner = list.value(row);
            let items = format_list_items(inner.as_ref(), 5);
            let total = inner.len();
            if total > 5 {
                format!("[{items} ...] ({total} items)")
            } else {
                format!("[{items}]")
            }
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let inner = list.value(row);
            let items = format_list_items(inner.as_ref(), 5);
            let total = inner.len();
            if total > 5 {
                format!("[{items} ...] ({total} items)")
            } else {
                format!("[{items}]")
            }
        }
        _ => {
            // Fallback: use Arrow's Display formatting.
            let formatter =
                arrow::util::display::ArrayFormatter::try_new(array, &Default::default());
            match formatter {
                Ok(fmt) => fmt.value(row).to_string(),
                Err(_) => "<unsupported>".to_string(),
            }
        }
    }
}

fn format_timestamp_value(
    value: i64,
    fractional_digits: usize,
    timezone_label: Option<&str>,
    nanos_per_unit: i64,
) -> String {
    use time::OffsetDateTime;

    let nanos = i128::from(value) * i128::from(nanos_per_unit);
    let dt = match OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(dt) => dt,
        Err(_) => return format!("{value} (invalid timestamp)"),
    };

    let mut formatted = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        dt.year(),
        dt.month() as u8,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    );

    if fractional_digits > 0 {
        let fractional = match fractional_digits {
            3 => dt.nanosecond() / 1_000_000,
            6 => dt.nanosecond() / 1_000,
            9 => dt.nanosecond(),
            _ => 0,
        };
        formatted.push('.');
        formatted.push_str(&format!("{fractional:0width$}", width = fractional_digits));
    }

    if let Some(label) = timezone_label {
        formatted.push(' ');
        formatted.push_str(label);
    }

    formatted
}

/// Format list items (up to `max`) from an inner array.
fn format_list_items(array: &dyn arrow::array::Array, max: usize) -> String {
    let n = array.len().min(max);
    let items: Vec<String> = (0..n).map(|i| format_array_value(array, i)).collect();
    items.join(", ")
}

/// Truncate long strings and add ellipsis.
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}…", &s[..max_len])
    }
}

/// Format a large number with a human-readable hint (e.g. "397234292 -- 397.23M").
fn format_number_with_hint(v: i128) -> String {
    let abs = v.unsigned_abs();
    let hint = if abs >= 1_000_000_000_000 {
        format!(" -- {:.2}T", v as f64 / 1_000_000_000_000.0)
    } else if abs >= 1_000_000_000 {
        format!(" -- {:.2}B", v as f64 / 1_000_000_000.0)
    } else if abs >= 1_000_000 {
        format!(" -- {:.2}M", v as f64 / 1_000_000.0)
    } else {
        return v.to_string();
    };
    format!("{v}{hint}")
}

// ---------------------------------------------------------------------------
// Inspect
// ---------------------------------------------------------------------------

/// Inspect a single parquet file's metadata.
///
/// Displays file-level key-value metadata, Arrow schema, row group details,
/// and per-column chunk information.
/// Supports local filesystem paths and S3 URIs (`s3://bucket/key.parquet`).
/// Non-URI relative paths resolve locally first; when no local path exists and
/// `S3_BUCKET` is configured, they fall back to `s3://<bucket>/<path>`.
pub fn inspect_parquet(
    path: &str,
    schema_only: bool,
    json: bool,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<()> {
    match resolve_parquet_input_path(path) {
        ParquetInputPath::S3(path) => inspect_parquet_s3(
            &path,
            schema_only,
            json,
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?,
        ),
        ParquetInputPath::Local(path) => {
            inspect_parquet_local(path.to_string_lossy().as_ref(), schema_only, json)
        }
    }
}

/// Inspect a local parquet file.
fn inspect_parquet_local(path: &str, schema_only: bool, json: bool) -> anyhow::Result<()> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs;

    let file = fs::File::open(path).map_err(|e| anyhow::anyhow!("opening {path}: {e}"))?;
    let file_size = file.metadata()?.len();
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    print_inspect(path, file_size, metadata, schema_only, json)?;
    Ok(())
}

/// Inspect an S3 parquet file.
fn inspect_parquet_s3(
    path: &str,
    schema_only: bool,
    json: bool,
    aws: &AwsConfig,
) -> anyhow::Result<()> {
    use crate::writer::parse_s3_url;
    use object_store::aws::AmazonS3Builder;
    use object_store::ObjectStore;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let (bucket, key) = parse_s3_url(path)?;

    let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);
    if let Some(ref v) = aws.aws_access_key_id {
        builder = builder.with_access_key_id(v);
    }
    if let Some(ref v) = aws.aws_secret_access_key {
        builder = builder.with_secret_access_key(v);
    }
    if let Some(ref v) = aws.aws_session_token {
        builder = builder.with_token(v);
    }
    if let Some(ref v) = aws.aws_region {
        builder = builder.with_region(v);
    }
    if let Some(ref v) = aws.aws_endpoint_url {
        builder = builder.with_endpoint(v);
    }

    let client = builder
        .build()
        .map_err(|e| anyhow::anyhow!("building S3 client for bucket {bucket}: {e}"))?;

    let obj_path = object_store::path::Path::from(key.as_str());
    let data = block_on_async(async { client.get(&obj_path).await?.bytes().await })
        .map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;

    let file_size = data.len() as u64;
    let reader = SerializedFileReader::new(bytes::Bytes::from(data))
        .map_err(|e| anyhow::anyhow!("parsing parquet from {path}: {e}"))?;
    let metadata = reader.metadata();

    print_inspect(path, file_size, metadata, schema_only, json)?;
    Ok(())
}

/// Print the full inspection output for a parquet file.
fn print_inspect(
    path: &str,
    file_size: u64,
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema_only: bool,
    json: bool,
) -> anyhow::Result<()> {
    let file_meta = metadata.file_metadata();
    let num_row_groups = metadata.num_row_groups();
    let total_rows: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();
    let num_columns = file_meta.schema().get_fields().len();
    let schema = build_schema_fields(file_meta.schema().get_fields());

    if json {
        let output = if schema_only {
            serde_json::json!({
                "path": path,
                "schema": schema,
            })
        } else {
            serde_json::json!({
                "path": path,
                "file_size_bytes": file_size,
                "rows": total_rows,
                "row_groups": num_row_groups,
                "columns": num_columns,
                "created_by": file_meta.created_by(),
                "version": file_meta.version(),
                "schema": schema,
            })
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(());
    }

    if schema_only {
        println!("{}", "═".repeat(72));
        println!("  Schema: {}", path);
        println!("{}", "─".repeat(72));
        for field in file_meta.schema().get_fields() {
            print_schema_field(field, 1);
        }
        println!("{}", "═".repeat(72));
        return Ok(());
    }

    // Header
    println!("{}", "═".repeat(72));
    println!("  {}", path);
    println!("{}", "─".repeat(72));
    println!(
        "  rows: {}  row_groups: {}  columns: {}  size: {}",
        total_rows,
        num_row_groups,
        num_columns,
        format_bytes(file_size),
    );
    if let Some(created_by) = file_meta.created_by() {
        println!("  created_by: {}", created_by);
    }
    println!("  version: {}", file_meta.version());

    // File-level key-value metadata
    if let Some(kv_meta) = file_meta.key_value_metadata() {
        if !kv_meta.is_empty() {
            println!("\n{}", "─".repeat(72));
            println!("  File Metadata ({} entries)", kv_meta.len());
            println!("{}", "─".repeat(72));
            let max_key_len = kv_meta.iter().map(|kv| kv.key.len()).max().unwrap_or(0);
            for kv in kv_meta {
                let value = kv.value.as_deref().unwrap_or("(null)");
                // Truncate very long values (e.g. serialized Arrow schema)
                let display_value = if value.len() > 120 {
                    format!("{}… ({} bytes)", &value[..120], value.len())
                } else {
                    value.to_string()
                };
                println!(
                    "  {:width$}  {}",
                    kv.key,
                    display_value,
                    width = max_key_len
                );
            }
        }
    }

    // Schema
    println!("\n{}", "─".repeat(72));
    println!("  Schema");
    println!("{}", "─".repeat(72));

    // Use the parquet schema for detailed type info.
    for field in file_meta.schema().get_fields() {
        print_schema_field(field, 1);
    }

    // Row groups
    println!("\n{}", "─".repeat(72));
    println!("  Row Groups");
    println!("{}", "─".repeat(72));

    for (i, rg) in metadata.row_groups().iter().enumerate() {
        let compressed = rg.compressed_size();
        let uncompressed = rg.total_byte_size();
        let ratio = if uncompressed > 0 {
            format!("{:.1}x", uncompressed as f64 / compressed as f64)
        } else {
            "N/A".to_string()
        };
        println!(
            "  [{}]  rows: {}  compressed: {}  uncompressed: {}  ratio: {}",
            i,
            rg.num_rows(),
            format_bytes(compressed as u64),
            format_bytes(uncompressed as u64),
            ratio,
        );
    }

    // Column details (from first row group for encoding/compression info)
    if num_row_groups > 0 {
        let rg = metadata.row_groups().first().unwrap();
        println!("\n{}", "─".repeat(72));
        println!("  Column Details (row group 0)");
        println!("{}", "─".repeat(72));

        let max_col_name = rg
            .columns()
            .iter()
            .map(|c| c.column_path().string().len())
            .max()
            .unwrap_or(0);

        for col in rg.columns() {
            let col_path = col.column_path().string();
            let compression = format!("{:?}", col.compression());
            let encodings: Vec<String> = col.encodings().map(|e| format!("{:?}", e)).collect();
            let compressed = col.compressed_size();
            let uncompressed = col.uncompressed_size();
            let ratio = if uncompressed > 0 {
                format!("{:.1}x", uncompressed as f64 / compressed as f64)
            } else {
                "N/A".to_string()
            };
            println!(
                "  {:width$}  {}  {}  compressed: {}  uncompressed: {}  ratio: {}",
                col_path,
                compression,
                encodings.join("+"),
                format_bytes(compressed as u64),
                format_bytes(uncompressed as u64),
                ratio,
                width = max_col_name,
            );
        }
    }

    println!("\n{}", "═".repeat(72));
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize)]
struct InspectSchemaField {
    name: String,
    kind: String,
    physical_type: Option<String>,
    logical_type: Option<String>,
    repetition: String,
    nullable: bool,
    type_length: Option<i32>,
    children: Vec<InspectSchemaField>,
}

fn build_schema_fields(fields: &[parquet::schema::types::TypePtr]) -> Vec<InspectSchemaField> {
    fields
        .iter()
        .map(|field| build_schema_field(field))
        .collect()
}

fn build_schema_field(field: &parquet::schema::types::Type) -> InspectSchemaField {
    use parquet::basic::Repetition;
    use parquet::schema::types::Type;

    match field {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            type_length,
            ..
        } => InspectSchemaField {
            name: basic_info.name().to_string(),
            kind: "primitive".to_string(),
            physical_type: Some(format!("{:?}", physical_type)),
            logical_type: basic_info
                .logical_type_ref()
                .map(|value| format!("{:?}", value)),
            repetition: format!("{:?}", basic_info.repetition()).to_lowercase(),
            nullable: matches!(basic_info.repetition(), Repetition::OPTIONAL),
            type_length: (*type_length > 0).then_some(*type_length),
            children: Vec::new(),
        },
        Type::GroupType {
            basic_info, fields, ..
        } => InspectSchemaField {
            name: basic_info.name().to_string(),
            kind: "group".to_string(),
            physical_type: None,
            logical_type: basic_info
                .logical_type_ref()
                .map(|value| format!("{:?}", value)),
            repetition: format!("{:?}", basic_info.repetition()).to_lowercase(),
            nullable: matches!(basic_info.repetition(), Repetition::OPTIONAL),
            type_length: None,
            children: build_schema_fields(fields),
        },
    }
}

/// Print a parquet schema field with indentation (supports nested types).
fn print_schema_field(field: &parquet::schema::types::Type, indent: usize) {
    use parquet::basic::Repetition;
    use parquet::schema::types::Type;

    let prefix = "  ".repeat(indent);
    match field {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            type_length,
            ..
        } => {
            let repetition = format!("{:?}", basic_info.repetition());
            let nullable = matches!(basic_info.repetition(), Repetition::OPTIONAL);
            let logical = basic_info
                .logical_type_ref()
                .map(|lt| format!(" ({:?})", lt))
                .unwrap_or_default();
            let len_info = if *type_length > 0 {
                format!("({})", type_length)
            } else {
                String::new()
            };
            println!(
                "{}{:30} {:?}{}{}  {} nullable={}",
                prefix,
                basic_info.name(),
                physical_type,
                len_info,
                logical,
                repetition.to_lowercase(),
                nullable,
            );
        }
        Type::GroupType {
            basic_info, fields, ..
        } => {
            let repetition = format!("{:?}", basic_info.repetition());
            let nullable = matches!(basic_info.repetition(), Repetition::OPTIONAL);
            let logical = basic_info
                .logical_type_ref()
                .map(|lt| format!(" ({:?})", lt))
                .unwrap_or_default();
            println!(
                "{}{:30} group{}  {} nullable={}",
                prefix,
                basic_info.name(),
                logical,
                repetition.to_lowercase(),
                nullable,
            );
            for f in fields {
                print_schema_field(f, indent + 1);
            }
        }
    }
}

/// Recursively collect `.parquet` files from a directory.
fn collect_parquet_files(dir: &PathBuf, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, out)?;
        } else if path.extension().map_or(false, |ext| ext == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}

/// Human-readable byte size formatting.
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

// ---------------------------------------------------------------------------
// Validate
// ---------------------------------------------------------------------------

/// Result of a gap in block sequence.
#[derive(Debug)]
pub struct BlockGap {
    pub from: u64,
    pub to: u64,
}

/// Result of a parent hash mismatch.
#[derive(Debug)]
pub struct ParentMismatch {
    pub block_num: u64,
    pub expected_parent_id: String,
    pub actual_parent_id: String,
}

/// A duplicate block_num found during validation.
#[derive(Debug)]
pub struct DuplicateBlock {
    pub block_num: u64,
    pub count: u64,
}

/// A timestamp reversal between consecutive blocks.
#[derive(Debug)]
pub struct TimestampReversal {
    pub block_num: u64,
    pub timestamp: i64,
    pub prev_block_num: u64,
    pub prev_timestamp: i64,
}

/// An empty partition (no files or 0 rows).
#[derive(Debug)]
pub struct EmptyPartition {
    pub partition: String,
    pub files: usize,
    pub reason: &'static str,
}

/// A schema mismatch between files.
#[derive(Debug)]
pub struct SchemaMismatch {
    pub file: String,
    pub details: Vec<String>,
}

/// Cross-partition boundary issue.
#[derive(Debug)]
pub struct CrossPartitionIssue {
    pub from_partition: String,
    pub to_partition: String,
    pub gap: Option<BlockGap>,
    pub parent_mismatch: Option<ParentMismatch>,
}

/// Per-partition validation result.
#[derive(Debug)]
pub struct PartitionResult {
    pub partition: String,
    pub files_scanned: usize,
    pub total_blocks: u64,
    pub min_block: Option<u64>,
    pub max_block: Option<u64>,
    pub gaps: Vec<BlockGap>,
    pub parent_mismatches: Vec<ParentMismatch>,
    pub duplicates: Vec<DuplicateBlock>,
    pub ordering_errors: u64,
    pub timestamp_reversals: Vec<TimestampReversal>,
}

impl PartitionResult {
    pub fn is_valid(&self) -> bool {
        self.gaps.is_empty()
            && self.parent_mismatches.is_empty()
            && self.duplicates.is_empty()
            && self.ordering_errors == 0
    }
}

/// Options for validate.
#[derive(Debug, Default)]
pub struct ValidateOptions {
    pub cross_partition: bool,
    pub allow_gaps: bool,
}

/// Summary of a validate run (with per-partition breakdown).
#[derive(Debug)]
pub struct ValidateResult {
    pub files_scanned: usize,
    pub total_blocks: u64,
    pub min_block: Option<u64>,
    pub max_block: Option<u64>,
    pub gaps: Vec<BlockGap>,
    pub parent_mismatches: Vec<ParentMismatch>,
    pub duplicates: Vec<DuplicateBlock>,
    pub ordering_errors: u64,
    pub timestamp_reversals: Vec<TimestampReversal>,
    /// Per-partition results (empty when data is not partitioned).
    pub partitions: Vec<PartitionResult>,
    /// Empty partitions (warning, not failure).
    pub empty_partitions: Vec<EmptyPartition>,
    /// Schema mismatches across files.
    pub schema_mismatches: Vec<SchemaMismatch>,
    /// Cross-partition boundary issues (only when --cross-partition).
    pub cross_partition_issues: Vec<CrossPartitionIssue>,
}

impl ValidateResult {
    pub fn is_valid(&self) -> bool {
        self.gaps.is_empty()
            && self.parent_mismatches.is_empty()
            && self.duplicates.is_empty()
            && self.ordering_errors == 0
            && self.schema_mismatches.is_empty()
            && self.cross_partition_issues.is_empty()
            && self.partitions.iter().all(|p| p.is_valid())
    }

    pub fn print(&self, path: &str) {
        println!("Validating blocks in {} ...\n", path);

        // Per-partition breakdown (if partitioned).
        // Only show partitions with issues or warnings; clean ones are counted in the summary.
        if !self.partitions.is_empty() {
            let valid_count = self.partitions.iter().filter(|p| p.is_valid()).count();
            let invalid_count = self.partitions.len() - valid_count;

            println!(
                "  Partitions:        {} total, {} valid, {} with issues\n",
                self.partitions.len(),
                valid_count,
                invalid_count
            );

            let mut listed_any = false;
            for pr in &self.partitions {
                // Skip clean partitions to keep output compact. Timestamp reversals are
                // warnings, so a partition whose only finding is a reversal is still listed.
                if pr.is_valid() && pr.timestamp_reversals.is_empty() {
                    continue;
                }
                listed_any = true;

                let range = match (pr.min_block, pr.max_block) {
                    (Some(min), Some(max)) => format!("{} — {}", min, max),
                    _ => "N/A".to_string(),
                };
                let marker = if pr.is_valid() { "⚠" } else { "✗" };
                println!("  {} {}", marker, pr.partition);
                println!(
                    "    files: {}  blocks: {}  range: {}",
                    pr.files_scanned, pr.total_blocks, range
                );

                for gap in &pr.gaps {
                    let missing = gap.to - gap.from;
                    println!(
                        "    gap: {} — {} ({} blocks missing)",
                        gap.from,
                        gap.to - 1,
                        missing
                    );
                }
                for mm in &pr.parent_mismatches {
                    println!(
                        "    parent mismatch at block {}: expected {} got {}",
                        mm.block_num, mm.expected_parent_id, mm.actual_parent_id
                    );
                }
                for dup in &pr.duplicates {
                    println!(
                        "    duplicate block {} ({} occurrences)",
                        dup.block_num, dup.count
                    );
                }
                if pr.ordering_errors > 0 {
                    println!("    ordering errors: {}", pr.ordering_errors);
                }
                for tr in &pr.timestamp_reversals {
                    println!(
                        "    timestamp reversal at block {}: {} < previous block {} timestamp {}",
                        tr.block_num,
                        format_epoch_seconds(tr.timestamp),
                        tr.prev_block_num,
                        format_epoch_seconds(tr.prev_timestamp)
                    );
                }
            }
            if listed_any {
                println!();
            }
        }

        // Schema mismatches.
        if !self.schema_mismatches.is_empty() {
            println!("  Schema mismatches: {}", self.schema_mismatches.len());
            for sm in &self.schema_mismatches {
                println!("    {}:", sm.file);
                for detail in &sm.details {
                    println!("      {}", detail);
                }
            }
            println!();
        }

        // Empty partitions (warnings).
        if !self.empty_partitions.is_empty() {
            println!(
                "  Empty partitions:  {} (warning)",
                self.empty_partitions.len()
            );
            for ep in &self.empty_partitions {
                println!("    {} ({})", ep.partition, ep.reason);
            }
            println!();
        }

        // Cross-partition issues.
        if !self.cross_partition_issues.is_empty() {
            println!(
                "  Cross-partition issues: {}",
                self.cross_partition_issues.len()
            );
            for cpi in &self.cross_partition_issues {
                if let Some(ref gap) = cpi.gap {
                    let missing = gap.to - gap.from;
                    println!(
                        "    between {} and {}: gap {} — {} ({} blocks missing)",
                        cpi.from_partition,
                        cpi.to_partition,
                        gap.from,
                        gap.to - 1,
                        missing
                    );
                }
                if let Some(ref mm) = cpi.parent_mismatch {
                    println!(
                        "    between {} and {}: parent mismatch at block {}",
                        cpi.from_partition, cpi.to_partition, mm.block_num
                    );
                }
            }
            println!();
        }

        // Global summary.
        let range = match (self.min_block, self.max_block) {
            (Some(min), Some(max)) => format!("{} — {}", min, max),
            _ => "N/A".to_string(),
        };

        println!("  Files scanned:     {}", self.files_scanned);
        println!("  Block range:       {}", range);
        println!("  Total blocks:      {}", self.total_blocks);
        println!("  Ordering errors:   {}", self.ordering_errors);
        println!("  Gaps:              {}", self.gaps.len());

        if self.partitions.is_empty() {
            for gap in &self.gaps {
                let missing = gap.to - gap.from;
                println!(
                    "    gap: {} — {} ({} blocks missing)",
                    gap.from,
                    gap.to - 1,
                    missing
                );
            }
        }

        println!("  Parent mismatches: {}", self.parent_mismatches.len());
        if self.partitions.is_empty() {
            for mm in &self.parent_mismatches {
                println!(
                    "    block {}: expected parent_id {} but got {}",
                    mm.block_num, mm.expected_parent_id, mm.actual_parent_id
                );
            }
        }

        println!("  Duplicates:        {}", self.duplicates.len());
        if self.partitions.is_empty() {
            for dup in &self.duplicates {
                println!("    block {} ({} occurrences)", dup.block_num, dup.count);
            }
        }

        println!("  Timestamp reversals: {}", self.timestamp_reversals.len());
        if self.partitions.is_empty() {
            for tr in &self.timestamp_reversals {
                println!(
                    "    block {}: timestamp {} < previous block {} timestamp {}",
                    tr.block_num,
                    format_epoch_seconds(tr.timestamp),
                    tr.prev_block_num,
                    format_epoch_seconds(tr.prev_timestamp)
                );
            }
        }

        println!();
        if self.is_valid() {
            println!("  ✓ All blocks valid");
        } else {
            println!("  ✗ Validation failed");
        }

        // Warnings after the pass/fail line.
        if !self.empty_partitions.is_empty() && self.is_valid() {
            println!(
                "  ⚠ {} empty partition(s) detected (see above)",
                self.empty_partitions.len()
            );
        }
        if !self.timestamp_reversals.is_empty() && self.is_valid() {
            println!(
                "  ⚠ {} timestamp reversal(s) detected (see above); reported as warnings because some chains allow non-monotonic block times",
                self.timestamp_reversals.len()
            );
        }
    }
}

/// Render epoch seconds as UTC `YYYY-MM-DD HH:MM:SS`, falling back to the raw value.
fn format_epoch_seconds(timestamp: i64) -> String {
    format_partition_timestamp(timestamp).unwrap_or_else(|_| timestamp.to_string())
}

/// A block tuple: (block_num, block_id, parent_id, timestamp in epoch seconds).
///
/// The timestamp is `None` when the table has no `timestamp` column or the value is null.
type BlockTuple = (u64, String, String, Option<i64>);

/// Read a string value from a column that may be Utf8 or Binary.
fn read_id_string(
    col: &dyn arrow::array::Array,
    row: usize,
    col_name: &str,
) -> anyhow::Result<String> {
    use arrow::array::{BinaryArray, StringArray};
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        Ok(s.value(row).to_string())
    } else if let Some(b) = col.as_any().downcast_ref::<BinaryArray>() {
        Ok(hex::encode(b.value(row)))
    } else {
        Err(anyhow::anyhow!("{} column is not Utf8 or Binary", col_name))
    }
}

/// Read a `timestamp` column as epoch seconds, whatever its unit.
///
/// Accepts any Arrow `Timestamp` unit (the canonical column is `Timestamp(Second, UTC)`,
/// and finer units such as milliseconds are truncated to whole seconds) as well as
/// legacy `Int64` epoch seconds. Null values stay null.
fn timestamp_column_as_epoch_seconds(
    column: &dyn arrow::array::Array,
) -> anyhow::Result<arrow::array::Int64Array> {
    use arrow::array::AsArray;
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Int64Type, TimeUnit};

    if !matches!(
        column.data_type(),
        DataType::Timestamp(_, _) | DataType::Int64
    ) {
        anyhow::bail!(
            "timestamp column has unsupported type {}: expected Timestamp or Int64 epoch seconds",
            column.data_type()
        );
    }
    let seconds = cast(column, &DataType::Timestamp(TimeUnit::Second, None))?;
    Ok(cast(&seconds, &DataType::Int64)?
        .as_primitive::<Int64Type>()
        .clone())
}

/// Extract block tuples from a parquet record batch reader.
fn extract_block_tuples(
    reader: impl Iterator<Item = Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>>,
    block_num_idx: usize,
    block_id_idx: usize,
    parent_id_idx: usize,
    timestamp_idx: Option<usize>,
) -> anyhow::Result<Vec<BlockTuple>> {
    use arrow::array::{Array, UInt64Array};

    let mut tuples = Vec::new();
    for batch_result in reader {
        let batch = batch_result?;
        let block_nums = batch
            .column(block_num_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow::anyhow!("block_num column is not UInt64"))?;
        let block_id_col = batch.column(block_id_idx).as_ref();
        let parent_id_col = batch.column(parent_id_idx).as_ref();
        let timestamps = timestamp_idx
            .map(|idx| timestamp_column_as_epoch_seconds(batch.column(idx).as_ref()))
            .transpose()?;

        for i in 0..batch.num_rows() {
            let ts = timestamps
                .as_ref()
                .filter(|seconds| seconds.is_valid(i))
                .map(|seconds| seconds.value(i));
            tuples.push((
                block_nums.value(i),
                read_id_string(block_id_col, i, "block_id")?,
                read_id_string(parent_id_col, i, "parent_id")?,
                ts,
            ));
        }
    }
    Ok(tuples)
}

/// Column indices for validation.
struct CanonicalIndices {
    block_num: usize,
    block_id: usize,
    parent_id: usize,
    timestamp: Option<usize>,
}

/// Find column indices for canonical fields in a schema.
fn find_canonical_indices(schema: &arrow::datatypes::Schema) -> anyhow::Result<CanonicalIndices> {
    let block_num = schema
        .index_of("block_num")
        .map_err(|_| anyhow::anyhow!("missing 'block_num' column — is this a blocks table?"))?;
    let block_id = schema
        .index_of("block_id")
        .map_err(|_| anyhow::anyhow!("missing 'block_id' column"))?;
    let parent_id = schema
        .index_of("parent_id")
        .map_err(|_| anyhow::anyhow!("missing 'parent_id' column"))?;
    let timestamp = schema.index_of("timestamp").ok();
    Ok(CanonicalIndices {
        block_num,
        block_id,
        parent_id,
        timestamp,
    })
}

/// Compare two schemas and return a list of differences.
fn compare_schemas(
    reference: &arrow::datatypes::Schema,
    other: &arrow::datatypes::Schema,
) -> Vec<String> {
    use std::collections::HashMap;

    let ref_fields: HashMap<&str, &arrow::datatypes::Field> = reference
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();
    let other_fields: HashMap<&str, &arrow::datatypes::Field> = other
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    let mut diffs = Vec::new();

    // Check for missing columns and type mismatches.
    for (name, ref_field) in &ref_fields {
        match other_fields.get(name) {
            None => diffs.push(format!("missing column: {}", name)),
            Some(other_field) => {
                if ref_field.data_type() != other_field.data_type() {
                    diffs.push(format!(
                        "type mismatch: {} ({} vs {})",
                        name,
                        ref_field.data_type(),
                        other_field.data_type()
                    ));
                }
            }
        }
    }

    // Check for extra columns.
    for name in other_fields.keys() {
        if !ref_fields.contains_key(name) {
            diffs.push(format!("extra column: {}", name));
        }
    }

    diffs
}

/// Validate parquet files at the given path (local or S3).
pub fn validate_parquet(
    path: &str,
    aws: Option<&AwsConfig>,
    opts: &ValidateOptions,
) -> anyhow::Result<ValidateResult> {
    let path = resolve_parquet_input_path_string(path);
    if path.starts_with("s3://") {
        validate_parquet_s3(
            &path,
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?,
            opts,
        )
    } else {
        validate_parquet_local(&PathBuf::from(path), opts)
    }
}

/// Check results from check_tuples.
struct CheckResult {
    gaps: Vec<BlockGap>,
    parent_mismatches: Vec<ParentMismatch>,
    duplicates: Vec<DuplicateBlock>,
    ordering_errors: u64,
    timestamp_reversals: Vec<TimestampReversal>,
}

/// Check a sorted list of block tuples for gaps, ordering, parent hash chain, and timestamp issues.
fn check_tuples(tuples: &[BlockTuple]) -> CheckResult {
    let mut gaps = Vec::new();
    let mut parent_mismatches = Vec::new();
    let mut duplicates = Vec::new();
    let mut timestamp_reversals = Vec::new();
    let mut ordering_errors = 0u64;

    // Track runs of duplicate block_num.
    let mut dup_start = 0usize;
    // Last (block_num, timestamp) seen with a non-null timestamp, so null timestamps
    // (e.g. Solana blocks without block_time) neither hide nor fake a reversal.
    let mut last_timestamp = tuples.first().and_then(|t| t.3.map(|ts| (t.0, ts)));

    for i in 1..tuples.len() {
        let (prev_num, ref prev_block_id, _, _) = tuples[i - 1];
        let (curr_num, _, ref curr_parent_id, curr_ts) = tuples[i];

        if curr_num == prev_num {
            continue;
        }

        let run_len = i - dup_start;
        if run_len > 1 {
            duplicates.push(DuplicateBlock {
                block_num: tuples[dup_start].0,
                count: run_len as u64,
            });
        }
        dup_start = i;

        if curr_num < prev_num {
            ordering_errors += 1;
        }
        if curr_num > prev_num + 1 {
            gaps.push(BlockGap {
                from: prev_num + 1,
                to: curr_num,
            });
        }
        if curr_num == prev_num + 1 && curr_parent_id != prev_block_id {
            parent_mismatches.push(ParentMismatch {
                block_num: curr_num,
                expected_parent_id: prev_block_id.clone(),
                actual_parent_id: curr_parent_id.clone(),
            });
        }
        // Timestamp monotonicity check (#87).
        if let Some(curr_ts) = curr_ts {
            if let Some((last_num, last_ts)) = last_timestamp {
                if curr_ts < last_ts && curr_num > last_num {
                    timestamp_reversals.push(TimestampReversal {
                        block_num: curr_num,
                        timestamp: curr_ts,
                        prev_block_num: last_num,
                        prev_timestamp: last_ts,
                    });
                }
            }
            last_timestamp = Some((curr_num, curr_ts));
        }
    }

    // Final run check.
    if !tuples.is_empty() {
        let run_len = tuples.len() - dup_start;
        if run_len > 1 {
            duplicates.push(DuplicateBlock {
                block_num: tuples[dup_start].0,
                count: run_len as u64,
            });
        }
    }

    CheckResult {
        gaps,
        parent_mismatches,
        duplicates,
        ordering_errors,
        timestamp_reversals,
    }
}

/// Detect the partition key from a file path by looking for Hive-style directories
/// (e.g. `date=2026-01-01`, `block_range=0-100000`). Returns the partition directory
/// path relative to the base, or "(root)" if no partition structure is detected.
fn detect_partition(file_path: &str, base_path: &str) -> String {
    let relative = file_path
        .strip_prefix(base_path)
        .unwrap_or(file_path)
        .trim_start_matches('/');

    // Walk directory components, collect Hive-style partition segments.
    let parts: Vec<&str> = relative
        .split('/')
        .filter(|seg| seg.contains('=') && !seg.ends_with(".parquet"))
        .collect();

    if parts.is_empty() {
        "(root)".to_string()
    } else {
        parts.join("/")
    }
}

/// Per-file metadata collected during scanning.
struct FileInfo {
    path: String,
    partition: String,
    schema: arrow::datatypes::Schema,
    tuples: Vec<BlockTuple>,
    row_count: u64,
}

fn validate_from_files(files: Vec<FileInfo>, opts: &ValidateOptions) -> ValidateResult {
    // Schema consistency check (#90).
    let mut schema_mismatches = Vec::new();
    if let Some(first) = files.first() {
        let ref_schema = &first.schema;
        for f in files.iter().skip(1) {
            let diffs = compare_schemas(ref_schema, &f.schema);
            if !diffs.is_empty() {
                schema_mismatches.push(SchemaMismatch {
                    file: f.path.clone(),
                    details: diffs,
                });
            }
        }
    }

    // Group by partition.
    let mut groups: std::collections::BTreeMap<String, (Vec<BlockTuple>, usize, u64)> =
        std::collections::BTreeMap::new();
    for fi in files {
        let entry = groups
            .entry(fi.partition)
            .or_insert_with(|| (Vec::new(), 0, 0));
        entry.0.extend(fi.tuples);
        entry.1 += 1;
        entry.2 += fi.row_count;
    }

    // Detect empty partitions (#89).
    let mut empty_partitions = Vec::new();
    let mut partitions = Vec::new();
    let mut all_tuples: Vec<BlockTuple> = Vec::new();
    let mut total_files = 0usize;

    for (partition_name, (tuples, file_count, row_count)) in &groups {
        let mut tuples = tuples.clone();
        total_files += file_count;

        if *file_count == 0 {
            empty_partitions.push(EmptyPartition {
                partition: partition_name.clone(),
                files: 0,
                reason: "no files",
            });
            continue;
        }
        if *row_count == 0 {
            empty_partitions.push(EmptyPartition {
                partition: partition_name.clone(),
                files: *file_count,
                reason: &"0 rows across all files",
            });
            continue;
        }

        tuples.sort_by_key(|t| t.0);
        let cr = check_tuples(&tuples);
        let min_block = tuples.first().map(|t| t.0);
        let max_block = tuples.last().map(|t| t.0);

        partitions.push(PartitionResult {
            partition: partition_name.clone(),
            files_scanned: *file_count,
            total_blocks: tuples.len() as u64,
            min_block,
            max_block,
            gaps: cr.gaps,
            parent_mismatches: cr.parent_mismatches,
            duplicates: cr.duplicates,
            ordering_errors: cr.ordering_errors,
            timestamp_reversals: cr.timestamp_reversals,
        });

        all_tuples.extend(tuples.iter().cloned());
    }

    // Cross-partition continuity (#88).
    let mut cross_partition_issues = Vec::new();
    if opts.cross_partition && partitions.len() > 1 {
        // Sort partitions by their min_block.
        let mut sorted_parts: Vec<&PartitionResult> = partitions
            .iter()
            .filter(|p| p.min_block.is_some())
            .collect();
        sorted_parts.sort_by_key(|p| p.min_block);

        for i in 1..sorted_parts.len() {
            let prev = sorted_parts[i - 1];
            let curr = sorted_parts[i];

            if let (Some(prev_max), Some(curr_min)) = (prev.max_block, curr.min_block) {
                // Find the actual last and first tuples.
                // We need the block_id of prev's last block and parent_id of curr's first block.
                // We already have all_tuples, but let's check from the group data.
                let gap = if curr_min > prev_max + 1 {
                    Some(BlockGap {
                        from: prev_max + 1,
                        to: curr_min,
                    })
                } else {
                    None
                };

                // For parent mismatch, we need the actual block_id/parent_id.
                // Find them from all_tuples (sorted later).
                let parent_mismatch = if curr_min == prev_max + 1 {
                    // Find prev's last block and curr's first block in all_tuples.
                    let prev_last = all_tuples.iter().rfind(|t| t.0 == prev_max);
                    let curr_first = all_tuples.iter().find(|t| t.0 == curr_min);
                    match (prev_last, curr_first) {
                        (Some(pl), Some(cf)) if cf.2 != pl.1 => Some(ParentMismatch {
                            block_num: cf.0,
                            expected_parent_id: pl.1.clone(),
                            actual_parent_id: cf.2.clone(),
                        }),
                        _ => None,
                    }
                } else {
                    None
                };

                if gap.is_some() || parent_mismatch.is_some() {
                    cross_partition_issues.push(CrossPartitionIssue {
                        from_partition: prev.partition.clone(),
                        to_partition: curr.partition.clone(),
                        gap,
                        parent_mismatch,
                    });
                }
            }
        }
    }

    // When --allow-gaps is set, clear gaps from per-partition results and
    // cross-partition issues (e.g. Solana skipped slots are expected).
    if opts.allow_gaps {
        for p in &mut partitions {
            p.gaps.clear();
        }
        for cpi in &mut cross_partition_issues {
            cpi.gap = None;
        }
        // Remove cross-partition issues that only had a gap (no parent mismatch).
        cross_partition_issues.retain(|cpi| cpi.parent_mismatch.is_some());
    }

    // Global validation across all partitions.
    all_tuples.sort_by_key(|t| t.0);
    let cr = check_tuples(&all_tuples);

    // Only include per-partition breakdown if there are multiple partitions.
    let show_partitions = partitions.len() > 1;

    ValidateResult {
        files_scanned: total_files,
        total_blocks: all_tuples.len() as u64,
        min_block: all_tuples.first().map(|t| t.0),
        max_block: all_tuples.last().map(|t| t.0),
        gaps: if opts.allow_gaps { vec![] } else { cr.gaps },
        parent_mismatches: cr.parent_mismatches,
        duplicates: cr.duplicates,
        ordering_errors: cr.ordering_errors,
        timestamp_reversals: cr.timestamp_reversals,
        partitions: if show_partitions { partitions } else { vec![] },
        empty_partitions,
        schema_mismatches,
        cross_partition_issues,
    }
}

fn validate_parquet_local(
    path: &PathBuf,
    opts: &ValidateOptions,
) -> anyhow::Result<ValidateResult> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut paths: Vec<PathBuf> = Vec::new();
    if path.is_file() {
        paths.push(path.clone());
    } else if path.is_dir() {
        collect_parquet_files(path, &mut paths)?;
        paths.sort();
    } else {
        anyhow::bail!("path does not exist: {}", path.display());
    }

    if paths.is_empty() {
        println!("No .parquet files found in {}", path.display());
        return Ok(ValidateResult {
            files_scanned: 0,
            total_blocks: 0,
            min_block: None,
            max_block: None,
            gaps: vec![],
            parent_mismatches: vec![],
            duplicates: vec![],
            ordering_errors: 0,
            timestamp_reversals: vec![],
            partitions: vec![],
            empty_partitions: vec![],
            schema_mismatches: vec![],
            cross_partition_issues: vec![],
        });
    }

    let base = path.to_string_lossy().to_string();
    let mut file_infos = Vec::new();

    for file_path in &paths {
        let file = std::fs::File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema();
        let arrow_schema: arrow::datatypes::Schema = (**schema).clone();
        let indices = find_canonical_indices(&arrow_schema)?;
        let metadata = builder.metadata().clone();
        let row_count: u64 = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as u64)
            .sum();
        let reader = builder.build()?;
        let tuples = extract_block_tuples(
            reader,
            indices.block_num,
            indices.block_id,
            indices.parent_id,
            indices.timestamp,
        )?;

        let partition_key = detect_partition(&file_path.to_string_lossy(), &base);
        let display = file_path
            .strip_prefix(path)
            .unwrap_or(file_path)
            .to_string_lossy()
            .to_string();

        file_infos.push(FileInfo {
            path: display,
            partition: partition_key,
            schema: arrow_schema,
            tuples,
            row_count,
        });
    }

    Ok(validate_from_files(file_infos, opts))
}

fn validate_parquet_s3(
    path: &str,
    aws: &AwsConfig,
    opts: &ValidateOptions,
) -> anyhow::Result<ValidateResult> {
    use crate::writer::parse_s3_url;
    use object_store::ObjectStore;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let (bucket, prefix) = parse_s3_url(path)?;
    let client = aws.build_s3_client(&bucket)?;

    let list_prefix = if prefix.is_empty() {
        None
    } else {
        Some(object_store::path::Path::from(prefix.as_str()))
    };

    let objects: Vec<object_store::ObjectMeta> = block_on_async(async {
        use futures::TryStreamExt;
        client.list(list_prefix.as_ref()).try_collect().await
    })
    .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let mut parquet_objects: Vec<_> = objects
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .collect();
    parquet_objects.sort_by(|a, b| a.location.cmp(&b.location));

    if parquet_objects.is_empty() {
        println!("No .parquet files found in {path}");
        return Ok(ValidateResult {
            files_scanned: 0,
            total_blocks: 0,
            min_block: None,
            max_block: None,
            gaps: vec![],
            parent_mismatches: vec![],
            duplicates: vec![],
            ordering_errors: 0,
            timestamp_reversals: vec![],
            partitions: vec![],
            empty_partitions: vec![],
            schema_mismatches: vec![],
            cross_partition_issues: vec![],
        });
    }

    let mut file_infos = Vec::new();

    for obj in &parquet_objects {
        let data = block_on_async(async { client.get(&obj.location).await?.bytes().await })
            .map_err(|e| anyhow::anyhow!("reading s3://{bucket}/{}: {e}", obj.location))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        let schema = builder.schema();
        let arrow_schema: arrow::datatypes::Schema = (**schema).clone();
        let indices = find_canonical_indices(&arrow_schema)?;
        let metadata = builder.metadata().clone();
        let row_count: u64 = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as u64)
            .sum();
        let reader = builder.build()?;
        let tuples = extract_block_tuples(
            reader,
            indices.block_num,
            indices.block_id,
            indices.parent_id,
            indices.timestamp,
        )?;

        let partition_key = detect_partition(obj.location.as_ref(), &prefix);
        let display = obj
            .location
            .as_ref()
            .strip_prefix(&prefix)
            .map(|s| s.trim_start_matches('/'))
            .unwrap_or(obj.location.as_ref())
            .to_string();

        file_infos.push(FileInfo {
            path: display,
            partition: partition_key,
            schema: arrow_schema,
            tuples,
            row_count,
        });
    }

    Ok(validate_from_files(file_infos, opts))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Compression, Partition};
    use crate::traits::BlockIdentity;
    use clap::{CommandFactory, Parser};
    use serial_test::serial;

    /// Minimal CLI wrapper used only for testing CommonArgs parsing.
    #[derive(Parser, Debug)]
    #[command(name = "test-cli")]
    struct TestCli {
        #[command(flatten)]
        common: CommonArgs,

        #[command(subcommand)]
        command: Option<Commands>,
    }

    fn parse(args: &[&str]) -> TestCli {
        TestCli::parse_from(args)
    }

    fn try_parse(args: &[&str]) -> Result<TestCli, clap::Error> {
        TestCli::try_parse_from(args)
    }

    struct CurrentDirGuard {
        previous: PathBuf,
    }

    impl CurrentDirGuard {
        fn set(path: &Path) -> Self {
            let previous = std::env::current_dir().expect("current dir");
            std::env::set_current_dir(path).expect("set current dir");
            Self { previous }
        }
    }

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            std::env::set_current_dir(&self.previous).expect("restore current dir");
        }
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = std::env::var(key).ok();
            std::env::remove_var(key);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    fn write_test_partitions_index(
        path: &std::path::Path,
        rows: Vec<PartitionBuildRow>,
    ) -> anyhow::Result<()> {
        write_partitions_index(&path.to_string_lossy(), &rows, None)
    }

    fn write_scan_test_parquet(path: &std::path::Path, values: &[i32]) {
        use arrow::array::Int32Array;
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::fs::File;
        use std::sync::Arc;

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parquet parent");
        }

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .expect("record batch");
        let file = File::create(path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create arrow writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }

    fn time_partition_row(
        chain: Option<&str>,
        partition_type: &str,
        partition_value: &str,
        start_block: u64,
        stop_block: u64,
    ) -> PartitionBuildRow {
        PartitionBuildRow {
            partition_type: partition_type.to_string(),
            partition_interval_seconds: PartitionBuildType::from_cli_value(partition_type)
                .expect("valid partition type")
                .interval_seconds(),
            partition_start_ts: partition_value.to_string(),
            partition_value: partition_value.to_string(),
            start_block,
            stop_block,
            start_time: None,
            end_time: None,
            chain: chain.map(str::to_string),
        }
    }

    #[test]
    #[serial]
    fn test_required_endpoint() {
        // endpoint is optional at the clap level (for subcommands like completions)
        // but build_config will fail without it
        let cli = parse(&["test-cli"]);
        assert!(cli.common.endpoint.is_none());
        assert!(build_config(&cli.common).is_err());
    }

    #[test]
    #[serial]
    fn test_defaults() {
        // Clear any AWS env vars that may leak from .env
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
            std::env::remove_var("AWS_SESSION_TOKEN");
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_ENDPOINT_URL_S3");
            std::env::remove_var("S3_BUCKET");
        }
        let cli = parse(&["test-cli", "--endpoint", "http://localhost:9000"]);
        assert_eq!(
            cli.common.endpoint.as_deref(),
            Some("http://localhost:9000")
        );
        assert_eq!(cli.common.output, PathBuf::from("."));
        assert_eq!(cli.common.partition, "none");
        assert_eq!(cli.common.block_range_size, 10000);
        assert!(cli.common.flush_rows.is_none());
        assert!(cli.common.flush_blocks.is_none());
        assert_eq!(cli.common.flush_bytes, DEFAULT_FLUSH_BYTES);
        assert_eq!(cli.common.compression, "zstd");
        assert_eq!(cli.common.log_level, "info");
        assert!(!cli.common.verbose);
        assert!(!cli.common.dry_run);
        assert!(cli.common.final_blocks_only);
        assert_eq!(cli.common.api_key_envvar, "SUBSTREAMS_API_KEY");
        assert_eq!(cli.common.api_token_envvar, "SUBSTREAMS_API_TOKEN");
        assert!(cli.common.start_block.is_none());
        assert!(cli.common.stop_block.is_none());
        assert_eq!(cli.common.cursor, PathBuf::from("cursor.parquet"));
        assert!(cli.common.cursor_template.is_none());
        assert!(cli.common.flush_interval_secs.is_none());
        assert!(cli.common.aws_access_key_id.is_none());
        assert!(cli.common.aws_secret_access_key.is_none());
        assert!(cli.common.aws_session_token.is_none());
        assert!(cli.common.aws_region.is_none());
        assert!(cli.common.aws_endpoint_url.is_none());
        assert!(cli.common.s3_bucket.is_none());
        assert_eq!(cli.common.stream_idle_timeout_secs, Some(120));
        assert_eq!(cli.common.reconnect_stall_timeout_secs, Some(900));
    }

    #[test]
    #[serial]
    fn test_all_flags() {
        let cli = parse(&[
            "test-cli",
            "-e",
            "https://eth.firehose.pinax.network:443",
            "--api-key-envvar",
            "MY_KEY_VAR",
            "--api-token-envvar",
            "MY_TOKEN_VAR",
            "-s",
            "100",
            "-t",
            "200",
            "-c",
            "cursor-mainnet-date.parquet",
            "--output",
            "/tmp/out",
            "--partition",
            "date",
            "--block-range-size",
            "5000",
            "--flush-rows",
            "10000",
            "--flush-blocks",
            "250",
            "--flush-bytes",
            "1000000",
            "--flush-interval-secs",
            "60",
            "--stream-idle-timeout-secs",
            "45",
            "--reconnect-stall-timeout-secs",
            "120",
            "--compression",
            "snappy",
            "--log-level",
            "debug",
            "--verbose",
            "--dry-run",
        ]);
        assert_eq!(
            cli.common.endpoint.as_deref(),
            Some("https://eth.firehose.pinax.network:443")
        );
        assert_eq!(cli.common.api_key_envvar, "MY_KEY_VAR");
        assert_eq!(cli.common.api_token_envvar, "MY_TOKEN_VAR");
        assert_eq!(cli.common.start_block, Some(100));
        assert_eq!(cli.common.stop_block, Some(200));
        assert_eq!(
            cli.common.cursor,
            PathBuf::from("cursor-mainnet-date.parquet")
        );
        assert_eq!(cli.common.output, PathBuf::from("/tmp/out"));
        assert_eq!(cli.common.partition, "date");
        assert_eq!(cli.common.block_range_size, 5000);
        assert_eq!(cli.common.flush_rows, Some(10000));
        assert_eq!(cli.common.flush_blocks, Some(250));
        assert_eq!(cli.common.flush_bytes, 1000000);
        assert_eq!(cli.common.flush_interval_secs, Some(60));
        assert_eq!(cli.common.stream_idle_timeout_secs, Some(45));
        assert_eq!(cli.common.reconnect_stall_timeout_secs, Some(120));
        assert_eq!(cli.common.compression, "snappy");
        assert_eq!(cli.common.log_level, "debug");
        assert!(cli.common.verbose);
        assert!(cli.common.dry_run);
    }

    #[test]
    fn test_effective_log_level_promotes_info_when_verbose() {
        assert_eq!(effective_log_level("info", true), "debug");
        assert_eq!(effective_log_level("INFO", true), "debug");
        assert_eq!(effective_log_level("Info", true), "debug");
        assert_eq!(effective_log_level("debug", true), "debug");
        assert_eq!(effective_log_level("warn", true), "warn");
        assert_eq!(effective_log_level("info", false), "info");
    }

    #[test]
    #[serial]
    fn test_parse_compression() {
        assert_eq!(parse_compression("zstd").unwrap(), Compression::Zstd);
        assert_eq!(parse_compression("snappy").unwrap(), Compression::Snappy);
        assert_eq!(parse_compression("gzip").unwrap(), Compression::Gzip);
        assert_eq!(parse_compression("none").unwrap(), Compression::None);
        assert_eq!(parse_compression("ZSTD").unwrap(), Compression::Zstd);
        assert!(parse_compression("unknown").is_err());
    }

    #[test]
    #[serial]
    fn test_parse_partition() {
        assert_eq!(parse_partition("none", 10000).unwrap(), Partition::None);
        assert_eq!(parse_partition("date", 10000).unwrap(), Partition::Date);
        assert_eq!(parse_partition("hour", 10000).unwrap(), Partition::Hour);
        assert_eq!(parse_partition("minute", 10000).unwrap(), Partition::Minute);
        assert_eq!(parse_partition("second", 10000).unwrap(), Partition::Second);
        assert_eq!(
            parse_partition("block_range", 5000).unwrap(),
            Partition::block_range(5000)
        );
        assert_eq!(
            parse_partition("BLOCK_RANGE", 20000).unwrap(),
            Partition::block_range(20000)
        );
        assert!(parse_partition("unknown", 10000).is_err());
    }

    #[test]
    fn test_detect_partition_accepts_exclusive_block_range_label() {
        assert_eq!(
            detect_partition(
                "/tmp/output/blocks/block_range=390500000-390600000/part-000001.parquet",
                "/tmp/output"
            ),
            "block_range=390500000-390600000"
        );
    }

    #[test]
    #[serial]
    fn test_build_config() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--start-block",
            "100",
            "--compression",
            "gzip",
            "--partition",
            "date",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.endpoint, "https://example.com:443");
        assert_eq!(config.start_block, Some(100));
        assert!(config.skip_missing_blocks);
        assert_eq!(config.compression, Compression::Gzip);
        assert_eq!(config.partition, Partition::Date);
        assert!(config.flush_rows.is_none());
        assert!(config.flush_blocks.is_none());
        assert!(config.final_blocks_only);
        assert_eq!(config.stream_idle_timeout_secs, Some(120));
        assert_eq!(config.reconnect_stall_timeout_secs, Some(900));
        // cursor defaults to cursor.parquet
        assert_eq!(config.cursor_path, Some("cursor.parquet".to_string()));
    }

    fn assert_rejected_value(args: &[&str], flag: &str) {
        let error = try_parse(args).expect_err("zero should be rejected");
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(error.to_string().contains(flag), "{error}");
    }

    #[test]
    #[serial]
    fn test_block_range_size_zero_is_rejected() {
        assert_rejected_value(
            &[
                "test-cli",
                "--partition",
                "block_range",
                "--block-range-size",
                "0",
            ],
            "--block-range-size",
        );
        assert!(parse_partition("block_range", 0).is_err());
    }

    #[test]
    fn test_partitions_build_block_range_size_zero_is_rejected() {
        assert_rejected_value(
            &[
                "test-cli",
                "partitions",
                "build",
                "--endpoint",
                "https://eth.firehose.pinax.network:443",
                "--stop-block",
                "200",
                "--partition",
                "block_range",
                "--block-range-size",
                "0",
                "--output",
                "./output",
            ],
            "--block-range-size",
        );
    }

    #[test]
    #[serial]
    fn test_stop_block_zero_is_rejected() {
        assert_rejected_value(&["test-cli", "--stop-block", "0"], "--stop-block");
    }

    #[test]
    #[serial]
    fn test_build_config_rejects_stop_block_not_after_start_block() {
        for (start, stop) in [("100", "100"), ("100", "50"), ("0", "0")] {
            let cli = TestCli::try_parse_from([
                "test-cli",
                "--endpoint",
                "https://example.com:443",
                "--start-block",
                start,
                "--stop-block",
                stop,
            ]);
            // --stop-block 0 is already rejected by the parser.
            let Ok(cli) = cli else { continue };
            let error = build_config(&cli.common).expect_err("empty range");
            assert!(
                error.to_string().contains("must be greater than"),
                "{error}"
            );
        }

        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--start-block",
            "0",
            "--stop-block",
            "1",
        ]);
        let config = build_config(&cli.common).expect("block 0 only is a valid range");
        assert_eq!(config.stop_block, Some(1));
        assert!(validate_stop_block_after_start(None, Some(1)).is_ok());
        assert!(validate_stop_block_after_start(Some(5), None).is_ok());
    }

    #[test]
    #[serial]
    fn test_zero_connection_timeouts_mean_disabled() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--stream-idle-timeout-secs",
            "0",
            "--reconnect-stall-timeout-secs",
            "0",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.stream_idle_timeout_secs, None);
        assert_eq!(config.reconnect_stall_timeout_secs, None);
        let rendered = config.to_string();
        assert!(
            rendered.contains("stream_idle_timeout disabled"),
            "{rendered}"
        );
        assert!(
            rendered.contains("reconnect_stall_timeout disabled"),
            "{rendered}"
        );
    }

    #[test]
    #[serial]
    fn test_flush_bytes_zero_means_disabled() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--flush-bytes",
            "0",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.flush_bytes, 0);
        assert!(config.to_string().contains("flush_bytes        disabled"));
    }

    #[test]
    #[serial]
    fn test_credentials_are_trimmed_and_blank_values_ignored() {
        let _key = EnvVarGuard::set("FIREPARQ_TEST_API_KEY_471", "key-from-secret-file\n");
        let _token = EnvVarGuard::set("FIREPARQ_TEST_API_TOKEN_471", " \n");
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--api-key-envvar",
            "FIREPARQ_TEST_API_KEY_471",
            "--api-token-envvar",
            "FIREPARQ_TEST_API_TOKEN_471",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.api_key.as_deref(), Some("key-from-secret-file"));
        assert_eq!(config.jwt_token, None);
    }

    #[test]
    #[serial]
    fn test_stop_block_can_be_omitted_for_inferred_live_mode() {
        let cli = parse(&["test-cli", "--endpoint", "https://example.com:443"]);

        assert!(cli.common.stop_block.is_none());
    }

    #[test]
    #[serial]
    fn test_cursor_must_be_parquet() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--cursor",
            "cursor.txt",
        ]);
        let result = build_config(&cli.common);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(".parquet"));
    }

    #[test]
    #[serial]
    fn test_cursor_custom_parquet_name() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--cursor",
            "cursor-mainnet-date.parquet",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(
            config.cursor_path,
            Some("cursor-mainnet-date.parquet".to_string())
        );
    }

    #[test]
    #[serial]
    fn test_cursor_template_must_end_in_parquet() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--cursor-template",
            "cursor/{partition_type}/{partition_value}.txt",
        ]);
        let result = build_config(&cli.common);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("--cursor-template must end in .parquet"));
    }

    #[test]
    fn test_resolve_cursor_template_single_partition() {
        let context = CursorTemplateContext {
            chain: Some("eth-mainnet".to_string()),
            partition_type: Some("hour".to_string()),
            partition_value: Some("2015-07-30 15:00:00".to_string()),
            partition_from: None,
            partition_to: None,
        };
        let resolved = resolve_cursor_template(
            "cursor/{chain}/{partition_type}/{partition_value}.parquet",
            &context,
        )
        .expect("template should resolve");
        assert_eq!(
            resolved,
            "cursor/eth-mainnet/hour/2015-07-30 15:00:00.parquet"
        );
    }

    #[test]
    fn test_resolve_cursor_template_window_partition() {
        let context = CursorTemplateContext {
            chain: Some("eth-mainnet".to_string()),
            partition_type: Some("hour".to_string()),
            partition_value: None,
            partition_from: Some("2015/07/30 14:00:00".to_string()),
            partition_to: Some("2015-07-30 18:00:00".to_string()),
        };
        let resolved = resolve_cursor_template(
            "cursor/{chain}/{partition_type}/{partition_from}-{partition_to}.parquet",
            &context,
        )
        .expect("template should resolve");
        assert_eq!(
            resolved,
            "cursor/eth-mainnet/hour/2015_07_30 14:00:00-2015-07-30 18:00:00.parquet"
        );
    }

    #[test]
    fn test_resolve_cursor_template_requires_context() {
        let context = CursorTemplateContext {
            chain: None,
            partition_type: None,
            partition_value: None,
            partition_from: None,
            partition_to: None,
        };
        let err = resolve_cursor_template("cursor/{partition_value}.parquet", &context)
            .expect_err("missing context should fail");
        assert!(err
            .to_string()
            .contains("requires partition selection context"));
    }

    #[test]
    #[serial]
    fn test_completions_subcommand_parse() {
        let cli = parse(&["test-cli", "completions", "bash"]);
        assert!(cli.command.is_some());
        match cli.command.unwrap() {
            Commands::Completions { shell } => assert_eq!(shell, Shell::Bash),
            _ => panic!("expected Completions subcommand"),
        }
    }

    #[test]
    fn test_partitions_resolve_subcommand_parse() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "resolve",
            "--partitions-index",
            "./partitions.parquet",
            "--partition-type",
            "hour",
            "--partition-value",
            "2015-07-30 15:00:00",
            "--partition-chain",
            "eth-mainnet",
            "--strict-single-chain",
            "--json",
        ]);
        assert!(cli.command.is_some());
        match cli.command.unwrap() {
            Commands::Partitions(PartitionsCommands::Resolve {
                partitions_index,
                partition_type,
                partition_value,
                partition_chain,
                strict_single_chain,
                json,
                ..
            }) => {
                assert_eq!(partitions_index, "./partitions.parquet");
                assert_eq!(partition_type, "hour");
                assert_eq!(partition_value, "2015-07-30 15:00:00");
                assert_eq!(partition_chain.as_deref(), Some("eth-mainnet"));
                assert!(strict_single_chain);
                assert!(json);
            }
            _ => panic!("expected partitions resolve subcommand"),
        }
    }

    #[test]
    fn test_partitions_build_subcommand_parse() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "build",
            "--endpoint",
            "https://eth.firehose.pinax.network:443",
            "--stop-block",
            "200",
            "--partition",
            "date",
            "--output",
            "./output",
            "--resume",
            "--json",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Partitions(PartitionsCommands::Build {
                endpoint,
                start_block,
                stop_block,
                live,
                partition,
                compression,
                output,
                s3_bucket,
                resume,
                overwrite,
                json,
                ..
            }) => {
                assert_eq!(
                    endpoint.as_deref(),
                    Some("https://eth.firehose.pinax.network:443")
                );
                assert_eq!(start_block, None);
                assert_eq!(stop_block, Some(200));
                assert!(!live);
                assert_eq!(partition, "date");
                assert_eq!(compression, "zstd");
                assert_eq!(output.as_deref(), Some("./output"));
                assert!(s3_bucket.is_none());
                assert!(resume);
                assert!(!overwrite);
                assert!(json);
            }
            _ => panic!("expected partitions build subcommand"),
        }
    }

    #[test]
    fn test_partitions_build_subcommand_parse_overwrite() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "build",
            "--endpoint",
            "https://eth.firehose.pinax.network:443",
            "--stop-block",
            "200",
            "--partition",
            "date",
            "--output",
            "./output",
            "--overwrite",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Partitions(PartitionsCommands::Build { overwrite, .. }) => {
                assert!(overwrite);
            }
            _ => panic!("expected partitions build subcommand"),
        }
    }

    #[test]
    fn test_partitions_build_subcommand_rejects_resume_with_overwrite() {
        let err = TestCli::try_parse_from([
            "test-cli",
            "partitions",
            "build",
            "--endpoint",
            "https://eth.firehose.pinax.network:443",
            "--stop-block",
            "200",
            "--partition",
            "date",
            "--output",
            "./output",
            "--resume",
            "--overwrite",
        ])
        .expect_err("resume and overwrite should conflict");

        let rendered = err.to_string();
        assert!(rendered.contains("--resume"));
        assert!(rendered.contains("--overwrite"));
    }

    #[test]
    fn test_partitions_build_subcommand_parse_s3_bucket_without_output() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "build",
            "--endpoint",
            "https://eth.firehose.pinax.network:443",
            "--stop-block",
            "200",
            "--partition",
            "hour",
            "--s3-bucket",
            "my-bucket",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Partitions(PartitionsCommands::Build {
                output, s3_bucket, ..
            }) => {
                assert!(output.is_none());
                assert_eq!(s3_bucket.as_deref(), Some("my-bucket"));
            }
            _ => panic!("expected partitions build subcommand"),
        }
    }

    #[test]
    fn test_partitions_build_subcommand_live_parse() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "build",
            "--network",
            "mainnet",
            "--partition",
            "date",
            "--output",
            "./output",
            "--live",
            "--poll-interval-secs",
            "15",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Partitions(PartitionsCommands::Build {
                network,
                stop_block,
                live,
                poll_interval_secs,
                partition,
                compression,
                output,
                ..
            }) => {
                assert_eq!(network.as_deref(), Some("mainnet"));
                assert_eq!(stop_block, None);
                assert!(live);
                assert_eq!(poll_interval_secs, 15);
                assert_eq!(partition, "date");
                assert_eq!(compression, "zstd");
                assert_eq!(output.as_deref(), Some("./output"));
            }
            _ => panic!("expected partitions build subcommand"),
        }
    }

    #[test]
    fn test_partitions_build_subcommand_rejects_removed_skip_missing_blocks_flag() {
        let err = TestCli::try_parse_from([
            "test-cli",
            "partitions",
            "build",
            "--network",
            "solana-mainnet-beta",
            "--partition",
            "date",
            "--output",
            "./output",
            "--live",
            "--skip-missing-blocks",
        ])
        .expect_err("removed skip-missing-blocks flag should fail clap parsing");

        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
        let rendered = err.to_string();
        assert!(rendered.contains("--skip-missing-blocks"));
        assert!(rendered.contains("unexpected argument"));
    }

    #[test]
    fn test_partitions_build_subcommand_compression_override_parse() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "build",
            "--endpoint",
            "https://eth.firehose.pinax.network:443",
            "--stop-block",
            "200",
            "--partition",
            "date",
            "--compression",
            "snappy",
            "--output",
            "./output",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Partitions(PartitionsCommands::Build { compression, .. }) => {
                assert_eq!(compression, "snappy");
            }
            _ => panic!("expected partitions build subcommand"),
        }
    }

    #[test]
    fn test_partitions_build_block_range_parse() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "build",
            "--endpoint",
            "https://sol.firehose.pinax.network:443",
            "--stop-block",
            "10000000",
            "--partition",
            "block_range",
            "--block-range-size",
            "1000000",
            "--output",
            "./output",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Partitions(PartitionsCommands::Build {
                partition,
                block_range_size,
                ..
            }) => {
                assert_eq!(partition, "block_range");
                assert_eq!(block_range_size, Some(1000000));
            }
            _ => panic!("expected partitions build subcommand"),
        }
    }

    #[test]
    fn test_partitions_build_live_block_range_parse_without_stop_block() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "build",
            "--network",
            "solana-mainnet-beta",
            "--partition",
            "block_range",
            "--block-range-size",
            "1000000",
            "--output",
            "./output",
            "--live",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Partitions(PartitionsCommands::Build {
                network,
                stop_block,
                live,
                partition,
                block_range_size,
                ..
            }) => {
                assert_eq!(network.as_deref(), Some("solana-mainnet-beta"));
                assert_eq!(stop_block, None);
                assert!(live);
                assert_eq!(partition, "block_range");
                assert_eq!(block_range_size, Some(1000000));
            }
            _ => panic!("expected partitions build subcommand"),
        }
    }

    #[test]
    fn test_partitions_build_help_uses_grouped_headings() {
        let cmd = TestCli::command();
        let partitions = cmd
            .get_subcommands()
            .find(|subcmd| subcmd.get_name() == "partitions")
            .expect("partitions subcommand should exist");
        let build = partitions
            .get_subcommands()
            .find(|subcmd| subcmd.get_name() == "build")
            .expect("partitions build subcommand should exist");

        let help = build.clone().render_long_help().to_string();

        for heading in [
            "Connection:",
            "Block Range:",
            "Output:",
            "AWS / S3:",
            "Partitioning:",
            "Runtime / Logging:",
        ] {
            assert!(
                help.contains(heading),
                "expected help to contain heading `{heading}`\n{help}"
            );
        }

        assert!(help.contains("--aws-region"));
        assert!(help.contains("--partition"));
        assert!(help.contains("--json"));
        assert!(!help.contains("--strict-timestamps"));
    }

    #[test]
    fn test_build_help_clarifies_flush_semantics() {
        let cmd = TestCli::command();
        let build = cmd
            .get_subcommands()
            .find(|subcmd| subcmd.get_name() == "build")
            .expect("build subcommand should exist");

        let help = build.clone().render_long_help().to_string();

        assert!(help.contains("Flush mapper state after this many rows"));
        assert!(help.contains("Flush written files after this many processed blocks"));
        assert!(help.contains("does not guarantee parquet files are materialized"));
        assert!(help.contains(
            "Flush mapper state at this many in-memory bytes and target roughly this many compressed bytes per parquet file"
        ));
        assert!(help.contains("Flush mapper state every N seconds"));
    }

    #[test]
    fn test_merge_help_aligns_flush_controls_with_build() {
        let cmd = TestCli::command();
        let build = cmd
            .get_subcommands()
            .find(|subcmd| subcmd.get_name() == "build")
            .expect("build subcommand should exist");
        let merge = cmd
            .get_subcommands()
            .find(|subcmd| subcmd.get_name() == "merge")
            .expect("merge subcommand should exist");

        let build_help = build.clone().render_long_help().to_string();
        let merge_help = merge.clone().render_long_help().to_string();
        let default_flush_bytes = DEFAULT_FLUSH_BYTES.to_string();

        assert!(build_help.contains(&default_flush_bytes));
        assert!(merge_help.contains(&default_flush_bytes));
        assert!(merge_help.contains("Flush:"));
        assert!(merge_help.contains("--flush-rows"));
        assert!(merge_help.contains("--flush-bytes"));
        assert!(!merge_help.contains("--flush-blocks"));
    }

    #[test]
    fn test_flush_blocks_rejects_zero() {
        let err = try_parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--flush-blocks",
            "0",
        ])
        .expect_err("flush-blocks=0 should fail clap parsing");

        let rendered = err.to_string();
        assert!(rendered.contains("--flush-blocks"));
    }

    #[test]
    fn test_build_help_does_not_mention_antelope_extended_behavior() {
        let cmd = TestCli::command();
        let build = cmd
            .get_subcommands()
            .find(|subcmd| subcmd.get_name() == "build")
            .expect("build subcommand should exist");

        let help = build.clone().render_long_help().to_string();

        assert!(!help.contains("Antelope always includes `db_ops` by default"));
        assert!(!help.contains("Enable extended detail level (extra tables: EVM calls/balance_changes/etc., Antelope db_ops)"));
        assert!(help.contains("Stream Antelope blocks"));
        assert!(help.contains("--without-extended"));
        assert!(help.contains("Disable extended detail tables for chains that support them"));
        assert!(help.contains("--without-votes"));
        assert!(help.contains("Disable Solana `vote_transactions` output"));
        assert!(!help.contains("--extended [<EXTENDED>]"));
        assert!(!help.contains("--with-votes [<WITH_VOTES>]"));
    }

    #[test]
    fn test_partitions_build_subcommand_rejects_removed_strict_timestamps_flag() {
        let err = TestCli::try_parse_from([
            "test-cli",
            "partitions",
            "build",
            "--network",
            "solana-mainnet-beta",
            "--partition",
            "block_range",
            "--block-range-size",
            "1000000",
            "--strict-timestamps",
            "false",
            "--output",
            "./output",
        ])
        .expect_err("removed strict timestamp flag should fail clap parsing");

        let rendered = err.to_string();
        assert!(rendered.contains("--strict-timestamps"));
        assert!(rendered.contains("unexpected argument"));
    }

    #[test]
    fn test_partition_build_type_block_range() {
        let bt = PartitionBuildType::from_cli_value("block_range").expect("parse");
        assert_eq!(bt, PartitionBuildType::BlockRange);
        assert_eq!(bt.as_str(), "block_range");
        assert!(!bt.is_time_based());
        assert_eq!(bt.interval_seconds(), 0);

        // Also accept alternate spellings
        assert_eq!(
            PartitionBuildType::from_cli_value("block-range").expect("parse"),
            PartitionBuildType::BlockRange
        );
        assert_eq!(
            PartitionBuildType::from_cli_value("blocks").expect("parse"),
            PartitionBuildType::BlockRange
        );
    }

    #[test]
    fn test_write_read_block_range_partitions_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("solana-mainnet").join("partitions.parquet");
        let rows = vec![
            PartitionBuildRow {
                partition_type: "block_range".to_string(),
                partition_interval_seconds: 1_000_000,
                partition_start_ts: "0".to_string(),
                partition_value: "0".to_string(),
                start_block: 0,
                stop_block: 1_000_000,
                start_time: None,
                end_time: Some("2021-04-06 12:00:00".to_string()),
                chain: Some("solana-mainnet".to_string()),
            },
            PartitionBuildRow {
                partition_type: "block_range".to_string(),
                partition_interval_seconds: 1_000_000,
                partition_start_ts: "1000000".to_string(),
                partition_value: "1000000".to_string(),
                start_block: 1_000_000,
                stop_block: 2_000_000,
                start_time: Some("2021-04-06 12:00:01".to_string()),
                end_time: Some("2021-04-10 08:30:00".to_string()),
                chain: Some("solana-mainnet".to_string()),
            },
        ];
        write_partitions_index(&path.to_string_lossy(), &rows, None).expect("write rows");

        let read_rows =
            read_partitions_build_rows(&path.to_string_lossy(), None).expect("read rows");
        assert_eq!(read_rows.len(), 2);
        assert_eq!(read_rows[0].partition_type, "block_range");
        assert_eq!(read_rows[0].partition_value, "0");
        assert_eq!(read_rows[0].start_block, 0);
        assert_eq!(read_rows[0].stop_block, 1_000_000);
        assert!(read_rows[0].start_time.is_none());
        assert_eq!(
            read_rows[0].end_time.as_deref(),
            Some("2021-04-06 12:00:00")
        );
        assert_eq!(read_rows[1].partition_value, "1000000");
        assert_eq!(read_rows[1].start_block, 1_000_000);
        assert_eq!(read_rows[1].stop_block, 2_000_000);
        assert_eq!(
            read_rows[1].start_time.as_deref(),
            Some("2021-04-06 12:00:01")
        );
    }

    #[test]
    fn test_write_read_time_partitions_with_nullable_timestamps() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("sol-mainnet").join("partitions.parquet");
        let rows = vec![PartitionBuildRow {
            partition_type: "date".to_string(),
            partition_interval_seconds: 86_400,
            partition_start_ts: "2021-04-06 00:00:00".to_string(),
            partition_value: "2021-04-06 00:00:00".to_string(),
            start_block: 100,
            stop_block: 200,
            start_time: None, // nullable
            end_time: None,   // nullable
            chain: Some("sol-mainnet".to_string()),
        }];
        write_partitions_index(&path.to_string_lossy(), &rows, None).expect("write rows");

        let read_rows =
            read_partitions_build_rows(&path.to_string_lossy(), None).expect("read rows");
        assert_eq!(read_rows.len(), 1);
        assert_eq!(read_rows[0].partition_type, "date");
        assert!(read_rows[0].start_time.is_none());
        assert!(read_rows[0].end_time.is_none());
    }

    #[test]
    fn test_inspect_subcommand_schema_only_parse() {
        let cli = parse(&[
            "test-cli",
            "inspect",
            "s3://bucket/mainnet/partitions.parquet",
            "--schema-only",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Inspect {
                path,
                schema_only,
                json,
                ..
            } => {
                assert_eq!(path, "s3://bucket/mainnet/partitions.parquet");
                assert!(schema_only);
                assert!(!json);
            }
            _ => panic!("expected inspect subcommand"),
        }
    }

    #[test]
    fn test_inspect_subcommand_schema_only_json_parse() {
        let cli = parse(&[
            "test-cli",
            "inspect",
            "./output/mainnet/partitions.parquet",
            "--schema-only",
            "--json",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Inspect {
                path,
                schema_only,
                json,
                ..
            } => {
                assert_eq!(path, "./output/mainnet/partitions.parquet");
                assert!(schema_only);
                assert!(json);
            }
            _ => panic!("expected inspect subcommand"),
        }
    }

    #[test]
    fn test_scan_subcommand_limit_parse() {
        let cli = parse(&[
            "test-cli",
            "scan",
            "./output/blocks/",
            "--limit",
            "50",
            "--schema-only",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Scan {
                path,
                limit,
                schema_only,
                order,
                vertical,
                json,
                ..
            } => {
                assert_eq!(path, "./output/blocks/");
                assert_eq!(limit, 50);
                assert!(schema_only);
                assert_eq!(order, ScanOrder::Asc);
                assert!(!vertical);
                assert!(!json);
            }
            _ => panic!("expected scan subcommand"),
        }
    }

    #[test]
    fn test_scan_subcommand_vertical_parse() {
        let cli = parse(&["test-cli", "scan", "./output/blocks/", "--vertical"]);
        match cli.command.expect("command should exist") {
            Commands::Scan { vertical, json, .. } => {
                assert!(vertical);
                assert!(!json);
            }
            _ => panic!("expected scan subcommand"),
        }
    }

    #[test]
    fn test_scan_subcommand_json_parse() {
        let cli = parse(&["test-cli", "scan", "./output/blocks/", "--json"]);
        match cli.command.expect("command should exist") {
            Commands::Scan { vertical, json, .. } => {
                assert!(!vertical);
                assert!(json);
            }
            _ => panic!("expected scan subcommand"),
        }
    }

    #[test]
    fn test_scan_subcommand_order_parse() {
        let cli = parse(&["test-cli", "scan", "./output/blocks/", "--order", "desc"]);
        match cli.command.expect("command should exist") {
            Commands::Scan { order, .. } => assert_eq!(order, ScanOrder::Desc),
            _ => panic!("expected scan subcommand"),
        }
    }

    #[test]
    fn test_scan_subcommand_json_conflicts_with_vertical() {
        let err = try_parse(&[
            "test-cli",
            "scan",
            "./output/blocks/",
            "--json",
            "--vertical",
        ])
        .expect_err("scan should reject conflicting output flags");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    #[serial]
    fn test_configured_s3_bucket_ignores_blank_values() {
        let _bucket = EnvVarGuard::set("S3_BUCKET", "   ");

        assert_eq!(configured_s3_bucket(), None);
    }

    #[test]
    fn test_shorthand_s3_key_normalizes_relative_paths() {
        assert_eq!(
            shorthand_s3_key(".//mainnet//partitions.parquet"),
            Some("mainnet/partitions.parquet".to_string())
        );
    }

    #[test]
    fn test_shorthand_s3_key_rejects_empty_and_non_relative_paths() {
        assert_eq!(shorthand_s3_key(""), None);
        assert_eq!(shorthand_s3_key("./"), None);
        assert_eq!(shorthand_s3_key("../partitions.parquet"), None);
        assert_eq!(shorthand_s3_key("/partitions.parquet"), None);
    }

    #[test]
    #[serial]
    fn test_resolve_parquet_input_path_prefers_existing_local_relative_path() {
        let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
        let dir = tempfile::tempdir().expect("tempdir");
        let _cwd = CurrentDirGuard::set(dir.path());
        let local_path = dir.path().join("mainnet").join("partitions.parquet");
        std::fs::create_dir_all(local_path.parent().expect("parent")).expect("create dir");
        std::fs::write(&local_path, b"not-a-real-parquet").expect("write file");

        let resolved = resolve_parquet_input_path("./mainnet/partitions.parquet");

        assert_eq!(
            resolved,
            ParquetInputPath::Local(PathBuf::from("./mainnet/partitions.parquet"))
        );
    }

    #[test]
    #[serial]
    fn test_resolve_parquet_input_path_falls_back_to_configured_s3_bucket() {
        let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
        let dir = tempfile::tempdir().expect("tempdir");
        let _cwd = CurrentDirGuard::set(dir.path());

        let resolved = resolve_parquet_input_path("./mainnet/partitions.parquet");

        assert_eq!(
            resolved,
            ParquetInputPath::S3("s3://configured-bucket/mainnet/partitions.parquet".to_string())
        );
    }

    #[test]
    #[serial]
    fn test_resolve_parquet_input_path_keeps_missing_local_path_without_bucket() {
        let _bucket = EnvVarGuard::remove("S3_BUCKET");
        let dir = tempfile::tempdir().expect("tempdir");
        let _cwd = CurrentDirGuard::set(dir.path());

        let resolved = resolve_parquet_input_path("./mainnet/partitions.parquet");

        assert_eq!(
            resolved,
            ParquetInputPath::Local(PathBuf::from("./mainnet/partitions.parquet"))
        );
    }

    #[test]
    #[serial]
    fn test_resolve_parquet_input_path_keeps_explicit_s3_uri() {
        let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");

        let resolved = resolve_parquet_input_path("s3://other-bucket/mainnet/partitions.parquet");

        assert_eq!(
            resolved,
            ParquetInputPath::S3("s3://other-bucket/mainnet/partitions.parquet".to_string())
        );
    }

    #[test]
    #[serial]
    fn test_resolve_parquet_input_path_does_not_rewrite_missing_absolute_paths() {
        let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");

        let resolved = resolve_parquet_input_path("/definitely/missing/partitions.parquet");

        assert_eq!(
            resolved,
            ParquetInputPath::Local(PathBuf::from("/definitely/missing/partitions.parquet"))
        );
    }

    #[test]
    #[serial]
    fn test_resolve_parquet_input_path_does_not_rewrite_parent_relative_paths() {
        let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
        let dir = tempfile::tempdir().expect("tempdir");
        let _cwd = CurrentDirGuard::set(dir.path());

        let resolved = resolve_parquet_input_path("./../../partitions.parquet");

        assert_eq!(
            resolved,
            ParquetInputPath::Local(PathBuf::from("./../../partitions.parquet"))
        );
    }

    #[test]
    #[serial]
    fn test_resolve_parquet_input_path_normalizes_redundant_current_dir_segments() {
        let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
        let dir = tempfile::tempdir().expect("tempdir");
        let _cwd = CurrentDirGuard::set(dir.path());

        let resolved = resolve_parquet_input_path(".//mainnet//partitions.parquet");

        assert_eq!(
            resolved,
            ParquetInputPath::S3("s3://configured-bucket/mainnet/partitions.parquet".to_string())
        );
    }

    fn test_verify_options() -> crate::verify::VerifyOptions {
        crate::verify::VerifyOptions {
            chain: None,
            table: None,
            hash_strategy: Some("auto".to_string()),
            checks: vec![],
            profile: crate::verify::VerifyProfile::Standard,
            scope: crate::verify::VerifyScope::Table,
            no_fail_fast: false,
            report_json: None,
            publish_report: false,
            publish_report_path: None,
            registry_path: None,
            update_registry: false,
        }
    }

    #[test]
    #[serial]
    fn test_updated_commands_fall_back_to_configured_s3_bucket_for_missing_relative_paths() {
        let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
        let dir = tempfile::tempdir().expect("tempdir");
        let _cwd = CurrentDirGuard::set(dir.path());

        let truncate_err = match crate::truncate::run_truncate(&crate::truncate::TruncateConfig {
            path: "./mainnet/blocks/".to_string(),
            partitions: vec![],
            dry_run: true,
            aws: None,
        }) {
            Ok(_) => panic!("truncate should resolve to S3 without a local path"),
            Err(err) => err,
        };
        assert!(truncate_err
            .to_string()
            .contains("AWS config required for S3 paths"));

        let merge_err = match crate::merge::run_merge(&crate::merge::MergeConfig {
            path: "./mainnet/blocks/".to_string(),
            compression: crate::config::Compression::Zstd,
            flush_rows: None,
            flush_bytes: 1024,
            dry_run: true,
            verbose: false,
            aws: None,
            cache_control: String::new(),
        }) {
            Ok(_) => panic!("merge should resolve to S3 without a local path"),
            Err(err) => err,
        };
        assert!(merge_err
            .to_string()
            .contains("AWS config required for S3 paths"));

        let rollup_err = crate::rollup::run_rollup(&crate::rollup::RollupConfig {
            source: "./mainnet/blocks/".to_string(),
            output: "./mainnet/blocks/".to_string(),
            target: crate::rollup::RollupTarget::Date,
            compression: crate::config::Compression::Zstd,
            flush_bytes: 1024,
            delete_source: true,
            aws: None,
            cache_control: String::new(),
        })
        .expect_err("rollup should resolve to S3 without a local path");
        assert!(rollup_err
            .to_string()
            .contains("AWS config required for S3 rollup"));

        let validate_err = match validate_parquet(
            "./mainnet/blocks/",
            None,
            &ValidateOptions {
                cross_partition: false,
                allow_gaps: false,
            },
        ) {
            Ok(_) => panic!("validate should resolve to S3 without a local path"),
            Err(err) => err,
        };
        assert!(validate_err
            .to_string()
            .contains("AWS config required for S3 paths"));

        let verify_err = match crate::verify::verify_parquet(
            "./mainnet/blocks/",
            None,
            &test_verify_options(),
        ) {
            Ok(_) => panic!("verify should resolve to S3 without a local path"),
            Err(err) => err,
        };
        assert!(verify_err
            .to_string()
            .contains("AWS config required for S3 paths"));

        let partitions_err = read_partitions_build_rows("./mainnet/partitions.parquet", None)
            .expect_err("partitions index reader should resolve to S3 without a local path");
        assert!(partitions_err
            .to_string()
            .contains("AWS config required for S3 paths"));
    }

    #[test]
    #[serial]
    fn test_updated_commands_prefer_existing_local_paths_over_configured_s3_bucket() {
        let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
        let dir = tempfile::tempdir().expect("tempdir");
        let _cwd = CurrentDirGuard::set(dir.path());

        let blocks_dir = dir.path().join("mainnet").join("blocks");
        std::fs::create_dir_all(&blocks_dir).expect("create blocks dir");

        let truncate_result = crate::truncate::run_truncate(&crate::truncate::TruncateConfig {
            path: "./mainnet/blocks/".to_string(),
            partitions: vec![],
            dry_run: true,
            aws: None,
        })
        .expect("truncate should stay local when the directory exists");
        assert_eq!(truncate_result.files_deleted, 0);

        let merge_result = crate::merge::run_merge(&crate::merge::MergeConfig {
            path: "./mainnet/blocks/".to_string(),
            compression: crate::config::Compression::Zstd,
            flush_rows: None,
            flush_bytes: 1024,
            dry_run: true,
            verbose: false,
            aws: None,
            cache_control: String::new(),
        })
        .expect("merge should stay local when the directory exists");
        assert_eq!(merge_result.files_read, 0);

        crate::rollup::run_rollup(&crate::rollup::RollupConfig {
            source: "./mainnet/blocks/".to_string(),
            output: "./mainnet/blocks/".to_string(),
            target: crate::rollup::RollupTarget::Date,
            compression: crate::config::Compression::Zstd,
            flush_bytes: 1024,
            delete_source: true,
            aws: None,
            cache_control: String::new(),
        })
        .expect("rollup should stay local when the directory exists");

        let validate_result = validate_parquet(
            "./mainnet/blocks/",
            None,
            &ValidateOptions {
                cross_partition: false,
                allow_gaps: false,
            },
        )
        .expect("validate should stay local when the directory exists");
        assert_eq!(validate_result.files_scanned, 0);

        let verify_err = match crate::verify::verify_parquet(
            "./mainnet/blocks/",
            None,
            &test_verify_options(),
        ) {
            Ok(_) => panic!("verify should stay local and report no parquet files"),
            Err(err) => err,
        };
        let verify_message = verify_err.to_string();
        assert!(verify_message.contains("no parquet files found in"));
        assert!(!verify_message.contains("AWS config required for S3 paths"));

        let partitions_path = dir.path().join("mainnet").join("partitions.parquet");
        write_test_partitions_index(
            &partitions_path,
            vec![PartitionBuildRow {
                partition_type: "date".to_string(),
                partition_interval_seconds: PartitionBuildType::Date.interval_seconds(),
                partition_start_ts: "2015-07-30 00:00:00".to_string(),
                partition_value: "2015-07-30 00:00:00".to_string(),
                start_block: 10,
                stop_block: 20,
                start_time: Some("2015-07-30 00:00:00".to_string()),
                end_time: Some("2015-07-31 00:00:00".to_string()),
                chain: Some("eth-mainnet".to_string()),
            }],
        )
        .expect("write partitions index");
        let rows = read_partitions_build_rows("./mainnet/partitions.parquet", None)
            .expect("partitions index reader should stay local when the file exists");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_rollup_subcommand_partition_parse() {
        let cli = parse(&[
            "test-cli",
            "rollup",
            "./output/blocks/",
            "--partition",
            "hour",
            "--delete-source",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Rollup {
                source,
                partition,
                delete_source,
                ..
            } => {
                assert_eq!(source, "./output/blocks/");
                assert_eq!(partition, "hour");
                assert!(delete_source);
            }
            _ => panic!("expected rollup subcommand"),
        }
    }

    #[test]
    fn test_merge_subcommand_flush_rows_parse() {
        let cli = parse(&[
            "test-cli",
            "merge",
            "./output/blocks/",
            "--flush-rows",
            "1000",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Merge {
                path,
                flush_rows,
                flush_bytes,
                ..
            } => {
                assert_eq!(path, "./output/blocks/");
                assert_eq!(flush_rows, Some(1000));
                assert_eq!(flush_bytes, DEFAULT_FLUSH_BYTES);
            }
            _ => panic!("expected merge subcommand"),
        }
    }

    #[test]
    fn test_partitions_ls_subcommand_parse() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "ls",
            "--partitions-index",
            "./partitions.parquet",
            "--partition-type",
            "date",
            "--partition-chain",
            "eth-mainnet",
            "--from",
            "2015-07-29 00:00:00",
            "--to",
            "2015-07-31 00:00:00",
            "--limit",
            "25",
            "--json",
        ]);
        assert!(cli.command.is_some());
        match cli.command.unwrap() {
            Commands::Partitions(PartitionsCommands::Ls {
                partitions_index,
                partition_type,
                partition_chain,
                from,
                to,
                limit,
                json,
                ..
            }) => {
                assert_eq!(partitions_index, "./partitions.parquet");
                assert_eq!(partition_type.as_deref(), Some("date"));
                assert_eq!(partition_chain.as_deref(), Some("eth-mainnet"));
                assert_eq!(from.as_deref(), Some("2015-07-29 00:00:00"));
                assert_eq!(to.as_deref(), Some("2015-07-31 00:00:00"));
                assert_eq!(limit, 25);
                assert!(json);
            }
            _ => panic!("expected partitions ls subcommand"),
        }
    }

    #[test]
    fn test_partitions_shard_subcommand_parse() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "shard",
            "--partitions-index",
            "./partitions.parquet",
            "--partition-type",
            "date",
            "--partition-chain",
            "eth-mainnet",
            "--from",
            "2015-07-29 00:00:00",
            "--to",
            "2015-07-31 00:00:00",
            "--shard-count",
            "4",
            "--shard-index",
            "1",
            "--strategy",
            "hash",
            "--json",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Partitions(PartitionsCommands::Shard {
                partitions_index,
                partition_type,
                partition_chain,
                from,
                to,
                shard_count,
                shard_index,
                strategy,
                json,
                ..
            }) => {
                assert_eq!(partitions_index, "./partitions.parquet");
                assert_eq!(partition_type.as_deref(), Some("date"));
                assert_eq!(partition_chain.as_deref(), Some("eth-mainnet"));
                assert_eq!(from.as_deref(), Some("2015-07-29 00:00:00"));
                assert_eq!(to.as_deref(), Some("2015-07-31 00:00:00"));
                assert_eq!(shard_count, 4);
                assert_eq!(shard_index, 1);
                assert_eq!(strategy, "hash");
                assert!(json);
            }
            _ => panic!("expected partitions shard subcommand"),
        }
    }

    #[test]
    fn test_partitions_validate_subcommand_parse() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "validate",
            "--partitions-index",
            "./partitions.parquet",
            "--partition-type",
            "date",
            "--partition-chain",
            "eth-mainnet",
            "--allow-gaps",
            "--json",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Partitions(PartitionsCommands::Validate {
                partitions_index,
                partition_type,
                partition_chain,
                allow_gaps,
                json,
                ..
            }) => {
                assert_eq!(partitions_index, "./partitions.parquet");
                assert_eq!(partition_type.as_deref(), Some("date"));
                assert_eq!(partition_chain.as_deref(), Some("eth-mainnet"));
                assert!(allow_gaps);
                assert!(json);
            }
            _ => panic!("expected partitions validate subcommand"),
        }
    }

    #[test]
    #[serial]
    fn test_completions_generation() {
        // Verify that shell completion generation runs without panicking
        // for each supported shell type.
        for shell in [
            Shell::Bash,
            Shell::Zsh,
            Shell::Fish,
            Shell::Elvish,
            Shell::PowerShell,
        ] {
            let mut cmd = TestCli::command();
            let name = cmd.get_name().to_string();
            let mut buf = Vec::new();
            generate(shell, &mut cmd, name, &mut buf);
            assert!(
                !buf.is_empty(),
                "completions for {shell:?} should not be empty"
            );
        }
    }

    #[test]
    #[serial]
    fn test_env_var_fallback() {
        // Verify that env vars are picked up when no CLI flags are given.
        // We set a few env vars and then parse with no CLI arguments.
        // Safety: test-only; concurrent tests that also touch these env vars
        // could race, but cargo test runs tests in separate processes for
        // integration tests and the clap env lookup is point-in-time.
        unsafe {
            std::env::set_var("ENDPOINT", "https://from-env.example.com:443");
            std::env::set_var("START_BLOCK", "42");
            std::env::set_var("COMPRESSION", "snappy");
        }

        let cli = parse(&["test-cli"]);
        assert_eq!(
            cli.common.endpoint.as_deref(),
            Some("https://from-env.example.com:443")
        );
        assert_eq!(cli.common.start_block, Some(42));
        assert_eq!(cli.common.compression, "snappy");

        // CLI flags take precedence over env vars
        let cli = parse(&[
            "test-cli",
            "-e",
            "https://from-cli.example.com:443",
            "--compression",
            "gzip",
        ]);
        assert_eq!(
            cli.common.endpoint.as_deref(),
            Some("https://from-cli.example.com:443")
        );
        assert_eq!(cli.common.compression, "gzip");
        // env var still applies for start_block since no CLI flag overrides it
        assert_eq!(cli.common.start_block, Some(42));

        // Clean up
        unsafe {
            std::env::remove_var("ENDPOINT");
            std::env::remove_var("START_BLOCK");
            std::env::remove_var("COMPRESSION");
        }
    }

    #[test]
    #[serial]
    fn test_api_key_envvar_resolution() {
        // Set up an env var with the actual API key
        unsafe {
            std::env::set_var("SUBSTREAMS_API_KEY", "my-test-key");
            std::env::set_var("SUBSTREAMS_API_TOKEN", "my-test-token");
        }

        let cli = parse(&["test-cli", "--endpoint", "https://example.com:443"]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.api_key.as_deref(), Some("my-test-key"));
        assert_eq!(config.jwt_token.as_deref(), Some("my-test-token"));

        // Clean up
        unsafe {
            std::env::remove_var("SUBSTREAMS_API_KEY");
            std::env::remove_var("SUBSTREAMS_API_TOKEN");
        }
    }

    #[test]
    #[serial]
    fn test_custom_api_key_envvar() {
        // Test using a custom envvar name
        unsafe {
            std::env::set_var("MY_CUSTOM_KEY", "custom-key-value");
        }

        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--api-key-envvar",
            "MY_CUSTOM_KEY",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.api_key.as_deref(), Some("custom-key-value"));

        // Clean up
        unsafe {
            std::env::remove_var("MY_CUSTOM_KEY");
        }
    }

    #[test]
    #[serial]
    fn test_aws_credentials_flags() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--aws-access-key-id",
            "AKID123",
            "--aws-secret-access-key",
            "secret456",
            "--aws-session-token",
            "token789",
            "--aws-region",
            "us-east-1",
            "--aws-endpoint-url",
            "https://s3.custom.endpoint",
        ]);
        assert_eq!(cli.common.aws_access_key_id.as_deref(), Some("AKID123"));
        assert_eq!(
            cli.common.aws_secret_access_key.as_deref(),
            Some("secret456")
        );
        assert_eq!(cli.common.aws_session_token.as_deref(), Some("token789"));
        assert_eq!(cli.common.aws_region.as_deref(), Some("us-east-1"));
        assert_eq!(
            cli.common.aws_endpoint_url.as_deref(),
            Some("https://s3.custom.endpoint")
        );

        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.aws_access_key_id.as_deref(), Some("AKID123"));
        assert_eq!(config.aws_secret_access_key.as_deref(), Some("secret456"));
        assert_eq!(config.aws_session_token.as_deref(), Some("token789"));
        assert_eq!(config.aws_region.as_deref(), Some("us-east-1"));
        assert_eq!(
            config.aws_endpoint_url.as_deref(),
            Some("https://s3.custom.endpoint")
        );
    }

    #[test]
    #[serial]
    fn test_aws_credentials_defaults_none() {
        // Clear any AWS env vars that may leak from .env
        unsafe {
            std::env::remove_var("ACCESS_KEY_ID");
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("SECRET_ACCESS_KEY");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
            std::env::remove_var("AWS_SESSION_TOKEN");
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_ENDPOINT_URL_S3");
            std::env::remove_var("S3_BUCKET");
        }
        let cli = parse(&["test-cli", "--endpoint", "http://localhost:9000"]);
        assert!(cli.common.aws_access_key_id.is_none());
        assert!(cli.common.aws_secret_access_key.is_none());
        assert!(cli.common.aws_session_token.is_none());
        assert!(cli.common.aws_region.is_none());
        assert!(cli.common.aws_endpoint_url.is_none());
        assert!(cli.common.s3_bucket.is_none());
    }

    #[test]
    #[serial]
    fn test_build_config_rejects_s3_bucket_without_explicit_aws_credentials() {
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
            std::env::set_var("ACCESS_KEY_ID", "alias-only-access-key");
            std::env::set_var("SECRET_ACCESS_KEY", "alias-only-secret-key");
        }

        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--s3-bucket",
            "my-bucket",
            "--output",
            "my-prefix",
        ]);
        let err = build_config(&cli.common)
            .expect_err("build_config should fail without AWS_* credentials");
        let message = err.to_string();
        assert!(message.contains("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY"));
        assert!(message.contains("metadata providers"));

        unsafe {
            std::env::remove_var("ACCESS_KEY_ID");
            std::env::remove_var("SECRET_ACCESS_KEY");
        }
    }

    #[test]
    #[serial]
    fn test_build_config_rejects_direct_s3_output_without_explicit_aws_credentials() {
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        }

        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--output",
            "s3://my-bucket/my-prefix",
        ]);
        let err = build_config(&cli.common)
            .expect_err("build_config should fail for direct s3 output without credentials");
        let message = err.to_string();
        assert!(message.contains("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY"));
        assert!(message.contains("metadata providers"));
    }

    #[test]
    #[serial]
    fn test_s3_bucket_constructs_output() {
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
            std::env::remove_var("AWS_SESSION_TOKEN");
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_ENDPOINT_URL_S3");
            std::env::remove_var("S3_BUCKET");
        }
        // When --s3-bucket is set, output should become s3://bucket/output
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--aws-access-key-id",
            "AKID123",
            "--aws-secret-access-key",
            "secret456",
            "--s3-bucket",
            "my-bucket",
            "--output",
            "my-prefix",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.output, PathBuf::from("s3://my-bucket/my-prefix"));
        assert_eq!(config.s3_bucket.as_deref(), Some("my-bucket"));
    }

    #[test]
    #[serial]
    fn test_s3_bucket_preserves_explicit_local_output() {
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
            std::env::remove_var("AWS_SESSION_TOKEN");
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_ENDPOINT_URL_S3");
            std::env::remove_var("S3_BUCKET");
        }
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--aws-access-key-id",
            "AKID123",
            "--aws-secret-access-key",
            "secret456",
            "--s3-bucket",
            "my-bucket",
            "--output",
            "./output",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.output, PathBuf::from("./output"));
        assert_eq!(config.s3_bucket.as_deref(), Some("my-bucket"));
    }

    #[test]
    #[serial]
    fn test_s3_bucket_no_double_prefix() {
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
            std::env::remove_var("AWS_SESSION_TOKEN");
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_ENDPOINT_URL_S3");
            std::env::remove_var("S3_BUCKET");
        }
        // When output already starts with s3://, s3_bucket should not double-prefix
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--aws-access-key-id",
            "AKID123",
            "--aws-secret-access-key",
            "secret456",
            "--s3-bucket",
            "my-bucket",
            "--output",
            "s3://other-bucket/prefix",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.output, PathBuf::from("s3://other-bucket/prefix"));
    }

    #[test]
    #[serial]
    fn test_s3_bucket_default_output() {
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
            std::env::remove_var("AWS_SESSION_TOKEN");
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_ENDPOINT_URL_S3");
            std::env::remove_var("S3_BUCKET");
            std::env::remove_var("OUTPUT");
        }
        // When --s3-bucket is set but output uses default "output", use bucket root
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--aws-access-key-id",
            "AKID123",
            "--aws-secret-access-key",
            "secret456",
            "--s3-bucket",
            "my-bucket",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.output, PathBuf::from("s3://my-bucket"));
    }

    #[test]
    fn test_common_args_reject_removed_partition_flags() {
        for (flag, value) in [
            ("--partitions-index", "./partitions.parquet"),
            ("--partition-from", "2015-07-30 15:00:00"),
            ("--partition-to", "2015-07-30 18:00:00"),
        ] {
            let err = try_parse(&[
                "test-cli",
                "--endpoint",
                "https://example.com:443",
                flag,
                value,
            ])
            .expect_err("removed build flag must be rejected");
            assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
            assert!(err.to_string().contains(flag));
        }
    }

    #[test]
    fn test_resolve_partition_bounds_from_index_local() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_test_partitions_index(
            &path,
            vec![
                time_partition_row(Some("eth-mainnet"), "hour", "2015-07-30 14:00:00", 100, 200),
                time_partition_row(Some("eth-mainnet"), "hour", "2015-07-30 15:00:00", 200, 300),
            ],
        )
        .expect("write partitions index");

        let request = PartitionBoundsRequest {
            index_path: path.to_string_lossy().to_string(),
            partition_type: "hour".to_string(),
            partition_value: "2015-07-30 15:00:00".to_string(),
            chain: Some("eth-mainnet".to_string()),
        };
        let bounds = resolve_partition_bounds_from_index(&request, None)
            .expect("partition bounds should resolve");
        assert_eq!(bounds.start_block, 200);
        assert_eq!(bounds.stop_block, 300);
    }

    #[test]
    fn test_resolve_partition_bounds_from_index_ambiguous() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_test_partitions_index(
            &path,
            vec![
                time_partition_row(Some("eth-mainnet"), "hour", "2015-07-30 15:00:00", 200, 300),
                time_partition_row(Some("eth-mainnet"), "hour", "2015-07-30 15:00:00", 201, 301),
            ],
        )
        .expect("write partitions index");

        let request = PartitionBoundsRequest {
            index_path: path.to_string_lossy().to_string(),
            partition_type: "hour".to_string(),
            partition_value: "2015-07-30 15:00:00".to_string(),
            chain: None,
        };
        let err = resolve_partition_bounds_from_index(&request, None)
            .expect_err("duplicate rows should be rejected");
        assert!(err.to_string().contains("ambiguous"));
    }

    #[test]
    fn test_resolve_partition_command_strict_single_chain_rejects_multi_chain() {
        use arrow::array::{StringArray, UInt64Array};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::metadata::KeyValue;
        use parquet::file::properties::WriterProperties;
        use std::fs::File;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("chain", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("partition", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("start_block", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("stop_block", arrow::datatypes::DataType::UInt64, false),
        ]));
        let partition_value =
            parse_partition_timestamp("2015-07-30 00:00:00").expect("partition timestamp") as u64;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["eth-mainnet", "polygon-mainnet"])),
                Arc::new(UInt64Array::from(vec![partition_value, partition_value])),
                Arc::new(UInt64Array::from(vec![100_u64, 200_u64])),
                Arc::new(UInt64Array::from(vec![200_u64, 300_u64])),
            ],
        )
        .expect("record batch");
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "firehose-parquet.partition".to_string(),
                "date".to_string(),
            )]))
            .build();
        let file = File::create(&path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let err = resolve_partition_command(
            PartitionBoundsRequest {
                index_path: path.to_string_lossy().to_string(),
                partition_type: "date".to_string(),
                partition_value: "2015-07-30 00:00:00".to_string(),
                chain: None,
            },
            None,
            &PartitionResolveOptions {
                strict_single_chain: true,
            },
        )
        .expect_err("strict single chain should fail on multi-chain match");
        assert!(err.to_string().contains("multiple chains"));
        assert!(err.to_string().contains("--partition-chain"));
    }

    #[test]
    fn test_resolve_partition_window_bounds_from_index_local() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_test_partitions_index(
            &path,
            vec![
                time_partition_row(Some("eth-mainnet"), "hour", "2015-07-30 14:00:00", 100, 200),
                time_partition_row(Some("eth-mainnet"), "hour", "2015-07-30 15:00:00", 200, 300),
                time_partition_row(Some("eth-mainnet"), "hour", "2015-07-30 16:00:00", 300, 400),
            ],
        )
        .expect("write partitions index");

        let request = PartitionWindowRequest {
            index_path: path.to_string_lossy().to_string(),
            partition_type: "hour".to_string(),
            partition_from: "2015-07-30 14:00:00".to_string(),
            partition_to: "2015-07-30 16:00:00".to_string(),
            chain: Some("eth-mainnet".to_string()),
        };
        let bounds = resolve_partition_window_bounds_from_index(&request, None)
            .expect("partition window bounds should resolve");
        assert_eq!(bounds.start_block, 100);
        assert_eq!(bounds.stop_block, 300);
        assert_eq!(bounds.partitions_count, 2);
    }

    #[test]
    fn test_list_partitions_from_index_filters_sort_and_limit() {
        use arrow::array::{StringArray, UInt64Array};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::metadata::KeyValue;
        use parquet::file::properties::WriterProperties;
        use std::fs::File;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        let partition_values = [
            "2015-07-30 00:00:00",
            "2015-07-29 00:00:00",
            "2015-07-31 00:00:00",
            "2015-07-29 00:00:00",
        ]
        .into_iter()
        .map(|value| parse_partition_timestamp(value).expect("partition timestamp") as u64)
        .collect::<Vec<_>>();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("chain", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("partition", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("start_block", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("stop_block", arrow::datatypes::DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "eth-mainnet",
                    "eth-mainnet",
                    "eth-mainnet",
                    "btc-mainnet",
                ])),
                Arc::new(UInt64Array::from(partition_values)),
                Arc::new(UInt64Array::from(vec![200_u64, 100_u64, 300_u64, 999_u64])),
                Arc::new(UInt64Array::from(vec![300_u64, 200_u64, 400_u64, 1000_u64])),
            ],
        )
        .expect("record batch");
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "firehose-parquet.partition".to_string(),
                "date".to_string(),
            )]))
            .build();
        let file = File::create(&path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let request = PartitionListRequest {
            index_path: path.to_string_lossy().to_string(),
            partition_type: Some("date".to_string()),
            chain: Some("eth-mainnet".to_string()),
            from: Some("2015-07-29 00:00:00".to_string()),
            to: Some("2015-07-31 00:00:00".to_string()),
            limit: 2,
        };

        let result = list_partitions_from_index(&request, None).expect("list should succeed");
        assert_eq!(result.total_matches, 3);
        assert_eq!(result.returned_rows, 2);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].partition_start_ts, "2015-07-29 00:00:00");
        assert_eq!(result.rows[0].start_block, 100);
        assert_eq!(result.rows[1].partition_start_ts, "2015-07-30 00:00:00");
        assert_eq!(result.rows[1].start_block, 200);
        assert!(result
            .rows
            .iter()
            .all(|row| row.chain.as_deref() == Some("eth-mainnet")));
    }

    #[test]
    fn test_parse_partition_shard_strategy() {
        assert_eq!(
            parse_partition_shard_strategy("ordinal").expect("ordinal should parse"),
            PartitionShardStrategy::Ordinal
        );
        assert_eq!(
            parse_partition_shard_strategy("hash").expect("hash should parse"),
            PartitionShardStrategy::Hash
        );
        assert!(parse_partition_shard_strategy("unknown").is_err());
    }

    #[test]
    fn test_shard_partitions_from_index_ordinal_completeness_and_non_overlap() {
        use std::collections::BTreeSet;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");

        let values = vec![
            "2015-07-29 00:00:00",
            "2015-07-30 00:00:00",
            "2015-07-31 00:00:00",
            "2015-08-01 00:00:00",
            "2015-08-02 00:00:00",
        ];
        let rows = values
            .into_iter()
            .zip([100_u64, 200, 300, 400, 500])
            .zip([200_u64, 300, 400, 500, 600])
            .map(|((partition_value, start_block), stop_block)| {
                time_partition_row(
                    Some("eth-mainnet"),
                    "date",
                    partition_value,
                    start_block,
                    stop_block,
                )
            })
            .collect::<Vec<_>>();
        write_test_partitions_index(&path, rows).expect("write partitions index");

        let base_request = PartitionListRequest {
            index_path: path.to_string_lossy().to_string(),
            partition_type: Some("date".to_string()),
            chain: Some("eth-mainnet".to_string()),
            from: None,
            to: None,
            limit: usize::MAX,
        };

        let mut seen = BTreeSet::new();
        for shard_index in 0..3 {
            let result = shard_partitions_from_index(
                &PartitionShardRequest {
                    list: base_request.clone(),
                    shard_count: 3,
                    shard_index,
                    strategy: PartitionShardStrategy::Ordinal,
                },
                None,
            )
            .expect("shard should succeed");

            for row in result.rows {
                let inserted = seen.insert(row.partition_value);
                assert!(inserted, "partition appeared in multiple shards");
            }
        }

        assert_eq!(seen.len(), 5);
    }

    #[test]
    fn test_list_partitions_from_index_rejects_legacy_partitions_metadata() {
        use arrow::array::{StringArray, UInt64Array};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::metadata::KeyValue;
        use parquet::file::properties::WriterProperties;
        use std::fs::File;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("chain", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("partition_type", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new(
                "partition_value",
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new("start_block", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("stop_block", arrow::datatypes::DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["eth-mainnet"])),
                Arc::new(StringArray::from(vec!["day"])),
                Arc::new(StringArray::from(vec!["2015-07-30 00:00:00"])),
                Arc::new(UInt64Array::from(vec![100_u64])),
                Arc::new(UInt64Array::from(vec![200_u64])),
            ],
        )
        .expect("record batch");

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "firehose-parquet.partitions.schema_version".to_string(),
                "999".to_string(),
            )]))
            .build();

        let file = File::create(&path).expect("create parquet");
        let mut writer =
            ArrowWriter::try_new(file, schema, Some(props)).expect("create arrow writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let err = list_partitions_from_index(
            &PartitionListRequest {
                index_path: path.to_string_lossy().to_string(),
                partition_type: Some("day".to_string()),
                chain: None,
                from: None,
                to: None,
                limit: 10,
            },
            None,
        )
        .expect_err("legacy partitions metadata should be rejected");
        let message = err.to_string();
        assert!(
            message.contains("missing required column: partition")
                || message.contains("missing required metadata: firehose-parquet.partition")
        );
    }

    #[test]
    fn test_read_partitions_build_rows_rejects_legacy_schema_and_metadata() {
        use arrow::array::{StringArray, UInt64Array};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::metadata::KeyValue;
        use parquet::file::properties::WriterProperties;
        use std::fs::File;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("chain", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("partition_type", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new(
                "partition_value",
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new("start_block", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("end_block", arrow::datatypes::DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["eth-mainnet"])),
                Arc::new(StringArray::from(vec!["day"])),
                Arc::new(StringArray::from(vec!["2015-07-30 00:00:00"])),
                Arc::new(UInt64Array::from(vec![100_u64])),
                Arc::new(UInt64Array::from(vec![200_u64])),
            ],
        )
        .expect("record batch");

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_key_value_metadata(Some(vec![
                KeyValue::new(
                    "firehose-parquet.partitions.partition_types".to_string(),
                    "hour".to_string(),
                ),
                KeyValue::new(
                    "firehose-parquet.partitions.max_end_block".to_string(),
                    "999999".to_string(),
                ),
            ]))
            .build();

        let file = File::create(&path).expect("create parquet");
        let mut writer =
            ArrowWriter::try_new(file, schema, Some(props)).expect("create arrow writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let err = read_partitions_build_rows(&path.to_string_lossy(), None)
            .expect_err("legacy partitions schema should be rejected");
        let message = err.to_string();
        assert!(
            message.contains("missing required column: partition")
                || message.contains("missing required metadata: firehose-parquet.partition")
        );
    }

    #[test]
    fn test_resolve_partition_bounds_from_index_rejects_legacy_end_block_column() {
        use arrow::array::{StringArray, UInt64Array};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::fs::File;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("chain", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("partition_type", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new(
                "partition_value",
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new("start_block", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("end_block", arrow::datatypes::DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["eth-mainnet"])),
                Arc::new(StringArray::from(vec!["hour"])),
                Arc::new(StringArray::from(vec!["2015-07-30 15:00:00"])),
                Arc::new(UInt64Array::from(vec![200_u64])),
                Arc::new(UInt64Array::from(vec![300_u64])),
            ],
        )
        .expect("record batch");

        let file = File::create(&path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let request = PartitionBoundsRequest {
            index_path: path.to_string_lossy().to_string(),
            partition_type: "hour".to_string(),
            partition_value: "2015-07-30 15:00:00".to_string(),
            chain: Some("eth-mainnet".to_string()),
        };
        let err = resolve_partition_bounds_from_index(&request, None)
            .expect_err("legacy end_block column should be rejected");
        let message = err.to_string();
        assert!(
            message.contains("missing required column: partition")
                || message.contains("missing required file metadata")
        );
    }

    /// Write a minimal blocks table (blocks `1..=n` with a valid parent chain) whose
    /// `timestamp` column is `timestamps`, so validate exercises real column types.
    fn write_validate_blocks_file(path: &std::path::Path, timestamps: arrow::array::ArrayRef) {
        use arrow::array::{StringArray, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let block_nums = (1..=timestamps.len() as u64).collect::<Vec<_>>();
        let block_ids = block_nums
            .iter()
            .map(|num| format!("id{num}"))
            .collect::<Vec<_>>();
        let parent_ids = block_nums
            .iter()
            .map(|num| format!("id{}", num - 1))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new("block_id", DataType::Utf8, false),
            Field::new("parent_id", DataType::Utf8, false),
            Field::new("timestamp", timestamps.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(block_nums)),
                Arc::new(StringArray::from(block_ids)),
                Arc::new(StringArray::from(parent_ids)),
                timestamps,
            ],
        )
        .expect("record batch");

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent dir");
        }
        let file = std::fs::File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }

    fn validate_local(path: &std::path::Path) -> ValidateResult {
        validate_parquet(&path.to_string_lossy(), None, &ValidateOptions::default())
            .expect("validate should run")
    }

    fn reversal_summary(reversals: &[TimestampReversal]) -> Vec<(u64, i64, u64, i64)> {
        reversals
            .iter()
            .map(|r| (r.block_num, r.timestamp, r.prev_block_num, r.prev_timestamp))
            .collect()
    }

    #[test]
    fn test_validate_parquet_reports_timestamp_reversal_in_any_timestamp_unit() {
        use arrow::array::{
            ArrayRef, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray,
        };
        use std::sync::Arc;

        // Epoch seconds with a reversal at block 3 (1_690_815_595 < 1_690_815_600).
        let seconds = [
            1_690_815_590_i64,
            1_690_815_600,
            1_690_815_595,
            1_690_815_610,
        ];
        let scaled = |factor: i64| seconds.iter().map(|s| s * factor).collect::<Vec<_>>();
        let columns: Vec<(&str, ArrayRef)> = vec![
            (
                "timestamp_second_utc",
                Arc::new(TimestampSecondArray::from(scaled(1)).with_timezone("UTC")),
            ),
            (
                "timestamp_millisecond_utc",
                Arc::new(TimestampMillisecondArray::from(scaled(1_000)).with_timezone("UTC")),
            ),
            (
                "timestamp_microsecond",
                Arc::new(TimestampMicrosecondArray::from(scaled(1_000_000))),
            ),
            (
                "timestamp_nanosecond_utc",
                Arc::new(
                    TimestampNanosecondArray::from(scaled(1_000_000_000)).with_timezone("UTC"),
                ),
            ),
            (
                "legacy_int64_seconds",
                Arc::new(Int64Array::from(scaled(1))),
            ),
        ];

        for (label, column) in columns {
            let dir = tempfile::tempdir().expect("tempdir");
            write_validate_blocks_file(&dir.path().join("blocks.parquet"), column);

            let result = validate_local(dir.path());
            assert_eq!(
                reversal_summary(&result.timestamp_reversals),
                [(3, 1_690_815_595, 2, 1_690_815_600)],
                "{label}"
            );
            assert_eq!(result.total_blocks, 4, "{label}");
        }
    }

    #[test]
    fn test_validate_parquet_compares_null_timestamps_against_last_known_timestamp() {
        use arrow::array::TimestampSecondArray;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        write_validate_blocks_file(
            &dir.path().join("blocks.parquet"),
            Arc::new(
                TimestampSecondArray::from(vec![
                    None,
                    Some(1_690_815_600),
                    None,
                    Some(1_690_815_590),
                    Some(1_690_815_610),
                ])
                .with_timezone("UTC"),
            ),
        );

        let result = validate_local(dir.path());
        assert_eq!(
            reversal_summary(&result.timestamp_reversals),
            [(4, 1_690_815_590, 2, 1_690_815_600)]
        );
    }

    #[test]
    fn test_validate_parquet_reports_timestamp_reversal_per_partition() {
        use arrow::array::TimestampSecondArray;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        write_validate_blocks_file(
            &dir.path().join("date=2023-07-31").join("blocks.parquet"),
            Arc::new(
                TimestampSecondArray::from(vec![1_690_815_590, 1_690_815_580]).with_timezone("UTC"),
            ),
        );
        write_validate_blocks_file(
            &dir.path().join("date=2023-08-01").join("blocks.parquet"),
            Arc::new(
                TimestampSecondArray::from(vec![1_690_900_000, 1_690_900_010]).with_timezone("UTC"),
            ),
        );

        let result = validate_local(dir.path());
        let reversals = result
            .partitions
            .iter()
            .map(|p| {
                (
                    p.partition.as_str(),
                    reversal_summary(&p.timestamp_reversals),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            reversals,
            [
                (
                    "date=2023-07-31",
                    vec![(2, 1_690_815_580, 1, 1_690_815_590)]
                ),
                ("date=2023-08-01", vec![]),
            ]
        );
    }

    #[test]
    fn test_validate_parquet_rejects_unsupported_timestamp_type() {
        use arrow::array::StringArray;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        write_validate_blocks_file(
            &dir.path().join("blocks.parquet"),
            Arc::new(StringArray::from(vec!["2023-07-31 14:59:50"])),
        );

        let err = validate_parquet(
            &dir.path().to_string_lossy(),
            None,
            &ValidateOptions::default(),
        )
        .expect_err("a Utf8 timestamp column should be rejected, not read as 0");
        assert!(err.to_string().contains("timestamp"), "{err}");
    }

    #[test]
    fn test_validate_partitions_index_detects_gap_and_overlap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_test_partitions_index(
            &path,
            vec![
                time_partition_row(Some("eth-mainnet"), "date", "2015-07-29 00:00:00", 100, 200),
                time_partition_row(Some("eth-mainnet"), "date", "2015-07-30 00:00:00", 250, 300),
                time_partition_row(Some("eth-mainnet"), "date", "2015-07-31 00:00:00", 290, 400),
            ],
        )
        .expect("write partitions index");

        let result = validate_partitions_index(
            &PartitionValidateRequest {
                list: PartitionListRequest {
                    index_path: path.to_string_lossy().to_string(),
                    partition_type: Some("date".to_string()),
                    chain: Some("eth-mainnet".to_string()),
                    from: None,
                    to: None,
                    limit: usize::MAX,
                },
                allow_gaps: false,
            },
            None,
        )
        .expect("validation should run");

        assert!(!result.valid);
        assert_eq!(result.issue_count, 2);
        assert!(result
            .issues
            .iter()
            .any(|i| i.kind == PartitionValidationIssueKind::Gap));
        assert!(result
            .issues
            .iter()
            .any(|i| i.kind == PartitionValidationIssueKind::Overlap));
    }

    #[test]
    fn test_parse_partition_build_types_accepts_date_and_day_alias() {
        let parsed = parse_partition_build_types("date").expect("parse types");
        assert_eq!(parsed, vec![PartitionBuildType::Date]);

        let parsed = parse_partition_build_types("day").expect("parse alias");
        assert_eq!(parsed, vec![PartitionBuildType::Date]);
    }

    #[test]
    fn test_parse_partition_build_types_rejects_multiple_values() {
        let err =
            parse_partition_build_types("date,hour").expect_err("multiple values should fail");
        assert!(err.to_string().contains("exactly one value per run"));
    }

    #[test]
    fn test_resolve_s3_output_root_prefers_explicit_output() {
        let resolved =
            resolve_s3_output_root(Some("./output"), Some("bucket-name")).expect("resolve");
        assert_eq!(resolved, "./output");

        let resolved =
            resolve_s3_output_root(Some("s3://other-bucket/prefix"), Some("bucket-name"))
                .expect("resolve s3");
        assert_eq!(resolved, "s3://other-bucket/prefix");
    }

    #[test]
    fn test_resolve_s3_output_root_accepts_bucket_without_output() {
        let resolved = resolve_s3_output_root(None, Some("bucket-name")).expect("resolve");
        assert_eq!(resolved, "s3://bucket-name");
    }

    #[test]
    fn test_resolve_s3_output_root_rewrites_implicit_relative_output() {
        let resolved =
            resolve_s3_output_root(Some("output"), Some("bucket-name")).expect("resolve");
        assert_eq!(resolved, "s3://bucket-name/output");
    }

    #[test]
    fn test_resolve_s3_output_root_requires_output_or_bucket() {
        let err = resolve_s3_output_root(None, None).expect_err("missing output should fail");
        assert!(err
            .to_string()
            .contains("--output is required unless --s3-bucket or S3_BUCKET is set"));
    }

    #[test]
    fn test_partition_index_builder_snapshot_includes_active_row() {
        let mut builder = PartitionIndexBuilder::new("eth-mainnet", vec![PartitionBuildType::Date])
            .expect("builder");
        builder
            .observe_block(&BlockIdentity {
                block_num: 100,
                timestamp: 1_690_815_540,
                ..Default::default()
            })
            .expect("observe first block");
        builder
            .observe_block(&BlockIdentity {
                block_num: 101,
                timestamp: 1_690_815_590,
                ..Default::default()
            })
            .expect("observe second block");

        let rows = builder.snapshot(102).expect("snapshot rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].partition_type, "date");
        assert_eq!(rows[0].start_block, 100);
        assert_eq!(rows[0].stop_block, 102);
        assert_eq!(builder.current_frontier(), Some(102));
    }

    #[test]
    fn test_build_partition_rows_from_blocks_mixed_types_and_contiguous() {
        let blocks = vec![
            BlockIdentity {
                block_num: 100,
                timestamp: 1_690_815_540,
                ..Default::default()
            },
            BlockIdentity {
                block_num: 101,
                timestamp: 1_690_815_590,
                ..Default::default()
            },
            BlockIdentity {
                block_num: 102,
                timestamp: 1_690_815_600,
                ..Default::default()
            },
        ];

        let rows = build_partition_rows_from_blocks(
            "eth-mainnet",
            vec![PartitionBuildType::Date, PartitionBuildType::Hour],
            &blocks,
            103,
        )
        .expect("build rows");

        assert_eq!(rows.len(), 3);

        let date_rows: Vec<_> = rows
            .iter()
            .filter(|row| row.partition_type == "date")
            .collect();
        assert_eq!(date_rows.len(), 1);
        assert_eq!(date_rows[0].partition_value, "2023-07-31 00:00:00");
        assert_eq!(date_rows[0].start_block, 100);
        assert_eq!(date_rows[0].stop_block, 103);

        let hour_rows: Vec<_> = rows
            .iter()
            .filter(|row| row.partition_type == "hour")
            .collect();
        assert_eq!(hour_rows.len(), 2);
        assert_eq!(hour_rows[0].partition_value, "2023-07-31 14:00:00");
        assert_eq!(hour_rows[0].start_block, 100);
        assert_eq!(hour_rows[0].stop_block, 102);
        assert_eq!(hour_rows[1].partition_value, "2023-07-31 15:00:00");
        assert_eq!(hour_rows[1].start_block, 102);
        assert_eq!(hour_rows[1].stop_block, 103);
    }

    #[test]
    fn test_build_partition_rows_from_blocks_handles_first_streamable_block() {
        let blocks = vec![BlockIdentity {
            block_num: 500,
            timestamp: 1_690_815_540,
            ..Default::default()
        }];

        let rows = build_partition_rows_from_blocks(
            "eth-mainnet",
            vec![PartitionBuildType::Hour],
            &blocks,
            501,
        )
        .expect("build rows");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].start_block, 500);
        assert_eq!(rows[0].stop_block, 501);
        assert_eq!(rows[0].partition_value, "2023-07-31 14:00:00");
    }

    #[test]
    fn test_partition_index_builder_resume_extends_terminal_rows() {
        let existing_rows = vec![PartitionBuildRow {
            partition_type: "hour".to_string(),
            partition_interval_seconds: 3_600,
            partition_start_ts: "2023-07-31 14:00:00".to_string(),
            partition_value: "2023-07-31 14:00:00".to_string(),
            start_block: 100,
            stop_block: 102,
            start_time: Some("2023-07-31 14:59:00".to_string()),
            end_time: Some("2023-07-31 14:59:50".to_string()),
            chain: Some("eth-mainnet".to_string()),
        }];

        let (mut builder, resume_start_block) = PartitionIndexBuilder::resume_from_existing(
            "eth-mainnet",
            vec![PartitionBuildType::Hour],
            existing_rows,
        )
        .expect("resume builder");
        assert_eq!(resume_start_block, 102);

        builder
            .observe_block(&BlockIdentity {
                block_num: 102,
                timestamp: 1_690_815_590,
                ..Default::default()
            })
            .expect("same-hour block");
        builder
            .observe_block(&BlockIdentity {
                block_num: 103,
                timestamp: 1_690_815_600,
                ..Default::default()
            })
            .expect("next-hour block");

        let rows = builder.finish(104).expect("finish resumed build");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].partition_value, "2023-07-31 14:00:00");
        assert_eq!(rows[0].start_block, 100);
        assert_eq!(rows[0].stop_block, 103);
        assert_eq!(rows[1].partition_value, "2023-07-31 15:00:00");
        assert_eq!(rows[1].start_block, 103);
        assert_eq!(rows[1].stop_block, 104);
    }

    fn block_range_row(
        start_block: u64,
        stop_block: u64,
        block_range_size: u64,
    ) -> PartitionBuildRow {
        PartitionBuildRow {
            partition_type: "block_range".to_string(),
            partition_interval_seconds: block_range_size as i64,
            partition_start_ts: start_block.to_string(),
            partition_value: start_block.to_string(),
            start_block,
            stop_block,
            start_time: None,
            end_time: None,
            chain: Some("solana-mainnet-beta".to_string()),
        }
    }

    #[test]
    fn test_partition_index_builder_resume_block_range_uses_numeric_frontier() {
        let existing_rows = vec![
            block_range_row(90_000_000, 100_000_000, 10_000_000),
            block_range_row(100_000_000, 110_000_000, 10_000_000),
        ];

        let (_builder, resume_start_block) = PartitionIndexBuilder::resume_from_existing(
            "solana-mainnet-beta",
            vec![PartitionBuildType::BlockRange],
            existing_rows,
        )
        .expect("resume builder");

        assert_eq!(resume_start_block, 110_000_000);
    }

    #[test]
    fn test_partition_index_builder_resume_block_range_accepts_partial_terminal_row() {
        let existing_rows = vec![
            block_range_row(400_000_000, 400_100_000, 100_000),
            block_range_row(400_100_000, 400_150_000, 100_000),
        ];

        let (_builder, resume_start_block) = PartitionIndexBuilder::resume_from_existing(
            "solana-mainnet-beta",
            vec![PartitionBuildType::BlockRange],
            existing_rows,
        )
        .expect("resume builder");

        assert_eq!(resume_start_block, 400_150_000);
    }

    #[test]
    fn test_partition_index_builder_resume_block_range_rejects_gap() {
        let err = PartitionIndexBuilder::resume_from_existing(
            "solana-mainnet-beta",
            vec![PartitionBuildType::BlockRange],
            vec![
                block_range_row(400_000_000, 400_100_000, 100_000),
                block_range_row(400_200_000, 400_300_000, 100_000),
            ],
        )
        .expect_err("gap should fail");

        assert!(err
            .to_string()
            .contains("partition type block_range has gap"));
    }

    #[test]
    fn test_partition_value_key_orders_block_ranges_and_timestamps_numerically() {
        let key = |partition_type, value| {
            partition_value_key(partition_type, value).expect("valid partition value")
        };
        assert_eq!(key("block_range", "10000000"), 10_000_000);
        assert!(key("block_range", "8000000") < key("block_range", "10000000"));
        assert_eq!(key("date", "2015-07-30 00:00:00"), 1_438_214_400);
        assert_eq!(key("hour", "1970-01-01 01:00:00"), 3_600);
        assert!(key("hour", "2015-07-30 09:00:00") < key("hour", "2015-07-30 10:00:00"));

        for (partition_type, value) in [
            ("block_range", "2015-07-30 00:00:00"),
            ("block_range", "-1"),
            ("date", "10000000"),
            ("date", "2015-07-30"),
            ("date", "1969-12-31 00:00:00"),
            ("unknown", "0"),
        ] {
            assert!(
                partition_value_key(partition_type, value).is_err(),
                "expected {partition_type}={value} to be rejected"
            );
        }
    }

    /// Block-range rows for [8M, 12M) written out of order, so that lexicographic
    /// sorting (`"10000000" < "8000000"`) and numeric sorting disagree.
    fn write_multi_digit_block_range_index(path: &std::path::Path) {
        write_test_partitions_index(
            path,
            vec![
                block_range_row(10_000_000, 11_000_000, 1_000_000),
                block_range_row(8_000_000, 9_000_000, 1_000_000),
                block_range_row(11_000_000, 12_000_000, 1_000_000),
                block_range_row(9_000_000, 10_000_000, 1_000_000),
            ],
        )
        .expect("write partitions index");
    }

    fn block_range_list_request(path: &std::path::Path) -> PartitionListRequest {
        PartitionListRequest {
            index_path: path.to_string_lossy().to_string(),
            partition_type: Some("block_range".to_string()),
            chain: None,
            from: None,
            to: None,
            limit: usize::MAX,
        }
    }

    #[test]
    fn test_read_partitions_build_rows_sorts_block_range_numerically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_multi_digit_block_range_index(&path);

        let rows = read_partitions_build_rows(&path.to_string_lossy(), None).expect("read rows");
        let values = rows
            .iter()
            .map(|row| row.partition_value.as_str())
            .collect::<Vec<_>>();
        assert_eq!(values, ["8000000", "9000000", "10000000", "11000000"]);
    }

    #[test]
    fn test_partition_index_builder_sorts_block_range_rows_numerically() {
        let mut builder =
            PartitionIndexBuilder::new("solana-mainnet-beta", vec![PartitionBuildType::BlockRange])
                .expect("builder")
                .with_block_range_size(1_000_000);
        for block_num in [8_000_000, 9_000_000, 10_000_000, 11_000_000] {
            builder
                .observe_block(&BlockIdentity {
                    block_num,
                    timestamp: 1_690_815_540,
                    ..Default::default()
                })
                .expect("observe block");
        }

        let starts =
            |rows: &[PartitionBuildRow]| rows.iter().map(|row| row.start_block).collect::<Vec<_>>();
        let expected = [8_000_000, 9_000_000, 10_000_000, 11_000_000];
        let snapshot = builder.snapshot(11_500_000).expect("snapshot rows");
        assert_eq!(starts(&snapshot), expected);
        let rows = builder.finish(12_000_000).expect("finish rows");
        assert_eq!(starts(&rows), expected);
    }

    #[test]
    fn test_list_partitions_from_index_orders_and_filters_block_range_numerically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_multi_digit_block_range_index(&path);

        let mut request = block_range_list_request(&path);
        request.limit = 2;
        let result = list_partitions_from_index(&request, None).expect("list should succeed");
        assert_eq!(result.total_matches, 4);
        let starts = result
            .rows
            .iter()
            .map(|row| row.start_block)
            .collect::<Vec<_>>();
        assert_eq!(starts, [8_000_000, 9_000_000]);

        let mut request = block_range_list_request(&path);
        request.from = Some("9000000".to_string());
        request.to = Some("10000000".to_string());
        let result = list_partitions_from_index(&request, None).expect("list should succeed");
        let starts = result
            .rows
            .iter()
            .map(|row| row.start_block)
            .collect::<Vec<_>>();
        assert_eq!(starts, [9_000_000, 10_000_000]);
    }

    #[test]
    fn test_list_partitions_from_index_rejects_bound_in_wrong_format() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_multi_digit_block_range_index(&path);

        let mut request = block_range_list_request(&path);
        request.from = Some("2015-07-29 00:00:00".to_string());
        let err = list_partitions_from_index(&request, None)
            .expect_err("timestamp bound should be rejected for block_range rows");
        assert!(err.to_string().contains("--from"), "{err}");
    }

    #[test]
    fn test_shard_partitions_from_index_ordinal_uses_numeric_block_range_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_multi_digit_block_range_index(&path);

        let shard_starts = |shard_index| {
            shard_partitions_from_index(
                &PartitionShardRequest {
                    list: block_range_list_request(&path),
                    shard_count: 2,
                    shard_index,
                    strategy: PartitionShardStrategy::Ordinal,
                },
                None,
            )
            .expect("shard should succeed")
            .rows
            .iter()
            .map(|row| row.start_block)
            .collect::<Vec<_>>()
        };
        assert_eq!(shard_starts(0), [8_000_000, 10_000_000]);
        assert_eq!(shard_starts(1), [9_000_000, 11_000_000]);
    }

    #[test]
    fn test_validate_partitions_index_accepts_contiguous_multi_digit_block_ranges() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_multi_digit_block_range_index(&path);

        let result = validate_partitions_index(
            &PartitionValidateRequest {
                list: block_range_list_request(&path),
                allow_gaps: false,
            },
            None,
        )
        .expect("validation should run");
        assert!(result.valid, "unexpected issues: {:?}", result.issues);
        assert_eq!(result.total_rows, 4);
    }

    #[test]
    fn test_validate_partitions_index_reports_single_multi_digit_block_range_gap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_test_partitions_index(
            &path,
            vec![
                block_range_row(11_000_000, 12_000_000, 1_000_000),
                block_range_row(8_000_000, 9_000_000, 1_000_000),
                block_range_row(9_000_000, 10_000_000, 1_000_000),
            ],
        )
        .expect("write partitions index");

        let result = validate_partitions_index(
            &PartitionValidateRequest {
                list: block_range_list_request(&path),
                allow_gaps: false,
            },
            None,
        )
        .expect("validation should run");
        assert_eq!(result.issue_count, 1, "issues: {:?}", result.issues);
        assert_eq!(result.issues[0].kind, PartitionValidationIssueKind::Gap);
        assert_eq!(result.issues[0].partition_value, "11000000");
    }

    #[test]
    fn test_resolve_partition_window_bounds_from_index_block_range_numeric() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_multi_digit_block_range_index(&path);

        let bounds = resolve_partition_window_bounds_from_index(
            &PartitionWindowRequest {
                index_path: path.to_string_lossy().to_string(),
                partition_type: "block_range".to_string(),
                partition_from: "9000000".to_string(),
                partition_to: "11000000".to_string(),
                chain: None,
            },
            None,
        )
        .expect("partition window bounds should resolve");
        assert_eq!(bounds.start_block, 9_000_000);
        assert_eq!(bounds.stop_block, 11_000_000);
        assert_eq!(bounds.partitions_count, 2);
    }

    #[test]
    fn test_resolve_partition_bounds_from_index_block_range_multi_digit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");
        write_multi_digit_block_range_index(&path);

        let bounds = resolve_partition_bounds_from_index(
            &PartitionBoundsRequest {
                index_path: path.to_string_lossy().to_string(),
                partition_type: "block_range".to_string(),
                partition_value: "10000000".to_string(),
                chain: None,
            },
            None,
        )
        .expect("partition bounds should resolve");
        assert_eq!(bounds.start_block, 10_000_000);
        assert_eq!(bounds.stop_block, 11_000_000);
    }

    #[test]
    fn test_write_and_read_partitions_build_rows_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("eth-mainnet").join("partitions.parquet");
        let rows = vec![PartitionBuildRow {
            partition_type: "hour".to_string(),
            partition_interval_seconds: 3_600,
            partition_start_ts: "2023-07-31 14:00:00".to_string(),
            partition_value: "2023-07-31 14:00:00".to_string(),
            start_block: 100,
            stop_block: 103,
            start_time: Some("2023-07-31 14:59:00".to_string()),
            end_time: Some("2023-07-31 15:00:00".to_string()),
            chain: Some("eth-mainnet".to_string()),
        }];

        write_partitions_index(&path.to_string_lossy(), &rows, None).expect("write rows");
        let read_back =
            read_partitions_build_rows(&path.to_string_lossy(), None).expect("read rows back");
        assert_eq!(read_back, rows);
    }

    #[test]
    fn test_write_partitions_index_uses_updated_schema() {
        use arrow::datatypes::{DataType, TimeUnit};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("eth-mainnet").join("partitions.parquet");
        let rows = vec![PartitionBuildRow {
            partition_type: "hour".to_string(),
            partition_interval_seconds: 3_600,
            partition_start_ts: "2023-07-31 14:00:00".to_string(),
            partition_value: "2023-07-31 14:00:00".to_string(),
            start_block: 100,
            stop_block: 103,
            start_time: Some("2023-07-31 14:59:00".to_string()),
            end_time: Some("2023-07-31 15:00:00".to_string()),
            chain: Some("eth-mainnet".to_string()),
        }];

        write_partitions_index(&path.to_string_lossy(), &rows, None).expect("write rows");

        let file = std::fs::File::open(&path).expect("open parquet");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("builder");
        let schema = builder.schema();

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            vec![
                "partition",
                "start_block",
                "stop_block",
                "start_time",
                "end_time",
            ]
        );
        // chain, type, interval are in file-level metadata, not columns
        assert!(schema.field_with_name("chain").is_err());
        assert!(schema.field_with_name("type").is_err());
        assert!(schema.field_with_name("interval").is_err());
        assert!(schema.field_with_name("partition_start_ts").is_err());
        assert_eq!(
            schema
                .field_with_name("partition")
                .expect("partition")
                .data_type(),
            &DataType::UInt64
        );

        // Verify file-level metadata uses the namespaced partition key only
        let file_metadata = builder.metadata().file_metadata();
        let kvs = file_metadata.key_value_metadata().expect("metadata");
        let partition_kv = kvs
            .iter()
            .find(|kv| kv.key == "firehose-parquet.partition")
            .expect("firehose-parquet.partition metadata");
        assert_eq!(partition_kv.value.as_deref(), Some("hour"));
        assert!(!kvs.iter().any(|kv| kv.key == "partition_type"));
        assert_eq!(
            schema
                .field_with_name("start_time")
                .expect("start_time")
                .data_type(),
            &DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC")))
        );
        assert!(
            schema
                .field_with_name("start_time")
                .expect("start_time")
                .is_nullable(),
            "start_time should be nullable"
        );
        assert_eq!(
            schema
                .field_with_name("end_time")
                .expect("end_time")
                .data_type(),
            &DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC")))
        );
        assert!(
            schema
                .field_with_name("end_time")
                .expect("end_time")
                .is_nullable(),
            "end_time should be nullable"
        );
    }

    #[test]
    fn test_write_partitions_index_with_metadata_preserves_firehose_metadata() {
        use crate::writer::ParquetFileMetadata;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("eth-mainnet").join("partitions.parquet");
        let rows = vec![PartitionBuildRow {
            partition_type: "hour".to_string(),
            partition_interval_seconds: 3_600,
            partition_start_ts: "2023-07-31 14:00:00".to_string(),
            partition_value: "2023-07-31 14:00:00".to_string(),
            start_block: 100,
            stop_block: 103,
            start_time: Some("2023-07-31 14:59:00".to_string()),
            end_time: Some("2023-07-31 15:00:00".to_string()),
            chain: Some("eth-mainnet".to_string()),
        }];
        let mut metadata = ParquetFileMetadata::new();
        metadata.add("firehose-parquet.version", "0.5.4-test");
        metadata.add("firehose-parquet.endpoint", "https://example.com:443");

        write_partitions_index_with_metadata(
            &path.to_string_lossy(),
            &rows,
            Compression::Zstd,
            None,
            Some(&metadata),
        )
        .expect("write rows with metadata");

        let file = std::fs::File::open(&path).expect("open parquet");
        let reader = SerializedFileReader::new(file).expect("reader");
        let file_meta = reader.metadata().file_metadata();
        let kv = file_meta
            .key_value_metadata()
            .expect("key value metadata should exist");

        assert!(kv.iter().any(|entry| {
            entry.key == "firehose-parquet.version" && entry.value.as_deref() == Some("0.5.4-test")
        }));
        assert!(kv.iter().any(|entry| {
            entry.key == "firehose-parquet.endpoint"
                && entry.value.as_deref() == Some("https://example.com:443")
        }));
        assert!(!kv
            .iter()
            .any(|entry| entry.key.starts_with("firehose-parquet.partitions.")));
    }

    #[test]
    fn test_collect_scan_s3_parquet_objects_treats_exact_file_as_single_object() {
        use bytes::Bytes;
        use object_store::memory::InMemory;
        use object_store::path::Path;
        use object_store::ObjectStore;

        let store = InMemory::new();
        let location = Path::from("mainnet/partitions.parquet");
        block_on_async(async {
            store
                .put(
                    &location,
                    object_store::PutPayload::from(Bytes::from_static(b"parquet")),
                )
                .await
        })
        .expect("put object");

        let (objects, exact_object_path) = block_on_async(collect_scan_s3_parquet_objects(
            &store,
            "mainnet/partitions.parquet",
        ))
        .expect("collect objects");

        assert!(exact_object_path);
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].location.as_ref(), "mainnet/partitions.parquet");
    }

    #[test]
    fn test_collect_scan_s3_parquet_objects_lists_prefix_and_filters_parquet() {
        use bytes::Bytes;
        use object_store::memory::InMemory;
        use object_store::path::Path;
        use object_store::ObjectStore;

        let store = InMemory::new();
        block_on_async(async {
            store
                .put(
                    &Path::from("mainnet/a.parquet"),
                    object_store::PutPayload::from(Bytes::from_static(b"a")),
                )
                .await?;
            store
                .put(
                    &Path::from("mainnet/nested/b.parquet"),
                    object_store::PutPayload::from(Bytes::from_static(b"b")),
                )
                .await?;
            store
                .put(
                    &Path::from("mainnet/notes.txt"),
                    object_store::PutPayload::from(Bytes::from_static(b"txt")),
                )
                .await
        })
        .expect("put objects");

        let (objects, exact_object_path) =
            block_on_async(collect_scan_s3_parquet_objects(&store, "mainnet"))
                .expect("collect objects");

        assert!(!exact_object_path);
        assert_eq!(
            objects
                .iter()
                .map(|obj| obj.location.as_ref())
                .collect::<Vec<_>>(),
            vec!["mainnet/a.parquet", "mainnet/nested/b.parquet"]
        );
    }

    #[test]
    fn test_scan_s3_display_key_keeps_exact_object_key() {
        assert_eq!(
            scan_s3_display_key(
                "mainnet/partitions.parquet",
                "mainnet/partitions.parquet",
                true
            ),
            "mainnet/partitions.parquet"
        );
    }

    #[test]
    fn test_write_partitions_index_defaults_to_zstd_compression() {
        use parquet::basic::Compression as PqCompression;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("eth-mainnet").join("partitions.parquet");
        let rows = vec![PartitionBuildRow {
            partition_type: "hour".to_string(),
            partition_interval_seconds: 3_600,
            partition_start_ts: "2023-07-31 14:00:00".to_string(),
            partition_value: "2023-07-31 14:00:00".to_string(),
            start_block: 100,
            stop_block: 103,
            start_time: Some("2023-07-31 14:59:00".to_string()),
            end_time: Some("2023-07-31 15:00:00".to_string()),
            chain: Some("eth-mainnet".to_string()),
        }];

        write_partitions_index(&path.to_string_lossy(), &rows, None).expect("write rows");

        let file = std::fs::File::open(&path).expect("open parquet");
        let reader = SerializedFileReader::new(file).expect("reader");
        let compression = reader.metadata().row_group(0).column(0).compression();

        assert!(matches!(compression, PqCompression::ZSTD(_)));
    }

    #[test]
    fn test_write_partitions_index_with_metadata_honors_snappy_compression() {
        use crate::writer::ParquetFileMetadata;
        use parquet::basic::Compression as PqCompression;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("eth-mainnet").join("partitions.parquet");
        let rows = vec![PartitionBuildRow {
            partition_type: "hour".to_string(),
            partition_interval_seconds: 3_600,
            partition_start_ts: "2023-07-31 14:00:00".to_string(),
            partition_value: "2023-07-31 14:00:00".to_string(),
            start_block: 100,
            stop_block: 103,
            start_time: Some("2023-07-31 14:59:00".to_string()),
            end_time: Some("2023-07-31 15:00:00".to_string()),
            chain: Some("eth-mainnet".to_string()),
        }];
        let mut metadata = ParquetFileMetadata::new();
        metadata.add("firehose-parquet.compression", "snappy");

        write_partitions_index_with_metadata(
            &path.to_string_lossy(),
            &rows,
            Compression::Snappy,
            None,
            Some(&metadata),
        )
        .expect("write rows with snappy compression");

        let file = std::fs::File::open(&path).expect("open parquet");
        let reader = SerializedFileReader::new(file).expect("reader");
        let compression = reader.metadata().row_group(0).column(0).compression();

        assert_eq!(compression, PqCompression::SNAPPY);
    }

    #[test]
    fn test_format_array_value_formats_timestamp_second_utc() {
        use arrow::array::TimestampSecondArray;

        let array = TimestampSecondArray::from(vec![0]).with_timezone("UTC");

        assert_eq!(format_array_value(&array, 0), "1970-01-01 00:00:00 UTC");
    }

    #[test]
    fn test_format_array_value_formats_timestamp_millisecond_utc() {
        use arrow::array::TimestampMillisecondArray;

        let array = TimestampMillisecondArray::from(vec![123]).with_timezone("UTC");

        assert_eq!(format_array_value(&array, 0), "1970-01-01 00:00:00.123 UTC");
    }

    #[test]
    fn test_collect_sample_rows_respects_ascending_and_descending_pagination() {
        use arrow::array::Int32Array;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50]))],
        )
        .expect("record batch");

        let asc_rows = collect_sample_rows(
            &schema,
            vec![Ok(batch.clone())].into_iter(),
            5,
            2,
            1,
            ScanOrder::Asc,
        );
        assert_eq!(
            asc_rows
                .iter()
                .map(|row| row.row_number)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            asc_rows
                .iter()
                .map(|row| row.cells[0].value.as_str())
                .collect::<Vec<_>>(),
            vec!["20", "30"]
        );

        let desc_rows = collect_sample_rows(
            &schema,
            vec![Ok(batch.clone())].into_iter(),
            5,
            2,
            1,
            ScanOrder::Desc,
        );
        assert_eq!(
            desc_rows
                .iter()
                .map(|row| row.row_number)
                .collect::<Vec<_>>(),
            vec![4, 3]
        );
        assert_eq!(
            desc_rows
                .iter()
                .map(|row| row.cells[0].value.as_str())
                .collect::<Vec<_>>(),
            vec!["40", "30"]
        );
    }

    #[test]
    fn test_collect_sample_rows_returns_empty_when_offset_exceeds_selected_order() {
        use arrow::array::Int32Array;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("record batch");

        let rows = collect_sample_rows(
            &schema,
            vec![Ok(batch)].into_iter(),
            3,
            2,
            3,
            ScanOrder::Desc,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn test_collect_scan_parquet_local_applies_limit_globally_across_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_scan_test_parquet(&dir.path().join("a.parquet"), &[1, 2]);
        write_scan_test_parquet(&dir.path().join("b.parquet"), &[3, 4]);
        write_scan_test_parquet(&dir.path().join("c.parquet"), &[5, 6]);

        let files =
            collect_scan_parquet_local(dir.path(), 3, 0, ScanOrder::Asc, false).expect("scan");

        assert_eq!(
            files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["a.parquet", "b.parquet"]
        );
        assert_eq!(
            files
                .iter()
                .flat_map(|file| file.sample_rows.iter())
                .map(|row| row.cells[0].value.as_str())
                .collect::<Vec<_>>(),
            vec!["1", "2", "3"]
        );
    }

    #[test]
    fn test_collect_scan_parquet_local_applies_offset_globally_across_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_scan_test_parquet(&dir.path().join("a.parquet"), &[1, 2]);
        write_scan_test_parquet(&dir.path().join("b.parquet"), &[3, 4]);
        write_scan_test_parquet(&dir.path().join("c.parquet"), &[5, 6]);

        let files =
            collect_scan_parquet_local(dir.path(), 2, 3, ScanOrder::Asc, false).expect("scan");

        assert_eq!(
            files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["a.parquet", "b.parquet", "c.parquet"]
        );
        assert_eq!(
            files
                .iter()
                .flat_map(|file| file.sample_rows.iter())
                .map(|row| row.cells[0].value.as_str())
                .collect::<Vec<_>>(),
            vec!["4", "5"]
        );
    }

    #[test]
    fn test_format_scan_rows_table_includes_headers_and_row_numbers() {
        let file = ScanFileResult {
            path: "blocks.parquet".to_string(),
            total_rows: 2,
            row_groups: 1,
            columns: 2,
            size_bytes: 42,
            size_human: "42 B".to_string(),
            schema: vec![
                ScanSchemaColumn {
                    name: "block_num".to_string(),
                    data_type: "UInt64".to_string(),
                    nullable: false,
                },
                ScanSchemaColumn {
                    name: "block_hash".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                },
            ],
            sample_rows: vec![
                ScanRow {
                    row_number: 1,
                    cells: vec![
                        ScanRowCell {
                            name: "block_num".to_string(),
                            value: "1".to_string(),
                        },
                        ScanRowCell {
                            name: "block_hash".to_string(),
                            value: "0xabc".to_string(),
                        },
                    ],
                },
                ScanRow {
                    row_number: 2,
                    cells: vec![
                        ScanRowCell {
                            name: "block_num".to_string(),
                            value: "2".to_string(),
                        },
                        ScanRowCell {
                            name: "block_hash".to_string(),
                            value: "0xdef".to_string(),
                        },
                    ],
                },
            ],
        };

        let rendered = format_scan_rows_table(&file);
        assert!(rendered.contains("┌"));
        assert!(rendered.contains("block_num"));
        assert!(rendered.contains("block_hash"));
        assert!(rendered.contains("1. │ 1"));
        assert!(rendered.contains("2. │ 2"));
    }

    #[test]
    fn test_format_scan_rows_table_aligns_border_with_single_digit_row_numbers() {
        let file = ScanFileResult {
            path: "blocks.parquet".to_string(),
            total_rows: 2,
            row_groups: 1,
            columns: 1,
            size_bytes: 42,
            size_human: "42 B".to_string(),
            schema: vec![ScanSchemaColumn {
                name: "block_num".to_string(),
                data_type: "UInt64".to_string(),
                nullable: false,
            }],
            sample_rows: vec![
                ScanRow {
                    row_number: 1,
                    cells: vec![ScanRowCell {
                        name: "block_num".to_string(),
                        value: "1".to_string(),
                    }],
                },
                ScanRow {
                    row_number: 2,
                    cells: vec![ScanRowCell {
                        name: "block_num".to_string(),
                        value: "2".to_string(),
                    }],
                },
            ],
        };

        let rendered = format_scan_rows_table(&file);
        let lines = rendered.lines().collect::<Vec<_>>();

        assert_eq!(lines[0].find('┌'), lines[1].find('│'));
        assert_eq!(lines[0].find('┌'), lines[2].find('├'));
        assert_eq!(lines[0].find('┌'), lines[3].find('│'));
        assert_eq!(lines[0].find('┌'), lines[4].find('│'));
        assert_eq!(lines[0].find('┌'), lines[5].find('└'));
    }

    #[test]
    fn test_format_scan_rows_vertical_matches_legacy_style() {
        let file = ScanFileResult {
            path: "blocks.parquet".to_string(),
            total_rows: 1,
            row_groups: 1,
            columns: 1,
            size_bytes: 42,
            size_human: "42 B".to_string(),
            schema: vec![ScanSchemaColumn {
                name: "block_num".to_string(),
                data_type: "UInt64".to_string(),
                nullable: false,
            }],
            sample_rows: vec![ScanRow {
                row_number: 1,
                cells: vec![ScanRowCell {
                    name: "block_num".to_string(),
                    value: "42".to_string(),
                }],
            }],
        };

        let rendered = format_scan_rows_vertical(&file);
        assert!(rendered.contains("Row 1:"));
        assert!(rendered.contains("block_num"));
        assert!(rendered.contains("42"));
    }
}
