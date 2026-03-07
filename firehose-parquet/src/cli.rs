use crate::config::{Compression, Config, Partition};
use clap::Args;
use clap_complete::{generate, Shell};
use std::io;
use std::path::PathBuf;

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

    /// Start block number (inclusive)
    #[arg(
        short = 's',
        long,
        env = "START_BLOCK",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub start_block: Option<u64>,

    /// Stop block number (exclusive, 0 = stream forever)
    #[arg(
        short = 't',
        long,
        env = "STOP_BLOCK",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub stop_block: Option<u64>,

    /// Path to partitions index parquet file (local path or s3:// URI), e.g. ./output/eth-mainnet/partitions.parquet
    #[arg(
        long,
        env = "PARTITIONS_INDEX",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub partitions_index: Option<String>,

    /// Partition type used to resolve start/stop range from --partitions-index, e.g. hour or date
    #[arg(
        long,
        env = "PARTITION_TYPE",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub partition_type: Option<String>,

    /// Partition value used to resolve start/stop range from --partitions-index, e.g. "2015-07-30 15:00:00"
    #[arg(
        long,
        env = "PARTITION_VALUE",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub partition_value: Option<String>,

    /// Inclusive partition window start used with --partition-from/--partition-to mode
    #[arg(
        long,
        env = "PARTITION_FROM",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub partition_from: Option<String>,

    /// Exclusive partition window end used with --partition-from/--partition-to mode
    #[arg(
        long,
        env = "PARTITION_TO",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub partition_to: Option<String>,

    /// Optional chain filter used with partition lookup (matches `chain` column), e.g. eth-mainnet
    #[arg(
        long,
        env = "PARTITION_CHAIN",
        hide_env_values = true,
        help_heading = "Block Range"
    )]
    pub partition_chain: Option<String>,

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

    /// Optional template used to derive a partition-aware cursor path.
    ///
    /// Supported variables: `{chain}`, `{partition_type}`, `{partition_value}`,
    /// `{partition_from}`, `{partition_to}`. Use `{{` and `}}` for literal braces.
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

    /// Block range size when partition=block_range
    #[arg(
        long,
        env = "BLOCK_RANGE_SIZE",
        default_value = "10000",
        hide_env_values = true,
        help_heading = "Output"
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

    /// Max rows per file before flush (disabled by default)
    #[arg(
        long,
        env = "FLUSH_ROWS",
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_rows: Option<u32>,

    /// Max bytes per file before flush (0 = disabled)
    #[arg(
        long,
        env = "FLUSH_BYTES",
        default_value = "134217728",
        hide_env_values = true,
        help_heading = "Flush"
    )]
    pub flush_bytes: u64,

    /// Time-based flush interval in seconds (disabled by default)
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

    /// Decode and map but don't write files
    #[arg(long, env = "DRY_RUN", default_value = "false", hide_env_values = true)]
    pub dry_run: bool,

    /// Prometheus /metrics HTTP port. When set, an HTTP server binds to 0.0.0.0:<PORT>/metrics.
    #[arg(long, env = "METRICS_PORT", hide_env_values = true)]
    pub metrics_port: Option<u16>,

    /// Force a reconnect if no stream message is received for N seconds
    #[arg(
        long,
        env = "STREAM_IDLE_TIMEOUT_SECS",
        default_value = "120",
        hide_env_values = true,
        help_heading = "Connection"
    )]
    pub stream_idle_timeout_secs: Option<u64>,

    /// Exit with an error if reconnecting continuously for N seconds
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

/// Subcommands shared by all binaries.
#[derive(clap::Subcommand, Debug)]
pub enum Commands {
    /// Generate shell completions for the given shell
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
    },
    /// Partition index utilities (`partitions.parquet` workflows).
    #[command(subcommand)]
    Partitions(PartitionsCommands),
    /// Read and inspect Parquet files (schema, row counts, sample rows).
    /// Supports local paths and S3 URIs (s3://bucket/prefix).
    #[command(after_long_help = "\
Examples:
  # Inspect a local parquet file
  fireparq scan ./output/blocks/part-000001.parquet

  # Scan all files in a directory (20 sample rows each)
  fireparq scan ./output/blocks/

  # Schema only, no data preview
  fireparq scan ./output/blocks/ --schema-only

  # Scan S3 files
  fireparq scan s3://bucket/eth-mainnet/blocks/

  # Show 50 sample rows per file
  fireparq scan ./output/blocks/ --limit 50
")]
    Scan {
        /// Path to a .parquet file or directory, or an S3 URI (s3://bucket/prefix)
        path: String,
        /// Number of sample rows to display per file (0 = schema only)
        /// Deprecated alias: `--rows`.
        #[arg(short = 'n', long = "limit", alias = "rows", default_value = "20")]
        limit: usize,
        /// Only show file metadata (schema, row count, size) without data
        #[arg(long, default_value = "false")]
        schema_only: bool,
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
    /// Validate block sequence integrity of Parquet files.
    ///
    /// Checks for gaps, parent hash chain, ordering, timestamps, schema
    /// consistency, empty partitions, and cross-partition continuity.
    #[command(after_long_help = "\
Examples:
  # Validate local blocks directory
  fireparq validate ./output/blocks/

  # Validate S3 path
  fireparq validate s3://bucket/eth-mainnet/blocks/

  # Check continuity across partition boundaries
  fireparq validate ./output/blocks/ --cross-partition
")]
    Validate {
        /// Path to a directory of .parquet files or an S3 URI (s3://bucket/prefix)
        path: String,
        /// Check continuity across partition boundaries
        #[arg(long, default_value = "false")]
        cross_partition: bool,
        /// Allow gaps in block numbers (e.g. Solana skipped slots)
        #[arg(long, default_value = "false")]
        allow_gaps: bool,
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
    /// Verify deterministic partition merkle roots for table parquet data.
    ///
    /// Reads parquet data, computes partition-level roots, compares to
    /// `merkle_roots.parquet`, and optionally writes missing/updated entries.
    #[command(after_long_help = "\
Examples:
  # Verify EVM blocks and auto-create missing root registry entries
  fireparq verify ./output/evm/mainnet/blocks --chain evm --table blocks

  # Quick profile (roots only)
  fireparq verify ./output/evm/mainnet/blocks --profile quick

  # Explicitly select checks regardless of profile
  fireparq verify ./output/evm/mainnet/blocks --checks roots,protocol

  # Continue scanning all partitions (no fail-fast) and emit JSON report
  fireparq verify ./output/evm/mainnet/blocks --no-fail-fast --report-json verify-report.json

  # Publish the report to the suggested verify artifact path
  fireparq verify ./output/evm/mainnet/blocks --publish-report

  # Publish the report to an explicit S3 location
  fireparq verify s3://bucket/evm/mainnet/blocks \
    --publish-report-path s3://bucket/evm/mainnet/verify_runs/custom-run/report.json

  # Verify S3 parquet data with explicit registry location
  fireparq verify s3://bucket/evm/mainnet/blocks \\
    --registry-path s3://bucket/evm/mainnet/merkle_roots.parquet

  # Override hash strategy (default: auto from chain)
  fireparq verify ./output/bitcoin/mainnet/blocks --chain bitcoin --hash-strategy sha256

  # Update mismatched registry roots (default behavior only fills missing roots)
  fireparq verify ./output/evm/mainnet/blocks --update-registry
")]
    Verify {
        /// Path to a directory of .parquet files, a single parquet file, or an S3 URI
        path: String,
        /// Chain identifier used in root registry keys
        #[arg(long, default_value = "evm")]
        chain: String,
        /// Table name used in root registry keys
        #[arg(long, default_value = "blocks")]
        table: String,
        /// Hash strategy used for leaf+merkle hashing (auto, keccak256, sha256)
        #[arg(long, default_value = "auto")]
        hash_strategy: String,
        /// Check families to run (`roots`, `protocol`, `continuity`, `completeness`)
        #[arg(long, value_enum, value_delimiter = ',')]
        checks: Vec<crate::verify::VerifyCheck>,
        /// Check profile (`quick`, `standard`, `deep`) used when --checks is not set
        #[arg(long, value_enum, default_value = "standard")]
        profile: crate::verify::VerifyProfile,
        /// Report scope tag for metadata (`chain`, `table`, `partition`, `run`)
        #[arg(long, value_enum, default_value = "table")]
        scope: crate::verify::VerifyScope,
        /// Continue scanning and aggregate findings instead of failing on first mismatch
        #[arg(long, default_value = "false")]
        no_fail_fast: bool,
        /// Optional path to write a JSON verification report
        #[arg(long)]
        report_json: Option<PathBuf>,
        /// Publish the JSON report to the suggested verify artifact path
        #[arg(long, default_value = "false")]
        publish_report: bool,
        /// Explicit path to publish the JSON report (local or s3://)
        #[arg(long)]
        publish_report_path: Option<String>,
        /// Explicit merkle roots registry path (local or s3://)
        #[arg(long)]
        registry_path: Option<String>,
        /// Overwrite mismatched roots in the registry with computed values
        #[arg(long, default_value = "false")]
        update_registry: bool,
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
    /// Roll up fine-grained partitioned Parquet files into coarser intervals.
    ///
    /// Reads minute/hour-partitioned files and merges them into hourly or daily
    /// partitions, respecting --flush-bytes for file size limits.
    #[command(after_long_help = "\
Examples:
  # Roll up minute partitions into daily (in-place)
  fireparq rollup ./output/blocks/

  # Roll up to hourly partitions with a separate output
  fireparq rollup ./output/blocks/ -o ./merged/ -p hour

  # Roll up S3 data, delete source files after
  fireparq rollup s3://bucket/blocks/ --delete-source

  # Custom file size limit (256 MB)
  fireparq rollup ./output/blocks/ --flush-bytes 268435456
")]
    Rollup {
        /// Source path (local directory or S3 URI) containing partitioned Parquet files
        source: String,
        /// Output path (local directory or S3 URI). Defaults to source (in-place rollup).
        #[arg(short = 'o', long)]
        output: Option<String>,
        /// Target partition interval: hour or date
        /// Deprecated alias: `--target-partition`.
        #[arg(
            short = 'p',
            long = "partition",
            alias = "target-partition",
            default_value = "date"
        )]
        partition: String,
        /// Compression codec: zstd, snappy, gzip, none
        #[arg(long, default_value = "zstd")]
        compression: String,
        /// Max compressed bytes per output file (0 = no limit)
        #[arg(long, default_value = "134217728")]
        flush_bytes: u64,
        /// Delete source files after successful rollup
        #[arg(long, default_value = "false")]
        delete_source: bool,
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
        /// Cache-Control header for S3 uploads (empty string = no header)
        #[arg(
            long,
            env = "CACHE_CONTROL",
            default_value = "public, max-age=31536000, immutable"
        )]
        cache_control: String,
    },
    /// Merge small parquet part files within each partition into larger files.
    ///
    /// Unlike rollup (which changes partition granularity), merge consolidates
    /// multiple small parts within each existing partition directory into fewer,
    /// larger files. Source parts are deleted after successful merge.
    #[command(after_long_help = "\
Examples:
  # Merge parts within each partition (local)
  fireparq merge ./output/blocks/

  # Merge S3 data
  fireparq merge s3://bucket/eth-mainnet/blocks/

  # Custom target file size (512 MB)
  fireparq merge ./output/blocks/ --flush-bytes 536870912

  # Preview what would be merged
  fireparq merge ./output/blocks/ --dry-run

  # Use snappy compression
  fireparq merge ./output/blocks/ --compression snappy
")]
    Merge {
        /// Path to a directory of partitioned .parquet files or an S3 URI
        path: String,
        /// Compression codec: zstd, snappy, gzip, none
        #[arg(long, default_value = "zstd")]
        compression: String,
        /// Max compressed bytes per output file
        #[arg(long, default_value = "268435456")]
        flush_bytes: u64,
        /// Show what would be merged without writing
        #[arg(long, default_value = "false")]
        dry_run: bool,
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
        /// Cache-Control header for S3 uploads
        #[arg(
            long,
            env = "CACHE_CONTROL",
            default_value = "public, max-age=31536000, immutable"
        )]
        cache_control: String,
    },
    /// Inspect a single Parquet file's metadata: file-level key-value pairs,
    /// schema, row group details, and column chunk info.
    /// Supports local paths and S3 URIs (s3://bucket/key.parquet).
    #[command(after_long_help = "\
Examples:
  # Inspect a local parquet file
  fireparq inspect ./output/blocks/part-000001.parquet

  # Inspect an S3 parquet file
  fireparq inspect s3://bucket/eth-mainnet/blocks/part-000001.parquet

  # Show only the schema with explicit nullability
  fireparq inspect s3://bucket/eth-mainnet/partitions.parquet --schema-only

  # Emit machine-readable schema details
  fireparq inspect s3://bucket/eth-mainnet/partitions.parquet --schema-only --json
")]
    Inspect {
        /// Path to a single .parquet file (local path or S3 URI)
        path: String,
        /// Only show the schema, including explicit nullability
        #[arg(long, default_value = "false")]
        schema_only: bool,
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
    /// Delete parquet files from local filesystem or S3, with optional partition filtering.
    ///
    /// Deletes only .parquet files. Never deletes buckets or non-parquet files.
    /// Use --partition to target specific partitions (supports glob patterns).
    #[command(after_long_help = "\
Examples:
  # Delete all parquet files under a path
  fireparq truncate ./output/blocks/

  # Delete only a specific date partition
  fireparq truncate ./output/blocks/ -p \"date=01\"

  # Delete with glob pattern (all of January)
  fireparq truncate s3://bucket/blocks/ -p \"month=01\"

  # Delete a specific year
  fireparq truncate ./output/ -p \"year=2026\"

  # Delete all minute-level partitions (key-only filter)
  fireparq truncate ./output/blocks/ -p minute

  # Preview what would be deleted
  fireparq truncate ./output/blocks/ --dry-run
")]
    Truncate {
        /// Path to a directory or S3 URI containing .parquet files
        path: String,
        /// Partition filter(s) — only delete files matching these partition segments.
        /// Use a key name to match all values (e.g. "date" matches all date=* partitions),
        /// or a key=value with optional glob (e.g. "date=2026-01-*"). Repeatable.
        #[arg(long, short = 'p')]
        partition: Vec<String>,
        /// Show what would be deleted without actually deleting
        #[arg(long, default_value = "false")]
        dry_run: bool,
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

/// Subcommands under `fireparq partitions`.
#[derive(clap::Subcommand, Debug)]
pub enum PartitionsCommands {
    /// Build `partitions.parquet` directly from Firehose block timestamps.
    #[command(after_long_help = "\
Examples:
  # Build a local date index for one chain
  fireparq partitions build \\
    --network mainnet \\
    --stop-block 10010000 \\
    --partition date \\
    --output ./output

  # Build to S3 with an explicit chain override and JSON output
  fireparq partitions build \\
    --network mainnet \\
    --chain eth-mainnet \\
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
")]
    Build {
        /// Firehose gRPC endpoint URL
        #[arg(long, env = "ENDPOINT", hide_env_values = true)]
        endpoint: Option<String>,
        /// Firehose network `chainName`
        #[arg(long, env = "NETWORK", hide_env_values = true)]
        network: Option<String>,
        /// Name of environment variable containing the API key for authentication
        #[arg(
            long,
            env = "API_KEY_ENVVAR",
            default_value = "SUBSTREAMS_API_KEY",
            hide_env_values = true
        )]
        api_key_envvar: String,
        /// Name of environment variable containing the JWT bearer token for authentication
        #[arg(
            long,
            env = "API_TOKEN_ENVVAR",
            default_value = "SUBSTREAMS_API_TOKEN",
            hide_env_values = true
        )]
        api_token_envvar: String,
        /// Optional chain name override; otherwise inferred from endpoint info
        #[arg(long)]
        chain: Option<String>,
        /// Start block number (inclusive).
        ///
        /// When omitted in bounded mode, falls back to a sibling `cursor.parquet`
        /// if present, then to the endpoint's first streamable block.
        ///
        /// When omitted in `--live` mode, existing `partitions.parquet` rows take
        /// precedence as the restart anchor.
        #[arg(long)]
        start_block: Option<u64>,
        /// Stop block number (exclusive).
        ///
        /// Required for bounded builds and incompatible with `--live`.
        #[arg(long, conflicts_with = "live")]
        stop_block: Option<u64>,
        /// Keep extending `partitions.parquet` from its latest covered frontier.
        #[arg(long, default_value = "false")]
        live: bool,
        /// Poll interval used by `--live` sparse probes while waiting for new blocks.
        #[arg(long, default_value_t = 30)]
        poll_interval_secs: u64,
        /// Partition to build: date, hour, minute, or second
        /// Deprecated alias: `--partition-types`.
        #[arg(long = "partition", alias = "partition-types")]
        partition: String,
        /// Output root path (local directory or s3:// URI prefix).
        ///
        /// When omitted, `--s3-bucket` or `S3_BUCKET` is required and the
        /// output root becomes `s3://<bucket>`.
        #[arg(long)]
        output: Option<String>,
        /// S3 bucket name used when `--output` is omitted or should be prefixed.
        #[arg(long, env = "S3_BUCKET", hide_env_values = true)]
        s3_bucket: Option<String>,
        /// Resume from an existing canonical index under the resolved output path
        #[arg(long, default_value = "false")]
        resume: bool,
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
        /// Path to partitions index parquet file (local path or s3:// URI)
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
        /// Path to partitions index parquet file (local path or s3:// URI)
        #[arg(long)]
        partitions_index: String,
        /// Optional partition type filter (e.g. hour, date)
        #[arg(long)]
        partition_type: Option<String>,
        /// Optional chain filter (matches `chain` column)
        #[arg(long)]
        partition_chain: Option<String>,
        /// Lower bound (inclusive) for partition_start_ts (`YYYY-MM-DD HH:MM:SS`)
        #[arg(long)]
        from: Option<String>,
        /// Upper bound (inclusive) for partition_start_ts (`YYYY-MM-DD HH:MM:SS`)
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
        /// Path to partitions index parquet file (local path or s3:// URI)
        #[arg(long)]
        partitions_index: String,
        /// Optional partition type filter (e.g. hour, date)
        #[arg(long)]
        partition_type: Option<String>,
        /// Optional chain filter (matches `chain` column)
        #[arg(long)]
        partition_chain: Option<String>,
        /// Lower bound (inclusive) for partition_start_ts (`YYYY-MM-DD HH:MM:SS`)
        #[arg(long)]
        from: Option<String>,
        /// Upper bound (inclusive) for partition_start_ts (`YYYY-MM-DD HH:MM:SS`)
        #[arg(long)]
        to: Option<String>,
        /// Maximum rows to return (sorted ascending by partition_start_ts)
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
        /// Path to partitions index parquet file (local path or s3:// URI)
        #[arg(long)]
        partitions_index: String,
        /// Partition type to resolve (e.g. hour, date)
        #[arg(long)]
        partition_type: String,
        /// Partition value to resolve (e.g. "2015-07-30 15:00:00")
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
}

impl PartitionBuildType {
    pub fn from_cli_value(value: &str) -> anyhow::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "day" | "date" => Ok(Self::Date),
            "hour" => Ok(Self::Hour),
            "minute" => Ok(Self::Minute),
            "second" => Ok(Self::Second),
            other => anyhow::bail!(
                "invalid partition type '{other}': expected one of date, hour, minute, second"
            ),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
        }
    }

    pub fn interval_seconds(&self) -> i64 {
        match self {
            Self::Date => 86_400,
            Self::Hour => 3_600,
            Self::Minute => 60,
            Self::Second => 1,
        }
    }

    pub fn round_timestamp(&self, timestamp: i64) -> anyhow::Result<i64> {
        use time::OffsetDateTime;

        let dt = OffsetDateTime::from_unix_timestamp(timestamp)
            .map_err(|e| anyhow::anyhow!("invalid unix timestamp {timestamp}: {e}"))?;
        let rounded = match self {
            Self::Date => dt.replace_time(time::Time::MIDNIGHT),
            Self::Hour => dt.replace_minute(0)?.replace_second(0)?,
            Self::Minute => dt.replace_second(0)?,
            Self::Second => dt,
        };
        Ok(rounded.unix_timestamp())
    }
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
    pub partition_start_ts: String,
    pub partition_value: String,
    pub start_block: u64,
    pub end_block: u64,
    pub start_time: String,
    pub end_time: String,
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
    start_time: i64,
    last_block: u64,
    last_time: i64,
}

#[derive(Debug, Clone)]
pub struct PartitionIndexBuilder {
    chain: String,
    partition_types: Vec<PartitionBuildType>,
    active: std::collections::BTreeMap<PartitionBuildType, ActivePartitionBuildRow>,
    rows: Vec<PartitionBuildRow>,
    first_seen_block: Option<u64>,
    last_seen_block: Option<u64>,
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
        })
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

        for partition_type in self.partition_types.clone() {
            let partition_start_ts = partition_type.round_timestamp(block.timestamp)?;
            let partition_value = format_partition_timestamp(partition_start_ts)?;

            match self.active.get_mut(&partition_type) {
                Some(active) if active.partition_start_ts == partition_start_ts => {
                    active.last_block = block.block_num;
                    active.last_time = block.timestamp;
                }
                Some(active) => {
                    let finalized = build_partition_row(
                        &self.chain,
                        partition_type,
                        active.clone(),
                        block.block_num,
                    )?;
                    self.rows.push(finalized);
                    *active = ActivePartitionBuildRow {
                        partition_start_ts,
                        partition_value,
                        start_block: block.block_num,
                        start_time: block.timestamp,
                        last_block: block.block_num,
                        last_time: block.timestamp,
                    };
                }
                None => {
                    self.active.insert(
                        partition_type,
                        ActivePartitionBuildRow {
                            partition_start_ts,
                            partition_value,
                            start_block: block.block_num,
                            start_time: block.timestamp,
                            last_block: block.block_num,
                            last_time: block.timestamp,
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
                )?);
            }
        }

        self.rows.sort_by(|left, right| {
            left.partition_type
                .cmp(&right.partition_type)
                .then_with(|| left.partition_start_ts.cmp(&right.partition_start_ts))
                .then_with(|| left.start_block.cmp(&right.start_block))
                .then_with(|| left.end_block.cmp(&right.end_block))
        });

        Ok(self.rows)
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
                )?);
            }
        }

        rows.sort_by(|left, right| {
            left.partition_type
                .cmp(&right.partition_type)
                .then_with(|| left.partition_start_ts.cmp(&right.partition_start_ts))
                .then_with(|| left.start_block.cmp(&right.start_block))
                .then_with(|| left.end_block.cmp(&right.end_block))
        });

        Ok(rows)
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
        let resume_block = existing_rows
            .iter()
            .map(|row| row.end_block)
            .max()
            .ok_or_else(|| anyhow::anyhow!("missing existing end_block for resume"))?;

        let mut active = std::collections::BTreeMap::new();
        let mut retained_rows = Vec::new();

        for partition_type in partition_types.iter().copied() {
            let mut matching = existing_rows
                .iter()
                .enumerate()
                .filter(|(_, row)| row.partition_type == partition_type.as_str())
                .collect::<Vec<_>>();
            matching.sort_by(|left, right| {
                left.1
                    .partition_start_ts
                    .cmp(&right.1.partition_start_ts)
                    .then_with(|| left.1.start_block.cmp(&right.1.start_block))
                    .then_with(|| left.1.end_block.cmp(&right.1.end_block))
            });

            let Some((last_index, last_row)) = matching.pop() else {
                anyhow::bail!(
                    "cannot resume partition build for chain {}: missing existing rows for partition type {}",
                    chain,
                    partition_type
                );
            };

            if last_row.end_block != resume_block {
                anyhow::bail!(
                    "cannot resume partition build: partition type {} ends at {}, expected common frontier {}",
                    partition_type,
                    last_row.end_block,
                    resume_block
                );
            }

            active.insert(
                partition_type,
                ActivePartitionBuildRow {
                    partition_start_ts: parse_partition_timestamp(&last_row.partition_start_ts)?,
                    partition_value: last_row.partition_value.clone(),
                    start_block: last_row.start_block,
                    start_time: parse_partition_timestamp(&last_row.start_time)?,
                    last_block: last_row.end_block.saturating_sub(1),
                    last_time: parse_partition_timestamp(&last_row.end_time)?,
                },
            );

            existing_rows.remove(last_index);
        }

        retained_rows.extend(existing_rows);

        Ok((
            Self {
                chain,
                partition_types,
                active,
                rows: retained_rows,
                first_seen_block: Some(resume_block),
                last_seen_block: resume_block.checked_sub(1),
            },
            resume_block,
        ))
    }
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
    pub end_block: u64,
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
        (Some(output), Some(bucket)) if !output.starts_with("s3://") => {
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
    end_block: u64,
) -> anyhow::Result<PartitionBuildRow> {
    if active.start_block >= end_block {
        anyhow::bail!(
            "invalid partition row for {} {}: start_block {} must be < end_block {}",
            partition_type,
            active.partition_value,
            active.start_block,
            end_block
        );
    }

    Ok(PartitionBuildRow {
        partition_type: partition_type.to_string(),
        partition_interval_seconds: partition_type.interval_seconds(),
        partition_start_ts: format_partition_timestamp(active.partition_start_ts)?,
        partition_value: active.partition_value,
        start_block: active.start_block,
        end_block,
        start_time: format_partition_timestamp(active.start_time)?,
        end_time: format_partition_timestamp(active.last_time)?,
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

pub fn read_partitions_build_rows(
    path: &str,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<Vec<PartitionBuildRow>> {
    use arrow::array::{
        Array, Int32Array, Int64Array, LargeStringArray, StringArray, UInt32Array, UInt64Array,
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

    fn collect_rows(
        batch: &arrow::record_batch::RecordBatch,
        rows: &mut Vec<PartitionBuildRow>,
        read_utf8_value: &impl Fn(&dyn Array, usize) -> anyhow::Result<Option<String>>,
        read_i64_value: &impl Fn(&dyn Array, usize) -> anyhow::Result<Option<i64>>,
        read_u64_value: &impl Fn(&dyn Array, usize) -> anyhow::Result<Option<u64>>,
    ) -> anyhow::Result<()> {
        let schema = batch.schema();
        let partition_type_idx = schema.index_of("partition_type")?;
        let partition_value_idx = schema.index_of("partition_value")?;
        let partition_start_ts_idx = schema.index_of("partition_start_ts").ok();
        let partition_interval_seconds_idx = schema.index_of("partition_interval_seconds").ok();
        let start_block_idx = schema.index_of("start_block")?;
        let end_block_idx = schema.index_of("end_block")?;
        let chain_idx = schema.index_of("chain").ok();
        let start_time_idx = schema.index_of("start_time").ok();
        let end_time_idx = schema.index_of("end_time").ok();

        for row_index in 0..batch.num_rows() {
            let partition_type = canonical_partition_type_label(
                read_utf8_value(batch.column(partition_type_idx).as_ref(), row_index)?
                    .ok_or_else(|| anyhow::anyhow!("partition_type cannot be null"))?
                    .as_str(),
            )?;
            let partition_value =
                read_utf8_value(batch.column(partition_value_idx).as_ref(), row_index)?
                    .ok_or_else(|| anyhow::anyhow!("partition_value cannot be null"))?;
            let partition_start_ts = partition_start_ts_idx
                .and_then(|idx| {
                    read_utf8_value(batch.column(idx).as_ref(), row_index)
                        .ok()
                        .flatten()
                })
                .unwrap_or_else(|| partition_value.clone());
            let partition_interval_seconds = match partition_interval_seconds_idx {
                Some(idx) => {
                    read_i64_value(batch.column(idx).as_ref(), row_index)?.unwrap_or_else(|| {
                        PartitionBuildType::from_cli_value(&partition_type)
                            .map(|kind| kind.interval_seconds())
                            .unwrap_or_default()
                    })
                }
                None => PartitionBuildType::from_cli_value(&partition_type)?.interval_seconds(),
            };
            let start_block = read_u64_value(batch.column(start_block_idx).as_ref(), row_index)?
                .ok_or_else(|| anyhow::anyhow!("start_block cannot be null"))?;
            let end_block = read_u64_value(batch.column(end_block_idx).as_ref(), row_index)?
                .ok_or_else(|| anyhow::anyhow!("end_block cannot be null"))?;
            let chain = chain_idx
                .map(|idx| read_utf8_value(batch.column(idx).as_ref(), row_index))
                .transpose()?
                .flatten()
                .ok_or_else(|| anyhow::anyhow!("chain cannot be null"))?;
            let start_time = start_time_idx
                .and_then(|idx| {
                    read_utf8_value(batch.column(idx).as_ref(), row_index)
                        .ok()
                        .flatten()
                })
                .unwrap_or_else(|| partition_start_ts.clone());
            let end_time = end_time_idx
                .and_then(|idx| {
                    read_utf8_value(batch.column(idx).as_ref(), row_index)
                        .ok()
                        .flatten()
                })
                .unwrap_or_else(|| partition_start_ts.clone());

            rows.push(PartitionBuildRow {
                partition_type,
                partition_interval_seconds,
                partition_start_ts,
                partition_value,
                start_block,
                end_block,
                start_time,
                end_time,
                chain: Some(chain),
            });
        }

        Ok(())
    }

    let mut rows = Vec::new();
    if path.starts_with("s3://") {
        use crate::writer::parse_s3_url;
        use object_store::ObjectStore;

        let aws = aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?;
        let (bucket, key) = parse_s3_url(path)?;
        let client = aws.build_s3_client(&bucket)?;
        let object_path = object_store::path::Path::from(key.as_str());
        let data = block_on_async(async { client.get(&object_path).await?.bytes().await })
            .map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        let schema = builder.schema();
        validate_partitions_schema(&schema)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            collect_rows(
                &batch?,
                &mut rows,
                &read_utf8_value,
                &read_i64_value,
                &read_u64_value,
            )?;
        }
    } else {
        let file = std::fs::File::open(path).map_err(|e| anyhow::anyhow!("opening {path}: {e}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema();
        validate_partitions_schema(&schema)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            collect_rows(
                &batch?,
                &mut rows,
                &read_utf8_value,
                &read_i64_value,
                &read_u64_value,
            )?;
        }
    }

    rows.sort_by(|left, right| {
        left.partition_type
            .cmp(&right.partition_type)
            .then_with(|| left.partition_start_ts.cmp(&right.partition_start_ts))
            .then_with(|| left.start_block.cmp(&right.start_block))
            .then_with(|| left.end_block.cmp(&right.end_block))
    });
    Ok(rows)
}

pub fn write_partitions_index(
    path: &str,
    rows: &[PartitionBuildRow],
    aws: Option<&AwsConfig>,
) -> anyhow::Result<()> {
    write_partitions_index_with_metadata(path, rows, aws, None)
}

pub fn write_partitions_index_with_metadata(
    path: &str,
    rows: &[PartitionBuildRow],
    aws: Option<&AwsConfig>,
    file_metadata: Option<&crate::writer::ParquetFileMetadata>,
) -> anyhow::Result<()> {
    use arrow::array::{Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    if rows.is_empty() {
        anyhow::bail!("cannot write an empty partitions index");
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("chain", DataType::Utf8, false),
        Field::new("partition_type", DataType::Utf8, false),
        Field::new("partition_interval_seconds", DataType::Int64, false),
        Field::new("partition_start_ts", DataType::Utf8, false),
        Field::new("partition_value", DataType::Utf8, false),
        Field::new("start_block", DataType::UInt64, false),
        Field::new("end_block", DataType::UInt64, false),
        Field::new("start_time", DataType::Utf8, false),
        Field::new("end_time", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| {
                        row.chain
                            .clone()
                            .ok_or_else(|| anyhow::anyhow!("chain cannot be null"))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?,
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.partition_type.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.partition_interval_seconds)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.partition_start_ts.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.partition_value.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.start_block).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.end_block).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.start_time.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.end_time.clone())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;

    let mut props_builder = WriterProperties::builder();
    if let Some(file_metadata) = file_metadata {
        let kvs = file_metadata
            .entries
            .iter()
            .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
            .collect::<Vec<_>>();
        if !kvs.is_empty() {
            props_builder = props_builder.set_key_value_metadata(Some(kvs));
        }
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

fn validate_partitions_schema(schema: &arrow::datatypes::Schema) -> anyhow::Result<()> {
    let chain = schema
        .field_with_name("chain")
        .map_err(|_| anyhow::anyhow!("missing required column: chain"))?;
    if !is_utf8_like(chain.data_type()) {
        anyhow::bail!(
            "invalid partitions.parquet column type for chain: expected Utf8/LargeUtf8, got {}",
            chain.data_type()
        );
    }
    if chain.is_nullable() {
        anyhow::bail!("invalid partitions.parquet schema: chain must be non-nullable");
    }

    let partition_type = schema
        .field_with_name("partition_type")
        .map_err(|_| anyhow::anyhow!("missing required column: partition_type"))?;
    if !is_utf8_like(partition_type.data_type()) {
        anyhow::bail!(
            "invalid partitions.parquet column type for partition_type: expected Utf8/LargeUtf8, got {}",
            partition_type.data_type()
        );
    }

    let partition_value = schema
        .field_with_name("partition_value")
        .map_err(|_| anyhow::anyhow!("missing required column: partition_value"))?;
    if !is_utf8_like(partition_value.data_type()) {
        anyhow::bail!(
            "invalid partitions.parquet column type for partition_value: expected Utf8/LargeUtf8, got {}",
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

    let end_block = schema
        .field_with_name("end_block")
        .map_err(|_| anyhow::anyhow!("missing required column: end_block"))?;
    if !is_integer_like(end_block.data_type()) {
        anyhow::bail!(
            "invalid partitions.parquet column type for end_block: expected integer, got {}",
            end_block.data_type()
        );
    }
    if let Ok(partition_start_ts) = schema.field_with_name("partition_start_ts") {
        if !is_utf8_like(partition_start_ts.data_type()) {
            anyhow::bail!(
                "invalid partitions.parquet column type for partition_start_ts: expected Utf8/LargeUtf8, got {}",
                partition_start_ts.data_type()
            );
        }
    }

    Ok(())
}

fn validate_partitions_metadata(
    _file_meta: &parquet::file::metadata::FileMetaData,
) -> anyhow::Result<()> {
    Ok(())
}

/// Resolve partition bounds and return a response payload suitable for CLI output.
fn resolve_partition_chains(
    request: &PartitionBoundsRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<Vec<Option<String>>> {
    let request = normalize_partition_bounds_request(request.clone())?;
    use arrow::array::{Array, LargeStringArray, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::BTreeSet;

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
        anyhow::bail!(
            "expected Utf8/LargeUtf8 column, found {}",
            column.data_type()
        )
    }

    let mut chains = BTreeSet::new();
    let mut collect = |batch: &arrow::record_batch::RecordBatch| -> anyhow::Result<()> {
        let schema = batch.schema();
        let partition_type_idx = schema
            .index_of("partition_type")
            .map_err(|_| anyhow::anyhow!("missing required column: partition_type"))?;
        let partition_value_idx = schema
            .index_of("partition_value")
            .map_err(|_| anyhow::anyhow!("missing required column: partition_value"))?;
        let chain_idx = schema.index_of("chain").ok();

        for row in 0..batch.num_rows() {
            let partition_type = read_utf8_value(batch.column(partition_type_idx).as_ref(), row)?
                .map(|value| canonical_partition_type_label(&value))
                .transpose()?;
            let partition_value = read_utf8_value(batch.column(partition_value_idx).as_ref(), row)?;

            if partition_type
                .as_deref()
                .map(|v| v.eq_ignore_ascii_case(&request.partition_type))
                != Some(true)
            {
                continue;
            }
            if partition_value.as_deref() != Some(request.partition_value.as_str()) {
                continue;
            }

            let row_chain = if let Some(idx) = chain_idx {
                read_utf8_value(batch.column(idx).as_ref(), row)?
            } else {
                None
            };
            chains.insert(row_chain);
        }
        Ok(())
    };

    if request.index_path.starts_with("s3://") {
        use crate::writer::parse_s3_url;
        use object_store::ObjectStore;

        let aws =
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 partition index"))?;
        let (bucket, key) = parse_s3_url(&request.index_path)?;
        let client = aws.build_s3_client(&bucket)?;
        let obj_path = object_store::path::Path::from(key.as_str());
        let data = block_on_async(async { client.get(&obj_path).await?.bytes().await })
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", request.index_path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            let batch = batch?;
            validate_partitions_schema(batch.schema().as_ref())?;
            collect(&batch)?;
        }
    } else {
        let file = std::fs::File::open(&request.index_path)
            .map_err(|e| anyhow::anyhow!("opening {}: {e}", request.index_path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            let batch = batch?;
            validate_partitions_schema(batch.schema().as_ref())?;
            collect(&batch)?;
        }
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
    use arrow::array::{
        Array, Int32Array, Int64Array, LargeStringArray, StringArray, UInt32Array, UInt64Array,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    if request.limit == 0 {
        anyhow::bail!("--limit must be greater than 0");
    }

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
        Ok(None)
    }

    fn read_u64_value(column: &dyn Array, row: usize) -> anyhow::Result<Option<u64>> {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
            return Ok(Some(arr.value(row)));
        }
        if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
            return Ok(Some(arr.value(row) as u64));
        }
        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
            let value = arr.value(row);
            if value < 0 {
                anyhow::bail!("negative block value: {value}");
            }
            return Ok(Some(value as u64));
        }
        if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
            let value = arr.value(row);
            if value < 0 {
                anyhow::bail!("negative block value: {value}");
            }
            return Ok(Some(value as u64));
        }
        anyhow::bail!(
            "expected integer block column, found {}",
            column.data_type()
        )
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct HeapEntry {
        sort_key: (String, String, Option<String>, u64, u64),
        row: PartitionListRow,
    }

    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            self.sort_key.cmp(&other.sort_key)
        }
    }

    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    fn collect_partition_rows<FStr, FNum>(
        batch: &arrow::record_batch::RecordBatch,
        request: &PartitionListRequest,
        heap: &mut BinaryHeap<HeapEntry>,
        total_matches: &mut usize,
        read_utf8_value: &FStr,
        read_u64_value: &FNum,
    ) -> anyhow::Result<()>
    where
        FStr: Fn(&dyn Array, usize) -> anyhow::Result<Option<String>>,
        FNum: Fn(&dyn Array, usize) -> anyhow::Result<Option<u64>>,
    {
        let schema = batch.schema();
        let partition_type_idx = schema
            .index_of("partition_type")
            .map_err(|_| anyhow::anyhow!("missing required column: partition_type"))?;
        let partition_value_idx = schema
            .index_of("partition_value")
            .map_err(|_| anyhow::anyhow!("missing required column: partition_value"))?;
        let start_block_idx = schema
            .index_of("start_block")
            .map_err(|_| anyhow::anyhow!("missing required column: start_block"))?;
        let end_block_idx = schema
            .index_of("end_block")
            .map_err(|_| anyhow::anyhow!("missing required column: end_block"))?;
        let chain_idx = schema.index_of("chain").ok();
        let partition_start_ts_idx = schema.index_of("partition_start_ts").ok();

        for row in 0..batch.num_rows() {
            let partition_type = canonical_partition_type_label(
                read_utf8_value(batch.column(partition_type_idx).as_ref(), row)?
                    .ok_or_else(|| anyhow::anyhow!("null partition_type at row {}", row))?
                    .as_str(),
            )?;
            let partition_value = read_utf8_value(batch.column(partition_value_idx).as_ref(), row)?
                .ok_or_else(|| anyhow::anyhow!("null partition_value at row {}", row))?;

            if let Some(filter) = request.partition_type.as_deref() {
                if !partition_type.eq_ignore_ascii_case(filter) {
                    continue;
                }
            }

            let chain = if let Some(idx) = chain_idx {
                read_utf8_value(batch.column(idx).as_ref(), row)?
            } else {
                None
            };
            if let Some(chain_filter) = request.chain.as_deref() {
                if chain.as_deref() != Some(chain_filter) {
                    continue;
                }
            }

            let partition_start_ts = partition_start_ts_idx
                .and_then(|idx| {
                    read_utf8_value(batch.column(idx).as_ref(), row)
                        .ok()
                        .flatten()
                })
                .unwrap_or_else(|| partition_value.clone());

            if let Some(from) = request.from.as_deref() {
                if partition_start_ts.as_str() < from {
                    continue;
                }
            }
            if let Some(to) = request.to.as_deref() {
                if partition_start_ts.as_str() > to {
                    continue;
                }
            }

            let start_block = read_u64_value(batch.column(start_block_idx).as_ref(), row)?
                .ok_or_else(|| anyhow::anyhow!("null start_block at row {}", row))?;
            let end_block = read_u64_value(batch.column(end_block_idx).as_ref(), row)?
                .ok_or_else(|| anyhow::anyhow!("null end_block at row {}", row))?;

            *total_matches += 1;
            let list_row = PartitionListRow {
                partition_type,
                partition_value,
                partition_start_ts,
                start_block,
                end_block,
                chain,
            };
            let entry = HeapEntry {
                sort_key: (
                    list_row.partition_start_ts.clone(),
                    list_row.partition_type.clone(),
                    list_row.chain.clone(),
                    list_row.start_block,
                    list_row.end_block,
                ),
                row: list_row,
            };
            heap.push(entry);
            if heap.len() > request.limit {
                heap.pop();
            }
        }

        Ok(())
    }

    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
    let mut total_matches = 0usize;

    if request.index_path.starts_with("s3://") {
        use crate::writer::parse_s3_url;
        use object_store::ObjectStore;

        let aws =
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 partition index"))?;
        let (bucket, key) = parse_s3_url(&request.index_path)?;
        let client = aws.build_s3_client(&bucket)?;
        let obj_path = object_store::path::Path::from(key.as_str());
        let data = block_on_async(async { client.get(&obj_path).await?.bytes().await })
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", request.index_path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            let batch = batch?;
            validate_partitions_schema(batch.schema().as_ref())?;
            collect_partition_rows(
                &batch,
                &request,
                &mut heap,
                &mut total_matches,
                &read_utf8_value,
                &read_u64_value,
            )?;
        }
    } else {
        let file = std::fs::File::open(&request.index_path)
            .map_err(|e| anyhow::anyhow!("opening {}: {e}", request.index_path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            let batch = batch?;
            validate_partitions_schema(batch.schema().as_ref())?;
            collect_partition_rows(
                &batch,
                &request,
                &mut heap,
                &mut total_matches,
                &read_utf8_value,
                &read_u64_value,
            )?;
        }
    }

    let mut rows: Vec<PartitionListRow> =
        heap.into_vec().into_iter().map(|entry| entry.row).collect();
    rows.sort_by(|left, right| {
        left.partition_start_ts
            .cmp(&right.partition_start_ts)
            .then_with(|| left.partition_type.cmp(&right.partition_type))
            .then_with(|| left.chain.cmp(&right.chain))
            .then_with(|| left.start_block.cmp(&right.start_block))
            .then_with(|| left.end_block.cmp(&right.end_block))
    });

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
        if row.start_block >= row.end_block {
            issues.push(PartitionValidationIssue {
                kind: PartitionValidationIssueKind::InvalidRange,
                partition_type: row.partition_type.clone(),
                partition_value: row.partition_value.clone(),
                chain: row.chain.clone(),
                message: format!(
                    "invalid range: start_block={} end_block={}",
                    row.start_block, row.end_block
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

            if current.partition_start_ts > next.partition_start_ts {
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

            if current.end_block < next.start_block {
                if !request.allow_gaps {
                    issues.push(PartitionValidationIssue {
                        kind: PartitionValidationIssueKind::Gap,
                        partition_type: next.partition_type.clone(),
                        partition_value: next.partition_value.clone(),
                        chain: next.chain.clone(),
                        message: format!(
                            "gap detected: previous end_block={} next start_block={}",
                            current.end_block, next.start_block
                        ),
                    });
                }
            } else if current.end_block > next.start_block {
                issues.push(PartitionValidationIssue {
                    kind: PartitionValidationIssueKind::Overlap,
                    partition_type: next.partition_type.clone(),
                    partition_value: next.partition_value.clone(),
                    chain: next.chain.clone(),
                    message: format!(
                        "overlap detected: previous end_block={} next start_block={}",
                        current.end_block, next.start_block
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
        "block_range" => Ok(Partition::BlockRange(block_range_size)),
        "date" => Ok(Partition::Date),
        "hour" => Ok(Partition::Hour),
        "minute" => Ok(Partition::Minute),
        "second" => Ok(Partition::Second),
        other => anyhow::bail!("invalid --partition '{other}': expected one of: none, block_range, date, hour, minute, second"),
    }
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
    let api_key = std::env::var(&args.api_key_envvar)
        .ok()
        .filter(|v| !v.is_empty());
    let jwt_token = std::env::var(&args.api_token_envvar)
        .ok()
        .filter(|v| !v.is_empty());

    // When S3_BUCKET is set and output isn't already an s3:// URL,
    // build the S3 path automatically: s3://<bucket>/<output>
    // If output is still the default ("."), use the bucket root.
    let output = if let Some(ref bucket) = args.s3_bucket {
        let path = args.output.to_string_lossy();
        if path.starts_with("s3://") {
            args.output.clone()
        } else if path == "." {
            PathBuf::from(format!("s3://{bucket}"))
        } else {
            PathBuf::from(format!("s3://{bucket}/{path}"))
        }
    } else {
        args.output.clone()
    };

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
        cursor_path: Some(args.cursor.to_string_lossy().to_string()),
        output,
        partition: parse_partition(&args.partition, args.block_range_size)?,
        flush_rows: args.flush_rows,
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
        stream_idle_timeout_secs: args.stream_idle_timeout_secs,
        reconnect_stall_timeout_secs: args.reconnect_stall_timeout_secs,
    })
}

/// Initialize tracing subscriber with the given log level.
pub fn init_tracing(log_level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_new(log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
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
pub enum PartitionSelectionRequest {
    Single(PartitionBoundsRequest),
    Window(PartitionWindowRequest),
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

pub fn cursor_template_context_from_selection(
    selection: Option<&PartitionSelectionRequest>,
) -> CursorTemplateContext {
    match selection {
        Some(PartitionSelectionRequest::Single(request)) => CursorTemplateContext {
            chain: request.chain.clone(),
            partition_type: Some(request.partition_type.clone()),
            partition_value: Some(request.partition_value.clone()),
            partition_from: None,
            partition_to: None,
        },
        Some(PartitionSelectionRequest::Window(request)) => CursorTemplateContext {
            chain: request.chain.clone(),
            partition_type: Some(request.partition_type.clone()),
            partition_value: None,
            partition_from: Some(request.partition_from.clone()),
            partition_to: Some(request.partition_to.clone()),
        },
        None => CursorTemplateContext {
            chain: None,
            partition_type: None,
            partition_value: None,
            partition_from: None,
            partition_to: None,
        },
    }
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

/// Parse and validate partition-bound lookup arguments from [`CommonArgs`].
///
/// Returns `Ok(None)` when no lookup args are provided.
/// Returns an error when lookup args are partially specified.
pub fn parse_partition_bounds_request(
    args: &CommonArgs,
) -> anyhow::Result<Option<PartitionBoundsRequest>> {
    let index_path = normalize_opt_string(&args.partitions_index);
    let partition_type = normalize_opt_string(&args.partition_type)
        .map(|value| canonical_partition_type_label(&value))
        .transpose()?;
    let partition_value = normalize_opt_string(&args.partition_value);
    let chain = normalize_opt_string(&args.partition_chain);

    let any_set = index_path.is_some()
        || partition_type.is_some()
        || partition_value.is_some()
        || chain.is_some();

    if !any_set {
        return Ok(None);
    }

    let mut missing = Vec::new();
    if index_path.is_none() {
        missing.push("--partitions-index");
    }
    if partition_type.is_none() {
        missing.push("--partition-type");
    }
    if partition_value.is_none() {
        missing.push("--partition-value");
    }

    if !missing.is_empty() {
        anyhow::bail!("partition lookup requires: {}", missing.join(", "));
    }

    Ok(Some(PartitionBoundsRequest {
        index_path: index_path.expect("checked above"),
        partition_type: partition_type.expect("checked above"),
        partition_value: partition_value.expect("checked above"),
        chain,
    }))
}

/// Parse and validate partition-selection arguments from [`CommonArgs`].
///
/// Supports either:
/// - single partition mode (`--partition-value`)
/// - window mode (`--partition-from` + `--partition-to`)
pub fn parse_partition_selection_request(
    args: &CommonArgs,
) -> anyhow::Result<Option<PartitionSelectionRequest>> {
    let index_path = normalize_opt_string(&args.partitions_index);
    let partition_type = normalize_opt_string(&args.partition_type)
        .map(|value| canonical_partition_type_label(&value))
        .transpose()?;
    let partition_value = normalize_opt_string(&args.partition_value);
    let partition_from = normalize_opt_string(&args.partition_from);
    let partition_to = normalize_opt_string(&args.partition_to);
    let chain = normalize_opt_string(&args.partition_chain);

    let any_set = index_path.is_some()
        || partition_type.is_some()
        || partition_value.is_some()
        || partition_from.is_some()
        || partition_to.is_some()
        || chain.is_some();
    if !any_set {
        return Ok(None);
    }

    let has_single = partition_value.is_some();
    let has_window = partition_from.is_some() || partition_to.is_some();

    if has_single && has_window {
        anyhow::bail!(
            "partition selection mode is ambiguous: use either --partition-value or --partition-from/--partition-to"
        );
    }

    if !has_single && !has_window {
        anyhow::bail!(
            "partition selection requires either --partition-value or --partition-from/--partition-to"
        );
    }

    let mut common_missing = Vec::new();
    if index_path.is_none() {
        common_missing.push("--partitions-index");
    }
    if partition_type.is_none() {
        common_missing.push("--partition-type");
    }
    if !common_missing.is_empty() {
        anyhow::bail!("partition lookup requires: {}", common_missing.join(", "));
    }

    if has_single {
        return Ok(Some(PartitionSelectionRequest::Single(
            PartitionBoundsRequest {
                index_path: index_path.expect("checked above"),
                partition_type: partition_type.expect("checked above"),
                partition_value: partition_value.expect("checked above"),
                chain,
            },
        )));
    }

    let mut window_missing = Vec::new();
    if partition_from.is_none() {
        window_missing.push("--partition-from");
    }
    if partition_to.is_none() {
        window_missing.push("--partition-to");
    }
    if !window_missing.is_empty() {
        anyhow::bail!(
            "partition window lookup requires: {}",
            window_missing.join(", ")
        );
    }

    let partition_from = partition_from.expect("checked above");
    let partition_to = partition_to.expect("checked above");
    if partition_from >= partition_to {
        anyhow::bail!(
            "invalid partition window: --partition-from must be less than --partition-to"
        );
    }

    Ok(Some(PartitionSelectionRequest::Window(
        PartitionWindowRequest {
            index_path: index_path.expect("checked above"),
            partition_type: partition_type.expect("checked above"),
            partition_from,
            partition_to,
            chain,
        },
    )))
}

/// Resolve `[start_block, stop_block)` from a `partitions.parquet` index file.
///
/// Expected columns:
/// - `partition_type` (utf8)
/// - `partition_value` (utf8)
/// - `start_block` (u64 or integer)
/// - `end_block` (u64 or integer)
/// - optional `chain` (utf8)
pub fn resolve_partition_bounds_from_index(
    request: &PartitionBoundsRequest,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionBounds> {
    let request = normalize_partition_bounds_request(request.clone())?;
    use arrow::array::{
        Array, Int32Array, Int64Array, LargeStringArray, StringArray, UInt32Array, UInt64Array,
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
        anyhow::bail!(
            "expected Utf8/LargeUtf8 column, found {}",
            column.data_type()
        )
    }

    fn read_u64_value(column: &dyn Array, row: usize) -> anyhow::Result<Option<u64>> {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
            return Ok(Some(arr.value(row)));
        }
        if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
            return Ok(Some(arr.value(row) as u64));
        }
        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
            let value = arr.value(row);
            if value < 0 {
                anyhow::bail!("negative block value: {value}");
            }
            return Ok(Some(value as u64));
        }
        if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
            let value = arr.value(row);
            if value < 0 {
                anyhow::bail!("negative block value: {value}");
            }
            return Ok(Some(value as u64));
        }
        anyhow::bail!(
            "expected integer block column, found {}",
            column.data_type()
        )
    }

    let mut matches = Vec::new();

    if request.index_path.starts_with("s3://") {
        use crate::writer::parse_s3_url;
        use object_store::ObjectStore;

        let aws =
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 partition index"))?;
        let (bucket, key) = parse_s3_url(&request.index_path)?;
        let client = aws.build_s3_client(&bucket)?;
        let obj_path = object_store::path::Path::from(key.as_str());
        let data = block_on_async(async { client.get(&obj_path).await?.bytes().await })
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", request.index_path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            let batch = batch?;
            validate_partitions_schema(batch.schema().as_ref())?;
            collect_partition_matches(
                &batch,
                &request,
                &mut matches,
                &read_utf8_value,
                &read_u64_value,
            )?;
        }
    } else {
        let file = std::fs::File::open(&request.index_path)
            .map_err(|e| anyhow::anyhow!("opening {}: {e}", request.index_path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            let batch = batch?;
            validate_partitions_schema(batch.schema().as_ref())?;
            collect_partition_matches(
                &batch,
                &request,
                &mut matches,
                &read_utf8_value,
                &read_u64_value,
            )?;
        }
    }

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
            "invalid partition bounds in {}: start_block={} end_block={}",
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
    use arrow::array::{
        Array, Int32Array, Int64Array, LargeStringArray, StringArray, UInt32Array, UInt64Array,
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
        anyhow::bail!(
            "expected Utf8/LargeUtf8 column, found {}",
            column.data_type()
        )
    }

    fn read_u64_value(column: &dyn Array, row: usize) -> anyhow::Result<Option<u64>> {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
            return Ok(Some(arr.value(row)));
        }
        if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
            return Ok(Some(arr.value(row) as u64));
        }
        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
            let value = arr.value(row);
            if value < 0 {
                anyhow::bail!("negative block value: {value}");
            }
            return Ok(Some(value as u64));
        }
        if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
            let value = arr.value(row);
            if value < 0 {
                anyhow::bail!("negative block value: {value}");
            }
            return Ok(Some(value as u64));
        }
        anyhow::bail!(
            "expected integer block column, found {}",
            column.data_type()
        )
    }

    let mut matches: Vec<(String, u64, u64)> = Vec::new();

    let mut collect = |batch: &arrow::record_batch::RecordBatch| -> anyhow::Result<()> {
        let schema = batch.schema();
        let partition_type_idx = schema
            .index_of("partition_type")
            .map_err(|_| anyhow::anyhow!("missing required column: partition_type"))?;
        let partition_value_idx = schema
            .index_of("partition_value")
            .map_err(|_| anyhow::anyhow!("missing required column: partition_value"))?;
        let start_block_idx = schema
            .index_of("start_block")
            .map_err(|_| anyhow::anyhow!("missing required column: start_block"))?;
        let end_block_idx = schema
            .index_of("end_block")
            .map_err(|_| anyhow::anyhow!("missing required column: end_block"))?;
        let chain_idx = schema.index_of("chain").ok();

        for row in 0..batch.num_rows() {
            let partition_type = read_utf8_value(batch.column(partition_type_idx).as_ref(), row)?;
            let partition_value = read_utf8_value(batch.column(partition_value_idx).as_ref(), row)?;

            if partition_type
                .as_deref()
                .map(|v| v.eq_ignore_ascii_case(&request.partition_type))
                != Some(true)
            {
                continue;
            }
            let partition_value = match partition_value {
                Some(value) => value,
                None => continue,
            };
            if partition_value.as_str() < request.partition_from.as_str()
                || partition_value.as_str() >= request.partition_to.as_str()
            {
                continue;
            }

            if let Some(ref chain_filter) = request.chain {
                let row_chain = if let Some(idx) = chain_idx {
                    read_utf8_value(batch.column(idx).as_ref(), row)?
                } else {
                    None
                };
                if row_chain.as_deref() != Some(chain_filter.as_str()) {
                    continue;
                }
            }

            let start_block = read_u64_value(batch.column(start_block_idx).as_ref(), row)?
                .ok_or_else(|| anyhow::anyhow!("null start_block at row {}", row))?;
            let end_block = read_u64_value(batch.column(end_block_idx).as_ref(), row)?
                .ok_or_else(|| anyhow::anyhow!("null end_block at row {}", row))?;

            matches.push((partition_value, start_block, end_block));
        }

        Ok(())
    };

    if request.index_path.starts_with("s3://") {
        use crate::writer::parse_s3_url;
        use object_store::ObjectStore;

        let aws =
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 partition index"))?;
        let (bucket, key) = parse_s3_url(&request.index_path)?;
        let client = aws.build_s3_client(&bucket)?;
        let obj_path = object_store::path::Path::from(key.as_str());
        let data = block_on_async(async { client.get(&obj_path).await?.bytes().await })
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", request.index_path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            let batch = batch?;
            validate_partitions_schema(batch.schema().as_ref())?;
            collect(&batch)?;
        }
    } else {
        let file = std::fs::File::open(&request.index_path)
            .map_err(|e| anyhow::anyhow!("opening {}: {e}", request.index_path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        validate_partitions_metadata(builder.metadata().file_metadata())?;
        let reader = builder.build()?;
        for batch in reader {
            let batch = batch?;
            validate_partitions_schema(batch.schema().as_ref())?;
            collect(&batch)?;
        }
    }

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

    matches.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    for (idx, window) in matches.windows(2).enumerate() {
        let current = &window[0];
        let next = &window[1];
        if current.0 == next.0 {
            anyhow::bail!(
                "partition window is ambiguous in {}: multiple rows for partition_value={} (rows {} and {})",
                request.index_path,
                current.0,
                idx,
                idx + 1
            );
        }
        if current.2 != next.1 {
            anyhow::bail!(
                "partition window has non-contiguous bounds in {} between {} and {}: end_block={} next_start_block={}",
                request.index_path,
                current.0,
                next.0,
                current.2,
                next.1
            );
        }
    }

    let first = matches.first().expect("non-empty checked above");
    let last = matches.last().expect("non-empty checked above");
    if last.2 <= first.1 {
        anyhow::bail!(
            "invalid partition window bounds in {}: start_block={} end_block={}",
            request.index_path,
            first.1,
            last.2
        );
    }

    Ok(PartitionWindowBounds {
        start_block: first.1,
        stop_block: last.2,
        partitions_count: matches.len(),
        partition_from: request.partition_from.clone(),
        partition_to: request.partition_to.clone(),
    })
}

fn collect_partition_matches<FStr, FNum>(
    batch: &arrow::record_batch::RecordBatch,
    request: &PartitionBoundsRequest,
    matches: &mut Vec<(u64, u64)>,
    read_utf8_value: &FStr,
    read_u64_value: &FNum,
) -> anyhow::Result<()>
where
    FStr: Fn(&dyn arrow::array::Array, usize) -> anyhow::Result<Option<String>>,
    FNum: Fn(&dyn arrow::array::Array, usize) -> anyhow::Result<Option<u64>>,
{
    let schema = batch.schema();
    let partition_type_idx = schema
        .index_of("partition_type")
        .map_err(|_| anyhow::anyhow!("missing required column: partition_type"))?;
    let partition_value_idx = schema
        .index_of("partition_value")
        .map_err(|_| anyhow::anyhow!("missing required column: partition_value"))?;
    let start_block_idx = schema
        .index_of("start_block")
        .map_err(|_| anyhow::anyhow!("missing required column: start_block"))?;
    let end_block_idx = schema
        .index_of("end_block")
        .map_err(|_| anyhow::anyhow!("missing required column: end_block"))?;
    let chain_idx = schema.index_of("chain").ok();

    for row in 0..batch.num_rows() {
        let partition_type = read_utf8_value(batch.column(partition_type_idx).as_ref(), row)?;
        let partition_value = read_utf8_value(batch.column(partition_value_idx).as_ref(), row)?;

        if partition_type
            .as_deref()
            .map(|v| v.eq_ignore_ascii_case(&request.partition_type))
            != Some(true)
        {
            continue;
        }
        if partition_value.as_deref() != Some(request.partition_value.as_str()) {
            continue;
        }

        if let Some(ref chain_filter) = request.chain {
            let row_chain = if let Some(idx) = chain_idx {
                read_utf8_value(batch.column(idx).as_ref(), row)?
            } else {
                None
            };
            if row_chain.as_deref() != Some(chain_filter.as_str()) {
                continue;
            }
        }

        let start_block = read_u64_value(batch.column(start_block_idx).as_ref(), row)?
            .ok_or_else(|| anyhow::anyhow!("null start_block at row {}", row))?;
        let end_block = read_u64_value(batch.column(end_block_idx).as_ref(), row)?
            .ok_or_else(|| anyhow::anyhow!("null end_block at row {}", row))?;

        matches.push((start_block, end_block));
    }

    Ok(())
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
/// If `path` is a file, inspects that single file.
/// If `path` is a directory, recursively finds all `.parquet` files.
pub fn scan_parquet(
    path: &str,
    rows: usize,
    schema_only: bool,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<()> {
    if path.starts_with("s3://") {
        scan_parquet_s3(
            path,
            rows,
            schema_only,
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?,
        )
    } else {
        scan_parquet_local(&PathBuf::from(path), rows, schema_only)
    }
}

/// Scan parquet files from the local filesystem.
fn scan_parquet_local(path: &PathBuf, rows: usize, schema_only: bool) -> anyhow::Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs;

    let mut files: Vec<PathBuf> = Vec::new();
    if path.is_file() {
        files.push(path.clone());
    } else if path.is_dir() {
        collect_parquet_files(path, &mut files)?;
        files.sort();
    } else {
        anyhow::bail!("path does not exist: {}", path.display());
    }

    if files.is_empty() {
        println!("No .parquet files found in {}", path.display());
        return Ok(());
    }

    for file_path in &files {
        let file = fs::File::open(file_path)?;
        let file_size = file.metadata()?.len();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = builder.metadata();

        let total_rows: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();
        let num_row_groups = metadata.num_row_groups();
        let num_columns = metadata.file_metadata().schema().get_fields().len();
        let schema = builder.schema();

        // Relative path for cleaner display.
        let display_path = file_path.strip_prefix(path).unwrap_or(file_path);

        println!("\n{}", "═".repeat(72));
        println!("  {}", display_path.display());
        println!("{}", "─".repeat(72));
        println!(
            "  rows: {}  row_groups: {}  columns: {}  size: {}",
            total_rows,
            num_row_groups,
            num_columns,
            format_bytes(file_size),
        );
        println!("{}", "─".repeat(72));

        // Print schema fields.
        for field in schema.fields() {
            let nullable = if field.is_nullable() {
                "nullable"
            } else {
                "not null"
            };
            println!(
                "  {:30} {:20} {}",
                field.name(),
                field.data_type(),
                nullable
            );
        }

        // Print sample rows (vertical format like ClickHouse's \G).
        if !schema_only && rows > 0 {
            let file = fs::File::open(file_path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;

            print_sample_rows(&schema, reader, rows, total_rows);
        }
    }

    // Summary.
    if files.len() > 1 {
        println!("\n{}", "═".repeat(72));
        println!("  {} parquet files scanned", files.len());
    }

    Ok(())
}

/// Scan parquet files from an S3 bucket.
fn scan_parquet_s3(
    path: &str,
    rows: usize,
    schema_only: bool,
    aws: &AwsConfig,
) -> anyhow::Result<()> {
    use crate::writer::parse_s3_url;
    use object_store::ObjectStore;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let (bucket, prefix) = parse_s3_url(path)?;
    let client = aws.build_s3_client(&bucket)?;

    // List all .parquet objects under the prefix.
    let list_prefix = if prefix.is_empty() {
        None
    } else {
        Some(object_store::path::Path::from(prefix.as_str()))
    };

    let objects: Vec<object_store::ObjectMeta> = block_on_async(async {
        use futures::TryStreamExt;
        let stream = client.list(list_prefix.as_ref());
        stream.try_collect().await
    })
    .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let mut parquet_objects: Vec<_> = objects
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .collect();
    parquet_objects.sort_by(|a, b| a.location.cmp(&b.location));

    if parquet_objects.is_empty() {
        println!("No .parquet files found in {path}");
        return Ok(());
    }

    for obj in &parquet_objects {
        // Download the object into memory.
        let data = block_on_async(async { client.get(&obj.location).await?.bytes().await })
            .map_err(|e| anyhow::anyhow!("reading s3://{bucket}/{}: {e}", obj.location))?;

        let file_size = data.len() as u64;
        let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())?;
        let metadata = builder.metadata();

        let total_rows: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();
        let num_row_groups = metadata.num_row_groups();
        let num_columns = metadata.file_metadata().schema().get_fields().len();
        let schema = builder.schema().clone();

        // Strip prefix for cleaner display.
        let display_key = obj
            .location
            .as_ref()
            .strip_prefix(&prefix)
            .map(|s| s.trim_start_matches('/'))
            .unwrap_or(obj.location.as_ref());

        println!("\n{}", "═".repeat(72));
        println!("  {}", display_key);
        println!("{}", "─".repeat(72));
        println!(
            "  rows: {}  row_groups: {}  columns: {}  size: {}",
            total_rows,
            num_row_groups,
            num_columns,
            format_bytes(file_size),
        );
        println!("{}", "─".repeat(72));

        for field in schema.fields() {
            let nullable = if field.is_nullable() {
                "nullable"
            } else {
                "not null"
            };
            println!(
                "  {:30} {:20} {}",
                field.name(),
                field.data_type(),
                nullable
            );
        }

        if !schema_only && rows > 0 {
            let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
            let reader = builder.build()?;
            print_sample_rows(&schema, reader, rows, total_rows);
        }
    }

    if parquet_objects.len() > 1 {
        println!("\n{}", "═".repeat(72));
        println!("  {} parquet files scanned", parquet_objects.len());
    }

    Ok(())
}

/// Print sample rows in vertical format (shared between local and S3 scan).
fn print_sample_rows(
    schema: &arrow::datatypes::SchemaRef,
    reader: impl Iterator<Item = Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>>,
    rows: usize,
    total_rows: i64,
) {
    let max_name_len = schema
        .fields()
        .iter()
        .map(|f| f.name().len())
        .max()
        .unwrap_or(0);
    let mut row_number = 0usize;

    'outer: for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                eprintln!("  error reading batch: {e}");
                break;
            }
        };
        for row_idx in 0..batch.num_rows() {
            if row_number >= rows {
                break 'outer;
            }
            row_number += 1;

            println!("\nRow {}:", row_number);
            println!("{}", "──────");
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let value = format_array_value(col.as_ref(), row_idx);
                println!("  {:width$}  {}", field.name(), value, width = max_name_len);
            }
        }
    }

    if row_number == 0 {
        println!("\n  (empty)");
    } else if (rows as i64) < total_rows {
        println!("\n  ... showing {rows} of {total_rows} rows");
    }
}

/// Format a single cell value from an Arrow array for vertical display.
fn format_array_value(array: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;

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
pub fn inspect_parquet(
    path: &str,
    schema_only: bool,
    json: bool,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<()> {
    if path.starts_with("s3://") {
        inspect_parquet_s3(
            path,
            schema_only,
            json,
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?,
        )
    } else {
        inspect_parquet_local(path, schema_only, json)
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
        // Only show partitions with issues; valid ones are counted in the summary.
        if !self.partitions.is_empty() {
            let valid_count = self.partitions.iter().filter(|p| p.is_valid()).count();
            let invalid_count = self.partitions.len() - valid_count;

            println!(
                "  Partitions:        {} total, {} valid, {} with issues\n",
                self.partitions.len(),
                valid_count,
                invalid_count
            );

            for pr in &self.partitions {
                if pr.is_valid() {
                    continue; // skip valid partitions to keep output compact
                }

                let range = match (pr.min_block, pr.max_block) {
                    (Some(min), Some(max)) => format!("{} — {}", min, max),
                    _ => "N/A".to_string(),
                };
                println!("  ✗ {}", pr.partition);
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
                        tr.block_num, tr.timestamp, tr.prev_block_num, tr.prev_timestamp
                    );
                }
            }
            if invalid_count > 0 {
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
                    tr.block_num, tr.timestamp, tr.prev_block_num, tr.prev_timestamp
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
    }
}

/// A block tuple: (block_num, block_id, parent_id, timestamp).
type BlockTuple = (u64, String, String, i64);

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

/// Extract block tuples from a parquet record batch reader.
fn extract_block_tuples(
    reader: impl Iterator<Item = Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>>,
    block_num_idx: usize,
    block_id_idx: usize,
    parent_id_idx: usize,
    timestamp_idx: Option<usize>,
) -> anyhow::Result<Vec<BlockTuple>> {
    use arrow::array::{Int64Array, UInt64Array};

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
            .map(|idx| batch.column(idx).as_any().downcast_ref::<Int64Array>())
            .flatten();

        for i in 0..batch.num_rows() {
            let ts = timestamps.map(|a| a.value(i)).unwrap_or(0);
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
    if path.starts_with("s3://") {
        validate_parquet_s3(
            path,
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

    for i in 1..tuples.len() {
        let (prev_num, ref prev_block_id, _, prev_ts) = tuples[i - 1];
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
        if curr_ts < prev_ts && curr_num > prev_num {
            timestamp_reversals.push(TimestampReversal {
                block_num: curr_num,
                timestamp: curr_ts,
                prev_block_num: prev_num,
                prev_timestamp: prev_ts,
            });
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
/// (e.g. `date=2026-01-01`, `block_range=0-10000`). Returns the partition directory
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
        assert_eq!(cli.common.flush_bytes, 134217728);
        assert_eq!(cli.common.compression, "zstd");
        assert_eq!(cli.common.log_level, "info");
        assert!(!cli.common.dry_run);
        assert!(cli.common.final_blocks_only);
        assert_eq!(cli.common.api_key_envvar, "SUBSTREAMS_API_KEY");
        assert_eq!(cli.common.api_token_envvar, "SUBSTREAMS_API_TOKEN");
        assert!(cli.common.start_block.is_none());
        assert!(cli.common.stop_block.is_none());
        assert!(cli.common.partitions_index.is_none());
        assert!(cli.common.partition_type.is_none());
        assert!(cli.common.partition_value.is_none());
        assert!(cli.common.partition_from.is_none());
        assert!(cli.common.partition_to.is_none());
        assert!(cli.common.partition_chain.is_none());
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
            "--dry-run",
            "--partitions-index",
            "./output/eth-mainnet/partitions.parquet",
            "--partition-type",
            "hour",
            "--partition-value",
            "2015-07-30 15:00:00",
            "--partition-chain",
            "eth-mainnet",
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
            cli.common.partitions_index.as_deref(),
            Some("./output/eth-mainnet/partitions.parquet")
        );
        assert_eq!(cli.common.partition_type.as_deref(), Some("hour"));
        assert_eq!(
            cli.common.partition_value.as_deref(),
            Some("2015-07-30 15:00:00")
        );
        assert_eq!(cli.common.partition_chain.as_deref(), Some("eth-mainnet"));
        assert_eq!(
            cli.common.cursor,
            PathBuf::from("cursor-mainnet-date.parquet")
        );
        assert_eq!(cli.common.output, PathBuf::from("/tmp/out"));
        assert_eq!(cli.common.partition, "date");
        assert_eq!(cli.common.block_range_size, 5000);
        assert_eq!(cli.common.flush_rows, Some(10000));
        assert_eq!(cli.common.flush_bytes, 1000000);
        assert_eq!(cli.common.flush_interval_secs, Some(60));
        assert_eq!(cli.common.stream_idle_timeout_secs, Some(45));
        assert_eq!(cli.common.reconnect_stall_timeout_secs, Some(120));
        assert_eq!(cli.common.compression, "snappy");
        assert_eq!(cli.common.log_level, "debug");
        assert!(cli.common.dry_run);
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
            Partition::BlockRange(5000)
        );
        assert_eq!(
            parse_partition("BLOCK_RANGE", 20000).unwrap(),
            Partition::BlockRange(20000)
        );
        assert!(parse_partition("unknown", 10000).is_err());
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
        assert_eq!(config.compression, Compression::Gzip);
        assert_eq!(config.partition, Partition::Date);
        assert!(config.flush_rows.is_none());
        assert!(config.final_blocks_only);
        assert_eq!(config.stream_idle_timeout_secs, Some(120));
        assert_eq!(config.reconnect_stall_timeout_secs, Some(900));
        // cursor defaults to cursor.parquet
        assert_eq!(config.cursor_path, Some("cursor.parquet".to_string()));
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
            "--chain",
            "eth-mainnet",
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
                chain,
                start_block,
                stop_block,
                live,
                partition,
                output,
                s3_bucket,
                resume,
                json,
                ..
            }) => {
                assert_eq!(
                    endpoint.as_deref(),
                    Some("https://eth.firehose.pinax.network:443")
                );
                assert_eq!(chain.as_deref(), Some("eth-mainnet"));
                assert_eq!(start_block, None);
                assert_eq!(stop_block, Some(200));
                assert!(!live);
                assert_eq!(partition, "date");
                assert_eq!(output.as_deref(), Some("./output"));
                assert!(s3_bucket.is_none());
                assert!(resume);
                assert!(json);
            }
            _ => panic!("expected partitions build subcommand"),
        }
    }

    #[test]
    fn test_partitions_build_subcommand_deprecated_alias_parse() {
        let cli = parse(&[
            "test-cli",
            "partitions",
            "build",
            "--endpoint",
            "https://eth.firehose.pinax.network:443",
            "--stop-block",
            "200",
            "--partition-types",
            "date",
            "--output",
            "./output",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Partitions(PartitionsCommands::Build { partition, .. }) => {
                assert_eq!(partition, "date");
            }
            _ => panic!("expected partitions build subcommand"),
        }
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
                output,
                ..
            }) => {
                assert_eq!(network.as_deref(), Some("mainnet"));
                assert_eq!(stop_block, None);
                assert!(live);
                assert_eq!(poll_interval_secs, 15);
                assert_eq!(partition, "date");
                assert_eq!(output.as_deref(), Some("./output"));
            }
            _ => panic!("expected partitions build subcommand"),
        }
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
                ..
            } => {
                assert_eq!(path, "./output/blocks/");
                assert_eq!(limit, 50);
                assert!(schema_only);
            }
            _ => panic!("expected scan subcommand"),
        }
    }

    #[test]
    fn test_scan_subcommand_deprecated_rows_alias_parse() {
        let cli = parse(&["test-cli", "scan", "./output/blocks/", "--rows", "12"]);
        match cli.command.expect("command should exist") {
            Commands::Scan { limit, .. } => assert_eq!(limit, 12),
            _ => panic!("expected scan subcommand"),
        }
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
    fn test_rollup_subcommand_deprecated_target_partition_alias_parse() {
        let cli = parse(&[
            "test-cli",
            "rollup",
            "./output/blocks/",
            "--target-partition",
            "hour",
        ]);
        match cli.command.expect("command should exist") {
            Commands::Rollup { partition, .. } => assert_eq!(partition, "hour"),
            _ => panic!("expected rollup subcommand"),
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
            std::env::remove_var("AWS_ACCESS_KEY_ID");
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
            "--s3-bucket",
            "my-bucket",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.output, PathBuf::from("s3://my-bucket"));
    }

    #[test]
    fn test_parse_partition_bounds_request_none() {
        let cli = parse(&["test-cli", "--endpoint", "https://example.com:443"]);
        assert!(parse_partition_bounds_request(&cli.common)
            .expect("partition request parsing should succeed")
            .is_none());
    }

    #[test]
    fn test_parse_partition_bounds_request_requires_all_flags() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--partitions-index",
            "./partitions.parquet",
        ]);
        let err = parse_partition_bounds_request(&cli.common)
            .expect_err("should fail with partial lookup args");
        assert!(err.to_string().contains("--partition-type"));
        assert!(err.to_string().contains("--partition-value"));
    }

    #[test]
    fn test_parse_partition_bounds_request_success() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--partitions-index",
            "./partitions.parquet",
            "--partition-type",
            "hour",
            "--partition-value",
            "2015-07-30 15:00:00",
            "--partition-chain",
            "eth-mainnet",
        ]);
        let request = parse_partition_bounds_request(&cli.common)
            .expect("partition request parsing should succeed")
            .expect("request should be present");
        assert_eq!(request.index_path, "./partitions.parquet");
        assert_eq!(request.partition_type, "hour");
        assert_eq!(request.partition_value, "2015-07-30 15:00:00");
        assert_eq!(request.chain.as_deref(), Some("eth-mainnet"));
    }

    #[test]
    fn test_parse_partition_selection_request_window_success() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--partitions-index",
            "./partitions.parquet",
            "--partition-type",
            "hour",
            "--partition-from",
            "2015-07-30 15:00:00",
            "--partition-to",
            "2015-07-30 18:00:00",
            "--partition-chain",
            "eth-mainnet",
        ]);

        let selection = parse_partition_selection_request(&cli.common)
            .expect("selection parse should succeed")
            .expect("selection should exist");
        match selection {
            PartitionSelectionRequest::Window(window) => {
                assert_eq!(window.index_path, "./partitions.parquet");
                assert_eq!(window.partition_type, "hour");
                assert_eq!(window.partition_from, "2015-07-30 15:00:00");
                assert_eq!(window.partition_to, "2015-07-30 18:00:00");
                assert_eq!(window.chain.as_deref(), Some("eth-mainnet"));
            }
            PartitionSelectionRequest::Single(_) => panic!("expected window mode"),
        }
    }

    #[test]
    fn test_parse_partition_selection_request_rejects_mixed_modes() {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--partitions-index",
            "./partitions.parquet",
            "--partition-type",
            "hour",
            "--partition-value",
            "2015-07-30 15:00:00",
            "--partition-from",
            "2015-07-30 15:00:00",
            "--partition-to",
            "2015-07-30 18:00:00",
        ]);
        let err = parse_partition_selection_request(&cli.common)
            .expect_err("mixed single/window selection must fail");
        assert!(err.to_string().contains("ambiguous"));
    }

    #[test]
    fn test_resolve_partition_bounds_from_index_local() {
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
                Arc::new(StringArray::from(vec![
                    Some("eth-mainnet"),
                    Some("eth-mainnet"),
                ])),
                Arc::new(StringArray::from(vec!["hour", "hour"])),
                Arc::new(StringArray::from(vec![
                    "2015-07-30 14:00:00",
                    "2015-07-30 15:00:00",
                ])),
                Arc::new(UInt64Array::from(vec![100_u64, 200_u64])),
                Arc::new(UInt64Array::from(vec![200_u64, 300_u64])),
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
        let bounds = resolve_partition_bounds_from_index(&request, None)
            .expect("partition bounds should resolve");
        assert_eq!(bounds.start_block, 200);
        assert_eq!(bounds.stop_block, 300);
    }

    #[test]
    fn test_resolve_partition_bounds_from_index_ambiguous() {
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
                Arc::new(StringArray::from(vec!["eth-mainnet", "eth-mainnet"])),
                Arc::new(StringArray::from(vec!["hour", "hour"])),
                Arc::new(StringArray::from(vec![
                    "2015-07-30 15:00:00",
                    "2015-07-30 15:00:00",
                ])),
                Arc::new(UInt64Array::from(vec![200_u64, 201_u64])),
                Arc::new(UInt64Array::from(vec![300_u64, 301_u64])),
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
                Arc::new(StringArray::from(vec![
                    Some("eth-mainnet"),
                    Some("polygon-mainnet"),
                ])),
                Arc::new(StringArray::from(vec!["day", "day"])),
                Arc::new(StringArray::from(vec![
                    "2015-07-30 00:00:00",
                    "2015-07-30 00:00:00",
                ])),
                Arc::new(UInt64Array::from(vec![100_u64, 200_u64])),
                Arc::new(UInt64Array::from(vec![200_u64, 300_u64])),
            ],
        )
        .expect("record batch");

        let file = File::create(&path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let err = resolve_partition_command(
            PartitionBoundsRequest {
                index_path: path.to_string_lossy().to_string(),
                partition_type: "day".to_string(),
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
                Arc::new(StringArray::from(vec![
                    Some("eth-mainnet"),
                    Some("eth-mainnet"),
                    Some("eth-mainnet"),
                ])),
                Arc::new(StringArray::from(vec!["hour", "hour", "hour"])),
                Arc::new(StringArray::from(vec![
                    "2015-07-30 14:00:00",
                    "2015-07-30 15:00:00",
                    "2015-07-30 16:00:00",
                ])),
                Arc::new(UInt64Array::from(vec![100_u64, 200_u64, 300_u64])),
                Arc::new(UInt64Array::from(vec![200_u64, 300_u64, 400_u64])),
            ],
        )
        .expect("record batch");

        let file = File::create(&path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

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
            arrow::datatypes::Field::new(
                "partition_start_ts",
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new("start_block", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("end_block", arrow::datatypes::DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("eth-mainnet"),
                    Some("eth-mainnet"),
                    Some("eth-mainnet"),
                    Some("btc-mainnet"),
                ])),
                Arc::new(StringArray::from(vec!["day", "day", "day", "day"])),
                Arc::new(StringArray::from(vec![
                    "2015-07-30 00:00:00",
                    "2015-07-29 00:00:00",
                    "2015-07-31 00:00:00",
                    "2015-07-29 00:00:00",
                ])),
                Arc::new(StringArray::from(vec![
                    "2015-07-30 00:00:00",
                    "2015-07-29 00:00:00",
                    "2015-07-31 00:00:00",
                    "2015-07-29 00:00:00",
                ])),
                Arc::new(UInt64Array::from(vec![200_u64, 100_u64, 300_u64, 999_u64])),
                Arc::new(UInt64Array::from(vec![300_u64, 200_u64, 400_u64, 1000_u64])),
            ],
        )
        .expect("record batch");

        let file = File::create(&path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let request = PartitionListRequest {
            index_path: path.to_string_lossy().to_string(),
            partition_type: Some("day".to_string()),
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
        use arrow::array::{StringArray, UInt64Array};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::collections::BTreeSet;
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
            arrow::datatypes::Field::new(
                "partition_start_ts",
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new("start_block", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("end_block", arrow::datatypes::DataType::UInt64, false),
        ]));

        let values = vec![
            "2015-07-29 00:00:00",
            "2015-07-30 00:00:00",
            "2015-07-31 00:00:00",
            "2015-08-01 00:00:00",
            "2015-08-02 00:00:00",
        ];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("eth-mainnet"); 5])),
                Arc::new(StringArray::from(vec!["day"; 5])),
                Arc::new(StringArray::from(values.clone())),
                Arc::new(StringArray::from(values)),
                Arc::new(UInt64Array::from(vec![100_u64, 200, 300, 400, 500])),
                Arc::new(UInt64Array::from(vec![200_u64, 300, 400, 500, 600])),
            ],
        )
        .expect("record batch");

        let file = File::create(&path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let base_request = PartitionListRequest {
            index_path: path.to_string_lossy().to_string(),
            partition_type: Some("day".to_string()),
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
    fn test_list_partitions_from_index_ignores_legacy_partitions_metadata() {
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

        let result = list_partitions_from_index(
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
        .expect("legacy partitions metadata should be ignored");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].start_block, 100);
        assert_eq!(result.rows[0].end_block, 200);
    }

    #[test]
    fn test_read_partitions_build_rows_ignores_legacy_partitions_metadata() {
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

        let rows = read_partitions_build_rows(&path.to_string_lossy(), None)
            .expect("legacy partitions metadata should be ignored");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].start_block, 100);
        assert_eq!(rows[0].end_block, 200);
    }

    #[test]
    fn test_validate_partitions_index_detects_gap_and_overlap() {
        let rows = vec![
            PartitionListRow {
                partition_type: "day".to_string(),
                partition_value: "2015-07-29 00:00:00".to_string(),
                partition_start_ts: "2015-07-29 00:00:00".to_string(),
                start_block: 100,
                end_block: 200,
                chain: Some("eth-mainnet".to_string()),
            },
            PartitionListRow {
                partition_type: "day".to_string(),
                partition_value: "2015-07-30 00:00:00".to_string(),
                partition_start_ts: "2015-07-30 00:00:00".to_string(),
                start_block: 250,
                end_block: 300,
                chain: Some("eth-mainnet".to_string()),
            },
            PartitionListRow {
                partition_type: "day".to_string(),
                partition_value: "2015-07-31 00:00:00".to_string(),
                partition_start_ts: "2015-07-31 00:00:00".to_string(),
                start_block: 290,
                end_block: 400,
                chain: Some("eth-mainnet".to_string()),
            },
        ];

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partitions.parquet");

        use arrow::array::{StringArray, UInt64Array};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::fs::File;
        use std::sync::Arc;

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("chain", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("partition_type", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new(
                "partition_value",
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new(
                "partition_start_ts",
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new("start_block", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("end_block", arrow::datatypes::DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.chain.clone()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.partition_type.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.partition_value.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.partition_start_ts.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.start_block).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.end_block).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch");
        let file = File::create(&path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

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
        assert_eq!(resolved, "s3://bucket-name/output");

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
        assert_eq!(rows[0].end_block, 102);
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
        assert_eq!(date_rows[0].end_block, 103);

        let hour_rows: Vec<_> = rows
            .iter()
            .filter(|row| row.partition_type == "hour")
            .collect();
        assert_eq!(hour_rows.len(), 2);
        assert_eq!(hour_rows[0].partition_value, "2023-07-31 14:00:00");
        assert_eq!(hour_rows[0].start_block, 100);
        assert_eq!(hour_rows[0].end_block, 102);
        assert_eq!(hour_rows[1].partition_value, "2023-07-31 15:00:00");
        assert_eq!(hour_rows[1].start_block, 102);
        assert_eq!(hour_rows[1].end_block, 103);
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
        assert_eq!(rows[0].end_block, 501);
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
            end_block: 102,
            start_time: "2023-07-31 14:59:00".to_string(),
            end_time: "2023-07-31 14:59:50".to_string(),
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
        assert_eq!(rows[0].end_block, 103);
        assert_eq!(rows[1].partition_value, "2023-07-31 15:00:00");
        assert_eq!(rows[1].start_block, 103);
        assert_eq!(rows[1].end_block, 104);
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
            end_block: 103,
            start_time: "2023-07-31 14:59:00".to_string(),
            end_time: "2023-07-31 15:00:00".to_string(),
            chain: Some("eth-mainnet".to_string()),
        }];

        write_partitions_index(&path.to_string_lossy(), &rows, None).expect("write rows");
        let read_back =
            read_partitions_build_rows(&path.to_string_lossy(), None).expect("read rows back");
        assert_eq!(read_back, rows);
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
            end_block: 103,
            start_time: "2023-07-31 14:59:00".to_string(),
            end_time: "2023-07-31 15:00:00".to_string(),
            chain: Some("eth-mainnet".to_string()),
        }];
        let mut metadata = ParquetFileMetadata::new();
        metadata.add("firehose-parquet.version", "0.5.4-test");
        metadata.add("firehose-parquet.endpoint", "https://example.com:443");

        write_partitions_index_with_metadata(&path.to_string_lossy(), &rows, None, Some(&metadata))
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
}
