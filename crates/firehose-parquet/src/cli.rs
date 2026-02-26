use crate::config::{Compression, Config, Partition};
use clap::Args;
use clap_complete::{generate, Shell};
use std::io;
use std::path::PathBuf;

// Shared CLI arguments for all firehose-to-parquet binaries.
//
// Embed in a per-chain `#[derive(Parser)]` struct with `#[command(flatten)]`.
#[derive(Args, Debug, Clone)]
pub struct CommonArgs {
    /// Firehose gRPC endpoint URL
    #[arg(long)]
    pub endpoint: Option<String>,

    /// API key for authentication
    #[arg(long, env = "FIREHOSE_API_KEY")]
    pub api_key: Option<String>,

    /// JWT bearer token for authentication
    #[arg(long, env = "SUBSTREAMS_API_TOKEN")]
    pub jwt_token: Option<String>,

    /// Start block number (inclusive)
    #[arg(long)]
    pub start_block: Option<u64>,

    /// Stop block number (inclusive, 0 = stream forever)
    #[arg(long)]
    pub stop_block: Option<u64>,

    /// Resume cursor from a previous session
    #[arg(long)]
    pub cursor: Option<String>,

    /// Output directory
    #[arg(long, default_value = "output")]
    pub output: PathBuf,

    /// Partitioning mode: none, block_range, date, hour
    #[arg(long, default_value = "none")]
    pub partition: String,

    /// Block range size when partition=block_range
    #[arg(long, default_value = "10000")]
    pub block_range_size: u64,

    /// Max rows per file before flush
    #[arg(long, default_value = "50000")]
    pub flush_rows: u32,

    /// Max bytes per file before flush
    #[arg(long, default_value = "134217728")]
    pub flush_bytes: u64,

    /// Time-based flush interval in seconds (disabled by default)
    #[arg(long)]
    pub flush_interval_secs: Option<u64>,

    /// Compression codec: zstd, snappy, gzip, none
    #[arg(long, default_value = "zstd")]
    pub compression: String,

    /// Log level: trace, debug, info, warn, error
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Decode and map but don't write files
    #[arg(long, default_value = "false")]
    pub dry_run: bool,

    /// Only process finalized blocks (when false, adds fork_step column)
    #[arg(long, default_value = "true")]
    pub final_blocks_only: bool,
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
}

/// Parse a compression string into a [`Compression`] variant.
pub fn parse_compression(s: &str) -> Compression {
    match s.to_lowercase().as_str() {
        "snappy" => Compression::Snappy,
        "gzip" => Compression::Gzip,
        "none" => Compression::None,
        _ => Compression::Zstd,
    }
}

/// Parse a partition string into a [`Partition`] variant.
pub fn parse_partition(s: &str, block_range_size: u64) -> Partition {
    match s.to_lowercase().as_str() {
        "block_range" => Partition::BlockRange(block_range_size),
        "date" => Partition::Date,
        "hour" => Partition::Hour,
        _ => Partition::None,
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
    Ok(Config {
        endpoint,
        api_key: args.api_key.clone(),
        jwt_token: args.jwt_token.clone(),
        start_block: args.start_block,
        stop_block: args.stop_block,
        cursor: args.cursor.clone(),
        output: args.output.clone(),
        partition: parse_partition(&args.partition, args.block_range_size),
        flush_rows: args.flush_rows,
        flush_bytes: args.flush_bytes,
        flush_interval_secs: args.flush_interval_secs,
        compression: parse_compression(&args.compression),
        final_blocks_only: args.final_blocks_only,
        dry_run: args.dry_run,
    })
}

/// Initialize tracing subscriber with the given log level.
pub fn init_tracing(log_level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_new(log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Generate shell completions for the given command and write to stdout.
pub fn generate_completions<C: clap::CommandFactory>(shell: Shell) {
    let mut cmd = C::command();
    let name = cmd.get_name().to_string();
    generate(shell, &mut cmd, name, &mut io::stdout());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Compression, Partition};
    use clap::{CommandFactory, Parser};

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
    fn test_required_endpoint() {
        // endpoint is optional at the clap level (for subcommands like completions)
        // but build_config will fail without it
        let cli = parse(&["test-cli"]);
        assert!(cli.common.endpoint.is_none());
        assert!(build_config(&cli.common).is_err());
    }

    #[test]
    fn test_defaults() {
        let cli = parse(&["test-cli", "--endpoint", "http://localhost:9000"]);
        assert_eq!(cli.common.endpoint.as_deref(), Some("http://localhost:9000"));
        assert_eq!(cli.common.output, PathBuf::from("output"));
        assert_eq!(cli.common.partition, "none");
        assert_eq!(cli.common.block_range_size, 10000);
        assert_eq!(cli.common.flush_rows, 50000);
        assert_eq!(cli.common.flush_bytes, 134217728);
        assert_eq!(cli.common.compression, "zstd");
        assert_eq!(cli.common.log_level, "info");
        assert!(!cli.common.dry_run);
        assert!(cli.common.final_blocks_only);
        assert!(cli.common.api_key.is_none());
        assert!(cli.common.jwt_token.is_none());
        assert!(cli.common.start_block.is_none());
        assert!(cli.common.stop_block.is_none());
        assert!(cli.common.cursor.is_none());
        assert!(cli.common.flush_interval_secs.is_none());
    }

    #[test]
    fn test_all_flags() {
        let cli = parse(&[
            "test-cli",
            "--endpoint", "https://eth.firehose.pinax.network:443",
            "--api-key", "my-key",
            "--start-block", "100",
            "--stop-block", "200",
            "--cursor", "abc123",
            "--output", "/tmp/out",
            "--partition", "date",
            "--block-range-size", "5000",
            "--flush-rows", "10000",
            "--flush-bytes", "1000000",
            "--flush-interval-secs", "60",
            "--compression", "snappy",
            "--log-level", "debug",
            "--dry-run",
        ]);
        assert_eq!(cli.common.endpoint.as_deref(), Some("https://eth.firehose.pinax.network:443"));
        assert_eq!(cli.common.api_key.as_deref(), Some("my-key"));
        assert_eq!(cli.common.start_block, Some(100));
        assert_eq!(cli.common.stop_block, Some(200));
        assert_eq!(cli.common.cursor.as_deref(), Some("abc123"));
        assert_eq!(cli.common.output, PathBuf::from("/tmp/out"));
        assert_eq!(cli.common.partition, "date");
        assert_eq!(cli.common.block_range_size, 5000);
        assert_eq!(cli.common.flush_rows, 10000);
        assert_eq!(cli.common.flush_bytes, 1000000);
        assert_eq!(cli.common.flush_interval_secs, Some(60));
        assert_eq!(cli.common.compression, "snappy");
        assert_eq!(cli.common.log_level, "debug");
        assert!(cli.common.dry_run);
    }

    #[test]
    fn test_parse_compression() {
        assert_eq!(parse_compression("zstd"), Compression::Zstd);
        assert_eq!(parse_compression("snappy"), Compression::Snappy);
        assert_eq!(parse_compression("gzip"), Compression::Gzip);
        assert_eq!(parse_compression("none"), Compression::None);
        assert_eq!(parse_compression("ZSTD"), Compression::Zstd);
        assert_eq!(parse_compression("unknown"), Compression::Zstd);
    }

    #[test]
    fn test_parse_partition() {
        assert_eq!(parse_partition("none", 10000), Partition::None);
        assert_eq!(parse_partition("date", 10000), Partition::Date);
        assert_eq!(parse_partition("hour", 10000), Partition::Hour);
        assert_eq!(parse_partition("block_range", 5000), Partition::BlockRange(5000));
        assert_eq!(parse_partition("BLOCK_RANGE", 20000), Partition::BlockRange(20000));
        assert_eq!(parse_partition("unknown", 10000), Partition::None);
    }

    #[test]
    fn test_build_config() {
        let cli = parse(&[
            "test-cli",
            "--endpoint", "https://example.com:443",
            "--start-block", "100",
            "--compression", "gzip",
            "--partition", "date",
        ]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.endpoint, "https://example.com:443");
        assert_eq!(config.start_block, Some(100));
        assert_eq!(config.compression, Compression::Gzip);
        assert_eq!(config.partition, Partition::Date);
        assert_eq!(config.flush_rows, 50000);
        assert!(config.final_blocks_only);
    }

    #[test]
    fn test_completions_subcommand_parse() {
        let cli = parse(&["test-cli", "completions", "bash"]);
        assert!(cli.command.is_some());
        match cli.command.unwrap() {
            Commands::Completions { shell } => assert_eq!(shell, Shell::Bash),
        }
    }

    #[test]
    fn test_completions_generation() {
        // Verify that shell completion generation runs without panicking
        // for each supported shell type.
        for shell in [Shell::Bash, Shell::Zsh, Shell::Fish, Shell::Elvish, Shell::PowerShell] {
            let mut cmd = TestCli::command();
            let name = cmd.get_name().to_string();
            let mut buf = Vec::new();
            generate(shell, &mut cmd, name, &mut buf);
            assert!(!buf.is_empty(), "completions for {shell:?} should not be empty");
        }
    }
}
