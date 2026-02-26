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

// Shared CLI arguments for all firehose-to-parquet binaries.
//
// Embed in a per-chain `#[derive(Parser)]` struct with `#[command(flatten)]`.
#[derive(Args, Debug, Clone)]
pub struct CommonArgs {
    /// Firehose gRPC endpoint URL
    #[arg(short = 'e', long, env = "ENDPOINT")]
    pub endpoint: Option<String>,

    /// Name of environment variable containing the API key for authentication
    #[arg(long, env = "API_KEY_ENVVAR", default_value = "SUBSTREAMS_API_KEY")]
    pub api_key_envvar: String,

    /// Name of environment variable containing the JWT bearer token for authentication
    #[arg(long, env = "API_TOKEN_ENVVAR", default_value = "SUBSTREAMS_API_TOKEN")]
    pub api_token_envvar: String,

    /// Start block number (inclusive)
    #[arg(short = 's', long, env = "START_BLOCK")]
    pub start_block: Option<u64>,

    /// Stop block number (inclusive, 0 = stream forever)
    #[arg(short = 't', long, env = "STOP_BLOCK")]
    pub stop_block: Option<u64>,

    /// Path to cursor file for resuming a previous session
    #[arg(short = 'c', long, env = "CURSOR")]
    pub cursor: Option<PathBuf>,

    /// Output directory
    #[arg(long, env = "OUTPUT", default_value = "output")]
    pub output: PathBuf,

    /// Partitioning mode: none, block_range, date, hour
    #[arg(long, env = "PARTITION", default_value = "none")]
    pub partition: String,

    /// Block range size when partition=block_range
    #[arg(long, env = "BLOCK_RANGE_SIZE", default_value = "10000")]
    pub block_range_size: u64,

    /// Max rows per file before flush
    #[arg(long, env = "FLUSH_ROWS", default_value = "50000")]
    pub flush_rows: u32,

    /// Max bytes per file before flush
    #[arg(long, env = "FLUSH_BYTES", default_value = "134217728")]
    pub flush_bytes: u64,

    /// Time-based flush interval in seconds (disabled by default)
    #[arg(long, env = "FLUSH_INTERVAL_SECS")]
    pub flush_interval_secs: Option<u64>,

    /// Compression codec: zstd, snappy, gzip, none
    #[arg(long, env = "COMPRESSION", default_value = "zstd")]
    pub compression: String,

    /// Log level: trace, debug, info, warn, error
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Decode and map but don't write files
    #[arg(long, env = "DRY_RUN", default_value = "false")]
    pub dry_run: bool,

    /// Only process finalized blocks (when false, adds fork_step column)
    #[arg(long, env = "FINAL_BLOCKS_ONLY", default_value = "true")]
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

    // Resolve the actual API key / JWT token by reading the environment variable
    // whose *name* is given by `--api-key-envvar` / `--api-token-envvar`.
    let api_key = std::env::var(&args.api_key_envvar).ok().filter(|v| !v.is_empty());
    let jwt_token = std::env::var(&args.api_token_envvar).ok().filter(|v| !v.is_empty());

    Ok(Config {
        endpoint,
        api_key,
        jwt_token,
        start_block: args.start_block,
        stop_block: args.stop_block,
        cursor_path: args.cursor.clone(),
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
        assert_eq!(cli.common.api_key_envvar, "SUBSTREAMS_API_KEY");
        assert_eq!(cli.common.api_token_envvar, "SUBSTREAMS_API_TOKEN");
        assert!(cli.common.start_block.is_none());
        assert!(cli.common.stop_block.is_none());
        assert!(cli.common.cursor.is_none());
        assert!(cli.common.flush_interval_secs.is_none());
    }

    #[test]
    fn test_all_flags() {
        let cli = parse(&[
            "test-cli",
            "-e", "https://eth.firehose.pinax.network:443",
            "--api-key-envvar", "MY_KEY_VAR",
            "--api-token-envvar", "MY_TOKEN_VAR",
            "-s", "100",
            "-t", "200",
            "-c", "cursor.txt",
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
        assert_eq!(cli.common.api_key_envvar, "MY_KEY_VAR");
        assert_eq!(cli.common.api_token_envvar, "MY_TOKEN_VAR");
        assert_eq!(cli.common.start_block, Some(100));
        assert_eq!(cli.common.stop_block, Some(200));
        assert_eq!(cli.common.cursor.as_deref(), Some(std::path::Path::new("cursor.txt")));
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

    #[test]
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
        assert_eq!(cli.common.endpoint.as_deref(), Some("https://from-env.example.com:443"));
        assert_eq!(cli.common.start_block, Some(42));
        assert_eq!(cli.common.compression, "snappy");

        // CLI flags take precedence over env vars
        let cli = parse(&["test-cli", "-e", "https://from-cli.example.com:443", "--compression", "gzip"]);
        assert_eq!(cli.common.endpoint.as_deref(), Some("https://from-cli.example.com:443"));
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
    fn test_custom_api_key_envvar() {
        // Test using a custom envvar name
        unsafe {
            std::env::set_var("MY_CUSTOM_KEY", "custom-key-value");
        }

        let cli = parse(&["test-cli", "--endpoint", "https://example.com:443", "--api-key-envvar", "MY_CUSTOM_KEY"]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.api_key.as_deref(), Some("custom-key-value"));

        // Clean up
        unsafe {
            std::env::remove_var("MY_CUSTOM_KEY");
        }
    }
}
