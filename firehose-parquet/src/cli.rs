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

    /// Partitioning mode: none, block_range, date, hour, minute
    #[arg(long, env = "PARTITION", default_value = "none")]
    pub partition: String,

    /// Block range size when partition=block_range
    #[arg(long, env = "BLOCK_RANGE_SIZE", default_value = "10000")]
    pub block_range_size: u64,

    /// Max rows per file before flush (disabled by default)
    #[arg(long, env = "FLUSH_ROWS")]
    pub flush_rows: Option<u32>,

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

    /// Public endpoint (skip authentication)
    #[arg(long, env = "PUBLIC", default_value = "false")]
    pub public: bool,

    /// AWS access key ID (for S3 output)
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    pub aws_access_key_id: Option<String>,

    /// AWS secret access key (for S3 output)
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub aws_secret_access_key: Option<String>,

    /// AWS session token (for S3 output)
    #[arg(long, env = "AWS_SESSION_TOKEN")]
    pub aws_session_token: Option<String>,

    /// AWS region (for S3 output)
    #[arg(long, env = "AWS_REGION")]
    pub aws_region: Option<String>,

    /// AWS endpoint URL (for S3-compatible services)
    #[arg(long, env = "AWS_ENDPOINT_URL")]
    pub aws_endpoint_url: Option<String>,
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
    /// Read and inspect Parquet files (schema, row counts, sample rows)
    Scan {
        /// Path to a .parquet file or directory containing parquet files
        path: PathBuf,
        /// Number of sample rows to display per file (0 = schema only)
        #[arg(short = 'n', long, default_value = "20")]
        rows: usize,
        /// Only show file metadata (schema, row count, size) without data
        #[arg(long, default_value = "false")]
        schema_only: bool,
    },
}

/// Parse a compression string into a [`Compression`] variant.
pub fn parse_compression(s: &str) -> anyhow::Result<Compression> {
    match s.to_lowercase().as_str() {
        "zstd" => Ok(Compression::Zstd),
        "snappy" => Ok(Compression::Snappy),
        "gzip" => Ok(Compression::Gzip),
        "none" => Ok(Compression::None),
        other => anyhow::bail!("invalid --compression '{other}': expected one of: zstd, snappy, gzip, none"),
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
        other => anyhow::bail!("invalid --partition '{other}': expected one of: none, block_range, date, hour, minute"),
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

    // When `--public` is set, skip authentication.
    let (api_key, jwt_token) = if args.public {
        (None, None)
    } else {
        // Resolve the actual API key / JWT token by reading the environment variable
        // whose *name* is given by `--api-key-envvar` / `--api-token-envvar`.
        let api_key = std::env::var(&args.api_key_envvar).ok().filter(|v| !v.is_empty());
        let jwt_token = std::env::var(&args.api_token_envvar).ok().filter(|v| !v.is_empty());
        (api_key, jwt_token)
    };

    Ok(Config {
        endpoint,
        api_key,
        jwt_token,
        start_block: args.start_block,
        stop_block: args.stop_block,
        cursor_path: args.cursor.clone(),
        public: args.public,
        output: args.output.clone(),
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

/// Scan and display parquet files at the given path.
///
/// If `path` is a file, inspects that single file.
/// If `path` is a directory, recursively finds all `.parquet` files.
pub fn scan_parquet(path: &PathBuf, rows: usize, schema_only: bool) -> anyhow::Result<()> {
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

        let total_rows: i64 = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .sum();
        let num_row_groups = metadata.num_row_groups();
        let num_columns = metadata.file_metadata().schema().get_fields().len();
        let schema = builder.schema();

        // Relative path for cleaner display.
        let display_path = file_path
            .strip_prefix(path)
            .unwrap_or(file_path);

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
            let nullable = if field.is_nullable() { "nullable" } else { "not null" };
            println!("  {:30} {:20} {}", field.name(), field.data_type(), nullable);
        }

        // Print sample rows (vertical format like ClickHouse's \G).
        if !schema_only && rows > 0 {
            let file = fs::File::open(file_path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;

            // Compute max field name width for alignment.
            let max_name_len = schema.fields().iter().map(|f| f.name().len()).max().unwrap_or(0);

            let mut row_number = 0usize;

            'outer: for batch_result in reader {
                let batch = batch_result?;
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
    }

    // Summary.
    if files.len() > 1 {
        println!("\n{}", "═".repeat(72));
        println!("  {} parquet files scanned", files.len());
    }

    Ok(())
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
            let v = array.as_any().downcast_ref::<UInt64Array>().unwrap().value(row);
            format_number_with_hint(v as i128)
        }
        DataType::UInt32 => {
            let v = array.as_any().downcast_ref::<UInt32Array>().unwrap().value(row);
            v.to_string()
        }
        DataType::Int64 => {
            let v = array.as_any().downcast_ref::<Int64Array>().unwrap().value(row);
            format_number_with_hint(v as i128)
        }
        DataType::Int32 => {
            let v = array.as_any().downcast_ref::<Int32Array>().unwrap().value(row);
            v.to_string()
        }
        DataType::Float64 => {
            let v = array.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
            format!("{v}")
        }
        DataType::Boolean => {
            let v = array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row);
            v.to_string()
        }
        DataType::Utf8 => {
            let v = array.as_any().downcast_ref::<StringArray>().unwrap().value(row);
            truncate_str(v, 80)
        }
        DataType::LargeUtf8 => {
            let v = array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row);
            truncate_str(v, 80)
        }
        DataType::Binary => {
            let v = array.as_any().downcast_ref::<BinaryArray>().unwrap().value(row);
            truncate_str(&format!("0x{}", hex::encode(v)), 80)
        }
        DataType::LargeBinary => {
            let v = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap().value(row);
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
            let formatter = arrow::util::display::ArrayFormatter::try_new(array, &Default::default());
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
    let items: Vec<String> = (0..n)
        .map(|i| format_array_value(array, i))
        .collect();
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
fn format_bytes(bytes: u64) -> String {
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Compression, Partition};
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
        let cli = parse(&["test-cli", "--endpoint", "http://localhost:9000"]);
        assert_eq!(cli.common.endpoint.as_deref(), Some("http://localhost:9000"));
        assert_eq!(cli.common.output, PathBuf::from("output"));
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
        assert!(cli.common.cursor.is_none());
        assert!(cli.common.flush_interval_secs.is_none());
        assert!(!cli.common.public);
        assert!(cli.common.aws_access_key_id.is_none());
        assert!(cli.common.aws_secret_access_key.is_none());
        assert!(cli.common.aws_session_token.is_none());
        assert!(cli.common.aws_region.is_none());
        assert!(cli.common.aws_endpoint_url.is_none());
    }

    #[test]
    #[serial]
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
        assert_eq!(cli.common.flush_rows, Some(10000));
        assert_eq!(cli.common.flush_bytes, 1000000);
        assert_eq!(cli.common.flush_interval_secs, Some(60));
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
        assert_eq!(parse_partition("block_range", 5000).unwrap(), Partition::BlockRange(5000));
        assert_eq!(parse_partition("BLOCK_RANGE", 20000).unwrap(), Partition::BlockRange(20000));
        assert!(parse_partition("unknown", 10000).is_err());
    }

    #[test]
    #[serial]
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
        assert!(config.flush_rows.is_none());
        assert!(config.final_blocks_only);
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
    #[serial]
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

        let cli = parse(&["test-cli", "--endpoint", "https://example.com:443", "--api-key-envvar", "MY_CUSTOM_KEY"]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.api_key.as_deref(), Some("custom-key-value"));

        // Clean up
        unsafe {
            std::env::remove_var("MY_CUSTOM_KEY");
        }
    }

    #[test]
    #[serial]
    fn test_public_flag_default() {
        let cli = parse(&["test-cli", "--endpoint", "http://localhost:9000"]);
        assert!(!cli.common.public);
    }

    #[test]
    #[serial]
    fn test_public_flag_set() {
        let cli = parse(&["test-cli", "--endpoint", "http://localhost:9000", "--public"]);
        assert!(cli.common.public);
    }

    #[test]
    #[serial]
    fn test_public_flag_skips_auth() {
        // When --public is set, API key and JWT token should be None even
        // if the corresponding env vars are defined.
        unsafe {
            std::env::set_var("SUBSTREAMS_API_KEY", "should-be-skipped");
            std::env::set_var("SUBSTREAMS_API_TOKEN", "should-be-skipped");
        }

        let cli = parse(&["test-cli", "--endpoint", "https://example.com:443", "--public"]);
        let config = build_config(&cli.common).expect("build_config should succeed");
        assert!(config.public);
        assert!(config.api_key.is_none());
        assert!(config.jwt_token.is_none());

        unsafe {
            std::env::remove_var("SUBSTREAMS_API_KEY");
            std::env::remove_var("SUBSTREAMS_API_TOKEN");
        }
    }

    #[test]
    #[serial]
    fn test_aws_credentials_flags() {
        let cli = parse(&[
            "test-cli",
            "--endpoint", "https://example.com:443",
            "--aws-access-key-id", "AKID123",
            "--aws-secret-access-key", "secret456",
            "--aws-session-token", "token789",
            "--aws-region", "us-east-1",
            "--aws-endpoint-url", "https://s3.custom.endpoint",
        ]);
        assert_eq!(cli.common.aws_access_key_id.as_deref(), Some("AKID123"));
        assert_eq!(cli.common.aws_secret_access_key.as_deref(), Some("secret456"));
        assert_eq!(cli.common.aws_session_token.as_deref(), Some("token789"));
        assert_eq!(cli.common.aws_region.as_deref(), Some("us-east-1"));
        assert_eq!(cli.common.aws_endpoint_url.as_deref(), Some("https://s3.custom.endpoint"));

        let config = build_config(&cli.common).expect("build_config should succeed");
        assert_eq!(config.aws_access_key_id.as_deref(), Some("AKID123"));
        assert_eq!(config.aws_secret_access_key.as_deref(), Some("secret456"));
        assert_eq!(config.aws_session_token.as_deref(), Some("token789"));
        assert_eq!(config.aws_region.as_deref(), Some("us-east-1"));
        assert_eq!(config.aws_endpoint_url.as_deref(), Some("https://s3.custom.endpoint"));
    }

    #[test]
    #[serial]
    fn test_aws_credentials_defaults_none() {
        let cli = parse(&["test-cli", "--endpoint", "http://localhost:9000"]);
        assert!(cli.common.aws_access_key_id.is_none());
        assert!(cli.common.aws_secret_access_key.is_none());
        assert!(cli.common.aws_session_token.is_none());
        assert!(cli.common.aws_region.is_none());
        assert!(cli.common.aws_endpoint_url.is_none());
    }
}
