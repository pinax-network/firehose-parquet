
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

fn write_test_verified_partitions_index(
    path: &std::path::Path,
    mut rows: Vec<PartitionBuildRow>,
) -> anyhow::Result<()> {
    use crate::grpc::FinalizedAnchor;
    use crate::partition_index::{CoveredBlock, IndexRoutingPolicy};
    rows.sort_by_key(|row| row.start_block);
    let first = rows.first().unwrap().start_block;
    let stop = rows.last().unwrap().stop_block;
    let block_number = rows[0].partition_type == "block_range";
    let identity = |number| CoveredBlock {
        block_num: number,
        block_id: format!("id-{number}"),
        parent_num: number.saturating_sub(1),
        parent_id: if number == 0 {
            String::new()
        } else {
            format!("id-{}", number - 1)
        },
    };
    let mut last_timestamp = None;
    let spans = rows
        .into_iter()
        .map(|mut row| {
            let timestamp = if block_number {
                None
            } else {
                Some(parse_partition_timestamp(&row.partition_value)?)
            };
            if row.start_time.is_none() {
                row.start_time = timestamp.map(format_partition_timestamp).transpose()?;
            }
            last_timestamp = timestamp;
            let proof = PartitionSpanProof {
                start_complete: true,
                end_complete: true,
                first_block: (!block_number).then(|| FinalizedAnchor {
                    block_num: row.start_block,
                    block_id: format!("id-{}", row.start_block),
                }),
                routing_start_timestamp: timestamp,
            };
            Ok(VerifiedPartitionSpan { row, proof })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let coverage = PartitionCoverage {
        version: 2,
        start_block: first,
        stop_block: stop,
        finalized: FinalizedAnchor {
            block_num: stop + 5,
            block_id: format!("id-{}", stop + 5),
        },
        routing_policy: if block_number {
            IndexRoutingPolicy::BlockNumber
        } else {
            IndexRoutingPolicy::CanonicalTimestamp
        },
        first_observed: (!block_number).then(|| identity(first)),
        last_observed: (!block_number).then(|| identity(stop - 1)),
        next_observed: (!block_number).then(|| identity(stop)),
        last_routing_timestamp: last_timestamp,
    };
    write_verified_partitions_index(
        path.to_str().unwrap(),
        &VerifiedPartitionIndex { coverage, spans },
        Compression::Zstd,
        None,
        None,
    )
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
fn final_blocks_only_accepts_explicit_values_and_preserves_bare_flag() {
    let _env = EnvVarGuard::remove("FINAL_BLOCKS_ONLY");
    for (args, expected) in [
        (vec!["test-cli"], true),
        (vec!["test-cli", "--final-blocks-only"], true),
        (vec!["test-cli", "--final-blocks-only=true"], true),
        (vec!["test-cli", "--final-blocks-only=false"], false),
    ] {
        let mut args = args;
        args.extend(["--endpoint", "http://localhost:9000", "--partition", "none"]);
        let parsed = try_parse(&args).unwrap();
        assert_eq!(parsed.common.final_blocks_only, expected);
        assert_eq!(
            build_config(&parsed.common).unwrap().final_blocks_only,
            expected
        );
    }
    assert!(try_parse(&["test-cli", "--final-blocks-only=maybe"]).is_err());
    // An optional bool must not consume the next subcommand as its value.
    assert!(try_parse(&["test-cli", "--final-blocks-only", "completions", "zsh"]).is_ok());
}

#[test]
#[serial]
fn final_blocks_only_environment_is_overridden_by_explicit_cli() {
    let _env = EnvVarGuard::set("FINAL_BLOCKS_ONLY", "false");
    assert!(!parse(&["test-cli"]).common.final_blocks_only);
    assert!(
        parse(&["test-cli", "--final-blocks-only"])
            .common
            .final_blocks_only
    );
    assert!(
        parse(&["test-cli", "--final-blocks-only=true"])
            .common
            .final_blocks_only
    );
    let _env_true = EnvVarGuard::set("FINAL_BLOCKS_ONLY", "true");
    assert!(
        !parse(&["test-cli", "--final-blocks-only=false"])
            .common
            .final_blocks_only
    );
}

#[test]
fn bounded_tail_warning_applies_only_to_non_final_bounded_runs() {
    assert!(non_final_bounded_warning(true, Some(100)).is_none());
    assert!(non_final_bounded_warning(true, None).is_none());
    assert!(non_final_bounded_warning(false, None).is_none());
    assert!(non_final_bounded_warning(false, Some(100))
        .unwrap()
        .contains("does not prove its tail is final"));
}

#[test]
#[serial]
fn grpc_transport_flags_validate_limits_and_apply_to_both_build_commands() {
    let _adaptive = EnvVarGuard::set("GRPC_ADAPTIVE_WINDOW", "false");
    let _window = EnvVarGuard::set("GRPC_WINDOW_BYTES", "16777216");
    let _limit = EnvVarGuard::set("GRPC_MAX_MESSAGE_BYTES", "134217728");
    let parsed = parse(&[
        "test-cli",
        "--endpoint",
        "http://localhost",
        "--partition",
        "none",
    ]);
    let default = build_config(&parsed.common).unwrap();
    assert_eq!(default.grpc, crate::config::GrpcConfig::default());
    for args in [
        vec![
            "test-cli",
            "--endpoint",
            "http://localhost",
            "--partition",
            "none",
            "--grpc-adaptive-window=false",
            "--grpc-max-message-bytes",
            "268435456",
        ],
        vec![
            "test-cli",
            "partitions",
            "build",
            "--endpoint",
            "http://localhost",
            "--partition",
            "date",
            "--stop-block",
            "10",
            "--grpc-adaptive-window=false",
            "--grpc-max-message-bytes",
            "268435456",
        ],
    ] {
        let parsed = try_parse(&args).unwrap();
        let grpc = match parsed.command {
            Some(Commands::Partitions(PartitionsCommands::Build { grpc, .. })) => grpc.config(),
            None => build_config(&parsed.common).unwrap().grpc,
            _ => unreachable!(),
        };
        assert!(!grpc.adaptive_window);
        assert_eq!(grpc.max_message_bytes, 268435456);
    }
    for value in ["0", "-1", "4294967296", "nope"] {
        assert!(try_parse(&["test-cli", "--grpc-max-message-bytes", value]).is_err());
    }
    assert!(try_parse(&["test-cli", "--grpc-adaptive-window=maybe"]).is_err());
    for value in ["-1", "2147483648", "nope"] {
        assert!(try_parse(&["test-cli", "--grpc-window-bytes", value]).is_err());
    }
    assert_eq!(
        parse(&["test-cli", "--grpc-window-bytes", "0"])
            .common
            .grpc
            .config()
            .initial_window_bytes,
        None
    );
    assert_eq!(
        parse(&["test-cli", "--grpc-window-bytes", "65535"])
            .common
            .grpc
            .config()
            .initial_window_bytes,
        Some(65535)
    );
    let _enabled = EnvVarGuard::set("GRPC_ADAPTIVE_WINDOW", "true");
    assert!(
        !parse(&["test-cli", "--grpc-adaptive-window=false"])
            .common
            .grpc
            .adaptive_window
    );
    let _adaptive = EnvVarGuard::set("GRPC_ADAPTIVE_WINDOW", "false");
    let _limit = EnvVarGuard::set("GRPC_MAX_MESSAGE_BYTES", "4096");
    let parsed = parse(&["test-cli"]);
    assert!(!parsed.common.grpc.adaptive_window);
    assert_eq!(parsed.common.grpc.max_message_bytes, 4096);
    let parsed = parse(&[
        "test-cli",
        "--grpc-adaptive-window",
        "--grpc-max-message-bytes",
        "8192",
    ]);
    assert!(parsed.common.grpc.adaptive_window);
    assert_eq!(parsed.common.grpc.max_message_bytes, 8192);
}

#[test]
#[serial]
fn shared_aws_args_preserve_env_precedence_and_recovery_endpoint_name() {
    use crate::recovery::RecoveryCommands;
    let _key = EnvVarGuard::set("AWS_ACCESS_KEY_ID", "synthetic-env-key");
    let _secret = EnvVarGuard::set("AWS_SECRET_ACCESS_KEY", "synthetic-env-secret");
    let _token = EnvVarGuard::set("AWS_SESSION_TOKEN", "synthetic-env-token");
    let _region = EnvVarGuard::set("AWS_REGION", "synthetic-env-region");
    let _endpoint = EnvVarGuard::set("AWS_ENDPOINT_URL_S3", "https://ordinary.example");
    let _recovery = EnvVarGuard::set("AWS_ENDPOINT_URL", "https://recovery.example");
    for (base, endpoint) in [
        (vec!["test-cli"], "https://ordinary.example"),
        (
            vec!["test-cli", "inspect", "fixture.parquet"],
            "https://ordinary.example",
        ),
        (
            vec!["test-cli", "recovery", "status", "fixture"],
            "https://recovery.example",
        ),
    ] {
        for explicit in [false, true] {
            let mut args = base.clone();
            if explicit {
                args.extend([
                    "--aws-access-key-id",
                    "synthetic-cli-key",
                    "--aws-secret-access-key",
                    "synthetic-cli-secret",
                    "--aws-session-token",
                    "synthetic-cli-token",
                    "--aws-region",
                    "synthetic-cli-region",
                    "--aws-endpoint-url",
                    "https://explicit.example",
                ]);
            }
            let cli = try_parse(&args).unwrap();
            let aws = match cli.command {
                None => cli.common.aws,
                Some(Commands::Inspect { aws, .. }) => aws,
                Some(Commands::Recovery(RecoveryCommands::Status(storage))) => storage.aws,
                _ => panic!("unexpected fixture command"),
            };
            let config = AwsConfig::from(&aws);
            let source = if explicit { "cli" } else { "env" };
            assert_eq!(
                config.aws_access_key_id,
                Some(format!("synthetic-{source}-key"))
            );
            assert_eq!(
                config.aws_secret_access_key,
                Some(format!("synthetic-{source}-secret"))
            );
            assert_eq!(
                config.aws_session_token,
                Some(format!("synthetic-{source}-token"))
            );
            assert_eq!(
                config.aws_region,
                Some(format!("synthetic-{source}-region"))
            );
            assert_eq!(
                config.aws_endpoint_url.as_deref(),
                Some(if explicit {
                    "https://explicit.example"
                } else {
                    endpoint
                })
            );
        }
    }
    let help = TestCli::command().render_long_help().to_string();
    for secret in [
        "synthetic-env-key",
        "synthetic-env-secret",
        "synthetic-env-token",
    ] {
        assert!(!help.contains(secret));
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
    assert_eq!(cli.common.flush_memory_bytes, DEFAULT_FLUSH_MEMORY_BYTES);
    assert_eq!(cli.common.compression, "zstd");
    assert_eq!(cli.common.log_level, "info");
    assert!(!cli.common.verbose);
    assert!(!cli.common.dry_run);
    assert!(cli.common.final_blocks_only);
    assert_eq!(cli.common.api_key_envvar, None);
    assert_eq!(cli.common.api_token_envvar, None);
    assert!(cli.common.start_block.is_none());
    assert!(cli.common.stop_block.is_none());
    assert_eq!(cli.common.cursor, PathBuf::from("cursor.parquet"));
    assert!(cli.common.cursor_template.is_none());
    assert!(cli.common.flush_interval_secs.is_none());
    assert!(cli.common.aws.aws_access_key_id.is_none());
    assert!(cli.common.aws.aws_secret_access_key.is_none());
    assert!(cli.common.aws.aws_session_token.is_none());
    assert!(cli.common.aws.aws_region.is_none());
    assert!(cli.common.aws.aws_endpoint_url.is_none());
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
    assert_eq!(cli.common.api_key_envvar.as_deref(), Some("MY_KEY_VAR"));
    assert_eq!(cli.common.api_token_envvar.as_deref(), Some("MY_TOKEN_VAR"));
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
    assert_eq!(parse_compression("zstd:3").unwrap(), Compression::Zstd);
    assert_eq!(parse_compression("ZSTD:6").unwrap().to_string(), "zstd:6");
    assert_eq!(parse_compression("zstd:-7").unwrap().to_string(), "zstd:-7");
    for invalid in [
        "zstd:0",
        "zstd:23",
        "zstd:-131073",
        "zstd:",
        "zstd:nan",
        "snappy:3",
    ] {
        assert!(parse_compression(invalid).is_err(), "{invalid}");
    }
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
#[serial]
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

    assert!(help.contains("Flush mapper state and write Parquet after this many rows"));
    assert!(help.contains("Flush written files after this many processed blocks"));
    assert!(help.contains("Target compressed bytes in the largest table file"));
    assert!(help.contains("--flush-memory-bytes"));
    assert!(help.contains("summed mapper byte estimate"));
    assert!(help.contains("Flush mapper state and write Parquet every N seconds"));
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
    let rollup_help = cmd
        .get_subcommands()
        .find(|command| command.get_name() == "rollup")
        .unwrap()
        .clone()
        .render_long_help()
        .to_string();
    assert!(rollup_help.contains(&default_flush_bytes));
    assert_eq!(Config::default().flush_bytes, DEFAULT_FLUSH_BYTES);
    assert_eq!(
        Config::default().flush_memory_bytes,
        DEFAULT_FLUSH_MEMORY_BYTES
    );
    assert!(merge_help.contains("Flush:"));
    assert!(merge_help.contains("--flush-rows"));
    assert!(merge_help.contains("--flush-bytes"));
    assert!(!merge_help.contains("--flush-blocks"));
}

#[test]
fn test_flush_memory_threshold_is_positive_and_propagated() {
    assert!(TestCli::try_parse_from([
        "test-cli",
        "--endpoint",
        "http://localhost",
        "--flush-memory-bytes",
        "0"
    ])
    .is_err());
    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "http://localhost",
        "--flush-memory-bytes",
        "123456",
    ]);
    assert_eq!(
        build_config(&cli.common).unwrap().flush_memory_bytes,
        123456
    );
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

    let read_rows = read_partitions_build_rows(&path.to_string_lossy(), None).expect("read rows");
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

    let read_rows = read_partitions_build_rows(&path.to_string_lossy(), None).expect("read rows");
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

    let verify_err =
        match crate::verify::verify_parquet("./mainnet/blocks/", None, &test_verify_options()) {
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

/// Commands that delete or rewrite files must not turn a missing (for example mistyped)
/// local path into `s3://$S3_BUCKET/<path>`.
#[test]
#[serial]
fn test_destructive_commands_do_not_fall_back_to_configured_s3_bucket() {
    let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
    let dir = tempfile::tempdir().expect("tempdir");
    let _cwd = CurrentDirGuard::set(dir.path());
    let assert_refused = |command: &str, err: anyhow::Error| {
        let message = err.to_string();
        assert!(
            message.contains("path does not exist: ./mainnet/blocks/"),
            "{command}: {message}"
        );
        assert!(
            message.contains("do not fall back to S3_BUCKET"),
            "{command}: {message}"
        );
        assert!(
            message.contains("s3://configured-bucket/mainnet/blocks"),
            "{command}: {message}"
        );
    };

    let truncate_err = crate::truncate::run_truncate(&crate::truncate::TruncateConfig {
        path: "./mainnet/blocks/".to_string(),
        partitions: vec![],
        dry_run: false,
        yes: true,
        aws: None,
    })
    .map(|_| ())
    .expect_err("truncate must not resolve to S3");
    assert_refused("truncate", truncate_err);

    let merge_err = crate::merge::run_merge(&crate::merge::MergeConfig {
        path: "./mainnet/blocks/".to_string(),
        compression: crate::config::Compression::Zstd,
        flush_rows: None,
        flush_bytes: 1024,
        dry_run: false,
        verbose: false,
        aws: None,
        cache_control: String::new(),
    })
    .map(|_| ())
    .expect_err("merge must not resolve to S3");
    assert_refused("merge", merge_err);

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
    .expect_err("rollup must not resolve to S3");
    assert_refused("rollup", rollup_err);
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
        yes: false,
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

    let verify_err =
        match crate::verify::verify_parquet("./mainnet/blocks/", None, &test_verify_options()) {
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
fn test_credentials_are_selected_from_resolved_endpoint_and_explicit_flags() {
    let _selector_key = EnvVarGuard::remove("API_KEY_ENVVAR");
    let _selector_token = EnvVarGuard::remove("API_TOKEN_ENVVAR");
    let _pinax = EnvVarGuard::set("PINAX_API_KEY", "pinax-test-key");
    let _legacy = EnvVarGuard::set("SUBSTREAMS_API_KEY", "legacy-test-key");
    let _sf_key = EnvVarGuard::remove("STREAMINGFAST_API_KEY");
    let _sf_token = EnvVarGuard::set("STREAMINGFAST_API_TOKEN", "sf-test-token");
    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "https://mainnet.tron.streamingfast.io",
    ]);
    let config = build_config(&cli.common).unwrap();
    assert_eq!(config.api_key, None);
    assert_eq!(config.jwt_token.as_deref(), Some("sf-test-token"));

    let cli = parse(&["test-cli", "--endpoint", "https://custom.example"]);
    let config = build_config(&cli.common).unwrap();
    assert_eq!(config.api_key, None);
    assert_eq!(config.jwt_token, None);

    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "https://custom.example",
        "--api-key-envvar",
        "SUBSTREAMS_API_KEY",
    ]);
    let config = build_config(&cli.common).unwrap();
    assert_eq!(config.api_key.as_deref(), Some("legacy-test-key"));
    assert_eq!(config.jwt_token, None);

    let _selector_key = EnvVarGuard::set("API_KEY_ENVVAR", "SUBSTREAMS_API_KEY");
    let cli = parse(&["test-cli", "--endpoint", "https://custom.example"]);
    assert_eq!(
        build_config(&cli.common).unwrap().api_key.as_deref(),
        Some("legacy-test-key")
    );
}

#[test]
#[serial]
fn test_partitions_auth_selectors_distinguish_default_from_explicit_legacy_name() {
    let _selector_key = EnvVarGuard::remove("API_KEY_ENVVAR");
    let _selector_token = EnvVarGuard::remove("API_TOKEN_ENVVAR");
    for explicit in [false, true] {
        let mut args = vec![
            "test-cli",
            "partitions",
            "build",
            "--partition",
            "date",
            "--network",
            "tron",
            "--stop-block",
            "100",
        ];
        if explicit {
            args.extend(["--api-token-envvar", "SUBSTREAMS_API_TOKEN"]);
        }
        let cli = parse(&args);
        let Some(Commands::Partitions(PartitionsCommands::Build {
            api_key_envvar,
            api_token_envvar,
            ..
        })) = cli.command
        else {
            panic!("expected partitions build");
        };
        assert_eq!(api_key_envvar, None);
        assert_eq!(
            api_token_envvar.as_deref(),
            explicit.then_some("SUBSTREAMS_API_TOKEN")
        );
    }
}

#[test]
#[serial]
fn test_api_key_envvar_resolution() {
    let _pinax_key = EnvVarGuard::remove("PINAX_API_KEY");
    let _pinax_token = EnvVarGuard::remove("PINAX_API_TOKEN");
    let _selector_key = EnvVarGuard::remove("API_KEY_ENVVAR");
    let _selector_token = EnvVarGuard::remove("API_TOKEN_ENVVAR");
    let _legacy_key = EnvVarGuard::set("SUBSTREAMS_API_KEY", "my-test-key");
    let _legacy_token = EnvVarGuard::set("SUBSTREAMS_API_TOKEN", "my-test-token");

    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "https://eth.firehose.pinax.network:443",
    ]);
    let config = build_config(&cli.common).expect("build_config should succeed");
    assert_eq!(config.api_key.as_deref(), Some("my-test-key"));
    assert_eq!(config.jwt_token.as_deref(), Some("my-test-token"));
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
    assert_eq!(cli.common.aws.aws_access_key_id.as_deref(), Some("AKID123"));
    assert_eq!(
        cli.common.aws.aws_secret_access_key.as_deref(),
        Some("secret456")
    );
    assert_eq!(
        cli.common.aws.aws_session_token.as_deref(),
        Some("token789")
    );
    assert_eq!(cli.common.aws.aws_region.as_deref(), Some("us-east-1"));
    assert_eq!(
        cli.common.aws.aws_endpoint_url.as_deref(),
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
    assert!(cli.common.aws.aws_access_key_id.is_none());
    assert!(cli.common.aws.aws_secret_access_key.is_none());
    assert!(cli.common.aws.aws_session_token.is_none());
    assert!(cli.common.aws.aws_region.is_none());
    assert!(cli.common.aws.aws_endpoint_url.is_none());
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
    let err =
        build_config(&cli.common).expect_err("build_config should fail without AWS_* credentials");
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
fn test_s3_bucket_rejects_conflicting_explicit_output() {
    unsafe {
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_SESSION_TOKEN");
        std::env::remove_var("AWS_REGION");
        std::env::remove_var("AWS_ENDPOINT_URL_S3");
        std::env::remove_var("S3_BUCKET");
    }
    // Conflicting buckets must fail before data and cursors can diverge.
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
    let error = build_config(&cli.common).expect_err("conflicting buckets must fail");
    let message = error.to_string();
    assert!(message.contains("S3 output bucket `other-bucket` disagrees"));
    assert!(message.contains("--s3-bucket / S3_BUCKET `my-bucket`"));
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
    write_test_verified_partitions_index(
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
    write_test_verified_partitions_index(
        &path,
        vec![
            time_partition_row(Some("eth-mainnet"), "hour", "2015-07-30 15:00:00", 200, 300),
            time_partition_row(Some("eth-mainnet"), "hour", "2015-07-30 16:00:00", 300, 400),
            time_partition_row(Some("eth-mainnet"), "hour", "2015-07-30 15:00:00", 400, 500),
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
fn test_resolve_partition_command_rejects_unverified_multi_chain_index() {
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
            all_spans: false,
        },
    )
    .expect_err("strict single chain should fail on multi-chain match");
    assert!(err.to_string().contains("rebuild"));
}

#[test]
fn test_resolve_partition_window_bounds_from_index_local() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("partitions.parquet");
    write_test_verified_partitions_index(
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
    write_test_verified_partitions_index(&path, rows).expect("write partitions index");

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
        arrow::datatypes::Field::new("partition_value", arrow::datatypes::DataType::Utf8, false),
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
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).expect("create arrow writer");
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
        arrow::datatypes::Field::new("partition_value", arrow::datatypes::DataType::Utf8, false),
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
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).expect("create arrow writer");
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
        arrow::datatypes::Field::new("partition_value", arrow::datatypes::DataType::Utf8, false),
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
            Arc::new(TimestampNanosecondArray::from(scaled(1_000_000_000)).with_timezone("UTC")),
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
    let err = parse_partition_build_types("date,hour").expect_err("multiple values should fail");
    assert!(err.to_string().contains("exactly one value per run"));
}

#[test]
#[serial]
fn test_build_config_allows_independent_cursor_bucket_with_local_output() {
    let _bucket = EnvVarGuard::set("S3_BUCKET", "data");
    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "http://localhost:9000",
        "--output",
        "./output",
        "--cursor",
        "s3://state/worker.parquet",
        "--aws-access-key-id",
        "test-key",
        "--aws-secret-access-key",
        "test-secret",
    ]);
    let config = build_config(&cli.common).unwrap();
    assert_eq!(config.output, PathBuf::from("./output"));
    assert_eq!(
        config.cursor_path.as_deref(),
        Some("s3://state/worker.parquet")
    );
}

#[test]
#[serial]
fn test_s3_bucket_env_rejects_conflicting_explicit_output() {
    let _bucket = EnvVarGuard::set("S3_BUCKET", "env-bucket");
    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "http://localhost:9000",
        "--output",
        "s3://data/mainnet",
    ]);
    let error = build_config(&cli.common).unwrap_err();
    assert!(error
        .to_string()
        .contains("--s3-bucket / S3_BUCKET `env-bucket`"));
}

#[test]
fn test_resolve_s3_output_root_rejects_mismatched_buckets() {
    let error = resolve_s3_output_root(Some("s3://data/mainnet"), Some("other")).unwrap_err();
    assert!(error
        .to_string()
        .contains("S3 output bucket `data` disagrees"));
    assert_eq!(
        resolve_s3_output_root(Some("s3://data/mainnet"), Some("data")).unwrap(),
        "s3://data/mainnet"
    );
    assert_eq!(
        resolve_s3_output_root(Some("/tmp/local"), Some("other")).unwrap(),
        "/tmp/local"
    );
}

#[test]
fn test_resolve_s3_output_root_prefers_explicit_output() {
    let resolved = resolve_s3_output_root(Some("./output"), Some("bucket-name")).expect("resolve");
    assert_eq!(resolved, "./output");

    let resolved =
        resolve_s3_output_root(Some("s3://other-bucket/prefix"), None).expect("resolve s3");
    assert_eq!(resolved, "s3://other-bucket/prefix");
}

#[test]
fn test_resolve_s3_output_root_accepts_bucket_without_output() {
    let resolved = resolve_s3_output_root(None, Some("bucket-name")).expect("resolve");
    assert_eq!(resolved, "s3://bucket-name");
}

#[test]
fn test_resolve_s3_output_root_rewrites_implicit_relative_output() {
    let resolved = resolve_s3_output_root(Some("output"), Some("bucket-name")).expect("resolve");
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
    let mut builder =
        PartitionIndexBuilder::new("eth-mainnet", vec![PartitionBuildType::Date]).expect("builder");
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

fn block_range_row(start_block: u64, stop_block: u64, block_range_size: u64) -> PartitionBuildRow {
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
    let rows = read_partitions_build_rows(path.to_str().unwrap(), None).unwrap();
    write_test_verified_partitions_index(&path, rows).unwrap();

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
    let rows = read_partitions_build_rows(path.to_str().unwrap(), None).unwrap();
    write_test_verified_partitions_index(&path, rows).unwrap();

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
    let rows = read_partitions_build_rows(path.to_str().unwrap(), None).unwrap();
    write_test_verified_partitions_index(&path, rows).unwrap();

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

    let files = collect_scan_parquet_local(dir.path(), 3, 0, ScanOrder::Asc, false).expect("scan");

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

    let files = collect_scan_parquet_local(dir.path(), 2, 3, ScanOrder::Asc, false).expect("scan");

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
fn verified_test_request(path: &std::path::Path, value: &str) -> PartitionBoundsRequest {
    PartitionBoundsRequest {
        index_path: path.to_string_lossy().into(),
        partition_type: "hour".into(),
        partition_value: value.into(),
        chain: Some("test-chain".into()),
    }
}
fn verified_test_rows() -> Vec<PartitionBuildRow> {
    [
        (10, "2023-11-14 22:00:00"),
        (11, "2023-11-14 23:00:00"),
        (12, "2023-11-14 22:00:00"),
    ]
    .into_iter()
    .map(|(number, value)| {
        time_partition_row(Some("test-chain"), "hour", value, number, number + 1)
    })
    .collect()
}
fn verified_list_request(path: &std::path::Path) -> PartitionListRequest {
    PartitionListRequest {
        index_path: path.to_string_lossy().into(),
        partition_type: Some("hour".into()),
        chain: Some("test-chain".into()),
        from: None,
        to: None,
        limit: 100,
    }
}

#[test]
fn verified_consumers_preserve_disjoint_runs_and_refuse_enclosing_holes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("index.parquet");
    write_test_verified_partitions_index(&path, verified_test_rows()).unwrap();
    let request = verified_test_request(&path, "2023-11-14 22:00:00");
    assert!(resolve_partition_bounds_from_index(&request, None)
        .unwrap_err()
        .to_string()
        .contains("ambiguous"));
    let all = resolve_partition_command(
        request,
        None,
        &PartitionResolveOptions {
            strict_single_chain: true,
            all_spans: true,
        },
    )
    .unwrap();
    assert_eq!(all.start_block, None);
    assert_eq!(all.stop_block, None);
    assert_eq!(all.coverage.start_block, 10);
    assert_eq!(all.coverage.stop_block, 13);
    let spans = all.spans.unwrap();
    assert_eq!(
        spans
            .iter()
            .map(|span| (span.start_block, span.stop_block))
            .collect::<Vec<_>>(),
        vec![(10, 11), (12, 13)]
    );
    let mut window = PartitionWindowRequest {
        index_path: path.to_string_lossy().into(),
        partition_type: "hour".into(),
        partition_from: "2023-11-14 22:00:00".into(),
        partition_to: "2023-11-14 23:00:00".into(),
        chain: None,
    };
    assert!(resolve_partition_window_bounds_from_index(&window, None)
        .unwrap_err()
        .to_string()
        .contains("non-contiguous"));
    window.partition_to = "2023-11-15 00:00:00".into();
    let bounds = resolve_partition_window_bounds_from_index(&window, None).unwrap();
    assert_eq!(
        (
            bounds.start_block,
            bounds.stop_block,
            bounds.partitions_count
        ),
        (10, 13, 3)
    );
    let report = validate_partitions_index(
        &PartitionValidateRequest {
            list: verified_list_request(&path),
            allow_gaps: false,
        },
        None,
    )
    .unwrap();
    assert!(report.valid);
    assert_eq!(report.incomplete_spans, 0);
    assert_eq!(report.unknown_spans, 0);
}

#[test]
fn incomplete_edges_remain_inspectable_but_never_resolve_or_shard() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("index.parquet");
    write_test_verified_partitions_index(&path, verified_test_rows()).unwrap();
    let mut index = read_verified_partitions_index(path.to_str().unwrap(), None).unwrap();
    index.spans[2].proof.end_complete = false;
    write_verified_partitions_index(
        path.to_str().unwrap(),
        &index,
        Compression::Zstd,
        None,
        None,
    )
    .unwrap();
    for all_spans in [false, true] {
        assert!(resolve_partition_command(
            verified_test_request(&path, "2023-11-14 22:00:00"),
            None,
            &PartitionResolveOptions {
                strict_single_chain: false,
                all_spans
            }
        )
        .unwrap_err()
        .to_string()
        .contains("incomplete"));
    }
    let list = verified_list_request(&path);
    let inspected = list_partitions_from_index(&list, None).unwrap();
    assert!(inspected.coverage.is_some());
    assert!(inspected
        .rows
        .iter()
        .any(|row| row.start_block == 12 && row.complete == Some(false)));
    assert!(shard_partitions_from_index(
        &PartitionShardRequest {
            list: list.clone(),
            shard_count: 2,
            shard_index: 0,
            strategy: PartitionShardStrategy::Ordinal
        },
        None
    )
    .unwrap_err()
    .to_string()
    .contains("incomplete"));
    let report = validate_partitions_index(
        &PartitionValidateRequest {
            list,
            allow_gaps: false,
        },
        None,
    )
    .unwrap();
    assert!(report.valid);
    assert_eq!(report.incomplete_spans, 1);
}

#[test]
fn legacy_completeness_is_unknown_for_inspection_and_refused_by_all_range_helpers() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("legacy.parquet");
    write_test_partitions_index(&path, verified_test_rows()).unwrap();
    let list = verified_list_request(&path);
    let inspected = list_partitions_from_index(&list, None).unwrap();
    assert_eq!(inspected.coverage, None);
    assert!(inspected.rows.iter().all(|row| row.complete.is_none()));
    assert!(resolve_partition_command(
        verified_test_request(&path, "2023-11-14 23:00:00"),
        None,
        &PartitionResolveOptions {
            strict_single_chain: false,
            all_spans: true
        }
    )
    .unwrap_err()
    .to_string()
    .contains("rebuild"));
    assert!(resolve_partition_bounds_from_index(
        &verified_test_request(&path, "2023-11-14 23:00:00"),
        None
    )
    .is_err());
    assert!(resolve_partition_window_bounds_from_index(
        &PartitionWindowRequest {
            index_path: path.to_string_lossy().into(),
            partition_type: "hour".into(),
            partition_from: "2023-11-14 22:00:00".into(),
            partition_to: "2023-11-15 00:00:00".into(),
            chain: None
        },
        None
    )
    .is_err());
    assert!(shard_partitions_from_index(
        &PartitionShardRequest {
            list: list.clone(),
            shard_count: 1,
            shard_index: 0,
            strategy: PartitionShardStrategy::Ordinal
        },
        None
    )
    .unwrap_err()
    .to_string()
    .contains("rebuild"));
    let report = validate_partitions_index(
        &PartitionValidateRequest {
            list,
            allow_gaps: false,
        },
        None,
    )
    .unwrap();
    assert_eq!(report.unknown_spans, 3);
    assert_eq!(report.coverage, None);
}

#[test]
fn numeric_ranges_refuse_unseen_routing_context_but_inspection_preserves_evidence() {
    use crate::grpc::FinalizedAnchor;
    use crate::partition_index::{
        ExactTimeIndexBuilder, IndexRoutingPolicy, SOLANA_GENESIS_TIMESTAMP,
    };
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("index.parquet");
    write_test_verified_partitions_index(&path, vec![verified_test_rows()[0].clone()]).unwrap();
    let mut index = read_verified_partitions_index(path.to_str().unwrap(), None).unwrap();
    index.coverage.routing_policy = IndexRoutingPolicy::SolanaPriorTimestamp;
    index.spans[0].row.start_time = None;
    write_verified_partitions_index(
        path.to_str().unwrap(),
        &index,
        Compression::Zstd,
        None,
        None,
    )
    .unwrap();
    let request = verified_test_request(&path, "2023-11-14 22:00:00");
    assert!(resolve_partition_bounds_from_index(&request, None)
        .unwrap_err()
        .to_string()
        .contains("unseen prior"));
    let all = resolve_partition_command(
        request,
        None,
        &PartitionResolveOptions {
            strict_single_chain: false,
            all_spans: true,
        },
    )
    .unwrap();
    assert!(all.spans.unwrap()[0].routing_context_required);
    assert!(shard_partitions_from_index(
        &PartitionShardRequest {
            list: verified_list_request(&path),
            shard_count: 1,
            shard_index: 0,
            strategy: PartitionShardStrategy::Ordinal
        },
        None
    )
    .unwrap_err()
    .to_string()
    .contains("unseen routing"));
    // Only actual block zero with its canonical genesis parent and shared seed is independent.
    let mut builder = ExactTimeIndexBuilder::new(
        "test-chain".into(),
        PartitionBuildType::Hour,
        0,
        1,
        FinalizedAnchor {
            block_num: 1,
            block_id: "id-1".into(),
        },
        IndexRoutingPolicy::SolanaPriorTimestamp,
        None,
    )
    .unwrap();
    builder
        .observe_final(
            &crate::traits::BlockIdentity {
                block_num: 0,
                block_id: "id-0".into(),
                ..Default::default()
            },
            3,
        )
        .unwrap();
    builder
        .observe_final(
            &crate::traits::BlockIdentity {
                block_num: 1,
                block_id: "id-1".into(),
                parent_num: 0,
                parent_id: "id-0".into(),
                timestamp: SOLANA_GENESIS_TIMESTAMP + 3_600,
                ..Default::default()
            },
            3,
        )
        .unwrap();
    let genesis = builder.finish().unwrap();
    let key = genesis.spans[0].row.partition_value.clone();
    write_verified_partitions_index(
        path.to_str().unwrap(),
        &genesis,
        Compression::Zstd,
        None,
        None,
    )
    .unwrap();
    let bounds =
        resolve_partition_bounds_from_index(&verified_test_request(&path, &key), None).unwrap();
    assert_eq!((bounds.start_block, bounds.stop_block), (0, 1));
}
#[test]
fn verified_reader_rejects_invalid_canonical_times_instead_of_nulling_them() {
    use arrow::{array::TimestampSecondArray, record_batch::RecordBatch};
    use parquet::{
        arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
        file::properties::WriterProperties,
    };
    use std::sync::Arc;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("index.parquet");
    write_test_verified_partitions_index(&path, verified_test_rows()).unwrap();
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap()).unwrap();
    let metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();
    let schema = reader.schema().clone();
    let batch = reader.build().unwrap().next().unwrap().unwrap();
    let mut columns = batch.columns().to_vec();
    columns[3] = Arc::new(
        TimestampSecondArray::from(vec![Some(i64::MAX); batch.num_rows()]).with_timezone("UTC"),
    );
    let bad = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(metadata)
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        schema,
        Some(properties),
    )
    .unwrap();
    writer.write(&bad).unwrap();
    writer.close().unwrap();
    assert!(read_verified_partitions_index(path.to_str().unwrap(), None).is_err());
    assert!(list_partitions_from_index(&verified_list_request(&path), None).is_err());
}
