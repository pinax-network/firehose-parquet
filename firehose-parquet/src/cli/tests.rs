use super::*;
use crate::config::Compression;
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
        args.extend(["--endpoint", "http://localhost:9000"]);
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
fn grpc_transport_flags_validate_limits_and_apply_to_build() {
    let _adaptive = EnvVarGuard::set("GRPC_ADAPTIVE_WINDOW", "false");
    let _window = EnvVarGuard::set("GRPC_WINDOW_BYTES", "16777216");
    let _limit = EnvVarGuard::set("GRPC_MAX_MESSAGE_BYTES", "134217728");
    let parsed = parse(&["test-cli", "--endpoint", "http://localhost"]);
    let default = build_config(&parsed.common).unwrap();
    assert_eq!(default.grpc, crate::config::GrpcConfig::default());
    let parsed = try_parse(&[
        "test-cli",
        "--endpoint",
        "http://localhost",
        "--grpc-adaptive-window=false",
        "--grpc-max-message-bytes",
        "268435456",
    ])
    .unwrap();
    let grpc = build_config(&parsed.common).unwrap().grpc;
    assert!(!grpc.adaptive_window);
    assert_eq!(grpc.max_message_bytes, 268435456);
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
fn shared_aws_args_preserve_env_precedence() {
    use crate::recovery::RecoveryCommands;
    let _key = EnvVarGuard::set("AWS_ACCESS_KEY_ID", "synthetic-env-key");
    let _secret = EnvVarGuard::set("AWS_SECRET_ACCESS_KEY", "synthetic-env-secret");
    let _token = EnvVarGuard::set("AWS_SESSION_TOKEN", "synthetic-env-token");
    let _region = EnvVarGuard::set("AWS_REGION", "synthetic-env-region");
    let _endpoint = EnvVarGuard::set("AWS_ENDPOINT_URL_S3", "https://ordinary.example");
    // Recovery's legacy name never overrides AWS_ENDPOINT_URL_S3.
    let _legacy = EnvVarGuard::set("AWS_ENDPOINT_URL", "https://legacy.example");
    for base in [
        vec!["test-cli"],
        vec!["test-cli", "inspect", "fixture.parquet"],
        vec!["test-cli", "recovery", "status", "fixture"],
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
            let config = match cli.command {
                None => AwsConfig::from(&cli.common.aws),
                Some(Commands::Inspect { aws, .. }) => AwsConfig::from(&aws),
                Some(Commands::Recovery(RecoveryCommands::Status(storage))) => storage.aws(),
                _ => panic!("unexpected fixture command"),
            };
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
                    "https://ordinary.example"
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

/// `recovery` reads `AWS_ENDPOINT_URL_S3` like every other command. Its former
/// `AWS_ENDPOINT_URL` is a fallback used only when `AWS_ENDPOINT_URL_S3` is unset
/// (not merely empty) and no `--aws-endpoint-url` is given.
#[test]
#[serial]
fn recovery_endpoint_reads_aws_endpoint_url_s3_then_the_legacy_name() {
    use crate::recovery::{RecoveryCommands, RecoveryStorageArgs};
    fn storage(args: &[&str]) -> RecoveryStorageArgs {
        match try_parse(args).unwrap().command {
            Some(Commands::Recovery(RecoveryCommands::Status(storage)))
            | Some(Commands::Recovery(RecoveryCommands::Recover(storage))) => storage,
            Some(Commands::Recovery(RecoveryCommands::Release(release))) => release.storage,
            _ => panic!("unexpected fixture command"),
        }
    }
    let commands: [&[&str]; 3] = [
        &["test-cli", "recovery", "status", "fixture"],
        &["test-cli", "recovery", "recover", "fixture"],
        &[
            "test-cli",
            "recovery",
            "release",
            "fixture",
            "--expected-owner",
            "owner",
            "--expected-generation",
            "1",
            "--stopped-writer-evidence",
            "stopped",
            "--provider-quiescence-evidence",
            "drained",
        ],
    ];
    let cases = [
        // (AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, --aws-endpoint-url, expected)
        (
            Some("https://s3.example"),
            Some("https://legacy.example"),
            false,
            Some("https://s3.example"),
        ),
        (
            Some("https://s3.example"),
            None,
            false,
            Some("https://s3.example"),
        ),
        (
            None,
            Some("https://legacy.example"),
            false,
            Some("https://legacy.example"),
        ),
        (
            None,
            Some("https://legacy.example"),
            true,
            Some("https://explicit.example"),
        ),
        (Some(""), Some("https://legacy.example"), false, Some("")),
        (None, None, false, None),
    ];
    for (s3, legacy, explicit, expected) in cases {
        let _s3 = match s3 {
            Some(value) => EnvVarGuard::set("AWS_ENDPOINT_URL_S3", value),
            None => EnvVarGuard::remove("AWS_ENDPOINT_URL_S3"),
        };
        let _legacy = match legacy {
            Some(value) => EnvVarGuard::set("AWS_ENDPOINT_URL", value),
            None => EnvVarGuard::remove("AWS_ENDPOINT_URL"),
        };
        for command in commands {
            let mut args = command.to_vec();
            if explicit {
                args.extend(["--aws-endpoint-url", "https://explicit.example"]);
            }
            assert_eq!(
                storage(&args).aws().aws_endpoint_url.as_deref(),
                expected,
                "{args:?} with AWS_ENDPOINT_URL_S3={s3:?} AWS_ENDPOINT_URL={legacy:?}"
            );
        }
    }

    let mut command = TestCli::command();
    let status = command
        .find_subcommand_mut("recovery")
        .and_then(|recovery| recovery.find_subcommand_mut("status"))
        .expect("recovery status");
    let help = status.render_long_help().to_string();
    assert!(help.contains("AWS_ENDPOINT_URL_S3"), "{help}");
    let endpoint = status
        .get_arguments()
        .find(|arg| arg.get_id() == "aws_endpoint_url")
        .expect("endpoint argument");
    assert_eq!(
        endpoint.get_env(),
        Some(std::ffi::OsStr::new("AWS_ENDPOINT_URL_S3"))
    );
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
    // The default mirror lives in the dataset's `_fireparq/` directory.
    assert_eq!(cli.common.cursor, PathBuf::from("_fireparq/cursor.parquet"));
    assert_eq!(
        cli.common.cursor,
        PathBuf::from(crate::artifacts::DEFAULT_CURSOR_MIRROR)
    );
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
fn test_detect_partition_reads_the_date_directory() {
    assert_eq!(
        detect_partition(
            "/tmp/output/blocks/date=2026-01-15/part-000001.parquet",
            "/tmp/output"
        ),
        "date=2026-01-15"
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
    ]);
    let config = build_config(&cli.common).expect("build_config should succeed");
    assert_eq!(config.endpoint, "https://example.com:443");
    assert_eq!(config.start_block, Some(100));
    assert_eq!(config.compression, Compression::Gzip);
    assert!(config.flush_rows.is_none());
    assert!(config.flush_blocks.is_none());
    assert!(config.final_blocks_only);
    assert_eq!(config.stream_idle_timeout_secs, Some(120));
    assert_eq!(config.reconnect_stall_timeout_secs, Some(900));
    // cursor defaults to the mirror in the dataset's `_fireparq/` directory
    assert_eq!(
        config.cursor_path,
        Some("_fireparq/cursor.parquet".to_string())
    );
}

fn assert_rejected_value(args: &[&str], flag: &str) {
    let error = try_parse(args).expect_err("zero should be rejected");
    assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
    assert!(error.to_string().contains(flag), "{error}");
}

/// `build` writes every table as `<table>/date=YYYY-MM-DD/` (#652): the
/// `--partition` output flag, `--block-range-size` and `PARTITION` are gone.
#[test]
#[serial]
fn build_has_no_partition_mode() {
    for args in [
        &["test-cli", "--partition", "date"][..],
        &["test-cli", "--partition", "block_range"],
        &["test-cli", "--block-range-size", "1000"],
    ] {
        let error = try_parse(args).expect_err("output partition flags are removed");
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::UnknownArgument,
            "{args:?}"
        );
    }
    let _partition = EnvVarGuard::set("PARTITION", "hour");
    let cli = parse(&["test-cli", "--endpoint", "http://localhost:9000"]);
    let config = build_config(&cli.common).expect("PARTITION is not read");
    assert!(config.to_string().contains("partition          date"));
    let cli = TestCli::command();
    let build = cli.find_subcommand("build").expect("build subcommand");
    assert!(build
        .get_arguments()
        .all(|arg| arg.get_id() != "partition" && arg.get_id() != "block_range_size"));
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
fn test_flush_rows_and_interval_zero_mean_disabled() {
    // `--flush-rows 0` / `--flush-interval-secs 0` used to flush after every
    // block (`rows >= 0`, `elapsed >= 0`); zero now disables them like
    // `--flush-bytes 0` and merge `--flush-rows 0`.
    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "https://example.com:443",
        "--flush-rows",
        "0",
        "--flush-interval-secs",
        "0",
    ]);
    assert_eq!(cli.common.flush_rows, Some(0));
    assert_eq!(cli.common.flush_interval_secs, Some(0));
    let config = build_config(&cli.common).expect("build_config should succeed");
    assert_eq!(config.flush_rows, None);
    assert_eq!(config.flush_interval_secs, None);
    let rendered = config.to_string();
    assert!(!rendered.contains("flush_rows"), "{rendered}");
    assert!(!rendered.contains("flush_interval"), "{rendered}");

    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "https://example.com:443",
        "--flush-rows",
        "5",
        "--flush-interval-secs",
        "7",
    ]);
    let config = build_config(&cli.common).expect("build_config should succeed");
    assert_eq!(config.flush_rows, Some(5));
    assert_eq!(config.flush_interval_secs, Some(7));
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
fn test_cursor_none_disables_the_mirror_case_insensitively() {
    for value in ["none", "NONE", "None", " none "] {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--cursor",
            value,
        ]);
        let config = build_config(&cli.common).expect("--cursor none should be accepted");
        assert_eq!(config.cursor_path, None, "{value:?}");
    }
    // The environment form uses the same parser.
    let _cursor = EnvVarGuard::set("CURSOR", "None");
    let cli = parse(&["test-cli", "--endpoint", "https://example.com:443"]);
    assert_eq!(build_config(&cli.common).unwrap().cursor_path, None);
}

#[test]
#[serial]
fn test_cursor_none_rejects_near_misses() {
    for value in ["nothing", "none.txt", "no"] {
        let cli = parse(&[
            "test-cli",
            "--endpoint",
            "https://example.com:443",
            "--cursor",
            value,
        ]);
        let error = build_config(&cli.common).unwrap_err().to_string();
        assert!(error.contains(".parquet"), "{value}: {error}");
    }
    // A mirror file literally named none.parquet remains an ordinary path.
    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "https://example.com:443",
        "--cursor",
        "none.parquet",
    ]);
    assert_eq!(
        build_config(&cli.common).unwrap().cursor_path.as_deref(),
        Some("none.parquet")
    );
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
#[serial]
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
fn test_inspect_subcommand_schema_only_parse() {
    let cli = parse(&[
        "test-cli",
        "inspect",
        "s3://bucket/mainnet/part-000001.parquet",
        "--schema-only",
    ]);
    match cli.command.expect("command should exist") {
        Commands::Inspect {
            path,
            schema_only,
            json,
            ..
        } => {
            assert_eq!(path, "s3://bucket/mainnet/part-000001.parquet");
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
        "./output/mainnet/part-000001.parquet",
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
            assert_eq!(path, "./output/mainnet/part-000001.parquet");
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
        shorthand_s3_key(".//mainnet//part-000001.parquet"),
        Some("mainnet/part-000001.parquet".to_string())
    );
}

#[test]
fn test_shorthand_s3_key_rejects_empty_and_non_relative_paths() {
    assert_eq!(shorthand_s3_key(""), None);
    assert_eq!(shorthand_s3_key("./"), None);
    assert_eq!(shorthand_s3_key("../part-000001.parquet"), None);
    assert_eq!(shorthand_s3_key("/part-000001.parquet"), None);
}

#[test]
#[serial]
fn test_resolve_parquet_input_path_prefers_existing_local_relative_path() {
    let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
    let dir = tempfile::tempdir().expect("tempdir");
    let _cwd = CurrentDirGuard::set(dir.path());
    let local_path = dir.path().join("mainnet").join("part-000001.parquet");
    std::fs::create_dir_all(local_path.parent().expect("parent")).expect("create dir");
    std::fs::write(&local_path, b"not-a-real-parquet").expect("write file");

    let resolved = resolve_parquet_input_path("./mainnet/part-000001.parquet");

    assert_eq!(
        resolved,
        ParquetInputPath::Local(PathBuf::from("./mainnet/part-000001.parquet"))
    );
}

#[test]
#[serial]
fn test_resolve_parquet_input_path_falls_back_to_configured_s3_bucket() {
    let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
    let dir = tempfile::tempdir().expect("tempdir");
    let _cwd = CurrentDirGuard::set(dir.path());

    let resolved = resolve_parquet_input_path("./mainnet/part-000001.parquet");

    assert_eq!(
        resolved,
        ParquetInputPath::S3("s3://configured-bucket/mainnet/part-000001.parquet".to_string())
    );
}

#[test]
#[serial]
fn test_resolve_parquet_input_path_keeps_missing_local_path_without_bucket() {
    let _bucket = EnvVarGuard::remove("S3_BUCKET");
    let dir = tempfile::tempdir().expect("tempdir");
    let _cwd = CurrentDirGuard::set(dir.path());

    let resolved = resolve_parquet_input_path("./mainnet/part-000001.parquet");

    assert_eq!(
        resolved,
        ParquetInputPath::Local(PathBuf::from("./mainnet/part-000001.parquet"))
    );
}

#[test]
#[serial]
fn test_resolve_parquet_input_path_keeps_explicit_s3_uri() {
    let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");

    let resolved = resolve_parquet_input_path("s3://other-bucket/mainnet/part-000001.parquet");

    assert_eq!(
        resolved,
        ParquetInputPath::S3("s3://other-bucket/mainnet/part-000001.parquet".to_string())
    );
}

#[test]
#[serial]
fn test_resolve_parquet_input_path_does_not_rewrite_missing_absolute_paths() {
    let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");

    let resolved = resolve_parquet_input_path("/definitely/missing/part-000001.parquet");

    assert_eq!(
        resolved,
        ParquetInputPath::Local(PathBuf::from("/definitely/missing/part-000001.parquet"))
    );
}

#[test]
#[serial]
fn test_resolve_parquet_input_path_does_not_rewrite_parent_relative_paths() {
    let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
    let dir = tempfile::tempdir().expect("tempdir");
    let _cwd = CurrentDirGuard::set(dir.path());

    let resolved = resolve_parquet_input_path("./../../part-000001.parquet");

    assert_eq!(
        resolved,
        ParquetInputPath::Local(PathBuf::from("./../../part-000001.parquet"))
    );
}

#[test]
#[serial]
fn test_resolve_parquet_input_path_normalizes_redundant_current_dir_segments() {
    let _bucket = EnvVarGuard::set("S3_BUCKET", "configured-bucket");
    let dir = tempfile::tempdir().expect("tempdir");
    let _cwd = CurrentDirGuard::set(dir.path());

    let resolved = resolve_parquet_input_path(".//mainnet//part-000001.parquet");

    assert_eq!(
        resolved,
        ParquetInputPath::S3("s3://configured-bucket/mainnet/part-000001.parquet".to_string())
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

    // Read-only protocol verification keeps the shorthand.
    let protocol_only = crate::verify::VerifyOptions {
        checks: vec![crate::verify::VerifyCheck::Protocol],
        ..test_verify_options()
    };
    let verify_err = match crate::verify::verify_parquet("./mainnet/blocks/", None, &protocol_only)
    {
        Ok(_) => panic!("verify should resolve to S3 without a local path"),
        Err(err) => err,
    };
    assert!(verify_err
        .to_string()
        .contains("AWS config required for S3 paths"));

    // Runs that write roots or reports (and take dataset ownership) refuse a
    // destination that only the S3_BUCKET shorthand selected (#617).
    for opts in [
        test_verify_options(),
        crate::verify::VerifyOptions {
            checks: vec![crate::verify::VerifyCheck::Protocol],
            publish_report: true,
            ..test_verify_options()
        },
        crate::verify::VerifyOptions {
            checks: vec![crate::verify::VerifyCheck::Protocol],
            report_json: Some(PathBuf::from("report.json")),
            ..test_verify_options()
        },
    ] {
        let message = crate::verify::verify_parquet("./mainnet/blocks/", None, &opts)
            .expect_err("implicit S3 writes must be refused")
            .to_string();
        assert!(
            message.contains("read-only S3_BUCKET shorthand"),
            "{message}"
        );
        assert!(
            message.contains("s3://configured-bucket/mainnet/blocks"),
            "{message}"
        );
    }
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
}

/// `rollup` is removed (#652): with one `date` partition key there is no
/// coarser layout to roll up into.
#[test]
fn rollup_subcommand_is_removed() {
    let error = try_parse(&["test-cli", "rollup", "./output/blocks/"])
        .expect_err("rollup is not a subcommand");
    assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    assert!(TestCli::command().find_subcommand("rollup").is_none());
}

/// `partitions` and every subcommand are removed (#653): Delta log metadata
/// replaces `partitions.parquet`.
#[test]
fn partitions_subcommands_are_removed() {
    for subcommand in ["build", "ls", "validate", "resolve", "shard"] {
        let error = try_parse(&["test-cli", "partitions", subcommand])
            .expect_err("partitions is not a subcommand");
        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }
    assert!(TestCli::command().find_subcommand("partitions").is_none());
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
        "s3://my-bucket/my-prefix",
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
fn test_s3_bucket_never_expands_relative_output() {
    unsafe {
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_SESSION_TOKEN");
        std::env::remove_var("AWS_REGION");
        std::env::remove_var("AWS_ENDPOINT_URL_S3");
        std::env::remove_var("S3_BUCKET");
    }
    // #617: a relative output with a bucket set used to become
    // s3://bucket/output. Writes now require the explicit URI.
    let args = |output: &'static str| {
        parse(&[
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
            output,
        ])
    };
    for output in ["my-prefix", "target/test-output", "."] {
        let message = build_config(&args(output).common)
            .expect_err("relative output with a bucket must be rejected")
            .to_string();
        assert!(message.contains("relative path"), "{message}");
        assert!(message.contains("S3_BUCKET"), "{message}");
        assert!(message.contains("s3://my-bucket/"), "{message}");
    }
    let config = build_config(&args("s3://my-bucket/my-prefix").common)
        .expect("explicit S3 output should succeed");
    assert_eq!(config.output, PathBuf::from("s3://my-bucket/my-prefix"));
    assert_eq!(config.s3_bucket.as_deref(), Some("my-bucket"));
}

#[test]
#[serial]
fn test_s3_bucket_env_does_not_redirect_relative_build_output() {
    // The #617 incident: S3_BUCKET inherited from an env file plus a relative
    // --output must never produce an S3 destination.
    let _bucket = EnvVarGuard::set("S3_BUCKET", "production-bucket");
    let _key = EnvVarGuard::set("AWS_ACCESS_KEY_ID", "AKID123");
    let _secret = EnvVarGuard::set("AWS_SECRET_ACCESS_KEY", "secret456");
    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "https://example.com:443",
        "--output",
        "target/test-output",
    ]);
    assert_eq!(cli.common.s3_bucket.as_deref(), Some("production-bucket"));
    let message = build_config(&cli.common)
        .expect_err("inherited S3_BUCKET must not turn relative output into S3")
        .to_string();
    assert!(
        message.contains("s3://production-bucket/target/test-output"),
        "{message}"
    );
    assert!(message.contains("./target/test-output"), "{message}");
}

#[test]
#[serial]
fn test_relative_build_output_without_bucket_stays_local() {
    let _bucket = EnvVarGuard::remove("S3_BUCKET");
    let cli = parse(&[
        "test-cli",
        "--endpoint",
        "https://example.com:443",
        "--output",
        "target/test-output",
    ]);
    let config = build_config(&cli.common).expect("relative local output");
    assert_eq!(config.output, PathBuf::from("target/test-output"));
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
    // The default output `.` with a bucket used to mean the bucket root; it is
    // now rejected rather than silently written locally or remotely (#617).
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
    let message = build_config(&cli.common)
        .expect_err("default output with a bucket is ambiguous")
        .to_string();
    assert!(
        message.contains("output `.` is a relative path"),
        "{message}"
    );
    assert!(message.contains("s3://my-bucket/<prefix>"), "{message}");
}

#[test]
fn test_common_args_reject_removed_partition_and_cursor_template_flags() {
    for (flag, value) in [
        ("--partitions-index", "./partitions.parquet"),
        ("--partition-from", "2015-07-30 15:00:00"),
        ("--partition-to", "2015-07-30 18:00:00"),
        ("--cursor-template", "cursor/{chain}.parquet"),
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
        .map(|r| {
            (
                r.block_num,
                r.timestamp_ms,
                r.prev_block_num,
                r.prev_timestamp_ms,
            )
        })
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
            [(3, 1_690_815_595_000, 2, 1_690_815_600_000)],
            "{label}"
        );
        assert_eq!(result.total_blocks, 4, "{label}");
    }
}

#[test]
fn test_validate_parquet_reports_sub_second_timestamp_reversal() {
    use arrow::array::{
        ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use std::sync::Arc;

    // Canonical millisecond timestamps: .900 then .100 within the same second is a
    // reversal that whole-second comparison would miss.
    let millis = [
        1_690_815_600_000_i64,
        1_690_815_600_900,
        1_690_815_600_100,
        1_690_815_601_000,
    ];
    let scaled = |factor: i64| millis.iter().map(|ms| ms * factor).collect::<Vec<_>>();
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "timestamp_millisecond_utc",
            Arc::new(TimestampMillisecondArray::from(scaled(1)).with_timezone("UTC")),
        ),
        (
            "timestamp_microsecond_utc",
            Arc::new(TimestampMicrosecondArray::from(scaled(1_000)).with_timezone("UTC")),
        ),
        (
            "timestamp_nanosecond",
            Arc::new(TimestampNanosecondArray::from(scaled(1_000_000))),
        ),
    ];
    for (label, column) in columns {
        let dir = tempfile::tempdir().expect("tempdir");
        write_validate_blocks_file(&dir.path().join("blocks.parquet"), column);
        let result = validate_local(dir.path());
        assert_eq!(
            reversal_summary(&result.timestamp_reversals),
            [(3, 1_690_815_600_100, 2, 1_690_815_600_900)],
            "{label}"
        );
    }

    // Second-precision data with equal and increasing times stays clean, and its
    // values are compared (and reported) in milliseconds.
    let dir = tempfile::tempdir().expect("tempdir");
    write_validate_blocks_file(
        &dir.path().join("blocks.parquet"),
        Arc::new(
            TimestampSecondArray::from(vec![
                1_690_815_600,
                1_690_815_600,
                1_690_815_601,
                1_690_815_612,
            ])
            .with_timezone("UTC"),
        ),
    );
    let result = validate_local(dir.path());
    assert!(result.timestamp_reversals.is_empty());
    assert!(result.is_valid());

    assert_eq!(
        super::validate::format_epoch_millis(1_690_815_600_100),
        "2023-07-31 15:00:00.100"
    );
    assert_eq!(
        super::validate::format_epoch_millis(-1),
        "1969-12-31 23:59:59.999"
    );
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
        [(4, 1_690_815_590_000, 2, 1_690_815_600_000)]
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
                vec![(2, 1_690_815_580_000, 1, 1_690_815_590_000)]
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
fn test_resolve_s3_output_root_requires_explicit_output_with_bucket() {
    let message = resolve_s3_output_root(None, Some("bucket-name"))
        .expect_err("a bucket alone no longer selects the output")
        .to_string();
    assert!(message.contains("--output is required"), "{message}");
    assert!(message.contains("s3://bucket-name/<prefix>"), "{message}");
}

#[test]
fn test_resolve_s3_output_root_rejects_implicit_relative_output() {
    for output in ["output", ".", "a/b"] {
        let message = resolve_s3_output_root(Some(output), Some("bucket-name"))
            .expect_err("relative output with a bucket is ambiguous")
            .to_string();
        assert!(message.contains("relative path"), "{output}: {message}");
    }
    // Without a bucket a relative output is local, and explicit forms are kept.
    assert_eq!(
        resolve_s3_output_root(Some("output"), None).unwrap(),
        "output"
    );
    for output in ["./output", "../output", "/abs/output", "s3://bucket-name/p"] {
        assert_eq!(
            resolve_s3_output_root(Some(output), Some("bucket-name")).unwrap(),
            output
        );
    }
}

/// The default output `.` gets natural local suggestions (never `./.`); other
/// relative outputs keep their own name in both suggestions.
#[test]
fn test_resolve_s3_output_root_suggests_natural_local_paths() {
    let message = resolve_s3_output_root(Some("."), Some("bucket-name"))
        .expect_err("`.` with a bucket is ambiguous")
        .to_string();
    assert!(
        message.contains("output `.` is a relative path"),
        "{message}"
    );
    assert!(message.contains("s3://bucket-name/<prefix>"), "{message}");
    assert!(
        message.contains("--output \"$(pwd)\" for the current directory"),
        "{message}"
    );
    assert!(message.contains("such as ./output"), "{message}");
    assert!(!message.contains("./."), "{message}");

    let message = resolve_s3_output_root(Some("data/eth"), Some("bucket-name"))
        .expect_err("relative output with a bucket is ambiguous")
        .to_string();
    assert!(message.contains("s3://bucket-name/data/eth"), "{message}");
    assert!(
        message.contains("--output ./data/eth or an absolute path"),
        "{message}"
    );
}

#[test]
fn test_resolve_s3_output_root_requires_output() {
    let err = resolve_s3_output_root(None, None).expect_err("missing output should fail");
    assert!(err.to_string().contains("--output is required"));
}

#[test]
// Reads the process cwd, which the `CurrentDirGuard` tests change.
#[serial]
fn test_write_destinations_are_absolute_for_logs() {
    let cwd = std::env::current_dir().unwrap();
    assert_eq!(display_destination("s3://b/p"), "s3://b/p");
    assert_eq!(
        display_destination("out/data"),
        cwd.join("out/data").to_string_lossy()
    );
    assert_eq!(
        display_cursor_destination("s3://b/p", "cursor.parquet"),
        "s3://b/p/cursor.parquet"
    );
    assert_eq!(
        display_cursor_destination("s3://b", "w/cursor.parquet"),
        "s3://b/w/cursor.parquet"
    );
    assert_eq!(
        display_cursor_destination("out", "cursor.parquet"),
        cwd.join("out/cursor.parquet").to_string_lossy()
    );
    assert_eq!(
        display_cursor_destination("out", "s3://c/cursor.parquet"),
        "s3://c/cursor.parquet"
    );
    assert_eq!(
        display_cursor_destination("s3://b/p", "/abs/cursor.parquet"),
        "/abs/cursor.parquet"
    );
}

#[test]
fn test_collect_scan_s3_parquet_objects_treats_exact_file_as_single_object() {
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    let store = InMemory::new();
    let location = Path::from("mainnet/part-000001.parquet");
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
        "mainnet/part-000001.parquet",
    ))
    .expect("collect objects");

    assert!(exact_object_path);
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].location.as_ref(), "mainnet/part-000001.parquet");
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
            "mainnet/part-000001.parquet",
            "mainnet/part-000001.parquet",
            true
        ),
        "mainnet/part-000001.parquet"
    );
}

/// Read-only commands on a dataset written with `--output s3://<bucket>` to a
/// bucket root: the registry is an exact object in `_fireparq/`, a table is a
/// prefix directly below the bucket, and a scan of the whole bucket reads only
/// table data.
#[test]
fn test_collect_scan_s3_parquet_objects_at_a_bucket_root_dataset() {
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    let store = InMemory::new();
    for key in [
        "_fireparq/cursor.parquet",
        "_fireparq/merkle_roots.parquet",
        "_fireparq/verify_runs/run-1/roots.parquet",
        // Legacy root artifacts of a release before v1.0.0.
        "merkle_roots.parquet",
        "cursor.parquet",
        "blocks/date=2023-11-14/part-v1-a.parquet",
        "blocks-archive/part-v1-b.parquet",
        ".fireparq-ingest/state.json",
        ".fireparq-ingest/hidden.parquet",
        ".fireparq-owner-probes-v1/probe.parquet",
    ] {
        block_on_async(store.put(
            &Path::from(key),
            object_store::PutPayload::from(Bytes::from_static(b"parquet")),
        ))
        .expect("put object");
    }
    let (objects, exact) = block_on_async(collect_scan_s3_parquet_objects(
        &store,
        "_fireparq/merkle_roots.parquet",
    ))
    .expect("collect registry");
    assert!(exact);
    assert_eq!(objects.len(), 1);
    assert_eq!(
        scan_s3_display_key(
            objects[0].location.as_ref(),
            "_fireparq/merkle_roots.parquet",
            exact
        ),
        "_fireparq/merkle_roots.parquet"
    );
    let (objects, exact) =
        block_on_async(collect_scan_s3_parquet_objects(&store, "blocks")).expect("collect table");
    assert!(!exact);
    let keys: Vec<_> = objects
        .iter()
        .map(|object| scan_s3_display_key(object.location.as_ref(), "blocks", exact))
        .collect();
    assert_eq!(keys, ["date=2023-11-14/part-v1-a.parquet"]);

    let keys = |prefix: &str| -> Vec<String> {
        block_on_async(collect_scan_s3_parquet_objects(&store, prefix))
            .expect("collect prefix")
            .0
            .iter()
            .map(|object| object.location.to_string())
            .collect()
    };
    // The whole bucket: tables only.
    assert_eq!(
        keys(""),
        [
            "blocks-archive/part-v1-b.parquet",
            "blocks/date=2023-11-14/part-v1-a.parquet",
        ]
    );
    // Asking for the artifact directory itself lists its files.
    assert_eq!(
        keys("_fireparq"),
        [
            "_fireparq/cursor.parquet",
            "_fireparq/merkle_roots.parquet",
            "_fireparq/verify_runs/run-1/roots.parquet",
        ]
    );
}

/// A local directory scan or validate skips `_fireparq/`, the legacy root
/// artifacts and control state below it, unless the scanned directory is
/// `_fireparq/` itself.
#[test]
fn test_local_directory_walks_skip_dataset_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("mainnet");
    let files = [
        "_fireparq/merkle_roots.parquet",
        "_fireparq/cursor.parquet",
        "_fireparq/verify_runs/run-1/roots.parquet",
        "cursor.parquet",
        "merkle_roots.parquet",
        ".fireparq-ingest/hidden.parquet",
        "blocks/date=2024-01-14/part-v1-a.parquet",
    ];
    for file in files {
        let path = root.join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"parquet").unwrap();
    }
    let walk = |dir: &std::path::Path| {
        let mut found = Vec::new();
        crate::maintenance::discovery::collect_local(
            dir,
            crate::maintenance::discovery::LocalPolicy::PARQUET,
            &mut found,
        )
        .unwrap();
        super::inspect::retain_table_files_local(dir, &mut found);
        let mut found: Vec<String> = found
            .iter()
            .map(|file| {
                file.strip_prefix(dir)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        found.sort();
        found
    };
    assert_eq!(walk(&root), ["blocks/date=2024-01-14/part-v1-a.parquet"]);
    assert_eq!(
        walk(&root.join("_fireparq")),
        [
            "cursor.parquet",
            "merkle_roots.parquet",
            "verify_runs/run-1/roots.parquet"
        ]
    );
}

/// Without a placeholder, `--output` is the dataset root byte for byte: no
/// `<chain_name>` directory is appended, locally or on S3. The artifacts sit
/// below the same root.
#[test]
fn test_output_root_default_is_byte_identical_to_output() {
    for output in [
        ".",
        "output",
        "./output",
        "./output/",
        "../output",
        "/data/output",
        "/data/output/",
        "./out put/mainnet-v1",
        "s3://ethereum-mainnet",
        "s3://bucket/v1",
        "s3://bucket/v1/raw",
    ] {
        let root = resolve_output_root(output, "mainnet").unwrap();
        assert_eq!(root, output, "{output}");
        assert_eq!(
            crate::artifacts::DatasetArtifact::CursorMirror.path_in(&root),
            crate::artifacts::DatasetArtifact::CursorMirror.path_in(output)
        );
        // The chain name is not needed by the template, so any nonempty
        // EndpointInfo name leaves the root unchanged.
        assert_eq!(resolve_output_root(output, "a/b c").unwrap(), output);
    }
    assert_eq!(
        crate::artifacts::DatasetArtifact::MerkleRoots
            .path_in(&resolve_output_root("./output", "mainnet").unwrap()),
        "./output/_fireparq/merkle_roots.parquet"
    );
    assert_eq!(
        crate::artifacts::DatasetArtifact::CursorMirror
            .path_in(&resolve_output_root("s3://bucket", "mainnet").unwrap()),
        "s3://bucket/_fireparq/cursor.parquet"
    );
}

/// `s3://bucket/` and `s3://bucket` are the same bucket root, and a trailing
/// `/` never changes an S3 prefix root. Local roots keep their spelling.
#[test]
fn test_output_root_normalizes_s3_trailing_slashes() {
    for (output, root) in [
        ("s3://ethereum-mainnet/", "s3://ethereum-mainnet"),
        ("s3://ethereum-mainnet//", "s3://ethereum-mainnet"),
        ("s3://bucket/v1/", "s3://bucket/v1"),
        ("s3://bucket/{chain}/", "s3://bucket/mainnet"),
        ("./output/", "./output/"),
    ] {
        assert_eq!(resolve_output_root(output, "mainnet").unwrap(), root);
    }
    assert_eq!(
        crate::artifacts::DatasetArtifact::MerkleRoots
            .path_in(&resolve_output_root("s3://ethereum-mainnet/", "mainnet").unwrap()),
        "s3://ethereum-mainnet/_fireparq/merkle_roots.parquet"
    );
}

/// `{chain}` expands to the EndpointInfo chain name in any position of a local
/// path or an S3 key prefix, as many times as it appears.
#[test]
fn test_output_root_expands_the_chain_placeholder() {
    for (output, root) in [
        // Local: the whole path, prefix, middle, suffix and inside a segment.
        ("{chain}", "mainnet"),
        ("{chain}/raw", "mainnet/raw"),
        ("./output/{chain}", "./output/mainnet"),
        ("/data/{chain}/raw", "/data/mainnet/raw"),
        ("./output/{chain}-final", "./output/mainnet-final"),
        ("./{chain}/{chain}", "./mainnet/mainnet"),
        // S3: first key segment, middle, suffix and inside a segment.
        ("s3://datasets/{chain}", "s3://datasets/mainnet"),
        ("s3://datasets/{chain}/v1", "s3://datasets/mainnet/v1"),
        (
            "s3://datasets/v1/{chain}/raw",
            "s3://datasets/v1/mainnet/raw",
        ),
        ("s3://datasets/eth-{chain}", "s3://datasets/eth-mainnet"),
    ] {
        assert_eq!(
            resolve_output_root(output, "mainnet").unwrap(),
            root,
            "{output}"
        );
    }
    assert_eq!(
        resolve_output_root("s3://datasets/{chain}", "solana-mainnet-beta").unwrap(),
        "s3://datasets/solana-mainnet-beta"
    );
    assert_eq!(
        crate::artifacts::DatasetArtifact::MerkleRoots
            .path_in(&resolve_output_root("s3://datasets/{chain}", "mainnet").unwrap()),
        "s3://datasets/mainnet/_fireparq/merkle_roots.parquet"
    );
}

/// `{{` and `}}` are literal braces; an escaped `{{chain}}` is not a
/// placeholder.
#[test]
fn test_output_root_escapes_braces() {
    for (output, root) in [
        ("./output/{{chain}}", "./output/{chain}"),
        ("./output/{{{chain}}}", "./output/{mainnet}"),
        ("./a}}b{{c", "./a}b{c"),
        (
            "s3://bucket/{{chain}}/{chain}",
            "s3://bucket/{chain}/mainnet",
        ),
    ] {
        assert_eq!(
            resolve_output_root(output, "mainnet").unwrap(),
            root,
            "{output}"
        );
        validate_output_template(output).unwrap();
    }
}

/// Unknown variables, unterminated or unmatched braces, a placeholder or brace
/// in the S3 bucket name, a missing bucket and a chain name that is not one
/// safe path segment are errors. Template errors surface through
/// `resolve_s3_output_root`, before any endpoint request.
#[test]
fn test_output_root_template_errors() {
    for (output, expected) in [
        ("./output/{network}", "unknown --output variable {network}"),
        ("./output/{Chain}", "unknown --output variable {Chain}"),
        ("./output/{}", "unknown --output variable {}"),
        (
            "s3://bucket/{chain_name}",
            "unknown --output variable {chain_name}",
        ),
        ("./output/{chain", "unterminated --output variable"),
        ("s3://bucket/v1/{", "unterminated --output variable"),
        ("./output/chain}", "unmatched } in --output"),
        ("s3://bucket/}", "unmatched } in --output"),
        ("s3://{chain}", "S3 bucket name"),
        ("s3://{chain}/raw", "S3 bucket name"),
        ("s3://data-{chain}/raw", "S3 bucket name"),
        ("s3://{{data}}/raw", "S3 bucket name"),
    ] {
        let resolved = resolve_output_root(output, "mainnet")
            .unwrap_err()
            .to_string();
        assert!(resolved.contains(expected), "{output}: {resolved}");
        let early = resolve_s3_output_root(Some(output), None)
            .unwrap_err()
            .to_string();
        assert_eq!(early, resolved, "{output}");
        // An S3_BUCKET value never masks the template error.
        let with_bucket = resolve_s3_output_root(Some(output), Some("bucket"))
            .unwrap_err()
            .to_string();
        assert_eq!(with_bucket, resolved, "{output}");
    }
    let message = resolve_output_root("s3://{chain}/raw", "mainnet")
        .unwrap_err()
        .to_string();
    assert!(message.contains("s3://<bucket>/{chain}"), "{message}");

    for output in ["s3://", "s3:///raw", "s3:///{chain}"] {
        let error = resolve_output_root(output, "mainnet")
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing bucket name"), "{output}: {error}");
    }

    // EndpointInfo must name the chain, whether or not the template uses it.
    for output in ["./output", "./output/{chain}", "s3://bucket"] {
        for chain_name in ["", "  "] {
            let error = resolve_output_root(output, chain_name)
                .unwrap_err()
                .to_string();
            assert!(error.contains("nonempty chain_name"), "{output}: {error}");
        }
    }
    // {chain} only expands to one safe path segment.
    for chain_name in ["a/b", "a\\b", ".", "..", "s3:", "main net", "main\nnet"] {
        let error = resolve_output_root("./output/{chain}", chain_name)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("cannot expand {chain}"),
            "{chain_name:?}: {error}"
        );
    }
}

/// `resolve_s3_output_root` keeps the template for `resolve_output_root`: the
/// bucket is literal, so the S3_BUCKET check still applies before the endpoint
/// is contacted.
#[test]
fn test_resolve_s3_output_root_keeps_a_valid_output_template() {
    assert_eq!(
        resolve_s3_output_root(Some("s3://data/{chain}"), Some("data")).unwrap(),
        "s3://data/{chain}"
    );
    assert_eq!(
        resolve_s3_output_root(Some("./output/{chain}"), None).unwrap(),
        "./output/{chain}"
    );
    let error = resolve_s3_output_root(Some("s3://data/{chain}"), Some("other"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("disagrees with --s3-bucket"), "{error}");
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

fn env_file_load(
    explicit: Option<&Path>,
    cwd: &Path,
    preset: &[&str],
) -> anyhow::Result<(Option<EnvFileLoad>, Vec<(String, String)>)> {
    let mut set = Vec::new();
    let loaded = load_env_file_with(
        explicit,
        cwd,
        |name| preset.contains(&name),
        |name, value| set.push((name.to_string(), value.to_string())),
    )?;
    Ok((loaded, set))
}

#[test]
fn env_file_in_a_parent_directory_is_never_loaded() {
    // #617: a worktree below a checkout must not inherit the checkout's .env.
    let checkout = tempfile::tempdir().unwrap();
    std::fs::write(
        checkout.path().join(".env"),
        "S3_BUCKET=production-bucket\nAWS_SECRET_ACCESS_KEY=secret-value\n",
    )
    .unwrap();
    let worktree = checkout.path().join("worktree/nested");
    std::fs::create_dir_all(&worktree).unwrap();
    let (loaded, set) = env_file_load(None, &worktree, &[]).unwrap();
    assert!(loaded.is_none());
    assert!(set.is_empty());

    // The same file is loaded from the current directory.
    let (loaded, set) = env_file_load(None, checkout.path(), &[]).unwrap();
    let loaded = loaded.unwrap();
    assert!(!loaded.explicit);
    assert_eq!(loaded.path, checkout.path().join(".env"));
    assert_eq!(loaded.supplied, ["S3_BUCKET", "AWS_SECRET_ACCESS_KEY"]);
    assert_eq!(set.len(), 2);
    let summary = loaded.summary();
    assert!(
        summary.contains("S3_BUCKET, AWS_SECRET_ACCESS_KEY"),
        "{summary}"
    );
    assert!(!summary.contains("secret-value"), "{summary}");
    assert!(!summary.contains("production-bucket"), "{summary}");
}

#[test]
fn explicit_env_file_replaces_the_current_directory_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(".env"), "S3_BUCKET=cwd-bucket\n").unwrap();
    std::fs::write(
        dir.path().join("custom.env"),
        "OUTPUT=./data\nFIREPARQ_ENV_FILE=other.env\nLOG_LEVEL=debug\nOUTPUT=./second\n",
    )
    .unwrap();
    let (loaded, set) =
        env_file_load(Some(Path::new("custom.env")), dir.path(), &["LOG_LEVEL"]).unwrap();
    let loaded = loaded.unwrap();
    assert!(loaded.explicit);
    assert_eq!(loaded.path, dir.path().join("custom.env"));
    // First value wins, process variables win, and the selector key is ignored.
    assert_eq!(set, [("OUTPUT".to_string(), "./data".to_string())]);
    assert_eq!(loaded.supplied, ["OUTPUT"]);
    assert_eq!(loaded.ignored, ["FIREPARQ_ENV_FILE", "LOG_LEVEL"]);

    let missing = env_file_load(Some(Path::new("absent.env")), dir.path(), &[])
        .expect_err("an explicit env file must exist");
    assert!(missing.to_string().contains("absent.env"), "{missing}");
}

#[test]
fn env_file_parse_errors_never_echo_the_line() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join(".env"),
        "GOOD=1\nnot a valid line with secret-token\n",
    )
    .unwrap();
    let error = env_file_load(None, dir.path(), &[])
        .expect_err("a malformed env file must fail")
        .to_string();
    assert!(error.contains(".env"), "{error}");
    assert!(!error.contains("secret-token"), "{error}");
}

#[test]
fn explicit_env_file_argument_is_found_before_clap_parsing() {
    let parse = |args: &[&str]| explicit_env_file_arg(args.iter().copied());
    assert_eq!(parse(&["fireparq", "build"]).unwrap(), None);
    assert_eq!(
        parse(&["fireparq", "--env-file", "a.env", "build"]).unwrap(),
        Some(PathBuf::from("a.env"))
    );
    assert_eq!(
        parse(&["fireparq", "build", "--env-file=b.env"]).unwrap(),
        Some(PathBuf::from("b.env"))
    );
    assert_eq!(
        parse(&["fireparq", "scan", "--", "--env-file=c.env"]).unwrap(),
        None
    );
    assert!(parse(&["fireparq", "--env-file"]).is_err());
    assert!(parse(&["fireparq", "--env-file="]).is_err());
}
