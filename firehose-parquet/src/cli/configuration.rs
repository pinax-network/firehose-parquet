//! CLI configuration conversion, runtime utilities and logging.
use super::*;

/// Completion of a bounded reversible stream does not establish tail finality.
/// Pass the resolved stop bound (`None` for an unbounded live stream).
pub fn non_final_bounded_warning(
    final_blocks_only: bool,
    stop_block: Option<u64>,
) -> Option<&'static str> {
    (!final_blocks_only && stop_block.is_some()).then_some(
        "Completion of a bounded non-final stream does not prove its tail is final. \
         Output retains append-only NEW/UNDO events; later UNDO events cannot be received after this stop. \
         Use a separate final-only dataset to select finalized block identities.",
    )
}

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

/// Parse a compression string into a [`Compression`] variant.
pub fn parse_compression(s: &str) -> anyhow::Result<Compression> {
    use anyhow::Context;
    let normalized = s.to_lowercase();
    if let Some(level) = normalized.strip_prefix("zstd:") {
        let level: i32 = level.parse().context("zstd level must be an integer")?;
        // Keep zero unambiguous: zstd's library-dependent default is not a
        // reproducible level. The CLI's documented default is always 3.
        anyhow::ensure!(level != 0, "zstd level 0 is ambiguous; use zstd or zstd:3");
        let value = parquet::basic::ZstdLevel::try_new(level)?;
        return Ok(if level == 3 {
            Compression::Zstd
        } else {
            Compression::ZstdWithLevel(value)
        });
    }
    match normalized.as_str() {
        "zstd" => Ok(Compression::Zstd),
        "snappy" => Ok(Compression::Snappy),
        "gzip" => Ok(Compression::Gzip),
        "none" => Ok(Compression::None),
        other => anyhow::bail!(
            "invalid --compression '{other}': expected one of: zstd, zstd:<level>, snappy, gzip, none"
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

/// Build a [`Config`] from [`CommonArgs`].
///
/// Returns an error if `--endpoint` was not provided (required for pipeline execution).
pub fn build_config(args: &CommonArgs) -> anyhow::Result<Config> {
    let endpoint = args
        .endpoint
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--endpoint is required"))?;

    let credentials = crate::auth::resolve_credentials(
        &endpoint,
        args.api_key_envvar.as_deref(),
        args.api_token_envvar.as_deref(),
    )?;

    validate_stop_block_after_start(args.start_block, args.stop_block)?;

    // Only reinterpret output as S3 when it is not an explicit local path.
    let output = PathBuf::from(resolve_s3_output_root(
        Some(args.output.to_string_lossy().as_ref()),
        args.s3_bucket.as_deref(),
    )?);

    validate_s3_output_credentials(
        output.to_string_lossy().as_ref(),
        args.aws.aws_access_key_id.as_deref(),
        args.aws.aws_secret_access_key.as_deref(),
    )?;

    // Validate that the cursor path has a .parquet extension.
    let cursor_str = args.cursor.to_string_lossy();
    if cursor_str.starts_with("s3://") {
        crate::writer::parse_s3_url(&cursor_str)?;
        validate_s3_output_credentials(
            &cursor_str,
            args.aws.aws_access_key_id.as_deref(),
            args.aws.aws_secret_access_key.as_deref(),
        )?;
    }
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
        grpc: args.grpc.config(),
        api_key: credentials.api_key,
        jwt_token: credentials.jwt_token,
        start_block: args.start_block,
        stop_block: args.stop_block,
        skip_missing_blocks: true,
        cursor_path: Some(args.cursor.to_string_lossy().to_string()),
        output,
        partition: parse_partition(&args.partition, args.block_range_size)?,
        // 0 disables the row and interval triggers, matching --flush-bytes 0.
        flush_rows: args.flush_rows.filter(|rows| *rows > 0),
        flush_blocks: args.flush_blocks,
        flush_bytes: args.flush_bytes,
        flush_memory_bytes: args.flush_memory_bytes,
        flush_interval_secs: args.flush_interval_secs.filter(|secs| *secs > 0),
        compression: parse_compression(&args.compression)?,
        final_blocks_only: args.final_blocks_only,
        dry_run: args.dry_run,
        aws_access_key_id: args.aws.aws_access_key_id.clone(),
        aws_secret_access_key: args.aws.aws_secret_access_key.clone(),
        aws_session_token: args.aws.aws_session_token.clone(),
        aws_region: args.aws.aws_region.clone(),
        aws_endpoint_url: args.aws.aws_endpoint_url.clone(),
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
