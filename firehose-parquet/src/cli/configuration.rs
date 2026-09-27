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

/// Environment variable naming an explicit env file (same as `--env-file`).
///
/// It is read from the process environment only; a key with this name inside
/// an env file is ignored.
pub const ENV_FILE_ENV_VAR: &str = "FIREPARQ_ENV_FILE";
/// Default env file name, looked up in the current working directory only.
pub const DEFAULT_ENV_FILE: &str = ".env";

/// Which env file was loaded at startup and which variable names it supplied.
/// Values are never retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvFileLoad {
    /// Absolute path of the loaded file.
    pub path: PathBuf,
    /// Selected with `--env-file` / `FIREPARQ_ENV_FILE` rather than `./.env`.
    pub explicit: bool,
    /// Variables set from the file, in file order.
    pub supplied: Vec<String>,
    /// Variables in the file that the process environment already defined
    /// (the process value wins), plus an ignored `FIREPARQ_ENV_FILE` key.
    pub ignored: Vec<String>,
}

impl EnvFileLoad {
    /// One-line, value-free description for startup logs.
    pub fn summary(&self) -> String {
        let names = |names: &[String]| {
            if names.is_empty() {
                "none".to_string()
            } else {
                names.join(", ")
            }
        };
        format!(
            "loaded env file {} ({}); supplied: {}; already set in the environment: {}",
            self.path.display(),
            if self.explicit {
                "--env-file / FIREPARQ_ENV_FILE"
            } else {
                "current directory"
            },
            names(&self.supplied),
            names(&self.ignored)
        )
    }
}

static ENV_FILE_LOAD: std::sync::OnceLock<Option<EnvFileLoad>> = std::sync::OnceLock::new();

/// The env file recorded by [`load_env_file`], if it has run and loaded one.
pub fn loaded_env_file() -> Option<&'static EnvFileLoad> {
    ENV_FILE_LOAD.get().and_then(Option::as_ref)
}

/// Find an explicit `--env-file PATH` / `--env-file=PATH` in raw arguments,
/// before clap parses them (clap `env` defaults must see the file's values).
/// Scanning stops at `--`.
pub fn explicit_env_file_arg<I, T>(args: I) -> anyhow::Result<Option<PathBuf>>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString>,
{
    let mut args = args.into_iter().map(Into::into).skip(1);
    let mut found = None;
    while let Some(arg) = args.next() {
        if arg == "--" {
            break;
        }
        if arg == "--env-file" {
            let value = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("--env-file requires a path"))?;
            found = Some(PathBuf::from(value));
        } else if let Some(value) = arg.to_str().and_then(|arg| arg.strip_prefix("--env-file=")) {
            found = Some(PathBuf::from(value));
        }
    }
    if found
        .as_ref()
        .is_some_and(|path| path.as_os_str().is_empty())
    {
        anyhow::bail!("--env-file requires a non-empty path");
    }
    Ok(found)
}

/// Load one env file without walking parent directories and record it.
///
/// An explicit `--env-file` in `args`, else a non-empty `FIREPARQ_ENV_FILE`,
/// selects the only file to load; it must exist. Otherwise `./.env` in the
/// current working directory is loaded when present. Parent directories are
/// never searched, so a run started below a checkout that holds a production
/// `.env` does not inherit its settings. Existing process variables win.
///
/// Call this **before** [`clap::Parser::parse`] so that `env` attributes on CLI
/// arguments pick up the values, then log [`EnvFileLoad::summary`].
pub fn load_env_file<I, T>(args: I) -> anyhow::Result<Option<EnvFileLoad>>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString>,
{
    let explicit = match explicit_env_file_arg(args)? {
        Some(path) => Some(path),
        None => std::env::var_os(ENV_FILE_ENV_VAR)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from),
    };
    let cwd = std::env::current_dir()
        .map_err(|error| anyhow::anyhow!("reading the current directory: {error}"))?;
    let loaded = load_env_file_with(
        explicit.as_deref(),
        &cwd,
        |name| std::env::var_os(name).is_some(),
        |name, value| std::env::set_var(name, value),
    )?;
    let _ = ENV_FILE_LOAD.set(loaded.clone());
    Ok(loaded)
}

/// Testable core of [`load_env_file`]: resolve against `cwd`, parse the whole
/// file before setting anything, and set only names `is_set` reports absent.
pub(crate) fn load_env_file_with(
    explicit: Option<&Path>,
    cwd: &Path,
    is_set: impl Fn(&str) -> bool,
    mut set: impl FnMut(&str, &str),
) -> anyhow::Result<Option<EnvFileLoad>> {
    let (path, is_explicit) = match explicit {
        Some(path) => (cwd.join(path), true),
        None => {
            let path = cwd.join(DEFAULT_ENV_FILE);
            if !path.is_file() {
                return Ok(None);
            }
            (path, false)
        }
    };
    let path = std::path::absolute(&path).unwrap_or(path);
    let entries = dotenvy::from_path_iter(&path)
        .map_err(|error| env_file_error(&path, error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| env_file_error(&path, error))?;
    let mut supplied = Vec::new();
    let mut ignored = Vec::new();
    for (name, value) in entries {
        if name == ENV_FILE_ENV_VAR || is_set(&name) || supplied.contains(&name) {
            if !ignored.contains(&name) && !supplied.contains(&name) {
                ignored.push(name);
            }
            continue;
        }
        set(&name, &value);
        supplied.push(name);
    }
    Ok(Some(EnvFileLoad {
        path,
        explicit: is_explicit,
        supplied,
        ignored,
    }))
}

/// Never echo a rejected line: env files hold secrets.
fn env_file_error(path: &Path, error: dotenvy::Error) -> anyhow::Error {
    match error {
        dotenvy::Error::LineParse(_, index) => anyhow::anyhow!(
            "invalid env file {}: cannot parse the line at byte offset {index}",
            path.display()
        ),
        dotenvy::Error::Io(error) => {
            anyhow::anyhow!("cannot read env file {}: {error}", path.display())
        }
        other => anyhow::anyhow!("cannot load env file {}: {other}", path.display()),
    }
}

/// Load `./.env` from the current working directory only, ignoring errors.
///
/// Retained for library callers; the `fireparq` binary uses [`load_env_file`],
/// which also honors `--env-file` / `FIREPARQ_ENV_FILE` and reports errors.
/// Unlike `dotenvy::dotenv`, parent directories are never searched.
pub fn load_dotenv() {
    let _ = load_env_file(std::iter::empty::<std::ffi::OsString>());
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

/// `--cursor` value that disables the optional cursor mirror (case-insensitive).
const CURSOR_MIRROR_DISABLED: &str = "none";

/// Resolve `--cursor` / `--cursor-template` into the configured mirror path.
///
/// `--cursor none` (any case) returns `None`: protected ingestion then keeps
/// only its mandatory output authority and writes no `cursor.parquet` mirror.
/// Every other value must be a `.parquet` path or `s3://` URI.
fn resolve_cursor_mirror_path(args: &CommonArgs) -> anyhow::Result<Option<String>> {
    let cursor_str = args.cursor.to_string_lossy();
    let template = normalize_opt_string(&args.cursor_template);
    if cursor_str
        .trim()
        .eq_ignore_ascii_case(CURSOR_MIRROR_DISABLED)
    {
        if let Some(template) = template {
            anyhow::bail!(
                "--cursor none disables the cursor mirror and cannot be combined with --cursor-template ({template})"
            );
        }
        return Ok(None);
    }
    if cursor_str.starts_with("s3://") {
        crate::writer::parse_s3_url(&cursor_str)?;
        validate_s3_output_credentials(
            &cursor_str,
            args.aws.aws_access_key_id.as_deref(),
            args.aws.aws_secret_access_key.as_deref(),
        )?;
    }
    if !cursor_str.ends_with(".parquet") {
        anyhow::bail!(
            "--cursor path must end in .parquet (or be `none` to disable the mirror), got: {cursor_str}"
        );
    }
    if let Some(template) = template {
        if !template.ends_with(".parquet") {
            anyhow::bail!("--cursor-template must end in .parquet, got: {template}");
        }
    }
    Ok(Some(cursor_str.into_owned()))
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

    let cursor_path = resolve_cursor_mirror_path(args)?;

    Ok(Config {
        endpoint,
        grpc: args.grpc.config(),
        api_key: credentials.api_key,
        jwt_token: credentials.jwt_token,
        start_block: args.start_block,
        stop_block: args.stop_block,
        cursor_path,
        output,
        partition: parse_partition(&args.partition, args.block_range_size)?,
        // 0 disables the row and interval triggers, matching --flush-bytes 0.
        flush_rows: args.flush_rows.filter(|rows| *rows > 0),
        flush_blocks: args.flush_blocks,
        flush_bytes: args.flush_bytes,
        flush_memory_bytes: args.flush_memory_bytes,
        flush_concurrency: crate::config::FlushConcurrency {
            encoders: args.flush_encode_concurrency,
            publications: args.flush_publish_concurrency,
            inflight_bytes: args.flush_inflight_bytes,
        },
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
    log_env_file_load();
}

/// Report the startup env file (names only) once tracing is available.
pub fn log_env_file_load() {
    match ENV_FILE_LOAD.get() {
        Some(Some(loaded)) => tracing::info!(
            env_file = %loaded.path.display(),
            explicit = loaded.explicit,
            supplied = %loaded.supplied.join(","),
            already_set = %loaded.ignored.join(","),
            "loaded env file"
        ),
        Some(None) => tracing::info!(
            "no env file loaded (./.env absent; parent directories are never searched)"
        ),
        None => {}
    }
}

/// Generate shell completions for the given command and write to stdout.
pub fn generate_completions<C: clap::CommandFactory>(shell: Shell) {
    let mut cmd = C::command();
    let name = cmd.get_name().to_string();
    generate(shell, &mut cmd, name, &mut io::stdout());
}
