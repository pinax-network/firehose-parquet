//! Local/S3 path policy, the `--output` dataset root and cursor-template resolution.
use super::*;

/// Resolve the output root of a command that writes (`build`, `partitions build`).
///
/// The result is still the `--output` template: [`resolve_output_root`]
/// expands its `{chain}` placeholder once EndpointInfo names the chain. The
/// template itself is checked here, before any endpoint request
/// ([`validate_output_template`]).
///
/// S3 writes require an explicit `s3://bucket/prefix`. A relative path is never
/// expanded into `--s3-bucket` / `S3_BUCKET`: an inherited bucket setting must
/// not turn a local-looking path into a remote write. When a bucket is set, a
/// relative output (including the default `.`) or a missing output is rejected
/// as ambiguous; write `./path` or an absolute path for local output. The bucket
/// option otherwise only checks that an explicit S3 output names the same bucket.
pub fn resolve_s3_output_root(
    output: Option<&str>,
    s3_bucket: Option<&str>,
) -> anyhow::Result<String> {
    if let Some(output) = output {
        validate_output_template(output.trim())?;
        crate::s3::validate_output_bucket(output.trim(), s3_bucket)?;
    }
    match (
        output.map(str::trim).filter(|value| !value.is_empty()),
        s3_bucket.map(str::trim).filter(|value| !value.is_empty()),
    ) {
        (Some(output), _) if output.starts_with("s3://") => Ok(output.to_string()),
        (Some(output), _) if is_explicit_local_output_path(output) => Ok(output.to_string()),
        (Some(output), None) => Ok(output.to_string()),
        (Some(output), Some(bucket)) => {
            let prefix = output.trim_start_matches("./").trim_start_matches('/');
            // `.` (the `build` default) names the current directory: suggest an explicit
            // form of it rather than `./.`.
            let (suggestion, local) = if prefix.is_empty() || prefix == "." {
                (
                    format!("s3://{bucket}/<prefix>"),
                    "--output \"$(pwd)\" for the current directory or a subdirectory such as \
                     ./output"
                        .to_string(),
                )
            } else {
                (
                    format!("s3://{bucket}/{prefix}"),
                    format!("--output ./{prefix} or an absolute path"),
                )
            };
            anyhow::bail!(
                "output `{output}` is a relative path while --s3-bucket / S3_BUCKET is set to \
                 `{bucket}`; writes no longer expand relative paths into S3_BUCKET. For S3 \
                 output pass --output {suggestion} (OUTPUT=s3://...); for local output pass \
                 {local}, or unset S3_BUCKET"
            )
        }
        (None, Some(bucket)) => anyhow::bail!(
            "--output is required: writes no longer default to --s3-bucket / S3_BUCKET; \
             pass --output s3://{bucket}/<prefix> for S3 or a local path"
        ),
        (None, None) => anyhow::bail!("--output is required"),
    }
}

/// Fail when a write destination came only from the read-only `S3_BUCKET`
/// shorthand, i.e. `original` is not an explicit `s3://` URI but resolved to
/// one. `writes` says whether this invocation writes at all.
pub fn reject_implicit_s3_write(
    command: &str,
    original: &str,
    resolved: &str,
    writes: bool,
) -> anyhow::Result<()> {
    if writes && resolved.starts_with("s3://") && !original.trim().starts_with("s3://") {
        anyhow::bail!(
            "{command} writes artifacts or takes dataset ownership next to its data, but \
             `{original}` was resolved to {resolved} only through the read-only S3_BUCKET \
             shorthand. Pass the S3 URI explicitly ({resolved}) to write there"
        );
    }
    Ok(())
}

/// The `--output` variable of `build` and `partitions build`: the endpoint's
/// canonical `chain_name`.
pub const OUTPUT_CHAIN_VARIABLE: &str = "chain";

/// Check an `--output` template of `build` / `partitions build` before any
/// endpoint request: the brace syntax shared with `--cursor-template`, the
/// variable names (only `{chain}`), and a literal S3 bucket.
pub fn validate_output_template(output: &str) -> anyhow::Result<()> {
    output_template_parts(output).map(|_| ())
}

fn output_template_parts(output: &str) -> anyhow::Result<Vec<TemplatePart>> {
    let parts = parse_template(output, "--output")?;
    if let Some(name) = parts.iter().find_map(|part| match part {
        TemplatePart::Variable(name) if name != OUTPUT_CHAIN_VARIABLE => Some(name),
        _ => None,
    }) {
        anyhow::bail!(
            "unknown --output variable {{{name}}} in `{output}`; the only variable is \
             {{{OUTPUT_CHAIN_VARIABLE}}} (the endpoint's chain_name). Write {{{{ and }}}} for \
             literal braces"
        );
    }
    // The bucket is taken literally: credentials, the S3_BUCKET check, the
    // endpoint's bucket binding and the bucket-wide owner record are all
    // settled per bucket before the endpoint is contacted.
    if let Some(rest) = output.strip_prefix("s3://") {
        let bucket = rest.split('/').next().unwrap_or_default();
        if bucket.contains(['{', '}']) {
            anyhow::bail!(
                "--output `{output}` has a placeholder or brace in its S3 bucket name; the \
                 bucket is used literally, so put {{{OUTPUT_CHAIN_VARIABLE}}} in the key \
                 prefix, for example s3://<bucket>/{{{OUTPUT_CHAIN_VARIABLE}}}"
            );
        }
    }
    Ok(parts)
}

/// A chain name that `{chain}` may expand to: one path segment of ASCII
/// letters, digits, `-`, `_` and `.`, other than `.` and `..`. Every built-in
/// network name qualifies; anything else could change the directory level or
/// turn a local path into a URI.
fn validate_output_chain_segment(chain_name: &str) -> anyhow::Result<()> {
    let valid = !matches!(chain_name, "." | "..")
        && chain_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'));
    anyhow::ensure!(
        valid,
        "EndpointInfo chain_name `{}` cannot expand {{{OUTPUT_CHAIN_VARIABLE}}} in --output: it \
         must be one path segment of ASCII letters, digits, `-`, `_` and `.`",
        chain_name.escape_debug()
    );
    Ok(())
}

/// The dataset root of `build` and `partitions build`: the one place where
/// `--output` becomes the directory or `s3://bucket[/prefix]` that holds the
/// table directories, `_fireparq/` and the protected control state.
///
/// - `--output` is the dataset root exactly as given. No `<chain_name>`
///   directory is appended.
/// - `{chain}` expands to the endpoint's canonical `chain_name`, in any
///   position of the path or S3 key prefix: `s3://datasets/{chain}`,
///   `s3://datasets/v1/{chain}/raw`, `./data/{chain}-final`. `{{` and `}}` are
///   literal braces, as in `--cursor-template`. The S3 bucket name is literal.
/// - An S3 root loses its trailing `/` separators, so `s3://bucket/` and
///   `s3://bucket` are the same bucket root. A local root is returned as given.
///
/// `chain_name` comes from EndpointInfo and must be nonempty even when the
/// template does not use it: it is still recorded in file metadata and in the
/// protected dataset identity. The cursor mirror, the ownership scopes,
/// recovery and `partitions build` (its index and the sibling cursor mirror)
/// all resolve against the returned root.
pub fn resolve_output_root(output: &str, chain_name: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        !chain_name.trim().is_empty(),
        "EndpointInfo with a nonempty chain_name is required before resolving output and cursor paths"
    );
    let mut root = String::with_capacity(output.len() + chain_name.len());
    for part in output_template_parts(output)? {
        match part {
            TemplatePart::Literal(text) => root.push_str(&text),
            TemplatePart::Variable(_) => {
                validate_output_chain_segment(chain_name)?;
                root.push_str(chain_name);
            }
        }
    }
    if let Some(rest) = root.strip_prefix("s3://") {
        root = format!("s3://{}", rest.trim_end_matches('/'));
        crate::writer::parse_s3_url(&root)
            .map_err(|error| anyhow::anyhow!("--output `{output}`: {error}"))?;
    }
    anyhow::ensure!(!root.trim().is_empty(), "--output is required");
    Ok(root)
}

/// Human-readable absolute destination for startup logs: S3 URIs as given,
/// local paths made absolute against the current directory (not canonicalized,
/// so a path that does not exist yet is still reported).
pub fn display_destination(path: &str) -> String {
    if path.starts_with("s3://") {
        return path.to_string();
    }
    std::path::absolute(path)
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| path.to_string())
}

/// Destination of the cursor mirror under `output`, with the same placement as
/// [`crate::cursor::CursorLocation::resolve`], for startup logs.
pub fn display_cursor_destination(output: &str, cursor: &str) -> String {
    if cursor.starts_with("s3://") {
        return cursor.to_string();
    }
    if output.starts_with("s3://") && !Path::new(cursor).is_absolute() {
        let relative = cursor.replace('\\', "/");
        return match crate::writer::parse_s3_url(output) {
            Ok((bucket, prefix)) if prefix.is_empty() => format!("s3://{bucket}/{relative}"),
            Ok((bucket, prefix)) => format!("s3://{bucket}/{prefix}/{relative}"),
            Err(_) => format!("{}/{relative}", output.trim_end_matches('/')),
        };
    }
    let path = Path::new(cursor);
    if path.is_absolute() {
        display_destination(cursor)
    } else {
        display_destination(&Path::new(output).join(path).to_string_lossy())
    }
}

pub(in crate::cli) fn is_explicit_local_output_path(output: &str) -> bool {
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
pub(in crate::cli) enum ParquetInputPath {
    Local(PathBuf),
    S3(String),
}

pub(in crate::cli) fn configured_s3_bucket() -> Option<String> {
    std::env::var("S3_BUCKET")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(in crate::cli) fn shorthand_s3_key(path: &str) -> Option<String> {
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

pub(in crate::cli) fn resolve_parquet_input_path(path: &str) -> ParquetInputPath {
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

/// Resolves the path argument of a command that deletes or rewrites files (`truncate`,
/// `merge`, `rollup`).
///
/// Unlike [`resolve_parquet_input_path_string`], a relative path that does not exist locally
/// is never turned into `s3://$S3_BUCKET/<path>` (with `.env` auto-loaded, a typo would
/// otherwise target a bucket). S3 must be requested with an explicit `s3://` URI.
pub fn resolve_destructive_input_path(path: &str) -> anyhow::Result<String> {
    if path.starts_with("s3://") || Path::new(path).exists() {
        return Ok(path.to_string());
    }
    match resolve_parquet_input_path(path) {
        ParquetInputPath::S3(url) => anyhow::bail!(
            "path does not exist: {path}. Commands that delete or rewrite files do not fall \
             back to S3_BUCKET; to use S3, pass the URI explicitly: {url}"
        ),
        ParquetInputPath::Local(_) => anyhow::bail!("path does not exist: {path}"),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorTemplateContext {
    pub chain: Option<String>,
    pub partition_type: Option<String>,
    pub partition_value: Option<String>,
    pub partition_from: Option<String>,
    pub partition_to: Option<String>,
}

pub(in crate::cli) fn normalize_opt_string(value: &Option<String>) -> Option<String> {
    value
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub(in crate::cli) fn sanitize_cursor_template_value(value: &str) -> String {
    value.replace(['/', '\\'], "_")
}

pub(in crate::cli) fn cursor_template_value<'a>(
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
    for part in parse_template(template, "--cursor-template")? {
        match part {
            TemplatePart::Literal(text) => out.push_str(&text),
            TemplatePart::Variable(key) => {
                let value = cursor_template_value(&key, context)?;
                out.push_str(&sanitize_cursor_template_value(value));
            }
        }
    }
    Ok(out)
}

/// A piece of a `{variable}` template (`--cursor-template`, `--output`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::cli) enum TemplatePart {
    /// Literal text, with `{{` and `}}` already unescaped.
    Literal(String),
    /// The name inside a `{name}` reference.
    Variable(String),
}

/// Split a template into literal text and `{variable}` references: the syntax
/// shared by `--cursor-template` and `--output`. `{{` and `}}` are literal
/// braces; any other `{` opens a variable that the next `}` closes, and any
/// other `}` is an error. `flag` names the option in errors; the caller decides
/// which variable names exist.
pub(in crate::cli) fn parse_template(
    template: &str,
    flag: &str,
) -> anyhow::Result<Vec<TemplatePart>> {
    let mut parts = Vec::new();
    let mut literal = String::new();
    let mut chars = template.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                literal.push('{');
            }
            '{' => {
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
                    anyhow::bail!(
                        "unterminated {flag} variable in `{template}`; close it with }} or \
                         write {{{{ for a literal brace"
                    );
                }
                if !literal.is_empty() {
                    parts.push(TemplatePart::Literal(std::mem::take(&mut literal)));
                }
                parts.push(TemplatePart::Variable(key));
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                literal.push('}');
            }
            '}' => {
                anyhow::bail!("unmatched }} in {flag} `{template}`; write }}}} for a literal brace")
            }
            other => literal.push(other),
        }
    }
    if !literal.is_empty() {
        parts.push(TemplatePart::Literal(literal));
    }
    Ok(parts)
}
