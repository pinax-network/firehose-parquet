//! Local/S3 path policy and cursor-template resolution.
use super::*;

pub fn resolve_s3_output_root(
    output: Option<&str>,
    s3_bucket: Option<&str>,
) -> anyhow::Result<String> {
    if let Some(output) = output {
        crate::s3::validate_output_bucket(output.trim(), s3_bucket)?;
    }
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
