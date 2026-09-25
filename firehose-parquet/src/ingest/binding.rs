//! Resolve durable identities from actual storage configuration, never from
//! credentials or an unverified descriptor loaded from the destination.

use anyhow::{bail, ensure, Context, Result};
use std::path::{Component, Path, PathBuf};

use super::state::{Digest, MirrorBinding, StorageIdentity, StreamDescriptor};
use crate::cli::AwsConfig;

/// This identity follows the same explicit endpoint, region and bucket-style
/// rules as both production S3 builders. It intentionally does not equate DNS
/// aliases or different regional/global endpoints: changing those requires a
/// separately qualified migration, not an implicit append to another service.
pub(crate) fn resolve_service_identity(aws: &AwsConfig, bucket: &str) -> Result<Digest> {
    validate_bucket(bucket)?;
    let region = aws.aws_region.as_deref().unwrap_or("us-east-1");
    ensure!(
        !region.is_empty()
            && region.len() <= 255
            && region
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid S3 service region"
    );
    let default_endpoint = format!("https://s3.{region}.amazonaws.com");
    let endpoint = aws.aws_endpoint_url.as_deref().unwrap_or(&default_endpoint);
    // Reject rather than persist or echo authentication-bearing endpoint data.
    ensure!(
        !endpoint.contains(['@', '?', '#', '\\', '%']) && endpoint.len() <= 4096,
        "protected S3 endpoints must not contain credentials, queries, fragments or encoded path components"
    );
    let uri: tonic::codegen::http::Uri = endpoint
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid protected S3 service endpoint"))?;
    ensure!(
        uri.scheme_str() == Some("https"),
        "protected S3 endpoints require HTTPS"
    );
    let mut host = uri
        .host()
        .context("protected S3 endpoint requires a host")?
        .to_ascii_lowercase();
    // The addressing validator preserves dotted bucket names and exact AWS /
    // Tigris suffixes. A bound endpoint for another bucket fails before I/O.
    if crate::s3::endpoint_is_bucket_bound(endpoint, bucket)
        .map_err(|_| anyhow::anyhow!("S3 endpoint is incompatible with the requested bucket"))?
    {
        host = host
            .strip_prefix(&format!("{bucket}."))
            .context("invalid bucket-bound S3 endpoint")?
            .to_string();
    }
    let port = match uri.port_u16() {
        None | Some(443) => None,
        other => other,
    };
    let path = uri.path().trim_end_matches('/');
    ensure!(
        path.is_empty()
            || (path.starts_with('/')
                && !path[1..]
                    .split('/')
                    .any(|part| part.is_empty() || matches!(part, "." | ".."))),
        "protected S3 endpoint has an ambiguous service path"
    );
    Digest::hash("s3-service-v1", &("https", host, port, path, region))
}

pub(crate) fn resolve_output_identity(path: &str, aws: &AwsConfig) -> Result<StorageIdentity> {
    let identity = if path.starts_with("s3://") {
        let (bucket, prefix) = parse_remote(path, true)?;
        StorageIdentity::S3 {
            service: resolve_service_identity(aws, &bucket)?,
            bucket,
            prefix,
        }
    } else {
        StorageIdentity::Local {
            canonical_root: canonical_directory(Path::new(path))?
                .to_str()
                .context("output path must be UTF-8")?
                .to_string(),
        }
    };
    identity.validate()?;
    Ok(identity)
}

/// Match CursorLocation::resolve without constructing a second client. A local
/// alias keeps its lexical spelling so the mirror can sync both alias and target
/// parent links while DatasetOwnership guards their canonical scopes.
pub(crate) fn resolve_mirror_binding(
    output: &str,
    cursor: Option<&str>,
    aws: &AwsConfig,
) -> Result<MirrorBinding> {
    let Some(cursor) = cursor else {
        return Ok(MirrorBinding::Disabled);
    };
    let binding = if cursor.starts_with("s3://") {
        let (bucket, key) = parse_remote(cursor, false)?;
        MirrorBinding::S3 {
            service: resolve_service_identity(aws, &bucket)?,
            bucket,
            key,
        }
    } else if output.starts_with("s3://") {
        ensure!(
            !Path::new(cursor).is_absolute(),
            "S3 output cannot use an absolute local cursor path"
        );
        let (bucket, prefix) = parse_remote(output, true)?;
        let relative = cursor.replace('\\', "/");
        super::state::validate_relative_path(&relative, false)?;
        let key = if prefix.is_empty() {
            relative
        } else {
            format!("{prefix}/{relative}")
        };
        MirrorBinding::S3 {
            service: resolve_service_identity(aws, &bucket)?,
            bucket,
            key,
        }
    } else {
        let cursor = Path::new(cursor);
        let path = if cursor.is_absolute() {
            cursor.to_path_buf()
        } else {
            Path::new(output).join(cursor)
        };
        MirrorBinding::Local {
            absolute_path: lexical_absolute(&path)?
                .to_str()
                .context("cursor path must be UTF-8")?
                .to_string(),
        }
    };
    binding.validate()?;
    Ok(binding)
}

pub(crate) fn validate_runtime_bindings(
    descriptor: &StreamDescriptor,
    output_path: &str,
    aws: &AwsConfig,
) -> Result<()> {
    descriptor.validate()?;
    ensure!(
        descriptor.output == resolve_output_identity(output_path, aws)?,
        "authoritative output binding differs from the selected storage destination"
    );
    if let MirrorBinding::S3 {
        service, bucket, ..
    } = &descriptor.mirror
    {
        ensure!(
            *service == resolve_service_identity(aws, bucket)?,
            "authoritative cursor service differs from the resolved storage service"
        );
    }
    Ok(())
}

pub(crate) fn mirror_service(binding: &MirrorBinding, aws: &AwsConfig) -> Result<Option<Digest>> {
    match binding {
        MirrorBinding::S3 { bucket, .. } => Ok(Some(resolve_service_identity(aws, bucket)?)),
        _ => Ok(None),
    }
}

pub(crate) fn output_path(identity: &StorageIdentity) -> String {
    match identity {
        StorageIdentity::Local { canonical_root } => canonical_root.clone(),
        StorageIdentity::S3 { bucket, prefix, .. } if prefix.is_empty() => format!("s3://{bucket}"),
        StorageIdentity::S3 { bucket, prefix, .. } => format!("s3://{bucket}/{prefix}"),
    }
}

fn validate_bucket(bucket: &str) -> Result<()> {
    ensure!(
        !bucket.is_empty()
            && bucket.len() <= 255
            && bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'.'))
            && !matches!(bucket, "." | ".."),
        "invalid protected S3 bucket name"
    );
    Ok(())
}
fn parse_remote(path: &str, empty_allowed: bool) -> Result<(String, String)> {
    let (bucket, key) = crate::writer::parse_s3_url(path)
        .map_err(|_| anyhow::anyhow!("invalid protected S3 path"))?;
    validate_bucket(&bucket)?;
    super::state::validate_relative_path(&key, empty_allowed)?;
    ensure!(
        !path.ends_with("//"),
        "ambiguous protected S3 path separators"
    );
    Ok((bucket, key))
}

fn lexical_absolute(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                bail!("protected storage paths must not contain parent traversal")
            }
            Component::CurDir => {}
            other => result.push(other.as_os_str()),
        }
    }
    Ok(result)
}

/// Canonicalize the existing prefix without creating an ineligible destination.
pub(crate) fn canonical_directory(path: &Path) -> Result<PathBuf> {
    let path = lexical_absolute(path)?;
    let mut existing = path.as_path();
    loop {
        match std::fs::canonicalize(existing) {
            Ok(mut canonical) => {
                ensure!(
                    canonical.is_dir(),
                    "protected output ancestor is not a directory"
                );
                let suffix = path.strip_prefix(existing)?;
                if !suffix.as_os_str().is_empty() {
                    canonical.push(suffix);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                existing = existing
                    .parent()
                    .context("protected output has no existing ancestor")?;
            }
            Err(_) => bail!("cannot resolve protected output ancestry"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aws(endpoint: Option<&str>) -> AwsConfig {
        AwsConfig {
            aws_endpoint_url: endpoint.map(str::to_string),
            aws_access_key_id: None,
            aws_secret_access_key: None,
            aws_session_token: None,
            aws_region: None,
        }
    }

    #[test]
    fn service_identity_uses_actual_endpoint_and_ignores_credentials() {
        let mut config = aws(None);
        let expected = resolve_service_identity(&config, "data").unwrap();
        config.aws_access_key_id = Some("private-key".into());
        config.aws_secret_access_key = Some("private-secret".into());
        config.aws_session_token = Some("private-token".into());
        assert_eq!(expected, resolve_service_identity(&config, "data").unwrap());
        config.aws_endpoint_url = Some("https://s3.us-east-1.amazonaws.com:443/".into());
        assert_eq!(
            expected,
            resolve_service_identity(&config, "state").unwrap()
        );
        config.aws_endpoint_url = Some("https://data.logs.s3.us-east-1.amazonaws.com".into());
        assert_eq!(
            expected,
            resolve_service_identity(&config, "data.logs").unwrap()
        );
        assert!(resolve_service_identity(&config, "other").is_err());
        for endpoint in [
            "https://storage.example",
            "https://s3.us-west-2.amazonaws.com",
            "https://s3.us-east-1.amazonaws.com/api",
        ] {
            assert_ne!(
                expected,
                resolve_service_identity(&aws(Some(endpoint)), "data").unwrap()
            );
        }
    }

    #[test]
    fn bound_hosts_and_service_hosts_agree_without_loose_suffix_matching() {
        for service in [
            "s3.amazonaws.com",
            "s3.dualstack.us-east-1.amazonaws.com",
            "s3-accelerate.amazonaws.com",
            "s3.cn-north-1.amazonaws.com.cn",
            "fly.storage.tigris.dev",
        ] {
            let bound = aws(Some(&format!("https://data.logs.{service}")));
            let plain = aws(Some(&format!("https://{service}")));
            assert_eq!(
                resolve_service_identity(&bound, "data.logs").unwrap(),
                resolve_service_identity(&plain, "data.logs").unwrap()
            );
        }
        let fake = aws(Some("https://data.fly.storage.tigris.dev.example.com"));
        assert_ne!(
            resolve_service_identity(&fake, "data").unwrap(),
            resolve_service_identity(&aws(Some("https://fly.storage.tigris.dev")), "data").unwrap()
        );
    }

    #[test]
    fn unsafe_endpoint_components_are_rejected_without_echoing_input() {
        for endpoint in [
            "https://private-token@host",
            "https://host/?private-token",
            "https://host/#private-token",
            "https://host/%2e%2e/private-token",
            "https://host/a/../private-token",
            "http://host/private-token",
        ] {
            let error = resolve_service_identity(&aws(Some(endpoint)), "data")
                .unwrap_err()
                .to_string();
            assert!(!error.contains("private-token"));
        }
    }

    #[test]
    fn mirror_and_output_paths_preserve_independent_buckets_and_local_overrides() {
        let config = aws(None);
        let output = resolve_output_identity("s3://data/chain/", &config).unwrap();
        assert_eq!(output_path(&output), "s3://data/chain");
        let remote = resolve_mirror_binding(
            "s3://data/chain",
            Some("s3://state/external/cursor.parquet"),
            &config,
        )
        .unwrap();
        assert!(
            matches!(remote, MirrorBinding::S3 { bucket, key, .. } if bucket=="state" && key=="external/cursor.parquet")
        );
        let relative =
            resolve_mirror_binding("s3://data/chain", Some("cursor.parquet"), &config).unwrap();
        assert!(
            matches!(relative, MirrorBinding::S3 { bucket, key, .. } if bucket=="data" && key=="chain/cursor.parquet")
        );
        let root = tempfile::tempdir().unwrap();
        let local = resolve_mirror_binding(
            root.path().to_str().unwrap(),
            Some("cursor.parquet"),
            &config,
        )
        .unwrap();
        assert!(
            matches!(local, MirrorBinding::Local { absolute_path } if absolute_path==root.path().join("cursor.parquet").to_str().unwrap())
        );
        assert!(matches!(
            resolve_mirror_binding("s3://data/chain", None, &config).unwrap(),
            MirrorBinding::Disabled
        ));
        for invalid in [
            "s3://data/a//b",
            "s3://data/../b",
            "s3://data/key?secret",
            "s3://data/root//",
        ] {
            assert!(resolve_output_identity(invalid, &config).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn canonical_output_keeps_missing_descendants_and_lexical_mirror_alias() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        std::fs::create_dir(&target).unwrap();
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&target, &alias).unwrap();
        let output = alias.join("new/chain");
        let identity = resolve_output_identity(output.to_str().unwrap(), &aws(None)).unwrap();
        assert!(
            matches!(&identity,StorageIdentity::Local{canonical_root} if Path::new(&canonical_root)==std::fs::canonicalize(target).unwrap().join("new/chain"))
        );
        assert!(!output.exists());
        let identity_again = resolve_output_identity(&output_path(&identity), &aws(None)).unwrap();
        assert!(identity == identity_again);
        let mirror =
            resolve_mirror_binding(output.to_str().unwrap(), Some("cursor.parquet"), &aws(None))
                .unwrap();
        assert!(
            matches!(mirror,MirrorBinding::Local{absolute_path} if Path::new(&absolute_path)==output.join("cursor.parquet"))
        );
        assert!(canonical_directory(&alias.join("../different")).is_err());
    }
}
