use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::ObjectStore;

use crate::config::Config;

pub(crate) mod delete;

/// Reject ambiguous S3 output configuration before any network or storage work.
/// Explicit local output paths keep their local meaning even when S3_BUCKET is set.
pub fn validate_output_bucket(output: &str, configured_bucket: Option<&str>) -> Result<()> {
    if output.starts_with("s3://") {
        let (bucket, _) = crate::writer::parse_s3_url(output)?;
        if let Some(configured) = configured_bucket.map(str::trim).filter(|b| !b.is_empty()) {
            if configured != bucket {
                anyhow::bail!(
                    "S3 output bucket `{bucket}` disagrees with --s3-bucket / S3_BUCKET `{configured}`; \
                     use the same bucket or unset the bucket option"
                );
            }
        }
    }
    Ok(())
}

/// Resolved AWS settings shared by CLI commands and ingestion.
/// This retains the historical `cli::AwsConfig` re-export.
#[derive(Debug, Clone)]
pub struct AwsConfig {
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub aws_region: Option<String>,
    pub aws_endpoint_url: Option<String>,
}

impl From<&Config> for AwsConfig {
    fn from(config: &Config) -> Self {
        Self {
            aws_access_key_id: config.aws_access_key_id.clone(),
            aws_secret_access_key: config.aws_secret_access_key.clone(),
            aws_session_token: config.aws_session_token.clone(),
            aws_region: config.aws_region.clone(),
            aws_endpoint_url: config.aws_endpoint_url.clone(),
        }
    }
}

/// Retry behavior is selected by operation, independently of credentials.
#[derive(Clone, Copy)]
pub(crate) enum S3Operation {
    ReadOnly,
    Mutation,
}

/// Preserve the existing command-specific treatment of absent credentials.
#[derive(Clone, Copy)]
pub(crate) enum CredentialPolicy {
    ProviderChain,
    AnonymousWithoutAccessKey,
}

pub(crate) fn build_s3_store(
    config: &AwsConfig,
    bucket: &str,
    operation: S3Operation,
    credentials: CredentialPolicy,
) -> Result<AmazonS3> {
    store_builder(config, bucket, operation, credentials)?
        .build()
        .with_context(|| format!("building S3 client for bucket {bucket}"))
}

fn store_builder(
    config: &AwsConfig,
    bucket: &str,
    operation: S3Operation,
    credentials: CredentialPolicy,
) -> Result<AmazonS3Builder> {
    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
    if let Some(ref key) = config.aws_access_key_id {
        builder = builder.with_access_key_id(key);
    }
    if let Some(ref secret) = config.aws_secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }
    if let Some(ref token) = config.aws_session_token {
        builder = builder.with_token(token);
    }
    if let Some(ref region) = config.aws_region {
        builder = builder.with_region(region);
    }
    if let Some(ref endpoint_url) = config.aws_endpoint_url {
        builder = configure_endpoint(builder, endpoint_url, bucket)?;
    }
    if matches!(credentials, CredentialPolicy::AnonymousWithoutAccessKey)
        && config.aws_access_key_id.is_none()
    {
        builder = builder.with_skip_signature(true);
    }
    if matches!(operation, S3Operation::Mutation) {
        builder = without_mutation_retries(builder);
    }
    Ok(builder)
}

/// Build a zero-retry client for the exact data or cursor bucket. The configured
/// default output bucket never overrides the bucket selected by an explicit URI.
pub fn build_s3_client(config: &Config, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(build_s3_store(
        &AwsConfig::from(config),
        bucket,
        S3Operation::Mutation,
        CredentialPolicy::ProviderChain,
    )?))
}

#[cfg(test)]
fn mutation_builder(config: &Config, bucket: &str) -> Result<AmazonS3Builder> {
    store_builder(
        &AwsConfig::from(config),
        bucket,
        S3Operation::Mutation,
        CredentialPolicy::ProviderChain,
    )
}

impl AwsConfig {
    /// Read-only access keeps default transport retries. Missing access keys
    /// select anonymous requests, without metadata-provider credential lookup.
    pub fn build_s3_client(&self, bucket: &str) -> Result<AmazonS3> {
        build_s3_store(
            self,
            bucket,
            S3Operation::ReadOnly,
            CredentialPolicy::AnonymousWithoutAccessKey,
        )
    }

    /// PUT/DELETE clients make one transport attempt. Callers retain ownership
    /// when an error leaves a provider mutation uncertain.
    pub fn build_s3_client_for_mutation(&self, bucket: &str) -> Result<AmazonS3> {
        build_s3_store(
            self,
            bucket,
            S3Operation::Mutation,
            CredentialPolicy::AnonymousWithoutAccessKey,
        )
    }

    #[cfg(test)]
    pub(crate) fn s3_client_builder(
        &self,
        bucket: &str,
        mutation: bool,
    ) -> Result<AmazonS3Builder> {
        store_builder(
            self,
            bucket,
            if mutation {
                S3Operation::Mutation
            } else {
                S3Operation::ReadOnly
            },
            CredentialPolicy::AnonymousWithoutAccessKey,
        )
    }
}

/// One transport attempt for each mutation. This also disables read retries on
/// the same client; use the separate read builder for read-only operations.
pub(crate) fn without_mutation_retries(builder: AmazonS3Builder) -> AmazonS3Builder {
    builder.with_retry(object_store::RetryConfig {
        max_retries: 0,
        ..Default::default()
    })
}

/// Configure the addressing style from the endpoint's host, never its path/query.
/// Known bucket-bound AWS and Tigris endpoints may only serve that exact bucket.
/// Other custom endpoints must be service endpoints supporting path-style access.
pub(crate) fn configure_endpoint(
    builder: AmazonS3Builder,
    endpoint: &str,
    bucket: &str,
) -> Result<AmazonS3Builder> {
    Ok(builder
        .with_endpoint(endpoint)
        .with_virtual_hosted_style_request(endpoint_is_bucket_bound(endpoint, bucket)?))
}

/// Check a known bucket-bound endpoint before connecting or selecting credentials.
pub fn endpoint_is_bucket_bound(endpoint: &str, bucket: &str) -> Result<bool> {
    let uri: tonic::codegen::http::Uri = endpoint.parse().context("invalid AWS endpoint URL")?;
    let host = uri
        .host()
        .context("AWS endpoint URL must contain a host")?
        .to_ascii_lowercase();
    let bound_bucket = if let Some(prefix) = host.strip_suffix(".fly.storage.tigris.dev") {
        Some(prefix.to_string())
    } else if let Some(prefix) = host
        .strip_suffix(".amazonaws.com")
        .or_else(|| host.strip_suffix(".amazonaws.com.cn"))
    {
        let labels: Vec<_> = prefix.split('.').collect();
        labels
            .iter()
            .rposition(|label| {
                *label == "s3" || label.starts_with("s3-") || label.starts_with("s3express-")
            })
            .filter(|index| *index > 0)
            .map(|index| labels[..index].join("."))
    } else {
        None
    };
    if let Some(bound) = bound_bucket {
        anyhow::ensure!(
            bound == bucket,
            "AWS endpoint host `{host}` is bound to bucket `{bound}`, but the requested bucket is `{bucket}`; \
             use a service endpoint for multiple buckets, or omit the endpoint for standard AWS S3"
        );
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::signer::Signer;

    fn credentials(endpoint: &str) -> Config {
        Config {
            aws_access_key_id: Some("test-key".into()),
            aws_secret_access_key: Some("test-secret".into()),
            aws_region: Some("us-east-1".into()),
            aws_endpoint_url: Some(endpoint.into()),
            ..Default::default()
        }
    }

    fn maintenance_config(config: &Config) -> AwsConfig {
        AwsConfig {
            aws_access_key_id: config.aws_access_key_id.clone(),
            aws_secret_access_key: config.aws_secret_access_key.clone(),
            aws_session_token: None,
            aws_region: config.aws_region.clone(),
            aws_endpoint_url: config.aws_endpoint_url.clone(),
        }
    }

    #[tokio::test]
    async fn all_s3_access_modes_sign_the_correct_bucket_and_key() {
        for (endpoint, bucket, expected_host, expected_path) in [
            (
                "https://data.s3.amazonaws.com",
                "data",
                "data.s3.amazonaws.com",
                "/worker/c.parquet",
            ),
            (
                "https://data.s3.us-east-1.amazonaws.com",
                "data",
                "data.s3.us-east-1.amazonaws.com",
                "/worker/c.parquet",
            ),
            (
                "https://data.logs.s3.dualstack.us-east-1.amazonaws.com",
                "data.logs",
                "data.logs.s3.dualstack.us-east-1.amazonaws.com",
                "/worker/c.parquet",
            ),
            (
                "https://data.s3-accelerate.amazonaws.com",
                "data",
                "data.s3-accelerate.amazonaws.com",
                "/worker/c.parquet",
            ),
            (
                "https://data.s3-us-west-2.amazonaws.com",
                "data",
                "data.s3-us-west-2.amazonaws.com",
                "/worker/c.parquet",
            ),
            (
                "https://data.s3.cn-north-1.amazonaws.com.cn",
                "data",
                "data.s3.cn-north-1.amazonaws.com.cn",
                "/worker/c.parquet",
            ),
            (
                "https://data.fly.storage.tigris.dev",
                "data",
                "data.fly.storage.tigris.dev",
                "/worker/c.parquet",
            ),
            (
                "https://s3.us-east-1.amazonaws.com",
                "data",
                "s3.us-east-1.amazonaws.com",
                "/data/worker/c.parquet",
            ),
            (
                "https://s3.us-east-1.amazonaws.com",
                "state",
                "s3.us-east-1.amazonaws.com",
                "/state/worker/c.parquet",
            ),
            (
                "https://storage.example.com",
                "state",
                "storage.example.com",
                "/state/worker/c.parquet",
            ),
        ] {
            let config = credentials(endpoint);
            let stores = [
                mutation_builder(&config, bucket).unwrap().build().unwrap(),
                build_s3_store(
                    &AwsConfig::from(&config),
                    bucket,
                    S3Operation::ReadOnly,
                    CredentialPolicy::ProviderChain,
                )
                .unwrap(),
                maintenance_config(&config).build_s3_client(bucket).unwrap(),
                maintenance_config(&config)
                    .build_s3_client_for_mutation(bucket)
                    .unwrap(),
            ];
            for store in stores {
                let url = store
                    .signed_url(
                        "PUT".parse().unwrap(),
                        &object_store::path::Path::from("worker/c.parquet"),
                        std::time::Duration::from_secs(60),
                    )
                    .await
                    .unwrap();
                assert_eq!(url.host_str(), Some(expected_host));
                assert_eq!(url.path(), expected_path);
            }
        }
    }

    #[test]
    fn all_s3_access_modes_reject_a_different_bucket_on_bound_endpoints() {
        for endpoint in [
            "https://data.s3.amazonaws.com",
            "https://data.s3.us-east-1.amazonaws.com",
            "https://data.logs.s3.dualstack.us-east-1.amazonaws.com",
            "https://data.s3-accelerate.amazonaws.com",
            "https://data.s3-us-west-2.amazonaws.com",
            "https://data.s3.cn-north-1.amazonaws.com.cn",
            "https://data.fly.storage.tigris.dev",
        ] {
            let config = credentials(endpoint);
            for error in [
                build_s3_client(&config, "state").unwrap_err(),
                build_s3_store(
                    &AwsConfig::from(&config),
                    "state",
                    S3Operation::ReadOnly,
                    CredentialPolicy::ProviderChain,
                )
                .unwrap_err(),
                maintenance_config(&config)
                    .build_s3_client("state")
                    .unwrap_err(),
                maintenance_config(&config)
                    .build_s3_client_for_mutation("state")
                    .unwrap_err(),
            ] {
                assert!(
                    error.to_string().contains("requested bucket is `state`"),
                    "{error}"
                );
            }
        }
    }

    #[test]
    fn endpoint_binding_checks_host_components_only() {
        for endpoint in [
            "https://data.fly.storage.tigris.dev.example.com",
            "https://data.s3.amazonaws.com.example.com",
            "https://storage.example.com/data.fly.storage.tigris.dev",
            "https://storage.example.com/?host=data.s3.amazonaws.com",
            "https://s3.dualstack.us-east-1.amazonaws.com",
            "https://fly.storage.tigris.dev",
        ] {
            assert!(!endpoint_is_bucket_bound(endpoint, "state").unwrap());
        }
    }
}

#[cfg(test)]
mod mutation_tests;
