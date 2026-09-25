use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;

use crate::config::Config;

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

/// Build an S3 client for the bucket selected by a data or cursor URI.
/// The configured default output bucket must not override an explicit cursor bucket.
pub fn build_s3_client(config: &Config, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
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
        builder = builder.with_endpoint(endpoint_url);
    }

    let client = builder
        .build()
        .with_context(|| format!("building S3 client for bucket {bucket}"))?;

    Ok(Arc::new(client))
}
