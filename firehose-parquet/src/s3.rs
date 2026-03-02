use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;

use crate::config::Config;

/// Build an S3 `ObjectStore` client from pipeline config credentials.
pub fn build_s3_client(config: &Config) -> Result<Arc<dyn ObjectStore>> {
    let output = config.output.to_string_lossy();
    let bucket = if let Some(ref b) = config.s3_bucket {
        b.clone()
    } else if output.starts_with("s3://") {
        crate::writer::parse_s3_url(&output)?.0
    } else {
        anyhow::bail!("no S3 bucket configured");
    };

    let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);

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
