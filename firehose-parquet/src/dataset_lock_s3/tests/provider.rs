//! Opt-in check of a real S3-compatible provider's conditional writes (#678).
//!
//! It runs only the ownership canary against the bucket
//! `FIREPARQ_S3_QUALIFY_BUCKET` below `FIREPARQ_S3_QUALIFY_PREFIX` at
//! `FIREPARQ_S3_QUALIFY_ENDPOINT`, then prints the `If-Match` ETag form it
//! chose. It creates, updates and deletes at most two probe objects
//! (`<prefix>/.fireparq-owner-probes-v1/<uuid>.json`), plus two more for the
//! per-form diagnosis when qualification fails. It never reads or writes an
//! owner record, control state or data. Credentials come from
//! `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, and
//! the region from `AWS_REGION` (default `us-east-1`).
//!
//! It is skipped without `FIREPARQ_S3_QUALIFY_ENDPOINT`, and always when `CI`
//! is set: CI never qualifies a real bucket. Use a disposable bucket or
//! prefix:
//!
//! ```sh
//! FIREPARQ_S3_QUALIFY_ENDPOINT=https://rgw.example.com \
//! FIREPARQ_S3_QUALIFY_BUCKET=scratch FIREPARQ_S3_QUALIFY_PREFIX=fireparq-qualify \
//! AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//! cargo test -p firehose-parquet --lib qualify_real_provider -- --nocapture
//! ```
use super::*;
use crate::s3::{build_s3_store, AwsConfig, CredentialPolicy, S3Operation};

#[tokio::test]
async fn qualify_real_provider_conditional_writes() {
    let Ok(endpoint) = std::env::var("FIREPARQ_S3_QUALIFY_ENDPOINT") else {
        eprintln!("skipped: set FIREPARQ_S3_QUALIFY_ENDPOINT to qualify a real provider");
        return;
    };
    if std::env::var_os("CI").is_some() {
        eprintln!("skipped: CI never qualifies a real bucket");
        return;
    }
    let bucket = std::env::var("FIREPARQ_S3_QUALIFY_BUCKET")
        .expect("FIREPARQ_S3_QUALIFY_BUCKET names the disposable bucket");
    let prefix = std::env::var("FIREPARQ_S3_QUALIFY_PREFIX")
        .expect("FIREPARQ_S3_QUALIFY_PREFIX names the prefix that holds the probes");
    let prefix = prefix.trim_matches('/').to_string();
    assert!(
        !prefix.is_empty(),
        "a non-empty FIREPARQ_S3_QUALIFY_PREFIX keeps the probes below the bucket root"
    );
    let config = AwsConfig {
        aws_access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
        aws_secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
        aws_session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        aws_region: Some(std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into())),
        aws_endpoint_url: Some(endpoint),
    };
    // The client `build` uses for its owner and state: one attempt per request.
    let client = build_s3_store(
        &config,
        &bucket,
        S3Operation::Mutation,
        CredentialPolicy::ProviderChain,
    )
    .expect("building the S3 client");
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        client,
        prefix.as_str(),
    ));
    match qualify_conditions(&store).await {
        Ok(form) => println!(
            "s3://{bucket}/{prefix}: conditional writes qualified, If-Match ETag form {form:?}"
        ),
        Err(error) => {
            // Diagnose each form on its own fresh probe.
            for form in [ETagForm::AsReturned, ETagForm::Unquoted] {
                println!(
                    "s3://{bucket}/{prefix}: canary with {form:?}: {:?}",
                    canary(&store, form).await
                );
            }
            panic!("s3://{bucket}/{prefix}: {error}");
        }
    }
}
