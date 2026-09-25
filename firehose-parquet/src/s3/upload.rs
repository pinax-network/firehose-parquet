//! Authenticated native uploads. The owner and signer share one AmazonS3 client.
//! Presigned URLs and HTTP errors deliberately never enter Debug/error chains.
use super::{store_builder, AwsConfig, CredentialPolicy, S3Operation};
use crate::dataset_lock_s3::usable_version;
use anyhow::{ensure, Context, Result};
use futures::StreamExt;
use object_store::{aws::AmazonS3, path::Path, signer::Signer, ObjectStore, UpdateVersion};
use reqwest::{header, Client, Method};
use std::{
    fs::File,
    io::{Seek, SeekFrom},
    sync::Arc,
    time::Duration,
};
use tokio_util::io::ReaderStream;

pub(crate) const MAX_PART_BYTES: u64 = 5_000_000_000;
pub(crate) const DATA_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SIGNED_URL_LIFETIME: Duration = Duration::from_secs(20 * 60);
pub(crate) const IO_BUFFER_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BODY: usize = 8 * 1024;
const MAX_RESPONSE_HEADERS: usize = 32 * 1024;

/// Constructed before ownership acquisition; never paired with an unrelated store.
#[derive(Clone)]
pub(crate) struct NativeS3Upload {
    store: Arc<AmazonS3>,
    http: Client,
}
impl NativeS3Upload {
    pub(crate) fn new(config: &AwsConfig, bucket: &str) -> Result<Self> {
        ensure!(
            config
                .aws_access_key_id
                .as_deref()
                .is_some_and(|s| !s.is_empty())
                && config
                    .aws_secret_access_key
                    .as_deref()
                    .is_some_and(|s| !s.is_empty()),
            "native streamed ingestion requires explicit S3 credentials"
        );
        // Reject unsupported native transport before any persistent owner or
        // canary is created. Ordinary maintenance clients retain their policy.
        if let Some(endpoint) = &config.aws_endpoint_url {
            let url = reqwest::Url::parse(endpoint)
                .map_err(|_| anyhow::anyhow!("invalid native ingestion S3 endpoint"))?;
            ensure!(
                url.scheme() == "https" && url.host_str().is_some(),
                "native streamed ingestion requires an HTTPS S3 endpoint"
            );
        }
        Self::configured(config, bucket, DATA_TIMEOUT, false)
    }

    fn configured(
        config: &AwsConfig,
        bucket: &str,
        timeout: Duration,
        allow_http: bool,
    ) -> Result<Self> {
        let http = Client::builder()
            .https_only(!allow_http)
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .retry(reqwest::retry::never())
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(timeout)
            .http2_max_header_list_size(MAX_RESPONSE_HEADERS as u32)
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .build()
            .map_err(|_| anyhow::anyhow!("building native upload transport failed"))?;
        let store = store_builder(
            config,
            bucket,
            S3Operation::Mutation,
            CredentialPolicy::ProviderChain,
        )?
        .with_client_options(
            object_store::ClientOptions::new()
                .with_allow_http(allow_http)
                .with_connect_timeout(CONNECT_TIMEOUT)
                .with_timeout(timeout),
        )
        // Validate singleton version headers before the SDK collapses them.
        // The same no-retry HTTP transport serves data and control requests.
        .with_http_connector(StrictConnector(http.clone()))
        .build()
        .map_err(|_| anyhow::anyhow!("building native ingestion S3 client failed"))?;
        Ok(Self {
            store: Arc::new(store),
            http,
        })
    }
    pub(crate) fn validate_cache_control(&self, cache_control: &str) -> Result<()> {
        header::HeaderValue::from_str(cache_control)
            .map_err(|_| anyhow::anyhow!("invalid S3 cache-control header"))?;
        Ok(())
    }
    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Signing and all local checks happen before the owner's mutation latch.
    pub(crate) async fn prepare(
        &self,
        key: &Path,
        mut file: File,
        size: u64,
        cache_control: &str,
    ) -> Result<PreparedUpload> {
        ensure!(
            size > 0 && size <= MAX_PART_BYTES,
            "native S3 part exceeds the single-PUT size limit"
        );
        let meta = file.metadata().context("inspecting upload spool")?;
        ensure!(
            meta.is_file() && meta.len() == size,
            "upload spool size changed"
        );
        file.seek(SeekFrom::Start(0))
            .context("rewinding upload spool")?;
        let mut headers = header::HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, header::HeaderValue::from_static("*"));
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/vnd.apache.parquet"),
        );
        headers.insert(
            header::CONTENT_LENGTH,
            header::HeaderValue::from_str(&size.to_string()).unwrap(),
        );
        if !cache_control.is_empty() {
            headers.insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_str(cache_control)
                    .map_err(|_| anyhow::anyhow!("invalid S3 cache-control header"))?,
            );
        }
        let url = tokio::time::timeout(
            CONNECT_TIMEOUT,
            self.store.signed_url(Method::PUT, key, SIGNED_URL_LIFETIME),
        )
        .await
        .map_err(|_| anyhow::anyhow!("signing native upload timed out"))?
        .map_err(|_| anyhow::anyhow!("signing native upload failed"))?;
        // Do not modify the returned host/path/query or add unsigned x-amz headers.
        let stream = ReaderStream::with_capacity(tokio::fs::File::from_std(file), IO_BUFFER_BYTES);
        let request = self
            .http
            .put(url)
            .headers(headers)
            .body(reqwest::Body::wrap_stream(stream))
            .build()
            .map_err(|_| anyhow::anyhow!("preparing native upload failed"))?;
        Ok(PreparedUpload {
            http: self.http.clone(),
            request,
        })
    }
}

pub(crate) struct PreparedUpload {
    http: Client,
    request: reqwest::Request,
}
impl PreparedUpload {
    /// Exactly one non-replayable send. The caller owns cancellation uncertainty.
    pub(crate) async fn send(self) -> Result<UpdateVersion> {
        let response = self.http.execute(self.request).await.map_err(|_| {
            anyhow::anyhow!(
                "native conditional upload failed; retain ownership for quiescent recovery"
            )
        })?;
        ensure!(
            response.status() == reqwest::StatusCode::OK,
            "native conditional upload returned HTTP {}; retain ownership",
            response.status().as_u16()
        );
        check_header_bound(response.headers())?;
        let version = response_version(response.headers())?;
        let mut stream = response.bytes_stream();
        let mut count = 0usize;
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|_| anyhow::anyhow!("reading native upload response failed"))?;
            count = count
                .checked_add(chunk.len())
                .context("native upload response size overflow")?;
            ensure!(
                count <= MAX_RESPONSE_BODY,
                "native upload response exceeds limit"
            );
        }
        Ok(version)
    }
}
fn check_header_bound(headers: &header::HeaderMap) -> Result<()> {
    let size = headers
        .iter()
        .try_fold(0usize, |total, (key, value)| {
            total
                .checked_add(key.as_str().len())?
                .checked_add(value.len())
        })
        .context("native response headers overflow")?;
    ensure!(
        size <= MAX_RESPONSE_HEADERS,
        "native response headers exceed limit"
    );
    Ok(())
}
fn response_version(headers: &header::HeaderMap) -> Result<UpdateVersion> {
    let version = UpdateVersion {
        e_tag: single_header(headers, header::ETAG.as_str())?,
        version: single_header(headers, "x-amz-version-id")?,
    };
    ensure!(
        usable_version(&version),
        "native response has no usable version"
    );
    Ok(version)
}

#[derive(Clone)]
struct StrictConnector(Client);
impl std::fmt::Debug for StrictConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeS3Transport")
    }
}
impl object_store::client::HttpConnector for StrictConnector {
    fn connect(
        &self,
        _: &object_store::ClientOptions,
    ) -> object_store::Result<object_store::client::HttpClient> {
        Ok(object_store::client::HttpClient::new(self.clone()))
    }
}
#[async_trait::async_trait]
impl object_store::client::HttpService for StrictConnector {
    async fn call(
        &self,
        request: object_store::client::HttpRequest,
    ) -> std::result::Result<object_store::client::HttpResponse, object_store::client::HttpError>
    {
        use object_store::client::{HttpError, HttpErrorKind, HttpService};
        // ListObjectsV2 is a bucket operation and has no object version. All
        // object GET/HEAD/PUT responses still pass the exact singleton parser.
        let listing = request.method() == Method::GET
            && request
                .uri()
                .query()
                .is_some_and(|query| query.split('&').any(|pair| pair == "list-type=2"));
        let needs_identity =
            !listing && matches!(*request.method(), Method::GET | Method::HEAD | Method::PUT);
        let response = HttpService::call(&self.0, request).await.map_err(|_| {
            HttpError::new(
                HttpErrorKind::Unknown,
                std::io::Error::other("native S3 transport failed"),
            )
        })?;
        let validated = check_header_bound(response.headers()).and_then(|_| {
            if response.status().is_success() && needs_identity {
                response_version(response.headers()).map(|_| ())
            } else {
                Ok(())
            }
        });
        validated.map_err(|_| {
            HttpError::new(
                HttpErrorKind::Decode,
                std::io::Error::other("native S3 response headers are invalid"),
            )
        })?;
        Ok(response)
    }
}

fn single_header(headers: &header::HeaderMap, name: &str) -> Result<Option<String>> {
    let mut values = headers.get_all(name).iter();
    let result = values
        .next()
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| anyhow::anyhow!("native upload version is not valid text"))?;
    ensure!(
        values.next().is_none(),
        "native upload returned duplicate version headers"
    );
    Ok(result)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod fixture;
