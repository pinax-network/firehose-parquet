//! Ordered finite windows of snapshot-pinned S3 merge reads.
//!
//! There are no detached tasks: the caller retains every returned byte buffer
//! until that entire window has been consumed, then requests the next window.
use anyhow::{ensure, Context, Result};
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use object_store::{GetOptions, GetRange, ObjectMeta, ObjectStore, UpdateVersion};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaDataReader;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

const MAX_ACTIVE: usize = 4;
const WINDOW_BYTES: u64 = 64 * 1024 * 1024;
const REQUEST_DEADLINE: Duration = Duration::from_secs(60);
const MAX_ATTEMPTS: usize = 5;
const RETRY_BASE: Duration = Duration::from_millis(100);

pub(super) struct Windows<'a> {
    remaining: &'a [ObjectMeta],
    max_active: usize,
    budget: u64,
}
pub(super) fn windows(objects: &[ObjectMeta]) -> Windows<'_> {
    Windows {
        remaining: objects,
        max_active: MAX_ACTIVE,
        budget: WINDOW_BYTES,
    }
}
impl<'a> Iterator for Windows<'a> {
    type Item = Result<&'a [ObjectMeta]>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }
        let mut count = 0;
        let mut bytes = 0u64;
        while count < self.remaining.len() && count < self.max_active {
            let next = match bytes.checked_add(self.remaining[count].size) {
                Some(next) => next,
                None => {
                    self.remaining = &[];
                    return Some(Err(anyhow::anyhow!("S3 read window size overflow")));
                }
            };
            if count > 0 && next > self.budget {
                break;
            }
            bytes = next;
            count += 1;
            // One oversized object keeps the existing whole-input lower bound,
            // but may never share a window with any other source.
            if bytes > self.budget {
                break;
            }
        }
        let (window, remaining) = self.remaining.split_at(count);
        self.remaining = remaining;
        Some(Ok(window))
    }
}

fn validate_window(window: &[ObjectMeta]) -> Result<()> {
    ensure!(
        window.len() <= MAX_ACTIVE,
        "S3 read window exceeds its request bound"
    );
    let bytes = window.iter().try_fold(0u64, |sum, object| {
        sum.checked_add(object.size)
            .context("S3 read window size overflow")
    })?;
    ensure!(
        window.len() <= 1 || bytes <= WINDOW_BYTES,
        "S3 read window exceeds its byte reservation"
    );
    for object in window {
        ensure!(
            crate::dataset_lock_s3::usable_version(&UpdateVersion {
                e_tag: object.e_tag.clone(),
                version: object.version.clone()
            }),
            "S3 merge source has no usable snapshot identity"
        );
    }
    Ok(())
}

pub(super) fn schemas(
    client: &Arc<dyn ObjectStore>,
    window: &[ObjectMeta],
) -> Result<Vec<SchemaRef>> {
    validate_window(window)?;
    crate::cli::block_on_async(async {
        stream::iter(window)
            .map(|object| arrow_schema(client, object, super::S3_FOOTER_PREFETCH_BYTES))
            .buffered(MAX_ACTIVE)
            .try_collect()
            .await
    })
}
pub(super) fn objects(client: &Arc<dyn ObjectStore>, window: &[ObjectMeta]) -> Result<Vec<Bytes>> {
    validate_window(window)?;
    crate::cli::block_on_async(async {
        stream::iter(window)
            .map(|object| pinned_range(client, object, 0..object.size))
            .buffered(MAX_ACTIVE)
            .try_collect()
            .await
    })
}

pub(super) async fn arrow_schema(
    client: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    prefetch: u64,
) -> Result<SchemaRef> {
    ensure!(prefetch > 0, "S3 footer prefetch must be positive");
    let mut tail_len = object.size.min(prefetch);
    let mut reader = ParquetMetaDataReader::new();
    loop {
        let tail = pinned_range(client, object, object.size - tail_len..object.size).await?;
        match reader.try_parse_sized(&tail, object.size) {
            Ok(()) => break,
            Err(ParquetError::NeedMoreData(needed)) => {
                let needed =
                    u64::try_from(needed).context("Parquet footer length is not representable")?;
                ensure!(
                    needed > tail_len && needed <= object.size,
                    "Parquet footer length is inconsistent with its listed object size"
                );
                tail_len = needed;
            }
            Err(_) => anyhow::bail!("S3 merge source has an invalid Parquet footer"),
        }
    }
    let metadata =
        ArrowReaderMetadata::try_new(Arc::new(reader.finish()?), ArrowReaderOptions::default())?;
    Ok(metadata.schema().clone())
}

#[derive(Debug, thiserror::Error)]
enum ReadFailure {
    #[error("S3 merge source snapshot or range is invalid")]
    InvalidRequest,
    #[error("S3 merge source changed or was removed after listing")]
    Changed,
    #[error("S3 merge response metadata does not match its listed snapshot")]
    Metadata,
    #[error("S3 merge response length does not match its requested range")]
    BodyLength,
    #[error("S3 merge response buffer cannot be allocated")]
    Allocation,
    #[error("S3 merge source request failed or its response was rejected")]
    Request,
    #[error("S3 merge source body read failed")]
    Transport,
    #[error("S3 merge source request timed out")]
    Timeout,
}
impl ReadFailure {
    fn retryable(&self) -> bool {
        matches!(self, Self::Request | Self::Transport | Self::Timeout)
    }
}

async fn pinned_range(
    client: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    range: Range<u64>,
) -> Result<Bytes> {
    pinned_range_with(
        client,
        object,
        range,
        MAX_ATTEMPTS,
        REQUEST_DEADLINE,
        RETRY_BASE,
    )
    .await
}
async fn pinned_range_with(
    client: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    range: Range<u64>,
    attempts: usize,
    deadline: Duration,
    retry_base: Duration,
) -> Result<Bytes> {
    // Validate before any request, and never replace these immutable conditions
    // with a freshly listed/unpinned identity during read-only retries.
    if !crate::dataset_lock_s3::usable_version(&UpdateVersion {
        e_tag: object.e_tag.clone(),
        version: object.version.clone(),
    }) || range.start > range.end
        || range.end > object.size
        || attempts == 0
    {
        return Err(ReadFailure::InvalidRequest.into());
    }
    if range.is_empty() {
        return Ok(Bytes::new());
    }
    for attempt in 0..attempts {
        let result = tokio::time::timeout(deadline, attempt_range(client, object, range.clone()))
            .await
            .unwrap_or(Err(ReadFailure::Timeout));
        match result {
            Ok(bytes) => return Ok(bytes),
            Err(error) if error.retryable() && attempt + 1 < attempts => {
                tracing::warn!(
                    operation = super::MergeS3Operation::Read.as_str(),
                    attempt = attempt + 1,
                    attempts,
                    "retrying read of the same pinned S3 snapshot"
                );
                tokio::time::sleep(retry_base.saturating_mul(1u32 << attempt.min(16))).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    unreachable!("positive attempt count always returns or retries within the bound")
}

async fn attempt_range(
    client: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    range: Range<u64>,
) -> std::result::Result<Bytes, ReadFailure> {
    let result = client
        .get_opts(
            &object.location,
            GetOptions {
                range: Some(GetRange::Bounded(range.clone())),
                if_match: object.e_tag.clone(),
                version: object.version.clone(),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| match error {
            object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. } => {
                ReadFailure::Changed
            }
            _ => ReadFailure::Request,
        })?;
    if result.range != range
        || result.meta.location != object.location
        || result.meta.size != object.size
        || object
            .e_tag
            .as_ref()
            .is_some_and(|tag| result.meta.e_tag.as_ref() != Some(tag))
        || object
            .version
            .as_ref()
            .is_some_and(|version| result.meta.version.as_ref() != Some(version))
    {
        return Err(ReadFailure::Metadata);
    }
    let expected = usize::try_from(range.end - range.start).map_err(|_| ReadFailure::Allocation)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(expected)
        .map_err(|_| ReadFailure::Allocation)?;
    let mut stream = result.into_stream();
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|_| ReadFailure::Transport)?
    {
        if bytes
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > expected)
        {
            return Err(ReadFailure::BodyLength);
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != expected {
        return Err(ReadFailure::BodyLength);
    }
    Ok(bytes.into())
}

#[cfg(test)]
pub(super) mod tests;
