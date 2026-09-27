//! Single-attempt data deletion within an already-owned maintenance phase.
//!
//! Callers must retain remote ownership on error/cancellation. Draining client
//! futures does not prove provider-level request quiescence. Do not use native
//! `delete_stream`: object_store 0.12 synthesizes successful per-key results from
//! incomplete bulk responses. Individual requests keep each result bound to its
//! exact key without relying on that mapping.

use anyhow::{ensure, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use object_store::{path::Path, ObjectStore};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const MAX_ACTIVE: usize = 10;
const REQUEST_DEADLINE: Duration = Duration::from_secs(60);

/// Delete exactly the selected data keys. Never list, retry, or release a guard.
/// The client must come from the zero-transport-retry mutation constructor.
/// Already-absent objects count as complete, matching S3's DeleteObject contract.
pub(crate) async fn delete_objects_once(
    client: &Arc<dyn ObjectStore>,
    keys: &[Path],
) -> Result<()> {
    delete_objects_observed(client, keys, |_| Ok(())).await
}

/// The observer runs after a successful request, while the operation is healthy.
/// An observer error stops scheduling and drains active requests before returning.
pub(crate) async fn delete_objects_observed<F>(
    client: &Arc<dyn ObjectStore>,
    keys: &[Path],
    after_delete: F,
) -> Result<()>
where
    F: FnMut(&Path) -> Result<()>,
{
    delete_with_options(client, keys, MAX_ACTIVE, REQUEST_DEADLINE, after_delete).await
}

async fn delete_with_options<F>(
    client: &Arc<dyn ObjectStore>,
    keys: &[Path],
    concurrency: usize,
    deadline: Duration,
    mut after_delete: F,
) -> Result<()>
where
    F: FnMut(&Path) -> Result<()>,
{
    ensure!(
        concurrency > 0,
        "data deletion concurrency must be positive"
    );
    let mut unique = HashSet::with_capacity(keys.len());
    for key in keys {
        ensure!(
            !key.as_ref().is_empty()
                && !crate::artifacts::is_control_path(key.as_ref())
                && !key.as_ref().split('/').any(|part| matches!(
                    part,
                    crate::merge_journal::JOURNAL_FILE | crate::merge_journal::LOCK_FILE
                )),
            "data deletion plan includes an empty or reserved control key"
        );
        ensure!(
            unique.insert(key),
            "data deletion plan contains duplicate keys"
        );
    }
    let stopped = AtomicBool::new(false);
    let mut remaining = keys.iter();
    let mut active = FuturesUnordered::new();
    for key in remaining.by_ref().take(concurrency) {
        active.push(delete_request(client, key, deadline, &stopped));
    }
    let mut failure = None;
    while let Some((key, result)) = active.next().await {
        // After a failure, keep polling requests that were already started, but
        // invoke no callbacks and start no new mutations. Timeouts stay errors:
        // a settled client future is not proof an accepted provider request ended.
        if failure.is_none() {
            if let Err(error) = result.and_then(|()| after_delete(key)) {
                failure = Some(error);
                stopped.store(true, Ordering::Relaxed);
            } else if let Some(next) = remaining.next() {
                active.push(delete_request(client, next, deadline, &stopped));
            }
        }
    }
    match failure {
        Some(error) => Err(error.context(
            "S3 data deletion stopped; retain ownership and any journal; no mutation was retried",
        )),
        None => Ok(()),
    }
}

async fn delete_request<'a>(
    client: &'a Arc<dyn ObjectStore>,
    key: &'a Path,
    deadline: Duration,
    stopped: &'a AtomicBool,
) -> (&'a Path, Result<()>) {
    // A queued future may not have been polled before another request failed.
    // Do not turn draining that queue into fresh mutations after the failure.
    if stopped.load(Ordering::Relaxed) {
        return (key, Err(anyhow::anyhow!("S3 data DELETE was not started")));
    }
    let result = match tokio::time::timeout(deadline, client.delete(key)).await {
        Ok(Ok(())) | Ok(Err(object_store::Error::NotFound { .. })) => Ok(()),
        Ok(Err(_)) => Err(anyhow::anyhow!("S3 data DELETE failed")),
        Err(_) => Err(anyhow::anyhow!("S3 data DELETE timed out")),
    };
    (key, result)
}

#[cfg(test)]
pub(crate) mod test_store;
#[cfg(test)]
mod tests;
