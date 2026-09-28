//! Shared read-only listing mechanics: the protected-root and eligibility
//! checks of `build` and `recovery` (#655), and `inspect`'s whole-object read.
//!
//! No table is read through a listing: `validate` reads the active files of a
//! pinned Delta snapshot, and engines read the tables through their logs
//! (#643). Nothing here creates clients, owns reservations or mutates.
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectMeta, ObjectStore};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// The bound on one listing request (#655). A listing has no total deadline:
/// a large tree keeps listing for as long as each page arrives in time.
pub(crate) const LIST_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Keys in one S3 `ListObjectsV2` response: the provider default, since
/// object_store sends no `max-keys`. [`ListingStats`] counts requests with it.
pub(crate) const LIST_PAGE_KEYS: u64 = 1_000;
/// How often a long listing logs its progress.
const LIST_PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

/// What a command's listings cost: requests (S3 pages, or local directory
/// reads), listed objects and time spent. `build` exports its startup totals
/// as metrics (#655).
#[derive(Debug, Default)]
pub(crate) struct ListingStats {
    requests: AtomicU64,
    objects: AtomicU64,
    micros: AtomicU64,
}

impl ListingStats {
    pub(crate) fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }
    pub(crate) fn objects(&self) -> u64 {
        self.objects.load(Ordering::Relaxed)
    }
    pub(crate) fn duration(&self) -> Duration {
        Duration::from_micros(self.micros.load(Ordering::Relaxed))
    }
    pub(crate) fn record(&self, requests: u64, objects: u64, elapsed: Duration) {
        self.requests.fetch_add(requests, Ordering::Relaxed);
        self.objects.fetch_add(objects, Ordering::Relaxed);
        self.micros.fetch_add(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

/// Lists `prefix` and hands each object to `visit` until it breaks, returning
/// the break value. Each request (one page of up to [`LIST_PAGE_KEYS`] keys)
/// must arrive within `request_timeout`; the listing as a whole has no
/// deadline, and one that runs longer than 10 seconds logs its progress.
/// `what` names the listing in errors and logs; no key or provider error is
/// echoed. Requests, objects and time are added to `stats`.
pub(crate) async fn visit_objects<B>(
    store: &dyn ObjectStore,
    prefix: Option<&ObjectPath>,
    what: &str,
    request_timeout: Duration,
    stats: &ListingStats,
    mut visit: impl FnMut(ObjectMeta) -> anyhow::Result<ControlFlow<B>>,
) -> anyhow::Result<Option<B>> {
    let started = Instant::now();
    let mut logged = started;
    let mut objects = 0_u64;
    let mut listing = store.list(prefix);
    let outcome = loop {
        let object = match tokio::time::timeout(request_timeout, listing.next()).await {
            Err(_) => {
                break Err(anyhow::anyhow!(
                    "{what} timed out: one listing request took longer than {request_timeout:?} \
                     (after {objects} objects)"
                ))
            }
            Ok(None) => break Ok(None),
            Ok(Some(Err(_))) => break Err(anyhow::anyhow!("{what} failed")),
            Ok(Some(Ok(object))) => object,
        };
        objects += 1;
        if logged.elapsed() >= LIST_PROGRESS_INTERVAL {
            logged = Instant::now();
            tracing::info!(
                listing = what,
                objects,
                elapsed_secs = started.elapsed().as_secs(),
                "listing in progress"
            );
        }
        match visit(object) {
            Err(error) => break Err(error),
            Ok(ControlFlow::Break(value)) => break Ok(Some(value)),
            Ok(ControlFlow::Continue(())) => {}
        }
    };
    let elapsed = started.elapsed();
    stats.record(objects.div_ceil(LIST_PAGE_KEYS).max(1), objects, elapsed);
    if elapsed >= LIST_PROGRESS_INTERVAL {
        tracing::info!(
            listing = what,
            objects,
            elapsed_secs = elapsed.as_secs(),
            "listing finished"
        );
    }
    outcome
}

/// Unversioned whole-object read of one file, used by `inspect`.
pub(crate) async fn read_object_bytes(
    store: &dyn ObjectStore,
    location: &ObjectPath,
) -> object_store::Result<bytes::Bytes> {
    store.get(location).await?.bytes().await
}

#[cfg(test)]
mod listing_tests;
#[cfg(test)]
pub(crate) mod paged_bucket;
#[cfg(test)]
mod tests;
