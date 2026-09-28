//! A test bucket with about a million generated data keys, for listing-cost
//! tests (#655). Control objects (owner record, authority, mirror) and any
//! data a test writes live in an in-memory store. The generated data keys are
//! computed, not stored, so a million of them cost no memory until listed.
//! Every listing is served in pages of [`LIST_PAGE_KEYS`] keys, like S3's
//! `ListObjectsV2`, and each page is counted as one request. One page of a
//! listing can be made slow.

use super::LIST_PAGE_KEYS;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::BTreeSet;
use std::iter::Peekable;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The generated data set: `<root>/<table>/date=<YYYY-MM-DD>/part-v1-<n>.parquet`.
#[derive(Clone, Debug)]
pub(crate) struct GeneratedData {
    /// Dataset root inside the bucket, `""` for the bucket root.
    pub root: String,
    /// Sorted table names.
    pub tables: Vec<String>,
    pub days: u32,
    pub parts_per_day: u32,
}

impl GeneratedData {
    /// About a million keys: 4 tables × 250 days × 1,000 parts.
    pub(crate) fn million(root: &str) -> Self {
        Self {
            root: root.to_owned(),
            tables: ["blocks", "logs", "traces", "transactions"]
                .map(String::from)
                .to_vec(),
            days: 250,
            parts_per_day: 1_000,
        }
    }

    pub(crate) fn keys(&self) -> u64 {
        self.tables.len() as u64 * u64::from(self.days) * u64::from(self.parts_per_day)
    }

    fn table_prefix(&self, table: &str) -> String {
        if self.root.is_empty() {
            table.to_owned()
        } else {
            format!("{}/{table}", self.root)
        }
    }

    fn contains(&self, key: &str) -> bool {
        self.tables.iter().any(|table| {
            key.strip_prefix(&self.table_prefix(table))
                .is_some_and(|tail| tail.starts_with('/'))
        })
    }

    /// Generated keys under `prefix` (whole path segments, like object_store),
    /// in lexicographic order. Tables outside the prefix are skipped without
    /// generating their keys.
    fn under(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + Send> {
        let within = |key: &str| {
            prefix.is_empty()
                || key == prefix
                || key
                    .strip_prefix(prefix)
                    .is_some_and(|tail| tail.starts_with('/'))
        };
        let mut tables = Vec::new();
        for table in &self.tables {
            let base = self.table_prefix(table);
            let whole = within(&base);
            let inside = base_contains(&base, prefix);
            if whole || inside {
                tables.push((base, !whole));
            }
        }
        let (days, parts) = (self.days, self.parts_per_day);
        let prefix = prefix.to_owned();
        let start = time::Date::from_calendar_date(2020, time::Month::January, 1).unwrap();
        Box::new(tables.into_iter().flat_map(move |(base, filter)| {
            let prefix = prefix.clone();
            (0..days).flat_map(move |day| {
                let date = start + time::Duration::days(i64::from(day));
                let directory = format!(
                    "{base}/date={:04}-{:02}-{:02}",
                    date.year(),
                    u8::from(date.month()),
                    date.day()
                );
                let prefix = prefix.clone();
                (0..parts)
                    .map(move |part| format!("{directory}/part-v1-{part:06}.parquet"))
                    .filter(move |key| {
                        !filter
                            || key
                                .strip_prefix(&prefix)
                                .is_some_and(|tail| tail.starts_with('/'))
                    })
            })
        }))
    }
}

/// Whether `prefix` is strictly inside the table directory `base`.
fn base_contains(base: &str, prefix: &str) -> bool {
    prefix
        .strip_prefix(base)
        .is_some_and(|tail| tail.starts_with('/'))
}

/// Requests the bucket served.
#[derive(Debug, Default)]
pub(crate) struct Requests {
    /// Listing pages, each one `ListObjectsV2` request.
    pub list_pages: AtomicU64,
    /// Pages served with the slow delay.
    pub slow_pages: AtomicU64,
    /// GET or HEAD requests for generated data keys.
    pub data_reads: AtomicU64,
}

impl Requests {
    pub(crate) fn reset(&self) {
        self.list_pages.store(0, Ordering::SeqCst);
        self.slow_pages.store(0, Ordering::SeqCst);
        self.data_reads.store(0, Ordering::SeqCst);
    }
    pub(crate) fn list_pages(&self) -> u64 {
        self.list_pages.load(Ordering::SeqCst)
    }
    pub(crate) fn slow_pages(&self) -> u64 {
        self.slow_pages.load(Ordering::SeqCst)
    }
    pub(crate) fn data_reads(&self) -> u64 {
        self.data_reads.load(Ordering::SeqCst)
    }
}

/// How pages are delayed.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Pacing {
    /// Added to every page.
    pub every_page: Duration,
    /// The page at this index of a listing (0-based) waits this much longer.
    pub slow_page: Option<(u64, Duration)>,
}

#[derive(Debug)]
pub(crate) struct PagedBucket {
    inner: InMemory,
    data: Mutex<Option<GeneratedData>>,
    pacing: Pacing,
    pub requests: Arc<Requests>,
}

impl std::fmt::Display for PagedBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PagedBucket")
    }
}

impl PagedBucket {
    pub(crate) fn new(pacing: Pacing) -> Self {
        Self {
            inner: InMemory::new(),
            data: Mutex::new(None),
            pacing,
            requests: Arc::default(),
        }
    }

    /// Adds (or with `None` removes) the generated data keys.
    pub(crate) fn generate(&self, data: Option<GeneratedData>) {
        *self.data.lock().unwrap() = data;
    }

    fn is_generated(&self, location: &Path) -> bool {
        self.data
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|data| data.contains(location.as_ref()))
    }
}

fn meta(key: String) -> ObjectMeta {
    ObjectMeta {
        location: Path::from(key),
        last_modified: Default::default(),
        size: 1_048_576,
        e_tag: Some("generated".into()),
        version: None,
    }
}

/// Stored and generated objects in one lexicographic order.
struct Merged {
    stored: Peekable<std::vec::IntoIter<ObjectMeta>>,
    generated: Peekable<Box<dyn Iterator<Item = String> + Send>>,
}

impl Iterator for Merged {
    type Item = ObjectMeta;
    fn next(&mut self) -> Option<ObjectMeta> {
        let stored_first = match (self.stored.peek(), self.generated.peek()) {
            (None, None) => return None,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some(stored), Some(generated)) => stored.location.as_ref() <= generated.as_str(),
        };
        if stored_first {
            self.stored.next()
        } else {
            self.generated.next().map(meta)
        }
    }
}

fn pages(
    objects: Merged,
    requests: Arc<Requests>,
    pacing: Pacing,
) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
    let page_size = usize::try_from(LIST_PAGE_KEYS).unwrap();
    stream::unfold(
        (objects, 0_u64, false),
        move |(mut objects, index, finished)| {
            let requests = requests.clone();
            async move {
                if finished {
                    return None;
                }
                let page: Vec<ObjectMeta> = objects.by_ref().take(page_size).collect();
                requests.list_pages.fetch_add(1, Ordering::SeqCst);
                let mut wait = pacing.every_page;
                if let Some((slow, delay)) = pacing.slow_page {
                    if slow == index {
                        requests.slow_pages.fetch_add(1, Ordering::SeqCst);
                        wait += delay;
                    }
                }
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                let last = page.len() < page_size;
                Some((
                    stream::iter(page.into_iter().map(Ok)),
                    (objects, index + 1, last),
                ))
            }
        },
    )
    .flatten()
    .boxed()
}

#[async_trait]
impl ObjectStore for PagedBucket {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if self.is_generated(location) {
            self.requests.data_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let stored = self.inner.list(prefix);
        let prefix = prefix
            .map(|path| path.as_ref().to_owned())
            .unwrap_or_default();
        let mut generated = Some(match self.data.lock().unwrap().as_ref() {
            Some(data) => data.under(&prefix),
            None => Box::new(std::iter::empty()),
        });
        let requests = self.requests.clone();
        let pacing = self.pacing;
        stream::once(async move { stored.try_collect::<Vec<_>>().await })
            .map(move |stored| match stored {
                Err(error) => stream::iter([Err(error)]).boxed(),
                Ok(stored) => pages(
                    Merged {
                        stored: stored.into_iter().peekable(),
                        generated: generated.take().expect("listed once").peekable(),
                    },
                    requests.clone(),
                    pacing,
                ),
            })
            .flatten()
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let base = prefix
            .map(|path| path.as_ref().to_owned())
            .unwrap_or_default();
        let objects: Vec<ObjectMeta> = self.list(prefix).try_collect().await?;
        let mut common_prefixes = BTreeSet::new();
        let mut direct = Vec::new();
        for object in objects {
            let location = object.location.as_ref();
            let relative = location
                .strip_prefix(base.as_str())
                .map(|tail| tail.trim_start_matches('/'))
                .unwrap_or(location)
                .to_owned();
            match relative.split_once('/') {
                Some((directory, _)) => {
                    common_prefixes.insert(if base.is_empty() {
                        directory.to_owned()
                    } else {
                        format!("{base}/{directory}")
                    });
                }
                None => direct.push(object),
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().map(Path::from).collect(),
            objects: direct,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
