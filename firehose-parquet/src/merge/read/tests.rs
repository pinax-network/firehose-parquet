mod benchmark;
mod wire;
use super::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    memory::InMemory, path::Path, GetResult, GetResultPayload, ListResult, MultipartUpload,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

#[derive(Debug, Default)]
pub(crate) struct Store {
    pub(crate) inner: InMemory,
    pub(crate) requests: Mutex<Vec<(Path, GetOptions)>>,
    pub(crate) completed: Mutex<Vec<Path>>,
    active: AtomicUsize,
    maximum: AtomicUsize,
    pub(crate) fault: AtomicUsize,
    failures: AtomicUsize,
    pub(crate) delay_ms: u64,
    pub(crate) body_failure_key: Option<Path>,
}
impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("pinned-read-fixture")
    }
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
fn transient() -> object_store::Error {
    object_store::Error::Generic {
        store: "synthetic",
        source: "synthetic read failure".into(),
    }
}
#[async_trait]
impl ObjectStore for Store {
    async fn get_opts(&self, key: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if !key.as_ref().ends_with(".parquet") {
            return self.inner.get_opts(key, options).await;
        }
        self.requests
            .lock()
            .unwrap()
            .push((key.clone(), options.clone()));
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        let _active = Active(&self.active);
        if self.delay_ms > 0 {
            // Source zero completes last, exercising ordered out-of-order results.
            let delay = if key.as_ref().ends_with("000000.parquet") {
                self.delay_ms * 4
            } else {
                self.delay_ms
            };
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let fault = self.fault.load(Ordering::SeqCst);
        let fail = self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if fail && fault != 8 {
            return Err(transient());
        }
        if fault == 10 && self.body_failure_key.as_ref() == Some(key) {
            let parent = key
                .as_ref()
                .rsplit_once('/')
                .map_or("", |(parent, _)| parent);
            let journal = Path::from(format!("{parent}/{}", crate::merge_journal::JOURNAL_FILE));
            if self.inner.head(&journal).await.is_ok() {
                return Err(transient());
            }
        }
        let mut actual = options;
        if fault == 1 {
            actual.if_match = None;
            actual.version = None;
        }
        let mut result = self.inner.get_opts(key, actual).await?;
        match fault {
            2 => result.range.start += 1,
            3 => result.meta.location = Path::from("foreign.parquet"),
            4 => result.meta.size += 1,
            5 => {
                result.meta.e_tag = None;
                result.meta.version = None;
            }
            _ => {}
        }
        if matches!(fault, 6 | 7) || (fault == 8 && fail) {
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let mut data = result.bytes().await?.to_vec();
            if fault == 6 {
                data.pop();
            } else if fault == 7 {
                data.push(255);
            }
            let item = if fault == 8 {
                Err(transient())
            } else {
                Ok(Bytes::from(data))
            };
            result = GetResult {
                meta,
                range,
                attributes,
                payload: GetResultPayload::Stream(stream::iter([item]).boxed()),
            };
        }
        self.completed.lock().unwrap().push(key.clone());
        Ok(result)
    }
    async fn put_opts(
        &self,
        p: &Path,
        b: PutPayload,
        o: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(p, b, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn delete(&self, p: &Path) -> object_store::Result<()> {
        self.inner.delete(p).await
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy(&self, a: &Path, b: &Path) -> object_store::Result<()> {
        self.inner.copy(a, b).await
    }
    async fn copy_if_not_exists(&self, a: &Path, b: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(a, b).await
    }
}
async fn fixture(store: &Store, sizes: &[usize]) -> Vec<ObjectMeta> {
    let mut objects = Vec::new();
    for (index, size) in sizes.iter().enumerate() {
        let key = Path::from(format!("blocks/part-{index:06}.parquet"));
        store
            .inner
            .put(&key, Bytes::from(vec![index as u8; *size]).into())
            .await
            .unwrap();
        objects.push(store.inner.head(&key).await.unwrap());
    }
    objects
}

#[tokio::test]
async fn windows_enforce_count_and_bytes_with_oversized_objects_alone() {
    let store = Store::default();
    let objects = fixture(&store, &[20, 30, 40, 50, 60, 101, 1, 1, 1, 1, 1]).await;
    let windows = Windows {
        remaining: &objects,
        max_active: 4,
        budget: 100,
    }
    .collect::<Result<Vec<_>>>()
    .unwrap();
    assert_eq!(
        windows
            .iter()
            .map(|window| window.iter().map(|meta| meta.size).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![
            vec![20, 30, 40],
            vec![50],
            vec![60],
            vec![101],
            vec![1, 1, 1, 1],
            vec![1]
        ]
    );
    assert!(windows.iter().all(|window| window.len() <= 4
        && (window.len() == 1 || window.iter().map(|meta| meta.size).sum::<u64>() <= 100)));
}

#[test]
fn invalid_later_window_identity_and_size_reservations_fail_before_any_get() {
    let store = Arc::new(Store::default());
    let client: Arc<dyn ObjectStore> = store.clone();
    let mut listed = crate::cli::block_on_async(fixture(&store, &[10; 5]));
    assert!(objects(&client, &listed).is_err());
    listed.truncate(2);
    listed[1].e_tag = None;
    listed[1].version = None;
    assert!(objects(&client, &listed).is_err());
    assert!(schemas(&client, &listed).is_err());
    listed[1] = listed[0].clone();
    listed[0].size = WINDOW_BYTES;
    listed[1].size = 1;
    assert!(objects(&client, &listed).is_err());
    listed[0].size = u64::MAX;
    assert!(objects(&client, &listed).is_err());
    assert!(store.requests.lock().unwrap().is_empty());
}

#[test]
fn parallel_read_results_remain_ordered_and_a_window_is_consumed_before_the_next() {
    let store = Arc::new(Store {
        delay_ms: 2,
        ..Default::default()
    });
    let client: Arc<dyn ObjectStore> = store.clone();
    let listed = crate::cli::block_on_async(fixture(&store, &[10; 11]));
    let mut consumed = 0;
    for window in windows(&listed) {
        let window = window.unwrap();
        let data = objects(&client, window).unwrap();
        assert_eq!(store.active.load(Ordering::SeqCst), 0);
        // No later-window request is issued while this Vec owns the reservation.
        assert_eq!(
            store.requests.lock().unwrap().len(),
            consumed + window.len()
        );
        for bytes in data {
            assert_eq!(bytes.as_ref(), &[consumed as u8; 10]);
            consumed += 1;
        }
    }
    assert_eq!(consumed, 11);
    assert_eq!(store.maximum.load(Ordering::SeqCst), 4);
    assert_ne!(store.completed.lock().unwrap()[0], listed[0].location);
}

#[tokio::test]
async fn invalid_identities_or_ranges_make_no_request() {
    let store = Arc::new(Store::default());
    let client: Arc<dyn ObjectStore> = store.clone();
    let original = fixture(&store, &[10]).await.remove(0);
    for (tag, version) in [
        (None, None),
        (Some("*"), None),
        (Some("a,b"), None),
        (None, Some("null")),
        (Some("bad\nvalue"), Some("v1")),
    ] {
        let mut meta = original.clone();
        meta.e_tag = tag.map(str::to_owned);
        meta.version = version.map(str::to_owned);
        assert!(pinned_range(&client, &meta, 0..10).await.is_err());
    }
    assert!(pinned_range(&client, &original, 9..11).await.is_err());
    assert!(pinned_range(&client, &original, 9..8).await.is_err());
    assert!(store.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn ignored_conditions_and_metadata_or_body_mismatches_fail_without_retry() {
    for fault in 1..=7 {
        let store = Arc::new(Store::default());
        let client: Arc<dyn ObjectStore> = store.clone();
        let meta = fixture(&store, &[10]).await.remove(0);
        store.fault.store(fault, Ordering::SeqCst);
        if fault == 1 {
            store
                .inner
                .put(&meta.location, Bytes::from_static(b"changed123").into())
                .await
                .unwrap();
        }
        assert!(
            pinned_range_with(
                &client,
                &meta,
                0..10,
                5,
                Duration::from_secs(1),
                Duration::ZERO
            )
            .await
            .is_err(),
            "fault {fault}"
        );
        assert_eq!(store.requests.lock().unwrap().len(), 1, "fault {fault}");
        assert_eq!(store.active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn conditional_failure_cannot_be_replaced_with_a_fresh_snapshot() {
    let store = Arc::new(Store::default());
    let client: Arc<dyn ObjectStore> = store.clone();
    let meta = fixture(&store, &[10]).await.remove(0);
    store
        .inner
        .put(&meta.location, Bytes::from_static(b"changed123").into())
        .await
        .unwrap();
    let error = pinned_range(&client, &meta, 0..10).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ReadFailure>(),
        Some(ReadFailure::Changed)
    ));
    assert_eq!(store.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn request_and_body_retries_keep_every_original_snapshot_condition() {
    for body_error in [false, true] {
        let store = Arc::new(Store::default());
        let client: Arc<dyn ObjectStore> = store.clone();
        let meta = fixture(&store, &[10]).await.remove(0);
        store.failures.store(1, Ordering::SeqCst);
        if body_error {
            store.fault.store(8, Ordering::SeqCst);
        }
        let data = pinned_range_with(
            &client,
            &meta,
            2..8,
            5,
            Duration::from_secs(1),
            Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(data.len(), 6);
        let requests = store.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for (key, options) in requests.iter() {
            assert_eq!(key, &meta.location);
            assert_eq!(options.if_match, meta.e_tag);
            assert_eq!(options.version, meta.version);
            assert_eq!(options.range, Some(GetRange::Bounded(2..8)));
        }
    }
}

#[tokio::test]
async fn deadline_and_exhausted_transport_errors_remain_distinct_from_snapshot_rejection() {
    for timeout in [false, true] {
        let store = Arc::new(Store {
            delay_ms: if timeout { 20 } else { 0 },
            ..Default::default()
        });
        let client: Arc<dyn ObjectStore> = store.clone();
        let meta = fixture(&store, &[10]).await.remove(0);
        if !timeout {
            store.failures.store(10, Ordering::SeqCst);
        }
        let error = pinned_range_with(
            &client,
            &meta,
            0..10,
            2,
            Duration::from_millis(1),
            Duration::ZERO,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ReadFailure>(),
            Some(ReadFailure::Request | ReadFailure::Transport | ReadFailure::Timeout)
        ));
        assert_eq!(store.requests.lock().unwrap().len(), 2);
        assert_eq!(store.active.load(Ordering::SeqCst), 0);
    }
}
