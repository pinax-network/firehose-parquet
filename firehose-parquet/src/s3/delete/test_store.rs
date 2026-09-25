use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    memory::InMemory, path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub(crate) started: Mutex<Vec<Path>>,
    pub(crate) completed: AtomicUsize,
    pub(crate) cancelled: AtomicUsize,
    pub(crate) active: AtomicUsize,
    pub(crate) maximum: AtomicUsize,
}
struct Active<'a> {
    pub(crate) counters: &'a Counters,
    pub(crate) completed: bool,
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.counters.active.fetch_sub(1, Ordering::SeqCst);
        if self.completed {
            self.counters.completed.fetch_add(1, Ordering::SeqCst);
        } else {
            self.counters.cancelled.fetch_add(1, Ordering::SeqCst);
        }
    }
}
#[derive(Debug)]
pub(crate) struct DelayedStore {
    pub(crate) inner: Arc<InMemory>,
    pub(crate) counters: Counters,
    pub(crate) delay: Duration,
    pub(crate) fail: Option<Path>,
    pub(crate) immediate_error: bool,
    pub(crate) lose_response: bool,
    pub(crate) delayed_gate: Option<Arc<tokio::sync::Semaphore>>,
}
impl DelayedStore {
    pub(crate) fn new(delay: Duration) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            counters: Counters::default(),
            delay,
            fail: None,
            immediate_error: false,
            lose_response: false,
            delayed_gate: None,
        }
    }
}
impl std::fmt::Display for DelayedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("delayed-delete-fixture")
    }
}
#[async_trait]
impl ObjectStore for DelayedStore {
    async fn delete(&self, key: &Path) -> object_store::Result<()> {
        // Ownership canary cleanup is outside the measured data operation.
        if !key.as_ref().ends_with(".parquet") {
            return self.inner.delete(key).await;
        }
        self.counters.started.lock().unwrap().push(key.clone());
        let active = self.counters.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.counters.maximum.fetch_max(active, Ordering::SeqCst);
        let mut request = Active {
            counters: &self.counters,
            completed: false,
        };
        let fails = self.fail.as_ref() == Some(key);
        if !(fails && self.immediate_error) {
            tokio::time::sleep(if fails { self.delay / 4 } else { self.delay }).await;
        }
        if !fails {
            if let Some(gate) = &self.delayed_gate {
                gate.acquire().await.unwrap().forget();
            }
        }
        request.completed = true;
        if !fails || self.lose_response {
            self.inner.delete(key).await?;
        }
        if fails {
            Err(object_store::Error::Generic {
                store: "synthetic",
                source: "synthetic response failure".into(),
            })
        } else {
            Ok(())
        }
    }
    async fn get_opts(&self, p: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(p, o).await
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
