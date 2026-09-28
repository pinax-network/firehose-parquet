//! In-memory model of Ceph RGW 19.2's `If-Match` comparison (#678), for tests.
//!
//! ETags are returned in the RFC 9110 quoted form, as S3 returns them, but an
//! `If-Match` value (PUT or GET) is compared literally with the stored ETag
//! *without* its quotes: the quoted form of the correct ETag is refused and
//! the unquoted one is accepted. Wrong and stale versions stay refused in
//! both forms. With `refuse_all`, no `If-Match` value ever matches.
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub(crate) struct Rgw19Store {
    inner: InMemory,
    refuse_all: AtomicBool,
}

impl Rgw19Store {
    /// A provider on which neither ETag form matches.
    pub(crate) fn refusing_every_version() -> Self {
        let store = Self::default();
        store.refuse_all.store(true, Ordering::SeqCst);
        store
    }

    /// The `If-Match` precondition fails unless `value` is unquoted (or `*`)
    /// and every version is not refused; InMemory then compares the value
    /// with its own unquoted ETag.
    fn refuses(&self, value: &str) -> bool {
        value != "*" && (value.starts_with('"') || self.refuse_all.load(Ordering::SeqCst))
    }
}

fn quoted(etag: Option<String>) -> Option<String> {
    etag.map(|etag| format!("\"{etag}\""))
}

fn precondition(key: &Path) -> object_store::Error {
    object_store::Error::Precondition {
        path: key.to_string(),
        source: "PreconditionFailed".into(),
    }
}

impl fmt::Display for Rgw19Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Rgw19Store")
    }
}
impl fmt::Debug for Rgw19Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Rgw19Store")
    }
}

#[async_trait]
impl ObjectStore for Rgw19Store {
    async fn put_opts(
        &self,
        key: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if let PutMode::Update(version) = &opts.mode {
            if version
                .e_tag
                .as_deref()
                .is_none_or(|etag| self.refuses(etag))
            {
                return Err(precondition(key));
            }
        }
        let mut result = self.inner.put_opts(key, payload, opts).await?;
        result.e_tag = quoted(result.e_tag);
        Ok(result)
    }
    async fn get_opts(&self, key: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        if opts
            .if_match
            .as_deref()
            .is_some_and(|etag| self.refuses(etag))
        {
            return Err(precondition(key));
        }
        let mut result = self.inner.get_opts(key, opts).await?;
        result.meta.e_tag = quoted(result.meta.e_tag);
        Ok(result)
    }
    async fn delete(&self, key: &Path) -> object_store::Result<()> {
        self.inner.delete(key).await
    }
    async fn put_multipart_opts(
        &self,
        key: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner
            .list(prefix)
            .map(|meta| {
                meta.map(|mut meta| {
                    meta.e_tag = quoted(meta.e_tag);
                    meta
                })
            })
            .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        for meta in &mut result.objects {
            meta.e_tag = quoted(meta.e_tag.take());
        }
        Ok(result)
    }
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
