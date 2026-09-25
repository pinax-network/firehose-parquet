use super::*;
use crate::dataset_lock_s3::S3Ownership;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::{
    path::Path as ObjectPath, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default, Debug)]
struct Remote {
    inner: InMemory,
    fault: AtomicU8,
    deletes: AtomicUsize,
}
impl std::fmt::Display for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("transaction-delete-fixture")
    }
}
#[async_trait]
impl ObjectStore for Remote {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn get_opts(
        &self,
        path: &ObjectPath,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, opts).await
    }
    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        if path.as_ref().ends_with(".parquet") {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            match self.fault.load(Ordering::SeqCst) {
                1 => {
                    self.inner.delete(path).await?;
                    return Err(object_store::Error::Generic {
                        store: "private-fixture",
                        source: "private-backend-token".into(),
                    });
                }
                2 => {
                    self.inner.delete(path).await?;
                    futures::future::pending::<()>().await;
                }
                _ => {}
            }
        }
        self.inner.delete(path).await
    }
    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[tokio::test]
async fn accepted_merge_delete_error_or_cancellation_retains_owner_and_journal() {
    use crate::dataset_lock_s3::OWNER_KEY;
    use crate::merge_journal::{Journal, RunContext, JOURNAL_FILE};
    for fault in [1, 2] {
        let backend = Arc::new(Remote::default());
        let store: Arc<dyn ObjectStore> = backend.clone();
        let remote = S3Ownership::acquire(store.clone(), "fixture", vec!["dataset".into()])
            .await
            .unwrap();
        let expected = remote.record().clone();
        let ownership = DatasetOwnership::from_remote_for_test("bucket", remote);
        let identity = resolve_output_identity("s3://bucket/dataset", &empty_aws()).unwrap();
        let journal = Journal::new(
            &RunContext {
                run_id: "prior".into(),
                lock: OWNER_KEY.into(),
            },
            vec!["part-000001.parquet".into()],
            2,
        );
        for (name, bytes) in [
            ("part-000001.parquet", b"original".to_vec()),
            ("part-000002.parquet", b"duplicate".to_vec()),
            (JOURNAL_FILE, serde_json::to_vec(&journal).unwrap()),
        ] {
            store
                .put(
                    &ObjectPath::from(format!("dataset/blocks/{name}")),
                    bytes.into(),
                )
                .await
                .unwrap();
        }
        backend.fault.store(fault, Ordering::SeqCst);
        let outcome = tokio::time::timeout(
            Duration::from_millis(50),
            crate::merge::recover_guarded_for_ingestion(&identity, &ownership, None),
        )
        .await;
        if fault == 1 {
            let error = outcome.unwrap().unwrap_err();
            assert!(!format!("{error:#}").contains("private-backend-token"));
        } else {
            assert!(outcome.is_err());
        }
        assert_eq!(backend.deletes.load(Ordering::SeqCst), 1);
        assert!(ownership.remote("bucket").unwrap().is_mutation_uncertain());
        assert!(store
            .head(&ObjectPath::from(format!("dataset/blocks/{JOURNAL_FILE}")))
            .await
            .is_ok());
        assert!(store
            .head(&ObjectPath::from("dataset/blocks/part-000001.parquet"))
            .await
            .is_ok());
        assert!(ownership.release().await.is_err());
        assert_eq!(
            S3Ownership::status(&store).await.unwrap().unwrap(),
            expected
        );
    }
}
