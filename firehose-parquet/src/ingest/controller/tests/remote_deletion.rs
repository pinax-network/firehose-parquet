use super::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    path::Path as ObjectPath, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

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
async fn accepted_delete_error_or_cancellation_retains_owner_and_journal_with_one_attempt() {
    for fault in [1, 2] {
        let backend = Arc::new(Remote::default());
        let store: Arc<dyn ObjectStore> = backend.clone();
        let owner = S3Ownership::acquire(store.clone(), "test-ingest", vec!["dataset".into()])
            .await
            .unwrap();
        let expected_owner = owner.record().clone();
        let temp = tempfile::tempdir().unwrap();
        let mut descriptor = actual_descriptor(temp.path());
        descriptor.output = StorageIdentity::S3 {
            service: Digest::hash("service", &"fixture").unwrap(),
            bucket: "bucket".into(),
            prefix: "dataset".into(),
        };
        let states = TransactionStateStore::s3("dataset", &owner).unwrap();
        states
            .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
            .await
            .unwrap();
        let mirror = Mirror::default();
        let mut controller = TransactionController::open(
            states,
            TransactionParts::s3("dataset", &owner, "").unwrap(),
            &mirror,
            &descriptor,
        )
        .await
        .unwrap();
        let injected = fail(Stage::Published(0));
        assert!(commit(&mut controller).await.is_err());
        drop(injected);
        drop(controller);
        backend.fault.store(fault, Ordering::SeqCst);
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            TransactionController::open(
                TransactionStateStore::s3("dataset", &owner).unwrap(),
                TransactionParts::s3("dataset", &owner, "").unwrap(),
                &mirror,
                &descriptor,
            ),
        )
        .await;
        if fault == 1 {
            let error = match result.unwrap() {
                Ok(_) => panic!("lost DELETE response was accepted"),
                Err(error) => error,
            };
            assert!(!format!("{error:#}").contains("private-backend-token"));
        } else {
            assert!(result.is_err());
        }
        assert!(owner.is_mutation_uncertain());
        assert_eq!(backend.deletes.load(Ordering::SeqCst), 1);
        let snapshot = TransactionStateStore::s3("dataset", &owner)
            .unwrap()
            .load()
            .await
            .unwrap();
        assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 0);
        assert!(snapshot.pending.is_some());
        assert!(owner.release().await.is_err());
        assert_eq!(
            S3Ownership::status(&store).await.unwrap().unwrap(),
            expected_owner
        );
        assert!(
            S3Ownership::acquire(store, "another-ingest", vec!["dataset".into()])
                .await
                .is_err()
        );
    }
}
