use super::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutPayload,
};
use std::sync::Mutex;

#[derive(Default)]
struct Faults {
    ignore_conditions: bool,
    unsupported_conditions: bool,
    hide_versions: bool,
    lose_owner_responses: usize,
    stale_owner_read: Option<(Bytes, UpdateVersion)>,
    stale_after_owner_put: Option<(Bytes, UpdateVersion)>,
    fail_owner_read: bool,
    old_modified_time: bool,
    lie_about_size: bool,
    owner_put_barrier: Option<Arc<tokio::sync::Barrier>>,
}

#[derive(Default)]
struct StatefulStore {
    inner: InMemory,
    faults: Mutex<Faults>,
    writes: Mutex<Vec<(String, PutMode)>>,
    deletes: Mutex<Vec<String>>,
}

impl fmt::Display for StatefulStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("https://user:fake-secret@example.invalid")
    }
}
impl fmt::Debug for StatefulStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
fn backend_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "fake-secret-provider",
        source: "fake-secret-response".into(),
    }
}

#[async_trait]
impl ObjectStore for StatefulStore {
    async fn put_opts(
        &self,
        key: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.writes
            .lock()
            .unwrap()
            .push((key.to_string(), opts.mode.clone()));
        assert_eq!(
            opts.attributes
                .get(&Attribute::CacheControl)
                .unwrap()
                .as_ref(),
            "no-store, no-cache, max-age=0"
        );
        let owner = key.as_ref() == OWNER_KEY;
        let (ignore, unsupported, lose) = {
            let mut faults = self.faults.lock().unwrap();
            let lose = owner && faults.lose_owner_responses > 0;
            if lose {
                faults.lose_owner_responses -= 1;
            }
            (
                faults.ignore_conditions,
                faults.unsupported_conditions,
                lose,
            )
        };
        if unsupported {
            return Err(object_store::Error::NotImplemented);
        }
        if ignore {
            opts.mode = PutMode::Overwrite;
        }
        let barrier = if owner {
            self.faults.lock().unwrap().owner_put_barrier.clone()
        } else {
            None
        };
        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
        let result = self.inner.put_opts(key, payload, opts).await?;
        if owner {
            let mut faults = self.faults.lock().unwrap();
            if let Some(stale) = faults.stale_after_owner_put.take() {
                faults.stale_owner_read = Some(stale);
            }
        }
        if lose {
            Err(backend_error())
        } else {
            Ok(result)
        }
    }
    async fn get_opts(&self, key: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let mut result = self.inner.get_opts(key, opts).await?;
        let faults = self.faults.lock().unwrap();
        if key.as_ref() == OWNER_KEY {
            if faults.fail_owner_read {
                return Err(backend_error());
            }
            if let Some((bytes, version)) = &faults.stale_owner_read {
                let bytes = bytes.clone();
                result.meta.size = bytes.len() as u64;
                result.range = 0..result.meta.size;
                result.meta.e_tag = version.e_tag.clone();
                result.meta.version = version.version.clone();
                result.payload = GetResultPayload::Stream(Box::pin(futures::stream::once(
                    async move { Ok(bytes) },
                )));
            }
        }
        if faults.hide_versions {
            result.meta.e_tag = None;
            result.meta.version = None;
        }
        if faults.old_modified_time {
            result.meta.last_modified = "2000-01-01T00:00:00Z".parse().unwrap();
        }
        if faults.lie_about_size {
            result.meta.size = 0;
        }
        Ok(result)
    }
    async fn delete(&self, key: &Path) -> object_store::Result<()> {
        self.deletes.lock().unwrap().push(key.to_string());
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
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn store() -> (Arc<StatefulStore>, Arc<dyn ObjectStore>) {
    let store = Arc::new(StatefulStore::default());
    (store.clone(), store)
}
async fn acquire(store: &Arc<dyn ObjectStore>) -> S3Ownership {
    S3Ownership::acquire(store.clone(), "ingest", vec!["mainnet".into()])
        .await
        .unwrap()
}
fn assert_owner_never_overwritten_or_deleted(store: &StatefulStore) {
    assert!(store
        .writes
        .lock()
        .unwrap()
        .iter()
        .filter(|(key, _)| key == OWNER_KEY)
        .all(|(_, mode)| matches!(mode, PutMode::Create | PutMode::Update(_))));
    assert!(store
        .deletes
        .lock()
        .unwrap()
        .iter()
        .all(|key| key != OWNER_KEY));
}

#[tokio::test]
async fn acquire_release_and_reacquire_increment_generation_without_deleting_record() {
    let (fake, store) = store();
    assert!(S3Ownership::status(&store).await.unwrap().is_none());
    assert!(fake.writes.lock().unwrap().is_empty());
    let guard = S3Ownership::acquire(
        store.clone(),
        "ingest",
        vec!["z/".into(), "a".into(), "z".into()],
    )
    .await
    .unwrap();
    assert_eq!(guard.record().scopes(), &["a", "z"]);
    assert_eq!(guard.record().generation(), 1);
    let first_owner = guard.record().owner_id().to_string();
    assert_eq!(
        S3Ownership::acquire(store.clone(), "merge", vec!["other-prefix".into()])
            .await
            .unwrap_err(),
        OwnershipError::Busy
    );
    guard.release().await.unwrap();
    let released = S3Ownership::status(&store).await.unwrap().unwrap();
    assert_eq!(released.state(), OwnerState::Released);
    assert_eq!(released.owner_id(), first_owner);
    let next = acquire(&store).await;
    assert_eq!(next.record().generation(), 2);
    assert_ne!(next.record().owner_id(), first_owner);
    next.release().await.unwrap();
    assert_owner_never_overwritten_or_deleted(&fake);
    assert!(fake
        .inner
        .list(Some(&Path::from(PROBE_PREFIX)))
        .next()
        .await
        .is_none());
}

#[tokio::test]
async fn concurrent_create_and_released_cas_have_only_one_winner() {
    let (fake, store) = store();
    for round in 0..2 {
        fake.faults.lock().unwrap().owner_put_barrier =
            Some(Arc::new(tokio::sync::Barrier::new(2)));
        let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                S3Ownership::acquire(store.clone(), "ingest", vec!["a".into()]),
                S3Ownership::acquire(store.clone(), "merge", vec!["b".into()]),
            )
        })
        .await
        .expect("ownership race must finish promptly");
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        fake.faults.lock().unwrap().owner_put_barrier = None;
        let winner = first.or(second).unwrap();
        assert_eq!(winner.record().generation(), round + 1);
        assert_eq!(
            S3Ownership::status(&store).await.unwrap().unwrap(),
            *winner.record()
        );
        winner.release().await.unwrap();
    }
    assert_owner_never_overwritten_or_deleted(&fake);
}

#[tokio::test]
async fn lost_success_responses_are_resolved_by_exact_records_and_versions() {
    let (fake, store) = store();
    fake.faults.lock().unwrap().lose_owner_responses = 1;
    let guard = acquire(&store).await;
    fake.faults.lock().unwrap().lose_owner_responses = 1;
    guard.release().await.unwrap();
    assert_eq!(
        S3Ownership::status(&store).await.unwrap().unwrap().state(),
        OwnerState::Released
    );
    assert_owner_never_overwritten_or_deleted(&fake);
}

#[tokio::test]
async fn ignored_or_unsupported_conditions_never_touch_the_owner_key() {
    for ignore in [true, false] {
        let (fake, store) = store();
        fake.faults.lock().unwrap().ignore_conditions = ignore;
        fake.faults.lock().unwrap().unsupported_conditions = !ignore;
        assert_eq!(
            S3Ownership::acquire(store.clone(), "ingest", vec!["a".into()])
                .await
                .unwrap_err(),
            OwnershipError::ConditionalWritesUnproven
        );
        assert!(S3Ownership::status(&store).await.unwrap().is_none());
        assert!(fake
            .writes
            .lock()
            .unwrap()
            .iter()
            .all(|(key, _)| key != OWNER_KEY));
    }
}

#[tokio::test]
async fn missing_versions_and_stale_reads_fail_closed() {
    let (fake, store) = store();
    fake.faults.lock().unwrap().hide_versions = true;
    assert!(
        S3Ownership::acquire(store.clone(), "ingest", vec!["a".into()])
            .await
            .is_err()
    );
    assert!(fake.inner.get(&Path::from(OWNER_KEY)).await.is_err());
    fake.faults.lock().unwrap().hide_versions = false;
    let guard = acquire(&store).await;
    guard.release().await.unwrap();
    let stale = read_bytes(&store, &Path::from(OWNER_KEY))
        .await
        .unwrap()
        .unwrap();
    fake.faults.lock().unwrap().stale_after_owner_put = Some(stale);
    assert_eq!(
        S3Ownership::acquire(store.clone(), "ingest", vec!["a".into()])
            .await
            .unwrap_err(),
        OwnershipError::MutationUncertain
    );
    fake.faults.lock().unwrap().stale_owner_read = None;
    assert_eq!(
        S3Ownership::status(&store).await.unwrap().unwrap().state(),
        OwnerState::Owned
    );
    assert_owner_never_overwritten_or_deleted(&fake);
}

#[tokio::test]
async fn missing_version_on_release_retains_the_owned_record() {
    let (fake, store) = store();
    let guard = acquire(&store).await;
    let expected = guard.record().clone();
    fake.faults.lock().unwrap().hide_versions = true;
    assert_eq!(
        guard.release().await.unwrap_err(),
        OwnershipError::MissingVersion
    );
    fake.faults.lock().unwrap().hide_versions = false;
    assert_eq!(
        S3Ownership::status(&store).await.unwrap().unwrap(),
        expected
    );
}

#[tokio::test]
async fn stale_release_read_and_foreign_owner_never_allow_release() {
    let (fake, store) = store();
    let guard = acquire(&store).await;
    let old = read_bytes(&store, &Path::from(OWNER_KEY))
        .await
        .unwrap()
        .unwrap();
    fake.faults.lock().unwrap().stale_after_owner_put = Some(old);
    assert_eq!(
        guard.release().await.unwrap_err(),
        OwnershipError::MutationUncertain
    );
    fake.faults.lock().unwrap().stale_owner_read = None;
    let guard = acquire(&store).await;
    let mut foreign = guard.record().clone();
    foreign.owner_id = Uuid::new_v4().to_string();
    fake.inner
        .put(
            &Path::from(OWNER_KEY),
            serde_json::to_vec(&foreign).unwrap().into(),
        )
        .await
        .unwrap();
    assert_eq!(
        guard.release().await.unwrap_err(),
        OwnershipError::StateChanged
    );
    assert_eq!(S3Ownership::status(&store).await.unwrap().unwrap(), foreign);
}

#[tokio::test]
async fn old_records_dropped_guards_and_uncertain_mutations_never_expire() {
    let (fake, store) = store();
    let guard = acquire(&store).await;
    let expected = guard.record().clone();
    drop(guard);
    fake.faults.lock().unwrap().old_modified_time = true;
    assert_eq!(
        S3Ownership::acquire(store.clone(), "merge", vec!["mainnet".into()])
            .await
            .unwrap_err(),
        OwnershipError::Busy
    );
    assert_eq!(
        S3Ownership::status(&store).await.unwrap().unwrap(),
        expected
    );
    let proof = RecoveryAuthorization::assert_provider_quiescence(
        &expected,
        "process-cessation-ticket",
        "provider-drain-confirmation",
    )
    .unwrap();
    S3Ownership::operator_release(store.clone(), &expected, proof)
        .await
        .unwrap();
    let guard = acquire(&store).await;
    let expected = guard.record().clone();
    guard.mark_mutation_uncertain();
    assert!(guard.is_mutation_uncertain());
    assert_eq!(
        guard.release().await.unwrap_err(),
        OwnershipError::DataMutationUncertain
    );
    assert_eq!(
        S3Ownership::status(&store).await.unwrap().unwrap(),
        expected
    );
}

#[tokio::test]
async fn generation_overflow_and_malformed_records_are_never_replaced() {
    let (fake, store) = store();
    let guard = acquire(&store).await;
    let mut record = guard.record().clone();
    record.state = OwnerState::Released;
    record.generation = u64::MAX;
    let bytes = serde_json::to_vec(&record).unwrap();
    fake.inner
        .put(&Path::from(OWNER_KEY), bytes.clone().into())
        .await
        .unwrap();
    assert_eq!(
        S3Ownership::acquire(store.clone(), "ingest", vec!["a".into()])
            .await
            .unwrap_err(),
        OwnershipError::GenerationExhausted
    );
    assert_eq!(
        fake.inner
            .get(&Path::from(OWNER_KEY))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        bytes
    );
    let mut invalid = serde_json::to_value(&record).unwrap();
    invalid["credential"] = "fake-secret".into();
    let mut unknown_format = serde_json::to_value(&record).unwrap();
    unknown_format["format_version"] = 2.into();
    let mut unknown_state = serde_json::to_value(&record).unwrap();
    unknown_state["state"] = "unknown".into();
    for bytes in [
        b"{broken".to_vec(),
        serde_json::to_vec(&invalid).unwrap(),
        serde_json::to_vec(&unknown_format).unwrap(),
        serde_json::to_vec(&unknown_state).unwrap(),
        vec![b'x'; MAX_RECORD_BYTES + 1],
    ] {
        fake.inner
            .put(&Path::from(OWNER_KEY), bytes.into())
            .await
            .unwrap();
        fake.faults.lock().unwrap().lie_about_size = true;
        assert_eq!(
            S3Ownership::status(&store).await.unwrap_err(),
            OwnershipError::InvalidRecord
        );
    }
}

#[tokio::test]
async fn wrong_generation_and_missing_quiescence_do_not_release() {
    let (_, store) = store();
    let guard = acquire(&store).await;
    let expected = guard.record().clone();
    drop(guard);
    assert_eq!(
        RecoveryAuthorization::assert_provider_quiescence(&expected, "process exited", "")
            .unwrap_err(),
        OwnershipError::RecoveryEvidenceRequired
    );
    assert_eq!(
        RecoveryAuthorization::assert_provider_quiescence(&expected, "", "provider drained")
            .unwrap_err(),
        OwnershipError::RecoveryEvidenceRequired
    );
    let mut wrong = expected.clone();
    wrong.generation += 1;
    let proof = RecoveryAuthorization::assert_provider_quiescence(
        &wrong,
        "process stopped",
        "provider drained",
    )
    .unwrap();
    assert_eq!(
        S3Ownership::operator_release(store.clone(), &wrong, proof)
            .await
            .unwrap_err(),
        OwnershipError::StateChanged
    );
    assert_eq!(
        S3Ownership::status(&store).await.unwrap().unwrap(),
        expected
    );
}

#[tokio::test]
async fn delayed_prior_put_requires_provider_drain_before_writing_rollback() {
    let (fake, store) = store();
    let guard = acquire(&store).await;
    let expected = guard.record().clone();
    let part = Path::from("mainnet/blocks/part-owned.parquet");
    fake.inner
        .put_opts(
            &part,
            Bytes::from_static(b"prior complete part").into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let (provider_continue, remote_wait) = tokio::sync::oneshot::channel();
    let remote = fake.clone();
    let remote_part = part.clone();
    // The request has reached the provider. Dropping the process-side guard
    // does not cancel this remote operation.
    let prior_put = tokio::spawn(async move {
        remote_wait.await.unwrap();
        remote
            .inner
            .put_opts(
                &remote_part,
                Bytes::from_static(b"prior complete part").into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
    });
    guard.mark_mutation_uncertain();
    assert_eq!(
        guard.release().await.unwrap_err(),
        OwnershipError::DataMutationUncertain
    );
    assert!(RecoveryAuthorization::assert_provider_quiescence(
        &expected,
        "old process stopped",
        ""
    )
    .is_err());
    assert_eq!(
        S3Ownership::acquire(store.clone(), "recover", vec!["mainnet".into()])
            .await
            .unwrap_err(),
        OwnershipError::Busy
    );
    // Deliberately bypass the protocol to reproduce the unsafe rollback:
    // deleting a Writing part while a prior conditional PUT is still in flight
    // makes its name available again. The protected recovery must not do this.
    fake.inner.delete(&part).await.unwrap();
    assert!(fake.inner.get(&part).await.is_err());
    provider_continue.send(()).unwrap();
    prior_put.await.unwrap();
    assert!(
        fake.inner.get(&part).await.is_ok(),
        "the old request arrived after process cessation"
    );
    // Only now can the provider's completed-request proof authorize unlocking.
    let proof = RecoveryAuthorization::assert_provider_quiescence(
        &expected,
        "confirmed old process stopped",
        "provider confirms prior request completed and no pending requests",
    )
    .unwrap();
    S3Ownership::operator_release(store.clone(), &expected, proof)
        .await
        .unwrap();
    let recovery = acquire(&store).await;
    assert_eq!(recovery.record().generation(), 2);
    // The integration verifies the Writing journal receipt before this exact
    // deletion. Ownership itself never deletes transaction data.
    fake.inner.delete(&part).await.unwrap();
    assert!(fake.inner.get(&part).await.is_err());
    recovery.release().await.unwrap();
}

#[tokio::test]
async fn errors_and_debug_omit_backend_payload_and_evidence_secrets() {
    let (fake, store) = store();
    let guard = acquire(&store).await;
    let proof = RecoveryAuthorization::assert_provider_quiescence(
        guard.record(),
        "fake-secret-process",
        "fake-secret-provider",
    )
    .unwrap();
    assert!(!format!("{guard:?} {proof:?}").contains("fake-secret"));
    fake.faults.lock().unwrap().fail_owner_read = true;
    let error = S3Ownership::status(&store).await.unwrap_err();
    assert!(!format!("{error:?} {error}").contains("fake-secret"));
    let bytes = fake
        .inner
        .get(&Path::from(OWNER_KEY))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("fake-secret"));
}

#[test]
fn bounded_canonical_requests_and_safe_versions() {
    for scope in [
        "/absolute",
        "../escape",
        "a/../b",
        "s3://bucket/key",
        "a?secret=x",
        "a//b",
        "a///",
        "a/./b",
        "a\\b",
    ] {
        assert_eq!(
            normalize_request("ingest", vec![scope.into()]).unwrap_err(),
            OwnershipError::InvalidRequest
        );
    }
    assert!(normalize_request("secret=https://x", vec!["a".into()]).is_err());
    assert!(normalize_request("ingest", vec!["a".repeat(MAX_SCOPE_BYTES + 1)]).is_err());
    assert!(normalize_request("ingest", vec!["a".into(); MAX_SCOPES + 1]).is_err());
    assert!(normalize_request(
        "ingest",
        (0..17)
            .map(|index| format!("p{index}/{}", "x".repeat(1000)))
            .collect()
    )
    .is_err());
    assert_eq!(
        normalize_request("ingest", vec!["".into()]).unwrap(),
        vec![""]
    );
    for etag in ["", "*", "a,b", "x\r\ny"] {
        assert!(!usable_version(&UpdateVersion {
            e_tag: Some(etag.into()),
            version: None
        }));
        assert!(!usable_version(&UpdateVersion {
            e_tag: Some(etag.into()),
            version: Some("valid-version".into())
        }));
    }
}

mod wire;
