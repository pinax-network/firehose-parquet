//! `DatasetOwnership::finish` decisions against a stateful in-memory bucket.
use super::*;
use crate::dataset_lock_s3::{OwnerState, OWNER_KEY};
use object_store::{memory::InMemory, ObjectStore};

async fn owner(store: &Arc<dyn ObjectStore>, scope: &str) -> S3Ownership {
    S3Ownership::acquire(store.clone(), "build", vec![scope.into()])
        .await
        .unwrap()
}

fn one(bucket: &str, owner: S3Ownership) -> DatasetOwnership {
    DatasetOwnership::from_remote_for_test(bucket, owner)
}

async fn state(store: &Arc<dyn ObjectStore>) -> OwnerState {
    S3Ownership::status(store).await.unwrap().unwrap().state()
}

/// The error Blocks returns when the provider rejects the stream's credentials.
fn unauthenticated() -> anyhow::Error {
    anyhow::Error::new(tonic::Status::unauthenticated("invalid API key"))
        .context("Firehose Blocks request failed")
}

#[tokio::test]
async fn failure_before_any_mutation_releases_and_the_bucket_is_immediately_writable() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let guard = owner(&store, "dataset/mainnet").await;
    let error = one("bucket", guard)
        .finish::<()>(Err(unauthenticated()))
        .await
        .unwrap_err();
    // The command's own error is returned unchanged.
    assert_eq!(
        format!("{error:#}"),
        format!("{:#}", unauthenticated()),
        "{error:#}"
    );
    assert!(error.chain().any(|cause| cause.is::<tonic::Status>()));
    assert_eq!(state(&store).await, OwnerState::Released);
    let next = owner(&store, "dataset/mainnet").await;
    assert_eq!(next.record().generation(), 2);
    one("bucket", next).finish(Ok(())).await.unwrap();
}

#[tokio::test]
async fn uncertain_failure_keeps_owner_and_names_the_exact_commands() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let guard = owner(&store, "dataset/mainnet").await;
    let record = guard.record().clone();
    guard.mark_mutation_uncertain();
    let error = one("bucket", guard)
        .finish::<()>(Err(anyhow::anyhow!("protected part upload timed out")))
        .await
        .unwrap_err();
    let message = format!("{error:?}");
    for expected in [
        "S3 bucket ownership was retained; every other writing command on the bucket fails with \"bucket ownership is held\"".to_string(),
        format!(
            "- s3://bucket/dataset/mainnet: owner {}, generation 1, kept because a request to this bucket had an uncertain outcome",
            record.owner_id()
        ),
        "  Inspect: fireparq recovery status s3://bucket/dataset/mainnet\n".into(),
        format!(
            "  Release: fireparq recovery release s3://bucket/dataset/mainnet --expected-owner {} --expected-generation 1 --stopped-writer-evidence <reference> --provider-quiescence-evidence <reference>\n",
            record.owner_id()
        ),
        "process exit alone is not that evidence".into(),
        // The cause is kept below the guidance.
        "Caused by:\n    protected part upload timed out".into(),
    ] {
        assert!(message.contains(&expected), "missing {expected:?} in {message}");
    }
    assert_eq!(S3Ownership::status(&store).await.unwrap(), Some(record));
    assert!(matches!(
        S3Ownership::acquire(store, "build", vec!["other".into()]).await,
        Err(OwnershipError::Busy)
    ));
}

#[tokio::test]
async fn success_releases_but_an_uncertain_success_is_refused_with_guidance() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    assert_eq!(
        one("bucket", owner(&store, "data").await)
            .finish(Ok(7))
            .await
            .unwrap(),
        7
    );
    assert_eq!(state(&store).await, OwnerState::Released);
    let guard = owner(&store, "data").await;
    guard.mark_mutation_uncertain();
    let error = one("bucket", guard).finish(Ok(7)).await.unwrap_err();
    assert!(format!("{error}").contains("fireparq recovery release s3://bucket/data "));
    assert_eq!(state(&store).await, OwnerState::Owned);
}

#[tokio::test]
async fn failed_release_request_keeps_owner_and_reports_why() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let guard = owner(&store, "").await;
    // The owner record no longer reads back as this guard's exact record.
    store
        .put(&object_store::path::Path::from(OWNER_KEY), "{}".into())
        .await
        .unwrap();
    let error = one("bucket", guard)
        .finish::<()>(Err(anyhow::anyhow!("stream failed")))
        .await
        .unwrap_err();
    let message = format!("{error:#}");
    assert!(
        message.contains(
            "kept because releasing the owner record failed (bucket ownership record is malformed"
        ),
        "{message}"
    );
    assert!(message.contains("only the owner record is in doubt"));
    // An empty scope is the bucket root.
    assert!(message.contains("fireparq recovery status s3://bucket\n"));
}

#[tokio::test]
async fn each_bucket_is_decided_by_its_own_latch() {
    let output: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cursor: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let output_owner = owner(&output, "dataset").await;
    let cursor_owner = owner(&cursor, "cursors/mainnet.parquet").await;
    cursor_owner.mark_mutation_uncertain();
    let ownership = DatasetOwnership {
        local: None,
        remote: BTreeMap::from([
            ("cursors".to_owned(), cursor_owner),
            ("data".to_owned(), output_owner),
        ]),
    };
    let error = ownership
        .finish::<()>(Err(anyhow::anyhow!("mirror upload timed out")))
        .await
        .unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("s3://cursors/cursors/mainnet.parquet: owner"));
    assert!(!message.contains("s3://data/"), "{message}");
    assert_eq!(state(&output).await, OwnerState::Released);
    assert_eq!(state(&cursor).await, OwnerState::Owned);
}

#[tokio::test]
async fn a_panic_or_drop_without_finish_keeps_the_remote_owner() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ownership = one("bucket", owner(&store, "dataset").await);
    let panicked = tokio::spawn(async move {
        let _held = ownership;
        panic!("injected panic while holding ownership");
    })
    .await
    .unwrap_err();
    assert!(panicked.is_panic());
    // Unwinding dropped the guard: no release was sent and writers stay blocked.
    assert_eq!(state(&store).await, OwnerState::Owned);
    assert!(matches!(
        S3Ownership::acquire(store, "build", vec!["dataset".into()]).await,
        Err(OwnershipError::Busy)
    ));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn local_only_failure_returns_the_error_unchanged_and_frees_the_os_lock() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("output");
    std::fs::create_dir(&root).unwrap();
    let ownership = DatasetOwnership::acquire(
        "build",
        vec![MutationScope::directory(root.to_string_lossy())],
        None,
    )
    .await
    .unwrap();
    assert!(LocalOwnership::acquire(std::slice::from_ref(&root)).is_err());
    let error = ownership
        .finish::<()>(Err(unauthenticated()))
        .await
        .unwrap_err();
    assert_eq!(format!("{error:#}"), format!("{:#}", unauthenticated()));
    reacquire_after_release(&root);
}

/// `finish` drops the local guard before it returns, but the lock is a
/// `flock` on the directory's open file description. Other tests in this
/// binary spawn child processes, and a child forked in the window before it
/// execs holds a copy of every descriptor, so the lock can outlive the drop
/// for a moment. Retry briefly: the lock must be free, just not always on the
/// first attempt.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn reacquire_after_release(root: &std::path::Path) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match LocalOwnership::acquire(&[root.to_path_buf()]) {
            Ok(_) => return,
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(error) => panic!("the OS lock was not freed within 5 s: {error:#}"),
        }
    }
}
