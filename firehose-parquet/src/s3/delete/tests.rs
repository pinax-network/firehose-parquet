use super::test_store::*;
use super::*;
use bytes::Bytes;
use std::sync::Mutex;

mod benchmark;
mod wire;

fn keys(count: usize) -> Vec<Path> {
    (0..count)
        .map(|n| Path::from(format!("blocks/part-{n:06}.parquet")))
        .collect()
}
async fn seed(store: &DelayedStore, keys: &[Path]) {
    for key in keys {
        store
            .inner
            .put(key, Bytes::from_static(b"retained source").into())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn concurrent_deletes_preserve_the_exact_set_and_never_exceed_ten() {
    for count in [0, 1, 10, 1_001] {
        let store = Arc::new(DelayedStore::new(Duration::from_millis(1)));
        let keys = keys(count);
        seed(&store, &keys).await;
        let client: Arc<dyn ObjectStore> = store.clone();
        delete_objects_once(&client, &keys).await.unwrap();
        assert_eq!(store.counters.completed.load(Ordering::SeqCst), count);
        assert_eq!(store.counters.cancelled.load(Ordering::SeqCst), 0);
        assert_eq!(store.counters.active.load(Ordering::SeqCst), 0);
        assert_eq!(store.counters.maximum.load(Ordering::SeqCst), count.min(10));
        let requests = store.counters.started.lock().unwrap();
        assert_eq!(requests.len(), count);
        assert_eq!(
            requests.iter().collect::<HashSet<_>>(),
            keys.iter().collect()
        );
    }
}

#[tokio::test]
async fn every_key_is_validated_before_any_request() {
    for bad in [
        "",
        ".fireparq-owner-v1.json",
        "mainnet/.fireparq-ingest/state.json",
        "blocks/_fireparq_merge.json",
        ".fireparq-merge.lock",
    ] {
        let store = Arc::new(DelayedStore::new(Duration::ZERO));
        let client: Arc<dyn ObjectStore> = store.clone();
        assert!(
            delete_objects_once(&client, &[keys(1)[0].clone(), Path::from(bad)])
                .await
                .is_err()
        );
        assert!(store.counters.started.lock().unwrap().is_empty());
    }
    let store = Arc::new(DelayedStore::new(Duration::ZERO));
    let client: Arc<dyn ObjectStore> = store.clone();
    assert!(
        delete_objects_once(&client, &[keys(1)[0].clone(), keys(1)[0].clone()])
            .await
            .is_err()
    );
    assert!(store.counters.started.lock().unwrap().is_empty());
}

#[tokio::test]
async fn failure_stops_new_dispatch_and_drains_started_requests_without_retry() {
    for immediate in [false, true] {
        let mut store = DelayedStore::new(Duration::from_millis(40));
        store.fail = Some(keys(1)[0].clone());
        store.immediate_error = immediate;
        let store = Arc::new(store);
        let client: Arc<dyn ObjectStore> = store.clone();
        seed(&store, &keys(100)).await;
        assert!(delete_objects_once(&client, &keys(100)).await.is_err());
        let started = store.counters.started.lock().unwrap().clone();
        assert_eq!(started.len(), if immediate { 1 } else { 10 });
        assert_eq!(
            store.counters.completed.load(Ordering::SeqCst),
            started.len()
        );
        assert_eq!(store.counters.active.load(Ordering::SeqCst), 0);
        assert_eq!(store.counters.cancelled.load(Ordering::SeqCst), 0);
        assert_eq!(started.iter().collect::<HashSet<_>>().len(), started.len());
        assert!(store.inner.head(&keys(100)[99]).await.is_ok());
    }
}

#[tokio::test]
async fn observer_failure_drains_requests_before_returning() {
    let store = Arc::new(DelayedStore::new(Duration::from_millis(5)));
    let client: Arc<dyn ObjectStore> = store.clone();
    let mut observations = 0;
    assert!(delete_objects_observed(&client, &keys(100), |_| {
        observations += 1;
        anyhow::bail!("synthetic commit-phase interruption")
    })
    .await
    .is_err());
    assert_eq!(observations, 1);
    assert_eq!(store.counters.started.lock().unwrap().len(), 10);
    assert_eq!(store.counters.completed.load(Ordering::SeqCst), 10);
    assert_eq!(store.counters.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn timeout_is_uncertain_and_never_becomes_a_retry() {
    let store = Arc::new(DelayedStore::new(Duration::from_secs(1)));
    let client: Arc<dyn ObjectStore> = store.clone();
    assert!(delete_with_options(
        &client,
        &keys(100),
        10,
        Duration::from_millis(5),
        |_| Ok(())
    )
    .await
    .is_err());
    assert_eq!(store.counters.started.lock().unwrap().len(), 10);
    assert_eq!(store.counters.cancelled.load(Ordering::SeqCst), 10);
    assert_eq!(store.counters.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn lost_response_or_cancellation_retains_the_persistent_owner() {
    use crate::dataset_lock_s3::{OwnerState, S3Ownership};
    for cancel in [false, true] {
        let mut store = DelayedStore::new(Duration::from_millis(20));
        if !cancel {
            store.fail = Some(keys(1)[0].clone());
            store.lose_response = true;
            store.delayed_gate = Some(Arc::new(tokio::sync::Semaphore::new(0)));
        }
        let store = Arc::new(store);
        let client: Arc<dyn ObjectStore> = store.clone();
        seed(&store, &keys(100)).await;
        let owner = S3Ownership::acquire(client.clone(), "delete-test", vec!["dataset".into()])
            .await
            .unwrap();
        let operation_client = client.clone();
        let task = tokio::spawn(async move {
            delete_objects_once(&operation_client, &keys(100)).await?;
            owner.release().await?;
            Ok::<_, anyhow::Error>(())
        });
        if cancel {
            while store.counters.active.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            // The failed response arrives while nine accepted deletes are still running.
            while store.counters.completed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            assert!(store.counters.active.load(Ordering::SeqCst) > 0);
            assert!(
                !task.is_finished(),
                "the caller must keep draining after the error"
            );
            assert_eq!(
                S3Ownership::status(&client).await.unwrap().unwrap().state(),
                OwnerState::Owned
            );
            store.delayed_gate.as_ref().unwrap().add_permits(100);
            assert!(task.await.unwrap().is_err());
            assert!(
                store.inner.head(&keys(1)[0]).await.is_err(),
                "remote deletion succeeded before the lost response"
            );
        }
        assert_eq!(
            S3Ownership::status(&client).await.unwrap().unwrap().state(),
            OwnerState::Owned
        );
        assert!(
            S3Ownership::acquire(client.clone(), "replacement", vec!["dataset".into()])
                .await
                .is_err()
        );
        assert_eq!(store.counters.active.load(Ordering::SeqCst), 0);
    }
}
