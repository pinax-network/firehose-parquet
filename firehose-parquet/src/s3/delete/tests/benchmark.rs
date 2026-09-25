//! Scheduling benchmark only: synthetic request delay, no network or production objects.
use super::*;
use futures::TryStreamExt;

#[tokio::test]
#[ignore = "explicit serialized synthetic latency benchmark; requires FIREPARQ_DELETE_BENCH_OUTPUT"]
async fn delayed_store_deletion_benchmark() {
    let output =
        std::env::var("FIREPARQ_DELETE_BENCH_OUTPUT").expect("explicit result path required");
    let mut measurements = Vec::new();
    for count in [100, 1_000, 10_000] {
        for sample in 0..3 {
            let order = if sample % 2 == 0 { [1, 10] } else { [10, 1] };
            for concurrency in order {
                let store = Arc::new(DelayedStore::new(Duration::from_millis(1)));
                let keys = keys(count);
                seed(&store, &keys).await;
                let client: Arc<dyn ObjectStore> = store.clone();
                let started = std::time::Instant::now();
                delete_with_options(&client, &keys, concurrency, REQUEST_DEADLINE, |_| Ok(()))
                    .await
                    .unwrap();
                let elapsed = started.elapsed().as_secs_f64();
                let completed = store.counters.completed.load(Ordering::SeqCst);
                let maximum = store.counters.maximum.load(Ordering::SeqCst);
                assert_eq!(completed, count);
                assert_eq!(store.counters.started.lock().unwrap().len(), count);
                assert_eq!(store.counters.cancelled.load(Ordering::SeqCst), 0);
                assert_eq!(store.counters.active.load(Ordering::SeqCst), 0);
                assert_eq!(maximum, concurrency);
                assert!(store
                    .inner
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                    .is_empty());
                measurements.push(serde_json::json!({
                    "keys": count, "sample": sample, "concurrency": concurrency,
                    "elapsed_seconds": elapsed, "delete_calls": completed, "maximum_active": maximum
                }));
            }
        }
    }
    let evidence = serde_json::json!({
        "benchmark": "individual DELETE scheduling with a delayed InMemory store",
        "synthetic_delay_milliseconds": 1,
        "clock": "Instant monotonic wall time; seed and exact-result checks excluded",
        "runtime": "Tokio current-thread; no network or external bucket",
        "measurements": measurements
    });
    std::fs::write(output, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
}
