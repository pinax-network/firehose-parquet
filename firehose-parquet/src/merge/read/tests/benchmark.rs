//! Snapshot-read scheduling measurements; all source objects are in memory.
use super::*;

#[test]
#[ignore = "explicit serialized read latency benchmark; requires FIREPARQ_READ_BENCH_OUTPUT"]
fn bounded_read_window_benchmark() {
    let output =
        std::env::var("FIREPARQ_READ_BENCH_OUTPUT").expect("explicit result path required");
    let mib = 1024 * 1024;
    let scenarios = [
        ("128 small objects", vec![64 * 1024; 128]),
        (
            "mixed sizes under byte budget",
            [4 * mib, 24 * mib, 24 * mib, 20 * mib].repeat(3),
        ),
        ("one oversized input", vec![65 * mib, 4 * mib, 4 * mib]),
    ];
    let mut measurements = Vec::new();
    for (scenario, sizes) in scenarios {
        for sample in 0..3 {
            for limit in if sample % 2 == 0 { [1, 4] } else { [4, 1] } {
                let store = Arc::new(Store {
                    delay_ms: 3,
                    ..Default::default()
                });
                let client: Arc<dyn ObjectStore> = store.clone();
                let listed = crate::cli::block_on_async(fixture(&store, &sizes));
                let windows = Windows {
                    remaining: &listed,
                    max_active: limit,
                    budget: WINDOW_BYTES,
                };
                let mut elapsed = Duration::ZERO;
                let mut bytes_read = 0u64;
                let mut maximum_window = 0u64;
                let mut window_count = 0;
                for window in windows {
                    let window = window.unwrap();
                    let reserved = window.iter().map(|meta| meta.size).sum::<u64>();
                    maximum_window = maximum_window.max(reserved);
                    window_count += 1;
                    let started = std::time::Instant::now();
                    let data = objects(&client, window).unwrap();
                    elapsed += started.elapsed();
                    assert_eq!(store.active.load(Ordering::SeqCst), 0);
                    assert_eq!(
                        data.iter().map(|bytes| bytes.len() as u64).sum::<u64>(),
                        reserved
                    );
                    for (meta, bytes) in window.iter().zip(data) {
                        let expected = crate::cli::block_on_async(async {
                            store
                                .inner
                                .get(&meta.location)
                                .await
                                .unwrap()
                                .bytes()
                                .await
                                .unwrap()
                        });
                        assert_eq!(bytes, expected);
                        bytes_read += bytes.len() as u64;
                    }
                }
                assert_eq!(
                    bytes_read,
                    sizes.iter().map(|size| *size as u64).sum::<u64>()
                );
                assert_eq!(store.requests.lock().unwrap().len(), sizes.len());
                assert!(store.maximum.load(Ordering::SeqCst) <= limit);
                measurements.push(serde_json::json!({
                    "scenario":scenario,"sample":sample,"request_limit":limit,"objects":sizes.len(),
                    "elapsed_read_window_seconds":elapsed.as_secs_f64(),"bytes_read":bytes_read,
                    "windows":window_count,"maximum_window_reserved_bytes":maximum_window,
                    "maximum_active_gets":store.maximum.load(Ordering::SeqCst)
                }));
            }
        }
    }
    let evidence = serde_json::json!({
        "benchmark":"ordered pinned GET windows in a delayed InMemory store",
        "synthetic_delay_milliseconds":3,"first_object_delay_multiplier":4,
        "window_byte_budget":WINDOW_BYTES,
        "clock":"sum of monotonic read-window durations; seeding and exact full-byte checks excluded",
        "runtime":"synchronous bridge to Tokio current-thread; no external data",
        "measurements":measurements
    });
    std::fs::write(output, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
}
