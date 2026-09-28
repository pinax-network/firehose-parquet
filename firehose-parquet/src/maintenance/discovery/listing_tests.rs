//! Paged listings with per-request timeouts and no total deadline (#655).
use super::paged_bucket::{GeneratedData, Pacing, PagedBucket};
use super::*;
use futures::TryStreamExt;

fn small_data() -> GeneratedData {
    GeneratedData {
        root: String::new(),
        tables: vec!["blocks".into()],
        days: 5,
        parts_per_day: 1_000,
    }
}

async fn count_all(
    store: &PagedBucket,
    timeout: Duration,
    stats: &ListingStats,
) -> anyhow::Result<u64> {
    let mut objects = 0;
    visit_objects(
        store,
        None,
        "listing fixture objects",
        timeout,
        stats,
        |_| {
            objects += 1;
            Ok(ControlFlow::<()>::Continue(()))
        },
    )
    .await?;
    Ok(objects)
}

/// Each page must arrive within the timeout, but the listing as a whole has
/// no deadline: 6 pages of 60 ms take 360 ms against a 200 ms bound.
#[tokio::test]
async fn a_listing_longer_than_the_request_timeout_completes() {
    let store = PagedBucket::new(Pacing {
        every_page: Duration::from_millis(60),
        slow_page: None,
    });
    store.generate(Some(small_data()));
    let stats = ListingStats::default();
    let started = Instant::now();
    let objects = count_all(&store, Duration::from_millis(200), &stats)
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert_eq!(objects, 5_000);
    // 5 full pages, then the empty page that ends the listing.
    assert_eq!(store.requests.list_pages(), 6);
    assert_eq!((stats.requests(), stats.objects()), (5, 5_000));
    assert!(stats.duration() >= Duration::from_millis(300));
}

/// One page slower than the bound fails the listing with a message that
/// names the per-request timeout and how far it got, never a key.
#[tokio::test]
async fn one_page_slower_than_the_request_timeout_fails_clearly() {
    let store = PagedBucket::new(Pacing {
        every_page: Duration::ZERO,
        slow_page: Some((3, Duration::from_millis(400))),
    });
    store.generate(Some(small_data()));
    let stats = ListingStats::default();
    let error = count_all(&store, Duration::from_millis(100), &stats)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("listing fixture objects timed out")
            && error.contains("one listing request took longer than 100ms")
            && error.contains("after 3000 objects"),
        "{error}"
    );
    assert!(!error.contains("part-v1-"), "{error}");
    assert_eq!(store.requests.slow_pages(), 1);
    assert_eq!(stats.objects(), 3_000);
}

/// A listing that stops early fetches only the pages it read.
#[tokio::test]
async fn an_early_break_requests_only_the_pages_read() {
    let store = PagedBucket::new(Pacing::default());
    store.generate(Some(small_data()));
    let stats = ListingStats::default();
    let found = visit_objects(
        &store,
        None,
        "listing fixture objects",
        LIST_REQUEST_TIMEOUT,
        &stats,
        |object| {
            Ok(
                if object.location.as_ref().ends_with("part-v1-000010.parquet") {
                    ControlFlow::Break(object.location)
                } else {
                    ControlFlow::Continue(())
                },
            )
        },
    )
    .await
    .unwrap();
    assert_eq!(
        found.unwrap().as_ref(),
        "blocks/date=2020-01-01/part-v1-000010.parquet"
    );
    assert_eq!(store.requests.list_pages(), 1);
    assert_eq!((stats.requests(), stats.objects()), (1, 11));
}

/// The fixture bucket itself: prefixes match whole path segments, generated
/// tables outside a prefix are neither listed nor counted, and the million
/// generated keys come in lexicographic order.
#[tokio::test]
async fn the_paged_bucket_lists_whole_segments_in_order() {
    let store = PagedBucket::new(Pacing::default());
    let data = GeneratedData::million("");
    assert_eq!(data.keys(), 1_000_000);
    store.generate(Some(data));
    for (prefix, expected) in [
        (".fireparq-ingest", 0_u64),
        ("block", 0),
        ("blocks/date=2020-01-02", 1_000),
        ("logs", 250_000),
    ] {
        store.requests.reset();
        let objects: Vec<_> = store
            .list(Some(&ObjectPath::from(prefix)))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(objects.len() as u64, expected, "{prefix}");
        assert!(
            objects
                .windows(2)
                .all(|pair| pair[0].location < pair[1].location),
            "{prefix}"
        );
        assert_eq!(
            store.requests.list_pages(),
            expected / LIST_PAGE_KEYS + 1,
            "{prefix}"
        );
    }
}
