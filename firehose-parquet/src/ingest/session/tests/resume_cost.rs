//! Resume cost independent of data size (#655): a resumed `build` reads its
//! control state and lists no data objects, on S3 and on local disk.
use super::*;
use crate::maintenance::discovery::paged_bucket::{GeneratedData, Pacing, PagedBucket};
use std::time::{Duration, Instant};

/// The slow page is the 501st of a full listing: only a data listing reaches it.
const SLOW_PAGE: (u64, Duration) = (500, Duration::from_millis(1_500));

fn paced() -> Pacing {
    Pacing {
        every_page: Duration::ZERO,
        slow_page: Some(SLOW_PAGE),
    }
}

async fn paged_owner(store: &Arc<PagedBucket>) -> DatasetOwnership {
    let store: Arc<dyn object_store::ObjectStore> = store.clone();
    let remote = crate::dataset_lock_s3::S3Ownership::acquire(store, "build", vec![String::new()])
        .await
        .unwrap();
    DatasetOwnership::from_remote_for_test("data", remote)
}

fn metrics() -> PipelineMetrics {
    PipelineMetrics::new(&mut prometheus_client::registry::Registry::default())
}

/// Creates the dataset of `config` in an empty bucket and commits one block.
async fn create_and_commit(store: &Arc<PagedBucket>, config: &Config) -> Digest {
    let owner = paged_owner(store).await;
    let mut session = IngestionSession::open(config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    flush(&mut session, &[100]).await;
    let checkpoint = session.authority().checkpoint.id.clone();
    drop(session);
    owner.finish(Ok(())).await.unwrap();
    checkpoint
}

/// A dataset that grew to about a million objects resumes with no data
/// listing: 0 LIST requests at the bucket root (the deployment layout), and
/// one request of the bucket root's control prefix for `s3://data/{chain}`.
/// The slow page and every data object stay untouched, and the startup
/// metrics report the same count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_resume_of_a_million_object_dataset_lists_no_data() {
    for (template, root, ancestor_requests) in
        [("s3://data", "", 0), ("s3://data/{chain}", "mainnet", 1)]
    {
        let store = Arc::new(PagedBucket::new(paced()));
        let config = bucket_root_config(template);
        let checkpoint = create_and_commit(&store, &config).await;
        let data = GeneratedData::million(root);
        let keys = data.keys();
        store.generate(Some(data));
        store.requests.reset();

        let owner = paged_owner(&store).await;
        let metrics = metrics();
        let started = Instant::now();
        let session = IngestionSession::open(
            &config,
            mapper(BlockFamily::Evm),
            &owner,
            Some(&metrics),
            None,
        )
        .await
        .unwrap();
        let elapsed = started.elapsed();
        println!(
            "{template}: {keys} generated objects, resume made {} LIST requests \
             ({} slow, {} data reads) in {elapsed:?}",
            store.requests.list_pages(),
            store.requests.slow_pages(),
            store.requests.data_reads()
        );
        assert_eq!(session.authority().checkpoint.id, checkpoint, "{template}");
        assert_eq!(keys, 1_000_000);
        assert_eq!(
            store.requests.list_pages(),
            ancestor_requests,
            "{template}: startup LIST requests"
        );
        assert_eq!(store.requests.slow_pages(), 0, "{template}");
        assert_eq!(store.requests.data_reads(), 0, "{template}");
        assert_eq!(
            metrics.startup_list_requests.get(),
            i64::try_from(ancestor_requests).unwrap(),
            "{template}"
        );
        assert!(elapsed < SLOW_PAGE.1, "{template}: {elapsed:?}");
        drop(session);
        owner.finish(Ok(())).await.unwrap();
    }
}

/// A dataset placed inside an existing one without `build` (a copy of its
/// control state) is refused from the child's side: the resume's ancestor
/// check finds the enclosing marker. The enclosing dataset itself resumes
/// without looking at descendants.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_resume_refuses_a_dataset_nested_under_an_existing_one() {
    use object_store::ObjectStore;
    let store = Arc::new(PagedBucket::new(Pacing::default()));
    let parent = bucket_root_config("s3://data");
    create_and_commit(&store, &parent).await;
    let state = store
        .get(&".fireparq-ingest/state.json".into())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    store
        .put(&"mainnet/.fireparq-ingest/state.json".into(), state.into())
        .await
        .unwrap();

    let owner = paged_owner(&store).await;
    let error = IngestionSession::open(
        &bucket_root_config("s3://data/{chain}"),
        mapper(BlockFamily::Evm),
        &owner,
        None,
        None,
    )
    .await
    .err()
    .expect("a nested copy is refused");
    assert!(
        error
            .to_string()
            .contains("overlaps another protected root"),
        "{error:#}"
    );
    store.requests.reset();
    drop(
        IngestionSession::open(&parent, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .unwrap(),
    );
    assert_eq!(store.requests.list_pages(), 0);
    owner.finish(Ok(())).await.unwrap();
}

/// Restores a directory's permissions when the test ends, even on panic.
#[cfg(unix)]
struct Restore(std::path::PathBuf);
#[cfg(unix)]
impl Drop for Restore {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
    }
}

/// The local-disk equivalent: a resume walks no data directory. The data tree
/// holds thousands of files, a symlink that every recursive walker refuses and
/// an unreadable directory; the creation-time tree check fails on it, while a
/// resume opens with no directory read at all.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_resume_walks_no_data_directory() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let owner = own(&config).await;
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    flush(&mut session, &[100]).await;
    let checkpoint = session.authority().checkpoint.id.clone();
    drop(session);
    owner.release().await.unwrap();

    let root = &config.output;
    for day in 1..=4 {
        let partition = root.join(format!("blocks/date=2020-01-0{day}"));
        std::fs::create_dir_all(&partition).unwrap();
        for part in 0..500 {
            std::fs::write(partition.join(format!("part-v1-{part:06}.parquet")), b"").unwrap();
        }
    }
    let linked = root.join("logs/date=2020-01-01");
    std::fs::create_dir_all(&linked).unwrap();
    std::os::unix::fs::symlink(temp.path(), linked.join("escape")).unwrap();
    let sealed = root.join("logs/date=2020-01-02");
    std::fs::create_dir_all(&sealed).unwrap();
    let _restore = Restore(sealed.clone());
    std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o000)).unwrap();

    // `recovery` ownership walks the tree and refuses the nested symlink;
    // `build` acquires without that walk and checks each path it touches.
    let recovery = DatasetOwnership::acquire(
        "recovery",
        vec![MutationScope::directory(root.to_string_lossy())],
        None,
    )
    .await
    .err()
    .expect("recovery ownership walks the tree");
    assert!(
        recovery.to_string().contains("nested symlinks"),
        "{recovery:#}"
    );
    let owner = DatasetOwnership::acquire_for_ingestion(
        ingestion_mutation_scopes(&config).unwrap(),
        None,
        root.to_str().unwrap(),
    )
    .await
    .unwrap();
    let identity = resolve_output_identity(root.to_str().unwrap(), &aws_config(&config)).unwrap();
    let walked = ListingStats::default();
    let error = crate::ingest::maintenance::validate_ingestion_target(
        &identity,
        &owner,
        IngestionTarget::Create,
        &walked,
    )
    .await
    .unwrap_err();
    // Whichever of the two the walk meets first refuses it.
    let error = format!("{error:#}");
    assert!(
        error.contains("symlink") || error.contains("listing protected dataset descendants"),
        "{error}"
    );
    assert!(walked.requests() > 0);

    let metrics = metrics();
    let session = IngestionSession::open(
        &config,
        mapper(BlockFamily::Evm),
        &owner,
        Some(&metrics),
        None,
    )
    .await
    .unwrap();
    assert_eq!(session.authority().checkpoint.id, checkpoint);
    assert_eq!(metrics.startup_list_requests.get(), 0);
    drop(session);
    owner.release().await.unwrap();
}

/// Locally too, a copy of a dataset's control state inside another dataset is
/// refused when resumed, by its ancestor check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_resume_refuses_a_dataset_nested_under_an_existing_one() {
    let temp = tempfile::tempdir().unwrap();
    let parent = config(temp.path());
    let owner = own(&parent).await;
    drop(
        IngestionSession::open(&parent, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .unwrap(),
    );
    owner.release().await.unwrap();
    let nested_root = parent.output.join("copied");
    let control = crate::durable_state::CONTROL_DIRECTORY;
    std::fs::create_dir_all(nested_root.join(control)).unwrap();
    std::fs::copy(
        parent.output.join(control).join("state.json"),
        nested_root.join(control).join("state.json"),
    )
    .unwrap();
    let nested = Config {
        output: nested_root,
        ..config(temp.path())
    };
    let owner = own(&nested).await;
    let error = IngestionSession::open(&nested, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .err()
        .expect("a nested copy is refused");
    assert!(
        error
            .to_string()
            .contains("overlaps another protected root"),
        "{error:#}"
    );
    owner.release().await.unwrap();
}
