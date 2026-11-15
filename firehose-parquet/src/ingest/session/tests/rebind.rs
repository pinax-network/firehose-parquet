//! `fireparq recovery rebind` (#635) through the real session: a dataset
//! copied to a new root is refused there, naming the command; after the
//! rebind it resumes there with continuous ordinals, every Delta table and
//! the cursor mirror carry the new stream, and the root it was copied from is
//! unchanged and still resumes. A rerun changes nothing, and completes a
//! rebind that stopped after its commit point. A copy that is behind its own
//! authority, of a source that changed since, or of a source still owned, is
//! refused before anything changes.
use super::*;
use crate::delta::store::{test_support::memory_for, DeltaStore};
use crate::ingest::rebind::{rebind, rebind_owned, Rebound, Root, Source, SourceReader};
use futures::StreamExt as _;
use object_store::ObjectStore as _;
use object_store_delta::{ObjectStore as _, ObjectStoreExt as _};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

const MIRROR: &str = "_fireparq/cursor.parquet";

async fn accept(session: &mut IngestionSession<'_>, numbers: &[u64]) {
    for number in numbers {
        let ordinal = receive(session, *number, 1_700_000_000, 1);
        session
            .accept_mapped(ordinal, Some(1_700_000_000), None)
            .unwrap();
    }
}

async fn ingest(session: &mut IngestionSession<'_>, numbers: &[u64]) -> CommittedFlush {
    accept(session, numbers).await;
    flush(session, numbers).await
}

async fn txn(session: &IngestionSession<'_>, table: &str) -> Option<i64> {
    session
        .delta_tables()
        .unwrap()
        .txn_version(table)
        .await
        .unwrap()
}

fn no_aws() -> crate::cli::AwsConfig {
    crate::cli::AwsConfig {
        aws_access_key_id: None,
        aws_secret_access_key: None,
        aws_session_token: None,
        aws_region: None,
        aws_endpoint_url: None,
    }
}

fn message<T>(result: Result<T>) -> String {
    format!("{:#}", result.err().expect("refused"))
}

/// Every file under `root`, by relative path.
fn tree(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, dir: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, files);
            } else {
                let relative = path.strip_prefix(root).unwrap().to_path_buf();
                files.insert(relative, std::fs::read(&path).unwrap());
            }
        }
    }
    let mut files = BTreeMap::new();
    walk(root, root, &mut files);
    files
}

/// `cp -R from to`.
fn copy_tree(from: &Path, to: &Path) {
    for (relative, bytes) in tree(from) {
        let path = to.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
}

/// The actions of a table's latest commit, by kind.
fn last_commit(table: &Path) -> (PathBuf, BTreeMap<String, Value>) {
    let log = table.join("_delta_log");
    let last = std::fs::read_dir(&log)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .max()
        .unwrap();
    let actions = std::fs::read_to_string(&last)
        .unwrap()
        .lines()
        .map(|line| {
            let action: serde_json::Map<String, Value> = serde_json::from_str(line).unwrap();
            action.into_iter().next().unwrap()
        })
        .collect();
    (last, actions)
}

fn local_config(output: PathBuf) -> Config {
    Config {
        output,
        start_block: Some(100),
        final_blocks_only: true,
        cursor_path: Some(MIRROR.into()),
        ..Default::default()
    }
}

async fn rebind_local(to: &Path, from: &Path) -> Result<Rebound> {
    rebind(to.to_str().unwrap(), from.to_str().unwrap(), &no_aws()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copied_local_dataset_resumes_at_its_new_root_after_a_rebind() {
    let dir = tempfile::tempdir().unwrap();
    let original = local_config(dir.path().join("chain"));
    let owner = own(&original).await;
    let mut session =
        IngestionSession::open(&original, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .unwrap();
    ingest(&mut session, &[100, 101]).await;
    ingest(&mut session, &[102]).await;
    let ordinal = session.authority().checkpoint.ordinal;
    assert_eq!(ordinal, 3);
    drop(session);
    drop(owner);

    let copy = local_config(dir.path().join("chain-v1"));
    copy_tree(&original.output, &copy.output);
    let before = tree(&original.output);

    // The copy is bound to the original root: refused, naming the command.
    let owner = own(&copy).await;
    let refused =
        message(IngestionSession::open(&copy, mapper(BlockFamily::Evm), &owner, None, None).await);
    assert!(refused.contains("fireparq recovery rebind"), "{refused}");
    drop(owner);

    let rebound = rebind_local(&copy.output, &original.output).await.unwrap();
    assert!(rebound.rebound_now);
    assert_eq!(rebound.ordinal, ordinal);
    assert_ne!(rebound.stream_id, rebound.previous_stream_id);
    assert_eq!(rebound.tables_updated, ["logs", "blocks"]);
    assert_eq!(rebound.mirror, "repaired");

    // One commit per table: the new descriptor, its `txn` at authority, and
    // the audit record; no file added or removed.
    for table in ["blocks", "logs"] {
        let (_, actions) = last_commit(&copy.output.join(table));
        let configuration = &actions["metaData"]["configuration"];
        assert_eq!(configuration["fireparq.descriptor"], rebound.stream_id);
        assert_eq!(
            actions["txn"]["appId"],
            format!("fireparq:{}", rebound.stream_id)
        );
        assert_eq!(actions["txn"]["version"], ordinal);
        let audit = &actions["commitInfo"]["fireparq.rebind"];
        assert_eq!(audit["previousDescriptor"], rebound.previous_stream_id);
        assert_eq!(audit["ordinal"], ordinal);
        assert!(!actions.contains_key("add") && !actions.contains_key("remove"));
    }

    // A rerun changes nothing.
    let again = rebind_local(&copy.output, &original.output).await.unwrap();
    assert!(!again.rebound_now);
    assert!(again.tables_updated.is_empty());
    assert_eq!(again.mirror, "unchanged");
    assert_eq!(again.stream_id, rebound.stream_id);

    // The copy resumes where the original stopped, under its new stream.
    let owner = own(&copy).await;
    let mut session = IngestionSession::open(&copy, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(session.authority().checkpoint.ordinal, ordinal);
    assert_eq!(
        session.delta_tables().unwrap().app_id(),
        format!("fireparq:{}", rebound.stream_id)
    );
    assert_eq!(ingest(&mut session, &[103]).await.ordinal, ordinal + 1);
    for table in ["blocks", "logs"] {
        assert_eq!(txn(&session, table).await, Some(4));
    }
    drop(session);
    drop(owner);

    // The original was only read, and still resumes at its own root.
    assert_eq!(tree(&original.output), before);
    let owner = own(&original).await;
    let mut session =
        IngestionSession::open(&original, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .unwrap();
    assert_eq!(ingest(&mut session, &[103]).await.ordinal, ordinal + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rerun_completes_a_rebind_interrupted_after_its_commit_point() {
    let dir = tempfile::tempdir().unwrap();
    let original = local_config(dir.path().join("chain"));
    let owner = own(&original).await;
    let mut session =
        IngestionSession::open(&original, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .unwrap();
    ingest(&mut session, &[100]).await;
    drop(session);
    drop(owner);
    let copy = local_config(dir.path().join("chain-v1"));
    copy_tree(&original.output, &copy.output);
    let copied_mirror = std::fs::read(copy.output.join(MIRROR)).unwrap();
    let rebound = rebind_local(&copy.output, &original.output).await.unwrap();

    // As if it stopped after `state.json`: `blocks` and the mirror still
    // hold the previous stream. `build` refuses the copy meanwhile.
    let (blocks_rebind, _) = last_commit(&copy.output.join("blocks"));
    std::fs::remove_file(blocks_rebind).unwrap();
    std::fs::write(copy.output.join(MIRROR), &copied_mirror).unwrap();
    let owner = own(&copy).await;
    let refused =
        message(IngestionSession::open(&copy, mapper(BlockFamily::Evm), &owner, None, None).await);
    assert!(refused.contains("belongs to another stream"), "{refused}");
    drop(owner);

    let completed = rebind_local(&copy.output, &original.output).await.unwrap();
    assert!(!completed.rebound_now);
    assert_eq!(completed.tables_updated, ["blocks"]);
    assert_eq!(completed.mirror, "repaired");
    assert_eq!(completed.stream_id, rebound.stream_id);

    let owner = own(&copy).await;
    let mut session = IngestionSession::open(&copy, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(ingest(&mut session, &[101]).await.ordinal, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incomplete_copy_or_a_source_that_moved_on_is_refused_before_any_change() {
    let dir = tempfile::tempdir().unwrap();
    let original = local_config(dir.path().join("chain"));
    let owner = own(&original).await;
    let mut session =
        IngestionSession::open(&original, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .unwrap();
    ingest(&mut session, &[100]).await;
    ingest(&mut session, &[101]).await;
    drop(session);
    drop(owner);

    // `blocks` lost its last commit in the copy: behind authority.
    let behind = local_config(dir.path().join("behind"));
    copy_tree(&original.output, &behind.output);
    let (last, _) = last_commit(&behind.output.join("blocks"));
    std::fs::remove_file(last).unwrap();
    let copied = tree(&behind.output);
    let refused = message(rebind_local(&behind.output, &original.output).await);
    assert!(refused.contains("behind authority"), "{refused}");
    assert_eq!(tree(&behind.output), copied, "nothing was changed");

    // The source accepted another transaction after the copy.
    let stale = local_config(dir.path().join("stale"));
    copy_tree(&original.output, &stale.output);
    let owner = own(&original).await;
    let mut session =
        IngestionSession::open(&original, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .unwrap();
    ingest(&mut session, &[102]).await;
    drop(session);
    drop(owner);
    let copied = tree(&stale.output);
    let refused = message(rebind_local(&stale.output, &original.output).await);
    assert!(
        refused.contains("has changed since it was copied"),
        "{refused}"
    );
    assert_eq!(tree(&stale.output), copied, "nothing was changed");

    // The source is not where the copy came from.
    let elsewhere = dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    let refused = message(rebind_local(&stale.output, &elsewhere).await);
    assert!(refused.contains("not to"), "{refused}");

    // A writer at the source holds its lock.
    let owner = own(&original).await;
    let refused = message(rebind_local(&stale.output, &original.output).await);
    assert!(!refused.is_empty());
    drop(owner);
    assert_eq!(tree(&stale.output), copied, "nothing was changed");
}

/// A bucket as `aws s3 sync` copies it: every object of the control store
/// and of its Delta store.
async fn sync_bucket(
    from: &Arc<object_store::memory::InMemory>,
    to: &Arc<object_store::memory::InMemory>,
) {
    let objects: Vec<_> = from.list(None).collect().await;
    for object in objects {
        let location = object.unwrap().location;
        let bytes = from.get(&location).await.unwrap().bytes().await.unwrap();
        to.put(&location, bytes.into()).await.unwrap();
    }
    let (from, to) = (delta_memory(from), delta_memory(to));
    let objects: Vec<_> = from.list(None).collect().await;
    for object in objects {
        let location = object.unwrap().location;
        let bytes = from.get(&location).await.unwrap().bytes().await.unwrap();
        to.put(&location, bytes.into()).await.unwrap();
    }
}

fn delta_memory(
    bucket: &Arc<object_store::memory::InMemory>,
) -> Arc<object_store_delta::memory::InMemory> {
    let bucket: Arc<dyn object_store::ObjectStore> = bucket.clone();
    memory_for(&bucket)
}

/// Every object of a bucket and of its Delta store, by key.
async fn bucket_contents(store: &Arc<object_store::memory::InMemory>) -> BTreeMap<String, Vec<u8>> {
    let mut contents = BTreeMap::new();
    let objects: Vec<_> = store.list(None).collect().await;
    for object in objects {
        let location = object.unwrap().location;
        let bytes = store.get(&location).await.unwrap().bytes().await.unwrap();
        contents.insert(format!("control/{location}"), bytes.to_vec());
    }
    let delta = delta_memory(store);
    let objects: Vec<_> = delta.list(None).collect().await;
    for object in objects {
        let location = object.unwrap().location;
        let bytes = delta.get(&location).await.unwrap().bytes().await.unwrap();
        contents.insert(format!("delta/{location}"), bytes.to_vec());
    }
    contents
}

fn remote(output: &str) -> Config {
    Config {
        output: output.into(),
        start_block: Some(100),
        cursor_path: Some(MIRROR.into()),
        ..Default::default()
    }
}

async fn owner_of(store: &Arc<object_store::memory::InMemory>, bucket: &str) -> DatasetOwnership {
    let remote =
        crate::dataset_lock_s3::S3Ownership::acquire(store.clone(), "build", vec!["chain".into()])
            .await
            .unwrap();
    DatasetOwnership::from_remote_for_test(bucket, remote)
}

/// `fireparq recovery rebind s3://data-v1/chain --from s3://data/chain`,
/// with the test buckets.
async fn rebind_remote(
    copy: &Arc<object_store::memory::InMemory>,
    original: &Arc<object_store::memory::InMemory>,
) -> Result<Rebound> {
    let aws = no_aws();
    let remote =
        crate::dataset_lock_s3::S3Ownership::acquire(copy.clone(), "rebind", vec!["chain".into()])
            .await?;
    let ownership = DatasetOwnership::from_remote_for_test("data-v1", remote);
    let control: Arc<dyn object_store::ObjectStore> = original.clone();
    let source = Source {
        root: Root::resolve("s3://data/chain", &aws)?,
        reader: SourceReader::S3 {
            delta: DeltaStore::s3("data", "chain", memory_for(&control))?,
            control,
        },
    };
    let result = rebind_owned(
        &Root::resolve("s3://data-v1/chain", &aws)?,
        &source,
        &ownership,
        &aws,
    )
    .await;
    ownership.finish(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dataset_synced_to_another_bucket_resumes_there_after_a_rebind() {
    let original = Arc::new(object_store::memory::InMemory::new());
    let copy = Arc::new(object_store::memory::InMemory::new());
    let config = remote("s3://data/chain");
    let owner = owner_of(&original, "data").await;
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    ingest(&mut session, &[100]).await;
    ingest(&mut session, &[101]).await;
    drop(session);
    owner.finish(Ok(())).await.unwrap();
    sync_bucket(&original, &copy).await;

    let copied = remote("s3://data-v1/chain");
    let owner = owner_of(&copy, "data-v1").await;
    let refused = message(
        IngestionSession::open(&copied, mapper(BlockFamily::Evm), &owner, None, None).await,
    );
    assert!(
        refused.contains("fireparq recovery rebind s3://data-v1/chain --from s3://data/chain"),
        "{refused}"
    );
    owner.finish(Ok(())).await.unwrap();

    // A writer holding the source's bucket: refused.
    let held = crate::dataset_lock_s3::S3Ownership::acquire(
        original.clone(),
        "build",
        vec!["chain".into()],
    )
    .await
    .unwrap();
    let refused = message(rebind_remote(&copy, &original).await);
    assert!(
        refused.contains("is owned (operation `build`)"),
        "{refused}"
    );
    held.release().await.unwrap();
    let before = bucket_contents(&original).await;

    let rebound = rebind_remote(&copy, &original).await.unwrap();
    assert!(rebound.rebound_now);
    assert_eq!(rebound.ordinal, 2);
    assert_eq!(rebound.tables_updated, ["logs", "blocks"]);
    assert_eq!(rebound.mirror, "repaired");
    let again = rebind_remote(&copy, &original).await.unwrap();
    assert!(!again.rebound_now && again.tables_updated.is_empty());
    assert_eq!(again.mirror, "unchanged");

    let owner = owner_of(&copy, "data-v1").await;
    let mut session = IngestionSession::open(&copied, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(ingest(&mut session, &[102]).await.ordinal, 3);
    for table in ["blocks", "logs"] {
        assert_eq!(txn(&session, table).await, Some(3));
    }
    drop(session);
    owner.finish(Ok(())).await.unwrap();
    assert_eq!(
        bucket_contents(&original).await,
        before,
        "the source was only read"
    );

    // A copy made while the source's owner was held carries that owner, and
    // its own ownership refuses the rebind.
    let held_copy = Arc::new(object_store::memory::InMemory::new());
    let held = crate::dataset_lock_s3::S3Ownership::acquire(
        original.clone(),
        "build",
        vec!["chain".into()],
    )
    .await
    .unwrap();
    sync_bucket(&original, &held_copy).await;
    assert!(rebind_remote(&held_copy, &original).await.is_err());
    held.release().await.unwrap();
}
