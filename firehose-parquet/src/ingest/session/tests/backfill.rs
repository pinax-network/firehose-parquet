//! #725: lane sessions fill one dataset's tables side by side, and the seal
//! leaves an ordinary dataset that a `build` session continues.
use super::*;
use crate::dataset_lock_s3::{LaneScope, S3Ownership};
use crate::ingest::backfill::{open_backfill, seal_owned};
use object_store::memory::InMemory;

const ROOT: &str = "s3://data/chain";
const TIME: i64 = 1_700_000_000;

/// The dataset: blocks from 100, no cursor mirror.
fn dataset(stop: Option<u64>) -> Config {
    Config {
        output: ROOT.into(),
        start_block: Some(100),
        stop_block: stop,
        cursor_path: None,
        ..Default::default()
    }
}

fn lane(start: u64, stop: u64) -> Config {
    Config {
        start_block: Some(start),
        backfill_lane: Some((start, stop)),
        ..dataset(Some(stop))
    }
}

async fn bucket_owner(store: &Arc<InMemory>) -> DatasetOwnership {
    let owner = S3Ownership::acquire(store.clone(), "build", vec!["chain".into()])
        .await
        .unwrap();
    DatasetOwnership::from_remote_for_test("data", owner)
}

async fn lane_owner(store: &Arc<InMemory>, start: u64, stop: u64) -> DatasetOwnership {
    let owner = S3Ownership::acquire_lane(
        store.clone(),
        LaneScope::new("chain", start, stop).unwrap(),
        "backfill-lane",
        vec!["chain".into()],
    )
    .await
    .unwrap();
    DatasetOwnership::from_remote_for_test("data", owner)
}

async fn open<'a>(config: &Config, owner: &'a DatasetOwnership) -> Result<IngestionSession<'a>> {
    IngestionSession::open(config, mapper(BlockFamily::Evm), owner, None, None).await
}

/// One block, one transaction.
async fn fill(session: &mut IngestionSession<'_>, block: u64) -> CommittedFlush {
    let ordinal = receive(session, block, TIME, 1);
    session.accept_mapped(ordinal, Some(TIME), None).unwrap();
    flush(session, &[block]).await
}

async fn seal(owner: &DatasetOwnership) -> Result<crate::ingest::BackfillSealed> {
    seal_owned(ROOT, "data", "chain", &aws_config(&dataset(None)), owner).await
}

fn refusal<T>(result: Result<T>) -> String {
    match result {
        Ok(_) => panic!("expected a refusal"),
        Err(error) => format!("{error:#}"),
    }
}

#[tokio::test]
async fn lanes_share_the_tables_and_the_seal_leaves_an_ordinary_dataset() {
    let store = Arc::new(InMemory::new());

    // Open [100, 104) in two lanes. Until the seal an ordinary session is
    // refused, and so is a backfill with another shape.
    let owner = bucket_owner(&store).await;
    let opened = open_backfill(&dataset(Some(104)), mapper(BlockFamily::Evm), &owner, 2)
        .await
        .unwrap();
    assert!(opened.created);
    assert_eq!((opened.start, opened.stop, opened.lanes), (100, 104, 2));
    let reopened = open_backfill(&dataset(Some(104)), mapper(BlockFamily::Evm), &owner, 2)
        .await
        .unwrap();
    assert!(!reopened.created);
    assert!(
        refusal(open_backfill(&dataset(Some(106)), mapper(BlockFamily::Evm), &owner, 2).await)
            .contains("another backfill is already open")
    );
    assert!(refusal(open(&dataset(None), &owner).await).contains("not sealed"));
    assert!(refusal(seal(&owner).await).contains("100-102 has not started"));
    owner.release().await.unwrap();

    // A lane must be planned, and run with the backfill's settings.
    let stray = lane_owner(&store, 101, 103).await;
    assert!(refusal(open(&lane(101, 103), &stray).await).contains("not a lane"));
    stray.release().await.unwrap();
    let other = lane_owner(&store, 100, 102).await;
    let solana = IngestionSession::open(
        &lane(100, 102),
        mapper(BlockFamily::Solana),
        &other,
        None,
        None,
    )
    .await;
    assert!(refusal(solana).contains("differ from the ones the backfill was opened with"));
    other.release().await.unwrap();

    // Both lanes are open at once and commit to the same tables in any order.
    let first_owner = lane_owner(&store, 100, 102).await;
    let second_owner = lane_owner(&store, 102, 104).await;
    let mut first = open(&lane(100, 102), &first_owner).await.unwrap();
    let mut second = open(&lane(102, 104), &second_owner).await.unwrap();
    fill(&mut second, 102).await;
    fill(&mut first, 100).await;
    fill(&mut first, 101).await;
    let last = fill(&mut second, 103).await;
    // Ordinals are each lane's own, and so is its `txn`.
    assert_eq!(last.ordinal, 2);
    let (first_app, second_app) = (
        first.delta_tables().unwrap().app_id(),
        second.delta_tables().unwrap().app_id(),
    );
    assert_ne!(first_app, second_app);
    for session in [&first, &second] {
        let tables = session.delta_tables().unwrap();
        assert_eq!(tables.txn_version("blocks").await.unwrap(), Some(2));
    }
    assert!(first.complete_request(102, true).await.unwrap());
    drop(first);
    first_owner.release().await.unwrap();

    // The seal waits for every lane.
    let sealer = bucket_owner(&store).await;
    assert!(refusal(seal(&sealer).await).contains("102-104 is running"));
    sealer.release().await.unwrap();
    assert!(second.complete_request(104, true).await.unwrap());
    drop(second);
    let sealer = bucket_owner(&store).await;
    assert!(
        refusal(seal(&sealer).await).contains("102-104 is complete but its owner is not released")
    );
    second_owner.release().await.unwrap();
    let sealed = seal(&sealer).await.unwrap();
    assert!(sealed.sealed_now);
    assert_eq!(
        (sealed.start, sealed.stop, sealed.last_block),
        (100, 104, 103)
    );
    assert!(!seal(&sealer).await.unwrap().sealed_now);
    sealer.release().await.unwrap();

    // An ordinary session continues after the last lane, under the dataset's
    // own application id and above every lane's ordinal.
    let owner = bucket_owner(&store).await;
    let mut live = open(&dataset(None), &owner).await.unwrap();
    assert_eq!(live.resume_cursor(), Some("private-cursor-103-1"));
    assert_eq!(live.authority().checkpoint.ordinal, 4);
    assert_eq!(live.authority().descriptor.origin_start, 100);
    assert!(live.request_already_complete(104).unwrap());
    let app = live.delta_tables().unwrap().app_id();
    assert!(app != first_app && app != second_app);
    assert_eq!(
        live.delta_tables()
            .unwrap()
            .txn_version("blocks")
            .await
            .unwrap(),
        None
    );
    assert_eq!(fill(&mut live, 104).await.ordinal, 5);
    let tables = live.delta_tables().unwrap();
    assert_eq!(tables.txn_version("blocks").await.unwrap(), Some(5));
    // Creation, four lane commits and one live commit.
    assert_eq!(tables.version("blocks").unwrap(), 5);
    drop(live);
    owner.release().await.unwrap();

    // A sealed backfill has no lanes left to run.
    let late = lane_owner(&store, 100, 102).await;
    assert!(refusal(open(&lane(100, 102), &late).await).contains("sealed"));
    late.release().await.unwrap();
}

/// A lane takes every block number of its range, from its first: what it
/// never received could not be told from lost blocks at the seal.
#[tokio::test]
async fn a_lane_refuses_a_late_first_block_and_a_skipped_block_number() {
    let store = Arc::new(InMemory::new());
    let owner = bucket_owner(&store).await;
    open_backfill(&dataset(Some(104)), mapper(BlockFamily::Evm), &owner, 4)
        .await
        .unwrap();
    owner.release().await.unwrap();

    let owner = lane_owner(&store, 100, 104).await;
    let mut late = open(&lane(100, 104), &owner).await.unwrap();
    let ordinal = receive(&mut late, 101, TIME, 1);
    assert!(refusal(late.accept_mapped(ordinal, Some(TIME), None))
        .contains("expected block 100 but received block 101"));
    drop(late);

    // Also after a restart: the expected block follows the stored checkpoint.
    let mut lane_session = open(&lane(100, 104), &owner).await.unwrap();
    fill(&mut lane_session, 100).await;
    drop(lane_session);
    let mut resumed = open(&lane(100, 104), &owner).await.unwrap();
    let ordinal = receive(&mut resumed, 102, TIME, 1);
    assert!(refusal(resumed.accept_mapped(ordinal, Some(TIME), None))
        .contains("expected block 101 but received block 102"));
    drop(resumed);
    owner.release().await.unwrap();
}
