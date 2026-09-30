//! Owner release after a failed build (`DatasetOwnership::finish`) against the
//! stateful loopback provider. The uncertainty latch, not the presence of a
//! pending journal, decides whether the bucket owner is kept.
use super::*;
use crate::dataset_lock::DatasetOwnership;
use crate::dataset_lock_s3::{OwnerState, OwnershipError};
use crate::s3::upload::fixture::{Fault, Server};
use object_store::ObjectStore;

fn s3_descriptor(root: &Path) -> StreamDescriptor {
    let mut descriptor = actual_descriptor(root);
    descriptor.output = StorageIdentity::S3 {
        service: Digest::hash("service", &"fixture").unwrap(),
        bucket: "bucket".into(),
        prefix: "dataset".into(),
    };
    descriptor
}

async fn try_owner(server: &Server) -> std::result::Result<S3Ownership, OwnershipError> {
    S3Ownership::acquire_native(server.client.clone(), "build", vec!["dataset".into()]).await
}

async fn initialized(server: &Server, root: &Path) -> (S3Ownership, StreamDescriptor) {
    let owner = try_owner(server).await.unwrap();
    let descriptor = s3_descriptor(root);
    TransactionStateStore::s3("dataset", &owner)
        .unwrap()
        .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
        .await
        .unwrap();
    (owner, descriptor)
}

async fn open_s3<'a>(
    owner: &'a S3Ownership,
    mirror: &'a Mirror,
    descriptor: &StreamDescriptor,
) -> Result<TransactionController<'a, &'a Mirror>> {
    TransactionController::open(
        TransactionStateStore::s3("dataset", owner)?,
        TransactionParts::s3("dataset", owner, "")?,
        mirror,
        descriptor,
    )
    .await
}

fn part_requests(server: &Server, method: &str) -> usize {
    server
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|r| r.method == method && r.path.ends_with(".parquet"))
        .count()
}

fn stored_parts(server: &Server) -> usize {
    server
        .state
        .lock()
        .unwrap()
        .objects
        .keys()
        .filter(|key| key.ends_with(".parquet"))
        .count()
}

async fn pending_phase(owner: &S3Ownership) -> Option<TransactionPhase> {
    TransactionStateStore::s3("dataset", owner)
        .unwrap()
        .load()
        .await
        .unwrap()
        .pending
        .map(|record| record.payload.phase)
}

async fn owner_state(store: &Arc<dyn ObjectStore>) -> OwnerState {
    S3Ownership::status(store).await.unwrap().unwrap().state()
}

/// The next command acquires the bucket at once, its startup recovery resolves
/// the journal, and the same window then commits exactly once.
async fn next_owner_recovers_and_commits(
    server: &Server,
    descriptor: &StreamDescriptor,
    generation: u64,
) {
    server.state.lock().unwrap().fault = Fault::None;
    let next = try_owner(server).await.unwrap();
    assert_eq!(next.record().generation(), generation + 1);
    let mirror = Mirror::default();
    let mut controller = open_s3(&next, &mirror, descriptor).await.unwrap();
    assert_eq!(pending_phase(&next).await, None);
    assert_eq!(stored_parts(server), 0, "recovery rolled Writing back");
    commit(&mut controller).await.unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    assert_eq!(stored_parts(server), 2);
    drop(controller);
    next.release().await.unwrap();
}

#[tokio::test]
async fn refused_part_put_releases_owner_and_next_owner_rolls_back_writing() {
    let server = Server::start().await;
    let temp = tempfile::tempdir().unwrap();
    let (owner, descriptor) = initialized(&server, temp.path()).await;
    let store = owner.object_store().clone();
    let generation = owner.record().generation();
    let mirror = Mirror::default();
    let mut controller = open_s3(&owner, &mirror, &descriptor).await.unwrap();
    server.state.lock().unwrap().fault = Fault::ForbiddenPart;
    let error = commit(&mut controller).await.err().expect("refused part");
    assert!(format!("{error:#}").contains("HTTP 403"), "{error:#}");
    drop(controller);
    // A 403 is the provider's final refusal: one PUT, nothing stored, and the
    // latch stays clear. The journal is still Writing with its frozen receipt.
    assert_eq!(part_requests(&server, "PUT"), 1);
    assert_eq!(stored_parts(&server), 0);
    assert!(!owner.is_mutation_uncertain());
    assert_eq!(pending_phase(&owner).await, Some(TransactionPhase::Writing));

    let ownership = DatasetOwnership::from_remote_for_test("bucket", owner);
    let returned = ownership.finish::<()>(Err(error)).await.unwrap_err();
    let message = format!("{returned:#}");
    assert!(
        message.contains("HTTP 403") && !message.contains("retained"),
        "{message}"
    );
    assert_eq!(owner_state(&store).await, OwnerState::Released);
    next_owner_recovers_and_commits(&server, &descriptor, generation).await;
}

#[tokio::test]
async fn resolved_failure_after_publication_releases_and_next_owner_removes_the_part() {
    let server = Server::start().await;
    let temp = tempfile::tempdir().unwrap();
    let (owner, descriptor) = initialized(&server, temp.path()).await;
    let store = owner.object_store().clone();
    let generation = owner.record().generation();
    let mirror = Mirror::default();
    let mut controller = open_s3(&owner, &mirror, &descriptor).await.unwrap();
    // A local error after the first part was published and verified: every
    // request had a definite outcome, but a published part is left behind.
    let injected = fail(Stage::Published(0));
    let error = commit(&mut controller)
        .await
        .err()
        .expect("injected failure");
    drop(injected);
    drop(controller);
    assert_eq!(stored_parts(&server), 1);
    assert!(!owner.is_mutation_uncertain());
    assert_eq!(pending_phase(&owner).await, Some(TransactionPhase::Writing));

    let ownership = DatasetOwnership::from_remote_for_test("bucket", owner);
    assert!(ownership.finish::<()>(Err(error)).await.is_err());
    assert_eq!(owner_state(&store).await, OwnerState::Released);
    let deletes = part_requests(&server, "DELETE");
    next_owner_recovers_and_commits(&server, &descriptor, generation).await;
    assert_eq!(part_requests(&server, "DELETE"), deletes + 1);
}

/// #646: the provider stored both parts but dropped their connections before
/// acknowledging. The exact readback proves each publication, so the latch
/// stays clear, the transaction commits without a retry, and the owner is
/// released on success.
#[tokio::test]
async fn lost_part_acknowledgement_is_resolved_by_exact_readback() {
    let server = Server::start().await;
    let temp = tempfile::tempdir().unwrap();
    let (owner, descriptor) = initialized(&server, temp.path()).await;
    let store = owner.object_store().clone();
    let mirror = Mirror::default();
    let mut controller = open_s3(&owner, &mirror, &descriptor).await.unwrap();
    server.state.lock().unwrap().fault = Fault::LostPartAck;
    commit(&mut controller).await.unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    drop(controller);
    assert_eq!(stored_parts(&server), 2);
    assert_eq!(
        part_requests(&server, "PUT"),
        2,
        "one PUT per part, no retry"
    );
    assert_eq!(part_requests(&server, "DELETE"), 0);
    assert!(!owner.is_mutation_uncertain());
    assert_eq!(pending_phase(&owner).await, None);
    let ownership = DatasetOwnership::from_remote_for_test("bucket", owner);
    ownership.finish(Ok(())).await.unwrap();
    assert_eq!(owner_state(&store).await, OwnerState::Released);
}

/// #646: a PUT the provider applies shortly after the connection dropped is
/// proven by a later readback in the window.
#[tokio::test]
async fn part_stored_within_the_readback_window_is_resolved() {
    let server = Server::start().await;
    let temp = tempfile::tempdir().unwrap();
    let (owner, descriptor) = initialized(&server, temp.path()).await;
    let mirror = Mirror::default();
    let mut controller = open_s3(&owner, &mirror, &descriptor).await.unwrap();
    server.state.lock().unwrap().fault = Fault::LatePart(Duration::from_millis(10));
    commit(&mut controller).await.unwrap();
    drop(controller);
    assert_eq!(stored_parts(&server), 2);
    assert!(!owner.is_mutation_uncertain());
    owner.release().await.unwrap();
}

/// A part PUT cut before the provider applied it is never proven: absence may
/// still be followed by the request landing. The owner is kept with the exact
/// recovery commands, nothing is resent and nothing is rolled back.
#[tokio::test]
async fn unstored_part_keeps_owner_and_names_recovery_commands() {
    let server = Server::start().await;
    let temp = tempfile::tempdir().unwrap();
    let (owner, descriptor) = initialized(&server, temp.path()).await;
    let store = owner.object_store().clone();
    let record = owner.record().clone();
    let mirror = Mirror::default();
    let mut controller = open_s3(&owner, &mirror, &descriptor).await.unwrap();
    server.state.lock().unwrap().fault = Fault::ResetPartUnstored;
    let error = commit(&mut controller).await.err().expect("unstored part");
    drop(controller);
    assert_eq!(stored_parts(&server), 0);
    assert!(owner.is_mutation_uncertain());

    let ownership = DatasetOwnership::from_remote_for_test("bucket", owner);
    let returned = ownership.finish::<()>(Err(error)).await.unwrap_err();
    let message = format!("{returned:#}");
    for expected in [
        "S3 bucket ownership was retained".to_string(),
        "\"bucket ownership is held\"".into(),
        "uncertain outcome".into(),
        "fireparq recovery status s3://bucket/dataset\n".into(),
        format!(
            "fireparq recovery release s3://bucket/dataset --expected-owner {} \
             --expected-generation {} --stopped-writer-evidence <reference> \
             --provider-quiescence-evidence <reference>",
            record.owner_id(),
            record.generation()
        ),
        // The build's own failure remains the cause.
        "native conditional upload failed".into(),
    ] {
        assert!(
            message.contains(&expected),
            "missing {expected:?} in {message}"
        );
    }
    for forbidden in [
        "fixture-key",
        "fixture-secret",
        "fixture-token",
        "X-Amz",
        "http://",
    ] {
        assert!(!message.contains(forbidden));
    }
    assert_eq!(S3Ownership::status(&store).await.unwrap().unwrap(), record);
    assert!(matches!(
        try_owner(&server).await,
        Err(OwnershipError::Busy)
    ));
    let puts = part_requests(&server, "PUT");
    assert!((1..=2).contains(&puts), "no retry: {puts} part PUTs");
    assert_eq!(part_requests(&server, "DELETE"), 0, "no rollback");
}

/// The delayed-PUT case: a part the provider applies only after the readback
/// window lands while the owner is still Owned. Absence was not taken as
/// proof, so no successor could have acquired the bucket and rolled the
/// Writing journal back before the late arrival.
#[tokio::test]
async fn part_landing_after_the_window_meets_a_retained_owner() {
    let server = Server::start().await;
    let temp = tempfile::tempdir().unwrap();
    let (owner, descriptor) = initialized(&server, temp.path()).await;
    let store = owner.object_store().clone();
    let record = owner.record().clone();
    let mirror = Mirror::default();
    let mut controller = open_s3(&owner, &mirror, &descriptor).await.unwrap();
    server.state.lock().unwrap().fault = Fault::LatePart(Duration::from_millis(400));
    let error = commit(&mut controller).await.err().expect("late part");
    drop(controller);
    assert!(owner.is_mutation_uncertain());
    let ownership = DatasetOwnership::from_remote_for_test("bucket", owner);
    assert!(ownership.finish::<()>(Err(error)).await.is_err());
    assert_eq!(stored_parts(&server), 0, "not yet applied");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(stored_parts(&server) >= 1, "the delayed PUT arrived");
    assert_eq!(S3Ownership::status(&store).await.unwrap().unwrap(), record);
    assert!(matches!(
        try_owner(&server).await,
        Err(OwnershipError::Busy)
    ));
    assert_eq!(part_requests(&server, "DELETE"), 0);
}

/// #646: control records (the pending journal and authority) whose PUTs the
/// provider applies shortly after dropping the connection are proven by a
/// later readback; one immediate read would have seen the old record.
#[tokio::test]
async fn control_records_stored_within_the_readback_window_are_resolved() {
    let server = Server::start().await;
    let temp = tempfile::tempdir().unwrap();
    let (owner, descriptor) = initialized(&server, temp.path()).await;
    let mirror = Mirror::default();
    let mut controller = open_s3(&owner, &mirror, &descriptor).await.unwrap();
    server.state.lock().unwrap().fault = Fault::LateControl(Duration::from_millis(10));
    commit(&mut controller).await.unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    drop(controller);
    assert!(!owner.is_mutation_uncertain());
    server.state.lock().unwrap().fault = Fault::None;
    assert_eq!(pending_phase(&owner).await, None);
    owner.release().await.unwrap();
}

#[tokio::test]
async fn refused_control_record_write_releases_owner() {
    let server = Server::start().await;
    let owner = try_owner(&server).await.unwrap();
    let store = owner.object_store().clone();
    let temp = tempfile::tempdir().unwrap();
    server.state.lock().unwrap().fault = Fault::ForbiddenControl;
    let error = TransactionStateStore::s3("dataset", &owner)
        .unwrap()
        .initialize(AuthorityState::initial(s3_descriptor(temp.path())).unwrap())
        .await
        .err()
        .expect("refused control write");
    assert!(format!("{error:#}").contains("HTTP 403"), "{error:#}");
    assert!(!owner.is_mutation_uncertain());
    assert_eq!(pending_phase(&owner).await, None);

    let ownership = DatasetOwnership::from_remote_for_test("bucket", owner);
    assert!(ownership.finish::<()>(Err(error)).await.is_err());
    assert_eq!(owner_state(&store).await, OwnerState::Released);
    server.state.lock().unwrap().fault = Fault::None;
    try_owner(&server).await.unwrap().release().await.unwrap();
}
