use super::*;
use crate::s3::upload::fixture::{Fault, Server};
use futures::TryStreamExt;

#[tokio::test]
async fn native_controller_spools_receipts_before_put_then_commits_verified_parts() {
    let server = Server::start().await;
    // Bucket responses deliberately omit ETag; strict object checks must not
    // break protected-root/eligibility ListObjects discovery.
    assert!(server
        .client
        .object_store()
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .is_empty());
    let owner = S3Ownership::acquire_native(server.client.clone(), "build", vec!["dataset".into()])
        .await
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut descriptor = actual_descriptor(temp.path());
    descriptor.output = StorageIdentity::S3 {
        service: Digest::hash("service", &"fixture").unwrap(),
        bucket: "bucket".into(),
        prefix: "dataset".into(),
    };
    let states = TransactionStateStore::s3("dataset", &owner).unwrap();
    states
        .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
        .await
        .unwrap();
    let mirror = Mirror::default();
    let mut controller = TransactionController::open(
        states,
        TransactionParts::s3("dataset", &owner, "private").unwrap(),
        &mirror,
        &descriptor,
    )
    .await
    .unwrap();
    commit(&mut controller).await.unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    assert_eq!(mirror.head.borrow().as_ref().unwrap().ordinal, 2);
    let snapshot = TransactionStateStore::s3("dataset", &owner)
        .unwrap()
        .load()
        .await
        .unwrap();
    assert!(snapshot.pending.is_none());
    {
        let state = server.state.lock().unwrap();
        let puts: Vec<_> = state
            .requests
            .iter()
            .filter(|r| r.method == "PUT" && r.path.ends_with(".parquet"))
            .collect();
        assert_eq!(puts.len(), 2);
        assert!(puts
            .iter()
            .all(|r| r.conditional && r.query_signed && r.receipt_verified));
        for request in puts {
            let stored = &state.objects[&request.path];
            assert!(state.requests.iter().any(|r| r.method == "GET"
                && r.path == request.path
                && r.if_match.as_ref() == Some(&stored.etag)
                && r.version.as_ref() == Some(&stored.version)));
            let batches =
                ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(stored.bytes.clone()))
                    .unwrap()
                    .build()
                    .unwrap();
            assert_eq!(batches.map(|b| b.unwrap().num_rows()).sum::<usize>(), 2);
        }
    }
    drop(controller);
    assert!(!owner.is_mutation_uncertain());
    owner.release().await.unwrap();
}

#[tokio::test]
async fn native_controller_retains_writing_owner_and_cursor_on_uncertain_publication() {
    for fault in [
        Fault::LostPartAck,
        Fault::DuplicateEtag,
        Fault::DuplicateVersion,
        Fault::WildcardEtag,
        Fault::ListEtag,
        Fault::NullOnlyVersion,
        Fault::ControlVersion,
        Fault::ChangedVersion,
        Fault::CorruptBody,
        Fault::TruncatedBody,
        Fault::OversizeHeaders,
    ] {
        let server = Server::start().await;
        let owner =
            S3Ownership::acquire_native(server.client.clone(), "build", vec!["dataset".into()])
                .await
                .unwrap();
        let owner_record = owner.record().clone();
        let store = owner.object_store().clone();
        let temp = tempfile::tempdir().unwrap();
        let mut descriptor = actual_descriptor(temp.path());
        descriptor.output = StorageIdentity::S3 {
            service: Digest::hash("service", &"fixture").unwrap(),
            bucket: "bucket".into(),
            prefix: "dataset".into(),
        };
        let states = TransactionStateStore::s3("dataset", &owner).unwrap();
        states
            .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
            .await
            .unwrap();
        let mirror = Mirror::default();
        let mut controller = TransactionController::open(
            states,
            TransactionParts::s3("dataset", &owner, "").unwrap(),
            &mirror,
            &descriptor,
        )
        .await
        .unwrap();
        server.state.lock().unwrap().fault = fault;
        let error = commit(&mut controller)
            .await
            .err()
            .expect("must fail closed");
        let safe = format!("{error:#}");
        for forbidden in [
            "fixture-key",
            "fixture-secret",
            "fixture-token",
            "X-Amz",
            "http://",
        ] {
            assert!(!safe.contains(forbidden));
        }
        assert_eq!(controller.authority().checkpoint.ordinal, 0);
        assert_eq!(mirror.head.borrow().as_ref().unwrap().ordinal, 0);
        assert!(owner.is_mutation_uncertain());
        let snapshot = TransactionStateStore::s3("dataset", &owner)
            .unwrap()
            .load()
            .await
            .unwrap();
        assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 0);
        let pending = snapshot.pending.unwrap().payload;
        assert_eq!(pending.phase, TransactionPhase::Writing);
        assert!(pending.parts[0].receipt.is_some());
        let state = server.state.lock().unwrap();
        assert_eq!(
            state
                .requests
                .iter()
                .filter(|r| r.method == "PUT" && r.path.ends_with(".parquet"))
                .count(),
            1
        );
        drop(state);
        drop(controller);
        assert!(owner.release().await.is_err());
        assert_eq!(
            S3Ownership::status(&store).await.unwrap().unwrap(),
            owner_record
        );
    }
}

#[tokio::test]
async fn cancelling_native_put_after_acceptance_retains_owner_and_writing() {
    let server = Server::start().await;
    let owner = S3Ownership::acquire_native(server.client.clone(), "build", vec!["dataset".into()])
        .await
        .unwrap();
    let expected_owner = owner.record().clone();
    let store = owner.object_store().clone();
    let temp = tempfile::tempdir().unwrap();
    let mut descriptor = actual_descriptor(temp.path());
    descriptor.output = StorageIdentity::S3 {
        service: Digest::hash("service", &"fixture").unwrap(),
        bucket: "bucket".into(),
        prefix: "dataset".into(),
    };
    let states = TransactionStateStore::s3("dataset", &owner).unwrap();
    states
        .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
        .await
        .unwrap();
    let mirror = Mirror::default();
    let mut controller = TransactionController::open(
        states,
        TransactionParts::s3("dataset", &owner, "").unwrap(),
        &mirror,
        &descriptor,
    )
    .await
    .unwrap();
    server.state.lock().unwrap().fault = Fault::DelayedPartAck;
    let mut committing = Box::pin(commit(&mut controller));
    tokio::select! {
        result = &mut committing => panic!("upload must still await acknowledgement: {}",result.is_ok()),
        _ = async {
            loop {
                if server.state.lock().unwrap().requests.iter().any(|r|r.method=="PUT" && r.path.ends_with(".parquet")) {break;}
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        } => {}
    }
    drop(committing);
    assert!(owner.is_mutation_uncertain());
    assert_eq!(controller.authority().checkpoint.ordinal, 0);
    let snapshot = TransactionStateStore::s3("dataset", &owner)
        .unwrap()
        .load()
        .await
        .unwrap();
    assert_eq!(
        snapshot.pending.unwrap().payload.phase,
        TransactionPhase::Writing
    );
    assert_eq!(mirror.head.borrow().as_ref().unwrap().ordinal, 0);
    assert_eq!(
        server
            .state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r.method == "PUT" && r.path.ends_with(".parquet"))
            .count(),
        1
    );
    drop(controller);
    assert!(owner.release().await.is_err());
    assert_eq!(
        S3Ownership::status(&store).await.unwrap().unwrap(),
        expected_owner
    );
}
