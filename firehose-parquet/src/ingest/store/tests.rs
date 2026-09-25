use super::*;
use crate::ingest::frontier::AcceptedFrontier;
use crate::ingest::state::tests::{descriptor, event, routing};
use crate::ingest::state::{Digest, PartCompression, RoutingPolicy, TablePlan};
use object_store::memory::InMemory;
use std::sync::Arc;

fn plan(authority: &AuthorityState, rows: u64) -> PendingTransaction {
    let mut frontier = AcceptedFrontier::resume(&authority.checkpoint);
    let ordinal = frontier.receive(event(100, 1)).unwrap();
    frontier
        .accept(ordinal, routing(RoutingPolicy::DirectV1))
        .unwrap();
    let tables = authority
        .descriptor
        .tables
        .iter()
        .map(|(name, digest)| TablePlan {
            table: name.clone(),
            schema_sha256: digest.clone(),
            rows,
            partition: String::new(),
        })
        .collect();
    PendingTransaction::prepare(
        authority,
        frontier.snapshot().unwrap().unwrap(),
        tables,
        PartCompression::Zstd,
    )
    .unwrap()
}

async fn exercise_reopen_and_commit(store: &TransactionStateStore<'_>) {
    let original = AuthorityState::initial(descriptor(RoutingPolicy::DirectV1)).unwrap();
    let authority = store.initialize(original).await.unwrap();
    let writing = store
        .begin(&authority, plan(&authority.payload, 0))
        .await
        .unwrap();
    assert!(store
        .begin(&authority, plan(&authority.payload, 0))
        .await
        .is_err());
    assert!(store.advance(&authority, &writing).await.is_err());
    let snapshot = store.load().await.unwrap();
    assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 0);
    assert_eq!(
        snapshot.pending.unwrap().payload.phase,
        TransactionPhase::Writing
    );
    let committed = store.mark_committed(&authority, &writing).await.unwrap();
    assert!(store.clear(&authority, &committed).await.is_err());
    assert!(store.clear(&authority, &writing).await.is_err());
    let snapshot = store.load().await.unwrap();
    assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 0);
    assert_eq!(
        snapshot.pending.unwrap().payload.phase,
        TransactionPhase::Committed
    );
    let next = store.advance(&authority, &committed).await.unwrap();
    assert!(store.advance(&authority, &committed).await.is_err());
    let snapshot = store.load().await.unwrap();
    assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 1);
    assert!(snapshot.pending.is_some());
    store.clear(&next, &committed).await.unwrap();
    let snapshot = store.load().await.unwrap();
    assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 1);
    assert!(snapshot.pending.is_none());
    assert!(store.clear(&next, &committed).await.is_err());
}

#[tokio::test]
async fn local_records_reopen_at_each_commit_boundary_without_replaying_empty_events() {
    let root = tempfile::tempdir().unwrap();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    exercise_reopen_and_commit(&TransactionStateStore::local(root.path(), &owner).unwrap()).await;
    drop(owner);
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let state = TransactionStateStore::local(root.path(), &owner)
        .unwrap()
        .load()
        .await
        .unwrap();
    assert_eq!(state.authority.unwrap().payload.checkpoint.ordinal, 1);
    assert!(state.pending.is_none());
}

#[tokio::test]
async fn remote_control_transitions_keep_a_tombstone_and_fresh_pending_incarnation() {
    let backend = Arc::new(InMemory::new());
    let owner = S3Ownership::acquire(backend.clone(), "test-ingest", vec!["dataset".into()])
        .await
        .unwrap();
    let store = TransactionStateStore::s3("dataset", &owner).unwrap();
    exercise_reopen_and_commit(&store).await;
    let snapshot = store.load().await.unwrap();
    let authority = snapshot.authority.unwrap();
    let writing = store
        .begin(&authority, plan(&authority.payload, 0))
        .await
        .unwrap();
    let incarnation = match &writing.version {
        Version::S3(v) => v.record.incarnation.clone(),
        _ => unreachable!(),
    };
    store.clear(&authority, &writing).await.unwrap();
    let again = store
        .begin(&authority, plan(&authority.payload, 0))
        .await
        .unwrap();
    assert!(matches!(&again.version,Version::S3(v) if v.record.incarnation != incarnation));
    assert!(store.clear(&authority, &writing).await.is_err());
    store.clear(&authority, &again).await.unwrap();
    owner.release().await.unwrap();
}

#[tokio::test]
async fn frozen_receipt_cas_rejects_stale_writer_and_commit_missing_table_receipt() {
    let root = tempfile::tempdir().unwrap();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let store = TransactionStateStore::local(root.path(), &owner).unwrap();
    let authority = store
        .initialize(AuthorityState::initial(descriptor(RoutingPolicy::DirectV1)).unwrap())
        .await
        .unwrap();
    let writing = store
        .begin(&authority, plan(&authority.payload, 1))
        .await
        .unwrap();
    let receipt = PartReceipt {
        byte_size: 100,
        sha256: Digest::hash("bytes", &1).unwrap(),
    };
    let first = store
        .record_receipt(&authority, &writing, 0, receipt.clone())
        .await
        .unwrap();
    assert!(store
        .record_receipt(&authority, &writing, 1, receipt.clone())
        .await
        .is_err());
    assert!(store.mark_committed(&authority, &first).await.is_err());
    let both = store
        .record_receipt(&authority, &first, 1, receipt)
        .await
        .unwrap();
    let committed = store.mark_committed(&authority, &both).await.unwrap();
    assert!(store.mark_committed(&authority, &both).await.is_err());
    let next = store.advance(&authority, &committed).await.unwrap();
    store.clear(&next, &committed).await.unwrap();
}

#[tokio::test]
async fn orphan_pending_and_inconsistent_authority_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let store = TransactionStateStore::local(root.path(), &owner).unwrap();
    let raw = LocalStateStore::new(root.path(), &owner).unwrap();
    let authority = AuthorityState::initial(descriptor(RoutingPolicy::DirectV1)).unwrap();
    let writing = plan(&authority, 0);
    let orphan = raw.create(ControlKey::Pending, &writing).unwrap();
    assert!(store.load().await.is_err());
    assert!(store.initialize(authority.clone()).await.is_err());
    raw.remove(ControlKey::Pending, &orphan).unwrap();
    let initialized = store.initialize(authority.clone()).await.unwrap();
    let pending = store.begin(&initialized, writing).await.unwrap();
    let committed = pending
        .payload
        .committed_after_verification(&authority.descriptor)
        .unwrap();
    let advanced = authority.install(&committed).unwrap();
    let Version::Local(version) = &initialized.version else {
        unreachable!()
    };
    // State advanced while journal still Writing is not a legal recovery case.
    raw.replace(ControlKey::State, version, &advanced).unwrap();
    assert!(store.load().await.is_err());
}

#[tokio::test]
async fn uncertain_remote_owner_prevents_further_state_mutation() {
    let backend = Arc::new(InMemory::new());
    let owner = S3Ownership::acquire(backend, "test-ingest", vec!["dataset".into()])
        .await
        .unwrap();
    let store = TransactionStateStore::s3("dataset", &owner).unwrap();
    let authority = store
        .initialize(AuthorityState::initial(descriptor(RoutingPolicy::DirectV1)).unwrap())
        .await
        .unwrap();
    owner.mark_mutation_uncertain();
    assert!(store
        .begin(&authority, plan(&authority.payload, 0))
        .await
        .is_err());
    assert!(store.load().await.unwrap().pending.is_none());
    assert!(owner.release().await.is_err());
}
