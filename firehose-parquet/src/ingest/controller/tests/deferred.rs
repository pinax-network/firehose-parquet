//! #728: a backfill lane defers its Delta commits. Authority accepts each
//! flush and owes its parts to the logs in one write; the owed parts are
//! committed together, one commit per table, `blocks` last.
//!
//! The tests interrupt a deferred flush and a batch at every boundary and
//! check that a restart leaves every published part in its table's log
//! exactly once, that a part gone missing fails closed, that a request never
//! completes with parts owed, and that a lane a release without deferral
//! left between two tables' commits is finished.
use super::delta::{
    active, commit_rows, delta_data, delta_descriptor, delta_tables, expect,
    interrupted_first_commit, journal, logs, restart, table_parts,
};
use super::*;
use crate::delta::commit::{PartAdd, LAST_TABLE};

async fn open_lane<'a>(
    root: &Path,
    owner: &'a LocalOwnership,
    mirror: &'a Mirror,
    descriptor: &StreamDescriptor,
    create: bool,
    transactions: usize,
) -> Result<TransactionController<'a, &'a Mirror>> {
    let tables = delta_tables(root, descriptor, create).await;
    TransactionController::open_lane(
        TransactionStateStore::local(root, owner)?,
        TransactionParts::local(root, owner)?,
        mirror,
        descriptor,
        tables,
        FlushConcurrency::SERIAL,
        DeltaCommits::Deferred { transactions },
    )
    .await
}

/// A new lane at `root` that commits `transactions` flushes together.
async fn new_lane<'a>(
    root: &Path,
    owner: &'a LocalOwnership,
    mirror: &'a Mirror,
    transactions: usize,
) -> (StreamDescriptor, TransactionController<'a, &'a Mirror>) {
    let descriptor = delta_descriptor(root);
    initialize(root, owner, &descriptor).await;
    let controller = open_lane(root, owner, mirror, &descriptor, true, transactions)
        .await
        .unwrap();
    (descriptor, controller)
}

/// The accepted windows authority owes to the Delta logs.
async fn owed(root: &Path, owner: &LocalOwnership) -> Vec<(u64, u64)> {
    TransactionStateStore::local(root, owner)
        .unwrap()
        .load()
        .await
        .unwrap()
        .authority
        .unwrap()
        .payload
        .deferred
        .iter()
        .map(|transaction| (transaction.first_ordinal, transaction.last_ordinal))
        .collect()
}

/// Whether the stored authority record has a `deferred` field at all.
fn stores_deferred(root: &Path) -> bool {
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join(".fireparq-ingest/state.json")).unwrap())
            .unwrap();
    state["payload"].get("deferred").is_some()
}

/// Every part file of `table` is in its log, and nothing else is.
fn assert_log_holds_every_part(root: &Path, table: &str, parts: usize) {
    let root = fs::canonicalize(root).unwrap();
    let mut stored: Vec<String> = table_parts(&root, table)
        .iter()
        .map(|path| {
            path.strip_prefix(root.join(table))
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        })
        .collect();
    stored.sort();
    assert_eq!(active(&root, table), stored, "{table}");
    assert_eq!(stored.len(), parts, "{table}");
}

#[tokio::test]
async fn a_lane_owes_its_flushes_and_commits_them_together_blocks_last() {
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let (descriptor, mut controller) = new_lane(root.path(), &owner, &mirror, 3).await;
    assert!(!stores_deferred(root.path()));

    // Two flushes: accepted, published, and in no log.
    for ordinal in [2, 4] {
        let committed = commit_rows(&mut controller, delta_data()).await.unwrap();
        assert_eq!((committed.ordinal, committed.files), (ordinal, 2));
        assert!(committed.delta.is_empty());
    }
    assert_eq!(owed(root.path(), &owner).await, [(1, 2), (3, 4)]);
    assert!(stores_deferred(root.path()));
    assert_eq!(journal(root.path(), &owner).await, (4, None));
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 0, None), ("logs", 0, None)])
    );
    assert_eq!(table_parts(root.path(), "blocks").len(), 2);

    // The third one reaches the limit: one commit per table holds all three
    // flushes' parts, with authority's ordinal as its `txn`.
    let committed = commit_rows(&mut controller, delta_data()).await.unwrap();
    let order: Vec<_> = committed.delta.iter().map(|c| c.table.as_str()).collect();
    assert_eq!(order, ["logs", LAST_TABLE], "blocks commits last");
    assert_eq!(owed(root.path(), &owner).await, []);
    assert!(
        !stores_deferred(root.path()),
        "a record that owes nothing is the one every release reads"
    );
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 1, Some(6)), ("logs", 1, Some(6))])
    );
    for table in ["blocks", "logs"] {
        assert_log_holds_every_part(root.path(), table, 3);
        let commit = fs::read_to_string(
            root.path()
                .join(table)
                .join("_delta_log/00000000000000000001.json"),
        )
        .unwrap();
        let info = commit
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find_map(|action| action.get("commitInfo").cloned())
            .unwrap();
        assert_eq!(info["fireparq.transactions"], 3, "{table}");
        assert_eq!(info["fireparq.firstOrdinal"], 1, "{table}");
        assert_eq!(info["fireparq.lastOrdinal"], 6, "{table}");
    }

    // A clean stop commits what is owed, and reports those commits.
    commit_rows(&mut controller, delta_data()).await.unwrap();
    assert_eq!(owed(root.path(), &owner).await, [(7, 8)]);
    assert!(controller.take_settled().is_empty());
    controller.settle().await.unwrap();
    assert_eq!(controller.take_settled().len(), 2);
    assert_eq!(owed(root.path(), &owner).await, []);
    controller.settle().await.unwrap();
    assert!(
        controller.take_settled().is_empty(),
        "nothing owed: a no-op"
    );
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 2, Some(8)), ("logs", 2, Some(8))])
    );

    // A flush without rows owes nothing.
    let empty: HashMap<_, _> = delta_data()
        .into_iter()
        .map(|(table, batch)| (table, batch.slice(0, 0)))
        .collect();
    let committed = commit_rows(&mut controller, empty).await.unwrap();
    assert_eq!((committed.ordinal, committed.files), (10, 0));
    assert_eq!(owed(root.path(), &owner).await, []);

    // A restart finds nothing to do.
    drop(controller);
    let controller = open_lane(root.path(), &owner, &mirror, &descriptor, false, 3)
        .await
        .unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 10);
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 2, Some(8)), ("logs", 2, Some(8))])
    );
}

#[tokio::test]
async fn every_boundary_of_a_deferred_flush_recovers_with_each_part_in_its_log_once() {
    for stage in [
        Stage::WritingPersisted,
        Stage::Published(0),
        Stage::CommittedPersisted,
        Stage::AuthorityAdvanced,
        Stage::MirrorReconciled,
        Stage::PendingCleared,
    ] {
        let root = tempfile::tempdir().unwrap();
        let mirror = Mirror::default();
        let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
        let (descriptor, mut controller) = new_lane(root.path(), &owner, &mirror, 5).await;
        commit_rows(&mut controller, delta_data()).await.unwrap();
        let injected = fail(stage);
        assert!(
            commit_rows(&mut controller, delta_data()).await.is_err(),
            "{stage:?}"
        );
        drop(injected);
        assert!(
            commit_rows(&mut controller, delta_data()).await.is_err(),
            "{stage:?}: the controller stays poisoned"
        );
        drop(controller);
        // Nothing reached a log before the restart.
        assert_eq!(
            logs(root.path(), &descriptor).await,
            expect(&[("blocks", 0, None), ("logs", 0, None)]),
            "{stage:?}"
        );

        // The restart rolls the interrupted flush back (before Committed) or
        // forward, then commits everything owed.
        let mut controller = open_lane(root.path(), &owner, &mirror, &descriptor, false, 5)
            .await
            .unwrap();
        let rolled_back = matches!(stage, Stage::WritingPersisted | Stage::Published(_));
        let (ordinal, flushes) = if rolled_back { (2, 1) } else { (4, 2) };
        assert_eq!(controller.authority().checkpoint.ordinal, ordinal);
        assert_eq!(journal(root.path(), &owner).await, (ordinal, None));
        assert_eq!(owed(root.path(), &owner).await, [], "{stage:?}");
        let txn = Some(i64::try_from(ordinal).unwrap());
        assert_eq!(
            logs(root.path(), &descriptor).await,
            expect(&[("blocks", 1, txn), ("logs", 1, txn)]),
            "{stage:?}"
        );
        for table in ["blocks", "logs"] {
            assert_log_holds_every_part(root.path(), table, flushes);
        }
        assert_eq!(controller.take_settled().len(), 2);

        // The lane continues from there.
        let committed = commit_rows(&mut controller, delta_data()).await.unwrap();
        assert_eq!(committed.ordinal, ordinal + 2);
    }
}

/// A batch dies between two tables' commits, again after the last one, and
/// again before authority is settled. Each restart reads every table's `txn`
/// and commits only what it lacks. Maintenance may have rewritten the parts
/// of a table that holds the batch, so they are not read again.
#[tokio::test]
async fn an_interrupted_batch_is_finished_from_each_tables_txn() {
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let (descriptor, mut controller) = new_lane(root.path(), &owner, &mirror, 2).await;
    commit_rows(&mut controller, delta_data()).await.unwrap();
    // `logs` (entry 1) gets the batch; the lane dies before `blocks`.
    let injected = fail(Stage::DeltaCommitted(1));
    assert!(commit_rows(&mut controller, delta_data()).await.is_err());
    drop(injected);
    drop(controller);
    assert_eq!(journal(root.path(), &owner).await, (4, None));
    assert_eq!(owed(root.path(), &owner).await, [(1, 2), (3, 4)]);
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 0, None), ("logs", 1, Some(4))])
    );
    assert_eq!(active(root.path(), "logs").len(), 2);

    // Maintenance compacted `logs` and vacuumed its two parts.
    for part in table_parts(root.path(), "logs") {
        fs::remove_file(part).unwrap();
    }

    // The restart commits `blocks` and dies before it is settled.
    for stage in [Stage::DeltaCommitted(0), Stage::DeltaCommittedAll] {
        let injected = fail(stage);
        assert!(
            open_lane(root.path(), &owner, &mirror, &descriptor, false, 2)
                .await
                .is_err(),
            "{stage:?}"
        );
        drop(injected);
        assert_eq!(owed(root.path(), &owner).await, [(1, 2), (3, 4)]);
        assert_eq!(
            logs(root.path(), &descriptor).await,
            expect(&[("blocks", 1, Some(4)), ("logs", 1, Some(4))]),
            "{stage:?}: no table is committed twice"
        );
    }

    // The next one finds both logs holding the batch and only settles.
    let mut controller = open_lane(root.path(), &owner, &mirror, &descriptor, false, 2)
        .await
        .unwrap();
    assert!(controller.take_settled().is_empty());
    assert_eq!(owed(root.path(), &owner).await, []);
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 1, Some(4)), ("logs", 1, Some(4))])
    );
    assert_log_holds_every_part(root.path(), "blocks", 2);
    assert_eq!(active(root.path(), "logs").len(), 2);

    // Settled authority that dies before it reports: nothing is owed twice.
    commit_rows(&mut controller, delta_data()).await.unwrap();
    let injected = fail(Stage::DeferredSettled);
    assert!(commit_rows(&mut controller, delta_data()).await.is_err());
    drop(injected);
    drop(controller);
    assert_eq!(owed(root.path(), &owner).await, []);
    let controller = open_lane(root.path(), &owner, &mirror, &descriptor, false, 2)
        .await
        .unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 8);
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 2, Some(8)), ("logs", 2, Some(8))])
    );
    assert_log_holds_every_part(root.path(), "blocks", 4);
}

#[tokio::test]
async fn an_owed_part_that_is_gone_fails_closed_with_the_state_kept() {
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let (descriptor, mut controller) = new_lane(root.path(), &owner, &mirror, 5).await;
    commit_rows(&mut controller, delta_data()).await.unwrap();
    commit_rows(&mut controller, delta_data()).await.unwrap();
    drop(controller);
    let state = root.path().join(".fireparq-ingest/state.json");
    let before = fs::read(&state).unwrap();

    let part = table_parts(root.path(), "blocks").remove(0);
    let bytes = fs::read(&part).unwrap();
    for damage in ["missing", "truncated"] {
        match damage {
            "missing" => fs::remove_file(&part).unwrap(),
            _ => fs::write(&part, &bytes[..bytes.len() - 1]).unwrap(),
        }
        let error = open_lane(root.path(), &owner, &mirror, &descriptor, false, 5)
            .await
            .err()
            .unwrap();
        let text = format!("{error:#}");
        assert!(text.contains("deferred parts of ordinals 1..=4"), "{text}");
        assert!(
            text.contains(if damage == "missing" {
                "is missing"
            } else {
                "not its receipt's"
            }),
            "{text}"
        );
        assert_eq!(fs::read(&state).unwrap(), before, "{damage}");
        assert_eq!(
            logs(root.path(), &descriptor).await,
            expect(&[("blocks", 0, None), ("logs", 0, None)]),
            "{damage}: nothing is committed"
        );
    }

    // With the part back, the lane recovers.
    fs::write(&part, &bytes).unwrap();
    open_lane(root.path(), &owner, &mirror, &descriptor, false, 5)
        .await
        .unwrap();
    assert_log_holds_every_part(root.path(), "blocks", 2);
}

#[tokio::test]
async fn a_request_completes_only_once_its_parts_are_in_the_logs() {
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let (descriptor, mut controller) = new_lane(root.path(), &owner, &mirror, 5).await;
    commit_rows(&mut controller, delta_data()).await.unwrap();
    assert_eq!(owed(root.path(), &owner).await, [(1, 2)]);

    // The completion's batch dies after `logs`: the request is not complete.
    let frontier = AcceptedFrontier::resume(&controller.authority().checkpoint);
    let injected = fail(Stage::DeltaCommitted(1));
    assert!(controller
        .complete_request(102, &frontier, true)
        .await
        .is_err());
    drop(injected);
    drop(controller);
    let stored = TransactionStateStore::local(root.path(), &owner)
        .unwrap()
        .load()
        .await
        .unwrap()
        .authority
        .unwrap();
    assert_eq!(stored.payload.checkpoint.completed_stop, None);
    // The store itself refuses to complete a request with parts owed.
    assert!(TransactionStateStore::local(root.path(), &owner)
        .unwrap()
        .complete_request(&stored, 102)
        .await
        .is_err());

    // The rerun commits the rest, then completes.
    let mut controller = open_lane(root.path(), &owner, &mirror, &descriptor, false, 5)
        .await
        .unwrap();
    assert!(!controller.request_already_complete(102).unwrap());
    let frontier = AcceptedFrontier::resume(&controller.authority().checkpoint);
    assert!(controller
        .complete_request(102, &frontier, true)
        .await
        .unwrap());
    assert_eq!(controller.authority().checkpoint.completed_stop, Some(102));
    assert_eq!(owed(root.path(), &owner).await, []);
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 1, Some(2)), ("logs", 1, Some(2))])
    );

    // Completing with parts still owed commits them first.
    let root = tempfile::tempdir().unwrap();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let (descriptor, mut controller) = new_lane(root.path(), &owner, &mirror, 5).await;
    commit_rows(&mut controller, delta_data()).await.unwrap();
    let frontier = AcceptedFrontier::resume(&controller.authority().checkpoint);
    assert!(controller
        .complete_request(102, &frontier, true)
        .await
        .unwrap());
    assert_eq!(controller.take_settled().len(), 2);
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 1, Some(2)), ("logs", 1, Some(2))])
    );
}

/// A lane written by a release that committed every flush died between two
/// tables' commits (as lanes of the first production backfill did, at the
/// commit retry limit). This release resumes it: the table that holds the
/// transaction is not read or committed again, the other gets its part.
#[tokio::test]
async fn a_journal_left_between_tables_by_an_every_flush_release_is_finished() {
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let descriptor = interrupted_first_commit(root.path(), &mirror, Stage::DeltaCommitted(1)).await;
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    assert_eq!(
        journal(root.path(), &owner).await,
        (0, Some(TransactionPhase::Committed))
    );
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 0, None), ("logs", 1, Some(2))])
    );
    for part in table_parts(root.path(), "logs") {
        fs::remove_file(part).unwrap();
    }
    let mut controller = open_lane(root.path(), &owner, &mirror, &descriptor, false, 5)
        .await
        .unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    assert_eq!(journal(root.path(), &owner).await, (2, None));
    assert_eq!(owed(root.path(), &owner).await, []);
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 1, Some(2)), ("logs", 1, Some(2))])
    );
    assert_log_holds_every_part(root.path(), "blocks", 1);
    assert_eq!(active(root.path(), "logs").len(), 1);
    let settled = controller.take_settled();
    assert_eq!(settled.len(), 1);
    assert_eq!(settled[0].table, "blocks");
}

#[tokio::test]
async fn only_a_lane_opens_a_dataset_that_owes_parts_to_the_logs() {
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let (descriptor, mut controller) = new_lane(root.path(), &owner, &mirror, 5).await;
    commit_rows(&mut controller, delta_data()).await.unwrap();
    drop(controller);
    let error = restart(root.path(), &owner, &mirror, &descriptor)
        .await
        .err()
        .unwrap();
    assert!(
        format!("{error:#}").contains("not in the Delta logs yet"),
        "{error:#}"
    );
    assert_eq!(owed(root.path(), &owner).await, [(1, 2)]);
    assert_eq!(
        logs(root.path(), &descriptor).await,
        expect(&[("blocks", 0, None), ("logs", 0, None)])
    );

    // A release that predates the field refuses the record, since it denies
    // unknown fields, where it would otherwise drop the owed parts.
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct AuthorityBeforeDeferral {
        descriptor: serde_json::Value,
        checkpoint: serde_json::Value,
    }
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(root.path().join(".fireparq-ingest/state.json")).unwrap())
            .unwrap();
    assert!(serde_json::from_value::<AuthorityBeforeDeferral>(state["payload"].clone()).is_err());
    open_lane(root.path(), &owner, &mirror, &descriptor, false, 5)
        .await
        .unwrap();
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(root.path().join(".fireparq-ingest/state.json")).unwrap())
            .unwrap();
    assert!(serde_json::from_value::<AuthorityBeforeDeferral>(state["payload"].clone()).is_ok());

    // The number of flushes a lane commits together is bounded.
    for transactions in [0, MAX_DEFERRED_TRANSACTIONS + 1] {
        assert!(open_lane(
            root.path(),
            &owner,
            &mirror,
            &descriptor,
            false,
            transactions
        )
        .await
        .is_err());
    }
}

/// A lane commits before the owed parts make its authority record large,
/// whatever its flush count.
#[tokio::test]
async fn a_lane_commits_before_its_state_record_grows_large() {
    let root = tempfile::tempdir().unwrap();
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    let (_, mut controller) = new_lane(root.path(), &owner, &mirror, 5).await;
    commit_rows(&mut controller, delta_data()).await.unwrap();
    let mut deferred = controller.authority().deferred.clone();
    assert!(!owes_a_batch(&deferred, 5).unwrap());
    assert!(owes_a_batch(&deferred, 1).unwrap());
    let bytes = serde_json::to_vec(&deferred).unwrap().len();
    assert!(
        bytes < 2 * 1024,
        "two parts take {bytes} bytes: 64 flushes of 20 tables stay far below the record's limit"
    );
    deferred[0].parts[0].receipt.stats = format!(
        r#"{{"numRecords":2,"padding":"{}"}}"#,
        "x".repeat(DEFERRED_BYTES_LIMIT)
    );
    assert!(owes_a_batch(&deferred, 5).unwrap());
}

/// A batch commit whose response was lost: the lane's next start reads the
/// table's `txn` before the delayed commit lands, tries the same commit and
/// loses to it. With the same `txn` in the log, the batch is there once.
#[tokio::test]
async fn a_delayed_copy_of_a_batch_commit_is_resolved_from_the_log() {
    let root = tempfile::tempdir().unwrap();
    let descriptor = delta_descriptor(root.path());
    drop(delta_tables(root.path(), &descriptor, true).await);
    let date = FIXTURE_DATE.strip_prefix("date=").unwrap();
    let stats = crate::delta::stats::stats_json(&delta_data()["blocks"]).unwrap();
    let batch = || -> Vec<PartAdd> {
        ["first", "second"]
            .iter()
            .map(|name| {
                PartAdd::new(
                    "blocks",
                    0,
                    &format!("{FIXTURE_DATE}/{name}.parquet"),
                    date,
                    1,
                    &stats,
                    1,
                )
                .unwrap()
            })
            .collect()
    };
    let mut recovering = delta_tables(root.path(), &descriptor, false).await;
    let mut unresolved = delta_tables(root.path(), &descriptor, false).await;
    assert_eq!(recovering.txn_versions(1).await.unwrap()["blocks"], None);
    let mut delayed = delta_tables(root.path(), &descriptor, false).await;
    let landed = delayed
        .commit_batch(batch(), 4, HashMap::new(), 1, &CommitHooks::NONE, false)
        .await
        .unwrap();
    assert_eq!((landed.len(), landed[0].version), (1, 1));

    // Outside recovery the lost race is an error, never a second copy.
    let failure = unresolved
        .commit_batch(batch(), 4, HashMap::new(), 1, &CommitHooks::NONE, false)
        .await
        .unwrap_err();
    assert!(!failure.unresolved);
    assert!(crate::delta::commit::is_same_app_conflict(&failure.error));

    let resolved = recovering
        .commit_batch(batch(), 4, HashMap::new(), 1, &CommitHooks::NONE, true)
        .await
        .unwrap();
    assert_eq!((resolved[0].version, resolved[0].found_in_log), (1, true));
    assert_eq!(recovering.txn_versions(1).await.unwrap()["blocks"], Some(4));
    assert_eq!(active(root.path(), "blocks").len(), 2);

    // A winning commit of this stream at another `txn` is not this batch.
    let mut stale = delta_tables(root.path(), &descriptor, false).await;
    delayed
        .commit_batch(
            vec![PartAdd::new(
                "blocks",
                0,
                &format!("{FIXTURE_DATE}/third.parquet"),
                date,
                1,
                &stats,
                1,
            )
            .unwrap()],
            6,
            HashMap::new(),
            1,
            &CommitHooks::NONE,
            false,
        )
        .await
        .unwrap();
    assert!(stale
        .commit_batch(batch(), 8, HashMap::new(), 1, &CommitHooks::NONE, true)
        .await
        .is_err());
    assert_eq!(active(root.path(), "blocks").len(), 3);
}
