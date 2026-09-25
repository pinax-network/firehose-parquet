use super::*;
use crate::ingest::state::tests::{descriptor, event, routing};
use crate::ingest::state::{AnchorProvenance, AuthorityState, RoutingPolicy, TimestampAnchor};

fn frontier(policy: RoutingPolicy) -> AcceptedFrontier {
    AcceptedFrontier::resume(
        &AuthorityState::initial(descriptor(policy))
            .unwrap()
            .checkpoint,
    )
}

#[test]
fn later_acceptance_never_crosses_an_unresolved_received_event() {
    let mut frontier = frontier(RoutingPolicy::DirectV1);
    assert_eq!(frontier.receive(event(100, 1)).unwrap(), 1);
    assert_eq!(frontier.receive(event(101, 1)).unwrap(), 2);
    frontier
        .accept(2, routing(RoutingPolicy::DirectV1))
        .unwrap();
    assert!(frontier.snapshot().unwrap().is_none());
    assert_eq!(frontier.unresolved_events(), 2);
    frontier
        .accept(1, routing(RoutingPolicy::DirectV1))
        .unwrap();
    let prefix = frontier.snapshot().unwrap().unwrap();
    assert_eq!((prefix.first_ordinal, prefix.last_ordinal), (1, 2));
    assert_eq!(prefix.last_event.block_num, 101);
    assert_eq!(frontier.unresolved_events(), 0);
}

#[test]
fn ordered_digest_is_independent_of_out_of_order_acceptance_completion() {
    let mut one = frontier(RoutingPolicy::DirectV1);
    let mut two = frontier(RoutingPolicy::DirectV1);
    for tracker in [&mut one, &mut two] {
        tracker.receive(event(100, 1)).unwrap();
        tracker.receive(event(100, 2)).unwrap();
    }
    one.accept(1, routing(RoutingPolicy::DirectV1)).unwrap();
    one.accept(2, routing(RoutingPolicy::DirectV1)).unwrap();
    two.accept(2, routing(RoutingPolicy::DirectV1)).unwrap();
    two.accept(1, routing(RoutingPolicy::DirectV1)).unwrap();
    assert!(one.snapshot().unwrap() == two.snapshot().unwrap());
}

#[test]
fn nonmonotonic_heights_fork_steps_and_zero_rows_still_advance_ordinals() {
    let mut tracker = frontier(RoutingPolicy::DirectV1);
    let mut previous = None;
    for (index, (height, step)) in [(101, 1), (101, 2), (100, 1), (100, 3)]
        .into_iter()
        .enumerate()
    {
        let ordinal = tracker.receive(event(height, step)).unwrap();
        // There is intentionally no row count: accepted filtered/empty events are real progress.
        tracker
            .accept(ordinal, routing(RoutingPolicy::DirectV1))
            .unwrap();
        let prefix = tracker.snapshot().unwrap().unwrap();
        assert_eq!(prefix.last_ordinal, index as u64 + 1);
        assert_eq!(prefix.last_event.block_num, height);
        assert_ne!(previous, Some(prefix.events_sha256.clone()));
        previous = Some(prefix.events_sha256);
    }
}

#[test]
fn frozen_prefix_keeps_lookahead_anchor_without_accepting_its_source() {
    let mut tracker = frontier(RoutingPolicy::SolanaLastKnownV1);
    tracker.receive(event(100, 1)).unwrap();
    tracker.receive(event(101, 1)).unwrap();
    let route = RoutingCheckpoint {
        policy: RoutingPolicy::SolanaLastKnownV1,
        anchor: Some(TimestampAnchor {
            source_ordinal: 2,
            source_block_num: 101,
            source_block_id: "block-101".into(),
            seconds: 1_700_000_000,
            provenance: AnchorProvenance::Lookahead,
        }),
    };
    tracker.accept(1, route.clone()).unwrap();
    let frozen = tracker.snapshot().unwrap().unwrap();
    assert_eq!(frozen.last_ordinal, 1);
    assert_eq!(frozen.last_event.block_num, 100);
    assert_eq!(frozen.routing, route);
    tracker.acknowledge(&frozen).unwrap();
    assert_eq!(tracker.unresolved_events(), 1);
    assert!(tracker.snapshot().unwrap().is_none());
    let mut accepted_route = route.clone();
    accepted_route.anchor.as_mut().unwrap().provenance = AnchorProvenance::AcceptedPrefix;
    tracker.accept(2, accepted_route.clone()).unwrap();
    let next = tracker.snapshot().unwrap().unwrap();
    assert_eq!(next.first_ordinal, 2);
    assert_eq!(next.routing, accepted_route);
    assert_eq!(frozen.routing, route);
}

#[test]
fn fabricated_future_or_mislabeled_anchor_does_not_accept() {
    let mut tracker = frontier(RoutingPolicy::GenesisLookaheadV1);
    tracker.receive(event(100, 1)).unwrap();
    let mut route = RoutingCheckpoint {
        policy: RoutingPolicy::GenesisLookaheadV1,
        anchor: Some(TimestampAnchor {
            source_ordinal: 2,
            source_block_num: 101,
            source_block_id: "block-101".into(),
            seconds: 1_700_000_000,
            provenance: AnchorProvenance::Lookahead,
        }),
    };
    assert!(tracker.accept(1, route.clone()).is_err());
    tracker.receive(event(101, 1)).unwrap();
    route.anchor.as_mut().unwrap().provenance = AnchorProvenance::AcceptedPrefix;
    assert!(tracker.accept(1, route).is_err());
    assert!(tracker.snapshot().unwrap().is_none());
}

#[test]
fn anchor_block_identity_and_time_must_match_actual_received_metadata() {
    let mut tracker = frontier(RoutingPolicy::SolanaLastKnownV1);
    tracker.receive(event(100, 1)).unwrap();
    tracker.receive(event(101, 1)).unwrap();
    let anchor = TimestampAnchor {
        source_ordinal: 2,
        source_block_num: 101,
        source_block_id: "block-101".into(),
        seconds: 1_700_000_000,
        provenance: AnchorProvenance::Lookahead,
    };
    for field in [0, 1, 2] {
        let mut forged = anchor.clone();
        match field {
            0 => forged.source_block_num = 102,
            1 => forged.source_block_id = "foreign".into(),
            _ => forged.seconds += 1,
        };
        assert!(tracker
            .accept(
                1,
                RoutingCheckpoint {
                    policy: RoutingPolicy::SolanaLastKnownV1,
                    anchor: Some(forged)
                }
            )
            .is_err());
    }
    tracker
        .accept(
            1,
            RoutingCheckpoint {
                policy: RoutingPolicy::SolanaLastKnownV1,
                anchor: Some(anchor),
            },
        )
        .unwrap();
}

#[test]
fn missing_source_time_stays_missing_under_explicit_versioned_solana_fallback() {
    let mut tracker = frontier(RoutingPolicy::SolanaLastKnownV1);
    let mut missing = event(100, 1);
    missing.source_timestamp = None;
    tracker.receive(missing).unwrap();
    let mut anchor = TimestampAnchor {
        source_ordinal: 0,
        source_block_num: 0,
        source_block_id: String::new(),
        seconds: crate::ingest::state::SOLANA_GENESIS_ROUTING_SECONDS,
        provenance: AnchorProvenance::SolanaGenesisFallback,
    };
    tracker
        .accept(
            1,
            RoutingCheckpoint {
                policy: RoutingPolicy::SolanaLastKnownV1,
                anchor: Some(anchor.clone()),
            },
        )
        .unwrap();
    let prefix = tracker.snapshot().unwrap().unwrap();
    assert_eq!(prefix.last_event.source_timestamp, None);
    assert_eq!(
        prefix.routing.anchor.unwrap().seconds,
        crate::ingest::state::SOLANA_GENESIS_ROUTING_SECONDS
    );
    anchor.seconds += 1;
    assert!(RoutingCheckpoint {
        policy: RoutingPolicy::SolanaLastKnownV1,
        anchor: Some(anchor)
    }
    .validate(1)
    .is_err());
}

#[test]
fn resumed_lookahead_provenance_must_match_the_actual_next_source_event() {
    use crate::ingest::state::{PartCompression, PendingTransaction, TablePlan};
    let authority = AuthorityState::initial(descriptor(RoutingPolicy::SolanaLastKnownV1)).unwrap();
    let mut tracker = AcceptedFrontier::resume(&authority.checkpoint);
    tracker.receive(event(100, 1)).unwrap();
    tracker.receive(event(101, 1)).unwrap();
    tracker
        .accept(
            1,
            RoutingCheckpoint {
                policy: RoutingPolicy::SolanaLastKnownV1,
                anchor: Some(TimestampAnchor {
                    source_ordinal: 2,
                    source_block_num: 101,
                    source_block_id: "block-101".into(),
                    seconds: 1_700_000_000,
                    provenance: AnchorProvenance::Lookahead,
                }),
            },
        )
        .unwrap();
    let tables = authority
        .descriptor
        .tables
        .iter()
        .map(|(table, schema)| TablePlan {
            table: table.clone(),
            rows: 0,
            schema_sha256: schema.clone(),
            partition: String::new(),
        })
        .collect();
    let pending = PendingTransaction::prepare(
        &authority,
        tracker.snapshot().unwrap().unwrap(),
        tables,
        PartCompression::Zstd,
    )
    .unwrap()
    .committed_after_verification(&authority.descriptor)
    .unwrap();
    let authority = authority.install(&pending).unwrap();
    let mut resumed = AcceptedFrontier::resume(&authority.checkpoint);
    assert!(resumed.receive(event(102, 1)).is_err());
    let mut wrong_time = event(101, 1);
    wrong_time.source_timestamp = Some(1_700_000_001);
    assert!(resumed.receive(wrong_time).is_err());
    assert_eq!(resumed.receive(event(101, 1)).unwrap(), 2);
}

#[test]
fn acknowledgement_rejects_stale_prefix_and_duplicate_or_unknown_acceptance() {
    let mut tracker = frontier(RoutingPolicy::DirectV1);
    assert!(tracker.accept(1, routing(RoutingPolicy::DirectV1)).is_err());
    tracker.receive(event(100, 1)).unwrap();
    tracker.accept(1, routing(RoutingPolicy::DirectV1)).unwrap();
    let old = tracker.snapshot().unwrap().unwrap();
    assert!(tracker.accept(1, routing(RoutingPolicy::DirectV1)).is_err());
    tracker.receive(event(101, 1)).unwrap();
    tracker.accept(2, routing(RoutingPolicy::DirectV1)).unwrap();
    assert!(tracker.acknowledge(&old).is_err());
    let current = tracker.snapshot().unwrap().unwrap();
    tracker.acknowledge(&current).unwrap();
    assert!(tracker.acknowledge(&current).is_err());
}

#[test]
fn bounded_unresolved_queue_and_ordinal_overflow_fail_closed() {
    let mut tracker = frontier(RoutingPolicy::DirectV1);
    tracker.assigned_ordinal = u64::MAX;
    assert!(tracker.receive(event(100, 1)).is_err());
    tracker.assigned_ordinal = 0;
    for _ in 0..MAX_BUFFERED_EVENTS {
        tracker.receive(event(100, 1)).unwrap();
    }
    assert!(tracker.receive(event(100, 1)).is_err());
    assert!(tracker.snapshot().unwrap().is_none());
}
