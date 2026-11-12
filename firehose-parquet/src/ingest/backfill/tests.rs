use super::*;
use crate::ingest::state::tests::{descriptor, event};
use crate::ingest::state::{Digest, MirrorBinding};

/// The fixture stream (start 100) with S3 output and a mirror.
fn dataset() -> StreamDescriptor {
    let service = Digest::hash("service-fixture", &"s3").unwrap();
    StreamDescriptor {
        output: StorageIdentity::S3 {
            service: service.clone(),
            bucket: "lake".into(),
            prefix: "mainnet".into(),
        },
        mirror: MirrorBinding::S3 {
            service,
            bucket: "lake".into(),
            key: "mainnet/_fireparq/cursor.parquet".into(),
        },
        ..descriptor(RoutingPolicy::GenesisLookaheadV1)
    }
}

#[test]
fn lanes_are_adjacent_disjoint_and_cover_the_range() {
    let plan = BackfillPlan::new(dataset(), 111, 4).unwrap();
    let lanes: Vec<_> = plan.lanes().collect();
    assert_eq!(lanes, [(100, 104), (104, 108), (108, 111)]);
    assert_eq!(plan.lane_count(), 3);
    for (start, stop) in &lanes {
        assert!(plan.has_lane(*start, *stop));
    }
    // Any other range is not a lane: shifted, merged, cut short or outside.
    for (start, stop) in [
        (100, 111),
        (100, 108),
        (101, 105),
        (104, 107),
        (108, 112),
        (96, 100),
        (111, 115),
        (104, 104),
    ] {
        assert!(!plan.has_lane(start, stop), "{start}..{stop}");
        assert!(plan.require_lane(start, stop).is_err());
    }
    // One lane holding the whole range.
    let whole = BackfillPlan::new(dataset(), 111, 1_000).unwrap();
    assert_eq!(whole.lanes().collect::<Vec<_>>(), [(100, 111)]);
}

#[test]
fn a_plan_requires_final_only_s3_output_with_per_block_routing() {
    assert!(BackfillPlan::new(dataset(), 100, 4).is_err(), "empty range");
    assert!(BackfillPlan::new(dataset(), 111, 0).is_err(), "empty lanes");
    assert!(
        BackfillPlan::new(dataset(), 100 + MAX_LANES + 1, 1).is_err(),
        "too many lanes"
    );
    let local = descriptor(RoutingPolicy::GenesisLookaheadV1);
    assert!(BackfillPlan::new(local, 111, 4).is_err(), "local output");
    let non_final = StreamDescriptor {
        final_blocks_only: false,
        ..dataset()
    };
    assert!(BackfillPlan::new(non_final, 111, 4).is_err());
    let solana = StreamDescriptor {
        routing_policy: RoutingPolicy::SolanaLastKnownV1,
        ..dataset()
    };
    assert!(BackfillPlan::new(solana, 111, 4).is_err());
}

#[test]
fn a_lane_is_the_dataset_stream_from_its_own_start_without_a_mirror() {
    let plan = BackfillPlan::new(dataset(), 111, 4).unwrap();
    let lane = plan.lane_descriptor(104);
    assert_eq!(lane.origin_start, 104);
    assert!(lane.mirror == MirrorBinding::Disabled);
    assert!(lane.output == plan.descriptor.output);
    assert!(lane.tables == plan.descriptor.tables);
    // The tables carry the dataset's identity whichever lane commits, and
    // every lane and the dataset commit under different application ids.
    let tables = crate::delta::DeltaIdentity::of(&plan.descriptor).unwrap();
    let mut apps = std::collections::BTreeSet::from([tables.app_id()]);
    for (start, stop) in plan.lanes() {
        let identity = tables.clone().for_lane(start, stop).unwrap();
        assert_eq!(identity.descriptor, tables.descriptor);
        assert_eq!(identity.configuration(), tables.configuration());
        apps.insert(identity.app_id());
    }
    assert_eq!(apps.len(), 4);

    // Without a cursor mirror the first lane's stream is the dataset's own.
    // Its application id still is not.
    let bare = StreamDescriptor {
        mirror: MirrorBinding::Disabled,
        ..dataset()
    };
    let plan = BackfillPlan::new(bare, 111, 4).unwrap();
    assert!(plan.lane_descriptor(100) == plan.descriptor);
    let tables = crate::delta::DeltaIdentity::of(&plan.descriptor).unwrap();
    assert_ne!(
        tables.clone().for_lane(100, 104).unwrap().app_id(),
        tables.app_id()
    );
}

#[test]
fn a_sealed_authority_is_complete_and_resumes_after_the_last_lane() {
    let plan = BackfillPlan::new(dataset(), 111, 4).unwrap();
    let sealed = AuthorityState::sealed(plan.descriptor.clone(), 11, event(110, 3), 111).unwrap();
    sealed.validate().unwrap();
    assert!(sealed.descriptor == plan.descriptor);
    assert_eq!(sealed.checkpoint.ordinal, 11);
    assert_eq!(sealed.checkpoint.completed_stop, Some(111));
    assert_eq!(sealed.checkpoint.event.as_ref().unwrap().block_num, 110);
    assert!(sealed.checkpoint.routing.anchor.is_none());
    // The last event must prove the stop, and something must be accepted.
    assert!(AuthorityState::sealed(plan.descriptor.clone(), 11, event(109, 3), 111).is_err());
    assert!(AuthorityState::sealed(plan.descriptor.clone(), 0, event(110, 3), 111).is_err());
}

#[test]
fn sealing_names_the_lanes_that_block_it() {
    let lane = |start, state, owner: &'static str| LaneReport {
        start,
        stop: start + 4,
        state,
        owner,
        last_block: None,
        ordinal: 4,
        last_event: Some(event(start + 3, 3)),
        unanchored: true,
    };
    let complete = [
        lane(100, LaneState::Complete, "released"),
        lane(104, LaneState::Complete, "released"),
    ];
    let (ordinal, last) = sealable(&complete).unwrap();
    assert_eq!((ordinal, last.block_num), (8, 107));
    for (reports, expected) in [
        (
            [
                lane(100, LaneState::Complete, "released"),
                lane(104, LaneState::NotStarted, "absent"),
            ],
            "104-108 has not started",
        ),
        (
            [
                lane(100, LaneState::Running, "owned"),
                lane(104, LaneState::Complete, "released"),
            ],
            "100-104 is running",
        ),
        (
            [
                lane(100, LaneState::Stopped, "released"),
                lane(104, LaneState::Complete, "released"),
            ],
            "100-104 stopped before its end",
        ),
        (
            [
                lane(100, LaneState::Complete, "owned"),
                lane(104, LaneState::Complete, "released"),
            ],
            "100-104 is complete but its owner is not released",
        ),
    ] {
        let error = sealable(&reports).unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn lanes_are_named_start_dash_stop() {
    let lane = parse_lane("mainnet", "104-108").unwrap();
    assert_eq!((lane.start(), lane.stop()), (104, 108));
    assert_eq!(lane.directory(), "mainnet/.fireparq-ingest/lanes/104-108");
    assert_eq!(
        parse_lane("", "0-250000").unwrap().directory(),
        ".fireparq-ingest/lanes/0-250000"
    );
    for bad in ["104", "108-104", "104-104", "a-b", "104-108-112", "-1-4"] {
        assert!(parse_lane("mainnet", bad).is_err(), "{bad}");
    }
}
