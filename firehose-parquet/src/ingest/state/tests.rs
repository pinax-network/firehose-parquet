use super::*;
use crate::ingest::frontier::AcceptedFrontier;

pub(crate) fn descriptor(policy: RoutingPolicy) -> StreamDescriptor {
    StreamDescriptor {
        format_version: FORMAT_VERSION,
        chain: "mainnet".into(),
        family: BlockFamily::Evm,
        bytes_encoding: "binary".into(),
        mapper_epoch: MAPPER_EPOCH.into(),
        partition: PartitionPolicy::BlockRange {
            size: 100,
            anchor: 100,
        },
        origin_start: 100,
        final_blocks_only: true,
        extended: false,
        with_votes: false,
        include_failed_transactions: true,
        routing_policy: policy,
        output: StorageIdentity::Local {
            canonical_root: "/tmp/dataset".into(),
        },
        mirror: MirrorBinding::Disabled,
        tables: ["blocks", "logs"]
            .into_iter()
            .map(|table| {
                (
                    table.into(),
                    Digest::hash("schema-fixture", &table).unwrap(),
                )
            })
            .collect(),
    }
}

pub(crate) fn event(number: u64, step: i32) -> EventIdentity {
    EventIdentity {
        cursor: OpaqueCursor::new(format!("private-cursor-{number}-{step}")).unwrap(),
        block_num: number,
        block_id: format!("block-{number}"),
        fork_step: step,
        source_timestamp: Some(1_700_000_000),
    }
}

pub(crate) fn routing(policy: RoutingPolicy) -> RoutingCheckpoint {
    RoutingCheckpoint {
        policy,
        anchor: None,
    }
}

fn prefix(authority: &AuthorityState) -> AcceptedPrefix {
    let mut frontier = AcceptedFrontier::resume(&authority.checkpoint);
    let ordinal = frontier.receive(event(100, 1)).unwrap();
    frontier
        .accept(ordinal, routing(authority.descriptor.routing_policy))
        .unwrap();
    frontier.snapshot().unwrap().unwrap()
}

fn table_plan(authority: &AuthorityState, rows: u64) -> Vec<TablePlan> {
    authority
        .descriptor
        .tables
        .iter()
        .map(|(table, schema)| TablePlan {
            table: table.clone(),
            rows,
            schema_sha256: schema.clone(),
            partition: "100-199".into(),
        })
        .collect()
}

fn pending(rows: u64) -> (AuthorityState, PendingTransaction) {
    let authority = AuthorityState::initial(descriptor(RoutingPolicy::DirectV1)).unwrap();
    let pending = PendingTransaction::prepare(
        &authority,
        prefix(&authority),
        table_plan(&authority, rows),
        PartCompression::Zstd,
    )
    .unwrap();
    (authority, pending)
}

#[test]
fn semantic_changes_bind_stream_while_table_insertion_order_does_not() {
    let original = descriptor(RoutingPolicy::DirectV1);
    let expected = original.id().unwrap();
    let mut reordered = original.clone();
    reordered.tables = original
        .tables
        .iter()
        .rev()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_eq!(expected, reordered.id().unwrap());
    let changes: Vec<Box<dyn Fn(&mut StreamDescriptor)>> = vec![
        Box::new(|d| d.bytes_encoding = "hex".into()),
        Box::new(|d| d.final_blocks_only = false),
        Box::new(|d| d.extended = true),
        Box::new(|d| d.with_votes = true),
        Box::new(|d| d.include_failed_transactions = false),
        Box::new(|d| d.family = BlockFamily::Solana),
        Box::new(|d| d.routing_policy = RoutingPolicy::SolanaLastKnownV1),
        Box::new(|d| {
            d.mirror = MirrorBinding::Local {
                absolute_path: "/tmp/cursor.parquet".into(),
            }
        }),
        Box::new(|d| {
            d.output = StorageIdentity::Local {
                canonical_root: "/tmp/other-dataset".into(),
            }
        }),
        Box::new(|d| {
            d.origin_start = 200;
            d.partition = PartitionPolicy::BlockRange {
                size: 100,
                anchor: 200,
            };
        }),
    ];
    for change in changes {
        let mut modified = original.clone();
        change(&mut modified);
        assert_ne!(expected, modified.id().unwrap());
    }
}

#[test]
fn semantic_epoch_encoding_and_original_anchor_fail_closed() {
    let mut d = descriptor(RoutingPolicy::DirectV1);
    d.mapper_epoch = "unknown".into();
    assert!(d.id().is_err());
    d.mapper_epoch = MAPPER_EPOCH.into();
    d.bytes_encoding = "base64".into();
    assert!(d.id().is_err());
    d.bytes_encoding = "binary".into();
    d.partition = PartitionPolicy::BlockRange {
        size: 100,
        anchor: 101,
    };
    assert!(d.id().is_err());
    d.partition = PartitionPolicy::BlockRange {
        size: 0,
        anchor: 100,
    };
    assert!(d.id().is_err());
}

#[test]
fn canonical_encoding_sorts_nested_maps_but_preserves_array_order() {
    let first = serde_json::json!({"z":[{"b":2,"a":1}],"a":"value"});
    assert_eq!(
        canonical_json(&first).unwrap(),
        br#"{"a":"value","z":[{"a":1,"b":2}]}"#
    );
    assert_ne!(
        Digest::hash("list", &vec![1, 2]).unwrap(),
        Digest::hash("list", &vec![2, 1]).unwrap()
    );
    assert_ne!(
        Digest::hash("one", &first).unwrap(),
        Digest::hash("two", &first).unwrap()
    );
}

#[test]
fn plan_is_complete_sorted_deterministic_and_declares_zero_row_tables() {
    let (authority, original) = pending(3);
    let mut tables = table_plan(&authority, 3);
    tables.reverse();
    let reordered = PendingTransaction::prepare(
        &authority,
        original.prefix.clone(),
        tables,
        PartCompression::Zstd,
    )
    .unwrap();
    assert!(original == reordered);
    let mut tables = table_plan(&authority, 3);
    tables[0].rows = 0;
    let mixed = PendingTransaction::prepare(
        &authority,
        original.prefix.clone(),
        tables,
        PartCompression::Zstd,
    )
    .unwrap();
    assert_eq!(mixed.tables.len(), 2);
    assert_eq!(mixed.parts.len(), 1);
    assert_eq!(mixed.parts[0].entry_index, 1);
    assert_eq!(mixed.parts[0].table, "logs");
    let (authority, empty) = pending(0);
    assert!(empty.parts.is_empty());
    let next = authority
        .install(
            &empty
                .committed_after_verification(&authority.descriptor)
                .unwrap(),
        )
        .unwrap();
    assert_eq!(next.checkpoint.ordinal, 1);
    assert_eq!(
        next.checkpoint.event.unwrap().cursor.as_str(),
        "private-cursor-100-1"
    );
}

#[test]
fn receipts_freeze_before_commit_and_are_not_transaction_identity() {
    let (authority, pending) = pending(1);
    assert!(pending
        .committed_after_verification(&authority.descriptor)
        .is_err());
    let receipt = PartReceipt {
        byte_size: 128,
        sha256: Digest::hash("bytes", &1).unwrap(),
    };
    let frozen = pending
        .with_receipt(0, receipt.clone(), &authority.descriptor)
        .unwrap();
    assert_eq!(pending.id, frozen.id);
    assert!(frozen
        .with_receipt(0, receipt.clone(), &authority.descriptor)
        .is_ok());
    let other = PartReceipt {
        byte_size: 129,
        ..receipt.clone()
    };
    assert!(frozen
        .with_receipt(0, other, &authority.descriptor)
        .is_err());
    assert!(frozen
        .with_receipt(99, receipt.clone(), &authority.descriptor)
        .is_err());
    let complete = frozen
        .with_receipt(1, receipt, &authority.descriptor)
        .unwrap()
        .committed_after_verification(&authority.descriptor)
        .unwrap();
    assert!(authority.install(&pending).is_err());
    let next = authority.install(&complete).unwrap();
    assert_eq!(next.checkpoint.ordinal, 1);
    assert!(next.install(&complete).is_err());
}

#[test]
fn malformed_inventory_schema_partition_and_planned_paths_are_rejected() {
    let (authority, pending) = pending(1);
    let mut tables = table_plan(&authority, 1);
    tables.pop();
    assert!(PendingTransaction::prepare(
        &authority,
        pending.prefix.clone(),
        tables,
        PartCompression::Zstd
    )
    .is_err());
    for path in [
        "../other",
        "/absolute",
        "x//y",
        "x/./y",
        "x\\y",
        ".fireparq-ingest",
    ] {
        let mut tables = table_plan(&authority, 1);
        tables[0].partition = path.into();
        assert!(
            PendingTransaction::prepare(
                &authority,
                pending.prefix.clone(),
                tables,
                PartCompression::Zstd
            )
            .is_err(),
            "{path}"
        );
    }
    let mut tables = table_plan(&authority, 1);
    tables[0].schema_sha256 = Digest::hash("schema", &"foreign").unwrap();
    assert!(PendingTransaction::prepare(
        &authority,
        pending.prefix.clone(),
        tables,
        PartCompression::Zstd
    )
    .is_err());
    let mut bad = pending.clone();
    bad.parts[0].final_relative_path = "blocks/foreign.parquet".into();
    assert!(bad.validate(&authority.descriptor).is_err());
    let mut bad = pending.clone();
    bad.parts[0].temporary_relative_path = "blocks/.foreign.tmp".into();
    assert!(bad.validate(&authority.descriptor).is_err());
    let mut bad = pending.clone();
    bad.tables[0].rows += 1;
    assert!(bad.validate(&authority.descriptor).is_err());
}

#[test]
fn strict_decode_rejects_unknown_fields_corruption_and_redacts_private_cursor() {
    let (authority, pending) = pending(1);
    let bytes = serde_json::to_vec(&pending).unwrap();
    let decoded: PendingTransaction = serde_json::from_slice(&bytes).unwrap();
    decoded.validate(&authority.descriptor).unwrap();
    let mut value = serde_json::to_value(&pending).unwrap();
    value["extra"] = true.into();
    assert!(serde_json::from_value::<PendingTransaction>(value).is_err());
    let mut value = serde_json::to_value(&pending).unwrap();
    value["prefix"]["last_event"]["block_num"] = 101.into();
    assert!(serde_json::from_value::<PendingTransaction>(value)
        .unwrap()
        .validate(&authority.descriptor)
        .is_err());
    let cursor = &pending.prefix.last_event.cursor;
    assert!(!format!("{cursor:?}").contains(cursor.as_str()));
    assert!(!format!("{:?}", pending.prefix.last_event).contains(cursor.as_str()));
    assert!(OpaqueCursor::new("").is_err());
    assert!(OpaqueCursor::new("x".repeat(MAX_CURSOR_BYTES + 1)).is_err());
    assert!(Digest::parse("A".repeat(64)).is_err());
}

#[test]
fn maximum_ordinals_keep_identity_filenames_below_component_limit() {
    let (_, mut pending) = pending(1);
    pending.prefix.first_ordinal = u64::MAX;
    pending.prefix.last_ordinal = u64::MAX;
    let parts = PendingTransaction::derive_parts(
        &pending.id,
        &pending.stream_id,
        &pending.prefix,
        &pending.tables,
    )
    .unwrap();
    for part in parts {
        assert!(part.final_relative_path.rsplit('/').next().unwrap().len() <= 255);
    }
}

#[test]
fn even_rehashed_pending_cannot_skip_authoritative_ordinal() {
    let (authority, mut pending) = pending(0);
    pending.prefix.first_ordinal = 2;
    pending.prefix.last_ordinal = 2;
    pending.target.ordinal = 2;
    pending.target.id = pending.target.identity(&pending.stream_id).unwrap();
    pending.id = PendingTransaction::identity(
        &pending.stream_id,
        &pending.predecessor,
        &pending.prefix,
        &pending.target,
        &pending.tables,
        pending.compression,
    )
    .unwrap();
    pending.phase = TransactionPhase::Committed;
    pending.validate(&authority.descriptor).unwrap();
    assert!(authority.install(&pending).is_err());
}
