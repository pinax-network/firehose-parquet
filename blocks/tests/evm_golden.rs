//! Fixed, independently reviewed raw Firehose data; no network in cargo test.
//!
//! The expectations describe what a build writes: the Delta data file batches
//! of the flush boundary (#643), with Delta types, microsecond timestamps and
//! the `date` partition column left out.
use arrow::{
    array::*,
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use blocks::{chain::ChainKind, evm::mapper::EvmBlockMapper};
use firehose_parquet::{
    config::BlockMetadata,
    date_partition::DatePartition,
    delta::types::is_delta_type,
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper, StreamEvent},
};
use firehose_protos::eth;
use prost::{bytes::Bytes, Message};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

/// The writer's in-block row order, as the maintenance job's repair sorts by it.
#[path = "../../maintenance/src/row_order.rs"]
mod row_order;

const STANDARD_TABLES: [&str; 6] = [
    "blocks",
    "transactions",
    "logs",
    "withdrawals",
    "access_lists",
    "set_code_authorizations",
];

/// One retained raw block and its independently reviewed oracle.
struct Fixture {
    name: &'static str,
    /// The unchanged `Any.value` payload (decompressed when stored as zstd).
    block: &'static [u8],
    metadata: &'static str,
    expected: &'static str,
    /// Transaction traces in the payload, the mapper's return value.
    transactions: u64,
    /// Canonical millisecond timestamp and UTC day, from the block header time.
    /// Delta data files store the timestamp in microseconds.
    timestamp_millis: i64,
    date: i32,
}

/// Mainnet 26,049,575: blob, legacy and dynamic-fee transactions, one reverted.
fn block_26049575() -> Fixture {
    Fixture {
        name: "evm-mainnet (26049575)",
        block: include_bytes!("fixtures/evm-mainnet/block.pb"),
        metadata: include_str!("fixtures/evm-mainnet/metadata.json"),
        expected: include_str!("fixtures/evm-mainnet/expected.json"),
        transactions: 182,
        timestamp_millis: 1_790_280_203_000,
        date: 20720,
    }
}

/// Mainnet 26,000,004: EIP-7702 authorizations, code changes and a reverted
/// SET_CODE transaction. Stored zstd-compressed; the checksum in its metadata
/// is for the decompressed original payload.
fn block_26000004() -> Fixture {
    static RAW: OnceLock<Vec<u8>> = OnceLock::new();
    let raw = RAW.get_or_init(|| {
        zstd::decode_all(&include_bytes!("fixtures/evm-mainnet-26000004/block.pb.zst")[..]).unwrap()
    });
    Fixture {
        name: "evm-mainnet-26000004",
        block: raw.as_slice(),
        metadata: include_str!("fixtures/evm-mainnet-26000004/metadata.json"),
        expected: include_str!("fixtures/evm-mainnet-26000004/expected.json"),
        transactions: 397,
        timestamp_millis: 1_789_681_811_000,
        date: 20713,
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut value = String::from("0x");
    for byte in bytes {
        use std::fmt::Write;
        write!(&mut value, "{byte:02x}").unwrap();
    }
    value
}

fn fixture_identity(fixture: &Fixture) -> BlockIdentity {
    let block = fixture.block;
    let metadata: Value = serde_json::from_str(fixture.metadata).unwrap();
    assert_eq!(metadata["format_version"], 1);
    assert_eq!(metadata["chain"], "mainnet");
    assert_eq!(metadata["fork_step"], "FINAL");
    assert_eq!(metadata["byte_length"], block.len());
    assert_eq!(metadata["sha256"], format!("{:x}", Sha256::digest(block)));
    let id = &metadata["identity"];
    let identity = BlockIdentity {
        block_num: id["block_num"].as_u64().unwrap(),
        block_id: id["block_id"].as_str().unwrap().into(),
        parent_num: id["parent_num"].as_u64().unwrap(),
        parent_id: id["parent_id"].as_str().unwrap().into(),
        lib_num: id["lib_num"].as_u64().unwrap(),
        timestamp: id["timestamp"].as_i64().unwrap(),
        timestamp_nanos: id["timestamp_nanos"].as_i64().unwrap().try_into().unwrap(),
        fork_step: None,
    };
    // The canonical columns are sourced from response metadata. Independently
    // verify it describes the retained payload, not a different block.
    let raw = eth::Block::decode(block).unwrap();
    let header = raw.header.unwrap();
    let time = header.timestamp.unwrap();
    assert_eq!(raw.number, identity.block_num);
    assert_eq!(hex(&raw.hash), format!("0x{}", identity.block_id));
    assert_eq!(
        hex(&header.parent_hash),
        format!("0x{}", identity.parent_id)
    );
    assert_eq!(identity.parent_num + 1, identity.block_num);
    assert_eq!(
        (time.seconds, time.nanos),
        (identity.timestamp, identity.timestamp_nanos)
    );
    assert_eq!(time.seconds * 1000, fixture.timestamp_millis);
    assert_eq!(time.seconds.div_euclid(86_400), i64::from(fixture.date));
    identity
}

/// Normalize Delta data file cells only for comparison. Binary values are
/// compared with the independent fixture's explicit hex bytes; no production
/// encoder is used. A `decimal(20,0)` is an exact decimal string, so a column
/// mapped to the wrong Delta type fails against the fixture. The mapper's
/// unsigned, dictionary and millisecond types never reach a data file.
fn cell(array: &dyn Array, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    macro_rules! primitive {
        ($ty:ty) => {
            json!(array.as_any().downcast_ref::<$ty>().unwrap().value(row))
        };
    }
    match array.data_type() {
        DataType::Int64 => primitive!(Int64Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int16 => primitive!(Int16Array),
        DataType::Decimal128(20, 0) => json!(array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value_as_string(row)),
        DataType::Boolean => primitive!(BooleanArray),
        DataType::Utf8 => primitive!(StringArray),
        DataType::Binary => json!(hex(array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(row))),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            primitive!(TimestampMicrosecondArray)
        }
        DataType::List(_) => {
            let values = array
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(row);
            Value::Array(
                (0..values.len())
                    .map(|i| cell(values.as_ref(), i))
                    .collect(),
            )
        }
        other => panic!("add an explicit comparison for {other:?}"),
    }
}

fn assert_value(batch: &RecordBatch, row: usize, column: &str, expected: &Value, source: &str) {
    let actual = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("missing {column}, source {source}"));
    assert_eq!(
        &cell(actual.as_ref(), row),
        expected,
        "row {row}, column {column}, source {source}"
    );
}

/// The accepted-event ordinal the fixture block is mapped with on non-final
/// streams; every row of every table must carry it.
const GOLDEN_STREAM_ORDINAL: u64 = 4_242;

/// The fixture flush as a build writes it: every table mapped onto its Delta
/// data file at the flush boundary (#643), routed to the block's UTC day.
fn delta_batches(
    batches: HashMap<String, RecordBatch>,
    identity: &BlockIdentity,
) -> HashMap<String, RecordBatch> {
    let metadata = BlockMetadata {
        min_block_number: identity.block_num,
        max_block_number: identity.block_num,
        min_timestamp: Some(identity.timestamp),
        max_timestamp: Some(identity.timestamp),
    };
    ChainKind::Evm
        .profile()
        .delta_types()
        .data_batches(batches, &metadata)
        .unwrap()
}

/// Map the fixture through the borrowed and the owned (production, #518) entry
/// points and require identical tables, schemas and rows, then map the flush
/// onto its Delta data files.
fn map_both_ways(
    fixture: &Fixture,
    identity: &BlockIdentity,
    extended: bool,
    encoding: &EncodeBytes,
    fork_step: bool,
) -> HashMap<String, RecordBatch> {
    let step = StreamEvent::new(fork_step.then_some("FINAL"), GOLDEN_STREAM_ORDINAL);
    let mut borrowed = EvmBlockMapper::new(extended, fork_step, encoding.clone(), true);
    assert_eq!(
        borrowed.map_block(fixture.block, identity, step).unwrap(),
        fixture.transactions
    );
    let borrowed = borrowed.flush().unwrap();
    let mut owned = EvmBlockMapper::new(extended, fork_step, encoding.clone(), true);
    assert_eq!(
        owned
            .map_block_bytes(Bytes::from_static(fixture.block), identity, step)
            .unwrap(),
        fixture.transactions
    );
    let owned = owned.flush().unwrap();
    let tables = |batches: &HashMap<String, RecordBatch>| {
        batches
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(tables(&borrowed), tables(&owned), "{}", fixture.name);
    for (table, batch) in &borrowed {
        let other = &owned[table];
        assert_eq!(batch.schema(), other.schema(), "{}: {table}", fixture.name);
        assert_eq!(
            batch, other,
            "{}: {table} differs between map_block and map_block_bytes \
             (extended={extended}, encoding={encoding:?}, fork_step={fork_step})",
            fixture.name
        );
    }
    delta_batches(borrowed, identity)
}

/// The rows of one block's `batch` are strictly increasing on the table's
/// row-order key.
fn assert_writer_row_order(table: &str, batch: &RecordBatch, source: &str) {
    let key = row_order::row_order_key(table)
        .unwrap_or_else(|| panic!("{source}: no row order for {table}"));
    assert!(row_order::EVM_TABLES.contains(&table), "{table}");
    let columns: Vec<(row_order::KeyPart, ArrayRef)> = key
        .iter()
        .map(|part| {
            let name = match part {
                row_order::KeyPart::Column(name) | row_order::KeyPart::IsSet(name) => name,
            };
            let column = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("{source}: {table} has no {name}"));
            (*part, column.clone())
        })
        .collect();
    let key_of = |row: usize| -> Vec<i64> {
        columns
            .iter()
            .map(|(part, column)| match part {
                row_order::KeyPart::IsSet(_) => i64::from(column.is_valid(row)),
                row_order::KeyPart::Column(name) => {
                    let values = arrow::compute::cast(column, &DataType::Int64).unwrap();
                    let values = values.as_primitive::<arrow::datatypes::Int64Type>();
                    assert!(values.is_valid(row), "{source}: {table}.{name} is null");
                    values.value(row)
                }
            })
            .collect()
    };
    for row in 1..batch.num_rows() {
        let (before, after) = (key_of(row - 1), key_of(row));
        assert!(
            before < after,
            "{source}: {table} rows {} and {row} are not in key order: {before:?}, {after:?}",
            row - 1
        );
    }
}

fn check_fixture(fixture: &Fixture) {
    let identity = fixture_identity(fixture);
    let expected: Value = serde_json::from_str(fixture.expected).unwrap();
    assert_eq!(expected["format_version"], 1);
    // The `date` partition of every table is the block's UTC day.
    assert_eq!(
        DatePartition::from_timestamp(identity.timestamp)
            .unwrap()
            .date32(),
        fixture.date
    );
    for extended in [false, true] {
        for encoding in [EncodeBytes::Binary, EncodeBytes::Hex] {
            for fork_step in [false, true] {
                let batches = map_both_ways(fixture, &identity, extended, &encoding, fork_step);
                let actual_counts: BTreeMap<_, _> = batches
                    .iter()
                    .map(|(table, batch)| (table.clone(), batch.num_rows()))
                    .collect();
                let expected_counts: BTreeMap<_, _> = expected["counts"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .filter(|(name, _)| extended || STANDARD_TABLES.contains(&name.as_str()))
                    .map(|(name, count)| (name.clone(), count.as_u64().unwrap() as usize))
                    .collect();
                assert_eq!(
                    actual_counts, expected_counts,
                    "{}: extended={extended}, encoding={encoding:?}, fork_step={fork_step}",
                    fixture.name
                );
                // Every row in every emitted table must remain attributable to
                // the same canonical block, including system and nested rows.
                for (table, batch) in &batches {
                    let canonical = json!({
                        "block_num": identity.block_num, "block_id": format!("0x{}", identity.block_id),
                        "parent_num": identity.parent_num, "parent_id": format!("0x{}", identity.parent_id),
                        "lib_num": identity.lib_num,
                        "timestamp": fixture.timestamp_millis * 1_000
                    });
                    for row in 0..batch.num_rows() {
                        for (name, value) in canonical.as_object().unwrap() {
                            assert_value(batch, row, name, value, table);
                        }
                        if fork_step {
                            assert_value(batch, row, "fork_step", &json!("FINAL"), table);
                            assert_value(
                                batch,
                                row,
                                "stream_ordinal",
                                &json!(GOLDEN_STREAM_ORDINAL),
                                table,
                            );
                        }
                    }
                    assert_eq!(batch.column_by_name("fork_step").is_some(), fork_step);
                    assert_eq!(batch.column_by_name("stream_ordinal").is_some(), fork_step);
                    // A Delta data file: Delta types only, and no `date`.
                    assert!(batch.column_by_name("date").is_none(), "{table}");
                    for field in batch.schema().fields() {
                        assert!(
                            is_delta_type(field.data_type()),
                            "{table}.{}: {}",
                            field.name(),
                            field.data_type()
                        );
                    }
                    // The byte encoding is a schema contract, not only equal
                    // printable values after conversion.
                    assert_eq!(
                        batch.column_by_name("block_id").unwrap().data_type(),
                        if encoding == EncodeBytes::Binary {
                            &DataType::Binary
                        } else {
                            &DataType::Utf8
                        }
                    );
                }
                // Each table's rows follow its row-order key strictly: sorting
                // by it gives the writer's order back (`maintenance/src/row_order.rs`).
                for (table, batch) in &batches {
                    assert_writer_row_order(table, batch, fixture.name);
                }
                for selection in expected["selections"].as_array().unwrap() {
                    let table = selection["table"].as_str().unwrap();
                    if !extended && !STANDARD_TABLES.contains(&table) {
                        continue;
                    }
                    let row = selection["row"].as_u64().unwrap() as usize;
                    let source = selection["source"].as_str().unwrap();
                    for (column, value) in selection["values"].as_object().unwrap() {
                        assert_value(&batches[table], row, column, value, source);
                    }
                }
            }
        }
    }
}

#[test]
fn mainnet_golden_block_matches_reviewed_counts_values_and_canonical_identity() {
    check_fixture(&block_26049575());
}

#[test]
fn mainnet_set_code_block_matches_reviewed_counts_values_and_canonical_identity() {
    check_fixture(&block_26000004());
}

/// The reverted EIP-7702 transaction at index 130 has two accepted
/// authorizations from one authority. Both authority nonce increments persist;
/// its root call recorded no code change and nothing else it did persists.
#[test]
fn mainnet_reverted_set_code_transaction_keeps_only_persistent_changes() {
    let fixture = block_26000004();
    let identity = fixture_identity(&fixture);
    // Raw source shape, read directly from the payload.
    let raw = eth::Block::decode(fixture.block).unwrap();
    let tx = &raw.transaction_traces[130];
    assert_eq!(tx.index, 130);
    assert_eq!(tx.status, eth::TransactionTraceStatus::Reverted as i32);
    assert_eq!(
        tx.r#type,
        eth::transaction_trace::Type::TrxTypeSetCode as i32
    );
    assert_eq!(tx.set_code_authorizations.len(), 2);
    let authority = tx.set_code_authorizations[0].authority.clone().unwrap();
    assert_eq!(authority.len(), 20);
    for auth in &tx.set_code_authorizations {
        assert!(!auth.discarded);
        assert_eq!(auth.authority.as_ref(), Some(&authority));
    }
    let root = &tx.calls[0];
    assert!(root.state_reverted);
    assert!(root.code_changes.is_empty());
    assert_eq!(
        root.nonce_changes
            .iter()
            .filter(|change| change.address == authority)
            .count(),
        2
    );

    let batches = map_both_ways(&fixture, &identity, true, &EncodeBytes::Binary, false);
    let rows_for_tx = |table: &str| {
        let batch = &batches[table];
        let tx_index = batch
            .column_by_name("tx_index")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..batch.num_rows())
            .filter(|&row| tx_index.value(row) == 130)
            .count()
    };
    assert_eq!(rows_for_tx("set_code_authorizations"), 2);
    // Sender nonce plus one increment per accepted authorization.
    assert_eq!(rows_for_tx("nonce_changes"), 3);
    // Gas buy, gas refund and fee payment.
    assert_eq!(rows_for_tx("balance_changes"), 3);
    assert_eq!(rows_for_tx("code_changes"), 0);
    assert_eq!(rows_for_tx("storage_changes"), 0);
    assert_eq!(rows_for_tx("gas_changes"), 0);
    assert_eq!(rows_for_tx("calls"), 3);
}
