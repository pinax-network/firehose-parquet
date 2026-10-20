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

const STANDARD_TABLES: [&str; 7] = [
    "blocks",
    "transactions",
    "logs",
    "erc20_transfers",
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
    /// Independently counted from the retained wire payload, not the mapper.
    transfer_counts: (usize, usize),
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
        transfer_counts: (190, 115),
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
        transfer_counts: (1_909, 801),
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

/// Re-derive every transfer directly from the fixed protobuf. This oracle uses
/// BigUint for amounts and explicit ABI checks, never the production decoder,
/// decimal formatter or byte encoder. The counts were also obtained with a
/// standalone protobuf wire reader; historical raw-table expectations stay
/// unchanged in expected.json.
fn transfer_expectations(fixture: &Fixture) -> BTreeMap<&'static str, Vec<Value>> {
    let raw = eth::Block::decode(fixture.block).unwrap();
    let mut erc20 = Vec::new();
    let mut native = Vec::new();
    for tx in &raw.transaction_traces {
        if tx.status != eth::TransactionTraceStatus::Succeeded as i32 {
            continue;
        }
        for log in tx.receipt.iter().flat_map(|receipt| &receipt.logs) {
            if log.topics.len() != 3
                || hex(&log.topics[0])
                    != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                || log.address.len() != 20
                || log.data.len() != 32
                || log.topics[1..]
                    .iter()
                    .any(|topic| topic.len() != 32 || topic[..12].iter().any(|&byte| byte != 0))
            {
                continue;
            }
            erc20.push(json!({
                "block_number": raw.number,
                "tx_hash": hex(&tx.hash),
                "tx_index": tx.index,
                "log_index": log.index,
                "block_index": log.block_index,
                "ordinal": log.ordinal,
                "token_address": hex(&log.address),
                "from": hex(&log.topics[1][12..]),
                "to": hex(&log.topics[2][12..]),
                "value": num_bigint::BigUint::from_bytes_be(&log.data).to_string(),
                "decoder_version": 1
            }));
        }
        for call in &tx.calls {
            let value = num_bigint::BigUint::from_bytes_be(
                call.value
                    .as_ref()
                    .map_or(&[][..], |value| value.bytes.as_ref()),
            );
            if call.state_reverted
                || call.status_failed
                || call.status_reverted
                || value == num_bigint::BigUint::from(0u32)
                || !matches!(
                    eth::CallType::try_from(call.call_type),
                    Ok(eth::CallType::Call | eth::CallType::Create)
                )
            {
                continue;
            }
            native.push(json!({
                "block_number": raw.number,
                "tx_hash": hex(&tx.hash),
                "tx_index": tx.index,
                "call_index": call.index,
                "call_type": match eth::CallType::try_from(call.call_type).unwrap() {
                    eth::CallType::Call => "CALL",
                    eth::CallType::Create => "CREATE",
                    _ => unreachable!()
                },
                "ordinal": call.begin_ordinal,
                "from": hex(&call.caller),
                "to": hex(&call.address),
                "value": value.to_string(),
                "decoder_version": 1
            }));
        }
    }
    assert_eq!((erc20.len(), native.len()), fixture.transfer_counts);
    // Fixed exact amounts and source positions qualify the independent
    // arithmetic and selection as well as comparing all rows below.
    let (first_erc20_tx, first_erc20_value, first_native_call, first_native_value) =
        match raw.number {
            26_049_575 => (0, "399700000", 1, "267080795608303"),
            26_000_004 => (11, "3253195078445409763328", 7, "89100000000000000"),
            _ => panic!("unqualified transfer fixture"),
        };
    assert_eq!(erc20[0]["tx_index"], first_erc20_tx);
    assert_eq!(erc20[0]["value"], first_erc20_value);
    assert_eq!(native[0]["call_index"], first_native_call);
    assert_eq!(native[0]["value"], first_native_value);
    BTreeMap::from([("erc20_transfers", erc20), ("native_transfers", native)])
}

fn check_fixture(fixture: &Fixture) {
    let identity = fixture_identity(fixture);
    let expected: Value = serde_json::from_str(fixture.expected).unwrap();
    let transfers = transfer_expectations(fixture);
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
                let mut expected_counts: BTreeMap<_, _> = expected["counts"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .filter(|(name, _)| extended || STANDARD_TABLES.contains(&name.as_str()))
                    .map(|(name, count)| (name.clone(), count.as_u64().unwrap() as usize))
                    .collect();
                expected_counts
                    .insert("erc20_transfers".into(), transfers["erc20_transfers"].len());
                if extended {
                    expected_counts.insert(
                        "native_transfers".into(),
                        transfers["native_transfers"].len(),
                    );
                }
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
                for (table, rows) in &transfers {
                    if !extended && *table == "native_transfers" {
                        continue;
                    }
                    for (row, values) in rows.iter().enumerate() {
                        for (column, value) in values.as_object().unwrap() {
                            assert_value(&batches[*table], row, column, value, fixture.name);
                        }
                    }
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
