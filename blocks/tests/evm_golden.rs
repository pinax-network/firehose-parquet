//! Fixed, independently reviewed raw Firehose data; no network in cargo test.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use blocks::evm::mapper::EvmBlockMapper;
use firehose_parquet::{
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper},
};
use firehose_protos::eth;
use prost::{bytes::Bytes, Message};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};

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
    /// The unchanged `Any.value` payload.
    block: &'static [u8],
    metadata: &'static str,
    expected: &'static str,
    /// Transaction traces in the payload, the mapper's return value.
    transactions: u64,
    /// Canonical millisecond timestamp and UTC day, from the block header time.
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

/// Normalize Arrow cells only for comparison. Binary values are compared with
/// the independent fixture's explicit hex bytes; no production encoder is used.
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
        DataType::UInt64 => primitive!(UInt64Array),
        DataType::UInt32 => primitive!(UInt32Array),
        DataType::Int64 => primitive!(Int64Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Boolean => primitive!(BooleanArray),
        DataType::Utf8 => primitive!(StringArray),
        DataType::Binary => json!(hex(array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(row))),
        DataType::Date32 => primitive!(Date32Array),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            primitive!(TimestampMillisecondArray)
        }
        DataType::Dictionary(_, _) => {
            let dict = array
                .as_any()
                .downcast_ref::<DictionaryArray<arrow::datatypes::Int32Type>>()
                .unwrap();
            cell(dict.values().as_ref(), dict.key(row).unwrap())
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

/// Map the fixture through the borrowed and the owned (production, #518) entry
/// points and require identical tables, schemas and rows.
fn map_both_ways(
    fixture: &Fixture,
    identity: &BlockIdentity,
    extended: bool,
    encoding: &EncodeBytes,
    fork_step: bool,
) -> HashMap<String, RecordBatch> {
    let step = fork_step.then_some("FINAL");
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
    borrowed
}

fn check_fixture(fixture: &Fixture) {
    let identity = fixture_identity(fixture);
    let expected: Value = serde_json::from_str(fixture.expected).unwrap();
    assert_eq!(expected["format_version"], 1);
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
                        "lib_num": identity.lib_num, "timestamp": fixture.timestamp_millis,
                        "date": fixture.date
                    });
                    for row in 0..batch.num_rows() {
                        for (name, value) in canonical.as_object().unwrap() {
                            assert_value(batch, row, name, value, table);
                        }
                        if fork_step {
                            assert_value(batch, row, "fork_step", &json!("FINAL"), table);
                        }
                    }
                    assert_eq!(batch.column_by_name("fork_step").is_some(), fork_step);
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
