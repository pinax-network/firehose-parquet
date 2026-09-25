use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::encode::{bytes_data_type, EncodeBytes};
use firehose_parquet::traits::{canonical_fields_with_encoding, fork_step_field};

fn maybe_fork_step(fields: &mut Vec<Field>, include: bool) {
    if include {
        fields.push(fork_step_field());
    }
}

pub fn blocks_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("height", DataType::Int64, false),
        Field::new("hash", bd.clone(), false),
        Field::new("time", DataType::Int64, false),
        Field::new("chain_id", DataType::Utf8, false),
        Field::new("proposer_address", bd.clone(), false),
        Field::new("last_block_id_hash", bd.clone(), false),
        Field::new("validators_hash", bd.clone(), false),
        Field::new("next_validators_hash", bd, false),
        Field::new("num_txs", DataType::UInt32, false),
        Field::new("tx_decode_failures", DataType::UInt32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("tx_hash", bd, false),
        Field::new("index", DataType::UInt32, false),
        Field::new("code", DataType::UInt32, true),
        Field::new("gas_wanted", DataType::Int64, true),
        Field::new("gas_used", DataType::Int64, true),
        Field::new("log", DataType::Utf8, true),
        Field::new("info", DataType::Utf8, true),
        Field::new("codespace", DataType::Utf8, true),
    ]);
    fields.extend(super::tx_metadata::fields());
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn events_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("source", DataType::Utf8, false),
        Field::new("tx_hash", bd, true),
        Field::new("tx_index", DataType::UInt32, true),
        Field::new("event_index", DataType::UInt32, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("attribute_index", DataType::UInt32, true),
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn messages_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("tx_hash", bd, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("message_index", DataType::UInt32, false),
        Field::new("type_url", DataType::Utf8, false),
        Field::new("value", DataType::Binary, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub const TABLE_NAMES: [&str; 4] = ["blocks", "transactions", "events", "messages"];
