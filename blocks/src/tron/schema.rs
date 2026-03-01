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
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", bd.clone(), false),
        Field::new("parent_hash", bd.clone(), false),
        Field::new("witness_address", bd.clone(), false),
        Field::new("version", DataType::UInt32, false),
        Field::new("tx_trie_root", bd, false),
        Field::new("parent_number", DataType::UInt64, false),
        Field::new("num_transactions", DataType::UInt32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("txid", bd, false),
        Field::new("result", DataType::Boolean, false),
        Field::new("code", DataType::Int32, false),
        Field::new("energy_used", DataType::Int64, false),
        Field::new("energy_penalty", DataType::Int64, false),
        Field::new("fee", DataType::Int64, false),
        Field::new("contract_type", DataType::Int32, false),
        Field::new("expiration", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn logs_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", bd.clone(), false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("address", bd.clone(), false),
        Field::new("topic0", bd.clone(), true),
        Field::new("topic1", bd.clone(), true),
        Field::new("topic2", bd.clone(), true),
        Field::new("topic3", bd.clone(), true),
        Field::new("data", bd, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn internal_transactions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", bd.clone(), false),
        Field::new("internal_index", DataType::UInt32, false),
        Field::new("hash", bd.clone(), false),
        Field::new("caller_address", bd.clone(), false),
        Field::new("transfer_to_address", bd, false),
        Field::new("note", DataType::Utf8, false),
        Field::new("rejected", DataType::Boolean, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub const TABLE_NAMES: [&str; 4] = ["blocks", "transactions", "logs", "internal_transactions"];
