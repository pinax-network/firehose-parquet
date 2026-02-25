use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::traits::{canonical_fields, fork_step_field};

fn maybe_fork_step(fields: &mut Vec<Field>, include: bool) {
    if include {
        fields.push(fork_step_field());
    }
}

pub fn blocks_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("parent_hash", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("witness_address", DataType::Utf8, false),
        Field::new("version", DataType::UInt32, false),
        Field::new("tx_trie_root", DataType::Utf8, false),
        Field::new("parent_number", DataType::UInt64, false),
        Field::new("num_transactions", DataType::UInt32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("txid", DataType::Utf8, false),
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

pub fn logs_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("topic0", DataType::Utf8, true),
        Field::new("topic1", DataType::Utf8, true),
        Field::new("topic2", DataType::Utf8, true),
        Field::new("topic3", DataType::Utf8, true),
        Field::new("data", DataType::Utf8, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn internal_transactions_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("internal_index", DataType::UInt32, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("caller_address", DataType::Utf8, false),
        Field::new("transfer_to_address", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, false),
        Field::new("rejected", DataType::Boolean, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub const TABLE_NAMES: [&str; 4] = ["blocks", "transactions", "logs", "internal_transactions"];
