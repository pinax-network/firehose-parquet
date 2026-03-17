use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use firehose_parquet::encode::{bytes_data_type, EncodeBytes};
use firehose_parquet::traits::{canonical_fields_with_encoding, fork_step_field};
use std::sync::Arc;

fn maybe_fork_step(fields: &mut Vec<Field>, include: bool) {
    if include {
        fields.push(fork_step_field());
    }
}

pub fn blocks_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("number", DataType::UInt32, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("producer", DataType::Utf8, false),
        Field::new("confirmed", DataType::UInt32, false),
        Field::new("schedule_version", DataType::UInt32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("index", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("cpu_usage_us", DataType::UInt32, false),
        Field::new("net_usage", DataType::UInt64, false),
        Field::new("elapsed", DataType::Int64, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn actions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("action_ordinal", DataType::UInt32, false),
        Field::new("creator_action_ordinal", DataType::UInt32, false),
        Field::new(
            "closest_unnotified_ancestor_action_ordinal",
            DataType::UInt32,
            false,
        ),
        Field::new("execution_index", DataType::UInt32, false),
        Field::new("receiver", DataType::Utf8, false),
        Field::new("account", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("authorization", DataType::Utf8, true),
        Field::new("json_data", DataType::Utf8, true),
        Field::new("raw_data", bd.clone(), true),
        Field::new("context_free", DataType::Boolean, false),
        Field::new("elapsed", DataType::Int64, false),
        Field::new("console", DataType::Utf8, true),
        Field::new("transaction_id", DataType::Utf8, false),
        Field::new("trace_block_num", DataType::UInt64, false),
        Field::new("producer_block_id", DataType::Utf8, false),
        Field::new(
            "block_time",
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
            true,
        ),
        Field::new("raw_return_value", bd, true),
        Field::new("json_return_value", DataType::Utf8, true),
        Field::new("exception", DataType::Utf8, true),
        Field::new("error_code", DataType::UInt64, false),
        Field::new("receipt_receiver", DataType::Utf8, false),
        Field::new("receipt_digest", DataType::Utf8, false),
        Field::new("receipt_global_sequence", DataType::UInt64, false),
        Field::new("receipt_auth_sequence", DataType::Utf8, true),
        Field::new("receipt_recv_sequence", DataType::UInt64, false),
        Field::new("receipt_code_sequence", DataType::UInt64, false),
        Field::new("receipt_abi_sequence", DataType::UInt64, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn db_ops_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("operation", DataType::Utf8, false),
        Field::new("action_index", DataType::UInt32, false),
        Field::new("code", DataType::Utf8, false),
        Field::new("scope", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("primary_key", DataType::Utf8, false),
        Field::new("old_payer", DataType::Utf8, false),
        Field::new("new_payer", DataType::Utf8, false),
        Field::new("old_data", bd.clone(), true),
        Field::new("new_data", bd, true),
        Field::new("old_data_json", DataType::Utf8, true),
        Field::new("new_data_json", DataType::Utf8, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// Antelope tables emitted by default.
pub const TABLE_NAMES: [&str; 4] = ["blocks", "transactions", "actions", "db_ops"];
