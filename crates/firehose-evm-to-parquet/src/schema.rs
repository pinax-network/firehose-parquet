use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::traits::{canonical_fields, fork_step_field};

fn maybe_fork_step(fields: &mut Vec<Field>, include: bool) {
    if include {
        fields.push(fork_step_field());
    }
}

// ==========================================================================
// Standard tables (BASE detail level)
// ==========================================================================

pub fn blocks_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("parent_hash", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("gas_limit", DataType::UInt64, false),
        Field::new("base_fee_per_gas", DataType::Utf8, true),
        Field::new("coinbase", DataType::Utf8, false),
        Field::new("size", DataType::UInt64, false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("state_root", DataType::Utf8, false),
        Field::new("transactions_root", DataType::Utf8, false),
        Field::new("receipt_root", DataType::Utf8, false),
        Field::new("difficulty", DataType::Utf8, true),
        Field::new("mix_hash", DataType::Utf8, false),
        Field::new("extra_data", DataType::Utf8, false),
        Field::new("num_transactions", DataType::UInt32, false),
        Field::new("detail_level", DataType::Int32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("index", DataType::UInt32, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("gas_limit", DataType::UInt64, false),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("gas_price", DataType::Utf8, true),
        Field::new("type", DataType::Int32, false),
        Field::new("status", DataType::Int32, false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("input", DataType::Utf8, false),
        Field::new("max_fee_per_gas", DataType::Utf8, true),
        Field::new("max_priority_fee_per_gas", DataType::Utf8, true),
        Field::new("cumulative_gas_used", DataType::UInt64, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn logs_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("block_index", DataType::UInt32, false),
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

// ==========================================================================
// Extended tables (EXTENDED detail level only)
// ==========================================================================

pub fn calls_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("call_index", DataType::UInt32, false),
        Field::new("parent_index", DataType::UInt32, false),
        Field::new("depth", DataType::UInt32, false),
        Field::new("call_type", DataType::Int32, false),
        Field::new("caller", DataType::Utf8, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("gas_limit", DataType::UInt64, false),
        Field::new("gas_consumed", DataType::UInt64, false),
        Field::new("input", DataType::Utf8, false),
        Field::new("output", DataType::Utf8, false),
        Field::new("status_failed", DataType::Boolean, false),
        Field::new("status_reverted", DataType::Boolean, false),
        Field::new("state_reverted", DataType::Boolean, false),
        Field::new("executed_code", DataType::Boolean, false),
        Field::new("suicide", DataType::Boolean, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn balance_changes_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, true),
        Field::new("ordinal", DataType::UInt64, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("old_value", DataType::Utf8, false),
        Field::new("new_value", DataType::Utf8, false),
        Field::new("reason", DataType::Int32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn code_changes_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, true),
        Field::new("ordinal", DataType::UInt64, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("old_hash", DataType::Utf8, false),
        Field::new("new_hash", DataType::Utf8, false),
        Field::new("old_code", DataType::Utf8, false),
        Field::new("new_code", DataType::Utf8, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn storage_changes_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("ordinal", DataType::UInt64, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("old_value", DataType::Utf8, false),
        Field::new("new_value", DataType::Utf8, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn nonce_changes_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("ordinal", DataType::UInt64, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("old_value", DataType::UInt64, false),
        Field::new("new_value", DataType::UInt64, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn gas_changes_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("ordinal", DataType::UInt64, false),
        Field::new("old_value", DataType::UInt64, false),
        Field::new("new_value", DataType::UInt64, false),
        Field::new("reason", DataType::Int32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn account_creations_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("ordinal", DataType::UInt64, false),
        Field::new("account", DataType::Utf8, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn system_calls_schema(include_fork_step: bool) -> Schema {
    calls_schema(include_fork_step)
}

/// Standard table names (available at BASE detail level).
pub const BASE_TABLE_NAMES: [&str; 3] = ["blocks", "transactions", "logs"];

/// Extended table names (available at EXTENDED detail level).
pub const EXTENDED_TABLE_NAMES: [&str; 10] = [
    "blocks",
    "transactions",
    "logs",
    "calls",
    "balance_changes",
    "code_changes",
    "storage_changes",
    "nonce_changes",
    "gas_changes",
    "account_creations",
];
