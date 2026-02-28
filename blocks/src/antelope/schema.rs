use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{canonical_fields, fork_step_field};

fn maybe_fork_step(fields: &mut Vec<Field>, include: bool) {
    if include {
        fields.push(fork_step_field());
    }
}

pub fn blocks_schema(include_fork_step: bool, _encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields();
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

pub fn transactions_schema(include_fork_step: bool, _encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("index", DataType::UInt64, false),
        Field::new("status", DataType::Int32, false),
        Field::new("cpu_usage_us", DataType::UInt32, false),
        Field::new("net_usage", DataType::UInt64, false),
        Field::new("elapsed", DataType::Int64, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn actions_schema(include_fork_step: bool, _encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("action_ordinal", DataType::UInt32, false),
        Field::new("receiver", DataType::Utf8, false),
        Field::new("account", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("authorization", DataType::Utf8, false),
        Field::new("data", DataType::Binary, false),
        Field::new("console", DataType::Utf8, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn db_ops_schema(include_fork_step: bool, _encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("action_index", DataType::UInt32, false),
        Field::new("operation", DataType::Int32, false),
        Field::new("code", DataType::Utf8, false),
        Field::new("scope", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("primary_key", DataType::Utf8, false),
        Field::new("old_data", DataType::Binary, true),
        Field::new("new_data", DataType::Binary, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// Standard table names (available at BASE detail level).
pub const BASE_TABLE_NAMES: [&str; 3] = ["blocks", "transactions", "actions"];

/// Extended table names (available at EXTENDED detail level).
pub const EXTENDED_TABLE_NAMES: [&str; 4] = ["blocks", "transactions", "actions", "db_ops"];
