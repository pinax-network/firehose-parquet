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
        Field::new("height", DataType::UInt64, false),
        Field::new("hash", bd.clone(), false),
        Field::new("prev_hash", bd.clone(), false),
        Field::new("prev_height", DataType::UInt64, false),
        Field::new("epoch_id", bd, false),
        Field::new("author", DataType::Utf8, false),
        Field::new("gas_price", DataType::Utf8, false),
        Field::new("total_supply", DataType::Utf8, false),
        Field::new("chunks_included", DataType::UInt64, false),
        Field::new("latest_protocol_version", DataType::UInt32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn chunks_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("shard_id", DataType::UInt64, false),
        Field::new("chunk_hash", bd.clone(), false),
        Field::new("prev_state_root", bd, false),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("gas_limit", DataType::UInt64, false),
        Field::new("height_created", DataType::UInt64, false),
        Field::new("height_included", DataType::UInt64, false),
        Field::new("encoded_length", DataType::UInt64, false),
        Field::new("author", DataType::Utf8, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("hash", bd, false),
        Field::new("signer_id", DataType::Utf8, false),
        Field::new("receiver_id", DataType::Utf8, false),
        Field::new("shard_id", DataType::UInt64, false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("actions", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("gas_burnt", DataType::UInt64, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn receipts_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("receipt_id", bd, false),
        Field::new("predecessor_id", DataType::Utf8, false),
        Field::new("receiver_id", DataType::Utf8, false),
        Field::new("shard_id", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("gas_burnt", DataType::UInt64, false),
        Field::new("executor_id", DataType::Utf8, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn state_changes_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("type", DataType::Utf8, false),
        Field::new("cause", DataType::Utf8, false),
        Field::new("account_id", DataType::Utf8, false),
        Field::new("key_base64", DataType::Utf8, false),
        Field::new("value_base64", DataType::Utf8, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub const TABLE_NAMES: [&str; 5] = [
    "blocks",
    "chunks",
    "transactions",
    "receipts",
    "state_changes",
];
