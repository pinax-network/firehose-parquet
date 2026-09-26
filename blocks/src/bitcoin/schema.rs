use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{canonical_fields_with_encoding, push_fork_step_field};
use std::sync::Arc;

pub fn blocks_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("hash", DataType::Utf8, false),
        Field::new("height", DataType::Int64, false),
        Field::new("previous_hash", DataType::Utf8, false),
        Field::new("merkle_root", DataType::Utf8, false),
        Field::new("time", DataType::Int64, false),
        Field::new("nonce", DataType::UInt32, false),
        Field::new("bits", DataType::Utf8, false),
        Field::new("difficulty", DataType::Float64, false),
        Field::new("size", DataType::Int32, false),
        Field::new("stripped_size", DataType::Int32, false),
        Field::new("weight", DataType::Int32, false),
        Field::new("version", DataType::Int32, false),
        Field::new("n_tx", DataType::UInt32, false),
        Field::new("mediantime", DataType::Int64, false),
        Field::new("chainwork", DataType::Utf8, false),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("txid", DataType::Utf8, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("size", DataType::Int32, false),
        Field::new("vsize", DataType::Int32, false),
        Field::new("weight", DataType::Int32, false),
        Field::new("version", DataType::UInt32, false),
        Field::new("locktime", DataType::UInt32, false),
        Field::new("block_hash", DataType::Utf8, false),
        Field::new("block_height", DataType::Int64, false),
        Field::new("block_time", DataType::Int64, false),
        Field::new("tx_index", DataType::UInt32, false),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn inputs_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("block_height", DataType::Int64, false),
        Field::new("input_index", DataType::UInt32, false),
        Field::new("prev_txid", DataType::Utf8, true),
        Field::new("prev_vout", DataType::UInt32, true),
        Field::new("sequence", DataType::UInt32, false),
        Field::new("script_sig_asm", DataType::Utf8, true),
        Field::new("script_sig_hex", DataType::Utf8, true),
        Field::new("coinbase", DataType::Utf8, true),
        Field::new(
            "witness",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("tx_index", DataType::UInt32, false),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn outputs_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("block_height", DataType::Int64, false),
        Field::new("output_index", DataType::UInt32, false),
        Field::new("value", DataType::Float64, false),
        Field::new("script_pubkey_asm", DataType::Utf8, true),
        Field::new("script_pubkey_hex", DataType::Utf8, true),
        Field::new("script_pubkey_type", DataType::Utf8, true),
        Field::new("script_pubkey_address", DataType::Utf8, true),
        Field::new("value_sats", DataType::UInt64, false),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub const TABLE_NAMES: [&str; 4] = ["blocks", "transactions", "inputs", "outputs"];
