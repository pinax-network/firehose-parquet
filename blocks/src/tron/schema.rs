use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::encode::{bytes_data_type, EncodeBytes};
use firehose_parquet::traits::{canonical_fields_with_encoding, fork_step_field};

fn maybe_fork_step(fields: &mut Vec<Field>, include: bool) {
    if include {
        fields.push(fork_step_field());
    }
}

fn tron_reserved_encoding(encoding: &EncodeBytes) -> EncodeBytes {
    match encoding {
        EncodeBytes::TronBase58 => EncodeBytes::HexNoPrefix,
        other => other.clone(),
    }
}

fn enum_data_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

pub fn blocks_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let reserved_bd = bytes_data_type(&tron_reserved_encoding(encoding));
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", reserved_bd.clone(), false),
        Field::new("parent_hash", reserved_bd.clone(), false),
        Field::new("witness_address", bd.clone(), false),
        Field::new("version", DataType::UInt32, false),
        Field::new("tx_trie_root", reserved_bd, false),
        Field::new("parent_number", DataType::UInt64, false),
        Field::new("num_transactions", DataType::UInt32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let reserved_bd = bytes_data_type(&tron_reserved_encoding(encoding));
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("txid", reserved_bd, false),
        Field::new("result", DataType::Boolean, false),
        Field::new("code", enum_data_type(), false),
        Field::new("energy_used", DataType::Int64, false),
        Field::new("energy_penalty", DataType::Int64, false),
        Field::new("fee", DataType::Int64, false),
        Field::new("contract_type", enum_data_type(), true),
        // Transaction expiration and creation times in unix milliseconds, named
        // apart from the canonical block `timestamp` column. The creation time is
        // set by the sender and not validated on chain (0 and other units occur),
        // so both stay raw Int64 rather than a Timestamp type.
        Field::new("expiration_ms", DataType::Int64, false),
        Field::new("tx_timestamp_ms", DataType::Int64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("receipt_energy_usage", DataType::Int64, true),
        Field::new("receipt_energy_fee", DataType::Int64, true),
        Field::new("receipt_origin_energy_usage", DataType::Int64, true),
        Field::new("receipt_energy_usage_total", DataType::Int64, true),
        Field::new("receipt_net_usage", DataType::Int64, true),
        Field::new("receipt_net_fee", DataType::Int64, true),
        Field::new("receipt_result", enum_data_type(), true),
        Field::new("receipt_energy_penalty_total", DataType::Int64, true),
        Field::new("contract_address", bytes_data_type(encoding), true),
        Field::new("res_message", DataType::Binary, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn logs_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let reserved_bd = bytes_data_type(&tron_reserved_encoding(encoding));
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", reserved_bd.clone(), false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("address", bd.clone(), false),
        Field::new("topic0", reserved_bd.clone(), true),
        Field::new("topic1", reserved_bd.clone(), true),
        Field::new("topic2", reserved_bd.clone(), true),
        Field::new("topic3", reserved_bd.clone(), true),
        Field::new("data", bd, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("block_log_index", DataType::UInt64, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn internal_transactions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let reserved_bd = bytes_data_type(&tron_reserved_encoding(encoding));
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", reserved_bd.clone(), false),
        Field::new("internal_index", DataType::UInt32, false),
        Field::new("hash", reserved_bd.clone(), false),
        Field::new("caller_address", bd.clone(), false),
        Field::new("transfer_to_address", bd, false),
        Field::new("note", DataType::Utf8, false),
        Field::new("rejected", DataType::Boolean, false),
        Field::new("transaction_index", DataType::UInt32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub const TABLE_NAMES: [&str; 6] = [
    "blocks",
    "transactions",
    "logs",
    "internal_transactions",
    "contracts",
    "internal_call_values",
];

pub fn contracts_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new(
            "tx_hash",
            bytes_data_type(&tron_reserved_encoding(encoding)),
            false,
        ),
        Field::new("contract_index", DataType::UInt32, false),
        Field::new("contract_type", enum_data_type(), false),
        Field::new("contract_type_id", DataType::Int32, false),
        Field::new("parameter_type_url", DataType::Utf8, true),
        Field::new("parameter", DataType::Binary, true),
        Field::new("permission_id", DataType::Int32, false),
        Field::new("owner_address", bytes_data_type(encoding), true),
        Field::new("to_address", bytes_data_type(encoding), true),
        Field::new("amount", DataType::Int64, true),
        Field::new("asset_name", DataType::Binary, true),
        Field::new("contract_address", bytes_data_type(encoding), true),
        Field::new("data", DataType::Binary, true),
        Field::new("call_value", DataType::Int64, true),
        Field::new("call_token_value", DataType::Int64, true),
        Field::new("token_id", DataType::Int64, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn internal_call_values_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new(
            "tx_hash",
            bytes_data_type(&tron_reserved_encoding(encoding)),
            false,
        ),
        Field::new("internal_index", DataType::UInt32, false),
        Field::new("call_value_index", DataType::UInt32, false),
        Field::new("call_value", DataType::Int64, false),
        Field::new("token_id", DataType::Utf8, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}
