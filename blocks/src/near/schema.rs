use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::encode::{bytes_data_type, BytesListColumn, EncodeBytes};
use firehose_parquet::traits::{
    canonical_fields_with_encoding, enum_data_type, push_fork_step_field,
};

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
    push_fork_step_field(&mut fields, include_fork_step);
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
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("hash", bd.clone(), false),
        // Position in the block: chunks in shard order, then each chunk's
        // transactions in order. Transactions dropped by the failed-transaction
        // filter keep their index, so the written values can have gaps.
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("signer_id", DataType::Utf8, false),
        Field::new("receiver_id", DataType::Utf8, false),
        Field::new("shard_id", DataType::UInt64, false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("actions", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("gas_burnt", DataType::UInt64, false),
        // yoctoNEAR, as a decimal string.
        Field::new("tokens_burnt", DataType::Utf8, false),
        // The outcome's `receipt_ids`.
        Field::new("receipt_ids", BytesListColumn::data_type(encoding), false),
        // The receipt the transaction was converted into: the first (and only)
        // entry of `receipt_ids`. Joins `receipts.receipt_id`.
        Field::new("converted_into_receipt_id", bd, true),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn receipts_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("receipt_id", bd.clone(), false),
        // Position of the execution outcome in the block: shards in order, then
        // each shard's executed receipts in order.
        Field::new("receipt_index", DataType::UInt32, false),
        // The originating transaction, when it is in the same block; null
        // otherwise (see `mapper::ReceiptOrigins`).
        Field::new("tx_hash", bd, true),
        Field::new("predecessor_id", DataType::Utf8, false),
        Field::new("receiver_id", DataType::Utf8, false),
        // `ReceiptAction.signer_id`: the signer of the transaction that started
        // the receipt chain. Null for data receipts.
        Field::new("signer_id", DataType::Utf8, true),
        Field::new("shard_id", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("gas_burnt", DataType::UInt64, false),
        // yoctoNEAR, as a decimal string.
        Field::new("tokens_burnt", DataType::Utf8, false),
        Field::new("executor_id", DataType::Utf8, false),
        // Receipts created by this execution (the outcome's `receipt_ids`).
        Field::new("receipt_ids", BytesListColumn::data_type(encoding), false),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// One row per action of an executed action receipt.
pub fn receipt_actions_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("receipt_id", bd.clone(), false),
        Field::new("receipt_index", DataType::UInt32, false),
        // Position of the action in the receipt's action list.
        Field::new("action_index", DataType::UInt32, false),
        Field::new("tx_hash", bd, true),
        Field::new("shard_id", DataType::UInt64, false),
        Field::new("predecessor_id", DataType::Utf8, false),
        Field::new("receiver_id", DataType::Utf8, false),
        Field::new("signer_id", DataType::Utf8, false),
        Field::new("action_kind", enum_data_type(), false),
        // FunctionCall only.
        Field::new("method_name", DataType::Utf8, true),
        // FunctionCall only. Raw bytes (usually JSON) whatever the encoding.
        Field::new("args", DataType::Binary, true),
        // FunctionCall only: the gas attached to the call.
        Field::new("gas", DataType::UInt64, true),
        // FunctionCall and Transfer only: yoctoNEAR, as a decimal string.
        Field::new("deposit", DataType::Utf8, true),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// One row per log line of an executed receipt (`ExecutionOutcome.logs`). NEP-297
/// events such as NEP-141 and NEP-171 are logs starting with `EVENT_JSON:`.
pub fn execution_logs_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("receipt_id", bd.clone(), false),
        Field::new("receipt_index", DataType::UInt32, false),
        // Position of the log in the outcome's logs.
        Field::new("log_index", DataType::UInt32, false),
        Field::new("tx_hash", bd, true),
        Field::new("shard_id", DataType::UInt64, false),
        // The account whose code emitted the log.
        Field::new("executor_id", DataType::Utf8, false),
        Field::new("predecessor_id", DataType::Utf8, false),
        Field::new("log", DataType::Utf8, false),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
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
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub const TABLE_NAMES: [&str; 7] = [
    "blocks",
    "chunks",
    "transactions",
    "receipts",
    "receipt_actions",
    "execution_logs",
    "state_changes",
];
