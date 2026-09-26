use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::encode::{bytes_data_type, BytesListColumn, EncodeBytes};
use firehose_parquet::traits::{
    canonical_fields_with_nullable_timestamps, enum_data_type, push_fork_step_field,
};
use std::sync::Arc;

fn solana_canonical_fields(encoding: &EncodeBytes) -> Vec<Field> {
    canonical_fields_with_nullable_timestamps(encoding)
}

pub(super) fn index_element_field() -> Field {
    Field::new("item", DataType::UInt8, false)
}

fn index_list_type() -> DataType {
    DataType::List(Arc::new(index_element_field()))
}

pub fn blocks_schema(
    include_fork_step: bool,
    encoding: &EncodeBytes,
    _synthetic_partition_routing: bool,
) -> Schema {
    let mut fields = solana_canonical_fields(encoding);
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("parent_slot", DataType::UInt64, false),
        Field::new("block_height", DataType::UInt64, true),
        Field::new("blockhash", bytes_data_type(encoding), false),
        Field::new("previous_blockhash", bytes_data_type(encoding), false),
        Field::new("block_time", DataType::Int64, true),
        Field::new("num_transactions", DataType::UInt32, false),
        Field::new("num_rewards", DataType::UInt32, false),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(
    include_fork_step: bool,
    encoding: &EncodeBytes,
    _synthetic_partition_routing: bool,
) -> Schema {
    let mut fields = solana_canonical_fields(encoding);
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("signature", bytes_data_type(encoding), false),
        Field::new("num_signatures", DataType::UInt32, false),
        Field::new("fee", DataType::UInt64, false),
        Field::new("err", DataType::Binary, true),
        Field::new("success", DataType::Boolean, false),
        Field::new("compute_units_consumed", DataType::UInt64, true),
        Field::new(
            "log_messages",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "pre_balances",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        ),
        Field::new(
            "post_balances",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        ),
        Field::new("cost_units", DataType::UInt64, true),
        Field::new("return_data_program_id", bytes_data_type(encoding), true),
        Field::new("return_data", DataType::Binary, true),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn messages_schema(
    include_fork_step: bool,
    encoding: &EncodeBytes,
    _synthetic_partition_routing: bool,
) -> Schema {
    let mut fields = solana_canonical_fields(encoding);
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("message_index", DataType::UInt32, false),
        Field::new("num_required_signatures", DataType::UInt32, false),
        Field::new("num_readonly_signed_accounts", DataType::UInt32, false),
        Field::new("num_readonly_unsigned_accounts", DataType::UInt32, false),
        Field::new("recent_blockhash", bytes_data_type(encoding), false),
        Field::new("versioned", DataType::Boolean, false),
        Field::new("account_keys", BytesListColumn::data_type(encoding), false),
        Field::new(
            "loaded_writable_addresses",
            BytesListColumn::data_type(encoding),
            true,
        ),
        Field::new(
            "loaded_readonly_addresses",
            BytesListColumn::data_type(encoding),
            true,
        ),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    fields.push(Field::new("transaction_success", DataType::Boolean, false));
    Schema::new(fields)
}

pub fn instructions_schema(
    include_fork_step: bool,
    encoding: &EncodeBytes,
    _synthetic_partition_routing: bool,
) -> Schema {
    let mut fields = solana_canonical_fields(encoding);
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("instruction_index", DataType::UInt32, false),
        Field::new("program_id_index", DataType::UInt32, false),
        Field::new("accounts", index_list_type(), false),
        Field::new("data", DataType::Binary, false),
        Field::new("is_inner", DataType::Boolean, false),
        Field::new("inner_index", DataType::UInt32, true),
        Field::new("stack_height", DataType::UInt32, true),
        Field::new("parent_instruction_index", DataType::UInt32, true),
        Field::new("inner_instruction_index", DataType::UInt32, true),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    fields.push(Field::new("transaction_success", DataType::Boolean, false));
    Schema::new(fields)
}

pub fn rewards_schema(
    include_fork_step: bool,
    encoding: &EncodeBytes,
    _synthetic_partition_routing: bool,
) -> Schema {
    let mut fields = solana_canonical_fields(encoding);
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("reward_index", DataType::UInt32, false),
        Field::new("pubkey", DataType::Utf8, false),
        Field::new("lamports", DataType::Int64, false),
        Field::new("post_balance", DataType::UInt64, false),
        Field::new("reward_type", enum_data_type(), false),
        Field::new("commission", DataType::Utf8, true),
        // "block" for block-level rewards, "transaction" for per-tx rewards
        Field::new("source", DataType::Utf8, false),
        Field::new("transaction_index", DataType::UInt32, true),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    fields.push(Field::new("transaction_success", DataType::Boolean, true));
    Schema::new(fields)
}

/// Token balance snapshots (pre/post) per transaction.
pub fn token_balances_schema(
    include_fork_step: bool,
    encoding: &EncodeBytes,
    _synthetic_partition_routing: bool,
) -> Schema {
    let mut fields = solana_canonical_fields(encoding);
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("balance_index", DataType::UInt32, false),
        // "pre" or "post"
        Field::new("balance_type", DataType::Utf8, false),
        Field::new("account_index", DataType::UInt32, false),
        Field::new("mint", DataType::Utf8, false),
        Field::new("owner", DataType::Utf8, false),
        Field::new("program_id", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
        Field::new("ui_amount", DataType::Float64, true),
        Field::new("decimals", DataType::UInt32, false),
        Field::new("ui_amount_string", DataType::Utf8, false),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    fields.push(Field::new("transaction_success", DataType::Boolean, false));
    Schema::new(fields)
}

/// Address table lookups from versioned transactions.
pub fn account_lookups_schema(
    include_fork_step: bool,
    encoding: &EncodeBytes,
    _synthetic_partition_routing: bool,
) -> Schema {
    let mut fields = solana_canonical_fields(encoding);
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("lookup_index", DataType::UInt32, false),
        Field::new("account_key", bytes_data_type(encoding), false),
        Field::new("writable_indexes", index_list_type(), false),
        Field::new("readonly_indexes", index_list_type(), false),
    ]);
    push_fork_step_field(&mut fields, include_fork_step);
    fields.push(Field::new("transaction_success", DataType::Boolean, false));
    Schema::new(fields)
}

/// Standard table names (available at BASE detail level).
pub const BASE_TABLE_NAMES: [&str; 7] = [
    "blocks",
    "transactions",
    "messages",
    "instructions",
    "rewards",
    "token_balances",
    "account_lookups",
];

/// Table names when Solana vote transactions are enabled.
pub const WITH_VOTES_TABLE_NAMES: [&str; 8] = [
    "blocks",
    "transactions",
    "vote_transactions",
    "messages",
    "instructions",
    "rewards",
    "token_balances",
    "account_lookups",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_canonical_time_field_nullability(schema: &Schema, nullable: bool) {
        let timestamp = schema
            .field_with_name("timestamp")
            .expect("timestamp field should be present");
        let date = schema
            .field_with_name("date")
            .expect("date field should be present");

        assert_eq!(
            timestamp.is_nullable(),
            nullable,
            "unexpected timestamp nullability in Solana schema"
        );
        assert_eq!(
            date.is_nullable(),
            nullable,
            "unexpected date nullability in Solana schema"
        );
    }

    #[test]
    fn test_all_solana_schemas_use_nullable_canonical_time_fields() {
        let schema_builders: [fn(bool, &EncodeBytes, bool) -> Schema; 7] = [
            blocks_schema,
            transactions_schema,
            messages_schema,
            instructions_schema,
            rewards_schema,
            token_balances_schema,
            account_lookups_schema,
        ];

        for include_fork_step in [false, true] {
            for build_schema in schema_builders {
                let schema = build_schema(include_fork_step, &EncodeBytes::Binary, false);
                assert_canonical_time_field_nullability(&schema, true);
            }
        }
    }

    #[test]
    fn test_all_solana_schemas_keep_nullable_canonical_time_fields_when_backfill_enabled() {
        let schema_builders: [fn(bool, &EncodeBytes, bool) -> Schema; 7] = [
            blocks_schema,
            transactions_schema,
            messages_schema,
            instructions_schema,
            rewards_schema,
            token_balances_schema,
            account_lookups_schema,
        ];

        for include_fork_step in [false, true] {
            for build_schema in schema_builders {
                let schema = build_schema(include_fork_step, &EncodeBytes::Binary, true);
                assert_canonical_time_field_nullability(&schema, true);
            }
        }
    }
}
