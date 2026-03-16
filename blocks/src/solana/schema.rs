use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::encode::{bytes_data_type, BytesListColumn, EncodeBytes};
use firehose_parquet::traits::{
    canonical_fields_with_nullable_timestamps, fork_step_field,
};
use std::sync::Arc;

fn maybe_fork_step(fields: &mut Vec<Field>, include: bool) {
    if include {
        fields.push(fork_step_field());
    }
}

fn solana_canonical_fields(encoding: &EncodeBytes) -> Vec<Field> {
    canonical_fields_with_nullable_timestamps(encoding)
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
    maybe_fork_step(&mut fields, include_fork_step);
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
        Field::new("err", bytes_data_type(encoding), true),
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
        Field::new("return_data", bytes_data_type(encoding), true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
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
    maybe_fork_step(&mut fields, include_fork_step);
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
        Field::new("accounts", bytes_data_type(encoding), false),
        Field::new("data", bytes_data_type(encoding), false),
        Field::new("is_inner", DataType::Boolean, false),
        Field::new("inner_index", DataType::UInt32, true),
        Field::new("stack_height", DataType::UInt32, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
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
        Field::new("reward_type", DataType::Int32, false),
        Field::new("commission", DataType::Utf8, true),
        // "block" for block-level rewards, "transaction" for per-tx rewards
        Field::new("source", DataType::Utf8, false),
        Field::new("transaction_index", DataType::UInt32, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// Token balance changes (pre/post) per transaction.
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
    maybe_fork_step(&mut fields, include_fork_step);
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
        Field::new("writable_indexes", bytes_data_type(encoding), false),
        Field::new("readonly_indexes", bytes_data_type(encoding), false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
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

/// Extended table names (available at EXTENDED detail level).
pub const EXTENDED_TABLE_NAMES: [&str; 8] = [
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
