use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::traits::{canonical_fields, fork_step_field};
use std::sync::Arc;

fn maybe_fork_step(fields: &mut Vec<Field>, include: bool) {
    if include {
        fields.push(fork_step_field());
    }
}

pub fn blocks_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("parent_slot", DataType::UInt64, false),
        Field::new("block_height", DataType::UInt64, true),
        Field::new("blockhash", DataType::Utf8, false),
        Field::new("previous_blockhash", DataType::Utf8, false),
        Field::new("block_time", DataType::Int64, true),
        Field::new("num_transactions", DataType::UInt32, false),
        Field::new("num_rewards", DataType::UInt32, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn transactions_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("signature", DataType::Binary, false),
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
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn messages_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("message_index", DataType::UInt32, false),
        Field::new("num_required_signatures", DataType::UInt32, false),
        Field::new("num_readonly_signed_accounts", DataType::UInt32, false),
        Field::new("num_readonly_unsigned_accounts", DataType::UInt32, false),
        Field::new("recent_blockhash", DataType::Binary, false),
        Field::new("versioned", DataType::Boolean, false),
        Field::new(
            "account_keys",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            false,
        ),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn instructions_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("instruction_index", DataType::UInt32, false),
        Field::new("program_id_index", DataType::UInt32, false),
        Field::new("accounts", DataType::Binary, false),
        Field::new("data", DataType::Binary, false),
        Field::new("is_inner", DataType::Boolean, false),
        Field::new("inner_index", DataType::UInt32, true),
        Field::new("stack_height", DataType::UInt32, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn rewards_schema(include_fork_step: bool) -> Schema {
    let mut fields = canonical_fields();
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("reward_index", DataType::UInt32, false),
        Field::new("pubkey", DataType::Utf8, false),
        Field::new("lamports", DataType::Int64, false),
        Field::new("post_balance", DataType::UInt64, false),
        Field::new("reward_type", DataType::Int32, false),
        Field::new("commission", DataType::Utf8, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub const TABLE_NAMES: [&str; 5] = ["blocks", "transactions", "messages", "instructions", "rewards"];
