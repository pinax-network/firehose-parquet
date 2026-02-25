use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Arrow schema for the **blocks** table (one row per block).
pub fn blocks_schema() -> Schema {
    Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("parent_slot", DataType::UInt64, false),
        Field::new("block_height", DataType::UInt64, true),
        Field::new("blockhash", DataType::Utf8, false),
        Field::new("previous_blockhash", DataType::Utf8, false),
        Field::new("block_time", DataType::Int64, true),
        Field::new("num_transactions", DataType::UInt32, false),
        Field::new("num_rewards", DataType::UInt32, false),
    ])
}

/// Arrow schema for the **transactions** table (one row per confirmed transaction).
pub fn transactions_schema() -> Schema {
    Schema::new(vec![
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
    ])
}

/// Arrow schema for the **messages** table (one row per transaction message).
pub fn messages_schema() -> Schema {
    Schema::new(vec![
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
    ])
}

/// Arrow schema for the **instructions** table (one row per instruction, top-level or inner).
pub fn instructions_schema() -> Schema {
    Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("instruction_index", DataType::UInt32, false),
        Field::new("program_id_index", DataType::UInt32, false),
        Field::new("accounts", DataType::Binary, false),
        Field::new("data", DataType::Binary, false),
        Field::new("is_inner", DataType::Boolean, false),
        Field::new("inner_index", DataType::UInt32, true),
        Field::new("stack_height", DataType::UInt32, true),
    ])
}

/// Arrow schema for the **rewards** table (one row per reward entry).
pub fn rewards_schema() -> Schema {
    Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("reward_index", DataType::UInt32, false),
        Field::new("pubkey", DataType::Utf8, false),
        Field::new("lamports", DataType::Int64, false),
        Field::new("post_balance", DataType::UInt64, false),
        Field::new("reward_type", DataType::Int32, false),
        Field::new("commission", DataType::Utf8, true),
    ])
}

/// All table names.
pub const TABLE_NAMES: [&str; 5] = ["blocks", "transactions", "messages", "instructions", "rewards"];
