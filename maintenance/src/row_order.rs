//! The writer's row order within a block, per EVM table.
//!
//! `fireparq build` writes each block's rows in the order the Firehose block
//! holds them (`blocks/src/evm/mapper.rs`), and a block's rows in block order.
//! A compaction keeps that order by concatenating parts; only the repair of a
//! file compacted out of order (delta-rs's OPTIMIZE, fireparq-maintenance up
//! to 1.0.5) sorts, by `block_num` and then the table's key below. Each key is
//! strictly increasing in the writer's order, so sorting by it gives that
//! order back exactly: `blocks/tests/evm_golden.rs` checks every table of the
//! reviewed mainnet blocks, and the repair refuses a file in which two rows of
//! a block tie on it.
//!
//! This module has no dependencies, so the writer's tests include it by path.

/// One part of a table's in-block key, compared in ascending order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyPart {
    /// A column's value.
    Column(&'static str),
    /// Whether a column is set: rows without it come first. The `system_*`
    /// change tables write a block's own changes (no `call_index`) before
    /// those of its system calls.
    IsSet(&'static str),
}

use KeyPart::{Column, IsSet};

const TX_ORDER: &[KeyPart] = &[Column("index")];
const LOG_ORDER: &[KeyPart] = &[Column("block_index")];
const CALL_ORDER: &[KeyPart] = &[Column("tx_index"), Column("call_index")];
// A transaction's changes in call order, then in the order each call records
// them; a failed transaction's persistent changes are its root call's.
const CHANGE_ORDER: &[KeyPart] = &[Column("tx_index"), Column("call_index"), Column("ordinal")];
// A system call's `call_index` counts within its own call tree, so two system
// calls of a block share them: system calls follow `begin_ordinal`.
const SYSTEM_CALL_ORDER: &[KeyPart] = &[Column("begin_ordinal")];
const SYSTEM_CHANGE_ORDER: &[KeyPart] = &[IsSet("call_index"), Column("ordinal")];

/// The key that orders a block's rows of `table` as the writer wrote them,
/// after `block_num`; `None` for a table this job knows no order for (another
/// chain's), which is never repaired.
pub fn row_order_key(table: &str) -> Option<&'static [KeyPart]> {
    Some(match table {
        "blocks" => &[],
        "transactions" | "withdrawals" => TX_ORDER,
        "logs" => LOG_ORDER,
        "access_lists" => &[Column("tx_index"), Column("access_index")],
        "set_code_authorizations" => &[Column("tx_index"), Column("authorization_index")],
        "calls" => CALL_ORDER,
        "balance_changes" | "code_changes" | "storage_changes" | "nonce_changes"
        | "gas_changes" | "account_creations" => CHANGE_ORDER,
        "system_calls" => SYSTEM_CALL_ORDER,
        "system_balance_changes"
        | "system_code_changes"
        | "system_storage_changes"
        | "system_nonce_changes"
        | "system_gas_changes"
        | "system_account_creations" => SYSTEM_CHANGE_ORDER,
        _ => return None,
    })
}

/// The tables [`row_order_key`] knows, the EVM tables of `fireparq build`.
pub const EVM_TABLES: [&str; 20] = [
    "blocks",
    "transactions",
    "logs",
    "withdrawals",
    "access_lists",
    "set_code_authorizations",
    "calls",
    "balance_changes",
    "code_changes",
    "storage_changes",
    "nonce_changes",
    "gas_changes",
    "account_creations",
    "system_calls",
    "system_balance_changes",
    "system_code_changes",
    "system_storage_changes",
    "system_nonce_changes",
    "system_gas_changes",
    "system_account_creations",
];
