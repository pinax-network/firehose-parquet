use super::decimal;
use super::proto::eth;
use super::schema;
use arrow::array::*;
use arrow::datatypes::{Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, BytesListColumn, EncodeBytes};
use firehose_parquet::traits::{
    est_bool, est_opt_str, est_str, est_u32, est_u64, BlockIdentity, BlockMapper, CanonicalBuilder,
    PreparedIdentity,
};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Bytes of a Firehose `BigInt`; empty (zero) when absent.
fn bigint_bytes(bi: &Option<eth::BigInt>) -> &[u8] {
    bi.as_ref().map_or(&[][..], |b| &b.bytes)
}

/// Decimal string of `bi`; `"0"` when absent or empty.
#[cfg(test)]
fn bigint_to_string(bi: &Option<eth::BigInt>) -> String {
    decimal::to_decimal(bigint_bytes(bi))
}

/// Append the decimal string of `bi` (`"0"` when absent or empty).
fn append_bigint(builder: &mut StringBuilder, bi: &Option<eth::BigInt>) {
    decimal::append_decimal(builder, bigint_bytes(bi));
}

/// Append `bytes`, or null when empty. For header fields that are absent
/// before the fork that introduced them (e.g. `withdrawals_root`).
fn append_non_empty_bytes(builder: &mut BytesColumn, bytes: &[u8]) {
    if bytes.is_empty() {
        builder.append_null();
    } else {
        builder.append_value(bytes);
    }
}

/// Append a decimal string for `value`, or null when absent.
fn append_optional_bigint(builder: &mut StringBuilder, value: &Option<eth::BigInt>) {
    if value.is_some() {
        append_bigint(builder, value);
    } else {
        builder.append_null();
    }
}

fn append_fork_step(builder: &mut Option<StringBuilder>, fork_step: Option<&str>) {
    if let Some(ref mut b) = builder {
        b.append_value(fork_step.unwrap_or("UNKNOWN"));
    }
}

fn remove_enum_prefix_if_present(name: &'static str, prefix: &str) -> &'static str {
    name.strip_prefix(prefix).unwrap_or(name)
}

fn estimated_dictionary_index_bytes(len: usize) -> usize {
    // Largest-table tracking only needs a cheap relative estimate. For enum-backed
    // dictionary columns, the shared string dictionary cardinality is fixed and
    // small, so counting the per-row indices is sufficient for that comparison.
    len * std::mem::size_of::<i32>()
}

fn detail_level_text(value: i32) -> &'static str {
    eth::block::DetailLevel::try_from(value)
        .map(|detail_level| {
            remove_enum_prefix_if_present(detail_level.as_str_name(), "DETAILLEVEL_")
        })
        .unwrap_or("UNKNOWN")
}

fn transaction_type_text(value: i32) -> &'static str {
    eth::transaction_trace::Type::try_from(value)
        .map(|tx_type| remove_enum_prefix_if_present(tx_type.as_str_name(), "TRX_TYPE_"))
        .unwrap_or("UNKNOWN")
}

fn transaction_status_text(value: i32) -> &'static str {
    eth::TransactionTraceStatus::try_from(value)
        .map(|status| status.as_str_name())
        .unwrap_or("UNKNOWN")
}

fn transaction_succeeded(tx: &eth::TransactionTrace) -> bool {
    tx.status == eth::TransactionTraceStatus::Succeeded as i32
}

/// State changes of a failed or reverted transaction that persist on chain.
#[derive(Debug, Default)]
struct PersistentChanges<'a> {
    balance_changes: Vec<&'a eth::BalanceChange>,
    nonce_changes: Vec<&'a eth::NonceChange>,
    code_changes: Vec<&'a eth::CodeChange>,
}

/// Select the state changes of a failed or reverted transaction that the chain
/// keeps. Everything else it recorded was rolled back.
///
/// Follows the rule documented on `TransactionTrace.status` in
/// `proto/ethereum.proto`. On the root call (`calls[0]`):
///
/// - balance changes for buying gas, refunding gas and paying the fee
///   recipients (`GAS_BUY`, `GAS_REFUND`, `REWARD_TRANSACTION_FEE`), plus
///   `INCREASE_MINT`, because OP Stack deposits keep their mint when they fail;
/// - the sender's nonce increment, which is the earliest nonce change;
/// - for EIP-7702 `SET_CODE` transactions, one nonce change and at most one
///   code change per accepted (not discarded) authorization: the earliest
///   remaining ones for its authority. An authority can appear in several
///   authorizations, and each accepted one increments its nonce.
///
/// Ordinals of reverted calls may be zero, so "earliest" falls back to the
/// recording order.
fn failed_transaction_persistent_changes(tx: &eth::TransactionTrace) -> PersistentChanges<'_> {
    use eth::balance_change::Reason;

    let Some(root) = tx.calls.first() else {
        return PersistentChanges::default();
    };

    let balance_changes = root
        .balance_changes
        .iter()
        .filter(|bc| {
            matches!(
                Reason::try_from(bc.reason),
                Ok(Reason::GasBuy
                    | Reason::GasRefund
                    | Reason::RewardTransactionFee
                    | Reason::IncreaseMint)
            )
        })
        .collect();

    // Root-call indices in execution order (ordinal, then recording order).
    let nonce_order = execution_order(&root.nonce_changes, |nc| nc.ordinal);
    let code_order = execution_order(&root.code_changes, |cc| cc.ordinal);

    let mut kept_nonces: Vec<usize> = nonce_order.first().copied().into_iter().collect();
    let mut kept_codes: Vec<usize> = Vec::new();
    for authority in tx
        .set_code_authorizations
        .iter()
        .filter(|auth| !auth.discarded)
        .filter_map(|auth| auth.authority.as_deref())
        .filter(|authority| !authority.is_empty())
    {
        if let Some(&i) = nonce_order
            .iter()
            .find(|&&i| root.nonce_changes[i].address == authority && !kept_nonces.contains(&i))
        {
            kept_nonces.push(i);
        }
        if let Some(&i) = code_order
            .iter()
            .find(|&&i| root.code_changes[i].address == authority && !kept_codes.contains(&i))
        {
            kept_codes.push(i);
        }
    }
    // Emit in recording order, like the other change rows.
    kept_nonces.sort_unstable();
    kept_codes.sort_unstable();

    PersistentChanges {
        balance_changes,
        nonce_changes: kept_nonces
            .into_iter()
            .map(|i| &root.nonce_changes[i])
            .collect(),
        code_changes: kept_codes
            .into_iter()
            .map(|i| &root.code_changes[i])
            .collect(),
    }
}

/// The transaction and call that recorded a transaction-scoped change.
#[derive(Debug, Clone, Copy)]
struct ChangeContext<'a> {
    tx_hash: &'a [u8],
    tx_index: u32,
    call_index: u32,
    /// `state_reverted` of the call that recorded the change, as in `calls`.
    state_reverted: bool,
    /// Whether the change is part of chain state after the transaction.
    persisted: bool,
}

impl<'a> ChangeContext<'a> {
    /// A change recorded by `call`. In a successful transaction a change
    /// persists unless its call was reverted. A failed transaction only writes
    /// its persistent changes ([`failed_transaction_persistent_changes`]), so
    /// its rows are persisted even though the root call has `state_reverted`.
    fn new(tx: &'a eth::TransactionTrace, call: &eth::Call) -> Self {
        Self {
            tx_hash: &tx.hash,
            tx_index: tx.index,
            call_index: call.index,
            state_reverted: call.state_reverted,
            persisted: !transaction_succeeded(tx) || !call.state_reverted,
        }
    }
}

/// Indices of `items` sorted by ordinal; ties keep the recording order.
fn execution_order<T>(items: &[T], ordinal: impl Fn(&T) -> u64) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..items.len()).collect();
    indices.sort_by_key(|&i| ordinal(&items[i]));
    indices
}

fn call_type_text(value: i32) -> &'static str {
    eth::CallType::try_from(value)
        .map(|call_type| call_type.as_str_name())
        .unwrap_or("UNKNOWN")
}

fn balance_change_reason_text(value: i32) -> &'static str {
    eth::balance_change::Reason::try_from(value)
        .map(|reason| remove_enum_prefix_if_present(reason.as_str_name(), "REASON_"))
        .unwrap_or("UNKNOWN")
}

fn gas_change_reason_text(value: i32) -> &'static str {
    eth::gas_change::Reason::try_from(value)
        .map(|reason| remove_enum_prefix_if_present(reason.as_str_name(), "REASON_"))
        .unwrap_or("UNKNOWN")
}

fn finish_fork_step(
    builder: &mut Option<StringBuilder>,
    columns: &mut Vec<Arc<dyn arrow::array::Array>>,
) {
    if let Some(ref mut b) = builder {
        columns.push(Arc::new(b.finish()) as Arc<dyn arrow::array::Array>);
    }
}

fn mk_fork_step(include: bool) -> Option<StringBuilder> {
    if include {
        Some(StringBuilder::new())
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// EVM BlockMapper
// ---------------------------------------------------------------------------

pub struct EvmBlockMapper {
    extended: bool,
    include_failed_transactions: bool,
    // Standard builders
    blocks: EvmBlocksBuilder,
    transactions: EvmTransactionsBuilder,
    logs: EvmLogsBuilder,
    withdrawals: EvmWithdrawalsBuilder,
    access_lists: EvmAccessListsBuilder,
    set_code_authorizations: EvmSetCodeAuthorizationsBuilder,
    // Extended builders (transaction-scoped)
    calls: Option<EvmCallsBuilder>,
    balance_changes: Option<EvmBalanceChangesBuilder>,
    code_changes: Option<EvmCodeChangesBuilder>,
    storage_changes: Option<EvmStorageChangesBuilder>,
    nonce_changes: Option<EvmNonceChangesBuilder>,
    gas_changes: Option<EvmGasChangesBuilder>,
    account_creations: Option<EvmAccountCreationsBuilder>,
    // System builders (block-level, no tx_hash)
    system_calls: Option<SystemCallsBuilder>,
    system_balance_changes: Option<SystemBalanceChangesBuilder>,
    system_code_changes: Option<SystemCodeChangesBuilder>,
    system_storage_changes: Option<SystemStorageChangesBuilder>,
    system_nonce_changes: Option<SystemNonceChangesBuilder>,
    system_gas_changes: Option<SystemGasChangesBuilder>,
    system_account_creations: Option<SystemAccountCreationsBuilder>,
    // Schemas
    blocks_schema: Schema,
    transactions_schema: Schema,
    logs_schema: Schema,
    withdrawals_schema: Schema,
    access_lists_schema: Schema,
    set_code_authorizations_schema: Schema,
    calls_schema: Schema,
    balance_changes_schema: Schema,
    code_changes_schema: Schema,
    storage_changes_schema: Schema,
    nonce_changes_schema: Schema,
    gas_changes_schema: Schema,
    account_creations_schema: Schema,
    system_calls_schema: Schema,
    system_balance_changes_schema: Schema,
    system_code_changes_schema: Schema,
    system_storage_changes_schema: Schema,
    system_nonce_changes_schema: Schema,
    system_gas_changes_schema: Schema,
    system_account_creations_schema: Schema,
}

impl EvmBlockMapper {
    pub fn new(
        extended: bool,
        include_fork_step: bool,
        encoding: EncodeBytes,
        include_failed_transactions: bool,
    ) -> Self {
        let ifs = include_fork_step;
        let enc = &encoding;
        Self {
            include_failed_transactions,
            extended,
            blocks: EvmBlocksBuilder::new(ifs, enc),
            transactions: EvmTransactionsBuilder::new(ifs, enc),
            logs: EvmLogsBuilder::new(ifs, enc),
            withdrawals: EvmWithdrawalsBuilder::new(ifs, enc),
            access_lists: EvmAccessListsBuilder::new(ifs, enc),
            set_code_authorizations: EvmSetCodeAuthorizationsBuilder::new(ifs, enc),
            calls: if extended {
                Some(EvmCallsBuilder::new(ifs, enc))
            } else {
                None
            },
            balance_changes: if extended {
                Some(EvmBalanceChangesBuilder::new(ifs, enc))
            } else {
                None
            },
            code_changes: if extended {
                Some(EvmCodeChangesBuilder::new(ifs, enc))
            } else {
                None
            },
            storage_changes: if extended {
                Some(EvmStorageChangesBuilder::new(ifs, enc))
            } else {
                None
            },
            nonce_changes: if extended {
                Some(EvmNonceChangesBuilder::new(ifs, enc))
            } else {
                None
            },
            gas_changes: if extended {
                Some(EvmGasChangesBuilder::new(ifs, enc))
            } else {
                None
            },
            account_creations: if extended {
                Some(EvmAccountCreationsBuilder::new(ifs, enc))
            } else {
                None
            },
            system_calls: if extended {
                Some(SystemCallsBuilder::new(ifs, enc))
            } else {
                None
            },
            system_balance_changes: if extended {
                Some(SystemBalanceChangesBuilder::new(ifs, enc))
            } else {
                None
            },
            system_code_changes: if extended {
                Some(SystemCodeChangesBuilder::new(ifs, enc))
            } else {
                None
            },
            system_storage_changes: if extended {
                Some(SystemStorageChangesBuilder::new(ifs, enc))
            } else {
                None
            },
            system_nonce_changes: if extended {
                Some(SystemNonceChangesBuilder::new(ifs, enc))
            } else {
                None
            },
            system_gas_changes: if extended {
                Some(SystemGasChangesBuilder::new(ifs, enc))
            } else {
                None
            },
            system_account_creations: if extended {
                Some(SystemAccountCreationsBuilder::new(ifs, enc))
            } else {
                None
            },
            blocks_schema: schema::blocks_schema(ifs, enc),
            transactions_schema: schema::transactions_schema(ifs, enc),
            logs_schema: schema::logs_schema(ifs, enc),
            withdrawals_schema: schema::withdrawals_schema(ifs, enc),
            access_lists_schema: schema::access_lists_schema(ifs, enc),
            set_code_authorizations_schema: schema::set_code_authorizations_schema(ifs, enc),
            calls_schema: schema::calls_schema(ifs, enc),
            balance_changes_schema: schema::balance_changes_schema(ifs, enc),
            code_changes_schema: schema::code_changes_schema(ifs, enc),
            storage_changes_schema: schema::storage_changes_schema(ifs, enc),
            nonce_changes_schema: schema::nonce_changes_schema(ifs, enc),
            gas_changes_schema: schema::gas_changes_schema(ifs, enc),
            account_creations_schema: schema::account_creations_schema(ifs, enc),
            system_calls_schema: schema::system_calls_schema(ifs, enc),
            system_balance_changes_schema: schema::system_balance_changes_schema(ifs, enc),
            system_code_changes_schema: schema::system_code_changes_schema(ifs, enc),
            system_storage_changes_schema: schema::system_storage_changes_schema(ifs, enc),
            system_nonce_changes_schema: schema::system_nonce_changes_schema(ifs, enc),
            system_gas_changes_schema: schema::system_gas_changes_schema(ifs, &encoding),
            system_account_creations_schema: schema::system_account_creations_schema(ifs, enc),
        }
    }

    fn map_evm_block(
        &mut self,
        block: &eth::Block,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let number = block.number;
        let header = block.header.as_ref();

        // -- blocks table --
        self.blocks.canonical.append(identity);
        self.blocks.number.append_value(number);
        self.blocks.hash.append_value(&block.hash);
        self.blocks
            .parent_hash
            .append_value(header.map_or(&[][..], |h| &h.parent_hash));

        self.blocks
            .gas_used
            .append_value(header.map_or(0, |h| h.gas_used));
        self.blocks
            .gas_limit
            .append_value(header.map_or(0, |h| h.gas_limit));
        append_optional_bigint(
            &mut self.blocks.base_fee_per_gas,
            header.map_or(&None, |h| &h.base_fee_per_gas),
        );
        self.blocks
            .coinbase
            .append_value(header.map_or(&[][..], |h| &h.coinbase));
        self.blocks.size.append_value(block.size);
        self.blocks
            .nonce
            .append_value(header.map_or(0, |h| h.nonce));
        self.blocks
            .state_root
            .append_value(header.map_or(&[][..], |h| &h.state_root));
        self.blocks
            .transactions_root
            .append_value(header.map_or(&[][..], |h| &h.transactions_root));
        self.blocks
            .receipt_root
            .append_value(header.map_or(&[][..], |h| &h.receipt_root));
        append_optional_bigint(
            &mut self.blocks.difficulty,
            header.map_or(&None, |h| &h.difficulty),
        );
        self.blocks
            .mix_hash
            .append_value(header.map_or(&[][..], |h| &h.mix_hash));
        self.blocks
            .extra_data
            .append_value(header.map_or(&[][..], |h| &h.extra_data));
        self.blocks
            .num_transactions
            .append_value(block.transaction_traces.len() as u32);
        self.blocks
            .detail_level
            .append_value(detail_level_text(block.detail_level));
        self.blocks
            .uncle_hash
            .append_value(header.map_or(&[][..], |h| &h.uncle_hash));
        self.blocks
            .logs_bloom
            .append_value(header.map_or(&[][..], |h| &h.logs_bloom));
        append_non_empty_bytes(
            &mut self.blocks.withdrawals_root,
            header.map_or(&[][..], |h| &h.withdrawals_root),
        );
        self.blocks
            .blob_gas_used
            .append_option(header.and_then(|h| h.blob_gas_used));
        self.blocks
            .excess_blob_gas
            .append_option(header.and_then(|h| h.excess_blob_gas));
        append_non_empty_bytes(
            &mut self.blocks.parent_beacon_root,
            header.map_or(&[][..], |h| &h.parent_beacon_root),
        );
        append_non_empty_bytes(
            &mut self.blocks.requests_hash,
            header.map_or(&[][..], |h| &h.requests_hash),
        );
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // -- withdrawals (Shanghai and later) --
        for withdrawal in &block.withdrawals {
            self.withdrawals
                .append(number, withdrawal, identity, fork_step);
        }

        // -- transaction traces --
        for tx in &block.transaction_traces {
            // Failed transactions are written unless --exclude-failed-transactions
            // is set; only their persistent state changes are kept.
            if !self.include_failed_transactions && !transaction_succeeded(tx) {
                continue;
            }
            self.map_transaction(number, tx, identity, fork_step);
        }

        // -- block-level events → system_* tables (EXTENDED) --
        if self.extended {
            for bc in &block.balance_changes {
                if let Some(ref mut builder) = self.system_balance_changes {
                    builder.append(number, None, bc, identity, fork_step);
                }
            }
            for cc in &block.code_changes {
                if let Some(ref mut builder) = self.system_code_changes {
                    builder.append(number, None, cc, identity, fork_step);
                }
            }
            for call in &block.system_calls {
                if let Some(ref mut builder) = self.system_calls {
                    builder.append(number, call, identity, fork_step);
                }
                self.extract_system_call_state_changes(number, call, identity, fork_step);
            }
        }
    }

    fn map_transaction(
        &mut self,
        block_number: u64,
        tx: &eth::TransactionTrace,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let tx_hash = &tx.hash;

        self.transactions.canonical.append(identity);
        self.transactions.block_number.append_value(block_number);
        self.transactions.index.append_value(tx.index);
        self.transactions.hash.append_value(tx_hash);
        self.transactions.from.append_value(&tx.from);
        self.transactions.to.append_value(&tx.to);
        append_bigint(&mut self.transactions.value, &tx.value);
        self.transactions.gas_limit.append_value(tx.gas_limit);
        self.transactions.gas_used.append_value(tx.gas_used);
        append_optional_bigint(&mut self.transactions.gas_price, &tx.gas_price);
        self.transactions
            .r#type
            .append_value(transaction_type_text(tx.r#type));
        self.transactions
            .status
            .append_value(transaction_status_text(tx.status));
        self.transactions.nonce.append_value(tx.nonce);
        self.transactions.input.append_value(&tx.input);
        append_optional_bigint(&mut self.transactions.max_fee_per_gas, &tx.max_fee_per_gas);
        append_optional_bigint(
            &mut self.transactions.max_priority_fee_per_gas,
            &tx.max_priority_fee_per_gas,
        );
        if let Some(ref receipt) = tx.receipt {
            self.transactions
                .cumulative_gas_used
                .append_value(receipt.cumulative_gas_used);
        } else {
            self.transactions.cumulative_gas_used.append_null();
        }
        self.transactions.v.append_value(&tx.v);
        self.transactions.r.append_value(&tx.r);
        self.transactions.s.append_value(&tx.s);
        self.transactions.return_data.append_value(&tx.return_data);
        let receipt = tx.receipt.as_ref();
        match receipt {
            Some(receipt) => self
                .transactions
                .logs_bloom
                .append_value(&receipt.logs_bloom),
            None => self.transactions.logs_bloom.append_null(),
        }
        self.transactions.blob_gas.append_option(tx.blob_gas);
        append_optional_bigint(
            &mut self.transactions.blob_gas_fee_cap,
            &tx.blob_gas_fee_cap,
        );
        for blob_hash in &tx.blob_hashes {
            self.transactions.blob_hashes.append_value(blob_hash);
        }
        self.transactions.blob_hashes.append(true);
        self.transactions
            .blob_gas_used
            .append_option(receipt.and_then(|r| r.blob_gas_used));
        append_optional_bigint(
            &mut self.transactions.blob_gas_price,
            receipt.map_or(&None, |r| &r.blob_gas_price),
        );
        self.transactions
            .begin_ordinal
            .append_value(tx.begin_ordinal);
        self.transactions.end_ordinal.append_value(tx.end_ordinal);
        append_fork_step(&mut self.transactions.fork_step, fork_step);

        // -- access list and EIP-7702 authorizations --
        for (access_index, tuple) in tx.access_list.iter().enumerate() {
            self.access_lists.append(
                block_number,
                tx,
                access_index as u32,
                tuple,
                identity,
                fork_step,
            );
        }
        for (authorization_index, auth) in tx.set_code_authorizations.iter().enumerate() {
            self.set_code_authorizations.append(
                block_number,
                tx,
                authorization_index as u32,
                auth,
                identity,
                fork_step,
            );
        }

        // -- logs from receipt --
        if let Some(ref receipt) = tx.receipt {
            for log in &receipt.logs {
                self.map_log(block_number, tx_hash, tx.index, log, identity, fork_step);
            }
        }

        // -- calls & state changes (EXTENDED) --
        if self.extended {
            for call in &tx.calls {
                if let Some(ref mut builder) = self.calls {
                    builder.append(block_number, tx_hash, tx.index, call, identity, fork_step);
                }
            }
            if transaction_succeeded(tx) {
                for call in &tx.calls {
                    self.extract_call_state_changes(block_number, tx, call, identity, fork_step);
                }
            } else {
                self.extract_failed_transaction_state_changes(
                    block_number,
                    tx,
                    identity,
                    fork_step,
                );
            }
        }
    }

    /// Write the state changes of a failed or reverted transaction that persist
    /// on chain (see [`failed_transaction_persistent_changes`]). Gas changes are
    /// all kept: they record gas that the transaction consumed and paid for.
    fn extract_failed_transaction_state_changes(
        &mut self,
        block_number: u64,
        tx: &eth::TransactionTrace,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        // Persistent changes all come from the root call.
        if let Some(root) = tx.calls.first() {
            let ctx = ChangeContext::new(tx, root);
            let persistent = failed_transaction_persistent_changes(tx);
            if let Some(ref mut builder) = self.balance_changes {
                for bc in persistent.balance_changes {
                    builder.append(block_number, &ctx, bc, identity, fork_step);
                }
            }
            if let Some(ref mut builder) = self.nonce_changes {
                for nc in persistent.nonce_changes {
                    builder.append(block_number, &ctx, nc, identity, fork_step);
                }
            }
            if let Some(ref mut builder) = self.code_changes {
                for cc in persistent.code_changes {
                    builder.append(block_number, &ctx, cc, identity, fork_step);
                }
            }
        }
        if let Some(ref mut builder) = self.gas_changes {
            for call in &tx.calls {
                let ctx = ChangeContext::new(tx, call);
                for gc in &call.gas_changes {
                    builder.append(block_number, &ctx, gc, identity, fork_step);
                }
            }
        }
    }

    fn map_log(
        &mut self,
        block_number: u64,
        tx_hash: &[u8],
        tx_index: u32,
        log: &eth::Log,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.logs.canonical.append(identity);
        self.logs.block_number.append_value(block_number);
        self.logs.tx_hash.append_value(tx_hash);
        self.logs.tx_index.append_value(tx_index);
        self.logs.log_index.append_value(log.index);
        self.logs.block_index.append_value(log.block_index);
        self.logs.address.append_value(&log.address);

        let topics = &log.topics;
        let topic_builders = [
            &mut self.logs.topic0,
            &mut self.logs.topic1,
            &mut self.logs.topic2,
            &mut self.logs.topic3,
        ];
        for (i, builder) in topic_builders.into_iter().enumerate() {
            if i < topics.len() {
                builder.append_value(&topics[i]);
            } else {
                builder.append_null();
            }
        }
        if log.data.is_empty() {
            self.logs.data.append_null();
        } else {
            self.logs.data.append_value(&log.data);
        }
        self.logs.ordinal.append_value(log.ordinal);
        append_fork_step(&mut self.logs.fork_step, fork_step);
    }

    /// Extract state changes from transaction-scoped calls → transaction tables.
    fn extract_call_state_changes(
        &mut self,
        block_number: u64,
        tx: &eth::TransactionTrace,
        call: &eth::Call,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        if !self.extended {
            return;
        }
        let ctx = ChangeContext::new(tx, call);

        for bc in &call.balance_changes {
            if let Some(ref mut builder) = self.balance_changes {
                builder.append(block_number, &ctx, bc, identity, fork_step);
            }
        }
        for cc in &call.code_changes {
            if let Some(ref mut builder) = self.code_changes {
                builder.append(block_number, &ctx, cc, identity, fork_step);
            }
        }
        for sc in &call.storage_changes {
            if let Some(ref mut builder) = self.storage_changes {
                builder.append(block_number, &ctx, sc, identity, fork_step);
            }
        }
        for nc in &call.nonce_changes {
            if let Some(ref mut builder) = self.nonce_changes {
                builder.append(block_number, &ctx, nc, identity, fork_step);
            }
        }
        for gc in &call.gas_changes {
            if let Some(ref mut builder) = self.gas_changes {
                builder.append(block_number, &ctx, gc, identity, fork_step);
            }
        }
        #[allow(deprecated)]
        for ac in &call.account_creations {
            if let Some(ref mut builder) = self.account_creations {
                builder.append(block_number, &ctx, ac, identity, fork_step);
            }
        }
    }

    /// Extract state changes from system calls → system_* tables.
    fn extract_system_call_state_changes(
        &mut self,
        block_number: u64,
        call: &eth::Call,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        if !self.extended {
            return;
        }

        for bc in &call.balance_changes {
            if let Some(ref mut builder) = self.system_balance_changes {
                builder.append(block_number, Some(call.index), bc, identity, fork_step);
            }
        }
        for cc in &call.code_changes {
            if let Some(ref mut builder) = self.system_code_changes {
                builder.append(block_number, Some(call.index), cc, identity, fork_step);
            }
        }
        for sc in &call.storage_changes {
            if let Some(ref mut builder) = self.system_storage_changes {
                builder.append(block_number, Some(call.index), sc, identity, fork_step);
            }
        }
        for nc in &call.nonce_changes {
            if let Some(ref mut builder) = self.system_nonce_changes {
                builder.append(block_number, Some(call.index), nc, identity, fork_step);
            }
        }
        for gc in &call.gas_changes {
            if let Some(ref mut builder) = self.system_gas_changes {
                builder.append(block_number, Some(call.index), gc, identity, fork_step);
            }
        }
        #[allow(deprecated)]
        for ac in &call.account_creations {
            if let Some(ref mut builder) = self.system_account_creations {
                builder.append(block_number, Some(call.index), ac, identity, fork_step);
            }
        }
    }
}

impl EvmBlockMapper {
    fn map_decoded(
        &mut self,
        block: eth::Block,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        let tx_count = block.transaction_traces.len() as u64;
        let identity = self.blocks.canonical.prepare(identity)?;
        self.map_evm_block(&block, &identity, fork_step);
        Ok(tx_count)
    }
}

impl BlockMapper for EvmBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        self.map_decoded(eth::Block::decode(block_bytes)?, identity, fork_step)
    }

    fn map_block_bytes(
        &mut self,
        block_bytes: prost::bytes::Bytes,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        self.map_decoded(eth::Block::decode(block_bytes)?, identity, fork_step)
    }

    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        result.insert(
            "blocks".to_string(),
            self.blocks.finish(&self.blocks_schema)?,
        );
        result.insert(
            "transactions".to_string(),
            self.transactions.finish(&self.transactions_schema)?,
        );
        result.insert("logs".to_string(), self.logs.finish(&self.logs_schema)?);
        result.insert(
            "withdrawals".to_string(),
            self.withdrawals.finish(&self.withdrawals_schema)?,
        );
        result.insert(
            "access_lists".to_string(),
            self.access_lists.finish(&self.access_lists_schema)?,
        );
        result.insert(
            "set_code_authorizations".to_string(),
            self.set_code_authorizations
                .finish(&self.set_code_authorizations_schema)?,
        );

        if self.extended {
            if let Some(ref mut b) = self.calls {
                result.insert("calls".to_string(), b.finish(&self.calls_schema)?);
            }
            if let Some(ref mut b) = self.balance_changes {
                result.insert(
                    "balance_changes".to_string(),
                    b.finish(&self.balance_changes_schema)?,
                );
            }
            if let Some(ref mut b) = self.code_changes {
                result.insert(
                    "code_changes".to_string(),
                    b.finish(&self.code_changes_schema)?,
                );
            }
            if let Some(ref mut b) = self.storage_changes {
                result.insert(
                    "storage_changes".to_string(),
                    b.finish(&self.storage_changes_schema)?,
                );
            }
            if let Some(ref mut b) = self.nonce_changes {
                result.insert(
                    "nonce_changes".to_string(),
                    b.finish(&self.nonce_changes_schema)?,
                );
            }
            if let Some(ref mut b) = self.gas_changes {
                result.insert(
                    "gas_changes".to_string(),
                    b.finish(&self.gas_changes_schema)?,
                );
            }
            if let Some(ref mut b) = self.account_creations {
                result.insert(
                    "account_creations".to_string(),
                    b.finish(&self.account_creations_schema)?,
                );
            }
            // System tables
            if let Some(ref mut b) = self.system_calls {
                result.insert(
                    "system_calls".to_string(),
                    b.finish(&self.system_calls_schema)?,
                );
            }
            if let Some(ref mut b) = self.system_balance_changes {
                result.insert(
                    "system_balance_changes".to_string(),
                    b.finish(&self.system_balance_changes_schema)?,
                );
            }
            if let Some(ref mut b) = self.system_code_changes {
                result.insert(
                    "system_code_changes".to_string(),
                    b.finish(&self.system_code_changes_schema)?,
                );
            }
            if let Some(ref mut b) = self.system_storage_changes {
                result.insert(
                    "system_storage_changes".to_string(),
                    b.finish(&self.system_storage_changes_schema)?,
                );
            }
            if let Some(ref mut b) = self.system_nonce_changes {
                result.insert(
                    "system_nonce_changes".to_string(),
                    b.finish(&self.system_nonce_changes_schema)?,
                );
            }
            if let Some(ref mut b) = self.system_gas_changes {
                result.insert(
                    "system_gas_changes".to_string(),
                    b.finish(&self.system_gas_changes_schema)?,
                );
            }
            if let Some(ref mut b) = self.system_account_creations {
                result.insert(
                    "system_account_creations".to_string(),
                    b.finish(&self.system_account_creations_schema)?,
                );
            }
        }
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        let mut max = self
            .blocks
            .canonical
            .len()
            .max(self.transactions.canonical.len())
            .max(self.logs.canonical.len())
            .max(self.withdrawals.canonical.len())
            .max(self.access_lists.canonical.len())
            .max(self.set_code_authorizations.canonical.len());
        if let Some(ref b) = self.calls {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.balance_changes {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.storage_changes {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.nonce_changes {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.gas_changes {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.code_changes {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.account_creations {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.system_calls {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.system_balance_changes {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.system_code_changes {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.system_storage_changes {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.system_nonce_changes {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.system_gas_changes {
            max = max.max(b.canonical.len());
        }
        if let Some(ref b) = self.system_account_creations {
            max = max.max(b.canonical.len());
        }
        max
    }

    fn total_rows(&self) -> usize {
        let mut total = self.blocks.canonical.len()
            + self.transactions.canonical.len()
            + self.logs.canonical.len()
            + self.withdrawals.canonical.len()
            + self.access_lists.canonical.len()
            + self.set_code_authorizations.canonical.len();
        if let Some(ref b) = self.calls {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.balance_changes {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.storage_changes {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.nonce_changes {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.gas_changes {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.code_changes {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.account_creations {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.system_calls {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.system_balance_changes {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.system_code_changes {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.system_storage_changes {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.system_nonce_changes {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.system_gas_changes {
            total += b.canonical.len();
        }
        if let Some(ref b) = self.system_account_creations {
            total += b.canonical.len();
        }
        total
    }

    fn largest_table(&mut self) -> (&str, usize) {
        // blocks
        let blocks = self.blocks.canonical.estimated_bytes()
            + est_u64(&self.blocks.number)
            + self.blocks.hash.estimated_bytes()
            + self.blocks.parent_hash.estimated_bytes()
            + est_u64(&self.blocks.gas_used)
            + est_u64(&self.blocks.gas_limit)
            + est_str(&self.blocks.base_fee_per_gas)
            + self.blocks.coinbase.estimated_bytes()
            + est_u64(&self.blocks.size)
            + est_u64(&self.blocks.nonce)
            + self.blocks.state_root.estimated_bytes()
            + self.blocks.transactions_root.estimated_bytes()
            + self.blocks.receipt_root.estimated_bytes()
            + est_str(&self.blocks.difficulty)
            + self.blocks.mix_hash.estimated_bytes()
            + self.blocks.extra_data.estimated_bytes()
            + est_u32(&self.blocks.num_transactions)
            + estimated_dictionary_index_bytes(self.blocks.detail_level.len())
            + self.blocks.uncle_hash.estimated_bytes()
            + self.blocks.logs_bloom.estimated_bytes()
            + self.blocks.withdrawals_root.estimated_bytes()
            + est_u64(&self.blocks.blob_gas_used)
            + est_u64(&self.blocks.excess_blob_gas)
            + self.blocks.parent_beacon_root.estimated_bytes()
            + self.blocks.requests_hash.estimated_bytes()
            + est_opt_str(&self.blocks.fork_step);
        // transactions
        let transactions = self.transactions.canonical.estimated_bytes()
            + est_u64(&self.transactions.block_number)
            + est_u32(&self.transactions.index)
            + self.transactions.hash.estimated_bytes()
            + self.transactions.from.estimated_bytes()
            + self.transactions.to.estimated_bytes()
            + est_str(&self.transactions.value)
            + est_u64(&self.transactions.gas_limit)
            + est_u64(&self.transactions.gas_used)
            + est_str(&self.transactions.gas_price)
            + estimated_dictionary_index_bytes(self.transactions.r#type.len())
            + estimated_dictionary_index_bytes(self.transactions.status.len())
            + est_u64(&self.transactions.nonce)
            + self.transactions.input.estimated_bytes()
            + est_str(&self.transactions.max_fee_per_gas)
            + est_str(&self.transactions.max_priority_fee_per_gas)
            + est_u64(&self.transactions.cumulative_gas_used)
            + self.transactions.v.estimated_bytes()
            + self.transactions.r.estimated_bytes()
            + self.transactions.s.estimated_bytes()
            + self.transactions.return_data.estimated_bytes()
            + self.transactions.logs_bloom.estimated_bytes()
            + est_u64(&self.transactions.blob_gas)
            + est_str(&self.transactions.blob_gas_fee_cap)
            + self.transactions.blob_hashes.estimated_bytes()
            + est_u64(&self.transactions.blob_gas_used)
            + est_str(&self.transactions.blob_gas_price)
            + est_u64(&self.transactions.begin_ordinal)
            + est_u64(&self.transactions.end_ordinal)
            + est_opt_str(&self.transactions.fork_step);
        // logs
        let logs = self.logs.canonical.estimated_bytes()
            + est_u64(&self.logs.block_number)
            + self.logs.tx_hash.estimated_bytes()
            + est_u32(&self.logs.tx_index)
            + est_u32(&self.logs.log_index)
            + est_u32(&self.logs.block_index)
            + self.logs.address.estimated_bytes()
            + self.logs.topic0.estimated_bytes()
            + self.logs.topic1.estimated_bytes()
            + self.logs.topic2.estimated_bytes()
            + self.logs.topic3.estimated_bytes()
            + self.logs.data.estimated_bytes()
            + est_u64(&self.logs.ordinal)
            + est_opt_str(&self.logs.fork_step);
        let mut tables: Vec<(&str, usize)> = vec![
            ("blocks", blocks),
            ("transactions", transactions),
            ("logs", logs),
            ("withdrawals", self.withdrawals.estimated_bytes()),
            ("access_lists", self.access_lists.estimated_bytes()),
            (
                "set_code_authorizations",
                self.set_code_authorizations.estimated_bytes(),
            ),
        ];
        // calls (tx-level)
        macro_rules! est_calls {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + $b.tx_hash.estimated_bytes()
                    + est_u32(&$b.tx_index)
                    + est_u32(&$b.call_index)
                    + est_u32(&$b.parent_index)
                    + est_u32(&$b.depth)
                    + estimated_dictionary_index_bytes($b.call_type.len())
                    + $b.caller.estimated_bytes()
                    + $b.address.estimated_bytes()
                    + est_str(&$b.value)
                    + est_u64(&$b.gas_limit)
                    + est_u64(&$b.gas_consumed)
                    + $b.input.estimated_bytes()
                    + $b.output.estimated_bytes()
                    + est_bool(&$b.status_failed)
                    + est_bool(&$b.status_reverted)
                    + est_bool(&$b.state_reverted)
                    + est_bool(&$b.executed_code)
                    + est_bool(&$b.suicide)
                    + est_str(&$b.failure_reason)
                    + $b.address_delegates_to.estimated_bytes()
                    + est_u64(&$b.begin_ordinal)
                    + est_u64(&$b.end_ordinal)
                    + est_opt_str(&$b.fork_step)
            };
        }
        // system_calls (block-level, no tx_hash/tx_index)
        macro_rules! est_sys_calls {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + est_u32(&$b.call_index)
                    + est_u32(&$b.parent_index)
                    + est_u32(&$b.depth)
                    + estimated_dictionary_index_bytes($b.call_type.len())
                    + $b.caller.estimated_bytes()
                    + $b.address.estimated_bytes()
                    + est_str(&$b.value)
                    + est_u64(&$b.gas_limit)
                    + est_u64(&$b.gas_consumed)
                    + $b.input.estimated_bytes()
                    + $b.output.estimated_bytes()
                    + est_bool(&$b.status_failed)
                    + est_bool(&$b.status_reverted)
                    + est_bool(&$b.state_reverted)
                    + est_bool(&$b.executed_code)
                    + est_bool(&$b.suicide)
                    + est_str(&$b.failure_reason)
                    + $b.address_delegates_to.estimated_bytes()
                    + est_u64(&$b.begin_ordinal)
                    + est_u64(&$b.end_ordinal)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_balance_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + $b.tx_hash.estimated_bytes()
                    + est_u32(&$b.tx_index)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + est_str(&$b.old_value)
                    + est_str(&$b.new_value)
                    + estimated_dictionary_index_bytes($b.reason.len())
                    + est_bool(&$b.state_reverted)
                    + est_bool(&$b.persisted)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_balance_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + est_str(&$b.old_value)
                    + est_str(&$b.new_value)
                    + estimated_dictionary_index_bytes($b.reason.len())
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_code_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + $b.tx_hash.estimated_bytes()
                    + est_u32(&$b.tx_index)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + $b.old_hash.estimated_bytes()
                    + $b.new_hash.estimated_bytes()
                    + $b.old_code.estimated_bytes()
                    + $b.new_code.estimated_bytes()
                    + est_bool(&$b.state_reverted)
                    + est_bool(&$b.persisted)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_code_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + $b.old_hash.estimated_bytes()
                    + $b.new_hash.estimated_bytes()
                    + $b.old_code.estimated_bytes()
                    + $b.new_code.estimated_bytes()
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_storage_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + $b.tx_hash.estimated_bytes()
                    + est_u32(&$b.tx_index)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + $b.key.estimated_bytes()
                    + $b.old_value.estimated_bytes()
                    + $b.new_value.estimated_bytes()
                    + est_bool(&$b.state_reverted)
                    + est_bool(&$b.persisted)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_storage_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + $b.key.estimated_bytes()
                    + $b.old_value.estimated_bytes()
                    + $b.new_value.estimated_bytes()
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_nonce_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + $b.tx_hash.estimated_bytes()
                    + est_u32(&$b.tx_index)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + est_u64(&$b.old_value)
                    + est_u64(&$b.new_value)
                    + est_bool(&$b.state_reverted)
                    + est_bool(&$b.persisted)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_nonce_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + est_u64(&$b.old_value)
                    + est_u64(&$b.new_value)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_gas_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + $b.tx_hash.estimated_bytes()
                    + est_u32(&$b.tx_index)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + est_u64(&$b.old_value)
                    + est_u64(&$b.new_value)
                    + estimated_dictionary_index_bytes($b.reason.len())
                    + est_bool(&$b.state_reverted)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_gas_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + est_u64(&$b.old_value)
                    + est_u64(&$b.new_value)
                    + estimated_dictionary_index_bytes($b.reason.len())
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_account_creations {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + $b.tx_hash.estimated_bytes()
                    + est_u32(&$b.tx_index)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + $b.account.estimated_bytes()
                    + est_bool(&$b.state_reverted)
                    + est_bool(&$b.persisted)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_account_creations {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + est_u32(&$b.call_index)
                    + est_u64(&$b.ordinal)
                    + $b.account.estimated_bytes()
                    + est_opt_str(&$b.fork_step)
            };
        }
        if let Some(ref b) = self.calls {
            tables.push(("calls", est_calls!(b)));
        }
        if let Some(ref b) = self.balance_changes {
            tables.push(("balance_changes", est_balance_changes!(b)));
        }
        if let Some(ref b) = self.code_changes {
            tables.push(("code_changes", est_code_changes!(b)));
        }
        if let Some(ref b) = self.storage_changes {
            tables.push(("storage_changes", est_storage_changes!(b)));
        }
        if let Some(ref b) = self.nonce_changes {
            tables.push(("nonce_changes", est_nonce_changes!(b)));
        }
        if let Some(ref b) = self.gas_changes {
            tables.push(("gas_changes", est_gas_changes!(b)));
        }
        if let Some(ref b) = self.account_creations {
            tables.push(("account_creations", est_account_creations!(b)));
        }
        if let Some(ref b) = self.system_calls {
            tables.push(("system_calls", est_sys_calls!(b)));
        }
        if let Some(ref b) = self.system_balance_changes {
            tables.push(("system_balance_changes", est_sys_balance_changes!(b)));
        }
        if let Some(ref b) = self.system_code_changes {
            tables.push(("system_code_changes", est_sys_code_changes!(b)));
        }
        if let Some(ref b) = self.system_storage_changes {
            tables.push(("system_storage_changes", est_sys_storage_changes!(b)));
        }
        if let Some(ref b) = self.system_nonce_changes {
            tables.push(("system_nonce_changes", est_sys_nonce_changes!(b)));
        }
        if let Some(ref b) = self.system_gas_changes {
            tables.push(("system_gas_changes", est_sys_gas_changes!(b)));
        }
        if let Some(ref b) = self.system_account_creations {
            tables.push(("system_account_creations", est_sys_account_creations!(b)));
        }
        tables
            .into_iter()
            .max_by_key(|&(_, s)| s)
            .unwrap_or(("blocks", 0))
    }

    fn table_names(&self) -> Vec<&str> {
        if self.extended {
            schema::EXTENDED_TABLE_NAMES.to_vec()
        } else {
            schema::BASE_TABLE_NAMES.to_vec()
        }
    }
}

// ===========================================================================
// Transaction-scoped Builders
// ===========================================================================

struct EvmBlocksBuilder {
    canonical: CanonicalBuilder,
    number: UInt64Builder,
    hash: BytesColumn,
    parent_hash: BytesColumn,
    gas_used: UInt64Builder,
    gas_limit: UInt64Builder,
    base_fee_per_gas: StringBuilder,
    coinbase: BytesColumn,
    size: UInt64Builder,
    nonce: UInt64Builder,
    state_root: BytesColumn,
    transactions_root: BytesColumn,
    receipt_root: BytesColumn,
    difficulty: StringBuilder,
    mix_hash: BytesColumn,
    extra_data: BytesColumn,
    num_transactions: UInt32Builder,
    detail_level: StringDictionaryBuilder<Int32Type>,
    uncle_hash: BytesColumn,
    logs_bloom: BytesColumn,
    withdrawals_root: BytesColumn,
    blob_gas_used: UInt64Builder,
    excess_blob_gas: UInt64Builder,
    parent_beacon_root: BytesColumn,
    requests_hash: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl EvmBlocksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            number: UInt64Builder::new(),
            hash: BytesColumn::new(encoding),
            parent_hash: BytesColumn::new(encoding),
            gas_used: UInt64Builder::new(),
            gas_limit: UInt64Builder::new(),
            base_fee_per_gas: StringBuilder::new(),
            coinbase: BytesColumn::new(encoding),
            size: UInt64Builder::new(),
            nonce: UInt64Builder::new(),
            state_root: BytesColumn::new(encoding),
            transactions_root: BytesColumn::new(encoding),
            receipt_root: BytesColumn::new(encoding),
            difficulty: StringBuilder::new(),
            mix_hash: BytesColumn::new(encoding),
            extra_data: BytesColumn::new(encoding),
            num_transactions: UInt32Builder::new(),
            detail_level: StringDictionaryBuilder::new(),
            uncle_hash: BytesColumn::new(encoding),
            logs_bloom: BytesColumn::new(encoding),
            withdrawals_root: BytesColumn::new(encoding),
            blob_gas_used: UInt64Builder::new(),
            excess_blob_gas: UInt64Builder::new(),
            parent_beacon_root: BytesColumn::new(encoding),
            requests_hash: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.number.finish()) as Arc<dyn arrow::array::Array>,
            self.hash.finish(),
            self.parent_hash.finish(),
            Arc::new(self.gas_used.finish()),
            Arc::new(self.gas_limit.finish()),
            Arc::new(self.base_fee_per_gas.finish()),
            self.coinbase.finish(),
            Arc::new(self.size.finish()),
            Arc::new(self.nonce.finish()),
            self.state_root.finish(),
            self.transactions_root.finish(),
            self.receipt_root.finish(),
            Arc::new(self.difficulty.finish()),
            self.mix_hash.finish(),
            self.extra_data.finish(),
            Arc::new(self.num_transactions.finish()),
            Arc::new(self.detail_level.finish()),
            self.uncle_hash.finish(),
            self.logs_bloom.finish(),
            self.withdrawals_root.finish(),
            Arc::new(self.blob_gas_used.finish()),
            Arc::new(self.excess_blob_gas.finish()),
            self.parent_beacon_root.finish(),
            self.requests_hash.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmTransactionsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    index: UInt32Builder,
    hash: BytesColumn,
    from: BytesColumn,
    to: BytesColumn,
    value: StringBuilder,
    gas_limit: UInt64Builder,
    gas_used: UInt64Builder,
    gas_price: StringBuilder,
    r#type: StringDictionaryBuilder<Int32Type>,
    status: StringDictionaryBuilder<Int32Type>,
    nonce: UInt64Builder,
    input: BytesColumn,
    max_fee_per_gas: StringBuilder,
    max_priority_fee_per_gas: StringBuilder,
    cumulative_gas_used: UInt64Builder,
    v: BytesColumn,
    r: BytesColumn,
    s: BytesColumn,
    return_data: BytesColumn,
    logs_bloom: BytesColumn,
    blob_gas: UInt64Builder,
    blob_gas_fee_cap: StringBuilder,
    blob_hashes: BytesListColumn,
    blob_gas_used: UInt64Builder,
    blob_gas_price: StringBuilder,
    begin_ordinal: UInt64Builder,
    end_ordinal: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmTransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            index: UInt32Builder::new(),
            hash: BytesColumn::new(encoding),
            from: BytesColumn::new(encoding),
            to: BytesColumn::new(encoding),
            value: StringBuilder::new(),
            gas_limit: UInt64Builder::new(),
            gas_used: UInt64Builder::new(),
            gas_price: StringBuilder::new(),
            r#type: StringDictionaryBuilder::new(),
            status: StringDictionaryBuilder::new(),
            nonce: UInt64Builder::new(),
            input: BytesColumn::new(encoding),
            max_fee_per_gas: StringBuilder::new(),
            max_priority_fee_per_gas: StringBuilder::new(),
            cumulative_gas_used: UInt64Builder::new(),
            v: BytesColumn::new(encoding),
            r: BytesColumn::new(encoding),
            s: BytesColumn::new(encoding),
            return_data: BytesColumn::new(encoding),
            logs_bloom: BytesColumn::new(encoding),
            blob_gas: UInt64Builder::new(),
            blob_gas_fee_cap: StringBuilder::new(),
            blob_hashes: BytesListColumn::new(encoding),
            blob_gas_used: UInt64Builder::new(),
            blob_gas_price: StringBuilder::new(),
            begin_ordinal: UInt64Builder::new(),
            end_ordinal: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.index.finish()),
            self.hash.finish(),
            self.from.finish(),
            self.to.finish(),
            Arc::new(self.value.finish()),
            Arc::new(self.gas_limit.finish()),
            Arc::new(self.gas_used.finish()),
            Arc::new(self.gas_price.finish()),
            Arc::new(self.r#type.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.nonce.finish()),
            self.input.finish(),
            Arc::new(self.max_fee_per_gas.finish()),
            Arc::new(self.max_priority_fee_per_gas.finish()),
            Arc::new(self.cumulative_gas_used.finish()),
            self.v.finish(),
            self.r.finish(),
            self.s.finish(),
            self.return_data.finish(),
            self.logs_bloom.finish(),
            Arc::new(self.blob_gas.finish()),
            Arc::new(self.blob_gas_fee_cap.finish()),
            self.blob_hashes.finish(),
            Arc::new(self.blob_gas_used.finish()),
            Arc::new(self.blob_gas_price.finish()),
            Arc::new(self.begin_ordinal.finish()),
            Arc::new(self.end_ordinal.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmLogsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    log_index: UInt32Builder,
    block_index: UInt32Builder,
    address: BytesColumn,
    topic0: BytesColumn,
    topic1: BytesColumn,
    topic2: BytesColumn,
    topic3: BytesColumn,
    data: BytesColumn,
    ordinal: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmLogsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            log_index: UInt32Builder::new(),
            block_index: UInt32Builder::new(),
            address: BytesColumn::new(encoding),
            topic0: BytesColumn::new(encoding),
            topic1: BytesColumn::new(encoding),
            topic2: BytesColumn::new(encoding),
            topic3: BytesColumn::new(encoding),
            data: BytesColumn::new(encoding),
            ordinal: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.log_index.finish()),
            Arc::new(self.block_index.finish()),
            self.address.finish(),
            self.topic0.finish(),
            self.topic1.finish(),
            self.topic2.finish(),
            self.topic3.finish(),
            self.data.finish(),
            Arc::new(self.ordinal.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmWithdrawalsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    index: UInt64Builder,
    validator_index: UInt64Builder,
    address: BytesColumn,
    amount_gwei: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmWithdrawalsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            index: UInt64Builder::new(),
            validator_index: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            amount_gwei: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        withdrawal: &eth::Withdrawal,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.index.append_value(withdrawal.index);
        self.validator_index
            .append_value(withdrawal.validator_index);
        self.address.append_value(&withdrawal.address);
        self.amount_gwei.append_value(withdrawal.amount);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.block_number)
            + est_u64(&self.index)
            + est_u64(&self.validator_index)
            + self.address.estimated_bytes()
            + est_u64(&self.amount_gwei)
            + est_opt_str(&self.fork_step)
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.index.finish()),
            Arc::new(self.validator_index.finish()),
            self.address.finish(),
            Arc::new(self.amount_gwei.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmAccessListsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    access_index: UInt32Builder,
    address: BytesColumn,
    storage_keys: BytesListColumn,
    fork_step: Option<StringBuilder>,
}

impl EvmAccessListsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            access_index: UInt32Builder::new(),
            address: BytesColumn::new(encoding),
            storage_keys: BytesListColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        tx: &eth::TransactionTrace,
        access_index: u32,
        tuple: &eth::AccessTuple,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(&tx.hash);
        self.tx_index.append_value(tx.index);
        self.access_index.append_value(access_index);
        self.address.append_value(&tuple.address);
        for key in &tuple.storage_keys {
            self.storage_keys.append_value(key);
        }
        self.storage_keys.append(true);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&mut self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.block_number)
            + self.tx_hash.estimated_bytes()
            + est_u32(&self.tx_index)
            + est_u32(&self.access_index)
            + self.address.estimated_bytes()
            + self.storage_keys.estimated_bytes()
            + est_opt_str(&self.fork_step)
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.access_index.finish()),
            self.address.finish(),
            self.storage_keys.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmSetCodeAuthorizationsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    authorization_index: UInt32Builder,
    chain_id: StringBuilder,
    address: BytesColumn,
    nonce: UInt64Builder,
    v: UInt32Builder,
    r: BytesColumn,
    s: BytesColumn,
    authority: BytesColumn,
    discarded: BooleanBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmSetCodeAuthorizationsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            authorization_index: UInt32Builder::new(),
            chain_id: StringBuilder::new(),
            address: BytesColumn::new(encoding),
            nonce: UInt64Builder::new(),
            v: UInt32Builder::new(),
            r: BytesColumn::new(encoding),
            s: BytesColumn::new(encoding),
            authority: BytesColumn::new(encoding),
            discarded: BooleanBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        tx: &eth::TransactionTrace,
        authorization_index: u32,
        auth: &eth::SetCodeAuthorization,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(&tx.hash);
        self.tx_index.append_value(tx.index);
        self.authorization_index.append_value(authorization_index);
        decimal::append_decimal(&mut self.chain_id, &auth.chain_id);
        append_non_empty_bytes(&mut self.address, &auth.address);
        self.nonce.append_value(auth.nonce);
        self.v.append_value(auth.v);
        self.r.append_value(&auth.r);
        self.s.append_value(&auth.s);
        append_non_empty_bytes(
            &mut self.authority,
            auth.authority.as_deref().unwrap_or_default(),
        );
        self.discarded.append_value(auth.discarded);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.block_number)
            + self.tx_hash.estimated_bytes()
            + est_u32(&self.tx_index)
            + est_u32(&self.authorization_index)
            + est_str(&self.chain_id)
            + self.address.estimated_bytes()
            + est_u64(&self.nonce)
            + est_u32(&self.v)
            + self.r.estimated_bytes()
            + self.s.estimated_bytes()
            + self.authority.estimated_bytes()
            + est_bool(&self.discarded)
            + est_opt_str(&self.fork_step)
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.authorization_index.finish()),
            Arc::new(self.chain_id.finish()),
            self.address.finish(),
            Arc::new(self.nonce.finish()),
            Arc::new(self.v.finish()),
            self.r.finish(),
            self.s.finish(),
            self.authority.finish(),
            Arc::new(self.discarded.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmCallsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    call_index: UInt32Builder,
    parent_index: UInt32Builder,
    depth: UInt32Builder,
    call_type: StringDictionaryBuilder<Int32Type>,
    caller: BytesColumn,
    address: BytesColumn,
    value: StringBuilder,
    gas_limit: UInt64Builder,
    gas_consumed: UInt64Builder,
    input: BytesColumn,
    output: BytesColumn,
    status_failed: BooleanBuilder,
    status_reverted: BooleanBuilder,
    state_reverted: BooleanBuilder,
    executed_code: BooleanBuilder,
    suicide: BooleanBuilder,
    failure_reason: StringBuilder,
    address_delegates_to: BytesColumn,
    begin_ordinal: UInt64Builder,
    end_ordinal: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmCallsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            call_index: UInt32Builder::new(),
            parent_index: UInt32Builder::new(),
            depth: UInt32Builder::new(),
            call_type: StringDictionaryBuilder::new(),
            caller: BytesColumn::new(encoding),
            address: BytesColumn::new(encoding),
            value: StringBuilder::new(),
            gas_limit: UInt64Builder::new(),
            gas_consumed: UInt64Builder::new(),
            input: BytesColumn::new(encoding),
            output: BytesColumn::new(encoding),
            status_failed: BooleanBuilder::new(),
            status_reverted: BooleanBuilder::new(),
            state_reverted: BooleanBuilder::new(),
            executed_code: BooleanBuilder::new(),
            suicide: BooleanBuilder::new(),
            failure_reason: StringBuilder::new(),
            address_delegates_to: BytesColumn::new(encoding),
            begin_ordinal: UInt64Builder::new(),
            end_ordinal: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        tx_hash: &[u8],
        tx_index: u32,
        call: &eth::Call,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
        self.tx_index.append_value(tx_index);
        self.call_index.append_value(call.index);
        self.parent_index.append_value(call.parent_index);
        self.depth.append_value(call.depth);
        self.call_type.append_value(call_type_text(call.call_type));
        self.caller.append_value(&call.caller);
        self.address.append_value(&call.address);
        append_bigint(&mut self.value, &call.value);
        self.gas_limit.append_value(call.gas_limit);
        self.gas_consumed.append_value(call.gas_consumed);
        self.input.append_value(&call.input);
        self.output.append_value(&call.return_data);
        self.status_failed.append_value(call.status_failed);
        self.status_reverted.append_value(call.status_reverted);
        self.state_reverted.append_value(call.state_reverted);
        self.executed_code.append_value(call.executed_code);
        self.suicide.append_value(call.suicide);
        if call.failure_reason.is_empty() {
            self.failure_reason.append_null();
        } else {
            self.failure_reason.append_value(&call.failure_reason);
        }
        append_non_empty_bytes(
            &mut self.address_delegates_to,
            call.address_delegates_to.as_deref().unwrap_or_default(),
        );
        self.begin_ordinal.append_value(call.begin_ordinal);
        self.end_ordinal.append_value(call.end_ordinal);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.call_index.finish()),
            Arc::new(self.parent_index.finish()),
            Arc::new(self.depth.finish()),
            Arc::new(self.call_type.finish()),
            self.caller.finish(),
            self.address.finish(),
            Arc::new(self.value.finish()),
            Arc::new(self.gas_limit.finish()),
            Arc::new(self.gas_consumed.finish()),
            self.input.finish(),
            self.output.finish(),
            Arc::new(self.status_failed.finish()),
            Arc::new(self.status_reverted.finish()),
            Arc::new(self.state_reverted.finish()),
            Arc::new(self.executed_code.finish()),
            Arc::new(self.suicide.finish()),
            Arc::new(self.failure_reason.finish()),
            self.address_delegates_to.finish(),
            Arc::new(self.begin_ordinal.finish()),
            Arc::new(self.end_ordinal.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmBalanceChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    address: BytesColumn,
    old_value: StringBuilder,
    new_value: StringBuilder,
    reason: StringDictionaryBuilder<Int32Type>,
    state_reverted: BooleanBuilder,
    persisted: BooleanBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmBalanceChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            old_value: StringBuilder::new(),
            new_value: StringBuilder::new(),
            reason: StringDictionaryBuilder::new(),
            state_reverted: BooleanBuilder::new(),
            persisted: BooleanBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        ctx: &ChangeContext,
        bc: &eth::BalanceChange,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(ctx.tx_hash);
        self.tx_index.append_value(ctx.tx_index);
        self.call_index.append_value(ctx.call_index);
        self.ordinal.append_value(bc.ordinal);
        self.address.append_value(&bc.address);
        append_bigint(&mut self.old_value, &bc.old_value);
        append_bigint(&mut self.new_value, &bc.new_value);
        self.reason
            .append_value(balance_change_reason_text(bc.reason));
        self.state_reverted.append_value(ctx.state_reverted);
        self.persisted.append_value(ctx.persisted);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.address.finish(),
            Arc::new(self.old_value.finish()),
            Arc::new(self.new_value.finish()),
            Arc::new(self.reason.finish()),
            Arc::new(self.state_reverted.finish()),
            Arc::new(self.persisted.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmCodeChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    address: BytesColumn,
    old_hash: BytesColumn,
    new_hash: BytesColumn,
    old_code: BytesColumn,
    new_code: BytesColumn,
    state_reverted: BooleanBuilder,
    persisted: BooleanBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmCodeChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            old_hash: BytesColumn::new(encoding),
            new_hash: BytesColumn::new(encoding),
            old_code: BytesColumn::new(encoding),
            new_code: BytesColumn::new(encoding),
            state_reverted: BooleanBuilder::new(),
            persisted: BooleanBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        ctx: &ChangeContext,
        cc: &eth::CodeChange,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(ctx.tx_hash);
        self.tx_index.append_value(ctx.tx_index);
        self.call_index.append_value(ctx.call_index);
        self.ordinal.append_value(cc.ordinal);
        self.address.append_value(&cc.address);
        self.old_hash.append_value(&cc.old_hash);
        self.new_hash.append_value(&cc.new_hash);
        self.old_code.append_value(&cc.old_code);
        self.new_code.append_value(&cc.new_code);
        self.state_reverted.append_value(ctx.state_reverted);
        self.persisted.append_value(ctx.persisted);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.address.finish(),
            self.old_hash.finish(),
            self.new_hash.finish(),
            self.old_code.finish(),
            self.new_code.finish(),
            Arc::new(self.state_reverted.finish()),
            Arc::new(self.persisted.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmStorageChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    address: BytesColumn,
    key: BytesColumn,
    old_value: BytesColumn,
    new_value: BytesColumn,
    state_reverted: BooleanBuilder,
    persisted: BooleanBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmStorageChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            key: BytesColumn::new(encoding),
            old_value: BytesColumn::new(encoding),
            new_value: BytesColumn::new(encoding),
            state_reverted: BooleanBuilder::new(),
            persisted: BooleanBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        ctx: &ChangeContext,
        sc: &eth::StorageChange,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(ctx.tx_hash);
        self.tx_index.append_value(ctx.tx_index);
        self.call_index.append_value(ctx.call_index);
        self.ordinal.append_value(sc.ordinal);
        self.address.append_value(&sc.address);
        self.key.append_value(&sc.key);
        self.old_value.append_value(&sc.old_value);
        self.new_value.append_value(&sc.new_value);
        self.state_reverted.append_value(ctx.state_reverted);
        self.persisted.append_value(ctx.persisted);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.address.finish(),
            self.key.finish(),
            self.old_value.finish(),
            self.new_value.finish(),
            Arc::new(self.state_reverted.finish()),
            Arc::new(self.persisted.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmNonceChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    address: BytesColumn,
    old_value: UInt64Builder,
    new_value: UInt64Builder,
    state_reverted: BooleanBuilder,
    persisted: BooleanBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmNonceChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            old_value: UInt64Builder::new(),
            new_value: UInt64Builder::new(),
            state_reverted: BooleanBuilder::new(),
            persisted: BooleanBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        ctx: &ChangeContext,
        nc: &eth::NonceChange,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(ctx.tx_hash);
        self.tx_index.append_value(ctx.tx_index);
        self.call_index.append_value(ctx.call_index);
        self.ordinal.append_value(nc.ordinal);
        self.address.append_value(&nc.address);
        self.old_value.append_value(nc.old_value);
        self.new_value.append_value(nc.new_value);
        self.state_reverted.append_value(ctx.state_reverted);
        self.persisted.append_value(ctx.persisted);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.address.finish(),
            Arc::new(self.old_value.finish()),
            Arc::new(self.new_value.finish()),
            Arc::new(self.state_reverted.finish()),
            Arc::new(self.persisted.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmGasChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    old_value: UInt64Builder,
    new_value: UInt64Builder,
    reason: StringDictionaryBuilder<Int32Type>,
    state_reverted: BooleanBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmGasChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            old_value: UInt64Builder::new(),
            new_value: UInt64Builder::new(),
            reason: StringDictionaryBuilder::new(),
            state_reverted: BooleanBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        ctx: &ChangeContext,
        gc: &eth::GasChange,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(ctx.tx_hash);
        self.tx_index.append_value(ctx.tx_index);
        self.call_index.append_value(ctx.call_index);
        self.ordinal.append_value(gc.ordinal);
        self.old_value.append_value(gc.old_value);
        self.new_value.append_value(gc.new_value);
        self.reason.append_value(gas_change_reason_text(gc.reason));
        self.state_reverted.append_value(ctx.state_reverted);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            Arc::new(self.old_value.finish()),
            Arc::new(self.new_value.finish()),
            Arc::new(self.reason.finish()),
            Arc::new(self.state_reverted.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmAccountCreationsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    account: BytesColumn,
    state_reverted: BooleanBuilder,
    persisted: BooleanBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmAccountCreationsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            account: BytesColumn::new(encoding),
            state_reverted: BooleanBuilder::new(),
            persisted: BooleanBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        ctx: &ChangeContext,
        ac: &eth::AccountCreation,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(ctx.tx_hash);
        self.tx_index.append_value(ctx.tx_index);
        self.call_index.append_value(ctx.call_index);
        self.ordinal.append_value(ac.ordinal);
        self.account.append_value(&ac.account);
        self.state_reverted.append_value(ctx.state_reverted);
        self.persisted.append_value(ctx.persisted);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.account.finish(),
            Arc::new(self.state_reverted.finish()),
            Arc::new(self.persisted.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

// ===========================================================================
// System Builders (block-level events, no tx_hash/tx_index)
// ===========================================================================

struct SystemCallsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    call_index: UInt32Builder,
    parent_index: UInt32Builder,
    depth: UInt32Builder,
    call_type: StringDictionaryBuilder<Int32Type>,
    caller: BytesColumn,
    address: BytesColumn,
    value: StringBuilder,
    gas_limit: UInt64Builder,
    gas_consumed: UInt64Builder,
    input: BytesColumn,
    output: BytesColumn,
    status_failed: BooleanBuilder,
    status_reverted: BooleanBuilder,
    state_reverted: BooleanBuilder,
    executed_code: BooleanBuilder,
    suicide: BooleanBuilder,
    failure_reason: StringBuilder,
    address_delegates_to: BytesColumn,
    begin_ordinal: UInt64Builder,
    end_ordinal: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl SystemCallsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            call_index: UInt32Builder::new(),
            parent_index: UInt32Builder::new(),
            depth: UInt32Builder::new(),
            call_type: StringDictionaryBuilder::new(),
            caller: BytesColumn::new(encoding),
            address: BytesColumn::new(encoding),
            value: StringBuilder::new(),
            gas_limit: UInt64Builder::new(),
            gas_consumed: UInt64Builder::new(),
            input: BytesColumn::new(encoding),
            output: BytesColumn::new(encoding),
            status_failed: BooleanBuilder::new(),
            status_reverted: BooleanBuilder::new(),
            state_reverted: BooleanBuilder::new(),
            executed_code: BooleanBuilder::new(),
            suicide: BooleanBuilder::new(),
            failure_reason: StringBuilder::new(),
            address_delegates_to: BytesColumn::new(encoding),
            begin_ordinal: UInt64Builder::new(),
            end_ordinal: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        call: &eth::Call,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.call_index.append_value(call.index);
        self.parent_index.append_value(call.parent_index);
        self.depth.append_value(call.depth);
        self.call_type.append_value(call_type_text(call.call_type));
        self.caller.append_value(&call.caller);
        self.address.append_value(&call.address);
        append_bigint(&mut self.value, &call.value);
        self.gas_limit.append_value(call.gas_limit);
        self.gas_consumed.append_value(call.gas_consumed);
        self.input.append_value(&call.input);
        self.output.append_value(&call.return_data);
        self.status_failed.append_value(call.status_failed);
        self.status_reverted.append_value(call.status_reverted);
        self.state_reverted.append_value(call.state_reverted);
        self.executed_code.append_value(call.executed_code);
        self.suicide.append_value(call.suicide);
        if call.failure_reason.is_empty() {
            self.failure_reason.append_null();
        } else {
            self.failure_reason.append_value(&call.failure_reason);
        }
        append_non_empty_bytes(
            &mut self.address_delegates_to,
            call.address_delegates_to.as_deref().unwrap_or_default(),
        );
        self.begin_ordinal.append_value(call.begin_ordinal);
        self.end_ordinal.append_value(call.end_ordinal);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.call_index.finish()),
            Arc::new(self.parent_index.finish()),
            Arc::new(self.depth.finish()),
            Arc::new(self.call_type.finish()),
            self.caller.finish(),
            self.address.finish(),
            Arc::new(self.value.finish()),
            Arc::new(self.gas_limit.finish()),
            Arc::new(self.gas_consumed.finish()),
            self.input.finish(),
            self.output.finish(),
            Arc::new(self.status_failed.finish()),
            Arc::new(self.status_reverted.finish()),
            Arc::new(self.state_reverted.finish()),
            Arc::new(self.executed_code.finish()),
            Arc::new(self.suicide.finish()),
            Arc::new(self.failure_reason.finish()),
            self.address_delegates_to.finish(),
            Arc::new(self.begin_ordinal.finish()),
            Arc::new(self.end_ordinal.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct SystemBalanceChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    address: BytesColumn,
    old_value: StringBuilder,
    new_value: StringBuilder,
    reason: StringDictionaryBuilder<Int32Type>,
    fork_step: Option<StringBuilder>,
}

impl SystemBalanceChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            old_value: StringBuilder::new(),
            new_value: StringBuilder::new(),
            reason: StringDictionaryBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        call_index: Option<u32>,
        bc: &eth::BalanceChange,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.call_index.append_option(call_index);
        self.ordinal.append_value(bc.ordinal);
        self.address.append_value(&bc.address);
        append_bigint(&mut self.old_value, &bc.old_value);
        append_bigint(&mut self.new_value, &bc.new_value);
        self.reason
            .append_value(balance_change_reason_text(bc.reason));
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.address.finish(),
            Arc::new(self.old_value.finish()),
            Arc::new(self.new_value.finish()),
            Arc::new(self.reason.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct SystemCodeChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    address: BytesColumn,
    old_hash: BytesColumn,
    new_hash: BytesColumn,
    old_code: BytesColumn,
    new_code: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl SystemCodeChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            old_hash: BytesColumn::new(encoding),
            new_hash: BytesColumn::new(encoding),
            old_code: BytesColumn::new(encoding),
            new_code: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        call_index: Option<u32>,
        cc: &eth::CodeChange,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.call_index.append_option(call_index);
        self.ordinal.append_value(cc.ordinal);
        self.address.append_value(&cc.address);
        self.old_hash.append_value(&cc.old_hash);
        self.new_hash.append_value(&cc.new_hash);
        self.old_code.append_value(&cc.old_code);
        self.new_code.append_value(&cc.new_code);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.address.finish(),
            self.old_hash.finish(),
            self.new_hash.finish(),
            self.old_code.finish(),
            self.new_code.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct SystemStorageChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    address: BytesColumn,
    key: BytesColumn,
    old_value: BytesColumn,
    new_value: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl SystemStorageChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            key: BytesColumn::new(encoding),
            old_value: BytesColumn::new(encoding),
            new_value: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        call_index: Option<u32>,
        sc: &eth::StorageChange,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.call_index.append_option(call_index);
        self.ordinal.append_value(sc.ordinal);
        self.address.append_value(&sc.address);
        self.key.append_value(&sc.key);
        self.old_value.append_value(&sc.old_value);
        self.new_value.append_value(&sc.new_value);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.address.finish(),
            self.key.finish(),
            self.old_value.finish(),
            self.new_value.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct SystemNonceChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    address: BytesColumn,
    old_value: UInt64Builder,
    new_value: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl SystemNonceChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            old_value: UInt64Builder::new(),
            new_value: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        call_index: Option<u32>,
        nc: &eth::NonceChange,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.call_index.append_option(call_index);
        self.ordinal.append_value(nc.ordinal);
        self.address.append_value(&nc.address);
        self.old_value.append_value(nc.old_value);
        self.new_value.append_value(nc.new_value);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.address.finish(),
            Arc::new(self.old_value.finish()),
            Arc::new(self.new_value.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct SystemGasChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    old_value: UInt64Builder,
    new_value: UInt64Builder,
    reason: StringDictionaryBuilder<Int32Type>,
    fork_step: Option<StringBuilder>,
}

impl SystemGasChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            old_value: UInt64Builder::new(),
            new_value: UInt64Builder::new(),
            reason: StringDictionaryBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        call_index: Option<u32>,
        gc: &eth::GasChange,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.call_index.append_option(call_index);
        self.ordinal.append_value(gc.ordinal);
        self.old_value.append_value(gc.old_value);
        self.new_value.append_value(gc.new_value);
        self.reason.append_value(gas_change_reason_text(gc.reason));
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            Arc::new(self.old_value.finish()),
            Arc::new(self.new_value.finish()),
            Arc::new(self.reason.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct SystemAccountCreationsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    call_index: UInt32Builder,
    ordinal: UInt64Builder,
    account: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl SystemAccountCreationsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            call_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            account: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        call_index: Option<u32>,
        ac: &eth::AccountCreation,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.call_index.append_option(call_index);
        self.ordinal.append_value(ac.ordinal);
        self.account.append_value(&ac.account);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.call_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.account.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[allow(deprecated)]
    pub(crate) fn make_test_evm_block(number: u64) -> eth::Block {
        eth::Block {
            ver: 4,
            hash: vec![0xab; 32].into(),
            number,
            size: 1000,
            header: Some(eth::BlockHeader {
                parent_hash: vec![0xcd; 32].into(),
                uncle_hash: vec![].into(),
                coinbase: vec![0x01; 20].into(),
                state_root: vec![0x02; 32].into(),
                transactions_root: vec![0x03; 32].into(),
                receipt_root: vec![0x04; 32].into(),
                logs_bloom: vec![].into(),
                difficulty: Some(eth::BigInt {
                    bytes: vec![0x01].into(),
                }),
                total_difficulty: None,
                number,
                gas_limit: 30_000_000,
                gas_used: 21_000,
                timestamp: Some(prost_types::Timestamp {
                    seconds: 1700000000,
                    nanos: 0,
                }),
                extra_data: vec![].into(),
                mix_hash: vec![0x05; 32].into(),
                nonce: 0,
                hash: vec![0xab; 32].into(),
                base_fee_per_gas: Some(eth::BigInt {
                    bytes: vec![0x3B, 0x9A, 0xCA, 0x00].into(),
                }),
                withdrawals_root: vec![].into(),
                tx_dependency: None,
                blob_gas_used: None,
                excess_blob_gas: None,
                parent_beacon_root: vec![].into(),
                requests_hash: vec![].into(),
            }),
            uncles: vec![],
            transaction_traces: vec![eth::TransactionTrace {
                to: vec![0xaa; 20].into(),
                nonce: 1,
                gas_price: Some(eth::BigInt {
                    bytes: vec![0x3B, 0x9A, 0xCA, 0x00].into(),
                }),
                gas_limit: 21000,
                value: Some(eth::BigInt {
                    bytes: vec![0x01].into(),
                }),
                input: vec![].into(),
                v: vec![].into(),
                r: vec![].into(),
                s: vec![].into(),
                gas_used: 21000,
                r#type: 0,
                access_list: vec![],
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                index: 0,
                hash: vec![0xbb; 32].into(),
                from: vec![0xcc; 20].into(),
                return_data: vec![].into(),
                public_key: vec![].into(),
                begin_ordinal: 0,
                end_ordinal: 10,
                status: 1,
                receipt: Some(eth::TransactionReceipt {
                    state_root: vec![].into(),
                    cumulative_gas_used: 21000,
                    logs_bloom: vec![].into(),
                    logs: vec![eth::Log {
                        address: vec![0xdd; 20].into(),
                        topics: vec![vec![0xee; 32].into(), vec![0xff; 32].into()],
                        data: vec![1, 2, 3].into(),
                        index: 0,
                        block_index: 0,
                        ordinal: 5,
                    }],
                    blob_gas_used: None,
                    blob_gas_price: None,
                }),
                calls: vec![eth::Call {
                    index: 0,
                    parent_index: 0,
                    depth: 0,
                    call_type: 1,
                    caller: vec![0xcc; 20].into(),
                    address: vec![0xaa; 20].into(),
                    address_delegates_to: None,
                    value: Some(eth::BigInt {
                        bytes: vec![0x01].into(),
                    }),
                    gas_limit: 21000,
                    gas_consumed: 21000,
                    return_data: vec![].into(),
                    input: vec![].into(),
                    executed_code: false,
                    suicide: false,
                    keccak_preimages: Default::default(),
                    storage_changes: vec![],
                    balance_changes: vec![eth::BalanceChange {
                        address: vec![0xcc; 20].into(),
                        old_value: Some(eth::BigInt {
                            bytes: vec![0x01].into(),
                        }),
                        new_value: Some(eth::BigInt {
                            bytes: vec![0x00].into(),
                        }),
                        reason: 5,
                        ordinal: 3,
                    }],
                    nonce_changes: vec![eth::NonceChange {
                        address: vec![0xcc; 20].into(),
                        old_value: 0,
                        new_value: 1,
                        ordinal: 1,
                    }],
                    logs: vec![],
                    code_changes: vec![],
                    gas_changes: vec![eth::GasChange {
                        old_value: 21000,
                        new_value: 0,
                        reason: 12,
                        ordinal: 2,
                    }],
                    status_failed: false,
                    status_reverted: false,
                    failure_reason: String::new(),
                    state_reverted: false,
                    begin_ordinal: 0,
                    end_ordinal: 10,
                    account_creations: vec![],
                }],
                blob_gas: None,
                blob_gas_fee_cap: None,
                blob_hashes: vec![],
                set_code_authorizations: vec![],
            }],
            balance_changes: vec![],
            detail_level: 0,
            code_changes: vec![],
            system_calls: vec![],
            withdrawals: vec![],
        }
    }

    fn get_string_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        batch
            .column(
                batch
                    .schema()
                    .index_of(name)
                    .expect("field should exist in batch schema"),
            )
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("field should be utf8")
    }

    fn get_string_value(batch: &RecordBatch, name: &str, row: usize) -> String {
        let column = batch.column(
            batch
                .schema()
                .index_of(name)
                .expect("field should exist in batch schema"),
        );

        if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
            return array.value(row).to_string();
        }

        if let Some(array) = column.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
            return array
                .downcast_dict::<StringArray>()
                .expect("dictionary values should be utf8")
                .value(row)
                .to_string();
        }

        panic!("field should be utf8 or dictionary-encoded utf8");
    }

    fn assert_dictionary_utf8_column(batch: &RecordBatch, name: &str) {
        let column = batch.column(
            batch
                .schema()
                .index_of(name)
                .expect("field should exist in batch schema"),
        );

        assert_eq!(
            column.data_type(),
            &arrow::datatypes::DataType::Dictionary(
                Box::new(arrow::datatypes::DataType::Int32),
                Box::new(arrow::datatypes::DataType::Utf8),
            ),
            "{name} should use dictionary-encoded Utf8"
        );
    }

    #[test]
    fn test_base_map_and_flush() {
        let block = make_test_evm_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(false, false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["logs"].num_rows(), 1);
        assert!(!batches.contains_key("calls"));
    }

    #[test]
    fn test_extended_map_and_flush() {
        let block = make_test_evm_block(200);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(true, false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["logs"].num_rows(), 1);
        assert_eq!(batches["calls"].num_rows(), 1);
        assert_eq!(batches["balance_changes"].num_rows(), 1);
        assert_eq!(batches["nonce_changes"].num_rows(), 1);
        assert_eq!(batches["gas_changes"].num_rows(), 1);
        // System tables should be empty (no system calls in test block)
        assert_eq!(batches["system_calls"].num_rows(), 0);
        assert_eq!(batches["system_balance_changes"].num_rows(), 0);

        let blocks_batch = &batches["blocks"];
        assert_dictionary_utf8_column(blocks_batch, "detail_level");
        assert_eq!(
            get_string_value(blocks_batch, "detail_level", 0),
            "EXTENDED"
        );

        let transactions_batch = &batches["transactions"];
        assert_dictionary_utf8_column(transactions_batch, "type");
        assert_dictionary_utf8_column(transactions_batch, "status");
        assert_eq!(get_string_value(transactions_batch, "type", 0), "LEGACY");
        assert_eq!(
            get_string_value(transactions_batch, "status", 0),
            "SUCCEEDED"
        );

        let calls_batch = &batches["calls"];
        assert_dictionary_utf8_column(calls_batch, "call_type");
        assert_eq!(get_string_value(calls_batch, "call_type", 0), "CALL");

        let balance_changes_batch = &batches["balance_changes"];
        assert_dictionary_utf8_column(balance_changes_batch, "reason");
        assert_eq!(
            get_string_value(balance_changes_batch, "reason", 0),
            "TRANSFER"
        );

        let gas_changes_batch = &batches["gas_changes"];
        assert_dictionary_utf8_column(gas_changes_batch, "reason");
        assert_eq!(
            get_string_value(gas_changes_batch, "reason", 0),
            "INTRINSIC_GAS"
        );

        assert_dictionary_utf8_column(&batches["system_calls"], "call_type");
        assert_dictionary_utf8_column(&batches["system_balance_changes"], "reason");
        assert_dictionary_utf8_column(&batches["system_gas_changes"], "reason");
    }

    #[test]
    fn test_calls_call_type_round_trips_through_parquet_writer() {
        use firehose_parquet::config::{BlockMetadata, Compression, Partition};
        use firehose_parquet::writer::{read_parquet, ParquetTableWriter};
        use std::time::{SystemTime, UNIX_EPOCH};

        let block = make_test_evm_block(300);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(true, false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        let calls_batch = &batches["calls"];

        let temp_dir = std::env::temp_dir().join(format!(
            "firehose-parquet-call-type-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after unix epoch")
                .as_nanos()
        ));

        let result = (|| -> anyhow::Result<()> {
            let mut writer =
                ParquetTableWriter::new(&temp_dir, Partition::None, Compression::Snappy);
            let (path, _) = writer.write_batch(
                "calls",
                calls_batch,
                &BlockMetadata {
                    min_block_number: block.number,
                    max_block_number: block.number,
                    min_timestamp: Some(1_700_000_000),
                    max_timestamp: Some(1_700_000_000),
                },
            )?;

            let read_batches = read_parquet(&path)?;
            assert_eq!(read_batches.len(), 1);
            assert_eq!(get_string_value(&read_batches[0], "call_type", 0), "CALL");
            Ok(())
        })();

        let _ = std::fs::remove_dir_all(&temp_dir);
        result.unwrap();
    }

    #[test]
    fn test_flush_resets() {
        let block = make_test_evm_block(1);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(true, false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    #[allow(deprecated)]
    fn test_empty_block() {
        let block = eth::Block {
            ver: 4,
            hash: vec![0x00; 32].into(),
            number: 0,
            size: 0,
            header: Some(eth::BlockHeader {
                parent_hash: vec![].into(),
                uncle_hash: vec![].into(),
                coinbase: vec![].into(),
                state_root: vec![].into(),
                transactions_root: vec![].into(),
                receipt_root: vec![].into(),
                logs_bloom: vec![].into(),
                difficulty: None,
                total_difficulty: None,
                number: 0,
                gas_limit: 0,
                gas_used: 0,
                timestamp: None,
                extra_data: vec![].into(),
                mix_hash: vec![].into(),
                nonce: 0,
                hash: vec![].into(),
                base_fee_per_gas: None,
                withdrawals_root: vec![].into(),
                tx_dependency: None,
                blob_gas_used: None,
                excess_blob_gas: None,
                parent_beacon_root: vec![].into(),
                requests_hash: vec![].into(),
            }),
            uncles: vec![],
            transaction_traces: vec![],
            balance_changes: vec![],
            detail_level: 2,
            code_changes: vec![],
            system_calls: vec![],
            withdrawals: vec![],
        };
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(false, false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 0);
        assert_eq!(batches["logs"].num_rows(), 0);
    }

    #[test]
    fn test_bigint_conversion() {
        let bi = Some(eth::BigInt {
            bytes: vec![0x3B, 0x9A, 0xCA, 0x00].into(),
        });
        assert_eq!(bigint_to_string(&bi), "1000000000");
        assert_eq!(bigint_to_string(&None), "0");
        let bi = Some(eth::BigInt {
            bytes: vec![0x01].into(),
        });
        assert_eq!(bigint_to_string(&bi), "1");
        let bi = Some(eth::BigInt {
            bytes: vec![0x01, 0x00].into(),
        });
        assert_eq!(bigint_to_string(&bi), "256");
    }

    #[test]
    fn test_table_names() {
        let mapper_base = EvmBlockMapper::new(false, false, EncodeBytes::Hex, false);
        assert_eq!(mapper_base.table_names().len(), 6);
        let mapper_ext = EvmBlockMapper::new(true, false, EncodeBytes::Hex, false);
        assert_eq!(mapper_ext.table_names().len(), 20);
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_evm_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(false, true, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), Some("NEW"))
            .unwrap();

        let batches = mapper.flush().unwrap();
        let blocks_batch = &batches["blocks"];
        let last_col = blocks_batch.num_columns() - 1;
        assert_eq!(blocks_batch.schema().field(last_col).name(), "fork_step");
        let fork_col = blocks_batch
            .column(last_col)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(fork_col.value(0), "NEW");
    }

    #[test]
    fn test_encode_bytes_binary() {
        let block = make_test_evm_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(false, false, EncodeBytes::Binary, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        let hash_col = batches["blocks"].column(
            batches["blocks"]
                .schema()
                .index_of("hash")
                .expect("hash field should exist"),
        );
        assert_eq!(*hash_col.data_type(), arrow::datatypes::DataType::Binary);
    }

    #[test]
    fn test_encode_bytes_base58() {
        let block = make_test_evm_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(false, false, EncodeBytes::Base58, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        let hash_col = batches["blocks"].column(
            batches["blocks"]
                .schema()
                .index_of("hash")
                .expect("hash field should exist"),
        );
        assert_eq!(*hash_col.data_type(), arrow::datatypes::DataType::Utf8);
    }

    #[test]
    fn test_tron_base58_mixed_encoding() {
        let block = make_test_evm_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let identity = BlockIdentity {
            block_num: 100,
            block_id: "0x010203".to_string(),
            parent_num: 99,
            parent_id: "0x000102".to_string(),
            lib_num: 100,
            timestamp: 1_700_000_000,
            timestamp_nanos: 0,
            fork_step: None,
        };
        let mut mapper = EvmBlockMapper::new(false, false, EncodeBytes::TronBase58, false);
        mapper.map_block(&block_bytes, &identity, None).unwrap();

        let batches = mapper.flush().unwrap();

        let blocks = &batches["blocks"];
        assert_eq!(
            get_string_column(blocks, "block_id").value(0),
            firehose_parquet::encode::encode_id("0x010203", &EncodeBytes::TronBase58)
        );
        assert_eq!(
            get_string_column(blocks, "parent_id").value(0),
            firehose_parquet::encode::encode_id("0x000102", &EncodeBytes::TronBase58)
        );
        assert_eq!(
            get_string_column(blocks, "hash").value(0),
            firehose_parquet::encode::encode_hex_no_prefix(&[0xab; 32])
        );
        assert_eq!(
            get_string_column(blocks, "parent_hash").value(0),
            firehose_parquet::encode::encode_hex_no_prefix(&[0xcd; 32])
        );
        assert_eq!(
            get_string_column(blocks, "coinbase").value(0),
            firehose_parquet::encode::encode_tron_base58(&[0x01; 20])
        );

        let transactions = &batches["transactions"];
        assert_eq!(
            get_string_column(transactions, "hash").value(0),
            firehose_parquet::encode::encode_hex_no_prefix(&[0xbb; 32])
        );
        assert_eq!(
            get_string_column(transactions, "from").value(0),
            firehose_parquet::encode::encode_tron_base58(&[0xcc; 20])
        );
        assert_eq!(
            get_string_column(transactions, "to").value(0),
            firehose_parquet::encode::encode_tron_base58(&[0xaa; 20])
        );

        let logs = &batches["logs"];
        assert_eq!(
            get_string_column(logs, "tx_hash").value(0),
            firehose_parquet::encode::encode_hex_no_prefix(&[0xbb; 32])
        );
        assert_eq!(
            get_string_column(logs, "address").value(0),
            firehose_parquet::encode::encode_tron_base58(&[0xdd; 20])
        );
        assert_eq!(
            get_string_column(logs, "topic0").value(0),
            firehose_parquet::encode::encode_hex_no_prefix(&[0xee; 32])
        );
        assert_eq!(
            get_string_column(logs, "data").value(0),
            firehose_parquet::encode::encode_hex_no_prefix(&[1, 2, 3])
        );
    }

    // -- failed transactions (#494) --

    const SENDER: [u8; 20] = [0xcc; 20];
    const TO: [u8; 20] = [0xaa; 20];
    const AUTHORITY: [u8; 20] = [0xa1; 20];
    const DISCARDED_AUTHORITY: [u8; 20] = [0xa2; 20];

    fn balance_change(address: &[u8], reason: i32, ordinal: u64) -> eth::BalanceChange {
        eth::BalanceChange {
            address: address.to_vec().into(),
            old_value: Some(eth::BigInt {
                bytes: vec![0x02].into(),
            }),
            new_value: Some(eth::BigInt {
                bytes: vec![0x01].into(),
            }),
            reason,
            ordinal,
        }
    }

    fn nonce_change(address: &[u8], ordinal: u64) -> eth::NonceChange {
        eth::NonceChange {
            address: address.to_vec().into(),
            old_value: 0,
            new_value: 1,
            ordinal,
        }
    }

    fn code_change(address: &[u8], ordinal: u64) -> eth::CodeChange {
        eth::CodeChange {
            address: address.to_vec().into(),
            old_hash: vec![0x01; 32].into(),
            old_code: vec![].into(),
            new_hash: vec![0x02; 32].into(),
            new_code: vec![0xef, 0x01, 0x00].into(),
            ordinal,
        }
    }

    fn gas_change(ordinal: u64) -> eth::GasChange {
        eth::GasChange {
            old_value: 100,
            new_value: 50,
            reason: 1,
            ordinal,
        }
    }

    /// A reverted SET_CODE transaction whose root call and child call carry both
    /// persistent and rolled-back state changes. Ordinals identify each change.
    #[allow(deprecated)]
    fn make_failed_set_code_tx() -> eth::TransactionTrace {
        let block = make_test_evm_block(1);
        let mut tx = block.transaction_traces[0].clone();
        tx.status = eth::TransactionTraceStatus::Reverted as i32;
        tx.r#type = eth::transaction_trace::Type::TrxTypeSetCode as i32;
        tx.receipt.as_mut().unwrap().logs.clear();
        tx.set_code_authorizations = vec![
            eth::SetCodeAuthorization {
                discarded: false,
                authority: Some(AUTHORITY.to_vec().into()),
                ..Default::default()
            },
            eth::SetCodeAuthorization {
                discarded: true,
                authority: Some(DISCARDED_AUTHORITY.to_vec().into()),
                ..Default::default()
            },
        ];

        let root = &mut tx.calls[0];
        root.status_failed = true;
        root.status_reverted = true;
        root.state_reverted = true;
        root.balance_changes = vec![
            balance_change(&SENDER, 7, 101),     // GAS_BUY
            balance_change(&SENDER, 5, 102),     // TRANSFER (rolled back)
            balance_change(&TO, 18, 103),        // INCREASE_MINT
            balance_change(&SENDER, 9, 104),     // GAS_REFUND
            balance_change(&[0x01; 20], 8, 105), // REWARD_TRANSACTION_FEE
        ];
        root.nonce_changes = vec![
            nonce_change(&AUTHORITY, 202),
            nonce_change(&SENDER, 201),
            nonce_change(&DISCARDED_AUTHORITY, 203),
            nonce_change(&AUTHORITY, 204), // CREATE during execution (rolled back)
        ];
        root.code_changes = vec![
            code_change(&AUTHORITY, 301),
            code_change(&DISCARDED_AUTHORITY, 302),
        ];
        root.storage_changes = vec![eth::StorageChange {
            address: TO.to_vec().into(),
            key: vec![0x01; 32].into(),
            old_value: vec![0x00; 32].into(),
            new_value: vec![0x01; 32].into(),
            ordinal: 401,
        }];
        root.account_creations = vec![eth::AccountCreation {
            account: vec![0x0f; 20].into(),
            ordinal: 501,
        }];
        root.gas_changes = vec![gas_change(601)];

        let mut child = root.clone();
        child.index = 1;
        child.parent_index = 0;
        child.depth = 1;
        child.balance_changes = vec![balance_change(&TO, 5, 111)];
        child.nonce_changes = vec![nonce_change(&TO, 211)];
        child.code_changes = vec![code_change(&TO, 311)];
        child.storage_changes[0].ordinal = 411;
        child.account_creations[0].ordinal = 511;
        child.gas_changes = vec![gas_change(611)];
        tx.calls.push(child);
        tx
    }

    fn ordinals(batch: &RecordBatch) -> Vec<u64> {
        batch
            .column(batch.schema().index_of("ordinal").unwrap())
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("ordinal should be u64")
            .values()
            .to_vec()
    }

    #[test]
    fn test_failed_tx_keeps_gas_fee_and_mint_balance_changes_only() {
        let tx = make_failed_set_code_tx();
        let persistent = failed_transaction_persistent_changes(&tx);
        let kept: Vec<u64> = persistent
            .balance_changes
            .iter()
            .map(|bc| bc.ordinal)
            .collect();
        // TRANSFER on the root call and everything on the child call are rolled back.
        assert_eq!(kept, vec![101, 103, 104, 105]);
    }

    #[test]
    fn test_failed_tx_keeps_sender_and_accepted_authority_nonce_changes() {
        let tx = make_failed_set_code_tx();
        let persistent = failed_transaction_persistent_changes(&tx);
        let kept: Vec<u64> = persistent
            .nonce_changes
            .iter()
            .map(|nc| nc.ordinal)
            .collect();
        // Sender (earliest ordinal) and the accepted authority's first nonce
        // change, in recording order. The discarded authority, the authority's
        // later nonce change and the child call's nonce change are dropped.
        assert_eq!(kept, vec![202, 201]);
    }

    #[test]
    fn test_failed_tx_keeps_one_nonce_change_per_accepted_authorization() {
        // Seen on mainnet (block 26000004, tx 130): the same authority signs two
        // accepted authorizations, and both nonce increments persist.
        let mut tx = make_failed_set_code_tx();
        tx.set_code_authorizations.push(eth::SetCodeAuthorization {
            discarded: false,
            authority: Some(AUTHORITY.to_vec().into()),
            ..Default::default()
        });
        let persistent = failed_transaction_persistent_changes(&tx);
        let kept: Vec<u64> = persistent
            .nonce_changes
            .iter()
            .map(|nc| nc.ordinal)
            .collect();
        assert_eq!(kept, vec![202, 201, 204]);
        // Only one code change was recorded for the authority.
        let codes: Vec<u64> = persistent
            .code_changes
            .iter()
            .map(|cc| cc.ordinal)
            .collect();
        assert_eq!(codes, vec![301]);
    }

    #[test]
    fn test_failed_self_sponsored_set_code_tx_keeps_sender_and_authorization_nonces() {
        let mut tx = make_failed_set_code_tx();
        tx.set_code_authorizations = vec![eth::SetCodeAuthorization {
            discarded: false,
            authority: Some(SENDER.to_vec().into()),
            ..Default::default()
        }];
        tx.calls[0].nonce_changes = vec![
            nonce_change(&SENDER, 201), // transaction nonce
            nonce_change(&SENDER, 202), // authorization nonce
            nonce_change(&SENDER, 205), // rolled back
        ];
        let persistent = failed_transaction_persistent_changes(&tx);
        let kept: Vec<u64> = persistent
            .nonce_changes
            .iter()
            .map(|nc| nc.ordinal)
            .collect();
        assert_eq!(kept, vec![201, 202]);
    }

    #[test]
    fn test_failed_tx_keeps_accepted_authority_code_changes_only() {
        let tx = make_failed_set_code_tx();
        let persistent = failed_transaction_persistent_changes(&tx);
        let kept: Vec<u64> = persistent
            .code_changes
            .iter()
            .map(|cc| cc.ordinal)
            .collect();
        assert_eq!(kept, vec![301]);
    }

    #[test]
    fn test_failed_non_set_code_tx_keeps_only_sender_nonce_change() {
        let mut tx = make_failed_set_code_tx();
        tx.r#type = eth::transaction_trace::Type::TrxTypeDynamicFee as i32;
        tx.set_code_authorizations.clear();
        let persistent = failed_transaction_persistent_changes(&tx);
        let nonces: Vec<u64> = persistent
            .nonce_changes
            .iter()
            .map(|nc| nc.ordinal)
            .collect();
        assert_eq!(nonces, vec![201]);
        assert!(persistent.code_changes.is_empty());
    }

    #[test]
    fn test_failed_tx_earliest_nonce_change_falls_back_to_recording_order() {
        // Ordinals of reverted calls may all be zero.
        let mut tx = make_failed_set_code_tx();
        tx.set_code_authorizations.clear();
        for nc in &mut tx.calls[0].nonce_changes {
            nc.ordinal = 0;
        }
        let persistent = failed_transaction_persistent_changes(&tx);
        assert_eq!(persistent.nonce_changes.len(), 1);
        assert_eq!(persistent.nonce_changes[0].address, AUTHORITY.to_vec());
    }

    #[test]
    fn test_failed_tx_without_calls_has_no_persistent_changes() {
        let mut tx = make_failed_set_code_tx();
        tx.calls.clear();
        let persistent = failed_transaction_persistent_changes(&tx);
        assert!(persistent.balance_changes.is_empty());
        assert!(persistent.nonce_changes.is_empty());
        assert!(persistent.code_changes.is_empty());
    }

    #[test]
    fn test_failed_tx_mapped_with_persistent_state_changes_calls_and_gas_changes() {
        let mut block = make_test_evm_block(300);
        block.transaction_traces = vec![make_failed_set_code_tx()];
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(true, false, EncodeBytes::Hex, true);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();

        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(
            get_string_value(&batches["transactions"], "status", 0),
            "REVERTED"
        );
        assert_eq!(batches["logs"].num_rows(), 0);
        // Calls and gas changes are execution traces: all of them are kept.
        assert_eq!(batches["calls"].num_rows(), 2);
        assert_eq!(ordinals(&batches["gas_changes"]), vec![601, 611]);
        // State changes: only the persistent subset.
        assert_eq!(
            ordinals(&batches["balance_changes"]),
            vec![101, 103, 104, 105]
        );
        assert_eq!(ordinals(&batches["nonce_changes"]), vec![202, 201]);
        assert_eq!(ordinals(&batches["code_changes"]), vec![301]);
        assert_eq!(batches["storage_changes"].num_rows(), 0);
        assert_eq!(batches["account_creations"].num_rows(), 0);
    }

    #[test]
    fn test_failed_tx_dropped_when_excluded() {
        let mut block = make_test_evm_block(301);
        block.transaction_traces = vec![make_failed_set_code_tx()];
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(true, false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();

        assert_eq!(batches["blocks"].num_rows(), 1);
        for table in [
            "transactions",
            "logs",
            "calls",
            "balance_changes",
            "nonce_changes",
            "code_changes",
            "storage_changes",
            "gas_changes",
            "account_creations",
        ] {
            assert_eq!(batches[table].num_rows(), 0, "{table} should be empty");
        }
    }

    #[test]
    fn test_successful_tx_keeps_all_state_changes() {
        let mut block = make_test_evm_block(302);
        let mut tx = make_failed_set_code_tx();
        tx.status = eth::TransactionTraceStatus::Succeeded as i32;
        block.transaction_traces = vec![tx];
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(true, false, EncodeBytes::Hex, true);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();

        assert_eq!(batches["balance_changes"].num_rows(), 6);
        assert_eq!(batches["nonce_changes"].num_rows(), 5);
        assert_eq!(batches["code_changes"].num_rows(), 3);
        assert_eq!(batches["storage_changes"].num_rows(), 2);
        assert_eq!(batches["account_creations"].num_rows(), 2);
        assert_eq!(batches["gas_changes"].num_rows(), 2);
    }

    // -- change-table call context (#495) --

    fn u32_values(batch: &RecordBatch, name: &str) -> Vec<Option<u32>> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("column should be u32")
            .iter()
            .collect()
    }

    fn bool_values(batch: &RecordBatch, name: &str) -> Vec<bool> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("column should be boolean")
            .iter()
            .map(|value| value.expect("column should not be null"))
            .collect()
    }

    fn map_extended(block: &eth::Block) -> HashMap<String, RecordBatch> {
        let block_bytes = prost::Message::encode_to_vec(block);
        let mut mapper = EvmBlockMapper::new(true, false, EncodeBytes::Hex, true);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        mapper.flush().unwrap()
    }

    /// A successful transaction at index 7 whose root call (index 1) succeeds
    /// and whose child call (index 2) is reverted. Every change table has one
    /// row per call.
    #[allow(deprecated)]
    fn make_successful_tx_with_reverted_child() -> eth::TransactionTrace {
        let mut tx = make_failed_set_code_tx();
        tx.status = eth::TransactionTraceStatus::Succeeded as i32;
        tx.r#type = eth::transaction_trace::Type::TrxTypeDynamicFee as i32;
        tx.set_code_authorizations.clear();
        tx.index = 7;
        let root = &mut tx.calls[0];
        root.index = 1;
        root.status_failed = false;
        root.status_reverted = false;
        root.state_reverted = false;
        root.balance_changes.truncate(1);
        root.nonce_changes.truncate(1);
        root.code_changes.truncate(1);
        let child = &mut tx.calls[1];
        child.index = 2;
        child.parent_index = 1;
        child.status_failed = true;
        child.status_reverted = true;
        child.state_reverted = true;
        tx
    }

    #[test]
    fn test_change_tables_carry_call_context_for_successful_tx() {
        let mut block = make_test_evm_block(400);
        block.transaction_traces = vec![make_successful_tx_with_reverted_child()];
        let batches = map_extended(&block);

        for table in [
            "balance_changes",
            "nonce_changes",
            "code_changes",
            "storage_changes",
            "account_creations",
        ] {
            let batch = &batches[table];
            assert_eq!(batch.num_rows(), 2, "{table}");
            assert_eq!(
                u32_values(batch, "tx_index"),
                vec![Some(7), Some(7)],
                "{table}"
            );
            assert_eq!(
                u32_values(batch, "call_index"),
                vec![Some(1), Some(2)],
                "{table}"
            );
            assert_eq!(
                bool_values(batch, "state_reverted"),
                vec![false, true],
                "{table}"
            );
            assert_eq!(
                bool_values(batch, "persisted"),
                vec![true, false],
                "{table}"
            );
        }
        let gas = &batches["gas_changes"];
        assert_eq!(u32_values(gas, "tx_index"), vec![Some(7), Some(7)]);
        assert_eq!(u32_values(gas, "call_index"), vec![Some(1), Some(2)]);
        assert_eq!(bool_values(gas, "state_reverted"), vec![false, true]);
        assert!(gas.schema().index_of("persisted").is_err());
    }

    #[test]
    fn test_failed_tx_persistent_changes_are_persisted_despite_reverted_root_call() {
        let mut block = make_test_evm_block(401);
        let mut tx = make_failed_set_code_tx();
        tx.index = 3;
        tx.calls[0].index = 1;
        tx.calls[1].index = 2;
        block.transaction_traces = vec![tx];
        let batches = map_extended(&block);

        for table in ["balance_changes", "nonce_changes", "code_changes"] {
            let batch = &batches[table];
            assert!(batch.num_rows() > 0, "{table}");
            assert!(u32_values(batch, "tx_index").iter().all(|v| *v == Some(3)));
            // Persistent changes come from the root call, which reverted.
            assert!(u32_values(batch, "call_index")
                .iter()
                .all(|v| *v == Some(1)));
            assert!(bool_values(batch, "state_reverted").iter().all(|v| *v));
            assert!(bool_values(batch, "persisted").iter().all(|v| *v));
        }
        let gas = &batches["gas_changes"];
        assert_eq!(u32_values(gas, "call_index"), vec![Some(1), Some(2)]);
        assert_eq!(bool_values(gas, "state_reverted"), vec![true, true]);
    }

    #[test]
    #[allow(deprecated)]
    fn test_system_change_tables_carry_system_call_index() {
        let mut block = make_test_evm_block(402);
        block.transaction_traces.clear();
        block.balance_changes = vec![balance_change(&SENDER, 16, 1)]; // WITHDRAWAL
        block.code_changes = vec![code_change(&TO, 2)];
        let mut system_call = make_failed_set_code_tx().calls.remove(0);
        system_call.index = 5;
        system_call.status_failed = false;
        system_call.status_reverted = false;
        system_call.state_reverted = false;
        block.system_calls = vec![system_call];
        let batches = map_extended(&block);

        assert_eq!(
            u32_values(&batches["system_calls"], "call_index"),
            vec![Some(5)]
        );
        // Block-level changes have no call; system call changes carry its index.
        let balance = &batches["system_balance_changes"];
        let mut expected = vec![None];
        expected.extend(std::iter::repeat_n(Some(5), balance.num_rows() - 1));
        assert_eq!(u32_values(balance, "call_index"), expected);
        assert_eq!(
            u32_values(&batches["system_code_changes"], "call_index"),
            vec![None, Some(5), Some(5)]
        );
        for table in [
            "system_storage_changes",
            "system_nonce_changes",
            "system_gas_changes",
            "system_account_creations",
        ] {
            let values = u32_values(&batches[table], "call_index");
            assert!(!values.is_empty(), "{table}");
            assert!(values.iter().all(|v| *v == Some(5)), "{table}");
        }
    }

    #[test]
    fn test_change_table_schemas_have_call_context_columns() {
        let enc = EncodeBytes::Hex;
        for (name, schema, persisted) in [
            (
                "balance_changes",
                schema::balance_changes_schema(false, &enc),
                true,
            ),
            (
                "code_changes",
                schema::code_changes_schema(false, &enc),
                true,
            ),
            (
                "storage_changes",
                schema::storage_changes_schema(false, &enc),
                true,
            ),
            (
                "nonce_changes",
                schema::nonce_changes_schema(false, &enc),
                true,
            ),
            (
                "account_creations",
                schema::account_creations_schema(false, &enc),
                true,
            ),
            (
                "gas_changes",
                schema::gas_changes_schema(false, &enc),
                false,
            ),
        ] {
            let tx_hash = schema.index_of("tx_hash").unwrap();
            assert_eq!(schema.index_of("tx_index").unwrap(), tx_hash + 1, "{name}");
            assert_eq!(
                schema.index_of("call_index").unwrap(),
                tx_hash + 2,
                "{name}"
            );
            for column in ["tx_index", "call_index", "state_reverted"] {
                assert!(
                    !schema.field_with_name(column).unwrap().is_nullable(),
                    "{name}.{column}"
                );
            }
            assert_eq!(schema.index_of("persisted").is_ok(), persisted, "{name}");
        }
        for (name, schema) in [
            (
                "system_balance_changes",
                schema::system_balance_changes_schema(false, &enc),
            ),
            (
                "system_code_changes",
                schema::system_code_changes_schema(false, &enc),
            ),
            (
                "system_storage_changes",
                schema::system_storage_changes_schema(false, &enc),
            ),
            (
                "system_nonce_changes",
                schema::system_nonce_changes_schema(false, &enc),
            ),
            (
                "system_gas_changes",
                schema::system_gas_changes_schema(false, &enc),
            ),
            (
                "system_account_creations",
                schema::system_account_creations_schema(false, &enc),
            ),
        ] {
            let field = schema.field_with_name("call_index").unwrap();
            assert!(field.is_nullable(), "{name}.call_index");
            assert_eq!(
                schema.index_of("call_index").unwrap(),
                schema.index_of("block_number").unwrap() + 1,
                "{name}"
            );
        }
    }

    // -- missing Firehose fields (#496) --

    fn hex(bytes: &[u8]) -> String {
        firehose_parquet::encode::encode_hex(bytes)
    }

    fn string_values(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
        let column = batch.column(batch.schema().index_of(name).unwrap());
        let array = column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column should be utf8");
        array.iter().map(|v| v.map(str::to_string)).collect()
    }

    fn u64_values(batch: &RecordBatch, name: &str) -> Vec<Option<u64>> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("column should be u64")
            .iter()
            .collect()
    }

    fn list_values(batch: &RecordBatch, name: &str, row: usize) -> Vec<String> {
        let list = batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("column should be a list");
        let values = list.value(row);
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        values.iter().map(|v| v.unwrap().to_string()).collect()
    }

    /// A post-Prague block with a blob transaction whose root call delegates
    /// (EIP-7702) and whose child call failed.
    fn make_post_prague_block() -> eth::Block {
        let mut block = make_test_evm_block(500);
        let header = block.header.as_mut().unwrap();
        header.uncle_hash = vec![0x1d; 32].into();
        header.logs_bloom = vec![0x0b; 256].into();
        header.withdrawals_root = vec![0x0c; 32].into();
        header.blob_gas_used = Some(393_216);
        header.excess_blob_gas = Some(0);
        header.parent_beacon_root = vec![0x0d; 32].into();
        header.requests_hash = vec![0x0e; 32].into();

        let tx = &mut block.transaction_traces[0];
        tx.r#type = eth::transaction_trace::Type::TrxTypeBlob as i32;
        tx.v = vec![0x01].into();
        tx.r = vec![0x02; 32].into();
        tx.s = vec![0x03; 32].into();
        tx.return_data = vec![0x04, 0x05].into();
        tx.blob_gas = Some(262_144);
        tx.blob_gas_fee_cap = Some(eth::BigInt {
            bytes: vec![0x01, 0x00].into(),
        });
        tx.blob_hashes = vec![vec![0x01; 32].into(), vec![0x02; 32].into()];
        tx.begin_ordinal = 11;
        tx.end_ordinal = 99;
        let receipt = tx.receipt.as_mut().unwrap();
        receipt.logs_bloom = vec![0x0f; 256].into();
        receipt.blob_gas_used = Some(262_144);
        receipt.blob_gas_price = Some(eth::BigInt {
            bytes: vec![0x07].into(),
        });
        receipt.logs[0].ordinal = 42;

        let root = &mut tx.calls[0];
        root.address_delegates_to = Some(vec![0xde; 20].into());
        root.begin_ordinal = 12;
        root.end_ordinal = 98;
        let mut child = root.clone();
        child.index = 2;
        child.address_delegates_to = None;
        child.status_failed = true;
        child.status_reverted = true;
        child.state_reverted = true;
        child.failure_reason = "execution reverted".to_string();
        child.begin_ordinal = 20;
        child.end_ordinal = 30;
        let mut system_call = root.clone();
        tx.calls.push(child);

        system_call.index = 1;
        system_call.failure_reason = "out of gas".to_string();
        system_call.address_delegates_to = None;
        system_call.begin_ordinal = 1;
        system_call.end_ordinal = 2;
        block.system_calls = vec![system_call];
        block
    }

    #[test]
    fn test_blocks_header_fields_written() {
        let batches = map_extended(&make_post_prague_block());
        let blocks = &batches["blocks"];
        assert_eq!(get_string_value(blocks, "uncle_hash", 0), hex(&[0x1d; 32]));
        assert_eq!(get_string_value(blocks, "logs_bloom", 0), hex(&[0x0b; 256]));
        assert_eq!(
            string_values(blocks, "withdrawals_root"),
            vec![Some(hex(&[0x0c; 32]))]
        );
        assert_eq!(u64_values(blocks, "blob_gas_used"), vec![Some(393_216)]);
        assert_eq!(u64_values(blocks, "excess_blob_gas"), vec![Some(0)]);
        assert_eq!(
            string_values(blocks, "parent_beacon_root"),
            vec![Some(hex(&[0x0d; 32]))]
        );
        assert_eq!(
            string_values(blocks, "requests_hash"),
            vec![Some(hex(&[0x0e; 32]))]
        );
    }

    #[test]
    fn test_blocks_pre_fork_header_fields_are_null() {
        // make_test_evm_block has no withdrawals, blob, beacon or requests fields.
        let batches = map_extended(&make_test_evm_block(501));
        let blocks = &batches["blocks"];
        for column in ["withdrawals_root", "parent_beacon_root", "requests_hash"] {
            assert_eq!(string_values(blocks, column), vec![None], "{column}");
        }
        assert_eq!(u64_values(blocks, "blob_gas_used"), vec![None]);
        assert_eq!(u64_values(blocks, "excess_blob_gas"), vec![None]);
    }

    #[test]
    fn test_transactions_signature_blob_and_ordinal_fields_written() {
        let batches = map_extended(&make_post_prague_block());
        let txs = &batches["transactions"];
        assert_eq!(get_string_value(txs, "type", 0), "BLOB");
        assert_eq!(get_string_value(txs, "v", 0), hex(&[0x01]));
        assert_eq!(get_string_value(txs, "r", 0), hex(&[0x02; 32]));
        assert_eq!(get_string_value(txs, "s", 0), hex(&[0x03; 32]));
        assert_eq!(get_string_value(txs, "return_data", 0), hex(&[0x04, 0x05]));
        assert_eq!(
            string_values(txs, "logs_bloom"),
            vec![Some(hex(&[0x0f; 256]))]
        );
        assert_eq!(u64_values(txs, "blob_gas"), vec![Some(262_144)]);
        assert_eq!(
            string_values(txs, "blob_gas_fee_cap"),
            vec![Some("256".to_string())]
        );
        assert_eq!(
            list_values(txs, "blob_hashes", 0),
            vec![hex(&[0x01; 32]), hex(&[0x02; 32])]
        );
        assert_eq!(u64_values(txs, "blob_gas_used"), vec![Some(262_144)]);
        assert_eq!(
            string_values(txs, "blob_gas_price"),
            vec![Some("7".to_string())]
        );
        assert_eq!(u64_values(txs, "begin_ordinal"), vec![Some(11)]);
        assert_eq!(u64_values(txs, "end_ordinal"), vec![Some(99)]);
    }

    #[test]
    fn test_transactions_non_blob_and_receipt_less_fields() {
        let mut block = make_test_evm_block(502);
        block.transaction_traces[0].receipt = None;
        let batches = map_extended(&block);
        let txs = &batches["transactions"];
        assert_eq!(string_values(txs, "logs_bloom"), vec![None]);
        assert_eq!(u64_values(txs, "blob_gas"), vec![None]);
        assert_eq!(string_values(txs, "blob_gas_fee_cap"), vec![None]);
        assert!(list_values(txs, "blob_hashes", 0).is_empty());
        assert_eq!(u64_values(txs, "blob_gas_used"), vec![None]);
        assert_eq!(string_values(txs, "blob_gas_price"), vec![None]);
    }

    #[test]
    fn test_calls_failure_reason_delegation_and_ordinals_written() {
        let batches = map_extended(&make_post_prague_block());
        let calls = &batches["calls"];
        assert_eq!(
            string_values(calls, "failure_reason"),
            vec![None, Some("execution reverted".to_string())]
        );
        assert_eq!(
            string_values(calls, "address_delegates_to"),
            vec![Some(hex(&[0xde; 20])), None]
        );
        assert_eq!(u64_values(calls, "begin_ordinal"), vec![Some(12), Some(20)]);
        assert_eq!(u64_values(calls, "end_ordinal"), vec![Some(98), Some(30)]);

        let system_calls = &batches["system_calls"];
        assert_eq!(
            string_values(system_calls, "failure_reason"),
            vec![Some("out of gas".to_string())]
        );
        assert_eq!(
            string_values(system_calls, "address_delegates_to"),
            vec![None]
        );
        assert_eq!(u64_values(system_calls, "begin_ordinal"), vec![Some(1)]);
        assert_eq!(u64_values(system_calls, "end_ordinal"), vec![Some(2)]);
    }

    #[test]
    fn test_logs_ordinal_written() {
        let batches = map_extended(&make_post_prague_block());
        assert_eq!(u64_values(&batches["logs"], "ordinal"), vec![Some(42)]);
    }

    #[test]
    fn test_blob_hashes_binary_encoding_round_trips_through_parquet() {
        use firehose_parquet::config::{BlockMetadata, Compression, Partition};
        use firehose_parquet::writer::{read_parquet, ParquetTableWriter};

        let block = make_post_prague_block();
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(true, false, EncodeBytes::Binary, true);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        let schema = batches["transactions"].schema();
        let temp_dir = std::env::temp_dir().join(format!(
            "firehose-parquet-blob-hashes-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let result = (|| -> anyhow::Result<Vec<RecordBatch>> {
            let mut writer =
                ParquetTableWriter::new(&temp_dir, Partition::None, Compression::Snappy);
            let (path, _) = writer.write_batch(
                "transactions",
                &batches["transactions"],
                &BlockMetadata {
                    min_block_number: 500,
                    max_block_number: 500,
                    min_timestamp: Some(1_700_000_000),
                    max_timestamp: Some(1_700_000_000),
                },
            )?;
            read_parquet(&path)
        })();
        let _ = std::fs::remove_dir_all(&temp_dir);
        let read = result.unwrap();
        assert_eq!(read[0].schema(), schema);
        let list = read[0]
            .column(schema.index_of("blob_hashes").unwrap())
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let hashes = list.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(hashes.value(0), [0x01; 32].as_slice());
        assert_eq!(hashes.value(1), [0x02; 32].as_slice());
    }

    // -- withdrawals, access_lists and set_code_authorizations (#497) --

    pub(crate) fn make_test_set_code_authorization() -> eth::SetCodeAuthorization {
        eth::SetCodeAuthorization {
            discarded: false,
            chain_id: vec![0x01].into(),
            address: vec![0xde; 20].into(),
            nonce: 7,
            v: 1,
            r: vec![0x0a; 32].into(),
            s: vec![0x0b; 32].into(),
            authority: Some(vec![0xa1; 20].into()),
        }
    }

    fn make_block_with_new_tables() -> eth::Block {
        let mut block = make_test_evm_block(600);
        block.withdrawals = vec![
            eth::Withdrawal {
                index: 100,
                validator_index: 200,
                address: vec![0x11; 20].into(),
                amount: 18_000_000,
            },
            eth::Withdrawal {
                index: 101,
                validator_index: 201,
                address: vec![0x12; 20].into(),
                amount: 0,
            },
        ];
        let tx = &mut block.transaction_traces[0];
        tx.index = 4;
        tx.access_list = vec![
            eth::AccessTuple {
                address: vec![0x21; 20].into(),
                storage_keys: vec![vec![0x01; 32].into(), vec![0x02; 32].into()],
            },
            eth::AccessTuple {
                address: vec![0x22; 20].into(),
                storage_keys: vec![],
            },
        ];
        let mut discarded = make_test_set_code_authorization();
        discarded.discarded = true;
        discarded.authority = None;
        discarded.address = vec![].into();
        discarded.chain_id = vec![].into();
        tx.set_code_authorizations = vec![make_test_set_code_authorization(), discarded];
        block
    }

    #[test]
    fn test_new_tables_are_base_tables() {
        let block_bytes = prost::Message::encode_to_vec(&make_block_with_new_tables());
        for extended in [false, true] {
            let mut mapper = EvmBlockMapper::new(extended, false, EncodeBytes::Hex, true);
            mapper
                .map_block(&block_bytes, &BlockIdentity::default(), None)
                .unwrap();
            let names: Vec<String> = mapper.table_names().iter().map(|n| n.to_string()).collect();
            let batches = mapper.flush().unwrap();
            for table in ["withdrawals", "access_lists", "set_code_authorizations"] {
                assert!(
                    names.iter().any(|n| n == table),
                    "{table} extended={extended}"
                );
                assert!(batches[table].num_rows() > 0, "{table} extended={extended}");
            }
        }
    }

    #[test]
    fn test_withdrawals_rows() {
        let batches = map_extended(&make_block_with_new_tables());
        let w = &batches["withdrawals"];
        assert_eq!(u64_values(w, "block_number"), vec![Some(600), Some(600)]);
        assert_eq!(u64_values(w, "index"), vec![Some(100), Some(101)]);
        assert_eq!(u64_values(w, "validator_index"), vec![Some(200), Some(201)]);
        assert_eq!(get_string_value(w, "address", 0), hex(&[0x11; 20]));
        assert_eq!(
            u64_values(w, "amount_gwei"),
            vec![Some(18_000_000), Some(0)]
        );
    }

    #[test]
    fn test_access_lists_rows() {
        let batches = map_extended(&make_block_with_new_tables());
        let a = &batches["access_lists"];
        assert_eq!(a.num_rows(), 2);
        assert_eq!(get_string_value(a, "tx_hash", 0), hex(&[0xbb; 32]));
        assert_eq!(u32_values(a, "tx_index"), vec![Some(4), Some(4)]);
        assert_eq!(u32_values(a, "access_index"), vec![Some(0), Some(1)]);
        assert_eq!(get_string_value(a, "address", 1), hex(&[0x22; 20]));
        assert_eq!(
            list_values(a, "storage_keys", 0),
            vec![hex(&[0x01; 32]), hex(&[0x02; 32])]
        );
        assert!(list_values(a, "storage_keys", 1).is_empty());
    }

    #[test]
    fn test_set_code_authorizations_rows() {
        let batches = map_extended(&make_block_with_new_tables());
        let s = &batches["set_code_authorizations"];
        assert_eq!(s.num_rows(), 2);
        assert_eq!(u32_values(s, "tx_index"), vec![Some(4), Some(4)]);
        assert_eq!(u32_values(s, "authorization_index"), vec![Some(0), Some(1)]);
        assert_eq!(
            string_values(s, "chain_id"),
            vec![Some("1".to_string()), Some("0".to_string())]
        );
        assert_eq!(
            string_values(s, "address"),
            vec![Some(hex(&[0xde; 20])), None]
        );
        assert_eq!(u64_values(s, "nonce"), vec![Some(7), Some(7)]);
        assert_eq!(u32_values(s, "v"), vec![Some(1), Some(1)]);
        assert_eq!(get_string_value(s, "r", 0), hex(&[0x0a; 32]));
        assert_eq!(get_string_value(s, "s", 0), hex(&[0x0b; 32]));
        assert_eq!(
            string_values(s, "authority"),
            vec![Some(hex(&[0xa1; 20])), None]
        );
        assert_eq!(bool_values(s, "discarded"), vec![false, true]);
    }

    #[test]
    fn test_failed_tx_access_list_and_authorizations_written() {
        let mut block = make_block_with_new_tables();
        block.transaction_traces[0].status = eth::TransactionTraceStatus::Reverted as i32;
        let batches = map_extended(&block);
        assert_eq!(batches["access_lists"].num_rows(), 2);
        assert_eq!(batches["set_code_authorizations"].num_rows(), 2);

        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(true, false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["access_lists"].num_rows(), 0);
        assert_eq!(batches["set_code_authorizations"].num_rows(), 0);
        assert_eq!(batches["withdrawals"].num_rows(), 2);
    }
}
