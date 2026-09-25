use super::proto::near;
use super::schema;
use arrow::array::*;
use arrow::datatypes::{Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, BytesListColumn, EncodeBytes, EncodedBytes};
use firehose_parquet::traits::{
    est_bin, est_opt_str, est_str, est_u32, est_u64, BlockIdentity, BlockMapper, CanonicalBuilder,
    PreparedIdentity,
};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

fn estimated_dictionary_index_bytes(len: usize) -> usize {
    // Largest-table tracking only needs a cheap relative estimate. Enum-like
    // dictionary columns have a small fixed set of values, so the per-row
    // indices dominate.
    len * std::mem::size_of::<i32>()
}

fn append_opt_encoded(column: &mut BytesColumn, value: Option<&EncodedBytes>) {
    match value {
        Some(value) => column.append_encoded(value),
        None => column.append_null(),
    }
}

fn append_receipt_ids(column: &mut BytesListColumn, receipt_ids: &[near::CryptoHash]) {
    for receipt_id in receipt_ids {
        column.append_value(&receipt_id.bytes);
    }
    column.append(true);
}

fn append_fork_step(builder: &mut Option<StringBuilder>, fork_step: Option<&str>) {
    if let Some(ref mut b) = builder {
        b.append_value(fork_step.unwrap_or("UNKNOWN"));
    }
}

fn finish_fork_step(builder: &mut Option<StringBuilder>, columns: &mut Vec<Arc<dyn Array>>) {
    if let Some(ref mut b) = builder {
        columns.push(Arc::new(b.finish()) as Arc<dyn Array>);
    }
}

fn mk_fork_step(include: bool) -> Option<StringBuilder> {
    if include {
        Some(StringBuilder::new())
    } else {
        None
    }
}

/// Extract bytes from a CryptoHash option, returning empty slice for None.
fn crypto_hash_bytes(hash: &Option<near::CryptoHash>) -> &[u8] {
    match hash {
        Some(h) => &h.bytes,
        None => &[],
    }
}

/// Convert a BigInt (big-endian two's complement) to a decimal string.
fn bigint_to_string(bi: &Option<near::BigInt>) -> String {
    match bi {
        Some(b) if !b.bytes.is_empty() => {
            // BigInt bytes are big-endian, unsigned for NEAR gas_price / total_supply
            let result = num_to_decimal(&b.bytes);
            if result.is_empty() {
                "0".to_string()
            } else {
                result
            }
        }
        _ => "0".to_string(),
    }
}

/// Simple big-endian unsigned bytes to decimal string conversion.
fn num_to_decimal(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "0".to_string();
    }
    let mut result = vec![0u8]; // decimal digits
    for &byte in bytes {
        // Multiply result by 256
        let mut carry: u16 = 0;
        for digit in result.iter_mut().rev() {
            let val = (*digit as u16) * 256 + carry;
            *digit = (val % 10) as u8;
            carry = val / 10;
        }
        while carry > 0 {
            result.insert(0, (carry % 10) as u8);
            carry /= 10;
        }
        // Add byte value
        let mut carry: u16 = byte as u16;
        for digit in result.iter_mut().rev() {
            let val = (*digit as u16) + carry;
            *digit = (val % 10) as u8;
            carry = val / 10;
        }
        while carry > 0 {
            result.insert(0, (carry % 10) as u8);
            carry /= 10;
        }
    }
    result.iter().map(|d| (b'0' + d) as char).collect()
}

/// Get the action type name from an Action enum variant.
fn action_type_name(action: &near::Action) -> &'static str {
    match &action.action {
        Some(near::action::Action::CreateAccount(_)) => "CreateAccount",
        Some(near::action::Action::DeployContract(_)) => "DeployContract",
        Some(near::action::Action::FunctionCall(_)) => "FunctionCall",
        Some(near::action::Action::Transfer(_)) => "Transfer",
        Some(near::action::Action::Stake(_)) => "Stake",
        Some(near::action::Action::AddKey(_)) => "AddKey",
        Some(near::action::Action::DeleteKey(_)) => "DeleteKey",
        Some(near::action::Action::DeleteAccount(_)) => "DeleteAccount",
        Some(near::action::Action::Delegate(_)) => "Delegate",
        None => "Unknown",
    }
}

/// The execution outcome of a transaction (its conversion into a receipt).
fn transaction_outcome(
    tx_with_outcome: &near::IndexerTransactionWithOutcome,
) -> Option<&near::ExecutionOutcome> {
    tx_with_outcome
        .outcome
        .as_ref()
        .and_then(|o| o.execution_outcome.as_ref())
        .and_then(|eo| eo.outcome.as_ref())
}

/// Originating transaction of the receipts executed in one block.
///
/// A transaction's outcome lists the receipt it was converted into. NEAR
/// executes that receipt in the same block when the transaction's signer is
/// also its receiver (a local receipt), so its origin is known from the block
/// alone. Every other receipt runs in a later block than the transaction or
/// receipt that created it and gets no `tx_hash`. The map is rebuilt for every
/// block, so the output never depends on where a run started. Across blocks,
/// follow `transactions.converted_into_receipt_id` and `receipts.receipt_ids`.
struct ReceiptOrigins<'a> {
    tx_hash_by_receipt_id: HashMap<&'a [u8], &'a [u8]>,
}

impl<'a> ReceiptOrigins<'a> {
    fn new(block: &'a near::Block) -> Self {
        let mut tx_hash_by_receipt_id = HashMap::new();
        let transactions = block
            .shards
            .iter()
            .filter_map(|shard| shard.chunk.as_ref())
            .flat_map(|chunk| &chunk.transactions);
        for tx_with_outcome in transactions {
            let Some(tx) = &tx_with_outcome.transaction else {
                continue;
            };
            let tx_hash = crypto_hash_bytes(&tx.hash);
            if tx_hash.is_empty() {
                continue;
            }
            let Some(outcome) = transaction_outcome(tx_with_outcome) else {
                continue;
            };
            for receipt_id in &outcome.receipt_ids {
                if !receipt_id.bytes.is_empty() {
                    tx_hash_by_receipt_id.insert(receipt_id.bytes.as_ref(), tx_hash);
                }
            }
        }
        Self {
            tx_hash_by_receipt_id,
        }
    }

    fn tx_hash(&self, receipt_id: &[u8]) -> Option<&'a [u8]> {
        self.tx_hash_by_receipt_id.get(receipt_id).copied()
    }
}

/// The values that one executed receipt repeats on each of its
/// `receipt_actions` and `execution_logs` rows, with the ids encoded once.
struct ReceiptRowContext<'a> {
    receipt_id: EncodedBytes,
    receipt_index: u32,
    tx_hash: Option<EncodedBytes>,
    shard_id: u64,
    predecessor_id: &'a str,
    receiver_id: &'a str,
}

/// Get execution status string from an ExecutionOutcome.
fn execution_status_str(outcome: &near::ExecutionOutcome) -> &'static str {
    match &outcome.status {
        Some(near::execution_outcome::Status::SuccessValue(_)) => "SuccessValue",
        Some(near::execution_outcome::Status::SuccessReceiptId(_)) => "SuccessReceiptId",
        Some(near::execution_outcome::Status::Failure(_)) => "Failure",
        Some(near::execution_outcome::Status::Unknown(_)) => "Unknown",
        None => "Unknown",
    }
}

/// Get the state change type name from a StateChangeValue.
fn state_change_type_name(value: &near::StateChangeValue) -> &'static str {
    match &value.value {
        Some(near::state_change_value::Value::AccountUpdate(_)) => "AccountUpdate",
        Some(near::state_change_value::Value::AccountDeletion(_)) => "AccountDeletion",
        Some(near::state_change_value::Value::AccessKeyUpdate(_)) => "AccessKeyUpdate",
        Some(near::state_change_value::Value::AccessKeyDeletion(_)) => "AccessKeyDeletion",
        Some(near::state_change_value::Value::DataUpdate(_)) => "DataUpdate",
        Some(near::state_change_value::Value::DataDeletion(_)) => "DataDeletion",
        Some(near::state_change_value::Value::ContractCodeUpdate(_)) => "ContractCodeUpdate",
        Some(near::state_change_value::Value::ContractDeletion(_)) => "ContractCodeDeletion",
        None => "Unknown",
    }
}

/// Get the state change cause name.
fn state_change_cause_name(cause: &near::StateChangeCause) -> &'static str {
    match &cause.cause {
        Some(near::state_change_cause::Cause::NotWritableToDisk(_)) => "NotWritableToDisk",
        Some(near::state_change_cause::Cause::InitialState(_)) => "InitialState",
        Some(near::state_change_cause::Cause::TransactionProcessing(_)) => "TransactionProcessing",
        Some(near::state_change_cause::Cause::ActionReceiptProcessingStarted(_)) => {
            "ActionReceiptProcessingStarted"
        }
        Some(near::state_change_cause::Cause::ActionReceiptGasReward(_)) => {
            "ActionReceiptGasReward"
        }
        Some(near::state_change_cause::Cause::ReceiptProcessing(_)) => "ReceiptProcessing",
        Some(near::state_change_cause::Cause::PostponedReceipt(_)) => "PostponedReceipt",
        Some(near::state_change_cause::Cause::UpdatedDelayedReceipts(_)) => {
            "UpdatedDelayedReceipts"
        }
        Some(near::state_change_cause::Cause::ValidatorAccountsUpdate(_)) => {
            "ValidatorAccountsUpdate"
        }
        Some(near::state_change_cause::Cause::Migration(_)) => "Migration",
        None => "Unknown",
    }
}

/// Extract account_id from a state change value.
fn state_change_account_id(value: &near::StateChangeValue) -> &str {
    match &value.value {
        Some(near::state_change_value::Value::AccountUpdate(v)) => &v.account_id,
        Some(near::state_change_value::Value::AccountDeletion(v)) => &v.account_id,
        Some(near::state_change_value::Value::AccessKeyUpdate(v)) => &v.account_id,
        Some(near::state_change_value::Value::AccessKeyDeletion(v)) => &v.account_id,
        Some(near::state_change_value::Value::DataUpdate(v)) => &v.account_id,
        Some(near::state_change_value::Value::DataDeletion(v)) => &v.account_id,
        Some(near::state_change_value::Value::ContractCodeUpdate(v)) => &v.account_id,
        Some(near::state_change_value::Value::ContractDeletion(v)) => &v.account_id,
        None => "",
    }
}

/// Extract key bytes from a state change value (data changes only), base64 encoded.
fn state_change_key_base64(value: &near::StateChangeValue) -> String {
    use base64_encode;
    match &value.value {
        Some(near::state_change_value::Value::DataUpdate(v)) => base64_encode(&v.key),
        Some(near::state_change_value::Value::DataDeletion(v)) => base64_encode(&v.key),
        _ => String::new(),
    }
}

/// Extract value bytes from a state change value (data updates only), base64 encoded.
fn state_change_value_base64(value: &near::StateChangeValue) -> String {
    use base64_encode;
    match &value.value {
        Some(near::state_change_value::Value::DataUpdate(v)) => base64_encode(&v.value),
        _ => String::new(),
    }
}

/// Simple base64 encoding (standard alphabet, with padding).
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

// ---------------------------------------------------------------------------
// NEAR BlockMapper
// ---------------------------------------------------------------------------

pub struct NearBlockMapper {
    include_failed_transactions: bool,
    blocks: BlocksBuilder,
    chunks: ChunksBuilder,
    transactions: TransactionsBuilder,
    receipts: ReceiptsBuilder,
    receipt_actions: ReceiptActionsBuilder,
    execution_logs: ExecutionLogsBuilder,
    state_changes: StateChangesBuilder,
    blocks_schema: Schema,
    chunks_schema: Schema,
    transactions_schema: Schema,
    receipts_schema: Schema,
    receipt_actions_schema: Schema,
    execution_logs_schema: Schema,
    state_changes_schema: Schema,
}

impl NearBlockMapper {
    pub fn new(
        include_fork_step: bool,
        encoding: EncodeBytes,
        include_failed_transactions: bool,
    ) -> Self {
        let enc = &encoding;
        Self {
            include_failed_transactions,
            blocks: BlocksBuilder::new(include_fork_step, enc),
            chunks: ChunksBuilder::new(include_fork_step, enc),
            transactions: TransactionsBuilder::new(include_fork_step, enc),
            receipts: ReceiptsBuilder::new(include_fork_step, enc),
            receipt_actions: ReceiptActionsBuilder::new(include_fork_step, enc),
            execution_logs: ExecutionLogsBuilder::new(include_fork_step, enc),
            state_changes: StateChangesBuilder::new(include_fork_step, enc),
            blocks_schema: schema::blocks_schema(include_fork_step, enc),
            chunks_schema: schema::chunks_schema(include_fork_step, enc),
            transactions_schema: schema::transactions_schema(include_fork_step, enc),
            receipts_schema: schema::receipts_schema(include_fork_step, enc),
            receipt_actions_schema: schema::receipt_actions_schema(include_fork_step, enc),
            execution_logs_schema: schema::execution_logs_schema(include_fork_step, enc),
            state_changes_schema: schema::state_changes_schema(include_fork_step, enc),
        }
    }

    fn map_near_block(
        &mut self,
        block: &near::Block,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let header = match &block.header {
            Some(h) => h,
            None => return,
        };

        // --- blocks table ---
        self.blocks.canonical.append(identity);
        self.blocks.height.append_value(header.height);
        self.blocks
            .hash
            .append_value(crypto_hash_bytes(&header.hash));
        self.blocks
            .prev_hash
            .append_value(crypto_hash_bytes(&header.prev_hash));
        self.blocks.prev_height.append_value(header.prev_height);
        self.blocks
            .epoch_id
            .append_value(crypto_hash_bytes(&header.epoch_id));
        self.blocks.author.append_value(&block.author);
        self.blocks
            .gas_price
            .append_value(&bigint_to_string(&header.gas_price));
        self.blocks
            .total_supply
            .append_value(&bigint_to_string(&header.total_supply));
        self.blocks
            .chunks_included
            .append_value(header.chunks_included);
        self.blocks
            .latest_protocol_version
            .append_value(header.latest_protocol_version);
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // --- shards: chunks, transactions, receipts, receipt actions, logs ---
        // Indexes count every entry of the block, in shard order, including
        // entries that are skipped (no payload, or filtered failed transactions),
        // so a row's index is its position in the Firehose block.
        let origins = ReceiptOrigins::new(block);
        let mut transaction_index: u32 = 0;
        let mut receipt_index: u32 = 0;
        for shard in &block.shards {
            let shard_id = shard.shard_id;

            if let Some(chunk) = &shard.chunk {
                if let Some(chunk_header) = &chunk.header {
                    self.map_chunk(chunk_header, &chunk.author, identity, fork_step);
                }

                for tx_with_outcome in &chunk.transactions {
                    self.map_transaction(
                        shard_id,
                        transaction_index,
                        tx_with_outcome,
                        identity,
                        fork_step,
                    );
                    transaction_index = transaction_index.saturating_add(1);
                }
            }

            for receipt_outcome in &shard.receipt_execution_outcomes {
                self.map_receipt(
                    shard_id,
                    receipt_index,
                    receipt_outcome,
                    &origins,
                    identity,
                    fork_step,
                );
                receipt_index = receipt_index.saturating_add(1);
            }
        }

        // --- state_changes ---
        for sc in &block.state_changes {
            self.map_state_change(sc, identity, fork_step);
        }
    }

    fn map_chunk(
        &mut self,
        header: &near::ChunkHeader,
        author: &str,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        self.chunks.canonical.append(identity);
        self.chunks.shard_id.append_value(header.shard_id);
        self.chunks.chunk_hash.append_value(&header.chunk_hash);
        self.chunks
            .prev_state_root
            .append_value(&header.prev_state_root);
        self.chunks.gas_used.append_value(header.gas_used);
        self.chunks.gas_limit.append_value(header.gas_limit);
        self.chunks
            .height_created
            .append_value(header.height_created);
        self.chunks
            .height_included
            .append_value(header.height_included);
        self.chunks
            .encoded_length
            .append_value(header.encoded_length);
        self.chunks.author.append_value(author);
        append_fork_step(&mut self.chunks.fork_step, fork_step);
    }

    fn map_transaction(
        &mut self,
        shard_id: u64,
        transaction_index: u32,
        tx_with_outcome: &near::IndexerTransactionWithOutcome,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let tx = match &tx_with_outcome.transaction {
            Some(t) => t,
            None => return,
        };

        let tx_hash = crypto_hash_bytes(&tx.hash);
        let actions: Vec<&str> = tx.actions.iter().map(|a| action_type_name(a)).collect();
        let actions_str = actions.join(",");

        let outcome = transaction_outcome(tx_with_outcome);
        let status = outcome.map_or("Unknown", execution_status_str);

        // Skip failed transactions unless flag is set
        if !self.include_failed_transactions && status == "Failure" {
            return;
        }

        let receipt_ids = outcome.map_or(&[][..], |o| o.receipt_ids.as_slice());
        let tx_builder = &mut self.transactions;
        tx_builder.canonical.append(identity);
        tx_builder.hash.append_value(tx_hash);
        tx_builder.transaction_index.append_value(transaction_index);
        tx_builder.signer_id.append_value(&tx.signer_id);
        tx_builder.receiver_id.append_value(&tx.receiver_id);
        tx_builder.shard_id.append_value(shard_id);
        tx_builder.nonce.append_value(tx.nonce);
        tx_builder.actions.append_value(&actions_str);
        tx_builder.status.append_value(status);
        tx_builder
            .gas_burnt
            .append_value(outcome.map_or(0, |o| o.gas_burnt));
        tx_builder.tokens_burnt.append_value(
            outcome.map_or_else(|| "0".to_string(), |o| bigint_to_string(&o.tokens_burnt)),
        );
        append_receipt_ids(&mut tx_builder.receipt_ids, receipt_ids);
        match receipt_ids.first().filter(|id| !id.bytes.is_empty()) {
            Some(converted) => tx_builder
                .converted_into_receipt_id
                .append_value(&converted.bytes),
            None => tx_builder.converted_into_receipt_id.append_null(),
        }
        append_fork_step(&mut tx_builder.fork_step, fork_step);
    }

    fn map_receipt(
        &mut self,
        shard_id: u64,
        receipt_index: u32,
        receipt_outcome: &near::IndexerExecutionOutcomeWithReceipt,
        origins: &ReceiptOrigins<'_>,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let receipt = match &receipt_outcome.receipt {
            Some(r) => r,
            None => return,
        };

        let receipt_id = crypto_hash_bytes(&receipt.receipt_id);
        let context = ReceiptRowContext {
            receipt_id: self.receipts.receipt_id.encode(receipt_id),
            receipt_index,
            tx_hash: origins
                .tx_hash(receipt_id)
                .map(|tx_hash| self.receipts.tx_hash.encode(tx_hash)),
            shard_id,
            predecessor_id: &receipt.predecessor_id,
            receiver_id: &receipt.receiver_id,
        };
        let outcome = receipt_outcome
            .execution_outcome
            .as_ref()
            .and_then(|eo| eo.outcome.as_ref());
        let action_receipt = match &receipt.receipt {
            Some(near::receipt::Receipt::Action(action)) => Some(action),
            Some(near::receipt::Receipt::Data(_)) | None => None,
        };

        let receipts = &mut self.receipts;
        receipts.canonical.append(identity);
        receipts.receipt_id.append_encoded(&context.receipt_id);
        receipts.receipt_index.append_value(receipt_index);
        append_opt_encoded(&mut receipts.tx_hash, context.tx_hash.as_ref());
        receipts.predecessor_id.append_value(context.predecessor_id);
        receipts.receiver_id.append_value(context.receiver_id);
        receipts
            .signer_id
            .append_option(action_receipt.map(|action| action.signer_id.as_str()));
        receipts.shard_id.append_value(shard_id);
        receipts
            .status
            .append_value(outcome.map_or("Unknown", execution_status_str));
        receipts
            .gas_burnt
            .append_value(outcome.map_or(0, |o| o.gas_burnt));
        receipts.tokens_burnt.append_value(
            outcome.map_or_else(|| "0".to_string(), |o| bigint_to_string(&o.tokens_burnt)),
        );
        receipts
            .executor_id
            .append_value(outcome.map_or("", |o| o.executor_id.as_str()));
        append_receipt_ids(
            &mut receipts.receipt_ids,
            outcome.map_or(&[][..], |o| o.receipt_ids.as_slice()),
        );
        append_fork_step(&mut receipts.fork_step, fork_step);

        if let Some(action_receipt) = action_receipt {
            for (action_index, action) in action_receipt.actions.iter().enumerate() {
                self.map_receipt_action(
                    &context,
                    &action_receipt.signer_id,
                    action_index as u32,
                    action,
                    identity,
                    fork_step,
                );
            }
        }

        // Only receipt outcomes carry logs: a transaction's own outcome is its
        // conversion into a receipt, which runs no contract code.
        if let Some(outcome) = outcome {
            let executor_id = outcome.executor_id.as_str();
            for (log_index, log) in outcome.logs.iter().enumerate() {
                let logs = &mut self.execution_logs;
                logs.canonical.append(identity);
                logs.receipt_id.append_encoded(&context.receipt_id);
                logs.receipt_index.append_value(context.receipt_index);
                logs.log_index.append_value(log_index as u32);
                append_opt_encoded(&mut logs.tx_hash, context.tx_hash.as_ref());
                logs.shard_id.append_value(context.shard_id);
                logs.executor_id.append_value(executor_id);
                logs.predecessor_id.append_value(context.predecessor_id);
                logs.log.append_value(log);
                append_fork_step(&mut logs.fork_step, fork_step);
            }
        }
    }

    fn map_receipt_action(
        &mut self,
        context: &ReceiptRowContext<'_>,
        signer_id: &str,
        action_index: u32,
        action: &near::Action,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let actions = &mut self.receipt_actions;
        actions.canonical.append(identity);
        actions.receipt_id.append_encoded(&context.receipt_id);
        actions.receipt_index.append_value(context.receipt_index);
        actions.action_index.append_value(action_index);
        append_opt_encoded(&mut actions.tx_hash, context.tx_hash.as_ref());
        actions.shard_id.append_value(context.shard_id);
        actions.predecessor_id.append_value(context.predecessor_id);
        actions.receiver_id.append_value(context.receiver_id);
        actions.signer_id.append_value(signer_id);
        actions.action_kind.append_value(action_type_name(action));

        let (method_name, args, gas, deposit) = match &action.action {
            Some(near::action::Action::FunctionCall(call)) => (
                Some(call.method_name.as_str()),
                Some(call.args.as_ref()),
                Some(call.gas),
                Some(bigint_to_string(&call.deposit)),
            ),
            Some(near::action::Action::Transfer(transfer)) => {
                (None, None, None, Some(bigint_to_string(&transfer.deposit)))
            }
            _ => (None, None, None, None),
        };
        actions.method_name.append_option(method_name);
        actions.args.append_option(args);
        actions.gas.append_option(gas);
        actions.deposit.append_option(deposit);
        append_fork_step(&mut actions.fork_step, fork_step);
    }

    fn map_state_change(
        &mut self,
        sc: &near::StateChangeWithCause,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let value = match &sc.value {
            Some(v) => v,
            None => return,
        };
        let cause = match &sc.cause {
            Some(c) => c,
            None => return,
        };

        self.state_changes.canonical.append(identity);
        self.state_changes
            .r#type
            .append_value(state_change_type_name(value));
        self.state_changes
            .cause
            .append_value(state_change_cause_name(cause));
        self.state_changes
            .account_id
            .append_value(state_change_account_id(value));
        self.state_changes
            .key_base64
            .append_value(&state_change_key_base64(value));
        self.state_changes
            .value_base64
            .append_value(&state_change_value_base64(value));
        append_fork_step(&mut self.state_changes.fork_step, fork_step);
    }
}

impl NearBlockMapper {
    fn map_decoded(
        &mut self,
        block: near::Block,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        let tx_count = block
            .shards
            .iter()
            .filter_map(|s| s.chunk.as_ref())
            .map(|c| c.transactions.len() as u64)
            .sum();
        let identity = match &block.header {
            Some(header) => self.blocks.canonical.prepare_with_ids(
                identity,
                crypto_hash_bytes(&header.hash),
                crypto_hash_bytes(&header.prev_hash),
            )?,
            None => self.blocks.canonical.prepare(identity)?,
        };
        self.map_near_block(&block, &identity, fork_step);
        Ok(tx_count)
    }
}

impl BlockMapper for NearBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        self.map_decoded(near::Block::decode(block_bytes)?, identity, fork_step)
    }

    fn map_block_bytes(
        &mut self,
        block_bytes: prost::bytes::Bytes,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        self.map_decoded(near::Block::decode(block_bytes)?, identity, fork_step)
    }

    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        result.insert(
            "blocks".to_string(),
            self.blocks.finish(&self.blocks_schema)?,
        );
        result.insert(
            "chunks".to_string(),
            self.chunks.finish(&self.chunks_schema)?,
        );
        result.insert(
            "transactions".to_string(),
            self.transactions.finish(&self.transactions_schema)?,
        );
        result.insert(
            "receipts".to_string(),
            self.receipts.finish(&self.receipts_schema)?,
        );
        result.insert(
            "receipt_actions".to_string(),
            self.receipt_actions.finish(&self.receipt_actions_schema)?,
        );
        result.insert(
            "execution_logs".to_string(),
            self.execution_logs.finish(&self.execution_logs_schema)?,
        );
        result.insert(
            "state_changes".to_string(),
            self.state_changes.finish(&self.state_changes_schema)?,
        );
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        self.blocks
            .canonical
            .len()
            .max(self.chunks.canonical.len())
            .max(self.transactions.canonical.len())
            .max(self.receipts.canonical.len())
            .max(self.receipt_actions.canonical.len())
            .max(self.execution_logs.canonical.len())
            .max(self.state_changes.canonical.len())
    }

    fn total_rows(&self) -> usize {
        self.blocks.canonical.len()
            + self.chunks.canonical.len()
            + self.transactions.canonical.len()
            + self.receipts.canonical.len()
            + self.receipt_actions.canonical.len()
            + self.execution_logs.canonical.len()
            + self.state_changes.canonical.len()
    }

    fn table_estimates(&mut self) -> Vec<(&str, usize)> {
        let blocks = self.blocks.canonical.estimated_bytes()
            + est_u64(&self.blocks.height)
            + self.blocks.hash.estimated_bytes()
            + self.blocks.prev_hash.estimated_bytes()
            + est_u64(&self.blocks.prev_height)
            + self.blocks.epoch_id.estimated_bytes()
            + est_str(&self.blocks.author)
            + est_str(&self.blocks.gas_price)
            + est_str(&self.blocks.total_supply)
            + est_u64(&self.blocks.chunks_included)
            + est_u32(&self.blocks.latest_protocol_version)
            + est_opt_str(&self.blocks.fork_step);
        let chunks = self.chunks.canonical.estimated_bytes()
            + est_u64(&self.chunks.shard_id)
            + self.chunks.chunk_hash.estimated_bytes()
            + self.chunks.prev_state_root.estimated_bytes()
            + est_u64(&self.chunks.gas_used)
            + est_u64(&self.chunks.gas_limit)
            + est_u64(&self.chunks.height_created)
            + est_u64(&self.chunks.height_included)
            + est_u64(&self.chunks.encoded_length)
            + est_str(&self.chunks.author)
            + est_opt_str(&self.chunks.fork_step);
        let transactions = self.transactions.canonical.estimated_bytes()
            + self.transactions.hash.estimated_bytes()
            + est_u32(&self.transactions.transaction_index)
            + est_str(&self.transactions.signer_id)
            + est_str(&self.transactions.receiver_id)
            + est_u64(&self.transactions.shard_id)
            + est_u64(&self.transactions.nonce)
            + est_str(&self.transactions.actions)
            + est_str(&self.transactions.status)
            + est_u64(&self.transactions.gas_burnt)
            + est_str(&self.transactions.tokens_burnt)
            + self.transactions.receipt_ids.estimated_bytes()
            + self
                .transactions
                .converted_into_receipt_id
                .estimated_bytes()
            + est_opt_str(&self.transactions.fork_step);
        let receipts = self.receipts.canonical.estimated_bytes()
            + self.receipts.receipt_id.estimated_bytes()
            + est_u32(&self.receipts.receipt_index)
            + self.receipts.tx_hash.estimated_bytes()
            + est_str(&self.receipts.predecessor_id)
            + est_str(&self.receipts.receiver_id)
            + est_str(&self.receipts.signer_id)
            + est_u64(&self.receipts.shard_id)
            + est_str(&self.receipts.status)
            + est_u64(&self.receipts.gas_burnt)
            + est_str(&self.receipts.tokens_burnt)
            + est_str(&self.receipts.executor_id)
            + self.receipts.receipt_ids.estimated_bytes()
            + est_opt_str(&self.receipts.fork_step);
        let receipt_actions = self.receipt_actions.canonical.estimated_bytes()
            + self.receipt_actions.receipt_id.estimated_bytes()
            + est_u32(&self.receipt_actions.receipt_index)
            + est_u32(&self.receipt_actions.action_index)
            + self.receipt_actions.tx_hash.estimated_bytes()
            + est_u64(&self.receipt_actions.shard_id)
            + est_str(&self.receipt_actions.predecessor_id)
            + est_str(&self.receipt_actions.receiver_id)
            + est_str(&self.receipt_actions.signer_id)
            + estimated_dictionary_index_bytes(self.receipt_actions.action_kind.len())
            + est_str(&self.receipt_actions.method_name)
            + est_bin(&self.receipt_actions.args)
            + est_u64(&self.receipt_actions.gas)
            + est_str(&self.receipt_actions.deposit)
            + est_opt_str(&self.receipt_actions.fork_step);
        let execution_logs = self.execution_logs.canonical.estimated_bytes()
            + self.execution_logs.receipt_id.estimated_bytes()
            + est_u32(&self.execution_logs.receipt_index)
            + est_u32(&self.execution_logs.log_index)
            + self.execution_logs.tx_hash.estimated_bytes()
            + est_u64(&self.execution_logs.shard_id)
            + est_str(&self.execution_logs.executor_id)
            + est_str(&self.execution_logs.predecessor_id)
            + est_str(&self.execution_logs.log)
            + est_opt_str(&self.execution_logs.fork_step);
        let state_changes = self.state_changes.canonical.estimated_bytes()
            + est_str(&self.state_changes.r#type)
            + est_str(&self.state_changes.cause)
            + est_str(&self.state_changes.account_id)
            + est_str(&self.state_changes.key_base64)
            + est_str(&self.state_changes.value_base64)
            + est_opt_str(&self.state_changes.fork_step);
        [
            ("blocks", blocks),
            ("chunks", chunks),
            ("transactions", transactions),
            ("receipts", receipts),
            ("receipt_actions", receipt_actions),
            ("execution_logs", execution_logs),
            ("state_changes", state_changes),
        ]
        .into_iter()
        .collect()
    }

    fn table_names(&self) -> Vec<&str> {
        schema::TABLE_NAMES.to_vec()
    }
}

// ===========================================================================
// Builders
// ===========================================================================

struct BlocksBuilder {
    canonical: CanonicalBuilder,
    height: UInt64Builder,
    hash: BytesColumn,
    prev_hash: BytesColumn,
    prev_height: UInt64Builder,
    epoch_id: BytesColumn,
    author: StringBuilder,
    gas_price: StringBuilder,
    total_supply: StringBuilder,
    chunks_included: UInt64Builder,
    latest_protocol_version: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            height: UInt64Builder::new(),
            hash: BytesColumn::new(encoding),
            prev_hash: BytesColumn::new(encoding),
            prev_height: UInt64Builder::new(),
            epoch_id: BytesColumn::new(encoding),
            author: StringBuilder::new(),
            gas_price: StringBuilder::new(),
            total_supply: StringBuilder::new(),
            chunks_included: UInt64Builder::new(),
            latest_protocol_version: UInt32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.height.finish()) as Arc<dyn Array>,
            self.hash.finish(),
            self.prev_hash.finish(),
            Arc::new(self.prev_height.finish()) as Arc<dyn Array>,
            self.epoch_id.finish(),
            Arc::new(self.author.finish()) as Arc<dyn Array>,
            Arc::new(self.gas_price.finish()) as Arc<dyn Array>,
            Arc::new(self.total_supply.finish()) as Arc<dyn Array>,
            Arc::new(self.chunks_included.finish()) as Arc<dyn Array>,
            Arc::new(self.latest_protocol_version.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct ChunksBuilder {
    canonical: CanonicalBuilder,
    shard_id: UInt64Builder,
    chunk_hash: BytesColumn,
    prev_state_root: BytesColumn,
    gas_used: UInt64Builder,
    gas_limit: UInt64Builder,
    height_created: UInt64Builder,
    height_included: UInt64Builder,
    encoded_length: UInt64Builder,
    author: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl ChunksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            shard_id: UInt64Builder::new(),
            chunk_hash: BytesColumn::new(encoding),
            prev_state_root: BytesColumn::new(encoding),
            gas_used: UInt64Builder::new(),
            gas_limit: UInt64Builder::new(),
            height_created: UInt64Builder::new(),
            height_included: UInt64Builder::new(),
            encoded_length: UInt64Builder::new(),
            author: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.shard_id.finish()) as Arc<dyn Array>,
            self.chunk_hash.finish(),
            self.prev_state_root.finish(),
            Arc::new(self.gas_used.finish()) as Arc<dyn Array>,
            Arc::new(self.gas_limit.finish()) as Arc<dyn Array>,
            Arc::new(self.height_created.finish()) as Arc<dyn Array>,
            Arc::new(self.height_included.finish()) as Arc<dyn Array>,
            Arc::new(self.encoded_length.finish()) as Arc<dyn Array>,
            Arc::new(self.author.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct TransactionsBuilder {
    canonical: CanonicalBuilder,
    hash: BytesColumn,
    transaction_index: UInt32Builder,
    signer_id: StringBuilder,
    receiver_id: StringBuilder,
    shard_id: UInt64Builder,
    nonce: UInt64Builder,
    actions: StringBuilder,
    status: StringBuilder,
    gas_burnt: UInt64Builder,
    tokens_burnt: StringBuilder,
    receipt_ids: BytesListColumn,
    converted_into_receipt_id: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            hash: BytesColumn::new(encoding),
            transaction_index: UInt32Builder::new(),
            signer_id: StringBuilder::new(),
            receiver_id: StringBuilder::new(),
            shard_id: UInt64Builder::new(),
            nonce: UInt64Builder::new(),
            actions: StringBuilder::new(),
            status: StringBuilder::new(),
            gas_burnt: UInt64Builder::new(),
            tokens_burnt: StringBuilder::new(),
            receipt_ids: BytesListColumn::new(encoding),
            converted_into_receipt_id: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            self.hash.finish(),
            Arc::new(self.transaction_index.finish()) as Arc<dyn Array>,
            Arc::new(self.signer_id.finish()) as Arc<dyn Array>,
            Arc::new(self.receiver_id.finish()) as Arc<dyn Array>,
            Arc::new(self.shard_id.finish()) as Arc<dyn Array>,
            Arc::new(self.nonce.finish()) as Arc<dyn Array>,
            Arc::new(self.actions.finish()) as Arc<dyn Array>,
            Arc::new(self.status.finish()) as Arc<dyn Array>,
            Arc::new(self.gas_burnt.finish()) as Arc<dyn Array>,
            Arc::new(self.tokens_burnt.finish()) as Arc<dyn Array>,
            self.receipt_ids.finish(),
            self.converted_into_receipt_id.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct ReceiptsBuilder {
    canonical: CanonicalBuilder,
    receipt_id: BytesColumn,
    receipt_index: UInt32Builder,
    tx_hash: BytesColumn,
    predecessor_id: StringBuilder,
    receiver_id: StringBuilder,
    signer_id: StringBuilder,
    shard_id: UInt64Builder,
    status: StringBuilder,
    gas_burnt: UInt64Builder,
    tokens_burnt: StringBuilder,
    executor_id: StringBuilder,
    receipt_ids: BytesListColumn,
    fork_step: Option<StringBuilder>,
}

impl ReceiptsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            receipt_id: BytesColumn::new(encoding),
            receipt_index: UInt32Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            predecessor_id: StringBuilder::new(),
            receiver_id: StringBuilder::new(),
            signer_id: StringBuilder::new(),
            shard_id: UInt64Builder::new(),
            status: StringBuilder::new(),
            gas_burnt: UInt64Builder::new(),
            tokens_burnt: StringBuilder::new(),
            executor_id: StringBuilder::new(),
            receipt_ids: BytesListColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            self.receipt_id.finish(),
            Arc::new(self.receipt_index.finish()) as Arc<dyn Array>,
            self.tx_hash.finish(),
            Arc::new(self.predecessor_id.finish()) as Arc<dyn Array>,
            Arc::new(self.receiver_id.finish()) as Arc<dyn Array>,
            Arc::new(self.signer_id.finish()) as Arc<dyn Array>,
            Arc::new(self.shard_id.finish()) as Arc<dyn Array>,
            Arc::new(self.status.finish()) as Arc<dyn Array>,
            Arc::new(self.gas_burnt.finish()) as Arc<dyn Array>,
            Arc::new(self.tokens_burnt.finish()) as Arc<dyn Array>,
            Arc::new(self.executor_id.finish()) as Arc<dyn Array>,
            self.receipt_ids.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct ReceiptActionsBuilder {
    canonical: CanonicalBuilder,
    receipt_id: BytesColumn,
    receipt_index: UInt32Builder,
    action_index: UInt32Builder,
    tx_hash: BytesColumn,
    shard_id: UInt64Builder,
    predecessor_id: StringBuilder,
    receiver_id: StringBuilder,
    signer_id: StringBuilder,
    action_kind: StringDictionaryBuilder<Int32Type>,
    method_name: StringBuilder,
    args: BinaryBuilder,
    gas: UInt64Builder,
    deposit: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl ReceiptActionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            receipt_id: BytesColumn::new(encoding),
            receipt_index: UInt32Builder::new(),
            action_index: UInt32Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            shard_id: UInt64Builder::new(),
            predecessor_id: StringBuilder::new(),
            receiver_id: StringBuilder::new(),
            signer_id: StringBuilder::new(),
            action_kind: StringDictionaryBuilder::new(),
            method_name: StringBuilder::new(),
            args: BinaryBuilder::new(),
            gas: UInt64Builder::new(),
            deposit: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            self.receipt_id.finish(),
            Arc::new(self.receipt_index.finish()) as Arc<dyn Array>,
            Arc::new(self.action_index.finish()) as Arc<dyn Array>,
            self.tx_hash.finish(),
            Arc::new(self.shard_id.finish()) as Arc<dyn Array>,
            Arc::new(self.predecessor_id.finish()) as Arc<dyn Array>,
            Arc::new(self.receiver_id.finish()) as Arc<dyn Array>,
            Arc::new(self.signer_id.finish()) as Arc<dyn Array>,
            Arc::new(self.action_kind.finish()) as Arc<dyn Array>,
            Arc::new(self.method_name.finish()) as Arc<dyn Array>,
            Arc::new(self.args.finish()) as Arc<dyn Array>,
            Arc::new(self.gas.finish()) as Arc<dyn Array>,
            Arc::new(self.deposit.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct ExecutionLogsBuilder {
    canonical: CanonicalBuilder,
    receipt_id: BytesColumn,
    receipt_index: UInt32Builder,
    log_index: UInt32Builder,
    tx_hash: BytesColumn,
    shard_id: UInt64Builder,
    executor_id: StringBuilder,
    predecessor_id: StringBuilder,
    log: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl ExecutionLogsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            receipt_id: BytesColumn::new(encoding),
            receipt_index: UInt32Builder::new(),
            log_index: UInt32Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            shard_id: UInt64Builder::new(),
            executor_id: StringBuilder::new(),
            predecessor_id: StringBuilder::new(),
            log: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            self.receipt_id.finish(),
            Arc::new(self.receipt_index.finish()) as Arc<dyn Array>,
            Arc::new(self.log_index.finish()) as Arc<dyn Array>,
            self.tx_hash.finish(),
            Arc::new(self.shard_id.finish()) as Arc<dyn Array>,
            Arc::new(self.executor_id.finish()) as Arc<dyn Array>,
            Arc::new(self.predecessor_id.finish()) as Arc<dyn Array>,
            Arc::new(self.log.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct StateChangesBuilder {
    canonical: CanonicalBuilder,
    r#type: StringBuilder,
    cause: StringBuilder,
    account_id: StringBuilder,
    key_base64: StringBuilder,
    value_base64: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl StateChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            r#type: StringBuilder::new(),
            cause: StringBuilder::new(),
            account_id: StringBuilder::new(),
            key_base64: StringBuilder::new(),
            value_base64: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.r#type.finish()) as Arc<dyn Array>,
            Arc::new(self.cause.finish()) as Arc<dyn Array>,
            Arc::new(self.account_id.finish()) as Arc<dyn Array>,
            Arc::new(self.key_base64.finish()) as Arc<dyn Array>,
            Arc::new(self.value_base64.finish()) as Arc<dyn Array>,
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
    use arrow::util::display::array_value_to_string;
    use firehose_parquet::encode::encode_base58;

    /// A NEAR `BigInt` (big-endian, no leading zero bytes).
    fn bigint(value: u128) -> near::BigInt {
        let bytes = value.to_be_bytes();
        let first = bytes.iter().position(|b| *b != 0).unwrap_or(bytes.len());
        near::BigInt {
            bytes: bytes[first..].to_vec().into(),
        }
    }

    fn hash(byte: u8) -> Option<near::CryptoHash> {
        Some(near::CryptoHash {
            bytes: vec![byte; 32].into(),
        })
    }

    fn action(action: near::action::Action) -> near::Action {
        near::Action {
            action: Some(action),
        }
    }

    fn function_call_action(method_name: &str, args: &[u8]) -> near::Action {
        action(near::action::Action::FunctionCall(
            near::FunctionCallAction {
                method_name: method_name.to_string(),
                args: args.to_vec().into(),
                gas: 30_000_000_000_000,
                deposit: Some(bigint(1)),
            },
        ))
    }

    fn transfer_action(deposit: u128) -> near::Action {
        action(near::action::Action::Transfer(near::TransferAction {
            deposit: Some(bigint(deposit)),
        }))
    }

    fn public_key(byte: u8) -> Option<near::PublicKey> {
        Some(near::PublicKey {
            r#type: near::CurveKind::Ed25519 as i32,
            bytes: vec![byte; 32].into(),
        })
    }

    /// One action of every kind, followed by an action with no variant set.
    fn every_action() -> Vec<near::Action> {
        use near::action::Action as A;
        vec![
            action(A::CreateAccount(near::CreateAccountAction {})),
            action(A::DeployContract(near::DeployContractAction {
                code: b"\0asm".to_vec().into(),
            })),
            function_call_action(
                "ft_transfer",
                br#"{"receiver_id":"carol.near","amount":"5"}"#,
            ),
            transfer_action(1_000_000_000_000_000_000_000_000),
            action(A::Stake(near::StakeAction {
                stake: Some(bigint(7)),
                public_key: public_key(0x40),
            })),
            action(A::AddKey(near::AddKeyAction {
                public_key: public_key(0x41),
                access_key: Some(near::AccessKey {
                    nonce: 0,
                    permission: Some(near::AccessKeyPermission {
                        permission: Some(near::access_key_permission::Permission::FullAccess(
                            near::FullAccessPermission {},
                        )),
                    }),
                }),
            })),
            action(A::DeleteKey(near::DeleteKeyAction {
                public_key: public_key(0x41),
            })),
            action(A::DeleteAccount(near::DeleteAccountAction {
                beneficiary_id: "carol.near".to_string(),
            })),
            action(A::Delegate(near::SignedDelegateAction {
                signature: None,
                delegate_action: Some(near::DelegateAction {
                    sender_id: "alice.near".to_string(),
                    receiver_id: "token.near".to_string(),
                    actions: vec![function_call_action("ft_transfer", b"{}")],
                    nonce: 1,
                    max_block_height: 1_000,
                    public_key: public_key(0x42),
                }),
            })),
            near::Action { action: None },
        ]
    }

    fn transaction(
        signer_id: &str,
        receiver_id: &str,
        tx_hash: u8,
        status: near::execution_outcome::Status,
        receipt_ids: Vec<near::CryptoHash>,
    ) -> near::IndexerTransactionWithOutcome {
        near::IndexerTransactionWithOutcome {
            transaction: Some(near::SignedTransaction {
                signer_id: signer_id.to_string(),
                receiver_id: receiver_id.to_string(),
                nonce: 7,
                hash: hash(tx_hash),
                actions: vec![transfer_action(1)],
                ..Default::default()
            }),
            outcome: Some(near::IndexerExecutionOutcomeWithOptionalReceipt {
                execution_outcome: Some(near::ExecutionOutcomeWithId {
                    id: hash(tx_hash),
                    outcome: Some(near::ExecutionOutcome {
                        receipt_ids,
                        gas_burnt: 1_000,
                        tokens_burnt: Some(bigint(100_000_000_000)),
                        executor_id: signer_id.to_string(),
                        status: Some(status),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                receipt: None,
            }),
        }
    }

    fn success_receipt_id(byte: u8) -> near::execution_outcome::Status {
        near::execution_outcome::Status::SuccessReceiptId(near::SuccessReceiptIdExecutionStatus {
            id: hash(byte),
        })
    }

    fn failure() -> near::execution_outcome::Status {
        near::execution_outcome::Status::Failure(near::FailureExecutionStatus {
            failure: Some(near::failure_execution_status::Failure::InvalidTxError(
                near::InvalidTxError::InvalidNonce as i32,
            )),
        })
    }

    /// A two-shard block exercising every new column and join:
    ///
    /// - shard 0 has a local transaction `0xa1` (alice → alice) converted into
    ///   receipt `0xb1`, and a failed transaction `0xa2` with no receipt;
    ///   receipt `0xb1` runs in the same block with one action of every kind,
    ///   two logs and a child receipt `0xb2`.
    /// - shard 1 has transaction `0xa3` (carol → dex) converted into receipt
    ///   `0xb9`, which runs in a later block; a failed receipt `0xb3` from an
    ///   earlier block; and a data receipt `0xb4` without an outcome.
    pub(crate) fn make_every_action_block(height: u64) -> near::Block {
        let mut block = make_test_block(height);
        block.state_changes.clear();
        let chunk = |shard_id: u64, transactions| near::IndexerChunk {
            author: format!("producer{shard_id}.near"),
            header: Some(near::ChunkHeader {
                chunk_hash: vec![0x10 + shard_id as u8; 32].into(),
                prev_state_root: vec![0x11; 32].into(),
                shard_id,
                height_created: height,
                height_included: height,
                ..Default::default()
            }),
            transactions,
            receipts: vec![],
        };
        block.shards = vec![
            near::IndexerShard {
                shard_id: 0,
                chunk: Some(chunk(
                    0,
                    vec![
                        transaction(
                            "alice.near",
                            "alice.near",
                            0xa1,
                            success_receipt_id(0xb1),
                            vec![hash(0xb1).unwrap()],
                        ),
                        transaction("mallory.near", "bob.near", 0xa2, failure(), vec![]),
                    ],
                )),
                receipt_execution_outcomes: vec![near::IndexerExecutionOutcomeWithReceipt {
                    execution_outcome: Some(near::ExecutionOutcomeWithId {
                        id: hash(0xb1),
                        outcome: Some(near::ExecutionOutcome {
                            logs: vec![
                                r#"EVENT_JSON:{"standard":"nep141","version":"1.0.0","event":"ft_transfer","data":[{"old_owner_id":"alice.near","new_owner_id":"carol.near","amount":"5"}]}"#.to_string(),
                                "Transfer 5 from alice.near to carol.near".to_string(),
                            ],
                            receipt_ids: vec![hash(0xb2).unwrap()],
                            gas_burnt: 5_000,
                            tokens_burnt: Some(bigint(500_000_000_000)),
                            executor_id: "alice.near".to_string(),
                            status: Some(near::execution_outcome::Status::SuccessValue(
                                near::SuccessValueExecutionStatus { value: vec![].into() },
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    receipt: Some(near::Receipt {
                        predecessor_id: "alice.near".to_string(),
                        receiver_id: "alice.near".to_string(),
                        receipt_id: hash(0xb1),
                        receipt: Some(near::receipt::Receipt::Action(near::ReceiptAction {
                            signer_id: "alice.near".to_string(),
                            signer_public_key: public_key(0x43),
                            gas_price: Some(bigint(100_000_000)),
                            actions: every_action(),
                            ..Default::default()
                        })),
                    }),
                }],
            },
            near::IndexerShard {
                shard_id: 1,
                chunk: Some(chunk(
                    1,
                    vec![transaction(
                        "carol.near",
                        "dex.near",
                        0xa3,
                        success_receipt_id(0xb9),
                        vec![hash(0xb9).unwrap()],
                    )],
                )),
                receipt_execution_outcomes: vec![
                    near::IndexerExecutionOutcomeWithReceipt {
                        execution_outcome: Some(near::ExecutionOutcomeWithId {
                            id: hash(0xb3),
                            outcome: Some(near::ExecutionOutcome {
                                gas_burnt: 3_000,
                                tokens_burnt: Some(bigint(300_000_000_000)),
                                executor_id: "dex.near".to_string(),
                                status: Some(near::execution_outcome::Status::Failure(
                                    near::FailureExecutionStatus::default(),
                                )),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        receipt: Some(near::Receipt {
                            predecessor_id: "relayer.near".to_string(),
                            receiver_id: "dex.near".to_string(),
                            receipt_id: hash(0xb3),
                            receipt: Some(near::receipt::Receipt::Action(near::ReceiptAction {
                                signer_id: "relayer.near".to_string(),
                                actions: vec![function_call_action("swap", b"\x01\x02")],
                                ..Default::default()
                            })),
                        }),
                    },
                    near::IndexerExecutionOutcomeWithReceipt {
                        execution_outcome: None,
                        receipt: Some(near::Receipt {
                            predecessor_id: "dex.near".to_string(),
                            receiver_id: "carol.near".to_string(),
                            receipt_id: hash(0xb4),
                            receipt: Some(near::receipt::Receipt::Data(near::ReceiptData {
                                data_id: hash(0xd4),
                                data: b"result".to_vec().into(),
                            })),
                        }),
                    },
                ],
            },
        ];
        block
    }

    pub(crate) fn make_test_block(height: u64) -> near::Block {
        near::Block {
            author: "test.near".to_string(),
            header: Some(near::BlockHeader {
                height,
                prev_height: height.saturating_sub(1),
                epoch_id: Some(near::CryptoHash {
                    bytes: vec![0xaa; 32].into(),
                }),
                next_epoch_id: Some(near::CryptoHash {
                    bytes: vec![0xbb; 32].into(),
                }),
                hash: Some(near::CryptoHash {
                    bytes: vec![0x01; 32].into(),
                }),
                prev_hash: Some(near::CryptoHash {
                    bytes: vec![0x02; 32].into(),
                }),
                prev_state_root: Some(near::CryptoHash {
                    bytes: vec![0x03; 32].into(),
                }),
                timestamp: 1_700_000_000,
                timestamp_nanosec: 1_700_000_000_000_000_000,
                gas_price: Some(near::BigInt {
                    bytes: vec![0x05, 0xF5, 0xE1, 0x00].into(),
                }), // 100_000_000
                total_supply: Some(near::BigInt {
                    bytes: vec![0x01, 0x00].into(),
                }), // 256
                chunks_included: 4,
                latest_protocol_version: 60,
                ..Default::default()
            }),
            shards: vec![near::IndexerShard {
                shard_id: 0,
                chunk: Some(near::IndexerChunk {
                    author: "chunk_producer.near".to_string(),
                    header: Some(near::ChunkHeader {
                        chunk_hash: vec![0x10; 32].into(),
                        prev_state_root: vec![0x11; 32].into(),
                        shard_id: 0,
                        gas_used: 12_000_000_000_000,
                        gas_limit: 1_000_000_000_000_000,
                        height_created: height,
                        height_included: height,
                        encoded_length: 1024,
                        ..Default::default()
                    }),
                    transactions: vec![near::IndexerTransactionWithOutcome {
                        transaction: Some(near::SignedTransaction {
                            signer_id: "alice.near".to_string(),
                            receiver_id: "bob.near".to_string(),
                            nonce: 42,
                            hash: Some(near::CryptoHash {
                                bytes: vec![0x20; 32].into(),
                            }),
                            actions: vec![near::Action {
                                action: Some(near::action::Action::Transfer(
                                    near::TransferAction {
                                        deposit: Some(near::BigInt {
                                            bytes: vec![0x01].into(),
                                        }),
                                    },
                                )),
                            }],
                            ..Default::default()
                        }),
                        outcome: Some(near::IndexerExecutionOutcomeWithOptionalReceipt {
                            execution_outcome: Some(near::ExecutionOutcomeWithId {
                                outcome: Some(near::ExecutionOutcome {
                                    receipt_ids: vec![near::CryptoHash {
                                        bytes: vec![0x30; 32].into(),
                                    }],
                                    gas_burnt: 2_428_000_000_000,
                                    tokens_burnt: Some(bigint(242_800_000_000_000_000_000)),
                                    executor_id: "alice.near".to_string(),
                                    status: Some(
                                        near::execution_outcome::Status::SuccessReceiptId(
                                            near::SuccessReceiptIdExecutionStatus {
                                                id: Some(near::CryptoHash {
                                                    bytes: vec![0x30; 32].into(),
                                                }),
                                            },
                                        ),
                                    ),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            receipt: None,
                        }),
                    }],
                    receipts: vec![],
                }),
                receipt_execution_outcomes: vec![near::IndexerExecutionOutcomeWithReceipt {
                    execution_outcome: Some(near::ExecutionOutcomeWithId {
                        outcome: Some(near::ExecutionOutcome {
                            logs: vec![r#"EVENT_JSON:{"standard":"nep141","version":"1.0.0","event":"ft_transfer","data":[{"old_owner_id":"alice.near","new_owner_id":"carol.near","amount":"5"}]}"#.to_string()],
                            receipt_ids: vec![near::CryptoHash {
                                bytes: vec![0x31; 32].into(),
                            }],
                            gas_burnt: 2_428_000_000_000,
                            tokens_burnt: Some(bigint(242_800_000_000_000_000_000)),
                            executor_id: "bob.near".to_string(),
                            status: Some(near::execution_outcome::Status::SuccessValue(
                                near::SuccessValueExecutionStatus {
                                    value: vec![].into(),
                                },
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    receipt: Some(near::Receipt {
                        predecessor_id: "alice.near".to_string(),
                        receiver_id: "bob.near".to_string(),
                        receipt_id: Some(near::CryptoHash {
                            bytes: vec![0x30; 32].into(),
                        }),
                        receipt: Some(near::receipt::Receipt::Action(near::ReceiptAction {
                            signer_id: "alice.near".to_string(),
                            actions: vec![
                                function_call_action("ft_transfer", br#"{"amount":"5"}"#),
                                transfer_action(1),
                            ],
                            ..Default::default()
                        })),
                    }),
                }],
            }],
            state_changes: vec![near::StateChangeWithCause {
                value: Some(near::StateChangeValue {
                    value: Some(near::state_change_value::Value::AccountUpdate(
                        near::state_change_value::AccountUpdate {
                            account_id: "bob.near".to_string(),
                            account: Some(near::Account {
                                amount: Some(near::BigInt {
                                    bytes: vec![0x01].into(),
                                }),
                                locked: Some(near::BigInt {
                                    bytes: vec![].into(),
                                }),
                                code_hash: Some(near::CryptoHash {
                                    bytes: vec![0x00; 32].into(),
                                }),
                                storage_usage: 100,
                            }),
                        },
                    )),
                }),
                cause: Some(near::StateChangeCause {
                    cause: Some(near::state_change_cause::Cause::TransactionProcessing(
                        near::state_change_cause::TransactionProcessing {
                            tx_hash: Some(near::CryptoHash {
                                bytes: vec![0x20; 32].into(),
                            }),
                        },
                    )),
                }),
            }],
            chunk_headers: vec![],
        }
    }

    #[test]
    fn test_map_and_flush() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = NearBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["chunks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["receipts"].num_rows(), 1);
        assert_eq!(batches["receipt_actions"].num_rows(), 2);
        assert_eq!(batches["execution_logs"].num_rows(), 1);
        assert_eq!(batches["state_changes"].num_rows(), 1);
    }

    #[test]
    fn test_empty_block() {
        let block = near::Block {
            author: "test.near".to_string(),
            header: Some(near::BlockHeader {
                height: 1,
                hash: Some(near::CryptoHash {
                    bytes: vec![0x01; 32].into(),
                }),
                prev_hash: Some(near::CryptoHash {
                    bytes: vec![0x00; 32].into(),
                }),
                timestamp: 1_700_000_000,
                ..Default::default()
            }),
            shards: vec![],
            state_changes: vec![],
            chunk_headers: vec![],
        };
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = NearBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["chunks"].num_rows(), 0);
        assert_eq!(batches["transactions"].num_rows(), 0);
        assert_eq!(batches["receipts"].num_rows(), 0);
        assert_eq!(batches["receipt_actions"].num_rows(), 0);
        assert_eq!(batches["execution_logs"].num_rows(), 0);
        assert_eq!(batches["state_changes"].num_rows(), 0);
    }

    #[test]
    fn test_flush_resets() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = NearBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_table_names() {
        let mapper = NearBlockMapper::new(false, EncodeBytes::Hex, false);
        assert_eq!(mapper.table_names().len(), 7);
        assert!(mapper.table_names().contains(&"blocks"));
        assert!(mapper.table_names().contains(&"chunks"));
        assert!(mapper.table_names().contains(&"transactions"));
        assert!(mapper.table_names().contains(&"receipts"));
        assert!(mapper.table_names().contains(&"receipt_actions"));
        assert!(mapper.table_names().contains(&"execution_logs"));
        assert!(mapper.table_names().contains(&"state_changes"));
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = NearBlockMapper::new(true, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), Some("FINAL"))
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
        assert_eq!(fork_col.value(0), "FINAL");
    }

    #[test]
    fn test_state_change_data_update() {
        let block = near::Block {
            author: "test.near".to_string(),
            header: Some(near::BlockHeader {
                height: 200,
                hash: Some(near::CryptoHash {
                    bytes: vec![0x01; 32].into(),
                }),
                prev_hash: Some(near::CryptoHash {
                    bytes: vec![0x00; 32].into(),
                }),
                timestamp: 1_700_000_000,
                ..Default::default()
            }),
            shards: vec![],
            state_changes: vec![near::StateChangeWithCause {
                value: Some(near::StateChangeValue {
                    value: Some(near::state_change_value::Value::DataUpdate(
                        near::state_change_value::DataUpdate {
                            account_id: "contract.near".to_string(),
                            key: b"mykey".to_vec().into(),
                            value: b"myvalue".to_vec().into(),
                        },
                    )),
                }),
                cause: Some(near::StateChangeCause {
                    cause: Some(near::state_change_cause::Cause::ReceiptProcessing(
                        near::state_change_cause::ReceiptProcessing {
                            tx_hash: Some(near::CryptoHash {
                                bytes: vec![0x20; 32].into(),
                            }),
                        },
                    )),
                }),
            }],
            chunk_headers: vec![],
        };
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = NearBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        let sc_batch = &batches["state_changes"];
        assert_eq!(sc_batch.num_rows(), 1);
        let schema = sc_batch.schema();

        // Verify type and cause
        let type_col = sc_batch
            .column(schema.index_of("type").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(type_col.value(0), "DataUpdate");
        let cause_col = sc_batch
            .column(schema.index_of("cause").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cause_col.value(0), "ReceiptProcessing");
        let account_col = sc_batch
            .column(schema.index_of("account_id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(account_col.value(0), "contract.near");
        // key_base64 for "mykey" is "bXlrZXk="
        let key_col = sc_batch
            .column(schema.index_of("key_base64").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key_col.value(0), "bXlrZXk=");
        // value_base64 for "myvalue" is "bXl2YWx1ZQ=="
        let val_col = sc_batch
            .column(schema.index_of("value_base64").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(val_col.value(0), "bXl2YWx1ZQ==");
    }

    #[test]
    fn test_bigint_to_string() {
        assert_eq!(bigint_to_string(&None), "0");
        assert_eq!(
            bigint_to_string(&Some(near::BigInt {
                bytes: vec![].into()
            })),
            "0"
        );
        assert_eq!(
            bigint_to_string(&Some(near::BigInt {
                bytes: vec![0x01].into()
            })),
            "1"
        );
        assert_eq!(
            bigint_to_string(&Some(near::BigInt {
                bytes: vec![0x01, 0x00].into()
            })),
            "256"
        );
        // 100_000_000 = 0x05F5E100
        assert_eq!(
            bigint_to_string(&Some(near::BigInt {
                bytes: vec![0x05, 0xF5, 0xE1, 0x00].into()
            })),
            "100000000"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn crypto_hash_hex(hash: &Option<near::CryptoHash>) -> String {
        hex(crypto_hash_bytes(hash))
    }

    #[test]
    fn test_hex() {
        assert_eq!(hex(&[]), "");
        assert_eq!(hex(&[0x01, 0x02, 0xff]), "0102ff");
        assert_eq!(crypto_hash_hex(&None), "");
    }

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"mykey"), "bXlrZXk=");
        assert_eq!(base64_encode(b"myvalue"), "bXl2YWx1ZQ==");
    }

    #[test]
    fn test_near_base58_encoding_aligns_canonical_ids_and_hash_fields() {
        const ENVELOPE_BLOCK_ID_BYTES: [u8; 32] = [0x09; 32];
        const ENVELOPE_PARENT_ID_BYTES: [u8; 32] = [0x08; 32];

        let block = make_test_block(100);
        let header = block.header.as_ref().unwrap();
        let block_bytes = prost::Message::encode_to_vec(&block);
        let identity = BlockIdentity {
            block_num: 100,
            block_id: format!("0x{}", hex(&ENVELOPE_BLOCK_ID_BYTES)),
            parent_num: 99,
            parent_id: format!("0x{}", hex(&ENVELOPE_PARENT_ID_BYTES)),
            lib_num: 98,
            timestamp: 1_700_000_000,
            timestamp_nanos: 0,
            fork_step: None,
        };
        let mut mapper = NearBlockMapper::new(false, EncodeBytes::Base58, false);
        mapper.map_block(&block_bytes, &identity, None).unwrap();

        let batches = mapper.flush().unwrap();

        let blocks_batch = &batches["blocks"];
        let blocks_schema = blocks_batch.schema();
        let block_id_col = blocks_batch
            .column(blocks_schema.index_of("block_id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let parent_id_col = blocks_batch
            .column(blocks_schema.index_of("parent_id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let hash_col = blocks_batch
            .column(blocks_schema.index_of("hash").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let prev_hash_col = blocks_batch
            .column(blocks_schema.index_of("prev_hash").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let epoch_id_col = blocks_batch
            .column(blocks_schema.index_of("epoch_id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            block_id_col.value(0),
            firehose_parquet::encode::encode_base58(crypto_hash_bytes(&header.hash))
        );
        assert_eq!(
            parent_id_col.value(0),
            firehose_parquet::encode::encode_base58(crypto_hash_bytes(&header.prev_hash))
        );
        assert_eq!(block_id_col.value(0), hash_col.value(0));
        assert_eq!(parent_id_col.value(0), prev_hash_col.value(0));
        assert_eq!(
            hash_col.value(0),
            firehose_parquet::encode::encode_base58(&[0x01; 32])
        );
        assert_eq!(
            prev_hash_col.value(0),
            firehose_parquet::encode::encode_base58(&[0x02; 32])
        );
        assert_eq!(
            epoch_id_col.value(0),
            firehose_parquet::encode::encode_base58(&[0xaa; 32])
        );
        assert_ne!(
            block_id_col.value(0),
            firehose_parquet::encode::encode_base58(&ENVELOPE_BLOCK_ID_BYTES)
        );
        assert_ne!(
            parent_id_col.value(0),
            firehose_parquet::encode::encode_base58(&ENVELOPE_PARENT_ID_BYTES)
        );

        let chunks_batch = &batches["chunks"];
        let chunks_schema = chunks_batch.schema();
        let chunk_hash_col = chunks_batch
            .column(chunks_schema.index_of("chunk_hash").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let prev_state_root_col = chunks_batch
            .column(chunks_schema.index_of("prev_state_root").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            chunk_hash_col.value(0),
            firehose_parquet::encode::encode_base58(&[0x10; 32])
        );
        assert_eq!(
            prev_state_root_col.value(0),
            firehose_parquet::encode::encode_base58(&[0x11; 32])
        );

        let transactions_batch = &batches["transactions"];
        let transactions_schema = transactions_batch.schema();
        let tx_hash_col = transactions_batch
            .column(transactions_schema.index_of("hash").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            tx_hash_col.value(0),
            firehose_parquet::encode::encode_base58(&[0x20; 32])
        );

        let receipts_batch = &batches["receipts"];
        let receipts_schema = receipts_batch.schema();
        let receipt_id_col = receipts_batch
            .column(receipts_schema.index_of("receipt_id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            receipt_id_col.value(0),
            firehose_parquet::encode::encode_base58(&[0x30; 32])
        );

        let state_changes_batch = &batches["state_changes"];
        let state_changes_schema = state_changes_batch.schema();
        let key_col = state_changes_batch
            .column(state_changes_schema.index_of("key_base64").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = state_changes_batch
            .column(state_changes_schema.index_of("value_base64").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key_col.value(0), "");
        assert_eq!(value_col.value(0), "");
    }

    // -----------------------------------------------------------------------
    // Join keys, positions, receipt actions and execution logs (#506)
    // -----------------------------------------------------------------------

    fn map_blocks(
        blocks: &[near::Block],
        encoding: EncodeBytes,
        include_failed_transactions: bool,
    ) -> HashMap<String, RecordBatch> {
        let mut mapper = NearBlockMapper::new(false, encoding, include_failed_transactions);
        for block in blocks {
            mapper
                .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
                .unwrap();
        }
        mapper.flush().unwrap()
    }

    /// Every value of `column`, rendered as text, with `None` for nulls.
    fn values(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing column {column}"));
        (0..array.len())
            .map(|row| (!array.is_null(row)).then(|| array_value_to_string(array, row).unwrap()))
            .collect()
    }

    fn some(values: &[&str]) -> Vec<Option<String>> {
        values.iter().map(|v| Some(v.to_string())).collect()
    }

    fn b58(byte: u8) -> String {
        encode_base58(&[byte; 32])
    }

    fn binary_values(batch: &RecordBatch, column: &str) -> Vec<Option<Vec<u8>>> {
        let array = batch
            .column_by_name(column)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap_or_else(|| panic!("{column} should be Binary"));
        array.iter().map(|v| v.map(<[u8]>::to_vec)).collect()
    }

    #[test]
    fn test_transactions_have_position_and_converted_receipt() {
        let block = make_every_action_block(100);

        // Failed transactions are excluded by default and keep their index.
        let batches = map_blocks(std::slice::from_ref(&block), EncodeBytes::Base58, false);
        let txs = &batches["transactions"];
        assert_eq!(values(txs, "hash"), some(&[&b58(0xa1), &b58(0xa3)]));
        assert_eq!(values(txs, "transaction_index"), some(&["0", "2"]));
        assert_eq!(values(txs, "shard_id"), some(&["0", "1"]));
        assert_eq!(
            values(txs, "converted_into_receipt_id"),
            some(&[&b58(0xb1), &b58(0xb9)])
        );
        assert_eq!(
            values(txs, "receipt_ids"),
            some(&[&format!("[{}]", b58(0xb1)), &format!("[{}]", b58(0xb9))])
        );
        assert_eq!(
            values(txs, "tokens_burnt"),
            some(&["100000000000", "100000000000"])
        );

        let batches = map_blocks(&[block], EncodeBytes::Base58, true);
        let txs = &batches["transactions"];
        assert_eq!(values(txs, "transaction_index"), some(&["0", "1", "2"]));
        assert_eq!(values(txs, "status")[1].as_deref(), Some("Failure"));
        assert_eq!(values(txs, "converted_into_receipt_id")[1], None);
        assert_eq!(values(txs, "receipt_ids")[1].as_deref(), Some("[]"));
    }

    #[test]
    fn test_receipts_have_position_origin_signer_and_children() {
        let batches = map_blocks(&[make_every_action_block(100)], EncodeBytes::Base58, false);
        let receipts = &batches["receipts"];
        assert_eq!(
            values(receipts, "receipt_id"),
            some(&[&b58(0xb1), &b58(0xb3), &b58(0xb4)])
        );
        assert_eq!(values(receipts, "receipt_index"), some(&["0", "1", "2"]));
        assert_eq!(values(receipts, "shard_id"), some(&["0", "1", "1"]));
        // Only the local receipt of a same-block transaction has an origin.
        assert_eq!(
            values(receipts, "tx_hash"),
            vec![Some(b58(0xa1)), None, None]
        );
        assert_eq!(
            values(receipts, "signer_id"),
            vec![
                Some("alice.near".to_string()),
                Some("relayer.near".to_string()),
                None
            ]
        );
        assert_eq!(
            values(receipts, "status"),
            some(&["SuccessValue", "Failure", "Unknown"])
        );
        assert_eq!(
            values(receipts, "tokens_burnt"),
            some(&["500000000000", "300000000000", "0"])
        );
        assert_eq!(
            values(receipts, "executor_id"),
            some(&["alice.near", "dex.near", ""])
        );
        assert_eq!(
            values(receipts, "receipt_ids"),
            some(&[&format!("[{}]", b58(0xb2)), "[]", "[]"])
        );
    }

    #[test]
    fn test_transactions_receipts_actions_and_logs_join() {
        let batches = map_blocks(&[make_every_action_block(100)], EncodeBytes::Base58, true);
        let receipt_ids: Vec<_> = values(&batches["receipts"], "receipt_id");

        // transactions.converted_into_receipt_id -> receipts.receipt_id, and
        // receipts.tx_hash -> transactions.hash.
        let txs = &batches["transactions"];
        let hashes = values(txs, "hash");
        let converted = values(txs, "converted_into_receipt_id");
        let receipt_tx_hashes = values(&batches["receipts"], "tx_hash");
        let joined: Vec<_> = converted
            .iter()
            .zip(&hashes)
            .filter_map(|(receipt_id, tx_hash)| {
                let row = receipt_ids.iter().position(|id| id == receipt_id)?;
                Some((tx_hash.clone(), receipt_tx_hashes[row].clone()))
            })
            .collect();
        assert_eq!(joined, vec![(Some(b58(0xa1)), Some(b58(0xa1)))]);

        // Every action and log row belongs to a written receipt, with the
        // same receipt_index and tx_hash.
        for table in ["receipt_actions", "execution_logs"] {
            let batch = &batches[table];
            let receipt_indexes = values(&batches["receipts"], "receipt_index");
            for (row, receipt_id) in values(batch, "receipt_id").iter().enumerate() {
                let receipt_row = receipt_ids
                    .iter()
                    .position(|id| id == receipt_id)
                    .unwrap_or_else(|| panic!("{table} row {row}: unknown receipt"));
                assert_eq!(
                    values(batch, "receipt_index")[row],
                    receipt_indexes[receipt_row],
                    "{table} row {row}"
                );
                assert_eq!(
                    values(batch, "tx_hash")[row],
                    receipt_tx_hashes[receipt_row],
                    "{table} row {row}"
                );
            }
        }
    }

    #[test]
    fn test_receipt_actions_cover_every_action_kind() {
        let batches = map_blocks(&[make_every_action_block(100)], EncodeBytes::Base58, false);
        let actions = &batches["receipt_actions"];
        assert_eq!(
            actions
                .schema()
                .field_with_name("action_kind")
                .unwrap()
                .data_type(),
            &schema::enum_data_type()
        );
        assert_eq!(
            values(actions, "action_kind"),
            some(&[
                "CreateAccount",
                "DeployContract",
                "FunctionCall",
                "Transfer",
                "Stake",
                "AddKey",
                "DeleteKey",
                "DeleteAccount",
                "Delegate",
                "Unknown",
                "FunctionCall",
            ])
        );
        assert_eq!(
            values(actions, "action_index"),
            some(&["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0"])
        );
        let mut receipt_indexes = vec!["0"; 10];
        receipt_indexes.push("1");
        assert_eq!(values(actions, "receipt_index"), some(&receipt_indexes));
        let mut receipt_ids = vec![b58(0xb1); 10];
        receipt_ids.push(b58(0xb3));
        assert_eq!(
            values(actions, "receipt_id"),
            receipt_ids.into_iter().map(Some).collect::<Vec<_>>()
        );
        let mut tx_hashes = vec![Some(b58(0xa1)); 10];
        tx_hashes.push(None);
        assert_eq!(values(actions, "tx_hash"), tx_hashes);
        let mut signers = vec!["alice.near"; 10];
        signers.push("relayer.near");
        assert_eq!(values(actions, "signer_id"), some(&signers));
        let mut predecessors = vec!["alice.near"; 10];
        predecessors.push("relayer.near");
        assert_eq!(values(actions, "predecessor_id"), some(&predecessors));
        let mut receivers = vec!["alice.near"; 10];
        receivers.push("dex.near");
        assert_eq!(values(actions, "receiver_id"), some(&receivers));

        // Payload columns are set only for the kinds that carry them.
        let only = |rows: &[(usize, &str)]| -> Vec<Option<String>> {
            let mut expected = vec![None; 11];
            for (row, value) in rows {
                expected[*row] = Some(value.to_string());
            }
            expected
        };
        assert_eq!(
            values(actions, "method_name"),
            only(&[(2, "ft_transfer"), (10, "swap")])
        );
        assert_eq!(
            values(actions, "gas"),
            only(&[(2, "30000000000000"), (10, "30000000000000")])
        );
        assert_eq!(
            values(actions, "deposit"),
            only(&[(2, "1"), (3, "1000000000000000000000000"), (10, "1")])
        );
        let mut args = vec![None; 11];
        args[2] = Some(br#"{"receiver_id":"carol.near","amount":"5"}"#.to_vec());
        args[10] = Some(b"\x01\x02".to_vec());
        assert_eq!(binary_values(actions, "args"), args);
    }

    #[test]
    fn test_execution_logs_keep_every_log_line() {
        let block = make_every_action_block(100);
        let expected_logs = block.shards[0].receipt_execution_outcomes[0]
            .execution_outcome
            .as_ref()
            .and_then(|eo| eo.outcome.as_ref())
            .unwrap()
            .logs
            .clone();
        let batches = map_blocks(&[block], EncodeBytes::Base58, false);
        let logs = &batches["execution_logs"];
        assert_eq!(
            values(logs, "log"),
            expected_logs.into_iter().map(Some).collect::<Vec<_>>()
        );
        assert!(values(logs, "log")[0]
            .as_deref()
            .unwrap()
            .starts_with("EVENT_JSON:"));
        assert_eq!(values(logs, "log_index"), some(&["0", "1"]));
        assert_eq!(values(logs, "receipt_index"), some(&["0", "0"]));
        assert_eq!(values(logs, "receipt_id"), some(&[&b58(0xb1), &b58(0xb1)]));
        assert_eq!(values(logs, "tx_hash"), some(&[&b58(0xa1), &b58(0xa1)]));
        assert_eq!(values(logs, "shard_id"), some(&["0", "0"]));
        assert_eq!(
            values(logs, "executor_id"),
            some(&["alice.near", "alice.near"])
        );
        assert_eq!(
            values(logs, "predecessor_id"),
            some(&["alice.near", "alice.near"])
        );
    }

    #[test]
    fn test_receipt_tx_hash_is_derived_within_one_block_only() {
        // Receipt 0xb9 comes from transaction 0xa3 in the first block, but
        // runs in the second one. The origin map is not carried across blocks,
        // so the output does not depend on where a run started.
        let first = make_every_action_block(100);
        let mut second = make_test_block(101);
        second.shards[0]
            .chunk
            .as_mut()
            .unwrap()
            .transactions
            .clear();
        second.shards[0].receipt_execution_outcomes[0]
            .receipt
            .as_mut()
            .unwrap()
            .receipt_id = hash(0xb9);

        let batches = map_blocks(&[first, second], EncodeBytes::Base58, false);
        let receipts = &batches["receipts"];
        assert_eq!(values(receipts, "receipt_id")[3], Some(b58(0xb9)));
        assert_eq!(values(receipts, "receipt_index")[3].as_deref(), Some("0"));
        assert_eq!(values(receipts, "tx_hash")[3], None);
    }

    #[test]
    fn test_binary_encoding_keeps_raw_ids_and_args() {
        let batches = map_blocks(&[make_every_action_block(100)], EncodeBytes::Binary, true);
        let txs = &batches["transactions"];
        assert_eq!(
            binary_values(txs, "converted_into_receipt_id"),
            vec![Some(vec![0xb1; 32]), None, Some(vec![0xb9; 32])]
        );
        let receipts = &batches["receipts"];
        assert_eq!(
            receipts
                .schema()
                .field_with_name("receipt_ids")
                .unwrap()
                .data_type(),
            &BytesListColumn::data_type(&EncodeBytes::Binary)
        );
        let children = receipts
            .column_by_name("receipt_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let children = children.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(children.value(0), [0xb2; 32].as_slice());
        assert_eq!(
            binary_values(receipts, "tx_hash"),
            vec![Some(vec![0xa1; 32]), None, None]
        );
        // args are raw bytes under every encoding.
        let actions = &batches["receipt_actions"];
        assert_eq!(
            binary_values(actions, "args")[2].as_deref(),
            Some(br#"{"receiver_id":"carol.near","amount":"5"}"#.as_slice())
        );
    }
}
