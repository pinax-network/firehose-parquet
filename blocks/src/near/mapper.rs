use super::proto::near;
use super::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, EncodeBytes};
use firehose_parquet::traits::{
    est_opt_str, est_str, est_u32, est_u64,
    BlockIdentity, BlockMapper, CanonicalBuilder,
};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

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
    if include { Some(StringBuilder::new()) } else { None }
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
        Some(near::state_change_cause::Cause::ActionReceiptProcessingStarted(_)) => "ActionReceiptProcessingStarted",
        Some(near::state_change_cause::Cause::ActionReceiptGasReward(_)) => "ActionReceiptGasReward",
        Some(near::state_change_cause::Cause::ReceiptProcessing(_)) => "ReceiptProcessing",
        Some(near::state_change_cause::Cause::PostponedReceipt(_)) => "PostponedReceipt",
        Some(near::state_change_cause::Cause::UpdatedDelayedReceipts(_)) => "UpdatedDelayedReceipts",
        Some(near::state_change_cause::Cause::ValidatorAccountsUpdate(_)) => "ValidatorAccountsUpdate",
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
    blocks: BlocksBuilder,
    chunks: ChunksBuilder,
    transactions: TransactionsBuilder,
    receipts: ReceiptsBuilder,
    state_changes: StateChangesBuilder,
    blocks_schema: Schema,
    chunks_schema: Schema,
    transactions_schema: Schema,
    receipts_schema: Schema,
    state_changes_schema: Schema,
}

impl NearBlockMapper {
    pub fn new(include_fork_step: bool, encoding: EncodeBytes) -> Self {
        let enc = &encoding;
        Self {
            blocks: BlocksBuilder::new(include_fork_step, enc),
            chunks: ChunksBuilder::new(include_fork_step, enc),
            transactions: TransactionsBuilder::new(include_fork_step, enc),
            receipts: ReceiptsBuilder::new(include_fork_step, enc),
            state_changes: StateChangesBuilder::new(include_fork_step),
            blocks_schema: schema::blocks_schema(include_fork_step, enc),
            chunks_schema: schema::chunks_schema(include_fork_step, enc),
            transactions_schema: schema::transactions_schema(include_fork_step, enc),
            receipts_schema: schema::receipts_schema(include_fork_step, enc),
            state_changes_schema: schema::state_changes_schema(include_fork_step, enc),
        }
    }

    fn map_near_block(&mut self, block: &near::Block, identity: &BlockIdentity, fork_step: Option<&str>) {
        let header = match &block.header {
            Some(h) => h,
            None => return,
        };

        // --- blocks table ---
        self.blocks.canonical.append(identity);
        self.blocks.height.append_value(header.height);
        self.blocks.hash.append_value(crypto_hash_bytes(&header.hash));
        self.blocks.prev_hash.append_value(crypto_hash_bytes(&header.prev_hash));
        self.blocks.prev_height.append_value(header.prev_height);
        self.blocks.epoch_id.append_value(crypto_hash_bytes(&header.epoch_id));
        self.blocks.author.append_value(&block.author);
        self.blocks.gas_price.append_value(&bigint_to_string(&header.gas_price));
        self.blocks.total_supply.append_value(&bigint_to_string(&header.total_supply));
        self.blocks.chunks_included.append_value(header.chunks_included);
        self.blocks.latest_protocol_version.append_value(header.latest_protocol_version);
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // --- shards: chunks, transactions, receipts ---
        for shard in &block.shards {
            let shard_id = shard.shard_id;

            if let Some(chunk) = &shard.chunk {
                if let Some(chunk_header) = &chunk.header {
                    self.map_chunk(chunk_header, &chunk.author, identity, fork_step);
                }

                for tx_with_outcome in &chunk.transactions {
                    self.map_transaction(shard_id, tx_with_outcome, identity, fork_step);
                }
            }

            for receipt_outcome in &shard.receipt_execution_outcomes {
                self.map_receipt(shard_id, receipt_outcome, identity, fork_step);
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
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.chunks.canonical.append(identity);
        self.chunks.shard_id.append_value(header.shard_id);
        self.chunks.chunk_hash.append_value(&header.chunk_hash);
        self.chunks.prev_state_root.append_value(&header.prev_state_root);
        self.chunks.gas_used.append_value(header.gas_used);
        self.chunks.gas_limit.append_value(header.gas_limit);
        self.chunks.height_created.append_value(header.height_created);
        self.chunks.height_included.append_value(header.height_included);
        self.chunks.encoded_length.append_value(header.encoded_length);
        self.chunks.author.append_value(author);
        append_fork_step(&mut self.chunks.fork_step, fork_step);
    }

    fn map_transaction(
        &mut self,
        shard_id: u64,
        tx_with_outcome: &near::IndexerTransactionWithOutcome,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        let tx = match &tx_with_outcome.transaction {
            Some(t) => t,
            None => return,
        };

        let tx_hash = crypto_hash_bytes(&tx.hash);
        let actions: Vec<&str> = tx.actions.iter().map(|a| action_type_name(a)).collect();
        let actions_str = actions.join(",");

        let (status, gas_burnt) = tx_with_outcome
            .outcome
            .as_ref()
            .and_then(|o| o.execution_outcome.as_ref())
            .and_then(|eo| eo.outcome.as_ref())
            .map(|outcome| (execution_status_str(outcome), outcome.gas_burnt))
            .unwrap_or(("Unknown", 0));

        self.transactions.canonical.append(identity);
        self.transactions.hash.append_value(tx_hash);
        self.transactions.signer_id.append_value(&tx.signer_id);
        self.transactions.receiver_id.append_value(&tx.receiver_id);
        self.transactions.shard_id.append_value(shard_id);
        self.transactions.nonce.append_value(tx.nonce);
        self.transactions.actions.append_value(&actions_str);
        self.transactions.status.append_value(status);
        self.transactions.gas_burnt.append_value(gas_burnt);
        append_fork_step(&mut self.transactions.fork_step, fork_step);
    }

    fn map_receipt(
        &mut self,
        shard_id: u64,
        receipt_outcome: &near::IndexerExecutionOutcomeWithReceipt,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        let receipt = match &receipt_outcome.receipt {
            Some(r) => r,
            None => return,
        };

        let receipt_id = crypto_hash_bytes(&receipt.receipt_id);

        let (status, gas_burnt, executor_id) = receipt_outcome
            .execution_outcome
            .as_ref()
            .and_then(|eo| eo.outcome.as_ref())
            .map(|outcome| (execution_status_str(outcome), outcome.gas_burnt, outcome.executor_id.as_str()))
            .unwrap_or(("Unknown", 0, ""));

        self.receipts.canonical.append(identity);
        self.receipts.receipt_id.append_value(receipt_id);
        self.receipts.predecessor_id.append_value(&receipt.predecessor_id);
        self.receipts.receiver_id.append_value(&receipt.receiver_id);
        self.receipts.shard_id.append_value(shard_id);
        self.receipts.status.append_value(status);
        self.receipts.gas_burnt.append_value(gas_burnt);
        self.receipts.executor_id.append_value(executor_id);
        append_fork_step(&mut self.receipts.fork_step, fork_step);
    }

    fn map_state_change(
        &mut self,
        sc: &near::StateChangeWithCause,
        identity: &BlockIdentity,
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
        self.state_changes.r#type.append_value(state_change_type_name(value));
        self.state_changes.cause.append_value(state_change_cause_name(cause));
        self.state_changes.account_id.append_value(state_change_account_id(value));
        self.state_changes.key_base64.append_value(&state_change_key_base64(value));
        self.state_changes.value_base64.append_value(&state_change_value_base64(value));
        append_fork_step(&mut self.state_changes.fork_step, fork_step);
    }
}

impl BlockMapper for NearBlockMapper {
    fn map_block(&mut self, block_bytes: &[u8], identity: &BlockIdentity, fork_step: Option<&str>) -> anyhow::Result<()> {
        let block = near::Block::decode(block_bytes)?;
        self.map_near_block(&block, identity, fork_step);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        result.insert("blocks".to_string(), self.blocks.finish(&self.blocks_schema)?);
        result.insert("chunks".to_string(), self.chunks.finish(&self.chunks_schema)?);
        result.insert("transactions".to_string(), self.transactions.finish(&self.transactions_schema)?);
        result.insert("receipts".to_string(), self.receipts.finish(&self.receipts_schema)?);
        result.insert("state_changes".to_string(), self.state_changes.finish(&self.state_changes_schema)?);
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        self.blocks.canonical.len()
            .max(self.chunks.canonical.len())
            .max(self.transactions.canonical.len())
            .max(self.receipts.canonical.len())
            .max(self.state_changes.canonical.len())
    }

    fn total_rows(&self) -> usize {
        self.blocks.canonical.len()
            + self.chunks.canonical.len()
            + self.transactions.canonical.len()
            + self.receipts.canonical.len()
            + self.state_changes.canonical.len()
    }

    fn largest_table(&mut self) -> (&str, usize) {
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
            + est_str(&self.transactions.signer_id)
            + est_str(&self.transactions.receiver_id)
            + est_u64(&self.transactions.shard_id)
            + est_u64(&self.transactions.nonce)
            + est_str(&self.transactions.actions)
            + est_str(&self.transactions.status)
            + est_u64(&self.transactions.gas_burnt)
            + est_opt_str(&self.transactions.fork_step);
        let receipts = self.receipts.canonical.estimated_bytes()
            + self.receipts.receipt_id.estimated_bytes()
            + est_str(&self.receipts.predecessor_id)
            + est_str(&self.receipts.receiver_id)
            + est_u64(&self.receipts.shard_id)
            + est_str(&self.receipts.status)
            + est_u64(&self.receipts.gas_burnt)
            + est_str(&self.receipts.executor_id)
            + est_opt_str(&self.receipts.fork_step);
        let state_changes = self.state_changes.canonical.estimated_bytes()
            + est_str(&self.state_changes.r#type)
            + est_str(&self.state_changes.cause)
            + est_str(&self.state_changes.account_id)
            + est_str(&self.state_changes.key_base64)
            + est_str(&self.state_changes.value_base64)
            + est_opt_str(&self.state_changes.fork_step);
        [("blocks", blocks), ("chunks", chunks), ("transactions", transactions), ("receipts", receipts), ("state_changes", state_changes)]
            .into_iter()
            .max_by_key(|&(_, s)| s)
            .unwrap_or(("blocks", 0))
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
    signer_id: StringBuilder,
    receiver_id: StringBuilder,
    shard_id: UInt64Builder,
    nonce: UInt64Builder,
    actions: StringBuilder,
    status: StringBuilder,
    gas_burnt: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            hash: BytesColumn::new(encoding),
            signer_id: StringBuilder::new(),
            receiver_id: StringBuilder::new(),
            shard_id: UInt64Builder::new(),
            nonce: UInt64Builder::new(),
            actions: StringBuilder::new(),
            status: StringBuilder::new(),
            gas_burnt: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            self.hash.finish(),
            Arc::new(self.signer_id.finish()) as Arc<dyn Array>,
            Arc::new(self.receiver_id.finish()) as Arc<dyn Array>,
            Arc::new(self.shard_id.finish()) as Arc<dyn Array>,
            Arc::new(self.nonce.finish()) as Arc<dyn Array>,
            Arc::new(self.actions.finish()) as Arc<dyn Array>,
            Arc::new(self.status.finish()) as Arc<dyn Array>,
            Arc::new(self.gas_burnt.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct ReceiptsBuilder {
    canonical: CanonicalBuilder,
    receipt_id: BytesColumn,
    predecessor_id: StringBuilder,
    receiver_id: StringBuilder,
    shard_id: UInt64Builder,
    status: StringBuilder,
    gas_burnt: UInt64Builder,
    executor_id: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl ReceiptsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            receipt_id: BytesColumn::new(encoding),
            predecessor_id: StringBuilder::new(),
            receiver_id: StringBuilder::new(),
            shard_id: UInt64Builder::new(),
            status: StringBuilder::new(),
            gas_burnt: UInt64Builder::new(),
            executor_id: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            self.receipt_id.finish(),
            Arc::new(self.predecessor_id.finish()) as Arc<dyn Array>,
            Arc::new(self.receiver_id.finish()) as Arc<dyn Array>,
            Arc::new(self.shard_id.finish()) as Arc<dyn Array>,
            Arc::new(self.status.finish()) as Arc<dyn Array>,
            Arc::new(self.gas_burnt.finish()) as Arc<dyn Array>,
            Arc::new(self.executor_id.finish()) as Arc<dyn Array>,
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
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
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
mod tests {
    use super::*;

    fn make_test_block(height: u64) -> near::Block {
        near::Block {
            author: "test.near".to_string(),
            header: Some(near::BlockHeader {
                height,
                prev_height: height.saturating_sub(1),
                epoch_id: Some(near::CryptoHash { bytes: vec![0xaa; 32] }),
                next_epoch_id: Some(near::CryptoHash { bytes: vec![0xbb; 32] }),
                hash: Some(near::CryptoHash { bytes: vec![0x01; 32] }),
                prev_hash: Some(near::CryptoHash { bytes: vec![0x02; 32] }),
                prev_state_root: Some(near::CryptoHash { bytes: vec![0x03; 32] }),
                timestamp: 1_700_000_000,
                timestamp_nanosec: 1_700_000_000_000_000_000,
                gas_price: Some(near::BigInt { bytes: vec![0x05, 0xF5, 0xE1, 0x00] }), // 100_000_000
                total_supply: Some(near::BigInt { bytes: vec![0x01, 0x00] }), // 256
                chunks_included: 4,
                latest_protocol_version: 60,
                ..Default::default()
            }),
            shards: vec![near::IndexerShard {
                shard_id: 0,
                chunk: Some(near::IndexerChunk {
                    author: "chunk_producer.near".to_string(),
                    header: Some(near::ChunkHeader {
                        chunk_hash: vec![0x10; 32],
                        prev_state_root: vec![0x11; 32],
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
                            hash: Some(near::CryptoHash { bytes: vec![0x20; 32] }),
                            actions: vec![
                                near::Action { action: Some(near::action::Action::Transfer(near::TransferAction {
                                    deposit: Some(near::BigInt { bytes: vec![0x01] }),
                                })) },
                            ],
                            ..Default::default()
                        }),
                        outcome: Some(near::IndexerExecutionOutcomeWithOptionalReceipt {
                            execution_outcome: Some(near::ExecutionOutcomeWithId {
                                outcome: Some(near::ExecutionOutcome {
                                    gas_burnt: 2_428_000_000_000,
                                    executor_id: "alice.near".to_string(),
                                    status: Some(near::execution_outcome::Status::SuccessReceiptId(
                                        near::SuccessReceiptIdExecutionStatus {
                                            id: Some(near::CryptoHash { bytes: vec![0x30; 32] }),
                                        },
                                    )),
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
                            gas_burnt: 2_428_000_000_000,
                            executor_id: "bob.near".to_string(),
                            status: Some(near::execution_outcome::Status::SuccessValue(
                                near::SuccessValueExecutionStatus { value: vec![] },
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    receipt: Some(near::Receipt {
                        predecessor_id: "alice.near".to_string(),
                        receiver_id: "bob.near".to_string(),
                        receipt_id: Some(near::CryptoHash { bytes: vec![0x30; 32] }),
                        receipt: None,
                    }),
                }],
            }],
            state_changes: vec![near::StateChangeWithCause {
                value: Some(near::StateChangeValue {
                    value: Some(near::state_change_value::Value::AccountUpdate(
                        near::state_change_value::AccountUpdate {
                            account_id: "bob.near".to_string(),
                            account: Some(near::Account {
                                amount: Some(near::BigInt { bytes: vec![0x01] }),
                                locked: Some(near::BigInt { bytes: vec![] }),
                                code_hash: Some(near::CryptoHash { bytes: vec![0x00; 32] }),
                                storage_usage: 100,
                            }),
                        },
                    )),
                }),
                cause: Some(near::StateChangeCause {
                    cause: Some(near::state_change_cause::Cause::TransactionProcessing(
                        near::state_change_cause::TransactionProcessing {
                            tx_hash: Some(near::CryptoHash { bytes: vec![0x20; 32] }),
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
        let mut mapper = NearBlockMapper::new(false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["chunks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["receipts"].num_rows(), 1);
        assert_eq!(batches["state_changes"].num_rows(), 1);
    }

    #[test]
    fn test_empty_block() {
        let block = near::Block {
            author: "test.near".to_string(),
            header: Some(near::BlockHeader {
                height: 1,
                hash: Some(near::CryptoHash { bytes: vec![0x01; 32] }),
                prev_hash: Some(near::CryptoHash { bytes: vec![0x00; 32] }),
                timestamp: 1_700_000_000,
                ..Default::default()
            }),
            shards: vec![],
            state_changes: vec![],
            chunk_headers: vec![],
        };
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = NearBlockMapper::new(false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["chunks"].num_rows(), 0);
        assert_eq!(batches["transactions"].num_rows(), 0);
        assert_eq!(batches["receipts"].num_rows(), 0);
        assert_eq!(batches["state_changes"].num_rows(), 0);
    }

    #[test]
    fn test_flush_resets() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = NearBlockMapper::new(false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_table_names() {
        let mapper = NearBlockMapper::new(false, EncodeBytes::Hex);
        assert_eq!(mapper.table_names().len(), 5);
        assert!(mapper.table_names().contains(&"blocks"));
        assert!(mapper.table_names().contains(&"chunks"));
        assert!(mapper.table_names().contains(&"transactions"));
        assert!(mapper.table_names().contains(&"receipts"));
        assert!(mapper.table_names().contains(&"state_changes"));
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = NearBlockMapper::new(true, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), Some("FINAL")).unwrap();

        let batches = mapper.flush().unwrap();
        let blocks_batch = &batches["blocks"];
        let last_col = blocks_batch.num_columns() - 1;
        assert_eq!(blocks_batch.schema().field(last_col).name(), "fork_step");
        let fork_col = blocks_batch.column(last_col).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(fork_col.value(0), "FINAL");
    }

    #[test]
    fn test_state_change_data_update() {
        let block = near::Block {
            author: "test.near".to_string(),
            header: Some(near::BlockHeader {
                height: 200,
                hash: Some(near::CryptoHash { bytes: vec![0x01; 32] }),
                prev_hash: Some(near::CryptoHash { bytes: vec![0x00; 32] }),
                timestamp: 1_700_000_000,
                ..Default::default()
            }),
            shards: vec![],
            state_changes: vec![near::StateChangeWithCause {
                value: Some(near::StateChangeValue {
                    value: Some(near::state_change_value::Value::DataUpdate(
                        near::state_change_value::DataUpdate {
                            account_id: "contract.near".to_string(),
                            key: b"mykey".to_vec(),
                            value: b"myvalue".to_vec(),
                        },
                    )),
                }),
                cause: Some(near::StateChangeCause {
                    cause: Some(near::state_change_cause::Cause::ReceiptProcessing(
                        near::state_change_cause::ReceiptProcessing {
                            tx_hash: Some(near::CryptoHash { bytes: vec![0x20; 32] }),
                        },
                    )),
                }),
            }],
            chunk_headers: vec![],
        };
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = NearBlockMapper::new(false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let batches = mapper.flush().unwrap();
        let sc_batch = &batches["state_changes"];
        assert_eq!(sc_batch.num_rows(), 1);

        // Verify type and cause
        let type_col = sc_batch.column(6).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(type_col.value(0), "DataUpdate");
        let cause_col = sc_batch.column(7).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(cause_col.value(0), "ReceiptProcessing");
        let account_col = sc_batch.column(8).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(account_col.value(0), "contract.near");
        // key_base64 for "mykey" is "bXlrZXk="
        let key_col = sc_batch.column(9).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(key_col.value(0), "bXlrZXk=");
        // value_base64 for "myvalue" is "bXl2YWx1ZQ=="
        let val_col = sc_batch.column(10).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(val_col.value(0), "bXl2YWx1ZQ==");
    }

    #[test]
    fn test_bigint_to_string() {
        assert_eq!(bigint_to_string(&None), "0");
        assert_eq!(bigint_to_string(&Some(near::BigInt { bytes: vec![] })), "0");
        assert_eq!(bigint_to_string(&Some(near::BigInt { bytes: vec![0x01] })), "1");
        assert_eq!(bigint_to_string(&Some(near::BigInt { bytes: vec![0x01, 0x00] })), "256");
        // 100_000_000 = 0x05F5E100
        assert_eq!(bigint_to_string(&Some(near::BigInt { bytes: vec![0x05, 0xF5, 0xE1, 0x00] })), "100000000");
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
}
