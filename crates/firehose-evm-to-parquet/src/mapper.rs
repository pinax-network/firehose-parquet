use crate::proto::eth;
use crate::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::traits::{BlockIdentity, BlockMapper, CanonicalBuilder};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn bigint_to_string(bi: &Option<eth::BigInt>) -> String {
    match bi {
        Some(b) if !b.bytes.is_empty() => {
            let n = num_bigint::BigUint::from_bytes_be(&b.bytes);
            n.to_string()
        }
        _ => "0".to_string(),
    }
}

fn append_fork_step(builder: &mut Option<StringBuilder>, fork_step: Option<&str>) {
    if let Some(ref mut b) = builder {
        b.append_value(fork_step.unwrap_or("UNKNOWN"));
    }
}

fn finish_fork_step(builder: &mut Option<StringBuilder>, columns: &mut Vec<Arc<dyn arrow::array::Array>>) {
    if let Some(ref mut b) = builder {
        columns.push(Arc::new(b.finish()) as Arc<dyn arrow::array::Array>);
    }
}

fn mk_fork_step(include: bool) -> Option<StringBuilder> {
    if include { Some(StringBuilder::new()) } else { None }
}

// ---------------------------------------------------------------------------
// EVM BlockMapper
// ---------------------------------------------------------------------------

pub struct EvmBlockMapper {
    extended: bool,
    include_fork_step: bool,
    // Standard builders
    blocks: EvmBlocksBuilder,
    transactions: EvmTransactionsBuilder,
    logs: EvmLogsBuilder,
    // Extended builders
    calls: Option<EvmCallsBuilder>,
    balance_changes: Option<EvmBalanceChangesBuilder>,
    code_changes: Option<EvmCodeChangesBuilder>,
    storage_changes: Option<EvmStorageChangesBuilder>,
    nonce_changes: Option<EvmNonceChangesBuilder>,
    gas_changes: Option<EvmGasChangesBuilder>,
    account_creations: Option<EvmAccountCreationsBuilder>,
    // Schemas
    blocks_schema: Schema,
    transactions_schema: Schema,
    logs_schema: Schema,
    calls_schema: Schema,
    balance_changes_schema: Schema,
    code_changes_schema: Schema,
    storage_changes_schema: Schema,
    nonce_changes_schema: Schema,
    gas_changes_schema: Schema,
    account_creations_schema: Schema,
}

impl EvmBlockMapper {
    pub fn new(extended: bool, include_fork_step: bool) -> Self {
        let ifs = include_fork_step;
        Self {
            extended,
            include_fork_step,
            blocks: EvmBlocksBuilder::new(ifs),
            transactions: EvmTransactionsBuilder::new(ifs),
            logs: EvmLogsBuilder::new(ifs),
            calls: if extended { Some(EvmCallsBuilder::new(ifs)) } else { None },
            balance_changes: if extended { Some(EvmBalanceChangesBuilder::new(ifs)) } else { None },
            code_changes: if extended { Some(EvmCodeChangesBuilder::new(ifs)) } else { None },
            storage_changes: if extended { Some(EvmStorageChangesBuilder::new(ifs)) } else { None },
            nonce_changes: if extended { Some(EvmNonceChangesBuilder::new(ifs)) } else { None },
            gas_changes: if extended { Some(EvmGasChangesBuilder::new(ifs)) } else { None },
            account_creations: if extended { Some(EvmAccountCreationsBuilder::new(ifs)) } else { None },
            blocks_schema: schema::blocks_schema(ifs),
            transactions_schema: schema::transactions_schema(ifs),
            logs_schema: schema::logs_schema(ifs),
            calls_schema: schema::calls_schema(ifs),
            balance_changes_schema: schema::balance_changes_schema(ifs),
            code_changes_schema: schema::code_changes_schema(ifs),
            storage_changes_schema: schema::storage_changes_schema(ifs),
            nonce_changes_schema: schema::nonce_changes_schema(ifs),
            gas_changes_schema: schema::gas_changes_schema(ifs),
            account_creations_schema: schema::account_creations_schema(ifs),
        }
    }

    fn map_evm_block(&mut self, block: &eth::Block, identity: &BlockIdentity, fork_step: Option<&str>) {
        let number = block.number;
        let block_hash = hex(&block.hash);
        let header = block.header.as_ref();

        // -- blocks table --
        self.blocks.canonical.append(identity);
        self.blocks.number.append_value(number);
        self.blocks.hash.append_value(&block_hash);
        self.blocks.parent_hash.append_value(hex(&header.map_or(&[][..], |h| &h.parent_hash)));
        self.blocks.timestamp.append_value(
            header.and_then(|h| h.timestamp.as_ref()).map_or(0, |t| t.seconds),
        );
        self.blocks.gas_used.append_value(header.map_or(0, |h| h.gas_used));
        self.blocks.gas_limit.append_value(header.map_or(0, |h| h.gas_limit));
        let base_fee = header.and_then(|h| h.base_fee_per_gas.as_ref());
        if base_fee.is_some() {
            self.blocks.base_fee_per_gas.append_value(bigint_to_string(&header.and_then(|h| h.base_fee_per_gas.clone())));
        } else {
            self.blocks.base_fee_per_gas.append_null();
        }
        self.blocks.coinbase.append_value(hex(header.map_or(&[][..], |h| &h.coinbase)));
        self.blocks.size.append_value(block.size);
        self.blocks.nonce.append_value(header.map_or(0, |h| h.nonce));
        self.blocks.state_root.append_value(hex(header.map_or(&[][..], |h| &h.state_root)));
        self.blocks.transactions_root.append_value(hex(header.map_or(&[][..], |h| &h.transactions_root)));
        self.blocks.receipt_root.append_value(hex(header.map_or(&[][..], |h| &h.receipt_root)));
        let difficulty = header.and_then(|h| h.difficulty.as_ref());
        if difficulty.is_some() {
            self.blocks.difficulty.append_value(bigint_to_string(&header.and_then(|h| h.difficulty.clone())));
        } else {
            self.blocks.difficulty.append_null();
        }
        self.blocks.mix_hash.append_value(hex(header.map_or(&[][..], |h| &h.mix_hash)));
        self.blocks.extra_data.append_value(hex(header.map_or(&[][..], |h| &h.extra_data)));
        self.blocks.num_transactions.append_value(block.transaction_traces.len() as u32);
        self.blocks.detail_level.append_value(block.detail_level);
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // -- transaction traces --
        for tx in &block.transaction_traces {
            self.map_transaction(number, &block_hash, tx, identity, fork_step);
        }

        // -- block-level balance changes (EXTENDED) --
        if self.extended {
            for bc in &block.balance_changes {
                if let Some(ref mut builder) = self.balance_changes {
                    builder.append(number, "", bc, identity, fork_step);
                }
            }
            for cc in &block.code_changes {
                if let Some(ref mut builder) = self.code_changes {
                    builder.append(number, "", cc, identity, fork_step);
                }
            }
            for call in &block.system_calls {
                if let Some(ref mut builder) = self.calls {
                    builder.append(number, "", 0, call, identity, fork_step);
                }
                self.extract_call_state_changes(number, "", call, identity, fork_step);
            }
        }
    }

    fn map_transaction(&mut self, block_number: u64, block_hash: &str, tx: &eth::TransactionTrace, identity: &BlockIdentity, fork_step: Option<&str>) {
        let tx_hash = hex(&tx.hash);
        let _ = block_hash;

        self.transactions.canonical.append(identity);
        self.transactions.block_number.append_value(block_number);
        self.transactions.index.append_value(tx.index);
        self.transactions.hash.append_value(&tx_hash);
        self.transactions.from.append_value(hex(&tx.from));
        self.transactions.to.append_value(hex(&tx.to));
        self.transactions.value.append_value(bigint_to_string(&tx.value));
        self.transactions.gas_limit.append_value(tx.gas_limit);
        self.transactions.gas_used.append_value(tx.gas_used);
        if let Some(ref gp) = tx.gas_price {
            self.transactions.gas_price.append_value(bigint_to_string(&Some(gp.clone())));
        } else {
            self.transactions.gas_price.append_null();
        }
        self.transactions.r#type.append_value(tx.r#type);
        self.transactions.status.append_value(tx.status);
        self.transactions.nonce.append_value(tx.nonce);
        self.transactions.input.append_value(hex(&tx.input));
        if let Some(ref mfpg) = tx.max_fee_per_gas {
            self.transactions.max_fee_per_gas.append_value(bigint_to_string(&Some(mfpg.clone())));
        } else {
            self.transactions.max_fee_per_gas.append_null();
        }
        if let Some(ref mpfpg) = tx.max_priority_fee_per_gas {
            self.transactions.max_priority_fee_per_gas.append_value(bigint_to_string(&Some(mpfpg.clone())));
        } else {
            self.transactions.max_priority_fee_per_gas.append_null();
        }
        if let Some(ref receipt) = tx.receipt {
            self.transactions.cumulative_gas_used.append_value(receipt.cumulative_gas_used);
            for log in &receipt.logs {
                self.map_log(block_number, &tx_hash, tx.index, log, identity, fork_step);
            }
        } else {
            self.transactions.cumulative_gas_used.append_null();
        }
        append_fork_step(&mut self.transactions.fork_step, fork_step);

        // Extended: calls and their nested state changes
        if self.extended {
            for call in &tx.calls {
                if let Some(ref mut builder) = self.calls {
                    builder.append(block_number, &tx_hash, tx.index, call, identity, fork_step);
                }
                self.extract_call_state_changes(block_number, &tx_hash, call, identity, fork_step);
            }
        }
    }

    fn map_log(&mut self, block_number: u64, tx_hash: &str, tx_index: u32, log: &eth::Log, identity: &BlockIdentity, fork_step: Option<&str>) {
        self.logs.canonical.append(identity);
        self.logs.block_number.append_value(block_number);
        self.logs.tx_hash.append_value(tx_hash);
        self.logs.tx_index.append_value(tx_index);
        self.logs.log_index.append_value(log.index);
        self.logs.block_index.append_value(log.block_index);
        self.logs.address.append_value(hex(&log.address));

        let topics = &log.topics;
        for i in 0..4 {
            let builder = match i {
                0 => &mut self.logs.topic0,
                1 => &mut self.logs.topic1,
                2 => &mut self.logs.topic2,
                3 => &mut self.logs.topic3,
                _ => unreachable!(),
            };
            if i < topics.len() {
                builder.append_value(hex(&topics[i]));
            } else {
                builder.append_null();
            }
        }
        self.logs.data.append_value(hex(&log.data));
        append_fork_step(&mut self.logs.fork_step, fork_step);
    }

    fn extract_call_state_changes(&mut self, block_number: u64, tx_hash: &str, call: &eth::Call, identity: &BlockIdentity, fork_step: Option<&str>) {
        if !self.extended {
            return;
        }

        for bc in &call.balance_changes {
            if let Some(ref mut builder) = self.balance_changes {
                builder.append(block_number, tx_hash, bc, identity, fork_step);
            }
        }
        for cc in &call.code_changes {
            if let Some(ref mut builder) = self.code_changes {
                builder.append(block_number, tx_hash, cc, identity, fork_step);
            }
        }
        for sc in &call.storage_changes {
            if let Some(ref mut builder) = self.storage_changes {
                builder.append(block_number, tx_hash, sc, identity, fork_step);
            }
        }
        for nc in &call.nonce_changes {
            if let Some(ref mut builder) = self.nonce_changes {
                builder.append(block_number, tx_hash, nc, identity, fork_step);
            }
        }
        for gc in &call.gas_changes {
            if let Some(ref mut builder) = self.gas_changes {
                builder.append(block_number, tx_hash, gc, identity, fork_step);
            }
        }
        #[allow(deprecated)]
        for ac in &call.account_creations {
            if let Some(ref mut builder) = self.account_creations {
                builder.append(block_number, tx_hash, ac, identity, fork_step);
            }
        }
    }
}

impl BlockMapper for EvmBlockMapper {
    fn map_block(&mut self, block_bytes: &[u8], identity: &BlockIdentity, fork_step: Option<&str>) -> anyhow::Result<()> {
        let block = eth::Block::decode(block_bytes)?;
        self.map_evm_block(&block, identity, fork_step);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        result.insert("blocks".to_string(), self.blocks.finish(&self.blocks_schema)?);
        result.insert("transactions".to_string(), self.transactions.finish(&self.transactions_schema)?);
        result.insert("logs".to_string(), self.logs.finish(&self.logs_schema)?);

        if self.extended {
            if let Some(ref mut b) = self.calls {
                result.insert("calls".to_string(), b.finish(&self.calls_schema)?);
            }
            if let Some(ref mut b) = self.balance_changes {
                result.insert("balance_changes".to_string(), b.finish(&self.balance_changes_schema)?);
            }
            if let Some(ref mut b) = self.code_changes {
                result.insert("code_changes".to_string(), b.finish(&self.code_changes_schema)?);
            }
            if let Some(ref mut b) = self.storage_changes {
                result.insert("storage_changes".to_string(), b.finish(&self.storage_changes_schema)?);
            }
            if let Some(ref mut b) = self.nonce_changes {
                result.insert("nonce_changes".to_string(), b.finish(&self.nonce_changes_schema)?);
            }
            if let Some(ref mut b) = self.gas_changes {
                result.insert("gas_changes".to_string(), b.finish(&self.gas_changes_schema)?);
            }
            if let Some(ref mut b) = self.account_creations {
                result.insert("account_creations".to_string(), b.finish(&self.account_creations_schema)?);
            }
        }
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        let mut max = self.blocks.canonical.len()
            .max(self.transactions.canonical.len())
            .max(self.logs.canonical.len());
        if let Some(ref b) = self.calls { max = max.max(b.canonical.len()); }
        if let Some(ref b) = self.balance_changes { max = max.max(b.canonical.len()); }
        if let Some(ref b) = self.storage_changes { max = max.max(b.canonical.len()); }
        if let Some(ref b) = self.nonce_changes { max = max.max(b.canonical.len()); }
        if let Some(ref b) = self.gas_changes { max = max.max(b.canonical.len()); }
        if let Some(ref b) = self.code_changes { max = max.max(b.canonical.len()); }
        if let Some(ref b) = self.account_creations { max = max.max(b.canonical.len()); }
        max
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
// Builders
// ===========================================================================

struct EvmBlocksBuilder {
    canonical: CanonicalBuilder,
    number: UInt64Builder,
    hash: StringBuilder,
    parent_hash: StringBuilder,
    timestamp: Int64Builder,
    gas_used: UInt64Builder,
    gas_limit: UInt64Builder,
    base_fee_per_gas: StringBuilder,
    coinbase: StringBuilder,
    size: UInt64Builder,
    nonce: UInt64Builder,
    state_root: StringBuilder,
    transactions_root: StringBuilder,
    receipt_root: StringBuilder,
    difficulty: StringBuilder,
    mix_hash: StringBuilder,
    extra_data: StringBuilder,
    num_transactions: UInt32Builder,
    detail_level: Int32Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmBlocksBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            number: UInt64Builder::new(),
            hash: StringBuilder::new(),
            parent_hash: StringBuilder::new(),
            timestamp: Int64Builder::new(),
            gas_used: UInt64Builder::new(),
            gas_limit: UInt64Builder::new(),
            base_fee_per_gas: StringBuilder::new(),
            coinbase: StringBuilder::new(),
            size: UInt64Builder::new(),
            nonce: UInt64Builder::new(),
            state_root: StringBuilder::new(),
            transactions_root: StringBuilder::new(),
            receipt_root: StringBuilder::new(),
            difficulty: StringBuilder::new(),
            mix_hash: StringBuilder::new(),
            extra_data: StringBuilder::new(),
            num_transactions: UInt32Builder::new(),
            detail_level: Int32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.hash.finish()),
            Arc::new(self.parent_hash.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.gas_used.finish()),
            Arc::new(self.gas_limit.finish()),
            Arc::new(self.base_fee_per_gas.finish()),
            Arc::new(self.coinbase.finish()),
            Arc::new(self.size.finish()),
            Arc::new(self.nonce.finish()),
            Arc::new(self.state_root.finish()),
            Arc::new(self.transactions_root.finish()),
            Arc::new(self.receipt_root.finish()),
            Arc::new(self.difficulty.finish()),
            Arc::new(self.mix_hash.finish()),
            Arc::new(self.extra_data.finish()),
            Arc::new(self.num_transactions.finish()),
            Arc::new(self.detail_level.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmTransactionsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    index: UInt32Builder,
    hash: StringBuilder,
    from: StringBuilder,
    to: StringBuilder,
    value: StringBuilder,
    gas_limit: UInt64Builder,
    gas_used: UInt64Builder,
    gas_price: StringBuilder,
    r#type: Int32Builder,
    status: Int32Builder,
    nonce: UInt64Builder,
    input: StringBuilder,
    max_fee_per_gas: StringBuilder,
    max_priority_fee_per_gas: StringBuilder,
    cumulative_gas_used: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmTransactionsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            index: UInt32Builder::new(),
            hash: StringBuilder::new(),
            from: StringBuilder::new(),
            to: StringBuilder::new(),
            value: StringBuilder::new(),
            gas_limit: UInt64Builder::new(),
            gas_used: UInt64Builder::new(),
            gas_price: StringBuilder::new(),
            r#type: Int32Builder::new(),
            status: Int32Builder::new(),
            nonce: UInt64Builder::new(),
            input: StringBuilder::new(),
            max_fee_per_gas: StringBuilder::new(),
            max_priority_fee_per_gas: StringBuilder::new(),
            cumulative_gas_used: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.index.finish()),
            Arc::new(self.hash.finish()),
            Arc::new(self.from.finish()),
            Arc::new(self.to.finish()),
            Arc::new(self.value.finish()),
            Arc::new(self.gas_limit.finish()),
            Arc::new(self.gas_used.finish()),
            Arc::new(self.gas_price.finish()),
            Arc::new(self.r#type.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.nonce.finish()),
            Arc::new(self.input.finish()),
            Arc::new(self.max_fee_per_gas.finish()),
            Arc::new(self.max_priority_fee_per_gas.finish()),
            Arc::new(self.cumulative_gas_used.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmLogsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: StringBuilder,
    tx_index: UInt32Builder,
    log_index: UInt32Builder,
    block_index: UInt32Builder,
    address: StringBuilder,
    topic0: StringBuilder,
    topic1: StringBuilder,
    topic2: StringBuilder,
    topic3: StringBuilder,
    data: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmLogsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: StringBuilder::new(),
            tx_index: UInt32Builder::new(),
            log_index: UInt32Builder::new(),
            block_index: UInt32Builder::new(),
            address: StringBuilder::new(),
            topic0: StringBuilder::new(),
            topic1: StringBuilder::new(),
            topic2: StringBuilder::new(),
            topic3: StringBuilder::new(),
            data: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.tx_hash.finish()),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.log_index.finish()),
            Arc::new(self.block_index.finish()),
            Arc::new(self.address.finish()),
            Arc::new(self.topic0.finish()),
            Arc::new(self.topic1.finish()),
            Arc::new(self.topic2.finish()),
            Arc::new(self.topic3.finish()),
            Arc::new(self.data.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmCallsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: StringBuilder,
    tx_index: UInt32Builder,
    call_index: UInt32Builder,
    parent_index: UInt32Builder,
    depth: UInt32Builder,
    call_type: Int32Builder,
    caller: StringBuilder,
    address: StringBuilder,
    value: StringBuilder,
    gas_limit: UInt64Builder,
    gas_consumed: UInt64Builder,
    input: StringBuilder,
    output: StringBuilder,
    status_failed: BooleanBuilder,
    status_reverted: BooleanBuilder,
    state_reverted: BooleanBuilder,
    executed_code: BooleanBuilder,
    suicide: BooleanBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmCallsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: StringBuilder::new(),
            tx_index: UInt32Builder::new(),
            call_index: UInt32Builder::new(),
            parent_index: UInt32Builder::new(),
            depth: UInt32Builder::new(),
            call_type: Int32Builder::new(),
            caller: StringBuilder::new(),
            address: StringBuilder::new(),
            value: StringBuilder::new(),
            gas_limit: UInt64Builder::new(),
            gas_consumed: UInt64Builder::new(),
            input: StringBuilder::new(),
            output: StringBuilder::new(),
            status_failed: BooleanBuilder::new(),
            status_reverted: BooleanBuilder::new(),
            state_reverted: BooleanBuilder::new(),
            executed_code: BooleanBuilder::new(),
            suicide: BooleanBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(&mut self, block_number: u64, tx_hash: &str, tx_index: u32, call: &eth::Call, identity: &BlockIdentity, fork_step: Option<&str>) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
        self.tx_index.append_value(tx_index);
        self.call_index.append_value(call.index);
        self.parent_index.append_value(call.parent_index);
        self.depth.append_value(call.depth);
        self.call_type.append_value(call.call_type);
        self.caller.append_value(hex(&call.caller));
        self.address.append_value(hex(&call.address));
        self.value.append_value(bigint_to_string(&call.value));
        self.gas_limit.append_value(call.gas_limit);
        self.gas_consumed.append_value(call.gas_consumed);
        self.input.append_value(hex(&call.input));
        self.output.append_value(hex(&call.return_data));
        self.status_failed.append_value(call.status_failed);
        self.status_reverted.append_value(call.status_reverted);
        self.state_reverted.append_value(call.state_reverted);
        self.executed_code.append_value(call.executed_code);
        self.suicide.append_value(call.suicide);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.tx_hash.finish()),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.call_index.finish()),
            Arc::new(self.parent_index.finish()),
            Arc::new(self.depth.finish()),
            Arc::new(self.call_type.finish()),
            Arc::new(self.caller.finish()),
            Arc::new(self.address.finish()),
            Arc::new(self.value.finish()),
            Arc::new(self.gas_limit.finish()),
            Arc::new(self.gas_consumed.finish()),
            Arc::new(self.input.finish()),
            Arc::new(self.output.finish()),
            Arc::new(self.status_failed.finish()),
            Arc::new(self.status_reverted.finish()),
            Arc::new(self.state_reverted.finish()),
            Arc::new(self.executed_code.finish()),
            Arc::new(self.suicide.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmBalanceChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: StringBuilder,
    ordinal: UInt64Builder,
    address: StringBuilder,
    old_value: StringBuilder,
    new_value: StringBuilder,
    reason: Int32Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmBalanceChangesBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: StringBuilder::new(),
            ordinal: UInt64Builder::new(),
            address: StringBuilder::new(),
            old_value: StringBuilder::new(),
            new_value: StringBuilder::new(),
            reason: Int32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(&mut self, block_number: u64, tx_hash: &str, bc: &eth::BalanceChange, identity: &BlockIdentity, fork_step: Option<&str>) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        if tx_hash.is_empty() {
            self.tx_hash.append_null();
        } else {
            self.tx_hash.append_value(tx_hash);
        }
        self.ordinal.append_value(bc.ordinal);
        self.address.append_value(hex(&bc.address));
        self.old_value.append_value(bigint_to_string(&bc.old_value));
        self.new_value.append_value(bigint_to_string(&bc.new_value));
        self.reason.append_value(bc.reason);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.tx_hash.finish()),
            Arc::new(self.ordinal.finish()),
            Arc::new(self.address.finish()),
            Arc::new(self.old_value.finish()),
            Arc::new(self.new_value.finish()),
            Arc::new(self.reason.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmCodeChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: StringBuilder,
    ordinal: UInt64Builder,
    address: StringBuilder,
    old_hash: StringBuilder,
    new_hash: StringBuilder,
    old_code: StringBuilder,
    new_code: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmCodeChangesBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: StringBuilder::new(),
            ordinal: UInt64Builder::new(),
            address: StringBuilder::new(),
            old_hash: StringBuilder::new(),
            new_hash: StringBuilder::new(),
            old_code: StringBuilder::new(),
            new_code: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(&mut self, block_number: u64, tx_hash: &str, cc: &eth::CodeChange, identity: &BlockIdentity, fork_step: Option<&str>) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        if tx_hash.is_empty() {
            self.tx_hash.append_null();
        } else {
            self.tx_hash.append_value(tx_hash);
        }
        self.ordinal.append_value(cc.ordinal);
        self.address.append_value(hex(&cc.address));
        self.old_hash.append_value(hex(&cc.old_hash));
        self.new_hash.append_value(hex(&cc.new_hash));
        self.old_code.append_value(hex(&cc.old_code));
        self.new_code.append_value(hex(&cc.new_code));
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.tx_hash.finish()),
            Arc::new(self.ordinal.finish()),
            Arc::new(self.address.finish()),
            Arc::new(self.old_hash.finish()),
            Arc::new(self.new_hash.finish()),
            Arc::new(self.old_code.finish()),
            Arc::new(self.new_code.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmStorageChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: StringBuilder,
    ordinal: UInt64Builder,
    address: StringBuilder,
    key: StringBuilder,
    old_value: StringBuilder,
    new_value: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmStorageChangesBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: StringBuilder::new(),
            ordinal: UInt64Builder::new(),
            address: StringBuilder::new(),
            key: StringBuilder::new(),
            old_value: StringBuilder::new(),
            new_value: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(&mut self, block_number: u64, tx_hash: &str, sc: &eth::StorageChange, identity: &BlockIdentity, fork_step: Option<&str>) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
        self.ordinal.append_value(sc.ordinal);
        self.address.append_value(hex(&sc.address));
        self.key.append_value(hex(&sc.key));
        self.old_value.append_value(hex(&sc.old_value));
        self.new_value.append_value(hex(&sc.new_value));
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.tx_hash.finish()),
            Arc::new(self.ordinal.finish()),
            Arc::new(self.address.finish()),
            Arc::new(self.key.finish()),
            Arc::new(self.old_value.finish()),
            Arc::new(self.new_value.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmNonceChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: StringBuilder,
    ordinal: UInt64Builder,
    address: StringBuilder,
    old_value: UInt64Builder,
    new_value: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmNonceChangesBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: StringBuilder::new(),
            ordinal: UInt64Builder::new(),
            address: StringBuilder::new(),
            old_value: UInt64Builder::new(),
            new_value: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(&mut self, block_number: u64, tx_hash: &str, nc: &eth::NonceChange, identity: &BlockIdentity, fork_step: Option<&str>) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
        self.ordinal.append_value(nc.ordinal);
        self.address.append_value(hex(&nc.address));
        self.old_value.append_value(nc.old_value);
        self.new_value.append_value(nc.new_value);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.tx_hash.finish()),
            Arc::new(self.ordinal.finish()),
            Arc::new(self.address.finish()),
            Arc::new(self.old_value.finish()),
            Arc::new(self.new_value.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmGasChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: StringBuilder,
    ordinal: UInt64Builder,
    old_value: UInt64Builder,
    new_value: UInt64Builder,
    reason: Int32Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmGasChangesBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: StringBuilder::new(),
            ordinal: UInt64Builder::new(),
            old_value: UInt64Builder::new(),
            new_value: UInt64Builder::new(),
            reason: Int32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(&mut self, block_number: u64, tx_hash: &str, gc: &eth::GasChange, identity: &BlockIdentity, fork_step: Option<&str>) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
        self.ordinal.append_value(gc.ordinal);
        self.old_value.append_value(gc.old_value);
        self.new_value.append_value(gc.new_value);
        self.reason.append_value(gc.reason);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.tx_hash.finish()),
            Arc::new(self.ordinal.finish()),
            Arc::new(self.old_value.finish()),
            Arc::new(self.new_value.finish()),
            Arc::new(self.reason.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmAccountCreationsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: StringBuilder,
    ordinal: UInt64Builder,
    account: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl EvmAccountCreationsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: StringBuilder::new(),
            ordinal: UInt64Builder::new(),
            account: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(&mut self, block_number: u64, tx_hash: &str, ac: &eth::AccountCreation, identity: &BlockIdentity, fork_step: Option<&str>) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
        self.ordinal.append_value(ac.ordinal);
        self.account.append_value(hex(&ac.account));
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.tx_hash.finish()),
            Arc::new(self.ordinal.finish()),
            Arc::new(self.account.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

// ===========================================================================
// Hex encoding module
// ===========================================================================
mod hex {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(HEX_CHARS[(b >> 4) as usize] as char);
            s.push(HEX_CHARS[(b & 0x0f) as usize] as char);
        }
        s
    }
}

mod num_bigint {
    pub struct BigUint {
        bytes: Vec<u8>,
    }

    impl BigUint {
        pub fn from_bytes_be(bytes: &[u8]) -> Self {
            Self { bytes: bytes.to_vec() }
        }

        pub fn to_string(&self) -> String {
            if self.bytes.is_empty() {
                return "0".to_string();
            }
            let mut result = vec![0u8];
            for &byte in &self.bytes {
                let mut carry = 0u16;
                for digit in result.iter_mut().rev() {
                    let val = (*digit as u16) * 256 + carry;
                    *digit = (val % 10) as u8;
                    carry = val / 10;
                }
                while carry > 0 {
                    result.insert(0, (carry % 10) as u8);
                    carry /= 10;
                }
                let mut carry = byte as u16;
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
            while result.len() > 1 && result[0] == 0 {
                result.remove(0);
            }
            result.into_iter().map(|d| (b'0' + d) as char).collect()
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_evm_block(number: u64) -> eth::Block {
        eth::Block {
            ver: 4,
            hash: vec![0xab; 32],
            number,
            size: 1000,
            header: Some(eth::BlockHeader {
                parent_hash: vec![0xcd; 32],
                uncle_hash: vec![],
                coinbase: vec![0x01; 20],
                state_root: vec![0x02; 32],
                transactions_root: vec![0x03; 32],
                receipt_root: vec![0x04; 32],
                logs_bloom: vec![],
                difficulty: Some(eth::BigInt { bytes: vec![0x01] }),
                total_difficulty: None,
                number,
                gas_limit: 30_000_000,
                gas_used: 21_000,
                timestamp: Some(prost_types::Timestamp { seconds: 1700000000, nanos: 0 }),
                extra_data: vec![],
                mix_hash: vec![0x05; 32],
                nonce: 0,
                hash: vec![0xab; 32],
                base_fee_per_gas: Some(eth::BigInt { bytes: vec![0x3B, 0x9A, 0xCA, 0x00] }),
                withdrawals_root: vec![],
                tx_dependency: None,
                blob_gas_used: None,
                excess_blob_gas: None,
                parent_beacon_root: vec![],
                requests_hash: vec![],
            }),
            uncles: vec![],
            transaction_traces: vec![eth::TransactionTrace {
                to: vec![0xaa; 20],
                nonce: 1,
                gas_price: Some(eth::BigInt { bytes: vec![0x3B, 0x9A, 0xCA, 0x00] }),
                gas_limit: 21000,
                value: Some(eth::BigInt { bytes: vec![0x01] }),
                input: vec![],
                v: vec![],
                r: vec![],
                s: vec![],
                gas_used: 21000,
                r#type: 0,
                access_list: vec![],
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                index: 0,
                hash: vec![0xbb; 32],
                from: vec![0xcc; 20],
                return_data: vec![],
                public_key: vec![],
                begin_ordinal: 0,
                end_ordinal: 10,
                status: 1,
                receipt: Some(eth::TransactionReceipt {
                    state_root: vec![],
                    cumulative_gas_used: 21000,
                    logs_bloom: vec![],
                    logs: vec![eth::Log {
                        address: vec![0xdd; 20],
                        topics: vec![vec![0xee; 32], vec![0xff; 32]],
                        data: vec![1, 2, 3],
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
                    caller: vec![0xcc; 20],
                    address: vec![0xaa; 20],
                    address_delegates_to: None,
                    value: Some(eth::BigInt { bytes: vec![0x01] }),
                    gas_limit: 21000,
                    gas_consumed: 21000,
                    return_data: vec![],
                    input: vec![],
                    executed_code: false,
                    suicide: false,
                    keccak_preimages: Default::default(),
                    storage_changes: vec![],
                    balance_changes: vec![eth::BalanceChange {
                        address: vec![0xcc; 20],
                        old_value: Some(eth::BigInt { bytes: vec![0x01] }),
                        new_value: Some(eth::BigInt { bytes: vec![0x00] }),
                        reason: 5,
                        ordinal: 3,
                    }],
                    nonce_changes: vec![eth::NonceChange {
                        address: vec![0xcc; 20],
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

    #[test]
    fn test_base_map_and_flush() {
        let block = make_test_evm_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(false, false);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();

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
        let mut mapper = EvmBlockMapper::new(true, false);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["logs"].num_rows(), 1);
        assert_eq!(batches["calls"].num_rows(), 1);
        assert_eq!(batches["balance_changes"].num_rows(), 1);
        assert_eq!(batches["nonce_changes"].num_rows(), 1);
        assert_eq!(batches["gas_changes"].num_rows(), 1);
    }

    #[test]
    fn test_flush_resets() {
        let block = make_test_evm_block(1);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(true, false);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_empty_block() {
        let block = eth::Block {
            ver: 4,
            hash: vec![0x00; 32],
            number: 0,
            size: 0,
            header: Some(eth::BlockHeader {
                parent_hash: vec![],
                uncle_hash: vec![],
                coinbase: vec![],
                state_root: vec![],
                transactions_root: vec![],
                receipt_root: vec![],
                logs_bloom: vec![],
                difficulty: None,
                total_difficulty: None,
                number: 0,
                gas_limit: 0,
                gas_used: 0,
                timestamp: None,
                extra_data: vec![],
                mix_hash: vec![],
                nonce: 0,
                hash: vec![],
                base_fee_per_gas: None,
                withdrawals_root: vec![],
                tx_dependency: None,
                blob_gas_used: None,
                excess_blob_gas: None,
                parent_beacon_root: vec![],
                requests_hash: vec![],
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
        let mut mapper = EvmBlockMapper::new(false, false);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 0);
        assert_eq!(batches["logs"].num_rows(), 0);
    }

    #[test]
    fn test_bigint_conversion() {
        let bi = Some(eth::BigInt { bytes: vec![0x3B, 0x9A, 0xCA, 0x00] });
        assert_eq!(bigint_to_string(&bi), "1000000000");
        assert_eq!(bigint_to_string(&None), "0");
        let bi = Some(eth::BigInt { bytes: vec![0x01] });
        assert_eq!(bigint_to_string(&bi), "1");
        let bi = Some(eth::BigInt { bytes: vec![0x01, 0x00] });
        assert_eq!(bigint_to_string(&bi), "256");
    }

    #[test]
    fn test_table_names() {
        let mapper_base = EvmBlockMapper::new(false, false);
        assert_eq!(mapper_base.table_names().len(), 3);
        let mapper_ext = EvmBlockMapper::new(true, false);
        assert_eq!(mapper_ext.table_names().len(), 10);
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_evm_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = EvmBlockMapper::new(false, true);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), Some("NEW")).unwrap();

        let batches = mapper.flush().unwrap();
        let blocks_batch = &batches["blocks"];
        let last_col = blocks_batch.num_columns() - 1;
        assert_eq!(blocks_batch.schema().field(last_col).name(), "fork_step");
        let fork_col = blocks_batch.column(last_col).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(fork_col.value(0), "NEW");
    }
}
