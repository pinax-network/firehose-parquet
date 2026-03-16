use super::proto::eth;
use super::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, EncodeBytes};
use firehose_parquet::traits::{
    est_bool, est_i32, est_opt_str, est_str, est_u32, est_u64, BlockIdentity, BlockMapper,
    CanonicalBuilder,
};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
        identity: &BlockIdentity,
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
        let base_fee = header.and_then(|h| h.base_fee_per_gas.as_ref());
        if base_fee.is_some() {
            self.blocks.base_fee_per_gas.append_value(bigint_to_string(
                &header.and_then(|h| h.base_fee_per_gas.clone()),
            ));
        } else {
            self.blocks.base_fee_per_gas.append_null();
        }
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
        let difficulty = header.and_then(|h| h.difficulty.as_ref());
        if difficulty.is_some() {
            self.blocks
                .difficulty
                .append_value(bigint_to_string(&header.and_then(|h| h.difficulty.clone())));
        } else {
            self.blocks.difficulty.append_null();
        }
        self.blocks
            .mix_hash
            .append_value(header.map_or(&[][..], |h| &h.mix_hash));
        self.blocks
            .extra_data
            .append_value(header.map_or(&[][..], |h| &h.extra_data));
        self.blocks
            .num_transactions
            .append_value(block.transaction_traces.len() as u32);
        self.blocks.detail_level.append_value(block.detail_level);
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // -- transaction traces --
        for tx in &block.transaction_traces {
            // Skip failed transactions (status != 1) unless flag is set
            if !self.include_failed_transactions && tx.status != 1 {
                continue;
            }
            self.map_transaction(number, tx, identity, fork_step);
        }

        // -- block-level events → system_* tables (EXTENDED) --
        if self.extended {
            for bc in &block.balance_changes {
                if let Some(ref mut builder) = self.system_balance_changes {
                    builder.append(number, bc, identity, fork_step);
                }
            }
            for cc in &block.code_changes {
                if let Some(ref mut builder) = self.system_code_changes {
                    builder.append(number, cc, identity, fork_step);
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
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        let tx_hash = &tx.hash;

        self.transactions.canonical.append(identity);
        self.transactions.block_number.append_value(block_number);
        self.transactions.index.append_value(tx.index);
        self.transactions.hash.append_value(tx_hash);
        self.transactions.from.append_value(&tx.from);
        self.transactions.to.append_value(&tx.to);
        self.transactions
            .value
            .append_value(bigint_to_string(&tx.value));
        self.transactions.gas_limit.append_value(tx.gas_limit);
        self.transactions.gas_used.append_value(tx.gas_used);
        let gas_price = &tx.gas_price;
        if gas_price.is_some() {
            self.transactions
                .gas_price
                .append_value(bigint_to_string(gas_price));
        } else {
            self.transactions.gas_price.append_null();
        }
        self.transactions.r#type.append_value(tx.r#type);
        self.transactions.status.append_value(tx.status);
        self.transactions.nonce.append_value(tx.nonce);
        self.transactions.input.append_value(&tx.input);
        if tx.max_fee_per_gas.is_some() {
            self.transactions
                .max_fee_per_gas
                .append_value(bigint_to_string(&tx.max_fee_per_gas));
        } else {
            self.transactions.max_fee_per_gas.append_null();
        }
        if tx.max_priority_fee_per_gas.is_some() {
            self.transactions
                .max_priority_fee_per_gas
                .append_value(bigint_to_string(&tx.max_priority_fee_per_gas));
        } else {
            self.transactions.max_priority_fee_per_gas.append_null();
        }
        if let Some(ref receipt) = tx.receipt {
            self.transactions
                .cumulative_gas_used
                .append_value(receipt.cumulative_gas_used);
        } else {
            self.transactions.cumulative_gas_used.append_null();
        }
        append_fork_step(&mut self.transactions.fork_step, fork_step);

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
                self.extract_call_state_changes(block_number, tx_hash, call, identity, fork_step);
            }
        }
    }

    fn map_log(
        &mut self,
        block_number: u64,
        tx_hash: &[u8],
        tx_index: u32,
        log: &eth::Log,
        identity: &BlockIdentity,
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
        append_fork_step(&mut self.logs.fork_step, fork_step);
    }

    /// Extract state changes from transaction-scoped calls → transaction tables.
    fn extract_call_state_changes(
        &mut self,
        block_number: u64,
        tx_hash: &[u8],
        call: &eth::Call,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
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

    /// Extract state changes from system calls → system_* tables.
    fn extract_system_call_state_changes(
        &mut self,
        block_number: u64,
        call: &eth::Call,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        if !self.extended {
            return;
        }

        for bc in &call.balance_changes {
            if let Some(ref mut builder) = self.system_balance_changes {
                builder.append(block_number, bc, identity, fork_step);
            }
        }
        for cc in &call.code_changes {
            if let Some(ref mut builder) = self.system_code_changes {
                builder.append(block_number, cc, identity, fork_step);
            }
        }
        for sc in &call.storage_changes {
            if let Some(ref mut builder) = self.system_storage_changes {
                builder.append(block_number, sc, identity, fork_step);
            }
        }
        for nc in &call.nonce_changes {
            if let Some(ref mut builder) = self.system_nonce_changes {
                builder.append(block_number, nc, identity, fork_step);
            }
        }
        for gc in &call.gas_changes {
            if let Some(ref mut builder) = self.system_gas_changes {
                builder.append(block_number, gc, identity, fork_step);
            }
        }
        #[allow(deprecated)]
        for ac in &call.account_creations {
            if let Some(ref mut builder) = self.system_account_creations {
                builder.append(block_number, ac, identity, fork_step);
            }
        }
    }
}

impl BlockMapper for EvmBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<()> {
        let block = eth::Block::decode(block_bytes)?;
        self.map_evm_block(&block, identity, fork_step);
        Ok(())
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
            .max(self.logs.canonical.len());
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
            + self.logs.canonical.len();
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
            + est_i32(&self.blocks.detail_level)
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
            + est_i32(&self.transactions.r#type)
            + est_i32(&self.transactions.status)
            + est_u64(&self.transactions.nonce)
            + self.transactions.input.estimated_bytes()
            + est_str(&self.transactions.max_fee_per_gas)
            + est_str(&self.transactions.max_priority_fee_per_gas)
            + est_u64(&self.transactions.cumulative_gas_used)
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
            + est_opt_str(&self.logs.fork_step);
        let mut tables: Vec<(&str, usize)> = vec![
            ("blocks", blocks),
            ("transactions", transactions),
            ("logs", logs),
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
                    + est_i32(&$b.call_type)
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
                    + est_i32(&$b.call_type)
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
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_balance_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + $b.tx_hash.estimated_bytes()
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + est_str(&$b.old_value)
                    + est_str(&$b.new_value)
                    + est_i32(&$b.reason)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_balance_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + est_str(&$b.old_value)
                    + est_str(&$b.new_value)
                    + est_i32(&$b.reason)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_code_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + $b.tx_hash.estimated_bytes()
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + $b.old_hash.estimated_bytes()
                    + $b.new_hash.estimated_bytes()
                    + $b.old_code.estimated_bytes()
                    + $b.new_code.estimated_bytes()
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_code_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
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
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + $b.key.estimated_bytes()
                    + $b.old_value.estimated_bytes()
                    + $b.new_value.estimated_bytes()
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_storage_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
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
                    + est_u64(&$b.ordinal)
                    + $b.address.estimated_bytes()
                    + est_u64(&$b.old_value)
                    + est_u64(&$b.new_value)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_nonce_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
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
                    + est_u64(&$b.ordinal)
                    + est_u64(&$b.old_value)
                    + est_u64(&$b.new_value)
                    + est_i32(&$b.reason)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_gas_changes {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + est_u64(&$b.ordinal)
                    + est_u64(&$b.old_value)
                    + est_u64(&$b.new_value)
                    + est_i32(&$b.reason)
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_account_creations {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
                    + $b.tx_hash.estimated_bytes()
                    + est_u64(&$b.ordinal)
                    + $b.account.estimated_bytes()
                    + est_opt_str(&$b.fork_step)
            };
        }
        macro_rules! est_sys_account_creations {
            ($b:expr) => {
                $b.canonical.estimated_bytes()
                    + est_u64(&$b.block_number)
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
    detail_level: Int32Builder,
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
            detail_level: Int32Builder::new(),
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
    r#type: Int32Builder,
    status: Int32Builder,
    nonce: UInt64Builder,
    input: BytesColumn,
    max_fee_per_gas: StringBuilder,
    max_priority_fee_per_gas: StringBuilder,
    cumulative_gas_used: UInt64Builder,
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
            r#type: Int32Builder::new(),
            status: Int32Builder::new(),
            nonce: UInt64Builder::new(),
            input: BytesColumn::new(encoding),
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
    call_type: Int32Builder,
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
            call_type: Int32Builder::new(),
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
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        tx_hash: &[u8],
        tx_index: u32,
        call: &eth::Call,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
        self.tx_index.append_value(tx_index);
        self.call_index.append_value(call.index);
        self.parent_index.append_value(call.parent_index);
        self.depth.append_value(call.depth);
        self.call_type.append_value(call.call_type);
        self.caller.append_value(&call.caller);
        self.address.append_value(&call.address);
        self.value.append_value(bigint_to_string(&call.value));
        self.gas_limit.append_value(call.gas_limit);
        self.gas_consumed.append_value(call.gas_consumed);
        self.input.append_value(&call.input);
        self.output.append_value(&call.return_data);
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
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EvmBalanceChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    ordinal: UInt64Builder,
    address: BytesColumn,
    old_value: StringBuilder,
    new_value: StringBuilder,
    reason: Int32Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmBalanceChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            ordinal: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            old_value: StringBuilder::new(),
            new_value: StringBuilder::new(),
            reason: Int32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        tx_hash: &[u8],
        bc: &eth::BalanceChange,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
        self.ordinal.append_value(bc.ordinal);
        self.address.append_value(&bc.address);
        self.old_value.append_value(bigint_to_string(&bc.old_value));
        self.new_value.append_value(bigint_to_string(&bc.new_value));
        self.reason.append_value(bc.reason);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
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

struct EvmCodeChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    ordinal: UInt64Builder,
    address: BytesColumn,
    old_hash: BytesColumn,
    new_hash: BytesColumn,
    old_code: BytesColumn,
    new_code: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl EvmCodeChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
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
        tx_hash: &[u8],
        cc: &eth::CodeChange,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
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
            self.tx_hash.finish(),
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

struct EvmStorageChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    ordinal: UInt64Builder,
    address: BytesColumn,
    key: BytesColumn,
    old_value: BytesColumn,
    new_value: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl EvmStorageChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
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
        tx_hash: &[u8],
        sc: &eth::StorageChange,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
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
            self.tx_hash.finish(),
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

struct EvmNonceChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    ordinal: UInt64Builder,
    address: BytesColumn,
    old_value: UInt64Builder,
    new_value: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmNonceChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
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
        tx_hash: &[u8],
        nc: &eth::NonceChange,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
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
            self.tx_hash.finish(),
            Arc::new(self.ordinal.finish()),
            self.address.finish(),
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
    tx_hash: BytesColumn,
    ordinal: UInt64Builder,
    old_value: UInt64Builder,
    new_value: UInt64Builder,
    reason: Int32Builder,
    fork_step: Option<StringBuilder>,
}

impl EvmGasChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            ordinal: UInt64Builder::new(),
            old_value: UInt64Builder::new(),
            new_value: UInt64Builder::new(),
            reason: Int32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        tx_hash: &[u8],
        gc: &eth::GasChange,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
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
            self.tx_hash.finish(),
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
    tx_hash: BytesColumn,
    ordinal: UInt64Builder,
    account: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl EvmAccountCreationsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            ordinal: UInt64Builder::new(),
            account: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        tx_hash: &[u8],
        ac: &eth::AccountCreation,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(tx_hash);
        self.ordinal.append_value(ac.ordinal);
        self.account.append_value(&ac.account);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            self.tx_hash.finish(),
            Arc::new(self.ordinal.finish()),
            self.account.finish(),
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
    call_type: Int32Builder,
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
            call_type: Int32Builder::new(),
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
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        call: &eth::Call,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.call_index.append_value(call.index);
        self.parent_index.append_value(call.parent_index);
        self.depth.append_value(call.depth);
        self.call_type.append_value(call.call_type);
        self.caller.append_value(&call.caller);
        self.address.append_value(&call.address);
        self.value.append_value(bigint_to_string(&call.value));
        self.gas_limit.append_value(call.gas_limit);
        self.gas_consumed.append_value(call.gas_consumed);
        self.input.append_value(&call.input);
        self.output.append_value(&call.return_data);
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
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct SystemBalanceChangesBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    ordinal: UInt64Builder,
    address: BytesColumn,
    old_value: StringBuilder,
    new_value: StringBuilder,
    reason: Int32Builder,
    fork_step: Option<StringBuilder>,
}

impl SystemBalanceChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            ordinal: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            old_value: StringBuilder::new(),
            new_value: StringBuilder::new(),
            reason: Int32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        bc: &eth::BalanceChange,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.ordinal.append_value(bc.ordinal);
        self.address.append_value(&bc.address);
        self.old_value.append_value(bigint_to_string(&bc.old_value));
        self.new_value.append_value(bigint_to_string(&bc.new_value));
        self.reason.append_value(bc.reason);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
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
        cc: &eth::CodeChange,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
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
        sc: &eth::StorageChange,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
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
        nc: &eth::NonceChange,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
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
    ordinal: UInt64Builder,
    old_value: UInt64Builder,
    new_value: UInt64Builder,
    reason: Int32Builder,
    fork_step: Option<StringBuilder>,
}

impl SystemGasChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            ordinal: UInt64Builder::new(),
            old_value: UInt64Builder::new(),
            new_value: UInt64Builder::new(),
            reason: Int32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        gc: &eth::GasChange,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
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
    ordinal: UInt64Builder,
    account: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl SystemAccountCreationsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            ordinal: UInt64Builder::new(),
            account: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block_number: u64,
        ac: &eth::AccountCreation,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.ordinal.append_value(ac.ordinal);
        self.account.append_value(&ac.account);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(self.ordinal.finish()),
            self.account.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

mod num_bigint {
    pub struct BigUint {
        bytes: Vec<u8>,
    }

    impl BigUint {
        pub fn from_bytes_be(bytes: &[u8]) -> Self {
            Self {
                bytes: bytes.to_vec(),
            }
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

    #[allow(deprecated)]
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
                timestamp: Some(prost_types::Timestamp {
                    seconds: 1700000000,
                    nanos: 0,
                }),
                extra_data: vec![],
                mix_hash: vec![0x05; 32],
                nonce: 0,
                hash: vec![0xab; 32],
                base_fee_per_gas: Some(eth::BigInt {
                    bytes: vec![0x3B, 0x9A, 0xCA, 0x00],
                }),
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
                gas_price: Some(eth::BigInt {
                    bytes: vec![0x3B, 0x9A, 0xCA, 0x00],
                }),
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
            bytes: vec![0x3B, 0x9A, 0xCA, 0x00],
        });
        assert_eq!(bigint_to_string(&bi), "1000000000");
        assert_eq!(bigint_to_string(&None), "0");
        let bi = Some(eth::BigInt { bytes: vec![0x01] });
        assert_eq!(bigint_to_string(&bi), "1");
        let bi = Some(eth::BigInt {
            bytes: vec![0x01, 0x00],
        });
        assert_eq!(bigint_to_string(&bi), "256");
    }

    #[test]
    fn test_table_names() {
        let mapper_base = EvmBlockMapper::new(false, false, EncodeBytes::Hex, false);
        assert_eq!(mapper_base.table_names().len(), 3);
        let mapper_ext = EvmBlockMapper::new(true, false, EncodeBytes::Hex, false);
        assert_eq!(mapper_ext.table_names().len(), 17);
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
}
