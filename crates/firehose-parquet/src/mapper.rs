use crate::schema;
use crate::solana;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Per-table RecordBatch output
// ---------------------------------------------------------------------------

/// A set of RecordBatches, one per output table, produced by a single flush.
pub struct TableBatches {
    pub blocks: RecordBatch,
    pub transactions: RecordBatch,
    pub messages: RecordBatch,
    pub instructions: RecordBatch,
    pub rewards: RecordBatch,
}

// ---------------------------------------------------------------------------
// Individual table builders
// ---------------------------------------------------------------------------

pub struct BlocksBuilder {
    pub slot: UInt64Builder,
    pub parent_slot: UInt64Builder,
    pub block_height: UInt64Builder,
    pub blockhash: StringBuilder,
    pub previous_blockhash: StringBuilder,
    pub block_time: Int64Builder,
    pub num_transactions: UInt32Builder,
    pub num_rewards: UInt32Builder,
}

impl Default for BlocksBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlocksBuilder {
    pub fn new() -> Self {
        Self {
            slot: UInt64Builder::new(),
            parent_slot: UInt64Builder::new(),
            block_height: UInt64Builder::new(),
            blockhash: StringBuilder::new(),
            previous_blockhash: StringBuilder::new(),
            block_time: Int64Builder::new(),
            num_transactions: UInt32Builder::new(),
            num_rewards: UInt32Builder::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.slot.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(self.slot.finish()),
                Arc::new(self.parent_slot.finish()),
                Arc::new(self.block_height.finish()),
                Arc::new(self.blockhash.finish()),
                Arc::new(self.previous_blockhash.finish()),
                Arc::new(self.block_time.finish()),
                Arc::new(self.num_transactions.finish()),
                Arc::new(self.num_rewards.finish()),
            ],
        )?;
        Ok(batch)
    }
}

pub struct TransactionsBuilder {
    pub slot: UInt64Builder,
    pub transaction_index: UInt32Builder,
    pub signature: BinaryBuilder,
    pub num_signatures: UInt32Builder,
    pub fee: UInt64Builder,
    pub err: BinaryBuilder,
    pub success: BooleanBuilder,
    pub compute_units_consumed: UInt64Builder,
    pub log_messages: ListBuilder<StringBuilder>,
    pub pre_balances: ListBuilder<UInt64Builder>,
    pub post_balances: ListBuilder<UInt64Builder>,
}

impl Default for TransactionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionsBuilder {
    pub fn new() -> Self {
        Self {
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            signature: BinaryBuilder::new(),
            num_signatures: UInt32Builder::new(),
            fee: UInt64Builder::new(),
            err: BinaryBuilder::new(),
            success: BooleanBuilder::new(),
            compute_units_consumed: UInt64Builder::new(),
            log_messages: ListBuilder::new(StringBuilder::new()),
            pre_balances: ListBuilder::new(UInt64Builder::new()),
            post_balances: ListBuilder::new(UInt64Builder::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.slot.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(self.slot.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.signature.finish()),
                Arc::new(self.num_signatures.finish()),
                Arc::new(self.fee.finish()),
                Arc::new(self.err.finish()),
                Arc::new(self.success.finish()),
                Arc::new(self.compute_units_consumed.finish()),
                Arc::new(self.log_messages.finish()),
                Arc::new(self.pre_balances.finish()),
                Arc::new(self.post_balances.finish()),
            ],
        )?;
        Ok(batch)
    }
}

pub struct MessagesBuilder {
    pub slot: UInt64Builder,
    pub transaction_index: UInt32Builder,
    pub message_index: UInt32Builder,
    pub num_required_signatures: UInt32Builder,
    pub num_readonly_signed_accounts: UInt32Builder,
    pub num_readonly_unsigned_accounts: UInt32Builder,
    pub recent_blockhash: BinaryBuilder,
    pub versioned: BooleanBuilder,
    pub account_keys: ListBuilder<BinaryBuilder>,
}

impl Default for MessagesBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MessagesBuilder {
    pub fn new() -> Self {
        Self {
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            message_index: UInt32Builder::new(),
            num_required_signatures: UInt32Builder::new(),
            num_readonly_signed_accounts: UInt32Builder::new(),
            num_readonly_unsigned_accounts: UInt32Builder::new(),
            recent_blockhash: BinaryBuilder::new(),
            versioned: BooleanBuilder::new(),
            account_keys: ListBuilder::new(BinaryBuilder::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.slot.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(self.slot.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.message_index.finish()),
                Arc::new(self.num_required_signatures.finish()),
                Arc::new(self.num_readonly_signed_accounts.finish()),
                Arc::new(self.num_readonly_unsigned_accounts.finish()),
                Arc::new(self.recent_blockhash.finish()),
                Arc::new(self.versioned.finish()),
                Arc::new(self.account_keys.finish()),
            ],
        )?;
        Ok(batch)
    }
}

pub struct InstructionsBuilder {
    pub slot: UInt64Builder,
    pub transaction_index: UInt32Builder,
    pub instruction_index: UInt32Builder,
    pub program_id_index: UInt32Builder,
    pub accounts: BinaryBuilder,
    pub data: BinaryBuilder,
    pub is_inner: BooleanBuilder,
    pub inner_index: UInt32Builder,
    pub stack_height: UInt32Builder,
}

impl Default for InstructionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InstructionsBuilder {
    pub fn new() -> Self {
        Self {
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            instruction_index: UInt32Builder::new(),
            program_id_index: UInt32Builder::new(),
            accounts: BinaryBuilder::new(),
            data: BinaryBuilder::new(),
            is_inner: BooleanBuilder::new(),
            inner_index: UInt32Builder::new(),
            stack_height: UInt32Builder::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.slot.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(self.slot.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.instruction_index.finish()),
                Arc::new(self.program_id_index.finish()),
                Arc::new(self.accounts.finish()),
                Arc::new(self.data.finish()),
                Arc::new(self.is_inner.finish()),
                Arc::new(self.inner_index.finish()),
                Arc::new(self.stack_height.finish()),
            ],
        )?;
        Ok(batch)
    }
}

pub struct RewardsBuilder {
    pub slot: UInt64Builder,
    pub reward_index: UInt32Builder,
    pub pubkey: StringBuilder,
    pub lamports: Int64Builder,
    pub post_balance: UInt64Builder,
    pub reward_type: Int32Builder,
    pub commission: StringBuilder,
}

impl Default for RewardsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RewardsBuilder {
    pub fn new() -> Self {
        Self {
            slot: UInt64Builder::new(),
            reward_index: UInt32Builder::new(),
            pubkey: StringBuilder::new(),
            lamports: Int64Builder::new(),
            post_balance: UInt64Builder::new(),
            reward_type: Int32Builder::new(),
            commission: StringBuilder::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.slot.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(self.slot.finish()),
                Arc::new(self.reward_index.finish()),
                Arc::new(self.pubkey.finish()),
                Arc::new(self.lamports.finish()),
                Arc::new(self.post_balance.finish()),
                Arc::new(self.reward_type.finish()),
                Arc::new(self.commission.finish()),
            ],
        )?;
        Ok(batch)
    }
}

// ---------------------------------------------------------------------------
// BlockMapper — central dispatcher
// ---------------------------------------------------------------------------

/// Receives decoded Solana blocks and dispatches rows into per-table Arrow
/// builders.  Call [`BlockMapper::flush`] to drain the builders and get a set
/// of [`RecordBatch`]es, one per table.
pub struct BlockMapper {
    blocks: BlocksBuilder,
    transactions: TransactionsBuilder,
    messages: MessagesBuilder,
    instructions: InstructionsBuilder,
    rewards: RewardsBuilder,
    // Schema references (cheap Arc clones)
    blocks_schema: Schema,
    transactions_schema: Schema,
    messages_schema: Schema,
    instructions_schema: Schema,
    rewards_schema: Schema,
}

impl BlockMapper {
    pub fn new() -> Self {
        Self {
            blocks: BlocksBuilder::new(),
            transactions: TransactionsBuilder::new(),
            messages: MessagesBuilder::new(),
            instructions: InstructionsBuilder::new(),
            rewards: RewardsBuilder::new(),
            blocks_schema: schema::blocks_schema(),
            transactions_schema: schema::transactions_schema(),
            messages_schema: schema::messages_schema(),
            instructions_schema: schema::instructions_schema(),
            rewards_schema: schema::rewards_schema(),
        }
    }

    /// Returns the current number of *block* rows buffered.
    pub fn block_rows(&self) -> usize {
        self.blocks.len()
    }

    /// Returns the maximum row count across all tables (useful for flush
    /// decisions).
    pub fn max_table_rows(&self) -> usize {
        [
            self.blocks.len(),
            self.transactions.len(),
            self.messages.len(),
            self.instructions.len(),
            self.rewards.len(),
        ]
        .into_iter()
        .max()
        .unwrap_or(0)
    }

    /// Map a single decoded Solana block into the per-table builders.
    pub fn map_block(&mut self, block: &solana::Block) {
        let slot = block.slot;

        // ---- blocks table ------------------------------------------------
        self.blocks.slot.append_value(slot);
        self.blocks.parent_slot.append_value(block.parent_slot);
        match &block.block_height {
            Some(bh) => self.blocks.block_height.append_value(bh.block_height),
            None => self.blocks.block_height.append_null(),
        }
        self.blocks.blockhash.append_value(&block.blockhash);
        self.blocks
            .previous_blockhash
            .append_value(&block.previous_blockhash);
        match &block.block_time {
            Some(bt) => self.blocks.block_time.append_value(bt.timestamp),
            None => self.blocks.block_time.append_null(),
        }
        self.blocks
            .num_transactions
            .append_value(block.transactions.len() as u32);
        self.blocks
            .num_rewards
            .append_value(block.rewards.len() as u32);

        // ---- per-transaction tables --------------------------------------
        for (tx_idx, confirmed_tx) in block.transactions.iter().enumerate() {
            self.map_transaction(slot, tx_idx as u32, confirmed_tx);
        }

        // ---- rewards table -----------------------------------------------
        for (reward_idx, reward) in block.rewards.iter().enumerate() {
            self.map_reward(slot, reward_idx as u32, reward);
        }
    }

    fn map_transaction(
        &mut self,
        slot: u64,
        tx_idx: u32,
        confirmed: &solana::ConfirmedTransaction,
    ) {
        let tx = match confirmed.transaction.as_ref() {
            Some(t) => t,
            None => return,
        };
        let meta = confirmed.meta.as_ref();
        let msg = match tx.message.as_ref() {
            Some(m) => m,
            None => return,
        };

        // -- transactions --------------------------------------------------
        self.transactions.slot.append_value(slot);
        self.transactions.transaction_index.append_value(tx_idx);
        if let Some(sig) = tx.signatures.first() {
            self.transactions.signature.append_value(sig);
        } else {
            self.transactions.signature.append_value(&[] as &[u8]);
        }
        self.transactions
            .num_signatures
            .append_value(tx.signatures.len() as u32);

        if let Some(m) = meta {
            self.transactions.fee.append_value(m.fee);
            if let Some(ref err) = m.err {
                self.transactions.err.append_value(&err.err);
                self.transactions.success.append_value(false);
            } else {
                self.transactions.err.append_null();
                self.transactions.success.append_value(true);
            }
            match m.compute_units_consumed {
                Some(cu) => self.transactions.compute_units_consumed.append_value(cu),
                None => self.transactions.compute_units_consumed.append_null(),
            }
            // log_messages
            {
                let vals = self.transactions.log_messages.values();
                for log in &m.log_messages {
                    vals.append_value(log);
                }
                self.transactions.log_messages.append(true);
            }
            // pre_balances
            {
                let vals = self.transactions.pre_balances.values();
                for b in &m.pre_balances {
                    vals.append_value(*b);
                }
                self.transactions.pre_balances.append(true);
            }
            // post_balances
            {
                let vals = self.transactions.post_balances.values();
                for b in &m.post_balances {
                    vals.append_value(*b);
                }
                self.transactions.post_balances.append(true);
            }
        } else {
            self.transactions.fee.append_value(0);
            self.transactions.err.append_null();
            self.transactions.success.append_value(true);
            self.transactions.compute_units_consumed.append_null();
            self.transactions.log_messages.append(false);
            self.transactions.pre_balances.append(false);
            self.transactions.post_balances.append(false);
        }

        // -- messages ------------------------------------------------------
        self.messages.slot.append_value(slot);
        self.messages.transaction_index.append_value(tx_idx);
        self.messages.message_index.append_value(0); // Solana: always 1 message per tx
        if let Some(h) = msg.header.as_ref() {
            self.messages
                .num_required_signatures
                .append_value(h.num_required_signatures);
            self.messages
                .num_readonly_signed_accounts
                .append_value(h.num_readonly_signed_accounts);
            self.messages
                .num_readonly_unsigned_accounts
                .append_value(h.num_readonly_unsigned_accounts);
        } else {
            self.messages.num_required_signatures.append_value(0);
            self.messages.num_readonly_signed_accounts.append_value(0);
            self.messages
                .num_readonly_unsigned_accounts
                .append_value(0);
        }
        self.messages
            .recent_blockhash
            .append_value(&msg.recent_blockhash);
        self.messages.versioned.append_value(msg.versioned);
        {
            let vals = self.messages.account_keys.values();
            for key in &msg.account_keys {
                vals.append_value(key);
            }
            self.messages.account_keys.append(true);
        }

        // -- instructions (top-level) --------------------------------------
        let mut global_instr_idx = 0u32;
        for instr in &msg.instructions {
            self.instructions.slot.append_value(slot);
            self.instructions.transaction_index.append_value(tx_idx);
            self.instructions
                .instruction_index
                .append_value(global_instr_idx);
            self.instructions
                .program_id_index
                .append_value(instr.program_id_index);
            self.instructions.accounts.append_value(&instr.accounts);
            self.instructions.data.append_value(&instr.data);
            self.instructions.is_inner.append_value(false);
            self.instructions.inner_index.append_null();
            self.instructions.stack_height.append_null();
            global_instr_idx += 1;
        }

        // -- instructions (inner) ------------------------------------------
        if let Some(m) = meta {
            for inner_set in &m.inner_instructions {
                for inner in &inner_set.instructions {
                    self.instructions.slot.append_value(slot);
                    self.instructions.transaction_index.append_value(tx_idx);
                    self.instructions
                        .instruction_index
                        .append_value(global_instr_idx);
                    self.instructions
                        .program_id_index
                        .append_value(inner.program_id_index);
                    self.instructions.accounts.append_value(&inner.accounts);
                    self.instructions.data.append_value(&inner.data);
                    self.instructions.is_inner.append_value(true);
                    self.instructions
                        .inner_index
                        .append_value(inner_set.index);
                    match inner.stack_height {
                        Some(sh) => self.instructions.stack_height.append_value(sh),
                        None => self.instructions.stack_height.append_null(),
                    }
                    global_instr_idx += 1;
                }
            }
        }
    }

    fn map_reward(&mut self, slot: u64, idx: u32, reward: &solana::Reward) {
        self.rewards.slot.append_value(slot);
        self.rewards.reward_index.append_value(idx);
        self.rewards.pubkey.append_value(&reward.pubkey);
        self.rewards.lamports.append_value(reward.lamports);
        self.rewards.post_balance.append_value(reward.post_balance);
        self.rewards.reward_type.append_value(reward.reward_type);
        if reward.commission.is_empty() {
            self.rewards.commission.append_null();
        } else {
            self.rewards.commission.append_value(&reward.commission);
        }
    }

    /// Drain all builders and return one [`RecordBatch`] per table.
    /// The builders are automatically reset and ready for more data.
    pub fn flush(&mut self) -> anyhow::Result<TableBatches> {
        Ok(TableBatches {
            blocks: self.blocks.finish(&self.blocks_schema)?,
            transactions: self.transactions.finish(&self.transactions_schema)?,
            messages: self.messages.finish(&self.messages_schema)?,
            instructions: self.instructions.finish(&self.instructions_schema)?,
            rewards: self.rewards.finish(&self.rewards_schema)?,
        })
    }
}

impl Default for BlockMapper {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solana;

    /// Build a minimal synthetic Solana block for testing.
    fn make_test_block(slot: u64) -> solana::Block {
        solana::Block {
            slot,
            parent_slot: slot.saturating_sub(1),
            blockhash: format!("hash_{slot}"),
            previous_blockhash: format!("hash_{}", slot.saturating_sub(1)),
            block_height: Some(solana::BlockHeight {
                block_height: slot,
            }),
            block_time: Some(solana::UnixTimestamp {
                timestamp: 1_700_000_000 + slot as i64,
            }),
            transactions: vec![solana::ConfirmedTransaction {
                transaction: Some(solana::Transaction {
                    signatures: vec![vec![1u8; 64]],
                    message: Some(solana::Message {
                        header: Some(solana::MessageHeader {
                            num_required_signatures: 1,
                            num_readonly_signed_accounts: 0,
                            num_readonly_unsigned_accounts: 1,
                        }),
                        account_keys: vec![vec![2u8; 32], vec![3u8; 32]],
                        recent_blockhash: vec![4u8; 32],
                        instructions: vec![solana::CompiledInstruction {
                            program_id_index: 1,
                            accounts: vec![0],
                            data: vec![5, 6, 7],
                        }],
                        versioned: false,
                        address_table_lookups: vec![],
                    }),
                }),
                meta: Some(solana::TransactionStatusMeta {
                    err: None,
                    fee: 5000,
                    pre_balances: vec![100_000, 0],
                    post_balances: vec![95_000, 0],
                    inner_instructions: vec![solana::InnerInstructions {
                        index: 0,
                        instructions: vec![solana::InnerInstruction {
                            program_id_index: 1,
                            accounts: vec![0],
                            data: vec![8, 9],
                            stack_height: Some(2),
                        }],
                    }],
                    log_messages: vec!["Program log: hello".to_string()],
                    pre_token_balances: vec![],
                    post_token_balances: vec![],
                    rewards: vec![],
                    loaded_writable_addresses: vec![],
                    loaded_readonly_addresses: vec![],
                    return_data: None,
                    compute_units_consumed: Some(1234),
                }),
            }],
            rewards: vec![solana::Reward {
                pubkey: "RewardPubkey".to_string(),
                lamports: 42,
                post_balance: 999,
                reward_type: 1, // Fee
                commission: String::new(),
            }],
        }
    }

    #[test]
    fn test_map_and_flush_single_block() {
        let block = make_test_block(100);
        let mut mapper = BlockMapper::new();
        mapper.map_block(&block);

        assert_eq!(mapper.block_rows(), 1);

        let batches = mapper.flush().unwrap();

        // blocks
        assert_eq!(batches.blocks.num_rows(), 1);
        assert_eq!(batches.blocks.num_columns(), 8);

        // transactions
        assert_eq!(batches.transactions.num_rows(), 1);

        // messages
        assert_eq!(batches.messages.num_rows(), 1);

        // instructions: 1 top-level + 1 inner = 2
        assert_eq!(batches.instructions.num_rows(), 2);

        // rewards
        assert_eq!(batches.rewards.num_rows(), 1);
    }

    #[test]
    fn test_flush_resets_builders() {
        let block = make_test_block(1);
        let mut mapper = BlockMapper::new();
        mapper.map_block(&block);
        let _ = mapper.flush().unwrap();

        assert_eq!(mapper.block_rows(), 0);
        assert_eq!(mapper.max_table_rows(), 0);

        // Second block
        mapper.map_block(&make_test_block(2));
        let batches = mapper.flush().unwrap();
        assert_eq!(batches.blocks.num_rows(), 1);
    }

    #[test]
    fn test_schema_column_count() {
        assert_eq!(schema::blocks_schema().fields().len(), 8);
        assert_eq!(schema::transactions_schema().fields().len(), 11);
        assert_eq!(schema::messages_schema().fields().len(), 9);
        assert_eq!(schema::instructions_schema().fields().len(), 9);
        assert_eq!(schema::rewards_schema().fields().len(), 7);
    }

    #[test]
    fn test_empty_block() {
        let block = solana::Block {
            slot: 0,
            parent_slot: 0,
            blockhash: "genesis".into(),
            previous_blockhash: "".into(),
            block_height: None,
            block_time: None,
            transactions: vec![],
            rewards: vec![],
        };
        let mut mapper = BlockMapper::new();
        mapper.map_block(&block);
        let batches = mapper.flush().unwrap();

        assert_eq!(batches.blocks.num_rows(), 1);
        assert_eq!(batches.transactions.num_rows(), 0);
        assert_eq!(batches.instructions.num_rows(), 0);
        assert_eq!(batches.rewards.num_rows(), 0);
    }
}
