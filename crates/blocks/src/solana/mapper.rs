use super::proto::solana;
use super::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, BytesListColumn, EncodeBytes};
use firehose_parquet::traits::{BlockIdentity, BlockMapper, CanonicalBuilder};
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

pub struct SolanaBlockMapper {
    include_fork_step: bool,
    encoding: EncodeBytes,
    blocks: BlocksBuilder,
    transactions: TransactionsBuilder,
    messages: MessagesBuilder,
    instructions: InstructionsBuilder,
    rewards: RewardsBuilder,
    blocks_schema: Schema,
    transactions_schema: Schema,
    messages_schema: Schema,
    instructions_schema: Schema,
    rewards_schema: Schema,
}

impl SolanaBlockMapper {
    pub fn new(include_fork_step: bool, encoding: EncodeBytes) -> Self {
        Self {
            include_fork_step,
            blocks: BlocksBuilder::new(include_fork_step),
            transactions: TransactionsBuilder::new(include_fork_step, &encoding),
            messages: MessagesBuilder::new(include_fork_step, &encoding),
            instructions: InstructionsBuilder::new(include_fork_step, &encoding),
            rewards: RewardsBuilder::new(include_fork_step),
            blocks_schema: schema::blocks_schema(include_fork_step),
            transactions_schema: schema::transactions_schema(include_fork_step, &encoding),
            messages_schema: schema::messages_schema(include_fork_step, &encoding),
            instructions_schema: schema::instructions_schema(include_fork_step, &encoding),
            rewards_schema: schema::rewards_schema(include_fork_step),
            encoding,
        }
    }

    fn map_solana_block(&mut self, block: &solana::Block, identity: &BlockIdentity, fork_step: Option<&str>) {
        let slot = block.slot;

        self.blocks.canonical.append(identity);
        self.blocks.slot.append_value(slot);
        self.blocks.parent_slot.append_value(block.parent_slot);
        match &block.block_height {
            Some(bh) => self.blocks.block_height.append_value(bh.block_height),
            None => self.blocks.block_height.append_null(),
        }
        self.blocks.blockhash.append_value(&block.blockhash);
        self.blocks.previous_blockhash.append_value(&block.previous_blockhash);
        match &block.block_time {
            Some(bt) => self.blocks.block_time.append_value(bt.timestamp),
            None => self.blocks.block_time.append_null(),
        }
        self.blocks.num_transactions.append_value(block.transactions.len() as u32);
        self.blocks.num_rewards.append_value(block.rewards.len() as u32);
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        for (tx_idx, confirmed_tx) in block.transactions.iter().enumerate() {
            self.map_transaction(slot, tx_idx as u32, confirmed_tx, identity, fork_step);
        }

        for (reward_idx, reward) in block.rewards.iter().enumerate() {
            self.map_reward(slot, reward_idx as u32, reward, identity, fork_step);
        }
    }

    fn map_transaction(&mut self, slot: u64, tx_idx: u32, confirmed: &solana::ConfirmedTransaction, identity: &BlockIdentity, fork_step: Option<&str>) {
        let tx = match confirmed.transaction.as_ref() {
            Some(t) => t,
            None => return,
        };
        let meta = confirmed.meta.as_ref();
        let msg = match tx.message.as_ref() {
            Some(m) => m,
            None => return,
        };

        self.transactions.canonical.append(identity);
        self.transactions.slot.append_value(slot);
        self.transactions.transaction_index.append_value(tx_idx);
        if let Some(sig) = tx.signatures.first() {
            self.transactions.signature.append_value(sig);
        } else {
            self.transactions.signature.append_value(&[] as &[u8]);
        }
        self.transactions.num_signatures.append_value(tx.signatures.len() as u32);

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
            {
                let vals = self.transactions.log_messages.values();
                for log in &m.log_messages {
                    vals.append_value(log);
                }
                self.transactions.log_messages.append(true);
            }
            {
                let vals = self.transactions.pre_balances.values();
                for b in &m.pre_balances {
                    vals.append_value(*b);
                }
                self.transactions.pre_balances.append(true);
            }
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
        append_fork_step(&mut self.transactions.fork_step, fork_step);

        // messages
        self.messages.canonical.append(identity);
        self.messages.slot.append_value(slot);
        self.messages.transaction_index.append_value(tx_idx);
        self.messages.message_index.append_value(0);
        if let Some(h) = msg.header.as_ref() {
            self.messages.num_required_signatures.append_value(h.num_required_signatures);
            self.messages.num_readonly_signed_accounts.append_value(h.num_readonly_signed_accounts);
            self.messages.num_readonly_unsigned_accounts.append_value(h.num_readonly_unsigned_accounts);
        } else {
            self.messages.num_required_signatures.append_value(0);
            self.messages.num_readonly_signed_accounts.append_value(0);
            self.messages.num_readonly_unsigned_accounts.append_value(0);
        }
        self.messages.recent_blockhash.append_value(&msg.recent_blockhash);
        self.messages.versioned.append_value(msg.versioned);
        for key in &msg.account_keys {
            self.messages.account_keys.append_value(key);
        }
        self.messages.account_keys.append(true);
        append_fork_step(&mut self.messages.fork_step, fork_step);

        // instructions (top-level)
        let mut global_instr_idx = 0u32;
        for instr in &msg.instructions {
            self.instructions.canonical.append(identity);
            self.instructions.slot.append_value(slot);
            self.instructions.transaction_index.append_value(tx_idx);
            self.instructions.instruction_index.append_value(global_instr_idx);
            self.instructions.program_id_index.append_value(instr.program_id_index);
            self.instructions.accounts.append_value(&instr.accounts);
            self.instructions.data.append_value(&instr.data);
            self.instructions.is_inner.append_value(false);
            self.instructions.inner_index.append_null();
            self.instructions.stack_height.append_null();
            append_fork_step(&mut self.instructions.fork_step, fork_step);
            global_instr_idx += 1;
        }

        // instructions (inner)
        if let Some(m) = meta {
            for inner_set in &m.inner_instructions {
                for inner in &inner_set.instructions {
                    self.instructions.canonical.append(identity);
                    self.instructions.slot.append_value(slot);
                    self.instructions.transaction_index.append_value(tx_idx);
                    self.instructions.instruction_index.append_value(global_instr_idx);
                    self.instructions.program_id_index.append_value(inner.program_id_index);
                    self.instructions.accounts.append_value(&inner.accounts);
                    self.instructions.data.append_value(&inner.data);
                    self.instructions.is_inner.append_value(true);
                    self.instructions.inner_index.append_value(inner_set.index);
                    match inner.stack_height {
                        Some(sh) => self.instructions.stack_height.append_value(sh),
                        None => self.instructions.stack_height.append_null(),
                    }
                    append_fork_step(&mut self.instructions.fork_step, fork_step);
                    global_instr_idx += 1;
                }
            }
        }
    }

    fn map_reward(&mut self, slot: u64, idx: u32, reward: &solana::Reward, identity: &BlockIdentity, fork_step: Option<&str>) {
        self.rewards.canonical.append(identity);
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
        append_fork_step(&mut self.rewards.fork_step, fork_step);
    }
}

impl BlockMapper for SolanaBlockMapper {
    fn map_block(&mut self, block_bytes: &[u8], identity: &BlockIdentity, fork_step: Option<&str>) -> anyhow::Result<()> {
        let block = solana::Block::decode(block_bytes)?;
        self.map_solana_block(&block, identity, fork_step);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        result.insert("blocks".to_string(), self.blocks.finish(&self.blocks_schema)?);
        result.insert("transactions".to_string(), self.transactions.finish(&self.transactions_schema)?);
        result.insert("messages".to_string(), self.messages.finish(&self.messages_schema)?);
        result.insert("instructions".to_string(), self.instructions.finish(&self.instructions_schema)?);
        result.insert("rewards".to_string(), self.rewards.finish(&self.rewards_schema)?);
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        [
            self.blocks.canonical.len(),
            self.transactions.canonical.len(),
            self.messages.canonical.len(),
            self.instructions.canonical.len(),
            self.rewards.canonical.len(),
        ]
        .into_iter()
        .max()
        .unwrap_or(0)
    }

    fn table_names(&self) -> Vec<&str> {
        schema::TABLE_NAMES.to_vec()
    }
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

struct BlocksBuilder {
    canonical: CanonicalBuilder,
    slot: UInt64Builder,
    parent_slot: UInt64Builder,
    block_height: UInt64Builder,
    blockhash: StringBuilder,
    previous_blockhash: StringBuilder,
    block_time: Int64Builder,
    num_transactions: UInt32Builder,
    num_rewards: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            slot: UInt64Builder::new(),
            parent_slot: UInt64Builder::new(),
            block_height: UInt64Builder::new(),
            blockhash: StringBuilder::new(),
            previous_blockhash: StringBuilder::new(),
            block_time: Int64Builder::new(),
            num_transactions: UInt32Builder::new(),
            num_rewards: UInt32Builder::new(),
            fork_step: if include_fork_step { Some(StringBuilder::new()) } else { None },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.parent_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.block_height.finish()) as Arc<dyn Array>,
            Arc::new(self.blockhash.finish()) as Arc<dyn Array>,
            Arc::new(self.previous_blockhash.finish()) as Arc<dyn Array>,
            Arc::new(self.block_time.finish()) as Arc<dyn Array>,
            Arc::new(self.num_transactions.finish()) as Arc<dyn Array>,
            Arc::new(self.num_rewards.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct TransactionsBuilder {
    canonical: CanonicalBuilder,
    slot: UInt64Builder,
    transaction_index: UInt32Builder,
    signature: BytesColumn,
    num_signatures: UInt32Builder,
    fee: UInt64Builder,
    err: BytesColumn,
    success: BooleanBuilder,
    compute_units_consumed: UInt64Builder,
    log_messages: ListBuilder<StringBuilder>,
    pre_balances: ListBuilder<UInt64Builder>,
    post_balances: ListBuilder<UInt64Builder>,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            signature: BytesColumn::new(encoding),
            num_signatures: UInt32Builder::new(),
            fee: UInt64Builder::new(),
            err: BytesColumn::new(encoding),
            success: BooleanBuilder::new(),
            compute_units_consumed: UInt64Builder::new(),
            log_messages: ListBuilder::new(StringBuilder::new()),
            pre_balances: ListBuilder::new(UInt64Builder::new()),
            post_balances: ListBuilder::new(UInt64Builder::new()),
            fork_step: if include_fork_step { Some(StringBuilder::new()) } else { None },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.transaction_index.finish()) as Arc<dyn Array>,
            self.signature.finish(),
            Arc::new(self.num_signatures.finish()) as Arc<dyn Array>,
            Arc::new(self.fee.finish()) as Arc<dyn Array>,
            self.err.finish(),
            Arc::new(self.success.finish()) as Arc<dyn Array>,
            Arc::new(self.compute_units_consumed.finish()) as Arc<dyn Array>,
            Arc::new(self.log_messages.finish()) as Arc<dyn Array>,
            Arc::new(self.pre_balances.finish()) as Arc<dyn Array>,
            Arc::new(self.post_balances.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct MessagesBuilder {
    canonical: CanonicalBuilder,
    slot: UInt64Builder,
    transaction_index: UInt32Builder,
    message_index: UInt32Builder,
    num_required_signatures: UInt32Builder,
    num_readonly_signed_accounts: UInt32Builder,
    num_readonly_unsigned_accounts: UInt32Builder,
    recent_blockhash: BytesColumn,
    versioned: BooleanBuilder,
    account_keys: BytesListColumn,
    fork_step: Option<StringBuilder>,
}

impl MessagesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            message_index: UInt32Builder::new(),
            num_required_signatures: UInt32Builder::new(),
            num_readonly_signed_accounts: UInt32Builder::new(),
            num_readonly_unsigned_accounts: UInt32Builder::new(),
            recent_blockhash: BytesColumn::new(encoding),
            versioned: BooleanBuilder::new(),
            account_keys: BytesListColumn::new(encoding),
            fork_step: if include_fork_step { Some(StringBuilder::new()) } else { None },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.transaction_index.finish()) as Arc<dyn Array>,
            Arc::new(self.message_index.finish()) as Arc<dyn Array>,
            Arc::new(self.num_required_signatures.finish()) as Arc<dyn Array>,
            Arc::new(self.num_readonly_signed_accounts.finish()) as Arc<dyn Array>,
            Arc::new(self.num_readonly_unsigned_accounts.finish()) as Arc<dyn Array>,
            self.recent_blockhash.finish(),
            Arc::new(self.versioned.finish()) as Arc<dyn Array>,
            self.account_keys.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct InstructionsBuilder {
    canonical: CanonicalBuilder,
    slot: UInt64Builder,
    transaction_index: UInt32Builder,
    instruction_index: UInt32Builder,
    program_id_index: UInt32Builder,
    accounts: BytesColumn,
    data: BytesColumn,
    is_inner: BooleanBuilder,
    inner_index: UInt32Builder,
    stack_height: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl InstructionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            instruction_index: UInt32Builder::new(),
            program_id_index: UInt32Builder::new(),
            accounts: BytesColumn::new(encoding),
            data: BytesColumn::new(encoding),
            is_inner: BooleanBuilder::new(),
            inner_index: UInt32Builder::new(),
            stack_height: UInt32Builder::new(),
            fork_step: if include_fork_step { Some(StringBuilder::new()) } else { None },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.transaction_index.finish()) as Arc<dyn Array>,
            Arc::new(self.instruction_index.finish()) as Arc<dyn Array>,
            Arc::new(self.program_id_index.finish()) as Arc<dyn Array>,
            self.accounts.finish(),
            self.data.finish(),
            Arc::new(self.is_inner.finish()) as Arc<dyn Array>,
            Arc::new(self.inner_index.finish()) as Arc<dyn Array>,
            Arc::new(self.stack_height.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct RewardsBuilder {
    canonical: CanonicalBuilder,
    slot: UInt64Builder,
    reward_index: UInt32Builder,
    pubkey: StringBuilder,
    lamports: Int64Builder,
    post_balance: UInt64Builder,
    reward_type: Int32Builder,
    commission: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl RewardsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            slot: UInt64Builder::new(),
            reward_index: UInt32Builder::new(),
            pubkey: StringBuilder::new(),
            lamports: Int64Builder::new(),
            post_balance: UInt64Builder::new(),
            reward_type: Int32Builder::new(),
            commission: StringBuilder::new(),
            fork_step: if include_fork_step { Some(StringBuilder::new()) } else { None },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.reward_index.finish()) as Arc<dyn Array>,
            Arc::new(self.pubkey.finish()) as Arc<dyn Array>,
            Arc::new(self.lamports.finish()) as Arc<dyn Array>,
            Arc::new(self.post_balance.finish()) as Arc<dyn Array>,
            Arc::new(self.reward_type.finish()) as Arc<dyn Array>,
            Arc::new(self.commission.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_block(slot: u64) -> solana::Block {
        solana::Block {
            slot,
            parent_slot: slot.saturating_sub(1),
            blockhash: format!("hash_{slot}"),
            previous_blockhash: format!("hash_{}", slot.saturating_sub(1)),
            block_height: Some(solana::BlockHeight { block_height: slot }),
            block_time: Some(solana::UnixTimestamp { timestamp: 1_700_000_000 + slot as i64 }),
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
                reward_type: 1,
                commission: String::new(),
            }],
        }
    }

    #[test]
    fn test_map_and_flush_single_block() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, EncodeBytes::Binary);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();

        assert_eq!(mapper.max_table_rows(), 2); // 2 instructions

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["messages"].num_rows(), 1);
        assert_eq!(batches["instructions"].num_rows(), 2);
        assert_eq!(batches["rewards"].num_rows(), 1);
    }

    #[test]
    fn test_flush_resets_builders() {
        let block = make_test_block(1);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, EncodeBytes::Binary);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
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
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, EncodeBytes::Binary);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 0);
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(true, EncodeBytes::Binary);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), Some("NEW")).unwrap();

        let batches = mapper.flush().unwrap();
        let blocks_batch = &batches["blocks"];
        let last_col = blocks_batch.num_columns() - 1;
        assert_eq!(blocks_batch.schema().field(last_col).name(), "fork_step");
        let fork_col = blocks_batch.column(last_col).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(fork_col.value(0), "NEW");
    }

    #[test]
    fn test_encode_bytes_hex() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["transactions"].num_rows(), 1);
        // signature column should be Utf8 when encoding is Hex
        let sig_idx = batches["transactions"].schema().index_of("signature").unwrap();
        let sig_col = batches["transactions"].column(sig_idx);
        assert_eq!(*sig_col.data_type(), arrow::datatypes::DataType::Utf8);
    }

    #[test]
    fn test_encode_bytes_base58() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, EncodeBytes::Base58);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["transactions"].num_rows(), 1);
        let sig_idx = batches["transactions"].schema().index_of("signature").unwrap();
        let sig_col = batches["transactions"].column(sig_idx);
        assert_eq!(*sig_col.data_type(), arrow::datatypes::DataType::Utf8);
    }
}
