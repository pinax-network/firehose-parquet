use super::proto::solana;
use super::schema;
use super::vote::is_vote_transaction;
use arrow::array::*;
use arrow::datatypes::{Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{decode_base58, BytesColumn, BytesListColumn, EncodeBytes};
use firehose_parquet::traits::{
    est_bin, est_bool, est_f64, est_i64, est_list_str, est_list_u64, est_opt_str, est_str, est_u32,
    est_u64, BlockIdentity, BlockMapper, CanonicalBuilder, PreparedIdentity,
};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

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

fn index_list_builder() -> ListBuilder<UInt8Builder> {
    ListBuilder::new(UInt8Builder::new()).with_field(schema::index_element_field())
}

fn append_indices(builder: &mut ListBuilder<UInt8Builder>, indices: &[u8]) {
    builder.values().append_slice(indices);
    builder.append(true);
}

fn estimated_index_list_bytes(builder: &mut ListBuilder<UInt8Builder>) -> usize {
    (builder.len() + 1) * std::mem::size_of::<i32>() + builder.values().len()
}

fn solana_hash_bytes(hash: &str) -> Vec<u8> {
    match decode_base58(hash) {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(
                %hash,
                %error,
                "invalid solana base58 hash, falling back to raw UTF-8 bytes; output may be inconsistent across encodings"
            );
            hash.as_bytes().to_vec()
        }
    }
}

fn reward_type_text(value: i32) -> &'static str {
    solana::RewardType::try_from(value)
        .map(|reward_type| reward_type.as_str_name())
        .unwrap_or("UNKNOWN")
}

fn estimated_dictionary_index_bytes(len: usize) -> usize {
    // Largest-table tracking only needs a cheap relative estimate. For enum-backed
    // dictionary columns, the shared string dictionary cardinality is fixed and
    // small, so counting the per-row indices is sufficient for that comparison.
    len * std::mem::size_of::<i32>()
}

/// Append a single transaction to the given [`TransactionsBuilder`].
fn append_transaction(
    builder: &mut TransactionsBuilder,
    slot: u64,
    tx_idx: u32,
    tx: &solana::Transaction,
    meta: &solana::TransactionStatusMeta,
    identity: &PreparedIdentity,
    fork_step: Option<&str>,
) {
    builder.canonical.append(identity);
    builder.slot.append_value(slot);
    builder.transaction_index.append_value(tx_idx);
    if let Some(sig) = tx.signatures.first() {
        builder.signature.append_value(sig);
    } else {
        builder.signature.append_value(&[] as &[u8]);
    }
    builder
        .num_signatures
        .append_value(tx.signatures.len() as u32);
    builder.fee.append_value(meta.fee);
    if let Some(ref err) = meta.err {
        if !err.err.is_empty() {
            builder.err.append_value(&err.err);
            builder.success.append_value(false);
        } else {
            builder.err.append_null();
            builder.success.append_value(true);
        }
    } else {
        builder.err.append_null();
        builder.success.append_value(true);
    }
    match meta.compute_units_consumed {
        Some(cu) => builder.compute_units_consumed.append_value(cu),
        None => builder.compute_units_consumed.append_null(),
    }
    match meta.cost_units {
        Some(cu) => builder.cost_units.append_value(cu),
        None => builder.cost_units.append_null(),
    }
    {
        let vals = builder.log_messages.values();
        for log in &meta.log_messages {
            vals.append_value(log);
        }
        builder.log_messages.append(true);
    }
    {
        let vals = builder.pre_balances.values();
        for b in &meta.pre_balances {
            vals.append_value(*b);
        }
        builder.pre_balances.append(true);
    }
    {
        let vals = builder.post_balances.values();
        for b in &meta.post_balances {
            vals.append_value(*b);
        }
        builder.post_balances.append(true);
    }
    // return_data (program_id + data) from meta
    match &meta.return_data {
        Some(rd) => {
            builder.return_data_program_id.append_value(&rd.program_id);
            builder.return_data.append_value(&rd.data);
        }
        None => {
            builder.return_data_program_id.append_null();
            builder.return_data.append_null();
        }
    }
    append_fork_step(&mut builder.fork_step, fork_step);
}

pub struct SolanaBlockMapper {
    with_votes: bool,
    include_failed_transactions: bool,
    blocks: BlocksBuilder,
    transactions: TransactionsBuilder,
    vote_transactions: Option<TransactionsBuilder>,
    messages: MessagesBuilder,
    instructions: InstructionsBuilder,
    rewards: RewardsBuilder,
    token_balances: TokenBalancesBuilder,
    account_lookups: AccountLookupsBuilder,
    blocks_schema: Schema,
    transactions_schema: Schema,
    vote_transactions_schema: Schema,
    messages_schema: Schema,
    instructions_schema: Schema,
    rewards_schema: Schema,
    token_balances_schema: Schema,
    account_lookups_schema: Schema,
}

impl SolanaBlockMapper {
    pub fn new(
        with_votes: bool,
        include_fork_step: bool,
        encoding: EncodeBytes,
        synthetic_partition_routing: bool,
        include_failed_transactions: bool,
    ) -> Self {
        Self {
            with_votes,
            include_failed_transactions,
            blocks: BlocksBuilder::new(include_fork_step, &encoding),
            transactions: TransactionsBuilder::new(include_fork_step, &encoding),
            vote_transactions: if with_votes {
                Some(TransactionsBuilder::new(include_fork_step, &encoding))
            } else {
                None
            },
            messages: MessagesBuilder::new(include_fork_step, &encoding),
            instructions: InstructionsBuilder::new(include_fork_step, &encoding),
            rewards: RewardsBuilder::new(include_fork_step, &encoding),
            token_balances: TokenBalancesBuilder::new(include_fork_step, &encoding),
            account_lookups: AccountLookupsBuilder::new(include_fork_step, &encoding),
            blocks_schema: schema::blocks_schema(
                include_fork_step,
                &encoding,
                synthetic_partition_routing,
            ),
            transactions_schema: schema::transactions_schema(
                include_fork_step,
                &encoding,
                synthetic_partition_routing,
            ),
            vote_transactions_schema: schema::transactions_schema(
                include_fork_step,
                &encoding,
                synthetic_partition_routing,
            ),
            messages_schema: schema::messages_schema(
                include_fork_step,
                &encoding,
                synthetic_partition_routing,
            ),
            instructions_schema: schema::instructions_schema(
                include_fork_step,
                &encoding,
                synthetic_partition_routing,
            ),
            rewards_schema: schema::rewards_schema(
                include_fork_step,
                &encoding,
                synthetic_partition_routing,
            ),
            token_balances_schema: schema::token_balances_schema(
                include_fork_step,
                &encoding,
                synthetic_partition_routing,
            ),
            account_lookups_schema: schema::account_lookups_schema(
                include_fork_step,
                &encoding,
                synthetic_partition_routing,
            ),
        }
    }

    fn map_solana_block(
        &mut self,
        block: &solana::Block,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<()> {
        let slot = block.slot;
        let blockhash_bytes = solana_hash_bytes(&block.blockhash);
        let previous_blockhash_bytes = solana_hash_bytes(&block.previous_blockhash);
        // Canonical ids and time come from the block: its base58 blockhashes and
        // `block_time` (null timestamp and date when missing).
        let prepared = self
            .blocks
            .canonical
            .prepare_with_ids(identity, &blockhash_bytes, &previous_blockhash_bytes)?
            .with_timestamp_seconds(block.block_time.as_ref().map(|bt| bt.timestamp))?;
        let identity = &prepared;

        self.blocks.canonical.append(identity);
        self.blocks.slot.append_value(slot);
        self.blocks.parent_slot.append_value(block.parent_slot);
        match &block.block_height {
            Some(bh) => self.blocks.block_height.append_value(bh.block_height),
            None => self.blocks.block_height.append_null(),
        }
        self.blocks.blockhash.append_value(&blockhash_bytes);
        self.blocks
            .previous_blockhash
            .append_value(&previous_blockhash_bytes);
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
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // Each accepted block envelope owns its reward numbering, independent
        // of earlier buffered blocks or mapper flushes. Preserve source order:
        // transaction rewards first, then block rewards.
        let mut next_reward_index = 0_u64;
        for (tx_idx, confirmed_tx) in block.transactions.iter().enumerate() {
            self.map_transaction(
                slot,
                tx_idx as u32,
                confirmed_tx,
                identity,
                fork_step,
                &mut next_reward_index,
            )?;
        }

        for reward in &block.rewards {
            self.append_reward(
                slot,
                &mut next_reward_index,
                reward,
                "block",
                None,
                identity,
                fork_step,
            )?;
        }
        Ok(())
    }

    fn map_transaction(
        &mut self,
        slot: u64,
        tx_idx: u32,
        confirmed: &solana::ConfirmedTransaction,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
        next_reward_index: &mut u64,
    ) -> anyhow::Result<()> {
        let tx = match confirmed.transaction.as_ref() {
            Some(t) => t,
            None => return Ok(()),
        };
        let meta = match confirmed.meta.as_ref() {
            Some(m) => m,
            None => return Ok(()),
        };
        // Skip failed transactions unless --include-failed-transactions is set.
        // Some Firehose endpoints include `TransactionError { err: vec![] }` for
        // successful txs instead of omitting the field, so check the inner bytes.
        if !self.include_failed_transactions && meta.err.as_ref().is_some_and(|e| !e.err.is_empty())
        {
            return Ok(());
        }
        let msg = match tx.message.as_ref() {
            Some(m) => m,
            None => return Ok(()),
        };

        // Vote transactions go to a separate table (no messages/instructions)
        if is_vote_transaction(tx) {
            if let Some(ref mut vote_txs) = self.vote_transactions {
                append_transaction(vote_txs, slot, tx_idx, tx, meta, identity, fork_step);
            }
            return Ok(());
        }

        // Successful non-vote transaction
        append_transaction(
            &mut self.transactions,
            slot,
            tx_idx,
            tx,
            meta,
            identity,
            fork_step,
        );

        // messages
        self.messages.canonical.append(identity);
        self.messages.slot.append_value(slot);
        self.messages.transaction_index.append_value(tx_idx);
        self.messages.message_index.append_value(0);
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
            self.messages.num_readonly_unsigned_accounts.append_value(0);
        }
        self.messages
            .recent_blockhash
            .append_value(&msg.recent_blockhash);
        self.messages.versioned.append_value(msg.versioned);
        for key in &msg.account_keys {
            self.messages.account_keys.append_value(key);
        }
        self.messages.account_keys.append(true);
        // loaded addresses from meta (resolved from address table lookups)
        if meta.loaded_writable_addresses.is_empty() {
            self.messages.loaded_writable_addresses.append(false);
        } else {
            for addr in &meta.loaded_writable_addresses {
                self.messages.loaded_writable_addresses.append_value(addr);
            }
            self.messages.loaded_writable_addresses.append(true);
        }
        if meta.loaded_readonly_addresses.is_empty() {
            self.messages.loaded_readonly_addresses.append(false);
        } else {
            for addr in &meta.loaded_readonly_addresses {
                self.messages.loaded_readonly_addresses.append_value(addr);
            }
            self.messages.loaded_readonly_addresses.append(true);
        }
        append_fork_step(&mut self.messages.fork_step, fork_step);

        // instructions (top-level)
        let mut global_instr_idx = 0u32;
        for instr in &msg.instructions {
            self.instructions.canonical.append(identity);
            self.instructions.slot.append_value(slot);
            self.instructions.transaction_index.append_value(tx_idx);
            self.instructions
                .instruction_index
                .append_value(global_instr_idx);
            self.instructions
                .program_id_index
                .append_value(instr.program_id_index);
            append_indices(&mut self.instructions.accounts, &instr.accounts);
            self.instructions.data.append_value(&instr.data);
            self.instructions.is_inner.append_value(false);
            self.instructions.inner_index.append_null();
            self.instructions.stack_height.append_null();
            self.instructions.parent_instruction_index.append_null();
            self.instructions.inner_instruction_index.append_null();
            append_fork_step(&mut self.instructions.fork_step, fork_step);
            global_instr_idx += 1;
        }

        // instructions (inner)
        for inner_set in &meta.inner_instructions {
            for (inner_position, inner) in inner_set.instructions.iter().enumerate() {
                let inner_position = u32::try_from(inner_position)?;
                self.instructions.canonical.append(identity);
                self.instructions.slot.append_value(slot);
                self.instructions.transaction_index.append_value(tx_idx);
                self.instructions
                    .instruction_index
                    .append_value(global_instr_idx);
                self.instructions
                    .program_id_index
                    .append_value(inner.program_id_index);
                append_indices(&mut self.instructions.accounts, &inner.accounts);
                self.instructions.data.append_value(&inner.data);
                self.instructions.is_inner.append_value(true);
                self.instructions.inner_index.append_value(inner_set.index);
                self.instructions
                    .parent_instruction_index
                    .append_value(inner_set.index);
                self.instructions
                    .inner_instruction_index
                    .append_value(inner_position);
                match inner.stack_height {
                    Some(sh) => self.instructions.stack_height.append_value(sh),
                    None => self.instructions.stack_height.append_null(),
                }
                append_fork_step(&mut self.instructions.fork_step, fork_step);
                global_instr_idx += 1;
            }
        }

        // token balances (pre + post)
        self.map_token_balances(
            slot,
            tx_idx,
            "pre",
            &meta.pre_token_balances,
            identity,
            fork_step,
        );
        self.map_token_balances(
            slot,
            tx_idx,
            "post",
            &meta.post_token_balances,
            identity,
            fork_step,
        );

        // address table lookups from the message
        for (lookup_idx, lookup) in msg.address_table_lookups.iter().enumerate() {
            self.account_lookups.canonical.append(identity);
            self.account_lookups.slot.append_value(slot);
            self.account_lookups.transaction_index.append_value(tx_idx);
            self.account_lookups
                .lookup_index
                .append_value(lookup_idx as u32);
            self.account_lookups
                .account_key
                .append_value(&lookup.account_key);
            append_indices(
                &mut self.account_lookups.writable_indexes,
                &lookup.writable_indexes,
            );
            append_indices(
                &mut self.account_lookups.readonly_indexes,
                &lookup.readonly_indexes,
            );
            append_fork_step(&mut self.account_lookups.fork_step, fork_step);
        }

        // per-transaction rewards
        for reward in &meta.rewards {
            self.append_reward(
                slot,
                next_reward_index,
                reward,
                "transaction",
                Some(tx_idx),
                identity,
                fork_step,
            )?;
        }
        Ok(())
    }

    fn map_token_balances(
        &mut self,
        slot: u64,
        tx_idx: u32,
        balance_type: &str,
        balances: &[solana::TokenBalance],
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        for (i, tb) in balances.iter().enumerate() {
            self.token_balances.canonical.append(identity);
            self.token_balances.slot.append_value(slot);
            self.token_balances.transaction_index.append_value(tx_idx);
            self.token_balances.balance_index.append_value(i as u32);
            self.token_balances.balance_type.append_value(balance_type);
            self.token_balances
                .account_index
                .append_value(tb.account_index);
            self.token_balances.mint.append_value(&tb.mint);
            self.token_balances.owner.append_value(&tb.owner);
            self.token_balances.program_id.append_value(&tb.program_id);
            // UiTokenAmount fields
            if let Some(ref ui) = tb.ui_token_amount {
                self.token_balances.amount.append_value(&ui.amount);
                self.token_balances.ui_amount.append_value(ui.ui_amount);
                self.token_balances.decimals.append_value(ui.decimals);
                self.token_balances
                    .ui_amount_string
                    .append_value(&ui.ui_amount_string);
            } else {
                self.token_balances.amount.append_value("");
                self.token_balances.ui_amount.append_null();
                self.token_balances.decimals.append_value(0);
                self.token_balances.ui_amount_string.append_value("");
            }
            append_fork_step(&mut self.token_balances.fork_step, fork_step);
        }
    }

    fn append_reward(
        &mut self,
        slot: u64,
        next_reward_index: &mut u64,
        reward: &solana::Reward,
        source: &str,
        tx_idx: Option<u32>,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<()> {
        let idx = u32::try_from(*next_reward_index)
            .map_err(|_| anyhow::anyhow!("slot {slot} has more rewards than UInt32 can index"))?;
        self.rewards.canonical.append(identity);
        self.rewards.slot.append_value(slot);
        self.rewards.reward_index.append_value(idx);
        self.rewards.pubkey.append_value(&reward.pubkey);
        self.rewards.lamports.append_value(reward.lamports);
        self.rewards.post_balance.append_value(reward.post_balance);
        self.rewards
            .reward_type
            .append_value(reward_type_text(reward.reward_type));
        if reward.commission.is_empty() {
            self.rewards.commission.append_null();
        } else {
            self.rewards.commission.append_value(&reward.commission);
        }
        self.rewards.source.append_value(source);
        match tx_idx {
            Some(i) => self.rewards.transaction_index.append_value(i),
            None => self.rewards.transaction_index.append_null(),
        }
        append_fork_step(&mut self.rewards.fork_step, fork_step);
        *next_reward_index += 1;
        Ok(())
    }
}

impl SolanaBlockMapper {
    fn map_decoded(
        &mut self,
        block: solana::Block,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        let tx_count = block.transactions.len() as u64;
        self.map_solana_block(&block, identity, fork_step)?;
        Ok(tx_count)
    }
}

impl BlockMapper for SolanaBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        self.map_decoded(solana::Block::decode(block_bytes)?, identity, fork_step)
    }

    fn map_block_bytes(
        &mut self,
        block_bytes: prost::bytes::Bytes,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        self.map_decoded(solana::Block::decode(block_bytes)?, identity, fork_step)
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
        if let Some(ref mut vote_txs) = self.vote_transactions {
            result.insert(
                "vote_transactions".to_string(),
                vote_txs.finish(&self.vote_transactions_schema)?,
            );
        }
        result.insert(
            "messages".to_string(),
            self.messages.finish(&self.messages_schema)?,
        );
        result.insert(
            "instructions".to_string(),
            self.instructions.finish(&self.instructions_schema)?,
        );
        result.insert(
            "rewards".to_string(),
            self.rewards.finish(&self.rewards_schema)?,
        );
        result.insert(
            "token_balances".to_string(),
            self.token_balances.finish(&self.token_balances_schema)?,
        );
        result.insert(
            "account_lookups".to_string(),
            self.account_lookups.finish(&self.account_lookups_schema)?,
        );
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        let mut max = self
            .blocks
            .canonical
            .len()
            .max(self.transactions.canonical.len())
            .max(self.messages.canonical.len())
            .max(self.instructions.canonical.len())
            .max(self.rewards.canonical.len())
            .max(self.token_balances.canonical.len())
            .max(self.account_lookups.canonical.len());
        if let Some(ref vote_txs) = self.vote_transactions {
            max = max.max(vote_txs.canonical.len());
        }
        max
    }

    fn total_rows(&self) -> usize {
        let mut total = self.blocks.canonical.len()
            + self.transactions.canonical.len()
            + self.messages.canonical.len()
            + self.instructions.canonical.len()
            + self.rewards.canonical.len()
            + self.token_balances.canonical.len()
            + self.account_lookups.canonical.len();
        if let Some(ref vote_txs) = self.vote_transactions {
            total += vote_txs.canonical.len();
        }
        total
    }

    fn largest_table(&mut self) -> (&str, usize) {
        let blocks = self.blocks.canonical.estimated_bytes()
            + est_u64(&self.blocks.slot)
            + est_u64(&self.blocks.parent_slot)
            + est_u64(&self.blocks.block_height)
            + self.blocks.blockhash.estimated_bytes()
            + self.blocks.previous_blockhash.estimated_bytes()
            + est_i64(&self.blocks.block_time)
            + est_u32(&self.blocks.num_transactions)
            + est_u32(&self.blocks.num_rewards)
            + est_opt_str(&self.blocks.fork_step);
        let transactions = self.transactions.estimated_bytes();
        let vote_transactions = match self.vote_transactions {
            Some(ref mut vt) => vt.estimated_bytes(),
            None => 0,
        };
        let messages = self.messages.canonical.estimated_bytes()
            + est_u64(&self.messages.slot)
            + est_u32(&self.messages.transaction_index)
            + est_u32(&self.messages.message_index)
            + est_u32(&self.messages.num_required_signatures)
            + est_u32(&self.messages.num_readonly_signed_accounts)
            + est_u32(&self.messages.num_readonly_unsigned_accounts)
            + self.messages.recent_blockhash.estimated_bytes()
            + est_bool(&self.messages.versioned)
            + self.messages.account_keys.estimated_bytes()
            + self.messages.loaded_writable_addresses.estimated_bytes()
            + self.messages.loaded_readonly_addresses.estimated_bytes()
            + est_opt_str(&self.messages.fork_step);
        let instructions = self.instructions.canonical.estimated_bytes()
            + est_u64(&self.instructions.slot)
            + est_u32(&self.instructions.transaction_index)
            + est_u32(&self.instructions.instruction_index)
            + est_u32(&self.instructions.program_id_index)
            + estimated_index_list_bytes(&mut self.instructions.accounts)
            + est_bin(&self.instructions.data)
            + est_bool(&self.instructions.is_inner)
            + est_u32(&self.instructions.inner_index)
            + est_u32(&self.instructions.stack_height)
            + est_u32(&self.instructions.parent_instruction_index)
            + est_u32(&self.instructions.inner_instruction_index)
            + est_opt_str(&self.instructions.fork_step);
        let rewards = self.rewards.canonical.estimated_bytes()
            + est_u64(&self.rewards.slot)
            + est_u32(&self.rewards.reward_index)
            + est_str(&self.rewards.pubkey)
            + est_i64(&self.rewards.lamports)
            + est_u64(&self.rewards.post_balance)
            + estimated_dictionary_index_bytes(self.rewards.reward_type.len())
            + est_str(&self.rewards.commission)
            + est_str(&self.rewards.source)
            + est_u32(&self.rewards.transaction_index)
            + est_opt_str(&self.rewards.fork_step);
        let token_balances = self.token_balances.canonical.estimated_bytes()
            + est_u64(&self.token_balances.slot)
            + est_u32(&self.token_balances.transaction_index)
            + est_u32(&self.token_balances.balance_index)
            + est_str(&self.token_balances.balance_type)
            + est_u32(&self.token_balances.account_index)
            + est_str(&self.token_balances.mint)
            + est_str(&self.token_balances.owner)
            + est_str(&self.token_balances.program_id)
            + est_str(&self.token_balances.amount)
            + est_f64(&self.token_balances.ui_amount)
            + est_u32(&self.token_balances.decimals)
            + est_str(&self.token_balances.ui_amount_string)
            + est_opt_str(&self.token_balances.fork_step);
        let account_lookups = self.account_lookups.canonical.estimated_bytes()
            + est_u64(&self.account_lookups.slot)
            + est_u32(&self.account_lookups.transaction_index)
            + est_u32(&self.account_lookups.lookup_index)
            + self.account_lookups.account_key.estimated_bytes()
            + estimated_index_list_bytes(&mut self.account_lookups.writable_indexes)
            + estimated_index_list_bytes(&mut self.account_lookups.readonly_indexes)
            + est_opt_str(&self.account_lookups.fork_step);
        [
            ("blocks", blocks),
            ("transactions", transactions),
            ("vote_transactions", vote_transactions),
            ("messages", messages),
            ("instructions", instructions),
            ("rewards", rewards),
            ("token_balances", token_balances),
            ("account_lookups", account_lookups),
        ]
        .into_iter()
        .max_by_key(|&(_, s)| s)
        .unwrap_or(("blocks", 0))
    }

    fn table_names(&self) -> Vec<&str> {
        if self.with_votes {
            schema::WITH_VOTES_TABLE_NAMES.to_vec()
        } else {
            schema::BASE_TABLE_NAMES.to_vec()
        }
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
    blockhash: BytesColumn,
    previous_blockhash: BytesColumn,
    block_time: Int64Builder,
    num_transactions: UInt32Builder,
    num_rewards: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            slot: UInt64Builder::new(),
            parent_slot: UInt64Builder::new(),
            block_height: UInt64Builder::new(),
            blockhash: BytesColumn::new(encoding),
            previous_blockhash: BytesColumn::new(encoding),
            block_time: Int64Builder::new(),
            num_transactions: UInt32Builder::new(),
            num_rewards: UInt32Builder::new(),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.parent_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.block_height.finish()) as Arc<dyn Array>,
            self.blockhash.finish(),
            self.previous_blockhash.finish(),
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
    err: BinaryBuilder,
    success: BooleanBuilder,
    compute_units_consumed: UInt64Builder,
    cost_units: UInt64Builder,
    log_messages: ListBuilder<StringBuilder>,
    pre_balances: ListBuilder<UInt64Builder>,
    post_balances: ListBuilder<UInt64Builder>,
    return_data_program_id: BytesColumn,
    return_data: BinaryBuilder,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            signature: BytesColumn::new(encoding),
            num_signatures: UInt32Builder::new(),
            fee: UInt64Builder::new(),
            err: BinaryBuilder::new(),
            success: BooleanBuilder::new(),
            compute_units_consumed: UInt64Builder::new(),
            cost_units: UInt64Builder::new(),
            log_messages: ListBuilder::new(StringBuilder::new()),
            pre_balances: ListBuilder::new(UInt64Builder::new()),
            post_balances: ListBuilder::new(UInt64Builder::new()),
            return_data_program_id: BytesColumn::new(encoding),
            return_data: BinaryBuilder::new(),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
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
            Arc::new(self.err.finish()) as Arc<dyn Array>,
            Arc::new(self.success.finish()) as Arc<dyn Array>,
            Arc::new(self.compute_units_consumed.finish()) as Arc<dyn Array>,
            Arc::new(self.log_messages.finish()) as Arc<dyn Array>,
            Arc::new(self.pre_balances.finish()) as Arc<dyn Array>,
            Arc::new(self.post_balances.finish()) as Arc<dyn Array>,
            Arc::new(self.cost_units.finish()) as Arc<dyn Array>,
            self.return_data_program_id.finish(),
            Arc::new(self.return_data.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }

    fn estimated_bytes(&mut self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.slot)
            + est_u32(&self.transaction_index)
            + self.signature.estimated_bytes()
            + est_u32(&self.num_signatures)
            + est_u64(&self.fee)
            + est_bin(&self.err)
            + est_bool(&self.success)
            + est_u64(&self.compute_units_consumed)
            + est_u64(&self.cost_units)
            + est_list_str(&mut self.log_messages)
            + est_list_u64(&mut self.pre_balances)
            + est_list_u64(&mut self.post_balances)
            + self.return_data_program_id.estimated_bytes()
            + est_bin(&self.return_data)
            + est_opt_str(&self.fork_step)
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
    loaded_writable_addresses: BytesListColumn,
    loaded_readonly_addresses: BytesListColumn,
    fork_step: Option<StringBuilder>,
}

impl MessagesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            message_index: UInt32Builder::new(),
            num_required_signatures: UInt32Builder::new(),
            num_readonly_signed_accounts: UInt32Builder::new(),
            num_readonly_unsigned_accounts: UInt32Builder::new(),
            recent_blockhash: BytesColumn::new(encoding),
            versioned: BooleanBuilder::new(),
            account_keys: BytesListColumn::new(encoding),
            loaded_writable_addresses: BytesListColumn::new(encoding),
            loaded_readonly_addresses: BytesListColumn::new(encoding),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
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
            self.loaded_writable_addresses.finish(),
            self.loaded_readonly_addresses.finish(),
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
    accounts: ListBuilder<UInt8Builder>,
    data: BinaryBuilder,
    is_inner: BooleanBuilder,
    inner_index: UInt32Builder,
    stack_height: UInt32Builder,
    parent_instruction_index: UInt32Builder,
    inner_instruction_index: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl InstructionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            instruction_index: UInt32Builder::new(),
            program_id_index: UInt32Builder::new(),
            accounts: index_list_builder(),
            data: BinaryBuilder::new(),
            is_inner: BooleanBuilder::new(),
            inner_index: UInt32Builder::new(),
            stack_height: UInt32Builder::new(),
            parent_instruction_index: UInt32Builder::new(),
            inner_instruction_index: UInt32Builder::new(),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.transaction_index.finish()) as Arc<dyn Array>,
            Arc::new(self.instruction_index.finish()) as Arc<dyn Array>,
            Arc::new(self.program_id_index.finish()) as Arc<dyn Array>,
            Arc::new(self.accounts.finish()) as Arc<dyn Array>,
            Arc::new(self.data.finish()) as Arc<dyn Array>,
            Arc::new(self.is_inner.finish()) as Arc<dyn Array>,
            Arc::new(self.inner_index.finish()) as Arc<dyn Array>,
            Arc::new(self.stack_height.finish()) as Arc<dyn Array>,
            Arc::new(self.parent_instruction_index.finish()) as Arc<dyn Array>,
            Arc::new(self.inner_instruction_index.finish()) as Arc<dyn Array>,
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
    reward_type: StringDictionaryBuilder<Int32Type>,
    commission: StringBuilder,
    source: StringBuilder,
    transaction_index: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl RewardsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            slot: UInt64Builder::new(),
            reward_index: UInt32Builder::new(),
            pubkey: StringBuilder::new(),
            lamports: Int64Builder::new(),
            post_balance: UInt64Builder::new(),
            reward_type: StringDictionaryBuilder::new(),
            commission: StringBuilder::new(),
            source: StringBuilder::new(),
            transaction_index: UInt32Builder::new(),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
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
            Arc::new(self.source.finish()) as Arc<dyn Array>,
            Arc::new(self.transaction_index.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct TokenBalancesBuilder {
    canonical: CanonicalBuilder,
    slot: UInt64Builder,
    transaction_index: UInt32Builder,
    balance_index: UInt32Builder,
    balance_type: StringBuilder,
    account_index: UInt32Builder,
    mint: StringBuilder,
    owner: StringBuilder,
    program_id: StringBuilder,
    amount: StringBuilder,
    ui_amount: Float64Builder,
    decimals: UInt32Builder,
    ui_amount_string: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl TokenBalancesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            balance_index: UInt32Builder::new(),
            balance_type: StringBuilder::new(),
            account_index: UInt32Builder::new(),
            mint: StringBuilder::new(),
            owner: StringBuilder::new(),
            program_id: StringBuilder::new(),
            amount: StringBuilder::new(),
            ui_amount: Float64Builder::new(),
            decimals: UInt32Builder::new(),
            ui_amount_string: StringBuilder::new(),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.transaction_index.finish()) as Arc<dyn Array>,
            Arc::new(self.balance_index.finish()) as Arc<dyn Array>,
            Arc::new(self.balance_type.finish()) as Arc<dyn Array>,
            Arc::new(self.account_index.finish()) as Arc<dyn Array>,
            Arc::new(self.mint.finish()) as Arc<dyn Array>,
            Arc::new(self.owner.finish()) as Arc<dyn Array>,
            Arc::new(self.program_id.finish()) as Arc<dyn Array>,
            Arc::new(self.amount.finish()) as Arc<dyn Array>,
            Arc::new(self.ui_amount.finish()) as Arc<dyn Array>,
            Arc::new(self.decimals.finish()) as Arc<dyn Array>,
            Arc::new(self.ui_amount_string.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct AccountLookupsBuilder {
    canonical: CanonicalBuilder,
    slot: UInt64Builder,
    transaction_index: UInt32Builder,
    lookup_index: UInt32Builder,
    account_key: BytesColumn,
    writable_indexes: ListBuilder<UInt8Builder>,
    readonly_indexes: ListBuilder<UInt8Builder>,
    fork_step: Option<StringBuilder>,
}

impl AccountLookupsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            slot: UInt64Builder::new(),
            transaction_index: UInt32Builder::new(),
            lookup_index: UInt32Builder::new(),
            account_key: BytesColumn::new(encoding),
            writable_indexes: index_list_builder(),
            readonly_indexes: index_list_builder(),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.transaction_index.finish()) as Arc<dyn Array>,
            Arc::new(self.lookup_index.finish()) as Arc<dyn Array>,
            self.account_key.finish(),
            Arc::new(self.writable_indexes.finish()) as Arc<dyn Array>,
            Arc::new(self.readonly_indexes.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::super::vote::VOTE_PROGRAM_ID;
    use super::*;
    use solana_vote_interface::{
        instruction::VoteInstruction,
        state::{TowerSync, Vote, VoteAuthorize, VoteStateUpdate},
    };

    fn vote_payloads() -> Vec<Vec<u8>> {
        let vote = Vote {
            slots: vec![98, 99],
            ..Default::default()
        };
        let update = VoteStateUpdate::from(vec![(98, 2), (99, 1)]);
        let tower = TowerSync::from(vec![(98, 2), (99, 1)]);
        [
            VoteInstruction::Vote(vote.clone()),
            VoteInstruction::VoteSwitch(vote, Default::default()),
            VoteInstruction::UpdateVoteState(update.clone()),
            VoteInstruction::UpdateVoteStateSwitch(update.clone(), Default::default()),
            VoteInstruction::CompactUpdateVoteState(update.clone()),
            VoteInstruction::CompactUpdateVoteStateSwitch(update, Default::default()),
            VoteInstruction::TowerSync(tower.clone()),
            VoteInstruction::TowerSyncSwitch(tower, Default::default()),
        ]
        .into_iter()
        .map(|vote| bincode::serialize(&vote).unwrap())
        .collect()
    }

    fn get_string_value(batch: &RecordBatch, name: &str, row: usize) -> String {
        let column = batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column {name}"));

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

        panic!("column {name} is not Utf8 or dictionary-encoded Utf8");
    }

    fn make_test_solana_hash(fill_byte: u8) -> String {
        firehose_parquet::encode::encode_base58(&[fill_byte; 32])
    }

    pub(crate) fn make_test_block(slot: u64) -> solana::Block {
        solana::Block {
            slot,
            parent_slot: slot.saturating_sub(1),
            blockhash: make_test_solana_hash(0x01),
            previous_blockhash: make_test_solana_hash(0x02),
            block_height: Some(solana::BlockHeight { block_height: slot }),
            block_time: Some(solana::UnixTimestamp {
                timestamp: 1_700_000_000 + slot as i64,
            }),
            transactions: vec![solana::ConfirmedTransaction {
                transaction: Some(solana::Transaction {
                    signatures: vec![vec![1u8; 64].into()],
                    message: Some(solana::Message {
                        header: Some(solana::MessageHeader {
                            num_required_signatures: 1,
                            num_readonly_signed_accounts: 0,
                            num_readonly_unsigned_accounts: 1,
                        }),
                        account_keys: vec![vec![2u8; 32].into(), vec![3u8; 32].into()],
                        recent_blockhash: vec![4u8; 32].into(),
                        instructions: vec![solana::CompiledInstruction {
                            program_id_index: 1,
                            accounts: vec![0].into(),
                            data: vec![5, 6, 7].into(),
                        }],
                        versioned: true,
                        address_table_lookups: vec![solana::MessageAddressTableLookup {
                            account_key: vec![10u8; 32].into(),
                            writable_indexes: vec![0, 1].into(),
                            readonly_indexes: vec![2].into(),
                        }],
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
                            accounts: vec![0].into(),
                            data: vec![8, 9].into(),
                            stack_height: Some(2),
                        }],
                    }],
                    log_messages: vec!["Program log: hello".to_string()],
                    pre_token_balances: vec![solana::TokenBalance {
                        account_index: 1,
                        mint: "So11111111111111111111111111111111111111112".into(),
                        ui_token_amount: Some(solana::UiTokenAmount {
                            ui_amount: 1.5,
                            decimals: 9,
                            amount: "1500000000".into(),
                            ui_amount_string: "1.5".into(),
                        }),
                        owner: "OwnerPubkey".into(),
                        program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(),
                    }],
                    post_token_balances: vec![solana::TokenBalance {
                        account_index: 1,
                        mint: "So11111111111111111111111111111111111111112".into(),
                        ui_token_amount: Some(solana::UiTokenAmount {
                            ui_amount: 0.5,
                            decimals: 9,
                            amount: "500000000".into(),
                            ui_amount_string: "0.5".into(),
                        }),
                        owner: "OwnerPubkey".into(),
                        program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(),
                    }],
                    rewards: vec![solana::Reward {
                        pubkey: "TxRewardPubkey".into(),
                        lamports: 10,
                        post_balance: 500,
                        reward_type: 1,
                        commission: String::new(),
                    }],
                    loaded_writable_addresses: vec![vec![20u8; 32].into()],
                    loaded_readonly_addresses: vec![vec![21u8; 32].into(), vec![22u8; 32].into()],
                    return_data: Some(solana::ReturnData {
                        program_id: vec![3u8; 32].into(),
                        data: vec![42, 43, 44].into(),
                    }),
                    compute_units_consumed: Some(1234),
                    cost_units: Some(5678),
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
    fn invalid_payload_time_preserves_all_previously_buffered_solana_rows() {
        let valid = make_test_block(100);
        for timestamp in [i64::MIN, i64::MAX, 1_700_000_000_000] {
            let mut invalid = valid.clone();
            invalid.block_time.as_mut().unwrap().timestamp = timestamp;
            let mut baseline =
                SolanaBlockMapper::new(false, true, EncodeBytes::Binary, false, false);
            let mut subject =
                SolanaBlockMapper::new(false, true, EncodeBytes::Binary, false, false);
            let identity = BlockIdentity::default();
            baseline
                .map_block(&valid.encode_to_vec(), &identity, None)
                .unwrap();
            subject
                .map_block(&valid.encode_to_vec(), &identity, None)
                .unwrap();
            assert!(subject
                .map_block(&invalid.encode_to_vec(), &identity, None)
                .is_err());
            assert_eq!(subject.flush().unwrap(), baseline.flush().unwrap());
        }
    }

    #[test]
    fn negative_payload_time_is_preserved_in_solana_timestamp_and_date() {
        let mut block = make_test_block(100);
        block.block_time.as_mut().unwrap().timestamp = -1;
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
            .unwrap();
        for batch in mapper
            .flush()
            .unwrap()
            .values()
            .filter(|batch| batch.num_rows() > 0)
        {
            let timestamps = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .unwrap();
            let dates = batch
                .column_by_name("date")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .unwrap();
            assert_eq!(timestamps.value(0), -1000);
            assert_eq!(dates.value(0), -1);
        }
    }

    #[test]
    fn test_map_and_flush_single_block() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["messages"].num_rows(), 1);
        assert_eq!(batches["instructions"].num_rows(), 2);
        // 1 block-level reward + 1 per-tx reward
        assert_eq!(batches["rewards"].num_rows(), 2);
        // 1 pre + 1 post token balance
        assert_eq!(batches["token_balances"].num_rows(), 2);
        // 1 address table lookup
        assert_eq!(batches["account_lookups"].num_rows(), 1);

        let rewards = &batches["rewards"];
        assert_eq!(
            rewards
                .column_by_name("reward_type")
                .expect("reward_type column should exist")
                .data_type(),
            &arrow::datatypes::DataType::Dictionary(
                Box::new(arrow::datatypes::DataType::Int32),
                Box::new(arrow::datatypes::DataType::Utf8),
            )
        );
        assert_eq!(get_string_value(rewards, "reward_type", 0), "Fee");
        assert_eq!(get_string_value(rewards, "reward_type", 1), "Fee");
    }

    #[test]
    fn test_missing_block_time_keeps_canonical_timestamps_null() {
        let mut block = make_test_block(100);
        block.block_time = None;
        let block_bytes = prost::Message::encode_to_vec(&block);
        let identity = BlockIdentity {
            block_num: 100,
            block_id: "firehose-envelope-id".to_string(),
            parent_num: 99,
            parent_id: "firehose-envelope-parent-id".to_string(),
            lib_num: 99,
            timestamp: 1_700_000_123,
            timestamp_nanos: 0,
            fork_step: None,
        };
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, true, false);
        mapper.map_block(&block_bytes, &identity, None).unwrap();

        let batches = mapper.flush().unwrap();
        let blocks = &batches["blocks"];
        let transactions = &batches["transactions"];

        let block_timestamp = blocks
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let block_date = blocks
            .column_by_name("date")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let block_time = blocks
            .column_by_name("block_time")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let tx_timestamp = transactions
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        assert!(
            block_timestamp.is_null(0),
            "canonical block timestamp should remain null when block_time is missing"
        );
        assert!(
            block_date.is_null(0),
            "canonical block date should remain null when block_time is missing"
        );
        assert!(
            block_time.is_null(0),
            "raw Solana block_time should remain null"
        );
        assert!(
            tx_timestamp.is_null(0),
            "child table canonical timestamps should remain null when block_time is missing"
        );
    }

    #[test]
    fn test_solana_canonical_ids_match_blockhash_fields() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);

        let identity = BlockIdentity {
            block_num: 100,
            block_id: "firehose-envelope-id".to_string(),
            parent_num: 99,
            parent_id: "firehose-envelope-parent-id".to_string(),
            lib_num: 99,
            timestamp: 1_700_000_100,
            timestamp_nanos: 0,
            fork_step: None,
        };

        mapper.map_block(&block_bytes, &identity, None).unwrap();
        let batches = mapper.flush().unwrap();
        let blocks = &batches["blocks"];

        let block_id = blocks
            .column_by_name("block_id")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let parent_id = blocks
            .column_by_name("parent_id")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let blockhash = blocks
            .column_by_name("blockhash")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let previous_blockhash = blocks
            .column_by_name("previous_blockhash")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        assert_eq!(block_id.value(0), blockhash.value(0));
        assert_eq!(parent_id.value(0), previous_blockhash.value(0));
    }

    #[test]
    fn test_solana_base58_canonical_ids_match_blockhash_fields() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Base58, false, false);

        let identity = BlockIdentity {
            block_num: 100,
            block_id: "firehose-envelope-id".to_string(),
            parent_num: 99,
            parent_id: "firehose-envelope-parent-id".to_string(),
            lib_num: 99,
            timestamp: 1_700_000_100,
            timestamp_nanos: 0,
            fork_step: None,
        };

        mapper.map_block(&block_bytes, &identity, None).unwrap();
        let batches = mapper.flush().unwrap();
        let blocks = &batches["blocks"];

        let block_id = blocks
            .column_by_name("block_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let parent_id = blocks
            .column_by_name("parent_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let blockhash = blocks
            .column_by_name("blockhash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let previous_blockhash = blocks
            .column_by_name("previous_blockhash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(block_id.value(0), blockhash.value(0));
        assert_eq!(parent_id.value(0), previous_blockhash.value(0));
    }

    fn block_with_vote_payload(data: Vec<u8>) -> solana::Block {
        let mut block = make_test_block(100);
        let message = block.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap();
        message.versioned = false;
        message.address_table_lookups.clear();
        message.account_keys[1] = VOTE_PROGRAM_ID.to_vec().into();
        message.instructions[0].data = data.into();
        block
    }

    fn assert_vote_activity_retained(label: &str, block: &solana::Block) {
        let confirmed = &block.transactions[0];
        let message = confirmed
            .transaction
            .as_ref()
            .unwrap()
            .message
            .as_ref()
            .unwrap();
        let meta = confirmed.meta.as_ref().unwrap();
        for with_votes in [false, true] {
            let mut mapper =
                SolanaBlockMapper::new(with_votes, false, EncodeBytes::Binary, false, false);
            mapper
                .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
                .unwrap();
            let batches = mapper.flush().unwrap();
            for (table, count) in [
                ("transactions", 1),
                ("messages", 1),
                ("instructions", message.instructions.len() + 1),
                ("token_balances", 2),
                ("rewards", 2),
                ("account_lookups", message.address_table_lookups.len()),
            ] {
                assert_eq!(
                    batches[table].num_rows(),
                    count,
                    "{label}: {table}, with_votes={with_votes}"
                );
            }
            if with_votes {
                assert_eq!(batches["vote_transactions"].num_rows(), 0, "{label}");
            }
            let instructions = &batches["instructions"];
            let payloads = instructions
                .column_by_name("data")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for (index, instruction) in message.instructions.iter().enumerate() {
                assert_eq!(payloads.value(index), instruction.data, "{label}");
            }
            let transactions = &batches["transactions"];
            for (name, expected) in [
                ("pre_balances", &meta.pre_balances),
                ("post_balances", &meta.post_balances),
            ] {
                let balances = transactions
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(0);
                assert_eq!(
                    balances
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .values(),
                    expected,
                    "{label}"
                );
            }
            let rewards = &batches["rewards"];
            let indices = rewards
                .column_by_name("reward_index")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            assert_eq!(indices.values(), &[0, 1], "{label}");
            assert_eq!(get_string_value(rewards, "source", 0), "transaction");
            assert_eq!(get_string_value(rewards, "pubkey", 0), "TxRewardPubkey");
            assert_eq!(
                get_string_value(&batches["token_balances"], "amount", 0),
                "1500000000"
            );
            assert_eq!(
                get_string_value(&batches["token_balances"], "amount", 1),
                "500000000"
            );
        }
    }

    #[test]
    fn test_vote_program_administration_and_mentions_retain_all_activity() {
        // Single-instruction administrative calls defeat a program-only simple-vote check.
        for instruction in [
            VoteInstruction::Withdraw(42),
            VoteInstruction::Authorize(Default::default(), VoteAuthorize::Voter),
            VoteInstruction::AuthorizeChecked(VoteAuthorize::Withdrawer),
            VoteInstruction::UpdateValidatorIdentity,
            VoteInstruction::UpdateCommission(5),
            VoteInstruction::InitializeAccount(Default::default()),
        ] {
            let block = block_with_vote_payload(bincode::serialize(&instruction).unwrap());
            assert_vote_activity_retained(&format!("{instruction:?}"), &block);
        }
        let mut mixed = block_with_vote_payload(vote_payloads().remove(0));
        let message = mixed.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap();
        message.account_keys.push(vec![3; 32].into());
        message.instructions.push(solana::CompiledInstruction {
            program_id_index: 2,
            accounts: vec![0].into(),
            data: vec![42].into(),
        });
        assert_vote_activity_retained("vote plus another instruction", &mixed);

        let mut create = block_with_vote_payload(
            bincode::serialize(&VoteInstruction::InitializeAccount(Default::default())).unwrap(),
        );
        let message = create.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap();
        message.account_keys.push(vec![0; 32].into()); // System-program create + Vote-program initialize.
        message.instructions.insert(
            0,
            solana::CompiledInstruction {
                program_id_index: 2,
                accounts: vec![0].into(),
                data: vec![0; 4].into(),
            },
        );
        assert_vote_activity_retained("create vote account", &create);

        let mut mention = make_test_block(100);
        let message = mention.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap();
        message.account_keys.push(VOTE_PROGRAM_ID.to_vec().into());
        assert_vote_activity_retained("unused Vote key with address lookup", &mention);
        let mut versioned = make_test_block(100);
        let message = versioned.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap();
        message.account_keys[1] = VOTE_PROGRAM_ID.to_vec().into();
        message.instructions[0].data = vote_payloads().remove(0).into();
        assert_vote_activity_retained("versioned transaction with address lookup", &versioned);
    }

    #[test]
    fn test_vote_filter_retains_unknown_malformed_and_ambiguous_transactions() {
        let valid = vote_payloads().remove(0);
        let mut trailing = valid.clone();
        trailing.push(0);
        let mut huge_length = 2_u32.to_le_bytes().to_vec();
        huge_length.extend(u64::MAX.to_le_bytes());
        for (label, data) in [
            ("empty", vec![]),
            ("truncated", valid[..valid.len() - 1].to_vec()),
            ("unknown variant", u32::MAX.to_le_bytes().to_vec()),
            ("trailing byte", trailing),
            ("oversized", vec![0; 1233]),
            ("unbounded slot count", huge_length),
            (
                "empty vote",
                bincode::serialize(&VoteInstruction::Vote(Vote::default())).unwrap(),
            ),
        ] {
            assert_vote_activity_retained(label, &block_with_vote_payload(data));
        }
        for case in 0..9 {
            let mut block = block_with_vote_payload(valid.clone());
            let tx = block.transactions[0].transaction.as_mut().unwrap();
            match case {
                0 => tx.message.as_mut().unwrap().instructions[0].program_id_index = u32::MAX,
                1 => tx.message.as_mut().unwrap().instructions[0].accounts = vec![255].into(),
                2 => tx.signatures.clear(),
                3 => tx.signatures = vec![vec![1; 64].into(); 3],
                4 => {
                    let len = tx.signatures[0].len();
                    tx.signatures[0].truncate(len - 1);
                }
                5 => tx.message.as_mut().unwrap().header = None,
                6 => {
                    tx.message
                        .as_mut()
                        .unwrap()
                        .header
                        .as_mut()
                        .unwrap()
                        .num_required_signatures = 2
                }
                7 => tx.message.as_mut().unwrap().recent_blockhash.clear(),
                8 => tx.message.as_mut().unwrap().account_keys[0].clear(),
                _ => unreachable!(),
            }
            assert_vote_activity_retained(&format!("invalid envelope {case}"), &block);
        }
    }

    #[test]
    fn test_all_recognized_vote_variants_use_optional_vote_table() {
        for (variant, payload) in vote_payloads().into_iter().enumerate() {
            for signature_count in [1, 2] {
                let mut block = block_with_vote_payload(payload.clone());
                if signature_count == 2 {
                    let tx = block.transactions[0].transaction.as_mut().unwrap();
                    tx.signatures.push(vec![2; 64].into());
                    let message = tx.message.as_mut().unwrap();
                    message.account_keys.insert(1, vec![3; 32].into());
                    message.instructions[0].program_id_index = 2;
                    message.header.as_mut().unwrap().num_required_signatures = 2;
                }
                for with_votes in [false, true] {
                    let mut mapper = SolanaBlockMapper::new(
                        with_votes,
                        false,
                        EncodeBytes::Binary,
                        false,
                        false,
                    );
                    mapper
                        .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
                        .unwrap();
                    let batches = mapper.flush().unwrap();
                    for table in [
                        "transactions",
                        "messages",
                        "instructions",
                        "token_balances",
                        "account_lookups",
                    ] {
                        assert_eq!(
                            batches[table].num_rows(),
                            0,
                            "variant={variant}, signatures={signature_count}, {table}"
                        );
                    }
                    assert_eq!(batches["rewards"].num_rows(), 1); // Existing vote-detail policy keeps block rewards.
                    if with_votes {
                        assert_eq!(batches["vote_transactions"].num_rows(), 1);
                    }
                }
            }
        }
    }

    #[test]
    fn test_vote_transactions_separated() {
        let mut block = make_test_block(100);
        // Add a vote transaction: account_keys[1] = Vote program ID
        block.transactions.push(solana::ConfirmedTransaction {
            transaction: Some(solana::Transaction {
                signatures: vec![vec![99u8; 64].into()],
                message: Some(solana::Message {
                    header: Some(solana::MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 1,
                    }),
                    account_keys: vec![vec![2u8; 32].into(), VOTE_PROGRAM_ID.to_vec().into()],
                    recent_blockhash: vec![4u8; 32].into(),
                    instructions: vec![solana::CompiledInstruction {
                        program_id_index: 1,
                        accounts: vec![0].into(),
                        data: vote_payloads().remove(0).into(),
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
                inner_instructions: vec![],
                log_messages: vec!["Program Vote111 invoke".into()],
                pre_token_balances: vec![],
                post_token_balances: vec![],
                rewards: vec![],
                loaded_writable_addresses: vec![],
                loaded_readonly_addresses: vec![],
                return_data: None,
                compute_units_consumed: Some(2100),
                cost_units: None,
            }),
        });

        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(true, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["transactions"].num_rows(), 1, "non-vote");
        assert_eq!(batches["vote_transactions"].num_rows(), 1, "vote");
        // messages and instructions only include non-vote transactions
        assert_eq!(batches["messages"].num_rows(), 1);
        assert_eq!(batches["instructions"].num_rows(), 2); // 1 top-level + 1 inner from non-vote tx
    }

    #[test]
    fn test_failed_transactions_skipped() {
        let mut block = make_test_block(100);
        // Add a failed transaction
        block.transactions.push(solana::ConfirmedTransaction {
            transaction: Some(solana::Transaction {
                signatures: vec![vec![88u8; 64].into()],
                message: Some(solana::Message {
                    header: Some(solana::MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 1,
                    }),
                    account_keys: vec![vec![2u8; 32].into(), vec![3u8; 32].into()],
                    recent_blockhash: vec![4u8; 32].into(),
                    instructions: vec![solana::CompiledInstruction {
                        program_id_index: 1,
                        accounts: vec![0].into(),
                        data: vec![9, 9, 9].into(),
                    }],
                    versioned: false,
                    address_table_lookups: vec![],
                }),
            }),
            meta: Some(solana::TransactionStatusMeta {
                err: Some(solana::TransactionError {
                    err: vec![1, 2, 3].into(),
                }),
                fee: 5000,
                pre_balances: vec![100_000, 0],
                post_balances: vec![95_000, 0],
                inner_instructions: vec![],
                log_messages: vec![],
                pre_token_balances: vec![],
                post_token_balances: vec![],
                rewards: vec![],
                loaded_writable_addresses: vec![],
                loaded_readonly_addresses: vec![],
                return_data: None,
                compute_units_consumed: Some(500),
                cost_units: None,
            }),
        });

        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(true, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        // Failed transaction is completely skipped
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["vote_transactions"].num_rows(), 0);
        assert_eq!(batches["messages"].num_rows(), 1);
        assert_eq!(batches["instructions"].num_rows(), 2);
    }

    #[test]
    fn test_failed_transactions_included() {
        let mut block = make_test_block(100);
        // Add a failed transaction
        block.transactions.push(solana::ConfirmedTransaction {
            transaction: Some(solana::Transaction {
                signatures: vec![vec![88u8; 64].into()],
                message: Some(solana::Message {
                    header: Some(solana::MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 1,
                    }),
                    account_keys: vec![vec![2u8; 32].into(), vec![3u8; 32].into()],
                    recent_blockhash: vec![4u8; 32].into(),
                    instructions: vec![solana::CompiledInstruction {
                        program_id_index: 1,
                        accounts: vec![0].into(),
                        data: vec![9, 9, 9].into(),
                    }],
                    versioned: false,
                    address_table_lookups: vec![],
                }),
            }),
            meta: Some(solana::TransactionStatusMeta {
                err: Some(solana::TransactionError {
                    err: vec![1, 2, 3].into(),
                }),
                fee: 5000,
                pre_balances: vec![100_000, 0],
                post_balances: vec![95_000, 0],
                inner_instructions: vec![],
                log_messages: vec![],
                pre_token_balances: vec![],
                post_token_balances: vec![],
                rewards: vec![],
                loaded_writable_addresses: vec![],
                loaded_readonly_addresses: vec![],
                return_data: None,
                compute_units_consumed: Some(500),
                cost_units: None,
            }),
        });

        let block_bytes = prost::Message::encode_to_vec(&block);
        // With include_failed_transactions = true, failed tx should be included
        let mut mapper = SolanaBlockMapper::new(true, false, EncodeBytes::Binary, false, true);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        // Failed transaction is included (1 successful + 1 failed = 2)
        assert_eq!(batches["transactions"].num_rows(), 2);
        assert_eq!(batches["vote_transactions"].num_rows(), 0);
        assert_eq!(batches["messages"].num_rows(), 2);
        assert_eq!(batches["instructions"].num_rows(), 3);

        // Verify the err and success columns for the failed transaction
        let success_col = batches["transactions"]
            .column_by_name("success")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        // First tx is successful, second is failed
        assert_eq!(success_col.value(0), true);
        assert_eq!(success_col.value(1), false);
    }

    #[test]
    fn test_flush_resets_builders() {
        let block = make_test_block(1);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
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
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 0);
        assert_eq!(batches["token_balances"].num_rows(), 0);
        assert_eq!(batches["account_lookups"].num_rows(), 0);
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, true, EncodeBytes::Binary, false, false);
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
    fn test_encode_bytes_hex() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Hex, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["transactions"].num_rows(), 1);
        // signature column should be Utf8 when encoding is Hex
        let sig_idx = batches["transactions"]
            .schema()
            .index_of("signature")
            .unwrap();
        let sig_col = batches["transactions"].column(sig_idx);
        assert_eq!(*sig_col.data_type(), arrow::datatypes::DataType::Utf8);
    }

    #[test]
    fn test_encode_bytes_base58() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Base58, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["transactions"].num_rows(), 1);
        let sig_idx = batches["transactions"]
            .schema()
            .index_of("signature")
            .unwrap();
        let sig_col = batches["transactions"].column(sig_idx);
        assert_eq!(*sig_col.data_type(), arrow::datatypes::DataType::Utf8);
    }

    #[test]
    fn test_token_balances_content() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();

        let tb = &batches["token_balances"];
        assert_eq!(tb.num_rows(), 2);
        // Check balance_type column: first row = "pre", second = "post"
        let bt_idx = tb.schema().index_of("balance_type").unwrap();
        let bt_col = tb
            .column(bt_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(bt_col.value(0), "pre");
        assert_eq!(bt_col.value(1), "post");
        // Check mint column
        let mint_idx = tb.schema().index_of("mint").unwrap();
        let mint_col = tb
            .column(mint_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            mint_col.value(0),
            "So11111111111111111111111111111111111111112"
        );
        // Check ui_amount
        let ua_idx = tb.schema().index_of("ui_amount").unwrap();
        let ua_col = tb
            .column(ua_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((ua_col.value(0) - 1.5).abs() < f64::EPSILON);
        assert!((ua_col.value(1) - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_account_lookups_content() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();

        let al = &batches["account_lookups"];
        assert_eq!(al.num_rows(), 1);
        let li_idx = al.schema().index_of("lookup_index").unwrap();
        let li_col = al
            .column(li_idx)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(li_col.value(0), 0);
    }

    #[test]
    fn test_instruction_positions_preserve_legacy_indices_and_expose_call_order() {
        let mut block = make_test_block(100);
        let transaction = &mut block.transactions[0];
        let message = transaction
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap();
        let top = message.instructions[0].clone();
        message.instructions.extend([top.clone(), top]);
        let meta = transaction.meta.as_mut().unwrap();
        let inner = meta.inner_instructions[0].instructions[0].clone();
        let mut nested = inner.clone();
        nested.stack_height = Some(3);
        let mut unknown_depth = inner.clone();
        unknown_depth.stack_height = None;
        // Deliberately reverse the groups: the parent key, not vector order,
        // determines which top-level instruction owns each ordered inner set.
        meta.inner_instructions = vec![
            solana::InnerInstructions {
                index: 2,
                instructions: vec![unknown_depth],
            },
            solana::InnerInstructions {
                index: 0,
                instructions: vec![inner, nested],
            },
        ];
        for encoding in [EncodeBytes::Binary, EncodeBytes::Hex, EncodeBytes::Base58] {
            for include_fork_step in [false, true] {
                let mut mapper = SolanaBlockMapper::new(
                    false,
                    include_fork_step,
                    encoding.clone(),
                    false,
                    false,
                );
                mapper
                    .map_block(
                        &block.encode_to_vec(),
                        &BlockIdentity::default(),
                        Some("NEW"),
                    )
                    .unwrap();
                let batch = mapper.flush().unwrap().remove("instructions").unwrap();
                let column = |name: &str| {
                    batch
                        .column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap()
                };
                let values = |name: &str| column(name).iter().collect::<Vec<_>>();
                assert_eq!(
                    values("instruction_index"),
                    (0..6).map(Some).collect::<Vec<_>>()
                );
                let parents = vec![None, None, None, Some(2), Some(0), Some(0)];
                assert_eq!(values("inner_index"), parents);
                assert_eq!(values("parent_instruction_index"), parents);
                assert_eq!(
                    values("inner_instruction_index"),
                    vec![None, None, None, Some(0), Some(0), Some(1)]
                );
                assert_eq!(
                    values("stack_height"),
                    vec![None, None, None, None, Some(2), Some(3)]
                );
                for name in ["parent_instruction_index", "inner_instruction_index"] {
                    let schema = batch.schema();
                    let field = schema.field_with_name(name).unwrap();
                    assert_eq!(field.data_type(), &arrow::datatypes::DataType::UInt32);
                    assert!(field.is_nullable());
                }
                let parent = column("parent_instruction_index");
                let inner = column("inner_instruction_index");
                let legacy = column("instruction_index");
                let mut ordered = (0..batch.num_rows()).collect::<Vec<_>>();
                ordered.sort_by_key(|&row| {
                    (
                        if parent.is_null(row) {
                            legacy.value(row)
                        } else {
                            parent.value(row)
                        },
                        !parent.is_null(row),
                        if inner.is_null(row) {
                            0
                        } else {
                            inner.value(row)
                        },
                    )
                });
                assert_eq!(ordered, vec![0, 4, 5, 1, 2, 3]);
                // Both new builders reset with the other instruction columns.
                mapper
                    .map_block(
                        &block.encode_to_vec(),
                        &BlockIdentity::default(),
                        Some("NEW"),
                    )
                    .unwrap();
                assert_eq!(mapper.flush().unwrap()["instructions"], batch);
            }
        }
    }

    #[test]
    fn test_reward_indexes_are_stable_across_flushes_and_restarts() {
        use arrow::compute::concat_batches;

        fn block_with_rewards(slot: u64) -> solana::Block {
            let mut block = make_test_block(slot);
            let mut second_tx = block.transactions[0].clone();
            second_tx
                .meta
                .as_mut()
                .unwrap()
                .rewards
                .push(solana::Reward {
                    pubkey: "SecondTransactionReward".to_string(),
                    lamports: -2,
                    ..Default::default()
                });
            let mut failed_tx = block.transactions[0].clone();
            failed_tx.meta.as_mut().unwrap().err = Some(solana::TransactionError {
                err: vec![1].into(),
            });
            block.transactions.extend([second_tx, failed_tx]);
            block.rewards.push(solana::Reward {
                pubkey: "SecondBlockReward".to_string(),
                lamports: 3,
                ..Default::default()
            });
            block
        }

        // Repeated envelopes deliberately include NEW(A), UNDO(A), NEW(A).
        // Numbering belongs to each envelope, never to prior buffered rows.
        let events = [(100, "NEW"), (101, "NEW"), (101, "UNDO"), (101, "NEW")];
        for include_failed in [false, true] {
            let mut results = Vec::new();
            for (flush_every, restart_after_flush) in
                [(4, false), (2, false), (1, false), (1, true)]
            {
                let new_mapper = || {
                    SolanaBlockMapper::new(false, true, EncodeBytes::Binary, false, include_failed)
                };
                let mut mapper = new_mapper();
                let mut batches = Vec::new();
                for (event_index, (slot, step)) in events.iter().enumerate() {
                    let block = block_with_rewards(*slot);
                    let identity = BlockIdentity {
                        block_num: *slot,
                        ..Default::default()
                    };
                    mapper
                        .map_block(&block.encode_to_vec(), &identity, Some(step))
                        .unwrap();
                    if (event_index + 1) % flush_every == 0 {
                        batches.push(mapper.flush().unwrap().remove("rewards").unwrap());
                        if restart_after_flush {
                            mapper = new_mapper();
                        }
                    }
                }
                results.push(concat_batches(&batches[0].schema(), &batches).unwrap());
            }
            for result in &results[1..] {
                assert_eq!(result, &results[0], "flush/restart changed reward output");
            }
            let result = &results[0];
            let indices = result
                .column_by_name("reward_index")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let rewards_per_event = if include_failed { 6 } else { 5 };
            assert_eq!(result.num_rows(), events.len() * rewards_per_event);
            for event in 0..events.len() {
                for index in 0..rewards_per_event {
                    let row = event * rewards_per_event + index;
                    assert_eq!(indices.value(row), index as u32);
                    assert_eq!(get_string_value(result, "fork_step", row), events[event].1);
                    assert_eq!(
                        get_string_value(result, "source", row),
                        if index < rewards_per_event - 2 {
                            "transaction"
                        } else {
                            "block"
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn test_rewards_source_column() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();

        let rw = &batches["rewards"];
        assert_eq!(rw.num_rows(), 2);
        let src_idx = rw.schema().index_of("source").unwrap();
        let src_col = rw
            .column(src_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // per-tx rewards come first (from map_transaction), then block rewards (from map_reward)
        assert_eq!(src_col.value(0), "transaction");
        assert_eq!(src_col.value(1), "block");
        // transaction_index: 0 for per-tx reward, null for block reward
        let ti_idx = rw.schema().index_of("transaction_index").unwrap();
        let ti_col = rw
            .column(ti_idx)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(ti_col.value(0), 0);
        assert!(ti_col.is_null(1));
    }

    #[test]
    fn test_return_data_and_cost_units() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();

        let txs = &batches["transactions"];
        // cost_units should be present
        let cu_idx = txs.schema().index_of("cost_units").unwrap();
        let cu_col = txs
            .column(cu_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(cu_col.value(0), 5678);
        // return_data should be non-null
        let rd_idx = txs.schema().index_of("return_data").unwrap();
        let rd_col = txs.column(rd_idx);
        assert!(!rd_col.is_null(0));
    }

    #[test]
    fn test_loaded_addresses() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();

        let msgs = &batches["messages"];
        assert_eq!(msgs.num_rows(), 1);
        // loaded_writable_addresses should not be null (has 1 address)
        let lw_idx = msgs.schema().index_of("loaded_writable_addresses").unwrap();
        let lw_col = msgs.column(lw_idx);
        assert!(!lw_col.is_null(0));
        // loaded_readonly_addresses should not be null (has 2 addresses)
        let lr_idx = msgs.schema().index_of("loaded_readonly_addresses").unwrap();
        let lr_col = msgs.column(lr_idx);
        assert!(!lr_col.is_null(0));
    }

    #[test]
    fn test_table_names_base() {
        let mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        let names = mapper.table_names();
        assert_eq!(names.len(), 7);
        assert!(names.contains(&"token_balances"));
        assert!(names.contains(&"account_lookups"));
        assert!(!names.contains(&"vote_transactions"));
    }

    #[test]
    fn test_table_names_with_votes() {
        let mapper = SolanaBlockMapper::new(true, false, EncodeBytes::Binary, false, false);
        let names = mapper.table_names();
        assert_eq!(names.len(), 8);
        assert!(names.contains(&"token_balances"));
        assert!(names.contains(&"account_lookups"));
        assert!(names.contains(&"vote_transactions"));
    }

    #[test]
    fn test_base_excludes_vote_transactions() {
        let mut block = make_test_block(100);
        // Add a vote transaction
        block.transactions.push(solana::ConfirmedTransaction {
            transaction: Some(solana::Transaction {
                signatures: vec![vec![99u8; 64].into()],
                message: Some(solana::Message {
                    header: Some(solana::MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 1,
                    }),
                    account_keys: vec![vec![2u8; 32].into(), VOTE_PROGRAM_ID.to_vec().into()],
                    recent_blockhash: vec![4u8; 32].into(),
                    instructions: vec![solana::CompiledInstruction {
                        program_id_index: 1,
                        accounts: vec![0].into(),
                        data: vote_payloads().remove(0).into(),
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
                inner_instructions: vec![],
                log_messages: vec!["Program Vote111 invoke".into()],
                pre_token_balances: vec![],
                post_token_balances: vec![],
                rewards: vec![],
                loaded_writable_addresses: vec![],
                loaded_readonly_addresses: vec![],
                return_data: None,
                compute_units_consumed: Some(2100),
                cost_units: None,
            }),
        });

        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = SolanaBlockMapper::new(false, false, EncodeBytes::Binary, false, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        // Non-vote transaction included
        assert_eq!(batches["transactions"].num_rows(), 1);
        // vote_transactions table not present in base mode
        assert!(!batches.contains_key("vote_transactions"));
    }
}
