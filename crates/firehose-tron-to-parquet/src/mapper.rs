use crate::proto::tron;
use crate::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
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

fn mk_fork_step(include: bool) -> Option<StringBuilder> {
    if include { Some(StringBuilder::new()) } else { None }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex_encode(bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Tron BlockMapper
// ---------------------------------------------------------------------------

pub struct TronBlockMapper {
    include_fork_step: bool,
    blocks: BlocksBuilder,
    transactions: TransactionsBuilder,
    logs: LogsBuilder,
    internal_transactions: InternalTransactionsBuilder,
    blocks_schema: Schema,
    transactions_schema: Schema,
    logs_schema: Schema,
    internal_transactions_schema: Schema,
}

impl TronBlockMapper {
    pub fn new(include_fork_step: bool) -> Self {
        Self {
            include_fork_step,
            blocks: BlocksBuilder::new(include_fork_step),
            transactions: TransactionsBuilder::new(include_fork_step),
            logs: LogsBuilder::new(include_fork_step),
            internal_transactions: InternalTransactionsBuilder::new(include_fork_step),
            blocks_schema: schema::blocks_schema(include_fork_step),
            transactions_schema: schema::transactions_schema(include_fork_step),
            logs_schema: schema::logs_schema(include_fork_step),
            internal_transactions_schema: schema::internal_transactions_schema(include_fork_step),
        }
    }

    fn map_tron_block(&mut self, block: &tron::Block, identity: &BlockIdentity, fork_step: Option<&str>) {
        let header = block.header.as_ref();
        let block_number = header.map_or(0, |h| h.number);
        let block_hash = bytes_to_hex(&block.id);

        self.blocks.canonical.append(identity);
        self.blocks.number.append_value(block_number);
        self.blocks.hash.append_value(&block_hash);
        self.blocks.parent_hash.append_value(header.map_or_else(String::new, |h| bytes_to_hex(&h.parent_hash)));
        self.blocks.timestamp.append_value(header.map_or(0, |h| h.timestamp));
        self.blocks.witness_address.append_value(header.map_or_else(String::new, |h| bytes_to_hex(&h.witness_address)));
        self.blocks.version.append_value(header.map_or(0, |h| h.version));
        self.blocks.tx_trie_root.append_value(header.map_or_else(String::new, |h| bytes_to_hex(&h.tx_trie_root)));
        self.blocks.parent_number.append_value(header.map_or(0, |h| h.parent_number));
        self.blocks.num_transactions.append_value(block.transactions.len() as u32);
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        for tx in &block.transactions {
            self.map_transaction(block_number, tx, identity, fork_step);
        }
    }

    fn map_transaction(
        &mut self,
        block_number: u64,
        tx: &tron::Transaction,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        let tx_hash = bytes_to_hex(&tx.txid);
        let info = tx.info.as_ref();
        let fee = info.map_or(0, |i| i.fee);
        let contract_type = tx.contracts.first().map_or(0, |c| c.r#type);

        self.transactions.canonical.append(identity);
        self.transactions.block_number.append_value(block_number);
        self.transactions.txid.append_value(&tx_hash);
        self.transactions.result.append_value(tx.result);
        self.transactions.code.append_value(tx.code);
        self.transactions.energy_used.append_value(tx.energy_used);
        self.transactions.energy_penalty.append_value(tx.energy_penalty);
        self.transactions.fee.append_value(fee);
        self.transactions.contract_type.append_value(contract_type);
        self.transactions.expiration.append_value(tx.expiration);
        self.transactions.timestamp.append_value(tx.timestamp);
        append_fork_step(&mut self.transactions.fork_step, fork_step);

        // Map logs from TransactionInfo
        if let Some(info) = info {
            for (log_index, log) in info.log.iter().enumerate() {
                self.logs.canonical.append(identity);
                self.logs.block_number.append_value(block_number);
                self.logs.tx_hash.append_value(&tx_hash);
                self.logs.log_index.append_value(log_index as u32);
                self.logs.address.append_value(bytes_to_hex(&log.address));

                let topics = &log.topics;
                self.logs.topic0.append_option(topics.first().map(|t| bytes_to_hex(t)));
                self.logs.topic1.append_option(topics.get(1).map(|t| bytes_to_hex(t)));
                self.logs.topic2.append_option(topics.get(2).map(|t| bytes_to_hex(t)));
                self.logs.topic3.append_option(topics.get(3).map(|t| bytes_to_hex(t)));
                self.logs.data.append_value(bytes_to_hex(&log.data));
                append_fork_step(&mut self.logs.fork_step, fork_step);
            }

            // Map internal transactions from TransactionInfo
            for (internal_index, itx) in info.internal_transactions.iter().enumerate() {
                self.internal_transactions.canonical.append(identity);
                self.internal_transactions.block_number.append_value(block_number);
                self.internal_transactions.tx_hash.append_value(&tx_hash);
                self.internal_transactions.internal_index.append_value(internal_index as u32);
                self.internal_transactions.hash.append_value(bytes_to_hex(&itx.hash));
                self.internal_transactions.caller_address.append_value(bytes_to_hex(&itx.caller_address));
                self.internal_transactions.transfer_to_address.append_value(bytes_to_hex(&itx.transfer_to_address));
                self.internal_transactions.note.append_value(String::from_utf8_lossy(&itx.note).as_ref());
                self.internal_transactions.rejected.append_value(itx.rejected);
                append_fork_step(&mut self.internal_transactions.fork_step, fork_step);
            }
        }
    }
}

impl BlockMapper for TronBlockMapper {
    fn map_block(&mut self, block_bytes: &[u8], identity: &BlockIdentity, fork_step: Option<&str>) -> anyhow::Result<()> {
        let block = tron::Block::decode(block_bytes)?;
        self.map_tron_block(&block, identity, fork_step);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        result.insert("blocks".to_string(), self.blocks.finish(&self.blocks_schema)?);
        result.insert("transactions".to_string(), self.transactions.finish(&self.transactions_schema)?);
        result.insert("logs".to_string(), self.logs.finish(&self.logs_schema)?);
        result.insert("internal_transactions".to_string(), self.internal_transactions.finish(&self.internal_transactions_schema)?);
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        self.blocks.canonical.len()
            .max(self.transactions.canonical.len())
            .max(self.logs.canonical.len())
            .max(self.internal_transactions.canonical.len())
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
    number: UInt64Builder,
    hash: StringBuilder,
    parent_hash: StringBuilder,
    timestamp: Int64Builder,
    witness_address: StringBuilder,
    version: UInt32Builder,
    tx_trie_root: StringBuilder,
    parent_number: UInt64Builder,
    num_transactions: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            number: UInt64Builder::new(),
            hash: StringBuilder::new(),
            parent_hash: StringBuilder::new(),
            timestamp: Int64Builder::new(),
            witness_address: StringBuilder::new(),
            version: UInt32Builder::new(),
            tx_trie_root: StringBuilder::new(),
            parent_number: UInt64Builder::new(),
            num_transactions: UInt32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.number.finish()) as Arc<dyn Array>,
            Arc::new(self.hash.finish()) as Arc<dyn Array>,
            Arc::new(self.parent_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.timestamp.finish()) as Arc<dyn Array>,
            Arc::new(self.witness_address.finish()) as Arc<dyn Array>,
            Arc::new(self.version.finish()) as Arc<dyn Array>,
            Arc::new(self.tx_trie_root.finish()) as Arc<dyn Array>,
            Arc::new(self.parent_number.finish()) as Arc<dyn Array>,
            Arc::new(self.num_transactions.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct TransactionsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    txid: StringBuilder,
    result: BooleanBuilder,
    code: Int32Builder,
    energy_used: Int64Builder,
    energy_penalty: Int64Builder,
    fee: Int64Builder,
    contract_type: Int32Builder,
    expiration: Int64Builder,
    timestamp: Int64Builder,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            txid: StringBuilder::new(),
            result: BooleanBuilder::new(),
            code: Int32Builder::new(),
            energy_used: Int64Builder::new(),
            energy_penalty: Int64Builder::new(),
            fee: Int64Builder::new(),
            contract_type: Int32Builder::new(),
            expiration: Int64Builder::new(),
            timestamp: Int64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn Array>,
            Arc::new(self.txid.finish()) as Arc<dyn Array>,
            Arc::new(self.result.finish()) as Arc<dyn Array>,
            Arc::new(self.code.finish()) as Arc<dyn Array>,
            Arc::new(self.energy_used.finish()) as Arc<dyn Array>,
            Arc::new(self.energy_penalty.finish()) as Arc<dyn Array>,
            Arc::new(self.fee.finish()) as Arc<dyn Array>,
            Arc::new(self.contract_type.finish()) as Arc<dyn Array>,
            Arc::new(self.expiration.finish()) as Arc<dyn Array>,
            Arc::new(self.timestamp.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct LogsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: StringBuilder,
    log_index: UInt32Builder,
    address: StringBuilder,
    topic0: StringBuilder,
    topic1: StringBuilder,
    topic2: StringBuilder,
    topic3: StringBuilder,
    data: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl LogsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: StringBuilder::new(),
            log_index: UInt32Builder::new(),
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
            Arc::new(self.block_number.finish()) as Arc<dyn Array>,
            Arc::new(self.tx_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.log_index.finish()) as Arc<dyn Array>,
            Arc::new(self.address.finish()) as Arc<dyn Array>,
            Arc::new(self.topic0.finish()) as Arc<dyn Array>,
            Arc::new(self.topic1.finish()) as Arc<dyn Array>,
            Arc::new(self.topic2.finish()) as Arc<dyn Array>,
            Arc::new(self.topic3.finish()) as Arc<dyn Array>,
            Arc::new(self.data.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct InternalTransactionsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: StringBuilder,
    internal_index: UInt32Builder,
    hash: StringBuilder,
    caller_address: StringBuilder,
    transfer_to_address: StringBuilder,
    note: StringBuilder,
    rejected: BooleanBuilder,
    fork_step: Option<StringBuilder>,
}

impl InternalTransactionsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: StringBuilder::new(),
            internal_index: UInt32Builder::new(),
            hash: StringBuilder::new(),
            caller_address: StringBuilder::new(),
            transfer_to_address: StringBuilder::new(),
            note: StringBuilder::new(),
            rejected: BooleanBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn Array>,
            Arc::new(self.tx_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.internal_index.finish()) as Arc<dyn Array>,
            Arc::new(self.hash.finish()) as Arc<dyn Array>,
            Arc::new(self.caller_address.finish()) as Arc<dyn Array>,
            Arc::new(self.transfer_to_address.finish()) as Arc<dyn Array>,
            Arc::new(self.note.finish()) as Arc<dyn Array>,
            Arc::new(self.rejected.finish()) as Arc<dyn Array>,
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
    use crate::proto::{protocol, tron};

    fn make_test_block(number: u64) -> tron::Block {
        tron::Block {
            id: vec![0x01, 0x02, 0x03],
            header: Some(tron::BlockHeader {
                number,
                tx_trie_root: vec![0xaa, 0xbb],
                witness_address: vec![0x41, 0x01, 0x02],
                parent_number: number.saturating_sub(1),
                parent_hash: vec![0x00, 0x01, 0x02],
                version: 28,
                timestamp: 1700000000000,
                witness_signature: vec![],
            }),
            transactions: vec![tron::Transaction {
                txid: vec![0xde, 0xad, 0xbe, 0xef],
                signature: vec![],
                ref_block_bytes: vec![],
                ref_block_hash: vec![],
                expiration: 1700000060000,
                timestamp: 1700000000000,
                contract_result: vec![],
                result: true,
                code: 0,
                message: vec![],
                energy_used: 50000,
                energy_penalty: 0,
                info: Some(protocol::TransactionInfo {
                    id: vec![0xde, 0xad, 0xbe, 0xef],
                    fee: 1000,
                    block_number: number as i64,
                    block_time_stamp: 1700000000000,
                    contract_result: vec![],
                    contract_address: vec![],
                    receipt: None,
                    log: vec![protocol::transaction_info::Log {
                        address: vec![0x41, 0x10, 0x20],
                        topics: vec![
                            vec![0xab, 0xcd],
                            vec![0xef, 0x01],
                        ],
                        data: vec![0x01, 0x02, 0x03],
                    }],
                    result: 0,
                    res_message: vec![],
                    asset_issue_id: String::new(),
                    withdraw_amount: 0,
                    unfreeze_amount: 0,
                    internal_transactions: vec![protocol::InternalTransaction {
                        hash: vec![0x11, 0x22],
                        caller_address: vec![0x41, 0xaa],
                        transfer_to_address: vec![0x41, 0xbb],
                        call_value_info: vec![],
                        note: b"call".to_vec(),
                        rejected: false,
                        extra: String::new(),
                    }],
                    exchange_received_amount: 0,
                    exchange_inject_another_amount: 0,
                    exchange_withdraw_another_amount: 0,
                    exchange_id: 0,
                    shielded_transaction_fee: 0,
                    order_id: vec![],
                    order_details: vec![],
                    packing_fee: 0,
                    withdraw_expire_amount: 0,
                    cancel_unfreeze_v2_amount: HashMap::new(),
                }),
                contracts: vec![protocol::transaction::Contract {
                    r#type: 31, // TriggerSmartContract
                    parameter: None,
                    provider: vec![],
                    contract_name: vec![],
                    permission_id: 0,
                }],
            }],
        }
    }

    #[test]
    fn test_map_and_flush() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = TronBlockMapper::new(false);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["logs"].num_rows(), 1);
        assert_eq!(batches["internal_transactions"].num_rows(), 1);
    }

    #[test]
    fn test_empty_block() {
        let block = tron::Block {
            id: vec![0x01],
            header: Some(tron::BlockHeader {
                number: 200,
                tx_trie_root: vec![],
                witness_address: vec![],
                parent_number: 199,
                parent_hash: vec![],
                version: 28,
                timestamp: 1700000000000,
                witness_signature: vec![],
            }),
            transactions: vec![],
        };
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = TronBlockMapper::new(false);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 0);
        assert_eq!(batches["logs"].num_rows(), 0);
        assert_eq!(batches["internal_transactions"].num_rows(), 0);
    }

    #[test]
    fn test_flush_resets() {
        let block = make_test_block(1);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = TronBlockMapper::new(false);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_table_names() {
        let mapper = TronBlockMapper::new(false);
        assert_eq!(mapper.table_names().len(), 4);
        assert!(mapper.table_names().contains(&"blocks"));
        assert!(mapper.table_names().contains(&"transactions"));
        assert!(mapper.table_names().contains(&"logs"));
        assert!(mapper.table_names().contains(&"internal_transactions"));
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_block(0);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = TronBlockMapper::new(true);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), Some("FINAL")).unwrap();

        let batches = mapper.flush().unwrap();
        let blocks_batch = &batches["blocks"];
        let last_col = blocks_batch.num_columns() - 1;
        assert_eq!(blocks_batch.schema().field(last_col).name(), "fork_step");
        let fork_col = blocks_batch.column(last_col).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(fork_col.value(0), "FINAL");
    }
}
