use super::proto::tron;
use super::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, EncodeBytes};
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

// ---------------------------------------------------------------------------
// Tron BlockMapper
// ---------------------------------------------------------------------------

pub struct TronBlockMapper {
    include_fork_step: bool,
    encoding: EncodeBytes,
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
    pub fn new(include_fork_step: bool, encoding: EncodeBytes) -> Self {
        let enc = &encoding;
        Self {
            include_fork_step,
            blocks: BlocksBuilder::new(include_fork_step, enc),
            transactions: TransactionsBuilder::new(include_fork_step, enc),
            logs: LogsBuilder::new(include_fork_step, enc),
            internal_transactions: InternalTransactionsBuilder::new(include_fork_step, enc),
            blocks_schema: schema::blocks_schema(include_fork_step, enc),
            transactions_schema: schema::transactions_schema(include_fork_step, enc),
            logs_schema: schema::logs_schema(include_fork_step, enc),
            internal_transactions_schema: schema::internal_transactions_schema(include_fork_step, enc),
            encoding,
        }
    }

    fn map_tron_block(&mut self, block: &tron::Block, identity: &BlockIdentity, fork_step: Option<&str>) {
        let header = block.header.as_ref();
        let block_number = header.map_or(0, |h| h.number);

        self.blocks.canonical.append(identity);
        self.blocks.number.append_value(block_number);
        self.blocks.hash.append_value(&block.id);
        self.blocks.parent_hash.append_value(header.map(|h| h.parent_hash.as_slice()).unwrap_or(&[]));
        self.blocks.timestamp.append_value(header.map_or(0, |h| h.timestamp));
        self.blocks.witness_address.append_value(header.map(|h| h.witness_address.as_slice()).unwrap_or(&[]));
        self.blocks.version.append_value(header.map_or(0, |h| h.version));
        self.blocks.tx_trie_root.append_value(header.map(|h| h.tx_trie_root.as_slice()).unwrap_or(&[]));
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
        let info = tx.info.as_ref();
        let fee = info.map_or(0, |i| i.fee);
        let contract_type = tx.contracts.first().map_or(0, |c| c.r#type);

        self.transactions.canonical.append(identity);
        self.transactions.block_number.append_value(block_number);
        self.transactions.txid.append_value(&tx.txid);
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
                self.logs.tx_hash.append_value(&tx.txid);
                self.logs.log_index.append_value(log_index as u32);
                self.logs.address.append_value(&log.address);

                let topics = &log.topics;
                if let Some(t) = topics.first() { self.logs.topic0.append_value(t); } else { self.logs.topic0.append_null(); }
                if let Some(t) = topics.get(1) { self.logs.topic1.append_value(t); } else { self.logs.topic1.append_null(); }
                if let Some(t) = topics.get(2) { self.logs.topic2.append_value(t); } else { self.logs.topic2.append_null(); }
                if let Some(t) = topics.get(3) { self.logs.topic3.append_value(t); } else { self.logs.topic3.append_null(); }
                self.logs.data.append_value(&log.data);
                append_fork_step(&mut self.logs.fork_step, fork_step);
            }

            // Map internal transactions from TransactionInfo
            for (internal_index, itx) in info.internal_transactions.iter().enumerate() {
                self.internal_transactions.canonical.append(identity);
                self.internal_transactions.block_number.append_value(block_number);
                self.internal_transactions.tx_hash.append_value(&tx.txid);
                self.internal_transactions.internal_index.append_value(internal_index as u32);
                self.internal_transactions.hash.append_value(&itx.hash);
                self.internal_transactions.caller_address.append_value(&itx.caller_address);
                self.internal_transactions.transfer_to_address.append_value(&itx.transfer_to_address);
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
    hash: BytesColumn,
    parent_hash: BytesColumn,
    timestamp: Int64Builder,
    witness_address: BytesColumn,
    version: UInt32Builder,
    tx_trie_root: BytesColumn,
    parent_number: UInt64Builder,
    num_transactions: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            number: UInt64Builder::new(),
            hash: BytesColumn::new(encoding),
            parent_hash: BytesColumn::new(encoding),
            timestamp: Int64Builder::new(),
            witness_address: BytesColumn::new(encoding),
            version: UInt32Builder::new(),
            tx_trie_root: BytesColumn::new(encoding),
            parent_number: UInt64Builder::new(),
            num_transactions: UInt32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.number.finish()) as Arc<dyn Array>,
            self.hash.finish(),
            self.parent_hash.finish(),
            Arc::new(self.timestamp.finish()) as Arc<dyn Array>,
            self.witness_address.finish(),
            Arc::new(self.version.finish()) as Arc<dyn Array>,
            self.tx_trie_root.finish(),
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
    txid: BytesColumn,
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
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            txid: BytesColumn::new(encoding),
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
            self.txid.finish(),
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
    tx_hash: BytesColumn,
    log_index: UInt32Builder,
    address: BytesColumn,
    topic0: BytesColumn,
    topic1: BytesColumn,
    topic2: BytesColumn,
    topic3: BytesColumn,
    data: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl LogsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            log_index: UInt32Builder::new(),
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
            Arc::new(self.block_number.finish()) as Arc<dyn Array>,
            self.tx_hash.finish(),
            Arc::new(self.log_index.finish()) as Arc<dyn Array>,
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

struct InternalTransactionsBuilder {
    canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    internal_index: UInt32Builder,
    hash: BytesColumn,
    caller_address: BytesColumn,
    transfer_to_address: BytesColumn,
    note: StringBuilder,
    rejected: BooleanBuilder,
    fork_step: Option<StringBuilder>,
}

impl InternalTransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            internal_index: UInt32Builder::new(),
            hash: BytesColumn::new(encoding),
            caller_address: BytesColumn::new(encoding),
            transfer_to_address: BytesColumn::new(encoding),
            note: StringBuilder::new(),
            rejected: BooleanBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn Array>,
            self.tx_hash.finish(),
            Arc::new(self.internal_index.finish()) as Arc<dyn Array>,
            self.hash.finish(),
            self.caller_address.finish(),
            self.transfer_to_address.finish(),
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
    use super::super::proto::{protocol, tron};

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
        let mut mapper = TronBlockMapper::new(false, EncodeBytes::Hex);
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
        let mut mapper = TronBlockMapper::new(false, EncodeBytes::Hex);
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
        let mut mapper = TronBlockMapper::new(false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_table_names() {
        let mapper = TronBlockMapper::new(false, EncodeBytes::Hex);
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
        let mut mapper = TronBlockMapper::new(true, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), Some("FINAL")).unwrap();

        let batches = mapper.flush().unwrap();
        let blocks_batch = &batches["blocks"];
        let last_col = blocks_batch.num_columns() - 1;
        assert_eq!(blocks_batch.schema().field(last_col).name(), "fork_step");
        let fork_col = blocks_batch.column(last_col).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(fork_col.value(0), "FINAL");
    }
}
