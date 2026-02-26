use super::proto::{cosmos, cosmos_tx};
use super::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, EncodeBytes};
use firehose_parquet::traits::{
    est_bin, est_i32, est_i64, est_opt_str, est_str, est_u32,
    BlockIdentity, BlockMapper, CanonicalBuilder,
};
use prost::Message;
use sha2::{Digest, Sha256};
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

/// Compute SHA256 hash of raw tx bytes, returning raw digest bytes.
fn tx_hash_bytes(raw: &[u8]) -> Vec<u8> {
    Sha256::digest(raw).to_vec()
}

// ---------------------------------------------------------------------------
// Cosmos BlockMapper
// ---------------------------------------------------------------------------

pub struct CosmosBlockMapper {
    blocks: BlocksBuilder,
    transactions: TransactionsBuilder,
    events: EventsBuilder,
    messages: MessagesBuilder,
    blocks_schema: Schema,
    transactions_schema: Schema,
    events_schema: Schema,
    messages_schema: Schema,
}

impl CosmosBlockMapper {
    pub fn new(include_fork_step: bool, encoding: EncodeBytes) -> Self {
        let enc = &encoding;
        Self {
            blocks: BlocksBuilder::new(include_fork_step, enc),
            transactions: TransactionsBuilder::new(include_fork_step, enc),
            events: EventsBuilder::new(include_fork_step, enc),
            messages: MessagesBuilder::new(include_fork_step, enc),
            blocks_schema: schema::blocks_schema(include_fork_step, enc),
            transactions_schema: schema::transactions_schema(include_fork_step, enc),
            events_schema: schema::events_schema(include_fork_step, enc),
            messages_schema: schema::messages_schema(include_fork_step, enc),
        }
    }

    fn map_cosmos_block(&mut self, block: &cosmos::Block, identity: &BlockIdentity, fork_step: Option<&str>) {
        let height = block.height;

        let header = block.header.as_ref();
        let chain_id = header.map_or("", |h| &h.chain_id);
        let proposer_address = header
            .map(|h| h.proposer_address.as_slice())
            .unwrap_or(&[]);
        let last_block_id_hash = header
            .and_then(|h| h.last_block_id.as_ref())
            .map(|bid| bid.hash.as_slice())
            .unwrap_or(&[]);
        let validators_hash = header
            .map(|h| h.validators_hash.as_slice())
            .unwrap_or(&[]);
        let next_validators_hash = header
            .map(|h| h.next_validators_hash.as_slice())
            .unwrap_or(&[]);
        let block_time = block
            .time
            .as_ref()
            .map(|t| t.seconds)
            .unwrap_or(0);
        let num_txs = block.txs.len() as u32;

        // blocks row
        self.blocks.canonical.append(identity);
        self.blocks.height.append_value(height);
        self.blocks.hash.append_value(&block.hash);
        self.blocks.time.append_value(block_time);
        self.blocks.chain_id.append_value(chain_id);
        self.blocks.proposer_address.append_value(proposer_address);
        self.blocks.last_block_id_hash.append_value(last_block_id_hash);
        self.blocks.validators_hash.append_value(validators_hash);
        self.blocks.next_validators_hash.append_value(next_validators_hash);
        self.blocks.num_txs.append_value(num_txs);
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // block-level events (begin_block / end_block)
        for (event_index, event) in block.events.iter().enumerate() {
            for attr in &event.attributes {
                self.events.canonical.append(identity);
                self.events.source.append_value("block");
                self.events.tx_hash.append_value(&[]);
                self.events.tx_index.append_null();
                self.events.event_index.append_value(event_index as u32);
                self.events.r#type.append_value(&event.r#type);
                self.events.key.append_value(&attr.key);
                self.events.value.append_value(&attr.value);
                append_fork_step(&mut self.events.fork_step, fork_step);
            }
        }

        // transactions, tx events, and messages
        for (tx_idx, raw_tx) in block.txs.iter().enumerate() {
            let hash = tx_hash_bytes(raw_tx);
            let tx_result = block.tx_results.get(tx_idx);

            // transaction row
            self.transactions.canonical.append(identity);
            self.transactions.tx_hash.append_value(&hash);
            self.transactions.index.append_value(tx_idx as u32);
            self.transactions.code.append_value(tx_result.map_or(0, |r| r.code));
            self.transactions.gas_wanted.append_value(tx_result.map_or(0, |r| r.gas_wanted));
            self.transactions.gas_used.append_value(tx_result.map_or(0, |r| r.gas_used));
            self.transactions.log.append_value(tx_result.map_or("", |r| &r.log));
            self.transactions.info.append_value(tx_result.map_or("", |r| &r.info));
            self.transactions.codespace.append_value(tx_result.map_or("", |r| &r.codespace));
            append_fork_step(&mut self.transactions.fork_step, fork_step);

            // tx-level events
            if let Some(result) = tx_result {
                for (event_index, event) in result.events.iter().enumerate() {
                    for attr in &event.attributes {
                        self.events.canonical.append(identity);
                        self.events.source.append_value("transaction");
                        self.events.tx_hash.append_value(&hash);
                        self.events.tx_index.append_value(tx_idx as i32);
                        self.events.event_index.append_value(event_index as u32);
                        self.events.r#type.append_value(&event.r#type);
                        self.events.key.append_value(&attr.key);
                        self.events.value.append_value(&attr.value);
                        append_fork_step(&mut self.events.fork_step, fork_step);
                    }
                }
            }

            // decode messages from raw tx bytes
            if let Ok(tx) = cosmos_tx::Tx::decode(raw_tx.as_ref()) {
                if let Some(body) = tx.body {
                    for (msg_idx, msg) in body.messages.iter().enumerate() {
                        self.messages.canonical.append(identity);
                        self.messages.tx_hash.append_value(&hash);
                        self.messages.tx_index.append_value(tx_idx as u32);
                        self.messages.message_index.append_value(msg_idx as u32);
                        self.messages.type_url.append_value(&msg.type_url);
                        self.messages.value.append_value(&msg.value);
                        append_fork_step(&mut self.messages.fork_step, fork_step);
                    }
                }
            }
        }
    }
}

impl BlockMapper for CosmosBlockMapper {
    fn map_block(&mut self, block_bytes: &[u8], identity: &BlockIdentity, fork_step: Option<&str>) -> anyhow::Result<()> {
        let block = cosmos::Block::decode(block_bytes)?;
        self.map_cosmos_block(&block, identity, fork_step);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        result.insert("blocks".to_string(), self.blocks.finish(&self.blocks_schema)?);
        result.insert("transactions".to_string(), self.transactions.finish(&self.transactions_schema)?);
        result.insert("events".to_string(), self.events.finish(&self.events_schema)?);
        result.insert("messages".to_string(), self.messages.finish(&self.messages_schema)?);
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        self.blocks.canonical.len()
            .max(self.transactions.canonical.len())
            .max(self.events.canonical.len())
            .max(self.messages.canonical.len())
    }

    fn estimated_bytes(&mut self) -> usize {
        // blocks
        self.blocks.canonical.estimated_bytes()
            + est_i64(&self.blocks.height)
            + self.blocks.hash.estimated_bytes()
            + est_i64(&self.blocks.time)
            + est_str(&self.blocks.chain_id)
            + self.blocks.proposer_address.estimated_bytes()
            + self.blocks.last_block_id_hash.estimated_bytes()
            + self.blocks.validators_hash.estimated_bytes()
            + self.blocks.next_validators_hash.estimated_bytes()
            + est_u32(&self.blocks.num_txs)
            + est_opt_str(&self.blocks.fork_step)
        // transactions
            + self.transactions.canonical.estimated_bytes()
            + self.transactions.tx_hash.estimated_bytes()
            + est_u32(&self.transactions.index)
            + est_u32(&self.transactions.code)
            + est_i64(&self.transactions.gas_wanted)
            + est_i64(&self.transactions.gas_used)
            + est_str(&self.transactions.log)
            + est_str(&self.transactions.info)
            + est_str(&self.transactions.codespace)
            + est_opt_str(&self.transactions.fork_step)
        // events
            + self.events.canonical.estimated_bytes()
            + est_str(&self.events.source)
            + self.events.tx_hash.estimated_bytes()
            + est_i32(&self.events.tx_index)
            + est_u32(&self.events.event_index)
            + est_str(&self.events.r#type)
            + est_str(&self.events.key)
            + est_str(&self.events.value)
            + est_opt_str(&self.events.fork_step)
        // messages
            + self.messages.canonical.estimated_bytes()
            + self.messages.tx_hash.estimated_bytes()
            + est_u32(&self.messages.tx_index)
            + est_u32(&self.messages.message_index)
            + est_str(&self.messages.type_url)
            + est_bin(&self.messages.value)
            + est_opt_str(&self.messages.fork_step)
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
    height: Int64Builder,
    hash: BytesColumn,
    time: Int64Builder,
    chain_id: StringBuilder,
    proposer_address: BytesColumn,
    last_block_id_hash: BytesColumn,
    validators_hash: BytesColumn,
    next_validators_hash: BytesColumn,
    num_txs: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            height: Int64Builder::new(),
            hash: BytesColumn::new(encoding),
            time: Int64Builder::new(),
            chain_id: StringBuilder::new(),
            proposer_address: BytesColumn::new(encoding),
            last_block_id_hash: BytesColumn::new(encoding),
            validators_hash: BytesColumn::new(encoding),
            next_validators_hash: BytesColumn::new(encoding),
            num_txs: UInt32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.height.finish()) as Arc<dyn Array>,
            self.hash.finish(),
            Arc::new(self.time.finish()) as Arc<dyn Array>,
            Arc::new(self.chain_id.finish()) as Arc<dyn Array>,
            self.proposer_address.finish(),
            self.last_block_id_hash.finish(),
            self.validators_hash.finish(),
            self.next_validators_hash.finish(),
            Arc::new(self.num_txs.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct TransactionsBuilder {
    canonical: CanonicalBuilder,
    tx_hash: BytesColumn,
    index: UInt32Builder,
    code: UInt32Builder,
    gas_wanted: Int64Builder,
    gas_used: Int64Builder,
    log: StringBuilder,
    info: StringBuilder,
    codespace: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            tx_hash: BytesColumn::new(encoding),
            index: UInt32Builder::new(),
            code: UInt32Builder::new(),
            gas_wanted: Int64Builder::new(),
            gas_used: Int64Builder::new(),
            log: StringBuilder::new(),
            info: StringBuilder::new(),
            codespace: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            self.tx_hash.finish(),
            Arc::new(self.index.finish()) as Arc<dyn Array>,
            Arc::new(self.code.finish()) as Arc<dyn Array>,
            Arc::new(self.gas_wanted.finish()) as Arc<dyn Array>,
            Arc::new(self.gas_used.finish()) as Arc<dyn Array>,
            Arc::new(self.log.finish()) as Arc<dyn Array>,
            Arc::new(self.info.finish()) as Arc<dyn Array>,
            Arc::new(self.codespace.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EventsBuilder {
    canonical: CanonicalBuilder,
    source: StringBuilder,
    tx_hash: BytesColumn,
    tx_index: Int32Builder,
    event_index: UInt32Builder,
    r#type: StringBuilder,
    key: StringBuilder,
    value: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl EventsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            source: StringBuilder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: Int32Builder::new(),
            event_index: UInt32Builder::new(),
            r#type: StringBuilder::new(),
            key: StringBuilder::new(),
            value: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.source.finish()) as Arc<dyn Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()) as Arc<dyn Array>,
            Arc::new(self.event_index.finish()) as Arc<dyn Array>,
            Arc::new(self.r#type.finish()) as Arc<dyn Array>,
            Arc::new(self.key.finish()) as Arc<dyn Array>,
            Arc::new(self.value.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct MessagesBuilder {
    canonical: CanonicalBuilder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    message_index: UInt32Builder,
    type_url: StringBuilder,
    value: BinaryBuilder,
    fork_step: Option<StringBuilder>,
}

impl MessagesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            message_index: UInt32Builder::new(),
            type_url: StringBuilder::new(),
            value: BinaryBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()) as Arc<dyn Array>,
            Arc::new(self.message_index.finish()) as Arc<dyn Array>,
            Arc::new(self.type_url.finish()) as Arc<dyn Array>,
            Arc::new(self.value.finish()) as Arc<dyn Array>,
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

    fn make_raw_tx(type_url: &str, value: &[u8]) -> Vec<u8> {
        let tx = cosmos_tx::Tx {
            body: Some(cosmos_tx::TxBody {
                messages: vec![prost_types::Any {
                    type_url: type_url.to_string(),
                    value: value.to_vec(),
                }],
                memo: String::new(),
                timeout_height: 0,
            }),
        };
        prost::Message::encode_to_vec(&tx)
    }

    fn make_test_block(height: i64) -> cosmos::Block {
        let raw_tx = make_raw_tx("/cosmos.bank.v1beta1.MsgSend", b"\x01\x02\x03");
        cosmos::Block {
            hash: vec![0xab, 0xcd, 0xef],
            height,
            time: Some(prost_types::Timestamp { seconds: 1700000000, nanos: 0 }),
            header: Some(cosmos::Header {
                version: None,
                chain_id: "cosmoshub-4".to_string(),
                height,
                time: Some(prost_types::Timestamp { seconds: 1700000000, nanos: 0 }),
                last_block_id: Some(cosmos::BlockId {
                    hash: vec![0x11, 0x22],
                    part_set_header: None,
                }),
                last_commit_hash: vec![],
                data_hash: vec![],
                validators_hash: vec![0xaa, 0xbb],
                next_validators_hash: vec![0xcc, 0xdd],
                consensus_hash: vec![],
                app_hash: vec![],
                last_results_hash: vec![],
                evidence_hash: vec![],
                proposer_address: vec![0xde, 0xad],
            }),
            misbehavior: vec![],
            events: vec![cosmos::Event {
                r#type: "coin_received".to_string(),
                attributes: vec![
                    cosmos::EventAttribute { key: "receiver".to_string(), value: "cosmos1abc".to_string() },
                    cosmos::EventAttribute { key: "amount".to_string(), value: "100uatom".to_string() },
                ],
            }],
            txs: vec![raw_tx],
            tx_results: vec![cosmos::TxResults {
                code: 0,
                data: vec![],
                log: "success".to_string(),
                info: String::new(),
                gas_wanted: 200000,
                gas_used: 150000,
                events: vec![cosmos::Event {
                    r#type: "transfer".to_string(),
                    attributes: vec![
                        cosmos::EventAttribute { key: "sender".to_string(), value: "cosmos1xyz".to_string() },
                    ],
                }],
                codespace: String::new(),
            }],
            validator_updates: vec![],
            consensus_param_updates: None,
        }
    }

    #[test]
    fn test_map_and_flush() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = CosmosBlockMapper::new(false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        // 2 block event attrs + 1 tx event attr = 3
        assert_eq!(batches["events"].num_rows(), 3);
        assert_eq!(batches["messages"].num_rows(), 1);
    }

    #[test]
    fn test_empty_block() {
        let block = cosmos::Block {
            hash: vec![0x00],
            height: 1,
            time: Some(prost_types::Timestamp { seconds: 1700000000, nanos: 0 }),
            header: Some(cosmos::Header {
                chain_id: "cosmoshub-4".to_string(),
                height: 1,
                ..Default::default()
            }),
            events: vec![],
            txs: vec![],
            tx_results: vec![],
            misbehavior: vec![],
            validator_updates: vec![],
            consensus_param_updates: None,
        };
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = CosmosBlockMapper::new(false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 0);
        assert_eq!(batches["events"].num_rows(), 0);
        assert_eq!(batches["messages"].num_rows(), 0);
    }

    #[test]
    fn test_flush_resets() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = CosmosBlockMapper::new(false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_table_names() {
        let mapper = CosmosBlockMapper::new(false, EncodeBytes::Hex);
        assert_eq!(mapper.table_names().len(), 4);
        assert!(mapper.table_names().contains(&"blocks"));
        assert!(mapper.table_names().contains(&"transactions"));
        assert!(mapper.table_names().contains(&"events"));
        assert!(mapper.table_names().contains(&"messages"));
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = CosmosBlockMapper::new(true, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), Some("FINAL")).unwrap();

        let batches = mapper.flush().unwrap();
        let blocks_batch = &batches["blocks"];
        let last_col = blocks_batch.num_columns() - 1;
        assert_eq!(blocks_batch.schema().field(last_col).name(), "fork_step");
        let fork_col = blocks_batch.column(last_col).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(fork_col.value(0), "FINAL");
    }
}
