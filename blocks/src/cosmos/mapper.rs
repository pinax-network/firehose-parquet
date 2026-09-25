use super::proto::cosmos;
use super::schema;
use super::tx_metadata::{decode_tx, TxMetadataBuilder};
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, EncodeBytes};
use firehose_parquet::traits::{
    est_bin, est_i64, est_opt_str, est_str, est_u32, BlockIdentity, BlockMapper, CanonicalBuilder,
    PreparedIdentity,
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
    if include {
        Some(StringBuilder::new())
    } else {
        None
    }
}

/// Compute SHA256 hash of raw tx bytes, returning raw digest bytes.
fn tx_hash_bytes(raw: &[u8]) -> Vec<u8> {
    Sha256::digest(raw).to_vec()
}

fn cosmos_parent_hash(block: &cosmos::Block) -> &[u8] {
    block
        .header
        .as_ref()
        .and_then(|header| header.last_block_id.as_ref())
        .map(|block_id| block_id.hash.as_slice())
        .unwrap_or(&[])
}

// ---------------------------------------------------------------------------
// Cosmos BlockMapper
// ---------------------------------------------------------------------------

pub struct CosmosBlockMapper {
    include_failed_transactions: bool,
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
    pub fn new(
        include_fork_step: bool,
        encoding: EncodeBytes,
        include_failed_transactions: bool,
    ) -> Self {
        let enc = &encoding;
        Self {
            include_failed_transactions,
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

    fn map_cosmos_block(
        &mut self,
        block: &cosmos::Block,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let height = block.height;

        let header = block.header.as_ref();
        let chain_id = header.map_or("", |h| &h.chain_id);
        let proposer_address = header.map(|h| h.proposer_address.as_slice()).unwrap_or(&[]);
        let last_block_id_hash = cosmos_parent_hash(block);
        let validators_hash = header.map(|h| h.validators_hash.as_slice()).unwrap_or(&[]);
        let next_validators_hash = header
            .map(|h| h.next_validators_hash.as_slice())
            .unwrap_or(&[]);
        let block_time = block.time.as_ref().map(|t| t.seconds).unwrap_or(0);
        let num_txs = block.txs.len() as u32;

        // blocks row
        self.blocks.canonical.append(identity);
        self.blocks.height.append_value(height);
        self.blocks.hash.append_value(&block.hash);
        self.blocks.time.append_value(block_time);
        self.blocks.chain_id.append_value(chain_id);
        self.blocks.proposer_address.append_value(proposer_address);
        self.blocks
            .last_block_id_hash
            .append_value(last_block_id_hash);
        self.blocks.validators_hash.append_value(validators_hash);
        self.blocks
            .next_validators_hash
            .append_value(next_validators_hash);
        self.blocks.num_txs.append_value(num_txs);
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // Preserve every event and every attribute in source order, including empty events.
        for (event_index, event) in block.events.iter().enumerate() {
            self.events.append_event(
                identity,
                "block",
                None,
                None,
                event_index as u32,
                event,
                fork_step,
            );
        }

        // transactions, tx events, and messages
        let mut decode_failures = 0;
        for (tx_idx, raw_tx) in block.txs.iter().enumerate() {
            let decoded = decode_tx(raw_tx);
            if decoded.is_err() {
                decode_failures += 1;
            }
            let hash = tx_hash_bytes(raw_tx);
            let tx_result = block.tx_results.get(tx_idx);

            // Skip failed transactions (code != 0) unless flag is set
            if !self.include_failed_transactions && tx_result.map_or(false, |r| r.code != 0) {
                continue;
            }

            // transaction row
            self.transactions.canonical.append(identity);
            self.transactions.tx_hash.append_value(&hash);
            self.transactions.index.append_value(tx_idx as u32);
            self.transactions
                .code
                .append_option(tx_result.map(|r| r.code));
            self.transactions
                .gas_wanted
                .append_option(tx_result.map(|r| r.gas_wanted));
            self.transactions
                .gas_used
                .append_option(tx_result.map(|r| r.gas_used));
            self.transactions
                .log
                .append_option(tx_result.map(|r| r.log.as_str()));
            self.transactions
                .info
                .append_option(tx_result.map(|r| r.info.as_str()));
            self.transactions
                .codespace
                .append_option(tx_result.map(|r| r.codespace.as_str()));
            self.transactions
                .metadata
                .append(raw_tx, decoded.as_ref().ok());
            append_fork_step(&mut self.transactions.fork_step, fork_step);

            // Transaction events use the same UInt32 source index as the parent row.
            if let Some(result) = tx_result {
                for (event_index, event) in result.events.iter().enumerate() {
                    self.events.append_event(
                        identity,
                        "transaction",
                        Some(&hash),
                        Some(tx_idx as u32),
                        event_index as u32,
                        event,
                        fork_step,
                    );
                }
            }

            // decode messages from raw tx bytes
            if let Ok(tx) = decoded {
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
        // Count malformed raw transactions across the complete source block,
        // including a malformed failed transaction omitted by the row filter.
        self.blocks.tx_decode_failures.append_value(decode_failures);
    }
}

impl BlockMapper for CosmosBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        let block = cosmos::Block::decode(block_bytes)?;
        let tx_count = block.txs.len() as u64;
        let identity = self.blocks.canonical.prepare_with_ids(
            identity,
            &block.hash,
            cosmos_parent_hash(&block),
        )?;
        self.map_cosmos_block(&block, &identity, fork_step);
        Ok(tx_count)
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
        result.insert(
            "events".to_string(),
            self.events.finish(&self.events_schema)?,
        );
        result.insert(
            "messages".to_string(),
            self.messages.finish(&self.messages_schema)?,
        );
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        self.blocks
            .canonical
            .len()
            .max(self.transactions.canonical.len())
            .max(self.events.canonical.len())
            .max(self.messages.canonical.len())
    }

    fn total_rows(&self) -> usize {
        self.blocks.canonical.len()
            + self.transactions.canonical.len()
            + self.events.canonical.len()
            + self.messages.canonical.len()
    }

    fn table_estimates(&mut self) -> Vec<(&str, usize)> {
        let blocks = self.blocks.canonical.estimated_bytes()
            + est_i64(&self.blocks.height)
            + self.blocks.hash.estimated_bytes()
            + est_i64(&self.blocks.time)
            + est_str(&self.blocks.chain_id)
            + self.blocks.proposer_address.estimated_bytes()
            + self.blocks.last_block_id_hash.estimated_bytes()
            + self.blocks.validators_hash.estimated_bytes()
            + self.blocks.next_validators_hash.estimated_bytes()
            + est_u32(&self.blocks.num_txs)
            + est_u32(&self.blocks.tx_decode_failures)
            + est_opt_str(&self.blocks.fork_step);
        let transactions = self.transactions.canonical.estimated_bytes()
            + self.transactions.tx_hash.estimated_bytes()
            + est_u32(&self.transactions.index)
            + est_u32(&self.transactions.code)
            + est_i64(&self.transactions.gas_wanted)
            + est_i64(&self.transactions.gas_used)
            + est_str(&self.transactions.log)
            + est_str(&self.transactions.info)
            + est_str(&self.transactions.codespace)
            + self.transactions.metadata.estimated_bytes()
            + est_opt_str(&self.transactions.fork_step);
        let events = self.events.canonical.estimated_bytes()
            + est_str(&self.events.source)
            + self.events.tx_hash.estimated_bytes()
            + est_u32(&self.events.tx_index)
            + est_u32(&self.events.event_index)
            + est_u32(&self.events.attribute_index)
            + est_str(&self.events.r#type)
            + est_str(&self.events.key)
            + est_str(&self.events.value)
            + est_opt_str(&self.events.fork_step);
        let messages = self.messages.canonical.estimated_bytes()
            + self.messages.tx_hash.estimated_bytes()
            + est_u32(&self.messages.tx_index)
            + est_u32(&self.messages.message_index)
            + est_str(&self.messages.type_url)
            + est_bin(&self.messages.value)
            + est_opt_str(&self.messages.fork_step);
        [
            ("blocks", blocks),
            ("transactions", transactions),
            ("events", events),
            ("messages", messages),
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
    height: Int64Builder,
    hash: BytesColumn,
    time: Int64Builder,
    chain_id: StringBuilder,
    proposer_address: BytesColumn,
    last_block_id_hash: BytesColumn,
    validators_hash: BytesColumn,
    next_validators_hash: BytesColumn,
    num_txs: UInt32Builder,
    tx_decode_failures: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            height: Int64Builder::new(),
            hash: BytesColumn::new(encoding),
            time: Int64Builder::new(),
            chain_id: StringBuilder::new(),
            proposer_address: BytesColumn::new(encoding),
            last_block_id_hash: BytesColumn::new(encoding),
            validators_hash: BytesColumn::new(encoding),
            next_validators_hash: BytesColumn::new(encoding),
            num_txs: UInt32Builder::new(),
            tx_decode_failures: UInt32Builder::new(),
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
            Arc::new(self.tx_decode_failures.finish()) as Arc<dyn Array>,
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
    metadata: TxMetadataBuilder,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            tx_hash: BytesColumn::new(encoding),
            index: UInt32Builder::new(),
            code: UInt32Builder::new(),
            gas_wanted: Int64Builder::new(),
            gas_used: Int64Builder::new(),
            log: StringBuilder::new(),
            info: StringBuilder::new(),
            codespace: StringBuilder::new(),
            metadata: TxMetadataBuilder::new(),
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
        columns.extend(self.metadata.finish());
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct EventsBuilder {
    canonical: CanonicalBuilder,
    source: StringBuilder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    event_index: UInt32Builder,
    attribute_index: UInt32Builder,
    r#type: StringBuilder,
    key: StringBuilder,
    value: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl EventsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            source: StringBuilder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            event_index: UInt32Builder::new(),
            attribute_index: UInt32Builder::new(),
            r#type: StringBuilder::new(),
            key: StringBuilder::new(),
            value: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_event(
        &mut self,
        identity: &PreparedIdentity,
        source: &str,
        tx_hash: Option<&[u8]>,
        tx_index: Option<u32>,
        event_index: u32,
        event: &cosmos::Event,
        fork_step: Option<&str>,
    ) {
        for attribute in 0..event.attributes.len().max(1) {
            self.canonical.append(identity);
            self.source.append_value(source);
            match tx_hash {
                Some(hash) => self.tx_hash.append_value(hash),
                None => self.tx_hash.append_null(),
            }
            self.tx_index.append_option(tx_index);
            self.event_index.append_value(event_index);
            self.r#type.append_value(&event.r#type);
            let value = event.attributes.get(attribute);
            self.attribute_index
                .append_option(value.map(|_| attribute as u32));
            self.key
                .append_option(value.map(|value| value.key.as_str()));
            self.value
                .append_option(value.map(|value| value.value.as_str()));
            append_fork_step(&mut self.fork_step, fork_step);
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
            Arc::new(self.attribute_index.finish()) as Arc<dyn Array>,
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
            canonical: CanonicalBuilder::with_encoding(encoding),
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
pub(crate) mod tests {
    use super::super::proto::cosmos_tx;
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
            ..Default::default()
        };
        prost::Message::encode_to_vec(&tx)
    }

    pub(crate) fn make_test_block(height: i64) -> cosmos::Block {
        let raw_tx = make_raw_tx("/cosmos.bank.v1beta1.MsgSend", b"\x01\x02\x03");
        cosmos::Block {
            hash: vec![0xab, 0xcd, 0xef],
            height,
            time: Some(prost_types::Timestamp {
                seconds: 1700000000,
                nanos: 0,
            }),
            header: Some(cosmos::Header {
                version: None,
                chain_id: "cosmoshub-4".to_string(),
                height,
                time: Some(prost_types::Timestamp {
                    seconds: 1700000000,
                    nanos: 0,
                }),
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
                    cosmos::EventAttribute {
                        key: "receiver".to_string(),
                        value: "cosmos1abc".to_string(),
                    },
                    cosmos::EventAttribute {
                        key: "amount".to_string(),
                        value: "100uatom".to_string(),
                    },
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
                    attributes: vec![cosmos::EventAttribute {
                        key: "sender".to_string(),
                        value: "cosmos1xyz".to_string(),
                    }],
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
        let mut mapper = CosmosBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

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
            time: Some(prost_types::Timestamp {
                seconds: 1700000000,
                nanos: 0,
            }),
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
        let mut mapper = CosmosBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

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
        let mut mapper = CosmosBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_table_names() {
        let mapper = CosmosBlockMapper::new(false, EncodeBytes::Hex, false);
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
        let mut mapper = CosmosBlockMapper::new(true, EncodeBytes::Hex, false);
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
    fn test_cosmos_canonical_ids_match_hash_fields() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = CosmosBlockMapper::new(false, EncodeBytes::Hex, false);
        let identity = BlockIdentity {
            block_num: 100,
            block_id: "0x9999".to_string(),
            parent_num: 99,
            parent_id: "0x8888".to_string(),
            lib_num: 98,
            timestamp: 1_700_000_000,
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
        let hash = blocks
            .column_by_name("hash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let last_block_id_hash = blocks
            .column_by_name("last_block_id_hash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(block_id.value(0), hash.value(0));
        assert_eq!(parent_id.value(0), last_block_id_hash.value(0));
        assert_ne!(block_id.value(0), "0x9999");
        assert_ne!(parent_id.value(0), "0x8888");
    }
}
