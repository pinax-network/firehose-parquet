use super::proto::{protocol, tron};
use super::schema;
use arrow::array::*;
use arrow::datatypes::{Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, EncodeBytes};
use firehose_parquet::traits::{
    est_bool, est_i64, est_opt_str, est_str, est_u32, est_u64, BlockIdentity, BlockMapper,
    CanonicalBuilder, PreparedIdentity,
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
    if include {
        Some(StringBuilder::new())
    } else {
        None
    }
}

fn tron_reserved_encoding(encoding: &EncodeBytes) -> EncodeBytes {
    match encoding {
        // Tron Base58Check is only appropriate for address/account-like values.
        // Keep protocol-native hash/topic identifiers as raw hex without `0x`.
        // Besides the requested block/parent/transaction hash and log topic fields,
        // `tx_trie_root` and internal transaction `hash` also stay hex because
        // they are hash-like protocol identifiers rather than addresses.
        EncodeBytes::TronBase58 => EncodeBytes::HexNoPrefix,
        other => other.clone(),
    }
}

fn response_code_text(value: i32) -> &'static str {
    tron::ResponseCode::try_from(value)
        .map(|code| code.as_str_name())
        .unwrap_or("UNKNOWN")
}

fn contract_type_text(value: i32) -> &'static str {
    protocol::transaction::contract::ContractType::try_from(value)
        .map(|contract_type| contract_type.as_str_name())
        .unwrap_or("UNKNOWN")
}

fn estimated_dictionary_index_bytes(len: usize) -> usize {
    // Largest-table tracking only needs a cheap relative estimate. For enum-backed
    // dictionary columns, the shared string dictionary cardinality is fixed and
    // small, so counting the per-row indices is sufficient for that comparison.
    len * std::mem::size_of::<i32>()
}

// ---------------------------------------------------------------------------
// Tron BlockMapper
// ---------------------------------------------------------------------------

pub struct TronBlockMapper {
    include_failed_transactions: bool,
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
            logs: LogsBuilder::new(include_fork_step, enc),
            internal_transactions: InternalTransactionsBuilder::new(include_fork_step, enc),
            blocks_schema: schema::blocks_schema(include_fork_step, enc),
            transactions_schema: schema::transactions_schema(include_fork_step, enc),
            logs_schema: schema::logs_schema(include_fork_step, enc),
            internal_transactions_schema: schema::internal_transactions_schema(
                include_fork_step,
                enc,
            ),
        }
    }

    fn map_tron_block(
        &mut self,
        block: &tron::Block,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let header = block.header.as_ref();
        let block_number = header.map_or(0, |h| h.number);

        self.blocks.canonical.append(identity);
        self.blocks.number.append_value(block_number);
        self.blocks.hash.append_value(&block.id);
        self.blocks
            .parent_hash
            .append_value(header.map(|h| h.parent_hash.as_slice()).unwrap_or(&[]));

        self.blocks
            .witness_address
            .append_value(header.map(|h| h.witness_address.as_slice()).unwrap_or(&[]));
        self.blocks
            .version
            .append_value(header.map_or(0, |h| h.version));
        self.blocks
            .tx_trie_root
            .append_value(header.map(|h| h.tx_trie_root.as_slice()).unwrap_or(&[]));
        self.blocks
            .parent_number
            .append_value(header.map_or(0, |h| h.parent_number));
        self.blocks
            .num_transactions
            .append_value(block.transactions.len() as u32);
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        for tx in &block.transactions {
            // Skip failed transactions (result != true) unless flag is set
            if !self.include_failed_transactions && !tx.result {
                continue;
            }
            self.map_transaction(block_number, tx, identity, fork_step);
        }
    }

    fn map_transaction(
        &mut self,
        block_number: u64,
        tx: &tron::Transaction,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let info = tx.info.as_ref();
        let fee = info.map_or(0, |i| i.fee);
        let contract_type = tx.contracts.first().map_or(0, |c| c.r#type);

        self.transactions.canonical.append(identity);
        self.transactions.block_number.append_value(block_number);
        self.transactions.txid.append_value(&tx.txid);
        self.transactions.result.append_value(tx.result);
        self.transactions
            .code
            .append_value(response_code_text(tx.code));
        self.transactions.energy_used.append_value(tx.energy_used);
        self.transactions
            .energy_penalty
            .append_value(tx.energy_penalty);
        self.transactions.fee.append_value(fee);
        self.transactions
            .contract_type
            .append_value(contract_type_text(contract_type));
        self.transactions.expiration_ms.append_value(tx.expiration);
        self.transactions.tx_timestamp_ms.append_value(tx.timestamp);
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
                if let Some(t) = topics.first() {
                    self.logs.topic0.append_value(t);
                } else {
                    self.logs.topic0.append_null();
                }
                if let Some(t) = topics.get(1) {
                    self.logs.topic1.append_value(t);
                } else {
                    self.logs.topic1.append_null();
                }
                if let Some(t) = topics.get(2) {
                    self.logs.topic2.append_value(t);
                } else {
                    self.logs.topic2.append_null();
                }
                if let Some(t) = topics.get(3) {
                    self.logs.topic3.append_value(t);
                } else {
                    self.logs.topic3.append_null();
                }
                self.logs.data.append_value(&log.data);
                append_fork_step(&mut self.logs.fork_step, fork_step);
            }

            // Map internal transactions from TransactionInfo
            for (internal_index, itx) in info.internal_transactions.iter().enumerate() {
                self.internal_transactions.canonical.append(identity);
                self.internal_transactions
                    .block_number
                    .append_value(block_number);
                self.internal_transactions.tx_hash.append_value(&tx.txid);
                self.internal_transactions
                    .internal_index
                    .append_value(internal_index as u32);
                self.internal_transactions.hash.append_value(&itx.hash);
                self.internal_transactions
                    .caller_address
                    .append_value(&itx.caller_address);
                self.internal_transactions
                    .transfer_to_address
                    .append_value(&itx.transfer_to_address);
                self.internal_transactions
                    .note
                    .append_value(String::from_utf8_lossy(&itx.note).as_ref());
                self.internal_transactions
                    .rejected
                    .append_value(itx.rejected);
                append_fork_step(&mut self.internal_transactions.fork_step, fork_step);
            }
        }
    }
}

impl BlockMapper for TronBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        let block = tron::Block::decode(block_bytes)?;
        let tx_count = block.transactions.len() as u64;
        let identity = self.blocks.canonical.prepare(identity);
        self.map_tron_block(&block, &identity, fork_step);
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
        result.insert("logs".to_string(), self.logs.finish(&self.logs_schema)?);
        result.insert(
            "internal_transactions".to_string(),
            self.internal_transactions
                .finish(&self.internal_transactions_schema)?,
        );
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        self.blocks
            .canonical
            .len()
            .max(self.transactions.canonical.len())
            .max(self.logs.canonical.len())
            .max(self.internal_transactions.canonical.len())
    }

    fn total_rows(&self) -> usize {
        self.blocks.canonical.len()
            + self.transactions.canonical.len()
            + self.logs.canonical.len()
            + self.internal_transactions.canonical.len()
    }

    fn largest_table(&mut self) -> (&str, usize) {
        let blocks = self.blocks.canonical.estimated_bytes()
            + est_u64(&self.blocks.number)
            + self.blocks.hash.estimated_bytes()
            + self.blocks.parent_hash.estimated_bytes()
            + self.blocks.witness_address.estimated_bytes()
            + est_u32(&self.blocks.version)
            + self.blocks.tx_trie_root.estimated_bytes()
            + est_u64(&self.blocks.parent_number)
            + est_u32(&self.blocks.num_transactions)
            + est_opt_str(&self.blocks.fork_step);
        let transactions = self.transactions.canonical.estimated_bytes()
            + est_u64(&self.transactions.block_number)
            + self.transactions.txid.estimated_bytes()
            + est_bool(&self.transactions.result)
            + estimated_dictionary_index_bytes(self.transactions.code.len())
            + est_i64(&self.transactions.energy_used)
            + est_i64(&self.transactions.energy_penalty)
            + est_i64(&self.transactions.fee)
            + estimated_dictionary_index_bytes(self.transactions.contract_type.len())
            + est_i64(&self.transactions.expiration_ms)
            + est_i64(&self.transactions.tx_timestamp_ms)
            + est_opt_str(&self.transactions.fork_step);
        let logs = self.logs.canonical.estimated_bytes()
            + est_u64(&self.logs.block_number)
            + self.logs.tx_hash.estimated_bytes()
            + est_u32(&self.logs.log_index)
            + self.logs.address.estimated_bytes()
            + self.logs.topic0.estimated_bytes()
            + self.logs.topic1.estimated_bytes()
            + self.logs.topic2.estimated_bytes()
            + self.logs.topic3.estimated_bytes()
            + self.logs.data.estimated_bytes()
            + est_opt_str(&self.logs.fork_step);
        let internal_transactions = self.internal_transactions.canonical.estimated_bytes()
            + est_u64(&self.internal_transactions.block_number)
            + self.internal_transactions.tx_hash.estimated_bytes()
            + est_u32(&self.internal_transactions.internal_index)
            + self.internal_transactions.hash.estimated_bytes()
            + self.internal_transactions.caller_address.estimated_bytes()
            + self
                .internal_transactions
                .transfer_to_address
                .estimated_bytes()
            + est_str(&self.internal_transactions.note)
            + est_bool(&self.internal_transactions.rejected)
            + est_opt_str(&self.internal_transactions.fork_step);
        [
            ("blocks", blocks),
            ("transactions", transactions),
            ("logs", logs),
            ("internal_transactions", internal_transactions),
        ]
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
    number: UInt64Builder,
    hash: BytesColumn,
    parent_hash: BytesColumn,
    witness_address: BytesColumn,
    version: UInt32Builder,
    tx_trie_root: BytesColumn,
    parent_number: UInt64Builder,
    num_transactions: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        let reserved_encoding = tron_reserved_encoding(encoding);
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            number: UInt64Builder::new(),
            hash: BytesColumn::new(&reserved_encoding),
            parent_hash: BytesColumn::new(&reserved_encoding),
            witness_address: BytesColumn::new(encoding),
            version: UInt32Builder::new(),
            tx_trie_root: BytesColumn::new(&reserved_encoding),
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
    code: StringDictionaryBuilder<Int32Type>,
    energy_used: Int64Builder,
    energy_penalty: Int64Builder,
    fee: Int64Builder,
    contract_type: StringDictionaryBuilder<Int32Type>,
    expiration_ms: Int64Builder,
    tx_timestamp_ms: Int64Builder,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        let reserved_encoding = tron_reserved_encoding(encoding);
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            txid: BytesColumn::new(&reserved_encoding),
            result: BooleanBuilder::new(),
            code: StringDictionaryBuilder::new(),
            energy_used: Int64Builder::new(),
            energy_penalty: Int64Builder::new(),
            fee: Int64Builder::new(),
            contract_type: StringDictionaryBuilder::new(),
            expiration_ms: Int64Builder::new(),
            tx_timestamp_ms: Int64Builder::new(),
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
            Arc::new(self.expiration_ms.finish()) as Arc<dyn Array>,
            Arc::new(self.tx_timestamp_ms.finish()) as Arc<dyn Array>,
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
        let reserved_encoding = tron_reserved_encoding(encoding);
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(&reserved_encoding),
            log_index: UInt32Builder::new(),
            address: BytesColumn::new(encoding),
            topic0: BytesColumn::new(&reserved_encoding),
            topic1: BytesColumn::new(&reserved_encoding),
            topic2: BytesColumn::new(&reserved_encoding),
            topic3: BytesColumn::new(&reserved_encoding),
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
        let reserved_encoding = tron_reserved_encoding(encoding);
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(&reserved_encoding),
            internal_index: UInt32Builder::new(),
            hash: BytesColumn::new(&reserved_encoding),
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
pub(crate) mod tests {
    use super::super::proto::{protocol, tron};
    use super::*;
    use firehose_parquet::encode::{encode_hex_no_prefix, encode_tron_base58};

    fn tron_address(seed: u8) -> Vec<u8> {
        let mut address = vec![0x41];
        address.extend(std::iter::repeat_n(seed, 20));
        address
    }

    fn string_col<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        let column = batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column {name}"));
        column
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column {name} is not Utf8"))
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

    pub(crate) fn make_test_block(number: u64) -> tron::Block {
        tron::Block {
            id: vec![0x01, 0x02, 0x03],
            header: Some(tron::BlockHeader {
                number,
                tx_trie_root: vec![0xaa, 0xbb],
                witness_address: tron_address(0x11),
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
                        address: tron_address(0x22),
                        topics: vec![vec![0xab, 0xcd], vec![0xef, 0x01]],
                        data: vec![0x01, 0x02, 0x03],
                    }],
                    result: 0,
                    res_message: vec![],
                    asset_issue_id: String::new(),
                    withdraw_amount: 0,
                    unfreeze_amount: 0,
                    internal_transactions: vec![protocol::InternalTransaction {
                        hash: vec![0x11, 0x22],
                        caller_address: tron_address(0xaa),
                        transfer_to_address: tron_address(0xbb),
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
        let mut mapper = TronBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["logs"].num_rows(), 1);
        assert_eq!(batches["internal_transactions"].num_rows(), 1);

        let transactions = &batches["transactions"];
        assert_eq!(
            transactions
                .column_by_name("code")
                .expect("code column should exist")
                .data_type(),
            &arrow::datatypes::DataType::Dictionary(
                Box::new(arrow::datatypes::DataType::Int32),
                Box::new(arrow::datatypes::DataType::Utf8),
            )
        );
        assert_eq!(
            transactions
                .column_by_name("contract_type")
                .expect("contract_type column should exist")
                .data_type(),
            &arrow::datatypes::DataType::Dictionary(
                Box::new(arrow::datatypes::DataType::Int32),
                Box::new(arrow::datatypes::DataType::Utf8),
            )
        );
        assert_eq!(get_string_value(transactions, "code", 0), "SUCCESS");
        assert_eq!(
            get_string_value(transactions, "contract_type", 0),
            "TriggerSmartContract"
        );
    }

    #[test]
    fn test_transaction_times_do_not_shadow_canonical_timestamp() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = TronBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        let transactions = &batches["transactions"];
        let schema = transactions.schema();
        let timestamp_columns: Vec<_> = schema
            .fields()
            .iter()
            .filter(|field| field.name().contains("timestamp"))
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(timestamp_columns, ["timestamp", "tx_timestamp_ms"]);

        let i64_col = |name: &str| {
            transactions
                .column_by_name(name)
                .unwrap_or_else(|| panic!("missing column {name}"))
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("column {name} is not Int64"))
                .value(0)
        };
        assert_eq!(i64_col("tx_timestamp_ms"), 1_700_000_000_000);
        assert_eq!(i64_col("expiration_ms"), 1_700_000_060_000);
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
        let mut mapper = TronBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
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
        let mut mapper = TronBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_table_names() {
        let mapper = TronBlockMapper::new(false, EncodeBytes::Hex, false);
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
        let mut mapper = TronBlockMapper::new(true, EncodeBytes::Hex, false);
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
    fn test_tron_base58_keeps_reserved_hash_fields_hex_without_prefix() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let identity = BlockIdentity {
            block_num: 100,
            block_id: "0x010203".to_string(),
            parent_num: 99,
            parent_id: "0x000102".to_string(),
            lib_num: 100,
            timestamp: 1_700_000_000,
            timestamp_nanos: 0,
            fork_step: None,
        };
        let mut mapper = TronBlockMapper::new(false, EncodeBytes::TronBase58, false);
        mapper.map_block(&block_bytes, &identity, None).unwrap();

        let batches = mapper.flush().unwrap();

        let blocks = &batches["blocks"];
        assert_eq!(string_col(blocks, "block_id").value(0), "010203");
        assert_eq!(string_col(blocks, "parent_id").value(0), "000102");
        assert_eq!(string_col(blocks, "hash").value(0), "010203");
        assert_eq!(string_col(blocks, "parent_hash").value(0), "000102");
        assert_eq!(string_col(blocks, "tx_trie_root").value(0), "aabb");
        assert_eq!(
            string_col(blocks, "witness_address").value(0),
            encode_tron_base58(&tron_address(0x11))
        );

        let transactions = &batches["transactions"];
        assert_eq!(string_col(transactions, "txid").value(0), "deadbeef");

        let logs = &batches["logs"];
        assert_eq!(string_col(logs, "tx_hash").value(0), "deadbeef");
        assert_eq!(
            string_col(logs, "address").value(0),
            encode_tron_base58(&tron_address(0x22))
        );
        assert_eq!(string_col(logs, "topic0").value(0), "abcd");
        assert_eq!(string_col(logs, "topic1").value(0), "ef01");
        assert_eq!(
            string_col(logs, "data").value(0),
            encode_hex_no_prefix(&[0x01, 0x02, 0x03])
        );

        let internal_transactions = &batches["internal_transactions"];
        assert_eq!(
            string_col(internal_transactions, "tx_hash").value(0),
            "deadbeef"
        );
        assert_eq!(string_col(internal_transactions, "hash").value(0), "1122");
        assert_eq!(
            string_col(internal_transactions, "caller_address").value(0),
            encode_tron_base58(&tron_address(0xaa))
        );
        assert_eq!(
            string_col(internal_transactions, "transfer_to_address").value(0),
            encode_tron_base58(&tron_address(0xbb))
        );
    }
}
