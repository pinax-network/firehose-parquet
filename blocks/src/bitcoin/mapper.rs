use super::proto::btc;
use super::schema;
use arrow::array::Array;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{
    est_f64, est_i32, est_i64, est_list_str, est_opt_str, est_str, est_u32, BlockIdentity,
    BlockMapper, CanonicalBuilder,
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

fn bitcoin_canonical_identity(block: &btc::Block, identity: &BlockIdentity) -> BlockIdentity {
    let mut canonical = identity.clone();
    canonical.block_id = block.hash.clone();
    canonical.parent_id = block.previous_hash.clone();
    canonical
}

// ---------------------------------------------------------------------------
// Bitcoin BlockMapper
// ---------------------------------------------------------------------------

pub struct BitcoinBlockMapper {
    blocks: BlocksBuilder,
    transactions: TransactionsBuilder,
    inputs: InputsBuilder,
    outputs: OutputsBuilder,
    blocks_schema: Schema,
    transactions_schema: Schema,
    inputs_schema: Schema,
    outputs_schema: Schema,
}

impl BitcoinBlockMapper {
    pub fn new(include_fork_step: bool, encoding: EncodeBytes) -> Self {
        Self {
            blocks: BlocksBuilder::new(include_fork_step, &encoding),
            transactions: TransactionsBuilder::new(include_fork_step, &encoding),
            inputs: InputsBuilder::new(include_fork_step, &encoding),
            outputs: OutputsBuilder::new(include_fork_step, &encoding),
            blocks_schema: schema::blocks_schema(include_fork_step, &encoding),
            transactions_schema: schema::transactions_schema(include_fork_step, &encoding),
            inputs_schema: schema::inputs_schema(include_fork_step, &encoding),
            outputs_schema: schema::outputs_schema(include_fork_step, &encoding),
        }
    }

    fn map_btc_block(
        &mut self,
        block: &btc::Block,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        let height = block.height;
        let block_hash = &block.hash;
        let block_time = block.time;
        let canonical_identity = bitcoin_canonical_identity(block, identity);

        self.blocks.canonical.append(&canonical_identity);
        self.blocks.hash.append_value(&block.hash);
        self.blocks.height.append_value(height);
        self.blocks.previous_hash.append_value(&block.previous_hash);
        self.blocks.merkle_root.append_value(&block.merkle_root);
        self.blocks.time.append_value(block.time);
        self.blocks.nonce.append_value(block.nonce);
        self.blocks.bits.append_value(&block.bits);
        self.blocks.difficulty.append_value(block.difficulty);
        self.blocks.size.append_value(block.size);
        self.blocks.stripped_size.append_value(block.stripped_size);
        self.blocks.weight.append_value(block.weight);
        self.blocks.version.append_value(block.version);
        self.blocks.n_tx.append_value(block.n_tx);
        self.blocks.mediantime.append_value(block.mediantime);
        self.blocks.chainwork.append_value(&block.chainwork);
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        for (tx_index, tx) in block.tx.iter().enumerate() {
            self.map_transaction(
                height,
                block_hash,
                block_time,
                tx_index as u32,
                tx,
                &canonical_identity,
                fork_step,
            );
        }
    }

    fn map_transaction(
        &mut self,
        block_height: i64,
        block_hash: &str,
        block_time: i64,
        tx_index: u32,
        tx: &btc::Transaction,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        let tx_hash = &tx.txid;

        self.transactions.canonical.append(identity);
        self.transactions.txid.append_value(&tx.txid);
        self.transactions.hash.append_value(&tx.hash);
        self.transactions.size.append_value(tx.size);
        self.transactions.vsize.append_value(tx.vsize);
        self.transactions.weight.append_value(tx.weight);
        self.transactions.version.append_value(tx.version);
        self.transactions.locktime.append_value(tx.locktime);
        self.transactions.block_hash.append_value(block_hash);
        self.transactions.block_height.append_value(block_height);
        self.transactions.block_time.append_value(block_time);
        self.transactions.tx_index.append_value(tx_index);
        append_fork_step(&mut self.transactions.fork_step, fork_step);

        for (i, vin) in tx.vin.iter().enumerate() {
            let script_sig = vin.script_sig.as_ref();
            self.inputs.canonical.append(identity);
            self.inputs.tx_hash.append_value(tx_hash);
            self.inputs.block_height.append_value(block_height);
            self.inputs.input_index.append_value(i as u32);
            self.inputs.prev_txid.append_value(&vin.txid);
            self.inputs.prev_vout.append_value(vin.vout);
            self.inputs.sequence.append_value(vin.sequence);
            self.inputs
                .script_sig_asm
                .append_value(script_sig.map_or("", |s| &s.asm));
            self.inputs
                .script_sig_hex
                .append_value(script_sig.map_or("", |s| &s.hex));
            self.inputs.coinbase.append_value(&vin.coinbase);

            let witness_values = self.inputs.witness.values();
            for w in &vin.txinwitness {
                witness_values.append_value(w);
            }
            self.inputs.witness.append(true);
            append_fork_step(&mut self.inputs.fork_step, fork_step);
        }

        for vout in &tx.vout {
            let script = vout.script_pub_key.as_ref();
            self.outputs.canonical.append(identity);
            self.outputs.tx_hash.append_value(tx_hash);
            self.outputs.block_height.append_value(block_height);
            self.outputs.output_index.append_value(vout.n);
            self.outputs.value.append_value(vout.value);
            self.outputs
                .script_pubkey_asm
                .append_value(script.map_or("", |s| &s.asm));
            self.outputs
                .script_pubkey_hex
                .append_value(script.map_or("", |s| &s.hex));
            self.outputs
                .script_pubkey_type
                .append_value(script.map_or("", |s| &s.r#type));
            self.outputs
                .script_pubkey_address
                .append_value(script.map_or("", |s| &s.address));
            append_fork_step(&mut self.outputs.fork_step, fork_step);
        }
    }
}

impl BlockMapper for BitcoinBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        let block = btc::Block::decode(block_bytes)?;
        let tx_count = block.tx.len() as u64;
        self.map_btc_block(&block, identity, fork_step);
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
            "inputs".to_string(),
            self.inputs.finish(&self.inputs_schema)?,
        );
        result.insert(
            "outputs".to_string(),
            self.outputs.finish(&self.outputs_schema)?,
        );
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        self.blocks
            .canonical
            .len()
            .max(self.transactions.canonical.len())
            .max(self.inputs.canonical.len())
            .max(self.outputs.canonical.len())
    }

    fn total_rows(&self) -> usize {
        self.blocks.canonical.len()
            + self.transactions.canonical.len()
            + self.inputs.canonical.len()
            + self.outputs.canonical.len()
    }

    fn largest_table(&mut self) -> (&str, usize) {
        let blocks = self.blocks.canonical.estimated_bytes()
            + est_str(&self.blocks.hash)
            + est_i64(&self.blocks.height)
            + est_str(&self.blocks.previous_hash)
            + est_str(&self.blocks.merkle_root)
            + est_i64(&self.blocks.time)
            + est_u32(&self.blocks.nonce)
            + est_str(&self.blocks.bits)
            + est_f64(&self.blocks.difficulty)
            + est_i32(&self.blocks.size)
            + est_i32(&self.blocks.stripped_size)
            + est_i32(&self.blocks.weight)
            + est_i32(&self.blocks.version)
            + est_u32(&self.blocks.n_tx)
            + est_i64(&self.blocks.mediantime)
            + est_str(&self.blocks.chainwork)
            + est_opt_str(&self.blocks.fork_step);
        let transactions = self.transactions.canonical.estimated_bytes()
            + est_str(&self.transactions.txid)
            + est_str(&self.transactions.hash)
            + est_i32(&self.transactions.size)
            + est_i32(&self.transactions.vsize)
            + est_i32(&self.transactions.weight)
            + est_u32(&self.transactions.version)
            + est_u32(&self.transactions.locktime)
            + est_str(&self.transactions.block_hash)
            + est_i64(&self.transactions.block_height)
            + est_i64(&self.transactions.block_time)
            + est_u32(&self.transactions.tx_index)
            + est_opt_str(&self.transactions.fork_step);
        let inputs = self.inputs.canonical.estimated_bytes()
            + est_str(&self.inputs.tx_hash)
            + est_i64(&self.inputs.block_height)
            + est_u32(&self.inputs.input_index)
            + est_str(&self.inputs.prev_txid)
            + est_u32(&self.inputs.prev_vout)
            + est_u32(&self.inputs.sequence)
            + est_str(&self.inputs.script_sig_asm)
            + est_str(&self.inputs.script_sig_hex)
            + est_str(&self.inputs.coinbase)
            + est_list_str(&mut self.inputs.witness)
            + est_opt_str(&self.inputs.fork_step);
        let outputs = self.outputs.canonical.estimated_bytes()
            + est_str(&self.outputs.tx_hash)
            + est_i64(&self.outputs.block_height)
            + est_u32(&self.outputs.output_index)
            + est_f64(&self.outputs.value)
            + est_str(&self.outputs.script_pubkey_asm)
            + est_str(&self.outputs.script_pubkey_hex)
            + est_str(&self.outputs.script_pubkey_type)
            + est_str(&self.outputs.script_pubkey_address)
            + est_opt_str(&self.outputs.fork_step);
        [
            ("blocks", blocks),
            ("transactions", transactions),
            ("inputs", inputs),
            ("outputs", outputs),
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
    hash: StringBuilder,
    height: Int64Builder,
    previous_hash: StringBuilder,
    merkle_root: StringBuilder,
    time: Int64Builder,
    nonce: UInt32Builder,
    bits: StringBuilder,
    difficulty: Float64Builder,
    size: Int32Builder,
    stripped_size: Int32Builder,
    weight: Int32Builder,
    version: Int32Builder,
    n_tx: UInt32Builder,
    mediantime: Int64Builder,
    chainwork: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            hash: StringBuilder::new(),
            height: Int64Builder::new(),
            previous_hash: StringBuilder::new(),
            merkle_root: StringBuilder::new(),
            time: Int64Builder::new(),
            nonce: UInt32Builder::new(),
            bits: StringBuilder::new(),
            difficulty: Float64Builder::new(),
            size: Int32Builder::new(),
            stripped_size: Int32Builder::new(),
            weight: Int32Builder::new(),
            version: Int32Builder::new(),
            n_tx: UInt32Builder::new(),
            mediantime: Int64Builder::new(),
            chainwork: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.hash.finish()) as Arc<dyn Array>,
            Arc::new(self.height.finish()) as Arc<dyn Array>,
            Arc::new(self.previous_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.merkle_root.finish()) as Arc<dyn Array>,
            Arc::new(self.time.finish()) as Arc<dyn Array>,
            Arc::new(self.nonce.finish()) as Arc<dyn Array>,
            Arc::new(self.bits.finish()) as Arc<dyn Array>,
            Arc::new(self.difficulty.finish()) as Arc<dyn Array>,
            Arc::new(self.size.finish()) as Arc<dyn Array>,
            Arc::new(self.stripped_size.finish()) as Arc<dyn Array>,
            Arc::new(self.weight.finish()) as Arc<dyn Array>,
            Arc::new(self.version.finish()) as Arc<dyn Array>,
            Arc::new(self.n_tx.finish()) as Arc<dyn Array>,
            Arc::new(self.mediantime.finish()) as Arc<dyn Array>,
            Arc::new(self.chainwork.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct TransactionsBuilder {
    canonical: CanonicalBuilder,
    txid: StringBuilder,
    hash: StringBuilder,
    size: Int32Builder,
    vsize: Int32Builder,
    weight: Int32Builder,
    version: UInt32Builder,
    locktime: UInt32Builder,
    block_hash: StringBuilder,
    block_height: Int64Builder,
    block_time: Int64Builder,
    tx_index: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            txid: StringBuilder::new(),
            hash: StringBuilder::new(),
            size: Int32Builder::new(),
            vsize: Int32Builder::new(),
            weight: Int32Builder::new(),
            version: UInt32Builder::new(),
            locktime: UInt32Builder::new(),
            block_hash: StringBuilder::new(),
            block_height: Int64Builder::new(),
            block_time: Int64Builder::new(),
            tx_index: UInt32Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.txid.finish()) as Arc<dyn Array>,
            Arc::new(self.hash.finish()) as Arc<dyn Array>,
            Arc::new(self.size.finish()) as Arc<dyn Array>,
            Arc::new(self.vsize.finish()) as Arc<dyn Array>,
            Arc::new(self.weight.finish()) as Arc<dyn Array>,
            Arc::new(self.version.finish()) as Arc<dyn Array>,
            Arc::new(self.locktime.finish()) as Arc<dyn Array>,
            Arc::new(self.block_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.block_height.finish()) as Arc<dyn Array>,
            Arc::new(self.block_time.finish()) as Arc<dyn Array>,
            Arc::new(self.tx_index.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct InputsBuilder {
    canonical: CanonicalBuilder,
    tx_hash: StringBuilder,
    block_height: Int64Builder,
    input_index: UInt32Builder,
    prev_txid: StringBuilder,
    prev_vout: UInt32Builder,
    sequence: UInt32Builder,
    script_sig_asm: StringBuilder,
    script_sig_hex: StringBuilder,
    coinbase: StringBuilder,
    witness: ListBuilder<StringBuilder>,
    fork_step: Option<StringBuilder>,
}

impl InputsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            tx_hash: StringBuilder::new(),
            block_height: Int64Builder::new(),
            input_index: UInt32Builder::new(),
            prev_txid: StringBuilder::new(),
            prev_vout: UInt32Builder::new(),
            sequence: UInt32Builder::new(),
            script_sig_asm: StringBuilder::new(),
            script_sig_hex: StringBuilder::new(),
            coinbase: StringBuilder::new(),
            witness: ListBuilder::new(StringBuilder::new()),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.tx_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.block_height.finish()) as Arc<dyn Array>,
            Arc::new(self.input_index.finish()) as Arc<dyn Array>,
            Arc::new(self.prev_txid.finish()) as Arc<dyn Array>,
            Arc::new(self.prev_vout.finish()) as Arc<dyn Array>,
            Arc::new(self.sequence.finish()) as Arc<dyn Array>,
            Arc::new(self.script_sig_asm.finish()) as Arc<dyn Array>,
            Arc::new(self.script_sig_hex.finish()) as Arc<dyn Array>,
            Arc::new(self.coinbase.finish()) as Arc<dyn Array>,
            Arc::new(self.witness.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct OutputsBuilder {
    canonical: CanonicalBuilder,
    tx_hash: StringBuilder,
    block_height: Int64Builder,
    output_index: UInt32Builder,
    value: Float64Builder,
    script_pubkey_asm: StringBuilder,
    script_pubkey_hex: StringBuilder,
    script_pubkey_type: StringBuilder,
    script_pubkey_address: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl OutputsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            tx_hash: StringBuilder::new(),
            block_height: Int64Builder::new(),
            output_index: UInt32Builder::new(),
            value: Float64Builder::new(),
            script_pubkey_asm: StringBuilder::new(),
            script_pubkey_hex: StringBuilder::new(),
            script_pubkey_type: StringBuilder::new(),
            script_pubkey_address: StringBuilder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.tx_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.block_height.finish()) as Arc<dyn Array>,
            Arc::new(self.output_index.finish()) as Arc<dyn Array>,
            Arc::new(self.value.finish()) as Arc<dyn Array>,
            Arc::new(self.script_pubkey_asm.finish()) as Arc<dyn Array>,
            Arc::new(self.script_pubkey_hex.finish()) as Arc<dyn Array>,
            Arc::new(self.script_pubkey_type.finish()) as Arc<dyn Array>,
            Arc::new(self.script_pubkey_address.finish()) as Arc<dyn Array>,
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
    use super::*;

    pub(crate) fn make_test_block(height: i64) -> btc::Block {
        btc::Block {
            hash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f".to_string(),
            size: 285,
            stripped_size: 285,
            weight: 1140,
            height,
            version: 1,
            version_hex: "00000001".to_string(),
            merkle_root: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b".to_string(),
            tx: vec![btc::Transaction {
                hex: String::new(),
                txid: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b".to_string(),
                hash: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b".to_string(),
                size: 204,
                vsize: 204,
                weight: 816,
                version: 1,
                locktime: 0,
                vin: vec![btc::Vin {
                    txid: String::new(),
                    vout: 0xffffffff,
                    script_sig: Some(btc::ScriptSig {
                        asm: "OP_PUSHBYTES_4 ffff001d OP_PUSHBYTES_1 04".to_string(),
                        hex: "04ffff001d0104".to_string(),
                    }),
                    sequence: 0xffffffff,
                    txinwitness: vec!["304402200a1b".to_string(), "03abc123".to_string()],
                    coinbase: "04ffff001d0104455468652054696d65732030332f4a616e2f32303039".to_string(),
                }],
                vout: vec![btc::Vout {
                    value: 50.0,
                    n: 0,
                    script_pub_key: Some(btc::ScriptPubKey {
                        asm: "04678afdb0 OP_CHECKSIG".to_string(),
                        hex: "4104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac".to_string(),
                        req_sigs: 1,
                        r#type: "pubkey".to_string(),
                        address: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(),
                        addresses: vec![],
                    }),
                }],
                blockhash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f".to_string(),
                blocktime: 1231006505,
            }],
            time: 1231006505,
            mediantime: 1231006505,
            nonce: 2083236893,
            bits: "1d00ffff".to_string(),
            difficulty: 1.0,
            chainwork: "0000000000000000000000000000000000000000000000000000000100010001".to_string(),
            n_tx: 1,
            previous_hash: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        }
    }

    #[test]
    fn test_map_and_flush() {
        let block = make_test_block(0);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BitcoinBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["inputs"].num_rows(), 1);
        assert_eq!(batches["outputs"].num_rows(), 1);
    }

    #[test]
    fn test_bitcoin_canonical_ids_match_block_hash_fields() {
        let block = make_test_block(0);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BitcoinBlockMapper::new(false, EncodeBytes::Hex);
        let identity = BlockIdentity {
            block_num: 0,
            block_id: "firehose-envelope-id".to_string(),
            parent_num: 0,
            parent_id: "firehose-envelope-parent-id".to_string(),
            lib_num: 0,
            timestamp: block.time,
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
        let previous_hash = blocks
            .column_by_name("previous_hash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(block_id.value(0), format!("0x{}", hash.value(0)));
        assert_eq!(parent_id.value(0), format!("0x{}", previous_hash.value(0)));
    }

    #[test]
    fn test_bitcoin_hex_no_prefix_canonical_ids_match_block_hash_fields() {
        let block = make_test_block(0);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BitcoinBlockMapper::new(false, EncodeBytes::HexNoPrefix);
        let identity = BlockIdentity {
            block_num: 0,
            block_id: "firehose-envelope-id".to_string(),
            parent_num: 0,
            parent_id: "firehose-envelope-parent-id".to_string(),
            lib_num: 0,
            timestamp: block.time,
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
        let previous_hash = blocks
            .column_by_name("previous_hash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(block_id.value(0), hash.value(0));
        assert_eq!(parent_id.value(0), previous_hash.value(0));
        assert!(!block_id.value(0).starts_with("0x"));
        assert!(!parent_id.value(0).starts_with("0x"));
    }

    #[test]
    fn test_empty_block() {
        let block = btc::Block {
            hash: "00000000".to_string(),
            height: 100,
            time: 1231006505,
            nonce: 0,
            bits: "1d00ffff".to_string(),
            difficulty: 1.0,
            chainwork: "0".to_string(),
            n_tx: 0,
            previous_hash: "00000000".to_string(),
            merkle_root: String::new(),
            tx: vec![],
            size: 0,
            stripped_size: 0,
            weight: 0,
            version: 1,
            version_hex: String::new(),
            mediantime: 0,
        };
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BitcoinBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 0);
        assert_eq!(batches["inputs"].num_rows(), 0);
        assert_eq!(batches["outputs"].num_rows(), 0);
    }

    #[test]
    fn test_flush_resets() {
        let block = make_test_block(1);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BitcoinBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_table_names() {
        let mapper = BitcoinBlockMapper::new(false, EncodeBytes::Hex);
        assert_eq!(mapper.table_names().len(), 4);
        assert!(mapper.table_names().contains(&"blocks"));
        assert!(mapper.table_names().contains(&"transactions"));
        assert!(mapper.table_names().contains(&"inputs"));
        assert!(mapper.table_names().contains(&"outputs"));
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_block(0);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BitcoinBlockMapper::new(true, EncodeBytes::Hex);
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
}
