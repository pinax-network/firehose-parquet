use super::amounts::transaction_satoshis;
use super::proto::btc;
use super::schema;
use arrow::array::Array;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{
    decode_id_bytes, est_f64, est_i32, est_i64, est_list_str, est_opt_str, est_str, est_u32,
    est_u64, BlockIdentity, BlockMapper, CanonicalBuilder, PreparedIdentity,
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

fn nonempty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
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
        output_satoshis: &[Vec<u64>],
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let height = block.height;
        let block_hash = &block.hash;
        let block_time = block.time;

        self.blocks.canonical.append(identity);
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
                &output_satoshis[tx_index],
                identity,
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
        output_satoshis: &[u64],
        identity: &PreparedIdentity,
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
            let coinbase = nonempty(&vin.coinbase);
            let previous = if coinbase.is_some() {
                None
            } else {
                nonempty(&vin.txid)
            };
            let script_sig = if coinbase.is_some() {
                None
            } else {
                vin.script_sig.as_ref()
            };
            self.inputs.canonical.append(identity);
            self.inputs.tx_hash.append_value(tx_hash);
            self.inputs.block_height.append_value(block_height);
            self.inputs.input_index.append_value(i as u32);
            self.inputs.prev_txid.append_option(previous);
            self.inputs
                .prev_vout
                .append_option(previous.map(|_| vin.vout));
            self.inputs.tx_index.append_value(tx_index);
            self.inputs.sequence.append_value(vin.sequence);
            self.inputs
                .script_sig_asm
                .append_option(script_sig.map(|s| s.asm.as_str()));
            self.inputs
                .script_sig_hex
                .append_option(script_sig.map(|s| s.hex.as_str()));
            self.inputs.coinbase.append_option(coinbase);

            let witness_values = self.inputs.witness.values();
            for w in &vin.txinwitness {
                witness_values.append_value(w);
            }
            self.inputs.witness.append(true);
            append_fork_step(&mut self.inputs.fork_step, fork_step);
        }

        for (vout, &satoshis) in tx.vout.iter().zip(output_satoshis) {
            let script = vout.script_pub_key.as_ref();
            self.outputs.canonical.append(identity);
            self.outputs.tx_hash.append_value(tx_hash);
            self.outputs.block_height.append_value(block_height);
            self.outputs.output_index.append_value(vout.n);
            self.outputs.value.append_value(vout.value);
            self.outputs.value_sats.append_value(satoshis);
            self.outputs
                .script_pubkey_asm
                .append_option(script.map(|s| s.asm.as_str()));
            self.outputs
                .script_pubkey_hex
                .append_option(script.map(|s| s.hex.as_str()));
            self.outputs
                .script_pubkey_type
                .append_option(script.map(|s| s.r#type.as_str()));
            self.outputs
                .script_pubkey_address
                .append_option(script.and_then(|s| {
                    nonempty(&s.address).or_else(|| s.addresses.first().and_then(|a| nonempty(a)))
                }));
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
        // Preflight every output before appending any row, including the block
        // row. An invalid later transaction must leave all existing buffers intact.
        let output_satoshis = block
            .tx
            .iter()
            .enumerate()
            .map(|(tx_index, tx)| {
                transaction_satoshis(tx).map_err(|error| {
                    anyhow::anyhow!(
                        "invalid Bitcoin output in block {}, transaction {tx_index}: {error:#}",
                        block.height
                    )
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        // Canonical ids come from the block's own hex hashes.
        let identity = self.blocks.canonical.prepare_with_ids(
            identity,
            &decode_id_bytes(&block.hash),
            &decode_id_bytes(&block.previous_hash),
        )?;
        self.map_btc_block(&block, &output_satoshis, &identity, fork_step);
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
            + est_u32(&self.inputs.tx_index)
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
            + est_u64(&self.outputs.value_sats)
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
    tx_index: UInt32Builder,
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
            tx_index: UInt32Builder::new(),
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
            Arc::new(self.tx_index.finish()) as Arc<dyn Array>,
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
    value_sats: UInt64Builder,
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
            value_sats: UInt64Builder::new(),
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
            Arc::new(self.value_sats.finish()) as Arc<dyn Array>,
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
    use crate::bitcoin::amounts::value_satoshis;

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
    fn satoshi_conversion_preserves_valid_amounts_and_rejects_ambiguous_values() {
        let amounts = [
            0,
            1,
            3,
            29,
            100_000,
            10_000_001,
            123_456_789,
            5_000_000_000,
            1_000_000_000_000_001,
            2_099_999_999_999_999,
            2_100_000_000_000_000,
        ];
        for satoshis in amounts {
            let btc = satoshis as f64 / 100_000_000.0;
            assert_eq!(value_satoshis(btc).unwrap(), satoshis);
        }
        // Spread across the whole money range, including large f64 ULPs.
        let mut sample = 17_u64;
        for _ in 0..10_000 {
            sample = sample.wrapping_mul(6364136223846793005).wrapping_add(1);
            let satoshis = sample % 2_100_000_000_000_001;
            assert_eq!(
                value_satoshis(satoshis as f64 / 100_000_000.0).unwrap(),
                satoshis
            );
        }
        for invalid in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -1.0,
            100_000_000.0,
            0.000000005,
            1.000000001,
            f64::from_bits(0.01_f64.to_bits() + 1),
        ] {
            assert!(value_satoshis(invalid).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn invalid_later_output_never_partially_appends_a_block() {
        let valid = make_test_block(1);
        for value in [f64::NAN, -0.1, 0.000000005, 100_000_000.0] {
            let mut invalid = make_test_block(2);
            let mut later = invalid.tx[0].clone();
            later.vout[0].value = value;
            invalid.tx.push(later);
            let mut mapper = BitcoinBlockMapper::new(false, EncodeBytes::Hex);
            mapper
                .map_block(&valid.encode_to_vec(), &BlockIdentity::default(), None)
                .unwrap();
            let before = mapper.total_rows();
            let error = mapper
                .map_block(&invalid.encode_to_vec(), &BlockIdentity::default(), None)
                .unwrap_err();
            assert!(error.to_string().contains("transaction 1: output 0"));
            assert_eq!(mapper.total_rows(), before);
            let batches = mapper.flush().unwrap();
            for batch in batches.values() {
                assert_eq!(batch.num_rows(), 1);
            }
        }
    }

    #[test]
    fn bitcoin_satoshis_input_joins_nulls_and_legacy_addresses() {
        let mut block = make_test_block(1);
        let mut tx = block.tx[0].clone();
        tx.txid = "ab".repeat(32);
        tx.vin = vec![
            btc::Vin {
                txid: "cd".repeat(32),
                vout: 0,
                script_sig: None,
                sequence: 7,
                ..Default::default()
            },
            btc::Vin {
                txid: "ef".repeat(32),
                vout: 2,
                script_sig: Some(btc::ScriptSig::default()),
                ..Default::default()
            },
            btc::Vin::default(),
        ];
        tx.vout = vec![
            btc::Vout {
                value: 1.1,
                n: 0,
                script_pub_key: Some(btc::ScriptPubKey {
                    address: "modern".into(),
                    addresses: vec!["legacy-ignored".into()],
                    ..Default::default()
                }),
            },
            btc::Vout {
                value: 0.00000001,
                n: 1,
                script_pub_key: Some(btc::ScriptPubKey {
                    addresses: vec!["legacy-first".into(), "legacy-second".into()],
                    ..Default::default()
                }),
            },
            btc::Vout {
                value: 0.3,
                n: 2,
                script_pub_key: Some(btc::ScriptPubKey::default()),
            },
            btc::Vout {
                value: 0.0,
                n: 3,
                script_pub_key: None,
            },
        ];
        block.tx.push(tx);
        for encoding in [EncodeBytes::Hex, EncodeBytes::Binary] {
            for fork_step in [false, true] {
                let mut mapper = BitcoinBlockMapper::new(fork_step, encoding.clone());
                for _ in 0..2 {
                    mapper
                        .map_block(
                            &block.encode_to_vec(),
                            &BlockIdentity::default(),
                            fork_step.then_some("FINAL"),
                        )
                        .unwrap();
                    let batches = mapper.flush().unwrap();
                    let input = &batches["inputs"];
                    let column = |name| input.column_by_name(name).unwrap();
                    assert_eq!(
                        column("tx_index")
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .values(),
                        &[0, 1, 1, 1]
                    );
                    for name in ["prev_txid", "prev_vout", "script_sig_asm", "script_sig_hex"] {
                        assert!(column(name).is_null(0), "coinbase {name}");
                        assert!(input.schema().field_with_name(name).unwrap().is_nullable());
                    }
                    let prev_vout = column("prev_vout")
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap();
                    assert!(!prev_vout.is_null(1));
                    assert_eq!(prev_vout.value(1), 0); // A real output zero stays zero.
                    assert_eq!(prev_vout.value(2), 2);
                    assert!(prev_vout.is_null(3)); // Missing previous tx identity stays unknown.
                    assert!(column("script_sig_hex").is_null(1));
                    assert!(!column("script_sig_hex").is_null(2)); // Present empty SegWit script.
                    assert_eq!(
                        column("script_sig_hex")
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(2),
                        ""
                    );
                    assert!(!column("coinbase").is_null(0));
                    for row in 1..4 {
                        assert!(column("coinbase").is_null(row));
                    }
                    // Native protobuf strings retain Bitcoin Core display order.
                    assert_eq!(
                        column("prev_txid")
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(1),
                        "cd".repeat(32)
                    );
                    let output = &batches["outputs"];
                    let sats = output
                        .column_by_name("value_sats")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    assert_eq!(
                        sats.values(),
                        &[5_000_000_000, 110_000_000, 1, 30_000_000, 0]
                    );
                    let addresses = output
                        .column_by_name("script_pubkey_address")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assert_eq!(addresses.value(1), "modern");
                    assert_eq!(addresses.value(2), "legacy-first");
                    assert!(addresses.is_null(3) && addresses.is_null(4));
                    for name in [
                        "script_pubkey_asm",
                        "script_pubkey_hex",
                        "script_pubkey_type",
                    ] {
                        let col = output.column_by_name(name).unwrap();
                        assert!(!col.is_null(3));
                        assert!(col.is_null(4));
                    }
                    assert_eq!(input.column_by_name("fork_step").is_some(), fork_step);
                    assert_eq!(output.column_by_name("fork_step").is_some(), fork_step);
                    assert_eq!(mapper.total_rows(), 0);
                }
            }
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
