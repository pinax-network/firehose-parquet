//! Arrow output for transfer projections. Extraction is kept in `transfers`.

use super::decimal;
use super::proto::eth;
use super::transfers::{Erc20Transfer, NativeTransfer, DECODER_VERSION};
use arrow::array::{
    Array, ArrayBuilder, StringBuilder, StringDictionaryBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, EncodeBytes};
use firehose_parquet::traits::{
    append_fork_step, est_fork_step, est_str, est_u32, est_u64, estimated_dictionary_index_bytes,
    finish_fork_step, fork_step_builder, CanonicalBuilder, ForkStepBuilder, PreparedIdentity,
    StreamEvent,
};
use std::sync::Arc;

pub(super) struct Erc20TransfersBuilder {
    pub(super) canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    log_index: UInt32Builder,
    block_index: UInt32Builder,
    ordinal: UInt64Builder,
    token_address: BytesColumn,
    from: BytesColumn,
    to: BytesColumn,
    value: StringBuilder,
    decoder_version: UInt32Builder,
    fork_step: Option<ForkStepBuilder>,
}

impl Erc20TransfersBuilder {
    pub(super) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            log_index: UInt32Builder::new(),
            block_index: UInt32Builder::new(),
            ordinal: UInt64Builder::new(),
            token_address: BytesColumn::new(encoding),
            from: BytesColumn::new(encoding),
            to: BytesColumn::new(encoding),
            value: StringBuilder::new(),
            decoder_version: UInt32Builder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    pub(super) fn append(
        &mut self,
        block_number: u64,
        tx: &eth::TransactionTrace,
        transfer: Erc20Transfer<'_>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(&tx.hash);
        self.tx_index.append_value(tx.index);
        self.log_index.append_value(transfer.log_index);
        self.block_index.append_value(transfer.block_index);
        self.ordinal.append_value(transfer.ordinal);
        self.token_address.append_value(transfer.token_address);
        self.from.append_value(&transfer.from);
        self.to.append_value(&transfer.to);
        self.value.append_value(&transfer.value);
        self.decoder_version.append_value(DECODER_VERSION);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    pub(super) fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.block_number)
            + self.tx_hash.estimated_bytes()
            + est_u32(&self.tx_index)
            + est_u32(&self.log_index)
            + est_u32(&self.block_index)
            + est_u64(&self.ordinal)
            + self.token_address.estimated_bytes()
            + self.from.estimated_bytes()
            + self.to.estimated_bytes()
            + est_str(&self.value)
            + est_u32(&self.decoder_version)
            + est_fork_step(&self.fork_step)
    }

    pub(super) fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.log_index.finish()),
            Arc::new(self.block_index.finish()),
            Arc::new(self.ordinal.finish()),
            self.token_address.finish(),
            self.from.finish(),
            self.to.finish(),
            Arc::new(self.value.finish()),
            Arc::new(self.decoder_version.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

pub(super) struct NativeTransfersBuilder {
    pub(super) canonical: CanonicalBuilder,
    block_number: UInt64Builder,
    tx_hash: BytesColumn,
    tx_index: UInt32Builder,
    call_index: UInt32Builder,
    call_type: StringDictionaryBuilder<Int32Type>,
    ordinal: UInt64Builder,
    from: BytesColumn,
    to: BytesColumn,
    value: StringBuilder,
    decoder_version: UInt32Builder,
    fork_step: Option<ForkStepBuilder>,
}

impl NativeTransfersBuilder {
    pub(super) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_number: UInt64Builder::new(),
            tx_hash: BytesColumn::new(encoding),
            tx_index: UInt32Builder::new(),
            call_index: UInt32Builder::new(),
            call_type: StringDictionaryBuilder::new(),
            ordinal: UInt64Builder::new(),
            from: BytesColumn::new(encoding),
            to: BytesColumn::new(encoding),
            value: StringBuilder::new(),
            decoder_version: UInt32Builder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    pub(super) fn append(
        &mut self,
        block_number: u64,
        tx: &eth::TransactionTrace,
        transfer: NativeTransfer<'_>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.canonical.append(identity);
        self.block_number.append_value(block_number);
        self.tx_hash.append_value(&tx.hash);
        self.tx_index.append_value(tx.index);
        self.call_index.append_value(transfer.call_index);
        self.call_type
            .append_value(transfer.call_type.as_str_name());
        self.ordinal.append_value(transfer.ordinal);
        self.from.append_value(transfer.from);
        self.to.append_value(transfer.to);
        decimal::append_decimal(&mut self.value, transfer.value);
        self.decoder_version.append_value(DECODER_VERSION);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    pub(super) fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.block_number)
            + self.tx_hash.estimated_bytes()
            + est_u32(&self.tx_index)
            + est_u32(&self.call_index)
            + estimated_dictionary_index_bytes(self.call_type.len())
            + est_u64(&self.ordinal)
            + self.from.estimated_bytes()
            + self.to.estimated_bytes()
            + est_str(&self.value)
            + est_u32(&self.decoder_version)
            + est_fork_step(&self.fork_step)
    }

    pub(super) fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_number.finish()) as Arc<dyn Array>,
            self.tx_hash.finish(),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.call_index.finish()),
            Arc::new(self.call_type.finish()),
            Arc::new(self.ordinal.finish()),
            self.from.finish(),
            self.to.finish(),
            Arc::new(self.value.finish()),
            Arc::new(self.decoder_version.finish()),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}
