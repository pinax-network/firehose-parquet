use arrow::array::{ArrayBuilder, Int64Builder, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// Canonical block identity from Firehose BlockMetadata.
/// Added to every output table for chain-agnostic identification.
#[derive(Debug, Clone, Default)]
pub struct BlockIdentity {
    pub block_num: u64,
    pub block_id: String,
    pub parent_num: u64,
    pub parent_id: String,
    pub lib_num: u64,
    pub timestamp: Option<i64>, // unix seconds
    /// Fork step: None when final_blocks_only=true, Some("NEW"/"UNDO"/"FINAL") otherwise.
    pub fork_step: Option<String>,
}

/// Returns the 6 canonical identity fields to prepend to every schema.
pub fn canonical_fields() -> Vec<Field> {
    vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new("block_id", DataType::Utf8, false),
        Field::new("parent_num", DataType::UInt64, false),
        Field::new("parent_id", DataType::Utf8, false),
        Field::new("lib_num", DataType::UInt64, false),
        Field::new("timestamp", DataType::Int64, true),
    ]
}

/// Builder for canonical identity columns. Embed in each table builder.
pub struct CanonicalBuilder {
    pub block_num: UInt64Builder,
    pub block_id: StringBuilder,
    pub parent_num: UInt64Builder,
    pub parent_id: StringBuilder,
    pub lib_num: UInt64Builder,
    pub timestamp: Int64Builder,
}

impl CanonicalBuilder {
    pub fn new() -> Self {
        Self {
            block_num: UInt64Builder::new(),
            block_id: StringBuilder::new(),
            parent_num: UInt64Builder::new(),
            parent_id: StringBuilder::new(),
            lib_num: UInt64Builder::new(),
            timestamp: Int64Builder::new(),
        }
    }

    pub fn append(&mut self, id: &BlockIdentity) {
        self.block_num.append_value(id.block_num);
        self.block_id.append_value(&id.block_id);
        self.parent_num.append_value(id.parent_num);
        self.parent_id.append_value(&id.parent_id);
        self.lib_num.append_value(id.lib_num);
        match id.timestamp {
            Some(ts) => self.timestamp.append_value(ts),
            None => self.timestamp.append_null(),
        }
    }

    pub fn finish(&mut self) -> Vec<Arc<dyn arrow::array::Array>> {
        vec![
            Arc::new(self.block_num.finish()),
            Arc::new(self.block_id.finish()),
            Arc::new(self.parent_num.finish()),
            Arc::new(self.parent_id.finish()),
            Arc::new(self.lib_num.finish()),
            Arc::new(self.timestamp.finish()),
        ]
    }

    pub fn len(&self) -> usize {
        self.block_num.len()
    }
}

/// Convert a Firehose ForkStep integer to a human-readable string.
pub fn fork_step_name(step: i32) -> Option<&'static str> {
    match step {
        1 => Some("NEW"),
        2 => Some("UNDO"),
        3 => Some("FINAL"),
        _ => None,
    }
}

/// Returns the fork_step field definition.
pub fn fork_step_field() -> Field {
    Field::new("fork_step", DataType::Utf8, false)
}

/// Builder for the fork_step column (a simple StringBuilder wrapper).
pub struct ForkStepBuilder {
    inner: StringBuilder,
}

impl ForkStepBuilder {
    pub fn new() -> Self {
        Self {
            inner: StringBuilder::new(),
        }
    }

    pub fn append(&mut self, value: &str) {
        self.inner.append_value(value);
    }

    pub fn finish(&mut self) -> Arc<dyn arrow::array::Array> {
        Arc::new(self.inner.finish())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Trait for mapping raw protobuf block bytes into Arrow RecordBatches.
pub trait BlockMapper {
    /// Map raw protobuf bytes (from Any.value) into internal builders.
    fn map_block(&mut self, block_bytes: &[u8], identity: &BlockIdentity, fork_step: Option<&str>) -> anyhow::Result<()>;

    /// Flush all buffered data into RecordBatches.
    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>>;

    /// Get current max rows across all tables.
    fn max_table_rows(&self) -> usize;

    /// Get table names.
    fn table_names(&self) -> Vec<&str>;
}
