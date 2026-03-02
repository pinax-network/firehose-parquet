use arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder,
    ListBuilder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use crate::encode::{bytes_data_type, BytesColumn, EncodeBytes};

// ---------------------------------------------------------------------------
// Arrow builder memory estimation helpers
// ---------------------------------------------------------------------------

/// Estimate memory usage of a `UInt64Builder`.
pub fn est_u64(b: &UInt64Builder) -> usize {
    b.len() * 8
}

/// Estimate memory usage of a `UInt32Builder`.
pub fn est_u32(b: &UInt32Builder) -> usize {
    b.len() * 4
}

/// Estimate memory usage of an `Int64Builder`.
pub fn est_i64(b: &Int64Builder) -> usize {
    b.len() * 8
}

/// Estimate memory usage of an `Int32Builder`.
pub fn est_i32(b: &Int32Builder) -> usize {
    b.len() * 4
}

/// Estimate memory usage of a `Float64Builder`.
pub fn est_f64(b: &Float64Builder) -> usize {
    b.len() * 8
}

/// Estimate memory usage of a `BooleanBuilder`.
pub fn est_bool(b: &BooleanBuilder) -> usize {
    (b.len() + 7) / 8
}

/// Estimate memory usage of a `StringBuilder` (offsets + values).
pub fn est_str(b: &StringBuilder) -> usize {
    b.values_slice().len() + (b.len() + 1) * 4
}

/// Estimate memory usage of an `Option<StringBuilder>`.
pub fn est_opt_str(b: &Option<StringBuilder>) -> usize {
    b.as_ref().map_or(0, est_str)
}

/// Estimate memory usage of a `BinaryBuilder` (offsets + values).
pub fn est_bin(b: &BinaryBuilder) -> usize {
    b.values_slice().len() + (b.len() + 1) * 4
}

/// Estimate memory usage of a `ListBuilder<StringBuilder>` (offsets + inner).
pub fn est_list_str(b: &mut ListBuilder<StringBuilder>) -> usize {
    (b.len() + 1) * 4 + est_str(b.values())
}

/// Estimate memory usage of a `ListBuilder<UInt64Builder>` (offsets + inner).
pub fn est_list_u64(b: &mut ListBuilder<UInt64Builder>) -> usize {
    (b.len() + 1) * 4 + est_u64(b.values())
}

/// Estimate memory usage of a `ListBuilder<BinaryBuilder>` (offsets + inner).
pub fn est_list_bin(b: &mut ListBuilder<BinaryBuilder>) -> usize {
    (b.len() + 1) * 4 + est_bin(b.values())
}

/// Canonical block identity from Firehose BlockMetadata.
/// Added to every output table for chain-agnostic identification.
#[derive(Debug, Clone, Default)]
pub struct BlockIdentity {
    pub block_num: u64,
    pub block_id: String,
    pub parent_num: u64,
    pub parent_id: String,
    pub lib_num: u64,
    pub timestamp: i64, // unix seconds
    /// Fork step: None when final_blocks_only=true, Some("NEW"/"UNDO"/"FINAL") otherwise.
    pub fork_step: Option<String>,
}

/// Returns the 6 canonical identity fields to prepend to every schema.
/// Uses the given encoding to determine the data type of block_id/parent_id.
pub fn canonical_fields_with_encoding(encoding: &EncodeBytes) -> Vec<Field> {
    let id_type = bytes_data_type(encoding);
    vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new("block_id", id_type.clone(), false),
        Field::new("parent_num", DataType::UInt64, false),
        Field::new("parent_id", id_type, false),
        Field::new("lib_num", DataType::UInt64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]
}

/// Returns the 6 canonical identity fields (Utf8 block_id/parent_id).
/// Use `canonical_fields_with_encoding` when encoding matters.
pub fn canonical_fields() -> Vec<Field> {
    canonical_fields_with_encoding(&EncodeBytes::Hex)
}

/// Decode a hex block ID string to raw bytes.
pub fn decode_id_bytes(id: &str) -> Vec<u8> {
    let hex_str = id.strip_prefix("0x").unwrap_or(id);
    hex::decode(hex_str).unwrap_or_else(|_| id.as_bytes().to_vec())
}

/// Builder for canonical identity columns. Embed in each table builder.
pub struct CanonicalBuilder {
    pub block_num: UInt64Builder,
    block_id: BytesColumn,
    pub parent_num: UInt64Builder,
    parent_id: BytesColumn,
    pub lib_num: UInt64Builder,
    pub timestamp: Int64Builder,
}

impl CanonicalBuilder {
    pub fn new() -> Self {
        Self::with_encoding(&EncodeBytes::Hex)
    }

    /// Create a builder that encodes block_id/parent_id with the given strategy.
    pub fn with_encoding(encoding: &EncodeBytes) -> Self {
        Self {
            block_num: UInt64Builder::new(),
            block_id: BytesColumn::new(encoding),
            parent_num: UInt64Builder::new(),
            parent_id: BytesColumn::new(encoding),
            lib_num: UInt64Builder::new(),
            timestamp: Int64Builder::new(),
        }
    }

    pub fn append(&mut self, id: &BlockIdentity) {
        self.block_num.append_value(id.block_num);
        self.parent_num.append_value(id.parent_num);
        self.lib_num.append_value(id.lib_num);
        self.timestamp.append_value(id.timestamp);

        // Decode hex ID strings to bytes, then encode through BytesColumn.
        let block_id_bytes = decode_id_bytes(&id.block_id);
        let parent_id_bytes = decode_id_bytes(&id.parent_id);
        self.block_id.append_value(&block_id_bytes);
        self.parent_id.append_value(&parent_id_bytes);
    }

    pub fn finish(&mut self) -> Vec<Arc<dyn arrow::array::Array>> {
        vec![
            Arc::new(self.block_num.finish()),
            self.block_id.finish(),
            Arc::new(self.parent_num.finish()),
            self.parent_id.finish(),
            Arc::new(self.lib_num.finish()),
            Arc::new(self.timestamp.finish()),
        ]
    }

    pub fn len(&self) -> usize {
        self.block_num.len()
    }

    /// Estimate in-memory byte usage of all canonical columns.
    pub fn estimated_bytes(&self) -> usize {
        est_u64(&self.block_num)
            + self.block_id.estimated_bytes()
            + est_u64(&self.parent_num)
            + self.parent_id.estimated_bytes()
            + est_u64(&self.lib_num)
            + est_i64(&self.timestamp)
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

    /// Get total rows across all tables (sum).
    fn total_rows(&self) -> usize;

    /// Return the name and estimated in-memory byte size of the largest table.
    ///
    /// The size is the **maximum** across all per-table estimates so that
    /// `flush_bytes` controls the size of the biggest output file rather
    /// than the sum across all tables (which would produce many small files
    /// after per-table splitting and compression).
    fn largest_table(&mut self) -> (&str, usize);

    /// Estimate the in-memory byte usage of the largest single table.
    fn estimated_bytes(&mut self) -> usize {
        self.largest_table().1
    }

    /// Get table names.
    fn table_names(&self) -> Vec<&str>;
}
