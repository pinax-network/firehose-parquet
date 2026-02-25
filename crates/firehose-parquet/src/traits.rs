use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// Trait for mapping raw protobuf block bytes into Arrow RecordBatches.
/// Implement this for each chain type (Solana, EVM, etc.).
pub trait BlockMapper {
    /// Map raw protobuf bytes (from Any.value) into internal builders.
    fn map_block(&mut self, block_bytes: &[u8]) -> anyhow::Result<()>;

    /// Flush all buffered data into RecordBatches.
    /// Returns a map of table_name -> RecordBatch.
    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>>;

    /// Get current max rows across all tables.
    fn max_table_rows(&self) -> usize;

    /// Get table names.
    fn table_names(&self) -> Vec<&str>;
}
