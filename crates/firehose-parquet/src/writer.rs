use crate::config::{Compression, Partition};
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use tracing::info;

/// Writes Arrow RecordBatches to Parquet files, handling partitioning and
/// file naming.
pub struct ParquetTableWriter {
    output_dir: PathBuf,
    partition: Partition,
    compression: Compression,
    /// Part counter per logical partition key (table + partition value).
    part_counters: HashMap<String, u32>,
}

impl ParquetTableWriter {
    pub fn new(output_dir: impl Into<PathBuf>, partition: Partition, compression: Compression) -> Self {
        Self {
            output_dir: output_dir.into(),
            partition,
            compression,
            part_counters: HashMap::new(),
        }
    }

    /// Write a single RecordBatch for `table` to a new Parquet part file.
    ///
    /// `slot_range` is `(min_slot, max_slot)` of the data in the batch and is
    /// used for block-range partitioning.
    pub fn write_batch(
        &mut self,
        table: &str,
        batch: &RecordBatch,
        slot_range: Option<(u64, u64)>,
    ) -> Result<PathBuf> {
        if batch.num_rows() == 0 {
            // Nothing to write; return a dummy path.
            let dir = self.output_dir.join(table);
            fs::create_dir_all(&dir)?;
            return Ok(dir);
        }

        let dir = self.partition_dir(table, slot_range);
        fs::create_dir_all(&dir).with_context(|| format!("creating dir {}", dir.display()))?;

        let counter_key = dir.to_string_lossy().to_string();
        let counter = self.part_counters.entry(counter_key).or_insert(0);
        *counter += 1;
        let path = dir.join(format!("part-{:06}.parquet", counter));

        let file =
            File::create(&path).with_context(|| format!("creating file {}", path.display()))?;
        let props = self.writer_properties();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;

        info!(
            table,
            path = %path.display(),
            rows = batch.num_rows(),
            "wrote parquet part"
        );
        Ok(path)
    }

    fn partition_dir(&self, table: &str, slot_range: Option<(u64, u64)>) -> PathBuf {
        let base = self.output_dir.join(table);
        match &self.partition {
            Partition::None => base,
            Partition::BlockRange(size) => {
                if let Some((min_slot, _)) = slot_range {
                    let start = (min_slot / size) * size;
                    let end = start + size - 1;
                    base.join(format!("block_range={start}-{end}"))
                } else {
                    base
                }
            }
        }
    }

    fn writer_properties(&self) -> WriterProperties {
        let compression = match self.compression {
            Compression::None => PqCompression::UNCOMPRESSED,
            Compression::Snappy => PqCompression::SNAPPY,
            Compression::Gzip => PqCompression::GZIP(Default::default()),
            Compression::Zstd => PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        };
        WriterProperties::builder()
            .set_compression(compression)
            .build()
    }
}

/// High-level writer that manages one [`ParquetTableWriter`] per output table.
pub struct OutputWriter {
    pub inner: ParquetTableWriter,
}

impl OutputWriter {
    pub fn new(output_dir: impl Into<PathBuf>, partition: Partition, compression: Compression) -> Self {
        Self {
            inner: ParquetTableWriter::new(output_dir, partition, compression),
        }
    }

    /// Write all five table batches produced by [`crate::mapper::BlockMapper::flush`].
    pub fn write_all(
        &mut self,
        batches: &crate::mapper::TableBatches,
        slot_range: Option<(u64, u64)>,
    ) -> Result<()> {
        self.inner
            .write_batch("blocks", &batches.blocks, slot_range)?;
        self.inner
            .write_batch("transactions", &batches.transactions, slot_range)?;
        self.inner
            .write_batch("messages", &batches.messages, slot_range)?;
        self.inner
            .write_batch("instructions", &batches.instructions, slot_range)?;
        self.inner
            .write_batch("rewards", &batches.rewards, slot_range)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers for reading parquet files back (used in tests)
// ---------------------------------------------------------------------------

/// Read a Parquet file and return all RecordBatches.
pub fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batches)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Compression, Partition};
    use crate::mapper::BlockMapper;
    use crate::solana;

    fn make_test_block(slot: u64) -> solana::Block {
        solana::Block {
            slot,
            parent_slot: slot.saturating_sub(1),
            blockhash: format!("hash_{slot}"),
            previous_blockhash: format!("hash_{}", slot.saturating_sub(1)),
            block_height: Some(solana::BlockHeight {
                block_height: slot,
            }),
            block_time: Some(solana::UnixTimestamp {
                timestamp: 1_700_000_000 + slot as i64,
            }),
            transactions: vec![solana::ConfirmedTransaction {
                transaction: Some(solana::Transaction {
                    signatures: vec![vec![1u8; 64]],
                    message: Some(solana::Message {
                        header: Some(solana::MessageHeader {
                            num_required_signatures: 1,
                            num_readonly_signed_accounts: 0,
                            num_readonly_unsigned_accounts: 1,
                        }),
                        account_keys: vec![vec![2u8; 32]],
                        recent_blockhash: vec![4u8; 32],
                        instructions: vec![solana::CompiledInstruction {
                            program_id_index: 0,
                            accounts: vec![0],
                            data: vec![5, 6],
                        }],
                        versioned: false,
                        address_table_lookups: vec![],
                    }),
                }),
                meta: Some(solana::TransactionStatusMeta {
                    err: None,
                    fee: 5000,
                    pre_balances: vec![100_000],
                    post_balances: vec![95_000],
                    inner_instructions: vec![],
                    log_messages: vec!["hello".into()],
                    pre_token_balances: vec![],
                    post_token_balances: vec![],
                    rewards: vec![],
                    loaded_writable_addresses: vec![],
                    loaded_readonly_addresses: vec![],
                    return_data: None,
                    compute_units_consumed: Some(100),
                }),
            }],
            rewards: vec![solana::Reward {
                pubkey: "abc".into(),
                lamports: 10,
                post_balance: 500,
                reward_type: 1,
                commission: String::new(),
            }],
        }
    }

    #[test]
    fn test_parquet_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut mapper = BlockMapper::new();
        mapper.map_block(&make_test_block(42));
        let batches = mapper.flush().unwrap();

        let mut writer =
            ParquetTableWriter::new(dir.path(), Partition::None, Compression::Snappy);
        let path = writer
            .write_batch("blocks", &batches.blocks, None)
            .unwrap();

        // Read back
        let read_batches = read_parquet(&path).unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 1);
        assert_eq!(read_batches[0].schema(), batches.blocks.schema());
    }

    #[test]
    fn test_block_range_partitioning() {
        let dir = tempfile::tempdir().unwrap();
        let mut mapper = BlockMapper::new();
        mapper.map_block(&make_test_block(150));
        let batches = mapper.flush().unwrap();

        let mut writer =
            ParquetTableWriter::new(dir.path(), Partition::BlockRange(100), Compression::None);
        let path = writer
            .write_batch("blocks", &batches.blocks, Some((150, 150)))
            .unwrap();

        assert!(path.to_string_lossy().contains("block_range=100-199"));
    }

    #[test]
    fn test_output_writer_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let mut mapper = BlockMapper::new();
        mapper.map_block(&make_test_block(1));
        let batches = mapper.flush().unwrap();

        let mut out = OutputWriter::new(dir.path(), Partition::None, Compression::Zstd);
        out.write_all(&batches, None).unwrap();

        // Verify all table directories exist
        for table in &["blocks", "transactions", "messages", "instructions", "rewards"] {
            let table_dir = dir.path().join(table);
            assert!(table_dir.exists(), "missing dir for {table}");
        }
    }
}
