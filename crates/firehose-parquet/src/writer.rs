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
    /// `block_range` is `(min_block, max_block)` of the data in the batch and is
    /// used for block-range partitioning.
    pub fn write_batch(
        &mut self,
        table: &str,
        batch: &RecordBatch,
        block_range: Option<(u64, u64)>,
    ) -> Result<PathBuf> {
        if batch.num_rows() == 0 {
            let dir = self.output_dir.join(table);
            fs::create_dir_all(&dir)?;
            return Ok(dir);
        }

        let dir = self.partition_dir(table, block_range);
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

    fn partition_dir(&self, table: &str, block_range: Option<(u64, u64)>) -> PathBuf {
        let base = self.output_dir.join(table);
        match &self.partition {
            Partition::None => base,
            Partition::BlockRange(size) => {
                if let Some((min_block, _)) = block_range {
                    let start = (min_block / size) * size;
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

/// High-level writer that accepts a generic HashMap<String, RecordBatch>
/// produced by any BlockMapper implementation.
pub struct OutputWriter {
    pub inner: ParquetTableWriter,
}

impl OutputWriter {
    pub fn new(output_dir: impl Into<PathBuf>, partition: Partition, compression: Compression) -> Self {
        Self {
            inner: ParquetTableWriter::new(output_dir, partition, compression),
        }
    }

    /// Write all table batches produced by a BlockMapper::flush().
    pub fn write_all(
        &mut self,
        batches: &HashMap<String, RecordBatch>,
        block_range: Option<(u64, u64)>,
    ) -> Result<()> {
        for (table, batch) in batches {
            self.inner.write_batch(table, batch, block_range)?;
        }
        Ok(())
    }
}

/// Read a Parquet file and return all RecordBatches.
pub fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Compression, Partition};
    use arrow::array::UInt64Builder;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
        ]));
        let mut builder = UInt64Builder::new();
        builder.append_value(42);
        RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap()
    }

    #[test]
    fn test_parquet_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer =
            ParquetTableWriter::new(dir.path(), Partition::None, Compression::Snappy);
        let path = writer.write_batch("blocks", &batch, None).unwrap();

        let read_batches = read_parquet(&path).unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 1);
    }

    #[test]
    fn test_block_range_partitioning() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut writer =
            ParquetTableWriter::new(dir.path(), Partition::BlockRange(100), Compression::None);
        let path = writer
            .write_batch("blocks", &batch, Some((150, 150)))
            .unwrap();
        assert!(path.to_string_lossy().contains("block_range=100-199"));
    }

    #[test]
    fn test_output_writer_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_test_batch();
        let mut batches = HashMap::new();
        batches.insert("blocks".to_string(), batch);

        let mut out = OutputWriter::new(dir.path(), Partition::None, Compression::Zstd);
        out.write_all(&batches, None).unwrap();
        assert!(dir.path().join("blocks").exists());
    }
}
