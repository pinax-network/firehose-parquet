use std::fs;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

use arrow::array::{Array, BooleanArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// The filename used for the cursor parquet file.
pub const CURSOR_PARQUET_FILENAME: &str = "cursor.parquet";

/// Cursor state and pipeline configuration stored as a single-row Parquet file.
#[derive(Debug, Clone, Default)]
pub struct CursorState {
    // -- Cursor state --
    pub cursor: String,
    pub last_block_num: u64,
    pub last_block_id: String,
    pub updated_at: String,

    // -- Pipeline parameters --
    pub endpoint: String,
    pub start_block: Option<u64>,
    pub stop_block: Option<u64>,
    pub partition: String,
    pub block_range_size: u64,
    pub compression: String,
    pub flush_bytes: u64,
    pub flush_rows: Option<u64>,
    pub bytes_encoding: String,
    pub extended: bool,
    pub final_blocks_only: bool,

    // -- Firehose metadata (from InfoResponse) --
    pub chain_name: String,
    pub chain_name_aliases: String,
    pub first_streamable_block_num: u64,
    pub first_streamable_block_id: String,
    pub block_id_encoding: String,
    pub block_features: String,
    pub firehose_parquet_version: String,
}

/// Build the Arrow schema for the cursor.parquet file.
fn cursor_schema() -> Schema {
    Schema::new(vec![
        // Cursor state
        Field::new("cursor", DataType::Utf8, false),
        Field::new("last_block_num", DataType::UInt64, false),
        Field::new("last_block_id", DataType::Utf8, false),
        Field::new("updated_at", DataType::Utf8, false),
        // Pipeline parameters
        Field::new("endpoint", DataType::Utf8, false),
        Field::new("start_block", DataType::UInt64, true),
        Field::new("stop_block", DataType::UInt64, true),
        Field::new("partition", DataType::Utf8, false),
        Field::new("block_range_size", DataType::UInt64, false),
        Field::new("compression", DataType::Utf8, false),
        Field::new("flush_bytes", DataType::UInt64, false),
        Field::new("flush_rows", DataType::UInt64, true),
        Field::new("bytes_encoding", DataType::Utf8, false),
        Field::new("extended", DataType::Boolean, false),
        Field::new("final_blocks_only", DataType::Boolean, false),
        // Firehose metadata
        Field::new("chain_name", DataType::Utf8, false),
        Field::new("chain_name_aliases", DataType::Utf8, false),
        Field::new("first_streamable_block_num", DataType::UInt64, false),
        Field::new("first_streamable_block_id", DataType::Utf8, false),
        Field::new("block_id_encoding", DataType::Utf8, false),
        Field::new("block_features", DataType::Utf8, false),
        Field::new("firehose_parquet_version", DataType::Utf8, false),
    ])
}

impl CursorState {
    /// Convert this state into a single-row RecordBatch.
    fn to_record_batch(&self) -> anyhow::Result<RecordBatch> {
        let schema = Arc::new(cursor_schema());
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![self.cursor.as_str()])),
                Arc::new(UInt64Array::from(vec![self.last_block_num])),
                Arc::new(StringArray::from(vec![self.last_block_id.as_str()])),
                Arc::new(StringArray::from(vec![self.updated_at.as_str()])),
                Arc::new(StringArray::from(vec![self.endpoint.as_str()])),
                Arc::new(UInt64Array::from(vec![self.start_block])),
                Arc::new(UInt64Array::from(vec![self.stop_block])),
                Arc::new(StringArray::from(vec![self.partition.as_str()])),
                Arc::new(UInt64Array::from(vec![self.block_range_size])),
                Arc::new(StringArray::from(vec![self.compression.as_str()])),
                Arc::new(UInt64Array::from(vec![self.flush_bytes])),
                Arc::new(UInt64Array::from(vec![self.flush_rows])),
                Arc::new(StringArray::from(vec![self.bytes_encoding.as_str()])),
                Arc::new(BooleanArray::from(vec![self.extended])),
                Arc::new(BooleanArray::from(vec![self.final_blocks_only])),
                Arc::new(StringArray::from(vec![self.chain_name.as_str()])),
                Arc::new(StringArray::from(vec![self.chain_name_aliases.as_str()])),
                Arc::new(UInt64Array::from(vec![self.first_streamable_block_num])),
                Arc::new(StringArray::from(vec![self.first_streamable_block_id.as_str()])),
                Arc::new(StringArray::from(vec![self.block_id_encoding.as_str()])),
                Arc::new(StringArray::from(vec![self.block_features.as_str()])),
                Arc::new(StringArray::from(vec![self.firehose_parquet_version.as_str()])),
            ],
        )?;
        Ok(batch)
    }

    /// Read a `CursorState` from a single-row RecordBatch.
    fn from_record_batch(batch: &RecordBatch) -> anyhow::Result<Self> {
        use arrow::array::AsArray;

        if batch.num_rows() == 0 {
            anyhow::bail!("cursor.parquet contains no rows");
        }

        let get_str = |name: &str| -> String {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_string_opt::<i32>())
                .and_then(|a| if a.is_null(0) { None } else { Some(a.value(0).to_string()) })
                .unwrap_or_default()
        };
        let get_u64 = |name: &str| -> u64 {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_primitive_opt::<arrow::datatypes::UInt64Type>())
                .and_then(|a| if a.is_null(0) { None } else { Some(a.value(0)) })
                .unwrap_or(0)
        };
        let get_opt_u64 = |name: &str| -> Option<u64> {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_primitive_opt::<arrow::datatypes::UInt64Type>())
                .and_then(|a| if a.is_null(0) { None } else { Some(a.value(0)) })
        };
        let get_bool = |name: &str| -> bool {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_boolean_opt())
                .and_then(|a| if a.is_null(0) { None } else { Some(a.value(0)) })
                .unwrap_or(false)
        };

        Ok(CursorState {
            cursor: get_str("cursor"),
            last_block_num: get_u64("last_block_num"),
            last_block_id: get_str("last_block_id"),
            updated_at: get_str("updated_at"),
            endpoint: get_str("endpoint"),
            start_block: get_opt_u64("start_block"),
            stop_block: get_opt_u64("stop_block"),
            partition: get_str("partition"),
            block_range_size: get_u64("block_range_size"),
            compression: get_str("compression"),
            flush_bytes: get_u64("flush_bytes"),
            flush_rows: get_opt_u64("flush_rows"),
            bytes_encoding: get_str("bytes_encoding"),
            extended: get_bool("extended"),
            final_blocks_only: get_bool("final_blocks_only"),
            chain_name: get_str("chain_name"),
            chain_name_aliases: get_str("chain_name_aliases"),
            first_streamable_block_num: get_u64("first_streamable_block_num"),
            first_streamable_block_id: get_str("first_streamable_block_id"),
            block_id_encoding: get_str("block_id_encoding"),
            block_features: get_str("block_features"),
            firehose_parquet_version: get_str("firehose_parquet_version"),
        })
    }
}

/// Save cursor state to a `cursor.parquet` file. Creates parent directories if needed.
pub fn save_cursor_parquet(path: &Path, state: &CursorState) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let batch = state.to_record_batch()?;
    let file = fs::File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Load cursor state from a `cursor.parquet` file. Returns `None` if the file does
/// not exist or cannot be read.
pub fn load_cursor_parquet(path: &Path) -> Option<CursorState> {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!(path = %path.display(), "cursor.parquet not found, starting fresh");
            return None;
        }
        Err(e) => {
            warn!(path = %path.display(), error = %e, "failed to open cursor.parquet, starting fresh");
            return None;
        }
    };
    let reader_builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "failed to read cursor.parquet metadata");
            return None;
        }
    };
    let reader = match reader_builder.build() {
        Ok(r) => r,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "failed to build parquet reader for cursor.parquet");
            return None;
        }
    };
    for batch_result in reader {
        match batch_result {
            Ok(batch) => match CursorState::from_record_batch(&batch) {
                Ok(state) => {
                    if state.cursor.is_empty() {
                        info!(path = %path.display(), "cursor.parquet has empty cursor, starting fresh");
                        return None;
                    }
                    info!(
                        path = %path.display(),
                        cursor = %state.cursor,
                        last_block_num = state.last_block_num,
                        "loaded cursor from cursor.parquet"
                    );
                    return Some(state);
                }
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "failed to parse cursor.parquet");
                    return None;
                }
            },
            Err(e) => {
                warn!(path = %path.display(), error = %e, "failed to read batch from cursor.parquet");
                return None;
            }
        }
    }
    info!(path = %path.display(), "cursor.parquet has no data, starting fresh");
    None
}

/// Load cursor string from a file. Returns `None` if the file does not exist
/// or is empty.
pub fn load_cursor(path: &Path) -> Option<String> {
    match fs::read_to_string(path) {
        Ok(content) => {
            let cursor = content.trim().to_string();
            if cursor.is_empty() {
                info!(path = %path.display(), "cursor file is empty, starting fresh");
                None
            } else {
                info!(path = %path.display(), cursor = %cursor, "loaded cursor from file");
                Some(cursor)
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!(path = %path.display(), "cursor file not found, starting fresh");
            None
        }
        Err(e) => {
            warn!(path = %path.display(), error = %e, "failed to read cursor file, starting fresh");
            None
        }
    }
}

/// Save cursor string to a file. Creates parent directories if needed.
pub fn save_cursor(path: &Path, cursor: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, cursor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_load_cursor_missing_file() {
        let result = load_cursor(Path::new("/tmp/nonexistent_cursor_file_12345"));
        assert!(result.is_none());
    }

    #[test]
    fn test_load_cursor_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");
        fs::write(&path, "").unwrap();
        assert!(load_cursor(&path).is_none());
    }

    #[test]
    fn test_load_cursor_whitespace_only() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");
        fs::write(&path, "  \n  ").unwrap();
        assert!(load_cursor(&path).is_none());
    }

    #[test]
    fn test_load_and_save_cursor() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");

        save_cursor(&path, "abc123").unwrap();
        assert_eq!(load_cursor(&path).as_deref(), Some("abc123"));
    }

    #[test]
    fn test_save_cursor_creates_parent_dirs() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nested").join("dir").join("cursor");

        save_cursor(&path, "xyz789").unwrap();
        assert_eq!(load_cursor(&path).as_deref(), Some("xyz789"));
    }

    #[test]
    fn test_save_cursor_overwrites() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");

        save_cursor(&path, "first").unwrap();
        save_cursor(&path, "second").unwrap();
        assert_eq!(load_cursor(&path).as_deref(), Some("second"));
    }

    #[test]
    fn test_load_cursor_trims_whitespace() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");
        fs::write(&path, "  abc123\n").unwrap();
        assert_eq!(load_cursor(&path).as_deref(), Some("abc123"));
    }

    #[test]
    fn test_save_cursor_to_bare_filename() {
        // When path has no parent dir component, save should still work
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");
        save_cursor(&path, "val").unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "val");
    }

    #[test]
    fn test_save_and_load_cursor_parquet() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);

        let state = CursorState {
            cursor: "cursor123".to_string(),
            last_block_num: 42000,
            last_block_id: "0xabc".to_string(),
            updated_at: "2025-01-15T12:00:00Z".to_string(),
            endpoint: "https://eth.firehose.pinax.network:443".to_string(),
            start_block: Some(100),
            stop_block: Some(200),
            partition: "date".to_string(),
            block_range_size: 10000,
            compression: "zstd".to_string(),
            flush_bytes: 134217728,
            flush_rows: Some(50000),
            bytes_encoding: "hex".to_string(),
            extended: true,
            final_blocks_only: true,
            chain_name: "eth-mainnet".to_string(),
            chain_name_aliases: "ethereum,eth".to_string(),
            first_streamable_block_num: 0,
            first_streamable_block_id: "0x0000".to_string(),
            block_id_encoding: "0x_hex".to_string(),
            block_features: "extended,base".to_string(),
            firehose_parquet_version: "0.2.5".to_string(),
        };

        save_cursor_parquet(&path, &state).unwrap();
        let loaded = load_cursor_parquet(&path).expect("should load cursor");

        assert_eq!(loaded.cursor, "cursor123");
        assert_eq!(loaded.last_block_num, 42000);
        assert_eq!(loaded.last_block_id, "0xabc");
        assert_eq!(loaded.updated_at, "2025-01-15T12:00:00Z");
        assert_eq!(loaded.endpoint, "https://eth.firehose.pinax.network:443");
        assert_eq!(loaded.start_block, Some(100));
        assert_eq!(loaded.stop_block, Some(200));
        assert_eq!(loaded.partition, "date");
        assert_eq!(loaded.block_range_size, 10000);
        assert_eq!(loaded.compression, "zstd");
        assert_eq!(loaded.flush_bytes, 134217728);
        assert_eq!(loaded.flush_rows, Some(50000));
        assert_eq!(loaded.bytes_encoding, "hex");
        assert!(loaded.extended);
        assert!(loaded.final_blocks_only);
        assert_eq!(loaded.chain_name, "eth-mainnet");
        assert_eq!(loaded.chain_name_aliases, "ethereum,eth");
        assert_eq!(loaded.first_streamable_block_num, 0);
        assert_eq!(loaded.first_streamable_block_id, "0x0000");
        assert_eq!(loaded.block_id_encoding, "0x_hex");
        assert_eq!(loaded.block_features, "extended,base");
        assert_eq!(loaded.firehose_parquet_version, "0.2.5");
    }

    #[test]
    fn test_save_cursor_parquet_overwrites() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);

        let state1 = CursorState {
            cursor: "first".to_string(),
            last_block_num: 100,
            ..CursorState::default()
        };
        save_cursor_parquet(&path, &state1).unwrap();

        let state2 = CursorState {
            cursor: "second".to_string(),
            last_block_num: 200,
            ..CursorState::default()
        };
        save_cursor_parquet(&path, &state2).unwrap();

        let loaded = load_cursor_parquet(&path).expect("should load cursor");
        assert_eq!(loaded.cursor, "second");
        assert_eq!(loaded.last_block_num, 200);
    }

    #[test]
    fn test_load_cursor_parquet_missing_file() {
        let result = load_cursor_parquet(Path::new("/tmp/nonexistent_cursor_parquet_12345.parquet"));
        assert!(result.is_none());
    }

    #[test]
    fn test_load_cursor_parquet_empty_cursor_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);

        let state = CursorState {
            cursor: "".to_string(),
            ..CursorState::default()
        };
        save_cursor_parquet(&path, &state).unwrap();
        assert!(load_cursor_parquet(&path).is_none());
    }

    #[test]
    fn test_save_cursor_parquet_creates_parent_dirs() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nested").join("dir").join(CURSOR_PARQUET_FILENAME);

        let state = CursorState {
            cursor: "abc".to_string(),
            ..CursorState::default()
        };
        save_cursor_parquet(&path, &state).unwrap();
        let loaded = load_cursor_parquet(&path).expect("should load cursor");
        assert_eq!(loaded.cursor, "abc");
    }

    #[test]
    fn test_cursor_parquet_nullable_fields() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);

        let state = CursorState {
            cursor: "test".to_string(),
            start_block: None,
            stop_block: None,
            flush_rows: None,
            ..CursorState::default()
        };
        save_cursor_parquet(&path, &state).unwrap();
        let loaded = load_cursor_parquet(&path).expect("should load cursor");
        assert_eq!(loaded.start_block, None);
        assert_eq!(loaded.stop_block, None);
        assert_eq!(loaded.flush_rows, None);
    }
}
