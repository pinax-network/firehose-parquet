use std::fs;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

use arrow::array::{Array, BooleanArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::ObjectStore;
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
    ///
    /// Non-critical fields use lenient defaults (empty string, 0, false) for
    /// forward compatibility — a file written by a newer version with extra
    /// columns can still be read by an older version. The `cursor` field is
    /// validated by the caller (`load_cursor_parquet`).
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

/// Where the cursor file lives — local filesystem or S3.
#[derive(Debug, Clone)]
pub enum CursorLocation {
    Local(std::path::PathBuf),
    S3 {
        client: Arc<dyn ObjectStore>,
        key: String,
    },
}

impl CursorLocation {
    /// Resolve cursor location from output path and cursor filename.
    ///
    /// If output is an S3 path, the cursor is placed alongside data in S3.
    /// If output is local, the cursor stays local.
    pub fn resolve(
        output: &str,
        cursor_filename: &str,
        s3_client: Option<Arc<dyn ObjectStore>>,
    ) -> anyhow::Result<Self> {
        if cursor_filename.starts_with("s3://") {
            // Explicit S3 cursor path
            let (_bucket, key) = crate::writer::parse_s3_url(cursor_filename)?;
            let client = s3_client.ok_or_else(|| {
                anyhow::anyhow!("cursor is an S3 path but no S3 client available")
            })?;
            return Ok(CursorLocation::S3 { client, key });
        }

        if output.starts_with("s3://") {
            // S3 output — place cursor alongside data
            if cursor_filename.contains('/') || cursor_filename.contains('\\') {
                anyhow::bail!(
                    "S3 output with local cursor path containing directories is not supported: {cursor_filename}. \
                     Use a bare filename (e.g. cursor.parquet) or an explicit s3:// URI."
                );
            }
            let (_bucket, prefix) = crate::writer::parse_s3_url(output)?;
            let key = if prefix.is_empty() {
                cursor_filename.to_string()
            } else {
                format!("{prefix}/{cursor_filename}")
            };
            let client = s3_client.ok_or_else(|| {
                anyhow::anyhow!("output is S3 but no S3 client available")
            })?;
            Ok(CursorLocation::S3 { client, key })
        } else {
            // Local output — local cursor
            if cursor_filename.starts_with("s3://") {
                anyhow::bail!("local output with S3 cursor path is not supported");
            }
            Ok(CursorLocation::Local(std::path::PathBuf::from(cursor_filename)))
        }
    }

    /// Save cursor state (blocking — safe to call from sync code inside tokio).
    pub fn save(&self, state: &CursorState) -> anyhow::Result<()> {
        match self {
            CursorLocation::Local(path) => save_cursor_parquet(path, state),
            CursorLocation::S3 { client, key } => {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(save_cursor_parquet_s3(client.as_ref(), key, state))
                })
            }
        }
    }

    /// Load cursor state (blocking — safe to call from sync code inside tokio).
    /// Returns `None` if not found or unreadable.
    pub fn load(&self) -> Option<CursorState> {
        match self {
            CursorLocation::Local(path) => load_cursor_parquet(path),
            CursorLocation::S3 { client, key } => {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(load_cursor_parquet_s3(client.as_ref(), key))
                })
            }
        }
    }
}

/// Save cursor state to S3 as a parquet file.
async fn save_cursor_parquet_s3(
    client: &dyn ObjectStore,
    key: &str,
    state: &CursorState,
) -> anyhow::Result<()> {
    let batch = state.to_record_batch()?;

    // Write parquet to in-memory buffer
    let mut buf = Vec::new();
    let props = WriterProperties::builder().build();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }

    let path = object_store::path::Path::from(key);
    let payload = object_store::PutPayload::from(Bytes::from(buf));
    let opts = crate::writer::s3_put_options("");
    client.put_opts(&path, payload, opts).await?;
    info!(key = %key, "saved cursor.parquet to S3");
    Ok(())
}

/// Load cursor state from S3. Returns `None` if not found or unreadable.
async fn load_cursor_parquet_s3(
    client: &dyn ObjectStore,
    key: &str,
) -> Option<CursorState> {
    let path = object_store::path::Path::from(key);
    let result = match client.get(&path).await {
        Ok(r) => r,
        Err(object_store::Error::NotFound { .. }) => {
            info!(key = %key, "cursor.parquet not found in S3, starting fresh");
            return None;
        }
        Err(e) => {
            warn!(key = %key, error = %e, "failed to read cursor.parquet from S3, starting fresh");
            return None;
        }
    };

    let bytes = match result.bytes().await {
        Ok(b) => b,
        Err(e) => {
            warn!(key = %key, error = %e, "failed to read cursor.parquet bytes from S3");
            return None;
        }
    };

    let reader_builder = match ParquetRecordBatchReaderBuilder::try_new(bytes) {
        Ok(b) => b,
        Err(e) => {
            warn!(key = %key, error = %e, "failed to read cursor.parquet metadata from S3");
            return None;
        }
    };
    let reader = match reader_builder.build() {
        Ok(r) => r,
        Err(e) => {
            warn!(key = %key, error = %e, "failed to build parquet reader for S3 cursor.parquet");
            return None;
        }
    };

    for batch_result in reader {
        match batch_result {
            Ok(batch) => match CursorState::from_record_batch(&batch) {
                Ok(state) => {
                    if state.cursor.is_empty() {
                        info!(key = %key, "S3 cursor.parquet has empty cursor, starting fresh");
                        return None;
                    }
                    info!(
                        key = %key,
                        cursor = %state.cursor,
                        last_block_num = state.last_block_num,
                        "loaded cursor from S3 cursor.parquet"
                    );
                    return Some(state);
                }
                Err(e) => {
                    warn!(key = %key, error = %e, "failed to parse S3 cursor.parquet");
                    return None;
                }
            },
            Err(e) => {
                warn!(key = %key, error = %e, "failed to read batch from S3 cursor.parquet");
                return None;
            }
        }
    }
    info!(key = %key, "S3 cursor.parquet has no data, starting fresh");
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
