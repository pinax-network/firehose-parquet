use std::fs;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

use arrow::array::{Array, BinaryArray, BooleanArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use crate::writer::ParquetFileMetadata;

/// The filename used for the cursor parquet file.
pub const CURSOR_PARQUET_FILENAME: &str = "cursor.parquet";

/// Cursor state stored as a single-row Parquet file.
///
/// Row data contains only the essential resume state. Pipeline configuration
/// and firehose endpoint metadata are stored in Parquet file-level key-value
/// metadata under `firehose-parquet.*` keys (same convention as table files).
#[derive(Debug, Clone, Default)]
pub struct CursorState {
    // -- Row data: essential resume state --
    pub cursor: String,
    pub last_block_num: u64,
    /// Block ID stored as raw bytes.
    pub last_block_id: Vec<u8>,
    pub updated_at: String,
    pub start_block: Option<u64>,
    pub stop_block: Option<u64>,
    pub extended: bool,
    pub final_blocks_only: bool,

    // -- File-level metadata (not stored in rows) --
    pub file_metadata: ParquetFileMetadata,
}

/// Build the Arrow schema for the cursor.parquet row data.
fn cursor_schema() -> Schema {
    Schema::new(vec![
        Field::new("cursor", DataType::Utf8, false),
        Field::new("last_block_num", DataType::UInt64, false),
        Field::new("last_block_id", DataType::Binary, false),
        Field::new("updated_at", DataType::Utf8, false),
        Field::new("start_block", DataType::UInt64, true),
        Field::new("stop_block", DataType::UInt64, true),
        Field::new("extended", DataType::Boolean, false),
        Field::new("final_blocks_only", DataType::Boolean, false),
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
                Arc::new(BinaryArray::from_vec(vec![self.last_block_id.as_slice()])),
                Arc::new(StringArray::from(vec![self.updated_at.as_str()])),
                Arc::new(UInt64Array::from(vec![self.start_block])),
                Arc::new(UInt64Array::from(vec![self.stop_block])),
                Arc::new(BooleanArray::from(vec![self.extended])),
                Arc::new(BooleanArray::from(vec![self.final_blocks_only])),
            ],
        )?;
        Ok(batch)
    }

    /// Build `WriterProperties` that embed file-level metadata as Parquet KV pairs.
    fn writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder();
        if !self.file_metadata.entries.is_empty() {
            let kvs: Vec<KeyValue> = self
                .file_metadata
                .entries
                .iter()
                .map(|(k, v)| KeyValue::new(k.clone(), Some(v.clone())))
                .collect();
            builder = builder.set_key_value_metadata(Some(kvs));
        }
        builder.build()
    }

    /// Read a `CursorState` from a single-row RecordBatch plus optional
    /// file-level key-value metadata.
    ///
    /// Non-critical fields use lenient defaults for forward/backward
    /// compatibility — a file written by a newer or older version can still
    /// be read.
    fn from_record_batch(
        batch: &RecordBatch,
        kv_metadata: Option<&[KeyValue]>,
    ) -> anyhow::Result<Self> {
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
        let get_bytes = |name: &str| -> Vec<u8> {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_binary_opt::<i32>())
                .and_then(|a| if a.is_null(0) { None } else { Some(a.value(0).to_vec()) })
                // Fallback: try reading as Utf8 for backward compat with old cursor files
                .or_else(|| {
                    batch
                        .column_by_name(name)
                        .and_then(|c| c.as_string_opt::<i32>())
                        .and_then(|a| if a.is_null(0) { None } else { Some(a.value(0).as_bytes().to_vec()) })
                })
                .unwrap_or_default()
        };

        // Reconstruct file_metadata from KV pairs.
        let mut file_metadata = ParquetFileMetadata::new();
        if let Some(kvs) = kv_metadata {
            for kv in kvs {
                if let Some(ref v) = kv.value {
                    file_metadata.add(kv.key.clone(), v.clone());
                }
            }
        }

        Ok(CursorState {
            cursor: get_str("cursor"),
            last_block_num: get_u64("last_block_num"),
            last_block_id: get_bytes("last_block_id"),
            updated_at: get_str("updated_at"),
            start_block: get_opt_u64("start_block"),
            stop_block: get_opt_u64("stop_block"),
            extended: get_bool("extended"),
            final_blocks_only: get_bool("final_blocks_only"),
            file_metadata,
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
    let props = state.writer_properties();
    let file = fs::File::create(path)?;
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

    // Extract file-level KV metadata before consuming the builder.
    let kv_metadata = reader_builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();

    let reader = match reader_builder.build() {
        Ok(r) => r,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "failed to build parquet reader for cursor.parquet");
            return None;
        }
    };
    for batch_result in reader {
        match batch_result {
            Ok(batch) => match CursorState::from_record_batch(&batch, kv_metadata.as_deref()) {
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
    let props = state.writer_properties();

    // Write parquet to in-memory buffer
    let mut buf = Vec::new();
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

    // Extract file-level KV metadata before consuming the builder.
    let kv_metadata = reader_builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();

    let reader = match reader_builder.build() {
        Ok(r) => r,
        Err(e) => {
            warn!(key = %key, error = %e, "failed to build parquet reader for S3 cursor.parquet");
            return None;
        }
    };

    for batch_result in reader {
        match batch_result {
            Ok(batch) => match CursorState::from_record_batch(&batch, kv_metadata.as_deref()) {
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

    fn test_file_metadata() -> ParquetFileMetadata {
        let mut meta = ParquetFileMetadata::new();
        meta.add("firehose-parquet.version", "0.3.2");
        meta.add("firehose-parquet.endpoint", "https://eth.firehose.pinax.network:443");
        meta.add("firehose-parquet.chain_name", "eth-mainnet");
        meta.add("firehose-parquet.chain_name_aliases", "ethereum,eth");
        meta.add("firehose-parquet.first_streamable_block_num", "0");
        meta.add("firehose-parquet.first_streamable_block_id", "0x0000");
        meta.add("firehose-parquet.block_id_encoding", "hex_0x");
        meta.add("firehose-parquet.block_features", "extended,base");
        meta.add("firehose-parquet.bytes_encoding", "hex");
        meta.add("firehose-parquet.compression", "zstd");
        meta.add("firehose-parquet.partition", "date");
        meta.add("firehose-parquet.block_range_size", "10000");
        meta
    }

    #[test]
    fn test_save_and_load_cursor_parquet() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);

        let state = CursorState {
            cursor: "cursor123".to_string(),
            last_block_num: 42000,
            last_block_id: b"\xab\xcd\xef".to_vec(),
            updated_at: "2025-01-15T12:00:00Z".to_string(),
            start_block: Some(100),
            stop_block: Some(200),
            extended: true,
            final_blocks_only: true,
            file_metadata: test_file_metadata(),
        };

        save_cursor_parquet(&path, &state).unwrap();
        let loaded = load_cursor_parquet(&path).expect("should load cursor");

        assert_eq!(loaded.cursor, "cursor123");
        assert_eq!(loaded.last_block_num, 42000);
        assert_eq!(loaded.last_block_id, b"\xab\xcd\xef".to_vec());
        assert_eq!(loaded.updated_at, "2025-01-15T12:00:00Z");
        assert_eq!(loaded.start_block, Some(100));
        assert_eq!(loaded.stop_block, Some(200));
        assert!(loaded.extended);
        assert!(loaded.final_blocks_only);

        // Verify file-level metadata was round-tripped.
        let meta_map: std::collections::HashMap<_, _> = loaded
            .file_metadata
            .entries
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(meta_map.get("firehose-parquet.version"), Some(&"0.3.2"));
        assert_eq!(meta_map.get("firehose-parquet.chain_name"), Some(&"eth-mainnet"));
        assert_eq!(meta_map.get("firehose-parquet.block_features"), Some(&"extended,base"));
        assert_eq!(meta_map.get("firehose-parquet.bytes_encoding"), Some(&"hex"));
        assert_eq!(meta_map.get("firehose-parquet.compression"), Some(&"zstd"));
        assert_eq!(meta_map.get("firehose-parquet.partition"), Some(&"date"));
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
            ..CursorState::default()
        };
        save_cursor_parquet(&path, &state).unwrap();
        let loaded = load_cursor_parquet(&path).expect("should load cursor");
        assert_eq!(loaded.start_block, None);
        assert_eq!(loaded.stop_block, None);
    }

    #[test]
    fn test_cursor_parquet_binary_block_id() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);

        // Test with a hex-like block ID stored as bytes.
        let block_id = hex::decode("deadbeef01020304").unwrap();
        let state = CursorState {
            cursor: "test_binary".to_string(),
            last_block_id: block_id.clone(),
            ..CursorState::default()
        };
        save_cursor_parquet(&path, &state).unwrap();
        let loaded = load_cursor_parquet(&path).expect("should load cursor");
        assert_eq!(loaded.last_block_id, block_id);
    }
}
