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
    pub include_failed_transactions: bool,

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
        Field::new("include_failed_transactions", DataType::Boolean, false),
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
                Arc::new(BooleanArray::from(vec![self.include_failed_transactions])),
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

    /// Look up a file-level metadata value by key.
    pub fn get_metadata(&self, key: &str) -> Option<&str> {
        self.file_metadata
            .entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Validate that the current pipeline parameters match those stored in the
    /// cursor. Returns a list of human-readable mismatch descriptions.
    ///
    /// Compares row-level fields (start_block, stop_block, extended,
    /// final_blocks_only, include_failed_transactions) and file-level metadata
    /// (endpoint, partition, block_range_size, compression, bytes_encoding).
    pub fn validate_params(&self, current: &CursorState) -> Vec<String> {
        let mut mismatches = Vec::new();

        // Row-level fields.
        if self.start_block != current.start_block {
            mismatches.push(format!(
                "start_block: cursor={:?} vs current={:?}",
                self.start_block, current.start_block
            ));
        }
        if self.stop_block != current.stop_block {
            mismatches.push(format!(
                "stop_block: cursor={:?} vs current={:?}",
                self.stop_block, current.stop_block
            ));
        }
        if self.extended != current.extended {
            mismatches.push(format!(
                "extended: cursor={} vs current={}",
                self.extended, current.extended
            ));
        }
        if self.final_blocks_only != current.final_blocks_only {
            mismatches.push(format!(
                "final_blocks_only: cursor={} vs current={}",
                self.final_blocks_only, current.final_blocks_only
            ));
        }
        if self.include_failed_transactions != current.include_failed_transactions {
            mismatches.push(format!(
                "include_failed_transactions: cursor={} vs current={}",
                self.include_failed_transactions, current.include_failed_transactions
            ));
        }

        // File-level metadata fields.
        let meta_keys = [
            "firehose-parquet.endpoint",
            "firehose-parquet.partition",
            "firehose-parquet.block_range_size",
            "firehose-parquet.compression",
            "firehose-parquet.bytes_encoding",
        ];
        for key in &meta_keys {
            let stored = self.get_metadata(key).unwrap_or("");
            let current_val = current.get_metadata(key).unwrap_or("");
            // Skip comparison when the stored value is empty (older cursor without metadata).
            if !stored.is_empty() && stored != current_val {
                let short_key = key.strip_prefix("firehose-parquet.").unwrap_or(key);
                mismatches.push(format!(
                    "{short_key}: cursor=\"{stored}\" vs current=\"{current_val}\""
                ));
            }
        }

        mismatches
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
                .and_then(|a| {
                    if a.is_null(0) {
                        None
                    } else {
                        Some(a.value(0).to_string())
                    }
                })
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
                .and_then(|a| {
                    if a.is_null(0) {
                        None
                    } else {
                        Some(a.value(0).to_vec())
                    }
                })
                // Fallback: try reading as Utf8 for backward compat with old cursor files
                .or_else(|| {
                    batch
                        .column_by_name(name)
                        .and_then(|c| c.as_string_opt::<i32>())
                        .and_then(|a| {
                            if a.is_null(0) {
                                None
                            } else {
                                Some(a.value(0).as_bytes().to_vec())
                            }
                        })
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
            include_failed_transactions: get_bool("include_failed_transactions"),
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
    /// If output is local, relative cursor paths are placed under the local
    /// output root while absolute local paths remain absolute.
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
            if std::path::Path::new(cursor_filename).is_absolute() {
                anyhow::bail!(
                    "S3 output with absolute local cursor path is not supported: {cursor_filename}. \
                     Use a relative path (e.g. cursor/worker/cursor.parquet) or an explicit s3:// URI."
                );
            }
            let (_bucket, prefix) = crate::writer::parse_s3_url(output)?;
            let relative_key = cursor_filename.replace('\\', "/");
            let key = if prefix.is_empty() {
                relative_key
            } else {
                format!("{prefix}/{relative_key}")
            };
            let client = s3_client
                .ok_or_else(|| anyhow::anyhow!("output is S3 but no S3 client available"))?;
            Ok(CursorLocation::S3 { client, key })
        } else {
            // Local output — local cursor
            if cursor_filename.starts_with("s3://") {
                anyhow::bail!("local output with S3 cursor path is not supported");
            }
            let cursor_path = std::path::PathBuf::from(cursor_filename);
            let resolved_path = if cursor_path.is_absolute() {
                cursor_path
            } else {
                std::path::PathBuf::from(output).join(cursor_path)
            };
            Ok(CursorLocation::Local(resolved_path))
        }
    }

    /// Save cursor state (blocking — safe to call from sync code inside tokio).
    pub fn save(&self, state: &CursorState) -> anyhow::Result<()> {
        match self {
            CursorLocation::Local(path) => save_cursor_parquet(path, state),
            CursorLocation::S3 { client, key } => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(save_cursor_parquet_s3(
                    client.as_ref(),
                    key,
                    state,
                ))
            }),
        }
    }

    /// Load cursor state (blocking — safe to call from sync code inside tokio).
    /// Returns `None` if not found or unreadable.
    pub fn load(&self) -> Option<CursorState> {
        match self {
            CursorLocation::Local(path) => load_cursor_parquet(path),
            CursorLocation::S3 { client, key } => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(load_cursor_parquet_s3(client.as_ref(), key))
            }),
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
async fn load_cursor_parquet_s3(client: &dyn ObjectStore, key: &str) -> Option<CursorState> {
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
        meta.add("firehose-parquet.version", "0.5.3");
        meta.add(
            "firehose-parquet.endpoint",
            "https://eth.firehose.pinax.network:443",
        );
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
            include_failed_transactions: false,
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
        assert_eq!(meta_map.get("firehose-parquet.version"), Some(&"0.5.3"));
        assert_eq!(
            meta_map.get("firehose-parquet.chain_name"),
            Some(&"eth-mainnet")
        );
        assert_eq!(
            meta_map.get("firehose-parquet.block_features"),
            Some(&"extended,base")
        );
        assert_eq!(
            meta_map.get("firehose-parquet.bytes_encoding"),
            Some(&"hex")
        );
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
        let result =
            load_cursor_parquet(Path::new("/tmp/nonexistent_cursor_parquet_12345.parquet"));
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
        let path = dir
            .path()
            .join("nested")
            .join("dir")
            .join(CURSOR_PARQUET_FILENAME);

        let state = CursorState {
            cursor: "abc".to_string(),
            ..CursorState::default()
        };
        save_cursor_parquet(&path, &state).unwrap();
        let loaded = load_cursor_parquet(&path).expect("should load cursor");
        assert_eq!(loaded.cursor, "abc");
    }

    #[test]
    fn test_cursor_location_resolve_s3_allows_relative_directories() {
        let location = CursorLocation::resolve(
            "s3://bucket/output",
            "cursor/hour/2015-07-30 15:00:00.parquet",
            Some(Arc::new(object_store::memory::InMemory::new())),
        )
        .expect("s3 relative cursor path should resolve");

        match location {
            CursorLocation::S3 { key, .. } => {
                assert_eq!(key, "output/cursor/hour/2015-07-30 15:00:00.parquet")
            }
            CursorLocation::Local(_) => panic!("expected S3 cursor location"),
        }
    }

    #[test]
    fn test_cursor_location_resolve_local_places_relative_cursor_under_output_root() {
        let location = CursorLocation::resolve("./output/mainnet", CURSOR_PARQUET_FILENAME, None)
            .expect("local relative cursor path should resolve");

        match location {
            CursorLocation::Local(path) => {
                assert_eq!(
                    path,
                    std::path::PathBuf::from("./output/mainnet").join(CURSOR_PARQUET_FILENAME)
                );
            }
            CursorLocation::S3 { .. } => panic!("expected local cursor location"),
        }
    }

    #[test]
    fn test_cursor_location_resolve_local_keeps_absolute_cursor_path() {
        let absolute_path = std::env::temp_dir().join(CURSOR_PARQUET_FILENAME);
        let location = CursorLocation::resolve(
            "./output/mainnet",
            absolute_path.to_string_lossy().as_ref(),
            None,
        )
        .expect("absolute local cursor path should resolve");

        match location {
            CursorLocation::Local(path) => assert_eq!(path, absolute_path),
            CursorLocation::S3 { .. } => panic!("expected local cursor location"),
        }
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

    #[test]
    fn test_validate_params_no_mismatch() {
        let meta = test_file_metadata();
        let state = CursorState {
            cursor: "c1".to_string(),
            start_block: Some(100),
            stop_block: Some(200),
            extended: true,
            final_blocks_only: true,
            include_failed_transactions: false,
            file_metadata: meta.clone(),
            ..CursorState::default()
        };
        let current = CursorState {
            start_block: Some(100),
            stop_block: Some(200),
            extended: true,
            final_blocks_only: true,
            include_failed_transactions: false,
            file_metadata: meta,
            ..CursorState::default()
        };
        assert!(state.validate_params(&current).is_empty());
    }

    #[test]
    fn test_validate_params_detects_mismatches() {
        let meta = test_file_metadata();
        let state = CursorState {
            cursor: "c1".to_string(),
            start_block: Some(100),
            extended: true,
            final_blocks_only: true,
            include_failed_transactions: false,
            file_metadata: meta,
            ..CursorState::default()
        };

        let mut different_meta = test_file_metadata();
        // Change compression in metadata.
        for entry in &mut different_meta.entries {
            if entry.0 == "firehose-parquet.compression" {
                entry.1 = "snappy".to_string();
            }
        }
        let current = CursorState {
            start_block: Some(500), // different
            extended: false,        // different
            final_blocks_only: true,
            include_failed_transactions: true, // different
            file_metadata: different_meta,
            ..CursorState::default()
        };

        let mismatches = state.validate_params(&current);
        assert_eq!(mismatches.len(), 4);
        assert!(mismatches.iter().any(|m| m.contains("start_block")));
        assert!(mismatches.iter().any(|m| m.contains("extended")));
        assert!(mismatches
            .iter()
            .any(|m| m.contains("include_failed_transactions")));
        assert!(mismatches.iter().any(|m| m.contains("compression")));
    }

    #[test]
    fn test_validate_params_skips_empty_stored_metadata() {
        // Old cursor without metadata should not flag mismatches on metadata keys.
        let state = CursorState {
            cursor: "c1".to_string(),
            extended: true,
            final_blocks_only: true,
            ..CursorState::default()
        };
        let current = CursorState {
            extended: true,
            final_blocks_only: true,
            file_metadata: test_file_metadata(),
            ..CursorState::default()
        };
        // No metadata mismatches — stored is empty, so skipped.
        assert!(state.validate_params(&current).is_empty());
    }
}
