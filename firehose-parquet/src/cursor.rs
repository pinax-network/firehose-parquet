use anyhow::Context;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

use arrow::array::{Array, BinaryArray, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use crate::encode::{parse_encode_bytes, EncodeBytes};
use crate::writer::ParquetFileMetadata;

/// The filename used for the cursor parquet file.
pub const CURSOR_PARQUET_FILENAME: &str = "cursor.parquet";
const CURSOR_METADATA_EXTENDED: &str = "firehose-parquet.extended";
const CURSOR_METADATA_FINAL_BLOCKS_ONLY: &str = "firehose-parquet.final_blocks_only";
const CURSOR_METADATA_INCLUDE_FAILED_TRANSACTIONS: &str =
    "firehose-parquet.include_failed_transactions";
const CURSOR_METADATA_SYNTHETIC_TIMESTAMPS: &str = "firehose-parquet.synthetic_timestamps";
const CURSOR_METADATA_SYNTHETIC_TIMESTAMP_POLICY: &str =
    "firehose-parquet.synthetic_timestamp_policy";

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
    pub last_timestamp: Option<i64>,
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
        Field::new("last_timestamp", DataType::Int64, true),
        Field::new("updated_at", DataType::Utf8, false),
        Field::new("start_block", DataType::UInt64, true),
        Field::new("stop_block", DataType::UInt64, true),
    ])
}

fn encode_bytes_label(encoding: &EncodeBytes) -> &'static str {
    match encoding {
        EncodeBytes::Binary => "binary",
        EncodeBytes::Hex => "hex",
        EncodeBytes::HexNoPrefix => "hex_no_prefix",
        EncodeBytes::Base58 => "base58",
        EncodeBytes::TronBase58 => "tron_base58",
    }
}

fn block_id_encoding_to_bytes_encoding(encoding: &str) -> Option<EncodeBytes> {
    match encoding {
        // Firehose block-id hints describe how canonical IDs are rendered, but
        // cursor compatibility only needs the effective byte-field encoding.
        // Both hex variants therefore normalize to the same EncodeBytes::Hex.
        "hex" | "hex_0x" => Some(EncodeBytes::Hex),
        "base58" => Some(EncodeBytes::Base58),
        _ => None,
    }
}

fn is_tron_style_chain_name(chain_name: &str) -> bool {
    chain_name.eq_ignore_ascii_case("tron") || chain_name.eq_ignore_ascii_case("tron-evm")
}

fn metadata_has_tron_style_chain(state: &CursorState) -> bool {
    state
        .get_metadata("firehose-parquet.chain_name")
        .map(is_tron_style_chain_name)
        .unwrap_or(false)
        || state
            .get_metadata("firehose-parquet.chain_name_aliases")
            .map(|aliases| aliases.split(',').any(is_tron_style_chain_name))
            .unwrap_or(false)
}

fn block_type_default_bytes_encoding(
    block_type: &str,
    tron_style_evm_profile: bool,
) -> Option<EncodeBytes> {
    match block_type {
        "evm" if tron_style_evm_profile => Some(EncodeBytes::TronBase58),
        "evm" | "bitcoin" | "antelope" | "cosmos" | "beacon" => Some(EncodeBytes::Hex),
        "solana" | "near" => Some(EncodeBytes::Base58),
        "tron" => Some(EncodeBytes::TronBase58),
        _ => None,
    }
}

/// Resolve `bytes_encoding=auto` from cursor metadata.
///
/// This prefers a known block-type output contract, then falls back to the
/// stored block-id encoding hint, and finally defaults to hex when neither is
/// available.
fn resolve_auto_bytes_encoding(state: &CursorState) -> EncodeBytes {
    let tron_style_evm_profile = metadata_has_tron_style_chain(state);

    if let Some(block_type) = state.get_metadata("firehose-parquet.block_type") {
        if let Some(encoding) =
            block_type_default_bytes_encoding(block_type, tron_style_evm_profile)
        {
            return encoding;
        }
    }

    state
        .get_metadata("firehose-parquet.block_id_encoding")
        .and_then(block_id_encoding_to_bytes_encoding)
        .unwrap_or(EncodeBytes::Hex)
}

fn effective_bytes_encoding_label(state: &CursorState, raw: &str) -> String {
    if let Some(encoding) = parse_encode_bytes(raw) {
        return encode_bytes_label(&encoding).to_string();
    }

    if raw.eq_ignore_ascii_case("auto") {
        return encode_bytes_label(&resolve_auto_bytes_encoding(state)).to_string();
    }

    raw.to_string()
}

/// Parse a cursor metadata boolean encoded as the literal string `true` or
/// `false`.
///
/// Returns `None` for missing or malformed values so callers can apply their
/// own legacy fallback behavior.
fn parse_cursor_metadata_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// Read a compatibility boolean from cursor file metadata, falling back to the
/// legacy row value when older cursors do not carry the metadata key yet.
///
/// The `legacy_value` parameter preserves `v0.7.x` backward compatibility for
/// row-based cursor formats while new cursors are validated from metadata.
fn cursor_metadata_bool(state: &CursorState, key: &str, legacy_value: bool) -> bool {
    state
        .get_metadata(key)
        .and_then(parse_cursor_metadata_bool)
        .unwrap_or(legacy_value)
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
                Arc::new(Int64Array::from(vec![self.last_timestamp])),
                Arc::new(StringArray::from(vec![self.updated_at.as_str()])),
                Arc::new(UInt64Array::from(vec![self.start_block])),
                Arc::new(UInt64Array::from(vec![self.stop_block])),
            ],
        )?;
        Ok(batch)
    }

    /// Build `WriterProperties` that embed file-level metadata as Parquet KV pairs.
    fn writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder();
        let mut metadata_entries = self.file_metadata.entries.clone();
        // Cursor compatibility metadata is normalized here so newly written
        // cursor.parquet files always carry the canonical metadata keys, while
        // the older descriptive synthetic-timestamp keys are intentionally
        // dropped as part of the v0.7.x cursor format cleanup.
        metadata_entries.retain(|(key, _)| {
            !matches!(
                key.as_str(),
                CURSOR_METADATA_EXTENDED
                    | CURSOR_METADATA_FINAL_BLOCKS_ONLY
                    | CURSOR_METADATA_INCLUDE_FAILED_TRANSACTIONS
                    | CURSOR_METADATA_SYNTHETIC_TIMESTAMPS
                    | CURSOR_METADATA_SYNTHETIC_TIMESTAMP_POLICY
            )
        });
        metadata_entries.push((
            CURSOR_METADATA_EXTENDED.to_string(),
            self.extended.to_string(),
        ));
        metadata_entries.push((
            CURSOR_METADATA_FINAL_BLOCKS_ONLY.to_string(),
            self.final_blocks_only.to_string(),
        ));
        metadata_entries.push((
            CURSOR_METADATA_INCLUDE_FAILED_TRANSACTIONS.to_string(),
            self.include_failed_transactions.to_string(),
        ));
        if !metadata_entries.is_empty() {
            let kvs: Vec<KeyValue> = metadata_entries
                .into_iter()
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
    /// Compares row-level fields (start_block, stop_block) and file-level metadata
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
        let stored_extended = cursor_metadata_bool(self, CURSOR_METADATA_EXTENDED, self.extended);
        let current_extended =
            cursor_metadata_bool(current, CURSOR_METADATA_EXTENDED, current.extended);
        if stored_extended != current_extended {
            mismatches.push(format!(
                "extended: cursor={} vs current={}",
                stored_extended, current_extended
            ));
        }
        let stored_final_blocks_only = cursor_metadata_bool(
            self,
            CURSOR_METADATA_FINAL_BLOCKS_ONLY,
            self.final_blocks_only,
        );
        let current_final_blocks_only = cursor_metadata_bool(
            current,
            CURSOR_METADATA_FINAL_BLOCKS_ONLY,
            current.final_blocks_only,
        );
        if stored_final_blocks_only != current_final_blocks_only {
            mismatches.push(format!(
                "final_blocks_only: cursor={} vs current={}",
                stored_final_blocks_only, current_final_blocks_only
            ));
        }
        let stored_include_failed_transactions = cursor_metadata_bool(
            self,
            CURSOR_METADATA_INCLUDE_FAILED_TRANSACTIONS,
            self.include_failed_transactions,
        );
        let current_include_failed_transactions = cursor_metadata_bool(
            current,
            CURSOR_METADATA_INCLUDE_FAILED_TRANSACTIONS,
            current.include_failed_transactions,
        );
        if stored_include_failed_transactions != current_include_failed_transactions {
            mismatches.push(format!(
                "include_failed_transactions: cursor={} vs current={}",
                stored_include_failed_transactions, current_include_failed_transactions
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
            if stored.is_empty() {
                continue;
            }

            if *key == "firehose-parquet.bytes_encoding" {
                let stored_effective = effective_bytes_encoding_label(self, stored);
                let current_effective = effective_bytes_encoding_label(current, current_val);
                if stored_effective != current_effective {
                    let short_key = key.strip_prefix("firehose-parquet.").unwrap_or(key);
                    mismatches.push(format!(
                        "{short_key}: cursor=\"{stored_effective}\" vs current=\"{current_effective}\""
                    ));
                }
            } else if stored != current_val {
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
        if batch
            .column_by_name("cursor")
            .and_then(|c| c.as_string_opt::<i32>())
            .is_none()
        {
            anyhow::bail!("not a cursor file: missing Utf8 `cursor` column");
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
        let get_opt_i64 = |name: &str| -> Option<i64> {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_primitive_opt::<arrow::datatypes::Int64Type>())
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
            last_timestamp: get_opt_i64("last_timestamp"),
            updated_at: get_str("updated_at"),
            start_block: get_opt_u64("start_block"),
            stop_block: get_opt_u64("stop_block"),
            extended: file_metadata
                .entries
                .iter()
                .find(|(key, _)| key == CURSOR_METADATA_EXTENDED)
                .and_then(|(_, value)| parse_cursor_metadata_bool(value))
                .unwrap_or_else(|| get_bool("extended")),
            final_blocks_only: file_metadata
                .entries
                .iter()
                .find(|(key, _)| key == CURSOR_METADATA_FINAL_BLOCKS_ONLY)
                .and_then(|(_, value)| parse_cursor_metadata_bool(value))
                .unwrap_or_else(|| get_bool("final_blocks_only")),
            include_failed_transactions: file_metadata
                .entries
                .iter()
                .find(|(key, _)| key == CURSOR_METADATA_INCLUDE_FAILED_TRANSACTIONS)
                .and_then(|(_, value)| parse_cursor_metadata_bool(value))
                .unwrap_or_else(|| get_bool("include_failed_transactions")),
            file_metadata,
        })
    }
}

/// Serialize cursor state to `cursor.parquet` bytes.
fn encode_cursor(state: &CursorState) -> anyhow::Result<Vec<u8>> {
    let batch = state.to_record_batch()?;
    let props = state.writer_properties();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buf)
}

/// Parse `cursor.parquet` bytes.
///
/// Returns `Ok(None)` when the file is valid but holds no row or an empty
/// cursor, i.e. there is nothing to resume from. A file that is not a
/// readable cursor (empty, truncated, corrupt, or missing the `cursor`
/// column) is an error.
pub(crate) fn parse_cursor(bytes: Bytes) -> anyhow::Result<Option<CursorState>> {
    let reader_builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).context("reading parquet metadata")?;

    // Extract file-level KV metadata before consuming the builder.
    let kv_metadata = reader_builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();

    let mut reader = reader_builder.build().context("building parquet reader")?;
    let Some(batch) = reader.next().transpose().context("reading cursor row")? else {
        return Ok(None);
    };
    let state = CursorState::from_record_batch(&batch, kv_metadata.as_deref())?;
    if state.cursor.is_empty() {
        return Ok(None);
    }
    Ok(Some(state))
}

/// Temporary sibling path used to write a cursor before renaming it into place.
fn temp_cursor_path(path: &Path) -> std::path::PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

/// fsync a directory so a rename inside it survives a crash. Only supported
/// on Unix; elsewhere this is a no-op.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Save cursor state to a `cursor.parquet` file. Creates parent directories if needed.
///
/// The save is atomic: the cursor is written to `<name>.tmp` in the same
/// directory, fsynced, and renamed over the target, then the directory is
/// fsynced. A crash mid-save leaves the previous cursor intact.
pub fn save_cursor_parquet(path: &Path, state: &CursorState) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if let Some(parent) = parent {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating cursor directory {}", parent.display()))?;
    }
    let buf = encode_cursor(state)?;

    let tmp_path = temp_cursor_path(path);
    let write_tmp = || -> anyhow::Result<()> {
        let mut file = fs::File::create(&tmp_path)
            .with_context(|| format!("creating {}", tmp_path.display()))?;
        file.write_all(&buf)
            .with_context(|| format!("writing {}", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("syncing {}", tmp_path.display()))?;
        fs::rename(&tmp_path, path)
            .with_context(|| format!("renaming {} to {}", tmp_path.display(), path.display()))?;
        Ok(())
    };
    if let Err(error) = write_tmp() {
        let _ = fs::remove_file(&tmp_path);
        return Err(error);
    }

    let dir = parent.unwrap_or_else(|| Path::new("."));
    if let Err(error) = sync_dir(dir) {
        warn!(dir = %dir.display(), error = %error, "failed to fsync cursor directory after rename");
    }
    Ok(())
}

/// Load cursor state from a `cursor.parquet` file.
///
/// Returns `Ok(None)` only when there is nothing to resume from: the file does
/// not exist, or it holds no row or an empty cursor. Any other failure
/// (permission denied, I/O error, empty, truncated or corrupt file) is an
/// error, so a damaged cursor never silently restarts ingestion from scratch.
pub fn load_cursor_parquet(path: &Path) -> anyhow::Result<Option<CursorState>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!(path = %path.display(), "cursor.parquet not found, starting fresh");
            return Ok(None);
        }
        Err(e) => {
            return Err(
                anyhow::Error::new(e).context(format!("reading cursor file {}", path.display()))
            );
        }
    };
    let state = parse_cursor(Bytes::from(bytes))
        .with_context(|| format!("parsing cursor file {}", path.display()))?;
    match &state {
        Some(state) => info!(
            path = %path.display(),
            cursor = %state.cursor,
            last_block_num = state.last_block_num,
            "loaded cursor from cursor.parquet"
        ),
        None => info!(path = %path.display(), "cursor.parquet has no cursor, starting fresh"),
    }
    Ok(state)
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
    /// Returns `Ok(None)` if there is no cursor to resume from, and an error
    /// if a cursor exists but cannot be read or parsed.
    pub fn load(&self) -> anyhow::Result<Option<CursorState>> {
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
    // A single PUT replaces the object atomically.
    let buf = encode_cursor(state)?;

    let path = object_store::path::Path::from(key);
    let payload = object_store::PutPayload::from(Bytes::from(buf));
    let opts = crate::writer::s3_put_options("");
    client.put_opts(&path, payload, opts).await?;
    info!(key = %key, "saved cursor.parquet to S3");
    Ok(())
}

/// Load cursor state from S3.
///
/// Same contract as [`load_cursor_parquet`]: only a missing object, or one
/// with no row or an empty cursor, is `Ok(None)`. Access errors (403, 5xx,
/// timeouts) and unreadable objects are errors.
async fn load_cursor_parquet_s3(
    client: &dyn ObjectStore,
    key: &str,
) -> anyhow::Result<Option<CursorState>> {
    let path = object_store::path::Path::from(key);
    let result = match client.get(&path).await {
        Ok(r) => r,
        Err(object_store::Error::NotFound { .. }) => {
            info!(key = %key, "cursor.parquet not found in S3, starting fresh");
            return Ok(None);
        }
        Err(e) => {
            return Err(anyhow::Error::new(e).context(format!("reading S3 cursor object {key}")));
        }
    };
    let bytes = result
        .bytes()
        .await
        .with_context(|| format!("reading S3 cursor object {key}"))?;

    let state = parse_cursor(bytes).with_context(|| format!("parsing S3 cursor object {key}"))?;
    match &state {
        Some(state) => info!(
            key = %key,
            cursor = %state.cursor,
            last_block_num = state.last_block_num,
            "loaded cursor from S3 cursor.parquet"
        ),
        None => info!(key = %key, "S3 cursor.parquet has no cursor, starting fresh"),
    }
    Ok(state)
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
        meta.add(CURSOR_METADATA_EXTENDED, "true");
        meta.add(CURSOR_METADATA_FINAL_BLOCKS_ONLY, "true");
        meta.add(CURSOR_METADATA_INCLUDE_FAILED_TRANSACTIONS, "false");
        meta
    }

    fn metadata_with_entries(updates: &[(&str, &str)], removals: &[&str]) -> ParquetFileMetadata {
        let mut meta = test_file_metadata();
        meta.entries
            .retain(|(key, _)| !removals.iter().any(|removal| removal == key));

        for (key, value) in updates {
            if let Some((_, existing)) = meta
                .entries
                .iter_mut()
                .find(|(existing, _)| existing == key)
            {
                *existing = value.to_string();
            } else {
                meta.add((*key).to_string(), (*value).to_string());
            }
        }

        meta
    }

    fn save_legacy_cursor_parquet(
        path: &Path,
        metadata: ParquetFileMetadata,
    ) -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("cursor", DataType::Utf8, false),
            Field::new("last_block_num", DataType::UInt64, false),
            Field::new("last_block_id", DataType::Binary, false),
            Field::new("updated_at", DataType::Utf8, false),
            Field::new("start_block", DataType::UInt64, true),
            Field::new("stop_block", DataType::UInt64, true),
            Field::new("extended", DataType::Boolean, false),
            Field::new("final_blocks_only", DataType::Boolean, false),
            Field::new("include_failed_transactions", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["legacy-cursor"])),
                Arc::new(UInt64Array::from(vec![123_u64])),
                Arc::new(BinaryArray::from_vec(vec![b"\xaa\xbb".as_slice()])),
                Arc::new(StringArray::from(vec!["2025-01-15T12:00:00Z"])),
                Arc::new(UInt64Array::from(vec![Some(100_u64)])),
                Arc::new(UInt64Array::from(vec![Some(200_u64)])),
                Arc::new(arrow::array::BooleanArray::from(vec![true])),
                Arc::new(arrow::array::BooleanArray::from(vec![false])),
                Arc::new(arrow::array::BooleanArray::from(vec![true])),
            ],
        )?;
        let kvs: Vec<KeyValue> = metadata
            .entries
            .into_iter()
            .map(|(key, value)| KeyValue::new(key, Some(value)))
            .collect();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(kvs))
            .build();
        let file = fs::File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    #[test]
    fn test_save_and_load_cursor_parquet() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);

        let state = CursorState {
            cursor: "cursor123".to_string(),
            last_block_num: 42000,
            last_block_id: b"\xab\xcd\xef".to_vec(),
            last_timestamp: Some(1_700_000_000),
            updated_at: "2025-01-15T12:00:00Z".to_string(),
            start_block: Some(100),
            stop_block: Some(200),
            extended: true,
            final_blocks_only: true,
            include_failed_transactions: false,
            file_metadata: test_file_metadata(),
        };

        save_cursor_parquet(&path, &state).unwrap();
        let loaded = load_cursor_parquet(&path)
            .unwrap()
            .expect("should load cursor");

        assert_eq!(loaded.cursor, "cursor123");
        assert_eq!(loaded.last_block_num, 42000);
        assert_eq!(loaded.last_block_id, b"\xab\xcd\xef".to_vec());
        assert_eq!(loaded.last_timestamp, Some(1_700_000_000));
        assert_eq!(loaded.updated_at, "2025-01-15T12:00:00Z");
        assert_eq!(loaded.start_block, Some(100));
        assert_eq!(loaded.stop_block, Some(200));
        assert!(loaded.extended);
        assert!(loaded.final_blocks_only);
        assert!(!loaded.include_failed_transactions);

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
        assert_eq!(meta_map.get(CURSOR_METADATA_EXTENDED), Some(&"true"));
        assert_eq!(
            meta_map.get(CURSOR_METADATA_FINAL_BLOCKS_ONLY),
            Some(&"true")
        );
        assert_eq!(
            meta_map.get(CURSOR_METADATA_INCLUDE_FAILED_TRANSACTIONS),
            Some(&"false")
        );
        assert_eq!(meta_map.get(CURSOR_METADATA_SYNTHETIC_TIMESTAMPS), None);
        assert_eq!(
            meta_map.get(CURSOR_METADATA_SYNTHETIC_TIMESTAMP_POLICY),
            None
        );
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

        let loaded = load_cursor_parquet(&path)
            .unwrap()
            .expect("should load cursor");
        assert_eq!(loaded.cursor, "second");
        assert_eq!(loaded.last_block_num, 200);
    }

    #[test]
    fn test_load_cursor_parquet_missing_file() {
        let dir = TempDir::new().unwrap();
        let result = load_cursor_parquet(&dir.path().join("missing").join(CURSOR_PARQUET_FILENAME));
        assert!(result.unwrap().is_none());
    }

    fn saved_cursor_bytes() -> Vec<u8> {
        encode_cursor(&CursorState {
            cursor: "cursor-to-damage".to_string(),
            last_block_num: 42,
            file_metadata: test_file_metadata(),
            ..CursorState::default()
        })
        .unwrap()
    }

    #[test]
    fn test_load_cursor_parquet_corrupt_file_is_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);
        fs::write(&path, b"definitely not parquet").unwrap();

        let error = load_cursor_parquet(&path).unwrap_err();
        assert!(
            format!("{error:#}").contains(&path.display().to_string()),
            "error should name the cursor file: {error:#}"
        );
    }

    #[test]
    fn test_load_cursor_parquet_truncated_file_is_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);
        let bytes = saved_cursor_bytes();
        fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();

        assert!(load_cursor_parquet(&path).is_err());
    }

    #[test]
    fn test_load_cursor_parquet_zero_byte_file_is_error() {
        // A zero-byte file is what a crash mid-save used to leave behind, so
        // it must not be mistaken for "no cursor".
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);
        fs::write(&path, b"").unwrap();

        assert!(load_cursor_parquet(&path).is_err());
    }

    #[test]
    fn test_load_cursor_parquet_without_cursor_column_is_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_num",
            DataType::UInt64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![1_u64]))]).unwrap();
        let mut writer =
            ArrowWriter::try_new(fs::File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        assert!(load_cursor_parquet(&path).is_err());
    }

    #[test]
    fn test_load_cursor_parquet_unreadable_path_is_error() {
        // A directory where the cursor file belongs cannot be read as a file,
        // even when running as root.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);
        fs::create_dir(&path).unwrap();

        assert!(load_cursor_parquet(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn test_load_cursor_parquet_permission_denied_is_error() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);
        fs::write(&path, saved_cursor_bytes()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
        if fs::read(&path).is_ok() {
            // Running as root: permissions are not enforced.
            return;
        }

        assert!(load_cursor_parquet(&path).is_err());
    }

    #[test]
    fn test_save_cursor_parquet_is_atomic_and_leaves_no_temp_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);

        for (cursor, block) in [("first", 100_u64), ("second", 200_u64)] {
            save_cursor_parquet(
                &path,
                &CursorState {
                    cursor: cursor.to_string(),
                    last_block_num: block,
                    ..CursorState::default()
                },
            )
            .unwrap();
        }

        let entries: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(
            entries,
            vec![std::ffi::OsString::from(CURSOR_PARQUET_FILENAME)]
        );
        let loaded = load_cursor_parquet(&path).unwrap().unwrap();
        assert_eq!(loaded.cursor, "second");
        assert_eq!(loaded.last_block_num, 200);
    }

    #[test]
    fn test_failed_cursor_save_keeps_previous_cursor() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);
        let previous = CursorState {
            cursor: "previous".to_string(),
            last_block_num: 100,
            ..CursorState::default()
        };
        save_cursor_parquet(&path, &previous).unwrap();

        // Block the temp file so the next save fails before the rename.
        fs::create_dir(temp_cursor_path(&path)).unwrap();
        let next = CursorState {
            cursor: "next".to_string(),
            last_block_num: 200,
            ..CursorState::default()
        };
        assert!(save_cursor_parquet(&path, &next).is_err());

        let loaded = load_cursor_parquet(&path).unwrap().unwrap();
        assert_eq!(loaded.cursor, "previous");
        assert_eq!(loaded.last_block_num, 100);
    }

    #[tokio::test]
    async fn test_load_cursor_parquet_s3_round_trip_and_missing_object() {
        let store = object_store::memory::InMemory::new();
        let key = "output/mainnet/cursor.parquet";
        assert!(load_cursor_parquet_s3(&store, key).await.unwrap().is_none());

        let state = CursorState {
            cursor: "s3-cursor".to_string(),
            last_block_num: 77,
            ..CursorState::default()
        };
        save_cursor_parquet_s3(&store, key, &state).await.unwrap();
        let loaded = load_cursor_parquet_s3(&store, key).await.unwrap().unwrap();
        assert_eq!(loaded.cursor, "s3-cursor");
        assert_eq!(loaded.last_block_num, 77);
    }

    #[tokio::test]
    async fn test_load_cursor_parquet_s3_corrupt_object_is_error() {
        let store = object_store::memory::InMemory::new();
        let key = "output/mainnet/cursor.parquet";
        let bytes = saved_cursor_bytes();
        store
            .put(
                &object_store::path::Path::from(key),
                Bytes::from(bytes[..bytes.len() / 2].to_vec()).into(),
            )
            .await
            .unwrap();

        let error = load_cursor_parquet_s3(&store, key).await.unwrap_err();
        assert!(format!("{error:#}").contains(key), "{error:#}");
    }

    #[tokio::test]
    async fn test_load_cursor_parquet_s3_access_error_is_error() {
        // Any error other than NotFound (403, 5xx, timeouts) must not look
        // like a missing cursor. A key below a regular file makes the local
        // object store fail with a non-NotFound error.
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("blocker"), b"").unwrap();
        let store = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();

        assert!(load_cursor_parquet_s3(&store, "blocker/cursor.parquet")
            .await
            .is_err());
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
        assert!(load_cursor_parquet(&path).unwrap().is_none());
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
        let loaded = load_cursor_parquet(&path)
            .unwrap()
            .expect("should load cursor");
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
    fn test_cursor_location_local_save_and_load_under_output_root() {
        let dir = TempDir::new().unwrap();
        let output_root = dir.path().join("output").join("mainnet");
        let location = CursorLocation::resolve(
            output_root.to_string_lossy().as_ref(),
            CURSOR_PARQUET_FILENAME,
            None,
        )
        .expect("local relative cursor path should resolve");

        let state = CursorState {
            cursor: "cursor-123".to_string(),
            last_block_num: 42,
            ..CursorState::default()
        };

        location.save(&state).expect("cursor save should succeed");

        match &location {
            CursorLocation::Local(path) => assert!(path.exists()),
            CursorLocation::S3 { .. } => panic!("expected local cursor location"),
        }

        let loaded = location
            .load()
            .unwrap()
            .expect("cursor load should succeed");
        assert_eq!(loaded.cursor, state.cursor);
        assert_eq!(loaded.last_block_num, state.last_block_num);
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
        let loaded = load_cursor_parquet(&path)
            .unwrap()
            .expect("should load cursor");
        assert_eq!(loaded.start_block, None);
        assert_eq!(loaded.stop_block, None);
        assert_eq!(loaded.last_timestamp, None);
    }

    #[test]
    fn test_cursor_parquet_schema_keeps_only_resume_row_fields() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);
        let state = CursorState {
            cursor: "schema-test".to_string(),
            last_timestamp: Some(1_700_000_000),
            file_metadata: test_file_metadata(),
            ..CursorState::default()
        };

        save_cursor_parquet(&path, &state).unwrap();

        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&path).unwrap()).unwrap();
        let fields: Vec<String> = reader_builder
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        assert_eq!(
            fields,
            vec![
                "cursor".to_string(),
                "last_block_num".to_string(),
                "last_block_id".to_string(),
                "last_timestamp".to_string(),
                "updated_at".to_string(),
                "start_block".to_string(),
                "stop_block".to_string(),
            ]
        );
    }

    #[test]
    fn test_load_legacy_cursor_without_last_timestamp() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CURSOR_PARQUET_FILENAME);
        let legacy_metadata = metadata_with_entries(
            &[],
            &[
                CURSOR_METADATA_EXTENDED,
                CURSOR_METADATA_FINAL_BLOCKS_ONLY,
                CURSOR_METADATA_INCLUDE_FAILED_TRANSACTIONS,
            ],
        );

        save_legacy_cursor_parquet(&path, legacy_metadata).unwrap();
        let loaded = load_cursor_parquet(&path)
            .unwrap()
            .expect("should load legacy cursor");

        assert_eq!(loaded.cursor, "legacy-cursor");
        assert_eq!(loaded.last_block_num, 123);
        assert_eq!(loaded.last_block_id, b"\xaa\xbb".to_vec());
        assert_eq!(loaded.last_timestamp, None);
        assert_eq!(loaded.start_block, Some(100));
        assert_eq!(loaded.stop_block, Some(200));
        assert!(loaded.extended);
        assert!(!loaded.final_blocks_only);
        assert!(loaded.include_failed_transactions);
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
        let loaded = load_cursor_parquet(&path)
            .unwrap()
            .expect("should load cursor");
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
            } else if entry.0 == CURSOR_METADATA_EXTENDED {
                entry.1 = "false".to_string();
            } else if entry.0 == CURSOR_METADATA_INCLUDE_FAILED_TRANSACTIONS {
                entry.1 = "true".to_string();
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
    fn test_validate_params_accepts_legacy_auto_bytes_encoding_metadata_when_compatible() {
        let stored_meta = metadata_with_entries(
            &[
                ("firehose-parquet.block_type", "solana"),
                ("firehose-parquet.chain_name", "solana-mainnet-beta"),
                ("firehose-parquet.chain_name_aliases", "solana"),
                ("firehose-parquet.block_id_encoding", "base58"),
                ("firehose-parquet.bytes_encoding", "auto"),
            ],
            &[],
        );
        let current_meta = metadata_with_entries(
            &[
                ("firehose-parquet.block_type", "solana"),
                ("firehose-parquet.chain_name", "solana-mainnet-beta"),
                ("firehose-parquet.chain_name_aliases", "solana"),
                ("firehose-parquet.block_id_encoding", "base58"),
                ("firehose-parquet.bytes_encoding", "base58"),
            ],
            &[],
        );
        let state = CursorState {
            cursor: "c1".to_string(),
            start_block: Some(100),
            stop_block: Some(200),
            extended: true,
            final_blocks_only: true,
            include_failed_transactions: false,
            file_metadata: stored_meta,
            ..CursorState::default()
        };
        let current = CursorState {
            start_block: Some(100),
            stop_block: Some(200),
            extended: true,
            final_blocks_only: true,
            include_failed_transactions: false,
            file_metadata: current_meta,
            ..CursorState::default()
        };

        assert!(state.validate_params(&current).is_empty());
    }

    #[test]
    fn test_validate_params_reports_legacy_auto_bytes_encoding_mismatch() {
        let stored_meta = metadata_with_entries(
            &[
                ("firehose-parquet.block_type", "solana"),
                ("firehose-parquet.chain_name", "solana-mainnet-beta"),
                ("firehose-parquet.chain_name_aliases", "solana"),
                ("firehose-parquet.block_id_encoding", "base58"),
                ("firehose-parquet.bytes_encoding", "auto"),
            ],
            &[],
        );
        let current_meta = metadata_with_entries(
            &[
                ("firehose-parquet.block_type", "evm"),
                ("firehose-parquet.chain_name", "eth-mainnet"),
                ("firehose-parquet.chain_name_aliases", "ethereum,eth"),
                ("firehose-parquet.block_id_encoding", "hex_0x"),
                ("firehose-parquet.bytes_encoding", "hex"),
            ],
            &[],
        );
        let state = CursorState {
            cursor: "c1".to_string(),
            start_block: Some(100),
            stop_block: Some(200),
            extended: true,
            final_blocks_only: true,
            include_failed_transactions: false,
            file_metadata: stored_meta,
            ..CursorState::default()
        };
        let current = CursorState {
            start_block: Some(100),
            stop_block: Some(200),
            extended: true,
            final_blocks_only: true,
            include_failed_transactions: false,
            file_metadata: current_meta,
            ..CursorState::default()
        };

        assert_eq!(
            state.validate_params(&current),
            vec!["bytes_encoding: cursor=\"base58\" vs current=\"hex\"".to_string()]
        );
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
