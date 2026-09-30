//! Preparation and single-part publication for owned ingestion transactions.
//!
//! The controller persists Writing before
//! staging, persists each receipt before final publication, and retains every batch
//! until its all-table commit is acknowledged. These primitives do not advance a
//! cursor, recover a transaction, retire batches, or make independent writes atomic.

use super::{local, ParquetFileMetadata};
use crate::config::{BlockMetadata, Compression};
use crate::dataset_lock::LocalOwnership;
use crate::dataset_lock_s3::{
    condition_refusal, provider_rejection, reconcile_by_readback, usable_version, Observed,
    ProviderRejected, S3Ownership,
};
use anyhow::{bail, ensure, Context, Result};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, PutMode, UpdateVersion};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

mod budget;
mod verification;
pub(crate) use budget::{InflightBudget, Reservation};

const SCHEMA_DOMAIN: &[u8] = b"fireparq-arrow-schema-json-v1\0";
const FOOTER_PREFIX: &str = "fireparq.ingest.";
const DATA_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_FOOTER_BYTES: u64 = 32 * 1024 * 1024;
const ROW_GROUP_MEMORY_BYTES: usize = 32 * 1024 * 1024;

/// Versioned schema digest, preserving ordered fields and recursively sorting
/// metadata/object keys. A future Arrow serialization change requires a new
/// schema epoch; hashes never depend on Rust Debug or HashMap iteration order.
pub fn schema_sha256(schema: &Schema) -> Result<String> {
    // IPC assigns dictionary IDs while serializing each schema. Those IDs may
    // differ from the mapper's defaults and do not describe table semantics.
    // Rebuild typed fields recursively so only these transport IDs become zero;
    // arbitrary user metadata (even a key named "dict_id") remains untouched.
    fn field(value: &Field) -> Field {
        Field::new(
            value.name(),
            data_type(value.data_type()),
            value.is_nullable(),
        )
        .with_metadata(value.metadata().clone())
        .with_dict_is_ordered(value.dict_is_ordered().unwrap_or(false))
    }
    fn data_type(value: &DataType) -> DataType {
        match value {
            DataType::List(inner) => DataType::List(Arc::new(field(inner))),
            DataType::ListView(inner) => DataType::ListView(Arc::new(field(inner))),
            DataType::LargeList(inner) => DataType::LargeList(Arc::new(field(inner))),
            DataType::LargeListView(inner) => DataType::LargeListView(Arc::new(field(inner))),
            DataType::FixedSizeList(inner, size) => {
                DataType::FixedSizeList(Arc::new(field(inner)), *size)
            }
            DataType::Struct(fields) => DataType::Struct(fields.iter().map(|f| field(f)).collect()),
            DataType::Union(fields, mode) => DataType::Union(
                fields
                    .iter()
                    .map(|(id, f)| (id, Arc::new(field(f))))
                    .collect(),
                *mode,
            ),
            DataType::Dictionary(key, value) => {
                DataType::Dictionary(Box::new(data_type(key)), Box::new(data_type(value)))
            }
            DataType::Map(inner, ordered) => DataType::Map(Arc::new(field(inner)), *ordered),
            DataType::RunEndEncoded(run_ends, values) => {
                DataType::RunEndEncoded(Arc::new(field(run_ends)), Arc::new(field(values)))
            }
            _ => value.clone(),
        }
    }
    fn canonical(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(values) => {
                let sorted: BTreeMap<_, _> = values
                    .into_iter()
                    .map(|(key, value)| (key, canonical(value)))
                    .collect();
                serde_json::Value::Object(sorted.into_iter().collect())
            }
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(canonical).collect())
            }
            value => value,
        }
    }
    let normalized = Schema::new_with_metadata(
        schema.fields().iter().map(|f| field(f)).collect::<Vec<_>>(),
        schema.metadata().clone(),
    );
    let value = canonical(serde_json::to_value(normalized).context("encoding Arrow schema")?);
    let mut hasher = Sha256::new();
    hasher.update(SCHEMA_DOMAIN);
    hasher.update(serde_json::to_vec(&value)?);
    Ok(hex::encode(hasher.finalize()))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedPart {
    pub table: String,
    pub final_relative_path: String,
    pub temporary_relative_path: String,
    pub schema_sha256: String,
    pub transaction_id: String,
    pub stream_id: String,
    pub first_ordinal: u64,
    pub last_ordinal: u64,
    pub entry_index: u32,
    pub row_count: u64,
}

/// Safe journal metadata; contains neither data bytes nor an opaque cursor.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PartReceipt {
    pub byte_size: u64,
    pub sha256: String,
    pub row_count: u64,
    pub schema_sha256: String,
}

/// Missing is only an authoritative NotFound. Present includes exact identity,
/// content and local durability verification; every other condition is an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartPresence {
    Missing,
    Present,
}

/// Retains the entire flush until its controller acknowledges all-table commit.
/// Encoding borrows it and never drains, removes or acknowledges individual data.
pub(crate) struct PreparedFlush {
    batches: HashMap<String, RecordBatch>,
    parts: Vec<PlannedPart>,
    compression: Compression,
    file_metadata: ParquetFileMetadata,
}

/// An encoder stopped because the in-flight budget could not grow its
/// reservation. Nothing was staged or published; the caller may encode the
/// same deterministic part again with an exclusive reservation.
#[derive(Debug)]
pub(crate) struct ReservationExceeded {
    pub reservation: u64,
}
impl std::fmt::Display for ReservationExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "encoded part could not grow its {}-byte in-flight reservation",
            self.reservation
        )
    }
}
impl std::error::Error for ReservationExceeded {}

/// Initial reservation floor: small tables still carry a footer, schema hint
/// and page headers that can exceed their Arrow buffers.
const MIN_PART_RESERVATION: u64 = 256 * 1024;

/// Complete bytes for one table only. No Debug implementation prints payloads.
pub(crate) struct EncodedPart {
    plan: PlannedPart,
    receipt: PartReceipt,
    /// The part's Delta `add.stats` JSON, from the encoded batch (#643 L3).
    stats: String,
    bytes: Bytes,
    // Native S3 owns a private disk spool instead of compressed heap bytes.
    spool: Option<File>,
    // Held only for its Drop: released after publication or on any failure.
    _reservation: Option<Reservation>,
}

impl EncodedPart {
    pub(crate) fn receipt(&self) -> &PartReceipt {
        &self.receipt
    }
    /// Encoded bytes held in memory or in the private spool.
    pub(crate) fn encoded_bytes(&self) -> u64 {
        self.receipt.byte_size
    }
    pub(crate) fn plan(&self) -> &PlannedPart {
        &self.plan
    }
    /// The Delta `add.stats` JSON of exactly the encoded rows.
    pub(crate) fn delta_stats(&self) -> &str {
        &self.stats
    }
}

impl PreparedFlush {
    pub(crate) fn new(
        batches: HashMap<String, RecordBatch>,
        inventory: &BTreeMap<String, String>,
        parts: Vec<PlannedPart>,
        metadata: BlockMetadata,
        compression: Compression,
        file_metadata: ParquetFileMetadata,
    ) -> Result<Self> {
        ensure!(
            !inventory.is_empty(),
            "protected inventory must include every declared table"
        );
        ensure!(
            file_metadata
                .entries
                .iter()
                .all(|(key, _)| !key.starts_with(FOOTER_PREFIX)),
            "caller metadata collides with protected footer keys"
        );
        for (table, digest) in inventory {
            validate_table(table)?;
            validate_digest(digest)?;
        }
        for (table, batch) in &batches {
            let expected = inventory
                .get(table)
                .context("batch table is absent from protected inventory")?;
            ensure!(
                schema_sha256(batch.schema().as_ref())? == *expected,
                "batch schema differs from protected inventory"
            );
        }
        let mut previous_table: Option<&str> = None;
        for plan in &parts {
            validate_plan(plan)?;
            let index = inventory
                .keys()
                .position(|table| table == &plan.table)
                .context("planned table is absent from protected inventory")?;
            ensure!(
                usize::try_from(plan.entry_index)? == index,
                "planned entry index must match the full sorted table inventory"
            );
            ensure!(
                previous_table.is_none_or(|previous| previous < plan.table.as_str()),
                "planned tables must be unique and sorted"
            );
            previous_table = Some(&plan.table);
            if let Some(first) = parts.first() {
                ensure!(
                    plan.stream_id == first.stream_id
                        && plan.transaction_id == first.transaction_id
                        && plan.first_ordinal == first.first_ordinal
                        && plan.last_ordinal == first.last_ordinal,
                    "planned parts disagree on transaction identity"
                );
            }
            ensure!(
                inventory.get(&plan.table) == Some(&plan.schema_sha256),
                "planned schema differs from protected inventory"
            );
            let batch = batches
                .get(&plan.table)
                .context("nonempty planned table is missing its batch")?;
            ensure!(
                u64::try_from(batch.num_rows())? == plan.row_count,
                "planned table row count differs from batch"
            );
            super::validate_partition(&plan.table, batch, &metadata)?;
            let directory = super::partition_suffix(&plan.table, &metadata)?;
            ensure!(
                plan.final_relative_path == format!("{directory}/{}", final_name(plan))
                    && plan.temporary_relative_path
                        == format!("{directory}/{}", temporary_name(plan)),
                "planned paths disagree with validated table partition"
            );
        }
        for (table, batch) in &batches {
            ensure!(
                batch.num_rows() == 0 || parts.iter().any(|part| part.table == *table),
                "nonempty batch has no planned part"
            );
        }
        // Inventory entries without a part explicitly declare zero rows. Missing
        // or empty batches are accepted only in that declaration.
        Ok(Self {
            batches,
            parts,
            compression,
            file_metadata,
        })
    }

    pub(crate) fn parts(&self) -> &[PlannedPart] {
        &self.parts
    }

    /// Initial reservation for one part: the codec's typical compressed share
    /// of the batch's Arrow buffers plus a footer floor. Encoders grow it while
    /// writing; one whose growth the budget refuses stops with
    /// [`ReservationExceeded`] instead of exceeding the budget.
    pub(crate) fn estimated_part_bytes(&self, entry_index: u32) -> Result<u64> {
        let plan = self
            .parts
            .iter()
            .find(|part| part.entry_index == entry_index)
            .context("unknown protected part index")?;
        let batch = self
            .batches
            .get(&plan.table)
            .context("prepared batch is missing")?;
        let arrow = batch.get_array_memory_size() as f64;
        let ratio = super::compression_ratio(&self.compression);
        Ok(((arrow * ratio) as u64).saturating_add(MIN_PART_RESERVATION))
    }

    pub(crate) fn encode_spooled(
        &self,
        entry_index: u32,
        reservation: Option<Reservation>,
    ) -> Result<EncodedPart> {
        let plan = self
            .parts
            .iter()
            .find(|part| part.entry_index == entry_index)
            .context("unknown protected part index")?;
        let batch = self
            .batches
            .get(&plan.table)
            .context("prepared batch is missing")?;
        let mut metadata = self.file_metadata.clone();
        metadata.entries.extend(footer_identity(plan));
        let mut spool = SpoolWriter::new(crate::s3::upload::MAX_PART_BYTES, reservation)?;
        let encoded = (|| -> Result<()> {
            let mut parquet = ArrowWriter::try_new(
                &mut spool,
                batch.schema(),
                Some(super::writer_properties(
                    self.compression,
                    batch,
                    &metadata,
                )?),
            )?;
            // A slice shares the already-owned mapper allocation. The separate row
            // group trigger bounds encoder accumulation without creating extra parts.
            for offset in (0..batch.num_rows()).step_by(4096) {
                parquet.write(&batch.slice(offset, (batch.num_rows() - offset).min(4096)))?;
                if parquet.memory_size() >= ROW_GROUP_MEMORY_BYTES {
                    parquet.flush()?;
                }
            }
            parquet.close()?;
            Ok(())
        })();
        if let Err(error) = encoded {
            if let Some(reservation) = spool.reservation_exceeded {
                return Err(anyhow::Error::new(ReservationExceeded { reservation }));
            }
            return Err(error);
        }
        let mut reservation = spool.reservation.take();
        if let Some(reservation) = &mut reservation {
            reservation.shrink_to(spool.size);
        }
        let receipt = PartReceipt {
            byte_size: spool.size,
            sha256: hex::encode(spool.hash.finalize()),
            row_count: plan.row_count,
            schema_sha256: plan.schema_sha256.clone(),
        };
        verify_file(plan, &receipt, &spool.file)?;
        Ok(EncodedPart {
            plan: plan.clone(),
            receipt,
            stats: crate::delta::stats::stats_json(batch)?,
            bytes: Bytes::new(),
            spool: Some(spool.file),
            _reservation: reservation,
        })
    }

    pub(crate) fn encode(
        &self,
        entry_index: u32,
        reservation: Option<Reservation>,
    ) -> Result<EncodedPart> {
        let plan = self
            .parts
            .iter()
            .find(|part| part.entry_index == entry_index)
            .context("unknown protected part index")?;
        let batch = self
            .batches
            .get(&plan.table)
            .context("prepared batch is missing")?;
        let mut metadata = self.file_metadata.clone();
        metadata.entries.extend(footer_identity(plan));
        let mut output = CappedBuffer {
            bytes: Vec::new(),
            reservation,
            exceeded: None,
        };
        let encoded = super::encode_into(&mut output, batch, self.compression, &metadata);
        if let Err(error) = encoded {
            if let Some(reservation) = output.exceeded {
                return Err(anyhow::Error::new(ReservationExceeded { reservation }));
            }
            return Err(error);
        }
        let CappedBuffer {
            bytes,
            mut reservation,
            ..
        } = output;
        if let Some(reservation) = &mut reservation {
            reservation.shrink_to(bytes.len() as u64);
        }
        let receipt = PartReceipt {
            byte_size: u64::try_from(bytes.len())?,
            sha256: hex::encode(Sha256::digest(&bytes)),
            row_count: plan.row_count,
            schema_sha256: plan.schema_sha256.clone(),
        };
        Ok(EncodedPart {
            plan: plan.clone(),
            receipt,
            stats: crate::delta::stats::stats_json(batch)?,
            bytes: Bytes::from(bytes),
            spool: None,
            _reservation: reservation,
        })
    }
}

fn validate_table(table: &str) -> Result<()> {
    ensure!(
        !table.is_empty()
            && table.len() <= 128
            && table
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "invalid protected table name"
    );
    Ok(())
}
fn validate_digest(digest: &str) -> Result<()> {
    ensure!(
        digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "protected digest must be 64 lowercase hexadecimal characters"
    );
    Ok(())
}
fn final_name(plan: &PlannedPart) -> String {
    format!(
        "part-v1-{}-{}-{}-{}-{}.parquet",
        plan.stream_id,
        plan.first_ordinal,
        plan.last_ordinal,
        plan.transaction_id,
        plan.entry_index
    )
}
fn temporary_name(plan: &PlannedPart) -> String {
    format!(
        ".fireparq-txn-{}-{}.tmp",
        plan.transaction_id, plan.entry_index
    )
}
fn validate_plan(plan: &PlannedPart) -> Result<()> {
    validate_table(&plan.table)?;
    for digest in [&plan.schema_sha256, &plan.transaction_id, &plan.stream_id] {
        validate_digest(digest)?;
    }
    ensure!(
        plan.row_count > 0 && plan.first_ordinal > 0 && plan.first_ordinal <= plan.last_ordinal,
        "protected part has invalid rows or accepted ordinal interval"
    );
    let final_path = validate_relative_path(&plan.final_relative_path)?;
    let temporary = validate_relative_path(&plan.temporary_relative_path)?;
    ensure!(
        final_path.parent() == temporary.parent()
            && final_path.file_name().and_then(|n| n.to_str()) == Some(final_name(plan).as_str())
            && temporary.file_name().and_then(|n| n.to_str())
                == Some(temporary_name(plan).as_str()),
        "protected part paths do not match its identity"
    );
    ensure!(
        final_path
            .components()
            .next()
            .and_then(|c| c.as_os_str().to_str())
            == Some(plan.table.as_str()),
        "protected part path is outside its table"
    );
    Ok(())
}
fn validate_relative_path(path: &str) -> Result<&Path> {
    ensure!(
        !path.is_empty()
            && path.len() <= 2048
            && !path.starts_with('/')
            && !path.contains(['\\', '?', '#', '\0'])
            && !path.contains("://")
            && path
                .split('/')
                .all(|c| !c.is_empty() && c != "." && c != "..")
            && !crate::artifacts::is_control_path(path),
        "invalid protected relative path"
    );
    Ok(Path::new(path))
}
fn footer_identity(plan: &PlannedPart) -> Vec<(String, String)> {
    [
        ("stream_id", plan.stream_id.clone()),
        ("transaction_id", plan.transaction_id.clone()),
        ("entry_index", plan.entry_index.to_string()),
        ("first_ordinal", plan.first_ordinal.to_string()),
        ("last_ordinal", plan.last_ordinal.to_string()),
        ("schema_sha256", plan.schema_sha256.clone()),
    ]
    .into_iter()
    .map(|(key, value)| (format!("{FOOTER_PREFIX}{key}"), value))
    .collect()
}

/// Verify a receipt against complete bytes and footer identity, never filename or
/// existence alone. Used by both publication and later journal recovery.
fn verify_bytes(plan: &PlannedPart, receipt: &PartReceipt, bytes: Bytes) -> Result<()> {
    validate_receipt(plan, receipt)?;
    ensure!(
        receipt.row_count == plan.row_count
            && receipt.schema_sha256 == plan.schema_sha256
            && receipt.byte_size == u64::try_from(bytes.len())?
            && receipt.sha256 == hex::encode(Sha256::digest(&bytes)),
        "protected part bytes differ from journal receipt"
    );
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .context("validating protected Parquet footer")?;
    verify_footer(plan, reader.metadata())
}

fn verify_footer(
    plan: &PlannedPart,
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Result<()> {
    let footer = metadata
        .file_metadata()
        .key_value_metadata()
        .context("protected Parquet identity is absent")?;
    // The default reader merges every file-footer key into schema metadata.
    // Digest the physical schema with only its embedded Arrow hint instead, so
    // transaction/operational footer metadata cannot change the mapper schema.
    let arrow_hint: Vec<_> = footer
        .iter()
        .filter(|entry| entry.key == parquet::arrow::ARROW_SCHEMA_META_KEY)
        .cloned()
        .collect();
    ensure!(
        arrow_hint.len() == 1,
        "protected Parquet Arrow schema hint is missing or duplicated"
    );
    let schema = parquet::arrow::parquet_to_arrow_schema(
        metadata.file_metadata().schema_descr(),
        Some(&arrow_hint),
    )?;
    ensure!(
        u64::try_from(metadata.file_metadata().num_rows())? == plan.row_count
            && schema_sha256(&schema)? == plan.schema_sha256,
        "protected Parquet schema or row count differs from plan"
    );
    for (key, value) in footer_identity(plan) {
        let values: Vec<_> = footer.iter().filter(|entry| entry.key == key).collect();
        ensure!(
            values.len() == 1 && values[0].value.as_deref() == Some(value.as_str()),
            "protected Parquet footer identity differs from plan"
        );
    }
    Ok(())
}

/// Hash and footer verification of complete in-memory bytes runs on a blocking
/// worker, so concurrent remote publications are not stalled behind it.
async fn verify_bytes_off_thread(
    plan: &PlannedPart,
    receipt: &PartReceipt,
    bytes: Bytes,
) -> Result<()> {
    let (plan, receipt) = (plan.clone(), receipt.clone());
    tokio::task::spawn_blocking(move || verify_bytes(&plan, &receipt, bytes))
        .await
        .context("protected verification worker failed")?
}

/// The new footer bound applies to native S3 recovery as well as new encoding.
/// Hashing and parsing use disk-backed reads, never a complete object Vec.
fn verify_file(plan: &PlannedPart, receipt: &PartReceipt, source: &File) -> Result<()> {
    verify_file_checked(
        plan,
        receipt,
        source,
        &std::sync::atomic::AtomicBool::new(false),
    )
}
fn verify_file_checked(
    plan: &PlannedPart,
    receipt: &PartReceipt,
    source: &File,
    cancelled: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    validate_receipt(plan, receipt)?;
    let mut file = source.try_clone().context("borrowing protected spool")?;
    ensure!(
        file.metadata()?.is_file() && file.metadata()?.len() == receipt.byte_size,
        "protected spool size differs from receipt"
    );
    ensure!(
        receipt.byte_size >= 12,
        "protected Parquet is shorter than its footer"
    );
    file.seek(SeekFrom::End(-8))?;
    let mut tail = [0u8; 8];
    file.read_exact(&mut tail)?;
    ensure!(
        &tail[4..] == b"PAR1",
        "protected Parquet footer marker is invalid"
    );
    let footer_size = u64::from(u32::from_le_bytes(tail[..4].try_into().unwrap()));
    ensure!(
        footer_size <= MAX_FOOTER_BYTES && footer_size <= receipt.byte_size - 12,
        "protected Parquet footer exceeds the 32 MiB limit or file size"
    );
    file.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut read = 0u64;
    let mut buffer = [0u8; crate::s3::upload::IO_BUFFER_BYTES];
    loop {
        ensure!(
            !cancelled.load(std::sync::atomic::Ordering::SeqCst),
            "protected file verification cancelled"
        );
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        read = read
            .checked_add(count as u64)
            .context("protected read size overflow")?;
        ensure!(
            read <= receipt.byte_size,
            "protected spool exceeds receipt size"
        );
        hash.update(&buffer[..count]);
    }
    ensure!(
        read == receipt.byte_size && hex::encode(hash.finalize()) == receipt.sha256,
        "protected part bytes differ from journal receipt"
    );
    ensure!(
        !cancelled.load(std::sync::atomic::Ordering::SeqCst),
        "protected file verification cancelled"
    );
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("validating protected Parquet footer")?;
    verify_footer(plan, reader.metadata())
}

/// Grow `reservation` to cover `total` bytes, recording a refusal.
fn grow_reservation(
    reservation: &mut Option<Reservation>,
    total: u64,
    exceeded: &mut Option<u64>,
) -> std::io::Result<()> {
    if let Some(reservation) = reservation {
        if !reservation.grow_to(total) {
            *exceeded = Some(reservation.held());
            return Err(std::io::Error::other(
                "encoded part could not grow its in-flight reservation",
            ));
        }
    }
    Ok(())
}

/// In-memory encoder output whose growth is admitted by its reservation.
struct CappedBuffer {
    bytes: Vec<u8>,
    reservation: Option<Reservation>,
    exceeded: Option<u64>,
}
impl Write for CappedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let total = (self.bytes.len() as u64).saturating_add(bytes.len() as u64);
        grow_reservation(&mut self.reservation, total, &mut self.exceeded)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct SpoolWriter {
    file: File,
    size: u64,
    limit: u64,
    reservation: Option<Reservation>,
    reservation_exceeded: Option<u64>,
    hash: Sha256,
}
impl SpoolWriter {
    fn new(limit: u64, reservation: Option<Reservation>) -> Result<Self> {
        Ok(Self {
            file: tempfile::tempfile().context("creating private S3 spool")?,
            size: 0,
            limit,
            reservation,
            reservation_exceeded: None,
            hash: Sha256::new(),
        })
    }
}
impl Write for SpoolWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if (bytes.len() as u64) > self.limit.saturating_sub(self.size) {
            return Err(std::io::Error::other(
                "native S3 part exceeds the single-PUT size limit",
            ));
        }
        grow_reservation(
            &mut self.reservation,
            self.size.saturating_add(bytes.len() as u64),
            &mut self.reservation_exceeded,
        )?;
        let written = self.file.write(bytes)?;
        self.size += written as u64;
        self.hash.update(&bytes[..written]);
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

fn validate_receipt(plan: &PlannedPart, receipt: &PartReceipt) -> Result<()> {
    validate_plan(plan)?;
    validate_digest(&receipt.sha256)?;
    ensure!(
        receipt.byte_size > 0
            && receipt.byte_size < u64::MAX
            && receipt.row_count == plan.row_count
            && receipt.schema_sha256 == plan.schema_sha256,
        "protected receipt differs from its plan"
    );
    Ok(())
}

/// Local roots must already exist under a held exclusive scope. The controller
/// creates its durable initial state before staging any table.
pub(crate) struct LocalPartStore<'a> {
    root: PathBuf,
    ownership: &'a LocalOwnership,
}

impl<'a> LocalPartStore<'a> {
    pub(crate) fn new(root: &Path, ownership: &'a LocalOwnership) -> Result<Self> {
        ownership.revalidate()?;
        let root = fs::canonicalize(root).context("resolving protected part root")?;
        ensure!(
            root.is_dir()
                && ownership
                    .roots()
                    .iter()
                    .any(|scope| root.starts_with(scope)),
            "protected part root is outside held ownership"
        );
        Ok(Self { root, ownership })
    }
    fn path(&self, relative: &str) -> Result<PathBuf> {
        self.ownership.revalidate()?;
        let relative = validate_relative_path(relative)?;
        let path = self.root.join(relative);
        // Existing descendants must never follow nested symlinks, including the
        // leaf. A different borrower of this guard cannot bypass this check.
        let mut current = self.root.clone();
        for component in relative.components() {
            current.push(component);
            match fs::symlink_metadata(&current) {
                Ok(meta) => ensure!(
                    !meta.file_type().is_symlink(),
                    "protected part path contains a symlink"
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error).context("inspecting protected part path"),
            }
        }
        Ok(path)
    }

    /// Resolve an owned path under the control lock. File I/O on the distinct
    /// transaction-owned names afterward runs without it, so parts of one
    /// transaction can stage, publish and verify concurrently; control records
    /// keep their own locked version check.
    fn locked_path(&self, relative: &str) -> Result<PathBuf> {
        let _mutation = self.ownership.lock_control_mutation()?;
        self.path(relative)
    }

    /// Creates only the predetermined temporary name; a collision is an error.
    /// No implicit cleanup on failure: Writing owns this path for reconciliation.
    pub(crate) fn stage(&self, encoded: &EncodedPart) -> Result<()> {
        verify_bytes(&encoded.plan, &encoded.receipt, encoded.bytes.clone())?;
        let path = self.locked_path(&encoded.plan.temporary_relative_path)?;
        local::create_dir_all_durable(path.parent().context("missing staging parent")?)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .context("creating planned staging file")?;
        #[cfg(test)]
        if tests::fails_writing() {
            file.write_all(&encoded.bytes[..encoded.bytes.len().min(16)])?;
            checkpoint(Stage::Writing)?;
        }
        file.write_all(&encoded.bytes)
            .context("writing planned staging file")?;
        checkpoint(Stage::FileSync)?;
        file.sync_all().context("syncing planned staging file")?;
        checkpoint(Stage::StagedDirectorySync)?;
        local::sync_directory(path.parent().unwrap())
    }

    /// The controller must durably record this exact receipt before calling.
    /// A complete final file remains after an unlink/directory-sync error.
    pub(crate) fn publish(&self, plan: &PlannedPart, receipt: &PartReceipt) -> Result<()> {
        let temporary = self.locked_path(&plan.temporary_relative_path)?;
        let final_path = self.locked_path(&plan.final_relative_path)?;
        verify_bytes(
            plan,
            receipt,
            read_local_bounded(&temporary, receipt.byte_size)?,
        )?;
        // Recovery can arrive here after a complete staging write whose fsync
        // failed. Re-establish durability before making its final name visible.
        File::open(&temporary)?
            .sync_all()
            .context("syncing verified staging file")?;
        checkpoint(Stage::Publish)?;
        local::publish_file_no_replace(&temporary, &final_path)?;
        checkpoint(Stage::TemporaryRemoval)?;
        fs::remove_file(&temporary).context("removing published staging name")?;
        checkpoint(Stage::PublishedDirectorySync)?;
        local::sync_directory(final_path.parent().context("missing final parent")?)
    }

    pub(crate) fn verify(
        &self,
        plan: &PlannedPart,
        receipt: &PartReceipt,
        staged: bool,
    ) -> Result<PartPresence> {
        validate_receipt(plan, receipt)?;
        let path = self.locked_path(if staged {
            &plan.temporary_relative_path
        } else {
            &plan.final_relative_path
        })?;
        match fs::symlink_metadata(&path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PartPresence::Missing)
            }
            Err(error) => return Err(error).context("inspecting protected part for recovery"),
        }
        verify_bytes(plan, receipt, read_local_bounded(&path, receipt.byte_size)?)?;
        // A visible complete file after a prior sync error is not yet proof of
        // durability. Recovery must establish that proof before committing state.
        checkpoint(Stage::VerifiedFileSync)?;
        File::open(&path)?
            .sync_all()
            .context("syncing verified protected file")?;
        checkpoint(Stage::VerifiedDirectorySync)?;
        local::create_dir_all_durable(path.parent().context("missing verified part parent")?)?;
        Ok(PartPresence::Present)
    }
}

fn read_local_bounded(path: &Path, expected_size: u64) -> Result<Bytes> {
    let metadata = fs::symlink_metadata(path).context("reading planned part metadata")?;
    ensure!(
        metadata.is_file() && metadata.len() == expected_size,
        "planned part is not a regular file of the expected size"
    );
    let mut bytes = Vec::new();
    File::open(path)?
        .take(expected_size.checked_add(1).context("part size overflow")?)
        .read_to_end(&mut bytes)?;
    Ok(Bytes::from(bytes))
}

/// Conditional S3 data publication tied to the same bucket/store as its owner.
/// The store must use the mutation client's zero transport retries.
pub(crate) struct S3PartStore<'a> {
    ownership: &'a S3Ownership,
    prefix: String,
    cache_control: String,
}
impl<'a> S3PartStore<'a> {
    pub(crate) fn new(ownership: &'a S3Ownership, prefix: &str) -> Result<Self> {
        if !prefix.is_empty() {
            validate_relative_path(prefix)?;
        }
        ensure!(
            ownership
                .record()
                .scopes()
                .iter()
                .any(|scope| scope.is_empty()
                    || prefix == scope
                    || prefix
                        .strip_prefix(scope)
                        .is_some_and(|rest| rest.starts_with('/'))),
            "protected part root is outside declared remote ownership"
        );
        Ok(Self {
            ownership,
            prefix: prefix.to_owned(),
            cache_control: String::new(),
        })
    }
    pub(crate) fn with_cache_control(mut self, cache_control: &str) -> Self {
        self.cache_control = cache_control.to_owned();
        self
    }
    fn key(&self, plan: &PlannedPart) -> Result<ObjectPath> {
        validate_plan(plan)?;
        Ok(ObjectPath::from(if self.prefix.is_empty() {
            plan.final_relative_path.clone()
        } else {
            format!("{}/{}", self.prefix, plan.final_relative_path)
        }))
    }
    /// Checked immediately before each data mutation: the persistent owner
    /// record must still be ours and no earlier attempt may be unresolved. The
    /// control lock orders the uncertainty check against in-process control
    /// mutations but is not held across the owner read or the data request, so
    /// several parts of one transaction can publish concurrently. Any unresolved
    /// attempt marks the owner uncertain and every later gate refuses to start.
    /// Only acquisition and release rewrite the owner record, and release needs
    /// exclusive ownership of the guard these publications borrow.
    async fn publication_gate(&self) -> Result<()> {
        {
            let _mutation = self.ownership.lock_control_mutation().await;
            ensure!(
                !self.ownership.is_mutation_uncertain(),
                "unresolved remote mutation requires quiescent recovery"
            );
        }
        ensure!(
            S3Ownership::status(self.ownership.object_store())
                .await?
                .as_ref()
                == Some(self.ownership.record()),
            "remote ownership changed before part publication"
        );
        ensure!(
            !self.ownership.is_mutation_uncertain(),
            "unresolved remote mutation requires quiescent recovery"
        );
        Ok(())
    }

    /// Single Create attempt. Even an acknowledged object must match exact bytes,
    /// version and footer before the attempt is resolved. Any error/cancellation
    /// permanently retains Owned; only provider-quiescent recovery can retry.
    pub(crate) async fn publish(&self, encoded: &EncodedPart) -> Result<()> {
        if let Some(native) = self.ownership.native_upload() {
            return self.publish_native(native, encoded).await;
        }
        ensure!(
            encoded.spool.is_none(),
            "spooled part requires its native upload capability"
        );
        ensure!(
            !self.ownership.is_mutation_uncertain(),
            "unresolved remote mutation requires quiescent recovery"
        );
        verify_bytes_off_thread(&encoded.plan, &encoded.receipt, encoded.bytes.clone()).await?;
        let key = self.key(&encoded.plan)?;
        self.publication_gate().await?;
        let mut attempt = MutationAttempt {
            ownership: self.ownership,
            resolved: false,
        };
        let mut options = super::s3_put_options(&self.cache_control);
        options.mode = PutMode::Create;
        let result = tokio::time::timeout(
            DATA_TIMEOUT,
            self.ownership
                .object_store()
                .put_opts(&key, encoded.bytes.clone().into(), options),
        )
        .await;
        let failure = match result {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => {
                if let Some(rejected) = provider_rejection(&error) {
                    // A refused request cannot complete later: resolved, not uncertain.
                    attempt.resolved = true;
                    return Err(anyhow::Error::new(rejected).context("publishing a protected part"));
                }
                if condition_refusal(&error) {
                    // Another request wrote the name: nothing to read back.
                    bail!("protected part conditional upload failed; retain ownership for quiescent recovery")
                }
                Err("protected part conditional upload failed")
            }
            Err(_) => Err("protected part upload timed out"),
        };
        let result = match failure {
            Ok(result) => result,
            Err(failure) => {
                // #646: the provider may have stored the part. Only the exact
                // part, read back and verified, proves it; absence never does.
                if reconcile_by_readback(|| self.observe_part(&key, encoded))
                    .await
                    .is_some()
                {
                    attempt.resolved = true;
                    tracing::warn!(
                        table = %encoded.plan.table,
                        "{failure}, but the exact part is stored: its publication is proven"
                    );
                    return Ok(());
                }
                bail!("{failure}; retain ownership for quiescent recovery")
            }
        };
        let version = UpdateVersion {
            e_tag: result.e_tag,
            version: result.version,
        };
        ensure!(
            usable_version(&version),
            "protected part upload returned no usable version"
        );
        let (bytes, observed) = self
            .read(&key, &encoded.receipt)
            .await?
            .context("acknowledged protected part is absent")?;
        ensure!(
            version == observed,
            "protected part version changed during publication"
        );
        verify_bytes_off_thread(&encoded.plan, &encoded.receipt, bytes).await?;
        attempt.resolved = true;
        Ok(())
    }
    /// Read-only; it takes no control lock so final verification of a
    /// transaction's parts can overlap.
    pub(crate) async fn verify(
        &self,
        plan: &PlannedPart,
        receipt: &PartReceipt,
    ) -> Result<PartPresence> {
        validate_receipt(plan, receipt)?;
        if self.ownership.native_upload().is_some() {
            return self.verify_native(plan, receipt, None).await;
        }
        let Some((bytes, _)) = self.read(&self.key(plan)?, receipt).await? else {
            return Ok(PartPresence::Missing);
        };
        verify_bytes_off_thread(plan, receipt, bytes).await?;
        Ok(PartPresence::Present)
    }
    async fn publish_native(
        &self,
        native: &crate::s3::upload::NativeS3Upload,
        encoded: &EncodedPart,
    ) -> Result<()> {
        ensure!(
            !self.ownership.is_mutation_uncertain(),
            "unresolved remote mutation requires quiescent recovery"
        );
        let spool = encoded
            .spool
            .as_ref()
            .context("native S3 publication requires a disk spool")?;
        tokio::time::timeout(
            crate::s3::upload::DATA_TIMEOUT,
            verification::verify(
                encoded.plan.clone(),
                encoded.receipt.clone(),
                spool.try_clone()?,
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("verifying native upload spool timed out"))??;
        let key = self.key(&encoded.plan)?;
        self.publication_gate().await?;
        let upload = native
            .prepare(
                &key,
                spool.try_clone()?,
                encoded.receipt.byte_size,
                &self.cache_control,
            )
            .await?;
        let mut attempt = MutationAttempt {
            ownership: self.ownership,
            resolved: false,
        };
        let sent = tokio::time::timeout(crate::s3::upload::DATA_TIMEOUT, upload.send()).await;
        let failure = match sent {
            Ok(Ok(version)) => Ok(version),
            Ok(Err(error)) if error.is::<ProviderRejected>() => {
                // A refused request cannot complete later: resolved, not uncertain.
                attempt.resolved = true;
                return Err(error.context("publishing a protected part"));
            }
            // Another request wrote the name: nothing to read back.
            Ok(Err(error)) if error.is::<crate::s3::upload::ConditionRefused>() => {
                return Err(error)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(anyhow::anyhow!(
                "native part upload timed out; retain ownership"
            )),
        };
        let version = match failure {
            Ok(version) => version,
            Err(failure) => {
                // #646: the provider may have stored the part. Only the exact
                // part, read back and verified, proves it; absence never does.
                let observe = || async {
                    match self
                        .verify_native(&encoded.plan, &encoded.receipt, None)
                        .await
                    {
                        Ok(PartPresence::Present) => Observed::Exact(()),
                        Ok(PartPresence::Missing) => Observed::Pending,
                        // Stored bytes that fail verification are not this part.
                        Err(_) if self.native_part_present(&encoded.plan).await => {
                            Observed::Refuted
                        }
                        Err(_) => Observed::Pending,
                    }
                };
                if reconcile_by_readback(observe).await.is_some() {
                    attempt.resolved = true;
                    tracing::warn!(
                        table = %encoded.plan.table,
                        "{failure:#}, but the exact part is stored: its publication is proven"
                    );
                    return Ok(());
                }
                return Err(failure);
            }
        };
        ensure!(
            self.verify_native(&encoded.plan, &encoded.receipt, Some(version))
                .await?
                == PartPresence::Present,
            "acknowledged protected part is absent"
        );
        attempt.resolved = true;
        Ok(())
    }

    /// One #646 readback of an unacknowledged `object_store` part publication.
    async fn observe_part(&self, key: &ObjectPath, encoded: &EncodedPart) -> Observed<()> {
        match self.read(key, &encoded.receipt).await {
            Ok(Some((bytes, _))) => {
                match verify_bytes_off_thread(&encoded.plan, &encoded.receipt, bytes).await {
                    Ok(()) => Observed::Exact(()),
                    // Other contents occupy the name: this Create cannot land.
                    Err(_) => Observed::Refuted,
                }
            }
            Ok(None) => Observed::Pending,
            // A failed read, or an object of another size under the name.
            Err(_) => Observed::Pending,
        }
    }

    /// Whether an object exists under the part's name, whatever its contents.
    async fn native_part_present(&self, plan: &PlannedPart) -> bool {
        let Ok(key) = self.key(plan) else {
            return false;
        };
        matches!(
            tokio::time::timeout(
                crate::s3::upload::DATA_TIMEOUT,
                self.ownership.object_store().head(&key)
            )
            .await,
            Ok(Ok(_))
        )
    }

    async fn verify_native(
        &self,
        plan: &PlannedPart,
        receipt: &PartReceipt,
        version: Option<UpdateVersion>,
    ) -> Result<PartPresence> {
        let key = self.key(plan)?;
        let plan = plan.clone();
        let receipt = receipt.clone();
        tokio::time::timeout(crate::s3::upload::DATA_TIMEOUT, async {
            let Some(file) = self.read_spooled(&key, &receipt, version).await? else {
                return Ok(PartPresence::Missing);
            };
            verification::verify(plan, receipt, file).await?;
            Ok(PartPresence::Present)
        })
        .await
        .map_err(|_| anyhow::anyhow!("native protected part verification timed out"))?
    }

    async fn read_spooled(
        &self,
        key: &ObjectPath,
        receipt: &PartReceipt,
        expected: Option<UpdateVersion>,
    ) -> Result<Option<File>> {
        use tokio::io::AsyncWriteExt;
        ensure!(
            receipt.byte_size <= crate::s3::upload::MAX_PART_BYTES,
            "native protected part exceeds the single-PUT size limit"
        );
        // Pinned in the owner's `If-Match` ETag form (#678); the observed
        // version is still compared with `expected` as returned.
        let if_match = match expected
            .as_ref()
            .and_then(|version| version.e_tag.as_deref())
        {
            Some(etag) => Some(
                self.ownership
                    .etag_form()
                    .if_match(etag)
                    .context("native protected part has no usable version")?,
            ),
            None => None,
        };
        let options = object_store::GetOptions {
            if_match,
            version: expected
                .as_ref()
                .and_then(|version| version.version.clone()),
            ..Default::default()
        };
        let response = match self.ownership.object_store().get_opts(key, options).await {
            Ok(response) => response,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(_) => return Err(anyhow::anyhow!("reading native protected part failed")),
        };
        ensure!(
            response.meta.location == *key
                && response.meta.size == receipt.byte_size
                && response.range == (0..receipt.byte_size),
            "native protected part metadata differs from receipt"
        );
        let observed = UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };
        ensure!(
            usable_version(&observed),
            "native protected part has no usable version"
        );
        ensure!(
            expected
                .as_ref()
                .is_none_or(|expected| *expected == observed),
            "native protected part version changed during publication"
        );
        let mut file = tokio::fs::File::from_std(
            tempfile::tempfile().context("creating private verification spool")?,
        );
        let mut stream = response.into_stream();
        let mut size = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|_| anyhow::anyhow!("reading native protected part body failed"))?;
            ensure!(
                chunk.len() as u64 <= receipt.byte_size.saturating_sub(size),
                "native protected part exceeds receipt size"
            );
            file.write_all(&chunk)
                .await
                .context("writing private verification spool")?;
            size += chunk.len() as u64;
        }
        ensure!(
            size == receipt.byte_size,
            "native protected part is shorter than receipt"
        );
        file.flush()
            .await
            .context("finishing private verification spool")?;
        Ok(Some(file.into_std().await))
    }

    async fn read(
        &self,
        key: &ObjectPath,
        receipt: &PartReceipt,
    ) -> Result<Option<(Bytes, UpdateVersion)>> {
        tokio::time::timeout(DATA_TIMEOUT, async {
            let response = match self.ownership.object_store().get(key).await {
                Ok(response) => response,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(_) => return Err(anyhow::anyhow!("reading protected part failed")),
            };
            ensure!(
                response.meta.size == receipt.byte_size,
                "protected part size differs from receipt"
            );
            let version = UpdateVersion {
                e_tag: response.meta.e_tag.clone(),
                version: response.meta.version.clone(),
            };
            ensure!(
                usable_version(&version),
                "protected part read has no usable version"
            );
            let expected_size = usize::try_from(receipt.byte_size)?;
            let mut stream = response.into_stream();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk =
                    chunk.map_err(|_| anyhow::anyhow!("reading protected part body failed"))?;
                ensure!(
                    chunk.len() <= expected_size.saturating_sub(bytes.len()),
                    "protected part exceeds receipt size"
                );
                bytes.extend_from_slice(&chunk);
            }
            Ok(Some((Bytes::from(bytes), version)))
        })
        .await
        .map_err(|_| anyhow::anyhow!("protected part read timed out"))?
    }
}
struct MutationAttempt<'a> {
    ownership: &'a S3Ownership,
    resolved: bool,
}
impl Drop for MutationAttempt<'_> {
    fn drop(&mut self) {
        if !self.resolved {
            self.ownership.mark_mutation_uncertain();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    #[cfg(test)]
    Writing,
    FileSync,
    StagedDirectorySync,
    Publish,
    TemporaryRemoval,
    PublishedDirectorySync,
    VerifiedFileSync,
    VerifiedDirectorySync,
}
fn checkpoint(stage: Stage) -> Result<()> {
    #[cfg(test)]
    tests::checkpoint(stage)?;
    let _ = stage;
    Ok(())
}
#[cfg(test)]
mod tests;
