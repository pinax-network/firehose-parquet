//! The Delta commit layer: create a table, commit pre-written parts with a `txn`
//! action, and read the `txn` version back for exactly-once recovery.
//!
//! No Arrow value crosses into delta-rs: the Delta schema is built from the
//! workspace's Arrow 60 types by name, and commits carry only paths, sizes,
//! partition values and statistics JSON. delta-rs keeps its own Arrow 59.

use std::collections::HashMap;

use arrow::datatypes::{DataType as ArrowType, Schema as ArrowSchema, TimeUnit};
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::kernel::{
    Action, Add, ArrayType, DataType, PrimitiveType, StructField, Transaction,
};
use deltalake_core::logstore::LogStoreRef;
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableError, TableProperty};
use serde_json::Value;

/// The partition column of every table.
pub const PARTITION_COLUMN: &str = "date";

/// Columns with file statistics (`delta.dataSkippingStatsColumns`).
pub const STATS_COLUMNS: &[&str] = &["block_num", "timestamp"];

/// Table properties proposed for fireparq tables (see docs/design/delta-lake.md).
pub fn table_properties() -> Vec<(TableProperty, &'static str)> {
    vec![
        (TableProperty::AppendOnly, "true"),
        (TableProperty::CheckpointInterval, "100"),
        (TableProperty::LogRetentionDuration, "interval 7 days"),
        (
            TableProperty::DeletedFileRetentionDuration,
            "interval 7 days",
        ),
        (
            TableProperty::DataSkippingStatsColumns,
            "block_num,timestamp",
        ),
    ]
}

/// Maps a Delta-compatible Arrow 60 type onto a Delta type.
pub fn delta_type(arrow: &ArrowType) -> DeltaResult<DataType> {
    Ok(match arrow {
        ArrowType::Int64 => DataType::LONG,
        ArrowType::Int32 => DataType::INTEGER,
        ArrowType::Int16 => DataType::SHORT,
        ArrowType::Boolean => DataType::BOOLEAN,
        ArrowType::Float64 => DataType::DOUBLE,
        ArrowType::Utf8 => DataType::STRING,
        ArrowType::Binary => DataType::BINARY,
        ArrowType::Date32 => DataType::DATE,
        ArrowType::Decimal128(p, s) => DataType::decimal(*p, *s as u8)?,
        ArrowType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
            DataType::TIMESTAMP
        }
        ArrowType::List(item) => DataType::Array(Box::new(ArrayType::new(
            delta_type(item.data_type())?,
            item.is_nullable(),
        ))),
        other => {
            return Err(DeltaTableError::Generic(format!(
                "no Delta type for Arrow {other}"
            )))
        }
    })
}

/// The Delta columns of a table: the data file schema plus the partition column.
pub fn delta_columns(data_schema: &ArrowSchema) -> DeltaResult<Vec<StructField>> {
    let mut columns = data_schema
        .fields()
        .iter()
        .map(|f| {
            Ok(StructField::new(
                f.name().clone(),
                delta_type(f.data_type())?,
                f.is_nullable(),
            ))
        })
        .collect::<DeltaResult<Vec<_>>>()?;
    columns.push(StructField::new(
        PARTITION_COLUMN,
        DataType::Primitive(PrimitiveType::Date),
        false,
    ));
    Ok(columns)
}

/// Creates a table (commit 0: protocol and metadata only).
pub async fn create_table(
    log_store: LogStoreRef,
    name: &str,
    columns: Vec<StructField>,
) -> DeltaResult<DeltaTable> {
    let mut builder = CreateBuilder::new()
        .with_log_store(log_store)
        .with_table_name(name)
        .with_columns(columns)
        .with_partition_columns([PARTITION_COLUMN])
        .with_save_mode(SaveMode::ErrorIfExists);
    for (key, value) in table_properties() {
        builder = builder.with_configuration_property(key, Some(value));
    }
    builder.await
}

/// Opens an existing table at its latest version.
pub async fn open_table(log_store: LogStoreRef) -> DeltaResult<DeltaTable> {
    let mut table = DeltaTable::new(log_store);
    table.load().await?;
    Ok(table)
}

/// One pre-written part to add, exactly as it was published.
#[derive(Clone, Debug)]
pub struct PartAdd {
    /// Path relative to the table root, e.g. `date=2026-09-25/part-v1-....parquet`.
    pub path: String,
    pub size: i64,
    pub date: String,
    pub stats: String,
}

/// Outcome of one commit.
#[derive(Clone, Copy, Debug)]
pub struct Committed {
    pub version: u64,
    pub retries: u64,
}

/// Commits pre-written parts as one Delta commit, with a `txn` action
/// (`appId`, `version`) that makes the commit detectable after a crash.
///
/// The writer never writes checkpoints or removes expired logs: the external
/// maintenance job does that. `isBlindAppend` tells concurrent OPTIMIZE and
/// other readers that this commit read nothing.
pub async fn commit_parts(
    table: &mut DeltaTable,
    parts: &[PartAdd],
    app_id: &str,
    txn_version: i64,
    commit_metadata: &[(&str, Value)],
) -> DeltaResult<Committed> {
    let actions = parts
        .iter()
        .map(|p| {
            Action::Add(Add {
                path: p.path.clone(),
                size: p.size,
                partition_values: HashMap::from([(
                    PARTITION_COLUMN.to_string(),
                    Some(p.date.clone()),
                )]),
                modification_time: now_millis(),
                data_change: true,
                stats: Some(p.stats.clone()),
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            })
        })
        .collect();
    let mut metadata: HashMap<String, Value> = commit_metadata
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect();
    metadata.insert("isBlindAppend".into(), Value::Bool(true));
    let properties = CommitProperties::default()
        .with_application_transaction(Transaction::new(app_id, txn_version))
        .with_create_checkpoint(false)
        .with_cleanup_expired_logs(Some(false))
        .with_max_retries(25)
        .with_metadata(metadata);
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: Some(vec![PARTITION_COLUMN.to_string()]),
        predicate: None,
    };
    let finalized = CommitBuilder::from(properties)
        .with_actions(actions)
        .build(Some(table.snapshot()?), table.log_store(), operation)
        .await?;
    let committed = Committed {
        version: finalized.version(),
        retries: finalized.metrics.num_retries,
    };
    table.state = Some(finalized.snapshot());
    Ok(committed)
}

/// The last `txn` version this application committed to the table, if any.
pub async fn txn_version(table: &DeltaTable, app_id: &str) -> DeltaResult<Option<i64>> {
    table
        .snapshot()?
        .transaction_version(table.log_store().as_ref(), app_id)
        .await
}

/// Rolls one table of a committed fireparq transaction forward exactly once.
///
/// Returns `None` when the table already holds this transaction (its `txn`
/// version equals `txn`), otherwise the new commit. A `txn` version above `txn`
/// would mean the log is ahead of fireparq's authority: that is refused.
pub async fn roll_forward(
    table: &mut DeltaTable,
    parts: &[PartAdd],
    app_id: &str,
    txn: i64,
) -> DeltaResult<Option<Committed>> {
    table.update_state().await?;
    match txn_version(table, app_id).await? {
        Some(v) if v == txn => Ok(None),
        Some(v) if v > txn => Err(DeltaTableError::Generic(format!(
            "table txn version {v} is ahead of the pending transaction {txn}"
        ))),
        _ => commit_parts(table, parts, app_id, txn, &[]).await.map(Some),
    }
}

/// Active files and their summed `numRecords`, from the current snapshot.
pub fn active_files(table: &DeltaTable) -> DeltaResult<(usize, usize, Vec<String>)> {
    let state = table.snapshot()?;
    let data = state.log_data();
    let mut rows = 0;
    let mut paths = Vec::new();
    for file in data.iter() {
        rows += file.num_records().unwrap_or(0);
        paths.push(file.path().to_string());
    }
    paths.sort();
    Ok((paths.len(), rows, paths))
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}
