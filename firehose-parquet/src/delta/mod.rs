//! Delta Lake output (#643, `docs/design/delta-lake.md`).
//!
//! Every `build` writes one Delta table per mapper table at
//! `<dataset root>/<table>/`. The parts fireparq encodes (with the workspace's
//! Parquet 60) are the tables' data files, committed as they are:
//!
//! - [`types`] maps each flush onto Delta data file types (L2);
//! - [`stats`] computes each part's `add.stats`, journaled with its receipt;
//! - [`store`] is where the tables live: local disk, or S3 through a
//!   single-attempt object_store 0.13 client and conditional-put commits;
//! - [`commit`] opens, creates and validates the tables, and commits each
//!   Committed transaction to them with a `txn` action, `blocks` last.
//!
//! This module pins the protocol and the table properties fireparq sets when
//! it creates a table (design §2), and the `fireparq.*` properties that bind a
//! table to its stream.
//!
//! delta-rs brings its own Arrow/Parquet 59 and object_store 0.13 next to the
//! workspace's 60 and 0.12. No Arrow value crosses between them. The Delta
//! schema is built by name from fireparq's Arrow 60 types ([`delta_columns`]),
//! and commits carry only paths, sizes, partition values and statistics JSON.

pub mod commit;
pub mod stats;
pub mod store;
pub mod types;

use std::collections::HashMap;

use anyhow::{bail, ensure, Context, Result};
use arrow::datatypes::{
    DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use deltalake_core::kernel::transaction::CommitProperties;
use deltalake_core::kernel::{ArrayType, DataType, StructField, StructType};
use deltalake_core::logstore::LogStoreRef;
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaResult, DeltaTable, TableProperty};

use crate::ingest::state::StreamDescriptor;

/// The partition column of every table: the `date=YYYY-MM-DD` key.
pub const PARTITION_COLUMN: &str = crate::date_partition::DATE_KEY;

/// The protocol's `minReaderVersion`. No reader features are used.
pub const MIN_READER_VERSION: i32 = 1;

/// The protocol's `minWriterVersion`. No writer features are used.
pub const MIN_WRITER_VERSION: i32 = 2;

/// Table property: the SHA-256 of the stream descriptor that writes the table.
/// Its `txn` application id is `fireparq:<this hash>`.
pub const DESCRIPTOR_PROPERTY: &str = "fireparq.descriptor";

/// Table property: the stream's chain, the EndpointInfo chain name (what the
/// `{chain}` output placeholder expands to).
pub const CHAIN_PROPERTY: &str = "fireparq.chain";

/// Table property: the stream's block family, the `--block-type` value
/// (`evm`, `solana`, ...).
pub const BLOCK_TYPE_PROPERTY: &str = "fireparq.blockType";

/// The Delta table properties fireparq sets when it creates a table.
///
/// `delta.setTransactionRetentionDuration` stays unset on purpose: `txn`
/// entries must never expire, or exactly-once recovery breaks.
pub fn table_properties() -> [(TableProperty, &'static str); 6] {
    [
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
        (TableProperty::TargetFileSize, "268435456"),
    ]
}

/// The stream a dataset's Delta tables belong to, from its authority's
/// descriptor. Every table records it in its `fireparq.*` properties, and
/// every commit carries `txn {appId: fireparq:<descriptor>}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaIdentity {
    /// The stream descriptor's SHA-256 (hex).
    pub descriptor: String,
    /// The descriptor's `chain`.
    pub chain: String,
    /// The descriptor's block family label.
    pub block_type: String,
}

impl DeltaIdentity {
    pub fn of(descriptor: &StreamDescriptor) -> Result<Self> {
        let block_type = serde_json::to_value(descriptor.family)?
            .as_str()
            .context("a block family serializes as its label")?
            .to_string();
        Ok(Self {
            descriptor: descriptor.id()?.as_str().to_string(),
            chain: descriptor.chain.clone(),
            block_type,
        })
    }

    /// The `txn` application id: `fireparq:<descriptor SHA-256>`.
    pub fn app_id(&self) -> String {
        format!("fireparq:{}", self.descriptor)
    }

    /// The `fireparq.*` table properties.
    pub fn properties(&self) -> [(&'static str, String); 3] {
        [
            (DESCRIPTOR_PROPERTY, self.descriptor.clone()),
            (CHAIN_PROPERTY, self.chain.clone()),
            (BLOCK_TYPE_PROPERTY, self.block_type.clone()),
        ]
    }

    /// Every table property of a table fireparq created for this stream:
    /// [`table_properties`] and [`Self::properties`].
    pub fn configuration(&self) -> HashMap<String, String> {
        table_properties()
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_string(), value.to_string()))
            .chain(
                self.properties()
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value)),
            )
            .collect()
    }
}

/// The columns of a Delta table whose data files have `data_schema` (Arrow
/// 60, already Delta types, `types::DeltaTypes::data_schema`), followed by
/// the `date` partition column. Field nullability is kept; types map by name.
pub fn delta_columns(data_schema: &ArrowSchema) -> Result<Vec<StructField>> {
    let mut columns = data_schema
        .fields()
        .iter()
        .map(|field| {
            ensure!(
                field.name() != PARTITION_COLUMN,
                "a Delta data file schema cannot contain the partition column `{PARTITION_COLUMN}`"
            );
            delta_field(field)
        })
        .collect::<Result<Vec<_>>>()?;
    columns.push(StructField::new(PARTITION_COLUMN, DataType::DATE, false));
    Ok(columns)
}

fn delta_field(field: &ArrowField) -> Result<StructField> {
    Ok(StructField::new(
        field.name(),
        delta_type(field.data_type()).with_context(|| format!("column `{}`", field.name()))?,
        field.is_nullable(),
    ))
}

fn delta_type(arrow: &ArrowType) -> Result<DataType> {
    Ok(match arrow {
        ArrowType::Boolean => DataType::BOOLEAN,
        ArrowType::Int8 => DataType::BYTE,
        ArrowType::Int16 => DataType::SHORT,
        ArrowType::Int32 => DataType::INTEGER,
        ArrowType::Int64 => DataType::LONG,
        ArrowType::Float32 => DataType::FLOAT,
        ArrowType::Float64 => DataType::DOUBLE,
        ArrowType::Utf8 => DataType::STRING,
        ArrowType::Binary => DataType::BINARY,
        ArrowType::Date32 => DataType::DATE,
        ArrowType::Decimal128(precision, scale) if *scale >= 0 => {
            DataType::decimal(*precision, *scale as u8)?
        }
        ArrowType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == "UTC" => {
            DataType::TIMESTAMP
        }
        ArrowType::List(item) => DataType::Array(Box::new(ArrayType::new(
            delta_type(item.data_type())?,
            item.is_nullable(),
        ))),
        ArrowType::Struct(fields) => DataType::Struct(Box::new(StructType::try_new(
            fields
                .iter()
                .map(|field| delta_field(field))
                .collect::<Result<Vec<_>>>()?,
        )?)),
        other => bail!("Arrow type {other} is not a Delta type"),
    })
}

/// Creates an empty table: commit 0 holds only `protocol` and `metaData`,
/// with [`table_properties`] and `identity`'s `fireparq.*` properties.
///
/// The protocol is what delta-rs picks for these properties:
/// [`MIN_READER_VERSION`] and [`MIN_WRITER_VERSION`], with no table features.
/// The unit test below pins it. `columns` must include [`PARTITION_COLUMN`]
/// ([`delta_columns`]). An existing table is refused, never overwritten.
pub async fn create_table(
    log_store: LogStoreRef,
    name: &str,
    columns: Vec<StructField>,
    identity: &DeltaIdentity,
) -> DeltaResult<DeltaTable> {
    let mut builder = CreateBuilder::new()
        .with_log_store(log_store)
        .with_table_name(name)
        .with_columns(columns)
        .with_partition_columns([PARTITION_COLUMN])
        .with_save_mode(SaveMode::ErrorIfExists)
        // A creator that loses the race for version 0 must fail (the caller
        // then validates the winner's table), not retry its create as a
        // second `protocol` + `metaData` commit. Writers never checkpoint.
        .with_commit_properties(
            CommitProperties::default()
                .with_max_retries(0)
                .with_create_checkpoint(false)
                .with_cleanup_expired_logs(Some(false)),
        )
        // The `fireparq.*` keys are not Delta properties.
        .with_raise_if_key_not_exists(false)
        .with_configuration(
            identity
                .properties()
                .into_iter()
                .map(|(key, value)| (key, Some(value))),
        );
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn local_log_store(dir: &std::path::Path, table: &str) -> LogStoreRef {
        store::DeltaStore::local(dir)
            .unwrap()
            .log_store(table)
            .unwrap()
    }

    fn identity() -> DeltaIdentity {
        DeltaIdentity {
            descriptor: "a".repeat(64),
            chain: "eth-mainnet".into(),
            block_type: "evm".into(),
        }
    }

    #[tokio::test]
    async fn creates_an_empty_table_with_the_design_protocol_and_properties() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let columns = vec![
            StructField::new("block_num", DataType::LONG, false),
            StructField::new("timestamp", DataType::TIMESTAMP, false),
            StructField::new(PARTITION_COLUMN, DataType::DATE, false),
        ];

        let created = create_table(
            local_log_store(&root, "blocks"),
            "blocks",
            columns.clone(),
            &identity(),
        )
        .await
        .unwrap();
        assert_eq!(created.version(), Some(0));
        assert!(root
            .join("blocks/_delta_log/00000000000000000000.json")
            .is_file());
        assert!(
            create_table(
                local_log_store(&root, "blocks"),
                "blocks",
                columns,
                &identity()
            )
            .await
            .is_err(),
            "an existing table must be refused, not overwritten"
        );

        let table = open_table(local_log_store(&root, "blocks")).await.unwrap();
        assert_eq!(table.version(), Some(0));
        let snapshot = table.snapshot().unwrap();
        let protocol = snapshot.protocol();
        assert_eq!(protocol.min_reader_version(), MIN_READER_VERSION);
        assert_eq!(protocol.min_writer_version(), MIN_WRITER_VERSION);
        assert_eq!(protocol.reader_features(), None);
        assert_eq!(protocol.writer_features(), None);
        let metadata = snapshot.metadata();
        assert_eq!(metadata.partition_columns(), [PARTITION_COLUMN.to_string()]);
        let mut expected = identity().configuration();
        assert_eq!(metadata.configuration(), &expected);
        assert_eq!(expected.remove(DESCRIPTOR_PROPERTY), Some("a".repeat(64)));
        assert_eq!(
            expected.remove(CHAIN_PROPERTY).as_deref(),
            Some("eth-mainnet")
        );
        assert_eq!(expected.remove(BLOCK_TYPE_PROPERTY).as_deref(), Some("evm"));
        assert_eq!(expected.len(), table_properties().len());
        assert!(!expected.contains_key(TableProperty::SetTransactionRetentionDuration.as_ref()));
        assert_eq!(table.get_file_uris().unwrap().count(), 0);
    }

    #[test]
    fn delta_columns_map_every_delta_data_file_type_and_append_the_partition() {
        let item = |data_type| Arc::new(ArrowField::new("item", data_type, false));
        let schema = ArrowSchema::new(vec![
            ArrowField::new("block_num", ArrowType::Int64, false),
            ArrowField::new(
                "timestamp",
                ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            ArrowField::new("fee", ArrowType::Decimal128(20, 0), false),
            ArrowField::new("accounts", ArrowType::List(item(ArrowType::Int16)), true),
            ArrowField::new(
                "signer",
                ArrowType::Struct(
                    vec![
                        ArrowField::new("sequence", ArrowType::Int64, false),
                        ArrowField::new("denom", ArrowType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
            ArrowField::new("flag", ArrowType::Boolean, false),
            ArrowField::new("id", ArrowType::Binary, false),
            ArrowField::new("small", ArrowType::Int8, false),
            ArrowField::new("int", ArrowType::Int32, false),
            ArrowField::new("ratio", ArrowType::Float64, false),
            ArrowField::new("single", ArrowType::Float32, false),
            ArrowField::new("day", ArrowType::Date32, false),
        ]);
        let columns = delta_columns(&schema).unwrap();
        let names: Vec<_> = columns.iter().map(|c| c.name().as_str()).collect();
        assert_eq!(names.last(), Some(&PARTITION_COLUMN));
        assert_eq!(columns.len(), schema.fields().len() + 1);
        let json = serde_json::to_value(StructType::try_new(columns).unwrap()).unwrap();
        let types: Vec<_> = json["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| {
                (
                    field["name"].clone(),
                    field["type"].clone(),
                    field["nullable"].clone(),
                )
            })
            .collect();
        assert_eq!(types[0], ("block_num".into(), "long".into(), false.into()));
        assert_eq!(
            types[1],
            ("timestamp".into(), "timestamp".into(), true.into())
        );
        assert_eq!(
            types[2],
            ("fee".into(), "decimal(20,0)".into(), false.into())
        );
        assert_eq!(types[3].1["type"], "array");
        assert_eq!(types[3].1["elementType"], "short");
        assert_eq!(types[3].1["containsNull"], false);
        assert_eq!(types[4].1["type"], "struct");
        assert_eq!(types[12], ("date".into(), "date".into(), false.into()));

        for refused in [
            ArrowType::UInt64,
            ArrowType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Utf8)),
            ArrowType::LargeUtf8,
        ] {
            let schema = ArrowSchema::new(vec![ArrowField::new("x", refused, false)]);
            assert!(delta_columns(&schema).is_err());
        }
        let with_date = ArrowSchema::new(vec![ArrowField::new("date", ArrowType::Date32, false)]);
        assert!(delta_columns(&with_date).is_err());
    }
}
