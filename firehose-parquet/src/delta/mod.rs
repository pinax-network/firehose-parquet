//! Delta Lake output (#643, `docs/design/delta-lake.md`).
//!
//! This is a skeleton. It pins the protocol and the table properties that
//! fireparq sets when it creates a table (design §2), and it keeps
//! `deltalake-core` built and linked in CI. Nothing calls it yet: the commit
//! layer wires table creation into `build`.
//!
//! delta-rs brings its own Arrow/Parquet 59 and object_store 0.13 next to the
//! workspace's 60 and 0.12. No Arrow value crosses between them. The Delta
//! schema is made of Delta types, and commits carry only paths, sizes,
//! partition values and statistics JSON.

use deltalake_core::kernel::StructField;
use deltalake_core::logstore::LogStoreRef;
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaResult, DeltaTable, TableProperty};

/// The partition column of every table: the `date=YYYY-MM-DD` key.
pub const PARTITION_COLUMN: &str = crate::date_partition::DATE_KEY;

/// The protocol's `minReaderVersion`. No reader features are used.
pub const MIN_READER_VERSION: i32 = 1;

/// The protocol's `minWriterVersion`. No writer features are used.
pub const MIN_WRITER_VERSION: i32 = 2;

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

/// Creates an empty table: commit 0 holds only `protocol` and `metaData`.
///
/// The protocol is what delta-rs picks for these properties:
/// [`MIN_READER_VERSION`] and [`MIN_WRITER_VERSION`], with no table features.
/// The unit test below pins it. `columns` must include [`PARTITION_COLUMN`].
/// An existing table is refused, never overwritten.
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use deltalake_core::kernel::DataType;
    use deltalake_core::{ensure_table_uri, DeltaTableBuilder};

    use super::*;

    fn local_log_store(dir: &std::path::Path) -> LogStoreRef {
        let url = ensure_table_uri(dir.to_str().expect("UTF-8 path")).expect("table URL");
        DeltaTableBuilder::from_url(url)
            .expect("table builder")
            .build_storage()
            .expect("local log store")
    }

    #[tokio::test]
    async fn creates_an_empty_table_with_the_design_protocol_and_properties() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("blocks");
        std::fs::create_dir_all(&root).unwrap();
        let columns = vec![
            StructField::new("block_num", DataType::LONG, false),
            StructField::new("timestamp", DataType::TIMESTAMP, false),
            StructField::new(PARTITION_COLUMN, DataType::DATE, false),
        ];

        let created = create_table(local_log_store(&root), "blocks", columns.clone())
            .await
            .unwrap();
        assert_eq!(created.version(), Some(0));
        assert!(root.join("_delta_log/00000000000000000000.json").is_file());
        assert!(
            create_table(local_log_store(&root), "blocks", columns)
                .await
                .is_err(),
            "an existing table must be refused, not overwritten"
        );

        let table = open_table(local_log_store(&root)).await.unwrap();
        assert_eq!(table.version(), Some(0));
        let snapshot = table.snapshot().unwrap();
        let protocol = snapshot.protocol();
        assert_eq!(protocol.min_reader_version(), MIN_READER_VERSION);
        assert_eq!(protocol.min_writer_version(), MIN_WRITER_VERSION);
        assert_eq!(protocol.reader_features(), None);
        assert_eq!(protocol.writer_features(), None);
        let metadata = snapshot.metadata();
        assert_eq!(metadata.partition_columns(), [PARTITION_COLUMN.to_string()]);
        let expected: HashMap<String, String> = table_properties()
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_string(), value.to_string()))
            .collect();
        assert_eq!(metadata.configuration(), &expected);
        assert_eq!(table.get_file_uris().unwrap().count(), 0);
    }
}
