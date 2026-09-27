//! Delta Lake spike for fireparq issue #643. See `docs/design/delta-lake.md`.
//!
//! Standalone crate, outside the fireparq workspace: `deltalake-core` 1.0.0
//! needs Rust 1.94.1 and brings Arrow/Parquet 59 and object_store 0.13, while
//! fireparq uses Arrow/Parquet 60 and object_store 0.12. This crate shows the
//! two coexist: parts are encoded with Parquet 60 and committed through
//! delta-rs without passing any Arrow value between the versions.

pub mod delta;
pub mod mapping;
pub mod part;
pub mod storage;

use deltalake_core::{DeltaResult, DeltaTable};
use serde_json::json;

use delta::{Committed, PartAdd};
use mapping::Fixture;
use storage::Lake;

/// Spike tables, in commit order: `blocks` is committed last in every
/// transaction, so a block visible in `blocks` has all its rows visible in
/// every other table.
pub const TABLES: &[&str] = &["transactions", "blocks"];

/// The `txn` application id of a stream (fireparq would derive it from the
/// stream descriptor hash).
pub fn app_id(stream: &str) -> String {
    format!("fireparq-{stream}")
}

/// Physical layout of the data files, for reader compatibility probes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Variant {
    /// The proposed layout: no `date` column in the file, microsecond timestamps.
    #[default]
    Standard,
    /// Keep the `date` partition column inside the file too (fireparq today).
    PhysicalDate,
    /// Keep millisecond timestamps in the file under a Delta `timestamp` column.
    MillisTimestamps,
}

impl std::str::FromStr for Variant {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "standard" => Ok(Variant::Standard),
            "physical-date" => Ok(Variant::PhysicalDate),
            "millis" => Ok(Variant::MillisTimestamps),
            other => Err(format!("unknown variant {other}")),
        }
    }
}

/// One fireparq-style transaction: the fixture rows of every table, the
/// accepted ordinal range and the deterministic identity.
#[derive(Clone, Debug)]
pub struct SpikeTransaction {
    pub stream: String,
    pub first_ordinal: u64,
    pub last_ordinal: u64,
    pub fixture: Fixture,
    pub variant: Variant,
}

impl SpikeTransaction {
    /// A stand-in for fireparq's transaction id (a hash in fireparq).
    pub fn id(&self) -> String {
        format!("t{:016x}", self.last_ordinal)
    }

    /// Builds, encodes and publishes this transaction's part of `table`.
    /// Returns the `add` it needs and the exact bytes that were published.
    pub async fn publish(&self, lake: &Lake, table: &str, index: usize) -> (PartAdd, Vec<u8>) {
        let mapping = mapping::table_mapping(table);
        let source = self.fixture.source(table);
        let batch =
            mapping::to_delta_batch(&source, mapping).expect("fixture rows fit the Delta mapping");
        let stats = part::stats_json(&batch, delta::STATS_COLUMNS);
        let batch = match self.variant {
            Variant::Standard => batch,
            Variant::PhysicalDate => mapping::to_delta_batch_with_physical_date(&source, mapping)
                .expect("fixture rows fit the Delta mapping"),
            Variant::MillisTimestamps => mapping::with_millis_timestamps(&batch),
        };
        let bytes = part::encode_part(
            &batch,
            &[
                ("fireparq.stream", self.stream.clone()),
                ("fireparq.transaction", self.id()),
            ],
        );
        let name = part::part_name(
            &self.stream,
            self.first_ordinal,
            self.last_ordinal,
            &self.id(),
            index,
        );
        let path = format!(
            "{}={}/{name}",
            delta::PARTITION_COLUMN,
            self.fixture.date_string()
        );
        lake.publish_part(table, &path, bytes.clone()).await;
        (
            PartAdd {
                path,
                size: bytes.len() as i64,
                date: self.fixture.date_string(),
                stats,
            },
            bytes,
        )
    }
}

/// Opens every spike table of a lake, creating the missing ones.
pub async fn open_or_create(lake: &Lake) -> DeltaResult<Vec<(String, DeltaTable)>> {
    let mut tables = Vec::new();
    for table in TABLES {
        let log_store = lake.log_store(table);
        let opened = match delta::open_table(log_store.clone()).await {
            Ok(t) => t,
            Err(deltalake_core::DeltaTableError::NotATable(_)) => {
                let fixture = Fixture {
                    date: 0,
                    first_block: 0,
                    blocks: 1,
                    txs_per_block: 1,
                };
                let data =
                    mapping::to_delta_batch(&fixture.source(table), mapping::table_mapping(table))
                        .expect("schema probe");
                let columns = delta::delta_columns(data.schema().as_ref())?;
                delta::create_table(log_store, table, columns).await?
            }
            Err(e) => return Err(e),
        };
        tables.push((table.to_string(), opened));
    }
    Ok(tables)
}

/// Publishes every part of a transaction, then commits one Delta commit per
/// table (in [`TABLES`] order), each with `txn = last_ordinal`.
pub async fn write_transaction(
    lake: &Lake,
    tables: &mut [(String, DeltaTable)],
    txn: &SpikeTransaction,
) -> DeltaResult<Vec<Committed>> {
    let mut adds = Vec::new();
    for (index, (name, _)) in tables.iter().enumerate() {
        adds.push(txn.publish(lake, name, index).await.0);
    }
    let mut commits = Vec::new();
    for ((_, table), add) in tables.iter_mut().zip(adds) {
        commits.push(
            delta::commit_parts(
                table,
                &[add],
                &app_id(&txn.stream),
                txn.last_ordinal as i64,
                &[
                    ("fireparq.transaction", json!(txn.id())),
                    ("fireparq.firstOrdinal", json!(txn.first_ordinal)),
                    ("fireparq.lastOrdinal", json!(txn.last_ordinal)),
                    ("fireparq.firstBlock", json!(txn.fixture.first_block)),
                    (
                        "fireparq.lastBlock",
                        json!(txn.fixture.first_block + txn.fixture.blocks - 1),
                    ),
                ],
            )
            .await?,
        );
    }
    Ok(commits)
}
