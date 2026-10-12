//! A full VACUUM, as `fireparq maintenance` runs one: delta-rs plans it (a
//! dry run, which commits nothing) and the crate deletes the planned files
//! itself. delta-rs's own VACUUM would commit its VACUUM START with 15
//! retries and the post-commit hooks; this one commits VACUUM START and
//! VACUUM END as the job commits everything, once and without hooks.
//!
//! It is full, not lite: a lite VACUUM finds replaced files only through
//! tombstones still in the snapshot, and every checkpoint (one per 20 commits)
//! drops those older than `delta.deletedFileRetentionDuration`, so most files
//! an OPTIMIZE replaced would stay forever. A full one lists `<table>/` and
//! deletes every file the current version does not reference, unless it was
//! removed, or written, within the retention (168 hours, the full-VACUUM
//! floor; real time, by tombstone and by object modification time): another
//! run's commit in progress keeps its file.

use deltalake_core::kernel::transaction::CommitBuilder;
use deltalake_core::operations::vacuum::VacuumMode;
use deltalake_core::protocol::DeltaOperation;
use deltalake_core::table::config::TablePropertiesExt as _;
use deltalake_core::{DeltaResult, DeltaTableError};
use futures::{stream, StreamExt};
use object_store::path::Path as ObjectPath;
use serde_json::json;

use crate::store::{Store, Table};

/// Vacuums `t` with a retention of `retention_hours`: the number of files
/// deleted. Nothing is committed when nothing is due. A VACUUM START that
/// loses to a concurrent commit deletes nothing; a failure after it leaves a
/// START without an END, and only unreferenced files deleted.
pub(crate) async fn vacuum(
    store: &Store,
    t: &mut Table,
    retention_hours: u64,
) -> DeltaResult<usize> {
    let retention = i64::try_from(retention_hours)
        .ok()
        .and_then(chrono::Duration::try_hours)
        .ok_or_else(|| DeltaTableError::Generic("the VACUUM retention is too long".into()))?;
    let (_, plan) = t
        .delta
        .clone()
        .vacuum()
        .with_mode(VacuumMode::Full)
        .with_retention_period(retention)
        .with_enforce_retention_duration(true)
        .with_dry_run(true)
        .await?;
    let planned = plan.files_deleted;
    if planned.is_empty() {
        return Ok(0);
    }
    let default_retention = t
        .delta
        .snapshot()?
        .table_config()
        .deleted_file_retention_duration();
    let start = DeltaOperation::VacuumStart {
        retention_check_enabled: true,
        specified_retention_millis: Some(retention.num_milliseconds()),
        default_retention_millis: i64::try_from(default_retention.as_millis()).unwrap_or(i64::MAX),
    };
    let before = t.version();
    let started = CommitBuilder::from(
        store.commit_metadata("VACUUM", Some(json!({"numFilesToDelete": planned.len()}))),
    )
    .build(Some(t.delta.snapshot()?), t.delta.log_store(), start)
    .await?;
    t.advance(&started, before + 1)?;

    // As delta-rs deletes: `Path::parse` keeps the plan's percent-encoding.
    let paths = planned
        .iter()
        .map(|path| ObjectPath::parse(path).map_err(object_store::Error::from))
        .collect::<Result<Vec<_>, _>>()?;
    let mut deleted = store
        .data_store(t)
        .delete_stream(stream::iter(paths.into_iter().map(Ok)).boxed());
    while let Some(result) = deleted.next().await {
        match result {
            // Already gone: deleted, as delta-rs counts it.
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }

    let end = DeltaOperation::VacuumEnd {
        status: "COMPLETED".into(),
    };
    let metrics = json!({"numDeletedFiles": planned.len(), "numVacuumedDirectories": 0});
    let ended = CommitBuilder::from(store.commit_metadata("VACUUM", Some(metrics)))
        .build(Some(t.delta.snapshot()?), t.delta.log_store(), end)
        .await?;
    t.advance(&ended, before + 2)?;
    Ok(planned.len())
}
