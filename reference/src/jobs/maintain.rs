//! maintain: OPTIMIZE and VACUUM now (the CronJobs suspended first).

use serde_json::json;

use super::{JobError, Run};
use crate::model::ALL_TABLES;
use crate::store::{MaintainOptions, OpenMode};

/// Compacts and vacuums every table that exists, or those of `names` (none
/// or an empty list: all), whatever their file count and the hour. A table's
/// failure is recorded and the next one is maintained.
pub async fn run_maintain(run: &mut Run<'_>, names: Option<&[String]>) -> Result<(), JobError> {
    let names = names.filter(|names| !names.is_empty());
    let options = MaintainOptions {
        force: true,
        ..MaintainOptions::default()
    };
    for spec in ALL_TABLES {
        if names.is_some_and(|names| !names.iter().any(|name| name == spec.name)) {
            continue;
        }
        let Some(mut table) = run.store.open(spec, OpenMode::Read).await? else {
            run.log.emit(
                "skipped",
                json!({"table": spec.name, "reason": "no table yet"}),
            );
            continue;
        };
        if let Err(error) = run.store.maintain(&mut table, &options).await {
            run.fail(error.to_string());
        }
    }
    Ok(())
}
