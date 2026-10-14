//! verify: the tables' invariants, read-only.

use std::collections::{BTreeSet, HashMap, HashSet};

use serde_json::json;

use super::{JobError, Run};
use crate::json::sha256_hex;
use crate::model::{Cell, Key, Row, FUNDING_HISTORY, OUTCOMES, QUESTIONS, REFERENCE_TABLES};
use crate::store::OpenMode;

/// Checks that every payload hash is its payload's, that no entity has two
/// consecutive rows (by `observed_at`) with the same payload, that funding
/// hours are unique, and that every question an outcome names has a row.
pub async fn run_verify(run: &mut Run<'_>) -> Result<(), JobError> {
    let store = run.store;
    for spec in REFERENCE_TABLES {
        let Some(table) = store.open(spec, OpenMode::Read).await? else {
            run.log.emit(
                "skipped",
                json!({"table": spec.name, "reason": "no table yet"}),
            );
            continue;
        };
        let mut bad_hash = 0u64;
        store
            .for_each_row(
                &table,
                &["payload_sha256", "payload_json"],
                None,
                &mut |cells| {
                    let good = match cells {
                        [Cell::Null, Cell::Null] => true,
                        [Cell::Text(digest), Cell::Text(text)] => sha256_hex(text) == *digest,
                        _ => false,
                    };
                    bad_hash += u64::from(!good);
                },
            )
            .await?;
        let mut columns: Vec<&str> = spec.key.to_vec();
        let (rows, entities, duplicates) = if spec.name == FUNDING_HISTORY.name {
            // Each coin's hours, 8 bytes a row: the table grows by every
            // coin's every hour, so no set of whole keys.
            let (mut rows, mut hours, mut other) = (0u64, HashMap::new(), HashSet::new());
            store
                .for_each_row(&table, &columns, None, &mut |cells| {
                    rows += 1;
                    match cells {
                        [Cell::Text(coin), Cell::Timestamp(hour)] => hours
                            .entry(coin.clone())
                            .or_insert_with(Vec::new)
                            .push(*hour),
                        _ => {
                            other.insert(Row::of(spec, &columns, cells).key());
                        }
                    }
                })
                .await?;
            let mut keys = other.len() as u64;
            for hours in hours.values_mut() {
                hours.sort_unstable();
                hours.dedup();
                keys += hours.len() as u64;
            }
            (rows, keys, rows - keys)
        } else {
            columns.extend(["observed_at", "payload_sha256"]);
            let mut history: Vec<(Key, Cell, Cell)> = Vec::new();
            store
                .for_each_row(&table, &columns, None, &mut |cells| {
                    let row = Row::of(spec, &columns, cells);
                    let observed = row.get("observed_at").clone();
                    let digest = row.get("payload_sha256").clone();
                    history.push((row.key(), observed, digest));
                })
                .await?;
            history.sort_by(|a, b| (&a.0, observed(&a.1)).cmp(&(&b.0, observed(&b.1))));
            let (mut entities, mut duplicates) = (history.len() as u64, 0u64);
            for pair in history.windows(2) {
                if pair[0].0 == pair[1].0 {
                    entities -= 1;
                    // Two removal rows in a row count too.
                    duplicates += u64::from(pair[0].2 == pair[1].2);
                }
            }
            (history.len() as u64, entities, duplicates)
        };
        run.log.emit(
            "verified",
            json!({
                "table": spec.name,
                "version": table.version(),
                "rows": rows,
                "entities": entities,
                "bad_hash": bad_hash,
                "duplicates": duplicates,
            }),
        );
        if bad_hash > 0 || duplicates > 0 {
            run.fail(format!(
                "{}: {bad_hash} rows with a wrong payload hash, {duplicates} duplicate rows",
                spec.name
            ));
        }
    }
    question_links(run).await
}

/// A timestamp cell's µs; NULL first.
fn observed(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::Timestamp(at) => Some(*at),
        _ => None,
    }
}

/// Every question an outcome row names has a hl_questions row: questions are
/// committed first, so this holds at every table version.
async fn question_links(run: &mut Run<'_>) -> Result<(), JobError> {
    let store = run.store;
    let Some(outcomes) = store.open(&OUTCOMES, OpenMode::Read).await? else {
        return Ok(());
    };
    let ids = |cells: &[Cell]| match cells {
        [Cell::Long(id)] => Some(*id),
        _ => None,
    };
    let mut named = BTreeSet::new();
    store
        .for_each_row(&outcomes, &["question_id"], None, &mut |cells| {
            named.extend(ids(cells));
        })
        .await?;
    let mut known = HashSet::new();
    if let Some(questions) = store.open(&QUESTIONS, OpenMode::Read).await? {
        store
            .for_each_row(&questions, &["question_id"], None, &mut |cells| {
                known.extend(ids(cells));
            })
            .await?;
    }
    let missing: Vec<String> = named
        .iter()
        .filter(|id| !known.contains(*id))
        .map(ToString::to_string)
        .collect();
    run.log.emit(
        "verified",
        json!({"check": "outcome questions", "questions": named.len(), "missing": missing.len()}),
    );
    if !missing.is_empty() {
        run.fail(format!(
            "hl_outcomes names {} questions without a hl_questions row: {}",
            missing.len(),
            missing[..missing.len().min(10)].join(", ")
        ));
    }
    Ok(())
}
