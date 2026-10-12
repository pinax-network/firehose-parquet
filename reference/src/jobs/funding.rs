//! funding: fundingHistory of every perp coin, each hour of each coin once.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::json;

use super::{hour_of, items, writable, JobError, Run};
use crate::info::InfoFailure;
use crate::json::Json;
use crate::model::{funding_row, Cell, Row, FUNDING_HISTORY};
use crate::values::{crc32_hour, from_millis, truthy};

/// A listed coin whose newest hour is older than this after a complete
/// incremental run fails the run.
const STALE_AFTER_HOURS: i64 = 2;
const HOUR_MICROS: i64 = 3_600_000_000;

/// Milliseconds of a time in µs, rounded down.
fn millis(micros: i64) -> i64 {
    micros.div_euclid(1000)
}

/// The rows to commit: the first of each `(coin, funding_time)`, and none
/// already stored (`known`, the repair window's stored keys).
fn missing(rows: Vec<Row>, known: &HashSet<(String, i64)>) -> Vec<Row> {
    let mut seen = HashSet::new();
    rows.into_iter()
        .filter(|row| {
            let (Cell::Text(coin), Cell::Timestamp(time)) =
                (row.get("coin"), row.get("funding_time"))
            else {
                return true;
            };
            let key = (coin.clone(), *time);
            !known.contains(&key) && seen.insert(key)
        })
        .collect()
}

pub async fn run_funding(run: &mut Run<'_>) -> Result<(), JobError> {
    let store = run.store;
    let mut table = writable(run, &FUNDING_HISTORY).await?;
    let (metas, _) = run.info.all_perp_metas().await?;
    let mut delisted: BTreeMap<String, bool> = BTreeMap::new();
    for meta in metas.as_arr().unwrap_or_default() {
        for asset in items(meta, "universe") {
            if let Some(name) = asset.get("name").and_then(Json::as_str) {
                delisted.insert(name.to_owned(), asset.get("isDelisted").is_some_and(truthy));
            }
        }
    }
    let named = run.options.coins.clone();
    let mut coins: Vec<String> = named
        .clone()
        .unwrap_or_else(|| delisted.keys().cloned().collect());

    // The incremental run resumes each coin after its latest stored hour. A
    // repair window (--funding-end) re-reads [--funding-start, --funding-end)
    // and appends only the hours missing at or before the coin's latest stored
    // hour: never ahead of it, which would leave a gap behind the cursor.
    let window = run.options.funding_end;
    let funding_start = run.options.funding_start;
    let wanted: Option<HashSet<String>> =
        named.as_ref().map(|coins| coins.iter().cloned().collect());
    let mut last: HashMap<String, i64> = HashMap::new();
    let mut known: HashSet<(String, i64)> = HashSet::new();
    store
        .for_each_row(&table, &["coin", "funding_time"], None, &mut |cells| {
            let [Cell::Text(coin), Cell::Timestamp(time)] = cells else {
                return;
            };
            if wanted.as_ref().is_some_and(|wanted| !wanted.contains(coin)) {
                return;
            }
            let newest = last.entry(coin.clone()).or_insert(*time);
            *newest = (*newest).max(*time);
            if window.is_some_and(|end| funding_start <= *time && *time < end) {
                known.insert((coin.clone(), *time));
            }
        })
        .await?;

    let now = run.now();
    let hour = now - now.rem_euclid(HOUR_MICROS);
    let end_ms = window.map(|end| millis(end) - 1);
    // Coins without stored hours first, then the oldest.
    coins.sort_by(|a, b| (last.get(a), a).cmp(&(last.get(b), b)));

    let page = run.options.funding_page;
    let (mut pending, mut fetched, mut skipped, mut stop) = (Vec::new(), 0u64, 0u64, false);
    let (mut complete, mut failed, mut requested) = (0u64, HashSet::new(), HashSet::new());
    for coin in &coins {
        let previous = last.get(coin).copied();
        let start = match (window, previous) {
            (Some(_), None) => {
                skipped += 1;
                continue;
            }
            (Some(_), Some(_)) => funding_start,
            (None, Some(previous)) if previous >= hour => {
                skipped += 1;
                continue;
            }
            // A delisted coin is read once a day, in its own hour, unless the
            // run names it: some still get an hourly (zero) rate, others none
            // at all, which every run would ask again.
            _ if delisted.get(coin) == Some(&true)
                && named.is_none()
                && hour_of(now) != crc32_hour(coin) =>
            {
                skipped += 1;
                continue;
            }
            (None, previous) => previous.map_or(funding_start, |previous| previous + 1000),
        };
        let mut start_ms = millis(start);
        let mut coin_failed = false;
        loop {
            if end_ms.is_some_and(|end| start_ms > end) {
                break;
            }
            requested.insert(coin.clone());
            let (entries, at) = match run.info.funding_history(coin, start_ms, end_ms).await {
                Ok(answer) => answer,
                Err(InfoFailure::Budget) => {
                    run.summary
                        .insert("funding_budget_exhausted".into(), json!(true));
                    stop = true;
                    break;
                }
                Err(InfoFailure::Error(error)) if error.entity_error() => {
                    // /info fails for this coin: the next run tries it again.
                    run.entity_failed("fundingHistory", &error, ("coin", json!(coin)));
                    failed.insert(coin.clone());
                    coin_failed = true;
                    break;
                }
                Err(InfoFailure::Error(error)) => {
                    run.fail(error.message);
                    stop = true;
                    break;
                }
            };
            run.answered("fundingHistory");
            fetched += 1;
            if fetched.is_multiple_of(100) {
                run.log.emit(
                    "progress",
                    json!({
                        "funding_requests": fetched,
                        "coins_complete": complete,
                        "rows_pending": pending.len(),
                        "coin": coin,
                    }),
                );
            }
            let mut newest = start_ms;
            for entry in &entries {
                // An entry of another coin, before the start or at a time no
                // timestamp holds.
                let inside = (entry.get("coin").and_then(Json::as_str) == Some(coin.as_str()))
                    .then(|| entry.get("time").and_then(Json::as_i64))
                    .flatten()
                    .filter(|time| *time >= start_ms)
                    .and_then(|time| Some((time, from_millis(time)?)));
                let Some((time, funding_time)) = inside else {
                    run.fail(format!(
                        "fundingHistory {coin} returned an entry outside the request"
                    ));
                    stop = true;
                    break;
                };
                newest = newest.max(time);
                // A repair never writes ahead of the coin's cursor, an incremental
                // run only after it.
                let ahead = previous.is_none_or(|previous| funding_time > previous);
                if window.is_some() == ahead {
                    continue;
                }
                pending.push(funding_row(entry, at));
                let stored = last.entry(coin.clone()).or_insert(funding_time);
                *stored = (*stored).max(funding_time);
            }
            if stop || entries.len() < page {
                break;
            }
            start_ms = newest + 1;
        }
        if stop {
            break;
        }
        if !coin_failed {
            complete += 1;
        }
        if pending.len() >= run.options.commit_rows {
            let rows = missing(std::mem::take(&mut pending), &known);
            if !run.commit(&mut table, rows).await {
                return Ok(());
            }
        }
    }
    if !pending.is_empty() && !run.commit(&mut table, missing(pending, &known)).await {
        return Ok(());
    }
    // Listed coins whose newest stored hour is over two hours old: a backfill
    // still in progress, or a coin the runs no longer reach.
    let mut stale: Vec<&String> = coins
        .iter()
        .filter(|coin| {
            !delisted.get(*coin).copied().unwrap_or(false)
                && last
                    .get(*coin)
                    .is_none_or(|last| now - last > STALE_AFTER_HOURS * HOUR_MICROS)
        })
        .collect();
    stale.sort();
    let incomplete = coins.len() as i64 - (complete + skipped) as i64 - failed.len() as i64;
    run.count("coins", coins.len() as u64);
    run.count("coins_complete", complete);
    run.count("coins_skipped", skipped);
    run.count("funding_requests", fetched);
    run.count("coins_failed", failed.len() as u64);
    run.summary
        .insert("coins_incomplete".into(), json!(incomplete));
    run.count("coins_stale", stale.len() as u64);
    run.summary.insert(
        "coins_stale_sample".into(),
        json!(stale.iter().take(5).collect::<Vec<_>>()),
    );
    run.check_outages();
    // After a complete incremental run (no budget end, no failure, every coin
    // due was asked), a listed coin with stored hours is stale only if /info
    // stopped funding it or keeps failing for it: fail, so the run alerts. A
    // listed coin without any hour yet (just listed) is only reported.
    if window.is_none() && named.is_none() && !stop && run.failures.is_empty() {
        let behind: Vec<&str> = stale
            .iter()
            .filter(|coin| requested.contains(**coin) && last.contains_key(**coin))
            .map(|coin| coin.as_str())
            .collect();
        if !behind.is_empty() {
            run.fail(format!(
                "fundingHistory: {} listed coins are more than {STALE_AFTER_HOURS} hours behind \
                 after a complete run: {}",
                behind.len(),
                behind[..behind.len().min(5)].join(", ")
            ));
        }
    }
    run.maintain(&mut [&mut table]).await;
    Ok(())
}
