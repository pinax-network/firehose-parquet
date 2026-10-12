//! meta: spotMeta, perpDexs and allPerpMetas, and tokenDetails.

use std::collections::{HashMap, HashSet};

use serde_json::json;

use super::{changed, hour_of, id_of, items, keyed, removals, writable, JobError, Run};
use crate::info::{InfoFailure, SERVER_ERROR_ATTEMPTS};
use crate::json::Json;
use crate::model::{
    perp_asset_row, perp_dex_row, spot_meta_row, spot_pair_row, spot_token_row, token_details_row,
    Cell, KeyPart, PERP_ASSETS, PERP_DEXS, SPOT_META, SPOT_PAIRS, SPOT_TOKENS, SPOT_TOKEN_DETAILS,
};
use crate::values::crc32_hour;

pub async fn run_meta(run: &mut Run<'_>) -> Result<(), JobError> {
    let store = run.store;
    let mut t_meta = writable(run, &SPOT_META).await?;
    let mut t_tokens = writable(run, &SPOT_TOKENS).await?;
    let mut t_details = writable(run, &SPOT_TOKEN_DETAILS).await?;
    let mut t_pairs = writable(run, &SPOT_PAIRS).await?;
    let mut t_dexs = writable(run, &PERP_DEXS).await?;
    let mut t_assets = writable(run, &PERP_ASSETS).await?;
    let (spot, spot_at) = run.info.spot_meta().await?;
    let (mut dexs, _) = run.info.perp_dexs().await?;
    let (mut metas, mut metas_at) = run.info.all_perp_metas().await?;
    let length = |value: &Json| value.as_arr().map_or(0, <[Json]>::len);
    if length(&dexs) != length(&metas) {
        // A dex deployed between the two requests: read both again once.
        dexs = run.info.perp_dexs().await?.0;
        (metas, metas_at) = run.info.all_perp_metas().await?;
    }
    let (dexs, metas) = (
        dexs.as_arr().unwrap_or_default(),
        metas.as_arr().unwrap_or_default(),
    );
    let complete = dexs.len().min(metas.len());
    if dexs.len() != metas.len() {
        run.log.emit(
            "warning",
            json!({"message": format!(
                "perpDexs has {} dexes and allPerpMetas {}; only the first {complete} are written",
                dexs.len(),
                metas.len()
            )}),
        );
    }
    let tokens = keyed(items(&spot, "tokens"), "index");
    let pairs = keyed(items(&spot, "universe"), "index");

    let meta_row = spot_meta_row(&spot, spot_at);
    let meta_rows = if changed(&store.latest(&t_meta).await?, &meta_row) {
        vec![meta_row]
    } else {
        Vec::new()
    };

    // Spot tokens, and tokenDetails once per token: when it is first seen (a
    // token gets its first row only once its details were asked, so a run
    // whose budget ends defers it), and once a day in its own hour, with a
    // single attempt, for a token /info failed (it answers HTTP 500 for a
    // third of them, every time). Details are their own table, so a later
    // answer is a new row.
    let latest = store.latest(&t_tokens).await?;
    let mut detailed = HashSet::new();
    store
        .for_each_row(&t_details, &["token_index"], None, &mut |cells| {
            if let [Cell::Long(index)] = cells {
                detailed.insert(*index);
            }
        })
        .await?;
    let hour = hour_of(run.now());
    let mut indices: Vec<i64> = tokens.iter().map(|(index, _)| *index).collect();
    indices.sort_unstable();
    let (mut rows, mut details, mut deferred, mut asking) = (Vec::new(), Vec::new(), 0u64, true);
    for index in indices {
        let token = tokens.get(&index).copied().unwrap_or(&Json::Null);
        let new = !latest.contains_key(&vec![KeyPart::Long(index)]);
        let due = token
            .get("tokenId")
            .and_then(Json::as_str)
            .filter(|token_id| !detailed.contains(&index) && (new || crc32_hour(token_id) == hour));
        if let Some(token_id) = due {
            if asking {
                let attempts = if new { SERVER_ERROR_ATTEMPTS } else { 1 };
                match run.info.token_details(token_id, attempts).await {
                    Err(InfoFailure::Budget) => asking = false,
                    Err(InfoFailure::Error(error)) if !error.entity_error() => {
                        run.fail(error.message);
                        asking = false;
                    }
                    Err(InfoFailure::Error(error)) if new => {
                        run.entity_failed("tokenDetails", &error, ("token_index", json!(index)));
                    }
                    Err(InfoFailure::Error(_)) => run.count("token_details_retry_failed", 1),
                    Ok((response, at)) => {
                        if new {
                            run.answered("tokenDetails");
                        }
                        let key = if new {
                            "token_details_fetched"
                        } else {
                            "token_details_retried"
                        };
                        run.count(key, 1);
                        if let Some(response) = response {
                            details.push(token_details_row(index, token_id, &response, at));
                        }
                        let asked = run.summary[key].as_u64().unwrap_or_default();
                        if asked.is_multiple_of(100) {
                            run.log.emit(
                                "progress",
                                json!({"token_details": asked, "token_index": index}),
                            );
                        }
                    }
                }
            }
            // A new token whose details were not asked waits for a run that
            // asks them.
            if !asking && new {
                deferred += 1;
                continue;
            }
        }
        let row = spot_token_row(token, spot_at);
        if changed(&latest, &row) {
            rows.push(row);
        }
    }
    let gone = removals(
        &latest,
        |key, _| !tokens.contains_key(&id_of(key)),
        spot_at,
        "spotMeta",
    );
    rows.extend(run.bounded_removals(&SPOT_TOKENS, &latest, gone));
    run.count("tokens_deferred", deferred);
    let undetailed = tokens
        .iter()
        .filter(|(index, _)| !detailed.contains(*index))
        .count();
    run.summary.insert(
        "token_details_missing".into(),
        json!(undetailed as i64 - details.len() as i64),
    );
    // Details before tokens: a token whose details row failed to commit is
    // then still new on the next run, and asked again at once.
    if run.commit(&mut t_meta, meta_rows).await && run.commit(&mut t_details, details).await {
        run.commit(&mut t_tokens, rows).await;
    }

    let latest = store.latest(&t_pairs).await?;
    let mut indices: Vec<i64> = pairs.iter().map(|(index, _)| *index).collect();
    indices.sort_unstable();
    let mut rows: Vec<_> = indices
        .into_iter()
        .filter_map(|index| pairs.get(&index))
        .map(|pair| spot_pair_row(pair, &tokens, spot_at))
        .filter(|row| changed(&latest, row))
        .collect();
    let gone = removals(
        &latest,
        |key, _| !pairs.contains_key(&id_of(key)),
        spot_at,
        "spotMeta",
    );
    rows.extend(run.bounded_removals(&SPOT_PAIRS, &latest, gone));
    run.commit(&mut t_pairs, rows).await;

    let mut names: HashMap<i64, Option<String>> = HashMap::from([(0, Some(String::new()))]);
    let latest = store.latest(&t_dexs).await?;
    let mut rows = Vec::new();
    for (index, (entry, meta)) in dexs.iter().zip(metas).take(complete).enumerate() {
        let index = index as i64;
        let row = perp_dex_row(index, entry, meta, &tokens, metas_at);
        let name = match row.get("name") {
            Cell::Text(name) => Some(name.clone()),
            _ => None,
        };
        names.insert(index, name);
        if changed(&latest, &row) {
            rows.push(row);
        }
    }
    let gone = removals(
        &latest,
        |key, _| {
            let index = id_of(key);
            index >= dexs.len() as i64 && index >= metas.len() as i64
        },
        metas_at,
        "perpDexs+allPerpMetas",
    );
    rows.extend(run.bounded_removals(&PERP_DEXS, &latest, gone));
    run.commit(&mut t_dexs, rows).await;

    let latest = store.latest(&t_assets).await?;
    let (mut rows, mut seen, mut mismatched) = (Vec::new(), HashSet::new(), 0u64);
    let mut assets = 0;
    for (dex_index, meta) in metas.iter().take(complete).enumerate() {
        let dex_index = dex_index as i64;
        let name = names.get(&dex_index).cloned().flatten();
        let universe = items(meta, "universe");
        assets += universe.len();
        for (asset_index, asset) in universe.iter().enumerate() {
            if asset.as_obj().is_none() {
                continue;
            }
            let asset_index = asset_index as i64;
            let row = perp_asset_row(dex_index, name.as_deref(), asset_index, asset, metas_at);
            seen.insert((dex_index, asset_index));
            let prefix = match row.get("coin") {
                Cell::Text(coin) => coin.split_once(':').map_or("", |(prefix, _)| prefix),
                _ => "",
            };
            if name.as_deref() != Some(prefix) {
                mismatched += 1;
            }
            if changed(&latest, &row) {
                rows.push(row);
            }
        }
    }
    let gone = removals(
        &latest,
        |key, _| match key.as_slice() {
            [KeyPart::Long(dex), KeyPart::Long(asset)] => {
                !seen.contains(&(*dex, *asset)) && *dex < complete as i64
            }
            _ => false,
        },
        metas_at,
        "allPerpMetas",
    );
    rows.extend(run.bounded_removals(&PERP_ASSETS, &latest, gone));
    if mismatched > 0 {
        run.log.emit(
            "warning",
            json!({"message": format!(
                "{mismatched} perp assets have a coin prefix other than their dex name"
            )}),
        );
    }
    run.commit(&mut t_assets, rows).await;
    run.count("tokens", tokens.len() as u64);
    run.count("pairs", pairs.len() as u64);
    run.count("dexs", complete as u64);
    run.count("assets", assets as u64);
    run.check_outages();
    run.maintain(&mut [
        &mut t_meta,
        &mut t_tokens,
        &mut t_details,
        &mut t_pairs,
        &mut t_dexs,
        &mut t_assets,
    ])
    .await;
    Ok(())
}
