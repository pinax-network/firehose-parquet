//! outcomes: outcomeMeta, then the settledOutcome sweep.

use std::collections::{HashMap, HashSet};

use serde_json::json;

use super::{changed, id_of, items, keyed, removals, writable, JobError, Run};
use crate::info::InfoFailure;
use crate::model::{
    membership, outcome_live_row, outcome_meta_row, outcome_settled_row, probe_row,
    question_live_row, question_settled_row, settled_question_id, Cell, KeyPart, ProbeResult, Row,
    OUTCOMES, OUTCOME_META, OUTCOME_PROBES, QUESTIONS,
};
use crate::store::Prune;
use crate::values::Ordered;

/// The newest probe of an id within the cool-down: when, and what it found.
struct Probe {
    at: i64,
    result: String,
}

pub async fn run_outcomes(run: &mut Run<'_>) -> Result<(), JobError> {
    let store = run.store;
    let mut t_meta = writable(run, &OUTCOME_META).await?;
    let mut t_outcomes = writable(run, &OUTCOMES).await?;
    let mut t_questions = writable(run, &QUESTIONS).await?;
    let mut t_probes = writable(run, &OUTCOME_PROBES).await?;
    let outcomes = store.latest(&t_outcomes).await?;
    let questions = store.latest(&t_questions).await?;
    // Only probes within the cool-down matter: read just those.
    let cutoff = run.now() - run.options.reprobe_after_us;
    let mut last_probe: HashMap<i64, Probe> = HashMap::new();
    let prune = Prune {
        column: "probed_at",
        greater_than: cutoff,
    };
    store
        .for_each_row(
            &t_probes,
            &["outcome_id", "probed_at", "result"],
            Some(prune),
            &mut |cells| {
                let [Cell::Long(id), Cell::Timestamp(at), Cell::Text(result)] = cells else {
                    return;
                };
                if *at > cutoff && last_probe.get(id).is_none_or(|probe| *at > probe.at) {
                    let result = result.clone();
                    last_probe.insert(*id, Probe { at: *at, result });
                }
            },
        )
        .await?;

    let (meta, observed) = run.info.outcome_meta().await?;
    let meta_row = outcome_meta_row(&meta, observed);
    let meta_rows = if changed(&store.latest(&t_meta).await?, &meta_row) {
        vec![meta_row]
    } else {
        Vec::new()
    };
    let live = keyed(items(&meta, "outcomes"), "outcome");
    let listed_questions = keyed(items(&meta, "questions"), "question");
    if live.len() != items(&meta, "outcomes").len()
        || listed_questions.len() != items(&meta, "questions").len()
    {
        run.fail("outcomeMeta has entries without a valid id; they were skipped".into());
    }
    let members = membership(&listed_questions);

    let mut new_outcomes: Ordered<i64, Row> = Ordered::new();
    let mut new_questions: Ordered<i64, Row> = Ordered::new();
    for (id, item) in live.iter() {
        let row = outcome_live_row(item, members.get(id).copied(), observed);
        if changed(&outcomes, &row) {
            new_outcomes.insert(*id, row);
        }
    }
    for (id, item) in listed_questions.iter() {
        let row = question_live_row(item, observed);
        if changed(&questions, &row) {
            new_questions.insert(*id, row);
        }
    }

    let options = &run.options;
    let known = |id: i64| outcomes.contains_key(&vec![KeyPart::Long(id)]);
    let cooling = |id: i64| {
        last_probe
            .get(&id)
            .is_some_and(|probe| probe.result != "settled" && probe.at > cutoff)
    };
    let (candidates, limit): (Box<dyn Iterator<Item = i64> + '_>, usize) = match &options
        .outcome_ids
    {
        // Named ids are all probed, whatever their state (a repair); the
        // time budget still ends the run.
        Some(ids) => (Box::new(ids.clone().into_iter()), ids.len()),
        None => {
            // Newest first: outcomes that just left outcomeMeta, then
            // known outcomes still waiting for their settlement, then ids
            // never seen (short-lived outcomes and, until the sweep
            // reaches 0, the HIP-4 backfill).
            let mut departed: Vec<i64> = outcomes
                .iter()
                .filter(|(key, row)| row.listed() == Some(true) && !live.contains_key(&id_of(key)))
                .map(|(key, _)| id_of(key))
                .collect();
            departed.sort_unstable_by(|a, b| b.cmp(a));
            let highest = live
                .iter()
                .map(|(id, _)| *id)
                .chain(outcomes.keys().map(id_of))
                .fold(-1, i64::max);
            let mut upper = highest.saturating_add(options.lookahead);
            if let Some(sweep_to) = options.sweep_to {
                upper = upper.min(sweep_to);
            }
            let departed_set: HashSet<i64> = departed.iter().copied().collect();
            let sweep_from = options.sweep_from;
            let mut pending: Vec<i64> = outcomes
                .iter()
                .filter(|(_, row)| !matches!(row.get("status"), Cell::Text(status) if status == "settled"))
                .map(|(key, _)| id_of(key))
                .filter(|id| {
                    !live.contains_key(id)
                        && !departed_set.contains(id)
                        && (sweep_from..=upper).contains(id)
                        && !cooling(*id)
                })
                .collect();
            pending.sort_unstable_by(|a, b| b.cmp(a));
            let live = &live;
            let unseen = (sweep_from..=upper)
                .rev()
                .filter(move |id| !live.contains_key(id) && !known(*id) && !cooling(*id));
            (
                Box::new(departed.into_iter().chain(pending).chain(unseen)),
                options.max_probes as usize,
            )
        }
    };

    let (mut probed, mut settled_now) = (0usize, 0usize);
    let mut probes = Vec::new();
    for id in candidates {
        if probed >= limit {
            break;
        }
        let (response, at) = match run.info.settled_outcome(id).await {
            Ok(answer) => answer,
            Err(InfoFailure::Budget) => {
                run.summary
                    .insert("probe_budget_exhausted".into(), json!(true));
                break;
            }
            Err(InfoFailure::Error(error)) if !error.entity_error() => {
                run.fail(error.message);
                break;
            }
            Err(InfoFailure::Error(error)) => {
                // /info fails for this id: cool it down like an unsettled one.
                run.entity_failed("settledOutcome", &error, ("outcome_id", json!(id)));
                probes.push(probe_row(id, run.now(), ProbeResult::Error, &run.run_id));
                probed += 1;
                continue;
            }
        };
        run.answered("settledOutcome");
        probed += 1;
        if probed.is_multiple_of(50) {
            run.log.emit(
                "progress",
                json!({"probed": probed, "settled": settled_now, "outcome_id": id}),
            );
        }
        let Some(response) = response else {
            probes.push(probe_row(id, at, ProbeResult::NotSettled, &run.run_id));
            continue;
        };
        settled_now += 1;
        probes.push(probe_row(id, at, ProbeResult::Settled, &run.run_id));
        let row = outcome_settled_row(&response, at);
        if changed(&outcomes, &row) {
            new_outcomes.insert(id, row);
        }
        // A listed question's own outcomeMeta row is richer; a settled
        // wrapper only records a question that left outcomeMeta.
        let Some(question_id) = settled_question_id(&response) else {
            continue;
        };
        if listed_questions.contains_key(&question_id) {
            continue;
        }
        let row = question_settled_row(&response, at);
        let previous = new_questions
            .get(&question_id)
            .or_else(|| questions.get(&row.key()));
        if previous.is_none_or(|previous| previous.payload_sha256() != row.payload_sha256()) {
            new_questions.insert(question_id, row);
        }
    }

    // Entities that left outcomeMeta without a settled row this run.
    let gone = removals(
        &outcomes,
        |key, _| !live.contains_key(&id_of(key)) && !new_outcomes.contains_key(&id_of(key)),
        observed,
        "outcomeMeta",
    );
    for row in run.bounded_removals(&OUTCOMES, &outcomes, gone) {
        new_outcomes.insert(id_of(&row.key()), row);
    }
    let gone = removals(
        &questions,
        |key, _| {
            !listed_questions.contains_key(&id_of(key)) && !new_questions.contains_key(&id_of(key))
        },
        observed,
        "outcomeMeta",
    );
    for row in run.bounded_removals(&QUESTIONS, &questions, gone) {
        new_questions.insert(id_of(&row.key()), row);
    }

    run.count("live_outcomes", live.len() as u64);
    run.count("listed_questions", listed_questions.len() as u64);
    run.count("probed", probed as u64);
    run.count("probed_settled", settled_now as u64);
    // Questions before outcomes: a question that left outcomeMeta is known
    // only from the settledOutcome answer of an outcome, which is never probed
    // again once its settled row is committed. If the outcomes commit fails
    // instead, the next run probes the outcome again. Probes last.
    if run.commit(&mut t_meta, meta_rows).await
        && run
            .commit(&mut t_questions, new_questions.into_values().collect())
            .await
        && run
            .commit(&mut t_outcomes, new_outcomes.into_values().collect())
            .await
    {
        run.commit(&mut t_probes, probes).await;
    }
    run.check_outages();
    run.maintain(&mut [
        &mut t_meta,
        &mut t_outcomes,
        &mut t_questions,
        &mut t_probes,
    ])
    .await;
    Ok(())
}
