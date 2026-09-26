//! NEAR receipt outcomes on child rows (#550).
//!
//! Failure happens per receipt: a failed receipt's actions roll back, while its
//! gas and tokens burnt persist and the logs it emitted stay in the outcome.
//! `--include-failed-transactions`/`--exclude-failed-transactions` select
//! transaction rows only; receipts and their child rows are always written.
use super::{
    mapper::{tests::make_every_action_block, NearBlockMapper},
    proto::near,
};
use arrow::array::*;
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;
use firehose_parquet::{
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper},
};
use prost::Message;
use std::collections::HashMap;

fn map(block: &near::Block, include_failed: bool, fork: bool) -> HashMap<String, RecordBatch> {
    let mut mapper = NearBlockMapper::new(fork, EncodeBytes::Base58, include_failed);
    mapper
        .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
        .unwrap();
    let batches = mapper.flush().unwrap();
    assert_eq!(mapper.total_rows(), 0);
    batches
}

fn labels(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = batch.column_by_name(name).unwrap();
    if let Some(dictionary) = column.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        return dictionary
            .downcast_dict::<StringArray>()
            .unwrap()
            .into_iter()
            .map(|value| value.unwrap().to_string())
            .collect();
    }
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_string())
        .collect()
}

fn u32s(batch: &RecordBatch, name: &str) -> Vec<u32> {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap()
        .values()
        .to_vec()
}

/// The fixture's failed `dex.near` receipt, with logs emitted before failing.
fn block_with_failed_receipt_logs() -> near::Block {
    let mut block = make_every_action_block(100);
    let failed = block.shards[1].receipt_execution_outcomes[0]
        .execution_outcome
        .as_mut()
        .unwrap()
        .outcome
        .as_mut()
        .unwrap();
    assert!(matches!(
        failed.status,
        Some(near::execution_outcome::Status::Failure(_))
    ));
    failed.logs = vec!["swap started".into(), "EVENT_JSON:{}".into()];
    block
}

#[test]
fn receipt_actions_and_logs_carry_their_receipt_status() {
    let block = block_with_failed_receipt_logs();
    for include_failed in [false, true] {
        let batches = map(&block, include_failed, false);
        let receipts = &batches["receipts"];
        let status_by_index: HashMap<u32, String> = u32s(receipts, "receipt_index")
            .into_iter()
            .zip(labels(receipts, "status"))
            .collect();
        assert_eq!(status_by_index[&1], "Failure");
        for table in ["receipt_actions", "execution_logs"] {
            let batch = &batches[table];
            let indices = u32s(batch, "receipt_index");
            let statuses = labels(batch, "receipt_status");
            assert_eq!(indices.len(), statuses.len());
            for (index, status) in indices.iter().zip(&statuses) {
                assert_eq!(status, &status_by_index[index], "{table} receipt {index}");
            }
            // The failed receipt's rows are written whatever the flag says.
            assert!(
                indices.contains(&1),
                "{table} include_failed={include_failed}"
            );
        }
        // Logs of the failed receipt are retained, in order.
        let logs = &batches["execution_logs"];
        let failed_logs: Vec<_> = u32s(logs, "receipt_index")
            .into_iter()
            .zip(labels(logs, "log"))
            .filter(|(index, _)| *index == 1)
            .map(|(_, log)| log)
            .collect();
        assert_eq!(failed_logs, ["swap started", "EVENT_JSON:{}"]);
        // Gas and tokens burnt by the failed receipt stay on the receipt row.
        assert_eq!(labels(receipts, "tokens_burnt")[1], "300000000000");
    }
}

#[test]
fn receipt_status_labels_match_receipts_status_values() {
    let mut block = make_every_action_block(100);
    // A receipt without an execution outcome is Unknown on every row.
    let shard = &mut block.shards[1].receipt_execution_outcomes;
    shard[0].execution_outcome = None;
    let batches = map(&block, true, false);
    assert_eq!(
        labels(&batches["receipts"], "status"),
        ["SuccessValue", "Unknown", "Unknown"]
    );
    let actions = &batches["receipt_actions"];
    let unknown: Vec<_> = u32s(actions, "receipt_index")
        .into_iter()
        .zip(labels(actions, "receipt_status"))
        .filter(|(index, _)| *index == 1)
        .map(|(_, status)| status)
        .collect();
    assert_eq!(unknown, ["Unknown"]);
    assert!(labels(actions, "receipt_status")
        .iter()
        .filter(|status| status.as_str() != "Unknown")
        .all(|status| status == "SuccessValue"));
}

#[test]
fn receipt_status_is_a_last_non_null_dictionary_column() {
    for fork in [false, true] {
        let batches = map(&make_every_action_block(100), false, fork);
        for table in ["receipt_actions", "execution_logs"] {
            let schema = batches[table].schema();
            let last = schema.fields().last().unwrap();
            assert_eq!(last.name(), "receipt_status", "{table}");
            assert_eq!(
                last.data_type(),
                &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            );
            assert!(!last.is_nullable());
            assert_eq!(
                schema.index_of("fork_step").ok(),
                fork.then(|| schema.fields().len() - 2)
            );
        }
        for table in ["receipts", "transactions", "state_changes"] {
            assert!(batches[table].schema().index_of("receipt_status").is_err());
        }
    }
}
