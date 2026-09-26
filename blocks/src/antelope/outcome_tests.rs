//! Antelope failed-transaction selection and parent outcome columns (#550).
//!
//! The producer (`firehose-antelope` console reader) emits a failed deferred
//! transaction as two traces: the failed deferred trace, which carries the
//! exception and whose operations it reverted, followed by the `onerror`
//! handler trace (`failed_dtrx_trace` set). `SOFTFAIL` on the handler means it
//! succeeded and its effects persisted; `HARDFAIL` means it failed or none ran.
use super::{
    mapper::{tests::make_test_block, transaction_success, AntelopeBlockMapper},
    proto::antelope,
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

const EXECUTED: i32 = 1;
const SOFTFAIL: i32 = 2;
const HARDFAIL: i32 = 3;
const DELAYED: i32 = 4;
const EXPIRED: i32 = 5;

fn trace(id: &str, index: u64, status: Option<i32>, failed: bool) -> antelope::TransactionTrace {
    let mut trace = make_test_block(100).unfiltered_transaction_traces.remove(0);
    trace.id = id.into();
    trace.index = index;
    trace.receipt = status.map(|status| antelope::TransactionReceiptHeader {
        status,
        ..trace.receipt.clone().unwrap()
    });
    for action in &mut trace.action_traces {
        action.transaction_id = id.into();
    }
    if failed {
        trace.exception = Some(antelope::Exception {
            code: 3050003,
            name: "eosio_assert_message_exception".into(),
            message: "assertion failure".into(),
            ..Default::default()
        });
        // The producer reverts a failed trace's database operations.
        trace.db_ops.clear();
    }
    trace
}

/// Every receipt role seen on EOS mainnet (see docs/audit/550-*), in order.
fn traces() -> Vec<(antelope::TransactionTrace, bool)> {
    let failed_deferred = trace("failed-deferred", 1, Some(SOFTFAIL), true);
    let mut onerror = trace("onerror-ok", 2, Some(SOFTFAIL), false);
    onerror.failed_dtrx_trace = Some(Box::new(failed_deferred.clone()));
    let failed_deferred_2 = trace("failed-deferred-2", 3, Some(SOFTFAIL), true);
    let mut onerror_failed = trace("onerror-failed", 4, Some(HARDFAIL), true);
    onerror_failed.failed_dtrx_trace = Some(Box::new(failed_deferred_2.clone()));
    let mut delayed = trace("delayed", 6, Some(DELAYED), false);
    delayed.action_traces.clear();
    delayed.db_ops.clear();
    let mut expired = trace("expired", 7, Some(EXPIRED), false);
    expired.action_traces.clear();
    expired.db_ops.clear();
    vec![
        (failed_deferred, false),
        (onerror, true),
        (failed_deferred_2, false),
        (onerror_failed, false),
        (trace("delayed-hardfail", 5, Some(HARDFAIL), true), false),
        (delayed, true),
        (expired, false),
        (trace("executed", 8, Some(EXECUTED), false), true),
        (trace("no-receipt", 9, None, false), false),
        (trace("unknown", 10, Some(6), false), false),
        (trace("canceled", 11, Some(7), false), false),
        (trace("unmapped", 12, Some(99), false), false),
        // An exception always means the effects did not persist.
        (
            trace("executed-with-exception", 13, Some(EXECUTED), true),
            false,
        ),
    ]
}

fn map(include_failed: bool, filtered: bool) -> HashMap<String, RecordBatch> {
    let mut block = make_test_block(100);
    let traces: Vec<_> = traces().into_iter().map(|(trace, _)| trace).collect();
    if filtered {
        block.filtering_applied = true;
        block.filtered_transaction_traces = traces;
    } else {
        block.unfiltered_transaction_traces = traces;
    }
    let mut mapper = AntelopeBlockMapper::new(true, EncodeBytes::HexNoPrefix, include_failed);
    mapper
        .map_block(
            &block.encode_to_vec(),
            &BlockIdentity::default(),
            Some("NEW"),
        )
        .unwrap();
    let batches = mapper.flush().unwrap();
    assert_eq!(mapper.total_rows(), 0);
    batches
}

fn strings(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = batch.column_by_name(name).unwrap();
    if let Some(dictionary) = column.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let values = dictionary.downcast_dict::<StringArray>().unwrap();
        return values.into_iter().map(|v| v.unwrap().to_string()).collect();
    }
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap().to_string())
        .collect()
}

fn bools(batch: &RecordBatch, name: &str) -> Vec<bool> {
    let column = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(column.null_count(), 0);
    column.iter().map(Option::unwrap).collect()
}

#[test]
fn success_follows_receipt_status_and_trace_exception() {
    for (trace, expected) in traces() {
        assert_eq!(transaction_success(&trace), expected, "{}", trace.id);
    }
}

#[test]
fn default_keeps_persisted_traces_and_drops_failed_ones() {
    for filtered in [false, true] {
        let expected: Vec<_> = traces()
            .into_iter()
            .filter(|(_, success)| *success)
            .map(|(trace, _)| trace.id)
            .collect();
        assert_eq!(expected, ["onerror-ok", "delayed", "executed"]);
        let batches = map(false, filtered);
        assert_eq!(strings(&batches["transactions"], "tx_hash"), expected);
        assert_eq!(
            strings(&batches["transactions"], "status"),
            ["SOFTFAIL", "DELAYED", "EXECUTED"]
        );
        for table in ["transactions", "actions", "db_ops"] {
            assert!(bools(&batches[table], "transaction_success")
                .iter()
                .all(|ok| *ok));
        }
        // The successful onerror handler's action and database rows are kept.
        let action_hashes = strings(&batches["actions"], "tx_hash");
        assert!(action_hashes.iter().any(|hash| hash == "onerror-ok"));
        assert!(action_hashes.iter().all(|hash| hash != "delayed"));
        let op_hashes = strings(&batches["db_ops"], "tx_hash");
        assert_eq!(op_hashes, ["onerror-ok", "executed"]);
    }
}

#[test]
fn included_failed_traces_carry_status_and_outcome_on_every_row() {
    let batches = map(true, false);
    let all = traces();
    let expected_ids: Vec<_> = all.iter().map(|(trace, _)| trace.id.clone()).collect();
    assert_eq!(strings(&batches["transactions"], "tx_hash"), expected_ids);
    assert_eq!(
        bools(&batches["transactions"], "transaction_success"),
        all.iter().map(|(_, ok)| *ok).collect::<Vec<_>>()
    );
    let by_id: HashMap<_, _> = all
        .iter()
        .map(|(trace, ok)| {
            let status = trace.receipt.as_ref().map_or(0, |receipt| receipt.status);
            let label = match status {
                0 => "NONE",
                1 => "EXECUTED",
                2 => "SOFTFAIL",
                3 => "HARDFAIL",
                4 => "DELAYED",
                5 => "EXPIRED",
                6 => "UNKNOWN",
                7 => "CANCELED",
                _ => "UNKNOWN",
            };
            (trace.id.clone(), (label, *ok))
        })
        .collect();
    for table in ["actions", "db_ops"] {
        let batch = &batches[table];
        let hashes = strings(batch, "tx_hash");
        let statuses = strings(batch, "transaction_status");
        let outcomes = bools(batch, "transaction_success");
        assert!(!hashes.is_empty());
        for ((hash, status), ok) in hashes.iter().zip(statuses).zip(outcomes) {
            assert_eq!(
                (status.as_str(), ok),
                by_id[hash.as_str()],
                "{table} {hash}"
            );
        }
        let schema = batch.schema();
        let status = schema.field_with_name("transaction_status").unwrap();
        assert_eq!(
            status.data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert!(!status.is_nullable());
        assert!(!schema
            .field_with_name("transaction_success")
            .unwrap()
            .is_nullable());
    }
    // The transactions table keeps its literal Utf8 status.
    assert_eq!(
        batches["transactions"]
            .schema()
            .field_with_name("status")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
    // Failed traces keep their action rows, labeled, when included.
    let action_hashes = strings(&batches["actions"], "tx_hash");
    assert!(action_hashes.iter().any(|hash| hash == "failed-deferred"));
    assert!(action_hashes.iter().any(|hash| hash == "onerror-failed"));
}

#[test]
fn schemas_do_not_depend_on_the_failed_filter() {
    for fork in [false, true] {
        let mut excluded = AntelopeBlockMapper::new(fork, EncodeBytes::Hex, false);
        let mut included = AntelopeBlockMapper::new(fork, EncodeBytes::Hex, true);
        let (excluded, included) = (excluded.flush().unwrap(), included.flush().unwrap());
        for table in ["blocks", "transactions", "actions", "db_ops"] {
            assert_eq!(excluded[table].schema(), included[table].schema());
        }
        assert!(excluded["blocks"]
            .schema()
            .index_of("transaction_success")
            .is_err());
    }
}
