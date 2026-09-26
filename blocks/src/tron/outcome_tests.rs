//! Tron execution outcome and failed-transaction selection (#550).
//!
//! java-tron sets the Firehose wrapper `result`/`code` to true/SUCCESS for every
//! included transaction, so failures come from `TransactionInfo.result` and the
//! receipt contract result.
use super::{
    mapper::{tests::make_test_block, transaction_success, TronBlockMapper},
    proto::{protocol, tron},
};
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use firehose_parquet::{
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper},
};
use prost::Message;
use std::collections::HashMap;

const OUTCOME_TABLES: [&str; 5] = [
    "transactions",
    "logs",
    "internal_transactions",
    "contracts",
    "internal_call_values",
];

fn with_outcome(info_result: i32, receipt_result: Option<i32>) -> tron::Transaction {
    let mut tx = make_test_block(100).transactions.remove(0);
    let info = tx.info.as_mut().unwrap();
    info.result = info_result;
    info.receipt = receipt_result.map(|result| protocol::ResourceReceipt {
        result,
        ..Default::default()
    });
    tx
}

fn bools(batch: &RecordBatch, name: &str) -> Vec<bool> {
    let column = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing {name}"))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(column.null_count(), 0);
    column.iter().map(Option::unwrap).collect()
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

fn map(block: &tron::Block, include_failed: bool) -> HashMap<String, RecordBatch> {
    let mut mapper = TronBlockMapper::new(true, EncodeBytes::Hex, include_failed);
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

#[test]
fn outcome_comes_from_transaction_info_not_the_api_wrapper() {
    use protocol::transaction::result::ContractResult as R;
    const SUCESS: i32 = 0;
    const FAILED: i32 = 1;
    let cases = [
        // Non-VM contracts have a DEFAULT receipt.
        (SUCESS, Some(R::Default as i32), true),
        (SUCESS, Some(R::Success as i32), true),
        (SUCESS, None, true),
        (FAILED, Some(R::Revert as i32), false),
        (FAILED, Some(R::OutOfEnergy as i32), false),
        (FAILED, Some(R::Success as i32), false),
        (FAILED, None, false),
        // Either field alone marks a failure.
        (SUCESS, Some(R::Revert as i32), false),
        (SUCESS, Some(R::TransferFailed as i32), false),
        (SUCESS, Some(R::Unknown as i32), false),
        // Values outside the known enums are not proven successful.
        (SUCESS, Some(99), false),
        (7, Some(R::Success as i32), false),
    ];
    for (info_result, receipt, expected) in cases {
        let tx = with_outcome(info_result, receipt);
        assert!(tx.result, "wrapper stays true/SUCCESS");
        assert_eq!(
            transaction_success(&tx),
            expected,
            "{info_result} {receipt:?}"
        );
    }
    let mut no_info = with_outcome(SUCESS, None);
    no_info.info = None;
    assert!(transaction_success(&no_info));
    // A false wrapper is still a failure, as before #550.
    let mut wrapper_false = with_outcome(SUCESS, Some(R::Success as i32));
    wrapper_false.result = false;
    assert!(!transaction_success(&wrapper_false));
}

/// A reverted TRC-20 style call: TransactionInfo FAILED, receipt REVERT, the
/// VM-rejected internal call, a submitted contract and its call value.
fn reverted_call() -> tron::Transaction {
    let mut tx = with_outcome(1, Some(2));
    tx.txid = vec![0xfa; 32].into();
    let info = tx.info.as_mut().unwrap();
    info.id = tx.txid.clone();
    info.fee = 2_500_000;
    info.res_message = b"REVERT opcode executed".to_vec().into();
    info.internal_transactions[0].rejected = true;
    tx
}

#[test]
fn failed_vm_transactions_are_excluded_by_default_and_labeled_when_included() {
    let mut block = make_test_block(100);
    let succeeded = with_outcome(0, Some(1));
    block.transactions = vec![reverted_call(), succeeded.clone(), reverted_call()];
    block.transactions[1].txid = vec![0x01; 32].into();

    let default = map(&block, false);
    for table in OUTCOME_TABLES {
        let batch = &default[table];
        assert!(batch.num_rows() > 0, "{table}");
        assert!(bools(batch, "transaction_success").iter().all(|ok| *ok));
        // Positions keep gaps from the excluded failed transactions.
        assert!(u32s(batch, "transaction_index").iter().all(|i| *i == 1));
    }
    assert_eq!(default["transactions"].num_rows(), 1);
    // The block-wide log position still counts the excluded transaction's log.
    let block_log_index = default["logs"]
        .column_by_name("block_log_index")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(block_log_index, 1);

    let included = map(&block, true);
    assert_eq!(
        bools(&included["transactions"], "transaction_success"),
        [false, true, false]
    );
    // Wrapper fields stay literal: they do not report the VM outcome.
    assert_eq!(bools(&included["transactions"], "result"), [true; 3]);
    for table in OUTCOME_TABLES {
        let batch = &included[table];
        let indices = u32s(batch, "transaction_index");
        let outcomes = bools(batch, "transaction_success");
        assert_eq!(indices.len(), outcomes.len());
        for (index, ok) in indices.iter().zip(outcomes) {
            assert_eq!(ok, *index == 1, "{table} row for transaction {index}");
        }
    }
    // The VM's own rejection flag is preserved beside the parent outcome.
    assert_eq!(
        bools(&included["internal_transactions"], "rejected"),
        [true, false, true]
    );
    // Schemas are identical with and without failed transactions.
    for table in OUTCOME_TABLES {
        assert_eq!(default[table].schema(), included[table].schema(), "{table}");
    }
}

#[test]
fn outcome_column_is_last_and_not_nullable_with_or_without_fork_step() {
    for fork in [false, true] {
        let mut mapper = TronBlockMapper::new(fork, EncodeBytes::Binary, true);
        let batches = mapper.flush().unwrap();
        for table in OUTCOME_TABLES {
            let schema = batches[table].schema();
            let last = schema.fields().last().unwrap();
            assert_eq!(last.name(), "transaction_success", "{table}");
            assert_eq!(last.data_type(), &arrow::datatypes::DataType::Boolean);
            assert!(!last.is_nullable());
            let fork_position = schema.index_of("fork_step").ok();
            assert_eq!(
                fork_position,
                fork.then(|| schema.fields().len() - 2),
                "{table}"
            );
        }
        for table in ["blocks"] {
            assert!(batches[table]
                .schema()
                .index_of("transaction_success")
                .is_err());
        }
    }
}

#[test]
fn contract_address_is_null_when_transaction_info_has_none() {
    let mut block = make_test_block(100);
    let mut created = block.transactions[0].clone();
    created.info.as_mut().unwrap().contract_address = vec![0x41; 21].into();
    block.transactions.push(created);
    let batches = map(&block, true);
    let column = batches["transactions"]
        .column_by_name("contract_address")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(column.is_null(0));
    assert_eq!(column.value(1), format!("0x{}", "41".repeat(21)));
}

#[test]
fn estimates_count_the_outcome_columns() {
    let mut block = make_test_block(100);
    block.transactions = vec![reverted_call()];
    let mut mapper = TronBlockMapper::new(false, EncodeBytes::Binary, true);
    mapper
        .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
        .unwrap();
    let estimates: HashMap<_, _> = mapper
        .table_estimates()
        .into_iter()
        .map(|(table, bytes)| (table.to_string(), bytes))
        .collect();
    for table in OUTCOME_TABLES {
        assert!(estimates[table] > 0, "{table}");
    }
}
