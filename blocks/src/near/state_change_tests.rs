//! NEAR state-change attribution, balances and encodings, and receipt
//! result links (#507).
use super::{mapper::NearBlockMapper, proto::near};
use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use firehose_parquet::{
    encode::{encode_base58, EncodeBytes},
    traits::{BlockIdentity, BlockMapper},
};
use prost::Message;
use std::collections::HashMap;

fn hash(byte: u8) -> Option<near::CryptoHash> {
    Some(near::CryptoHash {
        bytes: vec![byte; 32].into(),
    })
}

fn bigint(value: u128) -> Option<near::BigInt> {
    Some(near::BigInt {
        bytes: value.to_be_bytes().to_vec().into(),
    })
}

fn change(
    value: near::state_change_value::Value,
    cause: Option<near::state_change_cause::Cause>,
) -> near::StateChangeWithCause {
    near::StateChangeWithCause {
        value: Some(near::StateChangeValue { value: Some(value) }),
        cause: Some(near::StateChangeCause { cause }),
    }
}

fn block() -> near::Block {
    use near::state_change_cause::{self as cause, Cause};
    use near::state_change_value::{self as value, Value};
    near::Block {
        author: "producer.near".into(),
        header: Some(near::BlockHeader {
            height: 300,
            prev_height: 299,
            hash: hash(0x01),
            prev_hash: hash(0x00),
            timestamp_nanosec: 1_700_000_000_000_000_000,
            ..Default::default()
        }),
        state_changes: vec![
            change(
                Value::AccountUpdate(value::AccountUpdate {
                    account_id: "alice.near".into(),
                    account: Some(near::Account {
                        amount: bigint(370_416_707_672_993_464_458_810_570),
                        locked: bigint(0),
                        code_hash: hash(0x00),
                        storage_usage: 64_470,
                    }),
                }),
                Some(Cause::TransactionProcessing(cause::TransactionProcessing {
                    tx_hash: hash(0xa1),
                })),
            ),
            change(
                Value::DataUpdate(value::DataUpdate {
                    account_id: "dex.near".into(),
                    key: b"k".to_vec().into(),
                    value: vec![].into(),
                }),
                Some(Cause::ReceiptProcessing(cause::ReceiptProcessing {
                    tx_hash: hash(0xb1),
                })),
            ),
            // Skipped: no value. Its position still counts.
            near::StateChangeWithCause {
                value: None,
                cause: Some(near::StateChangeCause { cause: None }),
            },
            change(
                Value::DataDeletion(value::DataDeletion {
                    account_id: "dex.near".into(),
                    key: b"gone".to_vec().into(),
                }),
                Some(Cause::ActionReceiptGasReward(
                    cause::ActionReceiptGasReward {
                        tx_hash: hash(0xb2),
                    },
                )),
            ),
            change(
                Value::AccessKeyUpdate(value::AccessKeyUpdate {
                    account_id: "alice.near".into(),
                    ..Default::default()
                }),
                Some(Cause::ActionReceiptProcessingStarted(
                    cause::ActionReceiptProcessingStarted {
                        receipt_hash: hash(0xb3),
                    },
                )),
            ),
            change(
                Value::AccountDeletion(value::AccountDeletion {
                    account_id: "bob.near".into(),
                }),
                Some(Cause::PostponedReceipt(cause::PostponedReceipt {
                    tx_hash: hash(0xb4),
                })),
            ),
            change(
                Value::ContractCodeUpdate(value::ContractCodeUpdate {
                    account_id: "dex.near".into(),
                    code: vec![0; 4].into(),
                }),
                Some(Cause::ValidatorAccountsUpdate(
                    cause::ValidatorAccountsUpdate {},
                )),
            ),
            // A cause whose hash message is absent has no hash.
            change(
                Value::AccountUpdate(value::AccountUpdate {
                    account_id: "carol.near".into(),
                    account: None,
                }),
                Some(Cause::ReceiptProcessing(cause::ReceiptProcessing {
                    tx_hash: None,
                })),
            ),
        ],
        ..Default::default()
    }
}

fn map(block: &near::Block, encoding: EncodeBytes) -> HashMap<String, RecordBatch> {
    let mut mapper = NearBlockMapper::new(false, encoding, false);
    mapper
        .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
        .unwrap();
    let batches = mapper.flush().unwrap();
    assert_eq!(mapper.total_rows(), 0);
    batches
}

fn column(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let array = batch.column_by_name(name).unwrap();
    (0..array.len())
        .map(|row| (!array.is_null(row)).then(|| array_value_to_string(array, row).unwrap()))
        .collect()
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|value| Some(value.to_string())).collect()
}

#[test]
fn state_changes_carry_index_cause_hashes_balances_and_data() {
    let batches = map(&block(), EncodeBytes::Base58);
    let changes = &batches["state_changes"];
    assert_eq!(changes.num_rows(), 7);
    assert_eq!(
        column(changes, "state_change_index"),
        some(&["0", "1", "3", "4", "5", "6", "7"])
    );
    assert_eq!(
        column(changes, "type"),
        some(&[
            "AccountUpdate",
            "DataUpdate",
            "DataDeletion",
            "AccessKeyUpdate",
            "AccountDeletion",
            "ContractCodeUpdate",
            "AccountUpdate",
        ])
    );
    assert_eq!(
        column(changes, "cause"),
        some(&[
            "TransactionProcessing",
            "ReceiptProcessing",
            "ActionReceiptGasReward",
            "ActionReceiptProcessingStarted",
            "PostponedReceipt",
            "ValidatorAccountsUpdate",
            "ReceiptProcessing",
        ])
    );
    let b58 = |byte: u8| Some(encode_base58(&[byte; 32]));
    assert_eq!(
        column(changes, "cause_tx_hash"),
        vec![b58(0xa1), None, None, None, None, None, None]
    );
    // Receipt causes, whose protobuf field is misnamed `tx_hash` in three cases.
    assert_eq!(
        column(changes, "cause_receipt_hash"),
        vec![None, b58(0xb1), b58(0xb2), b58(0xb3), b58(0xb4), None, None]
    );
    assert_eq!(
        column(changes, "data_key"),
        vec![
            None,
            Some(encode_base58(b"k")),
            Some(encode_base58(b"gone")),
            None,
            None,
            None,
            None
        ]
    );
    // A present empty value is a value; deletions have none.
    assert_eq!(
        column(changes, "data_value"),
        vec![None, Some(String::new()), None, None, None, None, None]
    );
    assert_eq!(
        column(changes, "amount"),
        vec![
            Some("370416707672993464458810570".into()),
            None,
            None,
            None,
            None,
            None,
            None
        ]
    );
    assert_eq!(
        column(changes, "locked")[0].as_deref(),
        Some("0"),
        "zero is a value"
    );
    assert_eq!(
        column(changes, "storage_usage")[0].as_deref(),
        Some("64470")
    );
    assert_eq!(column(changes, "code_hash")[0], b58(0x00));
    // An AccountUpdate without an account has no balances.
    for name in ["amount", "locked", "storage_usage", "code_hash"] {
        assert_eq!(column(changes, name)[6], None, "{name}");
        assert!(column(changes, name)[1..6].iter().all(Option::is_none));
    }
}

#[test]
fn state_change_bytes_follow_every_table_encoding() {
    for (encoding, expected_type) in [
        (EncodeBytes::Binary, DataType::Binary),
        (EncodeBytes::Hex, DataType::Utf8),
        (EncodeBytes::HexNoPrefix, DataType::Utf8),
        (EncodeBytes::Base58, DataType::Utf8),
        (EncodeBytes::TronBase58, DataType::Utf8),
    ] {
        let batches = map(&block(), encoding.clone());
        let changes = &batches["state_changes"];
        let schema = changes.schema();
        for name in [
            "cause_tx_hash",
            "cause_receipt_hash",
            "data_key",
            "data_value",
            "code_hash",
        ] {
            let field = schema.field_with_name(name).unwrap();
            assert_eq!(field.data_type(), &expected_type, "{encoding:?} {name}");
            assert!(field.is_nullable());
        }
        if encoding == EncodeBytes::Binary {
            let keys = changes
                .column_by_name("data_key")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            assert_eq!(keys.value(1), b"k");
            assert_eq!(keys.value(2), b"gone");
        }
    }
}

#[test]
fn state_changes_schema_is_the_507_layout() {
    for fork in [false, true] {
        let mut mapper = NearBlockMapper::new(fork, EncodeBytes::Base58, false);
        let batches = mapper.flush().unwrap();
        let schema = batches["state_changes"].schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let mut expected = vec![
            "block_num",
            "block_id",
            "parent_num",
            "parent_id",
            "lib_num",
            "timestamp",
            "date",
            "state_change_index",
            "type",
            "cause",
            "cause_tx_hash",
            "cause_receipt_hash",
            "account_id",
            "data_key",
            "data_value",
            "amount",
            "locked",
            "storage_usage",
            "code_hash",
        ];
        if fork {
            expected.push("fork_step");
        }
        assert_eq!(names, expected);
        for name in ["type", "cause"] {
            assert_eq!(
                schema.field_with_name(name).unwrap().data_type(),
                &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            );
        }
        let receipts = batches["receipts"].schema();
        let last = receipts.fields().last().unwrap();
        assert_eq!(last.name(), "success_receipt_id");
        assert!(last.is_nullable());
    }
}

#[test]
fn receipts_record_the_success_receipt_id_target() {
    use near::execution_outcome::Status;
    let outcome = |id: u8, status: Status| near::IndexerExecutionOutcomeWithReceipt {
        execution_outcome: Some(near::ExecutionOutcomeWithId {
            id: hash(id),
            outcome: Some(near::ExecutionOutcome {
                executor_id: "dex.near".into(),
                status: Some(status),
                ..Default::default()
            }),
            ..Default::default()
        }),
        receipt: Some(near::Receipt {
            predecessor_id: "alice.near".into(),
            receiver_id: "dex.near".into(),
            receipt_id: hash(id),
            receipt: None,
        }),
    };
    let mut block = block();
    block.state_changes.clear();
    block.shards = vec![near::IndexerShard {
        shard_id: 0,
        chunk: None,
        receipt_execution_outcomes: vec![
            outcome(
                0xc1,
                Status::SuccessReceiptId(near::SuccessReceiptIdExecutionStatus { id: hash(0xc2) }),
            ),
            outcome(
                0xc2,
                Status::SuccessValue(near::SuccessValueExecutionStatus {
                    value: b"1".to_vec().into(),
                }),
            ),
            outcome(
                0xc3,
                Status::Failure(near::FailureExecutionStatus::default()),
            ),
            outcome(
                0xc4,
                Status::SuccessReceiptId(near::SuccessReceiptIdExecutionStatus { id: None }),
            ),
        ],
    }];
    let batches = map(&block, EncodeBytes::Base58);
    let receipts = &batches["receipts"];
    assert_eq!(
        column(receipts, "status"),
        some(&[
            "SuccessReceiptId",
            "SuccessValue",
            "Failure",
            "SuccessReceiptId"
        ])
    );
    assert_eq!(
        column(receipts, "success_receipt_id"),
        vec![Some(encode_base58(&[0xc2; 32])), None, None, None]
    );
}
