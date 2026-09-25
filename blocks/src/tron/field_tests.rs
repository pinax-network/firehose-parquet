use super::{
    mapper::{tests::make_test_block, TronBlockMapper},
    proto::{protocol, tron},
};
use arrow::{array::*, datatypes::Int32Type, record_batch::RecordBatch};
use firehose_parquet::{
    config::{BlockMetadata, Compression, Partition},
    encode::{encode_bytes, EncodeBytes},
    traits::{BlockIdentity, BlockMapper},
    writer::{read_parquet, ParquetTableWriter},
};
use prost::Message;

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap()
}
fn contract<T: Message>(kind: i32, name: &str, value: T) -> protocol::transaction::Contract {
    protocol::transaction::Contract {
        r#type: kind,
        parameter: Some(prost_types::Any {
            type_url: format!("type.googleapis.com/protocol.{name}"),
            value: value.encode_to_vec(),
        }),
        permission_id: 7,
        ..Default::default()
    }
}
fn address(seed: u8) -> Vec<u8> {
    let mut v = vec![0x41];
    v.extend([seed; 20]);
    v
}
fn sample() -> tron::Block {
    let mut block = make_test_block(100);
    let tx = &mut block.transactions[0];
    tx.contracts = vec![
        contract(
            1,
            "TransferContract",
            protocol::TransferContract {
                owner_address: address(1).into(),
                to_address: address(2).into(),
                amount: 1234567890123456789,
            },
        ),
        contract(
            2,
            "TransferAssetContract",
            protocol::TransferAssetContract {
                asset_name: b"1000001".to_vec().into(),
                owner_address: address(3).into(),
                to_address: address(4).into(),
                amount: 17,
            },
        ),
        contract(
            31,
            "TriggerSmartContract",
            protocol::TriggerSmartContract {
                owner_address: address(5).into(),
                contract_address: address(6).into(),
                call_value: 19,
                data: vec![0, 255, 128].into(),
                call_token_value: 23,
                token_id: 1000001,
            },
        ),
        protocol::transaction::Contract {
            r#type: 777,
            parameter: Some(prost_types::Any {
                type_url: "custom/unknown".into(),
                value: vec![255, 0, 128],
            }),
            ..Default::default()
        },
        protocol::transaction::Contract {
            r#type: 0,
            ..Default::default()
        },
    ];
    let info = tx.info.as_mut().unwrap();
    info.receipt = Some(protocol::ResourceReceipt {
        energy_usage: 11,
        energy_fee: 12,
        origin_energy_usage: 13,
        energy_usage_total: 14,
        net_usage: 15,
        net_fee: 16,
        result: 2,
        energy_penalty_total: 18,
    });
    info.contract_address = address(7).into();
    info.res_message = vec![0, 255, 128].into();
    info.internal_transactions[0].call_value_info = vec![
        protocol::internal_transaction::CallValueInfo {
            call_value: i64::MAX,
            token_id: "".into(),
        },
        protocol::internal_transaction::CallValueInfo {
            call_value: -17,
            token_id: "1000001".into(),
        },
        protocol::internal_transaction::CallValueInfo {
            call_value: 0,
            token_id: "1000001".into(),
        },
    ];
    block
}
fn roundtrip(table: &str, batch: &RecordBatch) {
    if batch.num_rows() == 0 {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut writer = ParquetTableWriter::new(tmp.path(), Partition::None, Compression::Zstd);
    let (path, _) = writer
        .write_batch(
            table,
            batch,
            &BlockMetadata {
                min_block_number: 0,
                max_block_number: 0,
                min_timestamp: None,
                max_timestamp: None,
            },
        )
        .unwrap();
    let batches = read_parquet(&path).unwrap();
    let actual = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(&actual, batch);
}
#[test]
fn all_contracts_receipts_and_call_values_survive_every_encoding_and_flush() {
    let block = sample();
    for encoding in [
        EncodeBytes::Binary,
        EncodeBytes::Hex,
        EncodeBytes::HexNoPrefix,
        EncodeBytes::Base58,
        EncodeBytes::TronBase58,
    ] {
        for fork in [false, true] {
            let mut mapper = TronBlockMapper::new(fork, encoding.clone(), true);
            for _ in 0..2 {
                mapper
                    .map_block(
                        &block.encode_to_vec(),
                        &BlockIdentity::default(),
                        Some("FINAL"),
                    )
                    .unwrap();
                assert!(mapper.largest_table().1 > 0);
                let batches = mapper.flush().unwrap();
                assert_eq!(mapper.total_rows(), 0);
                assert_eq!(batches.len(), 6);
                for (table, batch) in &batches {
                    roundtrip(table, batch);
                }
                let contracts = &batches["contracts"];
                assert_eq!(contracts.num_rows(), 5);
                assert_eq!(
                    column::<UInt32Array>(contracts, "contract_index")
                        .values()
                        .as_ref(),
                    [0, 1, 2, 3, 4]
                );
                assert_eq!(
                    column::<Int64Array>(contracts, "amount").value(0),
                    1234567890123456789
                );
                assert_eq!(column::<Int64Array>(contracts, "amount").value(1), 17);
                assert!(column::<Int64Array>(contracts, "amount").is_null(2));
                assert_eq!(
                    column::<BinaryArray>(contracts, "asset_name").value(1),
                    b"1000001"
                );
                assert_eq!(
                    column::<BinaryArray>(contracts, "data").value(2),
                    [0, 255, 128]
                );
                assert_eq!(column::<Int64Array>(contracts, "call_value").value(2), 19);
                assert_eq!(
                    column::<Int64Array>(contracts, "call_token_value").value(2),
                    23
                );
                assert_eq!(
                    column::<Int64Array>(contracts, "token_id").value(2),
                    1000001
                );
                assert_eq!(column::<Int32Array>(contracts, "permission_id").value(2), 7);
                assert_eq!(
                    column::<BinaryArray>(contracts, "parameter").value(3),
                    [255, 0, 128]
                );
                assert!(column::<BinaryArray>(contracts, "parameter").is_null(4));
                let labels = column::<DictionaryArray<Int32Type>>(contracts, "contract_type")
                    .downcast_dict::<StringArray>()
                    .unwrap();
                assert_eq!(
                    column::<Int32Array>(contracts, "contract_type_id").value(3),
                    777
                );
                assert_eq!(labels.value(3), "UNKNOWN");
                assert_eq!(labels.value(4), "AccountCreateContract");
                for (row, seed) in [(0, 1), (1, 3), (2, 5)] {
                    if encoding == EncodeBytes::Binary {
                        assert_eq!(
                            column::<BinaryArray>(contracts, "owner_address").value(row),
                            address(seed)
                        );
                    } else {
                        assert_eq!(
                            column::<StringArray>(contracts, "owner_address").value(row),
                            encode_bytes(&address(seed), &encoding)
                        );
                    }
                }
                let transactions = &batches["transactions"];
                for (name, expected) in [
                    ("receipt_energy_usage", 11),
                    ("receipt_energy_fee", 12),
                    ("receipt_origin_energy_usage", 13),
                    ("receipt_energy_usage_total", 14),
                    ("receipt_net_usage", 15),
                    ("receipt_net_fee", 16),
                    ("receipt_energy_penalty_total", 18),
                ] {
                    assert_eq!(column::<Int64Array>(transactions, name).value(0), expected);
                }
                assert_eq!(
                    column::<DictionaryArray<Int32Type>>(transactions, "receipt_result")
                        .downcast_dict::<StringArray>()
                        .unwrap()
                        .value(0),
                    "REVERT"
                );
                assert_eq!(
                    column::<BinaryArray>(transactions, "res_message").value(0),
                    [0, 255, 128]
                );
                let values = &batches["internal_call_values"];
                assert_eq!(
                    column::<UInt32Array>(values, "call_value_index")
                        .values()
                        .as_ref(),
                    [0, 1, 2]
                );
                assert_eq!(
                    column::<Int64Array>(values, "call_value").values().as_ref(),
                    [i64::MAX, -17, 0]
                );
                assert_eq!(column::<StringArray>(values, "token_id").value(0), "");
                assert_eq!(
                    column::<StringArray>(values, "token_id").value(2),
                    "1000001"
                );
            }
        }
    }
}
#[test]
fn missing_messages_are_null_but_present_defaults_are_values() {
    let mut block = sample();
    let mut absent = block.transactions[0].clone();
    absent.contracts.clear();
    absent.info = None;
    let mut present = absent.clone();
    present.info = Some(protocol::TransactionInfo {
        receipt: Some(protocol::ResourceReceipt::default()),
        ..Default::default()
    });
    present.contracts = vec![contract(
        1,
        "TransferContract",
        protocol::TransferContract::default(),
    )];
    let mut missing_parameter = present.clone();
    missing_parameter.info.as_mut().unwrap().receipt = None;
    missing_parameter.contracts[0].parameter = None;
    block.transactions = vec![absent, present, missing_parameter];
    let mut mapper = TronBlockMapper::new(false, EncodeBytes::Binary, true);
    mapper
        .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
        .unwrap();
    let batches = mapper.flush().unwrap();
    let tx = &batches["transactions"];
    for name in [
        "contract_type",
        "receipt_energy_fee",
        "receipt_net_fee",
        "receipt_result",
        "contract_address",
        "res_message",
    ] {
        assert!(tx.column_by_name(name).unwrap().is_null(0));
        assert!(!tx.column_by_name(name).unwrap().is_null(1));
    }
    assert_eq!(column::<Int64Array>(tx, "receipt_energy_fee").value(1), 0);
    assert!(!tx.column_by_name("contract_type").unwrap().is_null(2));
    assert!(tx.column_by_name("receipt_energy_fee").unwrap().is_null(2));
    assert!(tx.column_by_name("receipt_result").unwrap().is_null(2));
    assert!(!tx.column_by_name("contract_address").unwrap().is_null(2));
    for name in ["parameter", "owner_address", "amount"] {
        assert!(batches["contracts"]
            .column_by_name(name)
            .unwrap()
            .is_null(1));
    }
    assert_eq!(
        column::<Int64Array>(&batches["contracts"], "amount").value(0),
        0
    );
    assert!(
        column::<BinaryArray>(&batches["contracts"], "owner_address")
            .value(0)
            .is_empty()
    );
}
#[test]
fn source_positions_do_not_renumber_after_failed_filtering() {
    let mut block = sample();
    let mut failed = block.transactions[0].clone();
    failed.result = false;
    let extra = failed.info.as_ref().unwrap().log[0].clone();
    failed.info.as_mut().unwrap().log.push(extra);
    block.transactions.insert(0, failed);
    for include_failed in [false, true] {
        let mut mapper = TronBlockMapper::new(false, EncodeBytes::Binary, include_failed);
        mapper
            .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
            .unwrap();
        let b = mapper.flush().unwrap();
        let row = usize::from(include_failed) * 2;
        assert_eq!(
            column::<UInt32Array>(&b["logs"], "transaction_index").value(row),
            1
        );
        assert_eq!(column::<UInt32Array>(&b["logs"], "log_index").value(row), 0);
        assert_eq!(
            column::<UInt64Array>(&b["logs"], "block_log_index").value(row),
            2
        );
        for table in [
            "transactions",
            "contracts",
            "internal_transactions",
            "internal_call_values",
        ] {
            assert_eq!(
                column::<UInt32Array>(&b[table], "transaction_index")
                    .value(b[table].num_rows() - 1),
                1
            );
        }
    }
}
#[test]
fn malformed_recognized_payload_rejects_all_rows_of_block() {
    let valid = sample();
    let mut mapper = TronBlockMapper::new(false, EncodeBytes::Binary, true);
    mapper
        .map_block(&valid.encode_to_vec(), &BlockIdentity::default(), None)
        .unwrap();
    let rows = mapper.total_rows();
    for mismatch in [false, true] {
        let mut invalid = valid.clone();
        invalid.transactions.push(invalid.transactions[0].clone());
        let parameter = invalid.transactions[1].contracts[2]
            .parameter
            .as_mut()
            .unwrap();
        if mismatch {
            parameter.type_url = "type.googleapis.com/protocol.TransferContract".into();
        } else {
            parameter.value = vec![255];
        }
        assert!(mapper
            .map_block(&invalid.encode_to_vec(), &BlockIdentity::default(), None)
            .is_err());
        assert_eq!(mapper.total_rows(), rows);
    }
    let batches = mapper.flush().unwrap();
    assert_eq!(batches["blocks"].num_rows(), 1);
    assert_eq!(batches["contracts"].num_rows(), 5);
}
#[test]
fn pinned_transfer_wire_tags_decode_independently() {
    // owner="a", to="b", amount=150: protocol's exact wire field numbers.
    let c = protocol::transaction::Contract {
        r#type: 1,
        parameter: Some(prost_types::Any {
            type_url: "type.googleapis.com/protocol.TransferContract".into(),
            value: vec![0x0a, 1, b'a', 0x12, 1, b'b', 0x18, 0x96, 1],
        }),
        ..Default::default()
    };
    let d = super::contracts::decode(&c).unwrap();
    assert_eq!(d.owner_address.unwrap().as_ref(), b"a");
    assert_eq!(d.to_address.unwrap().as_ref(), b"b");
    assert_eq!(d.amount, Some(150));
}

#[test]
fn pinned_asset_and_trigger_wire_tags_decode_independently() {
    let make = |kind, name: &str, value| protocol::transaction::Contract {
        r#type: kind,
        parameter: Some(prost_types::Any {
            type_url: format!("type.googleapis.com/protocol.{name}"),
            value,
        }),
        ..Default::default()
    };
    // asset_name tag1, owner tag2, recipient tag3, amount tag4.
    let asset = make(
        2,
        "TransferAssetContract",
        vec![0x0a, 1, b't', 0x12, 1, b'a', 0x1a, 1, b'b', 0x20, 0x96, 1],
    );
    let d = super::contracts::decode(&asset).unwrap();
    assert_eq!(d.asset_name.unwrap().as_ref(), b"t");
    assert_eq!(d.owner_address.unwrap().as_ref(), b"a");
    assert_eq!(d.to_address.unwrap().as_ref(), b"b");
    assert_eq!(d.amount, Some(150));
    // owner/target tags1/2, call_value3, data4, token value5, token id6.
    let trigger = make(
        31,
        "TriggerSmartContract",
        vec![
            0x0a, 1, b'a', 0x12, 1, b'b', 0x18, 0x96, 1, 0x22, 2, 255, 0, 0x28, 2, 0x30, 3,
        ],
    );
    let d = super::contracts::decode(&trigger).unwrap();
    assert_eq!(d.owner_address.unwrap().as_ref(), b"a");
    assert_eq!(d.contract_address.unwrap().as_ref(), b"b");
    assert_eq!(d.call_value, Some(150));
    assert_eq!(d.data.unwrap().as_ref(), [255, 0]);
    assert_eq!(d.call_token_value, Some(2));
    assert_eq!(d.token_id, Some(3));
}
