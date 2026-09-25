use super::{
    mapper::{tests::make_test_block, CosmosBlockMapper},
    proto::{cosmos, cosmos_tx},
};
use arrow::array::*;
use arrow::datatypes::UInt32Type;
use arrow::record_batch::RecordBatch;
use firehose_parquet::{
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper},
};
use prost::Message;

fn fixture_tx() -> cosmos_tx::Tx {
    cosmos_tx::Tx {
        body: Some(cosmos_tx::TxBody {
            messages: vec![prost_types::Any {
                type_url: "/cosmos.bank.v1beta1.MsgSend".into(),
                value: vec![0, 255],
            }],
            memo: "memo\nwith unicode λ".into(),
            timeout_height: u64::MAX,
        }),
        auth_info: Some(cosmos_tx::AuthInfo {
            fee: Some(cosmos_tx::Fee {
                amount: vec![
                    cosmos_tx::Coin {
                        denom: "uatom".into(),
                        amount: "0".into(),
                    },
                    cosmos_tx::Coin {
                        denom: "uatom".into(),
                        amount: "340282366920938463463374607431768211456".into(),
                    },
                ],
                gas_limit: u64::MAX,
                payer: "cosmos1payer".into(),
                granter: String::new(),
            }),
            signer_infos: vec![
                cosmos_tx::SignerInfo {
                    public_key: Some(prost_types::Any {
                        type_url: "/custom.Key".into(),
                        value: vec![0, 255],
                    }),
                    mode_info: vec![vec![10, 2, 8, 1].into()],
                    sequence: u64::MAX,
                },
                cosmos_tx::SignerInfo {
                    public_key: None,
                    mode_info: vec![vec![].into()],
                    sequence: 0,
                },
            ],
        }),
        // Keep source order/cardinality even when signatures and signer_infos differ.
        signatures: vec![vec![0, 255].into()],
    }
}
fn map(
    mapper: &mut CosmosBlockMapper,
    block: &cosmos::Block,
) -> std::collections::HashMap<String, RecordBatch> {
    mapper
        .map_block(
            &block.encode_to_vec(),
            &BlockIdentity::default(),
            Some("NEW"),
        )
        .unwrap();
    mapper.flush().unwrap()
}
fn list<'a>(batch: &'a RecordBatch, name: &str) -> &'a ListArray {
    batch.column_by_name(name).unwrap().as_list::<i32>()
}

#[test]
fn transaction_metadata_preserves_native_values_in_all_encodings_and_flushes() {
    for encoding in [
        EncodeBytes::Binary,
        EncodeBytes::Hex,
        EncodeBytes::HexNoPrefix,
        EncodeBytes::Base58,
        EncodeBytes::TronBase58,
    ] {
        for fork in [false, true] {
            let tx = fixture_tx();
            let raw = tx.encode_to_vec();
            let mut block = make_test_block(100);
            block.txs = vec![raw.clone().into()];
            let mut mapper = CosmosBlockMapper::new(fork, encoding.clone(), true);
            for _ in 0..2 {
                let out = map(&mut mapper, &block);
                let b = &out["transactions"];
                assert_eq!(
                    b.column_by_name("raw_tx")
                        .unwrap()
                        .as_binary::<i32>()
                        .value(0),
                    raw
                );
                assert!(b
                    .column_by_name("decode_success")
                    .unwrap()
                    .as_boolean()
                    .value(0));
                assert_eq!(
                    b.column_by_name("memo")
                        .unwrap()
                        .as_string::<i32>()
                        .value(0),
                    tx.body.as_ref().unwrap().memo
                );
                assert_eq!(
                    b.column_by_name("fee_gas_limit")
                        .unwrap()
                        .as_primitive::<arrow::datatypes::UInt64Type>()
                        .value(0),
                    u64::MAX
                );
                let fees = list(b, "fee_amount").value(0);
                let fees = fees.as_struct();
                assert_eq!(
                    fees.column(0).as_string::<i32>().iter().collect::<Vec<_>>(),
                    [Some("uatom"), Some("uatom")]
                );
                assert_eq!(
                    fees.column(1).as_string::<i32>().value(1),
                    "340282366920938463463374607431768211456"
                );
                let signers = list(b, "signer_infos").value(0);
                let signers = signers.as_struct();
                assert_eq!(signers.len(), 2);
                assert_eq!(signers.column(0).as_string::<i32>().value(0), "/custom.Key");
                assert!(signers.column(0).is_null(1));
                assert!(signers.column(1).is_null(1));
                assert_eq!(signers.column(1).as_binary::<i32>().value(0), [0, 255]);
                assert_eq!(signers.column(2).as_binary::<i32>().value(0), [10, 2, 8, 1]);
                assert!(!signers.column(2).is_null(1));
                assert!(signers.column(2).as_binary::<i32>().value(1).is_empty());
                assert_eq!(
                    signers
                        .column(3)
                        .as_primitive::<arrow::datatypes::UInt64Type>()
                        .value(0),
                    u64::MAX
                );
                let signatures = list(b, "signatures").value(0);
                assert_eq!(signatures.len(), 1);
                assert_eq!(signatures.as_binary::<i32>().value(0), [0, 255]);
                for batch in out.values() {
                    let mut bytes = Vec::new();
                    let mut writer =
                        parquet::arrow::ArrowWriter::try_new(&mut bytes, batch.schema(), None)
                            .unwrap();
                    writer.write(batch).unwrap();
                    writer.close().unwrap();
                    let read =
                        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                            prost::bytes::Bytes::from(bytes),
                        )
                        .unwrap()
                        .build()
                        .unwrap()
                        .next()
                        .unwrap()
                        .unwrap();
                    assert_eq!(*batch, read);
                }
                assert_eq!(mapper.total_rows(), 0);
                assert_eq!(
                    mapper.largest_table().1,
                    CosmosBlockMapper::new(fork, encoding.clone(), true)
                        .largest_table()
                        .1
                );
            }
        }
    }
}

#[test]
fn absent_results_and_failed_decode_are_unknown_not_success_or_empty_metadata() {
    let mut block = make_test_block(100);
    let present = cosmos_tx::Tx {
        body: Some(Default::default()),
        auth_info: Some(cosmos_tx::AuthInfo {
            fee: Some(Default::default()),
            ..Default::default()
        }),
        ..Default::default()
    };
    block.txs = vec![
        fixture_tx().encode_to_vec().into(),
        vec![255].into(),
        vec![].into(),
        present.encode_to_vec().into(),
    ];
    block.tx_results.truncate(1);
    let out = map(
        &mut CosmosBlockMapper::new(false, EncodeBytes::Hex, false),
        &block,
    );
    let tx = &out["transactions"];
    assert_eq!(tx.num_rows(), 4);
    for name in ["code", "gas_wanted", "gas_used", "log", "info", "codespace"] {
        let c = tx.column_by_name(name).unwrap();
        assert!(!c.is_null(0));
        assert!((1..4).all(|row| c.is_null(row)));
    }
    assert_eq!(
        tx.column_by_name("decode_success")
            .unwrap()
            .as_boolean()
            .iter()
            .collect::<Vec<_>>(),
        [Some(true), Some(false), Some(true), Some(true)]
    );
    assert_eq!(
        out["blocks"]
            .column_by_name("tx_decode_failures")
            .unwrap()
            .as_primitive::<UInt32Type>()
            .value(0),
        1
    );
    for name in ["memo", "timeout_height", "fee_amount", "signer_infos"] {
        assert!(tx.column_by_name(name).unwrap().is_null(1));
        assert!(tx.column_by_name(name).unwrap().is_null(2));
        assert!(!tx.column_by_name(name).unwrap().is_null(3));
    }
    assert!(list(tx, "signatures").is_null(1));
    assert!(list(tx, "signatures").value(2).is_empty());
    assert!(list(tx, "fee_amount").value(3).is_empty());
    assert!(list(tx, "signer_infos").value(3).is_empty());
    assert_eq!(
        tx.column_by_name("memo")
            .unwrap()
            .as_string::<i32>()
            .value(3),
        ""
    );
}

#[test]
fn events_preserve_empty_events_repeated_attributes_and_source_indices() {
    let mut block = make_test_block(100);
    block.events = vec![
        cosmos::Event {
            r#type: "empty".into(),
            attributes: vec![],
        },
        cosmos::Event {
            r#type: "duplicates".into(),
            attributes: vec![
                cosmos::EventAttribute {
                    key: "key".into(),
                    value: "one".into(),
                },
                cosmos::EventAttribute {
                    key: "key".into(),
                    value: "two".into(),
                },
            ],
        },
        cosmos::Event {
            r#type: "empty-attribute".into(),
            attributes: vec![Default::default()],
        },
    ];
    let raw = fixture_tx().encode_to_vec();
    block.txs = vec![raw.clone().into(), vec![255].into(), raw.into()];
    let success = cosmos::TxResults {
        events: vec![cosmos::Event {
            r#type: "tx-empty".into(),
            attributes: vec![],
        }],
        ..Default::default()
    };
    block.tx_results = vec![
        success.clone(),
        cosmos::TxResults {
            code: 7,
            events: success.events.clone(),
            ..Default::default()
        },
        success,
    ];
    for include_failed in [false, true] {
        let out = map(
            &mut CosmosBlockMapper::new(false, EncodeBytes::Binary, include_failed),
            &block,
        );
        let events = &out["events"];
        assert_eq!(events.num_rows(), if include_failed { 7 } else { 6 });
        assert!((0..4).all(|row| events.column_by_name("tx_hash").unwrap().is_null(row)));
        assert_eq!(
            events
                .column_by_name("attribute_index")
                .unwrap()
                .as_primitive::<UInt32Type>()
                .iter()
                .take(4)
                .collect::<Vec<_>>(),
            [None, Some(0), Some(1), Some(0)]
        );
        assert!(events.column_by_name("key").unwrap().is_null(0));
        assert_eq!(
            events
                .column_by_name("key")
                .unwrap()
                .as_string::<i32>()
                .value(3),
            ""
        );
        assert_eq!(
            events
                .column_by_name("value")
                .unwrap()
                .as_string::<i32>()
                .value(2),
            "two"
        );
        let expected = if include_failed {
            vec![Some(0), Some(1), Some(2)]
        } else {
            vec![Some(0), Some(2)]
        };
        assert_eq!(
            events
                .column_by_name("tx_index")
                .unwrap()
                .as_primitive::<UInt32Type>()
                .iter()
                .skip(4)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            out["transactions"]
                .column_by_name("index")
                .unwrap()
                .as_primitive::<UInt32Type>()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            out["blocks"]
                .column_by_name("tx_decode_failures")
                .unwrap()
                .as_primitive::<UInt32Type>()
                .value(0),
            1
        );
    }
}

#[test]
fn independent_sdk_wire_vector_decodes_fee_signer_and_present_empty_mode() {
    // Handwritten standard SDK field tags, independent of the generated encoder.
    // Tx(body(memo="x"),auth(signer(sequence=7,mode_info=""),fee(gas=9)),sig="z").
    let raw = vec![
        0x0a, 3, 0x12, 1, b'x', 0x12, 10, 0x0a, 4, 0x12, 0, 0x18, 7, 0x12, 2, 0x10, 9, 0x1a, 1,
        b'z',
    ];
    let mut block = make_test_block(100);
    block.txs = vec![raw.clone().into()];
    let out = map(
        &mut CosmosBlockMapper::new(false, EncodeBytes::Hex, true),
        &block,
    );
    let tx = &out["transactions"];
    assert_eq!(
        tx.column_by_name("memo")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "x"
    );
    assert_eq!(
        tx.column_by_name("fee_gas_limit")
            .unwrap()
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        9
    );
    let signers = list(tx, "signer_infos").value(0);
    let signers = signers.as_struct();
    assert_eq!(
        signers
            .column(3)
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        7
    );
    assert!(!signers.column(2).is_null(0));
    assert!(signers.column(2).as_binary::<i32>().value(0).is_empty());
    assert_eq!(
        tx.column_by_name("raw_tx")
            .unwrap()
            .as_binary::<i32>()
            .value(0),
        raw
    );
}

#[test]
fn raw_envelope_uses_last_bytes_occurrence_and_rejects_malformed_nested_fields() {
    use super::tx_metadata::decode_tx;
    // body(memo=a), body(timeout=7), auth(fee(gas=9)), auth(empty).
    let raw = [
        0x0a, 3, 0x12, 1, b'a', 0x0a, 2, 0x18, 7, 0x12, 4, 0x12, 2, 0x10, 9, 0x12, 0,
    ];
    let tx = decode_tx(&raw[..]).unwrap();
    let body = tx.body.unwrap();
    assert_eq!(body.memo, "");
    assert_eq!(body.timeout_height, 7);
    assert!(tx.auth_info.unwrap().fee.is_none());
    let tx = decode_tx(&[0x0a, 3, 0x12, 1, b'a', 0x0a, 0][..]).unwrap();
    assert_eq!(tx.body.unwrap(), cosmos_tx::TxBody::default());
    assert!(decode_tx(&[0x0a, 1, 255][..]).is_err());
    assert!(decode_tx(&[0x12, 1, 255][..]).is_err());
}

#[test]
fn repeated_opaque_mode_messages_concatenate_without_losing_merge_semantics() {
    for second in [vec![], vec![10, 0]] {
        let mut tx = fixture_tx();
        let signer = &mut tx.auth_info.as_mut().unwrap().signer_infos[0];
        signer.mode_info = vec![vec![10, 2, 8, 1].into(), second.clone().into()];
        let mut block = make_test_block(100);
        block.txs = vec![tx.encode_to_vec().into()];
        let out = map(
            &mut CosmosBlockMapper::new(false, EncodeBytes::Binary, true),
            &block,
        );
        let signers = list(&out["transactions"], "signer_infos").value(0);
        let modes = signers.as_struct().column(2).as_binary::<i32>();
        assert_eq!(modes.value(0), [vec![10, 2, 8, 1], second].concat());
    }
}

#[test]
fn failed_transaction_filter_preserves_matching_source_indices_in_all_child_tables() {
    let mut block = make_test_block(100);
    block.txs = vec![fixture_tx().encode_to_vec().into(); 3];
    block.tx_results = vec![block.tx_results[0].clone(); 3];
    block.tx_results[1].code = 7;
    for include_failed in [false, true] {
        let out = map(
            &mut CosmosBlockMapper::new(false, EncodeBytes::Binary, include_failed),
            &block,
        );
        let expected = if include_failed {
            vec![0, 1, 2]
        } else {
            vec![0, 2]
        };
        for (table, column) in [
            ("transactions", "index"),
            ("messages", "tx_index"),
            ("events", "tx_index"),
        ] {
            let actual = out[table]
                .column_by_name(column)
                .unwrap()
                .as_primitive::<UInt32Type>()
                .iter()
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "{table}");
        }
    }
}

#[test]
fn captured_cosmos_hub_transaction_retains_fee_signer_and_message_metadata() {
    // Expectations independently extracted from the captured RPC transaction by
    // docs/audit/510-qualify.py, not generated by the mapper or SDK decoder.
    let raw = include_bytes!("../../tests/fixtures/cosmos-33121486/tx.pb");
    let mut block = make_test_block(33_121_486);
    block.txs = vec![raw.to_vec().into()];
    let out = map(
        &mut CosmosBlockMapper::new(false, EncodeBytes::Binary, true),
        &block,
    );
    let tx = &out["transactions"];
    assert_eq!(
        tx.column_by_name("raw_tx")
            .unwrap()
            .as_binary::<i32>()
            .value(0),
        raw
    );
    assert_eq!(
        tx.column_by_name("memo")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        ""
    );
    assert_eq!(
        tx.column_by_name("fee_gas_limit")
            .unwrap()
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        142_900
    );
    let fee = list(tx, "fee_amount").value(0);
    assert_eq!(fee.len(), 1);
    assert_eq!(
        fee.as_struct().column(0).as_string::<i32>().value(0),
        "uatom"
    );
    assert_eq!(fee.as_struct().column(1).as_string::<i32>().value(0), "715");
    let signers = list(tx, "signer_infos").value(0);
    assert_eq!(signers.len(), 1);
    let signer = signers.as_struct();
    assert_eq!(
        signer.column(0).as_string::<i32>().value(0),
        "/cosmos.crypto.secp256k1.PubKey"
    );
    assert_eq!(signer.column(1).as_binary::<i32>().value(0).len(), 35);
    assert_eq!(signer.column(2).as_binary::<i32>().value(0), [10, 2, 8, 1]);
    assert_eq!(
        signer
            .column(3)
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        0
    );
    let signatures = list(tx, "signatures").value(0);
    assert_eq!(signatures.len(), 1);
    assert_eq!(signatures.as_binary::<i32>().value(0).len(), 64);
    assert_eq!(out["messages"].num_rows(), 1);
    assert_eq!(
        out["messages"]
            .column_by_name("type_url")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "/cosmos.bank.v1beta1.MsgSend"
    );
}

#[test]
fn owned_cosmos_transaction_decode_keeps_nested_mode_info_in_original_buffer() {
    use prost::bytes::Bytes;
    let mut block = make_test_block(100);
    block.txs = vec![fixture_tx().encode_to_vec().into()];
    let wire = Bytes::from(block.encode_to_vec());
    let start = wire.as_ptr() as usize;
    let end = start + wire.len();
    let decoded = cosmos::Block::decode(wire.clone()).unwrap();
    let tx = super::tx_metadata::decode_tx(decoded.txs[0].clone()).unwrap();
    let mode = &tx.auth_info.as_ref().unwrap().signer_infos[0].mode_info[0];
    assert_eq!(mode.as_ref(), &[10, 2, 8, 1]);
    let ptr = mode.as_ptr() as usize;
    assert!(ptr >= start && ptr + mode.len() <= end);
    let mode = mode.clone();
    drop(wire);
    drop(decoded);
    drop(tx);
    assert_eq!(mode.as_ref(), &[10, 2, 8, 1]);
}
