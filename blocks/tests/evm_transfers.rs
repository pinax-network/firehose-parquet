//! Transfer table contracts through the public mapper, Delta conversion and
//! actual Parquet writer/reader. Real retained blocks are covered in evm_golden.
use std::collections::HashMap;

use arrow::{
    array::{BinaryArray, Int64Array, StringArray},
    compute::concat_batches,
    datatypes::DataType,
    record_batch::RecordBatch,
};
use blocks::{chain::ChainKind, evm::mapper::EvmBlockMapper};
use firehose_parquet::{
    config::{BlockMetadata, Compression},
    delta::types::is_delta_type,
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper, StreamEvent},
    writer::{decode_parquet, encode_parquet, ParquetFileMetadata},
};
use firehose_protos::eth;
use prost::{bytes::Bytes, Message};

const TRANSFER_TOPIC: [u8; 32] = [
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
];
const U256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";
const TIMESTAMP: i64 = 1_700_000_000;

fn topic(address: u8) -> Bytes {
    [vec![0; 12], vec![address; 20]].concat().into()
}

fn transfer_log(index: u32, from: u8, to: u8, value: Vec<u8>) -> eth::Log {
    eth::Log {
        address: vec![0xcc; 20].into(),
        topics: vec![TRANSFER_TOPIC.to_vec().into(), topic(from), topic(to)],
        data: value.into(),
        index,
        block_index: index + 50,
        ordinal: u64::from(index) + 100,
    }
}

fn call(index: u32, kind: eth::CallType, amount: Vec<u8>) -> eth::Call {
    eth::Call {
        index,
        call_type: kind as i32,
        caller: vec![0x11; 20].into(),
        address: vec![0x22; 20].into(),
        value: Some(eth::BigInt {
            bytes: amount.into(),
        }),
        begin_ordinal: 1_000 + u64::from(index),
        ..Default::default()
    }
}

fn fixture() -> eth::Block {
    let mut one = vec![0; 32];
    one[31] = 1;
    let mut two = vec![0; 32];
    two[31] = 2;
    let mut logs = vec![
        transfer_log(0, 0x11, 0x22, vec![0xff; 32]),
        // Zero, mint, burn and self-transfer remain observable events.
        transfer_log(1, 0, 0x22, vec![0; 32]),
        transfer_log(2, 0x11, 0, one),
        transfer_log(3, 0x11, 0x11, two),
    ];
    let base = logs[0].clone();
    let mut wrong_signature = base.clone();
    wrong_signature.topics[0] = vec![0xee; 32].into();
    let mut erc721 = base.clone();
    erc721.topics.push(topic(0x33));
    erc721.data = Bytes::new();
    let mut short_data = base.clone();
    short_data.data = vec![0xff; 31].into();
    let mut long_data = base.clone();
    long_data.data = vec![0xff; 33].into();
    let mut noncanonical_address = base.clone();
    let mut word = noncanonical_address.topics[1].to_vec();
    word[0] = 1;
    noncanonical_address.topics[1] = word.into();
    let mut short_address = base.clone();
    short_address.topics[2] = vec![0x22; 20].into();
    let mut missing_topic = base.clone();
    missing_topic.topics.pop();
    let mut bad_contract = base.clone();
    bad_contract.address = vec![0xcc; 19].into();
    logs.extend([
        wrong_signature,
        erc721,
        short_data,
        long_data,
        noncanonical_address,
        short_address,
        missing_topic,
        bad_contract,
    ]);

    let mut calls = vec![
        call(0, eth::CallType::Call, vec![0xff; 32]),
        call(1, eth::CallType::Create, vec![1]),
        call(2, eth::CallType::Call, vec![0; 32]),
        call(3, eth::CallType::Delegate, vec![1]),
        call(4, eth::CallType::Static, vec![1]),
        call(5, eth::CallType::Callcode, vec![1]),
        call(6, eth::CallType::Call, vec![1]),
        call(7, eth::CallType::Call, vec![1]),
        call(8, eth::CallType::Call, vec![1]),
        call(9, eth::CallType::Call, vec![]),
        call(10, eth::CallType::Unspecified, vec![1]),
        call(11, eth::CallType::Create, vec![0, 0]),
        call(12, eth::CallType::Call, vec![0, 1]),
    ];
    calls[6].status_failed = true;
    calls[7].status_reverted = true;
    // A child may have succeeded locally but be reverted by its parent.
    calls[8].state_reverted = true;
    calls[9].value = None;
    // Receipt logs are authoritative; the same call logs must not duplicate
    // transfers. The extra call-only log must not leak into receipt output.
    calls[0].logs = logs.clone();
    calls[0]
        .logs
        .push(transfer_log(99, 0x11, 0x22, vec![0xff; 32]));
    let tx = eth::TransactionTrace {
        index: 0,
        hash: vec![0xab; 32].into(),
        from: vec![0x11; 20].into(),
        to: vec![0x22; 20].into(),
        // Same transfer as the root call, which must be emitted exactly once.
        value: Some(eth::BigInt {
            bytes: vec![0xff; 32].into(),
        }),
        status: eth::TransactionTraceStatus::Succeeded as i32,
        receipt: Some(eth::TransactionReceipt {
            logs,
            ..Default::default()
        }),
        calls,
        ..Default::default()
    };
    let mut traces = vec![tx.clone()];
    for (offset, status) in [
        eth::TransactionTraceStatus::Failed,
        eth::TransactionTraceStatus::Reverted,
        eth::TransactionTraceStatus::Unknown,
    ]
    .into_iter()
    .enumerate()
    {
        let mut failed = tx.clone();
        failed.index = offset as u32 + 1;
        failed.hash = vec![offset as u8 + 1; 32].into();
        failed.status = status as i32;
        traces.push(failed);
    }
    eth::Block {
        number: 100,
        hash: vec![0xbb; 32].into(),
        header: Some(eth::BlockHeader {
            number: 100,
            timestamp: Some(prost_types::Timestamp {
                seconds: TIMESTAMP,
                nanos: 0,
            }),
            ..Default::default()
        }),
        transaction_traces: traces,
        // System calls are intentionally outside the transaction transfer table.
        system_calls: vec![tx.calls[0].clone()],
        ..Default::default()
    }
}

fn identity() -> BlockIdentity {
    BlockIdentity {
        block_num: 100,
        block_id: "bb".repeat(32),
        parent_num: 99,
        parent_id: "aa".repeat(32),
        lib_num: 98,
        timestamp: TIMESTAMP,
        timestamp_nanos: 0,
        fork_step: None,
    }
}

fn delta_batches(mapper: &mut EvmBlockMapper) -> HashMap<String, RecordBatch> {
    ChainKind::Evm
        .profile()
        .delta_types()
        .data_batches(
            mapper.flush().unwrap(),
            &BlockMetadata {
                min_block_number: 100,
                max_block_number: 100,
                min_timestamp: Some(TIMESTAMP),
                max_timestamp: Some(TIMESTAMP),
            },
        )
        .unwrap()
}

fn parquet_round_trip(batch: &RecordBatch) {
    assert!(batch.column_by_name("date").is_none());
    assert!(batch
        .schema()
        .fields()
        .iter()
        .all(|field| is_delta_type(field.data_type())));
    let encoded = encode_parquet(batch, Compression::Zstd, &ParquetFileMetadata::new()).unwrap();
    let decoded = decode_parquet(encoded).unwrap();
    let round_trip = concat_batches(&batch.schema(), &decoded).unwrap();
    assert_eq!(&round_trip, batch);
}

fn integer(batch: &RecordBatch, column: &str, row: usize) -> i64 {
    batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
}

fn text<'a>(batch: &'a RecordBatch, column: &str, row: usize) -> &'a str {
    batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row)
}

fn assert_bytes(batch: &RecordBatch, column: &str, row: usize, byte: u8, len: usize) {
    let values = batch.column_by_name(column).unwrap();
    match values.data_type() {
        DataType::Binary => assert_eq!(
            values
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row),
            vec![byte; len]
        ),
        DataType::Utf8 => {
            assert_eq!(
                text(batch, column, row),
                format!("0x{}", format!("{byte:02x}").repeat(len))
            )
        }
        other => panic!("unexpected bytes representation {other}"),
    }
}

#[test]
fn transfers_keep_exact_values_and_filter_non_transfers_through_delta_and_parquet() {
    let block = fixture().encode_to_vec();
    for encoding in [EncodeBytes::Binary, EncodeBytes::Hex] {
        for extended in [false, true] {
            for include_failed in [false, true] {
                let mut mapper =
                    EvmBlockMapper::new(extended, false, encoding.clone(), include_failed);
                mapper
                    .map_block(&block, &identity(), StreamEvent::default())
                    .unwrap();
                let batches = delta_batches(&mut mapper);
                let tokens = &batches["erc20_transfers"];
                assert_eq!(tokens.num_rows(), 4);
                for (row, amount) in [U256_MAX, "0", "1", "2"].into_iter().enumerate() {
                    assert_eq!(text(tokens, "value", row), amount);
                    assert_eq!(integer(tokens, "block_number", row), 100);
                    assert_eq!(integer(tokens, "tx_index", row), 0);
                    assert_eq!(integer(tokens, "log_index", row), row as i64);
                    assert_eq!(integer(tokens, "block_index", row), row as i64 + 50);
                    assert_eq!(integer(tokens, "ordinal", row), row as i64 + 100);
                    assert_eq!(integer(tokens, "decoder_version", row), 1);
                    assert_bytes(tokens, "token_address", row, 0xcc, 20);
                    assert_bytes(tokens, "tx_hash", row, 0xab, 32);
                    assert_bytes(tokens, "from", row, if row == 1 { 0 } else { 0x11 }, 20);
                    assert_bytes(
                        tokens,
                        "to",
                        row,
                        match row {
                            2 => 0,
                            3 => 0x11,
                            _ => 0x22,
                        },
                        20,
                    );
                }
                parquet_round_trip(tokens);
                assert_eq!(batches.contains_key("native_transfers"), extended);
                if extended {
                    let native = &batches["native_transfers"];
                    assert_eq!(native.num_rows(), 3);
                    for (row, (index, amount, kind)) in
                        [(0, U256_MAX, "CALL"), (1, "1", "CREATE"), (12, "1", "CALL")]
                            .into_iter()
                            .enumerate()
                    {
                        assert_eq!(integer(native, "tx_index", row), 0);
                        assert_eq!(integer(native, "call_index", row), index);
                        assert_eq!(integer(native, "ordinal", row), 1_000 + index);
                        assert_eq!(integer(native, "decoder_version", row), 1);
                        assert_eq!(text(native, "value", row), amount);
                        assert_eq!(text(native, "call_type", row), kind);
                        assert_bytes(native, "from", row, 0x11, 20);
                        assert_bytes(native, "to", row, 0x22, 20);
                    }
                    parquet_round_trip(native);
                }
                // Every builder must reset at flush, including both new tables.
                let empty = mapper.flush().unwrap();
                assert_eq!(empty["erc20_transfers"].num_rows(), 0);
                if extended {
                    assert_eq!(empty["native_transfers"].num_rows(), 0);
                }
            }
        }
    }
}

#[test]
fn receipt_only_blocks_emit_tokens_and_non_final_events_keep_distinct_ordinals() {
    let mut block = fixture();
    // BASE payloads have no call trace. ERC20 remains available, while native
    // value cannot be inferred by duplicating the transaction envelope.
    block.transaction_traces.truncate(1);
    block.transaction_traces[0].calls.clear();
    block.system_calls.clear();
    let raw = block.encode_to_vec();
    for encoding in [EncodeBytes::Binary, EncodeBytes::Hex] {
        let mut mapper = EvmBlockMapper::new(true, true, encoding, true);
        for (step, ordinal) in [("NEW", 7_000), ("UNDO", 7_001)] {
            mapper
                .map_block_bytes(
                    Bytes::from(raw.clone()),
                    &identity(),
                    StreamEvent::new(Some(step), ordinal),
                )
                .unwrap();
        }
        let batches = delta_batches(&mut mapper);
        let tokens = &batches["erc20_transfers"];
        assert_eq!(tokens.num_rows(), 8);
        assert_eq!(batches["native_transfers"].num_rows(), 0);
        for row in 0..8 {
            assert_eq!(integer(tokens, "block_num", row), 100);
            assert_eq!(
                integer(tokens, "stream_ordinal", row),
                7_000 + (row / 4) as i64
            );
            assert_eq!(
                text(tokens, "fork_step", row),
                if row < 4 { "NEW" } else { "UNDO" }
            );
            assert_eq!(integer(tokens, "ordinal", row), 100 + (row % 4) as i64);
        }
        parquet_round_trip(tokens);
    }
}
