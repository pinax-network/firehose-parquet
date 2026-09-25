use super::{
    mapper::{tests::make_test_block, SolanaBlockMapper},
    proto::solana,
    vote::VOTE_PROGRAM_ID,
};
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use firehose_parquet::{
    encode::{encode_bytes, EncodeBytes},
    traits::{BlockIdentity, BlockMapper},
};
use prost::Message;

fn binary<'a>(batch: &'a RecordBatch, name: &str) -> &'a BinaryArray {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap()
}

fn assert_indices(batch: &RecordBatch, name: &str, row: usize, expected: &[u8]) {
    let values = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(!values.is_null(row));
    let list = values.value(row);
    let bytes = list.as_any().downcast_ref::<UInt8Array>().unwrap();
    assert_eq!(bytes.null_count(), 0);
    assert_eq!(bytes.values().as_ref(), expected);
    let DataType::List(field) = values.data_type() else {
        panic!("not a list")
    };
    assert!(!field.is_nullable());
}

#[test]
fn payloads_are_binary_and_indices_are_lists_under_every_identity_encoding() {
    let mut block = make_test_block(100);
    let first = &mut block.transactions[0];
    let msg = first
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap();
    msg.instructions[0].accounts = vec![255, 0, 255, 1].into();
    msg.instructions[0].data = (0..=255).cycle().take(4096).collect();
    msg.instructions
        .push(solana::CompiledInstruction::default());
    msg.address_table_lookups[0].writable_indexes = vec![255, 0, 255].into();
    msg.address_table_lookups[0].readonly_indexes.clear();
    let meta = first.meta.as_mut().unwrap();
    meta.inner_instructions[0].instructions[0].accounts = vec![255, 0].into();
    meta.inner_instructions[0].instructions[0].data = vec![0, 255, 128, 0].into();
    meta.return_data.as_mut().unwrap().data = vec![0, 255, 128, 0].into();
    meta.err = Some(solana::TransactionError {
        err: vec![255, 0, 128].into(),
    });
    let mut absent = first.clone();
    absent.meta.as_mut().unwrap().return_data = None;
    absent.meta.as_mut().unwrap().err = Some(solana::TransactionError::default());
    let mut empty = absent.clone();
    empty.meta.as_mut().unwrap().return_data = Some(solana::ReturnData {
        program_id: vec![3; 32].into(),
        data: vec![].into(),
    });
    empty.meta.as_mut().unwrap().err = None;
    block.transactions.extend([absent, empty]);
    for encoding in [
        EncodeBytes::Base58,
        EncodeBytes::Binary,
        EncodeBytes::Hex,
        EncodeBytes::HexNoPrefix,
        EncodeBytes::TronBase58,
    ] {
        for fork in [false, true] {
            let mut mapper = SolanaBlockMapper::new(true, fork, encoding.clone(), false, true);
            for _ in 0..2 {
                mapper
                    .map_block(
                        &block.encode_to_vec(),
                        &BlockIdentity::default(),
                        Some("FINAL"),
                    )
                    .unwrap();
                // Exercise changed memory estimates without altering builder contents.
                assert!(mapper.largest_table().1 > 4096);
                let batches = mapper.flush().unwrap();
                let transactions = &batches["transactions"];
                assert_eq!(transactions.num_rows(), 3);
                assert_eq!(binary(transactions, "err").value(0), [255, 0, 128]);
                assert!(binary(transactions, "err").is_null(1));
                assert!(binary(transactions, "err").is_null(2));
                let returned = binary(transactions, "return_data");
                assert_eq!(returned.value(0), [0, 255, 128, 0]);
                assert!(returned.is_null(1));
                assert!(!returned.is_null(2));
                assert!(returned.value(2).is_empty());
                for name in ["signature", "return_data_program_id"] {
                    let expected = if name == "signature" {
                        vec![1; 64]
                    } else {
                        vec![3; 32]
                    };
                    if encoding == EncodeBytes::Binary {
                        assert_eq!(binary(transactions, name).value(0), expected);
                    } else {
                        let strings = transactions
                            .column_by_name(name)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        assert_eq!(strings.value(0), encode_bytes(&expected, &encoding));
                    }
                }
                let instructions = &batches["instructions"];
                assert_eq!(instructions.num_rows(), 9);
                for (tx_index, source) in block.transactions.iter().enumerate() {
                    let row = tx_index * 3;
                    let msg = source
                        .transaction
                        .as_ref()
                        .unwrap()
                        .message
                        .as_ref()
                        .unwrap();
                    assert_indices(instructions, "accounts", row, &[255, 0, 255, 1]);
                    assert_indices(instructions, "accounts", row + 1, &[]);
                    assert_indices(instructions, "accounts", row + 2, &[255, 0]);
                    assert_eq!(
                        binary(instructions, "data").value(row),
                        msg.instructions[0].data
                    );
                    assert!(!binary(instructions, "data").is_null(row + 1));
                    assert!(binary(instructions, "data").value(row + 1).is_empty());
                    assert_eq!(
                        binary(instructions, "data").value(row + 2),
                        [0, 255, 128, 0]
                    );
                    assert_indices(
                        &batches["account_lookups"],
                        "writable_indexes",
                        tx_index,
                        &[255, 0, 255],
                    );
                    assert_indices(
                        &batches["account_lookups"],
                        "readonly_indexes",
                        tx_index,
                        &[],
                    );
                }
                assert_eq!(mapper.total_rows(), 0);
                let empty = mapper.flush().unwrap();
                assert_eq!(empty["instructions"].num_rows(), 0);
                assert_eq!(empty["instructions"].schema(), instructions.schema());
            }
        }
    }
}

#[test]
fn vote_table_uses_identical_payload_types_and_retains_filtering() {
    use solana_vote_interface::{instruction::VoteInstruction, state::Vote};
    let mut block = make_test_block(100);
    let transaction = block.transactions[0].transaction.as_mut().unwrap();
    let msg = transaction.message.as_mut().unwrap();
    msg.versioned = false;
    msg.address_table_lookups.clear();
    msg.account_keys[1] = VOTE_PROGRAM_ID.to_vec().into();
    msg.instructions[0].data = bincode::serialize(&VoteInstruction::Vote(Vote {
        slots: vec![98, 99],
        ..Default::default()
    }))
    .unwrap()
    .into();
    let meta = block.transactions[0].meta.as_mut().unwrap();
    meta.err = Some(solana::TransactionError {
        err: vec![255, 128, 0].into(),
    });
    meta.return_data.as_mut().unwrap().data = vec![0, 255, 128].into();
    for with_votes in [false, true] {
        for include_failed in [false, true] {
            let mut mapper = SolanaBlockMapper::new(
                with_votes,
                false,
                EncodeBytes::Base58,
                false,
                include_failed,
            );
            mapper
                .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
                .unwrap();
            let batches = mapper.flush().unwrap();
            assert_eq!(batches["transactions"].num_rows(), 0);
            assert_eq!(batches["instructions"].num_rows(), 0);
            if with_votes {
                let votes = &batches["vote_transactions"];
                assert_eq!(votes.schema(), batches["transactions"].schema());
                assert_eq!(votes.num_rows(), usize::from(include_failed));
                if include_failed {
                    assert_eq!(binary(votes, "err").value(0), [255, 128, 0]);
                    assert_eq!(binary(votes, "return_data").value(0), [0, 255, 128]);
                }
            }
        }
    }
}
