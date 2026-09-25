use super::{
    mapper::{tests::make_test_block, SolanaBlockMapper},
    proto::solana,
    vote::VOTE_PROGRAM_ID,
};
use arrow::{
    array::{Array, BooleanArray, UInt32Array},
    compute::concat_batches,
    record_batch::RecordBatch,
};
use firehose_parquet::{
    encode::EncodeBytes,
    traits::{BlockIdentity, BlockMapper},
};
use prost::Message;
use std::collections::HashMap;

fn fixture(slot: u64) -> solana::Block {
    let mut block = make_test_block(slot);
    let success = block.transactions[0].clone();
    let mut failed = success.clone();
    // Serialized InstructionError(0, Custom(0)), also present in retained slot
    // 300000000 tx 163. The mapper only tests nonempty error bytes; it does not
    // infer per-instruction execution from them.
    let meta = failed.meta.as_mut().unwrap();
    meta.err = Some(solana::TransactionError {
        err: vec![8, 0, 0, 0, 0, 25, 0, 0, 0, 0, 0, 0, 0].into(),
    });
    meta.post_token_balances = meta.pre_token_balances.clone();
    let instructions = &mut failed
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .instructions;
    let later = instructions[0].clone();
    // These submitted instructions follow the failing first instruction; they
    // must remain in the plan without acquiring an invented instruction result.
    instructions.extend([later.clone(), later]);
    let mut empty_error = success.clone();
    empty_error.meta.as_mut().unwrap().err = Some(solana::TransactionError::default());
    let mut absent_meta = success.clone();
    absent_meta.meta = None;
    let mut vote = success.clone();
    let message = vote.transaction.as_mut().unwrap().message.as_mut().unwrap();
    message.versioned = false;
    message.address_table_lookups.clear();
    message.account_keys[1] = VOTE_PROGRAM_ID.to_vec().into();
    message.instructions[0].data =
        bincode::serialize(&solana_vote_interface::instruction::VoteInstruction::Vote(
            solana_vote_interface::state::Vote {
                slots: vec![98, 99],
                ..Default::default()
            },
        ))
        .unwrap()
        .into();
    let mut failed_vote = vote.clone();
    failed_vote.meta.as_mut().unwrap().err = failed.meta.as_ref().unwrap().err.clone();
    let mut absent_transaction = success.clone();
    absent_transaction.transaction = None;
    let mut absent_message = success.clone();
    absent_message.transaction.as_mut().unwrap().message = None;
    let mut opaque_error = success.clone();
    opaque_error.meta.as_mut().unwrap().err = Some(solana::TransactionError {
        err: vec![255].into(),
    });
    block.transactions = vec![
        success,
        failed,
        empty_error,
        absent_meta,
        vote,
        failed_vote,
        absent_transaction,
        absent_message,
        opaque_error,
    ];
    block
}

fn map(
    blocks: &[solana::Block],
    encoding: EncodeBytes,
    fork: bool,
    failed: bool,
    votes: bool,
    each: bool,
) -> HashMap<String, RecordBatch> {
    let mut mapper = SolanaBlockMapper::new(votes, fork, encoding, false, failed);
    let mut batches = HashMap::<String, Vec<RecordBatch>>::new();
    for (i, block) in blocks.iter().enumerate() {
        mapper
            .map_block(
                &block.encode_to_vec(),
                &BlockIdentity {
                    block_num: block.slot,
                    ..Default::default()
                },
                fork.then_some("FINAL"),
            )
            .unwrap();
        if each || i + 1 == blocks.len() {
            for (table, batch) in mapper.flush().unwrap() {
                batches.entry(table).or_default().push(batch);
            }
        }
    }
    assert!(mapper
        .flush()
        .unwrap()
        .values()
        .all(|batch| batch.num_rows() == 0));
    assert_eq!(mapper.total_rows(), 0);
    batches
        .into_iter()
        .map(|(table, batches)| {
            (
                table,
                concat_batches(&batches[0].schema(), &batches).unwrap(),
            )
        })
        .collect()
}

#[test]
fn context_preserves_filtering_votes_plans_snapshots_and_flushes_in_every_encoding() {
    let blocks = [fixture(100), fixture(101)];
    for encoding in [
        EncodeBytes::Binary,
        EncodeBytes::Base58,
        EncodeBytes::Hex,
        EncodeBytes::HexNoPrefix,
        EncodeBytes::TronBase58,
    ] {
        for fork in [false, true] {
            for include_failed in [false, true] {
                for votes in [false, true] {
                    let joined = map(
                        &blocks,
                        encoding.clone(),
                        fork,
                        include_failed,
                        votes,
                        false,
                    );
                    let split = map(&blocks, encoding.clone(), fork, include_failed, votes, true);
                    assert_eq!(
                        joined, split,
                        "encoding={encoding:?},fork={fork},failed={include_failed},votes={votes}"
                    );
                    let expected_indices = if include_failed {
                        vec![0, 1, 2, 8]
                    } else {
                        vec![0, 2]
                    };
                    for table in [
                        "messages",
                        "instructions",
                        "token_balances",
                        "account_lookups",
                        "rewards",
                    ] {
                        let batch = &joined[table];
                        assert_eq!(
                            batch.schema().fields().last().unwrap().name(),
                            "transaction_success"
                        );
                        assert_eq!(
                            batch
                                .schema()
                                .field_with_name("transaction_success")
                                .unwrap()
                                .is_nullable(),
                            table == "rewards"
                        );
                        let context = batch
                            .column_by_name("transaction_success")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap();
                        let indices = batch
                            .column_by_name("transaction_index")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap();
                        let mut observed = std::collections::BTreeSet::new();
                        let mut block_rewards = 0;
                        for row in 0..batch.num_rows() {
                            if indices.is_null(row) {
                                assert_eq!(table, "rewards");
                                assert!(context.is_null(row));
                                block_rewards += 1;
                            } else {
                                let index = indices.value(row);
                                assert!(
                                    expected_indices.contains(&index),
                                    "unexpected {table} transaction {index}"
                                );
                                assert!(context.is_valid(row));
                                assert_eq!(context.value(row), index == 0 || index == 2);
                                observed.insert(index);
                            }
                        }
                        assert_eq!(observed.into_iter().collect::<Vec<_>>(), expected_indices);
                        assert_eq!(block_rewards, if table == "rewards" { 2 } else { 0 });
                    }
                    let parents = &joined["transactions"];
                    let parent_indices = parents
                        .column_by_name("transaction_index")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap();
                    assert_eq!(parent_indices.values().to_vec(), expected_indices.repeat(2));
                    if votes {
                        assert_eq!(
                            joined["vote_transactions"].num_rows(),
                            if include_failed { 4 } else { 2 }
                        );
                    } else {
                        assert!(!joined.contains_key("vote_transactions"));
                    }
                    if include_failed {
                        let instructions = &joined["instructions"];
                        let indices = instructions
                            .column_by_name("transaction_index")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap();
                        let inner = instructions
                            .column_by_name("is_inner")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap();
                        let failed_rows = (0..indices.len())
                            .filter(|&row| indices.value(row) == 1)
                            .collect::<Vec<_>>();
                        assert_eq!(failed_rows.len(), 8); // Three submitted + one inner per block.
                        assert_eq!(
                            failed_rows.iter().filter(|&&row| inner.value(row)).count(),
                            2
                        );
                    }
                }
            }
        }
    }
}
