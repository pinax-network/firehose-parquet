//! Whole-block mapping benchmarks, ignored by default:
//! `cargo test --release -p blocks --lib bench_map_block -- --ignored --nocapture`

use std::hint::black_box;
use std::time::Instant;

use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{BlockIdentity, BlockMapper};
use prost::Message;

use crate::evm::mapper::EvmBlockMapper;
use crate::solana::mapper::SolanaBlockMapper;
use crate::{evm, solana};

const ITERATIONS: u32 = 200;

fn identity() -> BlockIdentity {
    BlockIdentity {
        block_num: 300_000_000,
        block_id: "0x".to_string() + &"ab".repeat(32),
        parent_num: 299_999_999,
        parent_id: "0x".to_string() + &"cd".repeat(32),
        lib_num: 299_999_968,
        timestamp: 1_700_000_000,
        timestamp_nanos: 0,
        fork_step: None,
    }
}

/// The Solana mapper test block with its transaction repeated `txs` times.
fn solana_block(txs: usize) -> Vec<u8> {
    let mut block = solana::mapper::tests::make_test_block(300_000_000);
    let tx = block.transactions[0].clone();
    block.transactions = vec![tx; txs];
    block.encode_to_vec()
}

/// The EVM mapper test block with `txs` transactions of `logs` logs each.
fn evm_block(txs: usize, logs: usize) -> Vec<u8> {
    let mut block = evm::mapper::tests::make_test_evm_block(20_000_000);
    let mut tx = block.transaction_traces[0].clone();
    let receipt = tx.receipt.as_mut().expect("fixture receipt");
    let log = receipt.logs[0].clone();
    receipt.logs = vec![log; logs];
    block.transaction_traces = vec![tx; txs];
    block.encode_to_vec()
}

/// Map and flush `block` repeatedly; print time per block and per output row.
fn bench(label: &str, mapper: &mut dyn BlockMapper, block: &[u8]) {
    let identity = identity();
    let mut rows = 0;
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        mapper
            .map_block(black_box(block), &identity, None)
            .expect("map block");
        let batches = mapper.flush().expect("flush");
        rows = batches
            .values()
            .map(|batch| batch.num_rows())
            .sum::<usize>();
        black_box(batches);
    }
    let per_block = start.elapsed().as_secs_f64() / f64::from(ITERATIONS);
    println!(
        "{label}: {:.3} ms/block, {rows} rows/block, {:.0} ns/row",
        per_block * 1e3,
        per_block * 1e9 / rows as f64
    );
}

#[test]
#[ignore = "benchmark"]
fn bench_map_block() {
    for encoding in [EncodeBytes::Base58, EncodeBytes::Binary] {
        let mut mapper = SolanaBlockMapper::new(true, false, encoding.clone(), false, true);
        bench(
            &format!("solana 1000 txs {encoding:?}"),
            &mut mapper,
            &solana_block(1_000),
        );
    }
    for encoding in [EncodeBytes::Hex, EncodeBytes::Binary] {
        let mut mapper = EvmBlockMapper::new(true, false, encoding.clone(), true);
        bench(
            &format!("evm 200 txs x 10 logs {encoding:?}"),
            &mut mapper,
            &evm_block(200, 10),
        );
    }
}
