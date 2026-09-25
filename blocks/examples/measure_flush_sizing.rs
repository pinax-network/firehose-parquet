//! Offline file-sizing simulation using retained raw payloads. Replaying a small
//! fixture set changes compression statistics; this is not a throughput or live
//! production distribution benchmark. No network or cursor data is used.
use anyhow::{ensure, Context, Result};
use blocks::{evm::mapper::EvmBlockMapper, solana::mapper::SolanaBlockMapper};
use clap::Parser;
use firehose_parquet::{
    config::{BlockMetadata, Compression, Partition},
    encode::EncodeBytes,
    flush::{FlushSizing, MapperBufferEstimate, SizeFlushTrigger},
    traits::{BlockIdentity, BlockMapper},
    writer::ParquetTableWriter,
};
use firehose_protos::{eth, solana};
use prost::Message;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long, value_parser = ["evm", "solana"])]
    family: String,
    #[arg(long, required = true)]
    block: Vec<PathBuf>,
    #[arg(long, default_value_t = 33_554_432)]
    target: u64,
    #[arg(long, default_value_t = 268_435_456)]
    memory: u64,
    #[arg(long, default_value_t = 4)]
    windows: usize,
    #[arg(long, default_value_t = 4096)]
    max_blocks: usize,
    #[arg(long)]
    adaptive: bool,
    /// Synthetic EVM byte variation, preserving field lengths and row counts.
    #[arg(long)]
    vary_bytes: bool,
}

fn varied_bytes(input: &[u8], ordinal: u64) -> Vec<u8> {
    let key = Sha256::digest(input);
    let mut output = Vec::with_capacity(input.len());
    for chunk in 0..input.len().div_ceil(32) {
        let mut hash = Sha256::new();
        hash.update(b"fireparq-sizing-synthetic-v1");
        hash.update(key);
        hash.update(ordinal.to_le_bytes());
        hash.update((chunk as u64).to_le_bytes());
        output.extend_from_slice(&hash.finalize());
    }
    output.truncate(input.len());
    output
}

fn vary_evm(bytes: &[u8], ordinal: u64) -> Result<Vec<u8>> {
    let mut block = eth::Block::decode(bytes)?;
    macro_rules! vary {
        ($field:expr, $ordinal:expr) => {
            $field = varied_bytes(&$field, $ordinal).into();
        };
    }
    fn vary_log(log: &mut eth::Log, ordinal: u64) {
        vary!(log.address, ordinal);
        vary!(log.data, ordinal);
        for topic in &mut log.topics {
            vary!(*topic, ordinal);
        }
    }
    fn vary_call(call: &mut eth::Call, ordinal: u64) {
        vary!(call.caller, ordinal);
        vary!(call.address, ordinal);
        vary!(call.input, ordinal);
        vary!(call.return_data, ordinal);
        for change in &mut call.storage_changes {
            vary!(change.address, ordinal);
            vary!(change.key, ordinal);
            vary!(change.old_value, ordinal);
            vary!(change.new_value, ordinal);
        }
        for change in &mut call.balance_changes {
            vary!(change.address, ordinal);
        }
        for change in &mut call.nonce_changes {
            vary!(change.address, ordinal);
        }
        for change in &mut call.code_changes {
            vary!(change.address, ordinal);
            vary!(change.old_hash, ordinal);
            vary!(change.new_hash, ordinal);
            vary!(change.old_code, ordinal);
            vary!(change.new_code, ordinal);
        }
        for log in &mut call.logs {
            vary_log(log, ordinal);
        }
    }
    for tx in &mut block.transaction_traces {
        vary!(tx.hash, ordinal);
        vary!(tx.from, ordinal);
        vary!(tx.to, ordinal);
        vary!(tx.input, ordinal);
        vary!(tx.return_data, ordinal);
        for call in &mut tx.calls {
            vary_call(call, ordinal);
        }
        if let Some(receipt) = &mut tx.receipt {
            for log in &mut receipt.logs {
                vary_log(log, ordinal);
            }
        }
    }
    for call in &mut block.system_calls {
        vary_call(call, ordinal);
    }
    Ok(block.encode_to_vec())
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.target > 0 && args.memory > 0 && args.windows > 0,
        "positive bounds required"
    );
    ensure!(
        !args.vary_bytes || args.family == "evm",
        "byte variation is supported for EVM only"
    );
    let fixtures: Vec<_> = args
        .block
        .iter()
        .map(|path| -> Result<_> {
            let bytes = std::fs::read(path)?;
            let identity = if args.family == "evm" {
                let block = eth::Block::decode(bytes.as_slice())?;
                let header = block.header.context("missing EVM header")?;
                let time = header.timestamp.context("missing EVM timestamp")?;
                let hex = |data: &[u8]| data.iter().map(|v| format!("{v:02x}")).collect::<String>();
                BlockIdentity {
                    block_num: block.number,
                    block_id: hex(&block.hash),
                    parent_num: block.number - 1,
                    parent_id: hex(&header.parent_hash),
                    timestamp: time.seconds,
                    timestamp_nanos: time.nanos,
                    ..Default::default()
                }
            } else {
                let block = solana::Block::decode(bytes.as_slice())?;
                BlockIdentity {
                    block_num: block.slot,
                    block_id: block.blockhash,
                    parent_num: block.parent_slot,
                    parent_id: block.previous_blockhash,
                    timestamp: block
                        .block_time
                        .context("missing Solana timestamp")?
                        .timestamp,
                    ..Default::default()
                }
            };
            Ok((bytes, identity))
        })
        .collect::<Result<_>>()?;
    let sources: Vec<_> = fixtures.iter().map(|(bytes, identity)| json!({"block": identity.block_num, "bytes": bytes.len(), "sha256": format!("{:x}", Sha256::digest(bytes))})).collect();
    let mut mapper: Box<dyn BlockMapper> = if args.family == "evm" {
        Box::new(EvmBlockMapper::new(true, false, EncodeBytes::Hex, true))
    } else {
        Box::new(SolanaBlockMapper::new(
            true,
            false,
            EncodeBytes::Base58,
            false,
            true,
        ))
    };
    let output = tempfile::tempdir()?;
    let mut writer = ParquetTableWriter::new(output.path(), Partition::None, Compression::Zstd);
    let mut windows = Vec::new();
    let mut sizing = FlushSizing::new(args.target, args.memory)?;
    let mut blocks_in_window = 0;
    let mut min_block = u64::MAX;
    let mut max_block = 0;
    for ordinal in 0..args.max_blocks {
        let (bytes, identity) = &fixtures[ordinal % fixtures.len()];
        let varied = args
            .vary_bytes
            .then(|| vary_evm(bytes, ordinal as u64))
            .transpose()?;
        let bytes = varied.as_deref().unwrap_or(bytes);
        mapper.map_block(bytes, identity, None)?;
        blocks_in_window += 1;
        min_block = min_block.min(identity.block_num);
        max_block = max_block.max(identity.block_num);
        let estimates: BTreeMap<String, u64> = mapper
            .table_estimates()
            .into_iter()
            .map(|(name, size)| (name.to_owned(), size as u64))
            .collect();
        let largest = estimates.values().copied().max().unwrap_or_default();
        let total = estimates
            .values()
            .fold(0_u64, |sum, &size| sum.saturating_add(size));
        let reason = match sizing.trigger(MapperBufferEstimate {
            largest_table_bytes: largest,
            total_bytes: total,
        }) {
            Some(SizeFlushTrigger::Memory) => "memory",
            Some(SizeFlushTrigger::Bytes) => "bytes",
            None if ordinal + 1 == args.max_blocks => "end",
            None => continue,
        };
        let batches = mapper.flush()?;
        let allocated: usize = batches
            .values()
            .map(|batch| batch.get_array_memory_size())
            .sum();
        let mut table_bytes = BTreeMap::new();
        let mut rows = 0;
        for (table, batch) in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            rows += batch.num_rows();
            let (path, size) = writer.write_batch(
                table,
                batch,
                &BlockMetadata {
                    min_block_number: min_block,
                    max_block_number: max_block,
                    min_timestamp: None,
                    max_timestamp: None,
                },
            )?;
            ensure!(
                std::fs::metadata(&path)?.len() == size as u64,
                "file receipt size differs"
            );
            table_bytes.insert(table.clone(), size as u64);
            std::fs::remove_file(path)?;
        }
        let maximum = table_bytes.values().copied().max().unwrap_or_default();
        let previous_ratio = sizing.ratio();
        if args.adaptive {
            sizing.observe_committed(largest, maximum);
        }
        let ratio = sizing.ratio();
        windows.push(json!({"trigger": reason, "blocks": blocks_in_window, "rows": rows, "max_estimated_bytes": largest, "sum_estimated_bytes": total, "arrow_allocated_bytes": allocated, "max_file_bytes": maximum, "ratio_before": previous_ratio, "ratio_after": ratio, "table_estimates": estimates, "table_file_bytes": table_bytes}));
        blocks_in_window = 0;
        min_block = u64::MAX;
        max_block = 0;
        if windows.len() >= args.windows {
            break;
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"family": args.family, "sources": sources, "target_bytes": args.target, "memory_threshold_bytes": args.memory, "adaptive": args.adaptive, "synthetic_byte_variation": args.vary_bytes, "notes": "Offline sizing simulation, not real network throughput. Without variation, payloads and identities repeat unchanged. Variation deterministically changes EVM transaction/call/log/storage byte values while preserving lengths, row-producing counts, flags and scalars. All tables flush together through the production Zstd table writer. Protected transaction footer adds small per-file overhead.", "windows": windows})
        )?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn synthetic_bytes_keep_lengths_and_row_producing_structure() {
        let block = eth::Block {
            number: 123,
            transaction_traces: vec![eth::TransactionTrace {
                hash: vec![1; 32].into(),
                input: vec![2; 100].into(),
                status: 1,
                calls: vec![eth::Call {
                    index: 7,
                    state_reverted: true,
                    caller: vec![3; 20].into(),
                    input: vec![4; 65].into(),
                    logs: vec![eth::Log {
                        topics: vec![vec![5; 32].into()],
                        data: vec![6; 123].into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let raw = block.encode_to_vec();
        let a = vary_evm(&raw, 1).unwrap();
        let b = vary_evm(&raw, 2).unwrap();
        assert_eq!(a, vary_evm(&raw, 1).unwrap());
        assert_ne!(a, b);
        let changed = eth::Block::decode(a.as_slice()).unwrap();
        assert_eq!(changed.number, block.number);
        assert_eq!(changed.transaction_traces.len(), 1);
        let tx = &changed.transaction_traces[0];
        assert_eq!(tx.status, 1);
        assert_eq!(tx.hash.len(), 32);
        assert_eq!(tx.input.len(), 100);
        assert_ne!(tx.input, block.transaction_traces[0].input);
        assert_eq!(tx.calls.len(), 1);
        let call = &tx.calls[0];
        assert_eq!(call.index, 7);
        assert!(call.state_reverted);
        assert_eq!(call.caller.len(), 20);
        assert_eq!(call.input.len(), 65);
        assert_eq!(call.logs.len(), 1);
        assert_eq!(call.logs[0].data.len(), 123);
        assert_eq!(call.logs[0].topics[0].len(), 32);
        assert_eq!(varied_bytes(&[], 1), Vec::<u8>::new());
    }
}
