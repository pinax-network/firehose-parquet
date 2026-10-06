//! The real HyperCore fixture blocks (`blocks/tests/fixtures/hypercore/`) and
//! the synthetic derivatives the cross-chain contract tests map.

use std::sync::OnceLock;

use firehose_parquet::traits::BlockIdentity;
use prost::Message;
use sha2::{Digest, Sha256};

use super::proto::hypercore as pb;

macro_rules! fixture {
    ($block:literal) => {
        (
            $block,
            include_bytes!(concat!(
                "../../tests/fixtures/hypercore/",
                stringify!($block),
                ".pb"
            ))
            .as_slice(),
        )
    };
}

/// The 35 small fixtures, by block number.
const SMALL: [(u64, &[u8]); 35] = [
    fixture!(846001240),
    fixture!(847193990),
    fixture!(889872017),
    fixture!(895702803),
    fixture!(897888967),
    fixture!(987247825),
    fixture!(1009557224),
    fixture!(1009612597),
    fixture!(1009686466),
    fixture!(1009701302),
    fixture!(1009721907),
    fixture!(1009855075),
    fixture!(1009867965),
    fixture!(1009868295),
    fixture!(1009877929),
    fixture!(1009907496),
    fixture!(1009925229),
    fixture!(1009958482),
    fixture!(1010128732),
    fixture!(1010355937),
    fixture!(1010423738),
    fixture!(1010581248),
    fixture!(1075395014),
    fixture!(1075987296),
    fixture!(1078677210),
    fixture!(1110656252),
    fixture!(1127672017),
    fixture!(1165601237),
    fixture!(1173346041),
    fixture!(1173352606),
    fixture!(1173408840),
    fixture!(1173546257),
    fixture!(1173674198),
    fixture!(1173744709),
    fixture!(1173886256),
];

/// The 2026-01-01T00:00:00Z funding, dust-conversion and validator-rewards
/// block, `zstd -19`.
pub(crate) const FUNDING_BLOCK: u64 = 846_903_317;
const FUNDING_BLOCK_ZST: &[u8] = include_bytes!("../../tests/fixtures/hypercore/846903317.pb.zst");
const FUNDING_BLOCK_SHA256: &str =
    "1d791d3f698c2fe24db9d9c9200897bf189c3f1b18e2252ca07374a5d26dd384";

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// All 36 fixture payloads, unmodified, in block order.
pub(crate) fn real_blocks() -> &'static [(u64, Vec<u8>)] {
    static BLOCKS: OnceLock<Vec<(u64, Vec<u8>)>> = OnceLock::new();
    BLOCKS.get_or_init(|| {
        let funding = zstd::decode_all(FUNDING_BLOCK_ZST).expect("decompress the funding block");
        assert_eq!(
            sha256_hex(&funding),
            FUNDING_BLOCK_SHA256,
            "846903317.pb.zst does not hold the recorded payload"
        );
        let mut blocks: Vec<(u64, Vec<u8>)> = SMALL
            .iter()
            .map(|(block, payload)| (*block, payload.to_vec()))
            .collect();
        blocks.push((FUNDING_BLOCK, funding));
        blocks.sort_by_key(|(block, _)| *block);
        blocks
    })
}

/// A fixture's true Firehose identity: the header's number and time, the
/// parent and LIB one block below (the endpoint's metadata), and the decimal
/// ids, which the mapper does not read.
pub(crate) fn identity_of(block: &pb::Block) -> BlockIdentity {
    let header = block.block_header.as_ref().expect("fixture header");
    let time = header.block_time.as_ref().expect("fixture block time");
    let number = header.block_number;
    BlockIdentity {
        block_num: number,
        block_id: number.to_string(),
        parent_num: number - 1,
        parent_id: (number - 1).to_string(),
        lib_num: number - 1,
        timestamp: time.seconds,
        timestamp_nanos: time.nanos,
        fork_step: None,
    }
}

/// Synthetic derivatives of the fixtures for harnesses with their own
/// identities: every fixture, in block order, with its header rewritten to
/// `first_block + offset` at `first_timestamp + offset` seconds and
/// `timestamp_nanos`. The funding block is cut to its first 8 fills and the
/// first 3 deltas of each funding event (its empty event stays empty); its 30
/// validator rewards are kept.
pub(crate) fn derived_blocks(
    first_block: u64,
    first_timestamp: i64,
    timestamp_nanos: i32,
) -> Vec<Vec<u8>> {
    real_blocks()
        .iter()
        .enumerate()
        .map(|(offset, (number, payload))| {
            let mut block = pb::Block::decode(payload.as_slice()).expect("decode fixture");
            if *number == FUNDING_BLOCK {
                block.fills.truncate(8);
                for event in &mut block.events {
                    for body in &mut event.events {
                        if let Some(pb::event_body::Event::Funding(funding)) = &mut body.event {
                            funding.deltas.truncate(3);
                        }
                    }
                }
            }
            block.block_header = Some(pb::BlockHeader {
                block_number: first_block + offset as u64,
                block_time: Some(prost_types::Timestamp {
                    seconds: first_timestamp + offset as i64,
                    nanos: timestamp_nanos,
                }),
            });
            block.encode_to_vec()
        })
        .collect()
}
