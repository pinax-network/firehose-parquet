//! Transfer projections independent of Arrow and Parquet.
//!
//! ERC-20 decoding uses Pinax's shared `substreams-abis` 2.0.0 decoder through
//! `substreams-ethereum` 0.12.0's `LogLike`, without copying or converting the
//! Firehose block. Call `match_log` and `decode` directly: `match_and_decode`
//! logs through the Substreams host on decode errors.
//!
//! Native transfers represent committed positive CALL/CREATE values. They are
//! not a ledger of every native balance change: fees, rewards, withdrawals,
//! minting and SELFDESTRUCT payouts remain in their source tables.

use super::proto::eth;
use substreams_abis::standard::erc20::events::Transfer;
use substreams_ethereum::LogLike;

pub const DECODER_VERSION: u32 = 1;

/// A standard ERC-20 receipt event. `value` is an exact unsigned decimal
/// integer in the token's raw units; mint, burn and zero-value events remain.
#[derive(Debug, PartialEq, Eq)]
pub struct Erc20Transfer<'a> {
    pub log_index: u32,
    pub block_index: u32,
    pub ordinal: u64,
    pub token_address: &'a [u8],
    pub from: Vec<u8>,
    pub to: Vec<u8>,
    pub value: String,
}

/// Adapter over our prost-generated log. The shared decoder uses only these
/// borrowed ABI fields; no second protobuf block or Substreams runtime is used.
struct AbiLog<'a>(&'a eth::Log);

impl LogLike for AbiLog<'_> {
    fn topic(&self, index: usize) -> Option<&[u8]> {
        self.0.topics.get(index).map(|topic| topic.as_ref())
    }

    fn topic_count(&self) -> usize {
        self.0.topics.len()
    }

    fn data(&self) -> &[u8] {
        &self.0.data
    }

    fn index(&self) -> u32 {
        self.0.index
    }
}

/// Decode only the standard three-topic, one-word ERC-20 event shape.
///
/// ABI addresses must have their twelve leading zero bytes. This rejects
/// malformed logs rather than silently truncating them. Four-topic ERC-721
/// events, which share the event signature, are not ERC-20 transfers.
pub fn decode_erc20_transfer(log: &eth::Log) -> Option<Erc20Transfer<'_>> {
    if log.address.len() != 20
        || log.topics.len() != 3
        || log.topics.iter().any(|topic| topic.len() != 32)
        || log.topics[1..]
            .iter()
            .any(|topic| topic[..12].iter().any(|&byte| byte != 0))
    {
        return None;
    }
    let abi_log = AbiLog(log);
    if !Transfer::match_log(&abi_log) {
        return None;
    }
    let decoded = Transfer::decode(&abi_log).ok()?;
    Some(Erc20Transfer {
        log_index: log.index,
        block_index: log.block_index,
        ordinal: log.ordinal,
        token_address: &log.address,
        from: decoded.from,
        to: decoded.to,
        value: decoded.value.to_string(),
    })
}

/// Read only successful transactions' receipt logs, which exclude events from
/// rolled-back child calls. Call logs are deliberately never decoded again.
pub fn erc20_transfers(tx: &eth::TransactionTrace) -> impl Iterator<Item = Erc20Transfer<'_>> {
    tx.receipt
        .iter()
        .filter(|_| tx.status == eth::TransactionTraceStatus::Succeeded as i32)
        .flat_map(|receipt| &receipt.logs)
        .filter_map(decode_erc20_transfer)
}

/// A committed positive call value. `value` contains exact big-endian unsigned
/// units; `ordinal` is the trace's recorded `begin_ordinal`.
#[derive(Debug, PartialEq, Eq)]
pub struct NativeTransfer<'a> {
    pub call_index: u32,
    pub call_type: eth::CallType,
    pub ordinal: u64,
    pub from: &'a [u8],
    pub to: &'a [u8],
    pub value: &'a [u8],
}

fn decode_native_transfer(call: &eth::Call) -> Option<NativeTransfer<'_>> {
    let call_type = eth::CallType::try_from(call.call_type).ok()?;
    if !matches!(call_type, eth::CallType::Call | eth::CallType::Create)
        || call.status_failed
        || call.status_reverted
        || call.state_reverted
        || call.caller.len() != 20
        || call.address.len() != 20
    {
        return None;
    }
    let value = call.value.as_ref()?.bytes.as_ref();
    if value.iter().all(|&byte| byte == 0) {
        return None;
    }
    Some(NativeTransfer {
        call_index: call.index,
        call_type,
        ordinal: call.begin_ordinal,
        from: &call.caller,
        to: &call.address,
        value,
    })
}

/// Root and internal CALL/CREATE values, each once. The transaction's `value`
/// is not emitted separately. `state_reverted` includes ancestor rollback.
pub fn native_transfers(tx: &eth::TransactionTrace) -> impl Iterator<Item = NativeTransfer<'_>> {
    tx.calls
        .iter()
        .filter(|_| tx.status == eth::TransactionTraceStatus::Succeeded as i32)
        .filter_map(decode_native_transfer)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRANSFER_TOPIC: [u8; 32] = [
        0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d,
        0xaa, 0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23,
        0xb3, 0xef,
    ];

    fn transfer_log(from: u8, to: u8, value: [u8; 32]) -> eth::Log {
        let topic = |address| {
            let mut bytes = vec![0; 12];
            bytes.extend([address; 20]);
            bytes.into()
        };
        eth::Log {
            address: vec![0xab; 20].into(),
            topics: vec![TRANSFER_TOPIC.to_vec().into(), topic(from), topic(to)],
            data: value.to_vec().into(),
            index: 3,
            block_index: 17,
            ordinal: 81,
        }
    }

    fn transaction() -> eth::TransactionTrace {
        eth::TransactionTrace {
            status: eth::TransactionTraceStatus::Succeeded as i32,
            receipt: Some(eth::TransactionReceipt {
                logs: vec![transfer_log(1, 2, [0xff; 32])],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn call(index: u32, call_type: eth::CallType) -> eth::Call {
        eth::Call {
            index,
            call_type: call_type as i32,
            caller: vec![1; 20].into(),
            address: vec![2; 20].into(),
            value: Some(eth::BigInt {
                bytes: vec![0xff; 32].into(),
            }),
            begin_ordinal: 11 + u64::from(index),
            ..Default::default()
        }
    }

    #[test]
    fn shared_decoder_preserves_full_uint256_and_source_identity() {
        let log = transfer_log(1, 2, [0xff; 32]);
        let transfer = decode_erc20_transfer(&log).unwrap();
        assert_eq!(transfer.token_address, &[0xab; 20]);
        assert_eq!(transfer.from, [1; 20]);
        assert_eq!(transfer.to, [2; 20]);
        assert_eq!(transfer.log_index, 3);
        assert_eq!(transfer.block_index, 17);
        assert_eq!(transfer.ordinal, 81);
        assert_eq!(
            transfer.value,
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
    }

    #[test]
    fn erc20_retains_mint_burn_and_zero_value_events() {
        for (from, to) in [(0, 2), (1, 0), (1, 2)] {
            let log = transfer_log(from, to, [0; 32]);
            let transfer = decode_erc20_transfer(&log).unwrap();
            assert_eq!(transfer.from, [from; 20]);
            assert_eq!(transfer.to, [to; 20]);
            assert_eq!(transfer.value, "0");
        }
    }

    #[test]
    fn erc20_rejects_nft_events_and_malformed_abi_words() {
        let valid = transfer_log(1, 2, [0; 32]);
        let mut invalid = Vec::new();
        let mut nft = valid.clone();
        nft.topics.push(vec![0; 32].into());
        nft.data = Default::default();
        invalid.push(nft);
        for len in [0, 19, 21, 32] {
            let mut log = valid.clone();
            log.address = vec![0; len].into();
            invalid.push(log);
        }
        for topic_index in 0..3 {
            for len in [0, 31, 33] {
                let mut log = valid.clone();
                log.topics[topic_index] = vec![0; len].into();
                invalid.push(log);
            }
        }
        for topic_index in [1, 2] {
            let mut log = valid.clone();
            let mut word = log.topics[topic_index].to_vec();
            word[0] = 1;
            log.topics[topic_index] = word.into();
            invalid.push(log);
        }
        for len in [0, 31, 33, 64] {
            let mut log = valid.clone();
            log.data = vec![0; len].into();
            invalid.push(log);
        }
        let mut wrong_signature = valid.clone();
        wrong_signature.topics[0] = vec![0; 32].into();
        invalid.push(wrong_signature);
        let mut missing_topic = valid;
        missing_topic.topics.pop();
        invalid.push(missing_topic);
        for log in invalid {
            assert!(decode_erc20_transfer(&log).is_none(), "{log:?}");
        }
    }

    #[test]
    fn only_succeeded_receipt_logs_are_decoded_once() {
        let mut tx = transaction();
        // Call logs also contain reverted events. None are another source.
        tx.calls = vec![eth::Call {
            logs: tx.receipt.as_ref().unwrap().logs.clone(),
            state_reverted: true,
            ..Default::default()
        }];
        assert_eq!(erc20_transfers(&tx).count(), 1);
        for status in [0, 2, 3, 99] {
            tx.status = status;
            assert_eq!(erc20_transfers(&tx).count(), 0);
        }
        tx.status = eth::TransactionTraceStatus::Succeeded as i32;
        tx.receipt = None;
        assert_eq!(erc20_transfers(&tx).count(), 0);
    }

    #[test]
    fn native_root_internal_call_and_create_values_are_emitted_once() {
        let mut tx = transaction();
        tx.value = Some(eth::BigInt {
            bytes: vec![1].into(),
        });
        tx.calls = vec![
            call(0, eth::CallType::Call),
            call(1, eth::CallType::Call),
            call(2, eth::CallType::Create),
        ];
        let transfers: Vec<_> = native_transfers(&tx).collect();
        assert_eq!(transfers.len(), 3);
        for (index, transfer) in transfers.iter().enumerate() {
            assert_eq!(transfer.call_index, index as u32);
            assert_eq!(transfer.ordinal, 11 + index as u64);
            assert_eq!(transfer.from, &[1; 20]);
            assert_eq!(transfer.to, &[2; 20]);
            assert_eq!(transfer.value, &[0xff; 32]);
        }
        assert_eq!(transfers[2].call_type, eth::CallType::Create);
    }

    #[test]
    fn native_filters_reverted_failed_nontransferring_and_zero_calls() {
        let valid = call(0, eth::CallType::Call);
        let mut excluded = Vec::new();
        for call_type in [
            eth::CallType::Unspecified,
            eth::CallType::Callcode,
            eth::CallType::Delegate,
            eth::CallType::Static,
        ] {
            excluded.push(call(0, call_type));
        }
        excluded.push(eth::Call {
            call_type: 99,
            ..valid.clone()
        });
        excluded.push(eth::Call {
            status_failed: true,
            ..valid.clone()
        });
        excluded.push(eth::Call {
            status_reverted: true,
            ..valid.clone()
        });
        // Successful child whose ancestor reverted still has state_reverted.
        excluded.push(eth::Call {
            state_reverted: true,
            ..valid.clone()
        });
        excluded.push(eth::Call {
            value: None,
            ..valid.clone()
        });
        for bytes in [vec![], vec![0], vec![0; 32]] {
            excluded.push(eth::Call {
                value: Some(eth::BigInt {
                    bytes: bytes.into(),
                }),
                ..valid.clone()
            });
        }
        excluded.push(eth::Call {
            caller: vec![0; 19].into(),
            ..valid.clone()
        });
        excluded.push(eth::Call {
            address: vec![0; 21].into(),
            ..valid
        });
        let mut tx = transaction();
        tx.calls = excluded;
        assert_eq!(native_transfers(&tx).count(), 0);
        tx.calls = vec![call(0, eth::CallType::Call)];
        for status in [0, 2, 3, 99] {
            tx.status = status;
            assert_eq!(native_transfers(&tx).count(), 0);
        }
    }
}
