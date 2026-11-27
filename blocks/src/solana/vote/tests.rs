//! The vote-program decoder (`simple_vote_last_slot`) on the payloads bincode
//! 1.3.3 gave `solana-vote-interface` 7.1.0's `VoteInstruction`, recorded
//! before #633 replaced both crates, and on every truncated, extended,
//! non-canonical and invalid variation of them. Before the replacement, the
//! decoder agreed with the old classifier (bincode decode, `is_simple_vote`,
//! a last voted slot, identical re-encoding) on 93.2 million generated and
//! mutated payloads (`docs/audit/501-solana-vote-classification.md`). The
//! transaction envelope checks are `mapper.rs`'s tests.
use super::payloads::{fixture_votes, simple_vote, vote, Kind, Tower};
use super::*;

/// 32 zero bytes, in hex: the default hash.
macro_rules! hash0 {
    () => {
        "0000000000000000000000000000000000000000000000000000000000000000"
    };
}

/// The recorded bytes, by `VoteInstruction` value, and the old classifier's
/// last voted slot for each (`None`: not a simple vote).
const RECORDED: &[(&str, &str, Option<u64>)] = &[
    // The #501 fixtures: the eight simple votes for slots 98 and 99.
    (
        "Vote { slots: [98, 99] }",
        concat!(
            "02000000",
            "0200000000000000",
            "6200000000000000",
            "6300000000000000",
            hash0!(),
            "00"
        ),
        Some(99),
    ),
    (
        "VoteSwitch",
        concat!(
            "06000000",
            "0200000000000000",
            "6200000000000000",
            "6300000000000000",
            hash0!(),
            "00",
            hash0!()
        ),
        Some(99),
    ),
    (
        "UpdateVoteState [(98, 2), (99, 1)]",
        concat!(
            "08000000",
            "0200000000000000",
            "620000000000000002000000",
            "630000000000000001000000",
            "00",
            hash0!(),
            "00"
        ),
        Some(99),
    ),
    (
        "UpdateVoteStateSwitch",
        concat!(
            "09000000",
            "0200000000000000",
            "620000000000000002000000",
            "630000000000000001000000",
            "00",
            hash0!(),
            "00",
            hash0!()
        ),
        Some(99),
    ),
    (
        "CompactUpdateVoteState",
        concat!(
            "0c000000",
            "ffffffffffffffff",
            "02",
            "6202",
            "0101",
            hash0!(),
            "00"
        ),
        Some(99),
    ),
    (
        "CompactUpdateVoteStateSwitch",
        concat!(
            "0d000000",
            "ffffffffffffffff",
            "02",
            "6202",
            "0101",
            hash0!(),
            "00",
            hash0!()
        ),
        Some(99),
    ),
    (
        "TowerSync",
        concat!(
            "0e000000",
            "ffffffffffffffff",
            "02",
            "6202",
            "0101",
            hash0!(),
            "00",
            hash0!()
        ),
        Some(99),
    ),
    (
        "TowerSyncSwitch",
        concat!(
            "0f000000",
            "ffffffffffffffff",
            "02",
            "6202",
            "0101",
            hash0!(),
            "00",
            hash0!(),
            hash0!()
        ),
        Some(99),
    ),
    // A root, a timestamp and multi-byte offsets (`rooted()`).
    (
        "UpdateVoteState, rooted",
        concat!(
            "08000000",
            "0300000000000000",
            "00a3e111000000001f000000",
            "80a3e111000000001e000000",
            "80e3e1110000000001000000",
            "01f6a2e11100000000",
            hash0!(),
            "0100f1536500000000"
        ),
        Some(300_016_512),
    ),
    (
        "CompactUpdateVoteState, rooted",
        concat!(
            "0c000000",
            "f6a2e11100000000",
            "03",
            "0a1f",
            "80011e",
            "80800101",
            hash0!(),
            "0100f1536500000000"
        ),
        Some(300_016_512),
    ),
    (
        "TowerSync, rooted",
        concat!(
            "0e000000",
            "f6a2e11100000000",
            "03",
            "0a1f",
            "80011e",
            "80800101",
            hash0!(),
            "0100f1536500000000",
            hash0!()
        ),
        Some(300_016_512),
    ),
    // Not simple votes: administrative instructions and an empty vote.
    ("Withdraw(42)", "030000002a00000000000000", None),
    (
        "Authorize(default, Voter)",
        concat!("01000000", hash0!(), "00000000"),
        None,
    ),
    ("AuthorizeChecked(Withdrawer)", "0700000001000000", None),
    ("UpdateValidatorIdentity", "04000000", None),
    ("UpdateCommission(5)", "0500000005", None),
    (
        "InitializeAccount(default)",
        concat!("00000000", hash0!(), hash0!(), hash0!(), "00"),
        None,
    ),
    (
        "Vote::default()",
        concat!("02000000", "0000000000000000", hash0!(), "00"),
        None,
    ),
];

fn bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
        .collect()
}

/// The tower of the rooted recordings: offsets of 1, 2 and 3 LEB128 bytes.
fn rooted() -> Tower {
    Tower {
        lockouts: vec![(300_000_000, 31), (300_000_128, 30), (300_016_512, 1)],
        root: Some(299_999_990),
        timestamp: Some(1_700_000_000),
    }
}

/// A `TowerSync` of 130 lockouts from slot 1000, whose `short_vec` length
/// takes two bytes, as recorded.
fn long_tower() -> (Vec<u8>, Vec<u8>) {
    let tower = Tower {
        lockouts: (0..130).map(|i| (1000 + i, 1)).collect(),
        ..Tower::default()
    };
    let recorded = bytes(&format!(
        "0e000000ffffffffffffffff8201e80701{}{}00{}",
        "0101".repeat(129),
        hash0!(),
        hash0!()
    ));
    (simple_vote(Kind::TowerSync, &tower), recorded)
}

#[test]
fn the_test_payloads_are_the_recorded_bytes() {
    let mut built = fixture_votes();
    for kind in [
        Kind::UpdateVoteState,
        Kind::CompactUpdateVoteState,
        Kind::TowerSync,
    ] {
        built.push(simple_vote(kind, &rooted()));
    }
    for ((label, hex, _), built) in RECORDED.iter().zip(&built) {
        assert_eq!(*built, bytes(hex), "{label}");
    }
    assert_eq!(built.len(), 11);
    assert_eq!(vote(&[98, 99]), bytes(RECORDED[0].1));
    let (built, recorded) = long_tower();
    assert_eq!(built, recorded);

    // The mapper tests' administrative instructions and empty vote.
    let mut not_votes = super::payloads::administrative();
    not_votes.push(("Vote::default()", vote(&[])));
    assert_eq!(not_votes.len(), RECORDED.len() - 11);
    for ((label, built), (recorded_label, hex, _)) in not_votes.iter().zip(&RECORDED[11..]) {
        assert_eq!(*built, bytes(hex), "{label} as {recorded_label}");
    }
}

#[test]
fn recorded_payloads_classify_as_before() {
    for (label, hex, last_slot) in RECORDED {
        assert_eq!(simple_vote_last_slot(&bytes(hex)), *last_slot, "{label}");
    }
    assert_eq!(simple_vote_last_slot(&long_tower().1), Some(1129));
}

/// Every simple-vote variant, with or without a root and a timestamp, is
/// recognized with its last slot; an empty one is not a vote.
#[test]
fn every_simple_vote_variant_is_recognized_unless_empty() {
    let towers = [
        Tower::fixture(),
        rooted(),
        Tower {
            lockouts: vec![(0, 0)],
            ..Tower::default()
        },
        Tower {
            lockouts: vec![(u64::MAX, 255)],
            root: Some(u64::MAX - 1),
            timestamp: Some(i64::MIN),
        },
    ];
    for kind in Kind::ALL {
        for tower in &towers {
            let last = tower.lockouts.last().unwrap().0;
            let data = simple_vote(kind, tower);
            assert_eq!(
                simple_vote_last_slot(&data),
                Some(last),
                "{kind:?} {tower:?}"
            );
        }
        let empty = Tower::default();
        assert_eq!(simple_vote_last_slot(&simple_vote(kind, &empty)), None);
    }
}

/// A payload cut short anywhere, or followed by any byte, is not a vote.
#[test]
fn truncated_or_extended_payloads_are_not_votes() {
    let mut payloads = fixture_votes();
    payloads.extend(Kind::ALL.map(|kind| simple_vote(kind, &rooted())));
    payloads.push(long_tower().0);
    for data in payloads {
        assert!(simple_vote_last_slot(&data).is_some());
        for len in 0..data.len() {
            assert_eq!(simple_vote_last_slot(&data[..len]), None, "{len} bytes");
        }
        for extra in 0..=u8::MAX {
            let mut longer = data.clone();
            longer.push(extra);
            assert_eq!(simple_vote_last_slot(&longer), None, "trailing {extra}");
        }
    }
}

/// A compact `CompactUpdateVoteState` with these raw `short_vec` length and
/// lockout bytes, a zero hash and no timestamp.
fn compact(root: u64, count: &[u8], lockouts: &[u8]) -> Vec<u8> {
    let mut data = variant::COMPACT_UPDATE_VOTE_STATE.to_le_bytes().to_vec();
    data.extend(root.to_le_bytes());
    data.extend(count);
    data.extend(lockouts);
    data.extend([0; 32]);
    data.push(0);
    data
}

/// A `Vote` declaring `count` slots, holding `slots`, with this timestamp tag.
fn vote_of(count: u64, slots: &[u64], timestamp_tag: u8) -> Vec<u8> {
    let mut data = variant::VOTE.to_le_bytes().to_vec();
    data.extend(count.to_le_bytes());
    slots
        .iter()
        .for_each(|slot| data.extend(slot.to_le_bytes()));
    data.extend([0; 32]);
    data.push(timestamp_tag);
    data
}

/// A LEB128 offset of `u64::MAX`: nine bytes of seven ones, then 1.
fn max_offset() -> Vec<u8> {
    let mut offset = vec![0xff; 9];
    offset.push(0x01);
    offset
}

/// Payloads bincode, upstream's varint decoders or the compact form refuse.
fn refused() -> Vec<(&'static str, Vec<u8>)> {
    let mut bad_root_tag = simple_vote(Kind::UpdateVoteState, &Tower::fixture());
    // The variant, the lockout count and two 12-byte lockouts precede it.
    bad_root_tag[4 + 8 + 2 * 12] = 2;
    let mut bad_offset = vec![0xff; 9];
    bad_offset.push(0x02);
    vec![
        ("empty", vec![]),
        ("a variant alone", variant::VOTE.to_le_bytes().to_vec()),
        ("an unknown variant", u32::MAX.to_le_bytes().to_vec()),
        ("timestamp Option tag 2", vote_of(1, &[99], 2)),
        ("root Option tag 2", bad_root_tag),
        ("more slots declared than held", vote_of(2, &[99], 0)),
        ("u64::MAX slots declared", vote_of(u64::MAX, &[99], 0)),
        (
            "short_vec alias",
            compact(u64::MAX, &[0x81, 0x00], &[0x63, 0x01]),
        ),
        (
            "short_vec third byte continues",
            compact(u64::MAX, &[0x81, 0x80, 0x80], &[]),
        ),
        (
            "short_vec above u16::MAX",
            compact(u64::MAX, &[0xff, 0xff, 0x04], &[]),
        ),
        (
            "offset with a trailing zero",
            compact(u64::MAX, &[1], &[0xe3, 0x00, 0x01]),
        ),
        (
            "offset of eleven bytes",
            compact(u64::MAX, &[1], &[[0xff; 10].as_slice(), &[1, 1]].concat()),
        ),
        (
            "offset above u64::MAX",
            compact(u64::MAX, &[1], &[bad_offset, vec![1]].concat()),
        ),
        ("slot overflow", compact(u64::MAX - 1, &[1], &[0x02, 0x01])),
        (
            "slot overflow after u64::MAX",
            compact(u64::MAX, &[2], &[max_offset(), vec![1, 1, 1]].concat()),
        ),
        (
            "a lockout without its count",
            compact(u64::MAX, &[1], &[0x63]),
        ),
        (
            "over the instruction limit",
            vec![0; MAX_VOTE_INSTRUCTION_BYTES + 1],
        ),
    ]
}

/// Their valid neighbours, with the last voted slot.
fn accepted() -> Vec<(&'static str, Vec<u8>, u64)> {
    vec![
        ("one slot", vote_of(1, &[99], 0), 99),
        (
            "an offset of u64::MAX",
            compact(u64::MAX, &[1], &[max_offset(), vec![1]].concat()),
            u64::MAX,
        ),
        (
            "the last slot",
            compact(u64::MAX - 1, &[1], &[0x01, 0x01]),
            u64::MAX,
        ),
        ("a zero offset", compact(7, &[1], &[0x00, 0x01]), 7),
        (
            "a zero first slot",
            compact(u64::MAX, &[1], &[0x00, 0x00]),
            0,
        ),
    ]
}

#[test]
fn non_canonical_and_invalid_encodings_are_not_votes() {
    for (label, data) in refused() {
        assert_eq!(simple_vote_last_slot(&data), None, "{label}");
    }
    for (label, data, last) in accepted() {
        assert_eq!(simple_vote_last_slot(&data), Some(last), "{label}");
    }
}

/// `short_vec` lengths and LEB128 offsets decode only in their one minimal
/// form.
#[test]
fn varints_decode_only_in_their_minimal_form() {
    let short = |bytes: &[u8]| {
        let mut input = Input(bytes);
        input.short_u16().filter(|_| input.0.is_empty())
    };
    assert_eq!(short(&[0x00]), Some(0));
    assert_eq!(short(&[0x7f]), Some(127));
    assert_eq!(short(&[0x80, 0x01]), Some(128));
    assert_eq!(short(&[0xff, 0x7f]), Some(16_383));
    assert_eq!(short(&[0x80, 0x80, 0x01]), Some(16_384));
    assert_eq!(short(&[0xff, 0xff, 0x03]), Some(u16::MAX));
    for refused in [
        &[][..],
        &[0x80],
        &[0x80, 0x00],
        &[0x81, 0x80, 0x00],
        &[0xff, 0xff, 0x04],
        &[0x80, 0x80, 0x80, 0x01],
    ] {
        assert_eq!(short(refused), None, "{refused:?}");
    }

    let varint = |bytes: &[u8]| {
        let mut input = Input(bytes);
        input.varint_u64().filter(|_| input.0.is_empty())
    };
    assert_eq!(varint(&[0x00]), Some(0));
    assert_eq!(varint(&[0x7f]), Some(127));
    assert_eq!(varint(&[0x80, 0x01]), Some(128));
    assert_eq!(varint(&max_offset()), Some(u64::MAX));
    let mut over = vec![0xff; 9];
    over.push(0x02);
    for refused in [
        &[][..],
        &[0x80],
        &[0x80, 0x00],
        &[0xff, 0x00],
        &over,
        &[0xff; 10],
    ] {
        assert_eq!(varint(refused), None, "{refused:?}");
    }
}

/// Arbitrary bytes after each simple-vote variant never panic.
#[test]
fn arbitrary_bytes_never_panic() {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    };
    let variants = Kind::ALL.map(Kind::variant);
    for _ in 0..50_000 {
        let variant = variants[(next() % 8) as usize];
        let len = (next() % 160) as usize;
        let mut data = variant.to_le_bytes().to_vec();
        data.extend((0..len).map(|_| if next() % 3 == 0 { 0 } else { next() as u8 }));
        let _ = simple_vote_last_slot(&data);
    }
}
