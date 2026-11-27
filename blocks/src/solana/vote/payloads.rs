//! Vote-program instruction data for tests: the bytes `solana-vote-interface`
//! 7.1.0's `VoteInstruction` had under bincode 1.3, which `vote/tests.rs` pins
//! to the output those crates recorded before #633 removed them. Every hash is
//! 32 zero bytes, as `Default::default()` gave the #501 fixtures.
use super::variant;

/// A simple-vote variant, the only kind the filter recognizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Vote,
    VoteSwitch,
    UpdateVoteState,
    UpdateVoteStateSwitch,
    CompactUpdateVoteState,
    CompactUpdateVoteStateSwitch,
    TowerSync,
    TowerSyncSwitch,
}

impl Kind {
    pub(crate) const ALL: [Kind; 8] = [
        Kind::Vote,
        Kind::VoteSwitch,
        Kind::UpdateVoteState,
        Kind::UpdateVoteStateSwitch,
        Kind::CompactUpdateVoteState,
        Kind::CompactUpdateVoteStateSwitch,
        Kind::TowerSync,
        Kind::TowerSyncSwitch,
    ];

    pub(crate) fn variant(self) -> u32 {
        match self {
            Kind::Vote => variant::VOTE,
            Kind::VoteSwitch => variant::VOTE_SWITCH,
            Kind::UpdateVoteState => variant::UPDATE_VOTE_STATE,
            Kind::UpdateVoteStateSwitch => variant::UPDATE_VOTE_STATE_SWITCH,
            Kind::CompactUpdateVoteState => variant::COMPACT_UPDATE_VOTE_STATE,
            Kind::CompactUpdateVoteStateSwitch => variant::COMPACT_UPDATE_VOTE_STATE_SWITCH,
            Kind::TowerSync => variant::TOWER_SYNC,
            Kind::TowerSyncSwitch => variant::TOWER_SYNC_SWITCH,
        }
    }

    fn is_switch(self) -> bool {
        matches!(
            self,
            Kind::VoteSwitch
                | Kind::UpdateVoteStateSwitch
                | Kind::CompactUpdateVoteStateSwitch
                | Kind::TowerSyncSwitch
        )
    }
}

/// What a simple vote votes for. A `Vote` keeps only the slots of `tower`.
#[derive(Debug, Clone, Default)]
pub(crate) struct Tower {
    /// `(slot, confirmation_count)`, oldest first.
    pub lockouts: Vec<(u64, u32)>,
    pub root: Option<u64>,
    pub timestamp: Option<i64>,
}

impl Tower {
    /// The #501 fixtures' tower: slots 98 and 99, no root, no timestamp.
    pub(crate) fn fixture() -> Self {
        Self {
            lockouts: vec![(98, 2), (99, 1)],
            ..Self::default()
        }
    }
}

/// The instruction data of `kind` voting for `tower`.
pub(crate) fn simple_vote(kind: Kind, tower: &Tower) -> Vec<u8> {
    let mut data = kind.variant().to_le_bytes().to_vec();
    match kind {
        Kind::Vote | Kind::VoteSwitch => {
            push_len(&mut data, tower.lockouts.len());
            for (slot, _) in &tower.lockouts {
                data.extend(slot.to_le_bytes());
            }
            push_hash(&mut data);
            push_option(&mut data, tower.timestamp.map(i64::to_le_bytes));
        }
        Kind::UpdateVoteState | Kind::UpdateVoteStateSwitch => {
            push_len(&mut data, tower.lockouts.len());
            for (slot, confirmation_count) in &tower.lockouts {
                data.extend(slot.to_le_bytes());
                data.extend(confirmation_count.to_le_bytes());
            }
            push_option(&mut data, tower.root.map(u64::to_le_bytes));
            push_hash(&mut data);
            push_option(&mut data, tower.timestamp.map(i64::to_le_bytes));
        }
        Kind::CompactUpdateVoteState
        | Kind::CompactUpdateVoteStateSwitch
        | Kind::TowerSync
        | Kind::TowerSyncSwitch => {
            data.extend(tower.root.unwrap_or(u64::MAX).to_le_bytes());
            let count = u16::try_from(tower.lockouts.len()).expect("a short_vec length");
            push_leb128(&mut data, count.into());
            let mut previous = tower.root.unwrap_or(0);
            for &(slot, confirmation_count) in &tower.lockouts {
                push_leb128(&mut data, slot - previous);
                data.push(u8::try_from(confirmation_count).expect("a compact count"));
                previous = slot;
            }
            push_hash(&mut data);
            push_option(&mut data, tower.timestamp.map(i64::to_le_bytes));
            if matches!(kind, Kind::TowerSync | Kind::TowerSyncSwitch) {
                push_hash(&mut data); // block_id
            }
        }
    }
    if kind.is_switch() {
        push_hash(&mut data);
    }
    data
}

/// The eight simple votes of the #501 fixtures, in [`Kind::ALL`] order.
pub(crate) fn fixture_votes() -> Vec<Vec<u8>> {
    Kind::ALL
        .into_iter()
        .map(|kind| simple_vote(kind, &Tower::fixture()))
        .collect()
}

/// The administrative instructions of the #501 fixtures, which must keep
/// their ordinary rows: a variant index, then the arguments.
pub(crate) fn administrative() -> Vec<(&'static str, Vec<u8>)> {
    let instruction = |index: u32, arguments: &[u8]| [&index.to_le_bytes(), arguments].concat();
    vec![
        ("Withdraw(42)", instruction(3, &42u64.to_le_bytes())),
        // A default Pubkey, then VoteAuthorize::Voter.
        ("Authorize", instruction(1, &[0; 36])),
        (
            "AuthorizeChecked(Withdrawer)",
            instruction(7, &1u32.to_le_bytes()),
        ),
        ("UpdateValidatorIdentity", instruction(4, &[])),
        ("UpdateCommission(5)", instruction(5, &[5])),
        // A default VoteInit: three Pubkeys and a commission.
        ("InitializeAccount", instruction(0, &[0; 97])),
    ]
}

/// A `Vote` for `slots`: the vote of the other test modules' fixtures.
pub(crate) fn vote(slots: &[u64]) -> Vec<u8> {
    let tower = Tower {
        lockouts: slots.iter().map(|&slot| (slot, 1)).collect(),
        ..Tower::default()
    };
    simple_vote(Kind::Vote, &tower)
}

fn push_len(data: &mut Vec<u8>, len: usize) {
    data.extend(u64::try_from(len).unwrap().to_le_bytes());
}

fn push_hash(data: &mut Vec<u8>) {
    data.extend([0; 32]);
}

fn push_option<const N: usize>(data: &mut Vec<u8>, value: Option<[u8; N]>) {
    match value {
        None => data.push(0),
        Some(bytes) => {
            data.push(1);
            data.extend(bytes);
        }
    }
}

/// LEB128, least significant 7 bits first: both a `short_vec` length and a
/// lockout offset.
fn push_leb128(data: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        data.push((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    data.push(value as u8);
}
