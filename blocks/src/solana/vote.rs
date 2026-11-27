//! Conservative classification for the optional vote-only output table.
use super::proto::solana;

/// Solana Vote program ID (`Vote111111111111111111111111111111111111111`).
pub(super) const VOTE_PROGRAM_ID: [u8; 32] = [
    7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211, 189, 179, 216, 53, 94, 115, 209,
    16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0,
];

// The upstream packet limit is a conservative ceiling for one instruction.
// Larger future formats remain ordinary transactions until explicitly supported.
const MAX_VOTE_INSTRUCTION_BYTES: usize = 1232;

/// Only a legacy single-instruction transaction carrying a fully recognized vote
/// payload may omit its ordinary detail rows. The upstream transaction shape
/// checker alone also accepts administrative Vote-program calls, so the payload
/// must also decode, strictly and completely, as one of the eight simple votes
/// (`simple_vote_last_slot`).
pub(super) fn is_vote_transaction(tx: &solana::Transaction) -> bool {
    let Some(msg) = tx.message.as_ref() else {
        return false;
    };
    let Some(header) = msg.header.as_ref() else {
        return false;
    };
    let signatures = tx.signatures.len();
    if msg.versioned
        || !msg.address_table_lookups.is_empty()
        || !(1..=2).contains(&signatures)
        || tx.signatures.iter().any(|signature| signature.len() != 64)
        || header.num_required_signatures as usize != signatures
        || header.num_readonly_signed_accounts as usize >= signatures
        || signatures > msg.account_keys.len()
        || header.num_readonly_unsigned_accounts as usize > msg.account_keys.len() - signatures
        || msg.account_keys.iter().any(|key| key.len() != 32)
        || msg.recent_blockhash.len() != 32
    {
        return false;
    }
    let [instruction] = msg.instructions.as_slice() else {
        return false;
    };
    let Some(program_id) = msg.account_keys.get(instruction.program_id_index as usize) else {
        return false;
    };
    if program_id.as_ref() != VOTE_PROGRAM_ID
        || instruction.program_id_index == 0
        || instruction
            .accounts
            .iter()
            .any(|index| usize::from(*index) >= msg.account_keys.len())
    {
        return false;
    }
    simple_vote_last_slot(&instruction.data).is_some()
}

/// The `VoteInstruction` variants that `VoteInstruction::is_simple_vote`
/// accepts, by their bincode variant index: a `u32`, little-endian, at the
/// start of the instruction data (`solana-vote-interface` 7.1.0).
mod variant {
    pub(super) const VOTE: u32 = 2;
    pub(super) const VOTE_SWITCH: u32 = 6;
    pub(super) const UPDATE_VOTE_STATE: u32 = 8;
    pub(super) const UPDATE_VOTE_STATE_SWITCH: u32 = 9;
    pub(super) const COMPACT_UPDATE_VOTE_STATE: u32 = 12;
    pub(super) const COMPACT_UPDATE_VOTE_STATE_SWITCH: u32 = 13;
    pub(super) const TOWER_SYNC: u32 = 14;
    pub(super) const TOWER_SYNC_SWITCH: u32 = 15;
}

/// The last voted slot of `data` when it is exactly one simple vote with at
/// least one slot, and `None` for anything else: another variant, an empty
/// vote, a truncated or malformed payload, one with bytes after it, or one
/// over [`MAX_VOTE_INSTRUCTION_BYTES`].
///
/// This decodes exactly the bytes the Vote program's own encoding produces
/// (bincode 1.3 with fixed-width integers, as `solana-vote-interface` 7.1.0
/// serializes `VoteInstruction`), and no other encoding of the same values:
/// - integers are little-endian; a length (`Vec`, `VecDeque`) is a `u64`;
/// - an `Option` is a tag byte, 0 (absent) or 1 (then the value), never
///   another tag;
/// - a hash is 32 bytes;
/// - the compact variants (`CompactUpdateVoteState*` and `TowerSync*`) store
///   the root as a `u64` (`u64::MAX` for none), then a `short_vec` of
///   lockouts, each a LEB128 slot offset from the previous slot (from the root,
///   or from 0 without one) and a `u8` confirmation count. Both varints must be
///   minimal, as upstream's decoders require, and a slot must not overflow.
///
/// A filter, not a validator: anything uncertain keeps its ordinary rows.
fn simple_vote_last_slot(data: &[u8]) -> Option<u64> {
    if data.len() > MAX_VOTE_INSTRUCTION_BYTES {
        return None;
    }
    let mut input = Input(data);
    let (last_slot, switch) = match input.u32()? {
        variant::VOTE => (input.vote()?, false),
        variant::VOTE_SWITCH => (input.vote()?, true),
        variant::UPDATE_VOTE_STATE => (input.vote_state_update()?, false),
        variant::UPDATE_VOTE_STATE_SWITCH => (input.vote_state_update()?, true),
        variant::COMPACT_UPDATE_VOTE_STATE => (input.compact_lockouts(false)?, false),
        variant::COMPACT_UPDATE_VOTE_STATE_SWITCH => (input.compact_lockouts(false)?, true),
        variant::TOWER_SYNC => (input.compact_lockouts(true)?, false),
        variant::TOWER_SYNC_SWITCH => (input.compact_lockouts(true)?, true),
        _ => return None,
    };
    if switch {
        // The switching proof's hash.
        input.hash()?;
    }
    if !input.0.is_empty() {
        return None;
    }
    last_slot
}

/// The unread rest of an instruction's data. Every read fails, returning
/// `None`, when the data ends first.
struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    fn bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.0.split_at_checked(len)?;
        self.0 = tail;
        Some(head)
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.bytes(N)?.try_into().ok()
    }

    fn u8(&mut self) -> Option<u8> {
        self.array().map(|[byte]: [u8; 1]| byte)
    }

    fn u32(&mut self) -> Option<u32> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Option<u64> {
        self.array().map(u64::from_le_bytes)
    }

    fn hash(&mut self) -> Option<()> {
        self.bytes(32).map(drop)
    }

    /// An `Option` of a `len`-byte value.
    fn option(&mut self, len: usize) -> Option<()> {
        match self.u8()? {
            0 => Some(()),
            1 => self.bytes(len).map(drop),
            _ => None,
        }
    }

    /// A `u64` count of `size`-byte elements: the last element, if any.
    fn last_of_sequence(&mut self, size: usize) -> Option<Option<&'a [u8]>> {
        let count = usize::try_from(self.u64()?).ok()?;
        let elements = self.bytes(count.checked_mul(size)?)?;
        Some(elements.len().checked_sub(size).map(|at| &elements[at..]))
    }

    /// `Vote { slots: Vec<u64>, hash, timestamp: Option<i64> }`: its last slot.
    fn vote(&mut self) -> Option<Option<u64>> {
        let last = self.last_of_sequence(8)?;
        self.hash()?;
        self.option(8)?;
        Some(last.map(leading_u64))
    }

    /// `VoteStateUpdate { lockouts, root: Option<u64>, hash, timestamp:
    /// Option<i64> }`, each lockout a `u64` slot and a `u32` confirmation
    /// count: its last slot.
    fn vote_state_update(&mut self) -> Option<Option<u64>> {
        let last = self.last_of_sequence(12)?;
        self.option(8)?;
        self.hash()?;
        self.option(8)?;
        Some(last.map(leading_u64))
    }

    /// The compact `VoteStateUpdate` (`root`, lockout offsets, `hash`,
    /// `timestamp`), or with `block_id`, the compact `TowerSync`: its last
    /// slot.
    fn compact_lockouts(&mut self, block_id: bool) -> Option<Option<u64>> {
        let root = self.u64()?;
        let mut slot = if root == u64::MAX { 0 } else { root };
        let mut last = None;
        for _ in 0..self.short_u16()? {
            slot = slot.checked_add(self.varint_u64()?)?;
            self.u8()?; // confirmation_count
            last = Some(slot);
        }
        self.hash()?;
        self.option(8)?;
        if block_id {
            self.hash()?;
        }
        Some(last)
    }

    /// A `short_vec` length (`solana-short-vec`): one to three bytes of 7 bits,
    /// least significant first. A zero byte after the first, a third byte
    /// with its continuation bit or a value above `u16::MAX` is refused.
    fn short_u16(&mut self) -> Option<u16> {
        let mut value = 0u32;
        for nth in 0..3 {
            let byte = self.u8()?;
            if nth > 0 && byte == 0 {
                return None;
            }
            value |= u32::from(byte & 0x7f) << (7 * nth);
            if byte & 0x80 == 0 {
                return u16::try_from(value).ok();
            }
        }
        None
    }

    /// A LEB128 `u64` (`solana-serde-varint`): at most ten bytes, no bits
    /// beyond 64, and a zero last byte only when it is the only byte.
    fn varint_u64(&mut self) -> Option<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                let truncated = (value >> shift) as u8 != byte;
                let trailing_zero = byte == 0 && shift != 0;
                return (!truncated && !trailing_zero).then_some(value);
            }
        }
        None
    }
}

/// The `u64` at the start of a fixed-size element (a slot, or a lockout's).
fn leading_u64(element: &[u8]) -> u64 {
    u64::from_le_bytes(element[..8].try_into().expect("an element holds a u64"))
}

#[cfg(test)]
pub(crate) mod payloads;
#[cfg(test)]
mod tests;
