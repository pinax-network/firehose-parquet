//! Conservative classification for the optional vote-only output table.
use super::proto::solana;
use bincode::Options;
use solana_vote_interface::instruction::VoteInstruction;

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
/// checker alone also accepts administrative Vote-program calls, so we require
/// VoteInstruction::is_simple_vote after strict, bounded deserialization.
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
    if program_id.as_slice() != VOTE_PROGRAM_ID
        || instruction.program_id_index == 0
        || instruction
            .accounts
            .iter()
            .any(|index| usize::from(*index) >= msg.account_keys.len())
        || instruction.data.len() > MAX_VOTE_INSTRUCTION_BYTES
    {
        return false;
    }
    let Ok(decoded) = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_VOTE_INSTRUCTION_BYTES as u64)
        .reject_trailing_bytes()
        .deserialize::<VoteInstruction>(&instruction.data)
    else {
        return false;
    };

    decoded.is_simple_vote()
        && decoded.last_voted_slot().is_some()
        // Keep noncanonical encodings as ordinary data too. This is a filter,
        // not a validator: uncertainty should retain rows, never remove them.
        && bincode::serialize(&decoded).is_ok_and(|encoded| encoded == instruction.data)
}
