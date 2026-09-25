use arrow::datatypes::{DataType, Field, Schema};
use firehose_parquet::encode::{bytes_data_type, EncodeBytes};
use firehose_parquet::traits::{canonical_fields_with_encoding, fork_step_field};
use std::sync::Arc;

fn maybe_fork_step(fields: &mut Vec<Field>, include: bool) {
    if include {
        fields.push(fork_step_field());
    }
}

/// `List<UInt64>`, the type of a `ListBuilder<UInt64Builder>` column.
fn u64_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::UInt64, true)))
}

pub fn blocks_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("parent_slot", DataType::UInt64, false),
        Field::new("proposer_index", DataType::UInt64, false),
        Field::new("root", bd.clone(), false),
        Field::new("parent_root", bd.clone(), false),
        Field::new("state_root", bd.clone(), false),
        Field::new("body_root", bd.clone(), false),
        Field::new("signature", bd.clone(), false),
        Field::new("spec", DataType::Utf8, false),
        // Null only when the block has no body.
        Field::new("graffiti", bd, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn attestations_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("attestation_index", DataType::UInt32, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("committee_index", DataType::UInt64, false),
        Field::new("aggregation_bits", bd.clone(), false),
        Field::new("beacon_block_root", bd.clone(), false),
        Field::new("source_epoch", DataType::UInt64, false),
        Field::new("source_root", bd.clone(), false),
        Field::new("target_epoch", DataType::UInt64, false),
        Field::new("target_root", bd.clone(), false),
        Field::new("signature", bd.clone(), false),
        // EIP-7549 (Electra+): the committees the attestation aggregates.
        // Null before Electra, where `committee_index` identifies the committee.
        Field::new("committee_bits", bd, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn deposits_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("deposit_index", DataType::UInt32, false),
        Field::new("pubkey", bd.clone(), false),
        Field::new("withdrawal_credentials", bd.clone(), false),
        Field::new("amount", DataType::UInt64, false),
        Field::new("signature", bd, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn proposer_slashings_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("slashing_index", DataType::UInt32, false),
        Field::new("header_1_slot", DataType::UInt64, false),
        Field::new("header_1_proposer_index", DataType::UInt64, false),
        Field::new("header_1_parent_root", bd.clone(), false),
        Field::new("header_1_state_root", bd.clone(), false),
        Field::new("header_1_body_root", bd.clone(), false),
        Field::new("header_2_slot", DataType::UInt64, false),
        Field::new("header_2_proposer_index", DataType::UInt64, false),
        Field::new("header_2_parent_root", bd.clone(), false),
        Field::new("header_2_state_root", bd.clone(), false),
        Field::new("header_2_body_root", bd, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn attester_slashings_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("slashing_index", DataType::UInt32, false),
        Field::new("attestation_1_slot", DataType::UInt64, false),
        Field::new("attestation_1_committee_index", DataType::UInt64, false),
        Field::new("attestation_1_beacon_block_root", bd.clone(), false),
        Field::new("attestation_1_source_epoch", DataType::UInt64, false),
        Field::new("attestation_1_source_root", bd.clone(), false),
        Field::new("attestation_1_target_epoch", DataType::UInt64, false),
        Field::new("attestation_1_target_root", bd.clone(), false),
        Field::new("attestation_2_slot", DataType::UInt64, false),
        Field::new("attestation_2_committee_index", DataType::UInt64, false),
        Field::new("attestation_2_beacon_block_root", bd.clone(), false),
        Field::new("attestation_2_source_epoch", DataType::UInt64, false),
        Field::new("attestation_2_source_root", bd.clone(), false),
        Field::new("attestation_2_target_epoch", DataType::UInt64, false),
        Field::new("attestation_2_target_root", bd, false),
        Field::new("attestation_1_attesting_indices", u64_list_type(), false),
        Field::new("attestation_2_attesting_indices", u64_list_type(), false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn voluntary_exits_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("exit_index", DataType::UInt32, false),
        Field::new("epoch", DataType::UInt64, false),
        Field::new("validator_index", DataType::UInt64, false),
        Field::new("signature", bd, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn execution_payload_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("parent_hash", bd.clone(), false),
        Field::new("fee_recipient", bd.clone(), false),
        Field::new("state_root", bd.clone(), false),
        Field::new("receipts_root", bd.clone(), false),
        Field::new("prev_randao", bd.clone(), false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("gas_limit", DataType::UInt64, false),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("payload_timestamp", DataType::Int64, true),
        Field::new("block_hash", bd.clone(), false),
        Field::new("base_fee_per_gas", bd, false),
        Field::new("blob_gas_used", DataType::UInt64, true),
        Field::new("excess_blob_gas", DataType::UInt64, true),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub fn blob_sidecars_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("blob_index", DataType::UInt64, false),
        Field::new("blob", bd.clone(), false),
        Field::new("kzg_commitment", bd.clone(), false),
        Field::new("kzg_proof", bd, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// Capella+: `execution_payload.withdrawals`, one row per withdrawal.
pub fn withdrawals_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("withdrawal_index", DataType::UInt64, false),
        Field::new("validator_index", DataType::UInt64, false),
        Field::new("address", bd, false),
        Field::new("amount", DataType::UInt64, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// Deneb+ (the Firehose Capella body does not carry them): signed
/// BLS-to-execution withdrawal credential changes.
pub fn bls_to_execution_changes_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("change_index", DataType::UInt32, false),
        Field::new("validator_index", DataType::UInt64, false),
        Field::new("from_bls_pubkey", bd.clone(), false),
        Field::new("to_execution_address", bd.clone(), false),
        Field::new("signature", bd, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// Electra+: EIP-6110 deposit requests from `execution_requests.deposits`.
pub fn deposit_requests_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("request_index", DataType::UInt32, false),
        Field::new("deposit_index", DataType::UInt64, false),
        Field::new("pubkey", bd.clone(), false),
        Field::new("withdrawal_credentials", bd.clone(), false),
        Field::new("amount", DataType::UInt64, false),
        Field::new("signature", bd, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// Electra+: EIP-7002 withdrawal requests from `execution_requests.withdrawals`.
pub fn withdrawal_requests_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("request_index", DataType::UInt32, false),
        Field::new("source_address", bd.clone(), false),
        Field::new("validator_pubkey", bd, false),
        Field::new("amount", DataType::UInt64, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// Electra+: EIP-7251 consolidation requests from
/// `execution_requests.consolidations`.
pub fn consolidation_requests_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(vec![
        Field::new("block_slot", DataType::UInt64, false),
        Field::new("request_index", DataType::UInt32, false),
        Field::new("source_address", bd.clone(), false),
        Field::new("source_pubkey", bd.clone(), false),
        Field::new("target_pubkey", bd, false),
    ]);
    maybe_fork_step(&mut fields, include_fork_step);
    Schema::new(fields)
}

pub const TABLE_NAMES: [&str; 13] = [
    "blocks",
    "attestations",
    "deposits",
    "proposer_slashings",
    "attester_slashings",
    "voluntary_exits",
    "execution_payload",
    "blob_sidecars",
    "withdrawals",
    "bls_to_execution_changes",
    "deposit_requests",
    "withdrawal_requests",
    "consolidation_requests",
];
