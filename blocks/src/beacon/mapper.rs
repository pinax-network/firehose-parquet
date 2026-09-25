use super::proto::beacon;
use super::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, EncodeBytes};
use firehose_parquet::traits::{
    est_i64, est_list_u64, est_opt_str, est_str, est_u32, est_u64, BlockIdentity, BlockMapper,
    CanonicalBuilder, PreparedIdentity,
};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn spec_name(spec: i32) -> &'static str {
    match spec {
        1 => "PHASE0",
        2 => "ALTAIR",
        3 => "BELLATRIX",
        4 => "CAPELLA",
        5 => "DENEB",
        6 => "ELECTRA",
        7 => "FUSAKA",
        _ => "UNSPECIFIED",
    }
}

fn append_fork_step(builder: &mut Option<StringBuilder>, fork_step: Option<&str>) {
    if let Some(ref mut b) = builder {
        b.append_value(fork_step.unwrap_or("UNKNOWN"));
    }
}

fn finish_fork_step(builder: &mut Option<StringBuilder>, columns: &mut Vec<Arc<dyn Array>>) {
    if let Some(ref mut b) = builder {
        columns.push(Arc::new(b.finish()) as Arc<dyn Array>);
    }
}

fn mk_fork_step(include: bool) -> Option<StringBuilder> {
    if include {
        Some(StringBuilder::new())
    } else {
        None
    }
}

/// Append one list entry holding `values`.
fn append_u64_list(builder: &mut ListBuilder<UInt64Builder>, values: &[u64]) {
    builder.values().append_slice(values);
    builder.append(true);
}

// ---------------------------------------------------------------------------
// Common body fields extracted across all fork variants
// ---------------------------------------------------------------------------

struct BodyFields<'a> {
    graffiti: Option<&'a [u8]>,
    attestations: Vec<AttestationFields<'a>>,
    deposits: &'a [beacon::Deposit],
    proposer_slashings: &'a [beacon::ProposerSlashing],
    attester_slashings: &'a [beacon::AttesterSlashing],
    voluntary_exits: &'a [beacon::SignedVoluntaryExit],
    execution_payload: Option<ExecutionPayloadFields<'a>>,
    /// Capella+: `execution_payload.withdrawals`.
    withdrawals: &'a [beacon::Withdrawal],
    /// Deneb+: the Firehose Capella body has no such field.
    bls_to_execution_changes: &'a [beacon::SignedBlsToExecutionChange],
    /// Electra+.
    execution_requests: Option<&'a beacon::ExecutionRequest>,
    blobs: &'a [beacon::Blob],
}

enum AttestationFields<'a> {
    Standard(&'a beacon::Attestation),
    Electra(&'a beacon::ElectraAttestation),
}

struct ExecutionPayloadFields<'a> {
    parent_hash: &'a [u8],
    fee_recipient: &'a [u8],
    state_root: &'a [u8],
    receipts_root: &'a [u8],
    prev_randao: &'a [u8],
    block_number: u64,
    gas_limit: u64,
    gas_used: u64,
    timestamp: Option<&'a prost_types::Timestamp>,
    block_hash: &'a [u8],
    base_fee_per_gas: &'a [u8],
    blob_gas_used: Option<u64>,
    excess_blob_gas: Option<u64>,
}

fn bellatrix_payload(ep: &beacon::BellatrixExecutionPayload) -> ExecutionPayloadFields<'_> {
    ExecutionPayloadFields {
        parent_hash: &ep.parent_hash,
        fee_recipient: &ep.fee_recipient,
        state_root: &ep.state_root,
        receipts_root: &ep.receipts_root,
        prev_randao: &ep.prev_randao,
        block_number: ep.block_number,
        gas_limit: ep.gas_limit,
        gas_used: ep.gas_used,
        timestamp: ep.timestamp.as_ref(),
        block_hash: &ep.block_hash,
        base_fee_per_gas: &ep.base_fee_per_gas,
        blob_gas_used: None,
        excess_blob_gas: None,
    }
}

fn capella_payload(ep: &beacon::CapellaExecutionPayload) -> ExecutionPayloadFields<'_> {
    ExecutionPayloadFields {
        parent_hash: &ep.parent_hash,
        fee_recipient: &ep.fee_recipient,
        state_root: &ep.state_root,
        receipts_root: &ep.receipts_root,
        prev_randao: &ep.prev_randao,
        block_number: ep.block_number,
        gas_limit: ep.gas_limit,
        gas_used: ep.gas_used,
        timestamp: ep.timestamp.as_ref(),
        block_hash: &ep.block_hash,
        base_fee_per_gas: &ep.base_fee_per_gas,
        blob_gas_used: None,
        excess_blob_gas: None,
    }
}

fn deneb_payload(ep: &beacon::DenebExecutionPayload) -> ExecutionPayloadFields<'_> {
    ExecutionPayloadFields {
        parent_hash: &ep.parent_hash,
        fee_recipient: &ep.fee_recipient,
        state_root: &ep.state_root,
        receipts_root: &ep.receipts_root,
        prev_randao: &ep.prev_randao,
        block_number: ep.block_number,
        gas_limit: ep.gas_limit,
        gas_used: ep.gas_used,
        timestamp: ep.timestamp.as_ref(),
        block_hash: &ep.block_hash,
        base_fee_per_gas: &ep.base_fee_per_gas,
        blob_gas_used: Some(ep.blob_gas_used),
        excess_blob_gas: Some(ep.excess_blob_gas),
    }
}

fn standard_attestations(atts: &[beacon::Attestation]) -> Vec<AttestationFields<'_>> {
    atts.iter().map(AttestationFields::Standard).collect()
}

fn extract_body_fields(block: &beacon::Block) -> BodyFields<'_> {
    // Fields that a fork's body does not have stay empty.
    let base = BodyFields {
        graffiti: None,
        attestations: vec![],
        deposits: &[],
        proposer_slashings: &[],
        attester_slashings: &[],
        voluntary_exits: &[],
        execution_payload: None,
        withdrawals: &[],
        bls_to_execution_changes: &[],
        execution_requests: None,
        blobs: &[],
    };

    match &block.body {
        Some(beacon::block::Body::Phase0(b)) => BodyFields {
            graffiti: Some(&b.graffiti),
            attestations: standard_attestations(&b.attestations),
            deposits: &b.deposits,
            proposer_slashings: &b.proposer_slashings,
            attester_slashings: &b.attester_slashings,
            voluntary_exits: &b.voluntary_exits,
            ..base
        },
        Some(beacon::block::Body::Altair(b)) => BodyFields {
            graffiti: Some(&b.graffiti),
            attestations: standard_attestations(&b.attestations),
            deposits: &b.deposits,
            proposer_slashings: &b.proposer_slashings,
            attester_slashings: &b.attester_slashings,
            voluntary_exits: &b.voluntary_exits,
            ..base
        },
        Some(beacon::block::Body::Bellatrix(b)) => BodyFields {
            graffiti: Some(&b.graffiti),
            attestations: standard_attestations(&b.attestations),
            deposits: &b.deposits,
            proposer_slashings: &b.proposer_slashings,
            attester_slashings: &b.attester_slashings,
            voluntary_exits: &b.voluntary_exits,
            execution_payload: b.execution_payload.as_ref().map(bellatrix_payload),
            ..base
        },
        Some(beacon::block::Body::Capella(b)) => BodyFields {
            graffiti: Some(&b.graffiti),
            attestations: standard_attestations(&b.attestations),
            deposits: &b.deposits,
            proposer_slashings: &b.proposer_slashings,
            attester_slashings: &b.attester_slashings,
            voluntary_exits: &b.voluntary_exits,
            execution_payload: b.execution_payload.as_ref().map(capella_payload),
            withdrawals: b
                .execution_payload
                .as_ref()
                .map_or(&[], |ep| &ep.withdrawals),
            ..base
        },
        Some(beacon::block::Body::Deneb(b)) => BodyFields {
            graffiti: Some(&b.graffiti),
            attestations: standard_attestations(&b.attestations),
            deposits: &b.deposits,
            proposer_slashings: &b.proposer_slashings,
            attester_slashings: &b.attester_slashings,
            voluntary_exits: &b.voluntary_exits,
            execution_payload: b.execution_payload.as_ref().map(deneb_payload),
            withdrawals: b
                .execution_payload
                .as_ref()
                .map_or(&[], |ep| &ep.withdrawals),
            bls_to_execution_changes: &b.bls_to_execution_changes,
            blobs: &b.embedded_blobs,
            ..base
        },
        Some(beacon::block::Body::Electra(b)) | Some(beacon::block::Body::Fusaka(b)) => {
            BodyFields {
                graffiti: Some(&b.graffiti),
                attestations: b
                    .attestations
                    .iter()
                    .map(AttestationFields::Electra)
                    .collect(),
                deposits: &b.deposits,
                proposer_slashings: &b.proposer_slashings,
                attester_slashings: &b.attester_slashings,
                voluntary_exits: &b.voluntary_exits,
                execution_payload: b.execution_payload.as_ref().map(deneb_payload),
                withdrawals: b
                    .execution_payload
                    .as_ref()
                    .map_or(&[], |ep| &ep.withdrawals),
                bls_to_execution_changes: &b.bls_to_execution_changes,
                execution_requests: b.execution_requests.as_ref(),
                blobs: &b.embedded_blobs,
            }
        }
        None => base,
    }
}

// ---------------------------------------------------------------------------
// Beacon BlockMapper
// ---------------------------------------------------------------------------

pub struct BeaconBlockMapper {
    blocks: BlocksBuilder,
    attestations: AttestationsBuilder,
    deposits: DepositsBuilder,
    proposer_slashings: ProposerSlashingsBuilder,
    attester_slashings: AttesterSlashingsBuilder,
    voluntary_exits: VoluntaryExitsBuilder,
    execution_payload: ExecutionPayloadBuilder,
    blob_sidecars: BlobSidecarsBuilder,
    withdrawals: WithdrawalsBuilder,
    bls_to_execution_changes: BlsToExecutionChangesBuilder,
    deposit_requests: DepositRequestsBuilder,
    withdrawal_requests: WithdrawalRequestsBuilder,
    consolidation_requests: ConsolidationRequestsBuilder,
    blocks_schema: Schema,
    attestations_schema: Schema,
    deposits_schema: Schema,
    proposer_slashings_schema: Schema,
    attester_slashings_schema: Schema,
    voluntary_exits_schema: Schema,
    execution_payload_schema: Schema,
    blob_sidecars_schema: Schema,
    withdrawals_schema: Schema,
    bls_to_execution_changes_schema: Schema,
    deposit_requests_schema: Schema,
    withdrawal_requests_schema: Schema,
    consolidation_requests_schema: Schema,
}

impl BeaconBlockMapper {
    pub fn new(include_fork_step: bool, encoding: EncodeBytes) -> Self {
        let enc = &encoding;
        Self {
            blocks: BlocksBuilder::new(include_fork_step, enc),
            attestations: AttestationsBuilder::new(include_fork_step, enc),
            deposits: DepositsBuilder::new(include_fork_step, enc),
            proposer_slashings: ProposerSlashingsBuilder::new(include_fork_step, enc),
            attester_slashings: AttesterSlashingsBuilder::new(include_fork_step, enc),
            voluntary_exits: VoluntaryExitsBuilder::new(include_fork_step, enc),
            execution_payload: ExecutionPayloadBuilder::new(include_fork_step, enc),
            blob_sidecars: BlobSidecarsBuilder::new(include_fork_step, enc),
            withdrawals: WithdrawalsBuilder::new(include_fork_step, enc),
            bls_to_execution_changes: BlsToExecutionChangesBuilder::new(include_fork_step, enc),
            deposit_requests: DepositRequestsBuilder::new(include_fork_step, enc),
            withdrawal_requests: WithdrawalRequestsBuilder::new(include_fork_step, enc),
            consolidation_requests: ConsolidationRequestsBuilder::new(include_fork_step, enc),
            blocks_schema: schema::blocks_schema(include_fork_step, enc),
            attestations_schema: schema::attestations_schema(include_fork_step, enc),
            deposits_schema: schema::deposits_schema(include_fork_step, enc),
            proposer_slashings_schema: schema::proposer_slashings_schema(include_fork_step, enc),
            attester_slashings_schema: schema::attester_slashings_schema(include_fork_step, enc),
            voluntary_exits_schema: schema::voluntary_exits_schema(include_fork_step, enc),
            execution_payload_schema: schema::execution_payload_schema(include_fork_step, enc),
            blob_sidecars_schema: schema::blob_sidecars_schema(include_fork_step, enc),
            withdrawals_schema: schema::withdrawals_schema(include_fork_step, enc),
            bls_to_execution_changes_schema: schema::bls_to_execution_changes_schema(
                include_fork_step,
                enc,
            ),
            deposit_requests_schema: schema::deposit_requests_schema(include_fork_step, enc),
            withdrawal_requests_schema: schema::withdrawal_requests_schema(include_fork_step, enc),
            consolidation_requests_schema: schema::consolidation_requests_schema(
                include_fork_step,
                enc,
            ),
        }
    }

    fn map_beacon_block(
        &mut self,
        block: &beacon::Block,
        identity: &PreparedIdentity,
        fork_step: Option<&str>,
    ) {
        let slot = block.slot;

        // blocks table
        self.blocks.canonical.append(identity);
        self.blocks.slot.append_value(slot);
        self.blocks.parent_slot.append_value(block.parent_slot);
        self.blocks
            .proposer_index
            .append_value(block.proposer_index);
        self.blocks.root.append_value(&block.root);
        self.blocks.parent_root.append_value(&block.parent_root);
        self.blocks.state_root.append_value(&block.state_root);
        self.blocks.body_root.append_value(&block.body_root);
        self.blocks.signature.append_value(&block.signature);
        self.blocks.spec.append_value(spec_name(block.spec));

        let body = extract_body_fields(block);

        match body.graffiti {
            Some(graffiti) => self.blocks.graffiti.append_value(graffiti),
            None => self.blocks.graffiti.append_null(),
        }
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // attestations
        for (i, att) in body.attestations.iter().enumerate() {
            self.attestations.canonical.append(identity);
            self.attestations.block_slot.append_value(slot);
            self.attestations.attestation_index.append_value(i as u32);
            match att {
                AttestationFields::Standard(a) => {
                    self.attestations
                        .aggregation_bits
                        .append_value(&a.aggregation_bits);
                    self.attestations.signature.append_value(&a.signature);
                    if let Some(data) = &a.data {
                        self.append_attestation_data(data);
                    } else {
                        self.append_empty_attestation_data();
                    }
                    self.attestations.committee_bits.append_null();
                }
                AttestationFields::Electra(a) => {
                    self.attestations
                        .aggregation_bits
                        .append_value(&a.aggregation_bits);
                    self.attestations.signature.append_value(&a.signature);
                    if let Some(data) = &a.data {
                        self.append_attestation_data(data);
                    } else {
                        self.append_empty_attestation_data();
                    }
                    self.attestations
                        .committee_bits
                        .append_value(&a.committee_bits);
                }
            }
            append_fork_step(&mut self.attestations.fork_step, fork_step);
        }

        // deposits
        for (i, deposit) in body.deposits.iter().enumerate() {
            self.deposits.canonical.append(identity);
            self.deposits.block_slot.append_value(slot);
            self.deposits.deposit_index.append_value(i as u32);
            if let Some(data) = &deposit.data {
                self.deposits.pubkey.append_value(&data.public_key);
                self.deposits
                    .withdrawal_credentials
                    .append_value(&data.withdrawal_credentials);
                self.deposits.amount.append_value(data.gwei);
                self.deposits.signature.append_value(&data.signature);
            } else {
                self.deposits.pubkey.append_value(&[]);
                self.deposits.withdrawal_credentials.append_value(&[]);
                self.deposits.amount.append_value(0);
                self.deposits.signature.append_value(&[]);
            }
            append_fork_step(&mut self.deposits.fork_step, fork_step);
        }

        // proposer_slashings
        for (i, slashing) in body.proposer_slashings.iter().enumerate() {
            self.proposer_slashings.canonical.append(identity);
            self.proposer_slashings.block_slot.append_value(slot);
            self.proposer_slashings
                .slashing_index
                .append_value(i as u32);
            self.append_proposer_slashing_header(slashing.signed_header_1.as_ref(), true);
            self.append_proposer_slashing_header(slashing.signed_header_2.as_ref(), false);
            append_fork_step(&mut self.proposer_slashings.fork_step, fork_step);
        }

        // attester_slashings
        for (i, slashing) in body.attester_slashings.iter().enumerate() {
            self.attester_slashings.canonical.append(identity);
            self.attester_slashings.block_slot.append_value(slot);
            self.attester_slashings
                .slashing_index
                .append_value(i as u32);
            self.append_indexed_attestation(slashing.attestation_1.as_ref(), true);
            self.append_indexed_attestation(slashing.attestation_2.as_ref(), false);
            append_u64_list(
                &mut self.attester_slashings.attestation_1_attesting_indices,
                slashing
                    .attestation_1
                    .as_ref()
                    .map_or(&[], |a| &a.attesting_indices),
            );
            append_u64_list(
                &mut self.attester_slashings.attestation_2_attesting_indices,
                slashing
                    .attestation_2
                    .as_ref()
                    .map_or(&[], |a| &a.attesting_indices),
            );
            append_fork_step(&mut self.attester_slashings.fork_step, fork_step);
        }

        // voluntary_exits
        for (i, exit) in body.voluntary_exits.iter().enumerate() {
            self.voluntary_exits.canonical.append(identity);
            self.voluntary_exits.block_slot.append_value(slot);
            self.voluntary_exits.exit_index.append_value(i as u32);
            self.voluntary_exits.signature.append_value(&exit.signature);
            if let Some(msg) = &exit.message {
                self.voluntary_exits.epoch.append_value(msg.epoch);
                self.voluntary_exits
                    .validator_index
                    .append_value(msg.validator_index);
            } else {
                self.voluntary_exits.epoch.append_value(0);
                self.voluntary_exits.validator_index.append_value(0);
            }
            append_fork_step(&mut self.voluntary_exits.fork_step, fork_step);
        }

        // execution_payload
        if let Some(ep) = body.execution_payload {
            self.execution_payload.canonical.append(identity);
            self.execution_payload.block_slot.append_value(slot);
            self.execution_payload
                .parent_hash
                .append_value(ep.parent_hash);
            self.execution_payload
                .fee_recipient
                .append_value(ep.fee_recipient);
            self.execution_payload
                .state_root
                .append_value(ep.state_root);
            self.execution_payload
                .receipts_root
                .append_value(ep.receipts_root);
            self.execution_payload
                .prev_randao
                .append_value(ep.prev_randao);
            self.execution_payload
                .block_number
                .append_value(ep.block_number);
            self.execution_payload.gas_limit.append_value(ep.gas_limit);
            self.execution_payload.gas_used.append_value(ep.gas_used);
            match ep.timestamp {
                Some(ts) => self
                    .execution_payload
                    .payload_timestamp
                    .append_value(ts.seconds),
                None => self.execution_payload.payload_timestamp.append_null(),
            }
            self.execution_payload
                .block_hash
                .append_value(ep.block_hash);
            self.execution_payload
                .base_fee_per_gas
                .append_value(ep.base_fee_per_gas);
            match ep.blob_gas_used {
                Some(v) => self.execution_payload.blob_gas_used.append_value(v),
                None => self.execution_payload.blob_gas_used.append_null(),
            }
            match ep.excess_blob_gas {
                Some(v) => self.execution_payload.excess_blob_gas.append_value(v),
                None => self.execution_payload.excess_blob_gas.append_null(),
            }
            append_fork_step(&mut self.execution_payload.fork_step, fork_step);
        }

        // blob_sidecars
        for blob in body.blobs {
            self.blob_sidecars.canonical.append(identity);
            self.blob_sidecars.block_slot.append_value(slot);
            self.blob_sidecars.blob_index.append_value(blob.index);
            self.blob_sidecars.blob.append_value(&blob.blob);
            self.blob_sidecars
                .kzg_commitment
                .append_value(&blob.kzg_commitment);
            self.blob_sidecars.kzg_proof.append_value(&blob.kzg_proof);
            append_fork_step(&mut self.blob_sidecars.fork_step, fork_step);
        }

        // withdrawals (Capella+)
        for withdrawal in body.withdrawals {
            let b = &mut self.withdrawals;
            b.canonical.append(identity);
            b.block_slot.append_value(slot);
            b.withdrawal_index.append_value(withdrawal.withdrawal_index);
            b.validator_index.append_value(withdrawal.validator_index);
            b.address.append_value(&withdrawal.address);
            b.amount.append_value(withdrawal.gwei);
            append_fork_step(&mut b.fork_step, fork_step);
        }

        // bls_to_execution_changes (Deneb+)
        for (i, change) in body.bls_to_execution_changes.iter().enumerate() {
            let b = &mut self.bls_to_execution_changes;
            b.canonical.append(identity);
            b.block_slot.append_value(slot);
            b.change_index.append_value(i as u32);
            match &change.message {
                Some(msg) => {
                    b.validator_index.append_value(msg.validator_index);
                    b.from_bls_pubkey.append_value(&msg.from_bls_pub_key);
                    b.to_execution_address
                        .append_value(&msg.to_execution_address);
                }
                None => {
                    b.validator_index.append_value(0);
                    b.from_bls_pubkey.append_value(&[]);
                    b.to_execution_address.append_value(&[]);
                }
            }
            b.signature.append_value(&change.signature);
            append_fork_step(&mut b.fork_step, fork_step);
        }

        // execution requests (Electra+)
        if let Some(requests) = body.execution_requests {
            for (i, request) in requests.deposits.iter().enumerate() {
                let b = &mut self.deposit_requests;
                b.canonical.append(identity);
                b.block_slot.append_value(slot);
                b.request_index.append_value(i as u32);
                b.deposit_index.append_value(request.index);
                b.pubkey.append_value(&request.pub_key);
                b.withdrawal_credentials
                    .append_value(&request.withdrawal_credentials);
                b.amount.append_value(request.amount);
                b.signature.append_value(&request.signature);
                append_fork_step(&mut b.fork_step, fork_step);
            }
            for (i, request) in requests.withdrawals.iter().enumerate() {
                let b = &mut self.withdrawal_requests;
                b.canonical.append(identity);
                b.block_slot.append_value(slot);
                b.request_index.append_value(i as u32);
                b.source_address.append_value(&request.source_address);
                b.validator_pubkey.append_value(&request.validator_pub_key);
                b.amount.append_value(request.amount);
                append_fork_step(&mut b.fork_step, fork_step);
            }
            for (i, request) in requests.consolidations.iter().enumerate() {
                let b = &mut self.consolidation_requests;
                b.canonical.append(identity);
                b.block_slot.append_value(slot);
                b.request_index.append_value(i as u32);
                b.source_address.append_value(&request.source_address);
                b.source_pubkey.append_value(&request.source_pub_key);
                b.target_pubkey.append_value(&request.target_pub_key);
                append_fork_step(&mut b.fork_step, fork_step);
            }
        }
    }

    fn append_attestation_data(&mut self, data: &beacon::AttestationData) {
        self.attestations.slot.append_value(data.slot);
        self.attestations
            .committee_index
            .append_value(data.committee_index);
        self.attestations
            .beacon_block_root
            .append_value(&data.beacon_block_root);
        if let Some(src) = &data.source {
            self.attestations.source_epoch.append_value(src.epoch);
            self.attestations.source_root.append_value(&src.root);
        } else {
            self.attestations.source_epoch.append_value(0);
            self.attestations.source_root.append_value(&[]);
        }
        if let Some(tgt) = &data.target {
            self.attestations.target_epoch.append_value(tgt.epoch);
            self.attestations.target_root.append_value(&tgt.root);
        } else {
            self.attestations.target_epoch.append_value(0);
            self.attestations.target_root.append_value(&[]);
        }
    }

    fn append_empty_attestation_data(&mut self) {
        self.attestations.slot.append_value(0);
        self.attestations.committee_index.append_value(0);
        self.attestations.beacon_block_root.append_value(&[]);
        self.attestations.source_epoch.append_value(0);
        self.attestations.source_root.append_value(&[]);
        self.attestations.target_epoch.append_value(0);
        self.attestations.target_root.append_value(&[]);
    }

    fn append_proposer_slashing_header(
        &mut self,
        signed: Option<&beacon::SignedBeaconBlockHeader>,
        is_first: bool,
    ) {
        let (slot_b, pi_b, pr_b, sr_b, br_b) = if is_first {
            (
                &mut self.proposer_slashings.header_1_slot,
                &mut self.proposer_slashings.header_1_proposer_index,
                &mut self.proposer_slashings.header_1_parent_root,
                &mut self.proposer_slashings.header_1_state_root,
                &mut self.proposer_slashings.header_1_body_root,
            )
        } else {
            (
                &mut self.proposer_slashings.header_2_slot,
                &mut self.proposer_slashings.header_2_proposer_index,
                &mut self.proposer_slashings.header_2_parent_root,
                &mut self.proposer_slashings.header_2_state_root,
                &mut self.proposer_slashings.header_2_body_root,
            )
        };

        if let Some(hdr) = signed.and_then(|s| s.message.as_ref()) {
            slot_b.append_value(hdr.slot);
            pi_b.append_value(hdr.proposer_index);
            pr_b.append_value(&hdr.parent_root);
            sr_b.append_value(&hdr.state_root);
            br_b.append_value(&hdr.body_root);
        } else {
            slot_b.append_value(0);
            pi_b.append_value(0);
            pr_b.append_value(&[]);
            sr_b.append_value(&[]);
            br_b.append_value(&[]);
        }
    }

    fn append_indexed_attestation(
        &mut self,
        att: Option<&beacon::IndexedAttestation>,
        is_first: bool,
    ) {
        let (slot_b, ci_b, bbr_b, se_b, sr_b, te_b, tr_b) = if is_first {
            (
                &mut self.attester_slashings.attestation_1_slot,
                &mut self.attester_slashings.attestation_1_committee_index,
                &mut self.attester_slashings.attestation_1_beacon_block_root,
                &mut self.attester_slashings.attestation_1_source_epoch,
                &mut self.attester_slashings.attestation_1_source_root,
                &mut self.attester_slashings.attestation_1_target_epoch,
                &mut self.attester_slashings.attestation_1_target_root,
            )
        } else {
            (
                &mut self.attester_slashings.attestation_2_slot,
                &mut self.attester_slashings.attestation_2_committee_index,
                &mut self.attester_slashings.attestation_2_beacon_block_root,
                &mut self.attester_slashings.attestation_2_source_epoch,
                &mut self.attester_slashings.attestation_2_source_root,
                &mut self.attester_slashings.attestation_2_target_epoch,
                &mut self.attester_slashings.attestation_2_target_root,
            )
        };

        if let Some(data) = att.and_then(|a| a.data.as_ref()) {
            slot_b.append_value(data.slot);
            ci_b.append_value(data.committee_index);
            bbr_b.append_value(&data.beacon_block_root);
            if let Some(src) = &data.source {
                se_b.append_value(src.epoch);
                sr_b.append_value(&src.root);
            } else {
                se_b.append_value(0);
                sr_b.append_value(&[]);
            }
            if let Some(tgt) = &data.target {
                te_b.append_value(tgt.epoch);
                tr_b.append_value(&tgt.root);
            } else {
                te_b.append_value(0);
                tr_b.append_value(&[]);
            }
        } else {
            slot_b.append_value(0);
            ci_b.append_value(0);
            bbr_b.append_value(&[]);
            se_b.append_value(0);
            sr_b.append_value(&[]);
            te_b.append_value(0);
            tr_b.append_value(&[]);
        }
    }
}

impl BlockMapper for BeaconBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        let block = beacon::Block::decode(block_bytes)?;
        let identity =
            self.blocks
                .canonical
                .prepare_with_ids(identity, &block.root, &block.parent_root)?;
        self.map_beacon_block(&block, &identity, fork_step);
        // Beacon chain uses attestations rather than traditional transactions.
        Ok(0)
    }

    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        result.insert(
            "blocks".to_string(),
            self.blocks.finish(&self.blocks_schema)?,
        );
        result.insert(
            "attestations".to_string(),
            self.attestations.finish(&self.attestations_schema)?,
        );
        result.insert(
            "deposits".to_string(),
            self.deposits.finish(&self.deposits_schema)?,
        );
        result.insert(
            "proposer_slashings".to_string(),
            self.proposer_slashings
                .finish(&self.proposer_slashings_schema)?,
        );
        result.insert(
            "attester_slashings".to_string(),
            self.attester_slashings
                .finish(&self.attester_slashings_schema)?,
        );
        result.insert(
            "voluntary_exits".to_string(),
            self.voluntary_exits.finish(&self.voluntary_exits_schema)?,
        );
        result.insert(
            "execution_payload".to_string(),
            self.execution_payload
                .finish(&self.execution_payload_schema)?,
        );
        result.insert(
            "blob_sidecars".to_string(),
            self.blob_sidecars.finish(&self.blob_sidecars_schema)?,
        );
        result.insert(
            "withdrawals".to_string(),
            self.withdrawals.finish(&self.withdrawals_schema)?,
        );
        result.insert(
            "bls_to_execution_changes".to_string(),
            self.bls_to_execution_changes
                .finish(&self.bls_to_execution_changes_schema)?,
        );
        result.insert(
            "deposit_requests".to_string(),
            self.deposit_requests
                .finish(&self.deposit_requests_schema)?,
        );
        result.insert(
            "withdrawal_requests".to_string(),
            self.withdrawal_requests
                .finish(&self.withdrawal_requests_schema)?,
        );
        result.insert(
            "consolidation_requests".to_string(),
            self.consolidation_requests
                .finish(&self.consolidation_requests_schema)?,
        );
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        self.blocks
            .canonical
            .len()
            .max(self.attestations.canonical.len())
            .max(self.deposits.canonical.len())
            .max(self.proposer_slashings.canonical.len())
            .max(self.attester_slashings.canonical.len())
            .max(self.voluntary_exits.canonical.len())
            .max(self.execution_payload.canonical.len())
            .max(self.blob_sidecars.canonical.len())
            .max(self.withdrawals.canonical.len())
            .max(self.bls_to_execution_changes.canonical.len())
            .max(self.deposit_requests.canonical.len())
            .max(self.withdrawal_requests.canonical.len())
            .max(self.consolidation_requests.canonical.len())
    }

    fn total_rows(&self) -> usize {
        self.blocks.canonical.len()
            + self.attestations.canonical.len()
            + self.deposits.canonical.len()
            + self.proposer_slashings.canonical.len()
            + self.attester_slashings.canonical.len()
            + self.voluntary_exits.canonical.len()
            + self.execution_payload.canonical.len()
            + self.blob_sidecars.canonical.len()
            + self.withdrawals.canonical.len()
            + self.bls_to_execution_changes.canonical.len()
            + self.deposit_requests.canonical.len()
            + self.withdrawal_requests.canonical.len()
            + self.consolidation_requests.canonical.len()
    }

    fn largest_table(&mut self) -> (&str, usize) {
        let blocks = self.blocks.canonical.estimated_bytes()
            + est_u64(&self.blocks.slot)
            + est_u64(&self.blocks.parent_slot)
            + est_u64(&self.blocks.proposer_index)
            + self.blocks.root.estimated_bytes()
            + self.blocks.parent_root.estimated_bytes()
            + self.blocks.state_root.estimated_bytes()
            + self.blocks.body_root.estimated_bytes()
            + self.blocks.signature.estimated_bytes()
            + est_str(&self.blocks.spec)
            + self.blocks.graffiti.estimated_bytes()
            + est_opt_str(&self.blocks.fork_step);
        let attestations = self.attestations.canonical.estimated_bytes()
            + est_u64(&self.attestations.block_slot)
            + est_u32(&self.attestations.attestation_index)
            + est_u64(&self.attestations.slot)
            + est_u64(&self.attestations.committee_index)
            + self.attestations.aggregation_bits.estimated_bytes()
            + self.attestations.beacon_block_root.estimated_bytes()
            + est_u64(&self.attestations.source_epoch)
            + self.attestations.source_root.estimated_bytes()
            + est_u64(&self.attestations.target_epoch)
            + self.attestations.target_root.estimated_bytes()
            + self.attestations.signature.estimated_bytes()
            + self.attestations.committee_bits.estimated_bytes()
            + est_opt_str(&self.attestations.fork_step);
        let deposits = self.deposits.canonical.estimated_bytes()
            + est_u64(&self.deposits.block_slot)
            + est_u32(&self.deposits.deposit_index)
            + self.deposits.pubkey.estimated_bytes()
            + self.deposits.withdrawal_credentials.estimated_bytes()
            + est_u64(&self.deposits.amount)
            + self.deposits.signature.estimated_bytes()
            + est_opt_str(&self.deposits.fork_step);
        let proposer_slashings = self.proposer_slashings.canonical.estimated_bytes()
            + est_u64(&self.proposer_slashings.block_slot)
            + est_u32(&self.proposer_slashings.slashing_index)
            + est_u64(&self.proposer_slashings.header_1_slot)
            + est_u64(&self.proposer_slashings.header_1_proposer_index)
            + self
                .proposer_slashings
                .header_1_parent_root
                .estimated_bytes()
            + self
                .proposer_slashings
                .header_1_state_root
                .estimated_bytes()
            + self.proposer_slashings.header_1_body_root.estimated_bytes()
            + est_u64(&self.proposer_slashings.header_2_slot)
            + est_u64(&self.proposer_slashings.header_2_proposer_index)
            + self
                .proposer_slashings
                .header_2_parent_root
                .estimated_bytes()
            + self
                .proposer_slashings
                .header_2_state_root
                .estimated_bytes()
            + self.proposer_slashings.header_2_body_root.estimated_bytes()
            + est_opt_str(&self.proposer_slashings.fork_step);
        let attester_slashings = self.attester_slashings.canonical.estimated_bytes()
            + est_u64(&self.attester_slashings.block_slot)
            + est_u32(&self.attester_slashings.slashing_index)
            + est_u64(&self.attester_slashings.attestation_1_slot)
            + est_u64(&self.attester_slashings.attestation_1_committee_index)
            + self
                .attester_slashings
                .attestation_1_beacon_block_root
                .estimated_bytes()
            + est_u64(&self.attester_slashings.attestation_1_source_epoch)
            + self
                .attester_slashings
                .attestation_1_source_root
                .estimated_bytes()
            + est_u64(&self.attester_slashings.attestation_1_target_epoch)
            + self
                .attester_slashings
                .attestation_1_target_root
                .estimated_bytes()
            + est_u64(&self.attester_slashings.attestation_2_slot)
            + est_u64(&self.attester_slashings.attestation_2_committee_index)
            + self
                .attester_slashings
                .attestation_2_beacon_block_root
                .estimated_bytes()
            + est_u64(&self.attester_slashings.attestation_2_source_epoch)
            + self
                .attester_slashings
                .attestation_2_source_root
                .estimated_bytes()
            + est_u64(&self.attester_slashings.attestation_2_target_epoch)
            + self
                .attester_slashings
                .attestation_2_target_root
                .estimated_bytes()
            + est_list_u64(&mut self.attester_slashings.attestation_1_attesting_indices)
            + est_list_u64(&mut self.attester_slashings.attestation_2_attesting_indices)
            + est_opt_str(&self.attester_slashings.fork_step);
        let voluntary_exits = self.voluntary_exits.canonical.estimated_bytes()
            + est_u64(&self.voluntary_exits.block_slot)
            + est_u32(&self.voluntary_exits.exit_index)
            + est_u64(&self.voluntary_exits.epoch)
            + est_u64(&self.voluntary_exits.validator_index)
            + self.voluntary_exits.signature.estimated_bytes()
            + est_opt_str(&self.voluntary_exits.fork_step);
        let execution_payload = self.execution_payload.canonical.estimated_bytes()
            + est_u64(&self.execution_payload.block_slot)
            + self.execution_payload.parent_hash.estimated_bytes()
            + self.execution_payload.fee_recipient.estimated_bytes()
            + self.execution_payload.state_root.estimated_bytes()
            + self.execution_payload.receipts_root.estimated_bytes()
            + self.execution_payload.prev_randao.estimated_bytes()
            + est_u64(&self.execution_payload.block_number)
            + est_u64(&self.execution_payload.gas_limit)
            + est_u64(&self.execution_payload.gas_used)
            + est_i64(&self.execution_payload.payload_timestamp)
            + self.execution_payload.block_hash.estimated_bytes()
            + self.execution_payload.base_fee_per_gas.estimated_bytes()
            + est_u64(&self.execution_payload.blob_gas_used)
            + est_u64(&self.execution_payload.excess_blob_gas)
            + est_opt_str(&self.execution_payload.fork_step);
        let blob_sidecars = self.blob_sidecars.canonical.estimated_bytes()
            + est_u64(&self.blob_sidecars.block_slot)
            + est_u64(&self.blob_sidecars.blob_index)
            + self.blob_sidecars.blob.estimated_bytes()
            + self.blob_sidecars.kzg_commitment.estimated_bytes()
            + self.blob_sidecars.kzg_proof.estimated_bytes()
            + est_opt_str(&self.blob_sidecars.fork_step);
        [
            ("blocks", blocks),
            ("attestations", attestations),
            ("deposits", deposits),
            ("proposer_slashings", proposer_slashings),
            ("attester_slashings", attester_slashings),
            ("voluntary_exits", voluntary_exits),
            ("execution_payload", execution_payload),
            ("blob_sidecars", blob_sidecars),
            ("withdrawals", self.withdrawals.estimated_bytes()),
            (
                "bls_to_execution_changes",
                self.bls_to_execution_changes.estimated_bytes(),
            ),
            ("deposit_requests", self.deposit_requests.estimated_bytes()),
            (
                "withdrawal_requests",
                self.withdrawal_requests.estimated_bytes(),
            ),
            (
                "consolidation_requests",
                self.consolidation_requests.estimated_bytes(),
            ),
        ]
        .into_iter()
        .max_by_key(|&(_, s)| s)
        .unwrap_or(("blocks", 0))
    }

    fn table_names(&self) -> Vec<&str> {
        schema::TABLE_NAMES.to_vec()
    }
}

// ===========================================================================
// Builders
// ===========================================================================

struct BlocksBuilder {
    canonical: CanonicalBuilder,
    slot: UInt64Builder,
    parent_slot: UInt64Builder,
    proposer_index: UInt64Builder,
    root: BytesColumn,
    parent_root: BytesColumn,
    state_root: BytesColumn,
    body_root: BytesColumn,
    signature: BytesColumn,
    spec: StringBuilder,
    graffiti: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            slot: UInt64Builder::new(),
            parent_slot: UInt64Builder::new(),
            proposer_index: UInt64Builder::new(),
            root: BytesColumn::new(encoding),
            parent_root: BytesColumn::new(encoding),
            state_root: BytesColumn::new(encoding),
            body_root: BytesColumn::new(encoding),
            signature: BytesColumn::new(encoding),
            spec: StringBuilder::new(),
            graffiti: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.parent_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.proposer_index.finish()) as Arc<dyn Array>,
            self.root.finish(),
            self.parent_root.finish(),
            self.state_root.finish(),
            self.body_root.finish(),
            self.signature.finish(),
            Arc::new(self.spec.finish()) as Arc<dyn Array>,
            self.graffiti.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct AttestationsBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    attestation_index: UInt32Builder,
    slot: UInt64Builder,
    committee_index: UInt64Builder,
    aggregation_bits: BytesColumn,
    beacon_block_root: BytesColumn,
    source_epoch: UInt64Builder,
    source_root: BytesColumn,
    target_epoch: UInt64Builder,
    target_root: BytesColumn,
    signature: BytesColumn,
    committee_bits: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl AttestationsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            attestation_index: UInt32Builder::new(),
            slot: UInt64Builder::new(),
            committee_index: UInt64Builder::new(),
            aggregation_bits: BytesColumn::new(encoding),
            beacon_block_root: BytesColumn::new(encoding),
            source_epoch: UInt64Builder::new(),
            source_root: BytesColumn::new(encoding),
            target_epoch: UInt64Builder::new(),
            target_root: BytesColumn::new(encoding),
            signature: BytesColumn::new(encoding),
            committee_bits: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.attestation_index.finish()) as Arc<dyn Array>,
            Arc::new(self.slot.finish()) as Arc<dyn Array>,
            Arc::new(self.committee_index.finish()) as Arc<dyn Array>,
            self.aggregation_bits.finish(),
            self.beacon_block_root.finish(),
            Arc::new(self.source_epoch.finish()) as Arc<dyn Array>,
            self.source_root.finish(),
            Arc::new(self.target_epoch.finish()) as Arc<dyn Array>,
            self.target_root.finish(),
            self.signature.finish(),
            self.committee_bits.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct DepositsBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    deposit_index: UInt32Builder,
    pubkey: BytesColumn,
    withdrawal_credentials: BytesColumn,
    amount: UInt64Builder,
    signature: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl DepositsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            deposit_index: UInt32Builder::new(),
            pubkey: BytesColumn::new(encoding),
            withdrawal_credentials: BytesColumn::new(encoding),
            amount: UInt64Builder::new(),
            signature: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.deposit_index.finish()) as Arc<dyn Array>,
            self.pubkey.finish(),
            self.withdrawal_credentials.finish(),
            Arc::new(self.amount.finish()) as Arc<dyn Array>,
            self.signature.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct ProposerSlashingsBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    slashing_index: UInt32Builder,
    header_1_slot: UInt64Builder,
    header_1_proposer_index: UInt64Builder,
    header_1_parent_root: BytesColumn,
    header_1_state_root: BytesColumn,
    header_1_body_root: BytesColumn,
    header_2_slot: UInt64Builder,
    header_2_proposer_index: UInt64Builder,
    header_2_parent_root: BytesColumn,
    header_2_state_root: BytesColumn,
    header_2_body_root: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl ProposerSlashingsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            slashing_index: UInt32Builder::new(),
            header_1_slot: UInt64Builder::new(),
            header_1_proposer_index: UInt64Builder::new(),
            header_1_parent_root: BytesColumn::new(encoding),
            header_1_state_root: BytesColumn::new(encoding),
            header_1_body_root: BytesColumn::new(encoding),
            header_2_slot: UInt64Builder::new(),
            header_2_proposer_index: UInt64Builder::new(),
            header_2_parent_root: BytesColumn::new(encoding),
            header_2_state_root: BytesColumn::new(encoding),
            header_2_body_root: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.slashing_index.finish()) as Arc<dyn Array>,
            Arc::new(self.header_1_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.header_1_proposer_index.finish()) as Arc<dyn Array>,
            self.header_1_parent_root.finish(),
            self.header_1_state_root.finish(),
            self.header_1_body_root.finish(),
            Arc::new(self.header_2_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.header_2_proposer_index.finish()) as Arc<dyn Array>,
            self.header_2_parent_root.finish(),
            self.header_2_state_root.finish(),
            self.header_2_body_root.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct AttesterSlashingsBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    slashing_index: UInt32Builder,
    attestation_1_slot: UInt64Builder,
    attestation_1_committee_index: UInt64Builder,
    attestation_1_beacon_block_root: BytesColumn,
    attestation_1_source_epoch: UInt64Builder,
    attestation_1_source_root: BytesColumn,
    attestation_1_target_epoch: UInt64Builder,
    attestation_1_target_root: BytesColumn,
    attestation_2_slot: UInt64Builder,
    attestation_2_committee_index: UInt64Builder,
    attestation_2_beacon_block_root: BytesColumn,
    attestation_2_source_epoch: UInt64Builder,
    attestation_2_source_root: BytesColumn,
    attestation_2_target_epoch: UInt64Builder,
    attestation_2_target_root: BytesColumn,
    attestation_1_attesting_indices: ListBuilder<UInt64Builder>,
    attestation_2_attesting_indices: ListBuilder<UInt64Builder>,
    fork_step: Option<StringBuilder>,
}

impl AttesterSlashingsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            slashing_index: UInt32Builder::new(),
            attestation_1_slot: UInt64Builder::new(),
            attestation_1_committee_index: UInt64Builder::new(),
            attestation_1_beacon_block_root: BytesColumn::new(encoding),
            attestation_1_source_epoch: UInt64Builder::new(),
            attestation_1_source_root: BytesColumn::new(encoding),
            attestation_1_target_epoch: UInt64Builder::new(),
            attestation_1_target_root: BytesColumn::new(encoding),
            attestation_2_slot: UInt64Builder::new(),
            attestation_2_committee_index: UInt64Builder::new(),
            attestation_2_beacon_block_root: BytesColumn::new(encoding),
            attestation_2_source_epoch: UInt64Builder::new(),
            attestation_2_source_root: BytesColumn::new(encoding),
            attestation_2_target_epoch: UInt64Builder::new(),
            attestation_2_target_root: BytesColumn::new(encoding),
            attestation_1_attesting_indices: ListBuilder::new(UInt64Builder::new()),
            attestation_2_attesting_indices: ListBuilder::new(UInt64Builder::new()),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.slashing_index.finish()) as Arc<dyn Array>,
            Arc::new(self.attestation_1_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.attestation_1_committee_index.finish()) as Arc<dyn Array>,
            self.attestation_1_beacon_block_root.finish(),
            Arc::new(self.attestation_1_source_epoch.finish()) as Arc<dyn Array>,
            self.attestation_1_source_root.finish(),
            Arc::new(self.attestation_1_target_epoch.finish()) as Arc<dyn Array>,
            self.attestation_1_target_root.finish(),
            Arc::new(self.attestation_2_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.attestation_2_committee_index.finish()) as Arc<dyn Array>,
            self.attestation_2_beacon_block_root.finish(),
            Arc::new(self.attestation_2_source_epoch.finish()) as Arc<dyn Array>,
            self.attestation_2_source_root.finish(),
            Arc::new(self.attestation_2_target_epoch.finish()) as Arc<dyn Array>,
            self.attestation_2_target_root.finish(),
            Arc::new(self.attestation_1_attesting_indices.finish()) as Arc<dyn Array>,
            Arc::new(self.attestation_2_attesting_indices.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct VoluntaryExitsBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    exit_index: UInt32Builder,
    epoch: UInt64Builder,
    validator_index: UInt64Builder,
    signature: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl VoluntaryExitsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            exit_index: UInt32Builder::new(),
            epoch: UInt64Builder::new(),
            validator_index: UInt64Builder::new(),
            signature: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.exit_index.finish()) as Arc<dyn Array>,
            Arc::new(self.epoch.finish()) as Arc<dyn Array>,
            Arc::new(self.validator_index.finish()) as Arc<dyn Array>,
            self.signature.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct ExecutionPayloadBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    parent_hash: BytesColumn,
    fee_recipient: BytesColumn,
    state_root: BytesColumn,
    receipts_root: BytesColumn,
    prev_randao: BytesColumn,
    block_number: UInt64Builder,
    gas_limit: UInt64Builder,
    gas_used: UInt64Builder,
    payload_timestamp: Int64Builder,
    block_hash: BytesColumn,
    base_fee_per_gas: BytesColumn,
    blob_gas_used: UInt64Builder,
    excess_blob_gas: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl ExecutionPayloadBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            parent_hash: BytesColumn::new(encoding),
            fee_recipient: BytesColumn::new(encoding),
            state_root: BytesColumn::new(encoding),
            receipts_root: BytesColumn::new(encoding),
            prev_randao: BytesColumn::new(encoding),
            block_number: UInt64Builder::new(),
            gas_limit: UInt64Builder::new(),
            gas_used: UInt64Builder::new(),
            payload_timestamp: Int64Builder::new(),
            block_hash: BytesColumn::new(encoding),
            base_fee_per_gas: BytesColumn::new(encoding),
            blob_gas_used: UInt64Builder::new(),
            excess_blob_gas: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            self.parent_hash.finish(),
            self.fee_recipient.finish(),
            self.state_root.finish(),
            self.receipts_root.finish(),
            self.prev_randao.finish(),
            Arc::new(self.block_number.finish()) as Arc<dyn Array>,
            Arc::new(self.gas_limit.finish()) as Arc<dyn Array>,
            Arc::new(self.gas_used.finish()) as Arc<dyn Array>,
            Arc::new(self.payload_timestamp.finish()) as Arc<dyn Array>,
            self.block_hash.finish(),
            self.base_fee_per_gas.finish(),
            Arc::new(self.blob_gas_used.finish()) as Arc<dyn Array>,
            Arc::new(self.excess_blob_gas.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct BlobSidecarsBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    blob_index: UInt64Builder,
    blob: BytesColumn,
    kzg_commitment: BytesColumn,
    kzg_proof: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl BlobSidecarsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            blob_index: UInt64Builder::new(),
            blob: BytesColumn::new(encoding),
            kzg_commitment: BytesColumn::new(encoding),
            kzg_proof: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.blob_index.finish()) as Arc<dyn Array>,
            self.blob.finish(),
            self.kzg_commitment.finish(),
            self.kzg_proof.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct WithdrawalsBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    withdrawal_index: UInt64Builder,
    validator_index: UInt64Builder,
    address: BytesColumn,
    amount: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl WithdrawalsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            withdrawal_index: UInt64Builder::new(),
            validator_index: UInt64Builder::new(),
            address: BytesColumn::new(encoding),
            amount: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.withdrawal_index.finish()) as Arc<dyn Array>,
            Arc::new(self.validator_index.finish()) as Arc<dyn Array>,
            self.address.finish(),
            Arc::new(self.amount.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.block_slot)
            + est_u64(&self.withdrawal_index)
            + est_u64(&self.validator_index)
            + self.address.estimated_bytes()
            + est_u64(&self.amount)
            + est_opt_str(&self.fork_step)
    }
}

struct BlsToExecutionChangesBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    change_index: UInt32Builder,
    validator_index: UInt64Builder,
    from_bls_pubkey: BytesColumn,
    to_execution_address: BytesColumn,
    signature: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl BlsToExecutionChangesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            change_index: UInt32Builder::new(),
            validator_index: UInt64Builder::new(),
            from_bls_pubkey: BytesColumn::new(encoding),
            to_execution_address: BytesColumn::new(encoding),
            signature: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.change_index.finish()) as Arc<dyn Array>,
            Arc::new(self.validator_index.finish()) as Arc<dyn Array>,
            self.from_bls_pubkey.finish(),
            self.to_execution_address.finish(),
            self.signature.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.block_slot)
            + est_u32(&self.change_index)
            + est_u64(&self.validator_index)
            + self.from_bls_pubkey.estimated_bytes()
            + self.to_execution_address.estimated_bytes()
            + self.signature.estimated_bytes()
            + est_opt_str(&self.fork_step)
    }
}

struct DepositRequestsBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    request_index: UInt32Builder,
    deposit_index: UInt64Builder,
    pubkey: BytesColumn,
    withdrawal_credentials: BytesColumn,
    amount: UInt64Builder,
    signature: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl DepositRequestsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            request_index: UInt32Builder::new(),
            deposit_index: UInt64Builder::new(),
            pubkey: BytesColumn::new(encoding),
            withdrawal_credentials: BytesColumn::new(encoding),
            amount: UInt64Builder::new(),
            signature: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.request_index.finish()) as Arc<dyn Array>,
            Arc::new(self.deposit_index.finish()) as Arc<dyn Array>,
            self.pubkey.finish(),
            self.withdrawal_credentials.finish(),
            Arc::new(self.amount.finish()) as Arc<dyn Array>,
            self.signature.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.block_slot)
            + est_u32(&self.request_index)
            + est_u64(&self.deposit_index)
            + self.pubkey.estimated_bytes()
            + self.withdrawal_credentials.estimated_bytes()
            + est_u64(&self.amount)
            + self.signature.estimated_bytes()
            + est_opt_str(&self.fork_step)
    }
}

struct WithdrawalRequestsBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    request_index: UInt32Builder,
    source_address: BytesColumn,
    validator_pubkey: BytesColumn,
    amount: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl WithdrawalRequestsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            request_index: UInt32Builder::new(),
            source_address: BytesColumn::new(encoding),
            validator_pubkey: BytesColumn::new(encoding),
            amount: UInt64Builder::new(),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.request_index.finish()) as Arc<dyn Array>,
            self.source_address.finish(),
            self.validator_pubkey.finish(),
            Arc::new(self.amount.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.block_slot)
            + est_u32(&self.request_index)
            + self.source_address.estimated_bytes()
            + self.validator_pubkey.estimated_bytes()
            + est_u64(&self.amount)
            + est_opt_str(&self.fork_step)
    }
}

struct ConsolidationRequestsBuilder {
    canonical: CanonicalBuilder,
    block_slot: UInt64Builder,
    request_index: UInt32Builder,
    source_address: BytesColumn,
    source_pubkey: BytesColumn,
    target_pubkey: BytesColumn,
    fork_step: Option<StringBuilder>,
}

impl ConsolidationRequestsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_slot: UInt64Builder::new(),
            request_index: UInt32Builder::new(),
            source_address: BytesColumn::new(encoding),
            source_pubkey: BytesColumn::new(encoding),
            target_pubkey: BytesColumn::new(encoding),
            fork_step: mk_fork_step(include_fork_step),
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.block_slot.finish()) as Arc<dyn Array>,
            Arc::new(self.request_index.finish()) as Arc<dyn Array>,
            self.source_address.finish(),
            self.source_pubkey.finish(),
            self.target_pubkey.finish(),
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u64(&self.block_slot)
            + est_u32(&self.request_index)
            + self.source_address.estimated_bytes()
            + self.source_pubkey.estimated_bytes()
            + self.target_pubkey.estimated_bytes()
            + est_opt_str(&self.fork_step)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn make_test_block(slot: u64) -> beacon::Block {
        beacon::Block {
            version: 1,
            spec: beacon::Spec::Phase0 as i32,
            slot,
            parent_slot: slot.saturating_sub(1),
            root: vec![0xab; 32],
            parent_root: vec![0xcd; 32],
            state_root: vec![0xef; 32],
            proposer_index: 42,
            body_root: vec![0x12; 32],
            signature: vec![0x34; 96],
            timestamp: Some(prost_types::Timestamp {
                seconds: 1700000000,
                nanos: 0,
            }),
            body: Some(beacon::block::Body::Phase0(beacon::Phase0Body {
                rando_reveal: vec![0x01; 96],
                eth1_data: Some(beacon::Eth1Data {
                    deposit_root: vec![0x02; 32],
                    deposit_count: 100,
                    block_hash: vec![0x03; 32],
                }),
                graffiti: vec![0x00; 32],
                proposer_slashings: vec![],
                attester_slashings: vec![],
                attestations: vec![beacon::Attestation {
                    aggregation_bits: vec![0xff],
                    data: Some(beacon::AttestationData {
                        slot: slot,
                        committee_index: 1,
                        beacon_block_root: vec![0xaa; 32],
                        source: Some(beacon::Checkpoint {
                            epoch: 10,
                            root: vec![0xbb; 32],
                        }),
                        target: Some(beacon::Checkpoint {
                            epoch: 11,
                            root: vec![0xcc; 32],
                        }),
                    }),
                    signature: vec![0xdd; 96],
                }],
                deposits: vec![beacon::Deposit {
                    proof: vec![vec![0x11; 32]],
                    data: Some(beacon::DepositData {
                        public_key: vec![0x22; 48],
                        withdrawal_credentials: vec![0x33; 32],
                        gwei: 32000000000,
                        signature: vec![0x44; 96],
                    }),
                }],
                voluntary_exits: vec![beacon::SignedVoluntaryExit {
                    message: Some(beacon::VoluntaryExit {
                        epoch: 100,
                        validator_index: 5,
                    }),
                    signature: vec![0x55; 96],
                }],
            })),
        }
    }

    pub(crate) fn make_deneb_block(slot: u64) -> beacon::Block {
        beacon::Block {
            version: 1,
            spec: beacon::Spec::Deneb as i32,
            slot,
            parent_slot: slot.saturating_sub(1),
            root: vec![0xab; 32],
            parent_root: vec![0xcd; 32],
            state_root: vec![0xef; 32],
            proposer_index: 42,
            body_root: vec![0x12; 32],
            signature: vec![0x34; 96],
            timestamp: Some(prost_types::Timestamp {
                seconds: 1700000000,
                nanos: 0,
            }),
            body: Some(beacon::block::Body::Deneb(beacon::DenebBody {
                rando_reveal: vec![0x01; 96],
                eth1_data: Some(beacon::Eth1Data {
                    deposit_root: vec![0x02; 32],
                    deposit_count: 200,
                    block_hash: vec![0x03; 32],
                }),
                graffiti: vec![0x00; 32],
                proposer_slashings: vec![],
                attester_slashings: vec![],
                attestations: vec![],
                deposits: vec![],
                voluntary_exits: vec![],
                sync_aggregate: None,
                execution_payload: Some(beacon::DenebExecutionPayload {
                    parent_hash: vec![0xa1; 32],
                    fee_recipient: vec![0xa2; 20],
                    state_root: vec![0xa3; 32],
                    receipts_root: vec![0xa4; 32],
                    logs_bloom: vec![0x00; 256],
                    prev_randao: vec![0xa5; 32],
                    block_number: 12345,
                    gas_limit: 30000000,
                    gas_used: 15000000,
                    timestamp: Some(prost_types::Timestamp {
                        seconds: 1700000000,
                        nanos: 0,
                    }),
                    extra_data: vec![],
                    base_fee_per_gas: vec![0x01],
                    block_hash: vec![0xa6; 32],
                    transactions: vec![],
                    withdrawals: test_withdrawals(),
                    blob_gas_used: 131072,
                    excess_blob_gas: 0,
                }),
                bls_to_execution_changes: vec![test_bls_change()],
                blob_kzg_commitments: vec![],
                embedded_blobs: vec![beacon::Blob {
                    index: 0,
                    blob: vec![0xff; 32],
                    kzg_commitment: vec![0xee; 48],
                    kzg_proof: vec![0xdd; 48],
                    kzg_commitment_inclusion_proof: vec![],
                }],
            })),
        }
    }

    fn test_withdrawals() -> Vec<beacon::Withdrawal> {
        vec![
            beacon::Withdrawal {
                withdrawal_index: 1_000,
                validator_index: 7,
                address: vec![0xb1; 20],
                gwei: 12_345,
            },
            beacon::Withdrawal {
                withdrawal_index: 1_001,
                validator_index: 8,
                address: vec![0xb2; 20],
                gwei: 32_000_000_000,
            },
        ]
    }

    fn test_bls_change() -> beacon::SignedBlsToExecutionChange {
        beacon::SignedBlsToExecutionChange {
            message: Some(beacon::BlsToExecutionChange {
                validator_index: 9,
                from_bls_pub_key: vec![0xc1; 48],
                to_execution_address: vec![0xc2; 20],
            }),
            signature: vec![0xc3; 96],
        }
    }

    /// An Electra block: on-chain aggregated attestations with committee bits,
    /// withdrawals, a BLS change and one execution request of each kind.
    pub(crate) fn make_electra_block(slot: u64) -> beacon::Block {
        let Some(beacon::block::Body::Deneb(deneb)) = make_deneb_block(slot).body else {
            unreachable!("make_deneb_block builds a Deneb body");
        };
        beacon::Block {
            spec: beacon::Spec::Electra as i32,
            body: Some(beacon::block::Body::Electra(beacon::ElectraBody {
                rando_reveal: deneb.rando_reveal,
                eth1_data: deneb.eth1_data,
                graffiti: b"electra graffiti".to_vec(),
                proposer_slashings: vec![],
                attester_slashings: vec![],
                attestations: vec![beacon::ElectraAttestation {
                    aggregation_bits: vec![0x0f],
                    data: Some(beacon::AttestationData {
                        slot,
                        committee_index: 0,
                        beacon_block_root: vec![0xaa; 32],
                        source: Some(beacon::Checkpoint {
                            epoch: 20,
                            root: vec![0xbb; 32],
                        }),
                        target: Some(beacon::Checkpoint {
                            epoch: 21,
                            root: vec![0xcc; 32],
                        }),
                    }),
                    signature: vec![0xdd; 96],
                    // Committees 0 and 9 of a 64-bit bitvector.
                    committee_bits: vec![0x01, 0x02, 0, 0, 0, 0, 0, 0],
                }],
                deposits: vec![],
                voluntary_exits: vec![],
                sync_aggregate: None,
                execution_payload: deneb.execution_payload,
                bls_to_execution_changes: deneb.bls_to_execution_changes,
                blob_kzg_commitments: vec![],
                execution_requests: Some(beacon::ExecutionRequest {
                    deposits: vec![beacon::DepositRequest {
                        pub_key: vec![0xd1; 48],
                        withdrawal_credentials: vec![0xd2; 32],
                        amount: 32_000_000_000,
                        signature: vec![0xd3; 96],
                        index: 2_000_000,
                    }],
                    withdrawals: vec![beacon::WithdrawalRequest {
                        source_address: vec![0xe1; 20],
                        validator_pub_key: vec![0xe2; 48],
                        amount: 0,
                    }],
                    consolidations: vec![beacon::ConsolidationRequest {
                        source_address: vec![0xf1; 20],
                        source_pub_key: vec![0xf2; 48],
                        target_pub_key: vec![0xf3; 48],
                    }],
                }),
                embedded_blobs: vec![],
            })),
            ..make_deneb_block(slot)
        }
    }

    fn map_one(block: &beacon::Block) -> HashMap<String, RecordBatch> {
        let mut mapper = BeaconBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
            .unwrap();
        mapper.flush().unwrap()
    }

    fn strings(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing column {column}"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("{column} is not Utf8"));
        array.iter().map(|v| v.map(str::to_string)).collect()
    }

    fn u64s(batch: &RecordBatch, column: &str) -> Vec<u64> {
        batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing column {column}"))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap_or_else(|| panic!("{column} is not UInt64"))
            .values()
            .to_vec()
    }

    fn u32s(batch: &RecordBatch, column: &str) -> Vec<u32> {
        batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing column {column}"))
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap_or_else(|| panic!("{column} is not UInt32"))
            .values()
            .to_vec()
    }

    fn u64_lists(batch: &RecordBatch, column: &str) -> Vec<Vec<u64>> {
        let list = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing column {column}"))
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap_or_else(|| panic!("{column} is not a list"));
        (0..list.len())
            .map(|row| {
                list.value(row)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn hex(byte: u8, len: usize) -> Option<String> {
        Some(format!("0x{}", format!("{byte:02x}").repeat(len)))
    }

    #[test]
    fn test_map_and_flush() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BeaconBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["attestations"].num_rows(), 1);
        assert_eq!(batches["deposits"].num_rows(), 1);
        assert_eq!(batches["voluntary_exits"].num_rows(), 1);
        assert_eq!(batches["proposer_slashings"].num_rows(), 0);
        assert_eq!(batches["attester_slashings"].num_rows(), 0);
        assert_eq!(batches["execution_payload"].num_rows(), 0);
        assert_eq!(batches["blob_sidecars"].num_rows(), 0);
    }

    #[test]
    fn test_empty_block() {
        let block = beacon::Block {
            version: 1,
            spec: beacon::Spec::Phase0 as i32,
            slot: 0,
            parent_slot: 0,
            root: vec![],
            parent_root: vec![],
            state_root: vec![],
            proposer_index: 0,
            body_root: vec![],
            signature: vec![],
            timestamp: None,
            body: None,
        };
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BeaconBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["attestations"].num_rows(), 0);
        assert_eq!(batches["deposits"].num_rows(), 0);
        assert_eq!(batches["proposer_slashings"].num_rows(), 0);
        assert_eq!(batches["attester_slashings"].num_rows(), 0);
        assert_eq!(batches["voluntary_exits"].num_rows(), 0);
        assert_eq!(batches["execution_payload"].num_rows(), 0);
        assert_eq!(batches["blob_sidecars"].num_rows(), 0);
        for table in NEW_TABLES {
            assert_eq!(batches[table].num_rows(), 0, "{table}");
        }
        // Graffiti is part of every fork's body; a block without one has none.
        assert_eq!(strings(&batches["blocks"], "graffiti"), vec![None]);
    }

    /// Tables added for #504. They get no rows before the fork that
    /// introduced their data.
    const NEW_TABLES: [&str; 5] = [
        "withdrawals",
        "bls_to_execution_changes",
        "deposit_requests",
        "withdrawal_requests",
        "consolidation_requests",
    ];

    #[test]
    fn test_phase0_block_leaves_later_fork_fields_empty() {
        let batches = map_one(&make_test_block(100));

        assert_eq!(strings(&batches["blocks"], "graffiti"), vec![hex(0, 32)]);
        // Before Electra, `committee_index` identifies the committee.
        assert_eq!(u64s(&batches["attestations"], "committee_index"), vec![1]);
        assert_eq!(
            strings(&batches["attestations"], "committee_bits"),
            vec![None]
        );
        for table in NEW_TABLES {
            assert_eq!(batches[table].num_rows(), 0, "{table}");
        }
    }

    #[test]
    fn test_capella_block_maps_withdrawals() {
        let Some(beacon::block::Body::Deneb(deneb)) = make_deneb_block(300).body else {
            unreachable!("make_deneb_block builds a Deneb body");
        };
        let payload = deneb.execution_payload.unwrap();
        let block = beacon::Block {
            spec: beacon::Spec::Capella as i32,
            body: Some(beacon::block::Body::Capella(beacon::CapellaBody {
                rando_reveal: deneb.rando_reveal,
                eth1_data: deneb.eth1_data,
                graffiti: b"capella".to_vec(),
                proposer_slashings: vec![],
                attester_slashings: vec![],
                attestations: vec![],
                deposits: vec![],
                voluntary_exits: vec![],
                sync_aggregate: None,
                execution_payload: Some(beacon::CapellaExecutionPayload {
                    parent_hash: payload.parent_hash,
                    fee_recipient: payload.fee_recipient,
                    state_root: payload.state_root,
                    receipts_root: payload.receipts_root,
                    logs_bloom: payload.logs_bloom,
                    prev_randao: payload.prev_randao,
                    block_number: payload.block_number,
                    gas_limit: payload.gas_limit,
                    gas_used: payload.gas_used,
                    timestamp: payload.timestamp,
                    extra_data: payload.extra_data,
                    base_fee_per_gas: payload.base_fee_per_gas,
                    block_hash: payload.block_hash,
                    transactions: vec![],
                    withdrawals: test_withdrawals(),
                }),
            })),
            ..make_deneb_block(300)
        };
        let batches = map_one(&block);

        let withdrawals = &batches["withdrawals"];
        assert_eq!(withdrawals.num_rows(), 2);
        assert_eq!(u64s(withdrawals, "block_slot"), vec![300, 300]);
        assert_eq!(u64s(withdrawals, "withdrawal_index"), vec![1_000, 1_001]);
        assert_eq!(u64s(withdrawals, "validator_index"), vec![7, 8]);
        assert_eq!(
            strings(withdrawals, "address"),
            vec![hex(0xb1, 20), hex(0xb2, 20)]
        );
        assert_eq!(u64s(withdrawals, "amount"), vec![12_345, 32_000_000_000]);
        // The Firehose Capella body has no BLS changes and Capella has no
        // execution requests.
        for table in &NEW_TABLES[1..] {
            assert_eq!(batches[*table].num_rows(), 0, "{table}");
        }
    }

    #[test]
    fn test_deneb_block_maps_withdrawals_and_bls_changes() {
        let batches = map_one(&make_deneb_block(200));

        assert_eq!(batches["withdrawals"].num_rows(), 2);
        let changes = &batches["bls_to_execution_changes"];
        assert_eq!(changes.num_rows(), 1);
        assert_eq!(u64s(changes, "block_slot"), vec![200]);
        assert_eq!(u32s(changes, "change_index"), vec![0]);
        assert_eq!(u64s(changes, "validator_index"), vec![9]);
        assert_eq!(strings(changes, "from_bls_pubkey"), vec![hex(0xc1, 48)]);
        assert_eq!(
            strings(changes, "to_execution_address"),
            vec![hex(0xc2, 20)]
        );
        assert_eq!(strings(changes, "signature"), vec![hex(0xc3, 96)]);
        for table in [
            "deposit_requests",
            "withdrawal_requests",
            "consolidation_requests",
        ] {
            assert_eq!(batches[table].num_rows(), 0, "{table}");
        }
    }

    #[test]
    fn test_electra_block_maps_committee_bits_and_execution_requests() {
        let batches = map_one(&make_electra_block(400));

        let graffiti: String = b"electra graffiti"
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(
            strings(&batches["blocks"], "graffiti"),
            vec![Some(format!("0x{graffiti}"))]
        );
        let attestations = &batches["attestations"];
        assert_eq!(u64s(attestations, "committee_index"), vec![0]);
        assert_eq!(
            strings(attestations, "committee_bits"),
            vec![Some("0x0102000000000000".to_string())]
        );
        assert_eq!(batches["withdrawals"].num_rows(), 2);
        assert_eq!(batches["bls_to_execution_changes"].num_rows(), 1);

        let deposits = &batches["deposit_requests"];
        assert_eq!(deposits.num_rows(), 1);
        assert_eq!(u64s(deposits, "block_slot"), vec![400]);
        assert_eq!(u32s(deposits, "request_index"), vec![0]);
        assert_eq!(u64s(deposits, "deposit_index"), vec![2_000_000]);
        assert_eq!(strings(deposits, "pubkey"), vec![hex(0xd1, 48)]);
        assert_eq!(
            strings(deposits, "withdrawal_credentials"),
            vec![hex(0xd2, 32)]
        );
        assert_eq!(u64s(deposits, "amount"), vec![32_000_000_000]);
        assert_eq!(strings(deposits, "signature"), vec![hex(0xd3, 96)]);

        let withdrawals = &batches["withdrawal_requests"];
        assert_eq!(withdrawals.num_rows(), 1);
        assert_eq!(u32s(withdrawals, "request_index"), vec![0]);
        assert_eq!(strings(withdrawals, "source_address"), vec![hex(0xe1, 20)]);
        assert_eq!(
            strings(withdrawals, "validator_pubkey"),
            vec![hex(0xe2, 48)]
        );
        assert_eq!(u64s(withdrawals, "amount"), vec![0]);

        let consolidations = &batches["consolidation_requests"];
        assert_eq!(consolidations.num_rows(), 1);
        assert_eq!(u32s(consolidations, "request_index"), vec![0]);
        assert_eq!(
            strings(consolidations, "source_address"),
            vec![hex(0xf1, 20)]
        );
        assert_eq!(
            strings(consolidations, "source_pubkey"),
            vec![hex(0xf2, 48)]
        );
        assert_eq!(
            strings(consolidations, "target_pubkey"),
            vec![hex(0xf3, 48)]
        );
    }

    #[test]
    fn test_electra_block_without_execution_requests() {
        let mut block = make_electra_block(400);
        let Some(beacon::block::Body::Electra(body)) = block.body.as_mut() else {
            unreachable!("make_electra_block builds an Electra body");
        };
        body.execution_requests = None;
        let batches = map_one(&block);
        for table in [
            "deposit_requests",
            "withdrawal_requests",
            "consolidation_requests",
        ] {
            assert_eq!(batches[table].num_rows(), 0, "{table}");
        }
    }

    #[test]
    fn test_fusaka_body_maps_like_electra() {
        let mut block = make_electra_block(500);
        let Some(beacon::block::Body::Electra(body)) = block.body.take() else {
            unreachable!("make_electra_block builds an Electra body");
        };
        block.spec = beacon::Spec::Fusaka as i32;
        block.body = Some(beacon::block::Body::Fusaka(body));
        let batches = map_one(&block);

        assert_eq!(
            strings(&batches["blocks"], "spec"),
            vec![Some("FUSAKA".to_string())]
        );
        assert_eq!(
            strings(&batches["attestations"], "committee_bits"),
            vec![Some("0x0102000000000000".to_string())]
        );
        for (table, rows) in [
            ("withdrawals", 2),
            ("bls_to_execution_changes", 1),
            ("deposit_requests", 1),
            ("withdrawal_requests", 1),
            ("consolidation_requests", 1),
        ] {
            assert_eq!(batches[table].num_rows(), rows, "{table}");
        }
    }

    #[test]
    fn test_attester_slashings_keep_attesting_indices() {
        let indexed = |indices: Vec<u64>| beacon::IndexedAttestation {
            attesting_indices: indices,
            data: None,
            signature: vec![],
        };
        let mut block = make_test_block(100);
        let Some(beacon::block::Body::Phase0(body)) = block.body.as_mut() else {
            unreachable!("make_test_block builds a Phase0 body");
        };
        body.attester_slashings = vec![
            beacon::AttesterSlashing {
                attestation_1: Some(indexed(vec![3, 5, 8])),
                attestation_2: Some(indexed(vec![5])),
            },
            beacon::AttesterSlashing {
                attestation_1: None,
                attestation_2: Some(indexed(vec![])),
            },
        ];
        let batches = map_one(&block);

        let slashings = &batches["attester_slashings"];
        assert_eq!(
            u64_lists(slashings, "attestation_1_attesting_indices"),
            vec![vec![3, 5, 8], vec![]]
        );
        assert_eq!(
            u64_lists(slashings, "attestation_2_attesting_indices"),
            vec![vec![5], vec![]]
        );
    }

    #[test]
    fn test_flush_resets() {
        let block = make_test_block(1);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BeaconBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_table_names() {
        let mapper = BeaconBlockMapper::new(false, EncodeBytes::Hex);
        assert_eq!(mapper.table_names().len(), 13);
        for table in NEW_TABLES {
            assert!(mapper.table_names().contains(&table), "{table}");
        }
        assert!(mapper.table_names().contains(&"blocks"));
        assert!(mapper.table_names().contains(&"attestations"));
        assert!(mapper.table_names().contains(&"deposits"));
        assert!(mapper.table_names().contains(&"proposer_slashings"));
        assert!(mapper.table_names().contains(&"attester_slashings"));
        assert!(mapper.table_names().contains(&"voluntary_exits"));
        assert!(mapper.table_names().contains(&"execution_payload"));
        assert!(mapper.table_names().contains(&"blob_sidecars"));
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_block(0);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BeaconBlockMapper::new(true, EncodeBytes::Hex);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), Some("FINAL"))
            .unwrap();

        let batches = mapper.flush().unwrap();
        let blocks_batch = &batches["blocks"];
        let last_col = blocks_batch.num_columns() - 1;
        assert_eq!(blocks_batch.schema().field(last_col).name(), "fork_step");
        let fork_col = blocks_batch
            .column(last_col)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(fork_col.value(0), "FINAL");
    }

    #[test]
    fn test_deneb_execution_payload_and_blobs() {
        let block = make_deneb_block(200);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BeaconBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["execution_payload"].num_rows(), 1);
        assert_eq!(batches["blob_sidecars"].num_rows(), 1);
    }

    #[test]
    fn test_beacon_canonical_ids_match_root_fields() {
        let block = make_deneb_block(200);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = BeaconBlockMapper::new(false, EncodeBytes::Hex);
        let identity = BlockIdentity {
            block_num: 200,
            block_id: "0x9999".to_string(),
            parent_num: 199,
            parent_id: "0x8888".to_string(),
            lib_num: 198,
            timestamp: 1_700_000_000,
            timestamp_nanos: 0,
            fork_step: None,
        };

        mapper.map_block(&block_bytes, &identity, None).unwrap();
        let batches = mapper.flush().unwrap();

        let blocks = &batches["blocks"];
        let block_id = blocks
            .column_by_name("block_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let parent_id = blocks
            .column_by_name("parent_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let root = blocks
            .column_by_name("root")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let parent_root = blocks
            .column_by_name("parent_root")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let execution_payload = &batches["execution_payload"];
        let execution_block_hash = execution_payload
            .column_by_name("block_hash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let execution_parent_hash = execution_payload
            .column_by_name("parent_hash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(block_id.value(0), root.value(0));
        assert_eq!(parent_id.value(0), parent_root.value(0));
        assert_ne!(block_id.value(0), execution_block_hash.value(0));
        assert_ne!(parent_id.value(0), execution_parent_hash.value(0));
        assert_ne!(block_id.value(0), "0x9999");
        assert_ne!(parent_id.value(0), "0x8888");
    }
}
