use super::proto::beacon;
use super::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{encode_hex, BytesColumn, EncodeBytes};
use firehose_parquet::traits::{
    est_i64, est_opt_str, est_str, est_u32, est_u64, BlockIdentity, BlockMapper, CanonicalBuilder,
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

fn beacon_canonical_identity(block: &beacon::Block, identity: &BlockIdentity) -> BlockIdentity {
    let mut canonical = identity.clone();
    canonical.block_id = encode_hex(&block.root);
    canonical.parent_id = encode_hex(&block.parent_root);
    canonical
}

// ---------------------------------------------------------------------------
// Common body fields extracted across all fork variants
// ---------------------------------------------------------------------------

struct BodyFields<'a> {
    attestations: Vec<AttestationFields<'a>>,
    deposits: &'a [beacon::Deposit],
    proposer_slashings: &'a [beacon::ProposerSlashing],
    attester_slashings: &'a [beacon::AttesterSlashing],
    voluntary_exits: &'a [beacon::SignedVoluntaryExit],
    execution_payload: Option<ExecutionPayloadFields<'a>>,
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

fn extract_body_fields(block: &beacon::Block) -> BodyFields<'_> {
    let empty_deposits: &[beacon::Deposit] = &[];
    let empty_proposer: &[beacon::ProposerSlashing] = &[];
    let empty_attester: &[beacon::AttesterSlashing] = &[];
    let empty_exits: &[beacon::SignedVoluntaryExit] = &[];
    let empty_blobs: &[beacon::Blob] = &[];

    match &block.body {
        Some(beacon::block::Body::Phase0(b)) => BodyFields {
            attestations: b
                .attestations
                .iter()
                .map(AttestationFields::Standard)
                .collect(),
            deposits: &b.deposits,
            proposer_slashings: &b.proposer_slashings,
            attester_slashings: &b.attester_slashings,
            voluntary_exits: &b.voluntary_exits,
            execution_payload: None,
            blobs: empty_blobs,
        },
        Some(beacon::block::Body::Altair(b)) => BodyFields {
            attestations: b
                .attestations
                .iter()
                .map(AttestationFields::Standard)
                .collect(),
            deposits: &b.deposits,
            proposer_slashings: &b.proposer_slashings,
            attester_slashings: &b.attester_slashings,
            voluntary_exits: &b.voluntary_exits,
            execution_payload: None,
            blobs: empty_blobs,
        },
        Some(beacon::block::Body::Bellatrix(b)) => BodyFields {
            attestations: b
                .attestations
                .iter()
                .map(AttestationFields::Standard)
                .collect(),
            deposits: &b.deposits,
            proposer_slashings: &b.proposer_slashings,
            attester_slashings: &b.attester_slashings,
            voluntary_exits: &b.voluntary_exits,
            execution_payload: b
                .execution_payload
                .as_ref()
                .map(|ep| ExecutionPayloadFields {
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
                }),
            blobs: empty_blobs,
        },
        Some(beacon::block::Body::Capella(b)) => BodyFields {
            attestations: b
                .attestations
                .iter()
                .map(AttestationFields::Standard)
                .collect(),
            deposits: &b.deposits,
            proposer_slashings: &b.proposer_slashings,
            attester_slashings: &b.attester_slashings,
            voluntary_exits: &b.voluntary_exits,
            execution_payload: b
                .execution_payload
                .as_ref()
                .map(|ep| ExecutionPayloadFields {
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
                }),
            blobs: empty_blobs,
        },
        Some(beacon::block::Body::Deneb(b)) => BodyFields {
            attestations: b
                .attestations
                .iter()
                .map(AttestationFields::Standard)
                .collect(),
            deposits: &b.deposits,
            proposer_slashings: &b.proposer_slashings,
            attester_slashings: &b.attester_slashings,
            voluntary_exits: &b.voluntary_exits,
            execution_payload: b
                .execution_payload
                .as_ref()
                .map(|ep| ExecutionPayloadFields {
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
                }),
            blobs: &b.embedded_blobs,
        },
        Some(beacon::block::Body::Electra(b)) | Some(beacon::block::Body::Fusaka(b)) => {
            BodyFields {
                attestations: b
                    .attestations
                    .iter()
                    .map(AttestationFields::Electra)
                    .collect(),
                deposits: &b.deposits,
                proposer_slashings: &b.proposer_slashings,
                attester_slashings: &b.attester_slashings,
                voluntary_exits: &b.voluntary_exits,
                execution_payload: b
                    .execution_payload
                    .as_ref()
                    .map(|ep| ExecutionPayloadFields {
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
                    }),
                blobs: &b.embedded_blobs,
            }
        }
        None => BodyFields {
            attestations: vec![],
            deposits: empty_deposits,
            proposer_slashings: empty_proposer,
            attester_slashings: empty_attester,
            voluntary_exits: empty_exits,
            execution_payload: None,
            blobs: empty_blobs,
        },
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
    blocks_schema: Schema,
    attestations_schema: Schema,
    deposits_schema: Schema,
    proposer_slashings_schema: Schema,
    attester_slashings_schema: Schema,
    voluntary_exits_schema: Schema,
    execution_payload_schema: Schema,
    blob_sidecars_schema: Schema,
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
            blocks_schema: schema::blocks_schema(include_fork_step, enc),
            attestations_schema: schema::attestations_schema(include_fork_step, enc),
            deposits_schema: schema::deposits_schema(include_fork_step, enc),
            proposer_slashings_schema: schema::proposer_slashings_schema(include_fork_step, enc),
            attester_slashings_schema: schema::attester_slashings_schema(include_fork_step, enc),
            voluntary_exits_schema: schema::voluntary_exits_schema(include_fork_step, enc),
            execution_payload_schema: schema::execution_payload_schema(include_fork_step, enc),
            blob_sidecars_schema: schema::blob_sidecars_schema(include_fork_step, enc),
        }
    }

    fn map_beacon_block(
        &mut self,
        block: &beacon::Block,
        identity: &BlockIdentity,
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
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        let body = extract_body_fields(block);

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
    ) -> anyhow::Result<()> {
        let block = beacon::Block::decode(block_bytes)?;
        let canonical = beacon_canonical_identity(&block, identity);
        self.map_beacon_block(&block, &canonical, fork_step);
        Ok(())
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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_block(slot: u64) -> beacon::Block {
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

    fn make_deneb_block(slot: u64) -> beacon::Block {
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
                    withdrawals: vec![],
                    blob_gas_used: 131072,
                    excess_blob_gas: 0,
                }),
                bls_to_execution_changes: vec![],
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
        assert_eq!(mapper.table_names().len(), 8);
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
