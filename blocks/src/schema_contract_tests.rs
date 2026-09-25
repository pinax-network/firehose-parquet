//! Cross-chain schema contract.
//!
//! Every table of every chain, under every bytes encoding and both
//! `fork_step` settings, must have unique column names and round-trip through
//! the Parquet writer and reader unchanged. Arrow and the Parquet writer accept
//! duplicate names silently, but Spark and Polars reject such files, DuckDB
//! renames the second column (`timestamp_1`), and `Schema::index_of` only ever
//! finds the first one.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use firehose_parquet::config::{BlockMetadata, Compression, Partition};
use firehose_parquet::encode::{decode_base58, EncodeBytes};
use firehose_parquet::traits::{timestamp_millis_utc_type, BlockIdentity, BlockMapper};
use firehose_parquet::writer::{read_parquet, ParquetTableWriter};
use prost::Message;

use crate::antelope::mapper::AntelopeBlockMapper;
use crate::beacon::mapper::BeaconBlockMapper;
use crate::beacon::proto::beacon as beacon_pb;
use crate::bitcoin::mapper::BitcoinBlockMapper;
use crate::cosmos::mapper::CosmosBlockMapper;
use crate::evm::mapper::EvmBlockMapper;
use crate::evm::proto::eth;
use crate::near::mapper::NearBlockMapper;
use crate::solana::mapper::SolanaBlockMapper;
use crate::tron::mapper::TronBlockMapper;
use crate::{antelope, beacon, bitcoin, cosmos, evm, near, solana, tron};

const BLOCK_NUM: u64 = 100;
const TIMESTAMP: i64 = 1_700_000_000;

/// Every `EncodeBytes` variant. Mappers accept any of them, so all are covered
/// rather than only the per-chain defaults.
fn all_encodings() -> Vec<EncodeBytes> {
    let encodings = vec![
        EncodeBytes::Binary,
        EncodeBytes::Hex,
        EncodeBytes::HexNoPrefix,
        EncodeBytes::Base58,
        EncodeBytes::TronBase58,
    ];
    // Exhaustive on purpose: a new variant stops this compiling, as a reminder
    // to add it to the list above.
    for encoding in &encodings {
        match encoding {
            EncodeBytes::Binary
            | EncodeBytes::Hex
            | EncodeBytes::HexNoPrefix
            | EncodeBytes::Base58
            | EncodeBytes::TronBase58 => {}
        }
    }
    encodings
}

// ---------------------------------------------------------------------------
// Fixtures: the mapper test blocks, extended so that every table gets rows.
// ---------------------------------------------------------------------------

fn beacon_block_with_slashings() -> beacon_pb::Block {
    let header = |slot| beacon_pb::SignedBeaconBlockHeader {
        message: Some(beacon_pb::BeaconBlockHeader {
            slot,
            proposer_index: 7,
            parent_root: vec![0x01; 32],
            state_root: vec![0x02; 32],
            body_root: vec![0x03; 32],
        }),
        signature: vec![0x04; 96],
    };
    let attestation = |epoch| beacon_pb::IndexedAttestation {
        attesting_indices: vec![1, 2],
        data: Some(beacon_pb::AttestationData {
            slot: BLOCK_NUM,
            committee_index: 1,
            beacon_block_root: vec![0x05; 32],
            source: Some(beacon_pb::Checkpoint {
                epoch,
                root: vec![0x06; 32],
            }),
            target: Some(beacon_pb::Checkpoint {
                epoch: epoch + 1,
                root: vec![0x07; 32],
            }),
        }),
        signature: vec![0x08; 96],
    };

    let mut block = beacon::mapper::tests::make_test_block(BLOCK_NUM);
    let Some(beacon_pb::block::Body::Phase0(body)) = block.body.as_mut() else {
        panic!("beacon fixture should have a Phase0 body");
    };
    body.proposer_slashings.push(beacon_pb::ProposerSlashing {
        signed_header_1: Some(header(BLOCK_NUM)),
        signed_header_2: Some(header(BLOCK_NUM)),
    });
    body.attester_slashings.push(beacon_pb::AttesterSlashing {
        attestation_1: Some(attestation(10)),
        attestation_2: Some(attestation(10)),
    });
    block
}

#[allow(deprecated)] // `Call::account_creations` is deprecated upstream but still mapped.
fn evm_block_with_every_table() -> eth::Block {
    let mut block = evm::mapper::tests::make_test_evm_block(BLOCK_NUM);
    let call = &mut block.transaction_traces[0].calls[0];
    call.code_changes.push(eth::CodeChange {
        address: vec![0xaa; 20],
        old_hash: vec![0x01; 32],
        old_code: vec![],
        new_hash: vec![0x02; 32],
        new_code: vec![0x60, 0x00],
        ordinal: 4,
    });
    call.storage_changes.push(eth::StorageChange {
        address: vec![0xaa; 20],
        key: vec![0x03; 32],
        old_value: vec![0x00; 32],
        new_value: vec![0x04; 32],
        ordinal: 6,
    });
    call.account_creations.push(eth::AccountCreation {
        account: vec![0xaa; 20],
        ordinal: 7,
    });
    // A system call carrying one of each state change feeds every system_* table.
    let system_call = call.clone();
    block.system_calls.push(system_call);
    block.withdrawals.push(eth::Withdrawal {
        index: 1,
        validator_index: 2,
        address: vec![0xaa; 20],
        amount: 3,
    });
    let tx = &mut block.transaction_traces[0];
    tx.access_list.push(eth::AccessTuple {
        address: vec![0xaa; 20],
        storage_keys: vec![vec![0x05; 32]],
    });
    tx.set_code_authorizations
        .push(evm::mapper::tests::make_test_set_code_authorization());
    block
}

fn solana_block_with_vote() -> Vec<u8> {
    let vote_program = decode_base58("Vote111111111111111111111111111111111111111")
        .expect("vote program id should be valid base58");
    let mut block = solana::mapper::tests::make_test_block(BLOCK_NUM);
    let mut vote = block.transactions[0].clone();
    let tx = vote.transaction.as_mut().expect("fixture transaction");
    tx.signatures = vec![vec![9u8; 64]];
    tx.message.as_mut().expect("fixture message").account_keys[1] = vote_program;
    block.transactions.push(vote);
    block.encode_to_vec()
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/// One mapper configuration and the fixture blocks it maps before a flush.
struct Case {
    label: String,
    mapper: Box<dyn BlockMapper>,
    blocks: Vec<Vec<u8>>,
}

impl Case {
    fn new(
        label: impl Into<String>,
        mapper: impl BlockMapper + 'static,
        blocks: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            label: label.into(),
            mapper: Box::new(mapper),
            blocks,
        }
    }
}

/// Every chain mapper, with each option that changes the set of tables or
/// their schemas. Failed transactions are included so that no fixture row is
/// filtered out.
fn cases(encoding: &EncodeBytes, fork_step: bool) -> Vec<Case> {
    let enc = || encoding.clone();
    let mut cases = vec![
        Case::new(
            "antelope",
            AntelopeBlockMapper::new(fork_step, enc(), true),
            vec![antelope::mapper::tests::make_test_block(BLOCK_NUM as u32).encode_to_vec()],
        ),
        Case::new(
            "beacon",
            BeaconBlockMapper::new(fork_step, enc()),
            vec![
                beacon_block_with_slashings().encode_to_vec(),
                beacon::mapper::tests::make_deneb_block(BLOCK_NUM + 1).encode_to_vec(),
            ],
        ),
        Case::new(
            "bitcoin",
            BitcoinBlockMapper::new(fork_step, enc()),
            vec![bitcoin::mapper::tests::make_test_block(BLOCK_NUM as i64).encode_to_vec()],
        ),
        Case::new(
            "cosmos",
            CosmosBlockMapper::new(fork_step, enc(), true),
            vec![cosmos::mapper::tests::make_test_block(BLOCK_NUM as i64).encode_to_vec()],
        ),
        Case::new(
            "near",
            NearBlockMapper::new(fork_step, enc(), true),
            vec![
                near::mapper::tests::make_test_block(BLOCK_NUM).encode_to_vec(),
                // Every receipt action kind, a data receipt, a failed
                // transaction and receipts without a same-block origin, so the
                // nullable columns hold nulls.
                near::mapper::tests::make_every_action_block(BLOCK_NUM + 1).encode_to_vec(),
            ],
        ),
        Case::new(
            "tron",
            TronBlockMapper::new(fork_step, enc(), true),
            vec![tron::mapper::tests::make_test_block(BLOCK_NUM).encode_to_vec()],
        ),
    ];
    for extended in [false, true] {
        cases.push(Case::new(
            format!("evm extended={extended}"),
            EvmBlockMapper::new(extended, fork_step, enc(), true),
            vec![evm_block_with_every_table().encode_to_vec()],
        ));
    }
    for with_votes in [false, true] {
        for synthetic_routing in [false, true] {
            cases.push(Case::new(
                format!("solana with_votes={with_votes} synthetic_routing={synthetic_routing}"),
                SolanaBlockMapper::new(with_votes, fork_step, enc(), synthetic_routing, true),
                vec![solana_block_with_vote()],
            ));
        }
    }
    cases
}

fn identity(block_num: u64, fork_step: Option<&str>) -> BlockIdentity {
    BlockIdentity {
        block_num,
        block_id: format!("0x{block_num:064x}"),
        parent_num: block_num - 1,
        parent_id: format!("0x{:064x}", block_num - 1),
        lib_num: block_num - 1,
        timestamp: TIMESTAMP + (block_num - BLOCK_NUM) as i64,
        timestamp_nanos: 250_000_000,
        fork_step: fork_step.map(str::to_string),
    }
}

/// The output of one case: a context label and one flushed batch per table.
struct Flushed {
    context: String,
    include_fork_step: bool,
    batches: HashMap<String, RecordBatch>,
}

/// Map and flush every case, for every encoding and both fork_step settings.
/// Also checks that each mapper flushes exactly the tables it declares.
fn flush_all_cases() -> Vec<Flushed> {
    let mut flushed = Vec::new();
    for encoding in all_encodings() {
        for include_fork_step in [false, true] {
            let fork_step = include_fork_step.then_some("NEW");
            for mut case in cases(&encoding, include_fork_step) {
                let context = format!(
                    "{} encoding={encoding:?} fork_step={include_fork_step}",
                    case.label
                );
                for (offset, block) in case.blocks.iter().enumerate() {
                    let identity = identity(BLOCK_NUM + offset as u64, fork_step);
                    case.mapper
                        .map_block(block, &identity, fork_step)
                        .unwrap_or_else(|err| panic!("{context}: map_block failed: {err:#}"));
                }

                let declared: BTreeSet<String> = case
                    .mapper
                    .table_names()
                    .into_iter()
                    .map(str::to_string)
                    .collect();
                let batches = case
                    .mapper
                    .flush()
                    .unwrap_or_else(|err| panic!("{context}: flush failed: {err:#}"));
                let tables: BTreeSet<String> = batches.keys().cloned().collect();
                assert_eq!(tables, declared, "{context}: flushed tables");

                flushed.push(Flushed {
                    context,
                    include_fork_step,
                    batches,
                });
            }
        }
    }
    flushed
}

/// Number of (case, table) pairs `flush_all_cases` should produce, so that a
/// chain or option silently dropping out of `cases` fails the tests.
fn expected_table_count() -> usize {
    let per_run = antelope::schema::TABLE_NAMES.len()
        + beacon::schema::TABLE_NAMES.len()
        + bitcoin::schema::TABLE_NAMES.len()
        + cosmos::schema::TABLE_NAMES.len()
        + near::schema::TABLE_NAMES.len()
        + tron::schema::TABLE_NAMES.len()
        + evm::schema::BASE_TABLE_NAMES.len()
        + evm::schema::EXTENDED_TABLE_NAMES.len()
        + 2 * solana::schema::BASE_TABLE_NAMES.len()
        + 2 * solana::schema::WITH_VOTES_TABLE_NAMES.len();
    per_run * all_encodings().len() * 2
}

fn sorted_tables(batches: &HashMap<String, RecordBatch>) -> Vec<(&String, &RecordBatch)> {
    let mut tables: Vec<_> = batches.iter().collect();
    tables.sort_by_key(|(table, _)| *table);
    tables
}

fn duplicate_field_names(batch: &RecordBatch) -> Vec<String> {
    let mut seen = HashSet::new();
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| !seen.insert(name.clone()))
        .collect()
}

/// Write `batch` with the production table writer, then read it back.
fn parquet_round_trip(dir: &Path, table: &str, batch: &RecordBatch) -> RecordBatch {
    let mut writer = ParquetTableWriter::new(dir, Partition::None, Compression::Zstd);
    let metadata = BlockMetadata {
        min_block_number: BLOCK_NUM,
        max_block_number: BLOCK_NUM,
        min_timestamp: Some(TIMESTAMP),
        max_timestamp: Some(TIMESTAMP),
    };
    let (path, _) = writer
        .write_batch(table, batch, &metadata)
        .expect("write parquet");
    let batches = read_parquet(&path).expect("read parquet");
    concat_batches(&batches[0].schema(), &batches).expect("concat read batches")
}

fn display_values(batch: &RecordBatch, column: &str) -> BTreeSet<String> {
    let array = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("missing column {column}"));
    (0..array.len())
        .map(|row| array_value_to_string(array, row).expect("display value"))
        .collect()
}

/// Removes the scratch directory even when an assertion fails.
struct ScratchDir(PathBuf);

impl ScratchDir {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        Self(std::env::temp_dir().join(format!("fireparq-schema-contract-{nanos}")))
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn every_table_schema_has_unique_field_names_and_round_trips_through_parquet() {
    let scratch = ScratchDir::new();
    let mut checked = 0;

    for (case_index, flushed) in flush_all_cases().iter().enumerate() {
        let case_dir = scratch.0.join(case_index.to_string());
        for (table, batch) in sorted_tables(&flushed.batches) {
            let context = format!("{} table={table}", flushed.context);

            let duplicates = duplicate_field_names(batch);
            assert!(
                duplicates.is_empty(),
                "{context}: duplicate column names {duplicates:?}"
            );
            assert_eq!(
                batch.schema().column_with_name("fork_step").is_some(),
                flushed.include_fork_step,
                "{context}: fork_step column"
            );
            assert_eq!(
                batch
                    .schema()
                    .field_with_name("timestamp")
                    .unwrap()
                    .data_type(),
                &timestamp_millis_utc_type(),
                "{context}: canonical timestamp type"
            );
            // ParquetTableWriter skips empty batches, so every table needs rows.
            assert!(batch.num_rows() > 0, "{context}: fixture produced no rows");

            let read = parquet_round_trip(&case_dir, table, batch);
            assert_eq!(
                read.schema().fields(),
                batch.schema().fields(),
                "{context}: schema changed through parquet"
            );
            assert_eq!(&read, batch, "{context}: values changed through parquet");
            checked += 1;
        }
    }

    assert_eq!(checked, expected_table_count());
}

#[test]
fn every_table_uses_the_blocks_table_canonical_ids() {
    let mut checked = 0;

    for flushed in flush_all_cases() {
        let blocks = &flushed.batches["blocks"];
        let block_ids = display_values(blocks, "block_id");
        let parent_ids = display_values(blocks, "parent_id");
        for (table, batch) in sorted_tables(&flushed.batches) {
            let context = format!("{} table={table}", flushed.context);
            for (column, expected) in [("block_id", &block_ids), ("parent_id", &parent_ids)] {
                assert_eq!(
                    batch.schema().field_with_name(column).unwrap().data_type(),
                    blocks.schema().field_with_name(column).unwrap().data_type(),
                    "{context}: {column} type differs from the blocks table"
                );
                let values = display_values(batch, column);
                assert!(
                    values.is_subset(expected),
                    "{context}: {column} values {values:?} are not in the blocks table {expected:?}"
                );
            }
            checked += 1;
        }
    }

    assert_eq!(checked, expected_table_count());
}
