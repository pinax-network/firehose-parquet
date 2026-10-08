//! Cross-chain schema contract.
//!
//! Every table of every chain, under every bytes encoding and both
//! `fork_step` settings, must have unique column names and round-trip through
//! the Parquet writer and reader unchanged. Non-final tables also carry the
//! envelope's `stream_ordinal` on every row, directly after `fork_step`. Arrow and the Parquet writer accept
//! duplicate names silently, but Spark and Polars reject such files, DuckDB
//! renames the second column (`timestamp_1`), and `Schema::index_of` only ever
//! finds the first one.
//!
//! The same tables, mapped onto their Delta data file types at the flush
//! boundary (#643, `firehose_parquet::delta::types`), must hold only Delta
//! types, leave out the `date` partition column, keep every value, store
//! exactly the chain's `decimal(20,0)` columns as decimals, and round-trip
//! through Parquet too.

use std::collections::{BTreeSet, HashMap, HashSet};

use arrow::array::{Array, Int64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use firehose_parquet::config::{BlockMetadata, Compression};
use firehose_parquet::date_partition::DatePartition;
use firehose_parquet::delta::types::{is_delta_type, PARTITION_COLUMN};
use firehose_parquet::encode::{decode_base58, EncodeBytes};
use firehose_parquet::traits::{
    timestamp_micros_utc_type, timestamp_millis_utc_type, BlockIdentity, BlockMapper, StreamEvent,
};
use firehose_parquet::writer::{decode_parquet, encode_parquet, ParquetFileMetadata};
use prost::Message;

use crate::antelope::mapper::AntelopeBlockMapper;
use crate::beacon::mapper::BeaconBlockMapper;
use crate::beacon::proto::beacon as beacon_pb;
use crate::bitcoin::mapper::BitcoinBlockMapper;
use crate::chain::ChainKind;
use crate::cosmos::mapper::CosmosBlockMapper;
use crate::evm::mapper::EvmBlockMapper;
use crate::evm::proto::eth;
use crate::hypercore::mapper::HypercoreBlockMapper;
use crate::near::mapper::NearBlockMapper;
use crate::sec::mapper::SecBlockMapper;
use crate::solana::mapper::SolanaBlockMapper;
use crate::tron::mapper::TronBlockMapper;
use crate::{antelope, beacon, bitcoin, cosmos, evm, hypercore, near, sec, solana, tron};

const BLOCK_NUM: u64 = 100;
const TIMESTAMP: i64 = 1_700_000_000;
/// The accepted-event ordinal of the fixture block `BLOCK_NUM + offset`,
/// deliberately unrelated to its block number.
const FIRST_STREAM_ORDINAL: u64 = 7_000;

fn stream_ordinal(block_num: u64) -> u64 {
    FIRST_STREAM_ORDINAL + (block_num - BLOCK_NUM)
}

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
            parent_root: vec![0x01; 32].into(),
            state_root: vec![0x02; 32].into(),
            body_root: vec![0x03; 32].into(),
        }),
        signature: vec![0x04; 96].into(),
    };
    let attestation = |epoch| beacon_pb::IndexedAttestation {
        attesting_indices: vec![1, 2],
        data: Some(beacon_pb::AttestationData {
            slot: BLOCK_NUM,
            committee_index: 1,
            beacon_block_root: vec![0x05; 32].into(),
            source: Some(beacon_pb::Checkpoint {
                epoch,
                root: vec![0x06; 32].into(),
            }),
            target: Some(beacon_pb::Checkpoint {
                epoch: epoch + 1,
                root: vec![0x07; 32].into(),
            }),
        }),
        signature: vec![0x08; 96].into(),
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
        address: vec![0xaa; 20].into(),
        old_hash: vec![0x01; 32].into(),
        old_code: vec![].into(),
        new_hash: vec![0x02; 32].into(),
        new_code: vec![0x60, 0x00].into(),
        ordinal: 4,
    });
    call.storage_changes.push(eth::StorageChange {
        address: vec![0xaa; 20].into(),
        key: vec![0x03; 32].into(),
        old_value: vec![0x00; 32].into(),
        new_value: vec![0x04; 32].into(),
        ordinal: 6,
    });
    call.account_creations.push(eth::AccountCreation {
        account: vec![0xaa; 20].into(),
        ordinal: 7,
    });
    // A system call carrying one of each state change feeds every system_* table.
    let system_call = call.clone();
    block.system_calls.push(system_call);
    block.withdrawals.push(eth::Withdrawal {
        index: 1,
        validator_index: 2,
        address: vec![0xaa; 20].into(),
        amount: 3,
    });
    let tx = &mut block.transaction_traces[0];
    tx.access_list.push(eth::AccessTuple {
        address: vec![0xaa; 20].into(),
        storage_keys: vec![vec![0x05; 32].into()],
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
    tx.signatures = vec![vec![9u8; 64].into()];
    let message = tx.message.as_mut().expect("fixture message");
    message.account_keys[1] = vote_program.into();
    message.versioned = false;
    message.address_table_lookups.clear();
    message.instructions[0].data =
        bincode::serialize(&solana_vote_interface::instruction::VoteInstruction::Vote(
            solana_vote_interface::state::Vote {
                slots: vec![BLOCK_NUM],
                ..Default::default()
            },
        ))
        .expect("serialize canonical vote fixture")
        .into();
    block.transactions.push(vote);
    block.encode_to_vec()
}

/// Synthetic HyperCore blocks: the 36 real fixtures with their headers
/// rewritten to this harness's identities (`BLOCK_NUM + offset`, `TIMESTAMP +
/// offset` seconds, 250 ms), and the funding block cut to 8 fills and 3 deltas
/// per funding event. The mapper refuses a header that differs from the
/// Firehose identity and ignores the metadata's hex ids. Together they give
/// every table rows and every event body and ledger delta.
fn hypercore_blocks() -> Vec<Vec<u8>> {
    static BLOCKS: std::sync::OnceLock<Vec<Vec<u8>>> = std::sync::OnceLock::new();
    BLOCKS
        .get_or_init(|| hypercore::fixtures::derived_blocks(BLOCK_NUM, TIMESTAMP, 250_000_000))
        .clone()
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/// One mapper configuration and the fixture blocks it maps before a flush.
struct Case {
    kind: ChainKind,
    label: String,
    mapper: Box<dyn BlockMapper>,
    blocks: Vec<Vec<u8>>,
}

impl Case {
    fn new(
        kind: ChainKind,
        label: impl Into<String>,
        mapper: impl BlockMapper + 'static,
        blocks: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            kind,
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
            ChainKind::Antelope,
            "antelope",
            AntelopeBlockMapper::new(fork_step, enc(), true),
            vec![antelope::mapper::tests::make_test_block(BLOCK_NUM as u32).encode_to_vec()],
        ),
        Case::new(
            ChainKind::Beacon,
            "beacon",
            BeaconBlockMapper::new(fork_step, enc()),
            vec![
                beacon_block_with_slashings().encode_to_vec(),
                beacon::mapper::tests::make_deneb_block(BLOCK_NUM + 1).encode_to_vec(),
                // Execution requests and committee bits only exist from Electra.
                beacon::mapper::tests::make_electra_block(BLOCK_NUM + 2).encode_to_vec(),
            ],
        ),
        Case::new(
            ChainKind::Bitcoin,
            "bitcoin",
            BitcoinBlockMapper::new(fork_step, enc()),
            vec![bitcoin::mapper::tests::make_test_block(BLOCK_NUM as i64).encode_to_vec()],
        ),
        Case::new(
            ChainKind::Cosmos,
            "cosmos",
            CosmosBlockMapper::new(fork_step, enc(), true),
            vec![cosmos::mapper::tests::make_test_block(BLOCK_NUM as i64).encode_to_vec()],
        ),
        Case::new(
            ChainKind::Near,
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
            ChainKind::Sec,
            "sec",
            SecBlockMapper::new(fork_step, enc()),
            vec![
                // Every body kind and repeated child, a non-empty raw_xml and
                // a parse issue: all 43 tables get rows.
                sec::mapper::tests::make_every_body_block(BLOCK_NUM, TIMESTAMP).encode_to_vec(),
                // An empty 10-minute window: only its `blocks` row.
                sec::mapper::tests::make_test_block(BLOCK_NUM + 1, TIMESTAMP + 1).encode_to_vec(),
            ],
        ),
        Case::new(
            ChainKind::Tron,
            "tron",
            TronBlockMapper::new(fork_step, enc(), true),
            vec![tron::mapper::tests::make_test_block(BLOCK_NUM).encode_to_vec()],
        ),
        Case::new(
            ChainKind::Hypercore,
            "hypercore",
            HypercoreBlockMapper::new(fork_step, enc()),
            hypercore_blocks(),
        ),
    ];
    for extended in [false, true] {
        cases.push(Case::new(
            ChainKind::Evm,
            format!("evm extended={extended}"),
            EvmBlockMapper::new(extended, fork_step, enc(), true),
            vec![evm_block_with_every_table().encode_to_vec()],
        ));
    }
    for with_votes in [false, true] {
        for synthetic_routing in [false, true] {
            cases.push(Case::new(
                ChainKind::Solana,
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
    kind: ChainKind,
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
                    let block_num = BLOCK_NUM + offset as u64;
                    let identity = identity(block_num, fork_step);
                    case.mapper
                        .map_block(
                            block,
                            &identity,
                            StreamEvent::new(fork_step, stream_ordinal(block_num)),
                        )
                        .unwrap_or_else(|err| panic!("{context}: map_block failed: {err:#}"));
                }

                let declared: BTreeSet<String> = case
                    .mapper
                    .table_names()
                    .into_iter()
                    .map(str::to_string)
                    .collect();
                let estimates: HashMap<String, usize> = case
                    .mapper
                    .table_estimates()
                    .into_iter()
                    .map(|(name, bytes)| (name.to_owned(), bytes))
                    .collect();
                let batches = case
                    .mapper
                    .flush()
                    .unwrap_or_else(|err| panic!("{context}: flush failed: {err:#}"));
                let tables: BTreeSet<String> = batches.keys().cloned().collect();
                assert_eq!(tables, declared, "{context}: flushed tables");
                for (table, batch) in &batches {
                    if batch.num_rows() > 0 {
                        assert!(
                            estimates.get(table).is_some_and(|bytes| *bytes > 0),
                            "{context}: nonempty {table} is missing from memory estimates"
                        );
                    }
                }
                assert!(
                    estimates.values().sum::<usize>()
                        > estimates.values().copied().max().unwrap_or_default(),
                    "{context}: the complete buffer estimate must include multiple tables"
                );

                flushed.push(Flushed {
                    kind: case.kind,
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
        + sec::schema::TABLE_NAMES.len()
        + tron::schema::TABLE_NAMES.len()
        + hypercore::schema::TABLE_NAMES.len()
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

/// Encode `batch` as an ingestion part is encoded, then read it back, with
/// the file's Arrow schema. A record batch reader leaves schema metadata out
/// of its batches (HyperCore's derivation version); the footer's Arrow schema
/// keeps it, as protected verification reads it.
fn parquet_round_trip(batch: &RecordBatch) -> RecordBatch {
    let bytes = encode_parquet(batch, Compression::Zstd, &ParquetFileMetadata::new())
        .expect("encode parquet");
    let footer_schema = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        prost::bytes::Bytes::from(bytes.clone()),
    )
    .expect("read the footer")
    .schema()
    .clone();
    let batches = decode_parquet(bytes).expect("read parquet");
    concat_batches(&batches[0].schema(), &batches)
        .expect("concat read batches")
        .with_schema(footer_schema)
        .expect("the footer schema describes the rows")
}

fn display_values(batch: &RecordBatch, column: &str) -> BTreeSet<String> {
    let array = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("missing column {column}"));
    (0..array.len())
        .map(|row| array_value_to_string(array, row).expect("display value"))
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn every_table_schema_has_unique_field_names_and_round_trips_through_parquet() {
    let mut checked = 0;

    for flushed in &flush_all_cases() {
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
            assert_stream_ordinals(&context, batch, flushed.include_fork_step);
            assert_eq!(
                batch
                    .schema()
                    .field_with_name("timestamp")
                    .unwrap()
                    .data_type(),
                &timestamp_millis_utc_type(),
                "{context}: canonical timestamp type"
            );
            // Every table needs rows: a table without rows gets no part.
            assert!(batch.num_rows() > 0, "{context}: fixture produced no rows");

            let read = parquet_round_trip(batch);
            assert_eq!(
                read.schema().fields(),
                batch.schema().fields(),
                "{context}: schema changed through parquet"
            );
            // Authority stores this digest, so Arrow's schema equality alone
            // is insufficient: IPC dictionary IDs can change without affecting
            // Field equality or data, and must not change a protected identity.
            assert_eq!(
                firehose_parquet::writer::protected::schema_sha256(read.schema().as_ref()).unwrap(),
                firehose_parquet::writer::protected::schema_sha256(batch.schema().as_ref())
                    .unwrap(),
                "{context}: protected schema identity changed through parquet"
            );
            assert_eq!(&read, batch, "{context}: values changed through parquet");
            checked += 1;
        }
    }

    assert_eq!(checked, expected_table_count());
}

/// The flush metadata of the fixture blocks: every case routes to the UTC day
/// of [`TIMESTAMP`].
fn fixture_metadata() -> BlockMetadata {
    BlockMetadata {
        min_block_number: BLOCK_NUM,
        max_block_number: BLOCK_NUM + 2,
        min_timestamp: Some(TIMESTAMP),
        max_timestamp: Some(TIMESTAMP + 2),
    }
}

/// The Delta type a mapper type must become (`docs/design/delta-lake.md` §6),
/// written out independently of `firehose_parquet::delta::types`.
fn expected_delta_type(source: &DataType, decimal: bool) -> DataType {
    use arrow::datatypes::{Field, TimeUnit};
    let item = |field: &Field| {
        Field::new(
            field.name(),
            expected_delta_type(field.data_type(), decimal),
            field.is_nullable(),
        )
    };
    match source {
        DataType::UInt64 if decimal => DataType::Decimal128(20, 0),
        DataType::UInt64 | DataType::UInt32 | DataType::UInt16 => DataType::Int64,
        DataType::UInt8 => DataType::Int16,
        DataType::Dictionary(_, value) if **value == DataType::Utf8 => DataType::Utf8,
        DataType::Timestamp(TimeUnit::Millisecond, zone) => {
            DataType::Timestamp(TimeUnit::Microsecond, zone.clone())
        }
        DataType::List(field) => DataType::List(std::sync::Arc::new(item(field))),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(|f| item(f)).collect()),
        other => other.clone(),
    }
}

/// Whether `data_type` is, or holds, a `decimal(20,0)`.
fn holds_decimal(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal128(20, 0) => true,
        DataType::List(field) => holds_decimal(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| holds_decimal(f.data_type())),
        _ => false,
    }
}

/// #643: every table of every chain, under every encoding and both
/// `fork_step` settings, maps onto its Delta data file: only Delta types, the
/// `date` partition column left out, `timestamp` in microseconds, exactly the
/// profile's `decimal(20,0)` columns, every value kept, and a Parquet
/// round trip that changes neither the values nor the protected schema digest.
#[test]
fn every_table_maps_onto_delta_types_and_round_trips_through_parquet() {
    let partition = DatePartition::from_timestamp(TIMESTAMP).unwrap();
    let mut checked = 0;
    for flushed in &flush_all_cases() {
        let types = flushed.kind.profile().delta_types();
        let data = types
            .data_batches(flushed.batches.clone(), &fixture_metadata())
            .unwrap_or_else(|error| panic!("{}: {error:#}", flushed.context));
        for (table, batch) in sorted_tables(&flushed.batches) {
            let context = format!("{} table={table}", flushed.context);
            let mapped = &data[table];
            let schema = mapped.schema();
            // The batch conversion and the declared schema agree.
            assert_eq!(
                schema.as_ref(),
                &types.data_schema(table, batch.schema().as_ref()).unwrap(),
                "{context}: data schema"
            );
            assert_eq!(
                mapped,
                &types.data_batch(table, batch, Some(partition)).unwrap(),
                "{context}: one table alone maps the same"
            );
            // Every mapper column but `date`, in order, with its Delta type.
            let names = |schema: &arrow::datatypes::Schema| -> Vec<String> {
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .filter(|name| name != PARTITION_COLUMN)
                    .collect()
            };
            assert_eq!(names(&schema), names(&batch.schema()), "{context}: columns");
            assert!(
                schema.field_with_name(PARTITION_COLUMN).is_err(),
                "{context}: date is a partition column only"
            );
            for field in schema.fields() {
                let source = batch
                    .schema()
                    .field_with_name(field.name())
                    .unwrap()
                    .clone();
                let decimal = types.decimal_column(table, field.name()).is_some();
                assert!(
                    is_delta_type(field.data_type()),
                    "{context}: {} is {}",
                    field.name(),
                    field.data_type()
                );
                assert_eq!(
                    field.data_type(),
                    &expected_delta_type(source.data_type(), decimal),
                    "{context}: {} type",
                    field.name()
                );
                assert_eq!(
                    field.is_nullable(),
                    source.is_nullable(),
                    "{context}: {} nullability",
                    field.name()
                );
            }
            assert_eq!(
                schema.field_with_name("timestamp").unwrap().data_type(),
                &timestamp_micros_utc_type(),
                "{context}: timestamp"
            );
            assert_eq!(
                schema.field_with_name("block_num").unwrap().data_type(),
                &DataType::Int64,
                "{context}: block_num"
            );
            assert_eq!(
                schema
                    .field_with_name("stream_ordinal")
                    .ok()
                    .map(|f| f.data_type().clone()),
                flushed.include_fork_step.then_some(DataType::Int64),
                "{context}: stream_ordinal"
            );
            // Exactly the profile's decimal columns hold decimals.
            let decimals: BTreeSet<&str> = schema
                .fields()
                .iter()
                .filter(|f| holds_decimal(f.data_type()))
                .map(|f| f.name().as_str())
                .collect();
            let listed: BTreeSet<&str> = types
                .decimal_columns()
                .iter()
                .filter(|decimal| decimal.table == table.as_str())
                .map(|decimal| decimal.column)
                .collect();
            assert_eq!(decimals, listed, "{context}: decimal(20,0) columns");
            // Every value survives: timestamps as the same instant, everything
            // else with the same display (a decimal(20,0), a long and a u64 of
            // one value print alike, and so do a dictionary and its string).
            assert_eq!(mapped.num_rows(), batch.num_rows(), "{context}: rows");
            for field in schema.fields() {
                let before = batch.column_by_name(field.name()).unwrap();
                let after = mapped.column_by_name(field.name()).unwrap();
                if field.data_type() == &timestamp_micros_utc_type() {
                    let millis = arrow::compute::cast(before, &DataType::Int64).unwrap();
                    let micros = arrow::compute::cast(after, &DataType::Int64).unwrap();
                    let millis = millis.as_any().downcast_ref::<Int64Array>().unwrap();
                    let micros = micros.as_any().downcast_ref::<Int64Array>().unwrap();
                    for row in 0..mapped.num_rows() {
                        assert_eq!(
                            micros.is_valid(row).then(|| micros.value(row)),
                            millis.is_valid(row).then(|| millis.value(row) * 1_000),
                            "{context}: {} row {row}",
                            field.name()
                        );
                    }
                    continue;
                }
                for row in 0..mapped.num_rows() {
                    assert_eq!(
                        array_value_to_string(after, row).unwrap(),
                        array_value_to_string(before, row).unwrap(),
                        "{context}: {} row {row}",
                        field.name()
                    );
                }
            }

            let read = parquet_round_trip(mapped);
            assert_eq!(
                read.schema().fields(),
                schema.fields(),
                "{context}: Delta schema changed through parquet"
            );
            assert_eq!(
                firehose_parquet::writer::protected::schema_sha256(read.schema().as_ref()).unwrap(),
                firehose_parquet::writer::protected::schema_sha256(schema.as_ref()).unwrap(),
                "{context}: protected Delta schema identity changed through parquet"
            );
            assert_eq!(
                &read, mapped,
                "{context}: Delta values changed through parquet"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, expected_table_count());
}

/// #643: a value above `i64::MAX` in a checked `long` column refuses the
/// flush with the table, column and value named, while a `decimal(20,0)`
/// column keeps every `u64` exactly. EVM gas is bounded by the protocol, the
/// PoW block nonce is not.
#[test]
fn values_above_i64_max_are_refused_in_long_columns_and_kept_in_decimal_columns() {
    let types = ChainKind::Evm.profile().delta_types();
    let flush = |gas_used: u64| {
        let mut block = evm::mapper::tests::make_test_evm_block(BLOCK_NUM);
        let header = block.header.as_mut().unwrap();
        header.nonce = u64::MAX;
        header.gas_used = gas_used;
        let mut mapper = EvmBlockMapper::new(false, false, EncodeBytes::Hex, true);
        mapper
            .map_block(
                &block.encode_to_vec(),
                &identity(BLOCK_NUM, None),
                StreamEvent::new(None, 1),
            )
            .unwrap();
        types.data_batches(mapper.flush().unwrap(), &fixture_metadata())
    };
    let too_big = i64::MAX as u64 + 1;
    let error = format!("{:#}", flush(too_big).unwrap_err());
    assert!(
        error.contains(&format!(
            "table `blocks` column `gas_used`: value {too_big} does not fit the Delta type `long`"
        )) && error.contains("refused before anything was written")
            && error.contains("ChainProfile::decimal_columns"),
        "{error}"
    );
    let data = flush(i64::MAX as u64).unwrap();
    let blocks = &data["blocks"];
    let gas = blocks
        .column_by_name("gas_used")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(gas.value(0), i64::MAX);
    let nonce = blocks
        .column_by_name("nonce")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .unwrap();
    assert_eq!(nonce.value_as_string(0), "18446744073709551615");
}

/// Non-final tables carry `stream_ordinal` (`UInt64`, not null) directly after
/// `fork_step`, and every row holds the ordinal of the envelope that produced
/// it, the same value in every table. Final-only tables carry neither column.
fn assert_stream_ordinals(context: &str, batch: &RecordBatch, include_fork_step: bool) {
    let schema = batch.schema();
    let ordinal = schema.index_of("stream_ordinal").ok();
    assert_eq!(
        ordinal.is_some(),
        include_fork_step,
        "{context}: stream_ordinal column"
    );
    let Some(ordinal) = ordinal else {
        return;
    };
    assert_eq!(
        schema.index_of("fork_step").ok(),
        Some(ordinal - 1),
        "{context}: stream_ordinal must directly follow fork_step"
    );
    let field = schema.field(ordinal);
    assert_eq!(
        (field.data_type(), field.is_nullable()),
        (&DataType::UInt64, false),
        "{context}: stream_ordinal type"
    );
    let block_nums = batch
        .column_by_name("block_num")
        .and_then(|column| column.as_any().downcast_ref::<arrow::array::UInt64Array>())
        .unwrap_or_else(|| panic!("{context}: block_num"));
    let ordinals = batch
        .column(ordinal)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap_or_else(|| panic!("{context}: stream_ordinal is UInt64"));
    for row in 0..batch.num_rows() {
        assert_eq!(
            ordinals.value(row),
            stream_ordinal(block_nums.value(row)),
            "{context}: row {row} does not carry its envelope's ordinal"
        );
    }
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

#[test]
fn invalid_identity_timestamps_leave_every_chain_mapper_unchanged() {
    let encoding = EncodeBytes::Binary;
    let mut baseline = cases(&encoding, true);
    let mut subject = cases(&encoding, true);
    for (expected, actual) in baseline.iter_mut().zip(&mut subject) {
        let valid = identity(BLOCK_NUM, Some("NEW"));
        expected
            .mapper
            .map_block(
                &expected.blocks[0],
                &valid,
                StreamEvent::new(Some("NEW"), 1),
            )
            .unwrap();
        actual
            .mapper
            .map_block(&actual.blocks[0], &valid, StreamEvent::new(Some("NEW"), 1))
            .unwrap();
        for (timestamp, timestamp_nanos) in [
            (i64::MIN, 0),
            (i64::MAX, 0),
            (1_700_000_000_000, 0),
            (TIMESTAMP, -1),
            (TIMESTAMP, 1_000_000_000),
        ] {
            let invalid = BlockIdentity {
                timestamp,
                timestamp_nanos,
                ..valid.clone()
            };
            assert!(
                actual
                    .mapper
                    .map_block(
                        &actual.blocks[0],
                        &invalid,
                        StreamEvent::new(Some("UNDO"), 1)
                    )
                    .is_err(),
                "{} accepted malformed identity",
                actual.label
            );
        }
        assert_eq!(
            actual.mapper.flush().unwrap(),
            expected.mapper.flush().unwrap(),
            "{} changed after rejected identity",
            actual.label
        );
    }
}

#[test]
fn protected_dictionary_digest_preserves_existing_v1_evm_schema_identity() {
    // A real interrupted Writing transaction already recorded this identity.
    // Normalizing Parquet's assigned IPC IDs must not change the original
    // zero-ID mapper schema hash or make that transaction unrecoverable.
    assert_eq!(
        firehose_parquet::writer::protected::schema_sha256(&evm::schema::transactions_schema(
            false,
            &EncodeBytes::Hex
        ))
        .unwrap(),
        "44b18c11097fad9f240941e6c670cb01f04386d7a990a91ca644b935b01c2563"
    );
}

/// The owned payload path must preserve every mapper option, table, value,
/// schema and flush/reset behavior of the borrowed compatibility path.
#[test]
fn owned_payload_mapping_matches_borrowed_for_every_chain_and_encoding() {
    for encoding in all_encodings() {
        for include_fork_step in [false, true] {
            let fork_step = include_fork_step.then_some("NEW");
            for (mut borrowed, mut owned) in cases(&encoding, include_fork_step)
                .into_iter()
                .zip(cases(&encoding, include_fork_step))
            {
                let context = format!("{} {encoding:?} fork={include_fork_step}", owned.label);
                for _ in 0..2 {
                    for (offset, block) in borrowed.blocks.iter().enumerate() {
                        let block_num = BLOCK_NUM + offset as u64;
                        let identity = identity(block_num, fork_step);
                        let event = StreamEvent::new(fork_step, stream_ordinal(block_num));
                        let payload = prost::bytes::Bytes::from(block.clone());
                        let expected = borrowed.mapper.map_block(block, &identity, event).unwrap();
                        let actual = owned
                            .mapper
                            .map_block_bytes(payload.clone(), &identity, event)
                            .unwrap();
                        drop(payload); // Arrow output must outlive the original protobuf allocation.
                        assert_eq!(actual, expected, "{context}: mapped transaction count");
                    }
                    assert_eq!(
                        owned.mapper.total_rows(),
                        borrowed.mapper.total_rows(),
                        "{context}: buffered rows"
                    );
                    let expected = borrowed.mapper.flush().unwrap();
                    let actual = owned.mapper.flush().unwrap();
                    assert_eq!(actual, expected, "{context}: complete output after flush");
                }
                let identity = identity(BLOCK_NUM, fork_step);
                let event = StreamEvent::new(fork_step, stream_ordinal(BLOCK_NUM));
                let expected = borrowed
                    .mapper
                    .map_block(&[255], &identity, event)
                    .unwrap_err();
                let actual = owned
                    .mapper
                    .map_block_bytes(prost::bytes::Bytes::from_static(&[255]), &identity, event)
                    .unwrap_err();
                assert_eq!(
                    actual.to_string(),
                    expected.to_string(),
                    "{context}: malformed wire payload"
                );
            }
        }
    }
}

/// These pointer checks distinguish true shared-buffer decoding from a
/// byte-identical result that secretly allocated/copied each bytes field.
#[test]
fn owned_decoding_shares_nested_payload_storage_and_retains_its_lifetime() {
    use prost::bytes::Bytes;
    fn shared<M: Message + Default>(block: M, select: impl Fn(&M) -> &Bytes) {
        let encoded = block.encode_to_vec();
        let allocation = encoded.as_ptr();
        let payload = Bytes::from(encoded);
        assert_eq!(
            allocation,
            payload.as_ptr(),
            "Vec-to-Bytes ownership transfer copied"
        );
        let start = payload.as_ptr() as usize;
        let end = start + payload.len();
        let decoded = M::decode(payload.clone()).unwrap();
        let field = select(&decoded);
        assert!(!field.is_empty());
        let pointer = field.as_ptr() as usize;
        assert!(
            pointer >= start && pointer + field.len() <= end,
            "nested bytes did not share original allocation"
        );
        let expected = field.to_vec();
        let retained = field.clone();
        drop(payload);
        drop(decoded);
        assert_eq!(
            retained.as_ref(),
            expected,
            "field must keep the shared allocation alive"
        );
    }
    shared(evm_block_with_every_table(), |b| {
        &b.transaction_traces[0].calls[0].code_changes[0].new_code
    });
    shared(solana::mapper::tests::make_test_block(BLOCK_NUM), |b| {
        &b.transactions[0]
            .transaction
            .as_ref()
            .unwrap()
            .message
            .as_ref()
            .unwrap()
            .account_keys[0]
    });
    shared(beacon::mapper::tests::make_deneb_block(BLOCK_NUM), |b| {
        &b.root
    });
    shared(
        cosmos::mapper::tests::make_test_block(BLOCK_NUM as i64),
        |b| &b.txs[0],
    );
    shared(
        sec::mapper::tests::make_every_body_block(BLOCK_NUM, TIMESTAMP),
        |b| {
            &b.filings
                .iter()
                .find(|filing| !filing.raw_xml.is_empty())
                .expect("the SEC fixture has a raw_xml filing")
                .raw_xml
        },
    );
    let (_, busy_block) = hypercore::fixtures::real_blocks()
        .iter()
        .find(|(number, _)| *number == 1_165_601_237)
        .expect("HyperCore fixture 1165601237");
    shared(
        hypercore::proto::hypercore::Block::decode(busy_block.as_slice()).unwrap(),
        |b| &b.fills[0].hash,
    );
}
