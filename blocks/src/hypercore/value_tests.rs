//! HyperCore golden, round-trip, refusal and unit tests (schema epoch 1).
//!
//! The 36 real fixtures (`blocks/tests/fixtures/hypercore/`) are mapped with
//! their true identities under `hex` and `binary`:
//!
//! - T1 row counts, T2 label coverage, T3 pinned values;
//! - T4 a byte-exact rebuild of every payload from the five output tables;
//! - T5 the populated-column matrix of `docs/chains/hypercore.md`;
//! - T6 `extra_json` is NULL everywhere;
//! - T7 a pinned hash of the whole output, the release invariant.
//!
//! Then every refusal rule (R1–R11) with its error text, atomicity, the
//! empty/NULL rules and the enum labels.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::OnceLock;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Int32Type, Int64Type, TimeUnit, TimestampMillisecondType,
    UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{BlockIdentity, BlockMapper, StreamEvent};
use prost::bytes::Bytes;
use prost::Message;
use sha2::{Digest, Sha256};

use super::decimal::canonical_text;
use super::fixtures::{identity_of, real_blocks, FUNDING_BLOCK};
use super::mapper::HypercoreBlockMapper;
use super::proto::hypercore as pb;
use super::schema::TABLE_NAMES;
use pb::{event_body, ledger_update_delta};

type Tables = HashMap<String, RecordBatch>;

const ONE: i128 = 10_000_000_000;

// ---------------------------------------------------------------------------
// Mapping the fixtures
// ---------------------------------------------------------------------------

fn decode(payload: &[u8]) -> pb::Block {
    pb::Block::decode(payload).expect("decode fixture")
}

/// The unmodified payload of one fixture block.
fn payload(number: u64) -> &'static [u8] {
    &real_blocks()
        .iter()
        .find(|(block, _)| *block == number)
        .unwrap_or_else(|| panic!("no fixture {number}"))
        .1
}

fn fixture(number: u64) -> pb::Block {
    decode(payload(number))
}

/// Every fixture, in block order, through one mapper and one flush.
fn map_all(encoding: EncodeBytes) -> Tables {
    let mut mapper = HypercoreBlockMapper::new(false, encoding.clone());
    for (number, payload) in real_blocks() {
        let block = decode(payload);
        let identity = identity_of(&block);
        assert_eq!(identity.block_num, *number);
        let fills = mapper
            .map_block(payload, &identity, StreamEvent::default())
            .unwrap_or_else(|error| panic!("{encoding:?} {number}: {error:#}"));
        assert_eq!(fills, block.fills.len() as u64, "{number}");
    }
    mapper.flush().unwrap()
}

fn golden(encoding: &EncodeBytes) -> &'static Tables {
    static HEX: OnceLock<Tables> = OnceLock::new();
    static BINARY: OnceLock<Tables> = OnceLock::new();
    match encoding {
        EncodeBytes::Hex => HEX.get_or_init(|| map_all(EncodeBytes::Hex)),
        EncodeBytes::Binary => BINARY.get_or_init(|| map_all(EncodeBytes::Binary)),
        other => panic!("no golden output for {other:?}"),
    }
}

const GOLDEN_ENCODINGS: [EncodeBytes; 2] = [EncodeBytes::Hex, EncodeBytes::Binary];

/// Map one (possibly modified) block with its own header identity.
fn map_one(block: &pb::Block) -> anyhow::Result<Tables> {
    map_with(block, &identity_of(block), EncodeBytes::Hex)
}

fn map_with(
    block: &pb::Block,
    identity: &BlockIdentity,
    encoding: EncodeBytes,
) -> anyhow::Result<Tables> {
    let mut mapper = HypercoreBlockMapper::new(false, encoding);
    mapper.map_block(&block.encode_to_vec(), identity, StreamEvent::default())?;
    mapper.flush()
}

/// The full error text of a refused block.
fn refusal(block: &pb::Block) -> String {
    refusal_with(block, &identity_of(block))
}

fn refusal_with(block: &pb::Block, identity: &BlockIdentity) -> String {
    format!(
        "{:#}",
        map_with(block, identity, EncodeBytes::Hex)
            .err()
            .expect("the block should be refused")
    )
}

#[track_caller]
fn assert_refused(block: &pb::Block, expected: &str) {
    let error = refusal(block);
    let number = identity_of(block).block_num;
    assert_eq!(error, format!("hypercore block {number}: {expected}"));
}

// ---------------------------------------------------------------------------
// Reading the output
// ---------------------------------------------------------------------------

/// Typed access to one output table under one encoding.
struct View<'a> {
    batch: &'a RecordBatch,
    encoding: EncodeBytes,
}

impl<'a> View<'a> {
    fn new(tables: &'a Tables, table: &str, encoding: &EncodeBytes) -> Self {
        Self {
            batch: &tables[table],
            encoding: encoding.clone(),
        }
    }

    fn len(&self) -> usize {
        self.batch.num_rows()
    }

    fn column(&self, name: &str) -> &'a dyn Array {
        self.batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no column {name}"))
            .as_ref()
    }

    fn is_null(&self, name: &str, row: usize) -> bool {
        self.column(name).is_null(row)
    }

    fn u64(&self, name: &str, row: usize) -> Option<u64> {
        let column = self.column(name).as_primitive::<UInt64Type>();
        column.is_valid(row).then(|| column.value(row))
    }

    fn u32(&self, name: &str, row: usize) -> Option<u32> {
        let column = self.column(name).as_primitive::<UInt32Type>();
        column.is_valid(row).then(|| column.value(row))
    }

    fn i64(&self, name: &str, row: usize) -> Option<i64> {
        let column = self.column(name).as_primitive::<Int64Type>();
        column.is_valid(row).then(|| column.value(row))
    }

    fn ts_ms(&self, name: &str, row: usize) -> Option<i64> {
        let column = self.column(name).as_primitive::<TimestampMillisecondType>();
        column.is_valid(row).then(|| column.value(row))
    }

    fn bool(&self, name: &str, row: usize) -> Option<bool> {
        let column = self.column(name).as_boolean();
        column.is_valid(row).then(|| column.value(row))
    }

    fn dec(&self, name: &str, row: usize) -> Option<i128> {
        let column = self.column(name).as_primitive::<Decimal128Type>();
        column.is_valid(row).then(|| column.value(row))
    }

    /// A `Utf8` or `Dictionary(Int32, Utf8)` value.
    fn text(&self, name: &str, row: usize) -> Option<&'a str> {
        let column = self.column(name);
        if column.is_null(row) {
            return None;
        }
        Some(match column.data_type() {
            DataType::Utf8 => column.as_string::<i32>().value(row),
            DataType::Dictionary(_, _) => {
                let dictionary = column.as_dictionary::<Int32Type>();
                let key = dictionary.keys().value(row) as usize;
                dictionary.values().as_string::<i32>().value(key)
            }
            other => panic!("{name} is {other}"),
        })
    }

    /// A bytes value, decoded from the table's encoding.
    fn bytes(&self, name: &str, row: usize) -> Option<Vec<u8>> {
        let column = self.column(name);
        column
            .is_valid(row)
            .then(|| decode_bytes(column, row, &self.encoding))
    }

    fn bytes_list(&self, name: &str, row: usize) -> Option<Vec<Vec<u8>>> {
        let column = self.column(name).as_list::<i32>();
        column.is_valid(row).then(|| {
            let items = column.value(row);
            (0..items.len())
                .map(|item| decode_bytes(items.as_ref(), item, &self.encoding))
                .collect()
        })
    }

    fn positions(&self, name: &str, row: usize) -> Option<Vec<(String, i128)>> {
        let column = self.column(name).as_list::<i32>();
        column.is_valid(row).then(|| {
            let items = column.value(row);
            let items = items.as_struct();
            let coins = items.column(0).as_string::<i32>();
            let sizes = items.column(1).as_primitive::<Decimal128Type>();
            (0..items.len())
                .map(|item| (coins.value(item).to_string(), sizes.value(item)))
                .collect()
        })
    }
}

fn unhex(text: &str) -> Vec<u8> {
    let digits = text.strip_prefix("0x").expect("0x-prefixed hex");
    assert_eq!(digits, digits.to_ascii_lowercase(), "lowercase hex");
    (0..digits.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&digits[at..at + 2], 16).expect("hex digit"))
        .collect()
}

fn decode_bytes(column: &dyn Array, row: usize, encoding: &EncodeBytes) -> Vec<u8> {
    match encoding {
        EncodeBytes::Binary => column.as_binary::<i32>().value(row).to_vec(),
        EncodeBytes::Hex => unhex(column.as_string::<i32>().value(row)),
        other => panic!("no decoder for {other:?}"),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::from("0x");
    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// Rows of a table whose `block_num` is `number`.
fn rows_of(view: &View<'_>, number: u64) -> Vec<usize> {
    (0..view.len())
        .filter(|row| view.u64("block_num", *row) == Some(number))
        .collect()
}

// ---------------------------------------------------------------------------
// T1 row counts
// ---------------------------------------------------------------------------

/// `(block, fills, events, funding_deltas, validator_rewards)` per fixture.
const ROW_COUNTS: [(u64, usize, usize, usize, usize); 36] = [
    (846001240, 0, 3, 0, 0),
    (846903317, 1527, 7, 202449, 30),
    (847193990, 0, 2, 0, 0),
    (889872017, 0, 1, 0, 0),
    (895702803, 0, 2, 0, 0),
    (897888967, 2, 2, 0, 0),
    (987247825, 0, 3, 0, 31),
    (1009557224, 0, 2, 0, 0),
    (1009612597, 0, 1, 0, 0),
    (1009686466, 0, 1, 0, 0),
    (1009701302, 2, 0, 0, 0),
    (1009721907, 0, 2, 0, 0),
    (1009855075, 314, 0, 0, 0),
    (1009867965, 0, 1, 0, 0),
    (1009868295, 0, 1, 0, 0),
    (1009877929, 0, 1, 0, 0),
    (1009907496, 0, 1, 0, 0),
    (1009925229, 0, 1, 0, 0),
    (1009958482, 0, 1, 0, 0),
    (1010128732, 0, 1, 0, 0),
    (1010355937, 2, 0, 0, 0),
    (1010423738, 0, 1, 0, 0),
    (1010581248, 16, 0, 0, 0),
    (1075395014, 3, 0, 0, 0),
    (1075987296, 26, 1, 0, 0),
    (1078677210, 3, 0, 0, 0),
    (1110656252, 2, 0, 0, 0),
    (1127672017, 38, 12, 0, 0),
    (1165601237, 28, 1, 0, 0),
    (1173346041, 0, 1, 0, 0),
    (1173352606, 0, 1, 0, 0),
    (1173408840, 6, 1, 0, 0),
    (1173546257, 0, 3, 0, 0),
    (1173674198, 2, 1, 0, 0),
    (1173744709, 0, 1, 0, 0),
    (1173886256, 0, 1, 0, 0),
];

#[test]
fn t1_row_counts_per_fixture_and_in_total() {
    for encoding in &GOLDEN_ENCODINGS {
        let tables = golden(encoding);
        let count = |table: &str| tables[table].num_rows();
        assert_eq!(count("blocks"), 36, "{encoding:?}");
        assert_eq!(count("fills"), 1_971, "{encoding:?}");
        assert_eq!(count("events"), 57, "{encoding:?}");
        assert_eq!(count("funding_deltas"), 202_449, "{encoding:?}");
        assert_eq!(count("validator_rewards"), 61, "{encoding:?}");
        let per_block = |table: &str| {
            let view = View::new(tables, table, encoding);
            let mut counts: BTreeMap<u64, usize> = BTreeMap::new();
            for row in 0..view.len() {
                *counts
                    .entry(view.u64("block_num", row).unwrap())
                    .or_default() += 1;
            }
            counts
        };
        let (blocks_of, fills_of, events_of, deltas_of, rewards_of) = (
            per_block("blocks"),
            per_block("fills"),
            per_block("events"),
            per_block("funding_deltas"),
            per_block("validator_rewards"),
        );
        let blocks = View::new(tables, "blocks", encoding);
        for (number, fills, events, deltas, rewards) in ROW_COUNTS {
            let of = |counts: &BTreeMap<u64, usize>| counts.get(&number).copied().unwrap_or(0);
            assert_eq!(
                (
                    of(&blocks_of),
                    of(&fills_of),
                    of(&events_of),
                    of(&deltas_of),
                    of(&rewards_of)
                ),
                (1, fills, events, deltas, rewards),
                "{number}"
            );
            let row = rows_of(&blocks, number)[0];
            assert_eq!(blocks.u32("fill_count", row), Some(fills as u32));
            assert_eq!(blocks.u32("event_count", row), Some(events as u32));
        }
    }
}

/// Every row's canonical identity is the block's: decimal text ids, the
/// parent one below, and the header time truncated to milliseconds.
#[test]
fn canonical_identity_is_the_decimal_block_number() {
    for encoding in &GOLDEN_ENCODINGS {
        let tables = golden(encoding);
        for table in TABLE_NAMES {
            let view = View::new(tables, table, encoding);
            for row in 0..view.len() {
                let number = view.u64("block_num", row).unwrap();
                assert_eq!(view.u64("parent_num", row), Some(number - 1));
                assert_eq!(view.u64("lib_num", row), Some(number - 1));
                let id = |column: &str| match encoding {
                    EncodeBytes::Binary => {
                        view.column(column).as_binary::<i32>().value(row).to_vec()
                    }
                    _ => view.text(column, row).unwrap().as_bytes().to_vec(),
                };
                assert_eq!(id("block_id"), number.to_string().into_bytes(), "{table}");
                assert_eq!(id("parent_id"), (number - 1).to_string().into_bytes());
            }
        }
        let blocks = View::new(tables, "blocks", encoding);
        for row in 0..blocks.len() {
            let ns = blocks.i64("block_time_ns", row).unwrap();
            assert_eq!(
                blocks.ts_ms("timestamp", row),
                Some(ns.div_euclid(1_000_000))
            );
            let date = blocks
                .column("date")
                .as_primitive::<Date32Type>()
                .value(row);
            assert_eq!(i64::from(date), ns.div_euclid(86_400_000_000_000));
        }
    }
}

// ---------------------------------------------------------------------------
// T2 label coverage
// ---------------------------------------------------------------------------

fn label_counts(view: &View<'_>, column: &str) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for row in 0..view.len() {
        if let Some(label) = view.text(column, row) {
            *counts.entry(label.to_string()).or_default() += 1;
        }
    }
    counts
}

const EVENT_TYPES: [&str; 8] = [
    "ledger_update",
    "funding",
    "validator_rewards",
    "c_withdrawal",
    "c_deposit",
    "delegation",
    "gossip_priority_auction_restart",
    "create_sub_account",
];

const LEDGER_TYPES: [&str; 22] = [
    "spot_transfer",
    "c_staking_transfer",
    "account_class_transfer",
    "internal_transfer",
    "sub_account_transfer",
    "send",
    "deposit",
    "withdraw",
    "vault_deposit",
    "rewards_claim",
    "vault_withdraw",
    "vault_leader_commission",
    "deploy_gas_auction",
    "account_activation_gas",
    "activate_dex_abstraction",
    "liquidation",
    "spot_genesis",
    "vault_distribution",
    "borrow_lend",
    "vault_create",
    "gossip_priority_gas_auction",
    "hip3_liquidator_deposit",
];

/// `TradingDirection` values 1..=22 and their labels.
const DIRECTIONS: [&str; 22] = [
    "BUY",
    "SELL",
    "OPEN_LONG",
    "CLOSE_LONG",
    "OPEN_SHORT",
    "CLOSE_SHORT",
    "LONG_TO_SHORT",
    "SHORT_TO_LONG",
    "SPOT_DUST_CONVERSION",
    "LIQUIDATED_CROSS_LONG",
    "LIQUIDATED_CROSS_SHORT",
    "LIQUIDATED_ISOLATED_LONG",
    "LIQUIDATED_ISOLATED_SHORT",
    "AUTO_DELEVERAGING",
    "SETTLEMENT",
    "NET_CHILD_VAULTS",
    "BACKSTOP_BORROW_LIQUIDATION",
    "PARTIAL_BORROW_LIQUIDATION",
    "SPLIT_OUTCOME",
    "MERGE_OUTCOME",
    "MERGE_QUESTION",
    "NEGATE_OUTCOME",
];

#[test]
fn t2_fixtures_cover_every_body_delta_and_seen_direction() {
    let encoding = &EncodeBytes::Hex;
    let tables = golden(encoding);
    let events = View::new(tables, "events", encoding);
    let fills = View::new(tables, "fills", encoding);

    let event_types = label_counts(&events, "event_type");
    assert_eq!(
        event_types
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(EVENT_TYPES)
    );
    let ledger_types = label_counts(&events, "ledger_type");
    let expected: BTreeMap<String, usize> = LEDGER_TYPES
        .iter()
        .map(|label| {
            let count = match *label {
                "liquidation" => 12,
                "borrow_lend" => 4,
                "send" => 3,
                "c_staking_transfer" | "vault_distribution" | "withdraw" => 2,
                _ => 1,
            };
            (label.to_string(), count)
        })
        .collect();
    assert_eq!(ledger_types, expected);

    let directions = label_counts(&fills, "direction");
    let never_seen = [
        "LIQUIDATED_CROSS_SHORT",
        "BACKSTOP_BORROW_LIQUIDATION",
        "PARTIAL_BORROW_LIQUIDATION",
    ];
    assert_eq!(
        directions
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        DIRECTIONS
            .into_iter()
            .filter(|label| !never_seen.contains(label))
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(directions.len(), 19);
    assert_eq!(
        label_counts(&fills, "side")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["ASK", "BUY"]
    );
    assert_eq!(
        label_counts(&events, "leverage_type")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["CROSS", "ISOLATED"]
    );
}

// ---------------------------------------------------------------------------
// T3 pinned values
// ---------------------------------------------------------------------------

const ZERO_HASH: [u8; 32] = [0; 32];
const ZERO_ADDRESS: [u8; 20] = [0; 20];

#[test]
fn t3_pinned_values() {
    for encoding in &GOLDEN_ENCODINGS {
        let tables = golden(encoding);
        let fills = View::new(tables, "fills", encoding);
        let events = View::new(tables, "events", encoding);

        // The funding, dust-conversion and validator-rewards block.
        let funding_events = rows_of(&events, FUNDING_BLOCK);
        let item_counts: Vec<Option<u32>> = funding_events
            .iter()
            .map(|row| events.u32("item_count", *row))
            .collect();
        assert_eq!(item_counts, [189821, 9532, 564, 609, 1923, 0, 30].map(Some));
        let types: Vec<&str> = funding_events
            .iter()
            .map(|row| events.text("event_type", *row).unwrap())
            .collect();
        assert_eq!(types[..6], ["funding"; 6]);
        assert_eq!(types[6], "validator_rewards");
        let funding_fills = rows_of(&fills, FUNDING_BLOCK);
        let dust: Vec<usize> = funding_fills
            .iter()
            .copied()
            .filter(|row| fills.u64("transaction_id", *row) == Some(0))
            .collect();
        assert_eq!(dust.len(), 1_451);
        assert!(dust
            .iter()
            .all(|row| fills.text("direction", *row) == Some("SPOT_DUST_CONVERSION")));
        let zero_hash = |rows: &[usize]| {
            rows.iter()
                .filter(|row| fills.bytes("hash", **row).unwrap() == ZERO_HASH)
                .count()
        };
        assert_eq!(zero_hash(&funding_fills), 1_523);

        // A delisted-perp settlement against the zero address.
        let settlement = rows_of(&fills, 1_009_855_075);
        assert_eq!(
            settlement
                .iter()
                .filter(|row| fills.bytes("user", **row).unwrap() == ZERO_ADDRESS)
                .count(),
            157
        );

        // Gossip slots: a winner on slot 0, none on slot 1.
        let gossip: Vec<usize> = rows_of(&events, 987_247_825)
            .into_iter()
            .filter(|row| {
                events.text("event_type", *row) == Some("gossip_priority_auction_restart")
            })
            .collect();
        assert_eq!(gossip.len(), 2);
        assert_eq!(events.u64("slot_id", gossip[0]), Some(0));
        assert_eq!(
            events.text("previous_winner_ip", gossip[0]),
            Some("54.64.2.87")
        );
        assert_eq!(events.dec("end_gas", gossip[0]), Some(5_829_161_100));
        assert_eq!(events.u64("slot_id", gossip[1]), Some(1));
        assert_eq!(events.text("previous_winner_ip", gossip[1]), None);
        assert_eq!(events.dec("end_gas", gossip[1]), None);

        // The first sub-account creation the endpoint carries.
        let [created] = rows_of(&events, 1_173_744_709)[..] else {
            panic!("one create_sub_account event");
        };
        assert_eq!(
            events.text("event_type", created),
            Some("create_sub_account")
        );
        assert_eq!(events.text("sub_account_name", created), Some("test"));
        assert_eq!(
            hex(&events.bytes("sub_account", created).unwrap()),
            "0x19c57799fa7288fbaa8d53eef6462c88369c7315"
        );

        // A HIP-3 liquidator deposit, with TWAP slices among the fills.
        let [deposit] = rows_of(&events, 1_075_987_296)[..] else {
            panic!("one ledger event");
        };
        assert_eq!(
            events.text("ledger_type", deposit),
            Some("hip3_liquidator_deposit")
        );
        assert_eq!(events.text("dex", deposit), Some("xyz"));
        assert_eq!(events.text("token", deposit), Some("USDC"));
        assert_eq!(events.dec("amount", deposit), Some(999_000 * ONE));
        assert_eq!(
            events
                .bytes_list("users", deposit)
                .unwrap()
                .iter()
                .map(|user| hex(user))
                .collect::<Vec<_>>(),
            [
                "0xa2358d49f40d6bc6a50de137c9e73843e62c101a",
                "0x4000000000000000000000000000000000000001"
            ]
        );
        let deposit_fills = rows_of(&fills, 1_075_987_296);
        assert_eq!(
            deposit_fills
                .iter()
                .filter(|row| !fills.is_null("twap_id", **row))
                .count(),
            4
        );
        assert_eq!(zero_hash(&deposit_fills), 8);

        // A backstop liquidation cascade. (The schema specification's 38
        // liquidation fills and 19 liquidated sides are the totals over all
        // fixtures, checked below; this block has 24 and 12.)
        let cascade = rows_of(&fills, 1_127_672_017);
        let liquidations: Vec<usize> = cascade
            .iter()
            .copied()
            .filter(|row| !fills.is_null("liquidation_method", *row))
            .collect();
        assert_eq!(liquidations.len(), 24);
        assert!(liquidations
            .iter()
            .all(|row| fills.text("liquidation_method", *row) == Some("backstop")));
        assert_eq!(
            liquidations
                .iter()
                .filter(|row| fills.bytes("user", **row) == fills.bytes("liquidated_user", **row))
                .count(),
            12
        );
        let first = rows_of(&events, 1_127_672_017)[0];
        assert_eq!(events.text("ledger_type", first), Some("liquidation"));
        assert_eq!(events.text("leverage_type", first), Some("ISOLATED"));
        assert_eq!(
            events.dec("liquidated_ntl_pos", first),
            Some(18_327_954_330_000)
        );
        assert_eq!(events.dec("account_value", first), Some(-17_248_590_000));
        assert_eq!(
            events.positions("liquidated_positions", first),
            Some(vec![("TRUMP".to_string(), 6_867 * ONE / 10)])
        );

        // Non-null counts over every fill of every fixture.
        let non_null = |column: &str| {
            (0..fills.len())
                .filter(|row| !fills.is_null(column, *row))
                .count()
        };
        assert_eq!(non_null("builder"), 6);
        assert_eq!(non_null("builder_fee"), 4);
        assert_eq!(non_null("deployer_fee"), 29);
        assert_eq!(non_null("priority_gas"), 5);
        assert_eq!(non_null("client_order_id"), 84);
        assert_eq!(non_null("twap_id"), 5);
        assert_eq!(non_null("liquidation_method"), 38);
        let liquidated_sides = (0..fills.len())
            .filter(|row| {
                !fills.is_null("liquidated_user", *row)
                    && fills.bytes("user", *row) == fills.bytes("liquidated_user", *row)
            })
            .count();
        assert_eq!(liquidated_sides, 19);
        assert_eq!(
            label_counts(&fills, "liquidation_method"),
            BTreeMap::from([("backstop".to_string(), 30), ("market".to_string(), 8)])
        );
    }
}

// ---------------------------------------------------------------------------
// T4 byte-exact round trip
// ---------------------------------------------------------------------------

fn timestamp_from_ns(ns: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ns.div_euclid(1_000_000_000),
        nanos: ns.rem_euclid(1_000_000_000) as i32,
    }
}

fn timestamp_from_ms(ms: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ms.div_euclid(1_000),
        nanos: (ms.rem_euclid(1_000) * 1_000_000) as i32,
    }
}

/// The inverse rules of the mapping (`docs/chains/hypercore.md`, "Lossless").
struct Rebuild<'a> {
    view: View<'a>,
}

impl Rebuild<'_> {
    fn dec(&self, name: &str, row: usize) -> String {
        canonical_text(self.view.dec(name, row).expect("non-null decimal"))
    }

    /// NULL is the empty string.
    fn dec_or_empty(&self, name: &str, row: usize) -> String {
        self.view
            .dec(name, row)
            .map(canonical_text)
            .unwrap_or_default()
    }

    fn text(&self, name: &str, row: usize) -> String {
        self.view
            .text(name, row)
            .expect("non-null text")
            .to_string()
    }

    fn text_or_empty(&self, name: &str, row: usize) -> String {
        self.view.text(name, row).unwrap_or_default().to_string()
    }

    fn bytes(&self, name: &str, row: usize) -> Bytes {
        self.view.bytes(name, row).expect("non-null bytes").into()
    }

    fn bytes_or_empty(&self, name: &str, row: usize) -> Bytes {
        self.view.bytes(name, row).unwrap_or_default().into()
    }

    fn u64(&self, name: &str, row: usize) -> u64 {
        self.view.u64(name, row).expect("non-null u64")
    }

    fn bool(&self, name: &str, row: usize) -> bool {
        self.view.bool(name, row).expect("non-null bool")
    }
}

fn rebuild_fill(fills: &Rebuild<'_>, row: usize) -> pb::Fill {
    let view = &fills.view;
    let side = view.text("side", row).unwrap();
    let direction = view.text("direction", row).unwrap();
    pb::Fill {
        user: fills.bytes("user", row),
        coin: fills.text("coin", row),
        price: fills.dec("price", row),
        size: fills.dec("size", row),
        side: pb::FillSide::from_str_name(&format!("FILL_SIDE_{side}")).unwrap() as i32,
        time: Some(timestamp_from_ms(view.ts_ms("fill_time", row).unwrap())),
        start_position: fills.dec("start_position", row),
        direction: pb::TradingDirection::from_str_name(&format!("TRADING_DIRECTION_{direction}"))
            .unwrap() as i32,
        closed_pnl: fills.dec("closed_pnl", row),
        hash: fills.bytes("hash", row),
        order_id: fills.u64("order_id", row),
        crossed: fills.bool("crossed", row),
        fee: fills.dec("fee", row),
        transaction_id: fills.u64("transaction_id", row),
        fee_token: fills.text("fee_token", row),
        twap_id: view.u64("twap_id", row),
        client_order_id: fills.bytes_or_empty("client_order_id", row),
        liquidation: view
            .text("liquidation_method", row)
            .map(|method| pb::FillLiquidation {
                liquidated_user: fills.bytes_or_empty("liquidated_user", row),
                mark_px: fills.dec("liquidation_mark_px", row),
                method: method.to_string(),
            }),
        deployer_fee: fills.dec_or_empty("deployer_fee", row),
        builder: fills.text_or_empty("builder", row),
        builder_fee: fills.dec_or_empty("builder_fee", row),
        priority_gas: fills.dec_or_empty("priority_gas", row),
    }
}

fn rebuild_delta(e: &Rebuild<'_>, row: usize) -> ledger_update_delta::Delta {
    use ledger_update_delta::Delta;
    match e.view.text("ledger_type", row).unwrap() {
        "spot_transfer" => Delta::SpotTransfer(pb::SpotTransfer {
            token: e.text("token", row),
            amount: e.dec("amount", row),
            usdc_value: e.dec("usdc_value", row),
            user: e.bytes("user", row),
            destination: e.bytes("destination", row),
            fee: e.dec("fee", row),
            native_token_fee: e.dec("native_token_fee", row),
            nonce: e.u64("nonce", row),
            fee_token: e.text_or_empty("fee_token", row),
        }),
        "c_staking_transfer" => Delta::CStakingTransfer(pb::CStakingTransfer {
            token: e.text("token", row),
            amount: e.dec("amount", row),
            is_deposit: e.bool("is_deposit", row),
        }),
        "account_class_transfer" => Delta::AccountClassTransfer(pb::AccountClassTransfer {
            usdc: e.dec("usdc", row),
            to_perp: e.bool("to_perp", row),
        }),
        "internal_transfer" => Delta::InternalTransfer(pb::InternalTransfer {
            usdc: e.dec("usdc", row),
            user: e.bytes("user", row),
            destination: e.bytes("destination", row),
            fee: e.dec("fee", row),
        }),
        "sub_account_transfer" => Delta::SubAccountTransfer(pb::SubAccountTransfer {
            usdc: e.dec("usdc", row),
            user: e.bytes("user", row),
            destination: e.bytes("destination", row),
        }),
        "send" => Delta::Send(pb::Send {
            user: e.bytes("user", row),
            destination: e.bytes("destination", row),
            source_dex: e.text("source_dex", row),
            destination_dex: e.text("destination_dex", row),
            token: e.text("token", row),
            amount: e.dec("amount", row),
            usdc_value: e.dec("usdc_value", row),
            fee: e.dec("fee", row),
            native_token_fee: e.dec("native_token_fee", row),
            nonce: e.u64("nonce", row),
            fee_token: e.text_or_empty("fee_token", row),
        }),
        "deposit" => Delta::Deposit(pb::Deposit {
            usdc: e.dec("usdc", row),
        }),
        "withdraw" => Delta::Withdraw(pb::Withdraw {
            usdc: e.dec("usdc", row),
            nonce: e.u64("nonce", row),
            fee: e.dec("fee", row),
        }),
        "vault_deposit" => Delta::VaultDeposit(pb::VaultDeposit {
            vault: e.bytes("vault", row),
            usdc: e.dec("usdc", row),
        }),
        "rewards_claim" => Delta::RewardsClaim(pb::RewardsClaim {
            amount: e.dec("amount", row),
            token: e.text("token", row),
        }),
        "vault_withdraw" => Delta::VaultWithdraw(pb::VaultWithdraw {
            vault: e.bytes("vault", row),
            user: e.bytes("user", row),
            requested_usd: e.dec("requested_usd", row),
            commission: e.dec("commission", row),
            closing_cost: e.dec("closing_cost", row),
            basis: e.dec("basis", row),
            net_withdrawn_usd: e.dec("net_withdrawn_usd", row),
        }),
        "vault_leader_commission" => Delta::VaultLeaderCommission(pb::VaultLeaderCommission {
            user: e.bytes("user", row),
            usdc: e.dec("usdc", row),
        }),
        "deploy_gas_auction" => Delta::DeployGasAuction(pb::DeployGasAuction {
            token: e.text("token", row),
            amount: e.dec("amount", row),
        }),
        "account_activation_gas" => Delta::AccountActivationGas(pb::AccountActivationGas {
            amount: e.dec("amount", row),
            token: e.text("token", row),
        }),
        "activate_dex_abstraction" => Delta::ActivateDexAbstraction(pb::ActivateDexAbstraction {
            dex: e.text("dex", row),
            token: e.text("token", row),
            amount: e.dec("amount", row),
        }),
        "liquidation" => Delta::Liquidation(pb::Liquidation {
            liquidated_ntl_pos: e.dec("liquidated_ntl_pos", row),
            account_value: e.dec("account_value", row),
            leverage_type: pb::LeverageType::from_str_name(&format!(
                "LEVERAGE_TYPE_{}",
                e.text("leverage_type", row)
            ))
            .unwrap() as i32,
            liquidated_positions: e
                .view
                .positions("liquidated_positions", row)
                .unwrap()
                .into_iter()
                .map(|(coin, szi)| pb::LiquidatedPosition {
                    coin,
                    szi: canonical_text(szi),
                })
                .collect(),
        }),
        "spot_genesis" => Delta::SpotGenesis(pb::SpotGenesis {
            token: e.text("token", row),
            amount: e.dec("amount", row),
        }),
        "vault_distribution" => Delta::VaultDistribution(pb::VaultDistribution {
            vault: e.bytes("vault", row),
            usdc: e.dec("usdc", row),
        }),
        "borrow_lend" => Delta::BorrowLend(pb::BorrowLend {
            token: e.text("token", row),
            amount: e.dec("amount", row),
            interest_amount: e.dec("interest_amount", row),
            operation: e.text("operation", row),
        }),
        "vault_create" => Delta::VaultCreate(pb::VaultCreate {
            vault: e.bytes("vault", row),
            usdc: e.dec("usdc", row),
            fee: e.dec("fee", row),
        }),
        "gossip_priority_gas_auction" => {
            Delta::GossipPriorityGasAuction(pb::GossipPriorityGasAuction {
                token: e.text("token", row),
                amount: e.dec("amount", row),
            })
        }
        "hip3_liquidator_deposit" => Delta::Hip3LiquidatorDeposit(pb::Hip3LiquidatorDeposit {
            dex: e.text("dex", row),
            token: e.text("token", row),
            amount: e.dec("amount", row),
        }),
        other => panic!("unknown ledger_type {other}"),
    }
}

/// Child rows by parent key, checking their positions.
fn children(view: &View<'_>, position: &str) -> HashMap<(u64, u32), Vec<usize>> {
    let mut children: HashMap<(u64, u32), Vec<usize>> = HashMap::new();
    for row in 0..view.len() {
        let key = (
            view.u64("block_num", row).unwrap(),
            view.u32("event_index", row).unwrap(),
        );
        let rows = children.entry(key).or_default();
        assert_eq!(view.u32(position, row), Some(rows.len() as u32));
        rows.push(row);
    }
    children
}

fn rows_by_block(view: &View<'_>) -> BTreeMap<u64, Vec<usize>> {
    let mut rows: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for row in 0..view.len() {
        rows.entry(view.u64("block_num", row).unwrap())
            .or_default()
            .push(row);
    }
    rows
}

/// Rebuild every block from the five tables with the inverse rules.
fn rebuild(tables: &Tables, encoding: &EncodeBytes) -> Vec<(u64, pb::Block)> {
    let blocks = View::new(tables, "blocks", encoding);
    let fills = Rebuild {
        view: View::new(tables, "fills", encoding),
    };
    let events = Rebuild {
        view: View::new(tables, "events", encoding),
    };
    let deltas = Rebuild {
        view: View::new(tables, "funding_deltas", encoding),
    };
    let rewards = Rebuild {
        view: View::new(tables, "validator_rewards", encoding),
    };
    let fills_of = rows_by_block(&fills.view);
    let events_of = rows_by_block(&events.view);
    let deltas_of = children(&deltas.view, "delta_index");
    let rewards_of = children(&rewards.view, "reward_index");

    let none = Vec::new();
    (0..blocks.len())
        .map(|row| {
            let number = blocks.u64("block_num", row).unwrap();
            let fill_rows = fills_of.get(&number).unwrap_or(&none);
            let event_rows = events_of.get(&number).unwrap_or(&none);
            for (index, fill) in fill_rows.iter().enumerate() {
                assert_eq!(fills.view.u32("fill_index", *fill), Some(index as u32));
            }
            let rebuilt_events = event_rows
                .iter()
                .enumerate()
                .map(|(index, event)| {
                    let event = *event;
                    let event_index = events.view.u32("event_index", event).unwrap();
                    assert_eq!(event_index, index as u32);
                    let items = |of: &HashMap<(u64, u32), Vec<usize>>| -> Vec<usize> {
                        let rows = of.get(&(number, event_index)).cloned().unwrap_or_default();
                        assert_eq!(
                            Some(rows.len() as u32),
                            events.view.u32("item_count", event),
                            "{number} event {event_index}: item_count"
                        );
                        rows
                    };
                    let body = match events.view.text("event_type", event).unwrap() {
                        "ledger_update" => event_body::Event::LedgerUpdate(pb::LedgerUpdate {
                            users: events
                                .view
                                .bytes_list("users", event)
                                .unwrap()
                                .into_iter()
                                .map(Bytes::from)
                                .collect(),
                            delta: Some(pb::LedgerUpdateDelta {
                                delta: Some(rebuild_delta(&events, event)),
                            }),
                        }),
                        "funding" => event_body::Event::Funding(pb::Funding {
                            deltas: items(&deltas_of)
                                .into_iter()
                                .map(|delta| pb::FundingDelta {
                                    user: deltas.bytes("user", delta),
                                    coin: deltas.text("coin", delta),
                                    funding_amount: deltas.dec("funding_amount", delta),
                                    szi: deltas.dec("szi", delta),
                                    funding_rate: deltas.dec("funding_rate", delta),
                                })
                                .collect(),
                        }),
                        "validator_rewards" => {
                            event_body::Event::ValidatorRewards(pb::ValidatorRewards {
                                validator_to_reward: items(&rewards_of)
                                    .into_iter()
                                    .map(|reward| pb::ValidatorReward {
                                        validator: rewards.bytes("validator", reward),
                                        reward: rewards.dec("reward", reward),
                                    })
                                    .collect(),
                            })
                        }
                        "c_withdrawal" => event_body::Event::CWithdrawal(pb::CWithdrawal {
                            user: events.bytes("user", event),
                            amount: events.dec("amount", event),
                            is_finalized: events.bool("is_finalized", event),
                        }),
                        "c_deposit" => event_body::Event::CDeposit(pb::CDeposit {
                            user: events.bytes("user", event),
                            amount: events.dec("amount", event),
                        }),
                        "delegation" => event_body::Event::Delegation(pb::Delegation {
                            user: events.bytes("user", event),
                            validator: events.bytes("validator", event),
                            amount: events.dec("amount", event),
                            is_undelegate: events.bool("is_undelegate", event),
                        }),
                        "gossip_priority_auction_restart" => {
                            event_body::Event::GossipPriorityAuctionRestart(
                                pb::GossipPriorityAuctionRestart {
                                    slot_id: events.u64("slot_id", event),
                                    previous_winner: events
                                        .view
                                        .text("previous_winner_ip", event)
                                        .map(|ip| pb::GossipPriorityAuctionPreviousWinner {
                                            ip: ip.to_string(),
                                        }),
                                    end_gas: events.view.dec("end_gas", event).map(canonical_text),
                                },
                            )
                        }
                        "create_sub_account" => {
                            event_body::Event::CreateSubAccount(pb::CreateSubAccount {
                                user: events.bytes("user", event),
                                sub_account: events.bytes("sub_account", event),
                                name: events.text("sub_account_name", event),
                            })
                        }
                        other => panic!("unknown event_type {other}"),
                    };
                    pb::Event {
                        time: Some(timestamp_from_ns(
                            events.view.i64("event_time_ns", event).unwrap(),
                        )),
                        hash: events.bytes("hash", event),
                        events: vec![pb::EventBody { event: Some(body) }],
                    }
                })
                .collect();
            let block = pb::Block {
                block_header: Some(pb::BlockHeader {
                    block_number: number,
                    block_time: Some(timestamp_from_ns(blocks.i64("block_time_ns", row).unwrap())),
                }),
                fills: fill_rows
                    .iter()
                    .map(|fill| rebuild_fill(&fills, *fill))
                    .collect(),
                events: rebuilt_events,
            };
            (number, block)
        })
        .collect()
}

/// The proof that the mapping loses nothing: every fixture payload is rebuilt
/// byte for byte from the five tables, under `hex` and `binary`. It also
/// confirms that prost re-encodes every payload to its own length (R2).
#[test]
fn t4_every_payload_is_rebuilt_byte_for_byte_from_the_tables() {
    for (number, payload) in real_blocks() {
        assert_eq!(decode(payload).encoded_len(), payload.len(), "{number}");
    }
    for encoding in &GOLDEN_ENCODINGS {
        let rebuilt = rebuild(golden(encoding), encoding);
        assert_eq!(rebuilt.len(), real_blocks().len());
        for ((number, block), (expected_number, payload)) in rebuilt.iter().zip(real_blocks()) {
            assert_eq!(number, expected_number);
            assert!(
                block.encode_to_vec() == *payload,
                "{encoding:?}: block {number} does not rebuild byte for byte"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// T5 populated-column matrix, T6 extra_json
// ---------------------------------------------------------------------------

/// The `events` columns after the canonical ones.
const EVENT_PAYLOAD_COLUMNS: [&str; 43] = [
    "event_index",
    "event_type",
    "ledger_type",
    "hash",
    "event_time_ns",
    "users",
    "user",
    "destination",
    "vault",
    "validator",
    "sub_account",
    "token",
    "amount",
    "usdc",
    "usdc_value",
    "fee",
    "fee_token",
    "native_token_fee",
    "nonce",
    "source_dex",
    "destination_dex",
    "dex",
    "is_deposit",
    "to_perp",
    "is_undelegate",
    "is_finalized",
    "requested_usd",
    "commission",
    "closing_cost",
    "basis",
    "net_withdrawn_usd",
    "interest_amount",
    "operation",
    "liquidated_ntl_pos",
    "account_value",
    "leverage_type",
    "liquidated_positions",
    "slot_id",
    "previous_winner_ip",
    "end_gas",
    "sub_account_name",
    "item_count",
    "extra_json",
];

type Matrix = BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)>;

/// The matrix as `docs/chains/hypercore.md` states it: `(columns, optional
/// columns)` keyed by `event_type` or `ledger_update/<ledger_type>`.
fn documented_matrix() -> Matrix {
    let doc = include_str!("../../../docs/chains/hypercore.md");
    let start = doc
        .find("| Type | Columns set |")
        .expect("the matrix in docs/chains/hypercore.md");
    let mut matrix = BTreeMap::new();
    for line in doc[start..].lines().skip(2) {
        let Some(row) = line.strip_prefix("| ") else {
            break;
        };
        let (kind, columns) = row
            .trim_end_matches(" |")
            .split_once(" | ")
            .expect("two cells");
        let names: Vec<&str> = kind
            .split(" / ")
            .map(|part| part.split(' ').next().unwrap().trim_matches('`'))
            .collect();
        let mut set = BTreeSet::new();
        let mut optional = BTreeSet::new();
        for column in columns.split(", ") {
            let name = column.trim_end_matches('?').trim_matches('`').to_string();
            if column.ends_with('?') {
                optional.insert(name.clone());
            }
            set.insert(name);
        }
        matrix.insert(names.join("/"), (set, optional));
    }
    matrix
}

#[test]
fn t5_each_event_type_sets_exactly_its_documented_columns() {
    let matrix = documented_matrix();
    assert_eq!(matrix.len(), 7 + 22, "{:?}", matrix.keys());
    for encoding in &GOLDEN_ENCODINGS {
        let tables = golden(encoding);
        let events = View::new(tables, "events", encoding);
        let mut seen = BTreeSet::new();
        for row in 0..events.len() {
            let event_type = events.text("event_type", row).unwrap();
            let mut always: BTreeSet<String> =
                ["event_index", "event_type", "hash", "event_time_ns"]
                    .map(String::from)
                    .into();
            let key = match events.text("ledger_type", row) {
                Some(ledger_type) => {
                    always.extend(["ledger_type".to_string(), "users".to_string()]);
                    format!("{event_type}/{ledger_type}")
                }
                None => event_type.to_string(),
            };
            let (columns, optional) = &matrix[&key];
            let expected: BTreeSet<String> = always.union(columns).cloned().collect();
            let set: BTreeSet<String> = EVENT_PAYLOAD_COLUMNS
                .iter()
                .filter(|column| !events.is_null(column, row))
                .map(|column| column.to_string())
                .collect();
            assert!(
                set.is_subset(&expected),
                "{key}: {set:?} not in {expected:?}"
            );
            let missing: BTreeSet<String> = expected.difference(&set).cloned().collect();
            assert!(missing.is_subset(optional), "{key}: {missing:?} are NULL");
            seen.insert(key);
        }
        assert_eq!(
            seen.len(),
            matrix.len(),
            "every documented type is in the fixtures"
        );
    }
}

#[test]
fn t6_extra_json_is_null_in_every_row() {
    for encoding in &GOLDEN_ENCODINGS {
        let tables = golden(encoding);
        for table in TABLE_NAMES {
            let batch = &tables[table];
            let extra = batch.column_by_name("extra_json").unwrap();
            assert_eq!(extra.data_type(), &DataType::Utf8);
            assert_eq!(extra.null_count(), batch.num_rows(), "{table}");
        }
    }
}

// ---------------------------------------------------------------------------
// T7 release invariant
// ---------------------------------------------------------------------------

/// One value in a canonical text form, independent of Arrow's display and IPC
/// formats.
fn write_value(array: &dyn Array, row: usize, out: &mut String) {
    use std::fmt::Write as _;
    if array.is_null(row) {
        out.push('~');
        return;
    }
    match array.data_type() {
        DataType::UInt64 => write!(out, "{}", array.as_primitive::<UInt64Type>().value(row)),
        DataType::UInt32 => write!(out, "{}", array.as_primitive::<UInt32Type>().value(row)),
        DataType::Int64 => write!(out, "{}", array.as_primitive::<Int64Type>().value(row)),
        DataType::Date32 => write!(out, "{}", array.as_primitive::<Date32Type>().value(row)),
        DataType::Timestamp(TimeUnit::Millisecond, _) => write!(
            out,
            "{}",
            array.as_primitive::<TimestampMillisecondType>().value(row)
        ),
        DataType::Decimal128(_, _) => {
            write!(out, "{}", array.as_primitive::<Decimal128Type>().value(row))
        }
        DataType::Boolean => write!(out, "{}", array.as_boolean().value(row)),
        DataType::Utf8 => {
            let text = array.as_string::<i32>().value(row);
            write!(out, "{}:{text}", text.len())
        }
        DataType::Binary => write!(out, "b{}", hex(array.as_binary::<i32>().value(row))),
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_dictionary::<Int32Type>();
            let text = dictionary
                .values()
                .as_string::<i32>()
                .value(dictionary.keys().value(row) as usize);
            write!(out, "{}:{text}", text.len())
        }
        DataType::List(_) => {
            let items = array.as_list::<i32>().value(row);
            out.push('[');
            for item in 0..items.len() {
                write_value(items.as_ref(), item, out);
                out.push(',');
            }
            out.push(']');
            Ok(())
        }
        DataType::Struct(_) => {
            out.push('{');
            for field in array.as_struct().columns() {
                write_value(field.as_ref(), row, out);
                out.push(',');
            }
            out.push('}');
            Ok(())
        }
        other => panic!("no canonical form for {other}"),
    }
    .expect("write to a String");
}

/// SHA-256 over every table, column and value of a flush.
fn output_digest(tables: &Tables) -> String {
    let mut hasher = Sha256::new();
    let mut text = String::new();
    for table in TABLE_NAMES {
        let batch = &tables[table];
        hasher.update(format!("table {table} rows {}\n", batch.num_rows()));
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            hasher.update(format!("column {}\n", field.name()));
            for row in 0..batch.num_rows() {
                text.clear();
                write_value(column.as_ref(), row, &mut text);
                text.push('\n');
                hasher.update(text.as_bytes());
            }
        }
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The release invariant of schema epoch 1: every later release must map the
/// 36 fixtures to exactly this output, value for value, under `hex` and
/// `binary`. A change here is a change of the data in existing roots.
#[test]
fn t7_fixture_output_matches_the_pinned_release_invariant() {
    assert_eq!(
        output_digest(golden(&EncodeBytes::Hex)),
        "0f2f619c947d8c73d68d7f58a483a9f1d6b3dd759ba08e2690c5731dd63c4da9",
        "hex output changed"
    );
    assert_eq!(
        output_digest(golden(&EncodeBytes::Binary)),
        "85156402652c799b88176dda9071989e39e322828648da7069d99d8366c82204",
        "binary output changed"
    );
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

/// One delegation event, no fills.
const SMALL_BLOCK: u64 = 1_009_958_482;
/// 28 fills and a `borrow_lend` event.
const BUSY_BLOCK: u64 = 1_165_601_237;
/// One `send` event.
const SEND_BLOCK: u64 = 1_009_907_496;
/// Twelve ledger liquidations and 38 fills.
const CASCADE_BLOCK: u64 = 1_127_672_017;
/// Two gossip restarts and validator rewards.
const GOSSIP_BLOCK: u64 = 987_247_825;

/// The `index`th event's body.
fn body_mut(block: &mut pb::Block, index: usize) -> &mut event_body::Event {
    block.events[index].events[0]
        .event
        .as_mut()
        .expect("fixture body")
}

/// The first event whose body matches.
fn find_event(block: &pb::Block, keep: impl Fn(&event_body::Event) -> bool) -> usize {
    block
        .events
        .iter()
        .position(|event| keep(event.events[0].event.as_ref().unwrap()))
        .expect("a matching fixture event")
}

fn delta_mut(block: &mut pb::Block, index: usize) -> &mut ledger_update_delta::Delta {
    match body_mut(block, index) {
        event_body::Event::LedgerUpdate(update) => update
            .delta
            .as_mut()
            .and_then(|delta| delta.delta.as_mut())
            .expect("fixture delta"),
        _ => panic!("event {index} is not a ledger update"),
    }
}

/// The first ledger event whose delta matches.
fn find_delta(block: &pb::Block, keep: impl Fn(&ledger_update_delta::Delta) -> bool) -> usize {
    find_event(block, |body| match body {
        event_body::Event::LedgerUpdate(update) => update
            .delta
            .as_ref()
            .and_then(|delta| delta.delta.as_ref())
            .is_some_and(&keep),
        _ => false,
    })
}

/// A valid synthetic event at the block's time with `body`.
fn synthetic_event(block: &pb::Block, body: event_body::Event) -> pb::Event {
    pb::Event {
        time: block.block_header.as_ref().unwrap().block_time,
        hash: Bytes::from(vec![0; 32]),
        events: vec![pb::EventBody { event: Some(body) }],
    }
}

fn funding_delta() -> pb::FundingDelta {
    pb::FundingDelta {
        user: Bytes::from(vec![0x11; 20]),
        coin: "BTC".to_string(),
        funding_amount: "-1.5".to_string(),
        szi: "0.25".to_string(),
        funding_rate: "0.0000125".to_string(),
    }
}

#[test]
fn r1_undecodable_payloads_are_refused() {
    let identity = identity_of(&fixture(SMALL_BLOCK));
    let mut mapper = HypercoreBlockMapper::new(false, EncodeBytes::Hex);
    let error = mapper
        .map_block(&[0xff, 0xff], &identity, StreamEvent::default())
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with(&format!(
            "hypercore block {SMALL_BLOCK}: cannot decode pinax.hypercore.v1.Block: "
        )),
        "{error}"
    );
}

fn guard_message(block: u64, payload: usize, encoded: usize) -> String {
    format!(
        "hypercore block {block}: payload is {payload} bytes but re-encodes to {encoded}: it \
         carries fields or oneof cases unknown to the vendored pinax.hypercore.v1 protos; \
         refresh them from buf.build/pinax/hypercore"
    )
}

/// Map a raw payload through both entry points; both must refuse it and
/// leave every table empty.
fn refuse_payload(payload: &[u8], identity: &BlockIdentity) -> String {
    let mut mapper = HypercoreBlockMapper::new(false, EncodeBytes::Hex);
    let owned = mapper
        .map_block_bytes(
            Bytes::copy_from_slice(payload),
            identity,
            StreamEvent::default(),
        )
        .unwrap_err()
        .to_string();
    let borrowed = mapper
        .map_block(payload, identity, StreamEvent::default())
        .unwrap_err()
        .to_string();
    assert_eq!(owned, borrowed);
    assert!(mapper
        .flush()
        .unwrap()
        .values()
        .all(|batch| batch.num_rows() == 0));
    borrowed
}

#[test]
fn r2_unknown_fields_and_cases_are_refused_before_any_other_check() {
    use prost::encoding::{encode_key, encode_varint, WireType};
    let small = fixture(SMALL_BLOCK);
    let identity = identity_of(&small);

    // An unknown top-level field 100 of length 0.
    let mut raw = payload(SMALL_BLOCK).to_vec();
    let len = raw.len();
    raw.extend([0xa2, 0x06, 0x00]);
    assert_eq!(
        refuse_payload(&raw, &identity),
        guard_message(SMALL_BLOCK, len + 3, len)
    );

    // A fill with an unknown varint field 23, re-wrapped in a block.
    let busy = fixture(BUSY_BLOCK);
    let mut fill = busy.fills[0].encode_to_vec();
    fill.extend([0xb8, 0x01, 0x01]);
    let mut raw = pb::Block {
        block_header: busy.block_header,
        fills: vec![],
        events: vec![],
    }
    .encode_to_vec();
    encode_key(2, WireType::LengthDelimited, &mut raw);
    encode_varint(fill.len() as u64, &mut raw);
    raw.extend(&fill);
    assert_eq!(
        refuse_payload(&raw, &identity_of(&busy)),
        guard_message(BUSY_BLOCK, raw.len(), raw.len() - 3)
    );

    // An unknown EventBody case 9 decodes as no case: R2 refuses it before R6
    // could, naming the protos instead of the symptom.
    let mut body = Vec::new();
    encode_key(9, WireType::LengthDelimited, &mut body);
    encode_varint(0, &mut body);
    let mut event = pb::Event {
        time: small.block_header.as_ref().unwrap().block_time,
        hash: Bytes::from(vec![0; 32]),
        events: vec![],
    }
    .encode_to_vec();
    encode_key(3, WireType::LengthDelimited, &mut event);
    encode_varint(body.len() as u64, &mut event);
    event.extend(&body);
    let mut raw = payload(SMALL_BLOCK).to_vec();
    encode_key(3, WireType::LengthDelimited, &mut raw);
    encode_varint(event.len() as u64, &mut raw);
    raw.extend(&event);
    let error = refuse_payload(&raw, &identity);
    assert_eq!(error, guard_message(SMALL_BLOCK, raw.len(), raw.len() - 2));
}

#[test]
fn r3_the_header_must_match_the_firehose_identity() {
    let block = fixture(SMALL_BLOCK);
    let identity = identity_of(&block);
    let prefix = format!("hypercore block {SMALL_BLOCK}: block_header");

    let mut missing = block.clone();
    missing.block_header = None;
    assert_eq!(
        refusal_with(&missing, &identity),
        format!("{prefix}: missing")
    );

    let mut number = block.clone();
    number.block_header.as_mut().unwrap().block_number += 1;
    assert_eq!(
        refusal_with(&number, &identity),
        format!(
            "{prefix}: block_number {} differs from the Firehose block number {SMALL_BLOCK}",
            SMALL_BLOCK + 1
        )
    );

    let mut no_time = block.clone();
    no_time.block_header.as_mut().unwrap().block_time = None;
    assert_eq!(
        refusal_with(&no_time, &identity),
        format!("{prefix}.block_time: missing")
    );

    let (seconds, nanos) = (identity.timestamp, identity.timestamp_nanos);
    fn time(block: &mut pb::Block) -> &mut prost_types::Timestamp {
        block
            .block_header
            .as_mut()
            .unwrap()
            .block_time
            .as_mut()
            .unwrap()
    }
    let mut later = block.clone();
    time(&mut later).seconds += 1;
    assert_eq!(
        refusal_with(&later, &identity),
        format!(
            "{prefix}: block_time {}s {nanos}ns differs from the Firehose block time \
             {seconds}s {nanos}ns",
            seconds + 1
        )
    );

    let mut other_nanos = block.clone();
    time(&mut other_nanos).nanos = nanos + 1;
    assert_eq!(
        refusal_with(&other_nanos, &identity),
        format!(
            "{prefix}: block_time {seconds}s {}ns differs from the Firehose block time \
             {seconds}s {nanos}ns",
            nanos + 1
        )
    );

    let mut bad_nanos = block.clone();
    time(&mut bad_nanos).nanos = -1;
    assert_eq!(
        refusal_with(&bad_nanos, &identity),
        format!("{prefix}.block_time: nanos -1 is outside [0, 1e9)")
    );
}

#[test]
fn r6_events_need_exactly_one_body_with_a_case() {
    let block = fixture(SMALL_BLOCK);

    let mut no_body = block.clone();
    no_body.events[0].events.clear();
    assert_refused(&no_body, "events[0].events: 0 bodies, expected 1");

    let mut two = block.clone();
    let body = two.events[0].events[0].clone();
    two.events[0].events.push(body);
    assert_refused(&two, "events[0].events: 2 bodies, expected 1");

    let mut empty_body = block.clone();
    empty_body.events[0].events[0].event = None;
    assert_refused(&empty_body, "events[0].events[0]: EventBody has no case");

    let send = fixture(SEND_BLOCK);
    let mut no_delta = send.clone();
    let event_body::Event::LedgerUpdate(update) = body_mut(&mut no_delta, 0) else {
        panic!("a ledger update");
    };
    update.delta = None;
    assert_refused(
        &no_delta,
        "events[0].events[0].ledger_update.delta: missing, so LedgerUpdateDelta has no case",
    );

    let mut no_case = send.clone();
    let event_body::Event::LedgerUpdate(update) = body_mut(&mut no_case, 0) else {
        panic!("a ledger update");
    };
    update.delta = Some(pb::LedgerUpdateDelta { delta: None });
    assert_refused(
        &no_case,
        "events[0].events[0].ledger_update.delta: LedgerUpdateDelta has no case",
    );
}

#[test]
fn r7_unknown_or_unspecified_enum_values_are_refused() {
    let busy = fixture(BUSY_BLOCK);
    for side in [0, 3] {
        let mut block = busy.clone();
        block.fills[2].side = side;
        assert_refused(
            &block,
            &format!("fills[2].side: unknown or unspecified FillSide value {side}"),
        );
    }
    for direction in [0, 23] {
        let mut block = busy.clone();
        block.fills[2].direction = direction;
        assert_refused(
            &block,
            &format!(
                "fills[2].direction: unknown or unspecified TradingDirection value {direction}"
            ),
        );
    }
    let cascade = fixture(CASCADE_BLOCK);
    for leverage in [0, 3] {
        let mut block = cascade.clone();
        let ledger_update_delta::Delta::Liquidation(liquidation) = delta_mut(&mut block, 4) else {
            panic!("a liquidation");
        };
        liquidation.leverage_type = leverage;
        assert_refused(
            &block,
            &format!(
                "events[4].events[0].ledger_update.delta.liquidation.leverage_type: unknown or \
                 unspecified LeverageType value {leverage}"
            ),
        );
    }
}

#[test]
fn r8_decimals_must_be_exact_plain_numbers() {
    let busy = fixture(BUSY_BLOCK);
    let refused = |price: &str| {
        let mut block = busy.clone();
        block.fills[1].price = price.to_string();
        refusal(&block)
    };
    let prefix = format!("hypercore block {BUSY_BLOCK}: fills[1].price");
    assert_eq!(refused(""), format!("{prefix}: empty string"));
    for syntax in ["1e-5", "+1.0", " 1.0", ".5", "5.", "NaN"] {
        assert_eq!(
            refused(syntax),
            format!("{prefix}: not a plain decimal number in decimal {syntax:?}")
        );
    }
    assert_eq!(
        refused("0.12345678901"),
        format!("{prefix}: more than 10 fractional digits in decimal \"0.12345678901\"")
    );
    let wide = format!("{}.0", "1".repeat(29));
    assert_eq!(
        refused(&wide),
        format!("{prefix}: more than 28 integer digits in decimal {wide:?}")
    );
    // The offending value is cut to 80 characters.
    let long = format!("x{}", "9".repeat(200));
    let error = refused(&long);
    assert!(error.ends_with('…'), "{error}");
    assert!(error.len() < 200, "{error}");

    // A decimal in a nested message is refused the same way.
    let mut block = fixture(SMALL_BLOCK);
    let mut delta = funding_delta();
    delta.szi = "1e3".to_string();
    let event = synthetic_event(
        &block,
        event_body::Event::Funding(pb::Funding {
            deltas: vec![funding_delta(), delta],
        }),
    );
    block.events.insert(0, event);
    assert_refused(
        &block,
        "events[0].events[0].funding.deltas[1].szi: not a plain decimal number in decimal \"1e3\"",
    );

    // Exact forms that are not canonical are accepted, as their value.
    for (text, value) in [
        ("1.50", 15 * ONE / 10),
        ("5", 5 * ONE),
        ("-0.0", 0),
        ("0.1234567890000", 1_234_567_890),
        ("00.5", ONE / 2),
    ] {
        let mut block = busy.clone();
        block.fills[1].price = text.to_string();
        let tables = map_one(&block).unwrap();
        let fills = View::new(&tables, "fills", &EncodeBytes::Hex);
        assert_eq!(fills.dec("price", 1), Some(value), "{text}");
    }
}

#[test]
fn r9_r10_empty_required_strings_and_bytes_are_refused() {
    let busy = fixture(BUSY_BLOCK);
    let fill_case = |edit: fn(&mut pb::Fill), expected: &str| {
        let mut block = busy.clone();
        edit(&mut block.fills[3]);
        assert_refused(&block, expected);
    };
    fill_case(|f| f.coin.clear(), "fills[3].coin: empty string");
    fill_case(|f| f.fee_token.clear(), "fills[3].fee_token: empty string");
    fill_case(|f| f.user = Bytes::new(), "fills[3].user: empty bytes");
    fill_case(|f| f.hash = Bytes::new(), "fills[3].hash: empty bytes");
    fill_case(
        |f| {
            f.liquidation = Some(pb::FillLiquidation {
                liquidated_user: Bytes::from(vec![1; 20]),
                mark_px: "1.0".to_string(),
                method: String::new(),
            })
        },
        "fills[3].liquidation.method: empty string",
    );
    fill_case(
        |f| {
            f.liquidation = Some(pb::FillLiquidation {
                liquidated_user: Bytes::from(vec![1; 20]),
                mark_px: String::new(),
                method: "market".to_string(),
            })
        },
        "fills[3].liquidation.mark_px: empty string",
    );

    let mut block = busy.clone();
    block.events[0].hash = Bytes::new();
    assert_refused(&block, "events[0].hash: empty bytes");

    let send = fixture(SEND_BLOCK);
    let mut block = send.clone();
    let ledger_update_delta::Delta::Send(transfer) = delta_mut(&mut block, 0) else {
        panic!("a send");
    };
    transfer.token.clear();
    assert_refused(
        &block,
        "events[0].events[0].ledger_update.delta.send.token: empty string",
    );
    let mut block = send.clone();
    let ledger_update_delta::Delta::Send(transfer) = delta_mut(&mut block, 0) else {
        panic!("a send");
    };
    transfer.destination = Bytes::new();
    assert_refused(
        &block,
        "events[0].events[0].ledger_update.delta.send.destination: empty bytes",
    );
    let mut block = send.clone();
    let event_body::Event::LedgerUpdate(update) = body_mut(&mut block, 0) else {
        panic!("a ledger update");
    };
    update.users.push(Bytes::new());
    let last = update.users.len() - 1;
    assert_refused(
        &block,
        &format!("events[0].events[0].ledger_update.users[{last}]: empty bytes"),
    );

    let mut block = fixture(1_173_886_256);
    let ledger_update_delta::Delta::BorrowLend(borrow) = delta_mut(&mut block, 0) else {
        panic!("a borrow_lend");
    };
    borrow.operation.clear();
    assert_refused(
        &block,
        "events[0].events[0].ledger_update.delta.borrow_lend.operation: empty string",
    );

    let mut block = fixture(CASCADE_BLOCK);
    let ledger_update_delta::Delta::Liquidation(liquidation) = delta_mut(&mut block, 0) else {
        panic!("a liquidation");
    };
    liquidation.liquidated_positions[0].coin.clear();
    assert_refused(
        &block,
        "events[0].events[0].ledger_update.delta.liquidation.liquidated_positions[0].coin: \
         empty string",
    );

    let gossip = fixture(GOSSIP_BLOCK);
    let slot = find_event(&gossip, |body| {
        matches!(body, event_body::Event::GossipPriorityAuctionRestart(restart)
            if restart.previous_winner.is_some())
    });
    let mut block = gossip.clone();
    let event_body::Event::GossipPriorityAuctionRestart(restart) = body_mut(&mut block, slot)
    else {
        panic!("a gossip restart");
    };
    restart.previous_winner = Some(pb::GossipPriorityAuctionPreviousWinner { ip: String::new() });
    assert_refused(
        &block,
        &format!(
            "events[{slot}].events[0].gossip_priority_auction_restart.previous_winner.ip: \
             empty string"
        ),
    );
    let mut block = gossip.clone();
    let event_body::Event::GossipPriorityAuctionRestart(restart) = body_mut(&mut block, slot)
    else {
        panic!("a gossip restart");
    };
    restart.end_gas = Some(String::new());
    assert_refused(
        &block,
        &format!("events[{slot}].events[0].gossip_priority_auction_restart.end_gas: empty string"),
    );

    let rewards = find_event(&gossip, |body| {
        matches!(body, event_body::Event::ValidatorRewards(_))
    });
    let mut block = gossip.clone();
    let event_body::Event::ValidatorRewards(list) = body_mut(&mut block, rewards) else {
        panic!("validator rewards");
    };
    list.validator_to_reward[5].validator = Bytes::new();
    assert_refused(
        &block,
        &format!(
            "events[{rewards}].events[0].validator_rewards.validator_to_reward[5].validator: \
             empty bytes"
        ),
    );

    let mut block = fixture(SMALL_BLOCK);
    let mut delta = funding_delta();
    delta.coin.clear();
    let event = synthetic_event(
        &block,
        event_body::Event::Funding(pb::Funding {
            deltas: vec![funding_delta(), delta],
        }),
    );
    block.events.insert(0, event);
    assert_refused(
        &block,
        "events[0].events[0].funding.deltas[1].coin: empty string",
    );
}

#[test]
fn r11_times_must_be_present_whole_and_in_range() {
    let busy = fixture(BUSY_BLOCK);
    let mut block = busy.clone();
    block.fills[0].time = None;
    assert_refused(&block, "fills[0].time: missing");
    let mut block = busy.clone();
    block.fills[0].time.as_mut().unwrap().nanos = 250_000;
    assert_refused(
        &block,
        "fills[0].time: nanos 250000 is not a whole millisecond",
    );
    let mut block = busy.clone();
    block.fills[0].time.as_mut().unwrap().nanos = 1_000_000_000;
    assert_refused(
        &block,
        "fills[0].time: nanos 1000000000 is outside [0, 1e9)",
    );
    let mut block = busy.clone();
    block.fills[0].time.as_mut().unwrap().seconds = i64::MAX;
    let error = refusal(&block);
    assert!(
        error.starts_with(&format!(
            "hypercore block {BUSY_BLOCK}: fills[0].time: invalid unix timestamp"
        )),
        "{error}"
    );

    let mut block = busy.clone();
    block.events[0].time = None;
    assert_refused(&block, "events[0].time: missing");
    let mut block = busy.clone();
    block.events[0].time.as_mut().unwrap().nanos = 1_000_000_000;
    assert_refused(
        &block,
        "events[0].time: nanos 1000000000 is outside [0, 1e9)",
    );
    let mut block = busy.clone();
    block.events[0].time.as_mut().unwrap().seconds = i64::MAX / 1_000;
    assert_refused(
        &block,
        &format!(
            "events[0].time: {}s {}ns overflows i64 nanoseconds",
            i64::MAX / 1_000,
            busy.events[0].time.unwrap().nanos
        ),
    );
}

#[test]
fn r5_unsigned_values_above_i64_max_are_refused() {
    let too_big = i64::MAX as u64 + 1;
    let suffix = format!("{too_big} exceeds the Delta long range");
    let busy = fixture(BUSY_BLOCK);
    let fill_edits: [(fn(&mut pb::Fill, u64), &str); 3] = [
        (|f, v| f.order_id = v, "fills[0].order_id"),
        (|f, v| f.transaction_id = v, "fills[0].transaction_id"),
        (|f, v| f.twap_id = Some(v), "fills[0].twap_id"),
    ];
    for (edit, path) in fill_edits {
        let mut block = busy.clone();
        edit(&mut block.fills[0], too_big);
        assert_refused(&block, &format!("{path}: {suffix}"));
        // i64::MAX itself fits.
        let mut block = busy.clone();
        edit(&mut block.fills[0], i64::MAX as u64);
        map_one(&block).unwrap();
    }

    let mut block = fixture(SEND_BLOCK);
    let ledger_update_delta::Delta::Send(transfer) = delta_mut(&mut block, 0) else {
        panic!("a send");
    };
    transfer.nonce = too_big;
    assert_refused(
        &block,
        &format!("events[0].events[0].ledger_update.delta.send.nonce: {suffix}"),
    );

    let spot = fixture(1_173_546_257);
    let index = find_delta(&spot, |delta| {
        matches!(delta, ledger_update_delta::Delta::SpotTransfer(_))
    });
    let mut block = spot.clone();
    let ledger_update_delta::Delta::SpotTransfer(transfer) = delta_mut(&mut block, index) else {
        panic!("a spot transfer");
    };
    transfer.nonce = too_big;
    assert_refused(
        &block,
        &format!("events[{index}].events[0].ledger_update.delta.spot_transfer.nonce: {suffix}"),
    );

    let withdraws = fixture(846_001_240);
    let index = find_delta(&withdraws, |delta| {
        matches!(delta, ledger_update_delta::Delta::Withdraw(_))
    });
    let mut block = withdraws.clone();
    let ledger_update_delta::Delta::Withdraw(withdraw) = delta_mut(&mut block, index) else {
        panic!("a withdraw");
    };
    withdraw.nonce = too_big;
    assert_refused(
        &block,
        &format!("events[{index}].events[0].ledger_update.delta.withdraw.nonce: {suffix}"),
    );

    let gossip = fixture(GOSSIP_BLOCK);
    let slot = find_event(&gossip, |body| {
        matches!(body, event_body::Event::GossipPriorityAuctionRestart(_))
    });
    let mut block = gossip.clone();
    let event_body::Event::GossipPriorityAuctionRestart(restart) = body_mut(&mut block, slot)
    else {
        panic!("a gossip restart");
    };
    restart.slot_id = too_big;
    assert_refused(
        &block,
        &format!("events[{slot}].events[0].gossip_priority_auction_restart.slot_id: {suffix}"),
    );
}

/// A block refused on its last event appends nothing to any table, and the
/// next valid block maps normally.
#[test]
fn a_refused_block_leaves_every_table_unchanged() {
    for include_fork_step in [false, true] {
        let first = fixture(BUSY_BLOCK);
        let next = fixture(CASCADE_BLOCK);
        let mut bad = fixture(1_173_546_257);
        let last = bad.events.len() - 1;
        bad.events[last].hash = Bytes::new();

        let event = StreamEvent::new(include_fork_step.then_some("NEW"), 1);
        let map = |mapper: &mut HypercoreBlockMapper, block: &pb::Block| {
            mapper.map_block(&block.encode_to_vec(), &identity_of(block), event)
        };
        let estimates = |mapper: &mut HypercoreBlockMapper| -> Vec<(String, usize)> {
            mapper
                .table_estimates()
                .into_iter()
                .map(|(table, bytes)| (table.to_string(), bytes))
                .collect()
        };
        let mut expected = HypercoreBlockMapper::new(include_fork_step, EncodeBytes::Hex);
        let mut actual = HypercoreBlockMapper::new(include_fork_step, EncodeBytes::Hex);
        map(&mut expected, &first).unwrap();
        map(&mut actual, &first).unwrap();
        let rows = actual.total_rows();
        let before = estimates(&mut actual);
        let error = map(&mut actual, &bad).unwrap_err().to_string();
        assert!(
            error.ends_with(&format!("events[{last}].hash: empty bytes")),
            "{error}"
        );
        assert_eq!(actual.total_rows(), rows);
        assert_eq!(estimates(&mut actual), before);
        map(&mut expected, &next).unwrap();
        map(&mut actual, &next).unwrap();
        assert_eq!(actual.flush().unwrap(), expected.flush().unwrap());
    }
}

// ---------------------------------------------------------------------------
// Empty, absent and NULL
// ---------------------------------------------------------------------------

#[test]
fn empty_values_follow_the_null_and_keep_lists() {
    let busy = fixture(BUSY_BLOCK);
    let row = busy
        .fills
        .iter()
        .position(|fill| !fill.builder.is_empty())
        .expect("a fill with a builder");
    let mut block = busy.clone();
    let fill = &mut block.fills[row];
    fill.builder.clear();
    fill.builder_fee.clear();
    fill.deployer_fee.clear();
    fill.priority_gas.clear();
    fill.client_order_id = Bytes::new();
    fill.twap_id = Some(0);
    fill.liquidation = Some(pb::FillLiquidation {
        liquidated_user: Bytes::new(),
        mark_px: "2.5".to_string(),
        method: "market".to_string(),
    });
    let tables = map_one(&block).unwrap();
    let fills = View::new(&tables, "fills", &EncodeBytes::Hex);
    for column in [
        "builder",
        "builder_fee",
        "deployer_fee",
        "priority_gas",
        "client_order_id",
        "liquidated_user",
    ] {
        assert!(fills.is_null(column, row), "{column}");
    }
    assert_eq!(fills.u64("twap_id", row), Some(0));
    assert_eq!(fills.text("liquidation_method", row), Some("market"));
    assert_eq!(fills.dec("liquidation_mark_px", row), Some(25 * ONE / 10));

    // send: `fee_token` '' is NULL, the dex names '' are kept, `users = []`
    // is an empty list.
    let mut block = fixture(SEND_BLOCK);
    let ledger_update_delta::Delta::Send(transfer) = delta_mut(&mut block, 0) else {
        panic!("a send");
    };
    transfer.fee_token.clear();
    transfer.source_dex.clear();
    transfer.destination_dex.clear();
    let event_body::Event::LedgerUpdate(update) = body_mut(&mut block, 0) else {
        panic!("a ledger update");
    };
    update.users.clear();
    let tables = map_one(&block).unwrap();
    let events = View::new(&tables, "events", &EncodeBytes::Hex);
    assert!(events.is_null("fee_token", 0));
    assert_eq!(events.text("source_dex", 0), Some(""));
    assert_eq!(events.text("destination_dex", 0), Some(""));
    assert_eq!(events.bytes_list("users", 0), Some(vec![]));

    // create_sub_account: an empty name is a name.
    let mut block = fixture(1_173_744_709);
    let event_body::Event::CreateSubAccount(created) = body_mut(&mut block, 0) else {
        panic!("a sub-account creation");
    };
    created.name.clear();
    let tables = map_one(&block).unwrap();
    let events = View::new(&tables, "events", &EncodeBytes::Hex);
    assert_eq!(events.text("sub_account_name", 0), Some(""));

    // An empty funding event is a row with item_count 0; an empty position
    // list is an empty list.
    let mut block = fixture(SMALL_BLOCK);
    let event = synthetic_event(
        &block,
        event_body::Event::Funding(pb::Funding { deltas: vec![] }),
    );
    block.events.insert(0, event);
    let tables = map_one(&block).unwrap();
    let events = View::new(&tables, "events", &EncodeBytes::Hex);
    assert_eq!(events.u32("item_count", 0), Some(0));
    assert_eq!(tables["funding_deltas"].num_rows(), 0);
    let mut block = fixture(CASCADE_BLOCK);
    let ledger_update_delta::Delta::Liquidation(liquidation) = delta_mut(&mut block, 0) else {
        panic!("a liquidation");
    };
    liquidation.liquidated_positions.clear();
    let tables = map_one(&block).unwrap();
    let events = View::new(&tables, "events", &EncodeBytes::Hex);
    assert_eq!(events.positions("liquidated_positions", 0), Some(vec![]));
}

// ---------------------------------------------------------------------------
// Labels, entry points, Bloom filters
// ---------------------------------------------------------------------------

#[test]
fn every_enum_value_maps_to_its_documented_label() {
    let mut block = fixture(BUSY_BLOCK);
    let template = block.fills[0].clone();
    block.fills = (1..=22)
        .map(|direction| pb::Fill {
            direction,
            side: if direction % 2 == 0 { 2 } else { 1 },
            ..template.clone()
        })
        .collect();
    let tables = map_one(&block).unwrap();
    let fills = View::new(&tables, "fills", &EncodeBytes::Hex);
    let directions: Vec<&str> = (0..22)
        .map(|row| fills.text("direction", row).unwrap())
        .collect();
    assert_eq!(directions, DIRECTIONS);
    assert_eq!(fills.text("side", 0), Some("ASK"));
    assert_eq!(fills.text("side", 1), Some("BUY"));
    assert_eq!(
        fills.column("direction").data_type(),
        &firehose_parquet::traits::enum_data_type()
    );

    let cascade = fixture(CASCADE_BLOCK);
    for (leverage, label) in [(1, "CROSS"), (2, "ISOLATED")] {
        let mut block = cascade.clone();
        let ledger_update_delta::Delta::Liquidation(liquidation) = delta_mut(&mut block, 0) else {
            panic!("a liquidation");
        };
        liquidation.leverage_type = leverage;
        let tables = map_one(&block).unwrap();
        let events = View::new(&tables, "events", &EncodeBytes::Hex);
        assert_eq!(events.text("leverage_type", 0), Some(label));
    }
}

/// The owned-buffer path maps every fixture exactly like the borrowed one.
#[test]
fn owned_and_borrowed_payloads_map_alike() {
    let mut borrowed = HypercoreBlockMapper::new(true, EncodeBytes::Hex);
    let mut owned = HypercoreBlockMapper::new(true, EncodeBytes::Hex);
    for (number, payload) in real_blocks() {
        if *number == FUNDING_BLOCK {
            continue;
        }
        let identity = identity_of(&decode(payload));
        let event = StreamEvent::new(Some("NEW"), *number);
        let a = borrowed.map_block(payload, &identity, event).unwrap();
        let b = owned
            .map_block_bytes(Bytes::from(payload.clone()), &identity, event)
            .unwrap();
        assert_eq!(a, b);
    }
    assert_eq!(borrowed.flush().unwrap(), owned.flush().unwrap());
}

/// The account and validator lookups get Bloom filters
/// (`docs/output-layout.md`, "Parquet lookup metadata"); `users`, a list, does
/// not.
#[test]
fn lookup_columns_get_bloom_filters() {
    use firehose_parquet::config::Compression;
    use parquet::schema::types::ColumnPath;
    let expected: [(&str, &[&str]); 5] = [
        ("blocks", &[]),
        ("fills", &["user", "hash", "liquidated_user"]),
        (
            "events",
            &[
                "hash",
                "user",
                "destination",
                "vault",
                "validator",
                "sub_account",
            ],
        ),
        ("funding_deltas", &["user"]),
        ("validator_rewards", &["validator"]),
    ];
    let mut mapper = HypercoreBlockMapper::new(false, EncodeBytes::Hex);
    let tables = mapper.flush().unwrap();
    for (table, columns) in expected {
        let batch = &tables[table];
        let properties =
            firehose_parquet::writer::properties::for_batch(Compression::Zstd, batch, None)
                .unwrap();
        let filtered: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| {
                properties
                    .bloom_filter_properties(&ColumnPath::from(name.as_str()))
                    .is_some()
            })
            .collect();
        assert_eq!(filtered, columns, "{table}");
    }
}
