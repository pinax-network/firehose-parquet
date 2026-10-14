//! The reference tables: their columns, and the rows built from `/info`
//! payloads.
//!
//! Every reference table ends with `observed_at` (when `/info` answered),
//! `source` (the request), `payload_sha256` and `payload_json` (the canonical
//! JSON of the entity as `/info` returned it). A row is appended only when an
//! entity's payload hash differs from its latest row, so each table is an
//! append-only change log, and `payload_json` keeps the fields that have no
//! typed column yet. A later release adds a typed column only as a nullable
//! column after `payload_json`.
//!
//! Snapshot tables (all but `hl_spot_token_details` and `hl_funding_history`)
//! also have `listed`: true when the entity was in the snapshot that produced
//! the row. When an entity leaves its snapshot, one row records it
//! ([`removal_row`]): `listed = false`, no payload, the key and typed columns
//! copied from its previous row. Settled outcomes and questions are
//! `listed = false` (they left outcomeMeta).
//!
//! Nothing a response carries is dropped: `hl_spot_meta` and `hl_outcome_meta`
//! keep the fields of spotMeta and outcomeMeta beside their entity lists, and a
//! dex's payload is its whole perpDexs entry and allPerpMetas object but the
//! universe (whose assets are `hl_perp_assets` rows). Columns derived from
//! context (a live outcome's question, a pair's symbols, a dex's collateral
//! symbol, an asset's dex) are not hashed.
//!
//! The vocabulary is the HyperCore lake's: dex `""` is the default dex, coins
//! are `@<pair>`, `PURR/USDC`, `<dex>:<SYM>` and `#<10 * outcome_id + side>`.

use std::collections::HashMap;

use self::ColumnType as T;
use crate::json::{payload, Json};
use crate::values::{
    asset_id, dex_of, from_millis, id_list, parse_bool, parse_decimal, parse_int, parse_kv,
    parse_naive_utc, parse_text, valid_id, Ordered,
};

/// A column's type; [`crate::arrow`] maps each to its Delta and Arrow types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnType {
    Long,
    Text,
    Bool,
    /// `decimal(38,10)`.
    Decimal,
    /// Microseconds, UTC.
    Timestamp,
    /// `map<string,string>`, in source order.
    Kv,
    TextList,
    LongList,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: &'static str,
    pub ty: ColumnType,
    pub nullable: bool,
}

/// NOT NULL: a key column or a tail column but the payload.
const fn required(name: &'static str, ty: ColumnType) -> Column {
    Column {
        name,
        ty,
        nullable: false,
    }
}

const fn nullable(name: &'static str, ty: ColumnType) -> Column {
    Column {
        name,
        ty,
        nullable: true,
    }
}

const LISTED: Column = required("listed", ColumnType::Bool);
const OBSERVED_AT: Column = required("observed_at", ColumnType::Timestamp);
const SOURCE: Column = required("source", ColumnType::Text);
const PAYLOAD_SHA256: Column = nullable("payload_sha256", ColumnType::Text);
const PAYLOAD_JSON: Column = nullable("payload_json", ColumnType::Text);

/// The columns every reference table ends with; a removal row copies the
/// others.
const TAIL: [&str; 5] = [
    "listed",
    "observed_at",
    "source",
    "payload_sha256",
    "payload_json",
];

/// A table's key and typed columns, then its tail: with `listed` for a
/// snapshot table, without for an entity table.
macro_rules! columns {
    ($($column:expr),+ $(,)?; snapshot) => {
        &[$($column,)+ LISTED, OBSERVED_AT, SOURCE, PAYLOAD_SHA256, PAYLOAD_JSON]
    };
    ($($column:expr),+ $(,)?; entity) => {
        &[$($column,)+ OBSERVED_AT, SOURCE, PAYLOAD_SHA256, PAYLOAD_JSON]
    };
}

/// A table: its path under the root, its key and its columns in order (the
/// order is the contract).
#[derive(Debug, PartialEq, Eq)]
pub struct TableSpec {
    /// `hl_outcomes`, or `_hlref/outcome_probes` for the private one.
    pub name: &'static str,
    pub key: &'static [&'static str],
    pub columns: &'static [Column],
}

impl TableSpec {
    /// Whether the table records snapshots: it has `listed`.
    pub fn snapshot(&self) -> bool {
        self.position("listed").is_some()
    }

    /// The position of `column`, if the table has it.
    pub fn position(&self, column: &str) -> Option<usize> {
        self.columns.iter().position(|known| known.name == column)
    }

    /// The position of `column`. Panics on a name the table lacks: the row
    /// builders name only their own table's columns (the tests build every
    /// row).
    pub fn index(&self, column: &str) -> usize {
        self.position(column)
            .unwrap_or_else(|| panic!("{} has no column {column}", self.name))
    }

    /// The positions of the key columns.
    pub fn key_columns(&self) -> impl Iterator<Item = usize> + '_ {
        self.key.iter().map(|column| self.index(column))
    }

    /// The positions of the columns a removal row copies: all but the key and
    /// the tail.
    pub fn typed_columns(&self) -> impl Iterator<Item = usize> + '_ {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, column)| !self.key.contains(&column.name) && !TAIL.contains(&column.name))
            .map(|(index, _)| index)
    }

    /// The Delta table's name: the last path segment (`outcome_probes`).
    pub fn delta_name(&self) -> &'static str {
        self.name.rsplit('/').next().unwrap_or(self.name)
    }
}

/// spotMeta's own fields: the response without its tokens and universe (today
/// an empty object). One entity, keyed by the request.
pub static SPOT_META: TableSpec = TableSpec {
    name: "hl_spot_meta",
    key: &["request"],
    columns: columns![required("request", T::Text); snapshot],
};

pub static SPOT_TOKENS: TableSpec = TableSpec {
    name: "hl_spot_tokens",
    key: &["token_index"],
    columns: columns![
        required("token_index", T::Long),
        nullable("name", T::Text),
        nullable("full_name", T::Text),
        nullable("token_id", T::Text),
        nullable("sz_decimals", T::Long),
        nullable("wei_decimals", T::Long),
        nullable("evm_contract", T::Text),
        nullable("evm_extra_wei_decimals", T::Long),
        nullable("is_canonical", T::Bool),
        nullable("deployer_trading_fee_share", T::Decimal);
        snapshot
    ],
};

/// tokenDetails, read once per token: a row when `/info` answers it.
pub static SPOT_TOKEN_DETAILS: TableSpec = TableSpec {
    name: "hl_spot_token_details",
    key: &["token_index"],
    columns: columns![
        required("token_index", T::Long),
        nullable("token_id", T::Text),
        nullable("deployer", T::Text),
        nullable("deploy_time", T::Timestamp),
        nullable("deploy_gas", T::Decimal);
        entity
    ],
};

pub static SPOT_PAIRS: TableSpec = TableSpec {
    name: "hl_spot_pairs",
    key: &["pair_index"],
    columns: columns![
        required("pair_index", T::Long),
        nullable("coin", T::Text),
        nullable("name", T::Text),
        nullable("base_token_index", T::Long),
        nullable("quote_token_index", T::Long),
        nullable("base_symbol", T::Text),
        nullable("quote_symbol", T::Text),
        nullable("is_canonical", T::Bool);
        snapshot
    ],
};

pub static PERP_DEXS: TableSpec = TableSpec {
    name: "hl_perp_dexs",
    key: &["dex_index"],
    columns: columns![
        required("dex_index", T::Long),
        nullable("name", T::Text),
        nullable("full_name", T::Text),
        nullable("deployer", T::Text),
        nullable("oracle_updater", T::Text),
        nullable("fee_recipient", T::Text),
        nullable("collateral_token_index", T::Long),
        nullable("collateral_symbol", T::Text);
        snapshot
    ],
};

pub static PERP_ASSETS: TableSpec = TableSpec {
    name: "hl_perp_assets",
    key: &["dex_index", "asset_index"],
    columns: columns![
        required("dex_index", T::Long),
        required("asset_index", T::Long),
        nullable("dex", T::Text),
        nullable("coin", T::Text),
        nullable("asset_id", T::Long),
        nullable("sz_decimals", T::Long),
        nullable("max_leverage", T::Long),
        nullable("margin_table_id", T::Long),
        nullable("is_delisted", T::Bool),
        nullable("only_isolated", T::Bool),
        nullable("margin_mode", T::Text),
        nullable("growth_mode", T::Text),
        nullable("deployer_fee_scale", T::Decimal);
        snapshot
    ],
};

/// outcomeMeta's own fields: the response without its outcomes and questions
/// (the venues' deployers and sub-deployers, feeScale). One entity, keyed by
/// the request.
pub static OUTCOME_META: TableSpec = TableSpec {
    name: "hl_outcome_meta",
    key: &["request"],
    columns: columns![
        required("request", T::Text),
        nullable("fee_scale", T::Decimal),
        nullable("venue_deployers", T::Kv);
        snapshot
    ],
};

pub static OUTCOMES: TableSpec = TableSpec {
    name: "hl_outcomes",
    key: &["outcome_id"],
    columns: columns![
        required("outcome_id", T::Long),
        nullable("question_id", T::Long),
        nullable("name", T::Text),
        nullable("description", T::Text),
        nullable("description_kv", T::Kv),
        nullable("side_names", T::TextList),
        nullable("side_coins", T::TextList),
        nullable("quote_token", T::Text),
        nullable("venue", T::Text),
        nullable("deployer_fee_scale", T::Decimal),
        nullable("status", T::Text),
        nullable("settle_fraction", T::Decimal),
        nullable("settle_details", T::Text);
        snapshot
    ],
};

pub static QUESTIONS: TableSpec = TableSpec {
    name: "hl_questions",
    key: &["question_id"],
    columns: columns![
        required("question_id", T::Long),
        nullable("name", T::Text),
        nullable("description", T::Text),
        nullable("description_kv", T::Kv),
        nullable("fallback_outcome_id", T::Long),
        nullable("named_outcome_ids", T::LongList),
        nullable("settled_outcome_ids", T::LongList),
        nullable("status", T::Text);
        snapshot
    ],
};

pub static FUNDING_HISTORY: TableSpec = TableSpec {
    name: "hl_funding_history",
    key: &["coin", "funding_time"],
    columns: columns![
        required("coin", T::Text),
        required("funding_time", T::Timestamp),
        nullable("dex", T::Text),
        nullable("funding_rate", T::Decimal),
        nullable("premium", T::Decimal);
        entity
    ],
};

/// Writer-only state, outside the public table prefixes: which outcome ids the
/// settledOutcome sweep probed and what it found, so ids that are not settled
/// yet are probed again after a cool-down instead of on every run. No tail.
pub static OUTCOME_PROBES: TableSpec = TableSpec {
    name: "_hlref/outcome_probes",
    key: &["outcome_id", "probed_at"],
    columns: &[
        required("outcome_id", T::Long),
        required("probed_at", T::Timestamp),
        required("result", T::Text),
        nullable("run_id", T::Text),
    ],
};

/// The ten public tables: the views read them, verify checks them.
pub static REFERENCE_TABLES: [&TableSpec; 10] = [
    &SPOT_META,
    &SPOT_TOKENS,
    &SPOT_TOKEN_DETAILS,
    &SPOT_PAIRS,
    &PERP_DEXS,
    &PERP_ASSETS,
    &OUTCOME_META,
    &OUTCOMES,
    &QUESTIONS,
    &FUNDING_HISTORY,
];

/// Every table the job writes: the reference tables and the probe state.
pub static ALL_TABLES: [&TableSpec; 11] = [
    &SPOT_META,
    &SPOT_TOKENS,
    &SPOT_TOKEN_DETAILS,
    &SPOT_PAIRS,
    &PERP_DEXS,
    &PERP_ASSETS,
    &OUTCOME_META,
    &OUTCOMES,
    &QUESTIONS,
    &FUNDING_HISTORY,
    &OUTCOME_PROBES,
];

/// A typed value of a row; [`Cell::Null`] is NULL whatever the column's type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell {
    Null,
    Long(i64),
    Text(String),
    Bool(bool),
    /// Scaled by 10^10.
    Decimal(i128),
    /// Microseconds since the epoch, UTC.
    Timestamp(i64),
    Kv(Vec<(String, String)>),
    TextList(Vec<Option<String>>),
    LongList(Vec<Option<i64>>),
}

fn cell<V>(value: Option<V>, make: impl FnOnce(V) -> Cell) -> Cell {
    value.map_or(Cell::Null, make)
}

/// A row of a table: one cell per column, in the table's order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub spec: &'static TableSpec,
    pub cells: Vec<Cell>,
}

/// A part of a row's key.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeyPart {
    Long(i64),
    Text(String),
    Timestamp(i64),
}

/// A row's key, in the table's key order.
pub type Key = Vec<KeyPart>;

impl Row {
    /// A row of NULLs.
    pub fn new(spec: &'static TableSpec) -> Row {
        Row {
            spec,
            cells: vec![Cell::Null; spec.columns.len()],
        }
    }

    /// A row holding `cells` in `columns`, NULL in the others: a row read
    /// back from the table.
    pub fn of(spec: &'static TableSpec, columns: &[&str], cells: &[Cell]) -> Row {
        let mut row = Row::new(spec);
        for (column, cell) in columns.iter().zip(cells) {
            row.set(column, cell.clone());
        }
        row
    }

    pub fn set(&mut self, column: &str, cell: Cell) {
        let index = self.spec.index(column);
        self.cells[index] = cell;
    }

    pub fn get(&self, column: &str) -> &Cell {
        &self.cells[self.spec.index(column)]
    }

    /// The row's key. Panics on a NULL key: the builders key every row by a
    /// valid id, and the tables' key columns are NOT NULL.
    pub fn key(&self) -> Key {
        self.spec
            .key_columns()
            .map(|index| match &self.cells[index] {
                Cell::Long(value) => KeyPart::Long(*value),
                Cell::Text(value) => KeyPart::Text(value.clone()),
                Cell::Timestamp(value) => KeyPart::Timestamp(*value),
                other => panic!(
                    "{}: key column {} holds {other:?}",
                    self.spec.name, self.spec.columns[index].name
                ),
            })
            .collect()
    }

    /// The payload's hash: `None` on a removal row (and in tables without one).
    pub fn payload_sha256(&self) -> Option<&str> {
        match self
            .spec
            .position("payload_sha256")
            .map(|index| &self.cells[index])
        {
            Some(Cell::Text(digest)) => Some(digest),
            _ => None,
        }
    }

    /// `listed` of a snapshot table's row.
    pub fn listed(&self) -> Option<bool> {
        match self.spec.position("listed").map(|index| &self.cells[index]) {
            Some(Cell::Bool(listed)) => Some(*listed),
            _ => None,
        }
    }

    /// Sets the tail: `listed` on a snapshot table, the time, the request and
    /// the payload.
    fn with_payload(mut self, value: &Json, observed_at: i64, source: &str, listed: bool) -> Row {
        let (text, digest) = payload(value);
        if self.spec.snapshot() {
            self.set("listed", Cell::Bool(listed));
        }
        self.set("observed_at", Cell::Timestamp(observed_at));
        self.set("source", Cell::Text(source.to_owned()));
        self.set("payload_sha256", Cell::Text(digest));
        self.set("payload_json", Cell::Text(text));
        self
    }
}

/// What a settledOutcome probe found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeResult {
    /// A settlement: the outcome's settled row is written.
    Settled,
    /// `null`: the outcome is live or does not exist (yet).
    NotSettled,
    /// HTTP 500 after retries: cooled down like an unsettled id.
    Error,
}

impl ProbeResult {
    pub fn as_str(self) -> &'static str {
        match self {
            ProbeResult::Settled => "settled",
            ProbeResult::NotSettled => "not_settled",
            ProbeResult::Error => "error",
        }
    }
}

static NULL: Json = Json::Null;

/// The value at `key`, or JSON null (a missing object reads as one with no
/// keys).
fn field<'a>(value: &'a Json, key: &str) -> &'a Json {
    value.get(key).unwrap_or(&NULL)
}

fn text(value: &Json, key: &str) -> Cell {
    cell(parse_text(value.get(key)), Cell::Text)
}

fn long(value: &Json, key: &str) -> Cell {
    cell(parse_int(value.get(key)), Cell::Long)
}

fn boolean(value: &Json, key: &str) -> Cell {
    cell(parse_bool(value.get(key)), Cell::Bool)
}

fn decimal(value: &Json, key: &str) -> Cell {
    cell(parse_decimal(value.get(key)), Cell::Decimal)
}

/// A description, and its `k:v|k:v` map.
fn description(row: &mut Row, value: &Json) {
    let description = parse_text(value.get("description"));
    row.set(
        "description_kv",
        cell(parse_kv(description.as_deref()), Cell::Kv),
    );
    row.set("description", cell(description, Cell::Text));
}

/// hl_spot_meta: spotMeta without its tokens and universe.
pub fn spot_meta_row(spot: &Json, at: i64) -> Row {
    let mut row = Row::new(&SPOT_META);
    row.set("request", Cell::Text("spotMeta".into()));
    row.with_payload(&spot.without(&["tokens", "universe"]), at, "spotMeta", true)
}

/// hl_spot_tokens: a spotMeta token (an object with a valid `index`).
pub fn spot_token_row(token: &Json, at: i64) -> Row {
    let evm = token
        .get("evmContract")
        .filter(|evm| evm.as_obj().is_some())
        .unwrap_or(&NULL);
    let mut row = Row::new(&SPOT_TOKENS);
    row.set("token_index", long(token, "index"));
    row.set("name", text(token, "name"));
    row.set("full_name", text(token, "fullName"));
    row.set("token_id", text(token, "tokenId"));
    row.set("sz_decimals", long(token, "szDecimals"));
    row.set("wei_decimals", long(token, "weiDecimals"));
    row.set("evm_contract", text(evm, "address"));
    // HyperLiquid's key is snake_case.
    row.set(
        "evm_extra_wei_decimals",
        long(evm, "evm_extra_wei_decimals"),
    );
    row.set("is_canonical", boolean(token, "isCanonical"));
    row.set(
        "deployer_trading_fee_share",
        decimal(token, "deployerTradingFeeShare"),
    );
    row.with_payload(token, at, "spotMeta", true)
}

/// hl_spot_token_details: the tokenDetails response of a token, asked with
/// its spotMeta `tokenId`.
pub fn token_details_row(token_index: i64, token_id: &str, response: &Json, at: i64) -> Row {
    let mut row = Row::new(&SPOT_TOKEN_DETAILS);
    row.set("token_index", Cell::Long(token_index));
    row.set("token_id", Cell::Text(token_id.to_owned()));
    row.set("deployer", text(response, "deployer"));
    row.set(
        "deploy_time",
        cell(parse_naive_utc(response.get("deployTime")), Cell::Timestamp),
    );
    row.set("deploy_gas", decimal(response, "deployGas"));
    row.with_payload(response, at, "tokenDetails", false)
}

/// The `name` of a token of this spotMeta.
fn symbol(tokens: &Ordered<i64, &Json>, index: Option<i64>) -> Option<String> {
    parse_text(tokens.get(&index?)?.get("name"))
}

/// hl_spot_pairs: a spotMeta pair. Its symbols and name come from this
/// spotMeta's tokens; `coin` is the lake's fill coin (`PURR/USDC`, `@107`).
pub fn spot_pair_row(pair: &Json, tokens: &Ordered<i64, &Json>, at: i64) -> Row {
    let indices = field(pair, "tokens")
        .as_arr()
        .filter(|indices| indices.len() == 2);
    let base_index = indices.and_then(|indices| indices[0].as_i64());
    let quote_index = indices.and_then(|indices| indices[1].as_i64());
    let base = symbol(tokens, base_index);
    let quote = symbol(tokens, quote_index);
    let name = match (&base, &quote) {
        (Some(base), Some(quote)) if !base.is_empty() && !quote.is_empty() => {
            Some(format!("{base}/{quote}"))
        }
        _ => None,
    };
    let mut row = Row::new(&SPOT_PAIRS);
    row.set("pair_index", long(pair, "index"));
    row.set("coin", text(pair, "name"));
    row.set("name", cell(name, Cell::Text));
    row.set("base_token_index", cell(base_index, Cell::Long));
    row.set("quote_token_index", cell(quote_index, Cell::Long));
    row.set("base_symbol", cell(base, Cell::Text));
    row.set("quote_symbol", cell(quote, Cell::Text));
    row.set("is_canonical", boolean(pair, "isCanonical"));
    row.with_payload(pair, at, "spotMeta", true)
}

/// hl_perp_dexs: dex `dex`, from its perpDexs entry (null for the default dex)
/// and its allPerpMetas object. The payload is both, without the universe; a
/// perpDexs entry that is not an object is null in it.
pub fn perp_dex_row(
    dex: i64,
    entry: &Json,
    meta: &Json,
    tokens: &Ordered<i64, &Json>,
    at: i64,
) -> Row {
    let entry = if entry.as_obj().is_some() {
        entry.clone()
    } else {
        Json::Null
    };
    let collateral = parse_int(meta.get("collateralToken"));
    let mut row = Row::new(&PERP_DEXS);
    row.set("dex_index", Cell::Long(dex));
    // As the lake's `fills.dex`.
    row.set(
        "name",
        if dex == 0 {
            Cell::Text(String::new())
        } else {
            text(&entry, "name")
        },
    );
    row.set("full_name", text(&entry, "fullName"));
    row.set("deployer", text(&entry, "deployer"));
    row.set("oracle_updater", text(&entry, "oracleUpdater"));
    row.set("fee_recipient", text(&entry, "feeRecipient"));
    row.set("collateral_token_index", cell(collateral, Cell::Long));
    row.set(
        "collateral_symbol",
        cell(symbol(tokens, collateral), Cell::Text),
    );
    let value = Json::obj([
        ("allPerpMetas", meta.without(&["universe"])),
        ("perpDexs", entry),
    ]);
    row.with_payload(&value, at, "perpDexs+allPerpMetas", true)
}

/// hl_perp_assets: item `asset` of dex `dex`'s universe; `dex_name` is this
/// run's hl_perp_dexs name of the dex. An absent `isDelisted` is false, a
/// present one that is not a boolean NULL.
pub fn perp_asset_row(dex: i64, dex_name: Option<&str>, asset: i64, value: &Json, at: i64) -> Row {
    let delisted = match value.get("isDelisted") {
        None => Some(false),
        present => parse_bool(present),
    };
    let mut row = Row::new(&PERP_ASSETS);
    row.set("dex_index", Cell::Long(dex));
    row.set("asset_index", Cell::Long(asset));
    row.set("dex", cell(dex_name.map(str::to_owned), Cell::Text));
    row.set("coin", text(value, "name"));
    row.set("asset_id", cell(asset_id(dex, asset), Cell::Long));
    row.set("sz_decimals", long(value, "szDecimals"));
    row.set("max_leverage", long(value, "maxLeverage"));
    row.set("margin_table_id", long(value, "marginTableId"));
    row.set("is_delisted", cell(delisted, Cell::Bool));
    row.set("only_isolated", boolean(value, "onlyIsolated"));
    row.set("margin_mode", text(value, "marginMode"));
    row.set("growth_mode", text(value, "growthMode"));
    row.set("deployer_fee_scale", decimal(value, "deployerFeeScale"));
    row.with_payload(value, at, "allPerpMetas", true)
}

/// hl_outcome_meta: outcomeMeta without its outcomes and questions.
/// `venue_deployers` maps each `deployers` item's venue to its deployer, in
/// list order: NULL unless every item has a string venue and deployer and the
/// venues are distinct (an empty list is an empty map).
pub fn outcome_meta_row(meta: &Json, at: i64) -> Row {
    let value = meta.without(&["outcomes", "questions"]);
    let venue_deployers = value
        .get("deployers")
        .and_then(Json::as_arr)
        .and_then(|items| {
            let mut pairs: Vec<(String, String)> = Vec::new();
            for item in items {
                let venue = parse_text(item.get("venue"))?;
                let deployer = parse_text(item.get("deployer"))?;
                if pairs.iter().any(|(seen, _)| *seen == venue) {
                    return None;
                }
                pairs.push((venue, deployer));
            }
            Some(pairs)
        });
    let mut row = Row::new(&OUTCOME_META);
    row.set("request", Cell::Text("outcomeMeta".into()));
    row.set("fee_scale", decimal(&value, "feeScale"));
    row.set("venue_deployers", cell(venue_deployers, Cell::Kv));
    row.with_payload(&value, at, "outcomeMeta", true)
}

/// The side names of an outcome spec: each `sideSpecs` item's name (NULL for
/// an item that is not an object); NULL without a `sideSpecs` list.
fn side_names(spec: &Json) -> Option<Vec<Option<String>>> {
    let sides = spec.get("sideSpecs")?.as_arr()?;
    Some(
        sides
            .iter()
            .map(|side| side.as_obj().and_then(|_| parse_text(side.get("name"))))
            .collect(),
    )
}

/// The lake coins of an outcome's sides, `#<10 * outcome_id + side>`: NULL
/// above ten sides, which that coin cannot encode.
fn side_coins(outcome_id: i64, sides: usize) -> Option<Vec<Option<String>>> {
    if sides > 10 {
        return None;
    }
    (0..sides as i64)
        .map(|side| {
            let coin = outcome_id.checked_mul(10)?.checked_add(side)?;
            Some(Some(format!("#{coin}")))
        })
        .collect()
}

/// The columns of hl_outcomes an outcome spec gives: an outcomeMeta item, or
/// a settledOutcome response's `spec`.
fn outcome_row(spec: &Json) -> Row {
    let outcome_id = parse_int(spec.get("outcome"));
    let sides = side_names(spec);
    let coins = outcome_id
        .zip(sides.as_ref())
        .and_then(|(outcome_id, sides)| side_coins(outcome_id, sides.len()));
    let mut row = Row::new(&OUTCOMES);
    row.set("outcome_id", cell(outcome_id, Cell::Long));
    row.set("name", text(spec, "name"));
    description(&mut row, spec);
    row.set("side_names", cell(sides, Cell::TextList));
    row.set("side_coins", cell(coins, Cell::TextList));
    row.set("quote_token", text(spec, "quoteToken"));
    row.set("venue", text(spec, "venue"));
    row.set("deployer_fee_scale", decimal(spec, "deployerFeeScale"));
    row
}

/// hl_outcomes: a live outcome of outcomeMeta, in question `question_id`
/// ([`membership`]; not hashed).
pub fn outcome_live_row(item: &Json, question_id: Option<i64>, at: i64) -> Row {
    let mut row = outcome_row(item);
    row.set("question_id", cell(question_id, Cell::Long));
    row.set("status", Cell::Text("live".into()));
    row.with_payload(item, at, "outcomeMeta", true)
}

/// The question of a settledOutcome response: the first of its wrapper's
/// `question.settled` and `question.live` that is an integer.
pub fn settled_question_id(response: &Json) -> Option<i64> {
    let state = response.get("question")?.get("question")?;
    state.as_obj()?;
    ["settled", "live"]
        .into_iter()
        .find_map(|key| parse_int(state.get(key)))
}

/// hl_outcomes: a settled outcome, from its whole settledOutcome response.
pub fn outcome_settled_row(response: &Json, at: i64) -> Row {
    let mut row = outcome_row(field(response, "spec"));
    row.set(
        "question_id",
        cell(settled_question_id(response), Cell::Long),
    );
    row.set("status", Cell::Text("settled".into()));
    row.set("settle_fraction", decimal(response, "settleFraction"));
    row.set("settle_details", text(response, "details"));
    row.with_payload(response, at, "settledOutcome", false)
}

/// hl_questions: a listed question of outcomeMeta.
pub fn question_live_row(item: &Json, at: i64) -> Row {
    let mut row = Row::new(&QUESTIONS);
    row.set("question_id", long(item, "question"));
    row.set("name", text(item, "name"));
    description(&mut row, item);
    row.set("fallback_outcome_id", long(item, "fallbackOutcome"));
    row.set("named_outcome_ids", ids(item, "namedOutcomes"));
    row.set("settled_outcome_ids", ids(item, "settledNamedOutcomes"));
    row.set("status", Cell::Text("live".into()));
    row.with_payload(item, at, "outcomeMeta", true)
}

fn ids(value: &Json, key: &str) -> Cell {
    cell(id_list(value.get(key)), |ids| {
        Cell::LongList(ids.into_iter().map(Some).collect())
    })
}

/// hl_questions: the question a settledOutcome response carries (its
/// `question` wrapper), for a question that left outcomeMeta. Only its name,
/// description and state are known: the id lists are NULL.
pub fn question_settled_row(response: &Json, at: i64) -> Row {
    let wrapper = field(response, "question");
    let settled = parse_int(field(wrapper, "question").get("settled")).is_some();
    let mut row = Row::new(&QUESTIONS);
    row.set(
        "question_id",
        cell(settled_question_id(response), Cell::Long),
    );
    row.set("name", text(wrapper, "name"));
    description(&mut row, wrapper);
    row.set(
        "status",
        Cell::Text(if settled { "settled" } else { "live" }.into()),
    );
    row.with_payload(wrapper, at, "settledOutcome", false)
}

/// hl_funding_history: a fundingHistory entry. `funding_time` is the lake's
/// funding block `timestamp`, to the millisecond.
pub fn funding_row(entry: &Json, at: i64) -> Row {
    let coin = parse_text(entry.get("coin"));
    let mut row = Row::new(&FUNDING_HISTORY);
    row.set("dex", cell(dex_of(coin.as_deref()), Cell::Text));
    row.set("coin", cell(coin, Cell::Text));
    row.set(
        "funding_time",
        cell(
            parse_int(entry.get("time")).and_then(from_millis),
            Cell::Timestamp,
        ),
    );
    row.set("funding_rate", decimal(entry, "fundingRate"));
    row.set("premium", decimal(entry, "premium"));
    row.with_payload(entry, at, "fundingHistory", false)
}

/// `_hlref/outcome_probes`: one settledOutcome probe.
pub fn probe_row(outcome_id: i64, probed_at: i64, result: ProbeResult, run_id: &str) -> Row {
    let mut row = Row::new(&OUTCOME_PROBES);
    row.set("outcome_id", Cell::Long(outcome_id));
    row.set("probed_at", Cell::Timestamp(probed_at));
    row.set("result", Cell::Text(result.as_str().into()));
    row.set("run_id", Cell::Text(run_id.to_owned()));
    row
}

/// The row recording that an entity left its snapshot: its key and typed
/// columns as `previous` (its latest row, read back) holds them,
/// `listed = false`, this snapshot's time and request, and no payload.
pub fn removal_row(previous: &Row, at: i64, source: &str) -> Row {
    let spec = previous.spec;
    let mut row = Row::new(spec);
    for index in spec.key_columns().chain(spec.typed_columns()) {
        row.cells[index] = previous.cells[index].clone();
    }
    row.set("listed", Cell::Bool(false));
    row.set("observed_at", Cell::Timestamp(at));
    row.set("source", Cell::Text(source.to_owned()));
    row
}

/// Each live outcome's question in this outcomeMeta: the listed questions in
/// their order, each naming its `namedOutcomes` then its `fallbackOutcome`, so
/// a later question in the list wins an outcome two of them name, whatever
/// their ids. A `namedOutcomes` that is not a list names none.
pub fn membership(questions: &Ordered<i64, &Json>) -> HashMap<i64, i64> {
    let mut members = HashMap::new();
    for (question_id, question) in questions.iter() {
        let named = question
            .get("namedOutcomes")
            .and_then(Json::as_arr)
            .unwrap_or_default();
        for outcome in named.iter().chain(question.get("fallbackOutcome")) {
            if let Some(outcome_id) = valid_id(Some(outcome)) {
                members.insert(outcome_id, *question_id);
            }
        }
    }
    members
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::sha256_hex;

    /// 2026-10-08T16:50:00Z, the golden rows' time.
    const AT: i64 = 1_791_478_200_000_000;
    const ONE: i128 = 10_000_000_000;

    fn fixture(name: &str) -> Json {
        let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
        Json::parse(&std::fs::read(path).unwrap()).unwrap()
    }

    fn text(value: &str) -> Cell {
        Cell::Text(value.into())
    }

    fn texts(values: &[&str]) -> Cell {
        Cell::TextList(values.iter().map(|value| Some(value.to_string())).collect())
    }

    fn kv(pairs: &[(&str, &str)]) -> Cell {
        Cell::Kv(
            pairs
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
    }

    fn check_payload(row: &Row, value: &Json) {
        assert_eq!(row.get("payload_json"), &Cell::Text(value.canonical()));
        assert_eq!(
            row.payload_sha256(),
            Some(sha256_hex(&value.canonical()).as_str())
        );
    }

    fn tokens(spot: &Json) -> Ordered<i64, &Json> {
        spot.get("tokens")
            .and_then(Json::as_arr)
            .unwrap()
            .iter()
            .map(|token| (token.get("index").unwrap().as_i64().unwrap(), token))
            .collect()
    }

    fn item<'a>(list: &'a Json, key: &str, id: i64) -> &'a Json {
        list.as_arr()
            .unwrap()
            .iter()
            .find(|item| item.get(key).and_then(Json::as_i64) == Some(id))
            .unwrap()
    }

    #[test]
    fn spot_pairs_use_the_lake_coin() {
        let spot = fixture("spotMeta.json");
        let tokens = tokens(&spot);
        let pairs = spot.get("universe").unwrap();
        let purr = spot_pair_row(item(pairs, "index", 0), &tokens, AT);
        for (column, value) in [
            ("coin", "PURR/USDC"),
            ("name", "PURR/USDC"),
            ("base_symbol", "PURR"),
            ("quote_symbol", "USDC"),
        ] {
            assert_eq!(purr.get(column), &text(value), "{column}");
        }
        let hype = spot_pair_row(item(pairs, "index", 107), &tokens, AT);
        assert_eq!(hype.get("coin"), &text("@107"));
        assert_eq!(hype.get("name"), &text("HYPE/USDC"));
        assert_eq!(hype.get("base_token_index"), &Cell::Long(150));
        assert_eq!(hype.get("quote_token_index"), &Cell::Long(0));
        let usdh: Vec<Row> = pairs
            .as_arr()
            .unwrap()
            .iter()
            .filter(|pair| pair.get("tokens").unwrap().as_arr().unwrap()[1].as_i64() == Some(360))
            .map(|pair| spot_pair_row(pair, &tokens, AT))
            .collect();
        assert!(!usdh.is_empty());
        assert!(usdh
            .iter()
            .all(|row| row.get("quote_symbol") == &text("USDH")));
        check_payload(&hype, item(pairs, "index", 107));
        assert_eq!(hype.listed(), Some(true));
        // Without exactly two token indices, or with an unknown token, the
        // derived columns are NULL.
        let odd = Json::parse(br#"{"index":9,"name":"@9","tokens":[1,0,2]}"#).unwrap();
        let row = spot_pair_row(&odd, &tokens, AT);
        assert_eq!(row.get("base_token_index"), &Cell::Null);
        assert_eq!(row.get("name"), &Cell::Null);
        let unknown = Json::parse(br#"{"index":9,"name":"@9","tokens":[999,0]}"#).unwrap();
        let row = spot_pair_row(&unknown, &tokens, AT);
        assert_eq!(row.get("base_token_index"), &Cell::Long(999));
        assert_eq!(row.get("base_symbol"), &Cell::Null);
        assert_eq!(row.get("name"), &Cell::Null);
    }

    #[test]
    fn spot_tokens_and_details() {
        let spot = fixture("spotMeta.json");
        let tokens = tokens(&spot);
        let hsei = *tokens.get(&437).unwrap();
        let row = spot_token_row(hsei, AT);
        assert_eq!(row.get("name"), &text("HSEI"));
        assert_eq!(row.get("full_name"), &text("hSEI"));
        assert_eq!(row.get("sz_decimals"), &Cell::Long(2));
        assert_eq!(row.get("wei_decimals"), &Cell::Long(8));
        assert_eq!(
            row.get("evm_contract"),
            &text("0xbdf43ecadc5cef51b7d1772f722e40596bc1788b")
        );
        assert_eq!(row.get("deployer_trading_fee_share"), &Cell::Decimal(ONE));
        check_payload(&row, hsei);
        let token_id = hsei.get("tokenId").unwrap().as_str().unwrap();
        let response = fixture("tokenDetails_HSEI.json");
        let details = token_details_row(437, token_id, &response, AT);
        assert_eq!(
            details.get("deployer"),
            &text("0x96fdb9e5c1a3d33f34109118457500b3d96c7094")
        );
        assert_eq!(
            details.get("deploy_time"),
            &Cell::Timestamp(1_770_192_537_325_998)
        );
        assert_eq!(details.get("deploy_gas"), &Cell::Decimal(5_298_506_899_600));
        assert_eq!(details.get("source"), &text("tokenDetails"));
        assert_eq!(details.get("token_id"), &text(token_id));
        check_payload(&details, &response);
        assert_eq!(details.listed(), None);
        assert_eq!(SPOT_TOKEN_DETAILS.position("listed"), None);
        let usdc_id = tokens
            .get(&0)
            .unwrap()
            .get("tokenId")
            .unwrap()
            .as_str()
            .unwrap();
        let usdc = token_details_row(0, usdc_id, &fixture("tokenDetails_USDC.json"), AT);
        assert_eq!(usdc.get("deployer"), &Cell::Null);
        let asi = spot_token_row(tokens.get(&40).unwrap(), AT);
        assert_eq!(asi.get("evm_contract"), &Cell::Null);
        assert_eq!(asi.get("full_name"), &text("Artificial Superintelligence"));
    }

    #[test]
    fn response_fields_beside_the_entity_lists_are_kept() {
        let mut meta = fixture("outcomeMeta_t0.json");
        if let Json::Obj(entries) = &mut meta {
            entries.insert(
                "newTopLevelField".into(),
                Json::parse(br#"{"added":"2026-11"}"#).unwrap(),
            );
        }
        let row = outcome_meta_row(&meta, AT);
        check_payload(&row, &meta.without(&["outcomes", "questions"]));
        assert!(
            matches!(row.get("payload_json"), Cell::Text(json) if json.contains("newTopLevelField"))
        );
        assert_eq!(row.get("request"), &text("outcomeMeta"));
        assert_eq!(row.get("fee_scale"), &Cell::Decimal(ONE));
        assert_eq!(row.listed(), Some(true));
        assert_eq!(
            row.get("venue_deployers"),
            &kv(&[("skew", "0x08e9c89f46dccee91bdb85c6532eb93a4c335efe")])
        );
        let mut spot = fixture("spotMeta.json");
        assert_eq!(spot_meta_row(&spot, AT).get("payload_json"), &text("{}"));
        if let Json::Obj(entries) = &mut spot {
            entries.insert("newField".into(), Json::Int("1".into()));
        }
        check_payload(
            &spot_meta_row(&spot, AT),
            &Json::obj([("newField", Json::Int("1".into()))]),
        );
    }

    #[test]
    fn venue_deployers_need_distinct_string_venues() {
        let venues = |deployers: &str| {
            let meta = Json::parse(
                format!(r#"{{"deployers":{deployers},"outcomes":[],"questions":[]}}"#).as_bytes(),
            )
            .unwrap();
            outcome_meta_row(&meta, AT).get("venue_deployers").clone()
        };
        assert_eq!(venues("[]"), Cell::Kv(vec![]));
        assert_eq!(
            venues(r#"[{"venue":"a","deployer":"x"},{"venue":"b","deployer":"y"}]"#),
            kv(&[("a", "x"), ("b", "y")])
        );
        for bad in [
            r#"[{"venue":"a","deployer":"x"},{"venue":"a","deployer":"y"}]"#,
            r#"[{"venue":"a"}]"#,
            r#"[{"venue":1,"deployer":"x"}]"#,
            r#"["a"]"#,
            r#"{}"#,
        ] {
            assert_eq!(venues(bad), Cell::Null, "{bad}");
        }
    }

    #[test]
    fn perp_dexes_and_assets() {
        let spot = fixture("spotMeta.json");
        let tokens = tokens(&spot);
        let dexs = fixture("perpDexs.json");
        let metas = fixture("allPerpMetas.json");
        let (dexs, metas) = (dexs.as_arr().unwrap(), metas.as_arr().unwrap());
        let default = perp_dex_row(0, &dexs[0], &metas[0], &tokens, AT);
        assert_eq!(default.get("name"), &text(""));
        assert_eq!(default.get("full_name"), &Cell::Null);
        assert_eq!(default.get("collateral_symbol"), &text("USDC"));
        let flx = perp_dex_row(2, &dexs[2], &metas[2], &tokens, AT);
        assert_eq!(flx.get("name"), &text("flx"));
        assert_eq!(flx.get("collateral_token_index"), &Cell::Long(360));
        assert_eq!(flx.get("collateral_symbol"), &text("USDH"));
        check_payload(
            &flx,
            &Json::obj([
                ("perpDexs", dexs[2].clone()),
                (
                    "allPerpMetas",
                    Json::obj([
                        ("collateralToken", Json::Int("360".into())),
                        (
                            "marginTables",
                            metas[2].get("marginTables").unwrap().clone(),
                        ),
                    ]),
                ),
            ]),
        );
        // Any dex-level field of allPerpMetas but the universe is in the payload.
        let mut newer = metas[2].clone();
        if let Json::Obj(entries) = &mut newer {
            entries.insert("fundingMultiplier".into(), Json::Str("2".into()));
        }
        let row = perp_dex_row(2, &dexs[2], &newer, &tokens, AT);
        let Cell::Text(payload) = row.get("payload_json") else {
            panic!("no payload")
        };
        assert!(payload.contains(r#""fundingMultiplier":"2""#));
        assert!(!payload.contains(r#""universe""#));
        // A perpDexs entry that is not an object is null in the payload (Q14).
        let row = perp_dex_row(1, &Json::Int("5".into()), &metas[1], &tokens, AT);
        assert!(
            matches!(row.get("payload_json"), Cell::Text(json) if json.ends_with(r#""perpDexs":null}"#))
        );
        let universe = |dex: usize| metas[dex].get("universe").unwrap().as_arr().unwrap();
        let matic = perp_asset_row(0, Some(""), 3, &universe(0)[3], AT);
        assert_eq!(matic.get("coin"), &text("MATIC"));
        assert_eq!(matic.get("asset_id"), &Cell::Long(3));
        assert_eq!(matic.get("is_delisted"), &Cell::Bool(true));
        let btc = perp_asset_row(0, Some(""), 0, &universe(0)[0], AT);
        assert_eq!(btc.get("is_delisted"), &Cell::Bool(false));
        assert_eq!(btc.get("only_isolated"), &Cell::Null);
        let xyz = perp_asset_row(1, Some("xyz"), 0, &universe(1)[0], AT);
        assert_eq!(xyz.get("coin"), &text("xyz:XYZ100"));
        assert_eq!(xyz.get("dex"), &text("xyz"));
        assert_eq!(xyz.get("asset_id"), &Cell::Long(110_000));
        // A present isDelisted that is not a boolean is NULL.
        let odd = Json::parse(br#"{"name":"X","isDelisted":null}"#).unwrap();
        assert_eq!(
            perp_asset_row(0, Some(""), 9, &odd, AT).get("is_delisted"),
            &Cell::Null
        );
    }

    #[test]
    fn outcomes() {
        let meta = fixture("outcomeMeta_t0.json");
        let live = item(meta.get("outcomes").unwrap(), "outcome", 1473);
        let row = outcome_live_row(live, Some(198), AT);
        assert_eq!(row.get("question_id"), &Cell::Long(198));
        assert_eq!(row.get("status"), &text("live"));
        assert_eq!(row.listed(), Some(true));
        assert_eq!(row.get("side_coins"), &texts(&["#14730", "#14731"]));
        assert_eq!(row.get("side_names"), &texts(&["Yes", "No"]));
        assert_eq!(
            row.get("description_kv"),
            &kv(&[("participant", "Arsenal")])
        );
        let settled = outcome_settled_row(&fixture("settledOutcome_7768.json"), AT);
        assert_eq!(settled.get("question_id"), &Cell::Long(373));
        assert_eq!(settled.get("status"), &text("settled"));
        assert_eq!(settled.listed(), Some(false));
        assert_eq!(settled.get("settle_fraction"), &Cell::Decimal(0));
        let legacy = outcome_settled_row(&fixture("settledOutcome_0.json"), AT);
        assert_eq!(legacy.get("quote_token"), &text("USDH"));
        assert_eq!(legacy.get("settle_details"), &text("price:78212.4"));
        assert_eq!(legacy.get("venue"), &Cell::Null);
        assert_eq!(legacy.get("side_coins"), &texts(&["#0", "#1"]));
        let Cell::Kv(pairs) = legacy.get("description_kv") else {
            panic!("no description map")
        };
        assert!(pairs.contains(&("targetPrice".into(), "78213".into())));
        let response = fixture("settledOutcome_9174.json");
        let template = outcome_settled_row(&response, AT);
        assert_eq!(template.get("venue"), &text("txyz"));
        assert_eq!(template.get("deployer_fee_scale"), &Cell::Null);
        assert_eq!(template.get("question_id"), &Cell::Null);
        let Cell::Kv(pairs) = template.get("description_kv") else {
            panic!("no description map")
        };
        assert!(pairs.contains(&("perp".into(), "xyz:SPCX".into())));
        check_payload(&template, &response);
    }

    #[test]
    fn side_coins_encode_at_most_ten_sides() {
        let sides = |count: usize| {
            let names: Vec<String> = (0..count)
                .map(|side| format!(r#"{{"name":"s{side}"}}"#))
                .collect();
            Json::parse(format!(r#"{{"outcome":7,"sideSpecs":[{}]}}"#, names.join(",")).as_bytes())
                .unwrap()
        };
        let ten = outcome_live_row(&sides(10), None, AT);
        let Cell::TextList(coins) = ten.get("side_coins") else {
            panic!("no side coins")
        };
        assert_eq!(coins.last(), Some(&Some("#79".into())));
        let eleven = outcome_live_row(&sides(11), None, AT);
        assert!(matches!(eleven.get("side_names"), Cell::TextList(names) if names.len() == 11));
        assert_eq!(eleven.get("side_coins"), &Cell::Null);
        let huge = Json::parse(br#"{"outcome":9223372036854775807,"sideSpecs":[{"name":"Yes"}]}"#)
            .unwrap();
        assert_eq!(
            outcome_live_row(&huge, None, AT).get("side_coins"),
            &Cell::Null
        );
        let odd = Json::parse(br#"{"outcome":1,"sideSpecs":[{"name":"Yes"},"No"]}"#).unwrap();
        assert_eq!(
            outcome_live_row(&odd, None, AT).get("side_names"),
            &Cell::TextList(vec![Some("Yes".into()), None])
        );
    }

    #[test]
    fn questions() {
        let meta = fixture("outcomeMeta_t0.json");
        let question = &meta.get("questions").unwrap().as_arr().unwrap()[0];
        let row = question_live_row(question, AT);
        assert_eq!(row.get("question_id"), &Cell::Long(198));
        assert_eq!(row.get("fallback_outcome_id"), &Cell::Long(1472));
        assert_eq!(
            row.get("named_outcome_ids"),
            &Cell::LongList(vec![Some(1473)])
        );
        assert_eq!(row.get("settled_outcome_ids"), &Cell::LongList(vec![]));
        assert_eq!(row.get("status"), &text("live"));
        let response = fixture("settledOutcome_7768.json");
        let settled = question_settled_row(&response, AT);
        assert_eq!(settled.get("question_id"), &Cell::Long(373));
        assert_eq!(settled.get("status"), &text("settled"));
        assert_eq!(settled.get("named_outcome_ids"), &Cell::Null);
        assert_eq!(settled.listed(), Some(false));
        check_payload(&settled, response.get("question").unwrap());
        // A wrapper whose question is still live.
        let live = Json::parse(br#"{"question":{"question":{"live":5},"name":"q"}}"#).unwrap();
        assert_eq!(settled_question_id(&live), Some(5));
        assert_eq!(question_settled_row(&live, AT).get("status"), &text("live"));
        let none = Json::parse(br#"{"question":{"question":5}}"#).unwrap();
        assert_eq!(settled_question_id(&none), None);
    }

    #[test]
    fn funding() {
        let entries = fixture("fundingHistory_xyz_XYZ100.json");
        let entry = &entries.as_arr().unwrap()[0];
        let row = funding_row(entry, AT);
        assert_eq!(row.get("coin"), &text("xyz:XYZ100"));
        assert_eq!(row.get("dex"), &text("xyz"));
        assert_eq!(
            row.get("funding_time"),
            &Cell::Timestamp(entry.get("time").unwrap().as_i64().unwrap() * 1000)
        );
        assert_eq!(row.get("funding_rate"), &Cell::Decimal(62_500));
        assert_eq!(row.get("premium"), &Cell::Decimal(-1_375_517));
        assert_eq!(row.listed(), None);
        assert_eq!(
            row.key(),
            vec![
                KeyPart::Text("xyz:XYZ100".into()),
                KeyPart::Timestamp(1_767_225_600_063_000)
            ]
        );
    }

    #[test]
    fn removal_rows_keep_the_last_values() {
        let meta = fixture("outcomeMeta_t0.json");
        let live = outcome_live_row(
            &meta.get("outcomes").unwrap().as_arr().unwrap()[1],
            Some(198),
            AT,
        );
        let gone = removal_row(&live, AT + 1, "outcomeMeta");
        assert_eq!(gone.listed(), Some(false));
        assert_eq!(gone.get("payload_json"), &Cell::Null);
        assert_eq!(gone.payload_sha256(), None);
        assert_eq!(gone.get("name"), live.get("name"));
        assert_eq!(gone.get("question_id"), &Cell::Long(198));
        assert_eq!(gone.get("status"), &text("live"));
        assert_eq!(gone.get("observed_at"), &Cell::Timestamp(AT + 1));
        assert_eq!(gone.key(), live.key());
    }

    #[test]
    fn probes_have_no_tail() {
        let row = probe_row(5, AT, ProbeResult::NotSettled, "run");
        assert_eq!(row.get("result"), &text("not_settled"));
        assert_eq!(row.key(), vec![KeyPart::Long(5), KeyPart::Timestamp(AT)]);
        assert_eq!(row.payload_sha256(), None);
        assert!(!OUTCOME_PROBES.snapshot());
        assert_eq!(OUTCOME_PROBES.delta_name(), "outcome_probes");
        assert_eq!(
            OUTCOME_PROBES
                .typed_columns()
                .map(|index| OUTCOME_PROBES.columns[index].name)
                .collect::<Vec<_>>(),
            ["result", "run_id"]
        );
    }

    #[test]
    fn membership_follows_the_list_order() {
        let question = |id: i64, named: &str, fallback: i64| {
            Json::parse(
                format!(
                    r#"{{"question":{id},"namedOutcomes":{named},"fallbackOutcome":{fallback}}}"#
                )
                .as_bytes(),
            )
            .unwrap()
        };
        // Outcome 5 is named by question 9, then by question 3: the later in
        // the list wins, whatever the ids.
        let nine = question(9, "[5, 6]", 7);
        let three = question(3, "[5]", 8);
        // A repeated question keeps its first position and its last value.
        let nine_again = question(9, "[6]", 7);
        let odd = question(4, "5", 10);
        let mut listed = Ordered::new();
        for item in [&nine, &three, &nine_again, &odd] {
            listed.insert(item.get("question").unwrap().as_i64().unwrap(), item);
        }
        assert_eq!(
            listed.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            [9, 3, 4]
        );
        let members = membership(&listed);
        assert_eq!(members.get(&5), Some(&3));
        assert_eq!(members.get(&6), Some(&9));
        assert_eq!(members.get(&7), Some(&9));
        assert_eq!(members.get(&8), Some(&3));
        // A namedOutcomes that is not a list names nothing (Q30); the
        // fallback still counts.
        assert_eq!(members.get(&10), Some(&4));
        assert_eq!(members.len(), 5);
    }

    #[test]
    fn every_table_has_its_key_first_and_its_tail_last() {
        for spec in ALL_TABLES {
            for (index, key) in spec.key.iter().enumerate() {
                assert_eq!(spec.columns[index].name, *key, "{}", spec.name);
                assert!(!spec.columns[index].nullable, "{}", spec.name);
            }
            if REFERENCE_TABLES.contains(&spec) {
                let tail: Vec<&str> = spec.columns.iter().rev().take(4).map(|c| c.name).collect();
                assert_eq!(
                    tail,
                    ["payload_json", "payload_sha256", "source", "observed_at"],
                    "{}",
                    spec.name
                );
            }
        }
        assert_eq!(ALL_TABLES.iter().filter(|spec| spec.snapshot()).count(), 8);
    }
}
