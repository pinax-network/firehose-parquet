//! Arrow schemas of the twelve HyperCore tables.
//!
//! Every table starts with the canonical identity columns
//! (`canonical_fields_with_encoding`); `block_id` and `parent_id` hold the
//! decimal block number as text (`PreparedIdentity::with_text_ids`). Each
//! comment beside a field is its description in `docs/schemas/hypercore.md`,
//! copied into `COLUMN_DESCRIPTIONS` (`blocks/src/schema_docs.rs`): update both
//! together (`schema_docs::tests::hypercore_schema_comments_are_the_column_descriptions`
//! checks it).
//!
//! The raw record is `blocks`, `fills` (but its three derived columns),
//! `funding_deltas`, `validator_rewards` and the five event tables, which
//! partition the block's events by product family (one column catalogue,
//! [`event_fields`], selected per table). `outcome_fills`, `liquidations` and
//! `funding_rates`, and `fills.market_type`, `fills.dex` and
//! `fills.counterparty`, are derived from the same block (rules R-D1 to R-D6 in
//! `docs/chains/hypercore.md`).
//!
//! The columns, their order, types and nullability, and the schema metadata
//! [`DERIVATION_KEY`], are bound into the protected stream identity: any change
//! needs a new output root (`docs/chains/hypercore.md`, "Schema changes").

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Fields, Schema};
use firehose_parquet::encode::{bytes_data_type, BytesListColumn, EncodeBytes};
use firehose_parquet::traits::{
    canonical_fields_with_encoding, enum_data_type, push_fork_step_field, timestamp_millis_utc_type,
};
use std::sync::Arc;

/// Precision of every HyperCore decimal column.
pub const DECIMAL_PRECISION: u8 = 38;
/// Scale of every HyperCore decimal column: HyperLiquid writes at most 10
/// fractional digits.
pub const DECIMAL_SCALE: i8 = 10;

/// Schema metadata key of every HyperCore table: the version of the mapper's
/// derivation rules (R-D1 to R-D6). It is part of each table's declared digest,
/// so a root refuses to resume under other rules. It guards resume only:
/// compaction rewrites files from the Delta schema, which does not carry it.
pub const DERIVATION_KEY: &str = "fireparq.hypercore.derivation";
/// The derivation rules this release writes.
pub const DERIVATION_VERSION: &str = "1";

/// The description of `extra_json` in the raw tables.
pub const EXTRA_JSON_DESCRIPTION: &str = "Reserved for fields that upstream adds after this \
    schema version: a JSON object of values that have no typed column (rules in the HyperCore \
    chain notes). NULL in every row written by this version.";

/// `Decimal128(38, 10)`, the type of every amount, price and size.
pub fn decimal_type() -> DataType {
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
}

/// The fields of one `liquidated_positions` item.
pub fn position_fields() -> Fields {
    vec![
        Field::new("coin", DataType::Utf8, false),
        Field::new("szi", decimal_type(), false),
    ]
    .into()
}

/// The non-null list item of `liquidated_positions`.
pub fn position_item() -> Arc<Field> {
    Arc::new(Field::new(
        "item",
        DataType::Struct(position_fields()),
        false,
    ))
}

/// `List<Struct<coin: Utf8, szi: Decimal128(38, 10)>>` with non-null items.
pub fn positions_type() -> DataType {
    DataType::List(position_item())
}

/// A table's schema: the canonical columns, `fields`, the non-final columns,
/// and the derivation version as schema metadata.
fn table_schema(fields: Vec<Field>, include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let mut all = canonical_fields_with_encoding(encoding);
    all.extend(fields);
    push_fork_step_field(&mut all, include_fork_step);
    Schema::new_with_metadata(
        all,
        HashMap::from([(DERIVATION_KEY.to_string(), DERIVATION_VERSION.to_string())]),
    )
}

/// `blocks`: One row per Firehose block, including blocks with no fills and no events (about a
/// third of all blocks).
pub fn blocks_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    table_schema(
        vec![
            // Consensus block time in nanoseconds since the Unix epoch (UTC). The only exact block
            // time: `timestamp` is this value truncated to milliseconds.
            Field::new("block_time_ns", DataType::Int64, false),
            // Number of fills in the block, equal to this block's row count in `fills`.
            Field::new("fill_count", DataType::UInt32, false),
            // Number of events in the block, equal to this block's row count over the five event
            // tables (`transfers`, `bridge_transfers`, `vault_events`, `staking_events`,
            // `other_events`).
            Field::new("event_count", DataType::UInt32, false),
            // EXTRA_JSON_DESCRIPTION (the same in every table).
            Field::new("extra_json", DataType::Utf8, true),
        ],
        include_fork_step,
        encoding,
    )
}

/// `fills`: One row per fill: each participant's side of a match, in execution order, for every
/// market (perps, spot and HIP-4 outcomes). A normal trade is two adjacent rows, `BUY` then
/// `ASK`, sharing `transaction_id`, `hash`, `price` and `size`, with exactly one `crossed` leg.
/// `market_type`, `dex` and `counterparty` are derived from the block.
pub fn fills_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    table_schema(
        vec![
            // 0-based position of the fill in the block (execution order). With `block_num`, the
            // only key that is always unique.
            Field::new("fill_index", DataType::UInt32, false),
            // Account this fill belongs to; every match writes one row per participant. The zero
            // address is the counterparty of delisted-perp `SETTLEMENT` fills.
            Field::new("user", bd.clone(), false),
            // Market symbol, verbatim: core perp `BTC`; HIP-3 perp `<dex>:<SYMBOL>`; spot
            // `@<index>` or `PURR/USDC`; HIP-4 outcome `#<10·outcome_id + side>`.
            Field::new("coin", DataType::Utf8, false),
            // Execution price: quote or collateral per unit of base. Never negative; outcome
            // prices are between 0 and 1.
            Field::new("price", decimal_type(), false),
            // Filled quantity in base units, positive.
            Field::new("size", decimal_type(), false),
            // `BUY` (HyperLiquid `B`, the buyer; `BID` in the Pinax API) or `ASK` (HyperLiquid
            // `A`, the seller). In a normal match the `BUY` row comes first.
            Field::new("side", enum_data_type(), false),
            // HyperLiquid fill time (epoch milliseconds). Equal to `timestamp` in every block
            // observed; stored as delivered, not checked.
            Field::new("fill_time", timestamp_millis_utc_type(), false),
            // Position before this fill, in coin units. Perps: signed size, negative = short. Spot
            // and outcomes: base balance.
            Field::new("start_position", decimal_type(), false),
            // HyperLiquid direction label with the `TRADING_DIRECTION_` prefix removed, e.g.
            // `OPEN_LONG`, `LONG_TO_SHORT`, `SETTLEMENT`. Market liquidations use the ordinary
            // open and close labels.
            Field::new("direction", enum_data_type(), false),
            // Realized PnL on the closed part, in the collateral or quote token; `0.0` on opening
            // fills. Whether it is gross or net of fees is not documented.
            Field::new("closed_pnl", decimal_type(), false),
            // L1 transaction hash of the taker action, shared by both legs and by every fill of a
            // sweeping order; not unique. All zero bytes when there is no L1 transaction (TWAP
            // slices and their counterparty, daily dust conversion, some outcome fills): exclude
            // it before joining on `hash`.
            Field::new("hash", bd.clone(), false),
            // This participant's order id (HyperLiquid `oid`). The two legs of a match have
            // different ids, except delisted-perp `SETTLEMENT`.
            Field::new("order_id", DataType::UInt64, false),
            // True when this leg crossed the spread: the taker. Exactly one leg of a normal match
            // is crossed; single outcome fills (split, merge, negate) are crossed.
            Field::new("crossed", DataType::Boolean, false),
            // Total fee in `fee_token`, including `builder_fee` and, by observation,
            // `deployer_fee`. Negative = maker rebate.
            Field::new("fee", decimal_type(), false),
            // HyperLiquid trade id `tid` (proto name kept), not a transaction id: a 50-bit hash of
            // the buyer and seller order ids, shared by both legs. 0 on daily dust-conversion
            // fills. Not globally unique; HyperLiquid identifies a trade by time, coin and tid.
            Field::new("transaction_id", DataType::UInt64, false),
            // Token `fee` is paid in: `USDC`, a HIP-3 dex collateral (`USDT0`, `USDH`, `USDE`),
            // the received asset on spot taker buys, or `+<n>` for outcome coin `#<n>`.
            Field::new("fee_token", DataType::Utf8, false),
            // TWAP order id, set only on the TWAP slice leg (the crossed one). NULL when the fill
            // is not a TWAP slice.
            Field::new("twap_id", DataType::UInt64, true),
            // Client order id (cloid, 16 bytes). NULL when the order had none. Not a taker marker.
            Field::new("client_order_id", bd.clone(), true),
            // Liquidated account, on both legs of a liquidation fill; the liquidated side is the
            // row where `user = liquidated_user`. NULL when the fill is not a liquidation (or,
            // never observed, the account was not reported).
            Field::new("liquidated_user", bd.clone(), true),
            // Mark price at liquidation. NULL when the fill is not a liquidation.
            Field::new("liquidation_mark_px", decimal_type(), true),
            // `market` (liquidation order sent to the book) or `backstop` (taken over by the
            // backstop liquidator, or settled against `AUTO_DELEVERAGING` counterparties; only
            // takeovers, where both legs are `LIQUIDATED_*`, have a ledger `liquidation` event of
            // the same hash), verbatim. Not NULL exactly when the fill is a liquidation.
            Field::new("liquidation_method", DataType::Utf8, true),
            // HIP-3 or HIP-4 deployer's share of `fee`, in `fee_token`; can be negative. NULL when
            // absent; before block 957002477 (2026-04-13) NULL means not captured.
            Field::new("deployer_fee", decimal_type(), true),
            // Builder-code address as delivered, `0x` plus 40 lowercase hex characters (a proto
            // string, so not re-encoded). NULL when none; before block 957002478 NULL can also
            // mean not captured (capture there is partial).
            Field::new("builder", DataType::Utf8, true),
            // Fee paid to `builder`, in `fee_token`, included in `fee`. HyperLiquid omits zero, so
            // a builder can appear with a NULL fee. Before block 957002478 NULL can also mean not
            // captured.
            Field::new("builder_fee", decimal_type(), true),
            // IOC priority fee paid in HYPE, on the taker leg only. NULL when none; the feature
            // launched around 2026-04-20.
            Field::new("priority_gas", decimal_type(), true),
            // Derived from `coin` (rule R-D1): `perp` (core and HIP-3 perps), `spot` (`@<n>` and
            // `PURR/USDC`) or `outcome` (HIP-4 `#<n>`). NULL when the coin has a form this version
            // does not know.
            Field::new("market_type", enum_data_type(), true),
            // Derived from `coin` (rule R-D1), perps only: `''` for the default (core) perp dex,
            // HyperLiquid's own name for it and the `source_dex` value of a `send`; otherwise the
            // HIP-3 dex name, the text before `:`. NULL for spot, outcomes and unknown coin forms.
            // HIP-3 is `market_type = 'perp' AND dex <> ''`.
            Field::new("dex", DataType::Utf8, true),
            // Derived (rule R-D2): the `user` of the other fill of the block with the same `coin`
            // and `transaction_id`, when exactly two fills share them and their sides differ.
            // NULL otherwise: `transaction_id` 0 (daily dust conversion) and single-leg HIP-4
            // fills (mint, burn, split, merge, negate). The zero address on delisted-perp
            // `SETTLEMENT` fills.
            Field::new("counterparty", bd.clone(), true),
            // EXTRA_JSON_DESCRIPTION (the same in every table).
            Field::new("extra_json", DataType::Utf8, true),
        ],
        include_fork_step,
        encoding,
    )
}

/// `outcome_fills`: One row per HIP-4 outcome fill (`fills.market_type = 'outcome'`, coin
/// `#<n>`), in fill order, copied from `fills` with the outcome and its side parsed from `coin`
/// (rule R-D3). These fills stay in `fills` too: never add the two tables.
pub fn outcome_fills_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    table_schema(
        vec![
            // Position of the fill in the block: joins `fills` on `(block_num, fill_index)`.
            Field::new("fill_index", DataType::UInt32, false),
            // Account this fill belongs to (`fills.user`).
            Field::new("user", bd.clone(), false),
            // Outcome side coin `#<n>`, where `n = 10·outcome_id + side_index` (`fills.coin`).
            Field::new("coin", DataType::Utf8, false),
            // HIP-4 outcome id: `n div 10` of coin `#<n>`.
            Field::new("outcome_id", DataType::Int64, false),
            // Outcome side: `n mod 10` of coin `#<n>`. Side 0 is the first side of the outcome's
            // specification ("Yes" on a binary outcome). Not `side` (`BUY` or `ASK`).
            Field::new("side_index", DataType::Int64, false),
            // Price of one share, a probability between 0 and 1 (`fills.price`).
            Field::new("price", decimal_type(), false),
            // Shares filled, positive (`fills.size`).
            Field::new("size", decimal_type(), false),
            // `BUY` = shares received, `ASK` = shares given (`fills.side`).
            Field::new("side", enum_data_type(), false),
            // `BUY`, `SELL`, `SETTLEMENT`, `SPLIT_OUTCOME`, `MERGE_OUTCOME`, `MERGE_QUESTION` or
            // `NEGATE_OUTCOME` (`fills.direction`).
            Field::new("direction", enum_data_type(), false),
            // Share balance of this side coin before the fill (`fills.start_position`).
            Field::new("start_position", decimal_type(), false),
            // `fills.closed_pnl`.
            Field::new("closed_pnl", decimal_type(), false),
            // `fills.hash`; all zero bytes when there is no L1 transaction.
            Field::new("hash", bd.clone(), false),
            // `fills.order_id`.
            Field::new("order_id", DataType::UInt64, false),
            // `fills.crossed`: the taker leg, and every single split, merge and negate fill.
            Field::new("crossed", DataType::Boolean, false),
            // `fills.fee`, in `fee_token`: outcome fills do pay fees.
            Field::new("fee", decimal_type(), false),
            // `fills.fee_token`: `+<n>` (the outcome token) in May 2026, `USDC` later.
            Field::new("fee_token", DataType::Utf8, false),
            // `fills.transaction_id`, the HyperLiquid trade id.
            Field::new("transaction_id", DataType::UInt64, false),
            // `fills.twap_id`.
            Field::new("twap_id", DataType::UInt64, true),
            // `fills.client_order_id`.
            Field::new("client_order_id", bd.clone(), true),
            // `fills.deployer_fee`.
            Field::new("deployer_fee", decimal_type(), true),
            // `fills.builder`.
            Field::new("builder", DataType::Utf8, true),
            // `fills.builder_fee`.
            Field::new("builder_fee", decimal_type(), true),
            // `fills.priority_gas`.
            Field::new("priority_gas", decimal_type(), true),
            // `fills.counterparty`: set on direct trades and settlements (a `0x3200…` system
            // account), NULL on mint, burn, split, merge and negate legs.
            Field::new("counterparty", bd.clone(), true),
            // The `extra_json` of the `fills` row, copied verbatim, so that a fill field upstream
            // adds later reaches this table without a rebuild. NULL in every row written by this
            // version.
            Field::new("extra_json", DataType::Utf8, true),
        ],
        include_fork_step,
        encoding,
    )
}

/// `liquidations`: One row per liquidated leg: a fill with a liquidation whose `user` is the
/// liquidated account (`fills.user = fills.liquidated_user`), in fill order, copied from `fills`
/// with its paired leg (rule R-D4). The ledger `liquidation` event is joined in a view, not
/// stored.
pub fn liquidations_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    table_schema(
        vec![
            // Position of the liquidated fill in the block: joins `fills` on `(block_num,
            // fill_index)`.
            Field::new("fill_index", DataType::UInt32, false),
            // The liquidated account (`fills.user`, equal to `fills.liquidated_user`).
            Field::new("liquidated_user", bd.clone(), false),
            // `fills.coin`.
            Field::new("coin", DataType::Utf8, false),
            // `fills.market_type`: `perp`.
            Field::new("market_type", enum_data_type(), true),
            // `fills.dex`: `''` for the default (core) perp dex, else the HIP-3 dex name.
            Field::new("dex", DataType::Utf8, true),
            // The liquidated side, `BUY` or `ASK` (`fills.side`).
            Field::new("side", enum_data_type(), false),
            // `fills.direction`: `market` liquidations use the ordinary `CLOSE_*` and `*_TO_*`
            // labels, `backstop` ones `LIQUIDATED_*`.
            Field::new("direction", enum_data_type(), false),
            // `fills.price`.
            Field::new("price", decimal_type(), false),
            // `fills.size`.
            Field::new("size", decimal_type(), false),
            // `fills.start_position`: the liquidated position before the fill.
            Field::new("start_position", decimal_type(), false),
            // `fills.closed_pnl`.
            Field::new("closed_pnl", decimal_type(), false),
            // `fills.fee`.
            Field::new("fee", decimal_type(), false),
            // `fills.fee_token`.
            Field::new("fee_token", DataType::Utf8, false),
            // `fills.crossed`: true on `market` liquidations (the liquidation order), false on
            // `backstop` ones (takeover and ADL alike).
            Field::new("crossed", DataType::Boolean, false),
            // `market` (a liquidation order sent to the book) or `backstop` (taken over by the
            // backstop liquidator, or settled against `AUTO_DELEVERAGING` counterparties)
            // (`fills.liquidation_method`).
            Field::new("liquidation_method", DataType::Utf8, false),
            // Mark price at liquidation (`fills.liquidation_mark_px`).
            Field::new("mark_price", decimal_type(), false),
            // `fills.order_id`.
            Field::new("order_id", DataType::UInt64, false),
            // `fills.transaction_id`, the HyperLiquid trade id.
            Field::new("transaction_id", DataType::UInt64, false),
            // `fills.hash`.
            Field::new("hash", bd.clone(), false),
            // `fills.counterparty`: the other leg's `user`, the resting order's account on a `market`
            // liquidation, the backstop liquidator on a takeover, the deleveraged account under
            // ADL. NULL when the fill has no single opposite leg (never observed).
            Field::new("counterparty", bd.clone(), true),
            // The other leg's `direction`: `AUTO_DELEVERAGING` marks ADL, `LIQUIDATED_*` a
            // backstop takeover. NULL with `counterparty`.
            Field::new("counterparty_direction", enum_data_type(), true),
            // The other leg's `fill_index`: joins its `fills` row. NULL with `counterparty`.
            Field::new("counterparty_fill_index", DataType::UInt32, true),
            // The `extra_json` of the `fills` row, copied verbatim, so that a fill field upstream
            // adds later reaches this table without a rebuild. NULL in every row written by this
            // version.
            Field::new("extra_json", DataType::Utf8, true),
        ],
        include_fork_step,
        encoding,
    )
}

/// The five event tables. Every event of a block is in exactly one of them
/// (routing in `docs/chains/hypercore.md`, rule R-D6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EventTable {
    Transfers,
    BridgeTransfers,
    VaultEvents,
    StakingEvents,
    OtherEvents,
}

impl EventTable {
    /// In table order.
    pub const ALL: [EventTable; 5] = [
        EventTable::Transfers,
        EventTable::BridgeTransfers,
        EventTable::VaultEvents,
        EventTable::StakingEvents,
        EventTable::OtherEvents,
    ];

    pub fn name(self) -> &'static str {
        match self {
            EventTable::Transfers => "transfers",
            EventTable::BridgeTransfers => "bridge_transfers",
            EventTable::VaultEvents => "vault_events",
            EventTable::StakingEvents => "staking_events",
            EventTable::OtherEvents => "other_events",
        }
    }

    /// The table's own columns of [`event_fields`], after the shared ones and
    /// in catalogue order; `None` for `other_events`, which has every column.
    pub fn own_columns(self) -> Option<&'static [&'static str]> {
        match self {
            EventTable::Transfers => Some(&[
                "user",
                "destination",
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
                "to_perp",
            ]),
            EventTable::BridgeTransfers => Some(&["usdc", "fee", "nonce"]),
            EventTable::VaultEvents => Some(&[
                "user",
                "vault",
                "usdc",
                "fee",
                "requested_usd",
                "commission",
                "closing_cost",
                "basis",
                "net_withdrawn_usd",
            ]),
            EventTable::StakingEvents => Some(&[
                "user",
                "validator",
                "token",
                "amount",
                "is_deposit",
                "is_undelegate",
                "is_finalized",
            ]),
            EventTable::OtherEvents => None,
        }
    }

    /// Whether the table has `column` of [`event_fields`].
    pub fn has_column(self, column: &str) -> bool {
        EVENT_SHARED_COLUMNS.contains(&column)
            || column == "extra_json"
            || self.own_columns().is_none_or(|own| own.contains(&column))
    }
}

/// The columns every event table has, after the canonical ones.
pub const EVENT_SHARED_COLUMNS: [&str; 6] = [
    "event_index",
    "event_type",
    "ledger_type",
    "hash",
    "event_time_ns",
    "users",
];

/// The column catalogue of the event tables, after the canonical columns:
/// `other_events` has all of them, the other event tables a subset in this
/// order ([`EventTable::own_columns`]). One `Event`: its single `EventBody`,
/// and for ledger updates its `LedgerUpdateDelta`, flattened into the row.
pub fn event_fields(encoding: &EncodeBytes) -> Vec<Field> {
    let bd = bytes_data_type(encoding);
    vec![
        // 0-based position of the event in the block (execution order), counted over all five
        // event tables. Key with `block_num`; joins `funding_deltas`, `funding_rates` and
        // `validator_rewards`.
        Field::new("event_index", DataType::UInt32, false),
        // `EventBody` case: `ledger_update`, `funding`, `validator_rewards`, `c_withdrawal`,
        // `c_deposit`, `delegation`, `gossip_priority_auction_restart` or `create_sub_account`.
        Field::new("event_type", enum_data_type(), false),
        // Ledger delta case for `ledger_update` rows (22 values, e.g. `send`, `withdraw`,
        // `liquidation`); NULL on other rows.
        Field::new("ledger_type", enum_data_type(), true),
        // Event hash, not unique. It is the HyperCore L1 transaction hash for user actions, the
        // Arbitrum One transaction hash for `deposit` and `withdraw`, and all zero bytes for
        // system and time-triggered events (funding, validator rewards, gossip restarts,
        // staking-withdrawal finalization and its transfer) and rare sends. Events from one
        // action share it.
        Field::new("hash", bd.clone(), false),
        // Event time in nanoseconds since the Unix epoch. Equal to `blocks.block_time_ns` in
        // every block observed; stored as delivered, not checked.
        Field::new("event_time_ns", DataType::Int64, false),
        // Accounts whose ledger changed (HyperLiquid's index for ledger history), proto order
        // kept, 1 or 2 entries; the order is a per-type convention, not a direction. For most
        // ledger types it holds the only address. NULL on non-ledger rows.
        Field::new("users", BytesListColumn::data_type(encoding), true),
        // The body's own `user`: sender (`send`, `spot_transfer`, `internal_transfer`,
        // `sub_account_transfer`), withdrawing depositor (`vault_withdraw`), vault leader
        // (`vault_leader_commission`), staker (`c_deposit`, `c_withdrawal`), delegator
        // (`delegation`) or master account (`create_sub_account`). NULL for types without one:
        // use `users`.
        Field::new("user", bd.clone(), true),
        // Recipient of `send`, `spot_transfer`, `internal_transfer` and `sub_account_transfer`.
        Field::new("destination", bd.clone(), true),
        // Vault address.
        Field::new("vault", bd.clone(), true),
        // Validator of a `delegation`.
        Field::new("validator", bd.clone(), true),
        // Sub-account created by `create_sub_account`.
        Field::new("sub_account", bd.clone(), true),
        // Token symbol of `amount`. NULL on `c_deposit`, `c_withdrawal` and `delegation`, whose
        // `amount` is HYPE.
        Field::new("token", DataType::Utf8, true),
        // Quantity in `token` units; HYPE for `c_deposit`, `c_withdrawal` and `delegation`. A
        // `c_deposit` and its paired `c_staking_transfer` (same hash and amount) describe one
        // move: do not add them.
        Field::new("amount", decimal_type(), true),
        // USDC amount, never negative; the direction comes from the type and its flags. For
        // `vault_create`, the leader's initial deposit.
        Field::new("usdc", decimal_type(), true),
        // USDC valuation of `amount`; the exact definition is not documented.
        Field::new("usdc_value", decimal_type(), true),
        // Fee of the action: in `fee_token` for `send` and `spot_transfer` (`0.0` when
        // `fee_token` is NULL); USDC for `internal_transfer` (0 or 1), `withdraw` (bridge fee, 1)
        // and `vault_create` (creation fee).
        Field::new("fee", decimal_type(), true),
        // Token of `fee` for `send` and `spot_transfer`; NULL when there is no fee.
        Field::new("fee_token", DataType::Utf8, true),
        // Fee in HYPE, e.g. for HyperEVM bridging.
        Field::new("native_token_fee", decimal_type(), true),
        // For `send` and `spot_transfer`: the action nonce, in epoch milliseconds for user-signed
        // actions or a global sequence number for HyperEVM-originated ones (can be 0). For
        // `withdraw`: the action nonce times 1000. Not a clock.
        Field::new("nonce", DataType::UInt64, true),
        // Balance a `send` debits: `''` = the default USDC perp dex (a value, not missing),
        // `spot`, or a HIP-3 dex name.
        Field::new("source_dex", DataType::Utf8, true),
        // Balance a `send` credits, with the same values as `source_dex`.
        Field::new("destination_dex", DataType::Utf8, true),
        // HIP-3 dex name.
        Field::new("dex", DataType::Utf8, true),
        // For `c_staking_transfer`: true = spot to staking (pairs with `c_deposit`); false =
        // staking to spot (pairs with a `c_withdrawal` finalization).
        Field::new("is_deposit", DataType::Boolean, true),
        // For `account_class_transfer`: true = spot to perp.
        Field::new("to_perp", DataType::Boolean, true),
        // For `delegation`: true = undelegate.
        Field::new("is_undelegate", DataType::Boolean, true),
        // For `c_withdrawal`: false = unstake request (user's hash, no balance change); true =
        // finalization about 7 days later (zero hash, paired with a `c_staking_transfer` whose
        // `is_deposit` is false).
        Field::new("is_finalized", DataType::Boolean, true),
        // For `vault_withdraw`: amount requested, USDC.
        Field::new("requested_usd", decimal_type(), true),
        // For `vault_withdraw`: the leader's profit share, USDC.
        Field::new("commission", decimal_type(), true),
        // For `vault_withdraw`: closing cost, USDC.
        Field::new("closing_cost", decimal_type(), true),
        // For `vault_withdraw`: cost basis of the withdrawn equity, USDC.
        Field::new("basis", decimal_type(), true),
        // For `vault_withdraw`: net amount withdrawn, USDC.
        Field::new("net_withdrawn_usd", decimal_type(), true),
        // For `borrow_lend`: interest realized with this operation, in `token`.
        Field::new("interest_amount", decimal_type(), true),
        // For `borrow_lend`: the operation, verbatim (`supply`, `withdraw`, `borrow`, `repay`
        // observed).
        Field::new("operation", DataType::Utf8, true),
        // For `liquidation`: notional liquidated, USDC. Ledger liquidations are backstop
        // takeovers; their hash equals the hash of the takeover's two `LIQUIDATED_*` fills.
        // ADL-settled backstop liquidations have no ledger event.
        Field::new("liquidated_ntl_pos", decimal_type(), true),
        // For `liquidation`: account value, can be negative.
        Field::new("account_value", decimal_type(), true),
        // For `liquidation`: `CROSS` or `ISOLATED`.
        Field::new("leverage_type", enum_data_type(), true),
        // For `liquidation`: positions liquidated as `coin` and `szi`, in proto order. One
        // element and positive sizes in every observation.
        Field::new("liquidated_positions", positions_type(), true),
        // For `gossip_priority_auction_restart`: auction slot; 0 is a real slot.
        Field::new("slot_id", DataType::UInt64, true),
        // For `gossip_priority_auction_restart`: IPv4 address of the previous slot winner; NULL
        // when there was none (always together with `end_gas`). Before block 957002477 it was
        // never captured.
        Field::new("previous_winner_ip", DataType::Utf8, true),
        // For `gossip_priority_auction_restart`: HYPE clearing price the winner paid, equal to
        // the amount of the preceding `gossip_priority_gas_auction` ledger delta (paid a few
        // seconds before the restart, in the same 3-minute auction). NULL when there was no
        // winner.
        Field::new("end_gas", decimal_type(), true),
        // For `create_sub_account`: the user-chosen name, verbatim.
        Field::new("sub_account_name", DataType::Utf8, true),
        // For `funding`: number of `funding_deltas` rows, 0 when the event had no payments. For
        // `validator_rewards`: number of `validator_rewards` rows. NULL for other types.
        Field::new("item_count", DataType::UInt32, true),
        // EXTRA_JSON_DESCRIPTION (the same in every table).
        Field::new("extra_json", DataType::Utf8, true),
    ]
}

/// The schema of one event table: the [`event_fields`] it has, in catalogue
/// order.
pub fn event_table_schema(
    table: EventTable,
    include_fork_step: bool,
    encoding: &EncodeBytes,
) -> Schema {
    let fields = event_fields(encoding)
        .into_iter()
        .filter(|field| table.has_column(field.name()))
        .collect();
    table_schema(fields, include_fork_step, encoding)
}

/// `transfers`: Ledger `send`, `spot_transfer`, `internal_transfer`, `sub_account_transfer` and
/// `account_class_transfer` events.
pub fn transfers_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    event_table_schema(EventTable::Transfers, include_fork_step, encoding)
}

/// `bridge_transfers`: Ledger `deposit` and `withdraw` events, the Arbitrum USDC bridge.
pub fn bridge_transfers_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    event_table_schema(EventTable::BridgeTransfers, include_fork_step, encoding)
}

/// `vault_events`: Ledger `vault_create`, `vault_deposit`, `vault_withdraw`, `vault_distribution`
/// and `vault_leader_commission` events.
pub fn vault_events_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    event_table_schema(EventTable::VaultEvents, include_fork_step, encoding)
}

/// `staking_events`: `c_deposit`, `c_withdrawal`, `delegation` and ledger `c_staking_transfer`
/// events (HYPE staking).
pub fn staking_events_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    event_table_schema(EventTable::StakingEvents, include_fork_step, encoding)
}

/// `other_events`: Every other event, with every column of the catalogue.
pub fn other_events_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    event_table_schema(EventTable::OtherEvents, include_fork_step, encoding)
}

/// `funding_deltas`: One row per `FundingDelta`: the hourly funding settlement per account and
/// perp coin, which also snapshots every open perp position. All rows of one hour arrive in one
/// block.
pub fn funding_deltas_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    table_schema(
        vec![
            // Position of the parent funding event in the block; joins its header in
            // `other_events` and its rows in `funding_rates`. The event's ordinal among the
            // block's funding events is the perp-dex index (observed, not documented upstream).
            Field::new("event_index", DataType::UInt32, false),
            // 0-based position of the payment in its funding event.
            Field::new("delta_index", DataType::UInt32, false),
            // Account paying or receiving funding.
            Field::new("user", bd.clone(), false),
            // Perp symbol; HIP-3 coins carry their `<dex>:` prefix.
            Field::new("coin", DataType::Utf8, false),
            // Signed change to the account balance in the dex collateral (USDC on the default
            // dex); negative = paid. Its sign is opposite to that of `szi` times `funding_rate`.
            Field::new("funding_amount", decimal_type(), false),
            // Signed position size at funding time, negative = short.
            Field::new("szi", decimal_type(), false),
            // Hourly funding rate, signed; the same for every row of a coin in one event.
            Field::new("funding_rate", decimal_type(), false),
            // EXTRA_JSON_DESCRIPTION (the same in every table).
            Field::new("extra_json", DataType::Utf8, true),
        ],
        include_fork_step,
        encoding,
    )
}

/// `funding_rates`: One row per funding event and coin with at least one `funding_deltas` row, in
/// event order, then in the order each coin first appears: the hourly settled funding rate, the
/// open-interest census and the funding flows, summed exactly from the event's deltas (rule
/// R-D5). A coin-hour without a row is unknown, not zero.
pub fn funding_rates_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    table_schema(
        vec![
            // Position of the funding event in the block: joins its header in `other_events` and
            // its `funding_deltas` rows.
            Field::new("event_index", DataType::UInt32, false),
            // 0-based ordinal of the event among the block's funding events. By observation it is
            // the perp-dex index of HyperLiquid's `perpDexs` (0 = the default dex); that meaning
            // is not documented upstream.
            Field::new("dex_index", DataType::UInt32, false),
            // Perp symbol (`funding_deltas.coin`); HIP-3 coins carry their `<dex>:` prefix.
            Field::new("coin", DataType::Utf8, false),
            // Perp dex from `coin`, as `fills.dex`: `''` for the default dex, else the HIP-3 dex
            // name. NULL only for a coin form this version does not know.
            Field::new("dex", DataType::Utf8, true),
            // The hourly funding rate of the coin's deltas, signed. NULL if they differ (never
            // observed).
            Field::new("funding_rate", decimal_type(), true),
            // Number of the coin's deltas: its open positions (`szi` is never 0).
            Field::new("positions", DataType::UInt32, false),
            // Number of deltas with `szi > 0`.
            Field::new("long_positions", DataType::UInt32, false),
            // Number of deltas with `szi < 0`.
            Field::new("short_positions", DataType::UInt32, false),
            // Σ|`szi`| in coin units: HyperLiquid's `openInterest`, which counts both sides. NULL
            // only if the sum does not fit `decimal(38,10)`.
            Field::new("open_interest", decimal_type(), true),
            // Σ`szi` over `szi > 0`. NULL only if the sum does not fit `decimal(38,10)`.
            Field::new("long_size", decimal_type(), true),
            // −Σ`szi` over `szi < 0`, positive; equal to `long_size` up to `f64` noise. NULL only
            // if the sum does not fit `decimal(38,10)`.
            Field::new("short_size", decimal_type(), true),
            // Σ`funding_amount` over amounts above 0 (received), in the dex collateral. NULL only
            // if the sum does not fit `decimal(38,10)`.
            Field::new("positive_funding", decimal_type(), true),
            // Σ`funding_amount` over amounts below 0 (paid), kept negative. NULL only if the sum
            // does not fit `decimal(38,10)`.
            Field::new("negative_funding", decimal_type(), true),
            // Reserved: NULL in every row. A field upstream adds to `Funding` or `FundingDelta`
            // goes to `other_events.extra_json` or `funding_deltas.extra_json`.
            Field::new("extra_json", DataType::Utf8, true),
        ],
        include_fork_step,
        encoding,
    )
}

/// `validator_rewards`: One row per `ValidatorReward`: the per-minute reward accrual of every
/// validator.
pub fn validator_rewards_schema(include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let bd = bytes_data_type(encoding);
    table_schema(
        vec![
            // Position of the parent validator-rewards event in the block; joins its header in
            // `other_events`.
            Field::new("event_index", DataType::UInt32, false),
            // 0-based position of the validator in the reward list.
            Field::new("reward_index", DataType::UInt32, false),
            // Validator address.
            Field::new("validator", bd.clone(), false),
            // Reward accrued in this minute, HYPE (often 0). Whether it is before or after
            // commission is not documented.
            Field::new("reward", decimal_type(), false),
            // EXTRA_JSON_DESCRIPTION (the same in every table).
            Field::new("extra_json", DataType::Utf8, true),
        ],
        include_fork_step,
        encoding,
    )
}

/// The twelve HyperCore tables, in mapping order.
pub const TABLE_NAMES: [&str; 12] = [
    "blocks",
    "fills",
    "outcome_fills",
    "liquidations",
    "transfers",
    "bridge_transfers",
    "vault_events",
    "staking_events",
    "other_events",
    "funding_deltas",
    "funding_rates",
    "validator_rewards",
];

/// The tables derived from the block rather than copied from it (rules R-D3
/// to R-D5); they play no part in the lossless rebuild.
pub const DERIVED_TABLE_NAMES: [&str; 3] = ["outcome_fills", "liquidations", "funding_rates"];

/// The `fills` columns derived from the block (rules R-D1 and R-D2).
pub const DERIVED_FILL_COLUMNS: [&str; 3] = ["market_type", "dex", "counterparty"];
