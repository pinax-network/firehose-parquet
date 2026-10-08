# HyperCore notes

Semantics of the HyperCore (HyperLiquid L1, `pinax.hypercore.v1`) tables beyond
their columns, which the [HyperCore schema reference](../schemas/hypercore.md)
lists. Twelve tables, organised by product family (see the
[table catalogue](#table-catalogue)): the raw record (`blocks`, `fills`, the
five event tables `transfers`, `bridge_transfers`, `vault_events`,
`staking_events` and `other_events`, `funding_deltas` and
`validator_rewards`) and three tables derived from the same block
(`outcome_fills`, `liquidations` and `funding_rates`).

`--network hypercore` streams Pinax's `hypercore.firehose.pinax.network:443`.
The Graph networks registry does not list HyperCore: the alias comes from
fireparq's reviewed list of Pinax-served networks the registry lacks
([internal Pinax networks](../network-registry-integration.md#internal-pinax-networks)).
It is a built-in Pinax host, so the ambient `PINAX_API_KEY` (or the legacy
`SUBSTREAMS_API_KEY`) is sent without a selector
([Authentication](../authentication.md)), and `FIREHOSE_ENDPOINT_HYPERCORE`
overrides the endpoint (an override to another host is a custom endpoint that
gets no ambient key: name its credential with `--api-key-envvar`). The
endpoint's `chainName` (`hypercore`) resolves the block type. HyperCore data
is known from 2026-01-01, so a new root starts at block 846903317 by default
and an earlier `--start-block` is refused
([data origin](#identity-and-coverage)):

```bash
fireparq build --network hypercore --output s3://hypercore-mainnet
```

Do not set `--flush-rows` below about 1M (one funding block alone has about
435k rows; see [Volume and flushes](#volume-and-flushes)). The
[maintenance job](../delta-maintenance.md) needs all twelve tables:
`LAKE_TABLES=blocks,fills,outcome_fills,liquidations,transfers,bridge_transfers,vault_events,staking_events,other_events,funding_deltas,funding_rates,validator_rewards`.

## Table catalogue

Rows per day are for October 2026; Parquet sizes are DuckDB zstd estimates.

| Family | Table | Layer | Grain and key | Purpose | Rows/day | Parquet/day |
|---|---|---|---|---|---|---|
| Perps (core and HIP-3), spot, HIP-4 | `fills` | raw, plus 3 derived columns | one fill leg; `(block_num, fill_index)` | Every trade leg of every market. `market_type` and `dex` select core perps (`perp`, `dex = ''`), HIP-3 perps (`perp`, the dex name), spot (`spot`) and HIP-4 outcomes (`outcome`); `counterparty` gives maker and taker. | 10.3M (13.6M peak) | 0.55–0.74 GB |
| Perps | `liquidations` | derived | the liquidated leg; `(block_num, fill_index)` | Every liquidation, with its method, mark price, counterparty and the counterparty's direction. | about 6k (3.1k–17.9k) | about 0.5 MB |
| Perps | `funding_rates` | derived | funding event × coin; `(block_num, event_index, coin)` | The hourly settled funding rate, the open-interest census, holder counts and funding flows. | about 8.0k | about 0.4 MB |
| Perps | `funding_deltas` | raw | account × coin × hour; `(block_num, event_index, delta_index)` | Funding paid per account, and an hourly snapshot of every open position. | about 10.4M | about 0.24 GB |
| HIP-4 outcomes | `outcome_fills` | derived | outcome fill leg; `(block_num, fill_index)` | Every outcome leg with typed `outcome_id` and `side_index`: trades, mints, burns, split, merge, negate and settlements. | about 55k (0 before 2026-05-02) | about 2.5 MB |
| Transfers and bridge | `transfers` | raw (event split) | event; `(block_num, event_index)` | `send`, `spot_transfer`, `internal_transfer`, `sub_account_transfer`, `account_class_transfer`. HyperEVM↔HyperCore moves are `send` and `spot_transfer` rows with a `0x20…` or `0x2222…` system address. | about 84k | about 7.6 MB |
| Transfers and bridge | `bridge_transfers` | raw (event split) | event; `(block_num, event_index)` | The Arbitrum USDC bridge: `deposit` and `withdraw`. | about 3.0k | about 0.3 MB |
| Vaults | `vault_events` | raw (event split) | event; `(block_num, event_index)` | `vault_create`, `vault_deposit`, `vault_withdraw`, `vault_distribution`, `vault_leader_commission`. | about 2.0k | about 0.2 MB |
| Staking | `staking_events` | raw (event split) | event; `(block_num, event_index)` | `c_deposit`, `c_withdrawal`, `delegation` and ledger `c_staking_transfer` (HYPE staking, not the bridge). | about 1.5k | about 0.15 MB |
| Staking | `validator_rewards` | raw | validator × minute; `(block_num, event_index, reward_index)` | Per-minute validator reward accrual. | about 50k | under 0.2 MB |
| System | `blocks` | raw | block; `block_num` | Block identity, time and counts. | 1.21M | 25–30 MB |
| System and catch-all | `other_events` | raw (event split) | event; `(block_num, event_index)` | Every other event ([routing](#event-tables-and-routing)), and later types that fit no other event table. | about 5.4k | about 0.5 MB |

- **HIP-3 and spot are columns, not tables.** They share the perp data model:
  HIP-3 is `market_type = 'perp' AND dex <> ''` (35% of fills), spot is
  `market_type = 'spot'`. 0xArchive's families map onto the lake as follows;
  `trades_v.family` names them:

  | Family | Filter |
  |---|---|
  | core | `market_type = 'perp' AND dex = ''` |
  | HIP-3 | `market_type = 'perp' AND dex <> ''` |
  | spot | `market_type = 'spot'` |
  | HIP-4 | `market_type = 'outcome'` |

- **`fills` stays complete.** HIP-4 and liquidation legs are in `fills` too:
  `outcome_fills` and `liquidations` are copies of `fills` rows, so never add
  them to `fills`.
- **The derived tables are facts only**: rows selected and copied from the same
  block, joins on exact keys within the block, exact sums and counts, and
  parsing of `coin` ([Derivation rules](#derivation-rules)). Every
  interpretation (liquidation kind, HIP-4 mint and burn matching, positions,
  the funding price, vocabularies) is a [view](#views-monitors-and-cookbook),
  free to change.
- **Names and labels from `/info`** (spot pair and token names, outcome and
  question labels, perp-dex names, the funding premium) are not in the
  protobuf and are not in the lake. A separate reference job can provide them
  as `hl_*` tables, joined at query time
  ([optional reference joins](#optional-reference-joins)).

## Identity and coverage

- HyperCore has no block hash. `block_id` and `parent_id` are the Firehose block
  ids, the decimal block number as text (`"1174085339"`), written verbatim under
  every encoding (their ASCII bytes under `binary`). Files record
  `firehose-parquet.block_id_encoding = decimal`. The optional cursor mirror
  (`_fireparq/cursor.parquet`) hex-decodes its `last_block_id`, so for
  HyperCore it holds different bytes for even- and odd-length ids: use its
  `last_block_num`.
- `parent_num` and `lib_num` are both `block_num - 1`: a block is final one
  block later, so the default `--final-blocks-only=true` is the mode to use.
- About 14 blocks per second: block intervals are 67 ms at the median and
  124–162 ms at p99; the longest seen was 73 s (2026-05-07). Block times strictly
  increase.
- **Data origin: 2026-01-01, block 846903317.** HyperCore data is known from
  2026-01-01; earlier blocks are not supported. 846903317
  (2026-01-01T00:00:00.063Z) is the first block of that day, with hourly
  funding, the daily dust conversion and validator rewards. The endpoint
  advertises `firstStreamableBlockNum` 846000000, so fireparq bounds new roots
  itself, by the EndpointInfo chain name `hypercore` (with `--network` or
  `--endpoint`, and in dry runs):
  - without `--start-block`, a new root starts at 846903317, and startup logs
    that the network's data origin was used;
  - an explicit `--start-block` below 846903317 is refused before streaming:
    use 846903317 or later;
  - resuming a root from its output authority is unaffected.
- **The hole.** Blocks 846903300–846903312 do not exist on the endpoint, but
  846903313 still names 846903312 as its parent. A stream cannot cross it: it
  delivers blocks up to 846903299, then retries `unexpected stream termination`
  until `--reconnect-stall-timeout-secs` passes (default 900 s; see
  [connection errors](../cli.md#connection-errors)) and exits 1 with
  `reconnect stalled`. The endpoint serves a start inside it from 846903313,
  without a warning. The data origin is past it, so no root reaches it,
  `fireparq validate` never reports it and `--allow-gaps` is not needed for it.
- **Other holes.** Upstream runs its readers with
  `--reader-node-skip-missing-blocks`, so other holes are possible; no full
  contiguity scan of the endpoint exists. A build that reaches one stops as at
  the hole above: blocks buffered since the last flush are discarded, committed
  flushes stay readable, and every restart resumes from the same cursor and
  stops there again. Ask upstream to re-extract the missing blocks (the same
  root then resumes), or build a new output root whose `--start-block` is after
  the hole, preferably the first block of the next UTC day so that no partition
  is partial.
- **Not captured before the 2026-04-13 reader cutover.** NULL there means "not
  captured", not zero:
  - `fills.deployer_fee` before block 957002477;
  - `fills.builder` and `fills.builder_fee` before block 957002478 (2,000-block
    samples from 2026-01-01 to 2026-04-13, about 71k fills, had no builder,
    while blocks before the data origin had some, so a NULL there may hide a
    builder);
  - `other_events.previous_winner_ip` and `other_events.end_gas` before block
    957002477.

  `fills.priority_gas` is NULL before about 2026-04-20 because the feature did
  not exist. The derived copies (`outcome_fills`, `liquidations`) carry the
  same NULLs.
- **First appearance** of each type, so that absence is not read as zero
  activity:

  | Type or field | First seen |
  |---|---|
  | `gossip_priority_auction_restart` | about 2026-04-13 (e.g. block 956997125) |
  | `gossip_priority_gas_auction` | block 957002528 |
  | outcome (`#`) coins, `outcome_fills` rows, and the `SPLIT_OUTCOME`, `MERGE_OUTCOME`, `NEGATE_OUTCOME` and `MERGE_QUESTION` directions | HIP-4 launched 2026-05-02 |
  | `hip3_liquidator_deposit` | block 1075986891 (2026-07-17; 2 seen ever) |
  | `create_sub_account` | block 1170059852 (2026-10-03). Sub-accounts created earlier have no row. |

## Time columns

- `timestamp` (milliseconds) is the partition time, the canonical block time
  truncated to milliseconds.
- `blocks.block_time_ns` is the exact block time in nanoseconds.
- `fills.fill_time` (milliseconds) and the event tables' `event_time_ns` are
  the payload's own times. They equal the block time in every block seen, are
  stored as delivered without being checked, and are monitored (M1, M2). A
  fill time that is not a whole millisecond is refused (R11), so `fill_time`
  never rounds. `outcome_fills` and `liquidations` leave `fill_time` out: it
  is `timestamp`.

## Decimals

- Every amount, price and size is `decimal(38,10)`, the exact value of
  HyperLiquid's decimal string. HyperLiquid's values are `f64` values rounded to
  10 places, so they can look like `6426274.5300000003`.
- The parser accepts only exact values: `^-?[0-9]+(\.[0-9]+)?$`, at most 10
  significant fractional digits and 28 integer digits. It never rounds, and it
  refuses exponents, a `+` sign, a leading or trailing `.`, whitespace and
  `NaN` (R8). Exact but non-canonical text (`"1.50"`, `"5"`) is stored as its
  value.
- The sums of `funding_rates` are exact `i128` sums of these values; a sum that
  would not fit `decimal(38,10)` is NULL, never rounded (it cannot happen at
  HyperLiquid's magnitudes).
- **DuckDB** (1.5.5 and 1.5.6):
  - `price * size` is `DECIMAL(38,20)`, exact; a product of two and its `sum`
    need no cast;
  - `sum(decimal)` is `DECIMAL(38,10)`;
  - `avg(decimal)` is `DOUBLE`;
  - a product of three decimals is `DECIMAL(38,30)`, which holds only 8
    integer digits. It type-checks, then raises "Overflow in multiplication of
    DECIMAL(38)" at runtime once a row's result reaches 1e8:
    `szi * funding_rate * szi` runs on rows with `abs(szi) < 100` but fails on
    real funding blocks, so a small sample can hide the error. Cast an
    intermediate first: to `DOUBLE` (approximate), or to `DECIMAL(38,10)`,
    which rounds each value to 10 places, so the result is not exact either
    (cookbook C13).
- **Spark:** with the default `spark.sql.decimalOperations.allowPrecisionLoss=true`,
  Spark's documented result-type rule lowers the scale of a `decimal(38,10)`
  product to 6 digits. Cast first when more precision is needed. This was not
  tested here.
- **Text:** the lake stores values, not text. HyperLiquid's canonical text is
  regenerable: the integer part without leading zeros (`0` if none), `.`, the
  fraction without trailing zeros (`0` if none), `-` before a negative value
  ([Lossless](#lossless)).

## Bytes and zero values

- Bytes are lowercase `0x` hex by default. Observed lengths: addresses 20 bytes,
  hashes 32, client order ids 16 (monitor M5). `fills.builder` is a protobuf
  string, kept verbatim (`0x` and 40 lowercase hex characters, monitor M7).
- **Zero values are stored as delivered:**
  - The all-zero hash means "no L1 transaction": 11.5% of fills (TWAP slices and
    their counterparty, the daily dust conversion, some outcome fills), every
    funding, validator-reward and gossip-restart event, staking-withdrawal
    finalization and its transfer, and rare sends (3 in about 220k events).
  - **Joining on `hash` without excluding the zero value fans out:** one dust
    block alone has 1,523 zero-hash fills (1,451 conversion fills, plus 36
    pairs in which `0xeeee…eeee` sells the collected dust). Use
    `hypercore_fills_v.tx_hash`, or `hash <> hc_zero_hash()`.
  - The zero address in `fills.user` (and so in the other leg's
    `counterparty`) is the counterparty of a delisted-perp `SETTLEMENT`.
  - `0x3200…02xx` addresses are the outcome-settlement counterparties;
    `0x2000…{token}` and `0x2222…2222` are HyperEVM system senders; `0x4000…{dex}`
    is a HIP-3 backstop liquidator; `0xfefe…fefe` is the assistance fund;
    `0xeeee…eeee` sells the daily spot dust it collects (crossed `ASK` legs
    with non-zero trade ids in the 00:00 UTC block).
- **Hash layout:** a non-zero HyperCore hash has `hash[10] = 0x04` and
  `hash[11..15]` = the block number, big-endian. Deposit and withdraw events
  carry Arbitrum One transaction hashes instead (monitor M4).

## Fills

- **Pairs.** A normal trade is two adjacent rows, `BUY` then `ASK`, sharing
  `transaction_id`, `hash`, `price` and `size`, with exactly one `crossed` leg
  (0 exceptions in about 34M pairs, monitors M9 and M14).
  - The taker is the `crossed` leg, the maker the other one. Each leg's
    `counterparty` is the other leg's `user` ([R-D2](#r-d2-counterparty)), so
    `fills` already has the shape of a trades table (`trades_v`).
  - Count trades, volume and notional on `crossed` rows only, or volume doubles.
  - `client_order_id` does not mark the taker.
- **Exceptions to pairing**, whose `counterparty` is NULL:
  - Outcome markets can produce single fills: split, merge, negate and
    merge-question fills, and complementary mint or burn with a separate trade
    id per leg (`outcome_matches_v` pairs mints and burns).
  - The 00:00 UTC block holds the daily spot dust conversion: 1.4–2.4k fills
    with `transaction_id = 0`, a zero hash, `ASK`, crossed and fee 0.
- **`transaction_id`** is HyperLiquid's trade id `tid`, not a transaction: a
  50-bit hash of the two order ids, shared by both legs. Use `hash` for the L1
  transaction. A globally unique trade is (time, coin, tid);
  `hypercore_fills_v.trade_id` is NULL for tid 0.
- **`side`.** `BUY` and `ASK` are the protobuf labels. HyperLiquid's raw values
  are `B` and `A`, and the Pinax API uses `BID` and `ASK`
  (`hypercore_fills_v.side_bid_ask`). For outcomes, `BUY` means shares received.
- **Directions.**
  - Non-trade (housekeeping or reshaping): `SPOT_DUST_CONVERSION`, `SETTLEMENT`,
    `NET_CHILD_VAULTS`, `SPLIT_OUTCOME`, `MERGE_OUTCOME`, `MERGE_QUESTION`,
    `NEGATE_OUTCOME` (`hypercore_fills_v.is_non_trade`).
  - Forced trades (off the order book): `AUTO_DELEVERAGING`, `LIQUIDATED_*`,
    `*_BORROW_LIQUIDATION` (`hypercore_fills_v.is_forced`). Price series
    usually exclude them. `market` liquidations use the ordinary directions
    and stay in.
  - `SETTLEMENT` covers both HIP-4 resolution (counterparty `0x3200…`) and a
    delisted-perp close-out (counterparty the zero address, one shared
    `order_id`, fee 0).
  - Never seen so far: `LIQUIDATED_CROSS_SHORT`, `BACKSTOP_BORROW_LIQUIDATION`,
    `PARTIAL_BORROW_LIQUIDATION`.
- **Liquidations.** `liquidations` holds the liquidated legs
  ([R-D4](#r-d4-liquidations)); `liquidations_v` adds the kind, the notional
  and the ledger event.
  - The liquidation columns of `fills` are set on **both** legs; a fill is a
    liquidation exactly when `liquidation_method IS NOT NULL`.
  - The liquidated side is the row where `user = liquidated_user`: the
    `liquidations` row. Which leg is crossed depends on the method:
    - `market`: the liquidated leg is crossed; it is the liquidation order.
    - `backstop`: the liquidated leg is not crossed. The crossed leg belongs to
      the liquidator (on `LIQUIDATED_*` pairs) or, under ADL, to the
      counterparty (`AUTO_DELEVERAGING`). This held in 15 of 15 fixture fills.

    Do not read `crossed` on the liquidated leg as "the liquidated user was
    the taker".
  - `market` liquidations use the ordinary open and close directions and emit
    no ledger event.
  - `backstop` fills come in two shapes, and only one has a ledger event:
    - Takeover by the backstop liquidator: both legs are `LIQUIDATED_*`
      (`liquidations.counterparty_direction LIKE 'LIQUIDATED_%'`). The
      takeover shares its hash with exactly one ledger `liquidation` event in
      `other_events`: 12 of 12 in the fixtures (block 1127672017), and all 16
      ledger liquidations in the live sample. `liquidations_v` links it.
    - Settlement by ADL: the liquidated leg is `LIQUIDATED_*` and the
      counterparty leg is `AUTO_DELEVERAGING`
      (`liquidations.counterparty_direction`). These emit no ledger event. In
      blocks 1010581248 and 1010581292 the ADL fills share their hash with the
      same liquidation's `market` fills.

    To count backstop liquidations, count `liquidations` rows with
    `liquidation_method = 'backstop'`, not ledger events.
- **Fees.**
  - `fee` is in `fee_token`; negative is a maker rebate (27% of fills).
  - `fee` includes `builder_fee` (documented) and `deployer_fee` (ratio
    evidence only).
  - `builder` can appear without `builder_fee`: HyperLiquid omits a zero fee.
  - `priority_gas` is HYPE, on the taker leg only.
  - Outcome fills pay fees too: `fee_token` is `+<n>` (the outcome token) in
    May 2026 and `USDC` in October.
- **Coins and market classes** (`market_type` and `dex`,
  [R-D1](#r-d1-market_type-and-dex)):

  | Coin form | `market_type` | `dex` |
  |---|---|---|
  | `BTC`, `kPEPE` | `perp` (core) | `''` |
  | `<dex>:<SYM>` (xyz, io, para, mkts, hyna, km, cash, flx, vntl) | `perp` (HIP-3) | the dex name |
  | `@<n>`, or `PURR/USDC` (the only named pair) | `spot` | NULL |
  | `#<10·outcome_id + side_index>` | `outcome`; its fee token is `+<n>` | NULL |

  Spot pair names and outcome metadata are **not** in the protobuf; they come
  from HyperLiquid's `/info` ([optional reference joins](#optional-reference-joins)).
  Do not derive "perp" from "no `:`" alone: `PURR/USDC` is spot.
- **`start_position`** is a signed size for perps and a balance for spot and
  outcomes. The position after the fill is `start_position ± size`.
- Whether **`closed_pnl`** is gross or net of fees is not documented.

## Event tables and routing

Every event of a block is one row in exactly one of the five event tables, by
its labels: `event_type` (the `EventBody` case) and, for `ledger_update`,
`ledger_type` (the `LedgerUpdateDelta` case).

| Table | Labels | Rows/day |
|---|---|---|
| `transfers` | ledger `send` (52.9k), `spot_transfer` (22.6k), `internal_transfer` (4.9k), `account_class_transfer` (2.0k), `sub_account_transfer` (1.2k) | 83.6k |
| `bridge_transfers` | ledger `deposit` (1,244), `withdraw` (1,758) | 3.0k |
| `vault_events` | ledger `vault_withdraw` (1,352), `vault_deposit` (384), `vault_leader_commission` (240), `vault_distribution` (18), `vault_create` (about 0) | 2.0k |
| `staking_events` | `c_deposit` (282), `c_withdrawal` (347), `delegation` (430); ledger `c_staking_transfer` (479) | 1.5k |
| `other_events` | `funding` (264 headers), `validator_rewards` (1,440 headers), `gossip_priority_auction_restart` (959), `create_sub_account` (102); ledger `gossip_priority_gas_auction` (960), `borrow_lend` (1,350), `rewards_claim` (278), `account_activation_gas` (63), `liquidation` (rare), `spot_genesis`, `deploy_gas_auction`, `activate_dex_abstraction` (0–5), `hip3_liquidator_deposit` (2 ever) | about 5.4k |

- **One catalogue of columns.** `other_events` has all 43 event columns;
  every other event table has the shared ones (`event_index`, `event_type`,
  `ledger_type`, `hash`, `event_time_ns`, `users`, `extra_json`) and the
  columns its types set, with the same names, types, nullability and
  descriptions, in the same order. Nothing is renamed or normalised.
- **The union is the event list.** `event_index` is the event's position in
  the block over all five tables, and `blocks.event_count` is the block's
  total over them. The `events` view (the first SQL block below) is the union,
  with `other_events`' columns in their order; `funding_deltas`,
  `funding_rates` and `validator_rewards` join their header in
  `other_events` on `(block_num, event_index)`.
- **Routing only grows** ([R-D6](#r-d6-event-routing)). A label stays in its
  table for the life of a root. A label that upstream adds is refused by the
  running release (R2) until a release vendors it; that release may route it
  to a domain table, since no earlier row of it exists, and otherwise routes
  it to `other_events`. A new field of a routed type goes to a typed column
  only if that table has a column with the same name, protobuf type and
  meaning, otherwise to `extra_json`. `value_tests.rs` pins every label's
  table against the matrix below, and monitor M21 flags a label it does not
  list.

### The columns each type sets

Every row sets the canonical columns, `event_index`, `event_type`, `hash` and
`event_time_ns`; every `ledger_update` row also sets `ledger_type` and `users`.
Every other column is NULL unless listed for the row's type. A listed column
holds the protobuf value, defaults included (`false`, `0`); `?` marks a column
that is NULL when the value is absent or empty. `Table` is the type's event
table. `blocks/src/hypercore/value_tests.rs` checks every fixture row against
this table, and that every column a type sets is a column of its table.

| Type | Table | Columns set |
|---|---|---|
| `funding` | `other_events` | `item_count` |
| `validator_rewards` | `other_events` | `item_count` |
| `c_withdrawal` | `staking_events` | `user`, `amount`, `is_finalized` |
| `c_deposit` | `staking_events` | `user`, `amount` |
| `delegation` | `staking_events` | `user`, `validator`, `amount`, `is_undelegate` |
| `gossip_priority_auction_restart` | `other_events` | `slot_id`, `previous_winner_ip`?, `end_gas`? |
| `create_sub_account` | `other_events` | `user`, `sub_account`, `sub_account_name` |
| `ledger_update` / `spot_transfer` (1) | `transfers` | `token`, `amount`, `usdc_value`, `user`, `destination`, `fee`, `native_token_fee`, `nonce`, `fee_token`? |
| `ledger_update` / `c_staking_transfer` (2) | `staking_events` | `token`, `amount`, `is_deposit` |
| `ledger_update` / `account_class_transfer` (3) | `transfers` | `usdc`, `to_perp` |
| `ledger_update` / `internal_transfer` (4) | `transfers` | `usdc`, `user`, `destination`, `fee` |
| `ledger_update` / `sub_account_transfer` (5) | `transfers` | `usdc`, `user`, `destination` |
| `ledger_update` / `send` (6) | `transfers` | `user`, `destination`, `source_dex`, `destination_dex`, `token`, `amount`, `usdc_value`, `fee`, `native_token_fee`, `nonce`, `fee_token`? |
| `ledger_update` / `deposit` (7) | `bridge_transfers` | `usdc` |
| `ledger_update` / `withdraw` (8) | `bridge_transfers` | `usdc`, `nonce`, `fee` |
| `ledger_update` / `vault_deposit` (9) | `vault_events` | `vault`, `usdc` |
| `ledger_update` / `rewards_claim` (10) | `other_events` | `amount`, `token` |
| `ledger_update` / `vault_withdraw` (11) | `vault_events` | `vault`, `user`, `requested_usd`, `commission`, `closing_cost`, `basis`, `net_withdrawn_usd` |
| `ledger_update` / `vault_leader_commission` (12) | `vault_events` | `user`, `usdc` |
| `ledger_update` / `deploy_gas_auction` (13) | `other_events` | `token`, `amount` |
| `ledger_update` / `account_activation_gas` (14) | `other_events` | `amount`, `token` |
| `ledger_update` / `activate_dex_abstraction` (15) | `other_events` | `dex`, `token`, `amount` |
| `ledger_update` / `liquidation` (16) | `other_events` | `liquidated_ntl_pos`, `account_value`, `leverage_type`, `liquidated_positions` |
| `ledger_update` / `spot_genesis` (17) | `other_events` | `token`, `amount` |
| `ledger_update` / `vault_distribution` (18) | `vault_events` | `vault`, `usdc` |
| `ledger_update` / `borrow_lend` (19) | `other_events` | `token`, `amount`, `interest_amount`, `operation` |
| `ledger_update` / `vault_create` (20) | `vault_events` | `vault`, `usdc`, `fee` |
| `ledger_update` / `gossip_priority_gas_auction` (21) | `other_events` | `token`, `amount` |
| `ledger_update` / `hip3_liquidator_deposit` (22) | `other_events` | `dex`, `token`, `amount` |

The numbers are the `LedgerUpdateDelta` case numbers.

### Hashes, accounts and pairs

- **Hash kinds:** the HyperCore L1 hash of the user action; the Arbitrum One
  transaction hash for `deposit` and `withdraw` (it can recur a few blocks
  later, and batched bridge transactions share one); zero for system events.
- **Hashes shared within a block:** `c_deposit` and `c_staking_transfer`
  (adjacent; the transfer comes first in the fixtures),
  `vault_withdraw` and `vault_leader_commission`, `account_activation_gas` and
  `spot_transfer`, batched withdraws, and `vault_distribution` (one vault-side
  event plus one per recipient; 1 to 204 recipients observed). Such pairs can
  sit in two event tables (`account_activation_gas` in `other_events`, its
  `spot_transfer` in `transfers`): join them on `(block_num, hash)`.
- **`users`**, HyperLiquid's ledger index, by type (monitor M6 checks the 1–2
  entries):

  | Type | `users` |
  |---|---|
  | `send`, `spot_transfer`, `internal_transfer` | `[user, destination]`; `[user]` for a cross-dex send to self |
  | `sub_account_transfer` | 2 entries, order varies (`[user, destination]` in about 51%) |
  | `vault_withdraw` | `[user, vault]` |
  | `vault_deposit`, `vault_create` | 2 entries including the vault |
  | `vault_leader_commission` | `[user]` |
  | `vault_distribution` | **1** entry. One event per affected ledger, all sharing the hash and `vault`. One event is the vault's own (`users = [vault]`) and its `usdc` is the distributed total. Each recipient gets one event (`users = [recipient]`) carrying its share, and the shares add up to the total. `sum(usdc)` over every event is therefore exactly twice the distribution: use `users[1] = vault` for totals and `users[1] <> vault` for per-recipient credits (`vault_events_v.is_vault_total`, cookbook C15). |
  | `hip3_liquidator_deposit` | `[depositor, 0x4000…{dex index}]` |
  | every other type | exactly the one account (no other address on the row) |

  `hypercore_account_events_v` gives one row per (event, account) from `users`
  and the body's `user`, over all five event tables.

### Staking, fees, nonces

- **Staking.** `c_deposit` and `c_withdrawal` are HYPE moves between spot and
  staking, **not** bridge flows; USDC bridge flows are the ledger `deposit` and
  `withdraw` (`bridge_transfers`).
  - `c_deposit` pairs with `c_staking_transfer` where `is_deposit` is true (same
    hash and amount): **count one of them** (`staking_flows_v`, cookbook C14).
  - A `c_withdrawal` request (`is_finalized` false) moves no balance. The
    finalization (true, about 7 days later, zero hash) pairs with a
    `c_staking_transfer` where `is_deposit` is false.
- **Fee units.** `send` and `spot_transfer`: `fee` in `fee_token`; `fee_token`
  is NULL exactly when `fee = 0` (0 exceptions, monitor M12);
  `native_token_fee` is HYPE. USDC: `internal_transfer` (0 or 1), `withdraw`
  (always 1) and `vault_create` (100 or 10000).
- **Nonces.** Epoch-millisecond action nonces for user-signed actions; a global
  sequence (about 3.88M) for HyperEVM-originated sends and spot transfers
  (CoreWriter, `0x2000…`); occasionally 0. `withdraw.nonce` is milliseconds
  times 1000 and differs from the event time by −12.6 h to +28.2 h. Not a clock.
- **`borrow_lend`** (HyperLiquid lending, in `other_events`): `operation` is
  `supply`, `withdraw`, `borrow` or `repay`; tokens observed: USDC, HYPE, UBTC,
  USDH, USDT0.
- **Ledger `liquidation`** (in `other_events`): backstop takeovers only
  (ADL-settled backstop liquidations have none); one position with a positive
  `szi` in every observation.
- **Ordering.** Funding events come first in their block. Do not assume other
  system events come first: about 8% of validator-reward blocks had user ledger
  events before `validator_rewards`.

## Funding

- Hourly, in the first block at or after `HH:00:00`. That block holds one
  funding event per perp dex, in perp-dex order, including dexes with no
  payments (`item_count = 0`; about 34% of events). The headers are
  `other_events` rows with `event_type = 'funding'`; the payments are
  `funding_deltas`, about 435k per hour in October 2026 (about 26 MB of
  protobuf in one block), about 10.4M per day, growing.
- `funding_rates` rolls each event's deltas up per coin
  ([R-D5](#r-d5-funding_rates)): the rate, the number of positions (long and
  short), the open interest, and the funding received and paid, 1,300 times
  fewer rows than `funding_deltas`.
- **The perp-dex index** is the event's ordinal among the block's funding
  events: `funding_rates.dex_index`, and `hypercore_funding_events_v.dex_index`
  for the headers, empty events included. Index order: 0 = the default dex
  (`''`), 1 xyz, 2 flx, 3 vntl, 4 hyna, 5 km, 6 abcd, 7 cash, 8 para, 9 mkts,
  10 io. There were 6 dexes in January 2026, 9 in May and 11 in October.
  **Observed, not documented upstream:** it was verified against HyperLiquid's
  `perpDexs` on two hours, the layout (funding events at positions 0..N−1) on
  every sampled funding block (monitor M3), and `dex` from the coin agrees
  with it in every `funding_rates` row seen (monitor M18).
- `(block_num, user, coin)` is unique in `funding_deltas`, and `funding_rate`
  is constant per coin per event (monitor M17).
- The sign of `funding_amount` is opposite to that of `szi × funding_rate`; 61%
  of amounts are negative.
- The collateral of HIP-3 dexes is inferred from `fee_token`: USDT0 for cash,
  USDH for km, flx and vntl, USDE for hyna.
- **Open interest.** `funding_rates.open_interest`, Σ|`szi`| per coin at a
  funding block, follows HyperLiquid's own `openInterest` definition, which
  counts both sides: across all 178 core coins compared, HyperLiquid's value
  was closer to Σ|`szi`| than to the one-sided size, with a median ratio of
  0.97. `long_size` (= `short_size` up to `f64` noise, monitor M20) is half of
  it. Positions opened and closed between two funding blocks are invisible.
- **A missing coin-hour is unknown, not zero.** No delta with
  `funding_rate = 0` exists in about 3.4M deltas sampled: HyperLiquid appears
  to omit zero-rate coin-hours. `xyz:GBP` had at least 6 open positions at
  19:00 on 2026-10-06 yet no deltas at 18:00–21:00. So `open_interest` is
  exact for the rows that exist; monitor M22 lists perp coins traded in the
  hour before a funding block without a row at it.

## Validator rewards and gossip

- **Validator rewards:** one event every minute listing all 30–35 validators;
  22% of rewards are 0. Whether `reward` is gross or net of commission is not
  documented.
- **Gossip auction:** restarts every 3 minutes, usually slots 0 and 1 (up to 4
  historically). `previous_winner_ip` and `end_gas` are present together (about
  88%, monitor M10). `end_gas` equals the amount of the
  `gossip_priority_gas_auction` ledger delta that paid for the slot. That
  payment comes *before* the restart: 6–18 s earlier, in the same 3-minute
  auction, observed in May, September and October 2026. To pair a restart with
  its payment, match on `amount = end_gas` within the preceding 3 minutes, not
  with the next payment. A restart near the first block of a root can have its
  payment before that block. That the payer is the winner is inferred: its
  address cannot be linked to `previous_winner_ip`.

## Derivation rules

The mapper derives `fills.market_type`, `fills.dex` and `fills.counterparty`,
the three derived tables and the event routing in one step after a block is
staged and validated, before any row is appended. The rules:

- read **one block only** and carry no state across blocks, so flushes,
  restarts and non-final `UNDO` events behave as for raw rows;
- **never refuse a block**: refusals are R1–R11 only. A shape a rule does not
  recognise gives NULL derived values (or no derived row), and a monitor
  catches it;
- are **facts only**: row selection, copies, joins on exact keys within the
  block, exact checked sums and counts, and parsing of `coin`. Classifications,
  heuristics, approximations, vocabularies and joins between two small tables
  are views;
- are **versioned**: every table's schema carries the metadata
  `fireparq.hypercore.derivation = "1"`.

The macros `hc_market_type`, `hc_dex` and `hc_event_table` of the
[view pack](#view-pack) state R-D1 and R-D6 in SQL (monitors M19 and M21
compare the lake with them), and `blocks/src/hypercore/value_tests.rs`
re-derives every derived value from the raw output with a second, naive
implementation and compares the two on all 36 fixtures.

### R-D1 `market_type` and `dex`

A pure function of `coin`. Anchored patterns, tried in order:

| Pattern | `market_type` | `dex` |
|---|---|---|
| `#[0-9]+` | `outcome` | NULL |
| `@[0-9]+` or `[A-Za-z0-9]+/[A-Za-z0-9]+` | `spot` | NULL |
| `[a-z][a-z0-9]*:[A-Za-z0-9]+` | `perp` | the text before `:` |
| `[A-Za-z0-9]+` | `perp` | `''` |
| anything else | NULL | NULL |

- `kPEPE`-style prefixes are core perps; `PURR/USDC` is spot.
- `''` names the default (core) perp dex, as HyperLiquid's `dex: ""`, the
  `source_dex` and `destination_dex` of a `send`, and `dex_index` 0 do. A
  `perp` covers HIP-3 too, so `WHERE market_type = 'perp'` keeps every perp.
- **A coin form HyperLiquid introduces later is NULL, never a guess and never
  a refusal**: a new form can arrive without a proto change. Monitor M19 is
  the early warning for a new market class; the raw `coin` is always there.
  An outcome number above `u64::MAX` is NULL too.
- Every coin of the sample lakes, all 536 names of `allPerpMetas` (delisted
  ones included) and every `spotMeta` pair match a pattern.

### R-D2 `counterparty`

- Within the block, fills with a non-zero `transaction_id` are grouped by
  `(coin, transaction_id)`. A group of **exactly two** fills on **different
  sides** pairs them: each fill's `counterparty` is the other's `user`.
  Every other fill's is NULL.
- The trade id is HyperLiquid's hash of the two order ids, so the key is the
  match. The rule does not require adjacency, equal price or size, or one
  crossed leg; monitor M14 checks those.
- **Edge cases:** the daily dust conversion (trade id 0) and single-leg HIP-4
  fills (mint, burn, split, merge, negate, merge-question) have no
  counterparty; a delisted-perp `SETTLEMENT` has the zero address and a HIP-4
  `SETTLEMENT` a `0x3200…` system account; a self-trade has `counterparty =
  user`. A group of three or more legs, or of two legs on one side, has never
  been seen: NULL, and monitor M15.
- In the samples every perp and spot leg with a non-zero trade id was paired;
  outcome legs were paired at 43–83% (the direct trades and settlements).

### R-D3 `outcome_fills`

- One row for every fill with `market_type = 'outcome'`, in fill order, with
  `n` the integer after `#`: `outcome_id = n div 10` and `side_index = n mod
  10` (HyperLiquid's asset id is 100,000,000 + 10·outcome + side). Side 0 is
  the outcome's first side ("Yes" on a binary outcome); only 0 and 1 have been
  seen, but nothing assumes two. `side_index` is not `side`.
- Every other column is the `fills` row's value; `counterparty` follows R-D2.
  `fill_time` (always `timestamp`), the liquidation columns (outcomes are fully
  collateralised: never set) and `market_type` and `dex` (constant) are left
  out.
- The legs stay in `fills`: `fills` is the complete record. Outcomes are 0.6%
  of fills but sit in every row group of `fills`, so this table reads about
  170 times less than a filter on `fills`.

### R-D4 `liquidations`

- One row for every fill with a liquidation whose `user` equals its
  `liquidated_user`, byte for byte: the liquidated leg. Its other values are
  the `fills` row's (`mark_price` is `liquidation_mark_px`); `counterparty`,
  `counterparty_direction` and `counterparty_fill_index` describe the R-D2
  paired leg.
- That is all: the link to the ledger `liquidation` event, the kind and the
  notional are `liquidations_v`. The link joins two small tables, and its
  evidence is 12 events in one fixture block, so it is not frozen into the
  root.
- **Edge cases:** a `market` liquidation's liquidated leg is crossed; a
  backstop takeover's counterparty is `LIQUIDATED_*` and has a ledger event; an
  ADL's counterparty is `AUTO_DELEVERAGING` and has none. An order filled
  against several counterparties gives one row per fill (552 legs made 257
  orders in a 4-hour sample; `liquidation_orders_v`). A liquidation pair with
  no liquidated leg has never been seen (no row; monitor M16); two liquidated
  legs would give two rows. Borrow-liquidation directions, never seen, are
  included when they carry a liquidation.

### R-D5 `funding_rates`

- For each funding event in block order, `dex_index` is its ordinal among the
  block's funding events. Its deltas are grouped by `coin`, in the order each
  coin first appears; a coin never appears in two funding events of one
  block.
- Per coin: `positions` and the long (`szi > 0`) and short (`szi < 0`) counts,
  and exact `i128` sums of |`szi`| (`open_interest`), positive `szi`
  (`long_size`), negated negative `szi` (`short_size`), positive amounts
  (`positive_funding`) and negative amounts (`negative_funding`, kept
  negative). A sum outside `decimal(38,10)` is NULL. `funding_rate` is set only
  when every delta of the coin carries the same rate (monitor M17). `dex` is
  R-D1 of the coin.
- An event without deltas has no row; its header, with `item_count = 0`, stays
  in `other_events`.

### R-D6 Event routing

A static match on `(event_type, ledger_type)` following
[the routing table](#event-tables-and-routing); every label it does not list
goes to `other_events`. The column lists of the event tables cover every
protobuf field of every type routed to them, so the split loses nothing: the
union of the five tables equals the event list value for value.

### Derivation version

- `fireparq.hypercore.derivation = "1"` is schema metadata on every HyperCore
  table. `schema_sha256` hashes schema metadata, so the key is part of each
  table's declared digest in the stream identity: a change to a derivation
  rule alone, with the same columns, refuses to resume into an existing root
  and needs a new one, without touching other chains' roots.
- It guards resume only. Every part fireparq writes carries it in its Arrow
  schema, but the maintenance job's compaction rewrites files from the Delta
  table's schema, which has no schema metadata: compacted files do not carry
  it, and readers must not rely on it. `validate` and resume work on compacted
  tables.
- A defect found in a rule is fixed by publishing a corrected view at once (the
  views above are free to change) and rebuilding when the defect is material.

## Refusals

Every block is decoded, guarded against unknown fields, checked against its
Firehose identity, then validated and converted completely before any row is
appended: a refused block appends nothing to any table, and `build` stops on it
with an error that names the block, the protobuf path (`fills[12].price`,
`events[3].events[0].ledger_update.delta.send.fee`) and the value. A refusal
needs a new fireparq release, never a skipped block. The derivations add no
refusal.

| Rule | Refused |
|---|---|
| R1 | A payload that does not decode as `pinax.hypercore.v1.Block`. |
| R2 | A payload whose prost re-encoding has another length: fields or oneof cases unknown to the vendored protos. The error says to refresh them from `buf.build/pinax/hypercore`. |
| R3 | A missing header or block time, or a header number or time that differs from the Firehose metadata. |
| R4 | A list of more than `u32::MAX` items. |
| R5 | `order_id`, `transaction_id`, `twap_id`, a `nonce` or `slot_id` above `i64::MAX`. |
| R6 | An event with other than one body, or a body or ledger delta without a case. |
| R7 | An unspecified or unknown `side`, `direction` or `leverage_type`. |
| R8 | A decimal that is not exact (see [Decimals](#decimals)). |
| R9 | `''` in a string other than the NULL list (`fills.builder`, `deployer_fee`, `builder_fee`, `priority_gas`, the `fee_token` of `send` and `spot_transfer`) and the keep list (`source_dex`, `destination_dex`, `sub_account_name`). |
| R10 | Empty bytes other than `fills.client_order_id` and `fills.liquidated_user` (NULL), including a `users` element. |
| R11 | A missing fill or event time, nanoseconds outside `[0, 1e9)`, a fill time that is not a whole millisecond, or a time out of range. |

Not validated, only documented and monitored by M1–M23: the payload times
against the block time, the funding layout, the hash layout, byte widths, the
`users` count, the `builder` format, fill pairing, the gossip presence pair,
`fee_token` against `fee`, and the shapes the derivations assume. Not
validated and only documented: the `users` composition, signs and the metadata
id strings; `fireparq validate` checks contiguous block numbers.

## Lossless

The raw tables keep every protobuf value: the fixture tests rebuild each of 36
real payloads byte for byte from `blocks`, the raw columns of `fills`, the
union of the five event tables in `event_index` order, `funding_deltas` and
`validator_rewards`, under `hex` and `binary`. The derived tables and the three
derived `fills` columns play no part. The inverse rules: decimals as canonical
text; NULL-on-empty columns as `''` or empty bytes; `fill_time` as a timestamp
with whole milliseconds; the `*_ns` columns as seconds and nanoseconds;
`liquidation_method IS NOT NULL` as a present `FillLiquidation`;
`previous_winner_ip IS NOT NULL` as a present previous winner; NULL `end_gas`
and `twap_id` as absent; child rows by `delta_index` and `reward_index`, as
many as `item_count`; labels back to enum and oneof values; columns an event
table lacks as NULL. `BlockHeader.block_number` is the canonical `block_num`,
and every event has exactly one body.

## `extra_json` and schema changes

The columns, their types and nullability, and the derivation version are bound
into the root's protected identity: an added or changed column, or a changed
derivation rule, needs a new output root, a rebuild from the origin (about
330M blocks; at the 3.6k blocks/s measured once, about a day of streaming, while
the old root keeps serving). `extra_json` (every table, the last column before
`fork_step`) is the lane that lets most upstream additions ship without one.
**This version writes NULL in every row.**

When upstream adds a field or case, the running release refuses the first block
that carries it (R2, R7). The release that vendors the new protos then either
writes the value into an existing typed column, only when its name, protobuf
type and documented meaning match that column (in the event table of the
type), or into `extra_json`; a new enum value needs no code, and a new oneof
case gets a new `event_type` or `ledger_type` label, routed as
[above](#event-tables-and-routing). The root then resumes from its protected
cursor with no rebuild. Such a release must keep `HYPERCORE_SCHEMA_DIGEST`
(`blocks/src/chain/tests.rs`) and the fixture output pin
(`blocks/src/hypercore/value_tests.rs`) unchanged and add fixtures for the new
type.

- **Derived tables:** `outcome_fills` and `liquidations` copy the `extra_json`
  of their `fills` row verbatim, so a new `Fill` field reaches them without a
  rebuild. `funding_rates.extra_json` stays NULL: a new `Funding` or
  `FundingDelta` field goes to `other_events.extra_json` or
  `funding_deltas.extra_json`, and a roll-up of it needs a new root.

**The `extra_json` format**, fixed now so every release writes the same:

- One JSON object of the values that no typed column holds; NULL, never `"{}"`,
  when there are none.
- The root object is the row's message: `Block` (`blocks`), `Fill` (`fills`),
  `FundingDelta`, `ValidatorReward`, and for the event tables the body's case
  message (for `ledger_update`, the delta's case message). New fields of the
  wrapper messages go under their message names as keys: `"Event"`,
  `"EventBody"`, `"LedgerUpdate"`, `"LedgerUpdateDelta"` (protobuf field
  names are lower_snake_case, so these cannot collide). A new field of
  `Funding` or `ValidatorRewards` goes to that event's `other_events` row.
- Nested messages are nested objects; repeated fields are arrays in protobuf
  order, aligned by index with `{}` for elements without new values.
- Values: strings verbatim (validated as decimals first when the release
  declares them decimals); bytes as lowercase `0x` hex whatever the encoding;
  integers as base-10 JSON strings; booleans as `true`/`false`; enums as labels
  without their prefix; timestamps as RFC 3339 UTC with 9 fractional digits
  (`"2026-10-06T17:27:16.123456789Z"`).
- Implicit-presence fields at their default (`''`, empty bytes, 0, `false`,
  enum 0, an empty list) are left out; explicit-presence fields (`optional`,
  messages, oneof members) are written whenever present, a present empty
  message as `{}`.
- Compact (no whitespace), RFC 8259 escaping, keys sorted bytewise at every
  level.

Keys written so far:

| Key | Table | Source field | Unit | First block |
|---|---|---|---|---|
| (none yet) | | | | |

Query patterns in DuckDB: `extra_json->>'field'`,
`(extra_json->>'field')::DECIMAL(38,10)` and `extra_json->'Event'->>'field'`;
in Spark, `get_json_object(extra_json, '$.field')`.

**New roots.** Promote `extra_json` keys to typed columns, move a label to
another event table, or add a table (`borrow_lend` as a `lending_events`
table is the first candidate) only in a new root, bundled with any rebuild
that is forced anyway (a `MAPPER_EPOCH` bump, a hole repair, a
partition-policy change). Build the new root (for example
`…/hypercore/v2/`) while the current one serves, and cut over when it has
caught up. A decimal with more than 10 fractional or 28 integer digits, a
`u64` above `i64::MAX`, a type change, a field removed behind a non-null
column or a new high-volume list (a second funding-like list) cannot be placed
and needs a new root. Upstream re-extracting history (for example the
pre-cutover builder fields) leaves the old rows stale: repair or rebuild.

| Schema | From release | Root | Notes |
|---|---|---|---|
| twelve tables, derivation version 1 | the first release with `--block-type hypercore` | origin 846903317 | `extra_json` unused |

Upstream merges new protos just before the data arrives (its own reader aborts
on unknown types), so a weekly comparison of `buf export buf.build/pinax/hypercore`
with `proto/pinax/hypercore/v1/` shortens the stop.

## Views, monitors and cookbook

The SQL below assumes the default `hex` encoding and DuckDB 1.5 or later with
its `delta` extension. It is not part of the schema: change it freely.
`blocks/tests/engine_compat.rs` runs all of it over a HyperCore build of the
fixture blocks. Start with a view per table, and `events`, the union of the
five event tables with `other_events`' columns in their order:

```sql
CREATE VIEW blocks AS SELECT * FROM delta_scan('<root>/blocks');
CREATE VIEW fills AS SELECT * FROM delta_scan('<root>/fills');
CREATE VIEW outcome_fills AS SELECT * FROM delta_scan('<root>/outcome_fills');
CREATE VIEW liquidations AS SELECT * FROM delta_scan('<root>/liquidations');
CREATE VIEW transfers AS SELECT * FROM delta_scan('<root>/transfers');
CREATE VIEW bridge_transfers AS SELECT * FROM delta_scan('<root>/bridge_transfers');
CREATE VIEW vault_events AS SELECT * FROM delta_scan('<root>/vault_events');
CREATE VIEW staking_events AS SELECT * FROM delta_scan('<root>/staking_events');
CREATE VIEW other_events AS SELECT * FROM delta_scan('<root>/other_events');
CREATE VIEW funding_deltas AS SELECT * FROM delta_scan('<root>/funding_deltas');
CREATE VIEW funding_rates AS SELECT * FROM delta_scan('<root>/funding_rates');
CREATE VIEW validator_rewards AS SELECT * FROM delta_scan('<root>/validator_rewards');
-- UNION ALL BY NAME orders columns by first appearance, so list them in other_events' order.
CREATE VIEW events AS
SELECT block_num, block_id, parent_num, parent_id, lib_num, timestamp, date, event_index, event_type,
       ledger_type, hash, event_time_ns, users, user, destination, vault, validator, sub_account, token,
       amount, usdc, usdc_value, fee, fee_token, native_token_fee, nonce, source_dex, destination_dex, dex,
       is_deposit, to_perp, is_undelegate, is_finalized, requested_usd, commission, closing_cost, basis,
       net_withdrawn_usd, interest_amount, operation, liquidated_ntl_pos, account_value, leverage_type,
       liquidated_positions, slot_id, previous_winner_ip, end_gas, sub_account_name, item_count, extra_json
FROM (SELECT * FROM transfers UNION ALL BY NAME SELECT * FROM bridge_transfers
      UNION ALL BY NAME SELECT * FROM vault_events UNION ALL BY NAME SELECT * FROM staking_events
      UNION ALL BY NAME SELECT * FROM other_events);
```

### View pack

Macros (R-D1 and R-D6 in SQL); fill, funding and account views; HIP-4,
liquidation, funding and transfer interpretations; the 0xArchive trades shape;
and one view per ledger type and per scalar body.

- **HIP-4** (`outcome_*`): `outcome_matches_v` gives one row per match: a
  trade or settlement from the counterparty pair (the taker's row), and a mint
  or burn from two adjacent single-leg fills of one outcome's two sides with
  the same hash and size, prices summing to exactly 1, maker then taker for a
  mint and taker then maker for a burn (a heuristic: without the price check it
  pairs 40 wrong legs in a 4-hour sample). `outcome_actions_v` groups split,
  merge, merge-question and negate legs into actions. `outcome_settlements_v`
  reads the settle fraction from the side-0 settlement price (85 of 85 against
  `/info`). `outcome_positions_v` gives exact share balances from
  `start_position`, which assumes outcome tokens move only through fills
  (monitor M23) and needs history from before an account's first outcome fill,
  which a root from the origin has (HIP-4 launched later).
- **Liquidations:** `liquidations_v` adds `notional`, `liquidation_kind`
  (`market`, `backstop_takeover` or `adl`) and the ledger `liquidation` event
  of the same block, hash and account (a takeover's; `ledger_event_index` and
  its fields). `liquidation_orders_v` groups the legs of one order.
- **Funding:** `funding_rates_v` adds net funding, net position, the
  open-interest change from the previous hour (NULL across a missing hour) and
  `funding_price_derived`, the price at which the funding paid matches the rate
  (median error 6e-6 against trades). It is believed to be HyperLiquid's oracle
  price but has not been checked against it.
- **Transfers:** `transfers_v` normalises sender, recipient, the balance each
  side uses (`''` the default perp dex, `spot`, or a HIP-3 dex; the
  assignment for the USDC types is by observation), token and quantity.
  `bridge_transfers_v` signs the bridge amount, `vault_events_v` resolves the
  depositor or recipient and the vault of a leader commission (from the
  same-hash `vault_withdraw`), and `staking_flows_v` counts each HYPE move once.
- **Trades:** `trades_v` has one row per paired taker leg with `maker :=
  counterparty` and `family` (`core`, `hip3`, `spot`, `hip4`); HIP-4 mints and
  burns, which have no counterparty, are in `outcome_matches_v`.

```sql
CREATE OR REPLACE MACRO hc_zero_hash() AS '0x' || repeat('0', 64);
-- R-D1: the market class and perp dex of a coin, the rule behind fills.market_type and fills.dex.
CREATE OR REPLACE MACRO hc_market_type(coin) AS CASE
    WHEN regexp_full_match(coin, '#[0-9]+') THEN 'outcome'
    WHEN regexp_full_match(coin, '@[0-9]+') OR regexp_full_match(coin, '[A-Za-z0-9]+/[A-Za-z0-9]+') THEN 'spot'
    WHEN regexp_full_match(coin, '([a-z][a-z0-9]*:)?[A-Za-z0-9]+') THEN 'perp' END;
CREATE OR REPLACE MACRO hc_dex(coin) AS CASE WHEN hc_market_type(coin) = 'perp'
    THEN CASE WHEN contains(coin, ':') THEN split_part(coin, ':', 1) ELSE '' END END;
-- R-D6: the event table of an event's labels; NULL for a label the routing table does not list.
CREATE OR REPLACE MACRO hc_event_table(event_type, ledger_type) AS CASE
    WHEN event_type = 'ledger_update' THEN CASE
        WHEN ledger_type IN ('send', 'spot_transfer', 'internal_transfer', 'sub_account_transfer',
                             'account_class_transfer') THEN 'transfers'
        WHEN ledger_type IN ('deposit', 'withdraw') THEN 'bridge_transfers'
        WHEN ledger_type IN ('vault_create', 'vault_deposit', 'vault_withdraw', 'vault_distribution',
                             'vault_leader_commission') THEN 'vault_events'
        WHEN ledger_type = 'c_staking_transfer' THEN 'staking_events'
        WHEN ledger_type IN ('rewards_claim', 'deploy_gas_auction', 'account_activation_gas',
                             'activate_dex_abstraction', 'liquidation', 'spot_genesis', 'borrow_lend',
                             'gossip_priority_gas_auction', 'hip3_liquidator_deposit') THEN 'other_events' END
    WHEN ledger_type IS NOT NULL THEN NULL
    WHEN event_type IN ('c_deposit', 'c_withdrawal', 'delegation') THEN 'staking_events'
    WHEN event_type IN ('funding', 'validator_rewards', 'gossip_priority_auction_restart',
                        'create_sub_account') THEN 'other_events' END;

CREATE OR REPLACE VIEW hypercore_fills_v AS
SELECT f.*,
       nullif(f.transaction_id, 0)                                   AS trade_id,
       nullif(f.hash, hc_zero_hash())                                AS tx_hash,
       CASE f.side WHEN 'BUY' THEN 'BID' ELSE 'ASK' END              AS side_bid_ask,
       f.crossed                                                     AS is_taker,
       coalesce(f.liquidation_method IS NOT NULL AND f.user = f.liquidated_user, false) AS is_liquidated_side,
       f.direction IN ('SPOT_DUST_CONVERSION', 'SETTLEMENT', 'NET_CHILD_VAULTS', 'SPLIT_OUTCOME',
                       'MERGE_OUTCOME', 'MERGE_QUESTION', 'NEGATE_OUTCOME') AS is_non_trade,
       f.direction IN ('AUTO_DELEVERAGING', 'LIQUIDATED_CROSS_LONG', 'LIQUIDATED_CROSS_SHORT',
                       'LIQUIDATED_ISOLATED_LONG', 'LIQUIDATED_ISOLATED_SHORT',
                       'BACKSTOP_BORROW_LIQUIDATION', 'PARTIAL_BORROW_LIQUIDATION') AS is_forced,
       f.price * f.size                                              AS notional
FROM fills f;

-- 0xArchive's trades shape: one row per paired taker leg (HIP-4 mints and burns: outcome_matches_v).
CREATE OR REPLACE VIEW trades_v AS
SELECT block_num, timestamp, date, fill_index, coin,
       CASE market_type WHEN 'perp' THEN CASE WHEN dex = '' THEN 'core' ELSE 'hip3' END
            WHEN 'spot' THEN 'spot' WHEN 'outcome' THEN 'hip4' END AS family,
       market_type, dex, transaction_id AS trade_id, nullif(hash, hc_zero_hash()) AS tx_hash,
       price, size, price * size AS notional, side AS taker_side, direction AS taker_direction,
       user AS taker, counterparty AS maker, fee AS taker_fee, fee_token AS taker_fee_token,
       liquidation_method
FROM fills WHERE crossed AND counterparty IS NOT NULL;

-- Funding headers, empty events included, with their perp-dex ordinal.
CREATE OR REPLACE VIEW hypercore_funding_events_v AS
SELECT block_num, timestamp, date, event_index, item_count,
       CAST(row_number() OVER (PARTITION BY block_num ORDER BY event_index) - 1 AS BIGINT) AS dex_index
FROM other_events WHERE event_type = 'funding';

CREATE OR REPLACE VIEW hypercore_funding_v AS
SELECT d.*, r.dex_index, r.dex
FROM funding_deltas d JOIN funding_rates r USING (block_num, event_index, coin);

-- Net funding, net position, the derived funding price and the open-interest change (NULL across a missing hour).
CREATE OR REPLACE VIEW funding_rates_v AS
SELECT *, positive_funding + negative_funding AS net_funding, long_size - short_size AS net_position,
       CASE WHEN funding_rate <> 0 AND open_interest > 0
            THEN (positive_funding - negative_funding)::DOUBLE
                 / (open_interest::DOUBLE * abs(funding_rate::DOUBLE)) END AS funding_price_derived,
       CASE WHEN lag(timestamp) OVER w >= timestamp - INTERVAL 61 MINUTE
            THEN open_interest - lag(open_interest) OVER w END AS open_interest_change
FROM funding_rates WINDOW w AS (PARTITION BY coin ORDER BY block_num);

CREATE OR REPLACE VIEW open_interest_v AS
SELECT block_num, timestamp, date, dex_index, dex, coin, open_interest, long_size, short_size,
       positions, long_positions, short_positions
FROM funding_rates;

-- Liquidation kind, notional and the ledger event of a backstop takeover.
CREATE OR REPLACE VIEW liquidations_v AS
WITH ledger AS (
    SELECT block_num, hash, users[1] AS account, any_value(event_index) AS event_index,
           any_value(liquidated_ntl_pos) AS liquidated_ntl_pos, any_value(account_value) AS account_value,
           any_value(leverage_type) AS leverage_type
    FROM other_events WHERE ledger_type = 'liquidation' AND hash <> hc_zero_hash()
    GROUP BY ALL HAVING count(*) = 1)
SELECT l.*, l.price * l.size AS notional,
       CASE WHEN l.liquidation_method = 'market' THEN 'market'
            WHEN l.counterparty_direction = 'AUTO_DELEVERAGING' THEN 'adl'
            WHEN l.counterparty_direction LIKE 'LIQUIDATED_%' THEN 'backstop_takeover' END AS liquidation_kind,
       e.event_index AS ledger_event_index, e.liquidated_ntl_pos, e.account_value, e.leverage_type
FROM liquidations l
LEFT JOIN ledger e ON e.block_num = l.block_num AND e.hash = l.hash AND e.account = l.liquidated_user;

-- One row per liquidation order: the liquidated account's legs of one coin in one action.
CREATE OR REPLACE VIEW liquidation_orders_v AS
SELECT block_num, any_value(timestamp) AS timestamp, hash, liquidated_user, coin,
       any_value(market_type) AS market_type, any_value(dex) AS dex, any_value(side) AS side,
       any_value(liquidation_kind) AS liquidation_kind, count(*) AS fills, sum(size) AS size,
       sum(notional) AS notional, sum(notional)::DOUBLE / sum(size)::DOUBLE AS average_price,
       any_value(mark_price) AS mark_price, any_value(ledger_event_index) AS ledger_event_index,
       any_value(liquidated_ntl_pos) AS liquidated_ntl_pos
FROM liquidations_v GROUP BY block_num, hash, liquidated_user, coin;

-- HIP-4: one row per match (trade and settlement pairs; mints and burns from adjacent single legs).
CREATE OR REPLACE VIEW outcome_matches_v AS
WITH single AS (
    SELECT * FROM outcome_fills WHERE counterparty IS NULL AND direction IN ('BUY', 'SELL')
), mint_burn AS (
    SELECT a.block_num, a.timestamp, a.hash, a.outcome_id,
           CASE a.side WHEN 'BUY' THEN 'mint' ELSE 'burn' END AS match_type,
           CASE WHEN a.crossed THEN a.fill_index ELSE b.fill_index END AS taker_fill_index,
           CASE WHEN a.crossed THEN b.fill_index ELSE a.fill_index END AS maker_fill_index,
           CASE WHEN a.crossed THEN a.user ELSE b.user END AS taker,
           CASE WHEN a.crossed THEN b.user ELSE a.user END AS maker,
           CASE WHEN a.crossed THEN a.coin ELSE b.coin END AS taker_coin,
           CASE WHEN a.crossed THEN a.price ELSE b.price END AS taker_price,
           a.size
    FROM single a JOIN single b
      ON b.block_num = a.block_num AND b.fill_index = a.fill_index + 1 AND b.side = a.side
     AND b.hash = a.hash AND b.size = a.size AND b.outcome_id = a.outcome_id
     AND b.side_index <> a.side_index AND a.price + b.price = 1
     AND ((a.side = 'BUY' AND NOT a.crossed AND b.crossed) OR (a.side = 'ASK' AND a.crossed AND NOT b.crossed))
), pairs AS (
    SELECT t.block_num, t.timestamp, t.hash, t.outcome_id,
           CASE WHEN t.direction = 'SETTLEMENT' THEN 'settlement' ELSE 'trade' END AS match_type,
           t.fill_index AS taker_fill_index, m.fill_index AS maker_fill_index, t.user AS taker,
           m.user AS maker, t.coin AS taker_coin, t.price AS taker_price, t.size
    FROM outcome_fills t JOIN outcome_fills m
      ON m.block_num = t.block_num AND m.coin = t.coin AND m.transaction_id = t.transaction_id
     AND m.fill_index <> t.fill_index
    WHERE t.crossed AND t.counterparty IS NOT NULL)
SELECT * FROM pairs UNION ALL SELECT * FROM mint_burn;

-- HIP-4 reshaping actions: one row per (block, hash, direction) with its legs.
CREATE OR REPLACE VIEW outcome_actions_v AS
SELECT block_num, any_value(timestamp) AS timestamp, hash, lower(direction) AS action_type,
       any_value(user) AS account, any_value(size) AS size, count(*) AS legs,
       list(DISTINCT outcome_id ORDER BY outcome_id) AS outcome_ids,
       list({'coin': coin, 'side': side, 'price': price, 'fee': fee} ORDER BY fill_index) AS leg_list
FROM outcome_fills
WHERE direction IN ('SPLIT_OUTCOME', 'MERGE_OUTCOME', 'NEGATE_OUTCOME', 'MERGE_QUESTION')
GROUP BY block_num, hash, direction;

-- HIP-4 settlement ("answer key"): the side-0 settlement price is the settle fraction.
CREATE OR REPLACE VIEW outcome_settlements_v AS
SELECT outcome_id, min(block_num) AS settled_block, min(timestamp) AS settled_at,
       coalesce(any_value(price) FILTER (WHERE side_index = 0 AND side = 'ASK'),
                1 - any_value(price) FILTER (WHERE side_index = 1 AND side = 'ASK')) AS settle_fraction,
       count(*) FILTER (WHERE side = 'ASK') AS settled_positions,
       sum(size) FILTER (WHERE side = 'ASK') AS settled_shares
FROM outcome_fills WHERE direction = 'SETTLEMENT' GROUP BY outcome_id;

-- HIP-4 share balance of each account and side coin after its latest fill.
CREATE OR REPLACE VIEW outcome_positions_v AS
SELECT user, coin, outcome_id, side_index,
       arg_max(start_position + CASE side WHEN 'BUY' THEN size ELSE -size END, (block_num, fill_index)) AS shares,
       max(block_num) AS last_block_num
FROM outcome_fills GROUP BY ALL;

CREATE OR REPLACE VIEW outcome_open_interest_v AS
SELECT coin, outcome_id, side_index, sum(shares) AS open_interest, count(*) AS holders
FROM outcome_positions_v WHERE shares > 0 GROUP BY ALL;

-- One row per balance move: sender and recipient, the balance each side uses, token and quantity.
CREATE OR REPLACE VIEW transfers_v AS
SELECT block_num, timestamp, date, event_index, hash, ledger_type AS transfer_type,
       coalesce(user, users[1]) AS sender, coalesce(destination, users[1]) AS recipient,
       CASE ledger_type WHEN 'send' THEN source_dex WHEN 'spot_transfer' THEN 'spot'
            WHEN 'account_class_transfer' THEN CASE WHEN to_perp THEN 'spot' ELSE '' END
            ELSE '' END AS source_balance,
       CASE ledger_type WHEN 'send' THEN destination_dex WHEN 'spot_transfer' THEN 'spot'
            WHEN 'account_class_transfer' THEN CASE WHEN to_perp THEN '' ELSE 'spot' END
            ELSE '' END AS destination_balance,
       coalesce(token, 'USDC') AS token, coalesce(amount, usdc) AS quantity, usdc_value, fee,
       coalesce(fee_token, CASE WHEN ledger_type = 'internal_transfer' THEN 'USDC' END) AS fee_token,
       native_token_fee, nonce
FROM transfers;

CREATE OR REPLACE VIEW bridge_transfers_v AS
SELECT block_num, timestamp, date, event_index, hash, ledger_type, users[1] AS account,
       CASE ledger_type WHEN 'deposit' THEN usdc ELSE -usdc END AS signed_usdc, usdc, fee, nonce
FROM bridge_transfers;

-- The vault, the depositor or recipient, and the vault-side total of a distribution.
CREATE OR REPLACE VIEW vault_events_v AS
WITH withdrawn AS (
    SELECT block_num, hash, any_value(vault) AS vault FROM vault_events
    WHERE ledger_type = 'vault_withdraw' AND hash <> hc_zero_hash()
    GROUP BY ALL HAVING count(DISTINCT vault) = 1)
SELECT v.block_num, v.timestamp, v.date, v.event_index, v.hash, v.ledger_type,
       coalesce(v.vault, w.vault) AS vault,
       CASE WHEN v.ledger_type = 'vault_distribution' THEN nullif(v.users[1], v.vault)
            ELSE coalesce(v.user, list_filter(v.users, lambda u: u <> v.vault)[1]) END AS account,
       coalesce(v.ledger_type = 'vault_distribution' AND v.users[1] = v.vault, false) AS is_vault_total,
       v.usdc, v.fee, v.requested_usd, v.commission, v.closing_cost, v.basis, v.net_withdrawn_usd
FROM vault_events v
LEFT JOIN withdrawn w ON v.ledger_type = 'vault_leader_commission' AND w.block_num = v.block_num AND w.hash = v.hash;

-- HYPE moves, each once: c_staking_transfer carries every move between spot and staking
-- (a c_deposit repeats its deposit; a c_withdrawal request moves nothing), delegations move staked HYPE.
CREATE OR REPLACE VIEW staking_flows_v AS
SELECT block_num, timestamp, date, event_index, hash, users[1] AS account,
       CASE WHEN is_deposit THEN 'stake' ELSE 'unstake' END AS flow, token, amount
FROM staking_events WHERE ledger_type = 'c_staking_transfer'
UNION ALL
SELECT block_num, timestamp, date, event_index, hash, user AS account,
       CASE WHEN is_undelegate THEN 'undelegate' ELSE 'delegate' END AS flow, 'HYPE' AS token, amount
FROM staking_events WHERE event_type = 'delegation';

-- One row per (event, account): HyperLiquid's ledger index (users) plus the body's user.
CREATE OR REPLACE VIEW hypercore_account_events_v AS
SELECT e.block_num, e.timestamp, e.date, e.event_index, e.event_type, e.ledger_type, u.account
FROM events e, unnest(e.users) AS u(account) WHERE e.event_type = 'ledger_update'
UNION ALL
SELECT block_num, timestamp, date, event_index, event_type, ledger_type, user AS account
FROM events WHERE event_type IN ('c_deposit', 'c_withdrawal', 'delegation', 'create_sub_account');

-- One view per ledger_type, over its event table, with the columns of the matrix above.
CREATE OR REPLACE VIEW ledger_spot_transfer AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount, usdc_value, user,
       destination, fee, native_token_fee, nonce, fee_token
FROM transfers WHERE ledger_type = 'spot_transfer';
CREATE OR REPLACE VIEW ledger_c_staking_transfer AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount, is_deposit
FROM staking_events WHERE ledger_type = 'c_staking_transfer';
CREATE OR REPLACE VIEW ledger_account_class_transfer AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, usdc, to_perp
FROM transfers WHERE ledger_type = 'account_class_transfer';
CREATE OR REPLACE VIEW ledger_internal_transfer AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, usdc, user, destination, fee
FROM transfers WHERE ledger_type = 'internal_transfer';
CREATE OR REPLACE VIEW ledger_sub_account_transfer AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, usdc, user, destination
FROM transfers WHERE ledger_type = 'sub_account_transfer';
CREATE OR REPLACE VIEW ledger_send AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, user, destination, source_dex,
       destination_dex, token, amount, usdc_value, fee, native_token_fee, nonce, fee_token
FROM transfers WHERE ledger_type = 'send';
CREATE OR REPLACE VIEW ledger_deposit AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, usdc
FROM bridge_transfers WHERE ledger_type = 'deposit';
CREATE OR REPLACE VIEW ledger_withdraw AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, usdc, nonce, fee
FROM bridge_transfers WHERE ledger_type = 'withdraw';
CREATE OR REPLACE VIEW ledger_vault_deposit AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, vault, usdc
FROM vault_events WHERE ledger_type = 'vault_deposit';
CREATE OR REPLACE VIEW ledger_rewards_claim AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, amount, token
FROM other_events WHERE ledger_type = 'rewards_claim';
CREATE OR REPLACE VIEW ledger_vault_withdraw AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, vault, user, requested_usd, commission,
       closing_cost, basis, net_withdrawn_usd
FROM vault_events WHERE ledger_type = 'vault_withdraw';
CREATE OR REPLACE VIEW ledger_vault_leader_commission AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, user, usdc
FROM vault_events WHERE ledger_type = 'vault_leader_commission';
CREATE OR REPLACE VIEW ledger_deploy_gas_auction AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount
FROM other_events WHERE ledger_type = 'deploy_gas_auction';
CREATE OR REPLACE VIEW ledger_account_activation_gas AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, amount, token
FROM other_events WHERE ledger_type = 'account_activation_gas';
CREATE OR REPLACE VIEW ledger_activate_dex_abstraction AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, dex, token, amount
FROM other_events WHERE ledger_type = 'activate_dex_abstraction';
CREATE OR REPLACE VIEW ledger_liquidation AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, users[1] AS liquidated_account,
       liquidated_ntl_pos, account_value, leverage_type, liquidated_positions
FROM other_events WHERE ledger_type = 'liquidation';
CREATE OR REPLACE VIEW ledger_spot_genesis AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount
FROM other_events WHERE ledger_type = 'spot_genesis';
CREATE OR REPLACE VIEW ledger_vault_distribution AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, vault, usdc
FROM vault_events WHERE ledger_type = 'vault_distribution';
CREATE OR REPLACE VIEW ledger_borrow_lend AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount, interest_amount, operation
FROM other_events WHERE ledger_type = 'borrow_lend';
CREATE OR REPLACE VIEW ledger_vault_create AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, vault, usdc, fee
FROM vault_events WHERE ledger_type = 'vault_create';
CREATE OR REPLACE VIEW ledger_gossip_priority_gas_auction AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount
FROM other_events WHERE ledger_type = 'gossip_priority_gas_auction';
CREATE OR REPLACE VIEW ledger_hip3_liquidator_deposit AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, dex, token, amount
FROM other_events WHERE ledger_type = 'hip3_liquidator_deposit';

-- One view per scalar body (funding and validator rewards are their own tables).
CREATE OR REPLACE VIEW c_deposits AS
SELECT block_num, block_id, timestamp, date, event_index, hash, user, amount FROM staking_events WHERE event_type = 'c_deposit';
CREATE OR REPLACE VIEW c_withdrawals AS
SELECT block_num, block_id, timestamp, date, event_index, hash, user, amount, is_finalized FROM staking_events WHERE event_type = 'c_withdrawal';
CREATE OR REPLACE VIEW delegations AS
SELECT block_num, block_id, timestamp, date, event_index, hash, user, validator, amount, is_undelegate FROM staking_events WHERE event_type = 'delegation';
CREATE OR REPLACE VIEW gossip_priority_auction_restarts AS
SELECT block_num, block_id, timestamp, date, event_index, slot_id, previous_winner_ip, end_gas FROM other_events WHERE event_type = 'gossip_priority_auction_restart';
CREATE OR REPLACE VIEW create_sub_accounts AS
SELECT block_num, block_id, timestamp, date, event_index, hash, user, sub_account, sub_account_name FROM other_events WHERE event_type = 'create_sub_account';
```

### Monitors

Data-quality monitors, not halts: each query returns the rows that break an
observation the mapper does not enforce, and an empty result means it still
holds. They need the macros and views above. All return no rows on the 36
fixture blocks, and M1–M21 and M23 none on about 266k sample blocks of
January and October 2026. M22 is informational: it lists the zero-rate
coin-hours HyperLiquid leaves out (3 in a 4-hour October sample, all
`xyz:GBP`).

```sql
-- M1 fill_time equals the block time truncated to ms
SELECT 'M1' m, block_num, fill_index FROM fills WHERE fill_time <> timestamp;
-- M2 event time equals block time (ns)
SELECT 'M2' m, e.block_num, e.event_index FROM events e JOIN blocks b USING (block_num) WHERE e.event_time_ns <> b.block_time_ns;
-- M3 funding events occupy event positions 0..N-1
SELECT 'M3' m, block_num FROM other_events WHERE event_type = 'funding' GROUP BY block_num HAVING max(event_index) <> count(*) - 1;
-- M4 non-zero HyperCore hashes: byte[10]=0x04, bytes[11..15)=block_num (deposit/withdraw excluded)
SELECT 'M4' m, block_num, fill_index FROM fills
WHERE hash <> hc_zero_hash() AND (substr(hash, 23, 2) <> '04' OR substr(hash, 25, 8) <> lpad(lower(hex(block_num)), 8, '0'));
SELECT 'M4e' m, block_num, event_index FROM events
WHERE hash <> hc_zero_hash() AND coalesce(ledger_type, '') NOT IN ('deposit', 'withdraw')
  AND (substr(hash, 23, 2) <> '04' OR substr(hash, 25, 8) <> lpad(lower(hex(block_num)), 8, '0'));
-- M5 byte widths: addresses 20 B, hashes 32 B, cloids 16 B (hex: 42/66/34 chars)
SELECT 'M5' m, block_num, fill_index FROM fills
WHERE length(user) <> 42 OR length(hash) <> 66 OR length(client_order_id) <> 34 OR length(liquidated_user) <> 42
   OR length(counterparty) <> 42;
SELECT 'M5e' m, block_num, event_index FROM events
WHERE length(hash) <> 66 OR length(user) <> 42 OR length(destination) <> 42 OR length(vault) <> 42
   OR length(validator) <> 42 OR length(sub_account) <> 42 OR list_bool_or([length(u) <> 42 FOR u IN users]);
-- M6 users has 1 or 2 entries
SELECT 'M6' m, block_num, event_index FROM events WHERE event_type = 'ledger_update' AND len(users) NOT IN (1, 2);
-- M7 builder is 0x + 40 lowercase hex
SELECT 'M7' m, block_num, fill_index FROM fills WHERE builder IS NOT NULL AND NOT regexp_full_match(builder, '0x[0-9a-f]{40}');
-- M8 counts agree with child rows
SELECT 'M8f' m, b.block_num FROM blocks b LEFT JOIN (SELECT block_num, count(*) n FROM fills GROUP BY ALL) f USING (block_num)
WHERE b.fill_count <> coalesce(f.n, 0);
SELECT 'M8e' m, b.block_num FROM blocks b LEFT JOIN (SELECT block_num, count(*) n FROM events GROUP BY ALL) e USING (block_num)
WHERE b.event_count <> coalesce(e.n, 0);
SELECT 'M8i' m, e.block_num, e.event_index FROM other_events e
LEFT JOIN (SELECT block_num, event_index, count(*) n FROM funding_deltas GROUP BY ALL
           UNION ALL SELECT block_num, event_index, count(*) FROM validator_rewards GROUP BY ALL) c USING (block_num, event_index)
WHERE e.item_count IS NOT NULL AND e.item_count <> coalesce(c.n, 0);
-- M9 non-outcome trades pair: exactly 2 adjacent legs, BUY first, one crossed
SELECT 'M9' m, block_num, transaction_id FROM fills
WHERE transaction_id <> 0 AND market_type IS DISTINCT FROM 'outcome'
GROUP BY block_num, transaction_id
HAVING count(*) <> 2 OR sum(crossed::INT) <> 1 OR arg_min(side, fill_index) <> 'BUY' OR max(fill_index) - min(fill_index) <> 1;
-- M10 gossip: previous_winner and end_gas present together
SELECT 'M10' m, block_num, event_index FROM other_events
WHERE event_type = 'gossip_priority_auction_restart' AND (previous_winner_ip IS NULL) <> (end_gas IS NULL);
-- M11 liquidation present but liquidated_user empty
SELECT 'M11' m, block_num, fill_index FROM fills WHERE liquidation_method IS NOT NULL AND liquidated_user IS NULL;
-- M12 send/spot fee_token NULL exactly when fee = 0
SELECT 'M12' m, block_num, event_index FROM transfers
WHERE ledger_type IN ('send', 'spot_transfer') AND (fee_token IS NULL) <> (fee = 0);
-- M13 extra_json is still NULL in every table (no release has used it yet)
SELECT 'M13' m, 'blocks' t, block_num, NULL::BIGINT i FROM blocks WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'fills', block_num, fill_index FROM fills WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'outcome_fills', block_num, fill_index FROM outcome_fills WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'liquidations', block_num, fill_index FROM liquidations WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'events', block_num, event_index FROM events WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'funding_deltas', block_num, delta_index FROM funding_deltas WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'funding_rates', block_num, event_index FROM funding_rates WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'validator_rewards', block_num, reward_index FROM validator_rewards WHERE extra_json IS NOT NULL;
-- M14 paired legs agree on price, size and hash, have exactly one crossed leg and are adjacent
SELECT 'M14' m, f.block_num, f.fill_index FROM fills f JOIN fills c
  ON c.block_num = f.block_num AND c.coin = f.coin AND c.transaction_id = f.transaction_id AND c.fill_index <> f.fill_index
WHERE f.counterparty IS NOT NULL
  AND (c.user <> f.counterparty OR c.price <> f.price OR c.size <> f.size OR c.hash <> f.hash
       OR c.crossed = f.crossed OR abs(c.fill_index - f.fill_index) <> 1);
-- M15 a non-outcome leg with a non-zero trade id but no counterparty (a match shape R-D2 does not pair)
SELECT 'M15' m, block_num, fill_index FROM fills
WHERE transaction_id <> 0 AND counterparty IS NULL AND market_type IS DISTINCT FROM 'outcome';
-- M16 a liquidation match without exactly one liquidated leg, or a ledger liquidation liquidations_v does not link
SELECT 'M16' m, block_num, min(fill_index) AS fill_index FROM fills
WHERE liquidation_method IS NOT NULL AND transaction_id <> 0
GROUP BY block_num, coin, transaction_id HAVING count(*) FILTER (WHERE user = liquidated_user) <> 1;
SELECT 'M16e' m, e.block_num, e.event_index FROM other_events e
WHERE e.ledger_type = 'liquidation'
  AND NOT EXISTS (SELECT 1 FROM liquidations_v l WHERE l.block_num = e.block_num AND l.ledger_event_index = e.event_index);
-- M17 a funding rate that is not the same for every delta of the coin
SELECT 'M17' m, block_num, event_index, coin FROM funding_rates WHERE funding_rate IS NULL;
-- M18 a perp-dex index that names more than one dex over time
SELECT 'M18' m, dex_index FROM funding_rates GROUP BY dex_index HAVING count(DISTINCT dex) > 1;
-- M19 a coin form R-D1 does not know, or a fill whose market_type or dex differs from the rule in SQL
SELECT 'M19' m, block_num, fill_index FROM fills
WHERE market_type IS NULL OR market_type IS DISTINCT FROM hc_market_type(coin) OR dex IS DISTINCT FROM hc_dex(coin);
-- M20 long and short sizes differ by more than f64 noise
SELECT 'M20' m, block_num, event_index, coin FROM funding_rates
WHERE abs(long_size - short_size) > open_interest * 0.000001;
-- M21 an event label the routing table does not list, or an event in another table than it says
SELECT 'M21' m, block_num, event_index, tbl FROM (
    SELECT 'transfers' AS tbl, block_num, event_index, event_type, ledger_type FROM transfers
    UNION ALL SELECT 'bridge_transfers', block_num, event_index, event_type, ledger_type FROM bridge_transfers
    UNION ALL SELECT 'vault_events', block_num, event_index, event_type, ledger_type FROM vault_events
    UNION ALL SELECT 'staking_events', block_num, event_index, event_type, ledger_type FROM staking_events
    UNION ALL SELECT 'other_events', block_num, event_index, event_type, ledger_type FROM other_events)
WHERE hc_event_table(event_type, ledger_type) IS DISTINCT FROM tbl;
-- M22 (informational) a perp coin traded in the hour before a funding block without a funding_rates row at it
SELECT 'M22' m, t.block_num, t.coin FROM (
    SELECT DISTINCT r.block_num, f.coin
    FROM (SELECT DISTINCT block_num, timestamp FROM funding_rates) r
    JOIN fills f ON f.timestamp >= r.timestamp - INTERVAL 1 HOUR AND f.timestamp < r.timestamp
    WHERE f.market_type = 'perp') t
LEFT JOIN funding_rates r USING (block_num, coin) WHERE r.coin IS NULL;
-- M23 an event moving an outcome token (+n or #n): outcome_positions_v assumes they move only through fills
SELECT 'M23' m, block_num, event_index FROM events WHERE regexp_full_match(token, '[+#][0-9]+');
```

### Cookbook

```sql
-- C1 taker notional per day, market type and perp dex (trades only)
SELECT date, market_type, dex, sum(notional) AS taker_notional, count(*) AS trades
FROM hypercore_fills_v WHERE is_taker AND NOT is_non_trade GROUP BY ALL ORDER BY ALL;
-- C2 the trades of one HIP-3 dex with taker and maker
SELECT block_num, timestamp, coin, trade_id, taker_side, price, size, taker, maker
FROM trades_v WHERE family = 'hip3' AND dex = 'xyz' ORDER BY block_num, fill_index;
-- C3 every liquidated leg, with the liquidator (or under ADL the deleveraged account)
SELECT block_num, fill_index, liquidated_user, coin, direction, liquidation_method, mark_price, price, size, counterparty
FROM liquidations;
-- C4 buyer and seller of each trade (dust conversions, HIP-4 mints and burns have no counterparty)
SELECT block_num, transaction_id, user AS buyer, counterparty AS seller, price, size, crossed AS buyer_is_taker
FROM fills WHERE side = 'BUY' AND counterparty IS NOT NULL;
-- C5 backstop takeovers with their ledger liquidation event (one row per liquidated leg)
SELECT block_num, fill_index, liquidated_user, coin, ledger_event_index, liquidated_ntl_pos, account_value, leverage_type
FROM liquidations_v WHERE liquidation_kind = 'backstop_takeover';
-- C6 liquidated notional per day, kind and perp dex
SELECT date, liquidation_kind, dex, count(*) AS legs, sum(notional) AS notional
FROM liquidations_v GROUP BY ALL ORDER BY ALL;
-- C7 an account's non-funding ledger history (its funding payments are in funding_deltas)
SELECT e.* FROM hypercore_account_events_v a JOIN events e USING (block_num, event_index)
WHERE a.account = '0x…' ORDER BY block_num, event_index;
-- C8 latest hourly position snapshot
SELECT user, coin, szi FROM funding_deltas
WHERE block_num = (SELECT max(block_num) FROM other_events WHERE event_type = 'funding');
-- C9 funding per dex per hour, keeping empty funding events
SELECT e.block_num, e.dex_index, e.item_count, coalesce(sum(r.positive_funding + r.negative_funding), 0) AS net_funding
FROM hypercore_funding_events_v e LEFT JOIN funding_rates r USING (block_num, event_index) GROUP BY ALL ORDER BY 1, 2;
-- C10 open interest per coin at the latest funding block (both sides, as HyperLiquid counts it)
SELECT dex_index, dex, coin, open_interest, long_size, long_positions, short_positions, funding_rate
FROM funding_rates WHERE block_num = (SELECT max(block_num) FROM funding_rates) ORDER BY open_interest DESC;
-- C11 hourly funding of one coin with the open-interest change (NULL across a missing hour)
SELECT timestamp, funding_rate, open_interest, open_interest_change, net_funding, funding_price_derived
FROM funding_rates_v WHERE coin = 'BTC' ORDER BY timestamp;
-- C12 builder revenue per fee token (builder_fee is in fee_token; fully captured from block 957002478)
SELECT builder, fee_token, sum(builder_fee) AS revenue, count(*) AS fills
FROM fills WHERE block_num >= 957002478 AND builder IS NOT NULL GROUP BY ALL ORDER BY revenue DESC NULLS LAST;
-- C13 three-decimal products: szi * funding_rate * szi overflows in DuckDB, so cast an intermediate first.
-- DOUBLE is approximate. CAST to DECIMAL(38,10) rounds each row to 10 places, so that column is not exact either.
-- A product of two needs no cast: sum(szi * funding_rate) is exact, DECIMAL(38,20).
SELECT sum(szi::DOUBLE * funding_rate::DOUBLE * szi::DOUBLE) AS approx,
       sum(CAST(CAST(szi * funding_rate AS DECIMAL(38, 10)) * szi AS DECIMAL(38, 10))) AS rounded_to_10dp,
       sum(szi * funding_rate) AS exact_pair
FROM funding_deltas;
-- C14 HYPE staking flows, each move once (c_deposit duplicates c_staking_transfer)
SELECT flow, sum(amount) AS hype, count(*) AS moves FROM staking_flows_v GROUP BY ALL;
-- C15 vault flows per vault and account (the vault-side vault_distribution row repeats its recipients' total)
SELECT ledger_type, vault, account, usdc, net_withdrawn_usd FROM vault_events_v WHERE NOT is_vault_total;
-- C16 HIP-4 matches per outcome and kind: trades, settlements, mints and burns
SELECT outcome_id, match_type, count(*) AS matches, sum(size) AS shares FROM outcome_matches_v GROUP BY ALL ORDER BY ALL;
-- C17 HIP-4 settled outcomes with the open interest left on each side
SELECT outcome_id, s.settled_at, s.settle_fraction, o.side_index, o.open_interest, o.holders
FROM outcome_settlements_v s FULL JOIN outcome_open_interest_v o USING (outcome_id) ORDER BY ALL;
-- C18 an account's HIP-4 share balances
SELECT coin, outcome_id, side_index, shares, last_block_num FROM outcome_positions_v
WHERE user = '0x…' AND shares <> 0;
```

### Optional reference joins

Names and labels that are not in the protobuf (spot pair and token names,
outcome and question text, perp-dex names, the funding premium) come from
HyperLiquid's `/info`, which returns current state, not state at a block. They
are never written into a fireparq root. A separate reference job (a
`k8s-parquet` CronJob, not part of fireparq) can write them as small
append-only Delta tables (`hl_spot_tokens`, `hl_spot_pairs`, `hl_perp_dexs`,
`hl_perp_assets`, `hl_outcomes`, `hl_questions`, `hl_funding_history`), each
with `*_current` and `*_history` views, using the lake's vocabulary: `''` for
the default dex and `side_index`. When those tables exist, join them at query
time:

```sql
-- R1 spot trades with pair names
SELECT f.block_num, f.fill_index, f.timestamp, p.base_symbol, p.quote_symbol, f.side, f.price, f.size,
       f.user, f.counterparty, f.crossed
FROM fills f LEFT JOIN hl_spot_pairs_current p ON p.coin = f.coin
WHERE f.market_type = 'spot';
-- R2 HIP-4 fills with labels (side_names is 1-based in DuckDB)
SELECT o.*, m.name AS outcome_name, m.side_names[o.side_index + 1] AS side_label,
       q.name AS question_name, s.settle_fraction
FROM outcome_fills o
LEFT JOIN hl_outcomes_current m USING (outcome_id)
LEFT JOIN hl_questions_current q ON q.question_id = m.question_id
LEFT JOIN outcome_settlements_v s USING (outcome_id);
-- R3 hourly funding with the premium and the dex name
SELECT r.*, h.premium, d.full_name AS dex_full_name
FROM funding_rates r
LEFT JOIN hl_funding_history h ON h.coin = r.coin AND h.funding_time = r.timestamp
LEFT JOIN hl_perp_dexs_current d ON d.dex_index = r.dex_index;
```

## Volume and flushes

Expected volume in October 2026, about 1.21M blocks per day (Parquet bytes
measured with zstd and 65,536-row groups on 118k mid-2026 blocks; the derived
tables and the event split from DuckDB prototypes of 24-hour windows):

| Table | Rows per day | Parquet bytes per row | Parquet per day | History to date (about 327M blocks) |
|---|---|---|---|---|
| `blocks` | 1.21M | 20–25 | 25–30 MB | 327M rows, about 8 GB |
| `fills` | 10.3M (13.6M at the September peak) | 53–57 | 0.55–0.74 GB | about 2.4B rows, about 130 GB |
| `outcome_fills` | about 55k (since 2026-05-02) | about 44 | about 2.5 MB | about 8M rows, under 0.5 GB |
| `liquidations` | about 6k (3.1k–17.9k) | about 76 | about 0.5 MB | about 2M rows, under 0.2 GB |
| `transfers` | about 84k | about 90 | about 7.6 MB | about 20M rows, about 2 GB |
| `bridge_transfers`, `vault_events`, `staking_events` | about 6.5k together | about 100 | about 0.65 MB | about 1.5M rows, under 0.2 GB |
| `other_events` | about 5.4k | about 90 | about 0.5 MB | about 1.3M rows, under 0.2 GB |
| `funding_deltas` | about 10.4M, in 24 blocks of about 435k rows | about 23 | about 0.24 GB | about 2.0B rows, about 46 GB |
| `funding_rates` | about 8.0k | about 45 | about 0.4 MB | about 2M rows, under 0.1 GB |
| `validator_rewards` | about 50k | 1–3 | under 0.2 MB | about 13M rows, under 0.1 GB |
| **Total** | about 22M | | **about 0.85–1.0 GB** | **about 190 GB** |

`counterparty` adds about 5.5% to `fills`; the event split saves no space.
With up to about 28 flushes a day and rows in every table, a day gets up to
about 336 small parts before the maintenance job compacts them.

With the defaults, `--flush-memory-bytes` (256 MiB, summed over all tables) is
the trigger that fires. HyperCore Parquet comes out at about 0.125 of the
mapper estimate, which is 32 MiB / 256 MiB. At that ratio `--flush-bytes`
(32 MiB) would need one table of about 256 MiB on its own, so it fires only on
the first flush, before a ratio has been learned. `fills` files are therefore
about 20–25 MB, below the 32 MiB target. One funding block adds about 435k
rows (about 78 MB of Arrow) in one step: it cannot trip the memory threshold
alone, but it can push a flush up to about 80 MB past 256 MiB, and the funding
parts are lumpy. The date boundary forces a flush just before the 00:00
funding and dust block. The largest funding payload (26.6 MB) is about five
times below the 128 MiB gRPC message limit and grows about 1.5 MB a month.
