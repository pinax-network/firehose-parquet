# HyperCore notes

Semantics of the HyperCore (HyperLiquid L1, `pinax.hypercore.v1`) tables beyond
their columns, which the [HyperCore schema reference](../schemas/hypercore.md)
lists. Five tables: `blocks`, `fills`, `events`, `funding_deltas` and
`validator_rewards`. This is **schema epoch 1** (see
[`extra_json` and schema epochs](#extra_json-and-schema-epochs)).

The registry has no HyperCore network, so `--network` has no alias for it and
the Pinax credential must be named explicitly. The endpoint's `chainName`
(`hypercore`) resolves the block type, so `--block-type hypercore` is optional:

```bash
fireparq build --block-type hypercore \
  --endpoint https://hypercore.firehose.pinax.network:443 \
  --api-key-envvar PINAX_API_KEY \
  --start-block 846903317 --output s3://hypercore-mainnet
```

Do not set `--flush-rows` below about 1M (one funding block alone has about
435k rows; see [Volume and flushes](#volume-and-flushes)). The
[maintenance job](../delta-maintenance.md) needs
`LAKE_TABLES=blocks,fills,events,funding_deltas,validator_rewards`.

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
- **The hole.** Blocks 846903300–846903312 do not exist on the endpoint, but
  846903313 still names 846903312 as its parent.
  - fireparq cannot stream across it. A stream delivers blocks up to
    846903299. After that, every attempt logs `WARN stream error, will
    reconnect` with `rpc error: code = Internal desc = unexpected stream
    termination`, because resuming from the cursor of 846903299 returns no
    block. `Internal` is retried with back-off of up to 60 s, so the build
    keeps retrying until `--reconnect-stall-timeout-secs` passes (default
    900 s, about 15 minutes; see [connection errors](../cli.md#connection-errors)).
    It then exits 1 with `reconnect stalled: no stream message for … after N
    failed attempts; last error: … unexpected stream termination`. Blocks
    buffered since the last flush are discarded; earlier committed flushes are
    kept and stay readable.
  - A root whose start is before 846903300 can never pass 846903299: running
    it again resumes from its cursor and fails the same way, and a different
    `--start-block` is refused with `explicit start differs from the stream's
    original start; use a new output root …`. Build into a new output root.
  - Start a root at **846903317**, the first block of 2026-01-01 (hourly
    funding, the daily dust conversion and validator rewards), unless upstream
    has re-extracted the hole. 846903313 also works but leaves a partial
    2025-12-31 partition (4 blocks). A `--start-block` inside the hole
    silently starts at 846903313, without a warning.
  - No root can hold blocks on both sides of the hole, so `fireparq validate`
    never reports it and `--allow-gaps` is not needed for it.
  - Upstream runs its readers with `--reader-node-skip-missing-blocks`, so
    other holes are possible. No full contiguity scan of the endpoint exists.
    A build that reaches one fails as above, and every restart resumes from
    the same cursor and stops there again. Ask upstream to re-extract the
    missing blocks; the same root then resumes from its cursor. Otherwise
    build a new output root whose `--start-block` is after the hole,
    preferably the first block of the next UTC day so that no partition is
    partial.
- **Not captured before the 2026-04-13 reader cutover.** NULL there means "not
  captured", not zero:
  - `fills.deployer_fee` before block 957002477;
  - `fills.builder` and `fills.builder_fee` before block 957002478 (capture
    there is partial, not absent: blocks just before the cutover have none,
    but December 2025 blocks have them, so NULL can mean either);
  - `events.previous_winner_ip` and `events.end_gas` before block 957002477.

  `fills.priority_gas` is NULL before about 2026-04-20 because the feature did
  not exist.
- **First appearance** of each type, so that absence is not read as zero
  activity:

  | Type or field | First seen |
  |---|---|
  | `gossip_priority_auction_restart` | about 2026-04-13 (e.g. block 956997125) |
  | `gossip_priority_gas_auction` | block 957002528 |
  | outcome (`#`) coins and the `SPLIT_OUTCOME`, `MERGE_OUTCOME`, `NEGATE_OUTCOME` and `MERGE_QUESTION` directions | by 2026-05 |
  | `hip3_liquidator_deposit` | block 1075986891 (2026-07-17; 2 seen ever) |
  | `create_sub_account` | block 1170059852 (2026-10-03). Sub-accounts created earlier have no row. |

## Time columns

- `timestamp` (milliseconds) is the partition time, the canonical block time
  truncated to milliseconds.
- `blocks.block_time_ns` is the exact block time in nanoseconds.
- `fills.fill_time` (milliseconds) and `events.event_time_ns` are the payload's
  own times. They equal the block time in every block seen, are stored as
  delivered without being checked, and are monitored (M1, M2). A fill time that
  is not a whole millisecond is refused (R11), so `fill_time` never rounds.

## Decimals

- Every amount, price and size is `decimal(38,10)`, the exact value of
  HyperLiquid's decimal string. HyperLiquid's values are `f64` values rounded to
  10 places, so they can look like `6426274.5300000003`.
- The parser accepts only exact values: `^-?[0-9]+(\.[0-9]+)?$`, at most 10
  significant fractional digits and 28 integer digits. It never rounds, and it
  refuses exponents, a `+` sign, a leading or trailing `.`, whitespace and
  `NaN` (R8). Exact but non-canonical text (`"1.50"`, `"5"`) is stored as its
  value.
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
    (cookbook C10).
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
  - The zero address in `fills.user` is the counterparty of a delisted-perp
    `SETTLEMENT`.
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
  (0 exceptions in about 34M pairs, monitor M9).
  - The taker is the `crossed` leg, the maker the other one.
  - Count trades, volume and notional on `crossed` rows only, or volume doubles.
  - `client_order_id` does not mark the taker.
- **Exceptions to pairing.**
  - Outcome markets can produce single fills: split, merge, negate and
    merge-question fills, and complementary mint or burn with a separate trade
    id per leg.
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
    `*_BORROW_LIQUIDATION` (`hypercore_fills_v.is_forced`). OHLC series usually
    exclude them too (cookbook C2). `market` liquidations use the ordinary
    directions and stay in.
  - `SETTLEMENT` covers both HIP-4 resolution (counterparty `0x3200…`) and a
    delisted-perp close-out (counterparty the zero address, one shared
    `order_id`, fee 0).
  - Never seen so far: `LIQUIDATED_CROSS_SHORT`, `BACKSTOP_BORROW_LIQUIDATION`,
    `PARTIAL_BORROW_LIQUIDATION`.
- **Liquidations.**
  - The liquidation columns are set on **both** legs; a fill is a liquidation
    exactly when `liquidation_method IS NOT NULL`.
  - The liquidated side is the row where `user = liquidated_user`. Which leg
    is crossed depends on the method:
    - `market`: the liquidated leg is crossed; it is the liquidation order.
    - `backstop`: the liquidated leg is not crossed. The crossed leg belongs to
      the liquidator (on `LIQUIDATED_*` pairs) or, under ADL, to the
      counterparty (`AUTO_DELEVERAGING`). This held in 15 of 15 fixture fills.

    Do not read `crossed` on the liquidated leg as "the liquidated user was
    the taker".
  - `market` liquidations use the ordinary open and close directions and emit
    no ledger event.
  - `backstop` fills come in two shapes, and only one has a ledger event:
    - Takeover by the backstop liquidator: both legs are `LIQUIDATED_*`. The
      takeover shares its hash with exactly one ledger `liquidation` event: 12
      of 12 in the fixtures (block 1127672017), and all 16 ledger liquidations
      in the live sample.
    - Settlement by ADL: the liquidated leg is `LIQUIDATED_*` and the
      counterparty leg is `AUTO_DELEVERAGING`. These emit no ledger event. In
      blocks 1010581248 and 1010581292 the ADL fills share their hash with the
      same liquidation's `market` fills.

    To count backstop liquidations, count fills with
    `liquidation_method = 'backstop' AND user = liquidated_user`, not ledger
    events.
- **Fees.**
  - `fee` is in `fee_token`; negative is a maker rebate (27% of fills).
  - `fee` includes `builder_fee` (documented) and `deployer_fee` (ratio
    evidence only).
  - `builder` can appear without `builder_fee`: HyperLiquid omits a zero fee.
  - `priority_gas` is HYPE, on the taker leg only.
- **Coins and market classes:**

  | Coin form | Class |
  |---|---|
  | `BTC`, `kPEPE` | core perp |
  | `<dex>:<SYM>` (xyz, io, para, mkts, hyna, km, cash, flx, vntl) | HIP-3 perp |
  | `@<n>`, or `PURR/USDC` (the only named pair) | spot |
  | `#<10·outcome_id + side>` | HIP-4 outcome; its fee token is `+<n>` |

  Spot pair names and outcome metadata are **not** in the protobuf; they come
  from HyperLiquid's `/info`. Do not derive "perp" from "no `:`" alone:
  `PURR/USDC` is spot (`hc_market_type`).
- **`start_position`** is a signed size for perps and a balance for spot and
  outcomes. The position after the fill is `start_position ± size`.
- Whether **`closed_pnl`** is gross or net of fees is not documented.

## Events

One row per event in execution order. `user`, `amount` and `fee` change
meaning by type; their column descriptions list each.

### The columns each type sets

Every row sets the canonical columns, `event_index`, `event_type`, `hash` and
`event_time_ns`; every `ledger_update` row also sets `ledger_type` and `users`.
Every other column is NULL unless listed for the row's type. A listed column
holds the protobuf value, defaults included (`false`, `0`); `?` marks a column
that is NULL when the value is absent or empty.
`blocks/src/hypercore/value_tests.rs` checks every fixture row against this
table.

| Type | Columns set |
|---|---|
| `funding` | `item_count` |
| `validator_rewards` | `item_count` |
| `c_withdrawal` | `user`, `amount`, `is_finalized` |
| `c_deposit` | `user`, `amount` |
| `delegation` | `user`, `validator`, `amount`, `is_undelegate` |
| `gossip_priority_auction_restart` | `slot_id`, `previous_winner_ip`?, `end_gas`? |
| `create_sub_account` | `user`, `sub_account`, `sub_account_name` |
| `ledger_update` / `spot_transfer` (1) | `token`, `amount`, `usdc_value`, `user`, `destination`, `fee`, `native_token_fee`, `nonce`, `fee_token`? |
| `ledger_update` / `c_staking_transfer` (2) | `token`, `amount`, `is_deposit` |
| `ledger_update` / `account_class_transfer` (3) | `usdc`, `to_perp` |
| `ledger_update` / `internal_transfer` (4) | `usdc`, `user`, `destination`, `fee` |
| `ledger_update` / `sub_account_transfer` (5) | `usdc`, `user`, `destination` |
| `ledger_update` / `send` (6) | `user`, `destination`, `source_dex`, `destination_dex`, `token`, `amount`, `usdc_value`, `fee`, `native_token_fee`, `nonce`, `fee_token`? |
| `ledger_update` / `deposit` (7) | `usdc` |
| `ledger_update` / `withdraw` (8) | `usdc`, `nonce`, `fee` |
| `ledger_update` / `vault_deposit` (9) | `vault`, `usdc` |
| `ledger_update` / `rewards_claim` (10) | `amount`, `token` |
| `ledger_update` / `vault_withdraw` (11) | `vault`, `user`, `requested_usd`, `commission`, `closing_cost`, `basis`, `net_withdrawn_usd` |
| `ledger_update` / `vault_leader_commission` (12) | `user`, `usdc` |
| `ledger_update` / `deploy_gas_auction` (13) | `token`, `amount` |
| `ledger_update` / `account_activation_gas` (14) | `amount`, `token` |
| `ledger_update` / `activate_dex_abstraction` (15) | `dex`, `token`, `amount` |
| `ledger_update` / `liquidation` (16) | `liquidated_ntl_pos`, `account_value`, `leverage_type`, `liquidated_positions` |
| `ledger_update` / `spot_genesis` (17) | `token`, `amount` |
| `ledger_update` / `vault_distribution` (18) | `vault`, `usdc` |
| `ledger_update` / `borrow_lend` (19) | `token`, `amount`, `interest_amount`, `operation` |
| `ledger_update` / `vault_create` (20) | `vault`, `usdc`, `fee` |
| `ledger_update` / `gossip_priority_gas_auction` (21) | `token`, `amount` |
| `ledger_update` / `hip3_liquidator_deposit` (22) | `dex`, `token`, `amount` |

The numbers are the `LedgerUpdateDelta` case numbers.

### Hashes, accounts and pairs

- **Hash kinds:** the HyperCore L1 hash of the user action; the Arbitrum One
  transaction hash for `deposit` and `withdraw` (it can recur a few blocks
  later, and batched bridge transactions share one); zero for system events.
- **Hashes shared within a block:** `c_deposit` and `c_staking_transfer`
  (adjacent; the transfer comes first in the fixtures),
  `vault_withdraw` and `vault_leader_commission`, `account_activation_gas` and
  `spot_transfer`, batched withdraws, and `vault_distribution` (one vault-side
  event plus one per recipient; 1 to 204 recipients observed).
- **`users`**, HyperLiquid's ledger index, by type (monitor M6 checks the 1–2
  entries):

  | Type | `users` |
  |---|---|
  | `send`, `spot_transfer`, `internal_transfer` | `[user, destination]`; `[user]` for a cross-dex send to self |
  | `sub_account_transfer` | 2 entries, order varies (`[user, destination]` in about 51%) |
  | `vault_withdraw` | `[user, vault]` |
  | `vault_deposit`, `vault_create` | 2 entries including the vault |
  | `vault_leader_commission` | `[user]` |
  | `vault_distribution` | **1** entry. One event per affected ledger, all sharing the hash and `vault`. One event is the vault's own (`users = [vault]`) and its `usdc` is the distributed total. Each recipient gets one event (`users = [recipient]`) carrying its share, and the shares add up to the total. `sum(usdc)` over every event is therefore exactly twice the distribution: use `users[1] = vault` for totals and `users[1] <> vault` for per-recipient credits (cookbook C12). |
  | `hip3_liquidator_deposit` | `[depositor, 0x4000…{dex index}]` |
  | every other type | exactly the one account (no other address on the row) |

  `hypercore_account_events_v` gives one row per (event, account) from `users`
  and the body's `user`.

### Staking, fees, nonces

- **Staking.** `c_deposit` and `c_withdrawal` are HYPE moves between spot and
  staking, **not** bridge flows; USDC bridge flows are the ledger `deposit` and
  `withdraw`.
  - `c_deposit` pairs with `c_staking_transfer` where `is_deposit` is true (same
    hash and amount): **count one of them** (cookbook C11).
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
- **`borrow_lend`:** `operation` is `supply`, `withdraw`, `borrow` or `repay`;
  tokens observed: USDC, HYPE, UBTC, USDH, USDT0.
- **Ledger `liquidation`:** backstop takeovers only (ADL-settled backstop
  liquidations have none); one position with a positive `szi` in every
  observation.
- **Ordering.** Funding events come first in their block. Do not assume other
  system events come first: about 8% of validator-reward blocks had user ledger
  events before `validator_rewards`.

## Funding

- Hourly, in the first block at or after `HH:00:00`. That block holds one
  funding event per perp dex, in perp-dex order, including dexes with no
  payments (`item_count = 0`; about 34% of events).
- The perp-dex index is the event's ordinal among the block's funding events
  (`hypercore_funding_events_v.dex_index`). Index order: 0 = the default dex
  (`''`), 1 xyz, 2 flx, 3 vntl, 4 hyna, 5 km, 6 abcd, 7 cash, 8 para, 9 mkts,
  10 io. There were 6 dexes in January 2026, 9 in May and 11 in October.
  **Observed, not documented upstream:** it was verified against HyperLiquid's
  `perpDexs` on two hours, and the layout (funding events at positions 0..N−1)
  on every sampled funding block (monitor M3).
- Rows: about 435k per hour in October 2026 (about 26 MB of protobuf in one
  block), about 10.4M per day, growing.
- `(block_num, user, coin)` is unique, and `funding_rate` is constant per coin
  per event.
- The sign of `funding_amount` is opposite to that of `szi × funding_rate`; 61%
  of amounts are negative.
- The collateral of HIP-3 dexes is inferred from `fee_token`: USDT0 for cash,
  USDH for km, flx and vntl, USDE for hyna.
- `sum(abs(szi))` per coin at a funding block is a full snapshot of open
  positions, about twice the one-sided open interest (inferred). Positions
  opened and closed between two funding blocks are invisible.

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

## Refusals

Every block is decoded, guarded against unknown fields, checked against its
Firehose identity, then validated and converted completely before any row is
appended: a refused block appends nothing to any table, and `build` stops on it
with an error that names the block, the protobuf path (`fills[12].price`,
`events[3].events[0].ledger_update.delta.send.fee`) and the value. A refusal
needs a new fireparq release, never a skipped block.

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

Not validated, only documented and monitored by M1–M13: the payload times
against the block time, the funding layout, the hash layout, byte widths, the
`users` count, the `builder` format, fill pairing, the gossip presence pair
and `fee_token` against `fee`. Not validated and only documented: the `users`
composition, signs and the metadata id strings; `fireparq validate` checks
contiguous block numbers.

## Lossless

The five tables keep every protobuf value: the fixture tests rebuild each of 36
real payloads byte for byte from the tables, under `hex` and `binary`. The
inverse rules: decimals as canonical text; NULL-on-empty columns as `''` or
empty bytes; `fill_time` as a timestamp with whole milliseconds; the `*_ns`
columns as seconds and nanoseconds; `liquidation_method IS NOT NULL` as a
present `FillLiquidation`; `previous_winner_ip IS NOT NULL` as a present
previous winner; NULL `end_gas` and `twap_id` as absent; child rows by
`delta_index` and `reward_index`, as many as `item_count`; labels back to enum
and oneof values. `BlockHeader.block_number` is the canonical `block_num`, and
every event has exactly one body.

## `extra_json` and schema epochs

The columns, their types and nullability are bound into the root's protected
identity: an added or changed column needs a new output root, a rebuild from
the origin (about 330M blocks). `extra_json` (every table, the last column
before `fork_step`) is the lane that lets most upstream additions ship without
one. **Schema epoch 1 writes NULL in every row.**

When upstream adds a field or case, the running release refuses the first block
that carries it (R2, R7). The release that vendors the new protos then either
writes the value into an existing typed column, only when its name, protobuf
type and documented meaning match that column, or into `extra_json`; a new enum
value needs no code, and a new oneof case gets a new `event_type` or
`ledger_type` label. The root then resumes from its protected cursor with no
rebuild. Such a release must keep `HYPERCORE_SCHEMA_DIGEST`
(`blocks/src/chain/tests.rs`) and the fixture output pin
(`blocks/src/hypercore/value_tests.rs`) unchanged and add fixtures for the new
type.

**The `extra_json` format**, fixed now so every release writes the same:

- One JSON object of the values that no typed column holds; NULL, never `"{}"`,
  when there are none.
- The root object is the row's message: `Block` (`blocks`), `Fill` (`fills`),
  `FundingDelta`, `ValidatorReward`, and for `events` the body's case message
  (for `ledger_update`, the delta's case message). New fields of the wrapper
  messages go under their message names as keys: `"Event"`, `"EventBody"`,
  `"LedgerUpdate"`, `"LedgerUpdateDelta"` (protobuf field names are
  lower_snake_case, so these cannot collide). A new field of `Funding` or
  `ValidatorRewards` goes to that event's `events` row.
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
| (none in epoch 1) | | | | |

Query patterns in DuckDB: `extra_json->>'field'`,
`(extra_json->>'field')::DECIMAL(38,10)` and `extra_json->'Event'->>'field'`;
in Spark, `get_json_object(extra_json, '$.field')`.

**Schema epochs.** Promote `extra_json` keys to typed columns only in a planned
epoch, bundled with any rebuild that is forced anyway (a `MAPPER_EPOCH` bump, a
hole repair, a partition-policy change). Build epoch N+1 into a new root (for
example `…/hypercore/epoch-2/`) while epoch N serves, and cut over when it has
caught up. A decimal with more than 10 fractional or 28 integer digits, a
`u64` above `i64::MAX`, a type change, a field removed behind a non-null column
or a new high-volume list (a second funding-like list) cannot be placed and
needs a new epoch. Upstream re-extracting history (for example the pre-cutover
builder fields) leaves the old rows stale: repair or rebuild.

| Epoch | From release | Root | Notes |
|---|---|---|---|
| 1 | the first release with `--block-type hypercore` | origin 846903317 | five tables, `extra_json` unused |

Upstream merges new protos just before the data arrives (its own reader aborts
on unknown types), so a weekly comparison of `buf export buf.build/pinax/hypercore`
with `proto/pinax/hypercore/v1/` shortens the stop.

## Views, monitors and cookbook

The SQL below assumes the default `hex` encoding and DuckDB 1.5 or later with
its `delta` extension. It is not part of the schema: change it freely.
`blocks/tests/engine_compat.rs` runs all of it over a HyperCore build of the
fixture blocks. Start with a view per table:

```sql
CREATE VIEW blocks AS SELECT * FROM delta_scan('<root>/blocks');
CREATE VIEW fills AS SELECT * FROM delta_scan('<root>/fills');
CREATE VIEW events AS SELECT * FROM delta_scan('<root>/events');
CREATE VIEW funding_deltas AS SELECT * FROM delta_scan('<root>/funding_deltas');
CREATE VIEW validator_rewards AS SELECT * FROM delta_scan('<root>/validator_rewards');
```

### View pack

Macros, fill and funding views, an account index, and one view per ledger type
and per scalar body.

```sql
CREATE OR REPLACE MACRO hc_zero_hash() AS '0x' || repeat('0', 64);
CREATE OR REPLACE MACRO hc_market_type(coin) AS CASE
    WHEN regexp_full_match(coin, '#[0-9]+') THEN 'outcome'
    WHEN regexp_full_match(coin, '@[0-9]+') OR contains(coin, '/') THEN 'spot'
    ELSE 'perp' END;
CREATE OR REPLACE MACRO hc_perp_dex(coin) AS CASE
    WHEN hc_market_type(coin) <> 'perp' THEN NULL
    WHEN contains(coin, ':') THEN split_part(coin, ':', 1)
    ELSE '' END;   -- '' = the default perp dex (HyperLiquid's own name for it)

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
       hc_market_type(f.coin)                                        AS market_type,
       hc_perp_dex(f.coin)                                           AS dex,
       CASE WHEN hc_market_type(f.coin) = 'outcome' THEN CAST(substr(f.coin, 2) AS BIGINT) // 10 END AS outcome_id,
       CASE WHEN hc_market_type(f.coin) = 'outcome' THEN CAST(substr(f.coin, 2) AS BIGINT) % 10 END  AS outcome_side,
       f.price * f.size                                              AS notional
FROM fills f;

CREATE OR REPLACE VIEW hypercore_funding_events_v AS
SELECT block_num, timestamp, date, event_index, item_count,
       CAST(row_number() OVER (PARTITION BY block_num ORDER BY event_index) - 1 AS BIGINT) AS dex_index
FROM events WHERE event_type = 'funding';

CREATE OR REPLACE VIEW hypercore_funding_v AS
SELECT d.*, e.dex_index, hc_perp_dex(d.coin) AS dex
FROM funding_deltas d JOIN hypercore_funding_events_v e USING (block_num, event_index);

-- One row per (event, account): HyperLiquid's ledger index (users) plus the body's user.
CREATE OR REPLACE VIEW hypercore_account_events_v AS
SELECT e.block_num, e.timestamp, e.date, e.event_index, e.event_type, e.ledger_type, u.account
FROM events e, unnest(e.users) AS u(account) WHERE e.event_type = 'ledger_update'
UNION ALL
SELECT block_num, timestamp, date, event_index, event_type, ledger_type, user AS account
FROM events WHERE event_type IN ('c_deposit', 'c_withdrawal', 'delegation', 'create_sub_account');

-- One view per ledger_type, with the columns of the matrix above.
CREATE OR REPLACE VIEW ledger_spot_transfer AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount, usdc_value, user,
       destination, fee, native_token_fee, nonce, fee_token
FROM events WHERE ledger_type = 'spot_transfer';
CREATE OR REPLACE VIEW ledger_c_staking_transfer AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount, is_deposit
FROM events WHERE ledger_type = 'c_staking_transfer';
CREATE OR REPLACE VIEW ledger_account_class_transfer AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, usdc, to_perp
FROM events WHERE ledger_type = 'account_class_transfer';
CREATE OR REPLACE VIEW ledger_internal_transfer AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, usdc, user, destination, fee
FROM events WHERE ledger_type = 'internal_transfer';
CREATE OR REPLACE VIEW ledger_sub_account_transfer AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, usdc, user, destination
FROM events WHERE ledger_type = 'sub_account_transfer';
CREATE OR REPLACE VIEW ledger_send AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, user, destination, source_dex,
       destination_dex, token, amount, usdc_value, fee, native_token_fee, nonce, fee_token
FROM events WHERE ledger_type = 'send';
CREATE OR REPLACE VIEW ledger_deposit AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, usdc
FROM events WHERE ledger_type = 'deposit';
CREATE OR REPLACE VIEW ledger_withdraw AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, usdc, nonce, fee
FROM events WHERE ledger_type = 'withdraw';
CREATE OR REPLACE VIEW ledger_vault_deposit AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, vault, usdc
FROM events WHERE ledger_type = 'vault_deposit';
CREATE OR REPLACE VIEW ledger_rewards_claim AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, amount, token
FROM events WHERE ledger_type = 'rewards_claim';
CREATE OR REPLACE VIEW ledger_vault_withdraw AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, vault, user, requested_usd, commission,
       closing_cost, basis, net_withdrawn_usd
FROM events WHERE ledger_type = 'vault_withdraw';
CREATE OR REPLACE VIEW ledger_vault_leader_commission AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, user, usdc
FROM events WHERE ledger_type = 'vault_leader_commission';
CREATE OR REPLACE VIEW ledger_deploy_gas_auction AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount
FROM events WHERE ledger_type = 'deploy_gas_auction';
CREATE OR REPLACE VIEW ledger_account_activation_gas AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, amount, token
FROM events WHERE ledger_type = 'account_activation_gas';
CREATE OR REPLACE VIEW ledger_activate_dex_abstraction AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, dex, token, amount
FROM events WHERE ledger_type = 'activate_dex_abstraction';
CREATE OR REPLACE VIEW ledger_liquidation AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, users[1] AS liquidated_account,
       liquidated_ntl_pos, account_value, leverage_type, liquidated_positions
FROM events WHERE ledger_type = 'liquidation';
CREATE OR REPLACE VIEW ledger_spot_genesis AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount
FROM events WHERE ledger_type = 'spot_genesis';
CREATE OR REPLACE VIEW ledger_vault_distribution AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, vault, usdc
FROM events WHERE ledger_type = 'vault_distribution';
CREATE OR REPLACE VIEW ledger_borrow_lend AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount, interest_amount, operation
FROM events WHERE ledger_type = 'borrow_lend';
CREATE OR REPLACE VIEW ledger_vault_create AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, vault, usdc, fee
FROM events WHERE ledger_type = 'vault_create';
CREATE OR REPLACE VIEW ledger_gossip_priority_gas_auction AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, token, amount
FROM events WHERE ledger_type = 'gossip_priority_gas_auction';
CREATE OR REPLACE VIEW ledger_hip3_liquidator_deposit AS
SELECT block_num, block_id, timestamp, date, event_index, hash, users, dex, token, amount
FROM events WHERE ledger_type = 'hip3_liquidator_deposit';

-- One view per scalar body (funding and validator rewards are their own tables).
CREATE OR REPLACE VIEW c_deposits AS
SELECT block_num, block_id, timestamp, date, event_index, hash, user, amount FROM events WHERE event_type = 'c_deposit';
CREATE OR REPLACE VIEW c_withdrawals AS
SELECT block_num, block_id, timestamp, date, event_index, hash, user, amount, is_finalized FROM events WHERE event_type = 'c_withdrawal';
CREATE OR REPLACE VIEW delegations AS
SELECT block_num, block_id, timestamp, date, event_index, hash, user, validator, amount, is_undelegate FROM events WHERE event_type = 'delegation';
CREATE OR REPLACE VIEW gossip_priority_auction_restarts AS
SELECT block_num, block_id, timestamp, date, event_index, slot_id, previous_winner_ip, end_gas FROM events WHERE event_type = 'gossip_priority_auction_restart';
CREATE OR REPLACE VIEW create_sub_accounts AS
SELECT block_num, block_id, timestamp, date, event_index, hash, user, sub_account, sub_account_name FROM events WHERE event_type = 'create_sub_account';
```

### Monitors

Data-quality monitors, not halts: each query returns the rows that break an
observation the mapper does not enforce, and an empty result means it still
holds. They need the macros above. All return no rows on the 36 fixture blocks
and on 118,001 sample blocks.

```sql
-- M1 fill_time equals the block time truncated to ms
SELECT 'M1' m, block_num, fill_index FROM fills WHERE fill_time <> timestamp;
-- M2 event time equals block time (ns)
SELECT 'M2' m, e.block_num, e.event_index FROM events e JOIN blocks b USING (block_num) WHERE e.event_time_ns <> b.block_time_ns;
-- M3 funding events occupy event positions 0..N-1
SELECT 'M3' m, block_num FROM events WHERE event_type = 'funding' GROUP BY block_num HAVING max(event_index) <> count(*) - 1;
-- M4 non-zero HyperCore hashes: byte[10]=0x04, bytes[11..15)=block_num (deposit/withdraw excluded)
SELECT 'M4' m, block_num, fill_index FROM fills
WHERE hash <> hc_zero_hash() AND (substr(hash, 23, 2) <> '04' OR substr(hash, 25, 8) <> lpad(lower(hex(block_num)), 8, '0'));
SELECT 'M4e' m, block_num, event_index FROM events
WHERE hash <> hc_zero_hash() AND coalesce(ledger_type, '') NOT IN ('deposit', 'withdraw')
  AND (substr(hash, 23, 2) <> '04' OR substr(hash, 25, 8) <> lpad(lower(hex(block_num)), 8, '0'));
-- M5 byte widths: addresses 20 B, hashes 32 B, cloids 16 B (hex: 42/66/34 chars)
SELECT 'M5' m, block_num, fill_index FROM fills
WHERE length(user) <> 42 OR length(hash) <> 66 OR length(client_order_id) <> 34 OR length(liquidated_user) <> 42;
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
SELECT 'M8i' m, e.block_num, e.event_index FROM events e
LEFT JOIN (SELECT block_num, event_index, count(*) n FROM funding_deltas GROUP BY ALL
           UNION ALL SELECT block_num, event_index, count(*) FROM validator_rewards GROUP BY ALL) c USING (block_num, event_index)
WHERE e.item_count IS NOT NULL AND e.item_count <> coalesce(c.n, 0);
-- M9 non-outcome trades pair: exactly 2 adjacent legs, BUY first, one crossed
SELECT 'M9' m, block_num, transaction_id FROM fills
WHERE transaction_id <> 0 AND NOT regexp_full_match(coin, '#[0-9]+')
GROUP BY block_num, transaction_id
HAVING count(*) <> 2 OR sum(crossed::INT) <> 1 OR arg_min(side, fill_index) <> 'BUY' OR max(fill_index) - min(fill_index) <> 1;
-- M10 gossip: previous_winner and end_gas present together
SELECT 'M10' m, block_num, event_index FROM events
WHERE event_type = 'gossip_priority_auction_restart' AND (previous_winner_ip IS NULL) <> (end_gas IS NULL);
-- M11 liquidation present but liquidated_user empty
SELECT 'M11' m, block_num, fill_index FROM fills WHERE liquidation_method IS NOT NULL AND liquidated_user IS NULL;
-- M12 send/spot fee_token NULL exactly when fee = 0
SELECT 'M12' m, block_num, event_index FROM events
WHERE ledger_type IN ('send', 'spot_transfer') AND (fee_token IS NULL) <> (fee = 0);
-- M13 extra_json is still NULL in every table (no release has used it yet)
SELECT 'M13' m, 'blocks' t, block_num, NULL::BIGINT i FROM blocks WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'fills', block_num, fill_index FROM fills WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'events', block_num, event_index FROM events WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'funding_deltas', block_num, delta_index FROM funding_deltas WHERE extra_json IS NOT NULL
UNION ALL SELECT 'M13', 'validator_rewards', block_num, reward_index FROM validator_rewards WHERE extra_json IS NOT NULL;
```

### Cookbook

```sql
-- C1 taker notional per day, market type and perp dex (trades only)
SELECT date, market_type, dex, sum(notional) AS taker_notional, count(*) AS trades
FROM hypercore_fills_v WHERE is_taker AND NOT is_non_trade GROUP BY ALL ORDER BY ALL;
-- C2 one-minute OHLCV for BTC from taker legs (trades only, forced trades excluded)
SELECT time_bucket(INTERVAL 1 MINUTE, timestamp) AS minute,
       arg_min(price, (block_num, fill_index)) AS open, max(price) AS high, min(price) AS low,
       arg_max(price, (block_num, fill_index)) AS close, sum(size) AS volume
FROM hypercore_fills_v WHERE coin = 'BTC' AND is_taker AND NOT is_non_trade AND NOT is_forced GROUP BY ALL ORDER BY 1;
-- C3 liquidated side of every liquidation fill
SELECT block_num, fill_index, user, coin, direction, liquidation_method, liquidation_mark_px, price, size
FROM fills WHERE user = liquidated_user;
-- C4 both legs of each trade (zero tid excluded by trade_id)
SELECT b.block_num, b.trade_id, b.user AS buyer, a.user AS seller, b.price, b.size, b.crossed AS buyer_is_taker
FROM hypercore_fills_v b JOIN hypercore_fills_v a
  ON a.block_num = b.block_num AND a.trade_id = b.trade_id AND a.fill_index = b.fill_index + 1
WHERE b.side = 'BUY' AND a.side = 'ASK';
-- C5 backstop takeovers joined to their ledger liquidation event (zero hashes excluded).
--    ADL-settled backstop liquidations have no ledger event. One hash can cover several
--    accounts' fills, so match the event's account too. Rows are per fill: liquidated_ntl_pos
--    repeats on each leg, so do not sum it over this result.
SELECT f.block_num, f.fill_index, e.event_index, e.liquidated_ntl_pos
FROM hypercore_fills_v f JOIN events e
  ON e.block_num = f.block_num AND e.hash = f.tx_hash AND e.users[1] = f.liquidated_user
WHERE e.ledger_type = 'liquidation' AND f.user = f.liquidated_user AND f.liquidation_method = 'backstop';
-- C6 an account's non-funding ledger history (its funding payments are in funding_deltas)
SELECT e.* FROM hypercore_account_events_v a JOIN events e USING (block_num, event_index)
WHERE a.account = '0x…' ORDER BY block_num, event_index;
-- C7 latest hourly position snapshot
SELECT user, coin, szi FROM funding_deltas
WHERE block_num = (SELECT max(block_num) FROM events WHERE event_type = 'funding');
-- C8 funding per dex per hour, keeping empty funding events
SELECT e.block_num, e.dex_index, e.item_count, coalesce(sum(d.funding_amount), 0) AS net_funding
FROM hypercore_funding_events_v e LEFT JOIN funding_deltas d USING (block_num, event_index) GROUP BY ALL ORDER BY 1, 2;
-- C9 builder revenue per fee token (builder_fee is in fee_token; fully captured from block 957002478)
SELECT builder, fee_token, sum(builder_fee) AS revenue, count(*) AS fills
FROM fills WHERE block_num >= 957002478 AND builder IS NOT NULL GROUP BY ALL ORDER BY revenue DESC NULLS LAST;
-- C10 three-decimal products: szi * funding_rate * szi overflows in DuckDB, so cast an intermediate first.
-- DOUBLE is approximate. CAST to DECIMAL(38,10) rounds each row to 10 places, so that column is not exact either.
-- A product of two needs no cast: sum(szi * funding_rate) is exact, DECIMAL(38,20).
SELECT sum(szi::DOUBLE * funding_rate::DOUBLE * szi::DOUBLE) AS approx,
       sum(CAST(CAST(szi * funding_rate AS DECIMAL(38, 10)) * szi AS DECIMAL(38, 10))) AS rounded_to_10dp,
       sum(szi * funding_rate) AS exact_pair
FROM funding_deltas;
-- C11 staking flows counted once (c_deposit duplicates c_staking_transfer)
SELECT ledger_type, is_deposit, sum(amount) AS hype FROM events WHERE ledger_type = 'c_staking_transfer' GROUP BY ALL;
-- C12 vault flows; depositor of vault_deposit/vault_create from users; vault_distribution recipients only (the vault-side row repeats their total)
SELECT ledger_type, vault, coalesce(user, list_filter(users, lambda u: u <> vault)[1]) AS account, usdc, net_withdrawn_usd
FROM events WHERE ledger_type IN ('vault_deposit', 'vault_withdraw', 'vault_create', 'vault_leader_commission')
   OR (ledger_type = 'vault_distribution' AND users[1] <> vault);
```

## Volume and flushes

Expected volume in October 2026, about 1.21M blocks per day (Parquet bytes
measured with zstd and 65,536-row groups on 118k mid-2026 blocks):

| Table | Rows per day | Parquet bytes per row | Parquet per day | History to date (about 327M blocks) |
|---|---|---|---|---|
| `blocks` | 1.21M | 20–25 | 25–30 MB | 327M rows, about 8 GB |
| `fills` | 10.3M (13.6M at the September peak) | 50–54 | 0.52–0.70 GB | about 2.4B rows, about 125 GB |
| `events` | about 95k | 80–93 | 8–9 MB | about 23M rows, about 2 GB |
| `funding_deltas` | about 10.4M, in 24 blocks of about 435k rows | about 23 | about 0.24 GB | about 2.0B rows, about 46 GB |
| `validator_rewards` | about 50k | 1–3 | under 0.2 MB | about 13M rows, under 0.1 GB |
| **Total** | about 22M | | **about 0.8–1.0 GB** | **about 180 GB** |

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
