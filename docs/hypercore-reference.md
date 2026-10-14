# HyperLiquid reference tables

A HyperCore lake ([HyperCore notes](chains/hypercore.md)) is built from
Firehose blocks, which carry no names: spot pairs are `@107`, outcome sides
`#99731`, HIP-3 perps `xyz:XYZ100`, and the funding premium is not there at
all. HyperLiquid's `POST /info` has them, but only as **current state**, not
state at a block. `fireparq hypercore-reference` snapshots it into small
append-only Delta tables (`hl_*`) that readers join with the lake at query
time.

The tables live in **their own root**, never a fireparq dataset root: a
`build` binds its root's exact table set, schemas and mapper version under
`.fireparq-ingest/`, with an owner record beside them, so extra tables there
would make it refuse to resume. Use a separate bucket, for example
`s3://hypercore-ref`, or a separate local directory.

The subcommand ships in `fireparq` 1.3.0 and later, in the writer's image
`ghcr.io/pinax-network/firehose-parquet`. It is a Rust port of the Python job
of k8s-parquet PR #82 (image `ghcr.io/pinax-network/hypercore-reference`,
never published), with the same tables, payload hashes and views; the
[differences](#differences-from-the-python-job) are listed below. Nothing in it
links DataFusion.

- [Tables](#tables)
- [Views](#views)
- [Joining the lake](#joining-the-lake)
- [Cross-checks against the lake](#cross-checks-against-the-lake)
- [Cadence, rate limits and backfills](#cadence-rate-limits-and-backfills)
- [When a run fails](#when-a-run-fails)
- [Coverage and point-in-time limits](#coverage-and-point-in-time-limits)
- [The command](#the-command)
- [Output](#output)
- [Concurrent runs](#concurrent-runs)
- [Compaction and VACUUM](#compaction-and-vacuum)
- [The S3 user](#the-s3-user)
- [Scheduling](#scheduling)
- [Operations](#operations)
- [Tests](#tests)

## Tables

Every table is an unpartitioned Delta table directly under the root, with
`delta.appendOnly`: Delta itself refuses a commit that removes rows. A row is
appended **only when an entity's payload hash changes**, so each table is a
change log, and rerunning a snapshot appends nothing. Every table also has:

| Column | Meaning |
|---|---|
| `listed` | (all but `hl_spot_token_details` and `hl_funding_history`) `true` if the entity was in the snapshot that wrote the row. When an entity leaves its snapshot, one row records it: `listed = false`, the typed columns copied from its previous row, `payload_json` and `payload_sha256` NULL. A snapshot may not delist more than the larger of 25 entities and 20% of a table's listed ones ([When a run fails](#when-a-run-fails)) |
| `observed_at` | when `/info` answered (UTC, microseconds) |
| `source` | the `/info` request: `spotMeta`, `tokenDetails`, `perpDexs+allPerpMetas`, `allPerpMetas`, `outcomeMeta`, `settledOutcome`, `fundingHistory` |
| `payload_sha256` | SHA-256 of `payload_json` |
| `payload_json` | the entity as `/info` returned it, as canonical JSON (keys sorted, no whitespace, numbers as sent). A field without a typed column (a new upstream field) is here, and so is every field of a response beside its entity lists (`hl_spot_meta`, `hl_outcome_meta`) |

The vocabulary is the lake's: `dex = ''` is the default (core) perp dex,
`side_index` an outcome side, and `coin` the lake's fill coin: `@<pair>`,
`PURR/USDC`, `<dex>:<SYM>`, `#<10 * outcome_id + side_index>`. Amounts are
`decimal(38,10)` exactly as sent; a value that does not fit is NULL in its
typed column and kept in `payload_json`. Timestamps are `timestamp` (UTC).

| Table | Key | Columns | Source |
|---|---|---|---|
| `hl_spot_meta` | `request` (`spotMeta`) | none: `payload_json` is `spotMeta` without its `tokens` and `universe` | `spotMeta` |
| `hl_spot_tokens` | `token_index` | `name`, `full_name`, `token_id`, `sz_decimals`, `wei_decimals`, `evm_contract`, `evm_extra_wei_decimals`, `is_canonical`, `deployer_trading_fee_share` | `spotMeta` token entry |
| `hl_spot_token_details` | `token_index` | `token_id`, `deployer`, `deploy_time`, `deploy_gas`; `payload_json` is the whole response, including supply and prices at that moment (up to a few MB for HYPE's genesis list). One row per token `/info` answered: read once when the token is first seen; a token it failed is asked again once a day | `tokenDetails` |
| `hl_spot_pairs` | `pair_index` | `coin` (`@N`, or `PURR/USDC` for pair 0), `name` (`BASE/QUOTE`), `base_token_index`, `quote_token_index`, `base_symbol`, `quote_symbol`, `is_canonical` | `spotMeta` pair entry |
| `hl_perp_dexs` | `dex_index` | `name` (`''` for 0, as `fills.dex`), `full_name`, `deployer`, `oracle_updater`, `fee_recipient`, `collateral_token_index`, `collateral_symbol` | the whole `perpDexs` entry and `allPerpMetas` object but its `universe`. Its parameters (OI caps, `assetToFundingInterestRate`) change within minutes: an hourly job records an hourly sample of them, not every change |
| `hl_perp_assets` | `(dex_index, asset_index)` | `dex`, `coin`, `asset_id` (the index, or 100000 + dex_index × 10000 + index), `sz_decimals`, `max_leverage`, `margin_table_id`, `is_delisted`, `only_isolated`, `margin_mode`, `growth_mode`, `deployer_fee_scale` | `allPerpMetas` universe entry |
| `hl_outcome_meta` | `request` (`outcomeMeta`) | `fee_scale`, `venue_deployers` (venue → deployer); `payload_json` is `outcomeMeta` without its `outcomes` and `questions` | `outcomeMeta` |
| `hl_outcomes` | `outcome_id` | `question_id`, `name`, `description`, `description_kv` (the `k:v\|k:v` description as a map; NULL if it is not one), `side_names[]` and `side_coins[]` (indexed by `side_index`; 1-based in DuckDB), `quote_token`, `venue`, `deployer_fee_scale`, `status` (`live`, `settled`), `settle_fraction`, `settle_details` | `outcomeMeta` while live; the `settledOutcome` response once settled |
| `hl_questions` | `question_id` | `name`, `description`, `description_kv`, `fallback_outcome_id`, `named_outcome_ids[]`, `settled_outcome_ids[]`, `status` | `outcomeMeta`; for a question that left it, the `question` of a `settledOutcome` response (name, description, status; NULL lists) |
| `hl_funding_history` | `(coin, funding_time)` | `dex`, `funding_time` (the funding block's millisecond `timestamp`), `funding_rate`, `premium` | `fundingHistory`, from 2026-01-01; each hour appended once |

The `outcomes` job also keeps `_hlref/outcome_probes` (`outcome_id`,
`probed_at`, `result`, `run_id`): every `settledOutcome` probe, the sweep's
own state. It is not a reference table, and the views do not read it.

Outcome and question names are often templates (`template:binaryPrice`,
`template:{shortNameA}`) whose parameters are in the description
(`perp:BTC|threshold:81299|time:20261008-1645`); `description_kv` exposes them
as a map. The job stores them as sent and does not render templates. Every
question an `hl_outcomes` row names has an `hl_questions` row: questions are
committed first, and `verify` checks it.

A release may add a typed column only at the end of a table, as a nullable
column: the first run of that release adds it with a metadata-only commit
(earlier rows read NULL there; their `payload_json` still has the field). The
job refuses any other schema difference (an inserted, renamed, retyped or
dropped column) with exit status 2.

## Views

The views are SQL, not part of the command. Replace `<root>` with your root
without a trailing `/` (for example `s3://hypercore-ref`, or an absolute
local path), and run them in DuckDB 1.5 or later (tested with 1.5.5) after
`INSTALL delta; LOAD delta;` and `SET TimeZone = 'UTC';`, with a scoped S3
secret for an `s3://` root ([Reading the tables](reading-tables.md)). DuckDB
resolves each `delta_scan` when it creates the view, so all ten tables must
exist: each job creates its tables on its first run.
`blocks/tests/hypercore_reference_cli.rs` runs all of them over tables the
command wrote.

| View | Rows |
|---|---|
| `hl_<table>` | the Delta table (`delta_scan`): an append-only change log, one row per payload change of an entity, and one when it leaves its snapshot |
| `hl_<table>_current` | the latest row of each entity. `listed = false` means the entity has left its snapshot (or, for outcomes and questions, has settled); its typed columns are its last known values |
| `hl_<table>_history` | every row with `valid_from` (its `observed_at`) and `valid_to` (the entity's next row's `observed_at`; NULL for the latest). The change itself happened between the previous successful snapshot and `valid_from` |
| `hl_outcome_sides_current` | one row per outcome side, keyed by the lake's coin `#<10 * outcome_id + side_index>`: outcome and side labels, question, status and settlement |

The base views:

```sql
CREATE OR REPLACE VIEW hl_spot_meta AS SELECT * FROM delta_scan('<root>/hl_spot_meta');
CREATE OR REPLACE VIEW hl_spot_tokens AS SELECT * FROM delta_scan('<root>/hl_spot_tokens');
CREATE OR REPLACE VIEW hl_spot_token_details AS SELECT * FROM delta_scan('<root>/hl_spot_token_details');
CREATE OR REPLACE VIEW hl_spot_pairs AS SELECT * FROM delta_scan('<root>/hl_spot_pairs');
CREATE OR REPLACE VIEW hl_perp_dexs AS SELECT * FROM delta_scan('<root>/hl_perp_dexs');
CREATE OR REPLACE VIEW hl_perp_assets AS SELECT * FROM delta_scan('<root>/hl_perp_assets');
CREATE OR REPLACE VIEW hl_outcome_meta AS SELECT * FROM delta_scan('<root>/hl_outcome_meta');
CREATE OR REPLACE VIEW hl_outcomes AS SELECT * FROM delta_scan('<root>/hl_outcomes');
CREATE OR REPLACE VIEW hl_questions AS SELECT * FROM delta_scan('<root>/hl_questions');
CREATE OR REPLACE VIEW hl_funding_history AS SELECT * FROM delta_scan('<root>/hl_funding_history');
```

The latest row of each entity:

```sql
CREATE OR REPLACE VIEW hl_spot_meta_current AS
SELECT * FROM hl_spot_meta
QUALIFY row_number() OVER (PARTITION BY request ORDER BY observed_at DESC) = 1;

-- tokenDetails is read once per token (again only while /info fails for it).
CREATE OR REPLACE VIEW hl_spot_token_details_current AS
SELECT * FROM hl_spot_token_details
QUALIFY row_number() OVER (PARTITION BY token_index ORDER BY observed_at DESC) = 1;

-- With the token's deployer, deploy time and gas from tokenDetails (NULL
-- while /info has not answered it; the whole response is in
-- hl_spot_token_details.payload_json).
CREATE OR REPLACE VIEW hl_spot_tokens_current AS
WITH latest AS (
    SELECT * FROM hl_spot_tokens
    QUALIFY row_number() OVER (PARTITION BY token_index ORDER BY observed_at DESC) = 1
)
SELECT latest.*, d.deployer, d.deploy_time, d.deploy_gas
FROM latest LEFT JOIN hl_spot_token_details_current d USING (token_index);

CREATE OR REPLACE VIEW hl_spot_pairs_current AS
SELECT * FROM hl_spot_pairs
QUALIFY row_number() OVER (PARTITION BY pair_index ORDER BY observed_at DESC) = 1;

CREATE OR REPLACE VIEW hl_perp_dexs_current AS
SELECT * FROM hl_perp_dexs
QUALIFY row_number() OVER (PARTITION BY dex_index ORDER BY observed_at DESC) = 1;

CREATE OR REPLACE VIEW hl_perp_assets_current AS
SELECT * FROM hl_perp_assets
QUALIFY row_number() OVER (PARTITION BY dex_index, asset_index ORDER BY observed_at DESC) = 1;

CREATE OR REPLACE VIEW hl_outcome_meta_current AS
SELECT * FROM hl_outcome_meta
QUALIFY row_number() OVER (PARTITION BY request ORDER BY observed_at DESC) = 1;

-- A settledOutcome row names its question only when the outcome belonged to
-- one; a live row only if outcomeMeta listed the question. Fill the gap from
-- the questions' own outcome lists.
CREATE OR REPLACE VIEW hl_questions_current AS
WITH known AS (
    SELECT *,
           last_value(fallback_outcome_id IGNORE NULLS) OVER w AS known_fallback,
           last_value(named_outcome_ids IGNORE NULLS) OVER w AS known_named,
           last_value(settled_outcome_ids IGNORE NULLS) OVER w AS known_settled
    FROM hl_questions
    WINDOW w AS (PARTITION BY question_id ORDER BY observed_at
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- A settled question's id lists are the last ones outcomeMeta listed.
SELECT * EXCLUDE (known_fallback, known_named, known_settled)
         REPLACE (coalesce(fallback_outcome_id, known_fallback) AS fallback_outcome_id,
                  coalesce(named_outcome_ids, known_named) AS named_outcome_ids,
                  coalesce(settled_outcome_ids, known_settled) AS settled_outcome_ids)
FROM known
QUALIFY row_number() OVER (PARTITION BY question_id ORDER BY observed_at DESC) = 1;

CREATE OR REPLACE VIEW hl_outcomes_current AS
WITH latest AS (
    SELECT * FROM hl_outcomes
    QUALIFY row_number() OVER (PARTITION BY outcome_id ORDER BY observed_at DESC) = 1
), members AS (
    SELECT DISTINCT ON (outcome_id) outcome_id, question_id
    FROM (SELECT unnest(list_append(coalesce(named_outcome_ids, []::BIGINT[]), fallback_outcome_id)) AS outcome_id,
                 question_id, observed_at
          FROM hl_questions)
    WHERE outcome_id IS NOT NULL
    ORDER BY outcome_id, observed_at DESC
)
SELECT latest.* REPLACE (coalesce(latest.question_id, members.question_id) AS question_id)
FROM latest LEFT JOIN members USING (outcome_id);

-- One row per outcome side, keyed by the lake's fill coin '#<10 * outcome_id + side_index>'.
CREATE OR REPLACE VIEW hl_outcome_sides_current AS
SELECT o.side_coins[s.i] AS coin, o.outcome_id, s.i - 1 AS side_index, o.side_names[s.i] AS side_name,
       o.name AS outcome_name, o.description AS outcome_description, o.question_id,
       q.name AS question_name, q.description AS question_description,
       o.venue, o.quote_token, o.status, o.settle_fraction
FROM hl_outcomes_current o
CROSS JOIN LATERAL (SELECT unnest(range(1, len(o.side_coins) + 1)) AS i) s
LEFT JOIN hl_questions_current q ON q.question_id = o.question_id;

-- The latest funding hour of each coin.
CREATE OR REPLACE VIEW hl_funding_history_current AS
SELECT * FROM hl_funding_history
QUALIFY row_number() OVER (PARTITION BY coin ORDER BY funding_time DESC) = 1;
```

Validity intervals. `hl_funding_history` is already a history (one row per
coin and hour) and `hl_spot_token_details` has one row per token, so neither
has a `_history` view:

```sql
CREATE OR REPLACE VIEW hl_spot_meta_history AS
SELECT *, observed_at AS valid_from,
       lead(observed_at) OVER (PARTITION BY request ORDER BY observed_at) AS valid_to
FROM hl_spot_meta;

CREATE OR REPLACE VIEW hl_spot_tokens_history AS
SELECT *, observed_at AS valid_from,
       lead(observed_at) OVER (PARTITION BY token_index ORDER BY observed_at) AS valid_to
FROM hl_spot_tokens;

CREATE OR REPLACE VIEW hl_spot_pairs_history AS
SELECT *, observed_at AS valid_from,
       lead(observed_at) OVER (PARTITION BY pair_index ORDER BY observed_at) AS valid_to
FROM hl_spot_pairs;

CREATE OR REPLACE VIEW hl_perp_dexs_history AS
SELECT *, observed_at AS valid_from,
       lead(observed_at) OVER (PARTITION BY dex_index ORDER BY observed_at) AS valid_to
FROM hl_perp_dexs;

CREATE OR REPLACE VIEW hl_perp_assets_history AS
SELECT *, observed_at AS valid_from,
       lead(observed_at) OVER (PARTITION BY dex_index, asset_index ORDER BY observed_at) AS valid_to
FROM hl_perp_assets;

CREATE OR REPLACE VIEW hl_outcome_meta_history AS
SELECT *, observed_at AS valid_from,
       lead(observed_at) OVER (PARTITION BY request ORDER BY observed_at) AS valid_to
FROM hl_outcome_meta;

CREATE OR REPLACE VIEW hl_outcomes_history AS
SELECT *, observed_at AS valid_from,
       lead(observed_at) OVER (PARTITION BY outcome_id ORDER BY observed_at) AS valid_to
FROM hl_outcomes;

CREATE OR REPLACE VIEW hl_questions_history AS
SELECT *, observed_at AS valid_from,
       lead(observed_at) OVER (PARTITION BY question_id ORDER BY observed_at) AS valid_to
FROM hl_questions;
```

## Joining the lake

Create the lake's table views and view pack of the
[HyperCore notes](chains/hypercore.md#views-monitors-and-cookbook) over the
lake's root, and the [views](#views) above over the reference root, in one
DuckDB session. On S3 give each root its own scoped secret with the endpoint,
region and key that serve it. Then join at query time.
`blocks/tests/engine_compat.rs` runs these queries over a `build` of the lake's
36 fixture blocks and reference tables the command wrote from recorded `/info`
answers.

J3 reads the `hl_funding_history` table itself (keyed by `coin,
funding_time`), not a `_history` view: a funding block's `timestamp` equals
`fundingHistory`'s `time`. It joins `hl_perp_dexs_current` on `dex_index`,
the observed ordinal that monitor M18 checks. J4 takes the row valid at a
fill's time: a field that changes (leverage, fee scales, a question's outcome
lists, dex parameters) is known only from the job's first run, so earlier
fills match no row there; for identity fields (names, symbols, token and pair
indices, outcome specifications, settlements) use `_current`.

```sql
-- J1 spot trades with pair names
SELECT f.block_num, f.fill_index, f.timestamp, p.base_symbol, p.quote_symbol, f.side, f.price, f.size,
       f.user, f.counterparty, f.crossed
FROM fills f LEFT JOIN hl_spot_pairs_current p ON p.coin = f.coin
WHERE f.market_type = 'spot';
-- J2 HIP-4 fills with labels (side_names is 1-based in DuckDB)
SELECT o.*, m.name AS outcome_name, m.side_names[o.side_index + 1] AS side_label,
       q.name AS question_name, s.settle_fraction
FROM outcome_fills o
LEFT JOIN hl_outcomes_current m USING (outcome_id)
LEFT JOIN hl_questions_current q ON q.question_id = m.question_id
LEFT JOIN outcome_settlements_v s USING (outcome_id);
-- J3 hourly funding with the premium and the dex name
SELECT r.*, h.premium, d.full_name AS dex_full_name
FROM funding_rates r
LEFT JOIN hl_funding_history h ON h.coin = r.coin AND h.funding_time = r.timestamp
LEFT JOIN hl_perp_dexs_current d ON d.dex_index = r.dex_index;
-- J4 perp fills with the leverage valid at their time
SELECT f.block_num, f.fill_index, f.timestamp, f.coin, h.max_leverage, h.is_delisted
FROM fills f
ASOF LEFT JOIN hl_perp_assets_history h ON h.coin = f.coin AND h.valid_from <= f.timestamp
WHERE f.market_type = 'perp';
```

## Cross-checks against the lake

The lake proves some reference facts by itself. Each query returns the rows
that disagree; empty is correct. Rows for entities the reference tables have
not reached yet (a backfill in progress) also appear, with NULL reference
columns.

```sql
-- X1 spot pairs: a crossed BUY pays its fee in the base token, a crossed ASK in the quote token
SELECT t.coin, t.base_fee_tokens, t.quote_fee_tokens, p.base_symbol, p.quote_symbol
FROM (SELECT coin,
             list(DISTINCT fee_token) FILTER (WHERE side = 'BUY') AS base_fee_tokens,
             list(DISTINCT fee_token) FILTER (WHERE side = 'ASK') AS quote_fee_tokens
      FROM fills
      WHERE crossed AND market_type = 'spot'
      GROUP BY coin) t
LEFT JOIN hl_spot_pairs_current p USING (coin)
WHERE p.coin IS NULL
   OR (len(t.base_fee_tokens) = 1 AND t.base_fee_tokens[1] <> p.base_symbol)
   OR (len(t.quote_fee_tokens) = 1 AND t.quote_fee_tokens[1] <> p.quote_symbol);
-- X2 HIP-4 settlements: the lake's settle fraction (the side-0 settlement price) is /info's
SELECT s.outcome_id, s.settle_fraction AS lake_fraction, o.settle_fraction
FROM outcome_settlements_v s
LEFT JOIN hl_outcomes_current o USING (outcome_id)
WHERE o.settle_fraction IS DISTINCT FROM s.settle_fraction;
-- X3 perp dexes: the coins of each funding event's dex_index carry that dex's name
SELECT r.dex_index, r.lake_names, d.name
FROM (SELECT dex_index, list(DISTINCT dex) AS lake_names FROM funding_rates GROUP BY dex_index) r
LEFT JOIN hl_perp_dexs_current d USING (dex_index)
WHERE len(r.lake_names) <> 1 OR r.lake_names[1] IS DISTINCT FROM d.name;
-- X4 spot tokens: the HyperEVM system address 0x20…<token index in hex> moves a token between Core and EVM
SELECT a.token, a.lake_index, t.token_index
FROM (SELECT token, min(CAST('0x' || substr(address, 35) AS BIGINT)) AS lake_index
      FROM (SELECT token, "user" AS address FROM events
            WHERE ledger_type IN ('spot_transfer', 'send') AND "user" LIKE '0x20000000000000000000000000000000000%'
            UNION ALL
            SELECT token, destination FROM events
            WHERE ledger_type IN ('spot_transfer', 'send') AND destination LIKE '0x20000000000000000000000000000000000%')
      GROUP BY token) a
LEFT JOIN hl_spot_tokens_current t ON t.name = a.token AND t.token_index = a.lake_index
WHERE t.token_index IS NULL;
```

## Cadence, rate limits and backfills

HyperLiquid allows 1200 weight per minute per IP across all `/info` callers;
most requests weigh 20 and `fundingHistory` adds one per 20 items. Each run
keeps its own sliding-window budget (`HLREF_WEIGHT_PER_MINUTE`) below that,
and retries with exponential backoff plus up to a second of jitter: HTTP 429,
timeouts, connection errors and unreadable answers get 6 attempts (waits of
2, 4, 8, 16 and 32 s), HTTP 500, 502, 503 and 504 get 3 (2 and 4 s). After a
429 every request of the run waits. Any other status, a redirect included, is
refused at once.

Three jobs, each the only writer of its tables, typically scheduled as in
[Scheduling](#scheduling):

| Job | Tables | `/info` requests | Example budget |
|---|---|---|---|
| `outcomes` (every 5 min) | `hl_outcome_meta`, `hl_outcomes`, `hl_questions`, `_hlref/outcome_probes` | `outcomeMeta`, then up to `HLREF_MAX_PROBES` `settledOutcome` | 400/min, 200 s |
| `meta` (hourly) | `hl_spot_meta`, `hl_spot_tokens`, `hl_spot_token_details`, `hl_spot_pairs`, `hl_perp_dexs`, `hl_perp_assets` | `spotMeta`, `perpDexs`, `allPerpMetas`, `tokenDetails` per new token and per token due again | 200/min, 45 min |
| `funding` (hourly) | `hl_funding_history` | `allPerpMetas`, `fundingHistory` per coin (at most 500 hours a page) | 400/min, 45 min |

A run that reaches its time budget commits what it fetched and exits 0; the
next run continues from the tables themselves, so every backfill is resumable
and needs no separate job. The snapshot requests (`outcomeMeta`, `spotMeta`,
`perpDexs`, `allPerpMetas`) still start once the budget is over, so a
`HLREF_TIME_BUDGET_SECONDS=0` run writes the snapshots and stops before its
first `settledOutcome`, `tokenDetails` or `fundingHistory` request.

- **HIP-4 registry.** Outcome ids are dense from 0. Each run probes, newest
  first: outcomes that just left `outcomeMeta`, then known outcomes not yet
  settled, then every id up to three past the highest known one that has no
  row. The last group is both the backfill (about 10,000 ids, 60 a run: about
  14 h) and how outcomes created and settled between two snapshots are found.
  A settled outcome is final and never probed again; an id that is not
  settled (or that `/info` fails for) is probed again after an hour
  (`--reprobe-after-minutes`). `_hlref/outcome_probes` records every probe.
- **Funding.** Each coin resumes after its latest stored hour; a coin with
  none starts at 2026-01-01 (`--funding-start`). Coins with the oldest data go
  first. Delisted coins are read once a day, each in its own UTC hour (the
  CRC-32 of the coin modulo 24), which also spreads their first backfill over
  a day. `--coins` reads the coins it names at any hour.
- **Token details.** A token's first row waits until its `tokenDetails` was
  asked (3 attempts), so the first `meta` runs ask it for every listed token
  and defer the tokens their budget does not reach (`tokens_deferred`).
  `/info` answers HTTP 500 for some tokens every time. A token without details
  is asked again once a day, in its own UTC hour (the CRC-32 of its token id
  modulo 24), with a single attempt for HTTP 500 (a gateway's 502, 503 or 504
  gets the usual three); an answer then appends its `hl_spot_token_details`
  row.

A request can outlast the time budget by at most its 300 s total timeout
(30 s to connect, 30 s between reads), so keep `HLREF_TIME_BUDGET_SECONDS` +
300 below the run's deadline.

## When a run fails

A run exits 1, and commits the rows it completed first, when:

- **`/info` refuses or keeps failing a request** (`<request> refused with
  HTTP <n>`, `<request> failed after <n> attempts (<reason>)`, or `<request>
  failed (<reason>) and the time budget ended`), an answer exceeds 64 MiB or
  would take more than 256 MiB parsed (`<request> response exceeded
  268435456 bytes parsed`; `/info`'s small values take 10 to 20 times their
  text, and the bound keeps the job inside its 1 GiB), or a snapshot is
  malformed: `spotMeta` with no tokens or pairs, `allPerpMetas`
  with no asset on the default dex, `outcomeMeta has entries without a valid
  id; they were skipped`. Messages name the request (`settledOutcome(outcome=9975)`),
  never the URL.
- **`<type>: N of M requests failed with HTTP 500: /info is failing this
  request type, not single entities`.** A final HTTP 500 for one outcome id,
  coin or new token is that entity's gap (a `warning`; it is asked again
  later), because `/info` answers 500 for some entities every time. When at
  least half of a request type's requests in a run fail that way, and at
  least three, the run fails: a run's one new token whose details fail stays
  a gap. A run that names its entities (`--coins`, `--outcome-ids`) fails on
  one, since the scheduled runs never refill a repair's gap behind a coin's
  cursor.
- **`<table>: the snapshot lacks N of the M listed entities`.** A snapshot that
  would delist more than the larger of 25 entities and 20% of a table's listed
  ones (an HTTP 200 with an empty or cut list) writes no removal row, since
  `delta.appendOnly` makes every row permanent. If `/info` really dropped them,
  record it with a run that passes `--allow-mass-removal`.
- **`fundingHistory: N listed coins are more than 2 hours behind after a
  complete run`.** A run that asked every due coin within its budget left a
  listed coin two hours behind: `/info` stopped funding it, or keeps failing
  it. A run that ends its budget (the backfill) or names `--coins` does not
  check this. `fundingHistory <coin> returned an entry outside the request`
  fails the run too.
- **A commit fails**: `<table>: commit after version V failed (a concurrent
  commit)` when another commit landed first, or `... failed (<kind>: <detail>);
  it may have landed` for any other error ([Concurrent runs](#concurrent-runs)).
  The commits that depend on it are skipped: `outcomes` commits
  `hl_outcome_meta`, `hl_questions`, `hl_outcomes` and the probes in that
  order, each only after the previous one, and `meta` commits `hl_spot_tokens`
  only after `hl_spot_token_details`.
- **A checkpoint, log cleanup, OPTIMIZE or VACUUM fails** (`<table>:
  checkpoint at version V failed`, `<table>: log cleanup at version V failed`,
  `<table>: OPTIMIZE failed`, `<table>: VACUUM failed`). The rows committed
  before it count.
- **A data file of the table's snapshot is gone** (`an active data file is
  gone (a VACUUM after version V); the next run retries`).
- **`verify` finds a violation** ([Operations](#operations)).

Exit status 2 is a configuration error, reported as a single `config_error`
line before any request: a missing root or credentials, an unsafe or stale S3
setting, a bad `HL_INFO_URL`, a weight below 20, a negative time budget, an
unknown `--tables` name; or a clap usage error on stderr (an unknown option, a
value that is not a number, a negative `--max-probes` or
`--reprobe-after-minutes`, a malformed date). A table whose schema this
release cannot extend is a `config_error` after `start`, with no `done` line.

## Coverage and point-in-time limits

- Identity fields never change once created: pair → tokens, token symbol and
  index, outcome specification and settlement, dex index and name. Their rows
  are correct for any time.
- Fields that change (leverage, fee scales, question lists, `isDelisted`,
  outcome venues' deployers) are known from the job's first run only: nothing
  collects them before. A dex's OI caps and funding parameters are hourly
  samples.
- `hl_funding_history` starts at 2026-01-01, the lake's data origin.
- Questions that settled before the first run are known only through their
  outcomes' `settledOutcome` responses: name, description and `settled`, no
  outcome lists.
- `tokenDetails` is the response at the time it was first answered (supply,
  prices); it is never refreshed. Tokens `/info` keeps failing have no
  `hl_spot_token_details` row.
- Not collected: live-only snapshots (`metaAndAssetCtxs`: mark, oracle and mid
  prices, open interest), predicted fundings, order books.

## The command

```
fireparq [--env-file F] hypercore-reference [OPTIONS] <outcomes|meta|funding|maintain|verify> [JOB OPTIONS]
```

Options come before the job and job options after it. `fireparq
hypercore-reference --help` prints them all.

| Variable | Flag | Default | Meaning |
|---|---|---|---|
| `HLREF_ROOT` | `--root` | (required) | `s3://<bucket>[/<prefix>]` or a local directory, the tables' own root. A local root is made absolute, with `.` and `..` collapsed |
| `HL_INFO_URL` | `--info-url` | `https://api.hyperliquid.xyz/info` | The `/info` endpoint, `https://`; plain `http://` only for a loopback host, which is asked directly, never through `HTTP_PROXY` or `ALL_PROXY`. A URL with an embedded key belongs in a secret: only its scheme and host are ever shown |
| `HLREF_WEIGHT_PER_MINUTE` | `--weight-per-minute` | `400` | This process's `/info` weight per minute, at least 20 |
| `HLREF_TIME_BUDGET_SECONDS` | `--time-budget-seconds` | `240` | Seconds after which requests stop; the run commits and exits 0 |
| | `--no-maintenance` | off | Skip the compaction and VACUUM after the run's commits |
| `HLREF_MAX_PROBES` | `outcomes --max-probes` | `80` | The sweep's `settledOutcome` probes per run (named ids are all probed) |
| `AWS_ENDPOINT_URL_S3` | `--aws-endpoint-url` | AWS | The S3 endpoint for an `s3://` root, `https://` only (the variable the writer reads) |
| `AWS_REGION` | `--aws-region` | `us-east-1` | |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `--aws-access-key-id`, `--aws-secret-access-key` | | Both required for an `s3://` root, else exit 2 before any request (no instance-metadata fallback). Never printed |
| `AWS_SESSION_TOKEN` | `--aws-session-token` | | Sent when set. Never printed |

An empty value is unset, as for an unset variable: an empty `HL_INFO_URL` is
the public endpoint, and an empty key is a missing key. `S3_ENDPOINT` and
`AWS_ENDPOINT_URL` are not read: a non-empty one that differs from
`AWS_ENDPOINT_URL_S3` is refused (exit 2), so a stale setting never sends the
key to another endpoint. `AWS_S3_ALLOW_UNSAFE_RENAME=1` or `true` is refused:
every commit is a conditional put (`If-None-Match: *`).

Job options:

| Job | Options |
|---|---|
| `outcomes` | `--max-probes N`, `--reprobe-after-minutes N` (60), `--sweep-from ID` (0), `--sweep-to ID` (a bounded slice), `--outcome-ids 1,2,3` (probe exactly these; no sweep), `--allow-mass-removal` |
| `meta` | `--allow-mass-removal` |
| `funding` | `--funding-start TIME` (2026-01-01), `--funding-end TIME` (repair `[start, end)`), `--coins BTC,xyz:XYZ100`, `--commit-rows N` (50000 rows per commit) |
| `maintain` | `--tables hl_outcomes,_hlref/outcome_probes` (default every table): OPTIMIZE and VACUUM now |
| `verify` | none: read-only invariants |

`TIME` is `YYYY-MM-DD` (00:00 UTC), or a date, `T` or a space, and
`HH[:MM[:SS[.fraction]]]` with an optional `Z`, `±HH`, `±HHMM` or `±HH:MM`;
without an offset it is UTC.

### Differences from the Python job

| Python job | `fireparq hypercore-reference` |
|---|---|
| image `ghcr.io/pinax-network/hypercore-reference`, `args: ["outcomes"]` | the writer's image, `args: ["hypercore-reference", "outcomes"]` |
| `S3_ENDPOINT` (then `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`), `AWS_ALLOW_HTTP` | `AWS_ENDPOINT_URL_S3` only, `https://` only; a differing `S3_ENDPOINT` or `AWS_ENDPOINT_URL` is refused |
| `views` printed the views | the views are the SQL [above](#views) |
| a non-numeric `HLREF_*` value crashed (exit 1) | a usage error (exit 2) |
| a negative `--max-probes` (`HLREF_MAX_PROBES`) probed nothing and exited 0; a negative `--reprobe-after-minutes` turned the cool-down off | a usage error (exit 2) |
| no root: a usage line on stderr | a `config_error` line (exit 2 both) |
| any float time budget | finite and 0 or more, else exit 2 |
| `--outcome-ids ""` swept; `maintain --tables` skipped unknown names | both refused (exit 2) |
| `--funding-start/-end`: any `fromisoformat` form; a naive time in local time | the forms of [The command](#the-command); a naive time is UTC |
| plain `http://` for any `HL_INFO_URL`, a loopback one through `HTTP_PROXY` too; 301, 302 and 303 followed | plain `http://` only for a loopback host, never through a proxy; every redirect refused |
| an entity request could start after the budget ended | `settledOutcome`, `tokenDetails` and `fundingHistory` never start after it; the snapshot requests keep the old rule |
| `start.info` showed IPv6 hosts unbracketed and IDN hosts in Unicode | bracketed and punycode |
| a funding entry outside years 1-9999 failed with `OverflowError` | `fundingHistory <coin> returned an entry outside the request` |
| any answer up to 64 MiB was parsed, however much memory its values took | an answer that would take more than 256 MiB parsed fails its request, without a retry |
| one entity's HTTP 500, with no other request of its type answered, failed the run | an outage needs at least three such failures, and half of the type's requests; one in a run that names its entities |
| the daily `tokenDetails` retry got one attempt for any 5xx | one for HTTP 500; 502, 503 and 504 get the usual three |
| a coin named twice in `--coins` was read twice, and a repair could append its hours twice | each coin once |
| a checkpoint or log cleanup failure after a commit failed the commit | the commit counts; the checkpoint or cleanup fails the run on its own |
| `delta.checkpointInterval` hook and `table.vacuum` | the same checkpoint versions (19, 39, …) and VACUUM, committed by the command itself ([Compaction and VACUUM](#compaction-and-vacuum)) |
| JSON lines in insertion order, `"0.1.0"` as `version` | keys sorted, compact; `version` is fireparq's, and `start` adds `deltalake` |
| `commitInfo` keys `hlref.version`, `hlref.run_id`, `hlref.mode` and `hlref.action` | `fireparq.reference.version`, `.run_id`, `.mode` and `.action` |

## Output

Each run prints one JSON object per line on stdout, keys sorted, `start` first
and `done` last (except after a `config_error`), so `kubectl logs job/<job> |
tail -1` shows the outcome. stderr carries only usage errors and the env-file
notice.

| `event` | Fields |
|---|---|
| `start` | `version`, `run_id` (`<job>-<UTC %Y%m%dT%H%M%SZ>-<6 hex>`), `mode` (the job), `root` (normalized), `info` (`<scheme>://<host>` only), `weight_per_minute`, `time_budget_seconds`, `deltalake` (delta-rs's version) |
| `done` | `run_id`, `mode`, `failed` (the failure messages; `[]` on success), `summary` (below), `requests` (HTTP attempts, retries included), `retries`, `weight` (total weight charged), `seconds` |
| `config_error` | `message` (exit 2) |
| `retry` | `request` (for example `settledOutcome(outcome=9975)`), `attempt`, `reason` (`HTTP 503`, `timeout`, `connect`, `body`, `request`, `invalid JSON`), `delay_seconds` |
| `error` | `message`: each failure as it is recorded |
| `warning` | `message`, plus `outcome_id`, `token_index` or `coin` for an entity's HTTP 500 gap |
| `progress` | outcomes: `probed`, `settled`, `outcome_id` (every 50 probes). meta: `token_details`, `token_index` (every 100). funding: `funding_requests`, `coins_complete`, `rows_pending`, `coin` (every 100 requests) |
| `created`, `columns_added` | `table`, `version` (and `columns`) |
| `appended` | `table`, `rows`, `version`: one per commit |
| `maintained` | `table`, `files_before`, `files_removed`, `files_added` after an OPTIMIZE, `vacuum_deleted` after a VACUUM, `version` |
| `skipped` | `table`, `reason` (`no table yet`): `maintain` and `verify` |
| `verified` | per table `table`, `version`, `rows`, `entities`, `bad_hash`, `duplicates`; then `check` (`outcome questions`), `questions`, `missing` |

`summary` holds the rows appended per table (0 included) and, per job:
outcomes `live_outcomes`, `listed_questions`, `probed`, `probed_settled`,
`probe_budget_exhausted`; meta `tokens`, `pairs`, `dexs`, `assets`,
`tokens_deferred`, `token_details_missing`, `token_details_fetched`,
`token_details_retried`, `token_details_retry_failed`; funding `coins`,
`coins_complete`, `coins_skipped`, `funding_requests`, `coins_failed`,
`coins_incomplete`, `coins_stale`, `coins_stale_sample` (at most 5 names),
`funding_budget_exhausted`.

Every commit carries the metadata `fireparq.reference.version`,
`fireparq.reference.run_id`, `fireparq.reference.mode` and
`fireparq.reference.action` (`CREATE`, `ADD COLUMNS`, `APPEND`, `OPTIMIZE`,
`VACUUM`) in its `commitInfo`. Nothing reads them back. There are no
Prometheus metrics: alert on the CronJobs' success (for example
`kube_cronjob_status_last_successful_time`), since a budget-limited run exits
0 and any recorded failure exits 1 or 2.

## Concurrent runs

Each run reads every table's version `V`, computes its rows against `V`, and
commits `V+1` once: a `PUT` of `<table>/_delta_log/<V+1>.json` with
`If-None-Match: *`, never retried and never rebased. Any other commit after
`V` (another run of the same job, a manual repair) makes the commit fail,
either before the `PUT` (delta-rs sees a newer version) or at it (the
conditional `PUT` is refused), and the run exits 1. Nothing is appended twice:
the next run diffs against whatever version is there. A commit whose answer
was lost is reported as `it may have landed`; the next run's diff then
appends nothing for the rows that did. The run's handle on a table moves only
to its own commit's version, so a later commit in the same run never lands on
top of a version it did not read. Tables commit one by one, and every table is
consistent at every version.

On S3, data files go to a client with three bounded retries (their names are
unique, so a resend cannot duplicate a row); log commits go out once. On a
local root a commit is a no-clobber link of the log entry. Two runs of one job
at once are safe but wasteful: one of them fails. So are two OPTIMIZEs of a
table, such as a `maintain` beside a scheduled run: delta-rs's own conflict
checker would let both commit and duplicate the rows they rewrite
(firehose-parquet#717), but the second one's single attempt at `V+1` is
refused.

## Compaction and VACUUM

The command maintains its own tables after a run without failures (not with
`--no-maintenance`), table by table:

1. **OPTIMIZE** once a table has 24 active files (`maintain` always): files
   below 128 MiB, in commit order, are concatenated into files of up to
   128 MiB (Zstandard level 3, 65,536-row row groups) and committed as one
   `OPTIMIZE` with `dataChange: false`, which `delta.appendOnly` allows. The
   row and NULL counts of the new files must equal their inputs', or nothing
   is committed. No DataFusion is involved: delta-rs 1.0.0's own OPTIMIZE needs
   it.
2. **VACUUM** in the 06:00 UTC hour, at most once per 20 h (`maintain`
   always): a full VACUUM with delta-rs's enforced 168 h retention. It deletes
   every file under the table that the current version does not reference,
   unless it was replaced less than 7 days ago or is younger than that
   (another run's write in progress). A lite VACUUM would miss most replaced
   files: it finds them only through their tombstones, which expire after the
   same 7 days and which every checkpoint then drops. Only runs in that hour
   read the table history.
3. **Checkpoints** at versions 19, 39, …, every 20th (`delta.checkpointInterval`),
   and after every commit, as delta-rs does, log cleanup: the entries older
   than 7 days that a checkpoint covers are deleted.

Never point `fireparq maintenance` at the `hl_*` tables: it compacts `date`
partitions in a writer's block order and refuses an unpartitioned table
([Delta maintenance](delta-maintenance.md)).

## The S3 user

Give the job its own S3 user with access to its bucket only, limited to what
it sends (`<T>` is one of the ten `hl_*` tables or `_hlref/outcome_probes`):

| Operation | Requests |
|---|---|
| open a table | `LIST prefix=<T>/_delta_log/`, `GET <T>/_delta_log/_last_checkpoint` (404 when there is none: the user must be allowed to learn that it is missing), `GET` of log entries and checkpoint parts |
| read rows | ranged `GET <T>/part-*.parquet` |
| append, OPTIMIZE output | `PUT <T>/part-*.parquet`, or above 5 MiB a multipart upload (`POST ?uploads`, `PUT ?partNumber&uploadId`, `POST ?uploadId`, `DELETE ?uploadId` on failure) |
| commit | `PUT <T>/_delta_log/<v>.json` with `If-None-Match: *` |
| checkpoint | `PUT <T>/_delta_log/<v>.checkpoint.parquet`, `PUT <T>/_delta_log/_last_checkpoint` |
| log cleanup, VACUUM | `LIST prefix=<T>/`, `POST /?delete` (DeleteObjects) of keys under `<T>/` |

It never lists the bucket root, never sends `?location`, `?acl`, `?policy`,
`?versioning` or `?tagging`, never sends an ACL, tagging or object-lock
header, and never creates a bucket.
`blocks/tests/hypercore_reference_s3.rs` checks that envelope on a loopback S3
endpoint. What only the target store can prove: that a missing
`_last_checkpoint` is 404 rather than 403 under its policy, that it refuses a
second conditional `PUT` with 412, and multipart uploads. Qualify those once
against the real endpoint before the first run. A multipart upload whose
completion fails can leave parts the user cannot list or abort; let the
bucket owner abort incomplete uploads with a lifecycle rule. Never version the
bucket: a store that evaluates `If-None-Match: *` against a new version accepts
both writers of a race.

A new `hl_*` table in a later release needs that user granted the new prefix
before the release runs: until then its first write is refused and the run
fails.

## Scheduling

[`deploy/examples/hypercore-reference-cronjob.yaml`](../deploy/examples/hypercore-reference-cronjob.yaml)
has the three CronJobs with placeholders: `concurrencyPolicy: Forbid`,
`backoffLimit: 0` (the next run is the retry), a read-only root filesystem,
`/tmp` as an `emptyDir`, UID 65534, a CPU limit (Tokio and delta-rs size
their worker pools by the CPUs the process sees: without one, the node's), and
each job's time budget plus 300 s below its `activeDeadlineSeconds`. Even once
caught up, `funding` spends its 400 weight a minute for about 18 minutes of
every hour (about 340 listed coins at 21 each), beside the `outcomes` runs;
`meta` runs at :47, after it. All three run at once, at most 1000 a minute,
only while backfilling. Confirm that nothing else behind the same egress
address calls `/info`, or lower the budgets.

## Operations

- **A failed run** changes nothing the next run cannot finish: it commits the
  rows it completed, and the next run diffs against the tables again.
- **Run a job now, or with arguments** (a repair, a backfill slice,
  `--allow-mass-removal`): suspend its CronJob first, then run a Job with the
  arguments, for example `args: ["hypercore-reference", "funding", "--coins",
  "BTC,xyz:XYZ100", "--funding-start", "2026-03-01", "--funding-end",
  "2026-03-08"]`. While the other jobs are backfilling, lower its budget so the
  IP stays under 1200 weight a minute.
- **`funding --funding-end`** re-reads `[--funding-start, --funding-end)` and
  appends only the hours missing at or before each coin's latest stored hour
  (never ahead of it, which would leave a gap the incremental runs skip).
  `outcomes --outcome-ids 1,2,3` probes exactly those ids now;
  `outcomes --sweep-from/--sweep-to` bounds the sweep. Rows are still appended
  only when a payload changed.
- **Check the tables:** `verify` (read-only) recomputes every payload hash,
  looks for an entity with two consecutive identical rows or a duplicated
  funding hour, and checks that every question an outcome names has a row; it
  exits 1 if it finds one.
- **Compact or vacuum now:** suspend the CronJobs, then run `maintain`
  (`--tables` to choose).
- **Backfill progress:** `probed` and `_hlref/outcome_probes` for outcomes,
  `coins_stale` for funding, `tokens_deferred` and `token_details_missing`
  for token details.

## Tests

- `reference/` (`cargo test -p fireparq-reference`): the canonical JSON,
  values, row builders and Delta schemas against `golden-vectors.json` (the
  Python job's own output on the recorded fixtures,
  `reference/tests/fixtures/README.md`), the `/info` client, the store
  (commits, races, checkpoints, OPTIMIZE, VACUUM) and the jobs over the
  fixtures, with a fake clock; `reference/tests/json_memory.rs` measures that
  the parse bound covers what parsing allocates.
- `blocks/tests/hypercore_reference_cli.rs`: the real binary over a loopback
  `/info`: exit statuses, configuration errors before any request, the URL and
  credentials never printed, the loopback `/info` never asked through a proxy,
  and the [views](#views) in DuckDB over the tables it wrote.
- `blocks/tests/hypercore_reference_s3.rs`: the real binary on a loopback HTTPS
  S3 endpoint: a lost conditional commit, a lost commit answer, an unknown
  outcome, a retried data write, and the requests' envelope, including a
  checkpoint and the bulk deletes of a log cleanup and a VACUUM.
- `blocks/tests/engine_compat.rs`: [Joining the lake](#joining-the-lake) and
  the [cross-checks](#cross-checks-against-the-lake) over a HyperCore fixture
  lake.
