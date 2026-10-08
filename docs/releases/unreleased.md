# firehose-parquet: unreleased

Changes merged since [v1.1.2](v1.1.2.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

- **SEC EDGAR filings: `--block-type sec`.** fireparq now maps
  `pinax.sec.v1.Block` (firesec ≥ 0.13.0: one block per 10-minute window of the
  EDGAR daily feed, 144 per feed day, empty windows included) into 43 Delta
  tables: the `filings` hub, the SGML header's parties, documents, fund series,
  share classes, signatures and raw XML, one table set per form family (Forms
  3/4/5, 13F, 13D/G, 144, N-PORT, Form D, N-PX, N-CEN, Form C), and
  `parse_issues`. Every protobuf field lands in a column; dates are `date`,
  amounts exact `decimal(38,s)` in five scale families, and every source value
  that a typed column does not reproduce exactly has a `parse_issues` row with
  its verbatim text. `block_id` and `parent_id` are decimal window numbers as
  text, and files record `firehose-parquet.block_id_encoding = decimal`.
  Heuristics (13F value units, accession dedup, effective 13F and N-PORT
  reports, N-PX vote normalization) are SQL views shipped in
  [docs/chains/sec.md](../chains/sec.md) (`sec-views-v1`), not mapper output,
  so improving one needs no rebuild; `blocks/tests/sec_docs_sql.rs` runs them,
  and `blocks/tests/sec_golden.rs` checks every column of every table on real
  0.13.0 filings against an independent reference prototype. Columns:
  [SEC schema](../schemas/sec.md). Existing chains' tables and schemas are
  unchanged; only the cross-family test digests were re-pinned.
- **`--flush-idle-secs` / `FLUSH_IDLE_SECS`: flush when the stream goes
  quiet.** The size, row, block and interval triggers are checked only when a
  block arrives, so rows mapped before a long silence waited, uncommitted and
  invisible to readers, for the next block. With `--flush-idle-secs N`, `build`
  commits them once the stream has delivered no message for `N` seconds, at
  any pace (`trigger="idle"`). Off by default; existing deployments are
  unchanged ([CLI](../cli.md#flush-when-the-stream-goes-quiet)).
- **Block-family `build` defaults.** `--block-type sec` defaults to
  `--grpc-max-message-bytes 536870912` (a 13F deadline-day window is 143.9 MB,
  above the generic 128 MiB, which stopped the stream at that window on every
  restart), `--flush-idle-secs 60`, `--stream-idle-timeout-secs 93600` and
  `--metrics-stale-after-secs 129600`, because firesec sends one burst per
  EDGAR feed day. Each applies only when neither the flag nor its environment
  variable is set, and the applied values are logged at startup. Other
  families keep the generic defaults ([CLI](../cli.md#family-defaults),
  [SEC notes](../chains/sec.md)).
- **HyperCore (`--block-type hypercore`)**: the HyperLiquid L1 from Pinax's
  `pinax.hypercore.v1` Firehose (`hypercore.firehose.pinax.network:443`), in
  twelve tables organised by product family
  ([schema](../schemas/hypercore.md), [notes](../chains/hypercore.md))
  (#709):
  - the raw record: `blocks`, `fills` (every leg of every market),
    `funding_deltas`, `validator_rewards`, and the events split by product
    family into `transfers`, `bridge_transfers`, `vault_events`,
    `staking_events` and the catch-all `other_events` (one row per event, its
    body and ledger delta flattened, the columns of one shared catalogue;
    their union is the block's event list);
  - derived from the same block by fact-only rules (R-D1–R-D6):
    `fills.market_type` (`perp`, `spot`, `outcome`), `fills.dex` (`''` for
    the default perp dex, else the HIP-3 dex) and `fills.counterparty` (the
    other leg of the match), and the tables `outcome_fills` (HIP-4 legs with
    `outcome_id` and `side_index`), `liquidations` (the liquidated leg with
    its counterparty and method) and `funding_rates` (the hourly rate, open
    interest, position counts and funding flows per coin). A rule never
    refuses a block: a shape it does not recognise gives NULL.
  - Every table's schema carries `fireparq.hypercore.derivation = "1"`, part
    of its declared digest: a change to a derivation rule needs a new root,
    and a root refuses to resume under other rules. Compacted files do not
    carry the key; `validate` and resume do not need it.
  - Amounts, prices and sizes are exact `decimal(38,10)` values, parsed
    without rounding; a string that is not exact refuses the block.
  - `block_id` and `parent_id` are the decimal block number as text
    (HyperCore has no block hash); files record
    `firehose-parquet.block_id_encoding = decimal`.
  - A block is validated completely before any row is appended, and a block
    with fields the vendored protos do not know, an unknown enum value or an
    empty required value is refused with its block, protobuf path and value
    (rules R1–R11). Such a stop needs a release with refreshed protos;
    `extra_json`, NULL in this version, lets most additions ship without a
    new output root, and a new event label is routed by the release that
    vendors it (its tests fail until the label's table is pinned and
    documented). A value the mapper stages for an event table without that
    column stops the block before any append instead of being dropped.
  - `--network hypercore` streams it with the ambient `PINAX_API_KEY` (or
    `SUBSTREAMS_API_KEY`), like any Pinax alias; see the internal Pinax
    networks below.
  - HyperCore data is known from 2026-01-01: a new root starts at block
    846903317 (2026-01-01T00:00:00.063Z) by default, and an explicit
    `--start-block` before it is refused. The endpoint advertises
    846000000 but lacks blocks 846903300–846903312, which a stream cannot
    cross; see the data origins below.
  - `docs/chains/hypercore.md` adds a DuckDB view pack (the `events` union,
    HIP-4 matches, settlements and positions, liquidation kinds with their
    ledger events, funding and open-interest views, normalised transfers,
    0xArchive-shaped trades, a view per ledger type and body), 23
    data-quality monitors (M1–M23) and a cookbook, all of which
    `engine_compat` runs over a build of 36 real fixture blocks, and a
    "where to start" map from questions to tables, views and queries. It also
    documents optional joins against the `hl_*` tables of a separate metadata
    job (run in `engine_compat` against empty stand-ins).
  - The maintenance job needs all twelve tables in `LAKE_TABLES`
    (`deploy/examples/delta-maintenance-cronjob.yaml`).
  - `user`, `destination`, `vault`, `validator`, `liquidated_user` and
    `sub_account` columns get Parquet Bloom filters; no other family has
    columns of those names.
  - New roots only: the family is part of the protected stream identity, so
    existing roots are unaffected and an older binary refuses a HyperCore
    root. `MAPPER_EPOCH` is unchanged.
- **Built-in aliases for Pinax networks the registry does not list yet**:
  `PINAX_NETWORKS` in `scripts/generate_networks.rs` is a reviewed list of
  Pinax-served Firehose networks outside The Graph networks registry, appended
  to the registry's aliases, starting with `hypercore` →
  `hypercore.firehose.pinax.network:443`. Their hosts are built-in Pinax hosts
  for credential selection, `FIREHOSE_ENDPOINT_*` overrides them, and the
  endpoint check covers them. Once the registry gives such a network an
  alias, the registry entry wins and the generator warns until the entry is
  dropped; it also warns when the registry lists the name without an accepted
  endpoint, or gives another name the same endpoint host. The registry
  snapshot is unchanged (v0.8.4)
  ([network registry integration](../network-registry-integration.md#internal-pinax-networks))
  (#709).
- **Per-network data origin**: `NETWORK_DATA_ORIGINS`
  (`firehose-parquet/src/networks.rs`) records where a network's known data
  starts when its endpoint advertises earlier blocks; HyperCore's is block
  846903317. Matched by the EndpointInfo chain name, a new stream without
  `--start-block` starts there with an info line, and an earlier
  `--start-block` is refused before streaming, in dry runs too. Resuming from
  output authority and other networks are unaffected
  ([start and stop blocks](../cursor-and-resume.md#start-and-stop-blocks))
  (#709).

## Fixes

## Performance

## Internal

- **Text block ids.** `PreparedIdentity::with_text_ids` and
  `CanonicalBuilder::prepare_with_text_ids` (`firehose_parquet::traits`) write
  block and parent ids that are already text verbatim (as their UTF-8 bytes
  under `Binary`); the SEC mapper uses them for its decimal window numbers and
  the HyperCore mapper for its decimal block numbers, and
  `ChainProfile::block_id_text` marks both families. SEC roots written by
  development builds of main between #710 and the merge of #709 record
  `firehose-parquet.block_id_encoding = hex_0x` in their earlier files (no key
  under `--bytes-encoding binary`); the key is not part of the protected
  identity, so they resume, and later files record `decimal`.
- **Schema digests guard the earlier families** (#711): with SEC and HyperCore
  both added, `CURRENT_SCHEMA_DIGEST` and `DELTA_DATA_SCHEMA_DIGEST` cover ten
  families, and `pre_sec_families_reproduce_their_pre_sec_digests` checks that
  the eight earlier families still reproduce, byte for byte, the digests pinned
  before either was added, so a re-pin for a new family cannot hide a change
  to an existing one. `each_later_family_reproduces_its_own_pinned_digests`
  pins SEC's and HyperCore's own mapper and Delta data schema digests over
  every option and encoding, so the same holds for them. The historical
  final-only and pre-#550 digests leave out both new families and are
  unchanged.
- **`engine_compat.rs` reads a SEC dataset**, with `decimal(38,s)`,
  non-partition `date`, `array<struct<…>>` and `binary` columns, in DuckDB and
  delta-rs, and the engine compatibility table of
  [reading tables](../reading-tables.md) now lists those types (checked by
  hand with Polars 1.44.2); a test keeps the table in step with the types the
  engine test pins. Its DuckDB scan names the file-path column
  `fireparq_data_file`, because SEC's `filing_documents` has a `filename`
  column, which shadows DuckDB's `filename` scan option: the column's
  description and the [SEC notes](../chains/sec.md) say how to read the file
  path there.
- **A local ownership guard unlocks its directories when it is dropped**, before
  closing them (#706). A process spawned while the guard was held keeps a copy
  of each locked descriptor until it execs, and a `flock` lock belongs to the
  open file description that the copy shares. So the lock could outlive the
  guard, and a new acquisition right after failed with "lock acquisition
  failed because the operation would block". In the test suite, where tests
  run child processes beside each other, that made
  `nested_symlinks_fail_before_mutation_but_explicit_root_aliases_work` fail
  now and then. `fireparq` itself starts no child processes, so it wasn't
  affected. `a_dropped_guard_releases_its_locks_while_inherited_descriptors_live`
  holds a copy of the descriptors and acquires again.
