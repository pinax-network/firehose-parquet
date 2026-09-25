# firehose-parquet: unreleased

Changes merged since the last release. Fold this file into `docs/releases/vX.Y.Z.md` when the next release is cut.

## CLI and query semantics

- Firehose receive windows default to 16 MiB per stream/connection. Both `build`
  and `partitions build` expose `--grpc-window-bytes`, `--grpc-adaptive-window`,
  and `--grpc-max-message-bytes` (the receive limit remains 128 MiB). Replies
  may use zstd as well as gzip/plain; request compression is unchanged. Oversized
  compressed responses stop without retrying the same payload. The default was
  selected using a bounded local benchmark, not a live-provider speed claim.
  See [#517 evidence](../audit/517-grpc-transport.md).

- `--final-blocks-only=false` now enables append-only reversible output directly;
  the default and bare flag remain true. Optional values use `=`, and explicit
  CLI values override `FINAL_BLOCKS_ONLY`. Successful bounded non-final runs warn
  that completion does not establish tail finality. The README documents NEW/UNDO
  recurrence, the absence of global event order, and a safe finalized-reference
  intersection query with explicit limits; it does not promise a canonical tail
  from unordered files. See [#474 validation](../audit/474-non-final-streams.md).

## Breaking changes

### Cosmos preserves event order, unknown results and SDK metadata (#510)

Empty events now produce one row; `attribute_index` preserves repeated-key order.
Event transaction indices are UInt32, block event transaction hashes are null,
and absent results produce nullable code/gas/text fields. Unknown results remain
included; use `code = 0` for confirmed source success. Transactions add exact
Binary raw bytes, subset decode status, memo, fee, signer and signature metadata;
blocks count decode failures even for filtered rows. Start a fresh dataset or
explicitly reconcile schemas. Lost source information requires replay. See the
[field contract, migration and RPC-backed comparison](../audit/510-cosmos-values.md).

### All-table ingestion transactions and output authority (#468)

`build` now journals each complete mapper flush and recovers it before opening
Blocks. Output authority under `.fireparq-ingest/` selects the exact accepted
cursor, including filtered zero-row events and persisted timestamp-routing
provenance. Deterministic owned parts are rolled back before replay or verified
and rolled forward after commit. `cursor.parquet` is an optional derived mirror;
its deletion cannot rewind output, and `--cursor none` keeps mandatory authority.

This requires a new empty dataset root and absent mirror. Existing random-name
output or legacy cursors are refused rather than adopted. Protected origin,
mapper/schema/encoding, effective feature flags, partition policy, storage and
mirror binding are immutable; `--cursor-override` cannot bypass them. Use a new
root for changed semantics. Unknown custom chain metadata needs an explicit
`--block-type` before recovery. Flush thresholds and compression remain tunable.

Guarded merge, metadata artifacts, and copy-only rollup remain available; protected
truncate, in-place rollup and source-deleting rollup are refused. Discovery covers
ancestor and descendant dataset roots plus external mirrors. `recovery recover`
performs offline owned recovery. S3 retains its explicit provider-quiescence
release requirement; no time-based takeover or generic request-drain claim is added.

A completed bounded request must have an acknowledged boundary event. A clean
sparse/empty tail alone no longer implies completion on skipped-height chains;
the prefix remains durable, but the command exits nonzero. Already proven bounds
make no Blocks request, and extensions retain their original origin and exact
cursor. Plain-glob readers still do not get atomic multi-table query snapshots.
See [the runtime contract and qualification](../audit/468-ingestion-runtime.md).

### Beacon numeric fees, binary blobs, and null presence (#505)

Beacon `execution_payload.base_fee_per_gas` is now an exact unsigned decimal
Utf8 string in wei per gas; conversion follows the producer's little-endian
Bellatrix/Capella and big-endian Deneb+ representations. `blob_sidecars.blob`
is always Binary, and `blocks.spec` is an Arrow string dictionary using generated
names (`UNKNOWN` is distinct from `UNSPECIFIED`). Missing nested messages now
produce null fields instead of fake zeros/empty bytes/lists; present zeros and
empty values retain their meaning. Other byte fields preserve their requested
encoding. Rebuild into a fresh root or explicitly convert and verify old files
before mixing schemas. Old placeholder zeros require source replay to recover
presence. See [the migration and validation record](../audit/505-beacon-values.md).

### Solana payloads use native bytes and account-index lists (#503)

`instructions.data` and ordinary/vote transaction `err` / `return_data` now use
Binary for every identifier encoding. `instructions.accounts` and account lookup
`writable_indexes` / `readonly_indexes` use non-null lists of non-null UInt8.
Keys, signatures, hashes and return-data program IDs keep their selected encoding.
Null/empty distinctions, list order, duplicate indices and filtering are preserved.

Use a new output root and rebuild, or explicitly convert old payloads and index
arrays into a separate dataset. This also affects existing Binary-mode output,
where index columns were Binary. Mixed-schema append/union does not perform this
conversion, and verification roots change. Default-profile mapping was 72–80%
faster on two retained blocks; this is not an end-to-end throughput or memory
reduction claim. See [the benchmark and 19,714-row comparison](../audit/503-solana-binary-payloads.md).

### Partition indexes require verified finalized coverage (#486)

Time index construction now checks every finalized canonical block and preserves
backward/repeated timestamp runs. Bounded requests stay clipped to their exact
bounds; current-head and clipped time spans remain incomplete. A bounded Stream
proof establishes the finalized anchor before accepting coverage. Sparse Fetch
head inference and future timestamp borrowing no longer determine time spans.
Long backfills should use successive bounded runs; scans now cost work
proportional to covered blocks and publish only validated snapshots.

V2 adds explicit coverage and span proof fields. Legacy indexes remain
inspectable with unknown completeness, but resolution, sharding and resume
require rebuilding. Default resolution refuses incomplete or disjoint matches;
`--all-spans --json` returns separate complete runs and their coverage. Complete
means a natural contiguous span in the observed snapshot, not globally complete
calendar coverage. Range helpers also refuse unseen initial routing context.
Solana uses proven prior anchors; unsupported metadata or missing context fails
closed without changing ingestion routing. See [the file contract](../partitions-parquet-contract.md)
and [design and validation record](../audit/486-partition-index-design.md).

### Mutations require common ownership (#468 prerequisite)

Build, partition-index construction, maintenance, and verification that writes
artifacts now coordinate ownership over their source/output/cursor locations.
Local mutation requires supported macOS/Linux directory inode locking and readable
ancestry. Nested symlinks inside guarded trees are refused; explicit root aliases
remain supported. Missing output roots are protected through their existing
ancestors without being created before input validation. Directory preflight adds
a recursive traversal.

S3 mutation holds a persistent bucket-wide owner, including copy/verification
sources that must remain stable while artifacts are written. It requires proven
conditional Create/Update support, usable object versions, and read/write access
to `.fireparq-owner-v1.json` plus read/write/delete access below
`.fireparq-owner-probes-v1/`. Distinct prefixes in one bucket serialize. Unsupported
conditional stores have no best-effort fallback. Remote data writes/deletes and
cursor saves make one attempt with zero transport retries; errors retain ownership
for explicit provider-quiescent recovery. Read retries and bounded local cursor
retries remain.

`recovery status` reads ownership/control summaries. Remote `recovery release`
requires the exact UUID/generation, stopped-writer evidence, and provider-confirmed
quiescence of every prior request. Process exit or elapsed time alone is
insufficient. Providers without that assurance cannot safely recover a plain-glob
dataset through this command. Legacy S3 merge journals using the old expiring lock
are refused automatically and need separately reviewed migration. Local legacy
merge recovery remains under the common OS guard.

This foundation also provides ownership for the all-table transaction protocol
above. See the
[scope, permissions, recovery procedure and qualification limits](../audit/468-stage1-ownership.md).

### Antelope database-operation transaction keys (#508)

`db_ops` adds non-null `tx_hash: Utf8`, `tx_index: UInt64` (original source trace
index), and `db_op_index: UInt32` (position within that transaction). Join through
canonical block identity; indices are not renumbered after filtering. Old files
need rebuilding or explicit schema reconciliation to use these fields. The action
aliases `transaction_id`, `trace_block_num`, `producer_block_id`, and `block_time`
are deprecated for ordinary joins/routing but remain verbatim source metadata
with no planned removal. Existing text values and row order are unchanged.
See [the validation and migration record](../audit/508-antelope-db-joins.md).

### Metrics names, labels and readiness reflect actual state (#475)

Counter names now emit one `_total` suffix; dashboards using accidental
`_total_total` names must migrate. `files_written_total` drops its unbounded
`partition` label. The cumulative-average rate gauges and always-zero Solana
`backfill_*` gauges are removed; use counter `rate()` expressions instead.
Separate mapper, writer and initial timestamp-bootstrap buffers are exposed.
The cursor gauge starts from the loaded checkpoint without incrementing save
counters. See [the metrics table](../../README.md#available-metrics).

`/ready` returns 503 before the first valid stream message, during reconnects,
after the stream ends and after `--metrics-stale-after-secs` (default 120) without
a valid message. `/health` remains live through reconnects and final file/cursor
commit, then reports stopped when the pipeline returns. New time metrics describe
message freshness and block timestamp age; they do not claim remote head lag.
The Rust `metrics::serve` API now requires the matching `PipelineMetrics` handle
between its registry and port arguments. Custom stream/pipeline integrations
must retain the respective activity guards through their actual lifetimes.

### Solana vote filtering preserves administrative activity (#501)

The vote-only table and `--without-votes` now apply only to conservative legacy,
single-instruction votes with fully decoded recognized payloads. Withdraw,
Authorize, vote-account creation, mixed instructions, unused Vote program keys,
versioned transactions and unknown/malformed payloads keep their ordinary detail
rows. The failed-transaction option still applies independently. Classification
is structural, not a claim of transaction execution validity.

Schemas are unchanged, but affected rows and verification roots change. Rebuild
old ranges separately to recover activity that the previous account-key check
omitted. See [the decision and validation record](../audit/501-solana-vote-classification.md).

### Solana instruction positions are explicit (#502)

`instructions` adds nullable `UInt32` columns `parent_instruction_index` and
`inner_instruction_index`. Both are null for top-level instructions. Inner rows
store their top-level parent's index and their zero-based position in that
parent's upstream inner set. The old `instruction_index`, `inner_index`, and row
order are unchanged. Sort one transaction's rows by
`coalesce(parent_instruction_index, instruction_index), is_inner,
inner_instruction_index` to place each top-level instruction before its recorded
inner calls. A nested call's parent column still names the top-level owner;
`stack_height` remains the optional depth signal.

Older files do not gain positions when read with schema union. Rebuild old ranges
into a separate output root if positions are required, and do not mix old and
new schemas in strict-schema merge/rollup operations. These fields do not resolve
forks, replay duplicates, or whether every listed instruction executed in a
failed transaction. See [the implementation record](../audit/502-solana-instruction-order.md).

### Invalid timestamps and missing streamed identities now fail (#476)

Malformed timestamp seconds or nanos now return errors before canonical rows or
partition files are written. Values outside years -9999 through 9999 no longer
fall back to 1970, saturate, or panic. Valid negative times and nullable Solana
payload times retain their values. Streamed blocks require a metadata object;
servers that omit it must provide one so ingestion can identify and checkpoint
blocks safely. Progress logging no longer propagates formatting failures.

Rust timestamp/date conversion, canonical identity preparation,
`Partition::partition_key` and `ParquetTableWriter::partition_suffix` helpers now
return `Result`; callers must handle or propagate errors. See the
[API migration and validation record](../audit/476-timestamp-validation.md).

### Solana reward indices are scoped to each block (#500)

`rewards.reward_index` now shares one zero-based sequence across emitted
transaction rewards and block rewards, in their existing output order. Indices
no longer depend on the flush window or collide between the two sources. The
UInt32 schema is unchanged, but affected row values and verification roots
change. Rebuild affected historical ranges into a separate root before joining
on `(block_id, reward_index)`. Reversible events can legitimately repeat that key.

See [the diagnosis and regression record](../audit/500-solana-reward-index.md).

### Local Parquet publication requires durable filesystem operations (#578)

Local table parts now become visible at their final `.parquet` name only after
the footer is complete and the file is synced. Final-name publication is atomic
and never overwrites an existing destination. Directory sync failures are fatal,
including sync of output directory ancestors. Filesystems must support atomic
hard links and file/directory sync; directory ancestors must be readable.

Final filenames and S3 writes are unchanged. This is single-file publication,
not a transaction across tables and the cursor: an error after publication can
leave a complete part, and replay may duplicate it. Abrupt termination may leave
hidden `.tmp` files. See [the guarantees and tests](../audit/578-atomic-local-parquet.md).

### Startup requires usable EndpointInfo (#467)

Ingestion now stops before output/cursor resolution if endpoint metadata cannot
be obtained. Info retries transient failures three times with bounded backoff;
authentication errors, unsupported Info and empty chain names fail promptly.
Explicit network/block type/start bounds and cursor override do not bypass this
requirement. Older servers must expose the Info RPC. This prevents transient
failures from silently changing the output prefix and resume checkpoint.

See [the process and validation record](../audit/467-endpoint-info.md).

### Firehose credentials are scoped to the destination provider (#562)

`build` and `partitions build` select credentials from the actual resolved host,
after endpoint overrides. Known Pinax HTTPS hosts use `PINAX_API_KEY` /
`PINAX_API_TOKEN`, with `SUBSTREAMS_API_KEY` / `SUBSTREAMS_API_TOKEN` as legacy
Pinax-only fallbacks. Known StreamingFast HTTPS hosts use
`STREAMINGFAST_API_KEY` / `STREAMINGFAST_API_TOKEN`. Automatic selection requires
port 443. Unknown hosts, other ports, and plaintext endpoints receive no ambient
credentials.

Migration: move StreamingFast credentials out of `SUBSTREAMS_*` into
`STREAMINGFAST_*`. For custom endpoints or custom secret names, explicitly choose
`--api-key-envvar` / `--api-token-envvar` (also configurable through
`API_KEY_ENVVAR` / `API_TOKEN_ENVVAR`). These selectors authorize the chosen
credential for that endpoint; an unset/blank explicit variable omits its header.
Startup logs show host and selected variable names, never secret values.

### Day-of-month partition directories are now `day=DD` instead of `date=DD` (#493)

Time-based partitioning (`--partition date`, `hour`, `minute` and `second`) wrote the day of the month as `date=DD`, while every table also has a canonical `date` column (`Date32`). Hive-partition-aware readers treat the directory key as a column, so the two collided:

- DuckDB (`hive_partitioning` is on by default for such paths) replaced the `date` DATE values with the partition value: `date` read as `BIGINT` day-of-month numbers (`26` instead of `2025-07-26`).
- Polars (`scan_parquet(..., hive_partitioning=True)`) failed with `could not find a 'date/datetime' pattern for '26'`.
- Spark's file-source partition discovery likewise merges a partition column over a data column of the same name. This was not run here.

New output uses `year=YYYY/month=MM/day=DD/...`. With it, DuckDB reads `date` as `DATE` plus a separate `day` partition column, and Polars reads `date` as `Date` plus `day`. The `--partition date` mode keeps its name; only the directory key changes. `year=`, `month=`, `hour=`, `minute=`, `second=` and `block_range=` are unchanged.

Migration:

- Existing `date=DD` trees stay usable. `rollup` accepts `date=` directories and keeps their key, and `truncate -p day=DD` (or `-p date=DD`) matches both `day=DD` and legacy `date=DD` directories.
- A pipeline that resumes into an existing `date=` tree writes new days under `day=`, so the table then holds both layouts. DuckDB's auto-detection turns hive partitioning off for such a mix: no partition columns, and `date` is the data column. Forcing `hive_partitioning = true` fails with `Hive partition mismatch ... key "date" not found`. To get partition columns back, rename the legacy directories: renaming `date=DD` to `day=DD` inside each `month=MM` directory is enough, and the files themselves don't change.
- Queries that used the `date` partition column as a day-of-month number should use `day`. Queries that disabled `hive_partitioning` to keep the `date` column no longer need to.

### Canonical `timestamp` is now `Timestamp(Millisecond, UTC)` on every table (#491)

The canonical `timestamp` column was Arrow `Timestamp(Second, UTC)`. Parquet has no logical type for seconds, so it was written as a plain `INT64`, and DuckDB, Spark, Trino and ClickHouse read it as `BIGINT` unix seconds. Only Arrow-based readers recovered the type from the embedded Arrow schema.

It is now Arrow `Timestamp(Millisecond, UTC)`, written as Parquet `TIMESTAMP(MILLIS, isAdjustedToUTC=true)`. `DESCRIBE` in DuckDB shows `TIMESTAMP WITH TIME ZONE`. Antelope `actions.block_time` uses the same type.

- **Sub-second precision.** The value now keeps the milliseconds of the Firehose block time. Antelope chains (500 ms blocks) show `12:00:00.500` where they used to show `12:00:00`, and so can other chains whose Firehose metadata carries sub-second times. Whole-second chains such as EVM keep the same instant.
- **Unchanged:** `date`, time-based partition directories (`year=`/`month=`/`day=`/`hour=`/`minute=`/`second=`; see the `day=` rename above) and the cursor's `last_timestamp` are still derived from the whole-second block time. Blocks land in the same partitions as before.
- **Unchanged:** Solana `timestamp`/`date` stay null when `block_time` is missing.

Migration:

- Queries that converted the column with `to_timestamp("timestamp")` should use `"timestamp"` directly. Queries that need numbers can use `epoch_ms("timestamp")` (milliseconds) or `epoch("timestamp")` (seconds, fractional) in DuckDB.
- Old and new files have different `timestamp` types. A single DuckDB scan over both fails with a cast error (`BIGINT -> TIMESTAMP WITH TIME ZONE`), with or without `union_by_name`. Query them separately and normalize the old files:

  ```sql
  SELECT * REPLACE (to_timestamp("timestamp") AS "timestamp")
  FROM read_parquet('old/blocks/**/*.parquet')
  UNION ALL BY NAME
  SELECT * FROM read_parquet('new/blocks/**/*.parquet');
  ```

- Do not `merge` or `rollup` old and new files together. Both commands now refuse (#479): they leave a partition with mixed schemas untouched and exit non-zero. Rebuild the old range, or keep it under a separate prefix.
- `fireparq verify` roots over new files can differ from roots over old files wherever block times have milliseconds, because the stored value changed. Recompute registries for rebuilt ranges.

### Tron `transactions`: transaction time columns renamed (#492)

The Tron `transactions` table had two columns named `timestamp`: the canonical block time and the transaction's own creation time. Spark and Polars reject files with duplicate column names, DuckDB exposes the second one as `timestamp_1`, and name-based lookups only find the first one.

The transaction-level time columns now have their own names:

| Before | After | Type |
|---|---|---|
| `expiration` | `expiration_ms` | `Int64`, unix milliseconds |
| `timestamp` (second occurrence; `timestamp_1` in DuckDB) | `tx_timestamp_ms` | `Int64`, unix milliseconds |

The canonical `timestamp` column (block time) is unchanged. The values are unchanged too. `tx_timestamp_ms` is set by the transaction's sender and is not validated on chain; in a sample of 1,548 mainnet transactions, 10 were `0` and one was in nanoseconds. That is why it stays a raw `Int64`.

Migration:

- Queries that read `timestamp_1` or `expiration` from Tron `transactions` should use `tx_timestamp_ms` and `expiration_ms`.
- Files written before and after this change have different `transactions` schemas. Rebuild existing Tron `transactions` output, or keep the old files under a separate prefix. Do not `merge` or `rollup` old and new files together.

### NEAR `state_changes`: canonical `block_id` / `parent_id` now match the other NEAR tables

`state_changes` wrote `block_id` and `parent_id` as `0x`-prefixed hex, while every other NEAR table writes them as base58, so `state_changes` could not be joined to `blocks` on `block_id`. They are now base58 like the rest of the NEAR tables. Existing `state_changes` files keep the old hex values; rebuild them to join across tables.

### `verify`: new `merkle_v2` roots; existing registries must be rebuilt (#487, #490)

`fireparq verify` computes partition roots with a new, versioned algorithm, `merkle_v2`. The version is recorded as `merkle_version` in `merkle_roots.parquet` and in the report (`report_schema_version` `2.0.0`). Every root changes, even for identical data.

What `merkle_v2` fixes:

- **Duplicated trailing rows were invisible (#487).** The old tree paired an odd last node with itself, so rows `[a, b, c]` and `[a, b, c, c]` had the same root, and a final block re-emitted on resume passed verification. Leaves and interior nodes are now domain-separated, odd nodes are promoted unchanged, and the row count is committed.
- **Roots depended on Arrow display formatting (#490).** Every Arrow type now has an explicit encoding, specified in `docs/verifiability-hash-strategy.md` and pinned by golden tests. Physical variants of the same data encode identically: `Utf8`/`LargeUtf8`/`Utf8View`, dictionaries and their values, the binary family, and timestamps in any unit. A null can no longer collide with the string `<null>`. Arrow types without an encoding make `verify` fail with an error instead of guessing.
- **Timestamps were not covered.** In v0.7.1 and earlier, every `Timestamp(_, "UTC")` value, including the canonical `timestamp` column of every table, was hashed as the same constant, because Arrow's formatter rejects named timezones without the `chrono-tz` feature. Changing a timestamp did not change the root. `merkle_v2` hashes the instant (nanoseconds since the epoch), so the switch of `timestamp` to milliseconds (#491) keeps roots unchanged for identical block times.

Migration:

- Existing registries have no `merkle_version` column and are read as `merkle_v1`. `verify` reports their partitions as `mismatch` with `merkle version mismatch: registry=merkle_v1 runtime=merkle_v2; ...` and exits 1.
- To rebuild, keep a copy of the old registry, then run `fireparq verify <path> --update-registry` against trusted data, and run `verify` again to confirm it passes. The full procedure is in `docs/verifiability-artifact-runbook.md` ("Migrating a legacy `merkle_v1` registry").
- Report consumers that compare roots should compare `algorithm` and `merkle_version` too (`docs/verify-report-contract.md`).

### `rollup`: in-place runs need `--delete-source`, outputs have new names, and only time partitions are rolled up (#478)

`rollup` used to write fixed file names (`part-000001.parquet`), so a re-run could overwrite its earlier output and then delete it (see Fixes). The fix changes three behaviors:

- **In-place rollups need `--delete-source`.** `fireparq rollup <dir>` without `--output` now fails unless `--delete-source` is passed. Without that flag, the rolled-up copy was written next to its sources, so every row was stored twice under the same root.
- **New output names.** With `--delete-source`, outputs are named like ingestion parts: `part-<run>-NNNNNN.parquet`, where `<run>` is a random id per run. Without it, outputs are named `part-rollup-<run>-NNNNNN.parquet`, and a re-run replaces the earlier `part-rollup-*` files in each target partition it rewrites. Existing files are never overwritten.
- **Only time partitions are rolled up.** Rollup now reads only `part-*.parquet` files below a partition finer than `--partition` (`hour=`, `minute=`, or `second=`). Files in `block_range=` or unpartitioned directories are no longer concatenated; use `merge` to compact those.

Migration:

- Add `--delete-source` to in-place `rollup` commands, or write to a separate `--output`.
- Don't rely on rollup outputs being named `part-000001.parquet`.

### Built-in `--network` aliases: 13 removed, 4 moved to StreamingFast (#535)

17 of the 54 built-in aliases pointed at Pinax hosts that no longer resolve or fail TLS, so `fireparq build --network <alias>` failed at startup. The registry is regenerated from The Graph networks registry (v0.8.4, 2026-09-24).

**Removed.** Neither Pinax nor any other provider in the registry serves these networks over Firehose any more:

`bnb-op`, `bnb-svm`, `fantom`, `fuse`, `gnosis-chiado-cl`, `mode-mainnet`, `moonbeam`, `moonriver`, `ronin`, `scroll`, `scroll-sepolia`, `telos`, `telos-testnet`

`--network` now rejects them during argument parsing. If you have a working endpoint for one of these chains, pass it with `--endpoint`.

**Moved to StreamingFast.** Pinax no longer serves these networks, so they use the StreamingFast endpoint the registry lists:

| Alias | Before | After |
|---|---|---|
| `near-mainnet` | `near.firehose.pinax.network` | `mainnet.near.streamingfast.io` |
| `near-testnet` | `neartest.firehose.pinax.network` | `testnet.near.streamingfast.io` |
| `tron` | `tron.firehose.pinax.network` | `mainnet.tron.streamingfast.io` |
| `tron-evm` | `tronevm.firehose.pinax.network` | `mainnet-evm.tron.streamingfast.io` |

- These endpoints need a credential that StreamingFast accepts, such as a The Graph Market API token in `STREAMINGFAST_API_TOKEN` (provider scoping: #562). Credentials are provider-specific: a token that works against Pinax can be rejected here with `invalid JWT token`; fatal authentication failures now stop the run (#472). Use `FIREHOSE_ENDPOINT_<ALIAS>` or `--endpoint` if you have another endpoint for these chains.
- `cursor.parquet` records the endpoint, so resuming output written through the old Pinax endpoint fails with an `endpoint` cursor mismatch. Rerun with `--cursor-override` and an explicit `--start-block` just after the cursor's last block.

**Added.** New Pinax networks in the registry: `arc`, `megaeth`, `robinhood`, `tempo`, `xlayer-mainnet`.

How aliases are chosen, and how to refresh them, is described in `docs/network-registry-integration.md`.

### `verify`: one registry per network, chain and table inferred from the data (#488)

`verify` used to default to `--chain evm --table blocks` and to derive the registry path from them, with a hard-coded `mainnet`. On S3, every network mapped to `s3://<bucket>/evm/mainnet/merkle_roots.parquet`, so verifying a second network overwrote the first network's roots. Locally, the registry was written under a fake `<output>/<chain_name>/evm/mainnet/` directory. When the verify path contained it (or `cursor.parquet`), that file was hashed as an `unpartitioned` partition, and the next run failed against the registry it had just written.

Now:

- **The chain and table are inferred.** The chain comes from the `firehose-parquet.block_type` file metadata and the table from the directory layout (`<output>/<chain_name>/<table>/...`). `--chain` and `--table` are only needed for data without that information. An explicit value that contradicts the data is an error, and so is a verify path that spans several tables or networks.
- **The registry lives in the network directory.** It is `<output>/<chain_name>/merkle_roots.parquet`, one per network and shared by its tables (rows are keyed by network, chain, table and partition, #489). Published reports go to `<output>/<chain_name>/verify_runs/<run_id>/report.json`. The report gains `network` and `warnings`.
- **Reserved artifacts are never scanned as data.** `cursor.parquet`, `partitions.parquet`, `merkle_roots.parquet` and `verify_runs/` are skipped. Paths are matched by component instead of by substring.

Migration:

- Scripts that passed `--chain evm --table blocks` for non-EVM or non-`blocks` data now fail with a conflict error. Drop the flags, or fix them.
- `verify` warns while a registry exists at the old default location (`<output>/<chain_name>/evm/mainnet/merkle_roots.parquet` locally, `s3://<bucket>/evm/mainnet/merkle_roots.parquet` on S3). Keep a copy of it, run `fireparq verify <output>/<chain_name>/<table> --update-registry` for each table against trusted data, then delete the old file (locally the whole `<output>/<chain_name>/evm/` directory). The details are in `docs/verifiability-artifact-runbook.md` ("Moving a registry from the old default location"). An S3 registry at the old location may mix several networks' rows, so do not reuse it as a baseline.

### `verify`: a failing run never changes the registry; `--update-registry` passes once it has written (#489)

An interrupted or failing `verify` could record wrong canonical roots:

- a fail-fast stop inside a partition recorded that partition's truncated root;
- roots were recorded even when protocol checks failed;
- the partition `build` was still writing got a root that the next run could no longer match;
- two runs writing at once lost one run's rows;
- local registry writes were not atomic;
- two networks sharing one `--registry-path` overwrote each other's rows.

Now:

- **A failing run never writes.** The registry is written only when no protocol check failed, the scan was not cut short, and no root differs (or `--update-registry` was given). A partly read partition is never compared or recorded. The report's `warnings` say why a write was held back.
- **`--update-registry` passes once it has written.** Replaced roots are reported with the new finding status `updated` (previous root in `expected_root`) and no longer count as mismatches. A rebuild runs to the end without `--no-fail-fast` and exits 0 once the registry is written. Previously it exited 1.
- **Partitions still being written are `open`.** When `<chain_root>/cursor.parquet` has not reached its stop block (a live or interrupted build), the newest partition and any partition with rows beyond the cursor are reported as `open`, and are neither compared nor recorded. The report gains `summary.updated` and `summary.open_partitions`.
- **Registry rows are keyed by network.** A new `network` column lets one custom registry serve several networks. Rows without a network (older registries) still apply to any network.
- **Writes are safe under concurrency.** Local writes take a lock file (`merkle_roots.parquet.lock`) and replace the registry atomically (temporary file, fsync, rename). S3 writes use a conditional put on the ETag and retry after a concurrent update. A run fails instead of overwriting a row that another run changed to a different root.

Migration:

- Scripts that expected `--update-registry` to exit 1 should check the report's `updated` findings instead.
- Runs that fail (a mismatch or a protocol failure) no longer fill missing roots. Fix the failure, or pass `--update-registry`, first.

### EVM: failed transactions are included by default, with their persistent state changes (#494)

A failed or reverted EVM transaction still pays for gas, pays the fee recipients and increments the sender's nonce. `build` used to drop failed transactions by default, so `balance_changes` and `nonce_changes` could not be reconciled with on-chain balances and nonces. On ETH mainnet blocks 26049575–26049579 that was 12 of 1,148 transactions, 34 gas/fee balance changes and 12 nonce changes.

EVM output now includes failed transactions by default. For each one, `build` writes the transaction, all of its calls and gas changes, and only the state changes that persist on chain. This follows the rule documented on `TransactionTrace.status` in `proto/ethereum.proto`:

- `balance_changes`: root-call changes with reason `GAS_BUY`, `GAS_REFUND`, `REWARD_TRANSACTION_FEE`, or `INCREASE_MINT` (OP Stack deposits keep their mint when they fail).
- `nonce_changes`: the sender's nonce increment, plus one per accepted EIP-7702 authorization.
- `code_changes`: at most one per accepted EIP-7702 authorization.
- `storage_changes`, `account_creations`, and the other balance changes (for example rolled-back `TRANSFER`s): none.

On the same 5 blocks, the failed transactions carried 62 rolled-back `TRANSFER` balance changes and 127 rolled-back storage changes. None of them are written.

Flags:

- `--exclude-failed-transactions` (env `EXCLUDE_FAILED_TRANSACTIONS`) drops failed transactions on every chain. It restores the previous EVM output.
- `--include-failed-transactions` is deprecated for EVM. It has no effect there and logs a warning.
- Non-EVM chains are unchanged: they still exclude failed transactions unless `--include-failed-transactions` is set. On those chains the included transactions still carry their rolled-back effects (for example Antelope `hard_fail` actions or Solana instructions of failed transactions); that is tracked separately.

Migration:

- Existing EVM outputs have no failed transactions. `transactions`, `calls`, `balance_changes`, `nonce_changes`, `code_changes` and `gas_changes` gain rows for them in new files. Queries that assumed every row belongs to a successful transaction should filter on `transactions.status = 'SUCCEEDED'`, or build with `--exclude-failed-transactions`.
- Resuming an EVM output whose `cursor.parquet` recorded `include_failed_transactions=false` (the old default, also assumed when a cursor predates the key) keeps excluding failed transactions, so one output does not mix both modes. `build` logs a warning; pass `--exclude-failed-transactions` to keep that silently. To switch an existing output to the new default, rerun with `--cursor-override` and an explicit `--start-block` just after the cursor's last block.
- EVM outputs built with `--include-failed-transactions` before this release wrote every state change of failed transactions, including rolled-back transfers and storage writes. Rebuild them if you need the change tables to reconcile.

### EVM change tables: new `tx_index`, `call_index`, `state_reverted` and `persisted` columns (#495)

Successful transactions include state changes made inside calls that were later reverted: on ETH mainnet blocks 26049575–26049579, 12 balance changes and 122 storage changes. The change tables only had `tx_hash`, so these could not be told apart from changes that persisted, and rebuilding balances or storage from them gave wrong values.

New columns on `balance_changes`, `nonce_changes`, `code_changes`, `storage_changes`, `account_creations` and `gas_changes`:

| Column | Type | Meaning |
|---|---|---|
| `tx_index` | `UInt32` | Index of the transaction in the block (joins `transactions.index`). |
| `call_index` | `UInt32` | Firehose index of the call that recorded the change (joins `calls.call_index` with `tx_hash`). |
| `state_reverted` | `Boolean` | That call's `state_reverted` flag, the same value as in `calls`. |
| `persisted` | `Boolean` | Whether the change is part of chain state after the transaction. Not on `gas_changes`. |

- `persisted` is `NOT state_reverted` for successful transactions. For failed transactions it is always `true`: only their persistent changes are written (#494), and they come from the root call, whose `state_reverted` is `true`. Filter on `persisted` to rebuild state. On the 5 blocks above, balances rebuilt this way chain without a gap (each change's `old_value` equals the previous change's `new_value`); without the filter, 10 links break.
- The `system_*` change tables get a nullable `call_index`: the system call that recorded the change, or `NULL` for block-level changes such as withdrawals.
- `tx_index` and `call_index` follow `tx_hash`, and `call_index` follows `block_number` in the `system_*` tables. `state_reverted` and `persisted` are the last columns before `fork_step`.
- The `logs` table is unchanged. It holds receipt logs only, which never include logs of reverted calls (58 of them in the blocks above).

Migration: files written before and after this change have different schemas for these 12 tables. Query them separately or with `union_by_name`, and do not `merge` or `rollup` old and new files together. Rebuild old ranges to get the new columns.

### `truncate`: filters on different keys must all match, deletes need `--yes`, and destructive commands no longer fall back to `S3_BUCKET` (#481)

- **Filters on different keys must all match.** `-p year=2026 -p month=01` used to delete every file under `year=2026` *or* `month=01`, including January 2025 and all of 2026. It now selects January 2026 only. Filters on the same key still match either value (`-p day=01 -p day=02`).
- **Partition-path filters work.** A filter with `/`, such as `year=2026/month=01/day=15`, matches files whose partition directories (below the given path, in every table) start with exactly those segments. It used to match nothing, because each filter was compared with a single directory name. Empty filters, path filters with a segment that is not `key=value`, and segments with more than one `*` are now rejected.
- **`--yes` is required to delete.** Without `--yes` (and without `--dry-run`), `truncate` prints a summary and exits non-zero without deleting anything. The summary lists the file count, total size, the first 10 paths, and any root artifacts such as `cursor.parquet`.
- **No `S3_BUCKET` fallback for `truncate`, `merge`, and `rollup`.** A relative path that did not exist locally used to become `s3://$S3_BUCKET/<path>`, and `.env` is loaded automatically, so a typo could target a bucket. These three commands now fail with `path does not exist`, naming the `s3://` URI to pass if S3 was intended. `scan`, `inspect`, `validate`, and `verify` keep the fallback.

Migration:

- Add `--yes` to scripts that run `truncate` for real.
- Check scripts that pass several `-p` filters on different keys: they now select the intersection instead of the union.
- Pass `s3://bucket/prefix` explicitly to `truncate`, `merge`, and `rollup` for S3 data.

### EVM: missing Firehose fields on `blocks`, `transactions`, `calls`, `system_calls` and `logs` (#496)

A field-by-field comparison with live ETH mainnet blocks showed that every value `build` wrote was correct, but some populated fields were not written. They are now, appended after the existing columns of each table:

- `blocks`: `uncle_hash`, `logs_bloom`, `withdrawals_root`, `blob_gas_used`, `excess_blob_gas`, `parent_beacon_root`, `requests_hash`.
- `transactions`: `v`, `r`, `s`, `return_data`, the receipt's `logs_bloom`, `blob_gas`, `blob_gas_fee_cap`, `blob_hashes` (a list), the receipt's `blob_gas_used` and `blob_gas_price`, `begin_ordinal`, `end_ordinal`.
- `calls` and `system_calls`: `failure_reason`, `address_delegates_to` (EIP-7702), `begin_ordinal`, `end_ordinal`.
- `logs`: `ordinal`.

Details:

- Fields introduced by a fork are `NULL` before it: `withdrawals_root` (Shanghai), the blob fields and `parent_beacon_root` (Cancun), `requests_hash` (Prague). The blob transaction fields are `NULL` for non-blob transactions, and `blob_hashes` is an empty list. `failure_reason` and `address_delegates_to` are `NULL` when absent.
- `blob_gas_fee_cap` and `blob_gas_price` are decimal strings, like the other big-integer columns.
- System call indexes restart at 1 for the system calls that run after the transactions (EIP-7002 and EIP-7251 requests), so `system_calls.call_index` alone does not identify a system call within a block. Join the `system_*` change tables (#495) on `call_index` and `ordinal BETWEEN begin_ordinal AND end_ordinal`. The README has the query.

Migration: files written before and after this change have different schemas for these 5 tables. Query them separately or with `union_by_name`, and do not `merge` or `rollup` old and new files together.

### Beacon: withdrawals, execution requests, BLS changes, committee bits and slashing indices (#504)

Beacon output dropped several parts of the block body. Since Electra, deposits reach the chain as execution requests, so post-Electra deposits were missing entirely: `deposits` only holds Eth1 bridge deposits, which stop once the bridge backlog is processed. Post-Electra attestations could not be tied to a committee either, because EIP-7549 fixed `committee_index` at `0`.

Five new tables:

| Table | Rows from | Content |
|---|---|---|
| `withdrawals` | Capella | `execution_payload.withdrawals`: `withdrawal_index`, `validator_index`, `address`, `amount` (Gwei) |
| `bls_to_execution_changes` | Deneb | `validator_index`, `from_bls_pubkey`, `to_execution_address`, `signature` |
| `deposit_requests` | Electra | EIP-6110 deposits: `deposit_index` (the deposit contract index), `pubkey`, `withdrawal_credentials`, `amount`, `signature` |
| `withdrawal_requests` | Electra | EIP-7002: `source_address`, `validator_pubkey`, `amount` (`0` = full exit) |
| `consolidation_requests` | Electra | EIP-7251: `source_address`, `source_pubkey`, `target_pubkey` |

New columns on existing tables:

- `blocks.graffiti`: the proposer's 32 graffiti bytes, on every fork.
- `attestations.committee_bits`: the committees an Electra attestation aggregates, as an 8-byte bitvector. Null before Electra.
- `attester_slashings.attestation_1_attesting_indices` / `attestation_2_attesting_indices`: `List<UInt64>` of each attestation's validators. The slashed validators are in both lists.

Blocks from before a table's fork add no rows to it. The Firehose Capella body has no BLS-to-execution changes, so the changes included in Capella blocks are not available; `bls_to_execution_changes` starts at Deneb. The README section "Beacon Chain Tables" describes every table.

On mainnet, slots 15292390–15292397 and 15292636–15292641 (Fusaka) and 6209540–6209548 (Capella) match the Firehose blocks row for row and value for value: 16 withdrawals per Fusaka block, 29 in the Capella blocks, and one deposit, one withdrawal and two consolidation requests.

Migration:

- `blocks`, `attestations` and `attester_slashings` have new columns. Do not `merge` or `rollup` files written before and after this change together; both commands refuse partitions with mixed schemas (#479). Rebuild existing Beacon output, or keep the old files under a separate prefix.
- The new tables only appear in newly built ranges. Rebuild historical ranges to backfill withdrawals and execution requests.
- Post-Electra queries that group attestations by `committee_index` should decode `committee_bits` instead.

## New features

### EVM: new `withdrawals`, `access_lists` and `set_code_authorizations` tables (#497)

Firehose provides beacon-chain withdrawals (16 per mainnet block), transaction access lists (EIP-2930) and EIP-7702 authorizations, but none of them were written. Withdrawals only showed up as `system_balance_changes` rows with reason `WITHDRAWAL`, in wei and without the validator or withdrawal index.

Three new EVM tables, written at both detail levels (also with `--without-extended`):

- `withdrawals`: one row per withdrawal, with `index`, `validator_index`, `address` and `amount_gwei` (in gwei, not wei).
- `access_lists`: one row per access-list entry, with `tx_hash`, `tx_index`, `access_index`, `address` and `storage_keys` (a list).
- `set_code_authorizations`: one row per EIP-7702 authorization, with `tx_hash`, `tx_index`, `authorization_index`, `chain_id` (decimal), `address` (delegation target), `nonce`, `v`, `r`, `s`, `authority` and `discarded`.

Rows of `access_lists` and `set_code_authorizations` follow their transaction: they are written for failed transactions and dropped by `--exclude-failed-transactions`. EVM outputs now have 6 base tables and 20 with extended detail.

## Fixes

- **SIGINT/SIGTERM interrupt endpoint waits promptly (#473).** The shutdown flag used to be checked only after a block was processed, so a stop request was ignored during idle waits (up to the 120 s idle timeout), reconnect back-off (up to 60 s), connection attempts (30 s) and startup checks, and Kubernetes escalated to SIGKILL on quiet chains. A cancellation token now interrupts every stream wait; a block being processed still finishes first. A second signal exits immediately with code 130. Shutdown is detected with a typed error instead of matching the string `"__shutdown__"`.

- **`build` no longer retries fatal gRPC errors forever (#472).** `Unauthenticated`, `PermissionDenied`, `InvalidArgument`, `FailedPrecondition`, `OutOfRange` and `Unimplemented` now end the run with an error and a hint, including statuses Firehose relays as `Unknown` with the real code in the message. So does `ResourceExhausted` when it reports an exhausted quota (e.g. `billable egress bytes quota exceeded`); other `ResourceExhausted` errors such as rate limits are still retried with back-off. An invalid cursor, a message over 128 MiB, or credentials that are not valid for the endpoint (such as a Pinax token against a StreamingFast endpoint after #535) used to reconnect about once a second indefinitely. Fatal errors are counted in `firehose_parquet_errors_total{kind="grpc_fatal"}`.
- **Reconnect back-off and the stall timer reset only when a stream message arrives (#472),** not when a connection or the `Blocks` RPC is accepted, so repeated failures now back off up to 60 s and trip `--reconnect-stall-timeout-secs`. A run also gives up after 30 consecutive failed attempts without a message, even with the stall timeout disabled. Idle-timeout reconnects are not counted as failures. The logged `attempt` now counts failures since the last message (it used to reset to 0 on every connection).

- **CLI values that crashed or misbehaved are now rejected or consistent (#471).**
  - `--block-range-size 0` (for `build` and `partitions build`) and `--stop-block 0` are rejected by the parser. `build` also rejects a `--stop-block` at or below the start block (`--stop-block` is exclusive). `--block-range-size 0` used to panic with a division by zero, and `--stop-block 0` streamed from genesis forever.
  - A bounded run never sends Firehose `stop_block_num = 0`, which means "stream forever": `--start-block 0 --stop-block 1` now ingests block 0 and stops instead of never ending.
  - `--flush-bytes 0` now means "byte-based flushing disabled" in `build`, as it already did in the writer, `merge` and `rollup`. It used to flush after every block, writing one file per table per block.
  - `--stream-idle-timeout-secs 0` and `--reconnect-stall-timeout-secs 0` disable those timeouts. They used to cause a reconnect storm and an immediate exit on the first connection error, respectively.
  - API keys and JWT tokens are trimmed, so a secret file with a trailing newline works, and they are parsed once when the client is created. A credential that cannot be sent as a gRPC header is a startup error instead of a panic (exit code 101).

- **`build --start-block` above the last irreversible block no longer writes earlier blocks (#466).** Firehose serves such a request from LIB+1, and those blocks used to be written. Blocks below the effective start block are now skipped before mapping, logged once, and counted in the new `firehose_parquet_blocks_skipped_below_start_total` metric and the `blocks_skipped_below_start` summary field.
- **Bounded `build` runs verify the stop block (#466).** A run with `--stop-block` exits 0 only once block `stop_block - 1` was received. A stream that ends earlier is resumed from the cursor. If the server then has no more blocks, the run completes with a warning on chains with skipped slots or heights (Solana, NEAR, Beacon), and otherwise writes what it received, saves the cursor there, and exits non-zero.
- **Live `build` runs reconnect when the stream closes cleanly (#466).** Previously a clean close by the server or a proxy ended the process with exit code 0, so `Restart=on-failure` supervisors never restarted it.

- **`build` no longer restarts from scratch when `cursor.parquet` cannot be read (#465).** Only a missing cursor, or one with no row or an empty cursor string, starts a fresh run. A cursor that exists but cannot be loaded (permission denied, S3 403/5xx/timeout, empty, truncated or corrupt file) now fails the run with an error that names the file. Previously the run logged "starting fresh", re-ingested from `--start-block` or genesis, and overwrote the good cursor on its first flush. To deliberately ignore an unreadable cursor and restart from the CLI bounds, pass `--cursor-override`. `partitions build` also fails instead of ignoring an unreadable sibling cursor when it uses it to infer `--start-block`.
- **Local cursor saves are atomic (#465).** `cursor.parquet` is written to `cursor.parquet.tmp` in the same directory, fsynced, renamed over the target, and the directory is fsynced. A crash mid-save leaves the previous cursor intact instead of a truncated file. S3 cursor saves were already atomic (single PUT).
- **A failed table write no longer loses rows (#464).** The writer keeps a table's buffered rows until its write succeeds. When a write, mapping or stream error ends `build`, partial buffers are discarded, `cursor.parquet` is not advanced, and the process exits non-zero, so the next run replays the uncommitted window. Previously the error path flushed the other tables and saved the cursor past the lost rows.
- **`rollup` re-runs no longer lose or duplicate rows (#478).** An in-place re-run with `--delete-source` overwrote its earlier output and then deleted it as a source; on mainnet test data, the second run deleted the whole dataset. A re-run without `--delete-source` read the earlier output again and stacked duplicate rows. Re-runs now only roll up files that are still below the target granularity, and never overwrite or delete their own output. With `--delete-source`, each target partition's sources are deleted as soon as its output is written, so a failure partway through no longer leaves finished partitions to be rolled up a second time.
- **`rollup` and `merge` leave root artifacts alone (#478).** `cursor.parquet`, `partitions.parquet`, `merkle_roots.parquet`, and anything under `verify_runs/` are skipped. Rolling up or merging a network root used to fold them into a `part-000001.parquet` and delete them, or fail on their mismatched schemas.
- **`merge` and `rollup` no longer combine files with different schemas (#479).** Both commands paired columns by position. When a partition mixed files from two tool versions or two schemas, an extra column was silently dropped, and two columns of the same type in a different order swapped values. The sources were then deleted. Both commands now compare every file's columns (names, types, nullability, and order) before writing. A partition with mixed schemas is left untouched, with nothing written or deleted in it. The other partitions are still processed, and the command then exits non-zero, listing the skipped partitions and how their files differ. `merge` reads each part's footer before merging a partition, which adds one small range request per S3 object, and `merge --dry-run` reports these partitions too.

- **An interrupted `merge` no longer leaves duplicate rows (#480).** A crash, a `kill`, or a failed upload between writing a partition's merged files and deleting its original parts used to leave both, and the next merge folded them into one file for good. Each partition merge is now journaled in `_fireparq_merge.json`, and the next run finishes or undoes an interrupted merge before doing anything else: it deletes the remaining original parts if the merge had committed, and otherwise it deletes the partial outputs and merges the partition again. Local outputs are written to a temporary file, fsynced, and renamed, so a partial Parquet file is never visible.
- **Overlapping `merge` runs are refused (#480).** `merge` holds `.fireparq-merge.lock` at its path: an OS file lock locally, and a conditionally created object on S3 that is refreshed while the run is active and taken over after 30 minutes without a refresh. A second merge on the same path now fails right away, and a partition that a merge on an enclosing or nested path is working on is skipped. On an S3 store without conditional writes, merge warns and the lock is best effort.

## Performance

### Projected dataset validation (#524)

`validate` decodes only canonical ID/height/time columns, preserves full-schema
checks, and compares cached partition endpoints instead of repeatedly scanning
all blocks. This also fixes endpoint selection when partitions contain overlapping
heights. Global validation still retains all canonical tuples, and S3 still
downloads whole objects. See [the checks and benchmark](../audit/524-validation-performance.md).

### Fixed-size Base58 conversion uses safe integer limbs (#565)

32-byte keys and 64-byte signatures use a safe stack-buffer encoder; all other
lengths, including Tron checksummed addresses, retain `bs58`. Output strings,
leading zeros, decoders and schemas are unchanged. No dependency or data
migration is added. Conversion plus Arrow append measured 13.4× faster for
32-byte and 14.4× for 64-byte values on one Apple M1 Max, with the 25-byte
fallback unchanged within measurement noise. These are conversion benchmarks,
not end-to-end ingestion claims. See [equivalence coverage, dependency review and measured
conversion performance](../audit/565-fixed-base58.md).

### EVM decimal conversion writes directly into Arrow (#513)

Recovered and validated the previous agent's u128/limb formatter. Up to 32
significant bytes use a stack buffer and direct StringBuilder append; larger
inputs retain an arbitrary-length fallback. Nulls, zero/leading-zero values,
decimal strings and schemas are unchanged. Five conversion-and-append benchmark
cases improved by 10.8–26.8× on one Apple M1 Max; this is not an end-to-end ingestion
claim. See [equivalence coverage, recovered-work provenance and all measurements](../audit/513-evm-decimal-fast-path.md).

- **Canonical identity columns are encoded once per block (#512).** `block_id` and `parent_id` used to be hex-decoded and re-encoded on every row of every table. Mappers now prepare them once per block and append the encoded values. The output is byte-identical. The canonical columns cost about 40 ns per row instead of 380 ns (binary), 830 ns (hex) or 3.1 µs (base58). Mapping a synthetic 1,000-transaction Solana block (base58) takes 19 ms instead of 43 ms, and a 200-transaction EVM block with 2,000 logs (hex) takes 3.7 ms instead of 6.0 ms.
- **Hex and base58 columns no longer allocate a string per value (#514).** Byte columns encode into a reused buffer (`hex::encode_to_slice`, `bs58` into a `Vec`), and byte and canonical column builders keep their capacity across flushes. The output is byte-identical. A 32-byte hex value costs about 32 ns instead of 230 ns, and the 200-transaction EVM block (hex) now maps in 1.55 ms instead of 3.8 ms. Base58 is dominated by the encoding itself (about 1.3 µs per 32-byte value), so Solana mapping changes little.

- **`verify` memory no longer grows with row count, and protocol-only runs skip hashing (#521).** Partition roots are built as rows stream in, with O(log n) memory per partition instead of 32 bytes per row. The roots are identical. `--checks protocol` reads only the columns the protocol checks use, and hashes nothing. S3 objects are prefetched, up to 4 at a time with a 256 MiB budget. On 300 EVM mainnet blocks, verifying `gas_changes` (9.0 million rows) peaks at 30 MiB instead of 940 MiB, in about the same time (14 s). A protocol-only run on `calls` (1.7 million rows) takes 0.2 s instead of 5.2 s.

## Tests

- A new cross-chain schema contract test maps one fixture batch for every table of every chain, under every bytes encoding and both `fork_step` settings. It checks that column names are unique, that each batch round-trips through the Parquet writer and reader with the same schema and values, and that every table's canonical `block_id` / `parent_id` match the `blocks` table.
- A weekly `Network endpoints` workflow runs `scripts/check_network_endpoints.sh`, which sends a Firehose `EndpointInfo` call to every built-in `--network` endpoint and fails when one no longer answers (#535). It also runs on pull requests that change the generated registry. Regular `cargo test` stays offline.
- A Parquet round-trip test asserts that the canonical `timestamp` is written as `TIMESTAMP(MILLIS, isAdjustedToUTC=true)` and reads back as `Timestamp(Millisecond, UTC)` without the embedded Arrow schema. The contract test also checks the canonical `timestamp` type on every table.

## Bitcoin amounts and input metadata (#511)

Bitcoin-family outputs add `value_sats: UInt64`; the original floating coin
amount remains unchanged. Exact serialized output units are preferred; absent
raw transaction bytes use a unique, checked conversion. Inconsistent or
unrecoverable amounts stop mapping before that block appends any rows. The shared
Litecoin mapper is not constrained by Bitcoin's monetary bound.

Inputs add `tx_index`. Coinbase/ordinary input fields and absent script messages
now use nulls, with real zero indices and present empty scripts preserved.
Output addresses support the legacy first-address fallback. Native protobuf text
encoding is unchanged and now documented accurately. These are intentional schema
changes; use a new/rebuilt dataset or explicit reader-side schema reconciliation.

- Chain protobuf byte fields now share owned Firehose payload storage during
  mapping (#518). Existing borrowed mapper calls remain available; generated
  Rust protobuf byte fields are now `Bytes` (`Vec` callers can use `.into()`).
  Protobuf wire and Parquet schemas are unchanged. See
  [decoding validation and benchmark](../audit/518-owned-protobuf-bytes.md).
