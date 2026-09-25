# firehose-parquet: unreleased

Changes merged since the last release. Fold this file into `docs/releases/vX.Y.Z.md` when the next release is cut.

## Breaking changes

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

- These endpoints need a credential that StreamingFast accepts, such as a The Graph Market API token in `SUBSTREAMS_API_TOKEN`. Credentials are provider-specific: a token that works against Pinax can be rejected here with `invalid JWT token`, and `build` then keeps reconnecting (#472). Use `FIREHOSE_ENDPOINT_<ALIAS>` or `--endpoint` if you have another endpoint for these chains.
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

## Fixes

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

## Performance

- **Canonical identity columns are encoded once per block (#512).** `block_id` and `parent_id` used to be hex-decoded and re-encoded on every row of every table. Mappers now prepare them once per block and append the encoded values. The output is byte-identical. The canonical columns cost about 40 ns per row instead of 380 ns (binary), 830 ns (hex) or 3.1 µs (base58). Mapping a synthetic 1,000-transaction Solana block (base58) takes 19 ms instead of 43 ms, and a 200-transaction EVM block with 2,000 logs (hex) takes 3.7 ms instead of 6.0 ms.

## Tests

- A new cross-chain schema contract test maps one fixture batch for every table of every chain, under every bytes encoding and both `fork_step` settings. It checks that column names are unique, that each batch round-trips through the Parquet writer and reader with the same schema and values, and that every table's canonical `block_id` / `parent_id` match the `blocks` table.
- A weekly `Network endpoints` workflow runs `scripts/check_network_endpoints.sh`, which sends a Firehose `EndpointInfo` call to every built-in `--network` endpoint and fails when one no longer answers (#535). It also runs on pull requests that change the generated registry. Regular `cargo test` stays offline.
- A Parquet round-trip test asserts that the canonical `timestamp` is written as `TIMESTAMP(MILLIS, isAdjustedToUTC=true)` and reads back as `Timestamp(Millisecond, UTC)` without the embedded Arrow schema. The contract test also checks the canonical `timestamp` type on every table.
