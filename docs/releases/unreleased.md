# firehose-parquet: unreleased

Changes merged since the last release. Fold this file into `docs/releases/vX.Y.Z.md` when the next release is cut.

## Breaking changes

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
- To rebuild, keep a copy of the old registry, then run `fireparq verify <path> --update-registry --no-fail-fast` against trusted data, and run `verify` again to confirm it passes. The full procedure is in `docs/verifiability-artifact-runbook.md` ("Migrating a legacy `merkle_v1` registry").
- Report consumers that compare roots should compare `algorithm` and `merkle_version` too (`docs/verify-report-contract.md`).

## Fixes

- **`build` no longer restarts from scratch when `cursor.parquet` cannot be read (#465).** Only a missing cursor, or one with no row or an empty cursor string, starts a fresh run. A cursor that exists but cannot be loaded (permission denied, S3 403/5xx/timeout, empty, truncated or corrupt file) now fails the run with an error that names the file. Previously the run logged "starting fresh", re-ingested from `--start-block` or genesis, and overwrote the good cursor on its first flush. To deliberately ignore an unreadable cursor and restart from the CLI bounds, pass `--cursor-override`. `partitions build` also fails instead of ignoring an unreadable sibling cursor when it uses it to infer `--start-block`.
- **Local cursor saves are atomic (#465).** `cursor.parquet` is written to `cursor.parquet.tmp` in the same directory, fsynced, renamed over the target, and the directory is fsynced. A crash mid-save leaves the previous cursor intact instead of a truncated file. S3 cursor saves were already atomic (single PUT).
- **A failed table write no longer loses rows (#464).** The writer keeps a table's buffered rows until its write succeeds. When a write, mapping or stream error ends `build`, partial buffers are discarded, `cursor.parquet` is not advanced, and the process exits non-zero, so the next run replays the uncommitted window. Previously the error path flushed the other tables and saved the cursor past the lost rows.

## Tests

- A new cross-chain schema contract test maps one fixture batch for every table of every chain, under every bytes encoding and both `fork_step` settings. It checks that column names are unique, that each batch round-trips through the Parquet writer and reader with the same schema and values, and that every table's canonical `block_id` / `parent_id` match the `blocks` table.
