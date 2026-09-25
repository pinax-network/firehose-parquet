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

## Fixes

- **`build` no longer restarts from scratch when `cursor.parquet` cannot be read (#465).** Only a missing cursor, or one with no row or an empty cursor string, starts a fresh run. A cursor that exists but cannot be loaded (permission denied, S3 403/5xx/timeout, empty, truncated or corrupt file) now fails the run with an error that names the file. Previously the run logged "starting fresh", re-ingested from `--start-block` or genesis, and overwrote the good cursor on its first flush. To deliberately ignore an unreadable cursor and restart from the CLI bounds, pass `--cursor-override`. `partitions build` also fails instead of ignoring an unreadable sibling cursor when it uses it to infer `--start-block`.
- **Local cursor saves are atomic (#465).** `cursor.parquet` is written to `cursor.parquet.tmp` in the same directory, fsynced, renamed over the target, and the directory is fsynced. A crash mid-save leaves the previous cursor intact instead of a truncated file. S3 cursor saves were already atomic (single PUT).
- **A failed table write no longer loses rows (#464).** The writer keeps a table's buffered rows until its write succeeds. When a write, mapping or stream error ends `build`, partial buffers are discarded, `cursor.parquet` is not advanced, and the process exits non-zero, so the next run replays the uncommitted window. Previously the error path flushed the other tables and saved the cursor past the lost rows.

## Tests

- A new cross-chain schema contract test maps one fixture batch for every table of every chain, under every bytes encoding and both `fork_step` settings. It checks that column names are unique, that each batch round-trips through the Parquet writer and reader with the same schema and values, and that every table's canonical `block_id` / `parent_id` match the `blocks` table.
