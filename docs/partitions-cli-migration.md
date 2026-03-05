# Partitions CLI Migration (Phases 1-7)

This document captures the first implementation slices for the `partitions <subcommand>` initiative.

Related issues:

- #182 (parent roadmap)
- #176 (`partitions build` canonical index writer)
- #183 (CLI namespace and migration plan)
- #184 (`partitions ls` query command)
- #187 (partition-window ingestion mode)
- #191 (partition-aware cursor path strategy)
- #188 (deterministic partition sharding)
- #189 (`partitions validate` integrity checks)

## What phases 1-7 ship

1. Adds a grouped CLI namespace: `firehose-parquet partitions ...`
2. Adds `firehose-parquet partitions build` for generating canonical `partitions.parquet` artifacts directly from Firehose
3. Introduces `firehose-parquet partitions resolve`
4. Keeps existing ingestion flags (`--partitions-index`, `--partition-type`, `--partition-value`) working for backward compatibility
5. Emits a deprecation warning in ingestion mode when those legacy flags are used to drive range resolution directly
6. Introduces `firehose-parquet partitions ls` for querying/filtering index rows
7. Adds ingestion-side partition window resolution via `--partition-from` + `--partition-to`
8. Adds partition-aware cursor templating via `--cursor-template`
9. Adds deterministic partition sharding via `partitions shard`
10. Adds partition-index integrity validation via `partitions validate`

## Command behavior

### Partition index builder

The grouped CLI now supports writing the canonical partition index directly from Firehose:

- `firehose-parquet partitions build`

Current behavior:

- scans the requested `[start_block, stop_block)` range from Firehose
- computes UTC interval starts for `day`, `hour`, `minute`, and `second`
- emits one row per discovered partition with contiguous `[start_block, end_block)` bounds
- writes `/<chain>/partitions.parquet` under the supplied local or S3 output root
- includes file metadata defined in `docs/partitions-parquet-contract.md`

Current limitations:

- requires a finite non-zero `--stop-block`
- writes the canonical parquet artifact only; optional sidecar generation remains future work

### Ingestion window mode

Main ingestion now supports partition windows without manual block math:

- `--partitions-index`
- `--partition-type`
- `--partition-from` (inclusive)
- `--partition-to` (exclusive)
- optional `--partition-chain`

Behavior:

- Resolves all matching partition rows in `[partition_from, partition_to)`.
- Requires contiguous/non-overlapping block bounds across resolved rows.
- Produces one resolved `[start_block, stop_block)` range before running ingestion.

### Partition-aware cursor mode

Main ingestion now supports deterministic cursor-path templating:

- `--cursor-template 'cursor/{chain}/{partition_type}/{partition_value}.parquet'`

Supported variables:

- `{chain}`
- `{partition_type}`
- `{partition_value}`
- `{partition_from}`
- `{partition_to}`

Behavior:

- expands against the effective partition selection mode
- rejects unknown variables and missing required context
- escapes literal braces via `{{` and `}}`
- rewrites `/` and `\` in variable values to `_` to avoid path collisions
- works for local paths and S3-relative cursor paths under the output prefix

Example patterns:

- single partition worker: `cursor/{chain}/{partition_type}/{partition_value}.parquet`
- partition window worker: `cursor/{chain}/{partition_type}/{partition_from}-{partition_to}.parquet`
- local chain-specific worker: `./cursor/{partition_type}/{partition_value}.parquet`

### Partition sharding mode

The grouped CLI now supports deterministic shard assignment:

- `firehose-parquet partitions shard --shard-count N --shard-index K`

Supported strategies:

- `ordinal` — sorted row ordinal modulo shard count
- `hash` — stable SHA-256-based hash of `(chain, partition_type, partition_value)` modulo shard count

Behavior:

- shards are computed from the same filtered partition set as `partitions ls`
- no row should appear in more than one shard for fixed inputs
- combined shard outputs cover the full selected set

### Partition validation mode

The grouped CLI now supports integrity validation for `partitions.parquet`:

- `firehose-parquet partitions validate`

Current checks:

- invalid ranges (`start_block >= end_block`)
- overlaps between adjacent rows in the same `(chain, partition_type)`
- gaps between adjacent rows unless `--allow-gaps` is enabled
- ordering issues based on `partition_start_ts`

### New command

`firehose-parquet partitions ls` lists index rows with optional filters:

- optional `--partition-type`
- optional `--partition-chain`
- optional `--from` / `--to` time window
- `--limit` (default `100`)
- optional `--json`

Output is sorted ascending by `partition_start_ts`, and JSON mode returns machine-readable rows for schedulers/UI.

Implementation note: rows are streamed in record batches and retained in a bounded in-memory top-N heap keyed by ascending sort order, capped by `--limit`.

### Existing command

`firehose-parquet partitions resolve` resolves a single partition row from `partitions.parquet` and returns exact block bounds:

- `start_block`: inclusive
- `stop_block`: exclusive

Inputs:

- `--partitions-index` (local path or `s3://` URI)
- `--partition-type`
- `--partition-value`
- optional `--partition-chain`
- optional `--strict-single-chain`
- optional `--json` for automation output

### Backward compatibility

The existing ingestion flow still supports resolving ranges via legacy flags to avoid breaking active workflows. This is intentionally preserved for one migration window.

When legacy flags are used in direct ingestion mode (without explicit `--start-block/--stop-block`), a warning is logged to guide users toward:

`firehose-parquet partitions resolve ...`

## Process used for phases 1-7

1. Branch from `main` using `codex/` prefix.
2. Add CLI tree scaffolding in shared CLI crate (`firehose-parquet/src/cli.rs`).
3. Reuse existing partition index resolution logic and wrap it in command-focused result types.
4. Add the canonical builder path that streams block identities from Firehose and finalizes partition rows.
5. Add partition row query/list command implementation with streaming record-batch reads.
6. Wire the binary entrypoint (`blocks/src/bin/main.rs`) to execute new subcommands.
7. Add docs and examples in `README.md` and `docs/`.
8. Add parsing/data-path test coverage for new commands.
9. Run formatting + targeted checks.

## Validation run in these phases

- `cargo fmt`
- `cargo test -p firehose-parquet test_partitions_build_subcommand_parse -- --nocapture`
- `cargo test -p firehose-parquet test_build_partition_rows_from_blocks_mixed_types_and_contiguous -- --nocapture`
- `cargo test -p firehose-parquet test_partitions_resolve_subcommand_parse -- --nocapture`
- `cargo test -p firehose-parquet test_partitions_ls_subcommand_parse -- --nocapture`
- `cargo test -p firehose-parquet test_list_partitions_from_index_filters_sort_and_limit -- --nocapture`
- `cargo check -p blocks`
- `cargo check -p firehose-parquet`

## Follow-up phases (not included here)

- `firehose-parquet partitions validate`
- `firehose-parquet partitions shard`
- optional bounded-concurrency partition window execution mode
- shard/run-range command integration with partition-aware cursor templates
- alias/deprecation lifecycle tests and eventual legacy removal
