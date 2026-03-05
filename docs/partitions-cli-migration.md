# Partitions CLI Migration (Phases 1-3)

This document captures the first implementation slices for the `partitions <subcommand>` initiative.

Related issues:

- #182 (parent roadmap)
- #183 (CLI namespace and migration plan)
- #184 (`partitions ls` query command)
- #187 (partition-window ingestion mode)

## What phases 1-3 ship

1. Adds a grouped CLI namespace: `firehose-parquet partitions ...`
2. Introduces `firehose-parquet partitions resolve`
3. Keeps existing ingestion flags (`--partitions-index`, `--partition-type`, `--partition-value`) working for backward compatibility
4. Emits a deprecation warning in ingestion mode when those legacy flags are used to drive range resolution directly
5. Introduces `firehose-parquet partitions ls` for querying/filtering index rows
6. Adds ingestion-side partition window resolution via `--partition-from` + `--partition-to`

## Command behavior

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
- optional `--json` for automation output

### Backward compatibility

The existing ingestion flow still supports resolving ranges via legacy flags to avoid breaking active workflows. This is intentionally preserved for one migration window.

When legacy flags are used in direct ingestion mode (without explicit `--start-block/--stop-block`), a warning is logged to guide users toward:

`firehose-parquet partitions resolve ...`

## Process used for phases 1-3

1. Branch from `main` using `codex/` prefix.
2. Add CLI tree scaffolding in shared CLI crate (`firehose-parquet/src/cli.rs`).
3. Reuse existing partition index resolution logic and wrap it in command-focused result types.
4. Add partition row query/list command implementation with streaming record-batch reads.
5. Wire the binary entrypoint (`blocks/src/bin/main.rs`) to execute new subcommands.
6. Add docs and examples in `README.md`.
7. Add parsing/data-path test coverage for new commands.
8. Run formatting + targeted checks.

## Validation run in these phases

- `cargo fmt`
- `cargo test -p firehose-parquet test_partitions_resolve_subcommand_parse -- --nocapture`
- `cargo test -p firehose-parquet test_partitions_ls_subcommand_parse -- --nocapture`
- `cargo test -p firehose-parquet test_list_partitions_from_index_filters_sort_and_limit -- --nocapture`
- `cargo check -p blocks`
- `cargo check -p firehose-parquet`

## Follow-up phases (not included here)

- `firehose-parquet partitions build`
- `firehose-parquet partitions validate`
- `firehose-parquet partitions shard`
- optional bounded-concurrency partition window execution mode
- alias/deprecation lifecycle tests and eventual legacy removal
