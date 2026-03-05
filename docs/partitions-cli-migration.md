# Partitions CLI Migration (Phase 1)

This document captures the first implementation slice for the `partitions <subcommand>` initiative.

Related issues:

- #182 (parent roadmap)
- #183 (CLI namespace and migration plan)

## What this phase ships

1. Adds a grouped CLI namespace: `firehose-parquet partitions ...`
2. Introduces `firehose-parquet partitions resolve`
3. Keeps existing ingestion flags (`--partitions-index`, `--partition-type`, `--partition-value`) working for backward compatibility
4. Emits a deprecation warning in ingestion mode when those legacy flags are used to drive range resolution directly

## Command behavior

### New command

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

## Process used for this phase

1. Branch from `main` using `codex/` prefix.
2. Add CLI tree scaffolding in shared CLI crate (`firehose-parquet/src/cli.rs`).
3. Reuse existing partition index resolution logic and wrap it in a command-focused result type.
4. Wire the binary entrypoint (`blocks/src/bin/main.rs`) to execute the new subcommand.
5. Add docs and examples in `README.md`.
6. Add parsing test coverage for the new command.
7. Run formatting + targeted checks.

## Validation run in this phase

- `cargo fmt`
- `cargo test -p firehose-parquet test_partitions_resolve_subcommand_parse -- --nocapture`
- `cargo check -p blocks`

## Follow-up phases (not included here)

- `firehose-parquet partitions build`
- `firehose-parquet partitions ls`
- `firehose-parquet partitions validate`
- `firehose-parquet partitions shard`
- alias/deprecation lifecycle tests and eventual legacy removal

