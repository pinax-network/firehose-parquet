# Partitions CLI Migration (Phases 1-7)

This document captures the first implementation slices for the `partitions <subcommand>` initiative.

Historical note: the main `fireparq build` ingestion flow now uses explicit
`--start-block` / `--stop-block` bounds or `--live` mode only. Partition-index
lookups stay under `fireparq partitions ...`, typically via `partitions resolve`
followed by an explicit `fireparq build`.

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

1. Adds a grouped CLI namespace: `fireparq partitions ...`
2. Adds `fireparq partitions build` for generating canonical `partitions.parquet` artifacts directly from Firehose
3. Introduces `fireparq partitions resolve`
4. Supports ingestion-side partition selection through `--partitions-index`, `--partition-type`, and `--partition-value`
5. Uses `fireparq build` as the only ingestion entrypoint
6. Introduces `fireparq partitions ls` for querying/filtering index rows
7. Adds ingestion-side partition window resolution via `--partition-from` + `--partition-to`
8. Adds partition-aware cursor templating via `--cursor-template`
9. Adds deterministic partition sharding via `partitions shard`
10. Adds partition-index integrity validation via `partitions validate`

## Command behavior

### Partition index builder

The grouped CLI now supports writing the canonical partition index directly from Firehose:

- `fireparq partitions build`

Current behavior:

- scans the requested `[start_block, stop_block)` range from Firehose
- computes UTC interval starts for `date`, `hour`, `minute`, and `second`
- emits one row per discovered partition with contiguous `[start_block, stop_block)` bounds
- writes `/<chain>/partitions.parquet` under the supplied local or S3 output root
- includes file metadata defined in `docs/partitions-parquet-contract.md`
- supports `--resume` by reusing trailing partition rows from the existing canonical artifact and continuing from the stored frontier
- refuses to modify an existing canonical artifact in bounded mode unless `--resume` or `--overwrite` is passed (see `docs/partitions-build-defaults.md`)
- infers `--start-block` from a sibling `cursor.parquet` or endpoint metadata when omitted
- infers the S3 output root from `--s3-bucket` / `S3_BUCKET` when `--output` is omitted
- supports `--live` to keep extending `partitions.parquet` from its latest covered frontier
- uses sparse finalized block probes instead of streaming every block for partition discovery

Current limitations:

- requires exactly one `--partition` value per run

### Explicit resolve-then-build flow

Current ingestion keeps partition index usage under the grouped CLI:

- `fireparq partitions resolve`
- `fireparq build --start-block ... --stop-block ...`

### Partition sharding mode

The grouped CLI now supports deterministic shard assignment:

- `fireparq partitions shard --shard-count N --shard-index K`

Supported strategies:

- `ordinal` — sorted row ordinal modulo shard count
- `hash` — stable SHA-256-based hash of `(chain, partition_type, partition_value)` modulo shard count

Behavior:

- shards are computed from the same filtered partition set as `partitions ls`
- no row should appear in more than one shard for fixed inputs
- combined shard outputs cover the full selected set

### Partition validation mode

The grouped CLI now supports integrity validation for `partitions.parquet`:

- `fireparq partitions validate`

Current checks:

- invalid ranges (`start_block >= stop_block`)
- overlaps between adjacent rows in the same `(chain, partition_type)`
- gaps between adjacent rows unless `--allow-gaps` is enabled
- ordering issues based on the numeric partition value

### New command

`fireparq partitions ls` lists index rows with optional filters:

- optional `--partition-type`
- optional `--partition-chain`
- optional `--from` / `--to` time window
- `--limit` (default `100`)
- optional `--json`

Output is sorted ascending by the numeric partition value (start block for `block_range`, UTC epoch seconds otherwise), and JSON mode returns machine-readable rows for schedulers/UI. `--from` / `--to` are compared numerically too, so they take a start block for `block_range` indexes and `YYYY-MM-DD HH:MM:SS` otherwise.

Implementation note: rows are read in record batches, filtered, sorted by ascending partition value, and truncated to `--limit`.

### Existing command

`fireparq partitions resolve` resolves a single partition row from `partitions.parquet` and returns exact block bounds:

- `start_block`: inclusive
- `stop_block`: exclusive

Inputs:

- `--partitions-index` (local path or `s3://` URI)
- `--partition-type`
- `--partition-value`
- optional `--partition-chain`
- optional `--strict-single-chain`
- optional `--json` for automation output

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
- `cargo test -p firehose-parquet test_partition_index_builder_resume_extends_terminal_rows -- --nocapture`
- `cargo test -p firehose-parquet test_write_and_read_partitions_build_rows_round_trip -- --nocapture`
- `cargo test -p firehose-parquet test_partitions_resolve_subcommand_parse -- --nocapture`
- `cargo test -p firehose-parquet test_partitions_ls_subcommand_parse -- --nocapture`
- `cargo test -p firehose-parquet test_list_partitions_from_index_filters_sort_and_limit -- --nocapture`
- `cargo check -p blocks`
- `cargo check -p firehose-parquet`

## Follow-up phases (not included here)

- `fireparq partitions validate`
- `fireparq partitions shard`
- optional bounded-concurrency partition window execution mode
- shard/run-range command integration with partition-aware cursor templates
- additional partition workflow polish
