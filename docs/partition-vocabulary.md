# Partition Vocabulary

This note records the one output partition key and the few CLI names that refer
to it.

## Decision

Every table is a Delta table whose data files are
`<table>/date=YYYY-MM-DD/part-*.parquet` (#652, #643). No command selects
another layout. fireparq keeps no partition index: the `partitions`
subcommands and `_fireparq/partitions.parquet` were removed in #653, and each
table's Delta log records the files of each `date` partition.

## Output directory key

Every table has one Hive-style directory key, the UTC day of the block time:

| Output | Directory |
|---|---|
| every table | `<table>/date=YYYY-MM-DD/` |

`firehose-parquet/src/date_partition.rs` is the one place the key is
formatted and parsed.

Why a single `date=YYYY-MM-DD` key:

- It is the table's `date` partition column (`date` in Delta, `Date32` in the
  mapper), which the Delta log records for every file (#643); the data files do
  not store it. It comes from the same whole-second block time as each row's
  `timestamp`, and the writer refuses a row whose mapper `date` disagrees with
  its partition.
- Readers (DuckDB `delta_scan`, Polars `scan_delta`) filter
  `date = DATE '2026-09-25'` and prune by the log, instead of combining `year`,
  `month` and `day` keys.

Earlier layouts are gone, with no compatibility handling. v0.x chose a layout
with `--partition`: unpartitioned (the default), `block_range`, `date`,
`hour`, `minute` or `second`. Its days were `year=YYYY/month=MM/date=DD/`, a
`date=DD` key that held only the day of the month, so DuckDB read the day
number over the `date` column and Polars failed to load it. Pre-release v1.0.0
builds renamed it to `year=YYYY/month=MM/day=DD/`. Every one of these layouts
was removed with `rollup` before the release (#652), because a single key
leaves nothing to roll up, and v1.0.0 files are Delta tables (#643).

## CLI terms

- `validate --cross-partition` checks block continuity between adjacent `date`
  partitions of a Delta snapshot.

## Resulting rename decisions

- `scan` removed (#643); DuckDB, Polars and the Delta log read the tables
- `build --partition` and `--block-range-size` removed (#652)
- `rollup` removed (#652)
- `partitions build/ls/validate/resolve/shard` and their flags (`--partition`,
  `--partition-type`, `--partition-value`, `--partition-chain`, `--from`,
  `--to`, `--all-spans`, ...) removed (#653)
- `truncate` and its `-p date=` filters, and `verify` with its per-`date=`
  roots, removed (#643)

These decisions are canonical. Deprecated aliases are not accepted.
