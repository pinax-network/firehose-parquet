# Partition Vocabulary

This note records the one output partition key and the few CLI names that refer
to it.

## Decision

Every table is written as `<table>/date=YYYY-MM-DD/part-*.parquet` (#652). No
command selects another layout. fireparq keeps no partition index: the
`partitions` subcommands and `_fireparq/partitions.parquet` were removed in
#653, and the Delta Lake output (#643) records the files of each `date`
partition in its log.

## Output directory key

Every table has one Hive-style directory key, the UTC day of the block time:

| Output | Directory |
|---|---|
| every table | `<table>/date=YYYY-MM-DD/` |

`firehose-parquet/src/date_partition.rs` is the one place the key is
formatted and parsed.

Why a single `date=YYYY-MM-DD` key:

- It has the type and the value of the `date` data column (`Date32`) that every
  table keeps: both come from the same whole-second block time, and the writer
  refuses a row whose `date` disagrees with its directory. A single file stays
  self-describing, and a Hive-partition-aware reader (DuckDB, Polars) sees one
  consistent `date` column.
- Readers filter `date = DATE '2026-09-25'` and prune directories, instead of
  combining `year`, `month` and `day` keys.
- The Delta Lake output (#643) partitions by `date` as well.

Earlier layouts are gone, with no compatibility handling. v0.x wrote
`year=YYYY/month=MM/date=DD/`: a `date=DD` key held only the day of the month,
so DuckDB read the day number over the `date` column and Polars failed to load
it. Pre-release v1.0.0 builds wrote `year=YYYY/month=MM/day=DD/` and offered
`hour`, `minute`, `second`, `block_range` and unpartitioned layouts; they were
removed with `rollup` before the release, because a single key leaves nothing
to roll up.

## CLI terms

- `truncate -p` / `--partition` takes `date=` filters: a date
  (`-p date=2026-01-15`) or one `*` glob over it (`-p "date=2026-01-*"`). A
  `date=` value that is not a date, such as `date=15`, and any other key are
  refused.
- `validate --cross-partition` checks block continuity between adjacent `date=`
  directories.
- `verify` reports roots per `date=` partition (`date=2026-01-15`), and marks the
  partitions `build` may still write as `open`.

## Resulting rename decisions

- `scan --rows` → `--limit`
- `build --partition` and `--block-range-size` removed (#652)
- `rollup` removed (#652)
- `partitions build/ls/validate/resolve/shard` and their flags (`--partition`,
  `--partition-type`, `--partition-value`, `--partition-chain`, `--from`,
  `--to`, `--all-spans`, ...) removed (#653)

These decisions are canonical. Deprecated aliases are not accepted.
