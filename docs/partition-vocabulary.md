# Partition Vocabulary

This note records the CLI naming convention for partition-related flags and
the output directory key.

## Decision

`build` has no partition flag: every table is written as
`<table>/date=YYYY-MM-DD/part-*.parquet` (#652). `--partition` names the
granularity of a `partitions build` index, and `--partition-type` names the
partition dimension to query inside `partitions.parquet`. The `partitions`
subcommands and their flags are scheduled for removal in v1.1.0 (#653).

## Terms

- `--partition`
  - Means the granularity of the index `partitions build` writes
    (`--partition date`, `hour`, `minute`, `second` or `block_range`).
  - `truncate -p` reuses the short flag for its `date=` filters
    (`-p date=2026-01-15`, `-p "date=2026-01-*"`).

- `--partition-type`
  - Means the partition dimension to query inside a canonical partition index.
  - Examples:
    - `partitions ls --partition-type hour`
    - `partitions resolve --partition-type date`

- `--partition-value`
  - Means one exact partition key value inside the selected `--partition-type`.

- `--partition-from` / `--partition-to`
  - Mean an inclusive/exclusive partition-value window inside the selected `--partition-type`.

- `--partition-chain`
  - Means an optional chain filter. Verified v2 indexes have one chain; legacy inspection can still filter multi-chain files.

- `--all-spans`
  - Requires `--json` on `partitions resolve` and returns separate complete runs for a repeated calendar key, within declared finalized coverage. It never merges intervening blocks into one range.

## Output directory keys

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
- The planned Delta Lake mode (#643) partitions by `date` as well.

Earlier layouts are gone, with no compatibility handling. v0.x wrote
`year=YYYY/month=MM/date=DD/`: a `date=DD` key held only the day of the month,
so DuckDB read the day number over the `date` column and Polars failed to load
it. Pre-release v1.0.0 builds wrote `year=YYYY/month=MM/day=DD/` and offered
`hour`, `minute`, `second`, `block_range` and unpartitioned layouts; they were
removed with `rollup` before the release, because a single key leaves nothing
to roll up. `truncate` accepts only `date=` filters, and a `date=` value that is
not a date, such as `date=15`, is refused.

## Resulting rename decisions

This convention led to the following CLI adjustments:

- `partitions build --partition-types` → `--partition`
- `scan --rows` → `--limit`
- `rollup --target-partition` → `--partition`, later removed with `rollup` (#652)
- `build --partition` and `--block-range-size` removed (#652)

These decisions are canonical. Deprecated aliases are not accepted.
