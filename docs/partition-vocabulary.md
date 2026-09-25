# Partition Vocabulary

This note records the CLI naming convention for partition-related flags.

## Decision

Use `--partition` for partition granularity or output layout, and keep `--partition-type` for partition-index lookup within `partitions.parquet` workflows.

## Terms

- `--partition`
  - Means the partition granularity being written, built, or targeted.
  - Examples:
    - main ingestion output layout (`--partition date`)
    - `partitions build` granularity (`--partition date`)
    - `rollup` destination granularity (`--partition hour`)

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

`--partition` modes write Hive-style directory keys:

| `--partition` | Directory keys |
|---|---|
| `block_range` | `block_range=<start>-<stop>/` |
| `date` | `year=YYYY/month=MM/day=DD/` |
| `hour` | `year=YYYY/month=MM/day=DD/hour=HH/` |
| `minute` | `.../hour=HH/minute=MM/` |
| `second` | `.../minute=MM/second=SS/` |

The mode is still called `date`, but its directory key is `day=`, never `date=`: every table has a canonical `date` data column, and Hive-partition-aware readers turn directory keys into columns. A `date=` key shadows the data column (DuckDB's default `hive_partitioning` reads it as the day-of-month number) or fails to load (Polars). Earlier releases wrote `date=DD`; `rollup` and `truncate` accept that legacy key as an alias of `day=`.

## Resulting rename decisions

This convention leads to the following CLI adjustments:

- `partitions build --partition-types` → `--partition`
- `rollup --target-partition` → `--partition`
- `scan --rows` → `--limit`

These rename decisions are now canonical. Deprecated aliases are no longer accepted.
