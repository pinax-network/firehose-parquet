# Partition Vocabulary

This note records the CLI naming convention for partition-related flags.

## Decision

Use `--partition` for partition granularity or output layout, and keep `--partition-type` for partition-index lookup within `partitions.parquet` workflows.

## Terms

- `--partition`
  - Means the partition granularity being written, built, or targeted.
  - Examples:
    - main ingestion output layout (`--partition date`)
    - `partitions build` granularities (`--partition day,hour`)
    - `rollup` destination granularity (`--partition hour`)

- `--partition-type`
  - Means the partition dimension to query inside a canonical partition index.
  - Examples:
    - `partitions ls --partition-type hour`
    - `partitions resolve --partition-type day`
    - ingestion-side partition selection with `--partitions-index`

- `--partition-value`
  - Means one exact partition key value inside the selected `--partition-type`.

- `--partition-from` / `--partition-to`
  - Mean an inclusive/exclusive partition-value window inside the selected `--partition-type`.

- `--partition-chain`
  - Means an optional chain filter when a partition index contains multiple chains.

## Resulting rename decisions

This convention leads to the following CLI adjustments:

- `partitions build --partition-types` → `--partition`
- `rollup --target-partition` → `--partition`
- `scan --rows` → `--limit`

Deprecated aliases remain accepted for one compatibility window so existing scripts keep working while docs and examples move to the new names.
