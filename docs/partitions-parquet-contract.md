# Partitions Parquet Contract

This document defines the current compatibility contract for `partitions.parquet`.

## Scope

The contract applies to partition index artifacts consumed by:

- `firehose-parquet partitions ls`
- `firehose-parquet partitions shard`
- `firehose-parquet partitions resolve`
- ingestion-side partition selection (`--partitions-index`, `--partition-type`, `--partition-value`, `--partition-from`, `--partition-to`)

## Versioning

- Metadata key: `firehose-parquet.partitions.schema_version`
- Current version: `1`

Compatibility policy:

- Missing schema version is treated as a legacy/unversioned artifact and remains readable.
- Matching version (`1`) is accepted.
- Unknown/incompatible versions must be rejected by readers with a clear error.

This allows current readers to remain backward-compatible while giving future builders a stable version gate.

## Required columns

These columns are required for all supported readers:

- `partition_type` — UTF-8 string
- `partition_value` — UTF-8 string
- `start_block` — integer
- `end_block` — integer

Range semantics:

- `start_block` is inclusive
- `end_block` is exclusive

## Optional columns

These columns are optional but recommended:

- `chain` — UTF-8 string
- `partition_start_ts` — UTF-8 string in canonical `YYYY-MM-DD HH:MM:SS` UTC form
- `partition_interval_seconds` — integer
- `start_time` — timestamp or canonical string
- `end_time` — timestamp or canonical string

Current CLI readers rely on `partition_start_ts` when present for listing/sorting windows and otherwise fall back to `partition_value`.

## Recommended file metadata

When a builder writes `partitions.parquet`, it should include these file-level metadata keys in the `firehose-parquet.*` namespace:

- `firehose-parquet.partitions.schema_version`
- `firehose-parquet.partitions.generated_at`
- `firehose-parquet.partitions.source`
- `firehose-parquet.partitions.chain_scope`
- `firehose-parquet.partitions.partition_types`
- `firehose-parquet.partitions.min_start_block`
- `firehose-parquet.partitions.max_end_block`

Reader behavior for versioned artifacts:

- legacy/unversioned artifacts remain readable without these keys
- artifacts declaring schema version `1` must include these keys with valid values
- inconsistent coverage metadata (for example `min_start_block >= max_end_block`) must be rejected

## Additive vs breaking changes

Additive changes:

- adding new optional metadata keys
- adding new optional columns
- adding new nullable fields that existing readers can ignore

Breaking changes:

- renaming required columns
- changing required column meaning
- changing required column types incompatibly
- changing inclusive/exclusive block semantics
- changing canonical partition-value interpretation

Breaking changes require a schema-version increment.

## Optional lookup sidecar

Readers may use an optional sidecar placed alongside the canonical index:

- canonical index: `partitions.parquet`
- sidecar: `partitions.lookup.json`

Current reader behavior:

- `partitions resolve` checks the sidecar first when present
- falls back to scanning `partitions.parquet` when the sidecar is absent

Current sidecar contract:

- `lookup_schema_version` — current value: `1`
- `source_schema_version` — expected canonical source schema version (`1`)
- `entries[]` with:
  - `chain` (nullable string)
  - `partition_type` (string)
  - `partition_value` (string)
  - `start_block` (u64)
  - `end_block` (u64)

Notes:

- The sidecar is an optimization only; `partitions.parquet` remains the source of truth.
- Unsupported sidecar schema versions are rejected.
- If no sidecar is present, readers continue with canonical parquet scans.
