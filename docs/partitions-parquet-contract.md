# Partitions Parquet Contract

This document defines the canonical `partitions.parquet` layout consumed by current CLI readers.

## Scope

The contract applies to partition index artifacts consumed by:

- `fireparq partitions ls`
- `fireparq partitions shard`
- `fireparq partitions resolve`

## Required columns

These columns are required for current readers:

- `partition` — integer
- `start_block` — integer
- `stop_block` — integer

Range semantics:

- `start_block` is inclusive
- `stop_block` is exclusive

## Optional columns

These columns are optional:

- `chain` — UTF-8 string
- `start_time` — timestamp(second, UTC)
- `end_time` — timestamp(second, UTC)

Interpretation rules:

- when `firehose-parquet.partition = block_range`, `partition` stores the partition start block
- block-range output paths should use the same `[start_block, stop_block)` naming (for example `block_range=390500000-390600000`)
- otherwise `partition` stores UTC epoch seconds and readers render it as canonical `YYYY-MM-DD HH:MM:SS`
- `chain` may be stored once in file metadata (`firehose-parquet.chain_name`) for single-chain indexes or per row in the optional `chain` column for shared/global indexes

## Recommended file metadata

Current readers require:

- `firehose-parquet.partition`

Additional metadata used when present:

- `firehose-parquet.chain_name`
- `firehose-parquet.block_range_size` (required when `firehose-parquet.partition = block_range`)

Older experimental layouts such as `partition_type`/`partition_value` row columns, `end_block`, and unversioned compatibility metadata are no longer part of the supported contract.

## Lookup behavior

`partitions resolve` reads the canonical `partitions.parquet` artifact directly.

`partitions.parquet` remains the only source of truth for partition lookups.
