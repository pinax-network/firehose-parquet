# Partitions Build Defaults

This note records the intended defaults and inference rules for `fireparq partitions build`.

## Decision

`fireparq partitions build` should be easy to resume and automate without repeating values that can be inferred safely, while still keeping the build range bounded and deterministic.

## Start Block

`--start-block` is optional.

Bounded-mode resolution order:

1. explicit `--start-block`
2. sibling `cursor.parquet` under the resolved chain output root
3. Firehose endpoint `first_streamable_block_num`

This lets operators resume partition index generation from an existing artifact directory without manually looking up the last written block.

## Live Mode

`fireparq partitions build --live` is the intended follow-up to a bounded backfill.

In live mode:

- `partitions.parquet` is the restart anchor
- the process resumes from the latest covered frontier already stored in that file
- `--stop-block` is not used
- `--poll-interval-secs` controls how long the process waits between sparse live probes
- no `partitions.lookup.json` sidecar is involved

The public artifact remains one stable canonical path:

- `partitions.parquet`

Implementations may still use temporary or staging paths internally for safe rewrites, but the canonical artifact name stays stable.

## Output Root

`--output` is optional when `--s3-bucket` (or `S3_BUCKET`) is set.

- `--output ./prefix --s3-bucket my-bucket` resolves to `s3://my-bucket/prefix`
- `--s3-bucket my-bucket` resolves to `s3://my-bucket`
- an explicit `s3://...` `--output` remains authoritative

This keeps S3-oriented automation concise while preserving explicit override behavior.

## Stop Block

`--stop-block` remains required for bounded mode.

Unlike the main ingestion pipeline, `partitions build` produces a canonical bounded artifact (`partitions.parquet`). Requiring a finite upper bound keeps that artifact deterministic, reviewable, and safe to rerun in scheduled jobs.

In short:

- ingestion may stream indefinitely
- bounded `partitions build` must describe a closed coverage interval
- `partitions build --live` keeps extending the canonical artifact from its stored frontier

## Sparse Probe Methodology

`partitions build` should not stream every block just to inspect timestamps.

For both bounded and live mode, the intended implementation is:

- fetch individual finalized block identities as sparse probes
- use exponential search to jump ahead within a partition
- use binary search to find the exact first block of the next partition
- write contiguous `[start_block, end_block)` rows to `partitions.parquet`

This keeps the command lightweight while still producing exact partition boundaries.
