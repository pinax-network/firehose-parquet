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

## Existing Index

A bounded build never modifies an existing `partitions.parquet` implicitly. When the index already exists, one of these is required:

- `--resume`: continue from the stored frontier (the `stop_block` of the terminal row)
- `--overwrite`: ignore the stored rows and replace the index

Without either flag the build fails before probing, for both time-based and `block_range` partitions. Previously a time-based build silently replaced the index with only the new range, and a `block_range` build appended rows that overlapped the stored ones.

When resuming (bounded `--resume` or `--live`):

- the stored frontier is the start block; the sibling `cursor.parquet` and endpoint metadata are not consulted
- an explicit `--start-block` past the frontier is rejected, because the terminal row would otherwise be stretched across unprobed blocks (live mode requires an explicit value to equal the frontier)
- an explicit `--start-block` before the frontier is accepted and the build still resumes from the frontier
- time-based builds never backtrack past the frontier when locating the first partition, so a terminal row that ends mid-partition (after a live run) is extended instead of rebuilt
- a `block_range` terminal row shorter than `--block-range-size` is rebuilt from its aligned start rather than duplicated
- a bounded run whose `--stop-block` is already covered by the stored frontier is a no-op

For bounded builds, the first emitted row may expand downward from the requested seed block so that the stored row begins at the exact first block in that partition.

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

Within that artifact, `chain` is a required non-null column on every row.

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

Bounded mode uses `--start-block` / `--stop-block` as discovery seeds, then expands to the enclosing partition boundaries so each emitted row is exact. If the trailing boundary has not happened yet, bounded mode should fail instead of writing an inexact terminal row.

## Sparse Probe Methodology

`partitions build` should not stream every block just to inspect timestamps.

For both bounded and live mode, the intended implementation is:

- fetch individual finalized block identities as sparse probes
- use the Firehose single-block fetch RPC for exact-height probes
- use exponential search to jump ahead within a partition
- use binary search to find the exact first block of the next partition
- write contiguous `[start_block, stop_block)` rows to `partitions.parquet`
- create an initial checkpoint as soon as the first row can be materialized
- continue checkpointing long runs based on elapsed time and partition rollovers

If a sparse probe returns a missing/non-positive timestamp, the probe logic should borrow the nearest subsequent finalized block timestamp within a small bounded scan window (linear scan of up to 16 blocks) and log that normalization. If no timestamp is found within the small window, the probe falls back to an exponential forward search — doubling the jump distance on each step — so that chains with large timestamp-less ranges (e.g. Solana legacy blocks) can still be partitioned without streaming every block. If the exponential search also fails to find any reachable block with a timestamp, the build fails instead of silently partitioning at `1970-01-01 00:00:00`.

This keeps the command lightweight while still producing exact partition boundaries.
