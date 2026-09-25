# Issue #515: adaptive compressed file sizing

Status: design and offline measurement in progress. Runtime behavior is not yet
changed. This work follows the protected ingestion transaction runtime (#600).

## Diagnosis

The build callback compares `flush_bytes` directly with the largest mapper
table's logical byte estimate. Every mapper flush is a complete all-table
transaction, so there is no later writer buffering stage which can accumulate
enough rows to approach a compressed file target. The former writer ratio cannot
correct this early flush. Config and rollup use 128 MiB defaults while build and
merge CLI use 32 MiB.

All eight mapper implementations already calculate per-table estimates, then
discard every value except the maximum. Exposing that inventory permits a
separate summed-estimate trigger without treating the largest table as the whole
mapper's memory usage. These estimates count populated values, not reserved
allocator capacity; they are not a process RSS limit.

## Proposed policy and scope

- Keep the shared target at 32 MiB, preserving build and merge CLI behavior's
  configured number; use one constant in Config and build/merge/rollup defaults.
- Interpret the build target as the largest compressed part in an all-table
  flush. Start with an estimated compressed/logical ratio of 1.0, then train only
  after a successful transaction returns its exact physical file receipts.
- The sample is maximum committed table bytes divided by the preflush maximum
  mapper estimate. Use the first qualifying sample directly and an equal-weight
  moving average thereafter, with finite nonzero bounds and saturating byte
  prediction. Ignore zero-row transactions and tiny samples below the lesser of
  target/4 and 1 MiB, where Parquet footer overhead would dominate the ratio.
- Add a separate positive, finite summed mapper estimate threshold. Evaluate it
  first after every mapped block, independently of the target. Measure both
  256 MiB and 512 MiB before choosing its default. `flush_bytes = 0` disables the
  compressed target only; it does not remove this memory trigger.
- Preserve partition, row, block, interval and clean EOF boundaries. These and
  the memory threshold can produce files smaller than the target. A single
  mapped block can overshoot either threshold. Shutdown and failures retain the
  existing discard/replay behavior and never train from an unsuccessful commit.
- Sizing observations are transient process tuning. A restart begins with a new
  calibration flush. No schema, journal, cursor, identity or recovery semantics
  change, and this task does not decompose the ingestion callback (#525).

The summed threshold excludes protobuf decoding, unresolved bootstrap payloads,
allocator reserve, materialized Arrow batches, and the active Parquet encoder
and encoded output. All tables are retained until their transaction completes.
The threshold bounds estimated mapper accumulation at block checkpoints, with
one-block overshoot; it cannot promise bounded RSS for an arbitrarily large
single block. Those distinctions must appear in CLI help and README guidance.

## Measurement protocol

`blocks/examples/measure_flush_sizing.rs` reads caller-supplied retained raw EVM
or Solana payloads. It maps repeated fixtures through the current real mappers,
records maximum and summed estimates, flushes all tables through the production
Zstd table writer, and checks every receipt against the file length. No network,
credentials, cursor values or production storage is involved.

This is a repeated-fixture **sizing simulation**, not real network throughput or
an unbiased production data distribution. Repetition improves compression and
can make the memory threshold dominate; that is an explicit result, not a
reason to remove the cap. Source hashes, replay counts, per-table file sizes and
Arrow allocation observations will be retained with the results. Transaction
footer overhead and protected publication/recovery memory are not simulated by
the low-level table writer.

The experiment compares the old largest-logical-byte trigger with adaptive
sizing, then compares 256/512 MiB memory thresholds. It will separate calibration,
target-triggered, memory-triggered and final partial files when reporting how
closely eligible files approach the target. Peak process RSS, if recorded, is an
observation on this harness and machine, never the configured memory guarantee.

## Validation plan

Exercise ratio initialization, bounded/overflow-safe arithmetic, successful-only
feedback, zero/tiny samples, changing table dominance, memory-first decisions,
target-disabled behavior, forced flushes, and one-block overshoot. Preserve
existing all-chain mapping/schema and protected transaction/CLI replay tests.
Add real CLI coverage proving committed compressed sizes affect later flushes
and the summed estimate forces a flush even when each table is below its limit.
The independent rollup work (#522) separates row-group memory flushing from file
closure; its memory budget remains distinct from this all-table mapper threshold.
