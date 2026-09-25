# Issue #515: adaptive compressed file sizing

Status: implemented and under independent review. Current-main full validation
is in progress; no PR has been published. This builds on protected ingestion
transactions (#600), with main #601/#510 (`9379883`) integrated.

## Diagnosis and implementation

The old build callback compared `flush_bytes` directly with the largest mapper
table's logical byte estimate. Every flush is a complete all-table transaction,
so no later writer buffer could accumulate enough rows to meet a compressed
file target. Config and rollup used 128 MiB defaults while build and merge CLI
used 32 MiB.

`flush::FlushSizing` now predicts the largest physical output file. It starts at
a compressed/logical ratio of 1.0 for a conservative calibration flush. Only a
successful transaction supplies feedback: maximum committed table bytes divided
by the preflush maximum mapper estimate. Those maxima may belong to different
tables. The first qualifying sample replaces the initial ratio; later samples
use an equal-weight moving average. Ratio bounds are 1/1024 through 1024 and
predictions/sums saturate instead of overflowing. Zero-row transactions and
samples below `min(target/4, 1 MiB)` do not train the ratio. Failed ingestion commits
never reach the observation helper.

All eight mappers expose their existing per-table calculations through the new
**required Rust trait method** `BlockMapper::table_estimates()`. Implementers of
that public trait must provide the complete table inventory's estimates;
`largest_table()` has a default implementation selecting the maximum. This is a
Rust source compatibility change for external mapper implementations. The full
nonempty chain/schema matrix asserts every emitted table has an estimate.

Build checks the independent sum first after each mapped block:

1. `--flush-memory-bytes` defaults to **256 MiB**, accepts positive values only,
   and flushes when the sum of all mapper table estimates reaches the threshold.
2. `--flush-bytes` targets the largest compressed file, now **32 MiB everywhere**:
   Config, build, merge and rollup. Zero disables this target only; the independent
   memory threshold stays active.
3. Existing row, block and interval triggers remain. Partition changes and clean
   EOF also force a flush. Shutdown/errors keep discard-and-replay behavior.

All-table journal, authority, cursor, accepted-prefix and recovery ordering stay
unchanged. Ratio state is transient and resets on restart. Dry runs never learn
because they create no committed files. Tuning thresholds does not change the
immutable stream descriptor. Structured commit logs expose the maximum physical
size, maximum mapper estimate, summed estimate and learned ratio. The new
`firehose_parquet_mapper_buffer_estimated_bytes` gauge exposes the sum.

## Limits and default tradeoff

**This is an estimated mapper accumulation threshold, not a process RSS limit.**
Estimates count populated values, not allocator reserve. Protobuf decoding,
unresolved bootstrap payloads, all-table Arrow materialization, the active
Parquet encoder and encoded output need additional memory. A single mapped block
can exceed the threshold before the next check. The transaction retains all
batches until completion. The configured threshold therefore cannot bound RSS
for arbitrary single blocks.

Adaptive windows can use substantially more memory than the former 32 MiB
largest-table trigger. The new independent threshold makes that tradeoff explicit;
operators can lower it. **The 32 MiB file target can be missed when memory,
partition or another trigger wins.** It is not a file-size minimum or maximum.
Highly compressible data may hit the memory threshold indefinitely. Increasing
the target does not disable the memory threshold.

The 256 MiB default is a conservative operational choice, not an inferred optimum
for all chains or a conclusion about real traffic from repeated fixtures. The
512 MiB alternative remains configurable and was measured separately. Rollup
and merge (#522) have a distinct active row-group budget; crossing that budget
flushes a row group, not necessarily a file.

## Offline measurement

[Machine-readable results](515-sizing-results.json) retain source SHA256 values,
all window counts/rows/sizes, estimates, ratios and observed peak RSS. Measurements
were collected during implementation on `e4bd9cf`; `dda3ada` contains the
reproduction harness. Initial repeated-payload runs used the identical sizing
policy locally in the example before its extraction into `FlushSizing`; later
varied-byte runs use that production module. Subsequent main changes affect
Cosmos/transport, not these EVM/Solana schemas. The release
example `blocks/examples/measure_flush_sizing.rs` uses actual EVM/Solana mappers
and the production local Zstd table writer, checks file lengths against receipts,
and deletes only its own temporary outputs. It makes no network requests and
uses no credentials or cursor data. Protected transaction footer overhead and
its readback/recovery memory are not simulated by this low-level writer; actual
CLI tests separately prove integration through committed transaction receipts.

Retained inputs are EVM block 26049575 (5,049 mapped rows) and Solana slots
300000000/300000001 (9,765/9,949 mapped rows). Replaying these small payload sets
can improve compression by orders of magnitude. The following are **sizing
simulations, not real network throughput or a production traffic distribution**.

### Repeated payloads, default 32 MiB target

| Case | Old raw-byte trigger: largest file | Adaptive, 256 MiB estimate cap | Adaptive, 512 MiB estimate cap |
|---|---:|---:|---:|
| EVM | 0.221 MiB | 0.418 MiB | 0.695 MiB |
| Solana | 0.672–0.676 MiB | 1.175 MiB | 2.069–2.076 MiB |

**Neither adaptive cap reaches the 32 MiB target on this repeated corpus.**
After calibration, every window is memory-triggered. At 256 MiB the observed
summed estimates were 257.9 MiB for EVM and up to 260.2 MiB for Solana (one-block
overshoot). At 512 MiB they reached 512.1/516.1 MiB. These runs observed roughly
622–670 MiB RSS at the lower cap and 1,209–1,212 MiB at the higher cap; those are
harness observations, not configured process limits.

### Target-eligible repeated payloads: 24 windows

To separate adaptation from a dominating memory cap, smaller file targets were
also tested with the 256 MiB summed threshold:

| Case | Target | Old largest files | Stable adaptive largest files | Stable summed estimate |
|---|---:|---:|---:|---:|
| EVM | 256 KiB | 60.0% of target | 102.0% (last four windows) | 93.4 MiB |
| Solana | 1 MiB | 14.5–19.0% | 100.9% (last seven windows) | 213.2 MiB |

Every window in these cases is byte-triggered. Initial convergence is slower
because growing a repeated-data window also improves footer/dictionary
amortization: EVM reached 83.8% by window 12 and Solana reached 95.3%. The full
24-window series is retained rather than excluding calibration from the data.
Observed process peaks were 602.5/732.0 MiB, reinforcing that logical estimates
must not be presented as RSS bounds.

### Varied-entropy synthetic EVM, default 32 MiB target

`--vary-bytes` deterministically replaces selected EVM transaction, call, log,
and storage byte values using a domain-separated SHA256 expansion. Every changed
field keeps its length; counts, status/reversion flags and other scalars remain
unchanged. Equal source values within one replay use the same replacement. This
retains field/row shapes while removing cross-replay byte repetition. It is
explicitly synthetic data, not another captured chain range.

The old trigger wrote largest files at **27.5% of target** (19 blocks). With the
256 MiB threshold, adaptive files reached **99.8–99.9%** (69 blocks, 257.9 MiB
estimate), but those windows are correctly classified as **memory-triggered**.
With a 512 MiB threshold, windows were **byte-triggered** and settled at **101.3%**
of target for ten consecutive windows (70 blocks, 261.7 MiB estimate). Observed
RSS peaks were about 755/998 MiB respectively. The default-target result therefore
has explicit evidence both with and without a dominating memory trigger.

Example reproduction using the checked-in EVM raw fixture:

```sh
cargo run --release --locked -p blocks --example measure_flush_sizing -- \
  --family evm --block blocks/tests/fixtures/evm-mainnet/block.pb \
  --target 33554432 --memory 536870912 --windows 12 --adaptive --vary-bytes
```

For unchanged-payload cases omit `--vary-bytes`; omit `--adaptive` for the old
trigger. Solana accepts two repeated `--block` arguments. The checked-in EVM
golden fixture and retained input were verified byte-identical (SHA256
`dad74257d32c66a056404add2a5f3360c288faedf0a026e5698394cd9d231b2a`).

## Validation

Focused checks passed before current-main integration: 18 core flush/config
checks, three binary trigger checks, nine actual CLI transaction tests and four
all-chain schema/identity tests. New CLI cases prove committed compressed receipts
expand the next flush window, and a summed estimate triggers memory flushing
while every individual table remains below its threshold. Existing restart,
zero-row, genesis/lookahead, stop-extension, sparse EOF and legacy-refusal cases
remain intact. Full workspace, examples, telemetry resets and final validation
will be recorded here before publication.
