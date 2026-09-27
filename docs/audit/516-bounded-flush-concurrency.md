# Issue #516, stage A: bounded concurrent table work inside one flush

Status: stage A of the [reviewed design](516-concurrency-design.md) is
implemented and qualified locally. Stage B (overlapping the gRPC stream and
mapping with the writer) is out of scope; its requirements are listed in the
design's [stage B follow-up](516-concurrency-design.md#stage-b-follow-up).

## What was serial

Every protected flush encoded, staged, journaled and published its tables one
after another inside `TransactionController::commit`: one table's Parquet
encode, then its local staging (file write, fsync, directory fsync), then one
journal CAS for its receipt, then its publication and fsyncs, then the next
table. Remote publication also held the S3 owner's control lock across each
PUT and readback, so even an overlapping implementation could not have issued
two data requests at once. The Firehose callback waits for the whole commit.

## Contract kept

The session and journal are unchanged: `IngestionSession` receive, freeze and
acknowledge semantics, one pending Writing transaction, the journal format,
all-table preflight before Writing, receipt before publication, verification
of every final before Committed, and Committed → authority → mirror → cleanup →
clear. Only a fully successful commit returns to the runtime, so #515 sizing
feedback and flush counters still train once per commit, in order.

## Implementation

`controller/pipeline.rs` runs the work between Writing and final verification.
One coordinator task owns the versioned pending record:

- **Encoders.** At most `--flush-encode-concurrency` (default 2) blocking
  encoders run at once. Each is admitted only when its byte reservation fits,
  and only then spawned: nothing is spawned and merely throttled at the await.
  The closure owns an `Arc` of the immutable prepared batches and writes a
  private buffer (or anonymous native-S3 spool); it touches no owned path.
- **Byte budget.** `writer/protected/budget.rs` holds `--flush-inflight-bytes`
  (default 256 MiB). A part's initial reservation is the codec's typical
  compressed share of its Arrow buffers plus a 256 KiB footer floor. The encoder
  grows the reservation as it writes, never blocking; if the budget refuses, the
  encoder stops with `ReservationExceeded`, releases everything, and the same
  deterministic part is encoded again later with an exclusive reservation. The
  reservation shrinks to the exact encoded size and is released when the part
  is dropped: after its publication, or on any failure path, including a
  detached encoder after cancellation. Reserved bytes therefore never exceed the
  budget, except one exclusive part larger than the whole budget, which runs
  alone and logs an overshoot warning. The budget counts encoded parts that are
  encoding, staged or publishing; it is not an RSS cap (mapper batches, encoder
  working memory and native-S3 readback spools are additional).
- **Receipts.** One journal CAS runs at a time. Receipts of every part that
  finished staging while the previous CAS was in flight go into the next one
  (`TransactionStateStore::record_receipts`, which validates each receipt as
  before). No part is published before the CAS carrying its receipt returns.
- **Publications.** At most `--flush-publish-concurrency` (default 4) parts
  publish at once, only after their receipts are durable. S3 publications are
  futures polled by the coordinator. `S3PartStore` now checks the owner's
  uncertainty latch under the control lock, then the persistent owner record,
  then the latch again, and no longer holds the lock across the PUT, readback
  or verification; in-memory verification hashing runs on a blocking worker.
  An unresolved attempt still marks the owner uncertain and every later gate
  refuses to start, with no retry or release.
- **Local I/O lane.** Local staging, publication and final verification are
  blocking file operations that borrow the ownership guard, so they cannot move
  to detached tasks. With more than one publication on a multi-thread runtime,
  `controller/lane.rs` runs them on `--flush-publish-concurrency` scoped threads
  (`std::thread::scope` inside `block_in_place`): every thread is joined before
  the commit can return, even on panic, and job panics become errors. On a
  current-thread runtime (or with one publication) they stay inline. The local
  part store takes the control lock only to resolve and revalidate paths; file
  I/O on the distinct transaction-owned names runs without it. Control records
  keep their locked version checks.
- **Final verification.** All finals are verified with the same concurrency
  (local on the lane, remote as concurrent read-only futures) before Committed.
- **Errors.** The first error stops admission and drops unstarted parts (their
  reservations with them). Every started encoder, receipt CAS, staging job and
  publication is drained before the error is returned. The journal stays
  pending, the controller is poisoned, and the existing cleanup removes this
  transaction's staging names. Authority and the mirror are never advanced.

`Config::flush_concurrency` carries the limits; `build` exposes
`--flush-encode-concurrency` / `FLUSH_ENCODE_CONCURRENCY` (1-64),
`--flush-publish-concurrency` / `FLUSH_PUBLISH_CONCURRENCY` (1-64) and
`--flush-inflight-bytes` / `FLUSH_INFLIGHT_BYTES` (at least 1). `.env.example`
lists them. `--flush-inflight-bytes 1` makes table work strictly one part at a
time. The committed-flush log line adds `commit_ms`, `files`, `peak_encoders`,
`peak_publications`, `peak_inflight_bytes` and `reencoded_parts`; the peaks
count actually executing encoder closures and publication futures.

Defaults are conservative: two encoders keep extra CPU and one extra encoded
part modest, and four publications already capture most of the local fsync
overlap and remote latency hiding measured below.

## Tests

Unit (core library):

- `budget` tests: growth, exact fallback, shrink, release on drop, and the single
  exclusive overshoot that blocks all other reservations.
- `lane` test: at most N threads, a panicking job reported as an error, a closed
  lane refusing work, all jobs joined before the scope ends.
- `parallel_commit_publishes_serial_bytes_with_bounded_work` (in-memory S3 with
  delayed PUTs): object keys and bytes identical to serial mode; concurrent part
  PUTs measured at the store stay within the publication limit and do overlap;
  encoder peaks within the limit; the store checks at every part PUT that the
  journal already holds that part's receipt.
- `byte_budget_bounds_inflight_parts_and_reencodes_refused_growth`: incompressible
  parts against a small budget stay within it, with refused growth re-encoded;
  one part larger than the budget is admitted alone; bytes equal serial output.
- `remote_lost_acknowledgement_drains_parallel_work_without_advancing_authority`:
  after an accepted-but-lost PUT, no PUT is left running, authority and mirror
  are unchanged, the Writing journal and uncertain owner are retained, and
  release is refused.
- `parallel_encode_publish_and_lost_ack_failures_recover_rows_exactly_once`
  (local, lane active): encode, publish and lost-acknowledgement faults leave
  authority unchanged and no staging names; recovery removes owned parts and
  the replayed commit has every row exactly once.
- `every_publication_boundary_recovers_to_one_complete_all_table_prefix` now runs
  every injected boundary in serial and parallel mode;
  `abrupt_process_death_recovers_parts_and_checkpoint` adds parallel subprocess
  deaths after the first and second publication.
- `batched_receipts_persist_together_and_keep_every_receipt_rule` (store).

Real binary (`blocks/tests/ingestion_transactions.rs`, retained real EVM block,
5,049 rows over 14 tables):

- `parallel_flush_matches_strict_serial_bytes_on_the_retained_evm_block`: strict
  serial, 4/4, 3/2 with a 600,000-byte budget, and defaults, all into the same
  canonical root, produce identical part names and bytes, authority payload and
  mirror row. Logged peaks respect every limit and show overlap.
- `parallel_flush_faults_recover_every_row_exactly_once`: in 4/4 mode, a failed
  `transactions` encode, a failed `logs` publication, a lost `calls`
  acknowledgement and a process abort right after publishing
  `balance_changes` each leave authority and the mirror bytes unchanged with a
  Writing journal (and the published orphan owned by it where applicable). The
  next ordinary run recovers and every table has exactly the clean reference
  row count. Faults are injected with `FIREPARQ_DEBUG_FAULT=<kind>:<table>`,
  read only by debug builds (as built by `cargo test`); release builds compile
  the checks to `false`.
- The existing storage-failure test pins `--flush-inflight-bytes 1` so its
  published-orphan scenario stays deterministic.

Validation on the branch rebased onto main `df551b4`: `cargo fmt --all --check`
and `cargo test --workspace --locked` passed **1,221 tests with 15 intentional
ignores**; the CI example test and `cargo build --bin fireparq` (no warnings)
also passed.

## Benchmark

Input: 200 finalized Ethereum mainnet blocks `[26049575, 26049775)` (487.9 MB of
raw Firehose payloads), captured once through the Pinax endpoint into a private
local file. All measurements are offline; the complete numbers, including every
run's load average, are in [516-benchmark.json](516-benchmark.json).

Machine: Apple M1 Max (10 cores), macOS 26.5.1, APFS SSD, rustc 1.93.1, release
builds. The machine was shared with other agents building and testing
concurrently, which made local disk latency very noisy; medians and the best of
five interleaved rounds are both reported.

### Local output, real CLI

`blocks/examples/bench_ingestion_concurrency.rs replay` serves the capture from
a loopback Firehose and runs `fireparq build` into the same fresh local root for
each run (`base` = binary built from merge-base `050f9ec`). Every run of every
setting, including `base`, produced byte-identical part names and contents.

Default flush thresholds (5 flushes, 70 parts, 77.8 MB):

| Setting | Wall s (median / best) | Flush s total (median / best) | Flush p50 ms (median / best) |
|---|---:|---:|---:|
| base | 9.51 / 6.11 | 8.11 / 5.08 | 1285 / 1169 |
| strict 1/1/1 B | 7.30 / 6.37 | 6.25 / 5.33 | 1320 / 1161 |
| 1/1 | 9.38 / 6.72 | 8.30 / 5.65 | 1195 / 1008 |
| default 2/4 | 9.18 / 4.76 | 7.62 / 3.64 | 1439 / 654 |
| 4/8 | 7.67 / 3.15 | 6.10 / 2.13 | 657 / 434 |
| 8/8 | 6.26 / 3.05 | 4.89 / 2.02 | 534 / 421 |

`--flush-blocks 20` (10 flushes):

| Setting | Wall s (median / best) | Flush s total (median / best) | Flush p50 ms (median / best) |
|---|---:|---:|---:|
| base | 11.75 / 10.13 | 10.66 / 9.04 | 978 / 777 |
| strict 1/1/1 B | 11.90 / 10.83 | 10.82 / 9.78 | 930 / 799 |
| default 2/4 | 7.23 / 6.71 | 6.08 / 5.56 | 524 / 493 |
| 4/8 | 9.67 / 5.64 | 8.54 / 4.52 | 452 / 407 |

Whole-process CPU stayed at about 3.3 s user (3.7 s with 20-block flushes) in
every setting, and peak RSS rose by about 20-80 MiB (for example 923 MiB base,
941 MiB default, 1001 MiB at 8/8). Local flushes are dominated by full fsyncs,
not encoding: a separate probe on this disk completed 120 file-plus-directory
full syncs in 3.3 s serially, 1.2 s on four threads and 0.6 s on eight, which is
the overlap the local lane exploits. Strict mode matches `base`, so the new code
path adds no serial overhead. In the best rounds the defaults cut total flush
time by about 30-40% and p50 flush latency by about 40%, and 4/8 or 8/8 by about
50-60%; medians show the same direction but disk contention from the other
agents dominated several rounds.

### S3 output, modeled latency

The ignored `s3_replay_benchmark` replays the five committed transactions of one
local run (70 parts) through the real controller into an in-memory object store
where every request waits 25 ms plus its size at 100 MB/s. This models request
latency; it is not live S3 throughput. Three rounds; every setting produced
identical objects.

| Setting | Total s (median / best) | Commit p50 ms | Journal writes | Peak concurrent part PUTs |
|---|---:|---:|---:|---:|
| strict 1/1/1 B | 24.50 / 24.49 | 5297 | 85 | 1 |
| 1/1 | 15.05 / 14.94 | 3233 | 36 | 1 |
| default 2/4 | 6.76 / 6.70 | 1421 | 31 | 4 |
| 4/4 | 6.89 / 6.88 | 1465 | 28 | 4 |
| 8/8 | 5.69 / 5.68 | 1219 | 28 | 8 |

With the defaults, commit latency and total time fall about 3.6x against strict
serial work. Batched receipts cut journal writes from 85 to about 30. Beyond
four publications the remaining serial work is the per-flush Writing, Committed,
authority and clear transitions and the receipt lane.

Reproduce:

```sh
cargo build --release --locked -p blocks --bin fireparq --example bench_ingestion_concurrency
target/release/examples/bench_ingestion_concurrency capture --start 26049575 --count 200 \
  --output /private/tmp/…/eth-26049575-200.fh          # provider credentials, once
target/release/examples/bench_ingestion_concurrency replay --input …/eth-26049575-200.fh \
  --work /private/tmp/…/work --repeat 5 --run base=/abs/old-fireparq \
  --run strict=/abs/fireparq@1:1:1 --run default=/abs/fireparq --run p48=/abs/fireparq@4:8
FIREPARQ_516_DATASET=/private/tmp/…/output/bench-mainnet cargo test -p firehose-parquet \
  --lib --release s3_replay_benchmark -- --ignored --nocapture
```

## Limits

- The gRPC stream still stops being read during a flush; stage B is needed to
  overlap it.
- The byte budget bounds encoded parts only. With native S3, each publication's
  readback verification holds a second private spool of that part.
- Final verification of remote parts drops the remaining read-only checks after
  the first failure instead of awaiting them.
- Debug-build fault injection is test infrastructure; it is not compiled into
  release binaries.
- No production S3 writes and no additional live requests beyond the one-time
  capture were made.
