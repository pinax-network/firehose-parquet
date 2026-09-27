# #658: writer throughput, head-cadence margin and commit phases on fast chains

Issue: [#658](https://github.com/pinax-network/firehose-parquet/issues/658), step 1
(benchmark and baseline). Part of [#463](https://github.com/pinax-network/firehose-parquet/issues/463).
This record only measures. No ingestion, writer or controller behavior
changed: the harness is an example, plus test-only hooks. The raw numbers are
in [658-live-flush-benchmark.json](658-live-flush-benchmark.json), and every
table below is rendered from that file by
[658-live-flush-report.py](658-live-flush-report.py).

## Target

The issue first asked for 1 s and 5 s flushes from a live non-final writer.
On 2026-09-27 the target became a **final-only lake**, launched as v1.0.0 on
Delta:

- at the chain head the primary commits every **60–120 s**;
- while backfilling it flushes by size only, at 32 MiB (#659);
- catch-up must run at **3× the chain rate or more**.

The chains are Robinhood (10 blocks/s) and Arbitrum One (4 blocks/s). This
record measures catch-up throughput, the margin at a 60–120 s cadence and
commit latency by phase. The 1 s and 5 s runs that finished before the change
are kept as supplementary data in the [appendix](#appendix-superseded-1-s-and-5-s-head-cadence).

The measured binary is `main` at `c6197d8` (`fireparq 1.0.0`). It writes plain
Parquet parts through the protected transaction journal of #468. The Delta
commit path (#643) does not exist yet, so these numbers are the baseline of
today's writer, not of Delta.

#652 (`date=YYYY-MM-DD` partitions for every table) and #653 landed on `main`
after these runs. They change part paths, not the requests a commit makes.
A smoke run of the rebased harness against `main` at `db84de7` matched round 2
of Robinhood catch-up at 30 ms: 111.8 against 111.2 blocks/s, with each
phase's p50 within 10 ms.

## Summary

**Catch-up** runs size-only (`--flush-bytes 32 MiB`, v1.0.0 defaults
otherwise). The table shows blocks/s, as a multiple of the chain rate, from
the lower of two rounds:

| Chain | Local disk | S3 0 ms | S3 10 ms | S3 30 ms | S3 80 ms | S3 30 ms, 1 in 200 at 1 s |
|---|---:|---:|---:|---:|---:|---:|
| Robinhood | 15.5× | 36.6× | 15.0× | 9.7× | 4.8× | 9.3× |
| Arbitrum One | 80× | 124× | 104× | 50× | 28× | 45× |

- **The 3× target is met everywhere.** The tightest case is Robinhood at
  80 ms per S3 request: 48–49 blocks/s.
- **The 32 MiB file target never fires.** The default 256 MiB memory trigger
  fires first, at every commit: every 269 Robinhood blocks or 620 Arbitrum
  blocks. That gives 16 parts per commit averaging 0.76–0.80 MB, or 59
  objects per 1,000 Robinhood blocks.
- **Bigger windows are the cheapest lever.** With a 1 GiB memory trigger the
  window is four times larger. Throughput rises 2.5–3.5×: Robinhood reaches
  28× at 30 ms and 17× at 80 ms. Objects per block fall 4× and peak RSS rises
  from about 1.0 GiB to about 2.8 GiB.

**Head cadence.** At 60–120 s the margin is large. Back to back, one
60 s window commits in 2.1–3.1 s at 30 ms and 5.1–5.3 s at 80 ms:

- Robinhood runs 15× faster than real time at 30 ms and 10× at 80 ms;
  Arbitrum 27× and 11×.
- 120 s windows raise those multiples to 32× and 18× on Robinhood, and 51×
  and 22× on Arbitrum.
- At the chain rate, a commit takes 1–8% of Arbitrum's 60 s period and 6–21%
  of Robinhood's.
- With default triggers Robinhood does not reach a 60–120 s cadence at all.
  The 256 MiB memory trigger fires about every 27 s. The binary logged a
  mapper estimate of 0.60 GB per 60 s Robinhood window and 1.20 GB per 120 s
  window. The 120 s windows ran at 2.6–2.7 GiB peak RSS with a 2 GiB trigger.

**Maximum sustained commits per second.** Back to back, the writer commits
about 3.6/s at 0 ms, 1.1/s at 10 ms, 0.45/s at 30 ms and 0.19/s at 80 ms,
with Arbitrum 60 s windows. The ceiling is set by the round trips per
commit, not by the window.

**Where the time goes.** Every S3 commit is round-trip bound. A commit costs
about the same at any size: 2.1–2.9 s at 30 ms and 5.1–5.4 s at 80 ms, for
240 to 2,475 blocks. That is about 63 sequential requests on the critical
path. At 80 ms, a Robinhood catch-up commit splits into:

| Share | Phase |
|---:|---|
| 34% | part publication with interleaved receipt writes, four parts at a time, three requests per part |
| 32% | Writing, Committed, authority, the cursor mirror and clear: four requests each |
| 26% | sequential HEADs of the 16 part names before Writing |
| 8% | a second full verification of every part |

**1 s flushes are not sustainable today.** A commit takes 2.1–5.4 s at
30–80 ms. Back to back, 1 s windows reach only 0.35× Robinhood's chain rate
at 30 ms with slow requests, and 1.07× Arbitrum's at 10 ms (see the appendix).

**Fixes, in order of expected catch-up gain.** Items 1, 4 and part of 2 are
measured. The rest are estimates, labeled [below](#fixes-in-priority-order).

1. Larger backfill windows (#659 sizing).
2. Fewer sequential control round trips.
3. Concurrent unoccupied-name checks.
4. Fully parallel part publication.
5. Skipping the redundant final verification.
6. #630 stage B. It gains at most 5–11% at 30–80 ms, because mapping takes
   only 0.25 s of each 2–5 s cycle.

## Method

### Harness

`blocks/examples/bench_live_flush` drives the real release binary. For each
scenario it starts:

- **A loopback Firehose** that replays a fixture in a loop. Block `n` carries
  the payload of fixture block `(n - base) mod 200`, with rewritten metadata:
  number, synthetic id and parent id, LIB, and a timestamp on the fixture's own
  block spacing, rebased to 12:00 UTC so no run crosses a day. The payload
  itself is not rewritten; the mapper takes the canonical identity columns
  from the metadata. It serves `EndpointInfo` (the captured response, so the
  binary resolves the same extended mode and 20 declared tables as against
  Pinax), `Fetch/Block` and `Stream/Blocks`. With a rate, block `start + k` is
  released no earlier than `request + k / rate`; without one, blocks are
  produced as fast as the client reads them.
- **Storage**: a fresh local directory, or a loopback **HTTPS S3** endpoint.
  Native ingestion refuses plain HTTP, so the harness makes a throwaway CA and
  a `127.0.0.1` certificate with the `openssl` CLI, and the child trusts it
  through `SSL_CERT_FILE` (read by `rustls-native-certs`). The endpoint keeps
  objects in memory and implements what protected ingestion uses: path-style
  GET/HEAD/PUT/DELETE, `If-None-Match: *`, `If-Match`, pinned `versionId`
  reads, ranges and ListObjectsV2, over HTTP/1.1 keep-alive. Each request waits
  half its injected latency before it is applied and half after, both anchored
  to its arrival. The slow variant makes every 200th request (by arrival) take
  1,000 ms instead. There is no bandwidth model.
- **`fireparq build`** with `--final-blocks-only=true` (the default), no stop
  block (a continuous primary), `--metrics-port`, and the scenario's flush and
  concurrency flags; everything else is the v1.0.0 default (zstd, 256 MiB
  `--flush-memory-bytes`, cursor mirror on, `--flush-encode-concurrency 2`,
  `--flush-publish-concurrency 4`). The environment is cleared, so no `.env`
  or credential reaches the child.

The harness polls `/metrics` every 20 ms, samples `ps` once a second, and
after the scenario's duration (counted from the first commit) sends SIGINT.
It then parses the child's log and, for S3, its own request log.

### Measurements

- **Commit latency**: `commit_ms` of each `committed flush size observation`
  line, the controller's wall time from commit start to the cleared journal.
- **Callback stall**: from `mapper flush emitted record batches` to that
  committed line. The Firehose callback blocks for this long; nothing is read
  or mapped meanwhile. On average it exceeds `commit_ms` by 0.3–5 ms in
  every scenario.
- **Mapping per window**: from one committed line to the next flush's emitted
  line: receiving and mapping the next window (catch-up only; at the head this
  is the wait for blocks).
- **Blocks/s**: growth of `firehose_parquet_cursor_last_block_num` between the
  first and the last measured commit, over that time. With `--cursor none`
  that gauge never moves, so the last mapped block seen during each commit is
  used instead (while the callback waits, it is the window's last block). The
  first commit is excluded from every statistic (startup).
- **Objects and bytes**: growth of `firehose_parquet_files_written_total` and
  `..._file_bytes_total` over the same window.
- **Lag** (steady state): for every block, from its scheduled release to the
  first metrics sample that shows it durable.
- **Phases** (S3): each commit's requests, from the endpoint log, split at
  request boundaries into a sequential critical path that sums to the callback
  stall:
  `prepare` (planning, until the first request), `unoccupied_heads` (the HEAD
  per planned part name before Writing), `writing` (the Writing journal
  write), `table_work` (encode, receipt writes, part PUTs and pinned readbacks),
  `final_verify` (the GET of every part before Committed), `committed`,
  `authority`, `mirror` (`_fireparq/cursor.parquet`), `clear` (the pending
  tombstone) and `tail`. Each control write is four requests: a read of the
  other control slot, a read of its own slot, the conditional PUT and a
  verifying read.

`firehose-parquet/src/ingest/controller/tests/live_flush.rs` is an ignored
in-process companion (`live_flush_phase_benchmark`). It replays the committed
transactions of a dataset written by the harness through the real controller,
back to back, into local disk or into the native loopback S3 fixture (which now
takes an injected latency), and records when each transaction boundary is
reached through the controller's test-only checkpoint hook. It is the only
source of local-disk phases; its cursor mirror is the test double.

### Scenarios

| Preset | Mode | Flags | Source |
|---|---|---|---|
| `final` | `catchup-size32m` | `--flush-bytes 33554432`, no interval (#659's backfill shape) | as fast as read |
| `final` | `catchup-size32m-mem1g` | the same plus `--flush-memory-bytes 1073741824` | as fast as read |
| `final` | `windows-60s` | `--flush-blocks` = 60 s of chain (600 / 240), 1 GiB memory | as fast as read |
| `final` | `windows-300s` | `--flush-blocks 1200` (Arbitrum only; no longer a target), 1 GiB memory | as fast as read |
| `steady` | `windows-120s` | `--flush-blocks` = 120 s of chain (1,200 / 480), 2 GiB memory | as fast as read |
| `steady` | `steady-120s` / `steady-60s` | `--flush-interval-secs 120` (Robinhood) / `60` (Arbitrum), default triggers | chain rate |
| `tuning` | the catch-up and 60 s modes | publish 8 or 16, encode 4; `--cursor none` | as fast as read |

Storage is local disk, or loopback S3 at 0, 10, 30 and 80 ms per request,
plus 30 ms with every 200th request at 1 s. The whole latency sweep covers the
catch-up and 60 s modes; the other modes use local disk, 30 ms and 80 ms.

The windows modes measure the head-cadence margin directly. Back to back,
each commit holds 60 s or 120 s of chain time, so `blocks/s ÷ chain rate` is
how many times faster than real time the writer can go at that cadence. They
raise the memory trigger, so the window decides when to flush, not the
256 MiB default.

The catch-up and 60 s modes ran twice (the `final` preset, then a second
round of the same scenarios about 90 minutes later).

## Results

### Catch-up

The memory trigger fired for every catch-up commit. Round 1 and round 2 are
the same scenarios run about 90 minutes apart. The commit, mapping and RSS
columns come from round 1.

#### Robinhood (10 blocks/s): catch-up, size-only flushes (`--flush-bytes 32 MiB`)

| Storage | Memory trigger | Blocks/s, rounds 1 / 2 | x chain (lower) | Blocks per commit | Objects per 1,000 blocks | Bytes per object | Commit p50 / p99 (s) | Mapping per window (s) | Peak RSS (MiB) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| local | 256 MiB (default) | 154.9 / 218.5 | 15.5x | 269 | 59.4 | 798,961 | 1.33 / 2.56 | 0.25 | 1,009 |
| s3-0ms | 256 MiB (default) | 366.2 / 479.4 | 36.6x | 269 | 59.4 | 798,912 | 0.33 / 2.73 | 0.22 | 1,046 |
| s3-10ms | 256 MiB (default) | 150.1 / 232.0 | 15.0x | 269 | 59.4 | 798,961 | 1.55 / 2.46 | 0.24 | 1,034 |
| s3-30ms | 256 MiB (default) | 96.8 / 111.2 | 9.7x | 269 | 59.4 | 799,067 | 2.31 / 3.34 | 0.24 | 1,028 |
| s3-80ms | 256 MiB (default) | 49.4 / 48.4 | 4.8x | 269 | 59.5 | 799,235 | 5.18 / 5.22 | 0.25 | 977 |
| s3-30ms-slow200 | 256 MiB (default) | 93.2 / 93.3 | 9.3x | 269 | 59.4 | 799,078 | 2.74 / 3.15 | 0.25 | 1,024 |
| local | 1 GiB | 491.8 / 344.8 | 34.5x | 1,075 | 14.9 | 2,881,774 | 1.24 / 2.84 | 0.87 | 2,923 |
| s3-30ms | 1 GiB | 295.1 / 282.1 | 28.2x | 1,075 | 14.9 | 2,881,947 | 2.76 / 2.81 | 0.89 | 2,797 |
| s3-80ms | 1 GiB | 171.1 / 170.2 | 17.0x | 1,075 | 14.9 | 2,882,388 | 5.38 / 5.41 | 0.91 | 2,767 |

#### Arbitrum One (4 blocks/s): catch-up, size-only flushes (`--flush-bytes 32 MiB`)

| Storage | Memory trigger | Blocks/s, rounds 1 / 2 | x chain (lower) | Blocks per commit | Objects per 1,000 blocks | Bytes per object | Commit p50 / p99 (s) | Mapping per window (s) | Peak RSS (MiB) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| local | 256 MiB (default) | 590.3 / 320.1 | 80.0x | 619 | 25.8 | 758,339 | 0.69 / 2.54 | 0.24 | 1,012 |
| s3-0ms | 256 MiB (default) | 1,092.6 / 496.9 | 124.2x | 620 | 25.8 | 758,382 | 0.33 / 0.51 | 0.23 | 987 |
| s3-10ms | 256 MiB (default) | 536.6 / 416.3 | 104.1x | 620 | 25.8 | 758,393 | 0.90 / 0.94 | 0.25 | 1,009 |
| s3-30ms | 256 MiB (default) | 261.2 / 199.7 | 49.9x | 619 | 25.8 | 758,424 | 2.11 / 2.16 | 0.25 | 966 |
| s3-80ms | 256 MiB (default) | 113.6 / 111.9 | 28.0x | 618 | 25.9 | 758,488 | 5.18 / 5.24 | 0.26 | 884 |
| s3-30ms-slow200 | 256 MiB (default) | 214.7 / 179.6 | 44.9x | 619 | 25.8 | 758,413 | 2.67 / 3.10 | 0.25 | 1,059 |
| local | 1 GiB | 1,126.2 / 1,184.8 | 281.5x | 2,475 | 6.5 | 2,806,975 | 1.12 / 2.68 | 0.92 | 2,889 |
| s3-30ms | 1 GiB | 682.2 / 682.2 | 170.6x | 2,476 | 6.5 | 2,807,358 | 2.65 / 2.85 | 0.96 | 2,825 |
| s3-80ms | 1 GiB | 379.7 / 387.7 | 94.9x | 2,475 | 6.5 | 2,807,459 | 5.43 / 5.79 | 0.96 | 2,859 |

At 80 ms both rounds agree within 2%, and the 1 GiB windows at 30 ms within
4%, because those commits are request-bound. At 0–30 ms with default windows
the rounds differ by up to 2.2×. The local work in a commit (encoding,
spooling and verifying each part) depends on the machine load, which ranged
from 2 to 27 during the runs. That spread shows in table work and final
verification, not in the request phases; see
[Where the time goes](#where-the-time-goes).

### Head-cadence margin

The window rows commit one window back to back as fast as the source is
read. Their chain-rate multiple is the margin at that cadence: 15× means one
window of chain time commits in a fifteenth of that time, so the writer can
run 15 times faster than real time.

#### Robinhood (10 blocks/s): back-to-back commits of 60 s, 120 s and 300 s windows

| Storage | Window | Blocks per commit | Commit p50 / p99 (s) | Mapping per window (s) | Blocks/s, rounds | x chain = margin (lower) | Peak RSS (MiB) |
|---|---|---:|---:|---:|---:|---:|---:|
| local | 60s | 600 | 1.30 / 3.33 | 0.46 | 299.2 / 476.8 | 29.9x | 1,448 |
| s3-0ms | 60s | 600 | 0.62 / 3.73 | 0.45 | 415.1 / 585.5 | 41.5x | 1,515 |
| s3-10ms | 60s | 600 | 1.84 / 2.29 | 0.46 | 270.1 / 369.9 | 27.0x | 1,445 |
| s3-30ms | 60s | 600 | 3.07 / 6.45 | 0.48 | 151.2 / 214.7 | 15.1x | 1,451 |
| s3-80ms | 60s | 600 | 5.29 / 5.35 | 0.47 | 103.7 / 103.5 | 10.4x | 1,424 |
| s3-30ms-slow200 | 60s | 600 | 2.99 / 3.31 | 0.47 | 181.2 / 179.9 | 18.0x | 1,424 |
| local | 120s | 1,200 | 1.18 / 3.10 | 0.87 | 501.7 | 50.2x | 2,796 |
| s3-30ms | 120s | 1,200 | 2.83 / 2.85 | 0.91 | 319.8 | 32.0x | 2,640 |
| s3-80ms | 120s | 1,200 | 5.43 / 6.11 | 1.00 | 178.9 | 17.9x | 2,658 |

#### Arbitrum One (4 blocks/s): back-to-back commits of 60 s, 120 s and 300 s windows

| Storage | Window | Blocks per commit | Commit p50 / p99 (s) | Mapping per window (s) | Blocks/s, rounds | x chain = margin (lower) | Peak RSS (MiB) |
|---|---|---:|---:|---:|---:|---:|---:|
| local | 60s | 240 | 0.61 / 0.92 | 0.10 | 331.4 / 256.1 | 64.0x | 580 |
| s3-0ms | 60s | 240 | 0.19 / 0.21 | 0.09 | 861.5 / 875.6 | 215.4x | 625 |
| s3-10ms | 60s | 240 | 0.79 / 0.85 | 0.11 | 265.5 / 260.2 | 65.1x | 575 |
| s3-30ms | 60s | 240 | 2.10 / 2.15 | 0.11 | 109.0 / 107.7 | 26.9x | 537 |
| s3-80ms | 60s | 240 | 5.14 / 5.19 | 0.11 | 45.6 / 45.1 | 11.3x | 473 |
| s3-30ms-slow200 | 60s | 240 | 2.72 / 3.14 | 0.12 | 89.4 / 88.5 | 22.1x | 486 |
| local | 120s | 480 | 0.73 / 3.98 | 0.19 | 417.4 | 104.4x | 812 |
| s3-30ms | 120s | 480 | 2.12 / 2.66 | 0.21 | 202.6 | 50.7x | 826 |
| s3-80ms | 120s | 480 | 5.21 / 5.57 | 0.22 | 87.3 | 21.8x | 779 |
| local | 300s | 1,200 | 0.80 / 1.14 | 0.40 | 971.7 | 242.9x | 1,386 |
| s3-30ms | 300s | 1,200 | 2.26 / 2.96 | 0.44 | 408.0 | 102.0x | 1,484 |
| s3-80ms | 300s | 1,200 | 5.28 / 5.38 | 0.43 | 209.3 | 52.3x | 1,345 |

At the chain rate with default triggers (`steady` preset), each chain commits
on this cadence:

| Chain | Storage | Interval | Triggers (fired) | Flush period mean (s) | Blocks per commit | Commit p50 / max (s) | Commit share of period | Lag p50 / max (s) |
|---|---|---:|---|---:|---:|---:|---:|---:|
| Robinhood | local | 120s | memory 4 | 26.6 | 270 | 0.76 / 4.28 | 6.2% | 14.66 / 29.52 |
| Robinhood | s3-30ms | 120s | memory 4 | 27.3 | 270 | 2.49 / 3.63 | 10.1% | 16.07 / 30.52 |
| Robinhood | s3-80ms | 120s | memory 4 | 27.2 | 270 | 5.73 / 6.17 | 21.2% | 18.88 / 32.87 |
| Arbitrum One | local | 60s | interval 3 | 60.8 | 243 | 0.67 / 0.84 | 1.2% | 30.93 / 61.34 |
| Arbitrum One | s3-30ms | 60s | interval 3 | 62.4 | 250 | 2.10 / 2.52 | 3.5% | 33.16 / 64.41 |
| Arbitrum One | s3-80ms | 60s | interval 2 | 65.6 | 263 | 5.25 / 5.51 | 8.2% | 37.71 / 70.71 |

- Robinhood's 120 s interval never fires, because the memory trigger fires
  every 27 s.
- Arbitrum's 60 s interval fires, and the next window starts after the commit,
  so the period is 60 s plus the commit.
- The lag is from a block's scheduled release to its commit. It is about half
  a period on average, and at most one period plus the commit.
- The first commit of each run is excluded, so these rows rest on two to four
  commits.

### Commit phases on loopback S3

These are the critical-path phases of a size-only catch-up commit, round 1.
The `windows-60s` breakdown is the same shape and is in the JSON.

#### Robinhood (10 blocks/s), `catchup-size32m`: phase p50 / p99 (ms)

| Phase | s3-0ms | s3-10ms | s3-30ms | s3-80ms | s3-30ms-slow200 |
|---|---:|---:|---:|---:|---:|
| prepare | 1 / 6 | 2 / 14 | 2 / 19 | 1 / 1 | 1 / 6 |
| unoccupied_heads | 22 / 24 | 196 / 293 | 522 / 528 | 1334 / 1347 | 529 / 1509 |
| writing | 6 / 7 | 50 / 84 | 131 / 135 | 335 / 341 | 134 / 164 |
| table_work | 233 / 1145 | 682 / 989 | 892 / 1683 | 1747 / 1789 | 746 / 1712 |
| final_verify | 42 / 1528 | 401 / 891 | 265 / 771 | 410 / 428 | 185 / 1122 |
| committed | 6 / 7 | 50 / 58 | 130 / 133 | 333 / 338 | 131 / 140 |
| authority | 6 / 6 | 50 / 57 | 131 / 135 | 334 / 341 | 133 / 139 |
| mirror | 6 / 7 | 50 / 72 | 133 / 140 | 336 / 346 | 135 / 150 |
| clear | 6 / 7 | 50 / 66 | 132 / 138 | 337 / 339 | 135 / 172 |
| tail | 0 / 0 | 0 / 2 | 0 / 0 | 1 / 3 | 1 / 4 |
| commit total | 328 / 2728 | 1553 / 2457 | 2311 / 3338 | 5177 / 5220 | 2742 / 3151 |
| requests per commit (mean) | 147 | 128 | 119 | 108 | 112 |
| receipt writes per commit | 11.9 | 7.1 | 4.8 | 2.0 | 2.9 |
| parts per commit | 16.0 | 16.0 | 16.0 | 16.0 | 16.0 |
| server time per request (ms, mean) | 1.4 | 11.9 | 32.5 | 82.4 | 36.0 |

#### Arbitrum One (4 blocks/s), `catchup-size32m`: phase p50 / p99 (ms)

| Phase | s3-0ms | s3-10ms | s3-30ms | s3-80ms | s3-30ms-slow200 |
|---|---:|---:|---:|---:|---:|
| prepare | 1 / 1 | 1 / 1 | 1 / 1 | 1 / 1 | 1 / 7 |
| unoccupied_heads | 23 / 26 | 194 / 202 | 527 / 547 | 1328 / 1339 | 527 / 1501 |
| writing | 6 / 7 | 50 / 75 | 134 / 150 | 334 / 336 | 132 / 139 |
| table_work | 233 / 270 | 378 / 400 | 735 / 757 | 1749 / 1802 | 747 / 1669 |
| final_verify | 41 / 169 | 83 / 89 | 180 / 192 | 400 / 449 | 182 / 1128 |
| committed | 6 / 11 | 48 / 50 | 131 / 135 | 335 / 337 | 131 / 136 |
| authority | 6 / 9 | 49 / 51 | 133 / 140 | 335 / 348 | 133 / 1103 |
| mirror | 6 / 8 | 49 / 52 | 134 / 147 | 336 / 348 | 135 / 139 |
| clear | 6 / 10 | 49 / 52 | 135 / 143 | 335 / 337 | 133 / 140 |
| tail | 0 / 0 | 0 / 0 | 0 / 5 | 0 / 1 | 0 / 1 |
| commit total | 329 / 506 | 902 / 938 | 2112 / 2160 | 5176 / 5241 | 2673 / 3105 |
| requests per commit (mean) | 148 | 120 | 112 | 108 | 113 |
| receipt writes per commit | 12.1 | 5.0 | 3.0 | 2.0 | 3.1 |
| parts per commit | 16.0 | 16.0 | 16.0 | 16.0 | 16.0 |
| server time per request (ms, mean) | 1.5 | 11.8 | 32.4 | 82.5 | 35.4 |

- **Round 1's 10 ms Robinhood row** ran during a load spike. Its table work
  and final verification were 0.68 s and 0.40 s p50, against 0.38 s and
  0.08 s in round 2. The request-only phases matched to the millisecond.
- **Slow requests.** In the slow-request variant, every 200th request takes
  1 s. It raises the p99 of whichever phase it lands in by about 1 s (the
  unoccupied HEADs, table work or authority), and the commit p50 by
  0.4–0.6 s.
- **Receipt writes adapt to latency.** Receipts of parts that finish encoding
  while a receipt write is in flight go into the next one. There are 2 per
  commit at 80 ms and up to 12 at 0 ms.

### In-process phases

`live_flush_phase_benchmark` replays 12 transactions of each chain's local
catch-up dataset (269 or 620 blocks each, 16 parts) back to back. It commits
into local disk and into the native loopback fixture at 0, 30 and 80 ms. The
cursor mirror is the test double, which costs nothing. "All parts encoded" is
the time from Writing until the last part was encoded and staged, inside
table work.

| Chain | Storage | Encode:publish | begin | table work | all parts encoded | final verify + Committed | authority | clear | total | Commits/s back to back |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| robinhood | local | 2:4 | 18 / 25 | 428 / 853 | 367 / 670 | 32 / 59 | 17 / 22 | 14 / 19 | 534 / 957 | 1.75 |
| robinhood | local | 2:16 | 18 / 25 | 388 / 524 | 337 / 421 | 29 / 41 | 19 / 26 | 16 / 25 | 475 / 635 | 2.00 |
| robinhood | s3-0ms | 2:4 | 33 / 33 | 246 / 264 | 199 / 214 | 33 / 61 | 6 / 7 | 6 / 7 | 325 / 348 | 3.12 |
| robinhood | s3-0ms | 2:16 | 33 / 33 | 242 / 247 | 196 / 198 | 26 / 31 | 6 / 7 | 6 / 7 | 314 / 321 | 3.29 |
| robinhood | s3-30ms | 2:4 | 696 / 731 | 770 / 789 | 234 / 249 | 308 / 314 | 139 / 142 | 138 / 151 | 2049 / 2088 | 0.49 |
| robinhood | s3-30ms | 2:16 | 691 / 765 | 564 / 601 | 236 / 265 | 188 / 206 | 136 / 197 | 137 / 152 | 1729 / 1798 | 0.58 |
| robinhood | s3-80ms | 2:4 | 1695 / 1715 | 1816 / 1858 | 216 / 241 | 769 / 794 | 340 / 347 | 338 / 358 | 4965 / 5015 | 0.20 |
| robinhood | s3-80ms | 2:16 | 1692 / 1711 | 1026 / 1049 | 225 / 247 | 449 / 464 | 338 / 347 | 339 / 347 | 3841 / 3880 | 0.26 |
| arbitrum | local | 2:4 | 24 / 45 | 621 / 819 | 496 / 727 | 41 / 244 | 23 / 52 | 18 / 236 | 726 / 1370 | 1.31 |
| arbitrum | local | 2:16 | 24 / 34 | 520 / 701 | 412 / 637 | 35 / 63 | 23 / 30 | 18 / 39 | 614 / 814 | 1.61 |
| arbitrum | s3-0ms | 2:4 | 33 / 33 | 239 / 249 | 194 / 205 | 30 / 32 | 6 / 7 | 6 / 7 | 315 / 326 | 3.27 |
| arbitrum | s3-0ms | 2:16 | 33 / 33 | 238 / 249 | 194 / 197 | 28 / 47 | 6 / 7 | 6 / 7 | 311 / 341 | 3.29 |
| arbitrum | s3-30ms | 2:4 | 684 / 692 | 770 / 845 | 229 / 287 | 310 / 329 | 138 / 144 | 136 / 143 | 2040 / 2109 | 0.49 |
| arbitrum | s3-30ms | 2:16 | 689 / 697 | 565 / 581 | 230 / 242 | 188 / 192 | 135 / 144 | 137 / 148 | 1718 / 1733 | 0.58 |
| arbitrum | s3-80ms | 2:4 | 1693 / 1704 | 1824 / 1865 | 229 / 240 | 749 / 820 | 337 / 347 | 337 / 346 | 4942 / 5072 | 0.20 |
| arbitrum | s3-80ms | 2:16 | 1695 / 1712 | 1017 / 1033 | 226 / 243 | 448 / 451 | 338 / 346 | 340 / 351 | 3843 / 3856 | 0.26 |

- **Local disk** commits are 0.5–0.7 s p50. Encoding and staging (writing
  and syncing the private temporary file) take about 70% of that.
- **On S3 at 0 ms**, encoding and spooling all 16 parts at two encoders takes
  about 0.2 s. That is the floor under every S3 commit.
- **The in-process S3 commits** agree with the real binary within 0.1 s:
  4.94–4.97 s at 80 ms, against 4.85 s for the binary without its 0.33 s
  mirror write.
- **Publishing 16 at a time** cuts the commit at 80 ms from 4.94–4.97 s to
  3.84 s (−22%). Most of the gain is in table work (1.82 s to 1.02 s) and in
  final verification plus the Committed write (0.75–0.77 s to 0.45 s), because
  final verification uses the same concurrency.

### Tuning with today's flags

#### Robinhood (10 blocks/s): publication concurrency and `--cursor none`

| Storage | Setting | Catch-up blocks/s | Catch-up commit p50 (s) | 60 s windows blocks/s | 60 s windows commit p50 (s) |
|---|---|---:|---:|---:|---:|
| s3-30ms | default (encode 2, publish 4), rounds 1 / 2 | 96.8 / 111.2 | 2.31 / 2.14 | 151.2 / 214.7 | 3.07 / 2.31 |
| s3-30ms | encode 2, publish 8 | 104.3 | 2.26 | 234.4 | 2.10 |
| s3-30ms | encode 2, publish 16 | 100.7 | 2.36 | 237.0 | 2.08 |
| s3-30ms | encode 4, publish 16 | 127.7 | 1.82 | 245.1 | 1.99 |
| s3-30ms | defaults, `--cursor none` | 120.0 | 1.99 | - | - |
| s3-80ms | default (encode 2, publish 4), rounds 1 / 2 | 49.4 / 48.4 | 5.18 / 5.25 | 103.7 / 103.5 | 5.29 / 5.30 |
| s3-80ms | encode 2, publish 8 | 52.9 | 4.83 | 116.4 | 4.58 |
| s3-80ms | encode 2, publish 16 | 55.2 | 4.52 | 120.5 | 4.50 |
| s3-80ms | encode 4, publish 16 | 51.6 | 4.61 | 127.0 | 4.21 |
| s3-80ms | defaults, `--cursor none` | 52.4 | 4.88 | - | - |

#### Arbitrum One (4 blocks/s): publication concurrency and `--cursor none`

| Storage | Setting | Catch-up blocks/s | Catch-up commit p50 (s) | 60 s windows blocks/s | 60 s windows commit p50 (s) |
|---|---|---:|---:|---:|---:|
| s3-30ms | default (encode 2, publish 4), rounds 1 / 2 | 261.2 / 199.7 | 2.11 / 2.69 | 109.0 / 107.7 | 2.10 / 2.11 |
| s3-30ms | encode 2, publish 8 | 279.7 | 1.92 | 112.1 | 1.85 |
| s3-30ms | encode 2, publish 16 | 241.3 | 2.24 | 114.3 | 1.70 |
| s3-30ms | encode 4, publish 16 | 240.4 | 2.32 | 134.3 | 1.66 |
| s3-30ms | defaults, `--cursor none` | 273.9 | 2.00 | - | - |
| s3-80ms | default (encode 2, publish 4), rounds 1 / 2 | 113.6 / 111.9 | 5.18 / 5.21 | 45.6 / 45.1 | 5.14 / 5.20 |
| s3-80ms | encode 2, publish 8 | 130.1 | 4.49 | 52.2 | 4.47 |
| s3-80ms | encode 2, publish 16 | 141.6 | 4.12 | 57.1 | 4.09 |
| s3-80ms | encode 4, publish 16 | 141.3 | 4.12 | 56.9 | 4.09 |
| s3-80ms | defaults, `--cursor none` | 120.2 | 4.89 | - | - |

- **More publications help once latency dominates.** At 80 ms, 16
  publications cut the commit by 0.7–1.1 s (13–21%) and raise throughput by
  12–27%. At 30 ms the differences are within the spread between rounds.
- **Four encoders** gave the best rows at 30 ms, but by less than that spread.
- **`--cursor none`** removes the four-request mirror write. It saves about
  0.3 s at 80 ms (5.18–5.25 s to 4.88 s) and 0.1–0.3 s at 30 ms.

## Where the time goes

Every S3 commit issues the same requests whatever its size. At 80 ms, a
Robinhood catch-up commit (16 parts) makes 108 requests:

| Requests | Count | On the critical path |
|---|---:|---|
| HEAD of each planned part name (`require_unoccupied`) | 16 | all 16, one after another |
| Control writes: Writing, receipts (2 at 80 ms, up to 12 at 0 ms), Committed, authority, clear | 6 PUTs + 18 reads | Writing, Committed, authority and clear, plus the first receipt write, each 4 requests in a row |
| Cursor mirror: read, owner read, PUT, verifying read | 4 | all 4 |
| Per part: owner read (publication gate), conditional PUT, pinned readback | 48 | 4 waves of 3 at `--flush-publish-concurrency 4` |
| Final verification GET of every part | 16 | 4 waves of 1 |

That is about 63 requests in sequence, so the commit takes about 63 × the
request time (5,177 ms / 82.4 ms). The window size barely matters. Ranked by
measured share of a Robinhood catch-up commit at 80 ms (p50):

1. **Part publication and receipts (`table_work`)**: 1,747 ms, 34%.
2. **Writing, Committed, authority, the cursor mirror and clear**: 1,675 ms,
   32%, about 335 ms each (four requests).
3. **Sequential unoccupied-name HEADs**: 1,334 ms, 26%.
4. **Final verification**: 410 ms, 8%.
5. **Planning and the tail**: about 2 ms. Encoding runs inside table work,
   overlapped with requests: the first receipt PUT starts 173 ms after
   Writing, which is two request times plus about 10 ms.

At 30 ms the shares are similar (table work 39%, control writes 28%, HEADs
23%, final verify 11%). Local work is small next to the requests, except near
zero latency or under load: at 0 ms a commit still takes 0.19–0.63 s p50.
Native S3 publication writes each part to a private disk spool and verifies
it in full three times: before the PUT, after the pinned readback, and in
final verification. Under load on this shared, 98%-full disk, that local work
stretched final verification to 0.4–2.0 s in some commits even at 0–10 ms.

Mapping is small next to the commit: 0.22–0.26 s per 269-block Robinhood
window and per 620-block Arbitrum window, against a 2.1–5.2 s commit at
30–80 ms. The callback therefore spends 78–95% of catch-up time blocked on
commits at 10–80 ms.

## Is the target met?

| Target | Today (v1.0.0 writer, plain Parquet) |
|---|---|
| Catch-up at 3× the chain rate or more, size-only flushes | **Met.** At least 4.8× on Robinhood and 28× on Arbitrum at 80 ms, and 9.7× and 50× at 30 ms. |
| Commit every 60–120 s at the head with margin | **Met.** One 60 s window commits 10–15× faster than real time on Robinhood and 11–27× on Arbitrum at 30–80 ms. Robinhood only reaches that cadence with `--flush-memory-bytes` above its window's mapper estimate: 0.60 GB for 60 s, 1.20 GB for 120 s. With the default it commits every 27 s. |
| Commit latency by phase | Recorded above and in the JSON. |
| 1 s flushes (superseded) | **Not sustainable**: 2.1–5.4 s per commit at 30–80 ms. |

The riv-dev1 RGW run (step 3 of #658) and the README sizing table are still
open.

## Fixes, in priority order

The target is catch-up throughput, so each fix is ranked by its expected gain
there. At the 60–120 s head cadence none of them is needed (see above).

Gains marked **measured** come from runs in this record. Gains marked
**estimate** are request-count arithmetic on the measured phase breakdown:
removed critical-path requests × the measured request time. They are not
measurements. For reference, a Robinhood catch-up commit at 80 ms takes
5,177 ms, with 63 requests of 82.4 ms on the critical path, and the same
commit at 30 ms takes 2,311 ms with 32.5 ms requests.

1. **Larger backfill windows** (#659 sizing, configuration only). Commit cost
   is almost independent of window size, so blocks per commit decide
   throughput.
   - The 256 MiB memory trigger fires long before the 32 MiB file target: at
     269 Robinhood blocks, with 0.8 MB parts.
   - **Measured** with a 1 GiB trigger (1,075 Robinhood blocks, largest part
     15.8 MB): throughput rises 2.5–3.5×, objects per block fall 4×, and peak
     RSS rises from about 1.0 GiB to about 2.8 GiB.
   - **Estimate**: reaching the 32 MiB target on Robinhood would take about
     2.2 GB of mapper estimate, by scaling the largest part linearly. #659
     should size the memory trigger, rather than assume the file target fires.
2. **Fewer sequential control round trips**, within what the #468 contract
   allows. Each control write is four requests in a row: the read of the other
   slot that `TransactionStateStore::require` does, the read of its own slot
   in `S3StateStore::replace`/`remove`/`create`, the conditional PUT, and the
   verifying read. The conditional PUT already enforces the version, and under
   exclusive ownership the other slot's version is known in memory.
   - One request per write instead of four removes three requests from each of
     the six writes on the critical path (Writing, the first receipt write,
     Committed, authority, the mirror and clear): 18 requests. **Estimate**:
     −1.5 s at 80 ms (−29%), −0.6 s at 30 ms (−25%).
   - Keeping the verifying read (two requests per write) removes 12:
     **estimate** −1.0 s at 80 ms (−19%), −0.4 s at 30 ms (−17%).
   - The mirror write (read, owner read, PUT, verifying read) can already be
     removed with `--cursor none`; that was **measured** (tuning section).
3. **Concurrent unoccupied-name checks.** `TransactionParts::require_unoccupied`
   HEADs the 16 planned names one after another before Writing. Running them
   concurrently keeps the rule that every check completes before Writing.
   **Estimate**: 16 → 1 request with 16 at a time: −1.25 s at 80 ms (−24%),
   −0.49 s at 30 ms (−21%); with 4 at a time, 16 → 4 requests.
4. **Fully parallel part publication.** With the default
   `--flush-publish-concurrency 4`, 16 parts publish in four waves of three
   requests: owner read, PUT and pinned readback.
   - **Measured** with 16 publications: −0.7 to −1.1 s per commit at 80 ms
     (−13 to −21%), and +12 to +27% throughput. In process it was −1.1 s
     (−22%). A default sized to the part count (about 16 here) would take this
     for free.
   - Reading the owner record once per transaction instead of once per part
     removes one more request per wave (**estimate**).
5. **Skip the redundant final verification.** `verify_all_finals` reads and
   verifies every part again right after the pinned readback verified the same
   version in the same transaction. Recovery would keep it. **Estimate**:
   −0.41 s at 80 ms (−8%), −0.27 s at 30 ms (−11%), plus one of the three
   local spool-and-verify passes per part.
6. **#630 stage B** (map the next window while the previous one commits).
   **Measured bound**: mapping is 0.22–0.26 s per catch-up window against a
   2.1–5.2 s commit, so overlapping the two gains at most 5% at 80 ms and 11%
   at 30 ms. It gains more near zero latency (up to 67% at 0 ms, where mapping
   and commit are similar). At the head it removes the 2–5 s callback stall
   per commit, which does not matter at a 60–120 s cadence.

Together, 2 to 5 would take a Robinhood catch-up commit from about 63
critical-path requests to about 10–14. The local work seen at 0 ms (about
0.25 s) then matters too. **Estimate**: about 1.0–1.4 s per commit at 80 ms
instead of 5.2 s, or roughly 160–210 blocks/s with today's 269-block windows
instead of 49, before any window-size change.

## Limits

- **Loopback S3 is a latency model, not RGW or AWS.** Each request waits the
  injected time plus about 1–2.5 ms: one timer tick, TLS and the handler. The
  "server time per request" rows give the measured value. There is no
  bandwidth limit, no provider-side variance other than the 1-in-200 slow
  request, and no throttling. The objects live in memory. Step 3 of #658
  (a real Robinhood run against riv-dev1 RGW) is still needed.
- **Shared, busy machine.** Other agents were building and testing
  throughout; 1-minute load averages ranged from 2 to 27 and are recorded
  per scenario. Local-disk runs and the local work in S3 commits are the most
  sensitive: the same Robinhood local catch-up gave 155 and 218 blocks/s in
  two rounds. At 80 ms the two rounds repeat within 2%.
- **The disk was 98% full** (45 GiB free), which slows APFS writes and
  `F_FULLFSYNC`. Local-disk commit times are pessimistic for that reason.
- **Looped fixtures.** Each chain's 200 captured blocks repeat with new
  numbers. Windows larger than 200 blocks contain repeated payloads, which
  compress better than real data would, so bytes per object for large windows
  are optimistic. Commit time is dominated by request count, not bytes, so
  this has little effect on the throughput results.
- **Few head-cadence commits.** A 60 s or 27 s period gives two to four
  measured commits per steady-state run. Those rows show the trigger, the
  period and the commit share; the percentile evidence for commit latency
  comes from the back-to-back windows.
- **Plain Parquet, not Delta.** The measured writer is `main` at `c6197d8`.
  A Delta commit (#643) adds its log write to each commit and may replace
  parts of the journal protocol, so step 2 should re-run this harness once it
  exists.
- The in-process companion uses the controller's test double for the cursor
  mirror and the native loopback fixture, which closes the connection after
  each request.

## Reproduce

```sh
cargo build --release --locked -p blocks --bin fireparq --example bench_live_flush
B=target/release/examples/bench_live_flush
# Once per chain; reads SUBSTREAMS_API_KEY from the shell and only talks to Pinax.
$B capture --endpoint https://robinhood.firehose.pinax.network:443 \
  --start 50000000 --count 200 --output /private/tmp/…/robinhood-50000000-200.fixture
$B capture --endpoint https://arbone.firehose.pinax.network:443 \
  --start 440000000 --count 200 --output /private/tmp/…/arbitrum-440000000-200.fixture
# Matrix presets: final, steady, tuning (results append to a JSON-lines file;
# finished labels are skipped on a rerun).
for preset in final steady tuning; do
  $B matrix --preset $preset --robinhood …/robinhood-50000000-200.fixture \
    --arbitrum …/arbitrum-440000000-200.fixture --binary "$PWD/target/release/fireparq" \
    --work /private/tmp/…/matrix --results /private/tmp/…/$preset.jsonl
done
# In-process phases, from a local dataset written by `$B run … --storage local --flush-bytes 33554432`:
FIREPARQ_658_DATASET=/private/tmp/…/out FIREPARQ_658_LIMIT=12 FIREPARQ_658_LATENCY_MS=0,30,80 \
  FIREPARQ_658_SETTINGS=2:4,2:16 cargo test --locked -p firehose-parquet --lib --release \
  live_flush_phase_benchmark -- --ignored --nocapture
python3 docs/audit/658-live-flush-report.py /private/tmp/…/final.jsonl /private/tmp/…/steady.jsonl …
```

The `capture` subcommand refuses any host other than
`*.firehose.pinax.network:443`, selects only the API key (never the bearer
token) and stores no provider cursor. The fixtures are 7.7 MB and 3.7 MB and
are not committed. The `openssl` CLI is needed for the loopback certificate.

## Appendix: superseded 1 s and 5 s head cadence

These ran before the target changed. They are non-final (`--final-blocks-only=false`),
with flush triggers `--flush-interval-secs 1` or `5` at the chain rate
(`steady-*`), or `--flush-blocks` of one or five seconds of chain back to back
(`windows-*`). The validation runs used 0.3× the listed durations.

| Scenario | Run | Commits | Commit p50 / p99 (s) | Flush period (s) | Blocks/s (x chain) | Lag p99 (s) |
|---|---|---:|---:|---:|---:|---:|
| `robinhood/local/steady-5s` | full | 15 | 0.71 / 1.20 | 5.78 | 10.0 (1.00x) | 6.6 |
| `robinhood/local/steady-1s` | full | 24 | 0.76 / 1.72 | 1.86 | 10.0 (1.00x) | 3.3 |
| `robinhood/local/windows-5s` | full | 33 | 0.73 / 2.41 | 0.88 | 56.8 (5.68x) | - |
| `robinhood/local/windows-1s` | full | 37 | 0.60 / 2.14 | 0.79 | 12.7 (1.27x) | - |
| `robinhood/s3-0ms/steady-5s` | full | 17 | 0.15 / 0.20 | 5.20 | 10.0 (1.00x) | 5.3 |
| `robinhood/s3-30ms-slow200/steady-5s` | 0.3x duration | 3 | 2.92 / 2.93 | 7.72 | 9.7 (0.97x) | 9.9 |
| `robinhood/s3-30ms-slow200/steady-1s` | 0.3x duration | 3 | 2.98 / 3.16 | 3.79 | 9.3 (0.93x) | 6.1 |
| `robinhood/s3-30ms-slow200/windows-5s` | 0.3x duration | 3 | 2.93 / 2.95 | 2.77 | 18.1 (1.81x) | - |
| `robinhood/s3-30ms-slow200/windows-1s` | 0.3x duration | 3 | 2.96 / 3.15 | 2.82 | 3.5 (0.35x) | - |
| `arbitrum/s3-10ms/steady-5s` | 0.3x duration | 4 | 0.94 / 0.99 | 6.06 | 4.0 (1.00x) | 6.9 |
| `arbitrum/s3-10ms/steady-1s` | 0.3x duration | 6 | 0.95 / 1.00 | 2.04 | 4.0 (1.00x) | 2.9 |
| `arbitrum/s3-10ms/windows-5s` | 0.3x duration | 9 | 0.98 / 0.99 | 0.99 | 20.2 (5.04x) | - |
| `arbitrum/s3-10ms/windows-1s` | 0.3x duration | 9 | 0.93 / 1.04 | 0.93 | 4.3 (1.07x) | - |

- The interval restarts after each commit, so a `steady-1s` run commits every
  1 s plus the commit: 1.9–3.8 s in practice.
- Back to back, 1 s windows fall behind Robinhood (0.35× at 30 ms with slow
  requests) and barely keep up with Arbitrum (1.07× at 10 ms).
- At a 5 s interval the head keeps up, but each commit stops the stream for
  0.15–2.9 s.
