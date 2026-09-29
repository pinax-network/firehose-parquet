# Issue #659: adaptive flush interval

Closes #659 (PR [#664](https://github.com/pinax-network/firehose-parquet/pull/664)); part of #463. Related: #658 (live-flush benchmark), #630 (stage B),
#515 (size targets).

## Decision

On 2026-09-27 the user decided:

- The deployment is one continuous, final-only writer per network, released as
  v1.0.0 (Delta Lake, #643, follows).
- At the head it commits every 60–120 s (300 s in v1.0.0). While catching up (a
  restart, an outage or a first start) it writes large, size-based batches (the
  32 MiB `--flush-bytes` target) instead of an interval's worth of historical
  blocks. That avoids tiny files on Ceph and speeds up catch-up.
- No new flag.

So `--flush-interval-secs` now applies only while the writer is caught up with
the chain head. While it catches up, the interval is suspended and every other
trigger keeps working: `--flush-bytes`, `--flush-memory-bytes`, `--flush-rows`,
`--flush-blocks`, day boundaries and the end of the stream. A bounded build that
is far behind flushes by size.

## Catch-up detection

`firehose_parquet::flush::pace::PaceDetector` (`firehose-parquet/src/flush/pace.rs`)
is a pure state machine over `(wall instant, block time)` observations. The
runtime feeds it every admitted block on arrival, with the block's own time in
milliseconds (`BlockIdentity::timestamp_millis`) or `None`. It never passes a
synthesized Solana routing time or a bootstrap time.

| Rule | Value |
|---|---|
| Sample | At least 5 s of wall time, ending at a block with a timestamp |
| Ratio | (newest block time at the end − at the start) / wall time |
| Fast sample | ratio > 2.0 |
| Real-time sample | ratio ≤ 1.25 |
| Neutral sample | 1.25 < ratio ≤ 2.0: breaks both streaks, changes nothing |
| Enter catching up | ≥ 3 consecutive fast samples covering ≥ 30 s |
| Leave catching up | consecutive real-time samples covering ≥ 20 s, not counting the writer's own commits |
| Missing timestamps | while catching up, 60 s without a timestamped block (commits excluded) means caught up |
| Start | caught up (today's behavior) |

The windows are fixed multiples of the sample (6, 4 and 12). Debug builds read
`FIREPARQ_DEBUG_PACE_SAMPLE_MS` to shorten them for real-binary tests, like
`FIREPARQ_DEBUG_FAULT`; release builds ignore it.

### Why not block age

A final-only stream is always about the finality lag behind the tip, even when
fully caught up. On 2026-09-27 the user measured this from block metadata
`lib_num`:

| Chain | Block time | LIB lag |
|---|---|---|
| Ethereum | 12 s | 15.8 min |
| Base | 2 s | 6.7 min |
| Arbitrum | 0.25 s | 1.0 min |
| Robinhood | 0.1 s | 0.4 min |

A block-age threshold would have to know each chain's lag. The pace does not
depend on it: at the head, block time advances at about wall-clock speed
whatever the lag. The detector uses only differences of block times and of a
monotonic clock, so clock skew between the host and the chain does not matter.

### Why not the buffered window alone

The issue suggested comparing the open window's block-time span with the wall
time since it opened. That enters catching up correctly, but it lags badly on
the way out: a window opened during a catch-up keeps a high ratio long after the
stream reaches the head. For example, a window holding 3 hours of history stays
above 2× for another 1.5 hours at the head. Samples measure the recent pace
instead, and hysteresis keeps them from flapping.

### Finality bursts

On Ethereum and Base a final-only stream delivers finalized blocks one epoch at
a time: 32 blocks (384 s of block time) every 6.4 minutes, or 192 Base blocks.
Within a burst the pace is hundreds of times real time. Two rules keep bursts
from reading as a catch-up:

- A burst that arrives within one 5 s sample never closes a sample on its own.
- A wait longer than one sample between two blocks is measured as a sample of
  its own. The partial sample before it is dropped, so the gap after a burst
  (about 1 block time over 6 minutes) reads as real time, and neither a burst
  nor the tail of a replay lends its pace to the wait that follows.

A burst that takes longer to map (for example 192 Base blocks over 10 s) closes
at most one or two fast samples before its gap resets the streak. That stays
under the 3-sample, 30 s entry rule. The same gap rule means that when a
catch-up reaches the head of a bursty chain, the first block after the finality
gap switches back to caught up. That is the first block that could flush
anyway, because flushes are decided when a block arrives.

### The writer's own commits

`commit_blocking` blocks the stream, and the runtime reports each commit's
duration (`PaceDetector::exclude_pause`). The pause stays in the measured
ratio, because the chain kept moving. Leaving it out would make the head look
fast whenever commits take most of an interval: with 700 ms commits every
second, the backlog after each one reads as 3×, which the unit tests check.
The pause does not count toward the 20 s real-time window that ends a
catch-up, nor toward the 60 s missing-timestamp window, so a slow commit during
a catch-up is not mistaken for a stall.

### Missing and non-monotonic timestamps

Block time is the newest timestamp seen so far. Missing, repeated or backward
timestamps never advance it, so they can only point to caught up. A far-future
outlier freezes block time until the chain passes it, which also reads as caught
up, today's behavior. A non-final stream's UNDO and older FINAL events do not
move block time back, so the pace follows the newest NEW block. When no block
has a timestamp, nothing is measured and the writer stays caught up.

### Stalls

A stall shorter than 20 s during a catch-up (a reconnect or back-off) changes
nothing. A longer one reads as caught up: at that moment it cannot be told
apart from the wait for the next finality burst at the head. The first block
after it can flush the open window on the interval, which writes a smaller
file. The writer switches back to catching up 30 s after the replay resumes.

## Runtime, logs and metrics

- `next_mapper_flush_trigger` (`blocks/src/bin/main.rs`) stays pure. It takes the
  window's age and the `StreamPace`, and suppresses only the interval while
  catching up. The size, memory, row and block triggers keep their order.
- `IngestionRuntime::observe` (`blocks/src/bin/ingestion/runtime.rs`) feeds the
  detector after the receipt, UNDO and start-block filters, before timestamp
  routing. The detector is also used in dry runs.
- Each switch is logged once at `info`:
  - `catching up: block time advances faster than wall-clock time, so --flush-interval-secs is suspended and the size, row and block triggers flush`
  - `caught up: block time advances at about wall-clock speed, so --flush-interval-secs applies again`
  - `caught up: no block timestamp to measure the pace, so the stream is treated as following the chain head`

  Each line carries `pace`, `block_num`, `block_time_ratio`, `blocks_per_sec`,
  `evidence_secs` and `flush_interval_secs`. Without an interval the messages
  drop the flag clause.
- `mapper flush emitted record batches` and `committed flush size observation`
  carry `pace`, and the former also carries the window's `blocks`.
- `firehose_parquet_catching_up` (gauge, 0/1) is new.
  `firehose_parquet_flushes_total` gains a `pace` label next to `trigger`, so
  its per-trigger series split by pace.
- The `--flush-interval-secs` help, `.env.example`, the README flush section
  ([Flush interval and catch-up](../cli.md#flush-interval-and-catch-up)),
  the two-bucket settings and the metrics table describe the rule.

## Tests

Detector unit tests (`firehose-parquet/src/flush/pace/tests.rs`, injected
clock):

| Test | Stream shapes |
|---|---|
| `a_fast_replay_counts_as_catching_up_after_sustained_evidence` | Ethereum history at 600× switches after 30 s, not at 29 s, with ratio ≈ 600 and 50 blocks/s. Robinhood at 3× switches. The 50 ms test scale switches at the same point. |
| `real_time_pace_counts_as_caught_up_for_every_block_time` | 0.25 s, 2 s and 12 s blocks at 1×, and 0.1 s blocks with whole-second timestamps: no switch in an hour. A catch-up that reaches the head leaves 20–26 s later with ratio ≈ 1. |
| `final_only_finality_bursts_at_the_head_stay_caught_up` | Ethereum epochs for four hours, the same with an 8 s interval commit inside each burst, and Base bursts mapped over 10 s: no switch. A catch-up that reaches a bursty head leaves at the first block after the gap. |
| `switches_both_ways_repeatedly` | Fast, real time, fast, real time: four switches, then none in 10 minutes. |
| `a_stall_shorter_than_the_exit_window_keeps_catching_up_and_a_longer_one_does_not` | A 15 s stall keeps catching up. A 45 s stall switches at its first block, and the resumed replay switches back 30–36 s later. |
| `the_writers_own_commits_are_not_stalls` | 60 s commits keep catching up, and the same pause as a stall does not. Commits do not count toward the missing-timestamp window. At the head, 700 ms commits every second stay caught up; the same stream without the pause in the ratio reads 3×. |
| `missing_timestamps_never_cause_catching_up` | No timestamps and a constant timestamp: no switch. Solana-style sparse times (every fourth slot missing) are still measured: a fast replay switches, and real time with whole-second times does not. |
| `timestamps_that_disappear_while_catching_up_mean_caught_up` | Caught up exactly 60 s after the last timestamp (`MissingTimestamps`, no ratio), not at 30 s, and back to catching up when times return. |
| `non_monotonic_timestamps_do_not_cause_wrong_switches` | ±3 s jitter at 1×, a block a year in the future, an hour backward jump while catching up, and non-final NEW/UNDO/FINAL interleaving: no wrong switch. |
| `hysteresis_at_the_boundaries` | Exactly 2.0× never enters and 2.01× enters after the sixth sample. 1.26× and 1.9× never leave, and exactly 1.25× leaves after 20 s. Alternating 1.9×/2.1× never enters. Two 60 s fast samples are not enough (three are), and blocks inside one sample do not close it. |
| `production_windows_are_multiples_of_one_sample` | 5/30/20/60 s defaults, the scale and the label values. |

`blocks/src/bin/main.rs` adds
`test_next_mapper_flush_trigger_suspends_only_the_interval_while_catching_up`.
It checks that the interval is suppressed only while catching up, is inclusive
at the head, and that bytes, memory, rows and blocks still fire while catching
up. The existing trigger tests pass an injected window age.
`metrics::tests::test_encode_metrics` checks the gauge and the
`trigger`/`pace` series encoding.

Real-binary tests (`blocks/tests/adaptive_flush.rs`) run a paced local
Firehose: each block is delivered at a scheduled offset with its own
millisecond block time, into a temporary directory with `env_clear()` and
`FIREPARQ_DEBUG_PACE_SAMPLE_MS=50`. The tests use no network and no S3.

| Test | Result |
|---|---|
| `a_fast_replay_suspends_the_interval_and_flushes_at_the_end` | 150 blocks of 12 s every 20 ms (600×, 3 s) with `--flush-interval-secs 1` and a 1 GB size target. One switch to catching up (the `catching_up` gauge reads 1 while running). No interval flush after it, and at most one before it on a loaded machine. The end of the stream flushes while catching up. At most 2 `blocks` parts, the largest with at least 100 of the 150 rows. The interval would have written about 3. |
| `a_real_time_stream_flushes_on_the_interval` | 125 blocks of 40 ms every 40 ms (1×, 5 s). No switch. At least 3 interval flushes, all `caught_up`, and the counter series `{trigger="interval",pace="caught_up"}` is scraped at 2. At least 4 parts, each under 60 rows. |
| `a_final_only_stream_switches_both_ways`, `a_non_final_stream_switches_both_ways` | Fast (100×, 1 s), a 1 s stall, fast (2.5 s), then real time (3.5 s), final-only and non-final (NEW events). Switches `catching_up`, `caught_up`, `catching_up`, `caught_up`. The first block after the stall flushes the open window on the interval, no interval flush happens while catching up, and there are at least two interval flushes at the head. Every block is written once. |

Local commits on the development Mac took 180–700 ms (APFS fsync). The phases
leave room for commits of about a second: a commit delays re-entry into
catching up because it stays in the ratio. The four tests passed six runs in a
row locally, about 10 s per run.

## Limits

- A catch-up slower than 2× the chain's rate keeps the interval, which is
  today's behavior. #658 targets at least 3×.
- The interval is suspended 30–35 s into a catch-up. With head intervals of
  60–300 s that is before the first interval flush. With an interval under
  30 s, each catch-up starts with a few interval-sized files.
- On a smooth chain the tail of a catch-up waits 20–25 s past the interval
  before the first flush at the head.
- A finality delay whose backlog takes more than 30 s to map can switch to
  catching up for one burst period. It switches back at the first block after
  the next gap.
- The pace is not persisted: every restart starts caught up.
- The #658 benchmark (fewer, larger objects and faster catch-up with size-based
  flushes, measured against the old interval cadence) is tracked in #658 and is
  not part of this change.
