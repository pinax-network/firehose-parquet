# Partition probe reliability (#485)

## Recovery provenance

The original stopped-agent worktree was preserved without edits:
`.claude/worktrees/agent-a4f86a41560fa5ac4`, branch commit
`2febdbdf9c1ac09cbab5fa1acb43db02c5b633b6` (`wip: partitions probing`).
Its two tracked uncommitted files were `blocks/src/bin/main.rs` and
`firehose-parquet/src/cli.rs`.

The exact combined binary diff from parent
`bf794c331749ff939fe229f8f01e66fe5079e6e1` to that worktree was saved before
recovery. It contains 45,596 bytes and has SHA-256
`3d16037bf9d4bc41e2470ef186cb8adc593b0cba70370c109de3589baae61fbf`.
It was applied in a new isolated worktree on branch
`codex/partition-probe-reliability`, starting from main `c88abcc`.
The only initial merge conflict was the already-present blocks dev dependencies;
current dependencies were retained. Current provider-scoped credentials and
mandatory endpoint metadata startup behavior were preserved.

## Diagnosis and changes

The original implementation returned `Ok(None)` for a timed-out unary fetch,
opened a new connection for each probe, and stopped looking after 16 missing
slots. The recovered WIP added typed timeouts, a cached channel, and a longer
search, but review found three correctness gaps:

- Broad error-message matching could turn storage or authentication failures
  into skipped blocks.
- If all exponential samples were missing, it returned no block without
  checking the unsampled gaps. A valid block between samples could be skipped,
  and exhausting a search window was indistinguishable from reaching the head.
- Block-range timestamp probes still swallowed every fetch error and could
  write an apparently valid partition row after a timeout or server failure.

Unary fetch timeouts now return `FetchTimeoutError`, including the requested
number and duration. A Tokio `OnceCell` caches the connected tonic channel;
separate RPC clients clone it and retain per-request auth metadata. A cancelled
RPC does not discard the channel.

Classification follows typed errors and status codes. Only gRPC `NotFound` or
the exact typed `Unknown` compatibility envelope for `block not found in files`
establish a missing block. Auth/permission/invalid-request failures fail fast.
Timeouts and transport/unavailable failures are retryable. Unrecognized errors
receive bounded retries and then surface; arbitrary text cannot establish
absence or justify indefinite live retry.

Long missing runs use exponential samples followed by a linear check of skipped
intervals to return the exact first available number. Gaps are checked even when
every exponential sample is missing. The 65,536-block search budget bounds work;
exhaustion without head evidence is an error, not a head result. The special
`u64::MAX` number is never sampled. Empty metadata and unexpected later-block
responses fail instead of becoming valid boundaries. Endpoints returning their
earlier head block for a request beyond the head retain that compatibility path.
This is inherited endpoint behavior used by these searches; the checked-in
Fetch protobuf does not guarantee that an earlier reply universally proves
the head. The change preserves this existing compatibility assumption rather
than establishing a new protocol guarantee.

Block-range boundary timestamps use the same retry/error policy. A legitimately
missing boundary can remain nullable when missing blocks are allowed, but a
timeout, invalid response, or server failure cannot produce a partition row.
Live frontier polling handles typed transient errors with an interruptible
backoff and also waits between already-caught-up polls.

## Verification

Tests cover typed/contextual error classification, successful RPC connection
reuse before and after a timed-out RPC, timeout retries without skipping,
authentication failure without retry, long missing runs, valid blocks between
exponential samples, overshooting the head, explicit exhaustion of the full
65,536-block search budget, the maximum-number sentinel, and an error inside a
skipped interval. Large search-window tests use in-memory scripted responses;
they make no wide live scan.

A real local gRPC endpoint drives the compiled CLI through deadline, internal
storage, auth, empty-metadata, and unexpected-number failures in both date and
block-range partition modes. Every scenario starts at the same invalid block;
none creates a fresh output directory or replaces an existing valid partitions
index. This tests the actual startup, auth, probing, and output path together.

Validation uses a whole-command Cargo lock, the shared audit target,
`CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`, and four build jobs.
Final integration `a8a562a` includes main `73257c2`, including shutdown (#473),
the single-partition writer contract (#477), and atomic local publication
(#578), plus the reviewed reward-index fix at `bd501f2` (#500). The initial import conflict in the blocks
manifest and the integration conflicts in gRPC test additions and main imports
were resolved by retaining both behaviors and all regressions.

- `cargo test --workspace --locked -j4`: **768 passed, 0 failed, 4 ignored**
  (118 + 206 + 1 + 1 + 1 + 435 + 3 + 3); all doc tests passed. One ignored
  helper is exercised by the active atomic-publication subprocess regression.
- The preceding integration on `fdb1f98` passed its then-current 757 tests.
- Binary build, formatting, `git diff --check`, and Bash/Zsh/Fish completions
  passed. The existing unused `transactions_processed` assignment warning remains.
- The CLI failure test completed all 20 combinations of failure type, partition
  mode, and fresh/existing destination. Authentication failures made one fetch;
  the other invalid responses exhausted four attempts, always at block 100.
- The original worktree HEAD, two modified files, and exact combined diff hash
  were checked again after implementation and remain unchanged.

A bounded live check on 2026-09-25 used the probe source committed as `9e5fa99`,
built and copied under the whole-command lock, the explicit Pinax Ethereum endpoint, provider-scoped
credentials, and fresh local output. `partitions build --partition block_range
--block-range-size 1 --start-block 26049575 --stop-block 26049577` completed with
exactly two fetch probes and two rows: `[26049575, 26049576)` and
`[26049576, 26049577)`. Their start/end timestamps were respectively
`1790280203` and `1790280215`, matching the already-verified block-table sample
from #469. No cursor file was created. Local inspection used the existing
integer-second Parquet timestamp columns; it did not repeat the live fetch.
Evidence is in `/tmp/fireparq-485-live-859e2_2_/summary.json` on the audit host.
The subsequent atomic-part writer and reward-index integration changes neither
this probe code nor the partitions index writer; its combined offline suite
passed as recorded above.

This live check qualifies two historical block-range boundaries only. It does
not qualify long missing-slot behavior against a production endpoint, live head
or incomplete partitions, nonmonotonic timestamps, or the separate #486
boundary-search issue. No live NEAR retry or broad production scan was made.

After timestamp validation (#476, PR #584) merged as `34cb6e2`, integration
commit `4104a1b` retained the cached Fetch channel, typed probe failures, checked
metadata conversion, negative timestamp support, and both sets of regressions.
The full workspace suite passed again: **779 passed, 0 failed, 4 ignored**;
formatting and all doc tests passed. This is the combined main state qualified
for this PR's merge; the previous bounded live evidence remains scoped as above.
