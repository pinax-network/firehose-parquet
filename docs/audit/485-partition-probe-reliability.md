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

Block-range boundary timestamps use the same retry/error policy. A legitimately
missing boundary can remain nullable when missing blocks are allowed, but a
timeout, invalid response, or server failure cannot produce a partition row.
Live frontier polling handles typed transient errors with an interruptible
backoff and also waits between already-caught-up polls.

## Verification

Tests cover typed/contextual error classification, successful RPC connection
reuse before and after a timed-out RPC, timeout retries without skipping,
authentication failure without retry, long missing runs, valid blocks between
exponential samples, overshooting the head, explicit search-budget exhaustion,
and an error inside a skipped interval.

A real local gRPC endpoint drives the compiled CLI through deadline, internal
storage, auth, empty-metadata, and unexpected-number failures in both date and
block-range partition modes. Every scenario starts at the same invalid block;
none creates a fresh output directory or replaces an existing valid partitions
index. This tests the actual startup, auth, probing, and output path together.

Validation uses a whole-command Cargo lock, the shared audit target,
`CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`, and four build jobs.
Final validation and independent review results are recorded before publication.
No live NEAR access or broad production scan is needed for these local failure
and protocol regressions.
