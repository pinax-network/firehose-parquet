# Cursor persistence failures stop ingestion (#469)

> Current path (after #600): `build` persists the cursor only as a protected
> mirror through `ProtectedMirror::reconcile`, after the all-table commit has
> advanced authority. The legacy `CursorLocation::save*` / `retry_cursor_save`
> APIs described below had no production caller and were removed in the
> [validation follow-up](validation-ingest-followups.md), which also added the
> real CLI regression `persistent_mirror_save_failure_exits_nonzero_and_rerun_repairs_the_mirror`
> and made every failed mirror reconciliation, including S3 failures before the
> PUT, count in `cursor_save_failures_total`.

## Diagnosis

Partition-boundary flushes, threshold flushes, and the final writer flush logged
cursor persistence errors and continued. A persistent filesystem or S3 failure
could leave the durable checkpoint behind an indefinitely growing data output.
The final-flush callback returned no result, so it could not propagate a failed
checkpoint. Local cursor writes also treated a directory fsync failure after
rename as success despite not establishing crash durability.

## Behavior

This section records the original #469 behavior. Stage 1 of #468 subsequently
retains the three-attempt policy only for local cursors; S3 cursors use one
application attempt and zero transport retries, retaining unresolved ownership.
See [the current remote policy](468-s3-mutation-attempts.md).

Every ingestion checkpoint uses `CursorLocation::save_with_retry_blocking`,
which bridges the synchronous block handler to `save_with_retry`. The same
checkpoint is attempted at most three times, with 1 second and 2 second backoff.
Ingestion does not process the next block while a checkpoint is pending. After
the third failure, the error propagates out of the block handler and ends the
run without reconnecting. The failed-stream exit path does not save a newer
cursor. Failure during a completed stream's final flush also exits nonzero.

Local Parquet encoding, file writes, fsync, and rename run on a Tokio blocking
worker. S3 PUTs and backoff are awaited. The synchronous callback uses
`block_in_place`, allowing even a single-worker multi-thread runtime to keep
serving signals and metrics. Current-thread runtime callers can use the async
API; the blocking bridge returns an explicit error instead of panicking on
that runtime.

A shutdown request after a failed attempt interrupts backoff within 50 ms and
returns a cursor persistence error. It does not convert lost durability into
successful graceful shutdown. An in-flight write is allowed to finish so its
outcome is known. A successful first attempt can therefore checkpoint an
already-written flush even if shutdown was just requested. The three-attempt
bound applies to completed save operations; it is not a filesystem or S3
request deadline, and underlying object-store request retries still apply.

Local directory fsync errors now propagate. The rename may already have
completed in this case; repeating the same checkpoint is safe, but a visible
cursor is not counted as successfully durable until the save returns success.
On platforms without directory fsync support, the existing no-op remains.

## Metrics

- `firehose_parquet_cursor_save_failures_total`: failed save attempts, including
  transient failures that later recover. The existing `errors_total` family
  with `kind="cursor_save"` is also incremented per failed attempt.
- `firehose_parquet_cursor_last_success_timestamp_seconds`: Unix seconds of the
  last successful save in this process. Zero means none has succeeded yet;
  loading a checkpoint at startup does not count as a save.
- Existing successful-save count and last-block gauges update only on success.

Age can be calculated without a periodically updated gauge:

```promql
(time() - firehose_parquet_cursor_last_success_timestamp_seconds)
and (firehose_parquet_cursor_last_success_timestamp_seconds > 0)
```

Treat the zero case separately when monitoring a pipeline expected to commit
checkpoints. A long interval without a flush is not itself a persistence error.

## Validation

- A real local gRPC server supplies two blocks. Persistent cursor filesystem
  failure retries three times on the first block, surfaces an error, performs
  no reconnect, and never invokes the handler for the second block.
- A final-flush test materializes buffered data and verifies repeated cursor
  failures propagate instead of reporting successful completion.
- Injected temporary failures recover on the third attempt; success metrics
  update once, failure metrics update twice, and rendered Prometheus output
  contains the new metrics.
- A blocked temporary cursor path repeatedly fails while preserving the prior
  cursor and its success metrics. An injected directory-fsync failure surfaces
  as an error after rename.
- A single-worker runtime remains responsive to a concurrent shutdown timer
  during the synchronous retry bridge and returns a durability error promptly.
- The async API saves an in-memory S3 checkpoint from a current-thread runtime.
- `cargo test --workspace --locked -j4` passed, including these regressions.
- `cargo build --bin fireparq --locked -j4`, `cargo fmt --all --check`, and
  `git diff --check` passed. The pre-existing unused `transactions_processed`
  assignment warning remains unchanged.

A bounded live check on 2026-09-25 used the explicit Pinax Ethereum endpoint,
blocks 26,049,575 through 26,049,576, one-block flushes, and a fresh local `/tmp`
output directory. It completed successfully and wrote Parquet plus a cursor.
No production output or bucket policy was changed. Failure injection remains
offline so persistence errors do not affect shared services. This does not make data files and cursor updates one
transaction: a failed checkpoint can still require replaying the most recent
already-written flush after restart, but the pipeline can no longer continue
indefinitely past that failure.

## Related follow-up

Review exposed a pre-existing completion path that skips the save entirely when
the final mapper write auto-materializes and empties the writer buffers. That
separate bug is tracked in #572 with a deterministic real-Solana-mapper
regression. This change makes attempted checkpoint saves durable or fatal; #572
ensures that finalization invokes the checkpoint when needed.
