# Resolve uncertain S3 mutations by exact readback (#646, option 3)

Refs #646, #468 and #591.

## Problem

On 2026-09-29 a rolling restart of the Ceph RGW gateway in front of the
riv-dev1 lake cut the in-flight requests of two backfilling writers. Each
`build` exited with an ambiguous request outcome, so its uncertainty latch was
set and `DatasetOwnership::finish` kept the bucket owner. Every restart then
failed with `bucket ownership is held` until an operator ran `fireparq recovery
status` and `recovery release` with evidence ([k8s-parquet runbook,
2026-09-29](https://github.com/pinax-network/k8s-parquet/blob/main/docs/riv-dev1-runbook.md)).

The latch is correct: a request whose outcome is unknown may still take
effect, and a successor must not act on its absence
([s3-owner-safe-release.md](s3-owner-safe-release.md)). But several requests
were left ambiguous only because nothing looked:

- A **part** PUT that failed or timed out set the latch at once. Unlike control
  records and the owner record, a part was never read back after an error, so
  a part the provider had stored, whose acknowledgement was lost, kept the
  owner.
- The **cursor mirror** PUT was treated the same way.
- **Control records** and **owner transitions** were read back once,
  immediately. A read during the same provider outage failed, and the attempt
  stayed uncertain even if the provider applied it a second later.
- The owner **release** in `finish` was sent once. If the provider was
  unavailable at that moment, the owner was kept even with a clear latch
  ("the release request itself fails" in the exit table).

## When is a readback proof?

A readback proves that an unacknowledged request *was applied* when all of
these hold. It never proves that a request *was not* applied.

1. **One request.** Mutation clients make exactly one attempt with transport
   retries disabled ([468-s3-mutation-attempts.md](468-s3-mutation-attempts.md)),
   so no second copy of the request exists. The provider is trusted to
   execute a request at most once, as the ownership design already assumes.
2. **Only this request could produce the observed state.** The key is written
   only under the bucket owner this process holds, and the exact bytes could
   not have been there before the request:
   - a part is a conditional Create (`If-None-Match: *`) of a name the
     transaction plan proved absent;
   - a control record carries a fresh incarnation;
   - the mirror is rewritten only when its contents change;
   - the owner record carries this owner's UUID and generation.
3. **Seeing the object means the request finished.** S3 and RGW make an object
   visible only when its PUT commits. After that the request can only send its
   response; it has no further effect.
4. **A duplicate would be refused.** Every such request is conditional: a
   Create fails while the key exists, and a compare-and-swap names a version
   that has already been replaced and never recurs.

If the readback sees the key absent, the old state, or other contents, the
request may still be in flight and may still land. That case keeps the latch,
exactly as before. No request is resent, and no absence is ever taken as proof.

### Why not fence the absent case

The issue asked whether a late arrival could be made harmless by conditioning
it on the owner generation. S3 conditions a write only on the state of its own
key, not on another object such as the owner record. Part names are
deterministic for a window, so a successor that rolls Writing back and reruns
the window reuses them. A delayed part Create could then recreate a name that
rollback removed. A delayed rollback DELETE could remove a part that the
successor later committed. Resending the Create to occupy the name would be a
retry, which breaks rule 1 without proving that the first request drained.
Those cases still need provider quiescence (`recovery release`) or option 1 or
2 of #646 (a fenced lease or automated evidence). They are out of scope here.

## What changes

`dataset_lock_s3::reconcile_by_readback` reads back on a bounded schedule:
0, 1, 3, 7 and 15 s after the failure, plus each read's own timeout. It returns
as soon as one read observes the exact state the request writes, and stops at
once on a state that can never become proof. Each call site defines "exact":

| Request | Before | Now |
|---|---|---|
| Part publication, `object_store` and native paths: a failed or timed-out conditional Create | Latch set at once; no read | Read back on the schedule. The part is resolved when the stored object matches the frozen receipt (size, SHA-256, footer and row count, the same verification as an acknowledged part). The publication then succeeds and the transaction continues. A 412/409 (`condition_refusal`, native `ConditionRefused`) is not read back: another request wrote the name, so the object is not this request's, and it keeps the latch as before. |
| Cursor mirror: a 412/409 | Latch set | Unchanged, for the same reason. |
| Cursor mirror conditional PUT: failed or timed out | Latch set at once | Resolved when the mirror holds exactly the new checkpoint bytes, as for an acknowledged save. |
| Control record (authority, pending journal, tombstone) conditional PUT | One immediate read | The same exact comparison on the schedule. |
| Owner record transition (acquire, release, operator release) | One immediate read | The same exact comparison on the schedule. |
| Owner release in `finish` / `release` | One attempt; kept on any failure | Up to five attempts, started 0, 5, 15, 30 and 60 s after the first while the owner record cannot be read or the release outcome is unknown. The release is a compare-and-swap from this owner's exact Owned version to one fixed Released record, so every arrival order of the attempts ends in that record. A read that finds it counts as released. A delayed attempt arriving after a successor acquired fails its `If-Match`, because the Owned version never recurs. Any other state (a changed record, a foreign owner, a missing version) stops at once, as before. |

Unchanged:

- **Absent after the window.** Any request whose readback never shows the exact
  state keeps the latch, so ownership is retained with the recovery commands.
- **DELETEs** (Writing rollback, merge recovery, maintenance cleanup) still set
  the latch on any error. A readback of absence cannot prove that a delayed
  DELETE will not remove a later object of the same name.
- **Delta log commits** stay outside the latch ([#643 L4](643-l4-delta-recovery.md#the-owner-latch-and-log-commits)).
- **401 and 403** still resolve immediately ([s3-owner-safe-release.md](s3-owner-safe-release.md)).
- **Cancelled futures** (a second signal, a panic) still set the latch through
  the drop guards. The readback runs inside the attempt, so cancelling it
  leaves the attempt unresolved.

## Which failures now self-resolve

- A **lost acknowledgement** after the provider stored the request (connection
  reset or gateway 5xx after commit, or a client timeout after commit). The
  build continues, with a warning, instead of exiting.
- A request the provider **finished after the error**, within the readback
  window.
- A **readback during a short outage**: the read is retried until the provider
  answers.
- A **release sent while the provider was down**, if it comes back within the
  release window.

## Which still need a manual release

- A PUT the provider may still apply but had not applied by the end of the
  window (typically a request cut mid-body, which the provider will never
  apply, but that cannot be told apart from one still being processed).
- Any ambiguous DELETE.
- A 409/412 on a part or mirror PUT: another writer occupies the name.
- A second signal, a panic, or a release that fails for the whole window.
- Other mutating commands (merge, rollup, truncate, partitions build,
  recovery recover), which still keep ownership after any error.

## Validation

All tests are hermetic: the stateful loopback provider (`s3/upload/fixture.rs`)
for the native path, and the in-memory stores for the rest. No real S3 bucket
or Firehose endpoint is contacted. The test build shortens both schedules to
milliseconds.

- `ingest/controller/tests/safe_release.rs` (native loopback):
  - **Lost part acknowledgement:** the provider stores the part and drops the
    connection. The readback proves it, the latch stays clear, the transaction
    commits with one part PUT (no retry), and `finish` releases. This replaces
    the old test that expected the owner to be kept.
  - **Reset before store:** nothing is stored. The latch is set, ownership is
    kept with the exact recovery commands, and reacquisition is `Busy`.
  - **Part stored late, inside the window:** resolved.
  - **Part stored late, after the window:** the owner is kept and the delayed
    PUT lands while it is still Owned. Reacquisition stays `Busy`, so no
    successor exists for the late arrival to race.
- `dataset_lock_s3/tests.rs`:
  - a release whose reads and CAS fail for the first attempts succeeds on a
    later one;
  - a deferred first release CAS replayed after release and a successor's
    acquisition is refused, leaving the successor's record unchanged;
  - an owner-record readback that fails once and then succeeds is resolved;
  - a stale or foreign record still stops at once.
- `safe_release.rs`: control records (journal and authority) whose PUTs the
  provider applies shortly after dropping the connection are resolved by a
  later readback; one immediate read would have seen the old record.
- `ingest/mirror/tests.rs`: a lost mirror acknowledgement is resolved, counts
  as a successful save, and leaves the latch clear.
- `writer/protected/tests.rs`: a lost `object_store` part response is resolved
  with one PUT; an identical object already under the name (412) still sets
  the latch.
- Existing tests that used a lost acknowledgement to reach the uncertain path
  (`native_upload.rs`, `concurrency.rs`, `writer/protected/tests.rs`) now use a
  request cut before the provider applied it, which still keeps the latch.

Reverting each piece fails its tests. With a single immediate read or a single
release attempt, the window, release and control-record tests fail. Without
accepting a proven part, the part readback tests fail.
