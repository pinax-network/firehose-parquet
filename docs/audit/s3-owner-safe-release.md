# Release S3 bucket ownership after a provably safe `build` failure

Refs #468 and #591; part of #463.

## Problem

An S3 `build` acquires the persistent bucket owner before it reads the resume
state. Only the success path released it: `run_ingestion` returned early with `?`
on every error after acquisition, so the guard was dropped and the owner record
stayed `Owned`. Dropping a guard sends nothing by design
([468-s3-ownership.md](468-s3-ownership.md)).

The owner was kept even when no remote mutation was attempted, for example
after a Blocks stream rejected with `Unauthenticated`, a stream error before the
first flush, a refused resume parameter, or a mapper error. Every other writing
command on the bucket then failed with "bucket ownership is held" until an
operator ran `fireparq recovery release`, which needs provider-quiescence
evidence that such a run never needed. Transient failures became manual incidents.

## Rule

`DatasetOwnership::finish(result)` (`firehose-parquet/src/dataset_lock/operation.rs`)
now ends every `build` after acquisition. `run_ingestion` runs the rest of the
command in `run_owned`, which borrows the guard, and passes its result to `finish`.

- **Success** releases every owner, as before.
- **Failure** releases each bucket owner whose uncertainty latch is clear. It
  keeps each owner whose latch is set, or whose release request itself fails.

A clear latch proves that no request this command sent can still take effect:

1. Every S3 mutation site arms a drop guard (`MutationAttempt`, `RemoteAttempt`,
   `RemoteDeletion` and merge recovery's `Attempt`). The guard sets the latch
   unless the request's outcome was proven. Early returns, errors, cancelled
   futures and unwinding all run the guard.
2. Mutation clients make exactly one attempt, with transport retries disabled
   ([468-s3-mutation-attempts.md](468-s3-mutation-attempts.md)). No hidden
   duplicate request exists.
3. `finish` consumes the guard. Every borrower (setup, session, runtime,
   pipeline futures) has ended, so no request can still be running.

The success path relies on the same condition: `S3Ownership::release` only
checks the latch and the exact owner record. `finish` applies that check after a
failure too. It matches the state table in `468-s3-ownership.md`: "own exact
record/version, all mutations resolved → conditional CAS to Released".

### Definite refusals

A proven outcome was a verified readback. A request that the provider refuses
with **HTTP 401 or 403** is now also proven. S3 does not apply a request it
rejects, so it cannot complete later. `dataset_lock_s3::provider_rejection` and
`ProviderRejected` classify only these two statuses (object_store maps only a
received 401/403 response to `Unauthenticated`/`PermissionDenied`). They are
applied to every ingestion PUT:

- protected part publication, native streamed (`PreparedUpload::send`) and
  object_store paths (`writer/protected.rs`)
- control records: authority, pending journal and tombstones (`durable_state_s3.rs`)
- the S3 cursor mirror (`ingest/mirror.rs`); the save still counts as failed

Everything else stays uncertain, as before: timeouts, transport errors,
connection resets, lost or unreadable responses, 5xx, 409/412, and any
acknowledgement whose exact readback fails. A 412 or 409 on a conditional Create
means another request occupies or is writing the deterministic name, so it is
not treated as proof of quiescence. DELETE paths (Writing rollback, merge
recovery, maintenance cleanup) are unchanged and still set the latch on any error.

### Journaled Writing and Committed state

A failure can leave a pending journal, for example a 403 on the first part after
Writing was persisted, or a local error after some parts were published. The
design rules are:

- Provider quiescence must precede Writing rollback: a delayed old PUT could
  otherwise recreate a removed part ([468-s3-ownership.md](468-s3-ownership.md)).
- A failed commit poisons its controller. The caller must reopen through recovery
  under resolved ownership ([468-transaction-controller.md](468-transaction-controller.md)).

A clear latch is that quiescence for this writer: every request it sent has a
known outcome, and none can arrive later. The pending journal is left in place.
The next owner acquires a new generation, and its mandatory startup recovery rolls
Writing back (verifying every present part against its frozen receipt) or
Committed forward before it opens Blocks. That is the path an operator release
leads to, without the operator having to assert what the process already proved.

So ownership is kept after a failure exactly when the latch is set: a
transaction in Writing or later is kept whenever one of its requests is not
definitely resolved. A pending journal whose requests are all resolved does not
keep it.

Update (#643 L4): a Delta log commit (a conditional create of
`_delta_log/<version>.json`, after the journal is Committed) does not set the
latch when its outcome is unknown. The next start reads the table's `txn` and
commits only what did not land, and every arrival order of a delayed copy ends
with one copy, so a late arrival cannot undo anything recovery did. The
[review](643-l4-delta-recovery.md#the-owner-latch-and-log-commits) has the
reasoning; every other request keeps the rule above.

## Exit paths

| Exit | S3 ownership | Reason |
|---|---|---|
| Success (stream completed, or requested range already complete) | Released | Unchanged. All synchronous work finished; the latch is clear. |
| Failure before any mutation after acquisition (Blocks `Unauthenticated`, stream error before the first flush, refused resume parameter, mapper error) | **Released** (new) | Only the owner CAS, the canary and possibly the verified initial authority write were sent, and each has a definite outcome. |
| Failure after resolved mutations, including a pending Writing or Committed journal | **Released** (new) | Latch clear, so no request can take effect later. The next build recovers the journal first. |
| Definite 401/403 on a part, control-record or mirror PUT | **Released** (new) | A refused request did not take effect. It was previously treated as uncertain. |
| Ambiguous request: timeout, lost acknowledgement, connection reset after send, unverifiable readback, 5xx, 409/412 | Retained | The latch is set: the request may still complete. The error names each owner, the reason, and the exact commands. |
| Failure where the release request itself fails (network down, record changed) | Retained | The owner record could not be proven Released. The message says only the owner record is in doubt. |
| First signal (SIGINT/SIGTERM), graceful shutdown | Released | Unchanged. The runtime discards the window and returns success. An in-flight flush completes or fails first. A failure goes through the failure rows above. |
| Second signal (forced exit, code 130) | Retained | `std::process::exit` runs no destructor or release, and a request may be mid-flight. The warning now says that ownership is retained and names `fireparq recovery status`. |
| Panic unwinding out of `run_ingestion` | Retained | Not provably safe: a panic is a broken invariant, and the process does not trust its own bookkeeping to send a new CAS. The guard's drop sends nothing and logs the recovery commands. Panics inside spawned encoder/lane tasks become ordinary errors and follow the failure rows. |
| Other mutating commands (merge, rollup, truncate, partitions build, recovery recover) | Unchanged: retained on any error | They still end with `release` on success only. Their drop now logs the same recovery commands. Adopting `finish` there is a separate change. |

Local ownership is unchanged. `flock` locks are released when the guard or the
process ends, on every path including the second signal and a panic. `finish`
drops the local guard after the remote decision; a test confirms that a failed
local-only run returns its error unchanged and frees the OS lock.

## Operator message

When an owner is kept, the build's error is wrapped with guidance. anyhow prints
the guidance first, followed by the build's own error under `Caused by:`:

```text
Error: S3 bucket ownership was retained; every other writing command on the bucket fails with "bucket ownership is held" until it is released
- s3://bucket/dataset/mainnet: owner 7c9e…, generation 4, kept because a request to this bucket had an uncertain outcome (it timed out, lost its acknowledgement, was interrupted, or its result could not be verified) and may still take effect.
  Inspect: fireparq recovery status s3://bucket/dataset/mainnet
  Release: fireparq recovery release s3://bucket/dataset/mainnet --expected-owner 7c9e… --expected-generation 4 --stopped-writer-evidence <reference> --provider-quiescence-evidence <reference>
Run the release only after confirming that this process has exited and that the provider has completed or permanently revoked every request it sent; elapsed time or process exit alone is not that evidence. Use the same AWS credential and endpoint settings. The next build then recovers any pending transaction before it streams.

Caused by:
    native conditional upload failed; retain ownership for quiescent recovery
```

The URI is the owner's first recorded scope. For build output, that is the
dataset root, so `recovery status` also shows its state and pending summaries.
The message contains only public owner metadata and bucket-relative scopes.
After a failure that releases ownership, the build's error is returned unchanged,
and an info log records the release.

## Validation

All tests are hermetic. The native tests use the stateful loopback provider in
`s3/upload/fixture.rs`, which gains `ForbiddenPart` and `ForbiddenControl` 403
faults. The others use the in-memory store. No real S3 bucket or Firehose
endpoint was contacted.

- `ingest/controller/tests/safe_release.rs` (native loopback provider):
  - a 403 on the first part: one PUT, nothing stored, latch clear, journal still
    Writing. `finish` releases, the next owner acquires generation + 1, its
    recovery rolls Writing back, and the same window commits once.
  - a resolved local failure after the first part was published: released; the
    next owner's recovery deletes that part, then commits.
  - a lost part acknowledgement: the part is stored, the latch is set, the owner
    is retained and reacquisition is `Busy`. The message names the exact status
    and release commands with the owner UUID and generation, keeps the cause, and
    leaks no credential, signature or endpoint. No retry and no rollback is sent.
  - a 403 on the initial authority write: latch clear, released.
- `ingest/session/tests.rs` (in-memory S3):
  - a Blocks `Unauthenticated` error before the first flush after a session
    initialized authority: released, the next command acquires immediately and
    resumes from the same authority.
  - graceful shutdown with one committed flush and one discarded buffered block:
    released, and the next run resumes after the committed flush.
- `ingest/mirror/tests.rs`: a 403 mirror PUT counts one failed save, stores
  nothing, leaves the latch clear, and `finish` releases.
- `dataset_lock/operation/finish_tests.rs`: an unchanged error and immediate
  reacquisition after a failure with no mutation; retained guidance text and
  `Busy` reacquisition for an uncertain failure; success, and an uncertain success
  refused; a failed release request reported with its reason (bucket-root URI);
  per-bucket decisions (output released, uncertain cursor bucket kept); a panic
  in a task holding the guard keeps the owner `Owned`; a local-only failure
  frees the `flock`.

Disabling the 403 classification in the native publication path makes the 403
part test fail on its latch assertion, so the test exercises the new rule.

These are 14 new tests. On macOS, `cargo test --workspace --locked` passed
**1,235 tests with 15 intentional ignores and no failures**: 810 core library,
200 `blocks` library, 174 binary unit and 51 integration/generator tests.
`cargo fmt --all -- --check` and `git diff --check` passed. Linux CI is checked
on the PR head.

## Limits

- The 401/403 rule assumes that the provider, like S3, does not apply a
  request that it refuses. S3-compatible backends must honor that, as they must
  honor conditional writes.
- Merge, rollup, truncate, partitions build and recovery recover still retain
  ownership after any error. A retained owner from any command still needs the
  existing provider-quiescence release; this change adds no bypass.
- The binary's S3 path cannot be driven by the subprocess suite, because native
  ingestion requires HTTPS. The decision is tested in the library against the
  loopback provider, and `run_ingestion` has one call site for it.
