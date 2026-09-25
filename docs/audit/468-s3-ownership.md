# Persistent S3 ownership primitive for #468

Current integration is recorded in [the runtime contract](468-ingestion-runtime.md).
The primitive qualification below remains narrower than production-provider or
end-to-end recovery qualification.

## Status and integration boundary

This is the bucket-ownership primitive for the staged ingestion-transaction
work. It does not enable protected ingestion, change the CLI default, recover
an ingestion journal, delete data parts, or replace the existing maintenance
ownership paths. The integration owner must wire common ownership and all
mutating commands together before any protected mode becomes available.
#468 remains outstanding.

The implementation lives in `firehose-parquet/src/dataset_lock_s3.rs`, with
stateful-store tests and an actual AmazonS3 client against a loopback HTTP test
server. `async-trait` is added only as a dev dependency for the test store;
the already-locked version is reused.

## Persistent record protocol

Callers supply an `Arc<dyn ObjectStore>` addressing the **bucket root**, without
a prefix wrapper. Every prefix in the bucket shares the fixed object
`.fireparq-owner-v1.json`; independent prefixes deliberately serialize.
Scopes are diagnostic bucket-relative paths, not separate leases. Paths are
validated, trailing slashes normalized, then sorted and deduplicated. The empty
scope denotes the bucket root. URLs, queries, absolute paths, dot/traversal
components, ambiguous separators and control characters are rejected.
Operation labels and scope paths must be public metadata, never credentials.

The JSON format is versioned, rejects unknown fields/states/versions, and is
bounded to 32 KiB even if an object lies about its advertised size. Limits also
apply to operation labels (64 ASCII bytes), individual scopes (1 KiB), scope
count (32) and aggregate scope bytes (16 KiB). Records contain a canonical random owner UUID, checked positive generation,
`Owned`/`Released` state, operation, normalized scopes and optional hashes of
operator evidence. No timestamp, expiry, heartbeat, cursor, endpoint, credential
or opaque object version is stored in the record.

| Observed state | Permitted transition |
|---|---|
| Object absent | Conditional Create of Owned, generation 1, fresh owner UUID |
| Released at generation G | Conditional CAS of its exact version to Owned G+1 with fresh UUID |
| Owned | Refuse ordinary acquisition, regardless of age or prefix |
| Own exact record/version, all mutations resolved | Conditional CAS to Released at the same generation |
| Own guard marked uncertain | Refuse ordinary release permanently |
| Corrupt/unknown state, no usable version, exhausted generation | Fail closed |

The owner key is never unconditionally overwritten or deleted. A released
record remains present, preventing delete/recreate ABA. Dropping a guard performs
no remote operation. Owner UUIDs are never reused, and generation increment is
checked rather than wrapping. Metadata is written with JSON content type and
`Cache-Control: no-store, no-cache, max-age=0`.

Every successful transition requires a fresh read matching the exact proposed
record and a usable opaque version. If a PUT response is lost, the same exact
readback can establish success. Returned response versions, when present, must
match that readback. Missing versions, foreign contents, stale responses and
unresolved reads return errors; there is no unconditional write fallback or
in-process takeover. A timed-out request may still complete remotely, so a
failed acquisition can leave an Owned record requiring operator diagnosis.

## Conditional-write qualification

Before touching an absent or Released owner record, acquisition runs a bounded
canary under a fresh, never-reused
`.fireparq-owner-probes-v1/<random-uuid>.json` key. It verifies:

1. Conditional Create succeeds, then exact bytes/version are readable.
2. Duplicate Create is rejected and leaves bytes/version unchanged.
3. A wrong-version CAS is rejected and leaves bytes/version unchanged.
4. Correct CAS succeeds, changes the version, and is read back exactly.
5. The old version no longer matches after that update.
6. The private canary is deleted and absence is confirmed.

Negative probes never target a real owner record. Unsupported or ignored
conditions, inconsistent reads, failed cleanup or ambiguous results prevent
acquisition. The only unconditional deletion is of this unique canary key.
Interrupted/ambiguous probe attempts may leave reserved control objects; their
names are never reused. Artifact walkers must reserve both ownership constants
when runtime integration is enabled.

A successful acquisition needs at most 15 calls to `ObjectStore`, each bounded
to ten seconds including streamed reads. The configured transport may retry
HTTP requests within a call. Probe permissions therefore include
Get/Put/Delete on the reserved canary prefix, and Get/conditional Put on the
fixed owner key. Status is strictly read-only and does not run canaries.
There is no production S3 qualification or probe in this change.

A canary tests the configured client/backend behavior at that point. It does
not certify an arbitrary malicious or changing backend, prove uncached reads
forever, fence external tools, or establish remote-request quiescence. The
backend must honor its conditional-write and consistency contracts. Amazon
S3 documents [If-None-Match/If-Match writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
and [strong consistency for completed operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel).
Those guarantees do not themselves prove that a prior unacknowledged request
has stopped executing.

## Uncertain mutations and operator recovery

`mark_mutation_uncertain(&self)` uses an atomic latch that cannot be cleared.
The transaction store may mark it through a shared borrowed guard, including
from a cancellation/drop latch when a PUT future exits before exact readback.
Ordinary release then refuses to unlock the bucket. Stores borrow
`object_store()` from the guard and share its async control-mutation mutex.
Conditional tombstones and fresh record incarnations are still required for
fixed state slots: a mutex cannot prevent a delayed DELETE from a prior attempt
removing a newly created journal.

`RecoveryAuthorization::assert_provider_quiescence` is explicitly an operator
**assertion**, not an automatic verification function. It requires the exact
expected owner/generation, a confirmed stopped-writer evidence reference, and a
provider confirmation that all prior remote mutations have completed or are
permanently prevented from completing. References are hashed immediately;
only their SHA-256 digests are persisted. Debug output and errors omit backend
objects, opaque versions, raw scope values, payloads and evidence references.

Stopping the old process, cancellation, waiting an interval or seeing an empty
listing is insufficient. A delayed old conditional Create can recreate a
Writing part after rollback deletes its name. If the provider cannot establish
remote-request quiescence, the caller must not create the assertion and safe
plain-glob recovery remains blocked. This module supplies no generic CLI method
for obtaining or fabricating such evidence.

Operator release compares the complete expected record, owner, generation and
fresh version, then conditionally marks it Released. It does not delete parts
or clear a transaction journal. The next recovery session must acquire its own
fresh guard before inspecting or repairing ingestion state. Provider quiescence
must precede Writing rollback, source deletion during maintenance, or any other
operation that could expose a previously occupied final name.

## Validation

Validation is hermetic. The initial focused suite passed 14 tests, covering
stateful conditional-write races, lost success responses, missing versions,
stale reads, corruption/unknown versions, overflow, no age takeover, wrong
operator generations, missing evidence and secret redaction. The race test
uses a barrier so both contenders actually attempt the same Create/CAS version.

The delayed-PUT regression deliberately demonstrates the unsafe sequence by
bypassing protected recovery: it removes a Writing part after process cessation,
then allows an already-sent Create to arrive and recreate it. The legitimate
path refuses release until the provider completion is confirmed, reacquires a
new generation, and only then allows journal-controlled cleanup.

Two HTTP tests use the real `AmazonS3` adapter with a loopback-only server and
synthetic credentials. They verify SigV4 presence, `If-None-Match: *`, exact
`If-Match` on release/reacquisition, non-cacheable metadata, persistent owner
records, lost acquisition/release response reconciliation, and refusal of a
server that silently ignores conditions. No real cloud bucket is contacted.

On integrated main `3cf984b` (including #476 and #485), the full workspace
suite passed **794 tests**, with four intentionally ignored tests (three
benchmarks and the atomic-publication subprocess helper). The final focused
ownership suite passed **15 tests** after the last strict scope-bound and
bounded-race refinements. Formatting and diff checks passed.

Commands used the whole-process Cargo lock, the shared Arrow 60 target, four
build jobs and disabled dev/test debug information:

```sh
python3 /tmp/fireparq-cargo-locked.py cargo test --workspace --locked -j4
python3 /tmp/fireparq-cargo-locked.py cargo test -p firehose-parquet dataset_lock_s3 --locked -j4
python3 /tmp/fireparq-cargo-locked.py cargo fmt --all -- --check
git diff --check
```

Local logs: `/tmp/fireparq-s3-owner-workspace.log` and
`/tmp/fireparq-s3-owner-final-focused.log`. This is offline protocol validation;
no claim of production provider qualification or available provider-quiescence
evidence is made.

The subsequent single-attempt data/cursor client prerequisite and its API
compatibility boundary are documented in `468-s3-mutation-attempts.md`.
