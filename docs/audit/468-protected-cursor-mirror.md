# Protected cursor mirror adapter

This is a staged prerequisite for the ingestion transaction controller. It adds
crate-private APIs in `firehose-parquet/src/ingest/mirror.rs`; it does not expose a
CLI mode or change existing ingestion defaults. Issue #468 remains outstanding
until the controller, recovery and complete caller integration are qualified.

## Authority and compatibility

`ProtectedMirror` borrows the already acquired `DatasetOwnership` capability and
a cloned exact `MirrorBinding`, avoiding self-referential session lifetimes. The session must supply the actual resolved S3 service
identity digest; a stored service identifier alone is not runtime validation.
Local and disabled mirrors reject a remote service identity. S3 mirrors require
the explicitly held cursor bucket and a key inside its declared scopes. No
second ownership acquisition or automatic owner release occurs in the adapter.

A protected mirror retains the existing seven-column `cursor.parquet` row schema.
Its strict versioned footer also carries the complete checkpoint, stream digest,
checkpoint digest, original origin/partition anchor, mapper epoch and semantic
flags. Every duplicated row/footer field is checked against those records. Cursor
and block identity, accepted ordinal, routing and proven completed request bound
come from mandatory authority. A requested but unproven stop bound is not
advertised as complete. Actual accepted source timestamps are stored separately
in the checkpoint.
Direct routing preserves that nullable source timestamp; synthetic routing uses
the committed anchor, falling back to the actual source timestamp when no anchor
exists. Negative and zero source seconds remain valid values. The cursor's update
time describes mirror publication; it does not participate in ingestion progress.

Initialization calls `require_absent_for_initialization` before creating initial
authority. Any existing object blocks initialization, including zero-byte files
and otherwise valid legacy cursors. The adapter never turns a mirror into
resume authority. An empty accepted prefix does not create a mirror.

Reconciliation repairs a missing mirror or a valid mirror at a lower accepted
ordinal. An exact checkpoint is unchanged. Ahead, foreign-stream, corrupt and
conflicting same-ordinal mirrors fail closed. A same-ordinal repair is permitted
only when the stored checkpoint is the authority's immediate predecessor, the
accepted event and routing are identical, and the proven completed stop advances.
This covers successful empty-tail completion without adopting arbitrary mirror
history.

## Bounded decoding and privacy

A mirror is limited to 4 MiB, exactly one row group and one row, the exact cursor
schema, at most 512 KiB of declared uncompressed column bytes, and uncompressed
Parquet pages. Physical data/dictionary pages are checked for bounded byte size
and one-value cardinality before Arrow value decoding; footer row counts alone
are insufficient. Version 1 accepts at most a dictionary page and a data page per
column. Duplicate, missing and unknown footer keys are rejected except the
standard embedded Arrow schema. Unknown envelope fields, invalid digests and
invalid checkpoint semantics are rejected.

These controls contain private opaque cursors. They are not logged, included in
`Debug`, or copied into backend error messages. The existing cursor encoder and
row decoder become crate-visible for reuse; their legacy behavior is unchanged.
The protected adapter applies strict validation around them and never invokes
legacy cursor loading/logging.

## Local publication and recovery

The guard's local path identity is revalidated before reads and writes. Explicit
root aliases retain their original spelling after canonical scope validation.
New directories use mode 0700 and temporary/final mirror files use mode 0600.
Publication writes a unique sibling temporary, syncs its contents, atomically
renames it, and syncs the full canonical and lexical directory ancestry.

A failure before rename leaves the previous complete mirror. A failure after
rename retains the new complete file and returns an error; it does not invent a
rollback. An unchanged local mirror still resets private permissions, syncs the
file and syncs both ancestry chains. Therefore seeing matching bytes after a
prior directory-sync failure cannot skip the outstanding durability work.
Temporary cleanup is best effort on returned errors; process death may leave a
private temporary which is never authoritative.

Local publication has three bounded attempts with one/two-second backoffs and
shutdown checks. Each attempt releases the in-process mutex before awaiting a
backoff, then revalidates and rereads under the guard. Invalid/conflicting input
is not retried. Existing-file durability failures fail closed. Cursor success
metrics advance only after durable publication; failed publication attempts
increment the existing cursor failure/error counters. An unchanged mirror does
not double-count a save.

## Remote publication and uncertainty

The session's `DatasetOwnership` constructs zero-transport-retry mutation
clients; reads through those clients also have zero transport retries. The
adapter uses the borrowed cursor bucket owner/store and its shared control mutex.
It performs one conditional Create for absence or CAS of the observed version
for a behind mirror, with private no-cache headers. It rechecks the persistent
owner immediately before publication, requires a usable acknowledgement version,
and verifies the exact bytes and version by a bounded readback.

Any error or cancellation after the PUT starts permanently marks that owner
uncertain and records one failed save. There is no application retry, delete,
overwrite fallback or Drop release. A lost success response may leave a complete
new mirror; a later ordinary retry is refused while uncertainty remains. Recovery
must use the established stopped-writer plus provider-confirmed remote-request
quiescence process. A later GET/PUT success cannot prove that earlier requests
have drained. Pre-publication validation failures do not falsely mark an attempted
remote mutation.

## Validation

Focused tests cover initialization refusal, legacy row readability, lower/equal/
higher ordinals, completion-only predecessor repair, all duplicated row columns,
malformed metadata, foreign streams, size/schema/row-count/compression limits,
forged footer counts, null/negative Solana routing timestamps, local publication
failure stages, unchanged-file re-sync, private permissions, alias ancestry and
retargeting, bounded retries/shutdown metrics, explicit S3 service binding,
Create/CAS behavior, lost successful responses, missing versions, readback
failures and cancellation. S3 tests use stateful conditional in-memory stores;
no production bucket or live Firehose requests are made.

Final qualification on the mirror feature plus source-evidence model commit
`8d66bed` (local equivalent `dc9c68b`) passed `cargo fmt --all --check` and
`cargo test --workspace --locked -j4`: **884 tests passed, five intentionally
ignored helper tests**. This includes all 15 mirror tests and the existing
mapper, final-write, cursor, protocol and metrics regressions. Cargo ran under
the whole-process shared-target lock. The final API owns a cloned binding while
borrowing ownership/metrics/shutdown; its construction from a short-lived binding
is compiled and exercised. No live qualification was claimed for this staged
adapter.
