# Issue 468: all-table controller and physical recovery

This private controller follows the reviewed model and prepared-writer
foundations. The command does not select it yet. Eligible-root initialization,
the real protected cursor mirror, received-envelope/routing integration, bounded
completion and protected maintenance behavior remain required before enabling
the complete path or declaring issue 468 satisfied.

Bounded completion is now a separate authority-only transition. It requires a
clean stream result, the exact fully acknowledged frontier with no received or
accepted-but-uncommitted envelopes, and an accepted last event at the final
requested block or beyond. Rows and a clean sparse/empty EOF are insufficient.
An unproven tail retains the durable accepted prefix and returns a diagnostic
requesting an observed ending bound. The completed stop may only extend; the same
bound is a true no-op and a shorter bound requires a new output. Completion
changes neither the accepted ordinal nor original partition origin. A crash
after authority but before mirror is repaired from that authority on reopen.
The focused suite now passes 37 tests and one exercised child-helper ignore,
including no-op byte preservation, extended-stop append and rejection of empty,
sparse, unresolved, unacknowledged or interrupted completion evidence.

`ingest/controller.rs` owns a flush's entire batch map until all-table commit. It
builds the complete sorted inventory from the authoritative descriptor, checks
every batch/schema/partition and planned destination before writing, and refuses
any preexisting planned final or temporary name. Only then does it persist
Writing. Each table is encoded separately, staged locally when applicable,
journaled with its immutable full-file receipt, and published. All final files
must verify before Committed; then authority advances, the mirror reconciles,
temporary names are removed and pending is durably cleared. No independent table
acknowledgement or mirror-leading-authority transition exists.

A separate fail-fast in-process session permit permits only one transaction
controller per borrowed local or S3 owner. It lasts for the controller's lifetime
and is distinct from each short control-record mutex. This prevents two borrowers
from racing multi-slot decisions (in particular a completion-only state update
versus creating Writing) while allowing the active controller's own part/control/
mirror calls. Dropping the controller releases this in-process permit; it never
releases persistent remote ownership. Local and S3 duplicate-controller/drop
regressions pass, alongside the source-timestamp follow-up's combined 34 focused
tests and one exercised subprocess-helper ignore.

Every failed or cancelled commit permanently poisons that controller instance.
The caller must stop and reopen through recovery under resolved ownership. A
Writing journal at its predecessor rolls back. A Committed journal at its
predecessor or target verifies every final file and rolls forward; missing or
corrupt committed data is an error, never a request to map the prefix again.
Once pending is absent and durable, old final receipts are not a permanent
physical inventory: later supported compaction may replace acknowledged parts.

`ingest/parts.rs` performs the exact-path recovery operations. Before any Writing
deletion it verifies **all** existing public files against their frozen receipts.
A public file without a receipt, corrupted bytes/footer/schema, a non-regular
entry or nested symlink stops cleanup. The prior pre-Writing absence check under
exclusive ownership establishes ownership of the deterministic temporary names;
these may contain incomplete bytes and are removed only by their exact journal
paths. Arbitrary prefix or filename-pattern deletion is not used. A Committed
transaction cannot use the rollback method.

Local cleanup syncs existing ancestry even for already absent names: an earlier
unlink may have succeeded before its directory sync failed. Likewise transaction
startup stabilizes the two observed local control slots under the shared guard,
rechecks exact versions/presence, syncs present control files and all existing
directory ancestors, then uses the observed pair for recovery. This prevents a
visible but unsynced pending unlink from being mistaken for durable cleanup.
Read-only recovery status continues to use observational loads without this
durability step. Local verification already reestablishes file/directory
durability through the prepared writer.

S3 uses the borrowed persistent bucket owner and its zero-transport-retry store.
There is one complete conditional Create per part and no temporary data object.
Rollback deletions have one application attempt, exact current-owner validation,
and an uncertainty guard spanning the request. A lost response or cancellation
retains Owned and pending even if the object disappeared. A later attempt cannot
take ownership automatically; operator recovery still requires separate process
cessation and provider-confirmed remote-request quiescence. The tests do not turn
these assertions into provider verification or qualify any production bucket.

Validation includes every boundary from Writing through pending clear, partial
multi-table output followed by exact rollback/replay, Committed roll-forward,
mirror failure after authority, zero-row accepted events, preexisting names and
bad batches failing before Writing, corruption/missing-file refusal, and two
actual child-process kills (after first table publication and after Committed).
The resumed files are decoded and compared table by table with the four expected
rows, rather than checking only file counts. Conditional in-memory S3 tests cover
the same transaction boundary plus accepted DELETE response loss/cancellation,
one-attempt counts, safe diagnostics, retained exact ownership and blocked
reacquisition. The control-store regression injects file/directory sync failures
and proves already-absent observations still require a successful recovery sync.

These are controller and storage tests. They do not yet prove main-loop routing,
request-bound completion, maintenance compatibility or a live Firehose restart.
All builds/tests use Arrow/Parquet 60 through the shared whole-process Cargo lock.
The final current-branch library suite passed **539 tests, five intentionally
ignored**; the new ignored child helper is invoked by the passing process-death
test. `cargo fmt --all -- --check` and `git diff --check` passed. The library
command was `cargo test -p firehose-parquet --lib --locked -j4`.

## Runtime storage binding foundation

The private binding resolver now derives service identity from the same effective
region and HTTPS endpoint used by production S3 builders. Recognized AWS/Tigris
bucket hosts normalize to their matching service host, including dotted bucket
names; credentials are excluded. Global/regional endpoints, custom aliases,
service paths and nondefault ports stay distinct unless their normalized address
is identical. Endpoint credentials, queries, fragments and ambiguous/encoded
paths are rejected without echoing them. Operators cannot silently migrate a
protected stream by changing its storage endpoint.

Local output identities canonicalize existing ancestors without creating missing
roots. Cursor bindings preserve lexical aliases for the mirror's dual ancestry
sync. Explicit remote cursors keep their independent bucket; disabled mirrors do
not disable authoritative state. Five focused binding tests passed after merging
strict v2 index main d5e1419 and the reviewed mirror adapter 7e03c09.

## Initialization eligibility and owned mirror assembly

New authority can be initialized only after an ownership-protected inspection of
an empty local directory tree or remote prefix, plus proof that the configured
mirror is absent. The sole data artifact permitted beforehand is root-level
`partitions.parquet`, decoded through the strict v2 reader and bound to the same
chain. Its granularity can differ and its last span may be incomplete because
initialization does not consume its bounds. Legacy random-name parts, any cursor,
unrelated files, orphan control directories and nested controls are refused.
The tree inspection is bounded at 100,000 entries; remote listing has a 60-second
limit. At bucket root, the exact common owner key and its reserved probe prefix
are outside the dataset inventory.

The controller can own the already-reviewed mirror adapter, avoiding a
self-referential session while retaining borrowed ownership. Local identity
resolution is idempotent before and after missing directories are created.
The combined private ingestion suite passed 60 tests with one intentionally
ignored subprocess child fixture (invoked by its parent crash test). Runtime
selection remains unavailable until caller and maintenance integration finish.

Remote standalone-index eligibility now uses the already-owned object store with
native async GET/stream reads under one 60-second deadline, including on a
current-thread Tokio runtime. The strict v2 decoder is shared with the existing
CLI reader. Initialization accepts at most 64 MiB compressed, 512 MiB of declared
row-group bytes, and 1,000,000 declared/observed rows; larger indexes require a
new empty destination instead of implicit adoption. A current-thread in-memory
S3 regression verifies valid eligibility, malformed rejection and bounded slow
GET behavior. No production S3 backend qualification is inferred from this test.
