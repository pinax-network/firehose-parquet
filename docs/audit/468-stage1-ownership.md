# #468 stage 1: ownership and durable control records

Stage 1 is implemented and independently reviewed, with final current-main
qualification recorded below. It includes local/S3 ownership, durable control
records, mutating-command coverage and explicit owner status/release. The earlier
foundation sections retain their historical test checkpoints; current command
coverage is described in the later sections.

The accepted full architecture is in
[468-ingestion-transaction-plan.md](468-ingestion-transaction-plan.md).
Stage 1 alone did not establish ingestion crash/replay safety. The subsequent
[runtime integration](468-ingestion-runtime.md) connects the transaction,
frontier, startup recovery and protected-maintenance layers. Historical stage-1
results below should not be read as qualification of the complete runtime.

## Local foundation

`dataset_lock::LocalOwnership` accepts all directory scopes for an operation in
one acquisition. It canonicalizes aliases, reduces nested roots and deduplicates
locks by device/inode. Mutation roots are exclusive; target and alias ancestors
are shared. Conflicts are nonblocking errors. This makes parent/child operations
conflict without preventing disjoint sibling roots from progressing. It also
prevents output plus an external cursor nested under output from locking the
same root twice and conflicting with itself.

Directories are opened as the stable OS lock objects. Guards never unlink lock
files or directories on release. Missing scopes are created while existing
ancestors are held, then resolved/locked/revalidated and synced before success.
Changes to the expected canonical scope graph or inode identities fail. Unsupported
platforms/filesystems fail; there is no best-effort ownership fallback.

`durable_state::LocalStateStore` borrows the held guard, refuses roots outside
its scope, and confines keys to `state.json` and `pending.json` beneath
`.fireparq-ingest`. Records use a versioned JSON envelope with checked revision,
canonical payload SHA-256 and a 4 MiB read/write limit. Each create chooses a
fresh record incarnation UUID; replacements retain it and increment revision.
The incarnation participates in the checksum/version, so removing and recreating
an identical payload cannot satisfy a stale version from the old record. This
metadata identity is separate from deterministic transaction/file identity.
Unknown fields/versions,
invalid checksums, malformed payloads and symlinked controls fail. Payload errors
never include the opaque cursor. The document type deliberately has no Debug.

New directories/files use private modes 0700/0600. File contents are completed
and synced before atomic create or replacement; directory sync is fatal. Creation
does not overwrite an existing name. Replacement/removal checks the expected
version while ownership is held. A mutex owned by the guard serializes validation
plus mutation across all stores and threads borrowing that guard; the OS lock
alone would not exclude two borrowers of the same guard. Syncing the control directory parent also occurs
on retry, because an earlier failed attempt may have created its link without
making that link durable. Normal errors clean temporary names. A complete new
record is retained on a post-publication directory-sync error, so the controller
must reload/recover instead of assuming the previous version remains current.

The shared artifact filter reserves ingestion controls, the bucket ownership
record and conditional-probe directory. It began as an exclusion primitive;
subsequent stage-1 command wiring, described below, adds common ownership at
mutating entry points.

## Historical local-foundation checkpoint

The initial macOS Rust subprocess tests passed parent/child/equal conflicts,
sibling progress, nested and symlink scopes, partial acquisition unwind and
owner process death. The implementation must also pass Ubuntu CI; a local
`flock` feasibility experiment alone is not a platform qualification.

State tests exercise create collision, stale replace/remove, strict decoding,
private permissions, failures before/after publication, normal temporary cleanup,
out-of-scope access and control-directory symlinks. Additional regressions cover
alias-parent ancestry, repeated control-parent sync failure, exactly one winner
among concurrent same-version replacements, and remove/recreate ABA rejection.

The local-foundation library run on 2026-09-25 passed **445 tests**, with four
intentionally ignored tests (including child entry points invoked by their
parent tests). Formatting and diff whitespace checks passed. This run used the
Arrow/Parquet 60 shared target with the whole-process Cargo lock; it is not yet
the final stage-1 current-main/integration qualification.

At that earlier local-foundation checkpoint, S3 ownership/storage, aggregate
mutator guards, status/explicit recovery CLI and full stage-1 integration were
outstanding. Those components are now implemented and qualified below.
No production bucket writes have been performed. In particular, stopping an S3
writer does not prove its previously sent requests cannot arrive later. Any
operator recovery must establish both writer cessation and provider-level request
quiescence; neither a timestamp nor a process-stop assertion is sufficient.

## Remote control slots and combined scopes

`S3StateStore` borrows the persistent bucket owner and its shared async mutation
mutex. Each fixed control key uses conditional create/CAS with usable object
version evidence. Logical removal publishes a checked tombstone; recreation CASes
that tombstone to a fresh incarnation. No fixed control key is deleted. Bounded
strict reads and exact content/version reconciliation suppress opaque payloads
from diagnostics. An unresolved or cancelled mutation permanently marks the
owner uncertain, so normal release and subsequent writes fail closed.

`DatasetOwnership` collects every output/source/cursor/artifact scope before one
acquisition, reducing local roots together and acquiring each remote bucket only
once in stable order. File scopes protect their containing directory. Missing
mutation inputs fail without creating directories. Direct control targets are
refused. Partial acquisitions release only earlier resolved owners that have not
performed data writes; uncertainty retains ownership. Local-only synchronous
callers work inside a current-thread runtime, while remote synchronous calls
return an error there rather than panicking.

The combined library check on 2026-09-25 passed **476 tests**, with four intended
ignores. This includes eleven durable state tests, remote tombstone/recreation,
concurrent same-version exclusion and cancellation retaining the owner. Mutator
wiring was being implemented during this check; complete command coverage and
CLI qualification remain required before the stage-1 review boundary.

### Deferred command scopes and aliases

Ordinary command guards use `LocalOwnership::acquire_without_creation`. For a
missing output, the nearest existing ancestor is held exclusively; all upgrades
are calculated before any lock is acquired. This conservatively serializes
siblings until that operation ends. It does not create an empty output during
an invalid endpoint probe or failed validation. The normal writer creates its
output only when it has valid data. The original durable-state acquisition API
still durably creates its requested roots.

The guard retains intended canonical and lexical roots and can revalidate their
inode coverage before publication. Newly created directories remain covered by
an exclusive ancestor. Retargeted aliases fail. Cooperating commands cannot
replace held ancestry; older binaries or external tools that ignore ownership
remain outside the guarantee.

A command also walks directories beneath its reduced local roots after locking
and rejects nested symlink entries before mutation. Explicit command-root aliases
and cursor-parent aliases are canonicalized/locked normally, but aliases nested
inside a traversed mutation tree are unsupported. This preflight reads directory
entries, not file contents, and costs a recursive directory traversal. It prevents
maintenance traversal or table routing from escaping the guarded root through a
nested alias. Commands with very broad roots should choose narrower scopes.

The integration run after these changes passed **822 tests**, with five intended
ignores across workspace suites. Existing subprocess endpoint/probe tests prove
that failed startup and invalid partition probes create no output and preserve
an existing index. Local tests prove missing roots remain absent, newly created
roots stay excluded, nested aliases fail, explicit root aliases work, and an
externally retargeted alias is detected before publication. Remote test support
includes a crate-private injected-owner constructor for hermetic command tests;
production acquisition still validates configured routing and conditional support.

## Command ownership and maintenance compatibility

Real merge, truncate and rollup acquire common ownership before listing or
changing their selected data. Truncate dry runs and unconfirmed plans, merge dry
runs, and verify protocol-only runs with no artifact output remain read-only.
Rollup protects both source and output, including copy-only runs. Verify protects
its source plus every default/explicit registry and report destination in one
acquisition; details and failure tests are in
[468-verify-ownership.md](468-verify-ownership.md).

Every S3 bucket in a mutating command is held bucket-wide, even when the command
selects a narrow prefix. This also means a public source bucket used by a
copy/verification operation that publishes results must grant ownership-control
writes. Unrelated prefixes in one bucket serialize. Different buckets can be
held together; explicit cursor buckets are independent of output buckets and
must all be acquired before ingestion reads the cursor. Ownership/control keys
are reserved and ordinary mutation targets cannot name them. Truncate's recursive
cleanup also preserves control directories.

S3 merge now uses the common persistent owner, with its UUID recorded as the run
identity. Its old local file lock is retained solely for recognizing/recovering
local pre-upgrade journals under the common directory guard. The former remote
TTL/takeover lock implementation has been removed. Automatic recovery refuses a
remote legacy journal whose recorded lock is not the persistent owner key: the
old timestamp cannot establish prior request quiescence. Preserve such journals
and data for a separately reviewed provider-confirmed migration; this stage adds
no bypass flag.

Remote merge data PUT/DELETE operations make exactly one application attempt,
even if a future caller accidentally supplies a larger retry bound. All mutating
S3 clients also disable transport retries. Any operation error retains `Owned`;
normal success alone releases the exact current owner. Conditional journal
creation has no check-then-overwrite fallback. Read retries remain bounded. The
single-attempt transport/cursor tests and their qualification limits are in
[468-s3-mutation-attempts.md](468-s3-mutation-attempts.md).

These guards do not make existing random-name output crash/replay safe. The old
merge journal and its rollback/roll-forward behavior remain separate from the
future all-table ingestion journal. In particular, a remote mutation can have
completed after its acknowledgement was lost; neither an ordinary retry nor a
process-stop assertion proves that a delayed request cannot reappear after
recovery. No production S3 qualification or bucket writes were performed.

## Status and explicit remote release

`fireparq recovery status <existing-local-root-or-s3-uri>` reports ownership plus
state/pending incarnation and revision summaries without exposing record payloads,
opaque object versions, cursor strings or evidence references. Remote status is
GET-only and never creates a canary or owner. Local status requires an existing
root and an available OS guard; a busy/unsupported scope is an error and no
control record is changed or created.

An abandoned remote owner has no expiry. First inspect the exact owner UUID and
generation with status. Establish that the prior writer cannot send more requests
and obtain provider confirmation that all previously sent PUT/DELETE requests
have completed or are permanently prevented from completing. A process exit,
revoked *future* access alone, empty listing, elapsed time, or a request timeout
is not proof that an earlier accepted request has drained. If that provider
cannot supply conclusive quiescence, generic plain-glob recovery remains blocked;
do not use an invented evidence reference as an override.

Only with that evidence, an operator can run:

```text
fireparq recovery release s3://bucket/dataset \
  --expected-owner <UUID-from-status> \
  --expected-generation <generation-from-status> \
  --stopped-writer-evidence <non-secret-process-evidence-reference> \
  --provider-quiescence-evidence <non-secret-provider-confirmation-reference>
```

These are explicit operator assertions, not automatic verification of provider
claims. Both references are bounded and hashed before persistence. The exact
identity/generation is checked again and release is conditional; it cannot release
a newer owner. This changes only the owner record to `Released`, retaining its
key and generation. It neither rolls back data nor restores a cursor, and does
not make existing random-name ingestion output safe to replay. Local owner
release is refused: stop its process and let the OS release the handles.

Remote status needs `GetObject` on the owner and selected state/pending keys.
Mutating commands additionally need `GetObject`/`PutObject` on the fixed root
`.fireparq-owner-v1.json`, and `GetObject`/`PutObject`/`DeleteObject` under the
random `.fireparq-owner-probes-v1/` prefix, plus their ordinary data permissions.
Object version/ETag evidence and correct conditional Create/Update semantics are
mandatory. Listing operations need the corresponding listing permission. Keys
are never silently adopted from an incompatible version. See
[468-s3-ownership.md](468-s3-ownership.md) for the exact probe/record protocol and
real local HTTP adapter tests; no production provider has been live-qualified.

## Current boundary

Build acquires resolved output and cursor scopes before loading the cursor;
partitions-build acquires the chain output before reading its index or sibling
cursor. Their write/checkpoint boundaries revalidate local paths, and a successful
run explicitly releases remote ownership. Error/cancellation drops retain remote
ownership. Index writes use the shared exact-bucket, zero-retry S3 constructor.
Real subprocess tests prove build, partitions-build, merge, truncate and rollup
conflict with a held descendant owner before reading private cursor bytes or
publishing output; an external cursor conflict leaves fresh output absent.

Protected ingestion is still inaccessible. State/control primitives and common
ownership are prerequisites only. Deterministic owned filenames, complete
all-table journals, accepted-event frontiers (including empty-output events and
routing anchors), output checkpoint authority, startup rollback/roll-forward and
legacy migration rules remain the next stages. Issue #468 remains open.

### Final stage-1 current-main qualification

At `3c3852674d430bd9b0590b19bfa5253b2d2a76cc`, integrated with main
`79793c3f2e66288430d50c00af89e6f6da230124`, the complete workspace suite passed
**839 tests**, with five intentional ignores across its suites. Workspace build,
formatting and diff checks passed. The combined run includes the new command
ownership subprocess test, existing invalid-probe/startup/shutdown tests, resumed
metrics/readiness integration, 49 verify tests, local lock/process-death tests,
S3 conditional HTTP fixtures and actual S3 cursor/PUT/DELETE timeout/ack-loss tests.
The build retains the pre-existing final-tail `transactions_processed` unused
assignment warning; it does not affect the tested behavior.

This is macOS plus hermetic HTTP/in-memory validation. Linux CI and complete
transaction runtime qualification remain gates for the later end-to-end release.
No real S3 bucket was modified, and there is no broad remote crash/replay guarantee
from these fixtures. Root independently reviewed ingestion/recovery caller wiring
and scope collection at this boundary without a blocker; further stages remain
in progress on the same isolated branch.

### Publication qualification against current main

The isolated publication branch integrated main `21de6af` (including #499, #511
and #508) as `d33c5ae`. The fresh workspace run passed **848 tests**, with five
intended ignores. The separate capture-authentication example passed one test
(with its child fixture ignored); workspace build, formatting and shell-completion
generation also passed. The only subsequent source edit aligns the S3 module's
historical header comment with its now-enabled common ownership integration.
Linux CI is checked on the final PR head before merge. The existing qualification
limits and the remaining full transaction/runtime work above still apply.
