# #468 stage 1: ownership and durable control records

Implementation in progress. The accepted architecture is in
[468-ingestion-transaction-plan.md](468-ingestion-transaction-plan.md).
These primitives do not yet establish ingestion crash/replay safety. Protected
ingestion remains inaccessible until the transaction, frontier, recovery and
maintenance integration stages are complete.

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
record and conditional-probe directory. This is an exclusion primitive, not yet
proof that every mutation entry point participates in ownership.

## Verification and remaining work

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

At this local-foundation checkpoint, S3 ownership/storage, aggregate mutator
guards, status/explicit recovery CLI and full stage-1 integration are outstanding.
No production bucket writes have been performed. In particular, stopping an S3
writer does not prove its previously sent requests cannot arrive later. Any
operator recovery must establish both writer cessation and provider-level request
quiescence; neither a timestamp nor a process-stop assertion is sufficient.
