# Atomic local Parquet publication (#578)

## Scope and diagnosis

Issue #578 is one prerequisite of #468. `ParquetTableWriter::write_batch` used
`File::create` at the final `.parquet` path before writing rows and the footer.
Readers could open an incomplete file; interruption left that final name behind.
The writer also reported success without syncing the file or directory, and a
rare final-name collision truncated the existing file.

This change affects only the local low-level part writer and directory setup.
It does not change `OutputWriter` buffering, random process-prefix/counter
filenames, the Parquet schema/encoding/compression/footer metadata, or S3 writes.
It adds no dependency. Existing file-creation mode and umask behavior are retained.

## Publication protocol

The private `writer/local.rs` helper performs these operations:

1. Create the destination directory and sync its full directory ancestry from
   leaf to root. Syncing existing ancestors too matters after a prior attempt
   created a directory but failed to sync its parent: existence alone is not
   proof that the directory link is durable. Empty-batch directory creation
   uses the same helper.
2. Exclusively create `.fireparq-<full-uuid>.tmp` in that destination directory.
   The name is hidden and its extension is `.tmp`, so Parquet collectors do not
   treat a process-crash remnant as table data.
3. Write the Arrow batch, close the Parquet writer including its footer, retain
   the underlying file handle, and call `sync_all` on that handle. A close or
   flush error is propagated; closing alone is not considered durable.
4. Create the final directory entry with `std::fs::hard_link(temp, final)`. It
   atomically names the completed inode and fails if the final destination
   already exists. There is no check-then-rename overwrite race, and the temp is
   on the same filesystem as the destination.
5. Remove the temporary name and sync the containing directory. Report the final
   path and file size only after all those operations succeed.

On an ordinary error the temporary name is removed. Cleanup errors are attached
to the original failure; a guard also attempts cleanup during panic unwinding.
A terminated process cannot run that guard, so hidden temporary remnants can
remain. This step deliberately does not scan/delete unknown temporary files:
without an ownership/recovery protocol they might belong to a live writer.

## Failure boundary

Before final-name publication, a failure leaves no new final `.parquet` file.
After publication, removal or directory-sync failure returns an error and
**retains the complete final file**. The result is an ambiguous durability state,
not rollback. Removing that complete file to disguise the ambiguity would risk
losing data that already became visible or durable.

Current ingestion callers stop on write errors and do not advance the cursor.
Retaining a failed batch in memory does not make retrying it under a new random
filename duplicate-free. #477's buffering work must preserve this distinction.
A process crash after publishing some tables but before the cursor still permits
replay duplicates. #468 remains open for the journal, deterministic transaction
identity, ownership, recovery, and coordinated table/cursor commit protocol.
Individual-file atomic visibility also does not provide a consistent snapshot
across tables to concurrent glob readers.

## Filesystem compatibility and cost

The filesystem must implement atomic same-filesystem hard-link creation without
replacement, file sync, and directory sync. Directory ancestors must be readable
for opening their handles. Unsupported operations or any sync error fail closed;
there is no non-atomic copy/overwrite or warning-only durability fallback. This
can reject filesystems/platforms or search-only directory permissions that the
old direct-write path accepted. S3 behavior is outside this change.

The sync operations intentionally add I/O per part, including ancestors that
already exist. A future cache would need a sound way to distinguish directories
whose links are durable from directories created by failed/concurrent attempts;
merely caching `exists()` would weaken the guarantee. Guarantees depend on the
filesystem honoring these operations. Tests exercise process interruption and
injected I/O errors, not physical power loss or every network filesystem.

## Validation

Focused tests in `writer/local.rs` cover:

- Successful output round-trip and byte size; the final name is absent during
  every underlying write; directory ancestry and persistence ordering are checked.
- An actual I/O error after 128 bytes during a large batch write, and another
  after all but the final four footer bytes. Both leave no final or temporary file.
- Injected ancestor-sync, file-sync, and publication failures before the final
  link; cleanup leaves no final or temporary file.
- Injected temporary-unlink and final-directory-sync failures after publication;
  the complete final file remains readable, cleanup removes the temporary name,
  and the caller receives an error.
- A retry after directory creation/sync failure syncs all already-existing
  ancestors rather than assuming they are durable.
- Existing destination contents remain byte-for-byte unchanged; simultaneous
  writers racing for the same final path produce exactly one winner.
- Child-process termination before footer close bypasses destructors and leaves
  a nonempty, unreadable Parquet fragment only under a hidden `.tmp` name.
  Termination after publication leaves a complete readable final file.

Initial base: `ad17332` (before the Arrow/Parquet 60 update). Validation uses
`CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`, a whole-process lock around
the shared Cargo target, `--locked`, and four build jobs.

- Focused local-publication tests: 9 passed; one helper ignored in the ordinary
  harness but invoked by the subprocess test at both crash boundaries.
- `cargo test --workspace --locked -j4`: 729 passed, 0 failed, 4 ignored
  (110 + 191 + 1 + 424 + 3), with all doc tests passing.
- `git diff --check`: passed.

Final current-main integration checks are recorded below before publication.
