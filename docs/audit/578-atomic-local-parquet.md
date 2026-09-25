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

1. Create the destination directory, resolve its canonical target, and sync both
   the target ancestry and lexical/alias ancestry from leaf to root, deduplicating
   identical paths. This preserves symlinked output directories without missing
   the target's parent links. Syncing existing ancestors too matters after a prior attempt
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
- A Unix symlinked output root with different target/alias ancestry syncs both
  chains. An injected failure at a target-only ancestor is fatal before any
  temporary or final file is created; the subsequent successful attempt syncs
  both chains again.
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

Arrow/Parquet 60 integration baseline: `8ff1939cd6646b24f85eacdc20f1e0898cd508cd`,
including main
`2793421272d8dff64c43d4b69bc4bf68a8a6cab9` (Arrow/Parquet 60).

After the review correction to sync canonical symlink targets as well as aliases:

- Focused local-publication tests: **10 passed**, including the new symlink case.
- `cargo test --workspace --locked -j4`: **733 passed, 0 failed, 4 ignored**
  (110 + 191 + 1 + 425 + 3 + 3), including Parquet 58 compatibility fixtures and
  all documentation tests. One of the ignored tests is the subprocess helper
  that the active crash test invokes twice.
- `cargo build --workspace --locked -j4`: passed.
- `cargo fmt --all --check`: passed.
- `git diff --check`: passed.

The existing unused `transactions_processed` assignment warning in the binary
remains. No warning was introduced in the local publication code.

Independent review of the publication protocol and symlink follow-up found no
remaining implementation blockers before publication. The tests were repeated
after that correction.

## Final Beacon integration and bounded Ethereum runtime check

Integrated main `c88abcca1199cb8a919d504b751dd7727c0bf047` (merged Beacon #560)
into this branch as `17b93f37f62a9a70e606f2bf37c1097611791e13`. The audit index
conflict was resolved by retaining both sets of records; production code merged
without conflicts. The following checks ran against that integrated source:

- `cargo test --workspace --locked -j4`: **740 passed, 0 failed, 4 ignored**
  (117 + 191 + 1 + 425 + 3 + 3), including the full Beacon schema matrix,
  Arrow/Parquet compatibility fixtures, publication faults and subprocess crashes.
- `cargo build --workspace --locked -j4` and `cargo fmt --all --check`: passed.
- The runtime binary was copied while holding the whole-process Cargo lock.

A fresh local ingestion on 2026-09-25 requested only Ethereum blocks
`[26049575,26049577)`, explicitly using `https://eth.firehose.pinax.network:443`
and the intended Pinax key through `PINAX_API_KEY`. The subprocess environment
contained only that key and `PATH`; the bearer-token selector named an absent
variable. No StreamingFast credential or S3 destination was supplied.

The run used `--partition none --compression zstd --flush-blocks 1
--final-blocks-only`, preserving the reference sample's extended EVM output,
hex encoding and failed-transaction inclusion. Both requested blocks were
processed, the run exited successfully in 2.97 seconds, and it produced 26 data
parts plus the cursor. No retry/probe events or `.fireparq-*.tmp` remnants were
observed. This exercises the actual local publication path with the normal
binary, without test-only fault wrappers.

The output at `/tmp/fireparq-578-live-20260925/mainnet` was compared with the
existing reference `/tmp/fireparq-469-live-20260925/mainnet`. For every table,
DuckDB `DESCRIBE` and the complete `parquet_schema()` result (excluding filenames)
match. `EXCEPT ALL` over all columns in **both directions** reports zero rows,
checking values and duplicate multiplicities independently of part filenames
or ordering. The exact observed rows were:

| Table | Reference rows | Atomic-publication rows | Difference either direction |
|---|---:|---:|---:|
| access_lists | 72 | 72 | 0 |
| balance_changes | 1,970 | 1,970 | 0 |
| blocks | 2 | 2 | 0 |
| calls | 4,261 | 4,261 | 0 |
| code_changes | 3 | 3 | 0 |
| logs | 1,438 | 1,438 | 0 |
| nonce_changes | 465 | 465 | 0 |
| set_code_authorizations | 2 | 2 | 0 |
| storage_changes | 3,545 | 3,545 | 0 |
| system_balance_changes | 32 | 32 | 0 |
| system_calls | 8 | 8 | 0 |
| system_storage_changes | 10 | 10 | 0 |
| transactions | 458 | 458 | 0 |
| withdrawals | 32 | 32 | 0 |
| **Total: 14 tables** | **12,298** | **12,298** | **0** |

Both `blocks` tables have exactly two rows, from 26049575 through 26049576.
Cursor rows also match after excluding the private `cursor` and expected
wall-clock `updated_at` fields: last block number/ID, timestamp, and requested
start/stop range agree. No private cursor values were printed or committed.
Local reproduction uses `/tmp/fireparq-578-compare.py`; the sanitized result is
`/tmp/fireparq-578-live-comparison.json`. Generated data and runtime logs remain
outside the repository.

This live success confirms equivalent output through the new publication path;
it does not extend the single-file guarantee to a multi-table/cursor transaction.
Issue #468 remains open. PR #580's GitHub closing references were checked and
contain only the narrow prerequisite issue #578.

## Current-main integration

The final integration includes merged writer simplification (#477, PR #581) and
responsive shutdown (#473, PR #579), based on main `fdb1f98`. The only source
conflict retained the writer's `Context` and `Array` imports needed by its new
partition validation. At integrated implementation `d440779`, **751 workspace
tests passed**, zero failed, four ignored (three benchmark helpers and the
child-process fault helper exercised by its parent tests). Build, formatting and
whitespace checks passed. README now describes the second-signal boundary:
hidden temporary fragments may remain; already published local table files
have complete footers. Cross-table replay remains outstanding in #468.
