# Issue 468: all-table controller and physical recovery

This private controller follows the reviewed model and prepared-writer
foundations. The command does not select it yet. Eligible-root initialization,
the real protected cursor mirror, received-envelope/routing integration, bounded
completion and protected maintenance behavior remain required before enabling
the complete path or declaring issue 468 satisfied.

`ingest/controller.rs` owns a flush's entire batch map until all-table commit. It
builds the complete sorted inventory from the authoritative descriptor, checks
every batch/schema/partition and planned destination before writing, and refuses
any preexisting planned final or temporary name. Only then does it persist
Writing. Each table is encoded separately, staged locally when applicable,
journaled with its immutable full-file receipt, and published. All final files
must verify before Committed; then authority advances, the mirror reconciles,
temporary names are removed and pending is durably cleared. No independent table
acknowledgement or mirror-leading-authority transition exists.

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
