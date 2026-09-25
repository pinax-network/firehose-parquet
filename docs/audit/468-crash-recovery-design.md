# Draft: crash-safe ingestion publication and replay (#468)

Design review, 2026-09-25. Base inspected: `origin/main` at
`f3e99f327d889cc466d5e1ce29cc708d9f73ddd3` (dependency refresh #569).
#467 has since merged in PR #570. Cursor-save retry work in PR #571 is a
dependency; it is not a transaction protocol. This document describes proposed
work and acceptance criteria, not implemented guarantees.

## Recommendation

Implement an ingestion transaction journal covering **all table outputs and the
resume frontier**, with deterministic transaction filenames, atomic local file
publication, and recovery **before** connecting a stream. Reuse the existing
merge journal's useful distinction: incomplete `writing` work is rolled back;
fully durable `committed` work is rolled forward. Do not reuse its partition-local
scope or time-based S3 lock takeover as a sufficient ingestion guarantee.

First/last-block filenames alone are insufficient. Example: cursor is at 100;
a timer flush publishes transaction rows for 101–110, then dies before other
tables/cursor. Replay flushes at 106 and 116. Files 101–110, 101–106 and 107–116
are different names and overlap. Hashing file contents instead of ranges has the
same problem. The missing mechanism is deciding whether the entire old flush
must be completed or removed before a new grouping is permitted.

The proposed contract is **no duplicate/lost event output after successful
recovery of an interrupted ingestion transaction**, given a supported durable
store and exclusive ownership. It is not cross-table snapshot isolation for
arbitrary Parquet globs, global exactly-once processing across independent
producers, or repair of pre-existing duplicate files.

## Evidence from current code

- `writer.rs::ParquetTableWriter::write_batch` creates random process/counter
  names and writes a local file at its final name. An ArrowWriter close does
  not fsync file contents or the directory.
- `OutputWriter::flush_remaining` iterates a HashMap of tables; successful
  tables are removed from memory before later table writes can fail. It also
  re-buffers `pending_after_flush` afterward. Its boolean reports that some
  data was written, not which input prefix is completely durable.
- `blocks/src/bin/main.rs::write_mapper_flush` forces materialization at current
  call sites, but can report success even if pending buffers remain. Partition
  boundaries, row/block/byte thresholds, elapsed time, and bounded EOF can all
  choose a flush boundary. Graceful shutdown and failures discard partial
  buffers. Current behavior intentionally avoids writing an arbitrary shutdown
  tail, but a previously attempted multi-table flush can already have published
  some files.
- Three ingestion sites save cursors separately after writer success. #469's
  bounded cursor-save failure handling is necessary but cannot remove already
  published duplicate replay data on its own.
- The inspected `cursor.rs::save_cursor_parquet` writes a synced temporary file
  and renames it, but treats directory fsync failure as a warning. PR #571 makes
  that failure fatal. New parent directories still require durable creation
  ordering for the transaction protocol.
- `merge_journal.rs` journals local/S3 merges, provides fault injection and
  directory syncing, and uses local OS locks. Its current S3 takeover judges
  liveness from elapsed time; a paused old writer can resume after takeover.
- Firehose's checked-in `proto/firehose.proto` specifies that a resume cursor
  starts immediately after its referenced block, taking precedence over start
  height. Preserve opaque cursors; never replace them with `last_height + 1`.

## Required durable objects and identities

Add reserved internal state beneath the canonical dataset root, for example
`.fireparq-ingest/`. This must be excluded by the shared artifact filter and all
maintenance/read collectors; no temporary object should end in `.parquet`.

Each logical stream has:

1. A versioned **stream descriptor**: canonical chain identity, validated output
   root and cursor destination, partition policy, mapping/schema epoch, output
   encoding/filter/fork options, starting boundary, and declared ownership scope.
   Exclude secrets, wall-clock timestamps and random process IDs from identity.
2. An authoritative **output-root checkpoint** with the full cursor state,
   transaction ID, previous checkpoint ID, and monotonically increasing delivery
   ordinal. The user-facing `cursor.parquet` is a compatible mirror. Keeping
   authority beside output prevents deleted/moved/external cursors or
   `--cursor none` from silently converting a resume into an overlapping ingest.
3. At most one durable **pending transaction journal** for the stream. Store
   journal version, stream identity/fingerprint, predecessor checkpoint ID,
   exact target checkpoint/cursor state, ordered event-window identity, complete
   output plan (including tables with zero rows), phase, and checksums for
   completed outputs. Validate all paths as relative to the owned output root.

Assign a delivery ordinal to input envelopes and carry it with buffered blocks.
The identity describes ordered accepted stream events, including raw cursor,
block number AND ID, and fork step. Do not identify a stream window solely by
height, timestamp, max block number, or `(block_id, step)`; a legitimate repeated
NEW after UNDO must remain distinguishable by delivery position/context. Rely on
the documented Firehose resume contract, not an assumed globally unique cursor
string. Detect incompatible replay/source responses and stop rather than merge
unproven event identities.

A transaction ID can be a full SHA-256 of a versioned canonical serialization of
stream identity, predecessor checkpoint ID and ordered event-window digest.
Sort table/partition output plans, so HashMap iteration never changes IDs.
Names remain ordinary table-partition files, for example:
`part-v2-<stream-hash>-<start-ordinal>-<end-ordinal>-<transaction-hash>.parquet`.
No process randomness, wall-clock time or process-local part counter is used.
Add the transaction identity and schema fingerprint to file metadata. If a
future transaction emits multiple files for a table, assign deterministic plan
ordinals and record each one; do not number by completion order.

This guarantees identical names for the same logical transaction/window and
allows an already committed rerun to be a no-op. It does **not** promise identical
physical layouts for two independent clean runs with different flush policies;
that would require fixed event chunks or one file per event. Layout independence
is not needed to prevent crash replay overlap: recovery resolves old work first.

## Transaction and recovery protocol

| Phase | Durable invariant | Recovery |
|---|---|---|
| None / buffering | Checkpoint H remains authoritative; no unjournaled final output may exist | Discard volatile buffers and resume H |
| `writing` | Complete ownership/output plan and target cursor are durable before first final publication; checkpoint still H | Remove ONLY this transaction's owned parts/temp objects; durably remove journal; resume H |
| `committed` | Every planned nonempty output is complete, verified and durable; immutable target cursor is recorded | Verify outputs; advance/repair checkpoint and cursor mirror to target; never replay that event window |
| Checkpoint advanced, journal retained | H' names the committed transaction; external mirror/cleanup may be incomplete | Reconcile mirror/cleanup idempotently; no new stream work yet |
| Acknowledged | H' durable, mirror acknowledged where configured, journal removal durable | Next transaction may begin |

Detailed order:

1. Acquire the correct ownership lock; validate descriptor, checkpoint and cursor
   relationship; recover any pending journal **before** mapper creation, data
   publication, a new stream request, or considering new flush thresholds.
2. Build a frozen `FlushTransaction` from a complete processed-event prefix and
   all mapped table batches. Record schemas, row counts and every planned path.
   Never call the old independently publishing `write_all`/`flush_remaining` path
   while constructing this object. Journal `writing` atomically and durably.
3. For each planned table output, write a same-filesystem hidden temp file, close
   the Parquet writer, fsync the file, then atomically publish its final name and
   fsync the containing directory. Ensure newly created ancestor directories are
   durably linked too. Obtain the underlying file safely after ArrowWriter close
   or retain a handle; close success alone is insufficient. Treat **every** file
   or directory sync failure as failure. Remove temp files on ordinary errors;
   crash recovery also owns exact temp names.
4. Never overwrite an unrelated existing part. Use create-only/no-replace
   publication or, under the exclusive lock, reject pre-existing final names
   unless the journal plus checksum prove this is the same operation being
   retried. A collision or mismatched footer/digest fails closed. S3 writes use
   conditional creation; after a lost response, inspect the exact object and
   verify its identity/digest instead of guessing whether PUT succeeded.
5. Once all outputs are durable, atomically persist `committed` with exact sizes
   and checksums. Only then advance the authoritative checkpoint, sync it and
   its directory, save the external cursor mirror with #469's bounded failure
   policy, and acknowledge the flush. Preserve the committed journal on any
   checkpoint/mirror failure. No later transaction may overtake this one.
6. Durably remove the completed journal before allowing another transaction.
   A crash after checkpoint but before cleanup is harmless: recovery recognizes
   the target checkpoint and finishes cleanup, not deletion/re-ingestion.

For `writing` recovery, compare the stored checkpoint to the recorded predecessor
before deleting anything. Unexpected advanced state, unknown journal version,
changed stream/config identity, missing/corrupt committed output, arbitrary
outside-root paths, or a mismatched existing object must stop for diagnosis.
Never infer ownership by a broad `part-*` glob or numeric range. Keep a journal
until rollback deletions and directory syncs succeed; an interrupted recovery
must safely repeat. A committed transaction whose data is missing cannot be
rolled back just because the source can be read again.

### Why this handles changed timer boundaries

Before durable `committed`, the checkpoint is H. Recovery removes every old
transaction output before reconnecting at H; replay may choose new boundaries
without overlapping old parts. After durable `committed`, recovery restores the
recorded target cursor H' without remapping the old window; replay begins after
it regardless of new timer/size settings. The durable phase, not the filename,
decides which branch is correct. Test this invariant at each persistence edge.

## Buffering, partitions and reversible events

Replace the materialized boolean with `CommittedFrontier { checkpoint_id,
last_delivery_ordinal, cursor }`. Only this value authorizes cursor advancement.
Keep all table batches in transaction ownership until acknowledgment; retiring
individual table buffers earlier is a source of ambiguous state on failure.

A flush must cover a contiguous stream prefix whose required output and resume
context are fully represented. A zero-row table belongs in the plan as zero;
an all-zero-output event prefix can still need a checkpoint-only transaction.
Do not equate “some rows written” with “all accepted events persisted.”

`pending_after_flush` must be removed from this publication path or become an
explicit later transaction with a separate frontier. The current split machinery
(#477) assumes timestamp representation/order and cannot establish the required
invariant. Assert one destination partition per batch at the ingestion boundary;
if retained support needs splitting, freeze a complete multi-partition plan in
one transaction. Rebuffered newer rows must never be covered by the prior cursor.

Persist the timestamp routing anchor that belongs to the committed prefix, not
an anchor observed from a later buffered block. Bootstrap/Solana delayed mapping
must carry input ordinals and must not advance over an unresolved earlier event.
Graceful shutdown may preserve its existing discard-unprepared-tail policy;
prepared/committed work still needs normal recovery, not best-effort cleanup.

For reversible output, NEW and UNDO are distinct events and must stay appended.
A reorg can move block numbers or partition timestamps backward, so neither
max block number nor finality alone is a commit frontier. Include event order,
block ID and step in the window identity, restore the exact opaque cursor, and
verify NEW(A), UNDO(A), NEW(B), and NEW(A) again survive crashes without collapsing
or duplicating events. This work does not change the public reorg dedup semantics
tracked in #474.

## Ownership, S3, readers and maintenance

There is no safe replay protocol if another writer or maintenance command can
change the same objects without participating. Add a shared dataset ownership
protocol used by ingest, merge, rollup and truncate; today's merge-only lock is
insufficient. The simplest safe first implementation serializes mutation of a
dataset root. **This is a compatibility decision** for parallel backfills.
To preserve concurrency, require declared disjoint final-only block ranges,
registered atomically in a dataset catalog, plus a lock per stream/range and
maintenance locks covering every affected claim. Claim an unbounded tail to
infinity. Reversible streams need a single ordered ownership lane. Do not infer
non-overlap merely from different cursor paths or process IDs.

Local OS locks can prove a dead process no longer owns its write capability.
For S3, conditional objects/CAS protect ownership metadata, but a timeout-based
lease alone cannot prevent a paused prior owner from later publishing files
into glob-visible paths. A pre-PUT lease check also has a time-of-check race.
Choose and document one complete policy before claiming remote safety:

- Preserve ordinary Parquet glob layout and **do not automatically steal a
  stale S3 writer lease**. Recover only when the prior writer is conclusively
  stopped/revoked; explicit operator recovery records that requirement.
- Or move to fenced immutable transaction objects plus an authoritative
  committed-manifest reader contract. Stale writers can create unreferenced
  objects but cannot CAS the manifest head. Every supported reader/maintenance
  path must honor the manifest, and plain external globs are no longer the
  committed dataset view. This is a larger format/API migration.

Do not import the existing merge journal's stale lease takeover and label the
result safe. Test the resumed-old-owner scenario explicitly. S3-compatible stores
also need conditional create/update and consistent reads; fail explicitly when
these prerequisites are absent. Resolve exact data/checkpoint/mirror buckets
(#470) before recording the descriptor. Cross-bucket cursor mirrors cannot be
atomically committed with data; the output checkpoint plus journal is the
recovery authority, and failed mirror persistence stops progress.

An individual final `.parquet` is always complete, but ordinary external globs
can observe a subset of a multi-table transaction while it is publishing or
being rolled back. The current layout cannot offer atomic cross-table snapshots.
Document that analytics requiring a consistent checkpoint run after recovery /
while writers are quiescent, or adopt manifest-aware reads. No “exactly once”
claim for arbitrary concurrent glob readers.

## Migration and contract decisions

- Existing random-name output has no transaction ownership proof. Do not delete
  or overwrite it by guessed height ranges. Require a documented adoption step
  with a trusted checkpoint and verified baseline, or rebuild into a new root.
  Adoption cannot retroactively prove old data was duplicate-free.
- Once protected state exists, a missing/stale external cursor is recoverable
  from the authoritative output checkpoint; a conflicting advanced cursor or
  different dataset fingerprint is an error. Manual rewind / `--cursor-override`
  cannot append overlapping data silently; use coordinated truncate/rebuild or
  a new root. This behavioral change needs explicit release notes.
- Semantic changes (schema/mapping epoch, filters, encoding, partition routing,
  final/reversible mode) cannot silently reuse the same stream descriptor.
  Flush size/timer may change between acknowledged transactions, but recovery
  must use the recorded pending plan and old target state first.
- A successfully completed repeat of the same bounded run resumes its durable
  checkpoint and produces no duplicate objects. Physical layout equivalence
  across entirely new runs with different flush policies is a separate feature.
- Keep a small durable head/descriptor after journal cleanup; bound temporary
  objects and recovery manifests. Never prune recovery evidence for unfinished
  work merely to meet a storage budget.

## Meaningful failure-injection acceptance

Use real child-process aborts for selected local durability boundaries, injected
I/O errors for the full matrix, and a stateful ObjectStore fake for ambiguous
S3 writes/CAS. An in-memory error test alone cannot prove restart behavior.

1. Publish a baseline event stream with multi-row/empty tables, then crash before
   journal creation, after journal sync, mid-temp write, after file fsync, after
   first final rename, after first directory sync, after the last table, after
   committed-marker sync, after checkpoint rename/sync, during cursor-mirror
   save, and before/after journal removal. Restart from disk and compare **all
   table rows as ordered event-owned multisets**, full block IDs/steps, cursor
   state and absence of unowned/overlapping files with the baseline.
2. Repeat each precommit crash with replay flush limits changed (e.g. 10 events
   becomes 6), timer schedules changed, HashMap insertion orders shuffled and
   partition boundaries crossed. Assert the old overlapping files are removed
   before any replay publication. Repeat postcommit crashes and assert no source
   request restarts inside the committed window.
3. Have a successful early table, a failed later table, zero-row tables and
   pending newer-partition batches. Verify no cursor advances across unwritten
   data; recovery never loses the successful table's rows or duplicates them.
4. Inject file/directory fsync failures, ENOSPC, rename failure, checksum mismatch,
   missing committed objects, corrupt/version-unknown journals and ownership
   conflicts. Assert nonzero exit, preserved recovery evidence and no deletion
   of unrelated/legacy files. Observe the final-name directory during writes:
   every visible final Parquet file must parse successfully.
5. Reversible sequence NEW(A), UNDO(A), NEW(B), NEW(A) again with repeated heights;
   crash at every commit boundary. Compare multiplicities/order and exact resume
   cursor. Also test missing timestamp bootstrap/anchor state and skipped blocks.
6. S3: lost PUT response after server acceptance, failure on table N, failure on
   checkpoint CAS, mirror in a separate bucket, crash during rollback delete,
   stale listing/error responses, and an old owner resuming after attempted
   takeover. Safety must hold or the operation must refuse to proceed; no test
   may treat elapsed time as proof of exclusive ownership.
7. Run concurrent ingest/merge/truncate (and two ingesters) against overlapping
   scopes; assert serialization/refusal. If disjoint claims are supported, prove
   they progress and cannot cross each other's ownership boundaries.
8. Repeat a completed bounded run: same files/names and row counts, no new writes.
   Change only timer/flush size: still no extra data. Attempt incompatible schema
   or rewind: fail with a concrete recovery/rebuild instruction.

Delivery can be staged in separate reviewed commits (durable publication helper,
transaction object/journal, ingestion frontier integration, recovery/ownership,
compatibility/docs), but do not close #468 after the first helper or a filename
unit test. The restart matrix and declared S3/concurrency contract are part of
the solution, not optional follow-up polish.
