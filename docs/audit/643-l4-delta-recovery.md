# Delta recovery and ownership (#643, lane L4)

Refs #643; closes #636; part of #463. PR: [#675](https://github.com/pinax-network/firehose-parquet/pull/675). Design:
[`docs/design/delta-lake.md`](../design/delta-lake.md) §3.5, §4, §4.1, §5
and the L4 row of §11. Ownership: [the #636 record](636-delta-ownership.md).
Index: the #643 rows of the [audit index](README.md).

## Diagnosis

L3 ([record](643-l3-delta-commits.md)) commits every transaction to the Delta
tables between Committed and the authority advance, but recovery still
treated a Committed journal as in #468: it verified every part and advanced
authority. A crash or failure in that window therefore lost the transaction
from the tables that had not committed it yet, and the restart re-read parts
that maintenance may have compacted and vacuumed since. L3's record lists the
eight crash rows of design §4 that this left open.

## Decision

- **Roll forward per table, gated by `txn`.** Every start reads each table's
  `txn` for this stream before it changes anything. For a Committed journal
  whose authority has not advanced, the tables whose `txn` is below the
  transaction's last ordinal `L` are committed, `blocks` last, after their
  parts (and only theirs) are verified against the journal; then authority
  advances. Tables at `L` are skipped without reading their parts.
- **No part is read once the transaction is in every log or in authority.**
  The `controller.rs` change of design §4: `verify_all_finals` no longer runs
  for a Committed journal whose authority reached the target, and a table
  whose log holds the transaction is not read either.
- **A log ahead of authority is refused** at every start, for every table:
  a `txn` above the authority's ordinal, or, with a pending transaction, a
  `txn` above the predecessor that is not `L`.
- **An uncommitted part that is gone fails closed** (§4.1), with the journal,
  authority and every log left as evidence.
- **An ambiguous log commit does not set the S3 owner's uncertainty latch**
  (§3.5). The review against #468 is [below](#the-owner-latch-and-log-commits).
- **The owner guards fireparq's writer and state only** (#636): the Delta
  tables are shared through their logs, and `deltalake` maintenance runs beside
  `build`. See [the #636 record](636-delta-ownership.md).

## Implementation

### Startup order (`ingest/session.rs`)

`IngestionSession::open` now opens the Delta tables before the controller's
recovery, because recovery commits to them:

1. read authority (or initialize it at a new root, after eligibility);
2. refuse a request whose descriptor differs from authority's, so no table is
   ever created or opened for another stream;
3. open every table, creating the missing ones only while authority is at
   ordinal 0 (unchanged from L3), and validate them;
4. open the controller with the tables, which runs recovery.

### Recovery (`ingest/controller.rs`, `delta/commit.rs`)

`TransactionController::open_with_delta` (and the session's
`open_reserved`) takes the tables and the flush concurrency:

1. `DeltaTables::check_progress(authority, committed, concurrency)` reads the
   `txn` of every table (a bounded concurrent read that stops at the newest
   commit holding this stream's `txn`, usually the last commit). It refuses a
   log ahead of authority and returns the tables of the Committed journal
   whose logs lack it. On local disk a table that holds it has its log tail
   (the commit files after the last checkpoint and the `_delta_log/`
   directory) synced again: the process that committed it may have died
   between the commit and its `fsync`.
2. `TransactionParts::verify_finals_of` verifies only the parts of those
   tables. A missing part, or one that does not match its receipt, stops
   recovery with the §4.1 message: the ordinals, how long the transaction
   has been pending (from its newest receipt), the tables it is missing from,
   each missing or differing part by table and dataset-relative path, the
   tables that already hold it, and "rebuild into a new, empty output root".
3. `DeltaTables::roll_forward` commits exactly those tables with the same
   commit as a flush (`add` from the journal, `txn {appId, version: L}`,
   blind append), the others first and `blocks` last. A commit that loses to
   a winning commit of this stream (`ConcurrentTransaction`: a delayed copy of
   an earlier commit whose outcome was unknown) reloads the table; when its
   `txn` is `L`, the commit counts as done (`TableCommit::found_in_log`) and
   no second copy is added.
4. Authority advances, the mirror reconciles, temporaries are removed and the
   journal clears, as before.

A roll-forward that is itself interrupted is recovered the same way at the
next start. The journal-only `TransactionController::open` (no Delta tables)
is now test-only; outside the #468 unit tests, a Committed journal without
its tables is refused.

### A log commit with an unknown outcome

`commit_delta` no longer calls `mark_remote_uncertain` (removed from
`ingest/parts.rs`). A failure whose outcome is unknown keeps
`CommitFailure::unresolved` for the log and the error, which now says "a Delta
commit's outcome is unknown (it may have landed); the next start resolves it
from the table's txn". The controller is poisoned and the journal stays
Committed, as for any failed commit.

### Table creation

`DeltaTables::open_with` calls a hook after each table it creates; the session
uses it for the `crash-after-delta-create:<table>` fault. Creation was
already idempotent (L3): the restart validates the tables that exist and
creates the others. A local table directory whose `_delta_log/` holds no
commit (for example only the staging file of an interrupted local write)
counts as missing. Once authority has accepted a transaction, a table without
a log is still refused ("its rows are unreachable ... build into a new, empty
output root").

### `recovery recover` (`ingest/maintenance.rs`)

`recover_roots` opens the root's tables with `DeltaTables::open_existing`
whenever the root has accepted a transaction or holds a Committed journal:
every table of the stream, which must exist, validated like a `build` start
(identity, properties, protocol, partition) except for the schema, which
needs the mapper and is checked by the next `build`. It then runs the same
recovery. On S3 it uses the owner's Delta log client, or builds the same
single-attempt client from the command's AWS settings. A root at ordinal 0
without a Committed journal needs no tables: after a start interrupted while
creating them, the next `build` creates the rest, and `open_existing` says so
if asked.

### Debug faults (debug builds, `FIREPARQ_DEBUG_FAULT`)

- `crash-at:<Stage>` aborts at a transaction boundary, for example
  `crash-at:CommittedPersisted` or `crash-at:AuthorityAdvanced`.
- `delta-commit-lost-response:<table>` lets the table's commit land, then
  fails it as a lost response (unresolved).
- `crash-after-delta-create:<table>` aborts once that table is created.
- `delta-commit:<table>` and `crash-after-delta-commit:<table>` (L3) now also
  fire during a recovery roll-forward.

## The owner latch and log commits

Design §3.5 proposed not to latch the S3 owner on an ambiguous log commit.
This is the review against #468's quiescence reasoning
([s3-owner-safe-release.md](s3-owner-safe-release.md),
[468-s3-ownership.md](468-s3-ownership.md)).

**What the latch is for.** A clear latch lets `finish` release the owner after
a failure because "every request it sent has a known outcome, and none can
arrive later". The next owner's recovery then acts on what it sees. The
hazard is a delayed request taking effect after recovery acted on its
absence: a part PUT that recreates a part Writing rollback deleted, or a
delayed write over a control record or the mirror. Provider quiescence
(`recovery release`) exists for exactly those.

**What a log commit is.** delta-rs's `DefaultLogStore` writes a commit as one
conditional create of `_delta_log/<version>.json` (`PutMode::Create`,
`If-None-Match: *`): no temporary object, no copy, no delete. fireparq's log
client makes one attempt per request (`max_retries: 0`), so one commit is one
request. Its only possible late effect is creating that key.

**Every arrival order ends with one copy.** Suppose the commit of table `T`
for transaction `L` targeted version `N` and its outcome is unknown:

| The delayed PUT | What the next start sees | Result |
|---|---|---|
| landed before the start read `txn` | `txn(T) = L` | `T` is skipped; its part is not read |
| lands after the read, before the roll-forward's commit | the roll-forward's PUT of `N` gets a 412; delta-rs reads the winner, finds this stream's `appId` and fails with `ConcurrentTransaction` | the table is reloaded, `txn(T) = L`, the commit counts as done (`found_in_log`) |
| lands after the roll-forward committed `T` at `M ≥ N` | versions are dense, so `N` exists | 412: no effect |
| never lands | `txn(T) < L` | the roll-forward commits `T` once |

**Nothing after a log commit depends on its absence.** fireparq never deletes
a log object (the writer needs no DeleteObject on `_delta_log/`), a
Committed journal is only rolled forward, never back, and its parts are only
read. The one delete that could meet a very late PUT is the maintenance job's
log cleanup, which removes commits older than seven days that a checkpoint
covers. A PUT delayed past that would recreate a commit file below the latest
checkpoint, which readers and writers do not replay and the next cleanup
removes. The latch would not prevent that either: the job never takes the
fireparq owner.

**Nothing else is outstanding when a log commit fails.** Every part was
verified before Committed, the Committed journal was persisted before the
commits, authority advances only after them, and the started commits are
drained before the error returns.

**Decision.** An ambiguous log commit does not set the latch, so a failed
`build` releases its S3 owner and the next start (for example the pod's
restart) resolves the commit from `txn`, without an operator release. Table
creation already worked this way in L3. Part PUTs, control records, the
mirror and deletions keep the latch and the quiescence rules unchanged. The
latch no longer proves that no request of the process can still take effect;
it proves that none can take effect in a way recovery does not resolve.

## Crash matrix: what each row does now

`L` is the pending transaction's last ordinal. Every test runs the real
binary against a mock Firehose, on local disk and on loopback HTTPS S3
(`blocks/tests/delta_recovery.rs`), and reads the result through the Delta
logs: the rows of every active file (each block exactly once per table),
`txn` rising by one commit per transaction to the authority's ordinal, and
`blocks` committed last in every transaction.

| §4 row | Before (L3) | Now (L4) | Real-binary test | Library test |
|---|---|---|---|---|
| `CommittedPersisted`, no Delta commit yet | parts verified, authority advanced; the transaction in no table | every table's `txn` is below `L`: their parts are verified, the tables committed (`blocks` last), then authority advances | `committed_without_a_delta_commit` (`crash-at:CommittedPersisted`) | `a_committed_journal_without_delta_commits_rolls_every_table_forward_once` |
| Between table commits (`DeltaCommitted(i)`) | the tables that had not committed miss the transaction | tables at `L` skipped without reading their parts; the others committed, `blocks` last | `between_table_commits` (`crash-after-delta-commit:logs`) | `a_roll_forward_skips_committed_tables_and_survives_its_own_interruption` (the committed table's parts deleted; the roll-forward itself interrupted) |
| A table's commit PUT is ambiguous | the S3 owner latched and retained until `recovery release`; restart as above | no latch: the owner is released; the next start reads `txn`, and a delayed copy is resolved from the log in every arrival order | `an_unknown_commit_outcome` (`delta-commit-lost-response:transactions`; on S3 `recovery status` shows `released` and the restart needs no release) | `an_unanswered_delta_commit_releases_ownership_and_the_next_start_reads_its_txn` (loopback S3 drops the response), `a_roll_forward_commits_each_part_once_in_every_arrival_order` (local, in-memory, loopback S3), `a_delayed_copy_of_an_unknown_commit_is_resolved_from_the_log` |
| All Delta commits done, before `AuthorityAdvanced` | recovered through `verify_all_finals` | every `txn` is `L`: no part is read, authority advances | `all_delta_commits_done` (`crash-after-delta-commit:blocks`, then OPTIMIZE and a lite VACUUM of retention 0 remove the transaction's parts) | `once_every_log_or_authority_holds_the_transaction_no_part_is_read` |
| `AuthorityAdvanced`, before mirror or clear | `verify_all_finals`, which fails once VACUUM removed a part | authority is at the target: no part is read; mirror, clear | `authority_advanced` (`crash-at:AuthorityAdvanced`, then the same maintenance) | same |
| During table creation at initialization | unit race test only | the restart validates the created tables unchanged and creates the rest; later, a table without a log is refused with guidance | `table_creation` (`crash-after-delta-create:logs`, tables created in name order) | `an_interrupted_creation_is_completed_and_recovery_opens_existing_tables_only` |
| `txn` > L on some table | not checked | refused at every start before any change, naming the table, its `txn` and the authority's ordinal | `log_ahead_of_authority` (authority restored from an older copy) | `a_log_ahead_of_authority_is_refused_before_anything_changes` (a foreign commit with this `appId`, with and without a pending transaction) |
| An uncommitted table's part is missing or corrupt | recovery stops | only the uncommitted tables' parts are read; a missing or corrupt one fails closed with the §4.1 message, the journal and authority kept | `a_vacuumed_uncommitted_part` (a full VACUUM of retention 0 between the crash and the restart) | `a_missing_or_corrupt_uncommitted_part_fails_closed_with_the_journal_kept` |

`recovery_recover` runs the between-commits crash through
`fireparq recovery recover` instead of a `build` start, and
`maintenance_beside_build` is the #636 test.

## Tests

- `blocks/tests/delta_recovery.rs` (new, 20 tests: 10 scenarios, each on
  local disk and on the loopback HTTPS S3 endpoint of
  `examples/bench_live_flush/s3.rs`): the eight rows above,
  `recovery_recover`, and `maintenance_beside_build`, which runs a
  `deltalake` loop (OPTIMIZE every date, lite VACUUM of retention 0,
  checkpoint) on the four tables with rows while `build` catches up on 40
  paced blocks in two runs, then a last round and a restart, and checks every
  flush, no maintenance error, OPTIMIZE commits between fireparq's commits in
  the same tables, each block once per table through the logs, and the same
  rows through Polars `scan_delta`. An aborted S3 build keeps its owner, which
  the tests release with `recovery release` as an operator would.
- Maintenance is `tests/engines/delta_maintain.py`, run with the Python in
  `FIREPARQ_POLARS_PYTHON` (the pinned `deltalake` of
  `tests/engines/requirements.txt`, required in CI by
  `FIREPARQ_REQUIRE_POLARS`). Without it, the compaction and full-VACUUM tests
  delete exactly the files VACUUM would and skip the row reads those files
  would serve, and `maintenance_beside_build` is skipped.
- The loopback endpoint gains `DeleteObjects` (`POST /?delete`), which
  `deltalake`'s VACUUM sends, and `put`/`remove` for restoring and deleting
  objects from a test.
- Library tests: see the table; `delta::commit::tests` runs its two new tests
  on local disk, an in-memory store and the loopback S3 of its own tests.
  `recovery_does_not_roll_delta_commits_forward_yet` (which pinned L3's
  behavior) is replaced, `an_unanswered_delta_commit_keeps_remote_ownership_uncertain`
  becomes the release test above, and the `recovery recover` fixture of
  `ingest::maintenance::tests` now writes Delta types and tables and checks
  the roll-forward.

## Validation

- `cargo fmt --all` and `cargo test --workspace --locked` with
  `FIREPARQ_POLARS_PYTHON` (Polars 1.44.2, `deltalake` 1.6.6),
  `FIREPARQ_REQUIRE_POLARS=1` and `FIREPARQ_REQUIRE_DUCKDB=1` (DuckDB 1.1.1)
  pass on origin/main `bd02f6a` (after L5b/L7) plus this change: 961 passed,
  13 ignored, 0 failed on macOS (1,002 and 14 on `d79ce49`, before L5b
  removed the plain-Parquet tests). 26 of them are new: 20 in
  `blocks/tests/delta_recovery.rs` and 6 in the `firehose-parquet` library
  (two tests replaced in place).
- `maintenance_beside_build` in one local run: 177 maintenance rounds, 152
  OPTIMIZE commits between fireparq commits; on loopback S3: 10 rounds, 40.
  No maintenance call failed and no fireparq flush failed.
- No real endpoint or bucket was contacted: the mock Firehose and the S3
  endpoints listen on `127.0.0.1`, and every child process runs with a cleared
  environment in a temporary directory.

## Limits

- `recovery recover` validates the tables without their schema (it has no
  mapper); the next `build` checks it.
- The pending journal's age is logged and reported in the fail-closed error,
  not exported as a metric (design §4.1 update).
- The log-ahead check reads every table's `txn` at every start: usually one
  commit file per table, and the checkpoint plus the tail for a table that
  never got rows.
- A roll-forward that finds a part gone still leaves the transaction visible
  in the tables that hold it and missing from the others: the dataset is
  rebuilt into a new root (design §4.1). Only a full VACUUM with a retention
  shorter than the outage, which the deployed job never runs, leads there.
