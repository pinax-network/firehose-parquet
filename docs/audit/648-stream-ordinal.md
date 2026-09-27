# Issue #648: per-row `stream_ordinal` for dedup-safe non-final output

Closes #648; refs #474; part of #463.

## Problem

Non-final output (`--final-blocks-only=false`) appends every NEW and UNDO
envelope as rows ([#474](474-non-final-streams.md)). Its schema had no
delivery order: block height, time, `lib_num`, file names and row order do not
order events, so `NEW(A), UNDO(A), NEW(A)` and `NEW(A), NEW(A), UNDO(A)` had the
same unordered rows and opposite current states. A "live" dataset could not show
the canonical head; the README only offered an intersection with a separate
final-only capture, which says nothing about the reversible tail.

## Column

Every table of a non-final stream gains **`stream_ordinal`**, `UInt64`, not
nullable, directly after `fork_step` on every chain (both are pushed by the
shared `push_fork_step_field`, so the pair sits wherever `fork_step` sat).
`UInt64` matches the block number columns; a later Delta or Iceberg mode maps
unsigned types to signed ones, and ordinals stay far below 2^63.

The value is the **accepted-event ordinal** of the protected ingestion session,
not a per-process counter:

1. `AcceptedFrontier::receive` (`firehose-parquet/src/ingest/frontier.rs`) assigns
   `assigned_ordinal + 1` to every envelope at receipt, before filtering,
   bootstrap buffering or mapping. `IngestionRuntime::observe` keeps it with the
   buffered block and `process_ready` passes it to the mapper as
   `StreamEvent { fork_step, stream_ordinal }` (`firehose-parquet/src/traits.rs`).
   Every builder of every table appends the same `StreamEvent`, so all rows of
   one envelope carry one value.
2. Mapping resolves envelopes strictly in order (`IngestionSession::accept_mapped`
   requires `ordinal == next_accepted_ordinal`), so a flush holds rows of one
   contiguous accepted prefix `first..=last`. `Checkpoint::advance` accepts a
   prefix only if `first == checkpoint.ordinal + 1`, and each part's name records
   the window (`part-v1-<stream>-<first>-<last>-<txn>-<index>.parquet`).
3. **Reconnects** keep the frontier: `FirehoseClient::stream_blocks` resumes from
   the last received cursor inside the same process, so numbering continues.
4. **Restarts** resume numbering from the durable checkpoint
   (`AcceptedFrontier::resume` starts after `checkpoint.ordinal`). Recovery runs
   before any Blocks request: a Committed transaction is rolled forward, keeping
   its ordinals; a Writing transaction is rolled back and its parts removed, so
   its ordinals are reassigned only to events with no remaining rows.
5. `IngestionSession::flush` now refuses a non-final flush whose rows lack the
   column or carry an ordinal outside the prefix being committed, before
   anything is journaled.

Consequences: ordinals are strictly increasing in delivery order, durable across
reconnects and restarts, and never shared by two committed events. They are not
contiguous: envelopes filtered below `--start-block` consume an ordinal and write
no rows. Dry runs, which write nothing, pass 0.

`BlockMapper::map_block` / `map_block_bytes` take a `StreamEvent` instead of
`Option<&str>`; `ForkStepBuilder` now builds both columns and `est_fork_step`
estimates them.

## Schema compatibility

Final-only schemas are byte-identical. A new pinned digest over the final-only
half of the mapper matrix (`final_only_schemas_match_the_pinned_pre_stream_ordinal_digest`
in `blocks/src/chain/tests.rs`) was computed on origin/main `081dea5` before the
change (`f0a9665b…`) and is unchanged after it, so every final-only protected
table digest is too. The full-matrix digest is re-pinned; the pre-#550 digest
still matches once `stream_ordinal` is removed. Non-final table digests change,
so a non-final root built before this change refuses to resume (no release has
written one). `MAPPER_EPOCH` is unchanged. `docs/schemas/` is regenerated.

## Canonical live view and two-bucket union

The README "Non-final streams and reorgs" section now documents the rule and
DuckDB views: for each `block_num` the event with the highest `stream_ordinal`
decides (NEW or FINAL of X: X is canonical; UNDO or an unknown step: no block),
and a row of any table belongs to the head only when its
`(block_num, block_id, stream_ordinal)` matches that event (an `EXISTS`
semi-join). The two-bucket union reads each table from the final-only dataset up
to its frontier (the highest `block_num` of its `blocks` table) and from the live
view above it, in the final schema. A Spark/Trino note covers the non-DuckDB
syntax. The README section "Live + final two-bucket deployment" lists the
settings of both writers, lifecycle rules, object counts and why `verify` does
not apply to live data.

## Lifecycle deletion of committed live parts

A live bucket expires old parts with an S3 lifecycle rule that fireparq does not
own. Findings from the code:

- **No read of committed parts.** The transaction controller reads, verifies,
  rolls back or rolls forward only the parts of its own pending transaction.
  Writing rollback tolerates parts that are already gone. The cursor mirror is a
  control file. `recovery status` reads only the owner record and the two control
  slots. `recovery recover` and every `build` start list the dataset (nested
  `.fireparq-ingest` markers twice, merge journals once on S3), which tolerates
  missing objects.
- **Listings scale with retained objects.** Each S3 listing of the root is limited
  to 60 seconds; the README asks to keep a live bucket's retained objects in the
  low hundreds of thousands and to prefer `FLUSH_INTERVAL_SECS` on fast chains.
- **One hazard.** A crash that leaves a Committed transaction pending, followed by
  an outage longer than the expiration, lets the rule delete that transaction's
  parts; the next start then refuses (`committed transaction is missing a required
  final part`, covered by
  `committed_missing_or_corrupt_part_cannot_roll_forward_or_replay`). The README
  recommends starting a new live dataset, since live data is disposable.
- **`verify`** reads every part but records nothing for a growing non-final
  dataset (every partition is `open`), and an expiry during its scan fails it.
- **`merge` / `rollup` journals** live inside table directories, where a table
  prefix rule would expire them; they must not run on a lifecycle-managed bucket
  (they cannot anyway while the live `build` owns it).

The lifecycle guidance scopes expiration to table prefixes (S3 filters cannot
exclude paths) and names the control state a rule must never match:
`.fireparq-ingest/`, the `.fireparq-owner*` records and `_fireparq/` (cursor
mirror, partition index, Merkle registry and verify reports).

## Validation

- `blocks/tests/non_final_stream.rs`,
  `stream_ordinals_are_durable_and_the_readme_live_view_selects_the_canonical_head`:
  the real binary against a cursor-aware mock Firehose replays NEW(A), UNDO(A),
  NEW(B) at 100; NEW(C), UNDO(C), NEW(C) at 101 (next hour); NEW(D), UNDO(D) at
  102 (an unreplaced UNDO at the tip). The first run is cut by an injected
  `Unavailable` after three envelopes and reconnects; a second process extends the
  bound. `blocks` and `transactions` carry ordinals 1 to 8 in delivery order, the
  same per envelope in both tables and inside each part's window. The test then
  extracts both README SQL blocks and runs them in DuckDB: `live_head` is B/3 and
  C/6, `live_transactions` keeps C's transaction once, and the union with a
  final-only capture up to block 100 takes 100 from the final dataset and 101 from
  the live view, without `fork_step` or `stream_ordinal`. Two mutations of the
  README SQL (ascending order; child join without the ordinal) fail the test.
- `expired_committed_parts_do_not_affect_a_running_or_restarted_live_build`: a
  live-style hour-partitioned build pauses after committing its first hour; the
  test deletes that hour of every table; the running build commits its next
  flushes and completes; `recovery status` (pending absent) and `recovery recover`
  succeed; a restarted build resumes from authority and numbers on (5, 6, then 7,
  8).
- `ingest::session::tests::remote_live_session_is_unaffected_when_expired_committed_parts_disappear`:
  the same on an in-memory S3 store, through `IngestionSession::open` (ownership,
  marker and merge-journal listings, recovery) and the next flushes.
- `ingest::session::tests::non_final_rows_carry_durable_strictly_increasing_stream_ordinals`
  and `ingest::controller::tests::recovery_never_reassigns_the_ordinal_of_a_committed_row`
  (a crash at every transaction boundary: rolled-forward windows keep their parts
  and the next ordinal follows them; rolled-back windows lose their parts and
  their first ordinal is reassigned).
- Mapper level: the schema contract test maps every table of every chain under
  every encoding with distinct ordinals and checks position, type and per-row
  values; the EVM golden and Solana reward tests check every row's ordinal.
- CI installs a pinned, checksum-verified DuckDB CLI (v1.1.1) and sets
  `FIREPARQ_REQUIRE_DUCKDB`, so the README query check cannot be skipped there.
  Locally the check runs when a `duckdb` CLI is on `PATH` or in `FIREPARQ_DUCKDB`.
- [474-check-query.py](474-check-query.py) still runs the superseded #474 query,
  now embedded in the script.
