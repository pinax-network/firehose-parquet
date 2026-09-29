# Non-final streams and reorgs

Finalized-only output is the default and the [recommended
deployment](deployment.md). Non-final output is a CLI capability for
datasets that must show the reversible chain head: use
`--final-blocks-only=false`, or set `FINAL_BLOCKS_ONLY=false`. An explicit CLI
value takes precedence over the environment. The bare `--final-blocks-only`
flag still means `true`; optional values use `=` so the flag cannot consume a
following command. Whether a run is live (no `--stop-block`) is independent of
whether blocks must be final.

Non-final output is an **append-only event history**. Every mapped envelope adds
its block's rows (to `blocks` and to every other table the block has rows in),
with two extra columns that final-only output does not have, `stream_ordinal`
directly after `fork_step`:

- `fork_step` (`string`): `NEW` adds a block, `UNDO` records its removal from the
  chain (the undone block's rows are written again, marked `UNDO`), and `FINAL`
  is an explicit final event if the endpoint sends it. The usual non-final
  protocol sends `NEW` and occasional `UNDO`, not a later `FINAL` for every
  block. UNDO does not delete earlier rows.
- `stream_ordinal` (`long`): the accepted-event ordinal of the envelope that
  produced the row, the same for every row of that envelope in every table. It
  is strictly increasing in delivery order and durable: the protected session
  assigns it when the envelope is received and continues it from the output
  authority (`.fireparq-ingest/`) across reconnects and restarts, and each
  part's name records the window of ordinals its rows belong to
  (`part-v1-<stream>-<first>-<last>-...`). After a crash, recovery either keeps
  a transaction's rows with their ordinals or removes its rows before those
  ordinals are assigned again, so no two committed events share an ordinal.
  Ordinals can skip values (envelopes below `--start-block` write no rows).

A block identity can return as `NEW` after an `UNDO`, and a replay or reconnect
can deliver the same block again; every delivery is a new event with a new
ordinal. Block height, block time, `lib_num`, file names and row order are not
delivery-order keys (the maintenance job's OPTIMIZE rewrites files and their
row order), and neither the steps alone nor counting NEW minus UNDO gives the
current state: `NEW(A), UNDO(A), NEW(A)` ends with A present, while
`NEW(A), NEW(A), UNDO(A)` ends with A absent despite the same unordered rows.
`stream_ordinal` is the order that decides.

## Canonical live view

For each `block_num`, the event with the highest `stream_ordinal` decides the
head:

- latest is `NEW` (or `FINAL`) of block X: X is canonical at that height;
- latest is `UNDO`: the height currently has no block (a reorg removed it and
  nothing has replaced it yet, typically at the tip);
- any other step is treated as unresolved: no block.

A row of any table, `blocks` included, belongs to the head only when its
`(block_num, block_id, stream_ordinal)` matches that latest event. Matching the
ordinal and not only the block identity keeps exactly one copy of a block that
was delivered more than once: after `NEW(A), UNDO(A), NEW(B)` only B's rows
remain, and after `NEW(A), UNDO(A), NEW(A)` only the rows of the second
`NEW(A)`.

```sql
-- DuckDB 1.5 or later (INSTALL delta; LOAD delta) views over one non-final
-- dataset root. Replace live/mainnet with that root, for example
-- s3://ethereum-mainnet-live.
CREATE OR REPLACE VIEW live_head AS
SELECT block_num, block_id, stream_ordinal
FROM (
  SELECT block_num, block_id, fork_step, stream_ordinal,
         row_number() OVER (PARTITION BY block_num ORDER BY stream_ordinal DESC) AS latest
  FROM delta_scan('live/mainnet/blocks')
) events
WHERE latest = 1 AND fork_step IN ('NEW', 'FINAL');

-- One view per table, blocks included: the rows of each height's latest event.
CREATE OR REPLACE VIEW live_blocks AS
SELECT t.*
FROM delta_scan('live/mainnet/blocks') t
WHERE EXISTS (
  SELECT 1 FROM live_head h
  WHERE h.block_num = t.block_num
    AND h.block_id = t.block_id
    AND h.stream_ordinal = t.stream_ordinal
);

CREATE OR REPLACE VIEW live_transactions AS
SELECT t.*
FROM delta_scan('live/mainnet/transactions') t
WHERE EXISTS (
  SELECT 1 FROM live_head h
  WHERE h.block_num = t.block_num
    AND h.block_id = t.block_id
    AND h.stream_ordinal = t.stream_ordinal
);
```

`live_head` reads only the `blocks` table, which has exactly one row per event.
Each view has the table's own columns, `date` included. `blocks` commits last,
so a child table can briefly lack the rows of the newest events in `blocks`:
apply the [frontier rule](reading-tables.md#consistent-reads-across-tables) when that matters.
The window subquery and the `EXISTS` semi-join are standard SQL (DuckDB's
`QUALIFY` is avoided); only `delta_scan` is DuckDB's. CI runs this exact SQL
against real non-final output (`blocks/tests/non_final_stream.rs`).

Use a separate dataset root for a non-final stream: final-only and non-final
modes cannot share a root, and resuming a root in the other mode is refused. On
S3 give it its own bucket, since ownership is bucket-wide. Its history is
append-only (`delta.appendOnly`) and only grows; the maintenance job compacts
and checkpoints it like any other table, and nothing expires old events
(deleting data files outside the log breaks readers). A bounded non-final run
warns on successful completion because reaching its stop does not prove that
its tail is final, and later UNDO events will not be received after it stops.
A saved cursor or successful exit is not a finality certificate. See the
[`stream_ordinal` and live view record](audit/648-stream-ordinal.md) and the
[original non-final implementation](audit/474-non-final-streams.md).
