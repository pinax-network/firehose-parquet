# Parallel backfill

A dataset has one writer, so its history is normally written by one
sequential `build`. A parallel backfill (#725) fills a bounded range of a
**new** dataset with one `build` per block range (lane), as many at a time as
wanted, then seals the result into an ordinary dataset that `build` continues
live.

```bash
# 1. Plan 0..20,000,000 in lanes of 250,000 blocks (writes the plan and empty tables)
fireparq backfill open --network base --output s3://base \
  --start-block 0 --stop-block 20000000 --lane-blocks 250000

# 2. One build per lane, with exactly its --start-block and --stop-block
fireparq build --network base --output s3://base \
  --start-block 250000 --stop-block 500000

# 3. Progress, then the seal
fireparq backfill status s3://base
fireparq backfill seal s3://base

# 4. Continue live from block 20,000,000
fireparq build --network base --output s3://base
```

## Rules

| Step | Rule |
|---|---|
| `open` | The root must be empty, as for any new dataset. It takes the options of `build` and binds them: every lane must run with the same chain, block type, encoding, feature flags and storage. Opening the same backfill again changes nothing; a different range or lane size is refused |
| Lanes | `[start, start + lane-blocks)` from `--start-block`; the last one ends at `--stop-block`. A `build` whose range is not exactly one lane is refused |
| Before the seal | Only lanes write. A `build` without a lane's range (a live writer included) fails with `this dataset is being backfilled in parallel and is not sealed`, before any Blocks request |
| One lane | One worker at a time: a second fails with `backfill lane <start>-<stop> is owned by another worker` |
| Every block number | A lane's first block is its start, and each next block the following number; anything else fails the lane with `backfill lane expected block <n> but received block <m>` |
| A complete lane | Every file it wrote is in the Delta logs. Running it again is a no-op |
| `seal` | Refuses unless every lane is complete with its owner released, and `blocks` holds exactly the backfill's range: its first and last block, no gap, no duplicate, every block linked to its parent, lane boundaries included. The check reads `blocks` one `date` at a time, so its memory does not grow with the range; `--no-validate` skips it. Sealing twice changes nothing |
| After the seal | An ordinary dataset whose start is the backfill's. `build` resumes from the stored cursor, at the backfill's stop block; `--start-block` must be omitted or equal that start, so a lane's range is refused |

Limits: S3 output, final-only streams, and block families whose rows are
routed by their own block time (not Solana). The chain must have every block
number of the range: chains that skip block numbers (NEAR, Beacon) are not
supported, and `--start-block` must not be below the chain's first block. An
existing dataset cannot be backfilled below its start.

## How it works

- `open` writes `.fireparq-ingest/backfill.json` (the dataset's stream
  descriptor, the stop block and the lane size), then creates the empty
  tables. The root has no authority until the seal, which is what refuses
  every other writer, older releases included. An interrupted `open` is
  completed by running it again.
- A lane is the ordinary [transaction machine](cursor-and-resume.md) for its
  range. Its owner record, checkpoint and pending journal are under
  `.fireparq-ingest/lanes/<start>-<stop>/`, it has no cursor mirror, and its
  parts go straight to `<table>/date=YYYY-MM-DD/`. Nothing is staged or copied.
- A lane does not commit every flush to the Delta logs, which all lanes
  share: lanes that did lost most of their commit attempts to one another
  (#728). A flush is published and accepted in the lane's own state, in one
  write that also lists its files as owed to the logs. The lane commits what
  it owes together, one commit per table and `blocks` last: every
  `--lane-commit-flushes` flushes (`LANE_COMMIT_FLUSHES`, default 64, 1-256),
  when it finishes, when it stops on SIGTERM, and at every start. A lane is
  complete only once it owes nothing.
- Each lane commits with its own `txn` application id, so lanes are blind
  appends beside one another, as `build` is beside the maintenance job. The
  tables record the dataset's `fireparq.descriptor`, not a lane's.
- `seal` writes the root's authority: the whole range accepted and complete,
  ending at the last lane's final block and cursor.

## Until the seal

- The [frontier rule](reading-tables.md#consistent-reads-across-tables) does
  not hold: the tables hold complete but disjoint ranges, and the newest
  `block_num` of `blocks` says nothing about the blocks below it. A running
  lane's newest flushes are in no table yet. Keep the bucket out of readers'
  hands until it is sealed.
- The [maintenance job](delta-maintenance.md) may run. It treats days a lane
  is still filling as closed and compacts them again as parts arrive, as
  `OPTIMIZE_DATES=all` does. Lanes commit in batches, which keeps the logs
  shorter, but a run over a large backfill takes hours and checkpoints each
  table once. A
  [checkpoint-only run](delta-maintenance.md#checkpoint-only-runs)
  (`OPTIMIZE=0`, `VACUUM=0`) beside it keeps the log tails short for readers
  and starting lanes; it does not make the backfill faster. A day
  two lanes share can be compacted before the earlier lane reaches it; the job then
  sorts that day back into block order as a
  [repair](delta-maintenance.md#row-order), `OPTIMIZE_REPAIR_DATES` days per
  table and run (default 1), so allow about one such day per lane.
- The files a lane owes to the logs are untracked until its next commit. The
  lite VACUUM never deletes an untracked file. The weekly full VACUUM
  (`FULL_VACUUM=1`) deletes those older than its retention, at least 7 days:
  do not leave a stopped lane that long beside it. Rerun the lane, or skip
  the full VACUUM until the seal. A lane whose owed file is gone stops with
  its state kept, and the backfill is redone into a new root.

## A lane that failed

Rerun it: the lane recovers its own journal before streaming, like any
`build`, and commits what it still owes to the Delta logs. If it stopped
inside such a commit, it reads each table's `txn` and adds only the files
that table lacks. `backfill status` reports a lane's owed flushes as
`uncommitted_flushes`. A lane that kept its owner (an uncertain request, a
killed process) blocks only itself and is released like a bucket owner, with
`--lane`:

```bash
fireparq recovery status s3://base --lane 250000-500000
fireparq recovery release s3://base --lane 250000-500000 \
  --expected-owner <owner-uuid-from-status> \
  --expected-generation <generation-from-status> \
  --stopped-writer-evidence <reference> \
  --provider-quiescence-evidence <reference>
```

`blocks/tests/parallel_backfill.rs` runs the whole sequence against a mock
Firehose and a loopback S3: concurrent lanes, a crashed lane, the refusals,
the seal and a `build` that extends the sealed dataset. Its second test
crashes a lane between two tables' commits and runs the maintenance job in
every mode before and while the lane recovers. Its third runs a lane that
commits every three flushes and dies inside the first such commit. Its fourth
stops a lane with SIGTERM: the flushes it accepted reach the logs, a block it
had only buffered is published nowhere, and its owner is released.
`firehose-parquet/src/ingest/controller/tests/deferred.rs` interrupts a
flush and a batch at every step.

Lanes written by v1.4.0, which committed every flush, are resumed by later
releases. The reverse is refused: v1.4.0 cannot read the state of a lane
that owes files, so it never drops them.
