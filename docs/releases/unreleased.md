# firehose-parquet: unreleased

Changes merged since [v1.3.0](v1.3.0.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

## Fixes

- **A replay with commits of 5 seconds or more is detected as a catch-up**
  (#721, #722).
  - **What was wrong:** a writer that started deep in a backfill could stay
    `caught_up` for the whole replay: `firehose_parquet_catching_up` stayed 0,
    flushes carried `pace="caught_up"`, and `--flush-interval-secs` stayed
    armed. It happened when each flush's commit blocked the stream for longer
    than one 5-second pace sample and the blocks between two commits arrived
    in less than that. Seen on Ethereum near block 22.8M with
    `--flush-memory-bytes` 1 GiB (240 blocks over 4 s, then a 5.4 to 6.5 s
    commit). A writer that entered the catch-up earlier, such as one started
    at genesis, kept it.
  - **Now:** the writer's own commit is no longer measured as a wait of the
    stream, and it does not count toward the 30 seconds of evidence that
    start a catch-up. The writer above switches after about 80 seconds.
  - **Operators:** nothing to do. An alert on `firehose_parquet_catching_up
    == 0` and a large `firehose_parquet_block_time_lag_seconds` no longer
    fires for such a backfill. No output, schema or dataset state changes.
  - [Record](../audit/659-adaptive-flush.md#the-writers-own-commits).

## Performance

## Internal
