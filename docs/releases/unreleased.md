# firehose-parquet: unreleased

Changes merged since [v1.0.5](v1.0.5.md). Fold this file into
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

- `fireparq-maintenance` keeps the writer's row order (#690, PR_PLACEHOLDER).
  - **What was wrong:** up to v1.0.5 it ran delta-rs's OPTIMIZE. Its compacted files held a day's blocks out of order and some blocks in pieces. Now and then a block's own rows were out of their Firehose order: on riv-dev1, one block a day in eth `logs` and in `calls`. No row was lost or changed.
  - **New days:** the job now compacts with its own planner. A day's writer parts are concatenated in block order, each file read start to finish.
  - **Old days:** files out of order are sorted back by `block_num` and the table's in-block key, `OPTIMIZE_REPAIR_DATES` days per table per run (default 1), newest first. `OPTIMIZE_REPAIR_WINDOW_BYTES` (default 256 MiB) bounds how much is sorted at once; the run peaks at about 1.3 GiB on eth `calls`. A tie on the key fails the repair, and a table without a known key is never repaired.
  - **Marking:** written files carry the tag `fireparq.rowOrder = writer` and the footer key `fireparq-maintenance.row_order = writer`.
  - **Removed setting:** `OPTIMIZE_MAX_CONCURRENT_TASKS` is gone (it configured delta-rs's OPTIMIZE) and is ignored if set.
  - **More output:** a `table` line's `compacted` entries add `rows` and `repaired_bins`, and the line adds `repairs_deferred`.
  - See [Row order](../delta-maintenance.md#row-order) and the [record](../audit/690-compaction-row-order.md).

## Performance

## Internal
