# firehose-parquet: unreleased

Changes merged since [v1.5.0](v1.5.0.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

- `fireparq maintenance` takes `OPTIMIZE=0`, which skips compaction. With `VACUUM=0` a run only checkpoints and cleans up the log, for frequent runs beside a compaction run that takes hours, as beside a parallel backfill ([Checkpoint-only runs](../delta-maintenance.md#checkpoint-only-runs)).

## Fixes

## Performance

## Internal
