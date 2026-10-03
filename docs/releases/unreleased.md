# firehose-parquet: unreleased

Changes merged since [v1.0.6](v1.0.6.md). Fold this file into
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

- The example maintenance CronJob
  (`deploy/examples/delta-maintenance-cronjob.yaml`) gives a run 3 h instead of
  50 min. With v1.0.6, a table's day is repaired all or nothing, and the first
  run on riv-dev1 took 90 min on BSC (`calls` alone 44 min). A shorter
  deadline kills every run in the same table, so that table and the ones after
  it are never compacted.

## Performance

## Internal
