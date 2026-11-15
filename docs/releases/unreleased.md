# firehose-parquet: unreleased

Changes merged since [v1.4.0](v1.4.0.md). Fold this file into
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

## Performance

- **Backfill lanes commit to the Delta logs in batches (#728).** A lane
  committed every flush to logs that all lanes share: ten lanes of the `base`
  backfill lost 1.3 attempts per commit, spent most of their time committing
  and failed at the 25-attempt limit. A lane now accepts each flush in its own
  state and commits its files together, one commit per table: every
  `--lane-commit-flushes` flushes (`LANE_COMMIT_FLUSHES`, default 64), when it
  finishes, on SIGTERM and at every start. `backfill status` reports
  `uncommitted_flushes`. Lanes of a backfill begun with v1.4.0 resume under
  this release; do not go back, since v1.4.0 refuses the state of a lane that
  owes files. Keep the weekly full VACUUM away from a lane stopped for 7 days
  ([details](../parallel-backfill.md#until-the-seal)).

## Internal
