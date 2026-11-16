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

- **Relocate a dataset: `fireparq recovery rebind` (#635).** A stopped dataset
  copied to a new root (`cp -R`, `aws s3 sync`) on the same storage service
  resumes there after `fireparq recovery rebind <new root> --from <old root>`.
  It checks that the copy is complete and that the old root is quiet, then
  changes only the copy's binding: `state.json`, each Delta table's
  `fireparq.descriptor` (one `SET TBLPROPERTIES` commit per table, `blocks`
  last) and the cursor mirror. The old root is only read, and a rerun completes
  an interrupted rebind. `build` and `recovery recover` now name the command when
  they find a dataset bound to another root. See [Relocating a
  dataset](../cursor-and-resume.md#relocating-a-dataset).

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
