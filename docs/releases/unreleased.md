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

## Internal
