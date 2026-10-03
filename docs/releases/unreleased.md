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

- `fireparq-maintenance` no longer repairs the same day over and over
  (#690, PR_PLACEHOLDER).
  - **The loop:** v1.0.6 rolled a repaired file to a second one at the target
    size, inside a block. Its planner then took the two files, which share
    that block, for an overlap and sorted them again on every run. The rows
    were rewritten unchanged, but each such table spent its one repair a run
    on that day, and its older days were never repaired. On riv-dev1 this hit
    Base and BSC `storage_changes`, whose old files were just over 256 MiB.
  - **The fix:** each bin is now written as one file, as delta-rs writes a
    bin. Two files in writer order that only share a boundary block are no
    longer an overlap.

## Performance

## Internal
