# firehose-parquet: unreleased

Changes merged since [v1.3.1](v1.3.1.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

- **Parallel backfill** (#725): `fireparq backfill open` plans fixed block
  ranges (lanes) in a new, empty S3 dataset root, one `build` per lane fills
  them at the same time into the same Delta tables, and `fireparq backfill
  seal` turns the result into an ordinary dataset that `build` continues
  live. Until the seal every other `build` into the root is refused.
  `recovery status` and `recovery release` take `--lane <start>-<stop>` for a
  lane's own owner. Final-only streams on chains with every block number
  (not Solana, NEAR or Beacon). See
  [Parallel backfill](../parallel-backfill.md).

## Fixes

## Performance

## Internal
