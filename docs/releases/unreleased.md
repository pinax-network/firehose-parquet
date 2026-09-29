# firehose-parquet: unreleased

Changes merged since [v1.0.0](v1.0.0.md). Fold this file into
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

## Internal

- **Documentation layout.** `README.md` is now a short front page: what
  `fireparq` does, supported chains, install, a quick start, the recommended
  deployment in brief and a table of every documentation page. Its detailed
  sections moved, unchanged, to pages under `docs/`: `getting-started.md`,
  `authentication.md`, `cli.md`, `features.md`, `non-final-streams.md`,
  `output-layout.md`, `reading-tables.md`, `deployment.md`,
  `cursor-and-resume.md`, `delta-maintenance.md`, `metrics.md`,
  `development.md` and the per-chain notes in `docs/chains/`. The "What's new
  in v1.0.0" summary is covered by the release notes. The live-view SQL that
  `blocks/tests/non_final_stream.rs` runs is now read from
  `docs/non-final-streams.md`.
