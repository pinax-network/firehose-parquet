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

- **Full VACUUM keeps Iceberg metadata** (#684, for #644). The weekly full
  VACUUM of `fireparq-maintenance` (`FULL_VACUUM=1`) never deletes a table's
  top-level `metadata/` directory, where an Apache XTable sync writes the
  table's Iceberg metadata beside `_delta_log/`. delta-rs's full VACUUM took
  those files for orphans and deleted them once they were older than the 168 h
  retention. The job now has delta-rs plan the full VACUUM (a dry run) and
  deletes the other planned files itself. The `table` line's `vacuum` object
  gains `iceberg_metadata_kept`, the `metadata/` files it kept, and
  `files_deleted` counts only the files it deleted (or, with `DRY_RUN=1`,
  would delete). A full VACUUM no longer adds `VACUUM START` and `VACUUM END`
  entries to the Delta log; a lite VACUUM still does. Nothing to do for
  operators; lite VACUUM is unchanged. See [Delta
  maintenance](../delta-maintenance.md).

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
