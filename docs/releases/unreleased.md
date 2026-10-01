# firehose-parquet: unreleased

Changes merged since [v1.0.4](v1.0.4.md). Fold this file into
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

- **Compacted files keep fireparq's footer metadata** (`fireparq-maintenance`).
  delta-rs's OPTIMIZE wrote compacted files with its own Parquet writer, so
  they lost every `firehose-parquet.*` key the writer puts in a part's footer
  (chain, endpoint, block type, encodings, version, first streamable block)
  and kept only `ARROW:schema`. The job now reads the day's footers before
  compacting it and gives the compacted file each `firehose-parquet.*` key the
  parts agree on, plus `fireparq-maintenance.version`. The parts'
  `fireparq.ingest.*` provenance isn't carried, a key whose values differ
  (for example `version` across a writer upgrade) is left out, and an
  unreadable footer leaves the day for the next run. Days compacted by an
  earlier job keep no metadata until compacted again; the table's Delta
  metadata (`fireparq.chain`, `fireparq.blockType`, `fireparq.descriptor`)
  was never affected. The `table` line's `compacted` entries gain
  `footer_keys`. See [Delta maintenance](../delta-maintenance.md).

## Performance

## Internal
