# firehose-parquet: unreleased

Changes merged since [v1.0.7](v1.0.7.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

- The maintenance job is `fireparq maintenance`: one binary, one image and one
  version with the writer.
  - **What's gone:** the separate `fireparq-maintenance` binary and its image,
    `ghcr.io/pinax-network/firehose-parquet-maintenance`, are no longer
    published (the last is v1.0.7). Release tarballs ship `fireparq` only.
  - **What to do:** run the writer's image,
    `ghcr.io/pinax-network/firehose-parquet` (entrypoint `fireparq`), with
    `args: ["maintenance"]`. The settings (environment variables, now also an
    `--env-file`), output and exit statuses are the same; see
    `deploy/examples/delta-maintenance-cronjob.yaml`.
  - **Metadata:** compacted files no longer carry
    `fireparq-maintenance.version`, since the job's version is the writer's.
    The row-order key is `fireparq.row_order` (it was
    `fireparq-maintenance.row_order`) in the footer and in the OPTIMIZE
    commit. Files already compacted keep their keys.
  - **No DataFusion:** nothing links it any more. The compaction has been the
    crate's own since v1.0.6, and its tests write their fixtures with
    delta-rs's Parquet writer.

## New features

## Fixes

## Performance

## Internal
