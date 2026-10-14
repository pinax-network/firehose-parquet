# firehose-parquet: unreleased

Changes merged since [v1.2.0](v1.2.0.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

- **HyperLiquid reference tables: `fireparq hypercore-reference`** (#716).
  A new subcommand of the same binary and image snapshots HyperLiquid's
  `/info` into ten small append-only Delta tables (`hl_*`): spot tokens and
  pairs, perp dexes and assets, HIP-4 outcomes and questions, and the hourly
  funding premium. Readers join them with a HyperCore lake at query time
  through the SQL views of [HyperLiquid reference
  tables](../hypercore-reference.md). Three jobs (`outcomes`, `meta`,
  `funding`) run on a schedule
  ([`deploy/examples/hypercore-reference-cronjob.yaml`](../../deploy/examples/hypercore-reference-cronjob.yaml));
  every commit is a single conditional attempt, never over a concurrent one,
  and each job compacts and vacuums its own tables and backfills across runs.
  It is the Rust port of the Python job of
  [k8s-parquet#82](https://github.com/pinax-network/k8s-parquet/pull/82), with the same
  tables, payload hashes and views. **It needs its own bucket or root**, for example
  `s3://hypercore-ref`, never a fireparq dataset root (whose `build` would then
  refuse to resume), with an S3 user limited to its table prefixes
  ([The S3 user](../hypercore-reference.md#the-s3-user)). Set
  `AWS_ENDPOINT_URL_S3` and both keys for an `s3://` root; a URL in
  `HL_INFO_URL` that embeds a key is never printed.

## Fixes

## Performance

## Internal
