# firehose-parquet: unreleased

Changes merged since [v1.6.0](v1.6.0.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

- **New EVM datasets no longer have `account_creations`,
  `system_account_creations` or `system_nonce_changes` (#743).** Current EVM
  Firehose blocks never fill them. Block version 4 and later record no account
  creations, and system calls change no nonce. On the public lake,
  `account_creations` has no rows after Pectra on Ethereum mainnet or after
  Pascal on BSC, and none on Base; the two system tables never had one.
  `--with-account-creations` (`WITH_ACCOUNT_CREATIONS=true`) and
  `--with-system-nonce-changes` (`WITH_SYSTEM_NONCE_CHANGES=true`) write them
  as before, with the same schemas and digests, like `--with-gas-changes`
  (#738). **Set both on every existing EVM dataset with extended output
  (writers and backfill lanes) before upgrading.** Without them, `build`
  refuses to start and names each missing flag, before any Blocks request.
  v1.6.0 ignores both variables, so they can be set before the upgrade. New
  EVM datasets with extended output have 15 tables
  ([Opt-in tables](../chains/evm.md#opt-in-tables)).
- **Rollback:** v1.6.0 always declares these three tables, so it refuses an
  EVM dataset created without them.

## New features

- **`fireparq recovery rebind --renamed` rebinds a dataset whose root was
  renamed in place (#635).** A bucket rename (Ceph RGW's `radosgw-admin
  bucket link --bucket-new-name`) or a local `mv` moves no data. It leaves
  nothing at the old root, so a rebind without `--renamed` has no copy to
  compare with and refuses ("holds no ingestion state", now naming the
  flag). With `--renamed` the rebind refuses unless nothing of the dataset is
  left at the old root: no local directory; on S3, no owner record in the
  old bucket, and no `state.json` or pending journal at the old root. On S3
  the new root must be the same prefix in another bucket. Every other check,
  the writes and the report are unchanged. A dataset can now change its
  bucket's name without a copy of the bucket. The bucket's policies and the
  readers' paths, Iceberg metadata included, still name the old bucket and
  are the operator's to move
  ([Renamed in place](../cursor-and-resume.md#renamed-in-place)).
- **Writer process RSS and thread count on Linux (#736, #723).** The
  metrics endpoint exports `process_resident_memory_bytes` and
  `process_threads`, read from `/proc/self/status` at each scrape, so memory
  outside the tracked buffers shows up beside the buffer estimates. At 64
  or more threads the writer also logs a `writer thread snapshot`, the
  thread names grouped by count, at most once every five minutes and over
  at most 256 threads ([Prometheus metrics](../metrics.md)). Nothing to
  configure; other platforms export neither.

## Fixes

## Performance

## Internal
