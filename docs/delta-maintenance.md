# Delta maintenance

fireparq only appends: each flush adds one file per table and day, and one
Delta commit per table. Compacting those files, deleting replaced ones and
checkpointing the logs is platform-side policy, run on a schedule beside a
running `build` (#643): `fireparq maintenance`, the same binary and version as
the writer (workspace crate [`maintenance/`](../maintenance/)). It uses delta-rs
(`deltalake-core` 1.0.0) for VACUUM, `create_checkpoint` and
`cleanup_metadata`, and compacts with its own planner over delta-rs's Parquet
writer and commit, so that compacted files keep the rows in the order `build`
wrote them ([Row order](#row-order)). Nothing links DataFusion. The job needs no fireparq ownership: fireparq's commits
are blind appends that rebase over the job's commits, and the job never touches
`.fireparq-ingest/` or `_fireparq/`.

For each table it runs, in order:

1. **OPTIMIZE** each closed `date`, to the table's `delta.targetFileSize`
   (256 MiB), keeping the writer's row order ([Row order](#row-order)). A day
   is closed once `blocks` holds a later day: `blocks` commits last, so every
   earlier day is complete in every table. delta-rs's Parquet writer keeps none
   of the parts' footer metadata, so the job first reads the day's footers and
   gives each compacted file:
   - every `firehose-parquet.*` key (chain, aliases, endpoint, block type and
     features, encodings, compression, version, first streamable block) with
     one value among the parts that have it. A key whose values differ, such as
     `firehose-parquet.version` on a day that spans a writer upgrade, is left
     out, and a file without the key (one compacted by an earlier job) doesn't
     drop it;
   - `fireparq.row_order = writer` (files compacted by 1.0.6 and 1.0.7 have
     `fireparq-maintenance.row_order` and `fireparq-maintenance.version`
     instead).

   The parts' `fireparq.ingest.*` keys (their ordinals, transaction and
   stream) describe the parts being merged, not the compacted file, and aren't
   carried. When a footer can't be read, the day is left for the next run
   (reported as an error) rather than compacted without its metadata. Days
   compacted by an earlier job keep no metadata until they are compacted
   again.
2. **VACUUM**, lite by default: it deletes only files that a `remove`
   tombstone older than the retention names, and never a part that `build`
   published but has not committed yet. A full VACUUM (`FULL_VACUUM=1`, run
   weekly) also deletes untracked files older than the retention (the files of
   a failed OPTIMIZE), so the job refuses it below 168 hours. It never deletes
   anything in a table's top-level `metadata/` directory, where an Apache
   XTable sync writes the table's Iceberg metadata (`*.metadata.json`,
   `snap-*.avro`, manifests, `version-hint.text`) over the same Parquet files;
   Delta writes nothing there. delta-rs's full VACUUM would take those files
   for orphans, so the job has delta-rs plan it (a dry run) and deletes every
   planned file outside `metadata/` itself. A full VACUUM therefore adds no
   `VACUUM START` or `VACUUM END` entry to the log (a lite one still does);
   its record is the job's `table` line.
3. **A checkpoint**, after VACUUM: a checkpoint drops expired tombstones, and
   a VACUUM after it would leave their files behind. It is skipped when
   VACUUM failed.
4. **Log cleanup** of commits older than `delta.logRetentionDuration`
   (7 days) behind a checkpoint.

OPTIMIZE and VACUUM commit with their own post-commit checkpoint and log
cleanup turned off, so the only checkpoint is step 3. A day's OPTIMIZE is one
commit (`OPTIMIZE`, `dataChange: false`, as delta-rs's), which tables with
`delta.appendOnly` accept. A table that does not
exist yet (the writer has not created it) is skipped, not failed.

Every step is idempotent: a failed or conflicting run changes nothing that
the next run cannot finish, and the writer never notices. One JSON object per
line goes to stdout:

- `start`: `root`, `tables`, `full_vacuum`, `retention_hours`,
  `optimize_dates`, `dry_run`, `deltalake` (the `deltalake-core` version) and
  `version` (the binary's);
- one line per table: `table` with `version_before`, `dates_to_compact`,
  `compacted` (`date`, `files_removed`, `files_added`, `rows`,
  `repaired_bins`: the bins sorted back into the writer's order, and
  `footer_keys`: the footer keys the compacted files were given),
  `repairs_deferred` (days out of order left for a later run),
  `repairs_unsupported` (days out of order in a table with no known row order,
  left as they are), `vacuum` (`mode`,
  `retention_hours`, `files_deleted`, and for a full VACUUM
  `iceberg_metadata_kept`: the files in `metadata/` that were old enough to
  delete and were kept), `checkpoint_version`, `version_after`,
  `conflicts`, `errors`, `open_date` and `seconds`; or `skipped` with `table`,
  `reason` and `open_date` for a table that does not exist yet;
- `blocks_error` when `blocks` cannot be read (no date is closed then);
- `done`: `tables`, `failed`, `skipped`, `conflicts` and `seconds`.

The exit status is 0 when every table was maintained or skipped, 1 when a
table failed, or 2 for a configuration error (a single `config_error` line,
before any request). A conflict is a lost commit race only (delta-rs's commit
conflict, too many commit attempts, or a version that already exists), left to
the next run; any other commit error fails the table. Credentials are read from
the environment and redacted from every error.

On S3, log commits are conditional creates (`If-None-Match: *`), as the
writer's are, with no DynamoDB lock; the job sends no `If-Match` request, so
Ceph RGW 19.2's `If-Match` quirk ([#678](audit/rgw-if-match-etag.md)) does
not apply to it. Requests use object_store's default retries.

| Variable | Default | Meaning |
|---|---|---|
| `LAKE_ROOT` or `LAKE_BUCKET` | (required) | The dataset root, `s3://bucket[/prefix]` or a local path; `LAKE_BUCKET=b` is `s3://b`, a dataset at the bucket root |
| `LAKE_TABLES` | (required) | Comma-separated tables, for example every table of the network's [schema](schemas/README.md); dates are closed by `blocks`, and a table that does not exist yet is skipped |
| `S3_ENDPOINT` | AWS | S3 endpoint URL, for example the in-cluster RGW |
| `AWS_REGION` | `us-east-1` | |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | (required on S3) | The maintenance user; `AWS_SESSION_TOKEN` is optional |
| `AWS_ALLOW_HTTP`, `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | `false` | Plain HTTP; virtual-hosted instead of path-style requests |
| `SSL_CERT_FILE` | system roots | A PEM bundle of the CAs to trust instead, for example with the RGW's private CA |
| `AWS_S3_ALLOW_UNSAFE_RENAME` | | Refused: unsafe beside a running writer |
| `FULL_VACUUM` | `0` | `1` for the weekly full VACUUM |
| `VACUUM_RETENTION_HOURS` | the table's 7 days | Lower only shortens how long readers of older snapshots find replaced files; a full VACUUM refuses less than 168 |
| `OPTIMIZE_DATES` | `closed` | `all` also compacts the newest day: safe beside the writer, but repeated every run; use it once the writer has stopped for good |
| `OPTIMIZE_TARGET_SIZE` | `delta.targetFileSize` | Bytes |
| `OPTIMIZE_ZSTD_LEVEL` | `3` | Compression of the compacted files |
| `OPTIMIZE_REPAIR_DATES` | `1` | Days per table a run sorts back into the writer's order ([Row order](#row-order)); `0` repairs none. New days are only concatenated |
| `OPTIMIZE_REPAIR_WINDOW_BYTES` | 256 MiB | Decoded bytes a repair sorts at once, at least 1 MiB; a 256 MiB window peaks at about 1.3 GiB on Ethereum `calls` |
| `DRY_RUN` | `0` | `1` reports what would be compacted and deleted, and changes nothing |

## Row order

`build` writes each day's rows in the stream's order: blocks ascending, and
each block's rows in the order the Firehose block holds them (transactions by
`index`, logs by `block_index`, calls by `tx_index` and `call_index`, a call's
state changes in the order it recorded them). The job keeps that order
(`maintenance/src/compact.rs`):

- It plans a day by the files' `block_num` ranges, from the log's stats: files
  whose ranges overlap form one unit, and units are packed, oldest first, into
  bins of at most the target size. Two files in writer order that only share a
  boundary block don't overlap: the first holds the block's earlier rows. A
  unit larger than the target is a bin of its own, and a bin of one file
  already in writer order is left as it is. Each bin is written as one file,
  as delta-rs writes a bin, so no block is split across two files.
- A bin of files in writer order (`part-v1-*` parts, and files the job wrote,
  tagged `fireparq.rowOrder = writer` in the log) is **concatenated**: each
  file is read start to finish, one after the other, and its rows written as
  they are. A block lower than the one before it fails the bin.
- A bin with a file out of that order is **repaired**: its rows are sorted by
  `block_num` and the table's in-block key
  ([`maintenance/src/row_order.rs`](../maintenance/src/row_order.rs)), a window
  of blocks at a time. Each key is strictly increasing in the writer's order:
  `blocks/tests/evm_golden.rs` checks every table of the reviewed mainnet
  blocks, and on riv-dev1 the writer parts of eth, Base and BSC
  (about 560 million rows) held no tie and no row out of key order
  ([#690](audit/690-compaction-row-order.md)). Two rows of a block that tie on the
  key fail the bin, since their order can't be recovered. A table without a
  known key (another chain's) is never repaired.

A bin's written rows must equal its files' `numRecords`. Files written for a
bin or commit that fails are aborted or deleted; anything left behind is an
orphan the weekly full VACUUM reclaims.

Up to 1.0.5 the job ran delta-rs's own OPTIMIZE, which bins a day's files
newest first and reads each bin through a parallel DataFusion scan, writing
batches as they arrive. Its files hold the day's blocks in runs out of order,
some blocks in pieces, and now and then a block's rows out of their order
(on riv-dev1, one block a day in eth `logs` and in `calls`). No row was lost or
changed. From 1.0.6 the job repairs those days, `OPTIMIZE_REPAIR_DATES` per
table per run, newest first. Delta and Parquet don't promise readers any row
order: a query that needs one should still `ORDER BY` it.

## Running the job

```bash
# Settings are environment variables, or an --env-file
DRY_RUN=1 LAKE_ROOT=output/mainnet LAKE_TABLES=blocks,transactions,logs \
  fireparq maintenance
```

In a container, the writer's image runs it, `fireparq` being its entrypoint:

```bash
docker run --rm -e DRY_RUN=1 -e LAKE_BUCKET=ethereum-mainnet -e LAKE_TABLES=blocks \
  -e S3_ENDPOINT=https://rgw.example.internal -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  ghcr.io/pinax-network/firehose-parquet:1.0.7 maintenance
```

Up to v1.0.7 the job was a separate binary, `fireparq-maintenance`, published
as the image `ghcr.io/pinax-network/firehose-parquet-maintenance` with the same
settings, output and exit statuses; images up to v1.0.1 held the former Python
job (`scripts/delta_maintenance.py`).

On Kubernetes,
[`deploy/examples/delta-maintenance-cronjob.yaml`](../deploy/examples/delta-maintenance-cronjob.yaml)
runs the writer's image with `args: ["maintenance"]` hourly (`17 * * * *`, `concurrencyPolicy: Forbid`) and a full VACUUM
weekly. Its `activeDeadlineSeconds` (3 h) leaves a run the time to repair a
table's day, which commits all or nothing: on riv-dev1 the first run of 1.0.6
took 6 min on eth, 35 min on Base and 90 min on BSC (`calls` alone 44 min).
It runs as an unprivileged user with a read-only root filesystem and a `/tmp`
`emptyDir`. Give the job its own S3 user, limited to the table prefixes
(their `_delta_log/` included), with no access to `.fireparq-ingest/`,
`_fireparq/` or the owner record, and give the writer no delete permission on
`*/_delta_log/*`.

- A transaction that `build` has committed but not yet added to every table's
  log (after a crash in between, until the next start recovers it) must not
  stay pending longer than `delta.deletedFileRetentionDuration` (7 days), or a
  full VACUUM may delete its parts. Alert on a writer that has been down for
  more than a day.
- Alert on `firehose_parquet_delta_log_tail_commits` (see
  [Prometheus Metrics](metrics.md)) growing past a few hundred: the
  hourly checkpoints have stopped, and every reader and restart replays the
  whole tail.
- `blocks/tests/delta_maintenance.rs` runs `fireparq maintenance` over and over
  beside a real `build`, on local disk and on a loopback S3 endpoint, and checks the
  exact rows (DuckDB and delta-rs), that every file holds its rows in writer
  order, the `txn` versions and the file counts afterwards;
  `maintenance/tests/compaction.rs` checks that a file out of order is sorted
  back into it (several windows), that a tie leaves it as it is, and that
  repairs are spread over runs; `blocks/tests/maintenance_cli.rs` checks its
  configuration errors, exit statuses, redaction, skipped tables, and a full
  VACUUM that deletes old orphans but keeps `metadata/`.
