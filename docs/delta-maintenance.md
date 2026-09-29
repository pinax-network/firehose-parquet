# Delta maintenance

fireparq only appends: each flush adds one file per table and day, and one
Delta commit per table. Compacting those files, deleting replaced ones and
checkpointing the logs is platform-side policy, not a `fireparq` command (#643):
the separate binary `fireparq-maintenance` (workspace crate
[`maintenance/`](../maintenance/)) runs on a schedule, beside a running `build`.
It calls delta-rs's own operations (`deltalake-core` 1.0.0: OPTIMIZE, which
needs DataFusion 55, VACUUM, `create_checkpoint` and `cleanup_metadata`) and has
no compaction logic of its own. DataFusion is linked into this binary only,
never into `fireparq`. The job needs no fireparq ownership: fireparq's commits
are blind appends that rebase over the job's commits, and the job never touches
`.fireparq-ingest/` or `_fireparq/`.

For each table it runs, in order:

1. **OPTIMIZE** each closed `date` with more than one file, to the table's
   `delta.targetFileSize` (256 MiB). A day is closed once `blocks` holds a
   later day: `blocks` commits last, so every earlier day is complete in every
   table.
2. **VACUUM**, lite by default: it deletes only files that a `remove`
   tombstone older than the retention names, and never a part that `build`
   published but has not committed yet. A full VACUUM (`FULL_VACUUM=1`, run
   weekly) also deletes untracked files older than the retention (the files of
   a failed OPTIMIZE), so the job refuses it below 168 hours.
3. **A checkpoint**, after VACUUM: a checkpoint drops expired tombstones, and
   a VACUUM after it would leave their files behind. It is skipped when
   VACUUM failed.
4. **Log cleanup** of commits older than `delta.logRetentionDuration`
   (7 days) behind a checkpoint.

OPTIMIZE and VACUUM commit with their own post-commit checkpoint and log
cleanup turned off, so the only checkpoint is step 3. A table that does not
exist yet (the writer has not created it) is skipped, not failed.

Every step is idempotent: a failed or conflicting run changes nothing that
the next run cannot finish, and the writer never notices. One JSON object per
line goes to stdout:

- `start`: `root`, `tables`, `full_vacuum`, `retention_hours`,
  `optimize_dates`, `dry_run`, `deltalake` (the `deltalake-core` version) and
  `version` (the binary's);
- one line per table: `table` with `version_before`, `dates_to_compact`,
  `compacted` (`date`, `files_removed`, `files_added`), `vacuum` (`mode`,
  `retention_hours`, `files_deleted`), `checkpoint_version`, `version_after`,
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
| `OPTIMIZE_MAX_CONCURRENT_TASKS` | CPU count | Bounds OPTIMIZE's memory |
| `DRY_RUN` | `0` | `1` reports what would be compacted and deleted, and changes nothing |

```bash
# From a checkout (the release tarballs also ship the binary)
cargo build --release -p fireparq-maintenance
DRY_RUN=1 LAKE_ROOT=output/mainnet LAKE_TABLES=blocks,transactions,logs \
  ./target/release/fireparq-maintenance
```

Each release also publishes the job as an image,
`ghcr.io/pinax-network/firehose-parquet-maintenance:<version>`
([`deploy/maintenance/Dockerfile`](../deploy/maintenance/Dockerfile)): the
`fireparq-maintenance` binary as its entrypoint on `debian:bookworm-slim` with
CA certificates, running as user `65534`:

```bash
docker run --rm -e DRY_RUN=1 -e LAKE_BUCKET=ethereum-mainnet -e LAKE_TABLES=blocks \
  -e S3_ENDPOINT=https://rgw.example.internal -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  ghcr.io/pinax-network/firehose-parquet-maintenance:1.0.2
```

Images up to v1.0.1 hold the former Python job
(`scripts/delta_maintenance.py` with `deltalake` 1.6.6), with the same
settings, order and exit statuses.

On Kubernetes,
[`deploy/examples/delta-maintenance-cronjob.yaml`](../deploy/examples/delta-maintenance-cronjob.yaml)
runs that image hourly (`17 * * * *`, `concurrencyPolicy: Forbid`) and a full VACUUM
weekly, as an unprivileged user with a read-only root filesystem and a `/tmp`
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
- `blocks/tests/delta_maintenance.rs` runs the binary over and over beside a
  real `build`, on local disk and on a loopback S3 endpoint, and checks the
  exact rows (DuckDB and delta-rs), the `txn` versions and the file counts
  afterwards; `maintenance/tests/cli.rs` checks its configuration errors,
  exit statuses, redaction and skipped tables. The `blocks` tests find the
  binary next to `fireparq` (`cargo test --workspace` or
  `cargo build -p fireparq-maintenance` builds it) or at
  `FIREPARQ_MAINTENANCE`, and skip without it unless
  `FIREPARQ_REQUIRE_MAINTENANCE` is set, as in CI.
