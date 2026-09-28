# Engine CI and the Delta maintenance job (#643, lanes L8 and L9)

Refs #643; part of #463. PR: [#673](https://github.com/pinax-network/firehose-parquet/pull/673). Design:
[`docs/design/delta-lake.md`](../design/delta-lake.md) §1.7, §1.8, §1.11,
§4.1, §9, §10 and the L8 and L9 rows of §11. Index: the #643 rows of the
[audit index](README.md).

## Diagnosis

Since L3 every `build` writes Delta tables, but:

- `blocks/tests/engine_compat.rs` still globbed each table's Parquet files
  with Hive partitioning. The target readers read through the Delta log, and a
  glob is wrong on a maintained table: after an OPTIMIZE and before the VACUUM
  that deletes the replaced files it reads those rows twice, and it can pick
  up a part that `build` published but has not committed.
- CI pinned DuckDB 1.1.1, whose `delta` extension (v0.2.1) fails anonymous S3
  reads once a table has a checkpoint (design §1.8), and the extension itself
  was fetched unpinned by the tests. DuckDB 1.5 also refuses `INSTALL` and
  `LOAD` without a home directory, which the tests' cleared environment does
  not have (found here: `delta_tables.rs` passed with 1.5.5 only because the
  extension was already installed).
- The maintenance job of design §9 existed only as a sketch, and the spike's
  concurrency test (`py/concurrent_maintenance.py`) drove the spike's writer,
  not `fireparq`.
- The VACUUM-then-checkpoint order of §4.1 came from reading the sources, not
  from a measurement.
- The spike crate and its `delta-spike` CI job duplicated what L2 and L3 had
  ported.

## Decision

- Compaction and cleanup are platform-side policy (the user's preference): an
  off-the-shelf `deltalake` job, not fireparq logic. The repository ships a
  reference script, its hash-pinned requirements and example CronJobs, to be
  adopted in k8s-parquet.
- Target readers are DuckDB (`delta_scan`) and Polars (`scan_delta`); JVM
  engines are not a target. CI pins DuckDB 1.5.5 and its `delta` extension by
  checksum, and Polars 1.44.2 with `deltalake` 1.6.6 by hash.
- DuckDB's extension repository has no versioned URLs for core extensions
  (`INSTALL delta VERSION '45c4087'` is a 404): it serves one build per DuckDB
  version and platform. CI downloads that build once, checks its SHA-256,
  installs it from the file, and the tests assert the loaded version
  (`FIREPARQ_DUCKDB_DELTA_VERSION`). If DuckDB replaces the build, the step
  fails and the new checksum and version are recorded there.
- The anonymous check against a deployment's RGW is opt-in and off in CI:
  there is no public bucket to read from CI, and the tests contact no real
  endpoint.
- The spike is deleted: every check it ran is now covered (table below).

## Implementation

### L8: engine CI

- `blocks/tests/engine_compat.rs` builds the same three datasets (EVM final,
  EVM non-final with a reorg at the tip, Solana), writes a checkpoint of
  every table with `deltalake` (so `_delta_log/` holds Parquet next to the
  Parquet cursor mirror in `_fireparq/`), and reads **every** table, 48 in
  all, 13 with rows, with both engines. For each: rows equal in DuckDB, in
  Polars and to the `numRecords` of the log's `add`s; only the log's
  `date=YYYY-MM-DD/part-v1-*.parquet` files (DuckDB's `filename` column,
  Polars' scan plan); `date` as the partition column, with a `date` filter
  returning exactly that day's rows and both engines pruning to that day's
  files (DuckDB's profile `Scanning Files: k/n`, Polars' plan); no `date`
  column in the data files; `block_num` `BIGINT`/`Int64`; `timestamp` as a
  microsecond UTC timestamp (Parquet `TIMESTAMP(MICROS, UTC)`) holding whole
  milliseconds; `stream_ordinal` only in non-final output. The listed columns
  keep their checks: `DECIMAL(20,0)` (an EVM nonce of `u64::MAX`, Solana
  `fee`, `pre_balances` as `DECIMAL(20,0)[]`, `rewards.post_balance`),
  `SMALLINT[]`, `VARCHAR[]`, `BLOB` and string enums, with exact minimums.
- `blocks/tests/common/mod.rs` holds the engine helpers the Delta tests share:
  DuckDB and Polars discovery (`FIREPARQ_REQUIRE_*`), a DuckDB session that
  loads `delta` with a home and extension directory and checks the pinned
  version, tagged multi-statement queries, the JSON log reader and row counts.
  `delta_tables.rs` keeps its log checks and now reads exact row counts after
  the restart through these helpers; types, pruning and the empty-table read
  are `engine_compat.rs`'s. `blocks/tests/engines/delta_check.py` reports what
  Polars and `deltalake` see; the plain-Parquet `polars_check.py` is removed.
- `anonymous_reads_of_a_public_deployment_bucket` (opt-in:
  `FIREPARQ_RGW_ENDPOINT`, `FIREPARQ_RGW_BUCKET`, optional
  `FIREPARQ_RGW_PREFIX`, `FIREPARQ_RGW_REGION`, `FIREPARQ_RGW_TABLE`) reads a
  deployment's bucket with unsigned requests only: the newest closed day of
  `blocks` from its log, then that day's rows and block range in `blocks` and
  a child table (with the frontier cut) in both engines, which must agree and
  prune to the same files. README "Engine compatibility" documents it for
  operators.
- `.github/workflows/ci.yml`: DuckDB v1.5.5 (`08c0ca11…643d05`); a new step
  installs the `delta` extension `45c4087` (`linux_amd64`,
  `c8ce674c…af90e9`) into `FIREPARQ_DUCKDB_EXTENSION_DIR` and sets
  `FIREPARQ_DUCKDB_DELTA_VERSION`; `FIREPARQ_REQUIRE_DUCKDB` and
  `FIREPARQ_REQUIRE_POLARS` stay, and `blocks/tests/engines/requirements.txt`
  (Polars 1.44.2, `deltalake` 1.6.6) stays `--require-hashes`. The
  `delta-spike` job is removed.

### L9: the maintenance job

`scripts/delta_maintenance.py`, for each table of `LAKE_TABLES` below
`LAKE_ROOT` (or `LAKE_BUCKET`):

1. OPTIMIZE (`compact`, ZSTD level 3, to `delta.targetFileSize`) of every
   closed `date` with more than one file. A date is closed once `blocks` holds
   a later one; the file counts come from one `get_add_actions` per table.
2. VACUUM: lite by default, full with `FULL_VACUUM=1`. The job refuses a full
   VACUUM below 168 h (setting or table property) and always enforces the
   table's retention for it; a lite VACUUM may run below the table's retention.
3. A checkpoint, only after a successful VACUUM (§4.1). OPTIMIZE and VACUUM run
   with their post-commit checkpoint and log cleanup off, so no checkpoint
   comes before the VACUUM.
4. Log cleanup (`cleanup_metadata`).

| Setting | Default | |
|---|---|---|
| `LAKE_ROOT` / `LAKE_BUCKET` | required, exactly one | `s3://bucket[/prefix]` or a local path; `LAKE_BUCKET=b` is `s3://b` |
| `LAKE_TABLES` | required | plain table names; `blocks` must exist |
| `S3_ENDPOINT`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_ALLOW_HTTP`, `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | AWS, `us-east-1`, required on S3, none, `false`, `false` | conditional-put log commits (`aws_conditional_put=etag`); `AWS_S3_ALLOW_UNSAFE_RENAME` is refused |
| `FULL_VACUUM` | `0` | |
| `VACUUM_RETENTION_HOURS` | the table's 7 days | a full VACUUM refuses < 168 |
| `OPTIMIZE_DATES` | `closed` | `all` also compacts the newest date |
| `OPTIMIZE_TARGET_SIZE`, `OPTIMIZE_ZSTD_LEVEL`, `OPTIMIZE_MAX_CONCURRENT_TASKS` | table property, 3, CPU count | |
| `DRY_RUN` | `0` | plan and VACUUM dry run only |

It refuses any `deltalake` but 1.6.6, prints one JSON object per line (`start`,
one `table` line per table, `done`), redacts the credentials from every error
it prints, and exits 0, 1 (a table failed) or 2 (configuration). A lost commit
race is reported as a conflict and left to the next run. Every step is
idempotent: a second run commits nothing (tested).

- `scripts/delta_maintenance.requirements.txt`: `deltalake` 1.6.6 and its
  dependencies, hash-pinned; a test checks that they equal the engine tests'
  pins and the script's `REQUIRED_DELTALAKE`.
- `deploy/examples/delta-maintenance-cronjob.yaml`: a settings ConfigMap, the
  hourly CronJob (`17 * * * *`, `concurrencyPolicy: Forbid`,
  `activeDeadlineSeconds: 3000`, `backoffLimit: 0`) and the weekly full-VACUUM
  CronJob (`47 3 * * 0`); `python:3.12-slim` installing the hash-pinned wheels
  into a `/tmp` `emptyDir`, UID 65534, `runAsNonRoot`, a read-only root
  filesystem, no privilege escalation, all capabilities dropped, the
  `RuntimeDefault` seccomp profile, no service account token; requests 500m
  CPU and 1 GiB, a 4 GiB memory limit; credentials from a Secret of a
  maintenance-only S3 user (design §5).
- The tests' loopback S3 endpoint (`blocks/examples/bench_live_flush/s3.rs`)
  gained multi-object DeleteObjects (`POST ?delete`, which `deltalake`'s VACUUM
  and log cleanup send) and records each object's PUT time as its
  `Last-Modified`, so a full VACUUM sees real ages instead of a fixed date.

### Tests (`blocks/tests/delta_maintenance.rs`)

- `maintenance_beside_a_local_build_keeps_exact_rows` and
  `maintenance_beside_an_s3_build_keeps_exact_rows`: the real binary on a
  cursor-aware mock Firehose (final EVM blocks, 10 per UTC day, one
  transaction per block) to block 130, then a clean restart to 140, while the
  job runs over and over in three modes: lite VACUUM with retention 0, the
  same with `OPTIMIZE_DATES=all` (compacting the date being appended), and a
  full VACUUM with the enforced 168 h. The stream waits on three blocks until
  one round of each mode has finished. Asserts: the writer never fails; no
  round has an error or a conflict; some OPTIMIZE commits land between writer
  commits, and some compact the open date beside the writer; DuckDB and Polars
  read exactly the written rows (each block once, with its rows); a closed
  date's filter scans one file; nothing under `_delta_log/` or `_fireparq/`
  is read; every table's `txn` equals the authority's ordinal; each date ends
  as one active file and one data file in storage; the full VACUUM deleted
  nothing. The S3 variant runs `fireparq` and the job against the same
  loopback HTTPS endpoint (`SSL_CERT_FILE`), placeholder credentials and a
  cleared environment.
- `vacuum_runs_before_the_checkpoint_and_never_deletes_untracked_parts`
  (`blocks/tests/engines/vacuum_check.py`): the §4.1 order (below), the
  job's first default run compacting exactly the closed date of every table
  with rows and a second run committing nothing, and a copy of a `blocks`
  data file that no log names (a published, uncommitted part): kept by a lite
  VACUUM with retention 0 and by a full VACUUM at 168 h; a full VACUUM with
  `VACUUM_RETENTION_HOURS=0` refused (exit 2) and the file kept; an unguarded
  full VACUUM (`deltalake` directly, retention 0, not enforced) would delete
  it; deleted by the job's full VACUUM once it is 8 days old; the committed
  rows unchanged.
- `the_job_refuses_unsafe_settings_and_never_prints_credentials`: nine
  settings refused before any request (no root, two roots, no tables, a path
  as a table, a full VACUUM below 168 h, an unknown flag, an unknown date
  scope, S3 without credentials, unsafe renames); a store error (a refused
  loopback connection) fails the table with exit 1 and never prints the
  secret.
- `the_job_pins_the_deltalake_that_ci_tests`.

## Measurements

**VACUUM, then checkpoint (§4.1), `deltalake` 1.6.6.** Three files
tombstoned by OPTIMIZE, a 2-second `delta.deletedFileRetentionDuration`,
3 seconds later:

| Order | Files deleted | Orphans left |
|---|---|---|
| checkpoint, then lite VACUUM from a fresh handle (the next run) | 0 | 3 |
| lite VACUUM, then checkpoint | 3 | 0 |
| the job | 3 | 0 |

A lite VACUUM on the same in-memory handle right after the checkpoint still
saw the tombstones (measured while probing), so the hazard is across runs,
which is what an hourly job does.

**The job beside a running `build`** (macOS arm64, debug binary, one local
run of the three test files):

| Store | Rounds | Date compactions | Of them, the open date beside the writer | Files compacted away | Writer failures | Errors, conflicts |
|---|---|---|---|---|---|---|
| Local disk | 100 | 108 | 108 | 252 | 0 | 0, 0 |
| Loopback S3 | 15 | 27 | 20 | 171 | 0 | 0, 0 |

The writer's commits rebased over the job's commits throughout (delta-rs logs
"table updated during transaction, checking for conflicts"). On local disk
the frequent `OPTIMIZE_DATES=all` rounds left each day with one file by the
time it closed, so the closed-date path ran in the S3 run and in the idempotency
check. Lite VACUUM with retention 0 deletes again, every run, the already
deleted files of tombstones that are not yet expired (design §1.10 item 13):
harmless, and it does not happen at the default retention.

## Spike coverage

| Spike check (`spikes/delta-lake/`, git history at `d79ce49`) | Covered by |
|---|---|
| `checked_casts_refuse_values_that_do_not_fit` | L2 `delta::types::tests::a_value_above_i64_max_in_a_long_column_refuses_the_flush`, `every_mapper_type_maps_onto_its_delta_type`, `conversions_keep_every_value_and_null` |
| `pre_written_parts_are_committed_byte_for_byte_with_a_txn` | L3 `delta::commit::tests::pre_written_parts_are_committed_byte_for_byte_with_a_txn_blocks_last` (local, in-memory, loopback S3) |
| `recovery_rolls_each_table_forward_exactly_once` | The delta-rs behavior it relied on: L3's `txn` read-back from a fresh handle (above) and same-`appId` refusal (below). fireparq's own roll-forward: L4's real-binary crash tests in `blocks/tests/delta_recovery.rs` |
| `stale_writers_rebase_on_blind_appends_but_not_on_their_own_app_id` | L3, same name |
| `concurrent_writers_serialize_through_conditional_puts` | L3, same name |
| `lite_vacuum_keeps_uncommitted_parts_and_full_vacuum_deletes_them` | L9 `vacuum_runs_before_the_checkpoint_and_never_deletes_untracked_parts`, on a real `build`'s table |
| `conditional_create_refuses_to_overwrite_a_part` | `writer::protected::tests::local_stage_and_publish_are_separate_durable_and_never_clobber`, `s3::upload::tests::conditional_native_put_keeps_existing_bytes_and_has_one_concurrent_winner` |
| `py/concurrent_maintenance.py` | L9 `maintenance_beside_a_local_build_keeps_exact_rows`, `maintenance_beside_an_s3_build_keeps_exact_rows` |
| `py/read_check.py` | L8 `engine_compat.rs`; anonymous S3: the opt-in deployment check |
| `--variant physical-date`, millisecond files | decided in §1.8 and §6; L2 writes neither, `engine_compat.rs` asserts both |
| `py/loopback_s3.py` (moto) | the Rust loopback S3 endpoint, now with DeleteObjects and real `Last-Modified` |
| `bench-load` | measurements (§1.9), not a test; L3's `the_log_tail_counts_commits_after_the_last_checkpoint` |

## Validation

- `cargo fmt --all` and `cargo test --workspace --locked` pass with
  `FIREPARQ_REQUIRE_DUCKDB=1` (DuckDB 1.5.5, `delta` `45c4087`) and
  `FIREPARQ_REQUIRE_POLARS=1` (Polars 1.44.2, `deltalake` 1.6.6): 982 passed,
  14 ignored, on origin/main `d79ce49` plus this change, and 941 passed, 13
  ignored after rebasing onto `bd02f6a` (#672, L5b/L7, which removed tests). Six tests are new:
  five in `delta_maintenance.rs` and the opt-in check in `engine_compat.rs`
  (skipped without its variables).
- The opt-in anonymous check was run once against a loopback moto 5.2.3
  server (plain HTTP on 127.0.0.1, a public-read bucket policy, a synthetic
  checkpointed two-day lake): both engines read it with unsigned requests, and
  both scanned 12 of the 20 `blocks` files for the closed day. It has not
  been run against RGW.
- The CronJob's install command was run locally (`pip install --only-binary
  --require-hashes --target` with Python 3.12, then `import deltalake`), and
  the manifest parses as three YAML documents. It was not applied to a
  cluster.
- The CI extension step's checksum and version were checked by downloading the
  `linux_amd64` build and reading its metadata footer (`45c4087`, `v1.5.5`);
  the `osx_arm64` build installed from a file loads with the same version.

## Limits

- The deployment check has not been run against RGW; the maintenance user's
  policy (design §5) is documented, not tested.
- If DuckDB republishes the 1.5.5 `delta` build, CI fails at the checksum
  until the pin is updated.
- Polars' pruning check reads the file count from its query plan text
  (Polars 1.44.2's `Parquet SCAN [...]`).
- The example CronJob downloads the wheels from PyPI on every run; an image
  with them preinstalled avoids that egress.
- The job maintains the tables it is told about (`LAKE_TABLES`); it does not
  discover them, because `deltalake` has no listing API and the job needs no
  other dependency.
- L4's `blocks/tests/delta_recovery.rs` runs its own `deltalake` helper
  (`tests/engines/delta_maintain.py`): its `full-vacuum` mode is the unsafe
  retention-0 full VACUUM that the reference job refuses, used there as fault
  injection, and its `compact` and `loop` modes report per-table counts that
  its assertions read. Moving those tests onto the script would mean
  rewriting them, so both stay.
