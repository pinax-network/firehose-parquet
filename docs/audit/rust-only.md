# Rust-only repository: the maintenance job in Rust, Python removed

Refs #643, #680 (item 3), #678. PR: [#682](https://github.com/pinax-network/firehose-parquet/pull/682). Design:
[`docs/design/delta-lake.md`](../design/delta-lake.md) §9. Earlier records:
[#643 L8/L9](643-l8-l9-engines-maintenance.md) (the Python job and engine CI),
[#678](rgw-if-match-etag.md) (the RGW 19.2 `If-Match` quirk). Index: the
"After v1.0.0" table of the [audit index](README.md).

## Diagnosis

After v1.0.1 the repository still needed Python in three places:

- the Delta maintenance job, `scripts/delta_maintenance.py`, the
  off-the-shelf `deltalake` 1.6.6 package with hash-pinned requirements, and
  its image (`python:3.12-slim`);
- the engine tests: `blocks/tests/engines/delta_check.py` (Polars
  `scan_delta` and `deltalake` reads), `vacuum_check.py` (the §4.1 VACUUM
  rules), `delta_maintain.py` (OPTIMIZE and VACUUM between a crash and a
  restart) and their `requirements.txt`, installed by CI in a venv and
  required by `FIREPARQ_REQUIRE_POLARS`;
- 33 historical validation scripts in `docs/audit/*.py`.

On the riv-dev1 rollout the job also reported a table the writer had not
created yet as failed (`TableNotFoundError`, exit 1): two hourly runs failed
while the writer was stopped before any table existed (#680, item 3).

## Decisions (2026-09-28)

- All code in the repository is Rust; no Python dependency remains.
- The maintenance job becomes a separate Rust binary, `fireparq-maintenance`,
  in a new workspace crate (`maintenance/`), running delta-rs's off-the-shelf
  operations: OPTIMIZE (which needs `deltalake-core`'s `datafusion`
  feature), VACUUM, `create_checkpoint` and `cleanup_metadata`. No custom
  compaction logic. DataFusion is the version `deltalake-core` 1.0.0 is built
  on (55).
- The writer binary `fireparq` links no DataFusion.
- The `ghcr.io/pinax-network/firehose-parquet-maintenance` image keeps its name
  and runs the binary.
- Behavior and interface stay identical to the script; a table that does
  not exist yet is skipped, not failed.
- The historical `docs/audit/*.py` tooling is deleted; it stays in git history
  at [v1.0.1](https://github.com/pinax-network/firehose-parquet/tree/v1.0.1/docs/audit),
  and the records link it there.
- User-facing Polars examples stay in the README: users run Polars
  themselves. CI does not run them.

## Implementation

### The crate

`maintenance/` (package `fireparq-maintenance`):

- `src/lib.rs` (`fireparq_maintenance`): `Settings::from_env` reads and checks
  every setting before any request; `Lake` opens tables through delta-rs's
  default log store, on local disk or S3 (object_store 0.13 with
  `S3ConditionalPut::ETagMatch`, the same conditional-create commits as the
  writer, registered for `s3://` as in `firehose-parquet/src/delta/store.rs`);
  `run` maintains each table and writes the JSON lines.
- `src/main.rs`: the binary; process environment in, JSON lines on stdout,
  exit status out.

`deltalake-core` is pinned `=1.0.0` with `default-features = false` and
`features = ["rustls", "datafusion"]`, only in this crate.

### Parity with `scripts/delta_maintenance.py`

| Script | Binary |
|---|---|
| `LAKE_ROOT` or `LAKE_BUCKET` (exactly one), `LAKE_TABLES` (names `[A-Za-z0-9_]+`) | Same checks and messages |
| `S3_ENDPOINT`, `AWS_REGION` (`us-east-1`), `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (required on S3), `AWS_SESSION_TOKEN`, `AWS_ALLOW_HTTP`, `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` (both `false`) | Same; object_store's builder, no `from_env` |
| `SSL_CERT_FILE` through the TLS stack | Same (rustls-native-certs: only that file's CAs when set) |
| `AWS_S3_ALLOW_UNSAFE_RENAME` refused | Refused |
| `FULL_VACUUM`, refused below 168 h (setting or table retention) | Same |
| `VACUUM_RETENTION_HOURS`: a lite VACUUM may go below the table's retention (not enforced), a full one is always enforced | Same rule |
| `OPTIMIZE_DATES=closed\|all`, `OPTIMIZE_TARGET_SIZE`, `OPTIMIZE_ZSTD_LEVEL` (3, 1–22), `OPTIMIZE_MAX_CONCURRENT_TASKS`, `DRY_RUN` | Same |
| Open date: the newest `date` of `blocks` | Same, from the snapshot's active files |
| Per table: OPTIMIZE each closed date with more than one file; VACUUM; checkpoint only after a successful VACUUM; `cleanup_metadata` | Same order and calls (`optimize` with a `date` partition filter, `vacuum` lite or full, `checkpoints::create_checkpoint`, `checkpoints::cleanup_metadata`) |
| OPTIMIZE and VACUUM with `create_checkpoint=False`, `cleanup_expired_logs=False` | `CommitProperties` with both off |
| Output: `config_error`, `start`, `blocks_error`, `table`, `done`, keys sorted | Same shapes; new `skipped` line, `done.skipped`, `start.version` (`start.deltalake` is the `deltalake-core` version) |
| Exit 0, 1 (a table failed), 2 (configuration) | Same |
| Conflicts: every `CommitFailedError` (any delta-rs transaction error) | Only a lost race: `CommitConflict`, `MaxCommitAttempts`, `VersionAlreadyExists`; any other commit error (for example a refused PUT) fails the table instead of passing as a conflict |
| A missing table: `TableNotFoundError`, failed | Skipped (`NotATable`, which delta-rs returns for "No files in log segment", `InvalidTableLocation`, or the kernel's invalid local location); an existing table that fails to open still fails |
| Credentials redacted from every error | Same |
| Required exactly `deltalake` 1.6.6 at runtime | `deltalake-core =1.0.0` pinned at build time |

Requests keep object_store's default retries, as the Python package had. An
OPTIMIZE commit removes files, so a retry whose first attempt landed fails as
a conflict rather than committing twice; the writer keeps its single-attempt
log client.

### `If-Match` (#678)

Ceph RGW 19.2 compares `If-Match` literally with the unquoted ETag. The job
sends no `If-Match`: `deltalake-core` 1.0.0 commits with `PutMode::Create`
(`If-None-Match: *` under `ETagMatch`), writes checkpoints and
`_last_checkpoint` with plain PUTs, deletes with DELETE or DeleteObjects, and
has no `PutMode::Update` or `if_match` outside its test utilities; OPTIMIZE's
reads through DataFusion and the Parquet reader set none either. The S3 test
beside `build` measures it: the loopback server
(`blocks/examples/bench_live_flush/s3.rs`) now records each request's
`If-Match` and `User-Agent`, and no request from delta-rs's client
(`object_store/0.13.x`: the job, and the writer's log commits) carried
`If-Match`. The first version of the check, over every request to a table key,
failed: the writer's pinned readbacks of its own
parts (`object_store/0.12.x`) send `If-Match`, in the owner's ETag form since
#678, and are unchanged.

### DataFusion only in the job

Only `maintenance/Cargo.toml` enables `datafusion`, and nothing depends on
`maintenance/`: `cargo tree -p blocks` lists no DataFusion crate, dev
dependencies included, and CI checks it. The `blocks` tests read the tables
with `deltalake-core` and object_store 0.13 at exactly `firehose-parquet`'s
features, and run OPTIMIZE and the job as the binary.

Keeping DataFusion out of `blocks`' test graph matters because cargo unifies
features across everything one invocation builds. The first version of this
change ran the job in process from the `blocks` tests (a dev-dependency on the
maintenance library, with DataFusion scans for the reads). `cargo test`
then built the writer under test with DataFusion's features, among them
serde_json's `preserve_order`, and
`antelope::text::tests::streamed_json_preserves_previous_bytes_and_null_boundaries`
failed: its `json!` reference serialized keys in insertion order rather than
serde_json's default sorted order, which the streamed writer keeps. The
writer's own `json!` paths (Delta `add` stats, durable state) would have
changed the same way in tests only. So:

- the `blocks` tests run the binary as a separate process (found next to
  `fireparq` in `target/`, or at `FIREPARQ_MAINTENANCE`; required in CI by
  `FIREPARQ_REQUIRE_MAINTENANCE`);
- CI runs `cargo test -p fireparq-maintenance` (which builds the binary) and
  then `cargo test --workspace --exclude fireparq-maintenance`, so the writer
  is tested with exactly its release features;
- a local `cargo test --workspace` still unifies the two; the antelope test's
  reference now sorts its keys, so it passes either way;
- the release workflow builds `fireparq` and `fireparq-maintenance` in
  separate cargo invocations, and the `fireparq` image builds `--bin fireparq`
  alone. A release `fireparq` has no DataFusion symbol.

### Image

`deploy/maintenance/Dockerfile` builds the binary in a `rust` builder stage,
like the main `Dockerfile`, and runs it on `debian:bookworm-slim` with
`ca-certificates` as `USER 65534:65534`, with `fireparq-maintenance` as the
entrypoint. `docker-publish.yml` still publishes both images under their
names.

### CI

`.github/workflows/ci.yml` has no `setup-python`, venv,
`FIREPARQ_POLARS_PYTHON` or `FIREPARQ_REQUIRE_POLARS`. It keeps the
checksum-verified DuckDB CLI and its `delta` extension with
`FIREPARQ_REQUIRE_DUCKDB`, and adds three checks: no tracked `.py` or
`requirements*.txt` file, no `datafusion` crate in `cargo tree -p blocks`
(and one in `fireparq-maintenance`'s), and the two test runs above with
`FIREPARQ_REQUIRE_MAINTENANCE=1`.

## Tests

- `maintenance/tests/cli.rs` runs the real binary with a cleared environment:
  `configuration_errors_exit_2_before_any_request` (every refused setting,
  one `config_error` line), `store_errors_fail_the_table_and_never_print_credentials`
  (a refused loopback connection: exit 1, `blocks` failed, the secret absent
  from stdout and stderr), `missing_tables_are_skipped_and_other_open_errors_fail`.
- `blocks/tests/delta_maintenance.rs`, ported from the script to the binary
  (a cleared environment, beside a real `fireparq build`):
  `maintenance_beside_a_local_build_keeps_exact_rows` and
  `maintenance_beside_an_s3_build_keeps_exact_rows` (no writer failure, no
  error or conflict in any round, exact rows through DuckDB and delta-rs,
  `txn` intact, one file per closed date, OPTIMIZE between writer commits,
  every table skipped before the first build, no `If-Match` from delta-rs);
  `vacuum_runs_before_the_checkpoint_and_never_deletes_untracked_parts` (the
  §4.1 order and its reverse on hand-written scratch tables with a 4 s
  retention, untracked parts kept by a lite VACUUM and a 168 h full VACUUM,
  a retention-0 full VACUUM refused with exit 2, the weekly full VACUUM
  deleting an 8-day-old untracked part, idempotent reruns).
- `blocks/tests/delta_recovery.rs` compacts between crash and restart with
  the job (`OPTIMIZE_DATES=all`, `VACUUM_RETENTION_HOURS=0`: one OPTIMIZE
  commit per date, where `delta_maintain.py` made one for all dates), runs
  the unguarded full VACUUM of retention 0 that the job refuses with delta-rs
  in process, and runs the job in a loop beside `build` for #636.
- `blocks/tests/engine_compat.rs`, `delta_tables.rs` and `delta_readers.rs`
  read through delta-rs (each snapshot's Arrow schema, active files, `txn` and
  partition pruning, then the rows of those files) and the DuckDB CLI. The
  Polars assertions are dropped: Polars' `scan_delta` reads through delta-rs
  the same way, which these reads cover. The types checked are delta-rs's
  Arrow types (`Utf8`, `Decimal128(20, 0)`, `Timestamp(µs, "UTC")`, …).

## Removed

- `scripts/delta_maintenance.py`, `scripts/delta_maintenance.requirements.txt`;
- `blocks/tests/engines/` (`delta_check.py`, `delta_maintain.py`,
  `vacuum_check.py`, `requirements.txt`);
- the 33 `docs/audit/*.py` scripts (at the
  [v1.0.1 tag](https://github.com/pinax-network/firehose-parquet/tree/v1.0.1/docs/audit));
- CI's Python setup.

## Limits

- The README's Polars programs are no longer executed; the delta-rs reads cover
  what Polars reads through, not Polars' own type spellings.
- Opening a local lake whose root does not exist skips every table (exit 0)
  rather than failing, as a table the writer has not created yet does; the
  `skipped` lines name each one.
