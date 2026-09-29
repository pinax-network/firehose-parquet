# Plain-Parquet output and readers removed; `validate`, `scan` and `inspect` on Delta (#643, lanes L5b and L7)

Refs #643; part of #463. PR: [#672](https://github.com/pinax-network/firehose-parquet/pull/672). Design:
[`docs/design/delta-lake.md`](../design/delta-lake.md) §7.1, §7.3 and the L5
and L7 rows of §11. Index: the #643 rows of the [audit index](README.md).

## Diagnosis

Since L3 every `build` commits its parts to one Delta table per mapper table.
Two things still assumed plain Parquet:

- **Writers.** `writer.rs` kept the unprotected `OutputWriter` and
  `ParquetTableWriter::write_batch` / `new_s3`: a local and an S3 writer of
  standalone table files (`part-<uuid>-NNNNNN.parquet`) outside the protected
  transaction, never committed to a log. `build` did not use them; tests,
  examples and the protected path's routing did (`ParquetTableWriter::new`
  served as a routing and properties helper).
- **Readers.** `scan` and `validate` walked a table directory
  (`discovery::collect_local`, or a full S3 listing) and read every
  `.parquet` file below it. On a Delta table that is wrong twice: after
  OPTIMIZE and before VACUUM the replaced files are still on disk, so a walk
  reads their rows twice (as duplicates), and once the maintenance job writes
  checkpoints a walk reads `_delta_log/*.checkpoint.parquet` as data.

The v1.0.0 launch is Delta-only: no legacy code, no compatibility shims,
minimal configuration.

## Decision

- Every table file is a part of the protected transaction. The unprotected
  writers go; what the transaction shares with them becomes plain functions.
- No command reads a table by walking its directory. `validate` reads the
  active files of a pinned snapshot; `inspect` reads one file.
- **`scan` is removed rather than rewritten** (the smaller and clearer of the
  design's two options, §7.3). A log-based summary would have kept a
  subcommand, its flags, table discovery at a dataset root and its output
  format for what `deltalake` already prints from the log. DuckDB and Polars
  read rows, schemas and samples, and the README's
  [`deltalake` program](../reading-tables.md) gives a table's
  files, rows (`numRecords`), bytes and first and last day from the log alone.
  A test runs that README program against a real `build`, so the documented
  replacement cannot drift.

## Removed

**Writers (L5b).**

- `writer::OutputWriter` with `TableBuffer`, `WriterBufferStats`,
  `write_all`, `flush_remaining`, `flush_table`, `buffered_stats`,
  `set_metrics` and its per-table retry semantics (the #477 buffering).
- `writer::ParquetTableWriter`: `new`, `new_s3` (the unprotected S3 writer),
  `write_batch`, `s3_object_key`, `partition_dir`, `set_file_metadata` and its
  part counters and random process prefixes; `is_s3_output`, `short_uuid` and
  `with_root_cause_context`, which only its upload used.
- `writer/local.rs::write_parquet` and `TemporaryFile`, the atomic
  publication of one standalone file (#578), with the tests of its
  temporaries, partial writes, footers and crash child. The protected store
  keeps using the primitives that record introduced: durable directory
  creation, directory sync and no-replace hard-link publication, whose tests
  now call them directly.

**Readers (L7).**

- `scan` and all of its code: `ScanOrder`, `scan_parquet`, the local and S3
  collectors, row sampling and pagination, the table and vertical renderers
  and their JSON output; the flags `-n/--limit`, `--offset`, `--order`,
  `--schema-only`, `--vertical` and `--json`.
- The directory walk of `validate` (`validate_parquet`, `validate_parquet_local`,
  `validate_parquet_s3`, `detect_partition` from Hive path segments) and three
  checks that only a walk needed:
  - the file-to-file schema comparison (`compare_schemas`, `SchemaMismatch`):
    the log holds one schema, which `build` checks against the stream at every
    start;
  - the empty-partition warnings (`EmptyPartition`): a log lists no partition
    without files, and fireparq never commits an empty part;
  - the ordering count: blocks are sorted before they are checked, so it could
    not fire.
  `validate` also no longer accepts a `UInt64` `block_num` or `Int64`
  epoch-second timestamps, which no Delta table has.
- `maintenance::discovery::{collect_local, list_objects, relative_key}` and
  `artifacts::{is_reserved_artifact_path, read_walk_skips}`: with no command
  walking a table, nothing skips `_fireparq/` or a root `cursor.parquet` on
  the way.

## Changed

### Routing and encoding

`writer.rs` now holds functions: `partition_suffix` (the
`<table>/date=YYYY-MM-DD` directory of a flush), `validate_partition` (the
partition contract), `writer_properties`, `encode_into` (one in-memory part,
which `PreparedFlush::encode` now calls; the spooled S3 encoder is unchanged)
and `encode_parquet` / `decode_parquet`, which tests and the offline replay and
sizing examples use to round-trip rows exactly as `build` encodes them. The
in-memory encoder makes the same writer calls with the same properties as
before, and the spooled one still produces its bytes
(`native_spool_preserves_small_part_bytes_schema_rows_and_receipt`). The
replay examples write
`<case>/<table>.parquet` files instead of `ParquetTableWriter` directories.

### `validate`

`fireparq validate <table>` takes a Delta table, normally
`<dataset root>/blocks`: a local path, an `s3://` URI or an `S3_BUCKET` key.

1. A local path must be a table directory with a `_delta_log/`; otherwise the
   run stops with "is not a Delta table". An S3 table is opened through a
   read-only object_store 0.13 client (the log store's builder, default
   retries, unsigned requests without an access key, like fireparq's other
   read-only clients).
2. It opens the table at its latest version and pins it: the version is
   printed (`Validating blocks in <table> at version <N>`), and the file set
   is exactly that snapshot's active `add`s (`log_data()`), each with its
   `partitionValues.date`.
3. It reads each file whole through the table's object store and decodes only
   `block_num`, `block_id`, `parent_id` and `timestamp` (the #524 projection).
   An active file that is gone (a VACUUM removed it after the snapshot was
   read) fails the run with the file and version named, instead of being
   skipped.
4. The checks are the same as before: gaps (unless `--allow-gaps`),
   duplicates, parent-hash mismatches and timestamp reversals (warnings),
   per `date=` partition and over the whole table, and with
   `--cross-partition` between adjacent partitions. The exit code is 1 when a
   check fails.

### `inspect` and the remaining walker

`inspect` never walked: it reads one Parquet file (a data file, a Delta
checkpoint, the mirror). It now refuses a directory with a message that
points at the Delta readers.

Three walkers remain over dataset trees, and none reads table data:

- The protected-root discovery of `recovery recover` and of `build`'s overlap
  check (`ingest/maintenance.rs`: `collect_local_markers` locally,
  `discovery::visit_objects` on S3) looks for `.fireparq-ingest/` markers. It
  now skips every `_delta_log/` (`artifacts::DELTA_LOG_DIR`,
  `is_in_delta_log`): locally it no longer descends into a log, which saves
  one directory read per table and the listing of every commit and checkpoint
  in it; on S3 it ignores keys in a log.
- The symlink check of `recovery`'s local ownership
  (`dataset_lock/operation.rs`) must see every directory, logs included, so it
  is unchanged.
- Eligibility (`ingest/eligibility.rs`) must count a `_delta_log/` as data, so
  a new root with one is still refused.

## Tests

- `blocks/tests/delta_readers.rs`, the real binary against a cursor-aware mock
  Firehose (four final EVM blocks over two days, one transaction per block):
  - `validate_reads_a_pinned_snapshot_through_optimize_and_checkpoints`
    (local): `validate` reads version 4 (four files, blocks 100–103, two
    partitions, no finding, `--cross-partition` clean); a simulated OPTIMIZE
    of the closed day (a commit that removes its two parts with
    `dataChange: false`, leaving them on disk, and adds their rows rewritten as
    one file, design §7.3) makes a directory walk read six block rows, and
    `validate` reads version 5 with three files and the same result; with the
    Python in `FIREPARQ_POLARS_PYTHON`, a real `deltalake` 1.6.6 OPTIMIZE of
    the other day and a checkpoint (`00000000000000000006.checkpoint.parquet`
    in `_delta_log/`) give version 6, two files and the same result,
    `inspect` reads the checkpoint, and the README's summary program prints
    version 6, 2 files, 4 rows, the active files' bytes and both days. An
    added copy of an active file then fails `validate` with two duplicate
    blocks and exit code 1; deleting that file fails the run as a missing
    active file; the dataset root is not a Delta table; `inspect` refuses the
    table directory and reads a data file.
  - `validate_reads_an_s3_table_through_its_log` (loopback HTTPS S3): the
    same table built on S3 validates at version 4, the run GETs exactly the
    four active parts once each and nothing else below `date=`, and a missing
    table is refused.
- CLI: `scan` is an unknown subcommand, the root help no longer lists it and
  names `delta_scan`, the `validate` and `inspect` help describe the snapshot
  and the single file (`blocks/src/bin/main.rs`,
  `firehose-parquet/src/cli/tests.rs`); a local directory without a log is
  refused before any S3 fallback.
- `validate` unit tests now feed encoded data files (Delta `long`
  `block_num`, any timestamp unit) through the same decoder: reversals in
  every unit and within a second, nulls against the last known time, per
  partition, and the refusal of a non-timestamp `timestamp`
  (`firehose-parquet/src/cli/tests.rs`, `cli/validate_tests.rs`).
- `writer.rs` keeps its partition-contract tests on the functions, and
  `writer/local.rs` tests durable directories (retry, symlinked ancestry) and
  no-replace publication (never overwritten, one concurrent winner) directly.
- `blocks/tests/ingestion_transactions.rs` no longer runs `scan`; its
  `validate` of the blocks table reads three active files.

## Validation

- `cargo fmt --all`; `cargo build --workspace --all-targets` has no compiler
  warnings (the example `bench_live_flush`'s unused `Server::objects`, a
  warning since L3, is allowed with a reason). On macOS the linker reports the
  size of the debug binary's unwind table, which is not a compiler warning and
  does not occur on Linux.
- `cargo test --workspace --locked` with `FIREPARQ_REQUIRE_DUCKDB=1` (DuckDB
  1.1.1) and `FIREPARQ_REQUIRE_POLARS=1` (Polars 1.44.2, `deltalake` 1.6.6):
  935 passed, 0 failed, 13 ignored, on origin/main `d79ce49` (after L3) plus
  this change;
  `cargo test -p blocks --example refresh_evm_golden --locked` passes.
- No real endpoint or bucket was used: tests run against loopback mock
  Firehose servers and the loopback S3 endpoint, in temporary directories with
  a cleared environment.

## Limits

- Anonymous S3 reads by `validate` (no access key) are not exercised: the
  loopback endpoint requires signed requests.
- `validate` reads each active file whole, one at a time, as the S3 path did
  before; a compacted day of about 256 MiB (`delta.targetFileSize`) is held in
  memory while its four columns are decoded.
- README examples outside the reader sections (the chain sections and the
  non-final live view, which `blocks/tests/non_final_stream.rs` runs) still
  glob data files; moving them to `delta_scan` is L10's, and the engine
  compatibility test is L8's.
