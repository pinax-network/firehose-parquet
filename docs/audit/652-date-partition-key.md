# One `date=YYYY-MM-DD` partition key (#652)

PR: [#660](https://github.com/pinax-network/firehose-parquet/pull/660).

## Diagnosis

Time-partitioned output used three Hive keys, `year=YYYY/month=MM/day=DD/`,
plus `hour=`, `minute=` and `second=` for the finer modes, and `build` also
wrote `block_range=` and unpartitioned layouts. `day=` existed only because the
v0.x key `date=DD` held the day of the month and collided with the `date` data
column (#493): DuckDB read the day number over the column and Polars failed to
load it. Engines and table formats prefer one date key, so a reader filters
`date = DATE '2026-09-25'` instead of `year = 2026 AND month = 9 AND day = 25`,
and the planned Delta mode (#643) partitions by `date`.

Engine facts from the issue, checked on 2026-09-27 with DuckDB 1.5.5 and 1.1.1,
Polars 1.44.2 and PyArrow 25.0.1 on files that also contain `date`:

- DuckDB (`hive_partitioning = true`) and Polars (`hive_partitioning=True`) read
  `date=YYYY-MM-DD` as a date and prune by `WHERE date = ...`. When a file's
  `date` disagrees with its directory, both take the directory.
- PyArrow `partitioning="hive"` infers a string key and fails to merge it with
  the column; it works with
  `ds.partitioning(pa.schema([("date", pa.date32())]), flavor="hive")`.
- JVM engines (Spark) reject a partition column that also appears in the data
  files and read `UInt64` as `DECIMAL(20,0)`. They are not a target for the
  plain Parquet layout; the Delta mode covers them.

## Scope decision

The first implementation replaced the multi-key layout with
`date=YYYY-MM-DD/[hour=HH/[minute=MM/[second=SS/]]]` and handled the older
layouts explicitly. On 2026-09-27 the user narrowed the scope, relayed by the
coordinator:

- keep only `date`: `build` has no `--partition` flag, and every table is
  always `<table>/date=YYYY-MM-DD/part-*.parquet`. The `none`, `hour`,
  `minute`, `second` and `block_range` modes and their code, tests and docs are
  removed, including finer-key parsing and `block_range=` handling in merge,
  verify, truncate and validate;
- remove `rollup` entirely, with no compatibility path: its only purpose was
  coarsening partitions;
- keep no legacy practice for backward compatibility (fireparq is a proof of
  concept and v1.x may break things), but make a protected build refuse,
  before Blocks, a root that holds old-layout partitions;
- leave the `partitions` subcommands and their own `--partition` index flag
  alone (removed in v1.1.0 by #653), making sure nothing in them depends on the
  removed output modes.

## Implementation

**One module.** `firehose-parquet/src/date_partition.rs` owns the key.
`DatePartition::from_timestamp` validates the whole-second block time
(`checked_timestamp`, and years before 0 have no key), `path` formats
`date=YYYY-MM-DD`, `parse` accepts exactly that shape (a v0.x `date=25`, a
`day=`/`year=` key, `block_range=` or a nested `hour=` never parse), `date32`
gives the Arrow `Date32` value, and `is_date_value_pattern` checks `truncate`
filter globs. Every consumer goes through it: the writer's partition directory
and contract, protected ingestion routing (the runtime's date-boundary flush),
`PendingTransaction` validation used by recovery, and `truncate`.
`config::Partition`, `Config::partition` and `cli::parse_partition` are gone;
`ParquetTableWriter` and `OutputWriter` no longer take a partition.

**Writer contract.** `ParquetTableWriter::partition_suffix` returns
`<table>/date=YYYY-MM-DD` from the flush's minimum routing time and refuses a
flush without one; the old flat-table fallback for time-less flushes is
removed (protected Solana ingestion always has a routing anchor, other chains
require a block time). `validate_partition` keeps the #477 checks (every
non-null row timestamp and both metadata endpoints select the same directory)
and adds one: when the table has a `date` column it must be `Date32`, and every
non-null value must equal the directory's day. The column and the directory are
derived from the same checked time, so this holds by construction; the check
makes a Hive read (directory) and a plain read (column) provably identical.

**Protected identity.** `MAPPER_EPOCH` is now `fireparq-mapping-v2`.
`PartitionPolicy` has one variant, `Date`, still serialized as
`{"kind":"date"}`, so the descriptor keeps recording the layout exactly; the
epoch fixes what that name means. The bump is needed because a pre-release
descriptor's `{"kind":"date"}` meant `year=/month=/day=`: without it an old
protected root would resume and append `date=` partitions beside the old ones.
The other old policies (`none`, `block_range`, `hour`, `minute`, `second`) no
longer deserialize. `RoutingPolicy::DirectV1` existed only for Solana
`block_range`/`none` streams and is removed. A layout field on the descriptor
was prototyped and dropped with the narrower scope: once every older layout is
unsupported, the documented rule (row or routing changes advance
`MAPPER_EPOCH`) refuses all of them with one check. `StreamDescriptor::validate`
names the epoch and the layout in its refusal ("this protected dataset was
created with semantic mapper epoch `fireparq-mapping-v1`; this fireparq writes
`fireparq-mapping-v2`, where every table is partitioned as
<table>/date=YYYY-MM-DD/ (#652). It cannot append to or maintain it: build into
a new, empty output root"). Every path that reads authority validates it, so
`build` refuses before any Blocks request (`load_authoritative_resume` and
`IngestionSession::open`), and maintenance discovery and `verify` refuse too.

**Recovery.** Recovery republishes only recorded paths.
`PendingTransaction::validate` now requires each table with rows to record one
`date=YYYY-MM-DD` directory and each zero-row table to record none, so a pending
transaction of another layout is refused instead of replayed.

**Maintenance.** `rollup` is deleted with its engine, `_fireparq_rollup.json`
journal, `firehose-parquet.rollup_copy` footer marker (and value-metadata key),
`FIREPARQ_TEST_ROLLUP_CRASH_AT`, merge's "waiting for an interrupted rollup"
skip, verify's rollup-journal refusal and journal-ancestor walk, and the
protected-root rollup guards. `merge` compacts within a partition directory and
never parsed keys; it is unchanged apart from those removals. `truncate -p`
accepts only `date=` filters: a `YYYY-MM-DD` date or one `*` glob whose literal
parts fit that shape and whose literal prefix spells a whole year
(`date=2026-01-*`, `date=2026-*`, `date=*-15`). Repeated filters match either.
Other keys, path filters, and values such as `date=15` or `date=1*` are refused
with a message instead of matching nothing. A file matches when a directory in
its path parses as a date partition whose value matches.

**Other surfaces.** Parquet footers keep `firehose-parquet.partition=date` and
no longer write `firehose-parquet.block_range_size`; `firehose_parquet_info`
drops its `partition` label; `.env.example` loses `PARTITION` and
`BLOCK_RANGE_SIZE`; the schema reference conventions describe the key.

## Older layouts, per command

No compatibility code is kept. What happens to data of an older layout
(`year=/month=/day=`, v0.x `year=/month=/date=DD`, `block_range=`, `hour=`, or
flat files):

| Command | Behavior |
|---|---|
| `build`, root without authority | Refused before any Blocks request: only an empty root initializes ("output contains legacy data ..."). Real-binary test `roots_with_older_partition_layouts_are_refused_before_blocks`. |
| `build`, pre-release protected root | Refused before any Blocks request by the epoch check, which names the layout. Library test `roots_in_an_older_partition_layout_are_refused_before_any_stream`. |
| `merge` | Layout-agnostic compaction of one directory's parts, as before. A pre-release protected root is refused at maintenance discovery (epoch). |
| `truncate` | `-p` takes only `date=` filters, so old keys are refused; `truncate <path> --yes` without filters deletes any tree. Protected roots are refused as before. |
| `verify` | A pre-release protected root is refused (epoch; test `a_pre_release_protected_root_is_refused_with_its_epoch`). Unprotected trees are read as plain Hive paths; the registry key is the verbatim path. |
| `validate`, `scan`, `inspect` | Read-only and layout-agnostic, as before. |
| `recovery` | Pre-release protected roots fail on their authority (epoch); a pending transaction of another layout fails validation. |
| `rollup` | Removed. |

## Verify registry impact

The registry `partition` column and report `findings[].partition` hold the Hive
path of a file's partition directories, so `build` output is keyed
`date=YYYY-MM-DD`. The key is not a `merkle_v2` input: leaves hash the rows
alone (`docs/verifiability-hash-strategy.md`), so the same rows give the same
root under either layout. Only keys change, and a registry built on an old
layout simply does not match the new keys (they are reported as new
partitions). `open` partitions lose the `block_range` exception: after a
completed request, the day that holds the last block stays open, because a
longer request appends to it; earlier days are compared. Report schema and
`merkle_version` are unchanged.

## Engine compatibility test

`blocks/tests/engine_compat.rs` writes real output with the `fireparq` binary
(`fireparq build` against a mock Firehose, `--flush-blocks 1`, blocks on both
sides of 2023-11-15T00:00:00Z) and reads it with the DuckDB CLI and with
Polars:

| Dataset | Tables checked | Column types exercised |
|---|---|---|
| EVM final | `blocks`, `transactions`, `logs`, `access_lists` | UInt64, UInt32, Dictionary, List<Utf8>, ms timestamps with 250 ms |
| EVM non-final (NEW, UNDO and a replacing NEW) | same | plus `fork_step` and `stream_ordinal` |
| Solana final | `transactions`, `instructions`, `rewards` | List<UInt64>, List<UInt8>, Binary, Dictionary |

For every table, both engines must read `date` as a date (`DATE` / `Date`), a
`date = '2023-11-15'` filter must return exactly that day's rows and files, the
stored `date` column (read without Hive partitioning) must equal each file's
directory, unsigned columns must stay unsigned (`UBIGINT`, `UINTEGER`,
`UTINYINT[]` / `UInt64`, `UInt32`, `List(UInt8)`), `timestamp` must be a UTC
millisecond timestamp (DuckDB: Parquet `TIMESTAMP_MILLIS`, UTC; Polars:
`Datetime(time_unit='ms', time_zone='UTC')`, sub-second values kept),
dictionary columns load (`VARCHAR` / `Categorical`), list and binary columns
load, no path under `_fireparq/` or a dot-prefixed directory is read although
`_fireparq/cursor.parquet` and `.fireparq-ingest/` exist, `stream_ordinal` is
present exactly in non-final output, and each engine's row count equals the
files' and the other engine's. There is no `hour` key left to check.

The DuckDB CLI comes from `FIREPARQ_DUCKDB` (else `duckdb` on `PATH`) and Polars
from the interpreter in `FIREPARQ_POLARS_PYTHON`, running
`blocks/tests/engines/polars_check.py`. `FIREPARQ_REQUIRE_DUCKDB` and
`FIREPARQ_REQUIRE_POLARS` turn a missing engine into a failure; CI sets both.
Locally a missing engine is skipped with a message. CI pins DuckDB 1.1.1 (the
existing checksum-verified CLI) and installs Polars 1.44.2 (with
`polars-runtime-32` 1.44.2) into a venv on Python 3.12 from
`blocks/tests/engines/requirements.txt` with `pip install --only-binary=:all:
--require-hashes`. PyArrow is not installed: Polars reads the files natively.

## Validation

Commands, from the worktree, with a local Polars venv built from the pinned
requirements (`uv venv --python 3.12`, then `pip install --require-hashes`):

```
cargo fmt --all
FIREPARQ_REQUIRE_DUCKDB=1 FIREPARQ_DUCKDB=/opt/homebrew/bin/duckdb \
FIREPARQ_REQUIRE_POLARS=1 FIREPARQ_POLARS_PYTHON=<venv>/bin/python \
  cargo test --workspace --locked
cargo run -p blocks --example dump_schemas   # only docs/schemas/README.md changed
```

- Workspace tests: 1,231 passed, 0 failed, 15 ignored; the engine test read all
  three datasets with DuckDB 1.1.1 and Polars 1.44.2.
- The engine test skips Polars without `FIREPARQ_POLARS_PYTHON` and fails with
  `FIREPARQ_REQUIRE_POLARS=1` when it is missing.
- Polars 1.44.2 sends unsigned S3 requests with
  `storage_options={"aws_skip_signature": "true"}` (checked against a local
  listener), which the README's anonymous-bucket example uses.
- No real endpoint or bucket was used: every build ran against a loopback mock
  Firehose into temporary directories with a cleared environment.

## Limits

- Protected datasets created by pre-release builds, including the live and
  final demo buckets, cannot be resumed or maintained; rebuild them into new
  roots.
- `partitions build` still writes `hour`, `minute`, `second` and `block_range`
  indexes; those index modes do not describe an output layout and go with #653.
- JVM engines remain unsupported for the plain layout until the Delta mode
  (#643).
