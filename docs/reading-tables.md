# Reading the tables

Read every table through its Delta log, never by globbing its files or
listing its directories. The log is the table's only file index: a glob or a
listing also finds the files that the [maintenance job](delta-maintenance.md)'s
OPTIMIZE replaced but VACUUM has not deleted yet (their rows twice), files of a
transaction that is not committed yet, and the log's own checkpoint Parquet
files. DuckDB (its
`delta` extension) and Polars read the `date` partition column from the log
and prune by it.

```sql
-- DuckDB 1.5.5, from a local dataset root
INSTALL delta; LOAD delta;
-- Rows, blocks and days of one table
SELECT count(*) AS rows, min(block_num) AS first_block, max(block_num) AS last_block,
       min(date) AS first_day, max(date) AS last_day
FROM delta_scan('output/mainnet/blocks');
-- Its schema, and the latest rows
DESCRIBE SELECT * FROM delta_scan('output/mainnet/blocks');
SELECT * FROM delta_scan('output/mainnet/blocks') ORDER BY block_num DESC LIMIT 20;
-- One day: pruned by the partition, then by the block_num statistics
SELECT count(*) FROM delta_scan('output/mainnet/blocks') WHERE date = DATE '2026-02-25';

-- From a bucket with anonymous public read: an empty key and secret send
-- unsigned requests. Set the endpoint and region of the S3-compatible service.
CREATE SECRET lake (TYPE s3, KEY_ID '', SECRET '', REGION 'us-east-1',
                    ENDPOINT 'storage.example.com', URL_STYLE 'path');
SELECT count(*) FROM delta_scan('s3://ethereum-mainnet/blocks')
WHERE date = DATE '2026-02-25';
```

```python
# Polars 1.44 with deltalake 1.6 (pip install polars deltalake), from a local dataset root
import datetime
import polars as pl

blocks = pl.scan_delta("output/mainnet/blocks")
day = blocks.filter(pl.col("date") == datetime.date(2026, 2, 25)).collect()

# From a bucket with anonymous public read (no credentials, unsigned
# requests); set the endpoint and region of the S3-compatible service
remote = pl.scan_delta(
    "s3://ethereum-mainnet/blocks",
    storage_options={
        "aws_endpoint_url": "https://storage.example.com",
        "aws_region": "us-east-1",
        "aws_skip_signature": "true",
    },
)
```

Use DuckDB 1.5 or later for S3: DuckDB 1.1's `delta` extension fails
anonymous reads once a table has a checkpoint, which the maintenance job
writes every hour.

The Python examples here are for Polars users; no repository code needs Python,
and CI does not run them. CI reads the same tables with DuckDB and through
delta-rs, which Polars' `scan_delta` uses
([engine compatibility](#engine-compatibility)).

A table's files, rows (each file's `numRecords`), bytes and days come from
its log alone, without reading any data file. This is what `fireparq scan`
reported before v1.0.0:

```python
# Table summary from the Delta log
import polars as pl
from deltalake import DeltaTable

table = DeltaTable("output/mainnet/blocks")
files = pl.DataFrame(table.get_add_actions(flatten=True))
summary = files.select(
    files=pl.len(),
    rows=pl.col("num_records").sum(),
    bytes=pl.col("size_bytes").sum(),
    first_day=pl.col("partition.date").min(),
    last_day=pl.col("partition.date").max(),
)
print(f"version {table.version()}", summary)
```

`fireparq validate <root>/blocks` checks block continuity over the same
snapshot of the log, and `fireparq inspect` reads the footer of one file.

## Consistent reads across tables

Each flush is one fireparq transaction, but its tables commit to their logs one
after another: every other table first, `blocks` last. A reader can therefore
see a flush in some tables before others, never in `blocks` before the rest.
That gives the **frontier rule**:

- The **frontier** is the newest `block_num` of `blocks`. Every block up to it
  has all of its rows in every table.
- Read `blocks` first, then read every other table up to the frontier. A table
  read after `blocks` holds at least every row up to the frontier; it may
  already hold rows of the next flush, which the bound leaves out.
- In a final-only dataset, a `date` is **closed** once `blocks` holds a later
  date: every table then has all of that day's rows, and only compaction
  rewrites its files. The maintenance job compacts closed dates only.

```sql
-- DuckDB: the frontier first, then the other tables up to it
SET VARIABLE frontier = (SELECT max(block_num) FROM delta_scan('s3://ethereum-mainnet/blocks'));
SELECT count(*) FROM delta_scan('s3://ethereum-mainnet/logs')
WHERE date >= DATE '2026-09-25' AND block_num <= getvariable('frontier');
```

```python
# Polars: collect the frontier before opening the other tables
import datetime
import polars as pl

frontier = pl.scan_delta("output/mainnet/blocks").select(pl.col("block_num").max()).collect().item()
logs = pl.scan_delta("output/mainnet/logs").filter(
    pl.col("date") >= datetime.date(2026, 9, 25), pl.col("block_num") <= frontier
)
```

For a final-only dataset this cut is the canonical chain up to the frontier.
Non-final output applies the same rule with `stream_ordinal` in place of
`block_num` (heights repeat in its event history): bound every other table by
the newest `stream_ordinal` of `blocks`, then apply the
[canonical live view](non-final-streams.md#canonical-live-view).

## Block range of a day

fireparq keeps no partition index (`partitions.parquet` and the `partitions`
subcommands were removed in #653): each table's log records every file's
`date` partition and `block_num` statistics, which answer the lookup. The
block range of one UTC day is:

```sql
-- DuckDB delta extension
SELECT min(block_num), max(block_num)
FROM delta_scan('<root>/blocks')
WHERE date = DATE '2026-09-25';
```

## Engine compatibility

DuckDB and Polars are the supported engines. CI builds real EVM (final and
non-final) and Solana output with a mock Firehose, writes a checkpoint of
every table, and reads every table through its Delta log
(`blocks/tests/engine_compat.rs`) with the DuckDB 1.5.5 CLI and its `delta`
extension `45c4087` (both checksum-verified), and with delta-rs itself
(`deltalake-core` 1.0.0: each snapshot's schema, active files and partition
pruning, then the rows of those files). Polars' `scan_delta` reads through
delta-rs the same way, so the delta-rs reads cover it; CI installs no Python,
and the Polars column below was last checked
in CI with Polars 1.44.2 and `deltalake` 1.6.6 (v1.0.1).
`blocks/tests/delta_maintenance.rs` reads the tables again after the
maintenance job compacted and vacuumed them beside a running `build`.

| Delta type written | DuckDB | Polars | Notes |
|---|---|---|---|
| `long` | `BIGINT` | `Int64` | The mapper's `UInt64` (checked: a value above `i64::MAX` refuses the flush), `UInt32` and `UInt16` |
| `short` | `SMALLINT` | `Int16` | The mapper's `UInt8` |
| `decimal(20,0)` | `DECIMAL(20,0)` | `Decimal(precision=20, scale=0)` | Currency amounts and unchecked 64-bit values, exact up to `u64::MAX` |
| `timestamp` | `TIMESTAMP WITH TIME ZONE` | `Datetime(time_unit='us', time_zone='UTC')` | Parquet `TIMESTAMP(MICROS, isAdjustedToUTC=true)` holding whole milliseconds |
| `date` (partition column) | `DATE` | `Date` | Filters on `date` read only that day's files |
| `string` enum labels | `VARCHAR` | `String` | Pages still dictionary-encoded |
| `array<T>` | `T[]`, for example `SMALLINT[]` | `List(T)`, for example `List(Int16)` | |
| `binary` | `BLOB` | `Binary` | |

- Both engines read `date` from the Delta log (`delta_scan`, `scan_delta`); the
  data files do not contain it.
- Non-final output adds `fork_step` (`VARCHAR` / `String`) and `stream_ordinal`
  (`BIGINT` / `Int64`).
- [`docs/schemas/`](schemas/README.md) lists every column's Delta type and
  each chain's mapping from the mapper's Arrow types.
- JVM engines (Spark, Trino) are not a target: the tables use reader
  version 1 with no table features, but CI does not test these engines.

To check anonymous reads of a deployment's public-read bucket (for example
Ceph RGW) with DuckDB and delta-rs, run the opt-in test against it. It sends
only unsigned requests, reads the newest closed day of `blocks` and one other
table, and compares the two readers' rows, block ranges and pruning:

```bash
FIREPARQ_RGW_ENDPOINT=https://storage.example.com FIREPARQ_RGW_BUCKET=ethereum-mainnet \
FIREPARQ_DUCKDB=/path/to/duckdb \
cargo test -p blocks --test engine_compat anonymous -- --nocapture
```

`FIREPARQ_RGW_PREFIX` names a dataset below the bucket root,
`FIREPARQ_RGW_REGION` the region (default `us-east-1`) and
`FIREPARQ_RGW_TABLE` the other table (default `transactions`). Without
`FIREPARQ_RGW_ENDPOINT` the test is skipped, as in CI.
