"""Read fireparq's Delta tables with Polars `scan_delta` and `deltalake`, and
report what they see (#643).

Run by the Rust tests (`blocks/tests/engine_compat.rs`, `delta_tables.rs`,
`delta_maintenance.rs`) with the interpreter named by `FIREPARQ_POLARS_PYTHON`
(Polars and `deltalake`, pinned in `requirements.txt`); the Rust tests make the
assertions. Usage:

    python delta_check.py '{"root": "...", "tables": ["blocks"], ...}'

Spec keys: `root` (a local dataset root or `s3://bucket[/prefix]`), `tables`,
and optionally:

- `day` (`YYYY-MM-DD`): also read with a `date` partition filter;
- `day_only`: compute nothing over the whole table, only over `day` (for large
  deployed tables);
- `minimums` (`{table: [column, ...]}`): minimums as text;
- `storage_options`: for S3, passed to both `deltalake` and Polars;
- `checkpoint`: write a checkpoint of every table first, so `_delta_log/`
  holds Parquet that no read may pick up;
- `stored_columns`: the columns of one data file (local tables only);
- `history`: the operation of every retained commit;
- `closed_day`: also report `closed_day`, the newest `date` of `blocks` that
  is not its last one (from the log alone).

Every read goes through the table's Delta log, never a directory listing. It
prints one JSON object on stdout with, per table: the schema, the table
version, row counts and block numbers, the files Polars scans with and without
the `day` filter (from the query plan), the fireparq `txn` version and the
active files per `date`.
"""

import datetime
import json
import re
import sys

import deltalake
import polars as pl

# "Parquet SCAN [first, ... N other sources]" or "Parquet SCAN [a, b]".
SCAN = re.compile(r"Parquet SCAN \[(.*?)\]$", re.MULTILINE)
OTHERS = re.compile(r"^(.*), \.\.\. (\d+) other sources?$")


def scanned(lazy):
    """(number of files, first file) that the plan of `lazy` scans."""
    match = SCAN.search(lazy.explain())
    if match is None:
        return 0, None
    sources = match.group(1)
    others = OTHERS.match(sources)
    if others:
        return int(others.group(2)) + 1, others.group(1)
    files = [source for source in sources.split(", ") if source]
    return len(files), files[0] if files else None


def relative(uri, path):
    return None if path is None else path.removeprefix(uri).removeprefix("/")


def stats(lazy, prefix, uri):
    """Row and block statistics of `lazy`, with keys prefixed by `prefix`."""
    row = lazy.select(
        pl.len().alias("rows"),
        pl.col("block_num").n_unique().alias("distinct_blocks"),
        pl.col("block_num").min().alias("min_block"),
        pl.col("block_num").max().alias("max_block"),
        pl.col("timestamp").dt.epoch("ms").max().alias("max_timestamp_ms"),
        pl.col("date").unique().sort().cast(pl.String).implode().alias("dates"),
    ).collect().row(0, named=True)
    per_block = (
        lazy.group_by("block_num").len().select(pl.col("len").unique().sort()).collect()
    )
    files, first = scanned(lazy)
    row |= {
        "rows_per_block": per_block.get_column("len").to_list(),
        "scan_files": files,
        "first_scan_file": relative(uri, first),
    }
    return {f"{prefix}{key}": value for key, value in row.items()}


def table_report(spec, table, day):
    storage = spec.get("storage_options") or None
    uri = f"{spec['root'].rstrip('/')}/{table}"
    dt = deltalake.DeltaTable(uri, storage_options=storage)
    if spec.get("checkpoint"):
        dt.create_checkpoint()
        dt = deltalake.DeltaTable(uri, storage_options=storage)
    scan = pl.scan_delta(uri, storage_options=storage)
    report = {
        "schema": {name: str(dtype) for name, dtype in scan.collect_schema().items()},
        "version": dt.version(),
    }
    if not spec.get("day_only"):
        report |= stats(scan, "", uri)
        report["minimums"] = {
            name: str(value)
            for name, value in scan.select(
                pl.col(name).min() for name in spec.get("minimums", {}).get(table, [])
            )
            .collect()
            .row(0, named=True)
            .items()
        } if spec.get("minimums", {}).get(table) else {}
        adds = dt.get_add_actions(flatten=True)
        dates = [str(value) for value in adds.column("partition.date").to_pylist()]
        report["active_files_per_date"] = {
            date: dates.count(date) for date in sorted(set(dates))
        }
        paths = adds.column("path").to_pylist()
        if spec.get("stored_columns") and paths and "://" not in spec["root"]:
            report["stored_columns"] = list(pl.read_parquet_schema(f"{uri}/{paths[0]}"))
    if day is not None:
        report |= stats(scan.filter(pl.col("date") == day), "day_", uri)
    descriptor = dt.metadata().configuration.get("fireparq.descriptor")
    report["txn"] = (
        None if descriptor is None else dt.transaction_version(f"fireparq:{descriptor}")
    )
    if spec.get("history"):
        report["operations"] = sorted(
            [entry["version"], entry["operation"]] for entry in dt.history()
        )
    return report


def main():
    spec = json.loads(sys.argv[1])
    day = spec.get("day")
    day = None if day is None else datetime.date.fromisoformat(day)
    report = {
        "polars": pl.__version__,
        "deltalake": deltalake.__version__,
        "tables": {table: table_report(spec, table, day) for table in spec["tables"]},
    }
    if spec.get("closed_day"):
        blocks = deltalake.DeltaTable(
            f"{spec['root'].rstrip('/')}/blocks",
            storage_options=spec.get("storage_options") or None,
        )
        dates = sorted({partition["date"] for partition in blocks.partitions()})
        report["closed_day"] = dates[-2] if len(dates) > 1 else None
    json.dump(report, sys.stdout, default=str)


if __name__ == "__main__":
    main()
