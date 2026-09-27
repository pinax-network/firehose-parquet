"""Read real fireparq output with Polars and report what it sees (#652, #643).

Run by `blocks/tests/engine_compat.rs` with the interpreter named by
`FIREPARQ_POLARS_PYTHON`; the Rust test makes the assertions. Usage:

    python polars_check.py '{"root": "...", "tables": ["blocks"], "minimums": {"blocks": ["nonce"]}, "day": "2023-11-15"}'

For each table it scans `<root>/<table>/**/*.parquet` twice: with Hive
partitioning (the documented read, where the `date=YYYY-MM-DD` directory is the
`date` column) and without (the columns stored in each file, which are Delta
data files without a `date` column), and prints one JSON object on stdout.
"""

import datetime
import json
import re
import sys

import polars as pl

DATE_DIRECTORY = re.compile(r"/date=(\d{4}-\d{2}-\d{2})/[^/]+$")


def table_report(root, table, day, minimums):
    glob = f"{root}/{table}/**/*.parquet"
    hive = pl.scan_parquet(glob, hive_partitioning=True, include_file_paths="__path")
    schema = hive.collect_schema()
    rows = hive.collect()
    on_day = hive.filter(pl.col("date") == day).collect()

    plain = pl.scan_parquet(glob, hive_partitioning=False)
    stored_columns = list(plain.collect_schema().names())

    mismatches = 0
    for partition_date, path in rows.select("date", "__path").iter_rows():
        directory = DATE_DIRECTORY.search(path)
        if directory is None or partition_date != datetime.date.fromisoformat(
            directory.group(1)
        ):
            mismatches += 1

    timestamps = rows.get_column("timestamp").drop_nulls()
    sample = {}
    for name, dtype in schema.items():
        if name == "__path":
            continue
        values = rows.get_column(name).drop_nulls()
        if values.len() == 0:
            continue
        value = values[0]
        if isinstance(value, pl.Series):
            value = value.to_list()
        if isinstance(value, bytes):
            value = value.hex()
        sample[name] = repr(value)
    return {
        "schema": {
            name: str(dtype) for name, dtype in schema.items() if name != "__path"
        },
        "stored_columns": stored_columns,
        "rows": rows.height,
        "rows_on_day": on_day.height,
        "days_on_day": sorted({str(value) for value in on_day.get_column("date")}),
        "paths_on_day": sorted(set(on_day.get_column("__path"))),
        "paths": sorted(set(rows.get_column("__path"))),
        "partition_mismatches": mismatches,
        "max_timestamp_ms": (
            None
            if timestamps.len() == 0
            else int(timestamps.dt.epoch("ms").max())
        ),
        "minimums": {name: str(rows.get_column(name).min()) for name in minimums},
        "sample": sample,
    }


def main():
    spec = json.loads(sys.argv[1])
    day = datetime.date.fromisoformat(spec["day"])
    minimums = spec.get("minimums", {})
    report = {
        "polars": pl.__version__,
        "tables": {
            table: table_report(spec["root"], table, day, minimums.get(table, []))
            for table in spec["tables"]
        },
    }
    json.dump(report, sys.stdout)


if __name__ == "__main__":
    main()
