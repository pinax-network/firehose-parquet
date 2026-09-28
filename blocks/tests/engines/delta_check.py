"""Read fireparq's Delta tables with Polars `scan_delta` and report what it sees (#643).

Run by `blocks/tests/delta_tables.rs` with the interpreter named by
`FIREPARQ_POLARS_PYTHON` (Polars and `deltalake`, pinned in
`requirements.txt`); the Rust test makes the assertions. Usage:

    python delta_check.py '{"root": "...", "tables": ["blocks"], "day": "2023-11-15", "decimals": {"blocks": ["nonce"]}}'

For each table it reads `<root>/<table>` through its Delta log only (never a
directory listing) and prints one JSON object on stdout: the schema, the row
count, the rows of `day` (a partition filter), the sorted block numbers, the
Delta table version, and the minimum of each listed decimal column as text.
"""

import datetime
import json
import sys

import deltalake
import polars as pl


def table_report(root, table, day, decimals):
    uri = f"{root}/{table}"
    scan = pl.scan_delta(uri)
    schema = scan.collect_schema()
    rows = scan.collect()
    on_day = scan.filter(pl.col("date") == day).select(pl.len()).collect().item()
    return {
        "schema": {name: str(dtype) for name, dtype in schema.items()},
        "rows": rows.height,
        "rows_on_day": on_day,
        "block_nums": sorted(rows.get_column("block_num").to_list()),
        "version": deltalake.DeltaTable(uri).version(),
        "minimums": {
            name: str(rows.get_column(name).min()) for name in decimals.get(table, [])
        },
    }


def main():
    spec = json.loads(sys.argv[1])
    day = datetime.date.fromisoformat(spec["day"])
    report = {
        "polars": pl.__version__,
        "deltalake": deltalake.__version__,
        "tables": {
            table: table_report(spec["root"], table, day, spec.get("decimals", {}))
            for table in spec["tables"]
        },
    }
    json.dump(report, sys.stdout)


if __name__ == "__main__":
    main()
