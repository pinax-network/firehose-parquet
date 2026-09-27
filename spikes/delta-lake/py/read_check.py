#!/usr/bin/env python3
"""Read the spike's Delta tables with Polars `scan_delta` and the DuckDB CLI.

Usage:
  read_check.py --lake <dir|s3://bucket/prefix> --transactions N --blocks B \
      [--duckdb /path/to/duckdb ...] [--extension-dir DIR] [--s3-endpoint URL]

Checks the exact values of the fixture written by `delta-lake-spike write`
(ordinals 1..N, B blocks per transaction, 3 transactions per block): row
counts, Delta types, u64 values above i64::MAX in Decimal(20,0) columns,
UInt8 values above 127 in Int16 lists, microsecond UTC timestamps, lists,
binary payloads, and the `date` partition column. An S3 lake is read
anonymously (unsigned requests), as the public RGW buckets are.
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal

import polars as pl

U64_MAX = 2**64 - 1
DATE_MS = 20_721 * 86_400_000  # 2026-09-25, the writer's default --date


def expected(n: int, b: int) -> dict:
    blocks = n * b
    txs = blocks * 3
    return {
        "blocks.rows": blocks,
        "blocks.sum_block_num": blocks * (blocks - 1) // 2,
        "blocks.max_nonce": str(U64_MAX),
        "blocks.min_nonce": str(U64_MAX - (blocks - 1)),
        "blocks.min_ts_ms": DATE_MS,
        "blocks.max_ts_ms": DATE_MS + (blocks - 1) * 250,
        "blocks.dates": 1,
        "transactions.rows": txs,
        "transactions.max_amount": str(U64_MAX),
        "transactions.max_balance": str(U64_MAX),
        "transactions.sum_account_255": 255 * txs,
        "transactions.failed": blocks,
        "transactions.min_data_hex": "DEAD0000",
    }


def table_uri(lake: str, table: str) -> str:
    return f"{lake.rstrip('/')}/{table}"


def polars_results(lake: str, storage: dict) -> dict:
    opts = {"storage_options": storage} if storage else {}
    blocks = pl.scan_delta(table_uri(lake, "blocks"), **opts)
    txs = pl.scan_delta(table_uri(lake, "transactions"), **opts)
    schema_b = blocks.collect_schema()
    schema_t = txs.collect_schema()
    types = {
        "block_num": str(schema_b["block_num"]),
        "nonce": str(schema_b["nonce"]),
        "timestamp": str(schema_b["timestamp"]),
        "date": str(schema_b["date"]),
        "detail_level": str(schema_b["detail_level"]),
        "pre_balances": str(schema_t["pre_balances"]),
        "accounts": str(schema_t["accounts"]),
        "topics": str(schema_t["topics"]),
        "data": str(schema_t["data"]),
    }
    b = blocks.select(
        pl.len().alias("rows"),
        pl.col("block_num").sum().alias("sum"),
        pl.col("nonce").max().alias("max_nonce"),
        pl.col("nonce").min().alias("min_nonce"),
        pl.col("timestamp").min().alias("min_ts"),
        pl.col("timestamp").max().alias("max_ts"),
        pl.col("date").n_unique().alias("dates"),
    ).collect().row(0, named=True)
    t = txs.select(
        pl.len().alias("rows"),
        pl.col("amount").max().alias("max_amount"),
        pl.col("pre_balances").list.get(0).max().alias("max_balance"),
        pl.col("accounts").list.get(1).cast(pl.Int64).sum().alias("acc"),
        (pl.col("status") == "FAILED").sum().alias("failed"),
        pl.col("data").min().alias("min_data"),
    ).collect().row(0, named=True)
    pruned = blocks.filter(pl.col("block_num") < 5).select(pl.len()).collect().item()
    to_ms = lambda ts: int(ts.replace(tzinfo=ts.tzinfo or timezone.utc).timestamp() * 1000)
    return {
        "types": types,
        "blocks.rows": b["rows"],
        "blocks.sum_block_num": int(b["sum"]),
        "blocks.max_nonce": str(Decimal(b["max_nonce"])),
        "blocks.min_nonce": str(Decimal(b["min_nonce"])),
        "blocks.min_ts_ms": to_ms(b["min_ts"]),
        "blocks.max_ts_ms": to_ms(b["max_ts"]),
        "blocks.dates": b["dates"],
        "transactions.rows": t["rows"],
        "transactions.max_amount": str(Decimal(t["max_amount"])),
        "transactions.max_balance": str(Decimal(t["max_balance"])),
        "transactions.sum_account_255": int(t["acc"]),
        "transactions.failed": int(t["failed"]),
        "transactions.min_data_hex": bytes(t["min_data"]).hex().upper(),
        "filtered_rows": pruned,
    }


def duckdb_results(
    duckdb: str, lake: str, ext_dir: str, s3_endpoint: str | None, signed: bool
) -> dict:
    setup = [f"SET extension_directory='{ext_dir}';", "INSTALL delta;", "LOAD delta;"]
    if s3_endpoint:
        host = s3_endpoint.split("://", 1)[1]
        # Anonymous reads use an empty key and secret (DuckDB 1.5.5 then sends
        # unsigned requests); `signed` uses placeholder loopback credentials.
        key, secret = ("spike", "spike") if signed else ("", "")
        setup.append(
            f"CREATE SECRET spike (TYPE s3, KEY_ID '{key}', SECRET '{secret}', REGION 'us-east-1', "
            f"ENDPOINT '{host}', URL_STYLE 'path', USE_SSL false);"
        )
    b = table_uri(lake, "blocks")
    t = table_uri(lake, "transactions")
    sql = "\n".join(
        setup
        + [
            f"""SELECT count(*) AS rows, sum(block_num)::VARCHAR AS sum,
       max(nonce)::VARCHAR AS max_nonce, min(nonce)::VARCHAR AS min_nonce,
       epoch_ms(min(timestamp)) AS min_ts, epoch_ms(max(timestamp)) AS max_ts,
       count(DISTINCT date) AS dates,
       typeof(any_value(block_num)) AS t_block_num, typeof(any_value(nonce)) AS t_nonce,
       typeof(any_value(timestamp)) AS t_timestamp, typeof(any_value(date)) AS t_date
FROM delta_scan('{b}');""",
            f"""SELECT count(*) AS rows, max(amount)::VARCHAR AS max_amount,
       max(pre_balances[1])::VARCHAR AS max_balance, sum(accounts[2])::BIGINT AS acc,
       count(*) FILTER (WHERE status = 'FAILED') AS failed, hex(min(data)) AS min_data,
       typeof(any_value(pre_balances)) AS t_pre_balances, typeof(any_value(accounts)) AS t_accounts,
       typeof(any_value(data)) AS t_data, typeof(any_value(topics)) AS t_topics
FROM delta_scan('{t}');""",
            f"SELECT count(*) AS filtered FROM delta_scan('{b}') WHERE block_num < 5;",
        ]
    )
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": os.environ.get("HOME", "/tmp")}
    out = subprocess.run(
        [duckdb, "-json", "-c", sql], capture_output=True, text=True, env=env, check=False
    )
    if out.returncode != 0:
        raise RuntimeError(f"{duckdb} failed:\n{out.stderr}\n{out.stdout}")
    # One JSON array per SELECT (INSTALL/LOAD/SET print nothing).
    arrays = [json.loads(chunk) for chunk in split_json_arrays(out.stdout)]
    bl, tx, filt = arrays[-3][0], arrays[-2][0], arrays[-1][0]
    return {
        "types": {k[2:]: v for k, v in {**bl, **tx}.items() if k.startswith("t_")},
        "blocks.rows": bl["rows"],
        "blocks.sum_block_num": int(bl["sum"]),
        "blocks.max_nonce": bl["max_nonce"],
        "blocks.min_nonce": bl["min_nonce"],
        "blocks.min_ts_ms": bl["min_ts"],
        "blocks.max_ts_ms": bl["max_ts"],
        "blocks.dates": bl["dates"],
        "transactions.rows": tx["rows"],
        "transactions.max_amount": tx["max_amount"],
        "transactions.max_balance": tx["max_balance"],
        "transactions.sum_account_255": tx["acc"],
        "transactions.failed": tx["failed"],
        "transactions.min_data_hex": tx["min_data"],
        "filtered_rows": filt["filtered"],
    }


def split_json_arrays(text: str):
    depth, start = 0, None
    in_string, escape = False, False
    for i, ch in enumerate(text):
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "[":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0 and start is not None:
                yield text[start : i + 1]
                start = None


def compare(engine: str, got: dict, want: dict, filtered: int) -> list[str]:
    errors = [
        f"{engine}: {k} = {got.get(k)!r}, expected {v!r}" for k, v in want.items() if got.get(k) != v
    ]
    if got.get("filtered_rows") != filtered:
        errors.append(f"{engine}: filtered rows {got.get('filtered_rows')} != {filtered}")
    return errors


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", required=True)
    ap.add_argument("--transactions", type=int, required=True)
    ap.add_argument("--blocks", type=int, default=20)
    ap.add_argument("--duckdb", action="append", default=[], help="anonymous S3 reads")
    ap.add_argument(
        "--duckdb-signed",
        action="append",
        default=[],
        help="signed S3 reads (DuckDB 1.1.1's delta v0.2.1 fails anonymous reads of checkpoints)",
    )
    ap.add_argument("--extension-dir", default=os.path.join(os.getcwd(), ".duckdb-extensions"))
    ap.add_argument("--s3-endpoint")
    args = ap.parse_args()

    want = expected(args.transactions, args.blocks)
    storage = {}
    if args.s3_endpoint:
        storage = {
            "aws_endpoint_url": args.s3_endpoint,
            "aws_region": "us-east-1",
            "aws_allow_http": "true",
            "aws_skip_signature": "true",
        }
    report, errors = {}, []
    got = polars_results(args.lake, storage)
    report[f"polars {pl.__version__}"] = got
    errors += compare("polars", got, want, 5)
    runs = [(d, False) for d in args.duckdb] + [(d, True) for d in args.duckdb_signed]
    for duckdb, signed in runs:
        version = subprocess.run([duckdb, "-version"], capture_output=True, text=True).stdout.strip()
        got = duckdb_results(duckdb, args.lake, args.extension_dir, args.s3_endpoint, signed)
        report[f"duckdb {version}" + (" signed" if signed and args.s3_endpoint else "")] = got
        errors += compare(f"duckdb {version}", got, want, 5)
    print(json.dumps(report, indent=1, sort_keys=True))
    for e in errors:
        print("MISMATCH", e, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
