#!/usr/bin/env python3
"""Run Delta maintenance (OPTIMIZE, VACUUM, checkpoint, log cleanup) with the
`deltalake` Python package while the spike writer keeps appending.

Usage:
  concurrent_maintenance.py --bin target/debug/delta-lake-spike --lake <dir|s3://bucket/prefix> \
      --transactions N [--interval-ms MS] [--blocks B] [--s3-endpoint URL]

The writer (`delta-lake-spike write`) commits one Delta commit per table per
transaction, with a `txn` action. Maintenance compacts the very `date`
partition the writer appends to, which is the worst case: the deployed job only
touches closed dates. Passes when the writer never fails, maintenance commits
interleave with the writer's commits, every `txn` reaches N, and the
compacted, vacuumed tables hold exactly the written rows (no loss, no
duplicates).
"""
import argparse
import json
import os
import subprocess
import sys
import time

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError, DeltaError, TableNotFoundError

TABLES = ["transactions", "blocks"]
DATE = "2026-09-25"


def maintain(uri: str, storage: dict, stats: dict) -> None:
    """One CronJob-style round, in the proposed order."""
    try:
        dt = DeltaTable(uri, storage_options=storage)
    except TableNotFoundError:
        time.sleep(0.05)  # the writer has not created the table yet
        return
    stats["rounds"] += 1
    for step in ("optimize", "vacuum", "checkpoint", "cleanup"):
        try:
            if step == "optimize":
                m = dt.optimize.compact(partition_filters=[("date", "=", DATE)])
                if m.get("numFilesRemoved", 0):
                    stats["optimize_commits"] += 1
                    stats["files_compacted"] += m["numFilesRemoved"]
            elif step == "vacuum":
                # Lite mode (the default): only files that a `remove` names.
                deleted = dt.vacuum(
                    retention_hours=0, enforce_retention_duration=False, dry_run=False
                )
                stats["files_vacuumed"].update(deleted)
            elif step == "checkpoint":
                dt.create_checkpoint()
                stats["checkpoints"] += 1
            else:
                dt.cleanup_metadata()
        except CommitFailedError as e:
            stats["maintenance_conflicts"].append(f"{step}: {e}")
        except DeltaError as e:
            stats["maintenance_errors"].append(f"{step}: {e}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--lake", required=True)
    ap.add_argument("--transactions", type=int, default=150)
    ap.add_argument("--interval-ms", type=int, default=20)
    ap.add_argument("--blocks", type=int, default=20)
    ap.add_argument("--s3-endpoint")
    args = ap.parse_args()

    storage = {}
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": os.environ.get("HOME", "/tmp")}
    if args.s3_endpoint:
        storage = {
            "aws_endpoint_url": args.s3_endpoint,
            "aws_region": "us-east-1",
            "aws_access_key_id": "spike",
            "aws_secret_access_key": "spike",
            "aws_allow_http": "true",
            "aws_conditional_put": "etag",
        }
        bucket, _, prefix = args.lake.removeprefix("s3://").partition("/")
        spec = f"s3:{prefix}"
        env |= {"DELTA_SPIKE_S3_ENDPOINT": args.s3_endpoint, "DELTA_SPIKE_S3_BUCKET": bucket}
    else:
        spec = args.lake
    uris = {t: f"{args.lake.rstrip('/')}/{t}" for t in TABLES}

    writer = subprocess.Popen(
        [
            args.bin, "write", "--lake", spec,
            "--transactions", str(args.transactions),
            "--interval-ms", str(args.interval_ms),
            "--blocks", str(args.blocks),
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
    )
    stats = {
        "rounds": 0, "optimize_commits": 0, "files_compacted": 0, "files_vacuumed": set(),
        "checkpoints": 0, "maintenance_conflicts": [], "maintenance_errors": [],
    }
    started = time.monotonic()
    while writer.poll() is None:
        for uri in uris.values():
            maintain(uri, storage, stats)
    out, err = writer.communicate()
    stats["writer_exit"] = writer.returncode
    stats["writer"] = json.loads(out) if writer.returncode == 0 else err[-2000:]
    stats["elapsed_s"] = round(time.monotonic() - started, 1)
    for uri in uris.values():  # a last round after the writer stopped
        maintain(uri, storage, stats)

    errors = []
    if writer.returncode != 0:
        errors.append(f"writer failed: {err[-2000:]}")
    per_block = {"blocks": 1, "transactions": 3}
    for table, uri in uris.items():
        dt = DeltaTable(uri, storage_options=storage)
        ops = [(h["version"], h["operation"]) for h in dt.history()]
        writes = sorted(v for v, op in ops if op == "WRITE")
        optimizes = sorted(v for v, op in ops if op == "OPTIMIZE")
        interleaved = [v for v in optimizes if writes and writes[0] < v < writes[-1]]
        rows = pl.scan_delta(uri, storage_options=storage or None).select(pl.len()).collect().item()
        distinct = (
            pl.scan_delta(uri, storage_options=storage or None)
            .select(pl.struct(["block_num"] + (["tx_index"] if table == "transactions" else [])).n_unique())
            .collect()
            .item()
        )
        want = args.transactions * args.blocks * per_block[table]
        stats[table] = {
            "version": dt.version(),
            "txn": dt.transaction_version("fireparq-s0"),
            "writes": len(writes),
            "optimizes": len(optimizes),
            "optimizes_between_writes": len(interleaved),
            "active_files": len(dt.file_uris()),
            "rows": rows,
            "distinct_keys": distinct,
        }
        if dt.transaction_version("fireparq-s0") != args.transactions:
            errors.append(f"{table}: txn {dt.transaction_version('fireparq-s0')} != {args.transactions}")
        if rows != want or distinct != want:
            errors.append(f"{table}: {rows} rows / {distinct} keys, expected {want}")
        if not interleaved:
            errors.append(f"{table}: no OPTIMIZE commit landed between writer commits")
    stats["files_vacuumed"] = len(stats["files_vacuumed"])
    print(json.dumps(stats, indent=1))
    for e in errors:
        print("FAIL", e, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
