"""Run `deltalake` maintenance on fireparq's Delta tables (#643 L4, #636).

Run by `blocks/tests/delta_recovery.rs` with the interpreter named by
`FIREPARQ_POLARS_PYTHON` (`deltalake`, pinned in `requirements.txt`), beside
or between runs of the real `fireparq build`. The Rust test makes the
assertions. Usage:

    python delta_maintain.py '{"root": "...", "tables": ["blocks"], "mode": "compact"}'

`root` is a local dataset root or `s3://bucket/prefix`, with `storage` holding
the object_store options for S3. Modes:

- `compact`: for each table, OPTIMIZE (compact) every date, then a lite VACUUM
  with retention 0, which deletes the compacted parts at once, then a
  checkpoint: the maintenance CronJob's calls (design §9) with the shortest
  possible retention. The lite VACUUM never deletes an untracked part.
- `full-vacuum`: for each table, a full VACUUM with retention 0, which also
  deletes every untracked file, such as a pending transaction's uncommitted
  parts (design §4.1). The deployed job never runs this.
- `loop`: `compact` rounds until `stop_file` exists and at least `min_rounds`
  ran, for maintenance beside a running build.

Prints one JSON object on stdout: the rounds, and per table the OPTIMIZE
commits, the files they removed and added, the files VACUUM deleted, and every
error (a maintenance call that failed, for example on a conflict).
"""

import json
import os
import sys
import time

from deltalake import DeltaTable


def open_table(spec, table):
    return DeltaTable(f"{spec['root']}/{table}", storage_options=spec.get("storage"))


def compact(spec, table, report):
    dt = open_table(spec, table)
    metrics = dt.optimize.compact()
    if metrics.get("numFilesRemoved", 0) > 0:
        report["optimize_commits"] += 1
        report["files_removed"] += metrics["numFilesRemoved"]
        report["files_added"] += metrics["numFilesAdded"]
    vacuumed = dt.vacuum(
        retention_hours=0, enforce_retention_duration=False, dry_run=False
    )
    report["vacuumed"] += len(vacuumed)
    dt.create_checkpoint()


def full_vacuum(spec, table, report):
    dt = open_table(spec, table)
    vacuumed = dt.vacuum(
        retention_hours=0, enforce_retention_duration=False, dry_run=False, full=True
    )
    report["vacuumed"] += len(vacuumed)


def run_round(spec, action, reports):
    for table in spec["tables"]:
        try:
            action(spec, table, reports[table])
        except Exception as error:  # a failed call is reported, not fatal
            reports[table]["errors"].append(f"{type(error).__name__}: {error}")


def main():
    spec = json.loads(sys.argv[1])
    reports = {
        table: {
            "optimize_commits": 0,
            "files_removed": 0,
            "files_added": 0,
            "vacuumed": 0,
            "errors": [],
        }
        for table in spec["tables"]
    }
    mode = spec["mode"]
    rounds = 0
    if mode == "compact":
        run_round(spec, compact, reports)
        rounds = 1
    elif mode == "full-vacuum":
        run_round(spec, full_vacuum, reports)
        rounds = 1
    elif mode == "loop":
        while rounds < spec.get("min_rounds", 1) or not os.path.exists(spec["stop_file"]):
            run_round(spec, compact, reports)
            rounds += 1
            time.sleep(spec.get("pause_secs", 0.05))
    else:
        raise SystemExit(f"unknown mode {mode}")
    json.dump({"rounds": rounds, "tables": reports}, sys.stdout)


if __name__ == "__main__":
    main()
