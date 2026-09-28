"""Check the VACUUM rules of `scripts/delta_maintenance.py` (#643, design §4.1).

Run by `blocks/tests/delta_maintenance.rs` with the interpreter named by
`FIREPARQ_POLARS_PYTHON`; the Rust test makes the assertions. Usage:

    python vacuum_check.py '{"script": ".../delta_maintenance.py", "lake": "<fireparq root>",
                             "tables": ["blocks", ...], "scratch": "<empty dir>"}'

It prints one JSON object with three parts:

- `order`: three scratch tables whose `delta.deletedFileRetentionDuration` is
  2 seconds, each with three files tombstoned by OPTIMIZE more than that ago.
  A checkpoint drops expired tombstones, so a checkpoint followed by a VACUUM
  from a fresh handle (the next run) deletes nothing and leaves orphans, while
  VACUUM then checkpoint, and the job itself, delete them all.
- `idempotent`: two default runs of the job over the fireparq lake; the
  `table/date` partitions the first compacted, and the tables whose version
  the second run changed.
- `untracked`: a copy of a `blocks` data file that no log entry names, as a
  part fireparq published but has not committed yet. Whether each job mode
  kept it, whether an unguarded full VACUUM (retention 0, not enforced) would
  delete it, and the committed rows before and after.
"""

import datetime
import json
import os
import shutil
import subprocess
import sys
import time

import deltalake
import polars as pl


def run_job(spec, root, tables, **settings):
    env = {
        "PYTHONDONTWRITEBYTECODE": "1",
        "LAKE_ROOT": root,
        "LAKE_TABLES": ",".join(tables),
        **settings,
    }
    out = subprocess.run(
        [sys.executable, spec["script"]], capture_output=True, text=True, env=env, check=False
    )
    lines = [json.loads(line) for line in out.stdout.splitlines() if line.strip()]
    return out.returncode, lines, out.stderr


def data_files(uri):
    found = set()
    for directory, _, files in os.walk(uri):
        if "_delta_log" in directory:
            continue
        found |= {
            os.path.relpath(os.path.join(directory, name), uri)
            for name in files
            if name.endswith(".parquet")
        }
    return found


def orphans(uri):
    active = set(deltalake.DeltaTable(uri).get_add_actions(flatten=True).column("path").to_pylist())
    return len(data_files(uri) - active)


def tombstoned_table(uri):
    for block in range(3):
        frame = pl.DataFrame({"block_num": [block], "date": [datetime.date(2023, 11, 14)]})
        deltalake.write_deltalake(
            uri,
            frame,
            mode="append",
            partition_by=["date"],
            configuration={"delta.deletedFileRetentionDuration": "interval 2 seconds"},
        )
    deltalake.DeltaTable(uri).optimize.compact()


def order(spec):
    scratch = spec["scratch"]
    shutil.rmtree(scratch, ignore_errors=True)
    reverse, forward = f"{scratch}/checkpoint-first", f"{scratch}/vacuum-first"
    job_lake = f"{scratch}/job-lake"
    for uri in (reverse, forward, f"{job_lake}/blocks"):
        tombstoned_table(uri)
    time.sleep(3)  # every tombstone is now older than the retention

    deltalake.DeltaTable(reverse).create_checkpoint()
    deleted_reverse = deltalake.DeltaTable(reverse).vacuum(dry_run=False)

    table = deltalake.DeltaTable(forward)
    deleted_forward = table.vacuum(dry_run=False)
    table.create_checkpoint()

    status, lines, stderr = run_job(spec, job_lake, ["blocks"])
    reports = [line for line in lines if line.get("event") == "table"]
    if status != 0 or not reports:
        raise SystemExit(f"job failed: {status} {lines} {stderr}")
    return {
        "checkpoint_then_vacuum": {"deleted": len(deleted_reverse), "orphans": orphans(reverse)},
        "vacuum_then_checkpoint": {"deleted": len(deleted_forward), "orphans": orphans(forward)},
        "job": {
            "deleted": reports[0]["vacuum"]["files_deleted"],
            "orphans": orphans(f"{job_lake}/blocks"),
        },
    }


def idempotent(spec):
    report = {}
    for attempt in range(2):
        status, lines, stderr = run_job(spec, spec["lake"], spec["tables"])
        if status != 0:
            raise SystemExit(f"job failed: {status} {lines} {stderr}")
        tables = [line for line in lines if line.get("event") == "table"]
        if attempt == 0:
            report["first_run_compacted"] = sorted(
                f"{line['table']}/{compaction['date']}"
                for line in tables
                for compaction in line["compacted"]
            )
        else:
            report["versions_changed"] = [
                line["table"] for line in tables if line["version_before"] != line["version_after"]
            ]
    return report


def untracked(spec):
    uri = f"{spec['lake']}/blocks"
    table = deltalake.DeltaTable(uri)
    active = table.get_add_actions(flatten=True).column("path").to_pylist()
    source = f"{uri}/{active[0]}"
    part = os.path.join(os.path.dirname(source), "part-v1-untracked-copy.parquet")
    shutil.copyfile(source, part)

    def rows():
        return pl.scan_delta(uri).select(pl.len()).collect().item()

    def job(**settings):
        status, _, _ = run_job(spec, spec["lake"], spec["tables"], **settings)
        return {"exit": status, "kept": os.path.exists(part)}

    report = {"rows_before": rows()}
    report["lite_retention_0"] = job(VACUUM_RETENTION_HOURS="0")
    report["full_enforced"] = job(FULL_VACUUM="1")
    report["full_retention_0"] = job(FULL_VACUUM="1", VACUUM_RETENTION_HOURS="0")
    would = deltalake.DeltaTable(uri).vacuum(
        retention_hours=0, enforce_retention_duration=False, full=True, dry_run=True
    )
    report["unguarded_full_vacuum_would_delete"] = any(
        path.endswith("part-v1-untracked-copy.parquet") for path in would
    )
    eight_days_ago = time.time() - 8 * 86_400
    os.utime(part, (eight_days_ago, eight_days_ago))
    report["full_enforced_after_8_days"] = job(FULL_VACUUM="1")
    report["rows_after"] = rows()
    return report


def main():
    spec = json.loads(sys.argv[1])
    report = {"order": order(spec), "idempotent": idempotent(spec), "untracked": untracked(spec)}
    json.dump(report, sys.stdout)


if __name__ == "__main__":
    main()
