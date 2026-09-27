#!/usr/bin/env python3
"""Reproduce the validated rollup/merge defects with the main binary and check the fix.

Local data only; every run has empty S3/AWS variables and a cwd outside the repository.

Case 1b sets FIREPARQ_TEST_ROLLUP_CRASH_AT, which only debug builds read: --fixed
must be a debug binary (target/debug/fireparq) or an optimized one built with debug
assertions (CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true cargo build --release). A
plain release binary ignores the hook, so those runs never crash.
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--main", required=True, type=Path)
p.add_argument("--fixed", required=True, type=Path)
p.add_argument("--pristine", required=True, type=Path)
p.add_argument("--work", required=True, type=Path)
p.add_argument("--report", required=True, type=Path)
a = p.parse_args()

ENV = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR") if k in os.environ}
ENV["NO_COLOR"] = "1"
for k in ("S3_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_URL_S3",
          "AWS_ENDPOINT_URL", "AWS_REGION", "AWS_SESSION_TOKEN"):
    ENV[k] = ""
TABLES = [
    "balance_changes", "blocks", "calls", "code_changes", "gas_changes", "logs",
    "nonce_changes", "storage_changes", "system_balance_changes", "system_calls",
    "system_gas_changes", "system_storage_changes", "transactions",
]


def duck(sql):
    out = subprocess.run(["duckdb", "-csv", "-noheader", "-c", sql], capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr)
    return out.stdout.strip()


def rows(root):
    return {t: int(duck(f"select count(*) from read_parquet('{root}/{t}/**/*.parquet')"))
            for t in TABLES if list((root / t).rglob("*.parquet"))}


def except_all(a_root, b_root):
    """Rows of b not in a plus rows of a not in b, per table (multiset difference)."""
    out = {}
    for t in TABLES:
        ga, gb = f"{a_root}/{t}/**/*.parquet", f"{b_root}/{t}/**/*.parquet"
        d1 = int(duck(f"select count(*) from (select * from read_parquet('{ga}', hive_partitioning=false) except all select * from read_parquet('{gb}', hive_partitioning=false))"))
        d2 = int(duck(f"select count(*) from (select * from read_parquet('{gb}', hive_partitioning=false) except all select * from read_parquet('{ga}', hive_partitioning=false))"))
        out[t] = [d1, d2]
    return out


def fresh(name):
    d = a.work / name
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    shutil.copytree(a.pristine, d / "data", symlinks=True)
    return d


def run(binary, args, cwd, extra=None):
    env = dict(ENV)
    env.update(extra or {})
    proc = subprocess.run([str(binary)] + [str(x) for x in args], cwd=cwd, env=env,
                          capture_output=True, text=True, timeout=600)
    return proc.returncode, proc.stdout + proc.stderr


def run_kill_after_first_part(binary, args, cwd):
    """Start the command and SIGKILL it as soon as it logs its first written part."""
    proc = subprocess.Popen([str(binary)] + [str(x) for x in args], cwd=cwd, env=ENV,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    killed = False
    for line in proc.stdout:
        if "wrote rolled-up part" in line:
            proc.send_signal(signal.SIGKILL)
            killed = True
            break
    proc.wait()
    return killed, proc.returncode


pristine_rows = rows(a.pristine)
report = {"pristine_rows": pristine_rows, "pristine_total": sum(pristine_rows.values()), "cases": {}}
rollup = ["rollup", "data", "--delete-source", "-p", "date", "--flush-bytes", "65536"]

# 1. In-place rollup killed after its first part, then re-run.
for label, binary in [("main", a.main), ("fixed", a.fixed)]:
    d = fresh(f"kill-{label}")
    killed, code = run_kill_after_first_part(binary, rollup, d)
    rerun_code, _ = run(binary, rollup, d)
    r = rows(d / "data")
    report["cases"][f"inplace-sigkill-{label}"] = {
        "killed": killed, "exit": code, "rerun_exit": rerun_code, "total": sum(r.values()),
        "extra_rows": sum(r.values()) - report["pristine_total"],
        "journals_left": len(list((d / "data").rglob("_fireparq_rollup.json"))),
    }

# 1b. The fixed binary at every deterministic crash point, in place and copy mode.
for step in ["after-first-part", "after-outputs", "after-commit", "after-first-delete"]:
    for mode in ["inplace", "copy"]:
        if mode == "copy" and step == "after-first-delete":
            continue
        d = fresh(f"crash-{mode}-{step}")
        args = rollup if mode == "inplace" else ["rollup", "data", "-o", "out", "-p", "date", "--flush-bytes", "65536"]
        code, _ = run(a.fixed, args, d, {"FIREPARQ_TEST_ROLLUP_CRASH_AT": step})
        rerun_code, log = run(a.fixed, args, d)
        target = d / ("data" if mode == "inplace" else "out")
        r = rows(target)
        diff = except_all(a.pristine, target)
        report["cases"][f"crash-{mode}-{step}"] = {
            "crash_exit": code, "rerun_exit": rerun_code, "total": sum(r.values()),
            "rows_equal_pristine": r == pristine_rows,
            "except_all_zero": all(v == [0, 0] for v in diff.values()),
            "sources_left": len([f for f in (d / "data").rglob("*.parquet") if "hour=" in str(f)]),
            "journals_left": len(list(target.rglob("_fireparq_rollup.json"))),
            "recovered": "interrupted rollup" in log,
        }

# 2. Copy rollup, merge the output, copy rollup again.
for label, binary in [("main", a.main), ("fixed", a.fixed)]:
    d = fresh(f"copy-merge-{label}")
    # Small parts give each target partition several copies, so merge renames them.
    copy = ["rollup", "data", "-o", "out", "-p", "date", "--flush-bytes", "65536"]
    c1, _ = run(binary, copy, d)
    c2, _ = run(binary, ["merge", "out"], d)
    c3, _ = run(binary, copy, d)
    r = rows(d / "out")
    report["cases"][f"copy-merge-copy-{label}"] = {
        "exits": [c1, c2, c3], "balance_changes": r.get("balance_changes"),
        "total": sum(r.values()), "rows_equal_pristine": r == pristine_rows,
    }

# 3. Same columns, hex vs base58 block-id encoding in one partition.
part_dir = "blocks/year=2025/month=12/date=13/hour=00/minute=09"
for label, binary in [("main", a.main), ("fixed", a.fixed)]:
    d = fresh(f"encoding-{label}")
    parts = sorted((d / "data" / part_dir).glob("*.parquet"))
    for part, enc in zip(parts, ["hex", "base58"]):
        tmp = part.with_suffix(".tmp")
        duck(f"copy (select * from read_parquet('{part}')) to '{tmp}' (format parquet, "
             f"kv_metadata {{'firehose-parquet.block_type': 'evm', 'firehose-parquet.block_id_encoding': '{enc}'}})")
        tmp.replace(part)
    before = sorted(p.name for p in (d / "data" / part_dir).glob("*.parquet"))
    code, log = run(binary, ["merge", f"data/{part_dir}"], d)
    after = sorted(p.name for p in (d / "data" / part_dir).glob("*.parquet"))
    labels = duck(f"select distinct value from parquet_kv_metadata('{d}/data/{part_dir}/*.parquet') "
                  f"where decode(key) = 'firehose-parquet.block_id_encoding'")
    report["cases"][f"hex-base58-merge-{label}"] = {
        "exit": code, "files_before": before, "files_after": after,
        "block_id_encoding_labels_after": sorted(labels.split()),
        "reported": "block_id_encoding" in log,
    }

a.report.write_text(json.dumps(report, indent=1, sort_keys=True))
print(json.dumps(report["cases"], indent=1, sort_keys=True))
