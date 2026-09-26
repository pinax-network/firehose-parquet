#!/usr/bin/env python3
"""Baseline/candidate CLI equivalence for #529 maintenance consolidation.

Runs the same maintenance scenarios with two fireparq binaries on fresh copies of
one local dataset, then compares exit codes, normalized stdout/stderr (debug logs),
file inventories and SHA-256 digests, and DuckDB row counts with EXCEPT ALL in both
directions. Requires the `duckdb` CLI. The TABLES list matches an EVM dataset.

Local data only. Each binary runs with a scrubbed environment in which the S3 and
AWS variables are present but empty, so neither the caller's shell nor a `.env`
found by dotenv can supply a bucket or credentials. Keep --work outside the
repository tree. Compare a binary with a copy of itself first: the result must be
all-equal, which validates the normalization.

    529-equivalence.py --baseline main/fireparq --candidate branch/fireparq \
        --pristine /abs/dataset --work /abs/scratch/run --report /abs/report.json
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--baseline", required=True, type=Path)
p.add_argument("--candidate", required=True, type=Path)
p.add_argument("--pristine", required=True, type=Path)
p.add_argument("--work", required=True, type=Path)
p.add_argument("--report", required=True, type=Path)
a = p.parse_args()

ENV = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR") if k in os.environ}
ENV["NO_COLOR"] = "1"
# Present-but-empty values stop dotenv from injecting a real bucket or credentials.
for _k in ("S3_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_URL_S3",
           "AWS_ENDPOINT_URL", "AWS_REGION", "AWS_SESSION_TOKEN"):
    ENV[_k] = ""
ENV["LOG_LEVEL"] = "debug"
ANSI = re.compile(r"\x1b\[[0-9;]*m")
STAMP = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z")
RUN = re.compile(r"part-(rollup-)?[0-9a-f]{8}-(\d{6})\.parquet")
TMP = re.compile(r"\.([A-Za-z0-9_.-]+)\.[0-9a-f]{12,32}\.tmp")
UUIDISH = re.compile(r"\b[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}\b")
TABLES = [
    "balance_changes", "blocks", "calls", "code_changes", "gas_changes", "logs",
    "nonce_changes", "storage_changes", "system_balance_changes", "system_calls",
    "system_gas_changes", "system_storage_changes", "transactions",
]

# Each scenario: list of (argv-after-binary, extra env). "data" and "out" are relative
# to the scenario directory, so displayed paths match between binaries.
SCENARIOS = {
    "merge-default": [(["merge", "data"], {})],
    "merge-rows-snappy": [(["merge", "data", "--flush-rows", "700", "--compression", "snappy"], {})],
    "merge-dry-run": [(["merge", "data", "--dry-run"], {})],
    "merge-table-dir": [(["merge", "data/logs", "--flush-bytes", "65536"], {})],
    "merge-crash-after-outputs": [
        (["merge", "data"], {"FIREPARQ_TEST_MERGE_CRASH_AT": "after-outputs"}),
        (["merge", "data"], {}),
    ],
    "merge-crash-after-commit": [
        (["merge", "data"], {"FIREPARQ_TEST_MERGE_CRASH_AT": "after-commit"}),
        (["merge", "data", "--dry-run"], {}),
        (["merge", "data"], {}),
    ],
    "merge-crash-after-first-delete": [
        (["merge", "data"], {"FIREPARQ_TEST_MERGE_CRASH_AT": "after-first-delete"}),
        (["merge", "data"], {}),
    ],
    "rollup-copy-hour": [
        (["rollup", "data", "-o", "out", "-p", "hour"], {}),
        (["rollup", "data", "-o", "out", "-p", "hour"], {}),
    ],
    "rollup-copy-date-small": [(["rollup", "data", "-o", "out", "-p", "date", "--flush-bytes", "65536"], {})],
    "rollup-inplace-date": [(["rollup", "data", "--delete-source", "-p", "date"], {})],
    "rollup-inplace-hour-small": [
        (["rollup", "data", "--delete-source", "-p", "hour", "--flush-bytes", "65536", "--compression", "snappy"], {}),
    ],
    "rollup-inplace-refused": [(["rollup", "data", "-p", "date"], {})],
    "truncate-dry-run": [(["truncate", "data", "-p", "minute=0*", "--dry-run"], {})],
    "truncate-no-yes": [(["truncate", "data", "-p", "minute=0*"], {})],
    "truncate-yes-filter": [(["truncate", "data", "-p", "minute=0*", "-p", "day=13", "--yes"], {})],
    "truncate-yes-table": [(["truncate", "data/blocks", "--yes"], {})],
    "truncate-yes-root": [(["truncate", "data", "--yes"], {})],
    "verify-blocks": [
        (["verify", "data/blocks", "--registry-path", "registry.parquet", "--update-registry", "--report-json", "report.json"], {}),
        (["verify", "data/blocks", "--registry-path", "registry.parquet", "--profile", "deep", "--report-json", "report2.json"], {}),
    ],
    "verify-logs-nofailfast": [
        (["verify", "data/logs", "--registry-path", "registry.parquet", "--update-registry", "--no-fail-fast", "--report-json", "report.json"], {}),
        (["verify", "data/logs", "--registry-path", "registry.parquet", "--checks", "roots", "--checks", "protocol", "--report-json", "report2.json"], {}),
    ],
    "verify-before-after-merge": [
        (["verify", "data/transactions", "--registry-path", "registry.parquet", "--update-registry", "--report-json", "report.json"], {}),
        (["merge", "data"], {}),
        (["verify", "data/transactions", "--registry-path", "registry.parquet", "--report-json", "report2.json"], {}),
    ],
    "validate-blocks": [(["validate", "data/blocks"], {})],
    "scan-blocks": [(["scan", "data/blocks", "-n", "3", "--json"], {})],
    "inspect-part": [(["inspect", "data/blocks/year=2025/month=12/date=13/hour=00/minute=09/part-a4213a22-000001.parquet"], {})],
    "scan-root-schema": [(["scan", "data", "--schema-only"], {})],
}


def normalize(text, scenario_dir):
    text = ANSI.sub("", text)
    text = text.replace(str(scenario_dir.resolve()), "<SCENARIO>")
    text = text.replace(str(scenario_dir), "<SCENARIO>")
    text = STAMP.sub("<TS>", text)
    text = RUN.sub(lambda m: f"part-{m.group(1) or ''}<RUN>-{m.group(2)}.parquet", text)
    text = TMP.sub(lambda m: f".{m.group(1)}.<TMPID>.tmp", text)
    text = UUIDISH.sub("<UUID>", text)
    text = re.sub(r"\b[0-9a-f]{12}\b", "<RUNID>", text)
    text = re.sub(r"fireparq-main(-copy)?|fireparq-branch", "fireparq", text)
    # Elapsed timings in logs are the only nondeterministic numbers.
    text = re.sub(r"(elapsed|duration)[^ ]*=[0-9.]+[a-zµ]*", r"\1=<T>", text)
    text = re.sub(r"(duration ms:\s+)\d+", r"\1<T>", text)
    return text


def normalize_json(value):
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if k in ("generated_at", "started_at", "finished_at", "updated_at", "run_id", "timestamp") or "duration" in k:
                out[k] = "<volatile>"
            else:
                out[k] = normalize_json(v)
        return out
    if isinstance(value, list):
        return [normalize_json(v) for v in value]
    return value


def snapshot(scenario_dir):
    files = {}
    for path in sorted(scenario_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = str(path.relative_to(scenario_dir))
        if rel.startswith("log-"):
            continue
        key = normalize(rel, scenario_dir)
        data = path.read_bytes()
        if path.suffix == ".json":
            try:
                parsed = json.loads(normalize(data.decode(), scenario_dir))
                blob = json.dumps(normalize_json(parsed), sort_keys=True).encode()
            except ValueError:
                blob = data
            files[key] = hashlib.sha256(blob).hexdigest()
        elif path.name == "registry.parquet":
            rows = duck(f"select * exclude (updated_at) from read_parquet('{path}') order by all")
            files[key] = "registry:" + hashlib.sha256(rows.encode()).hexdigest()
        elif path.suffix == ".lock" or path.name.startswith(".fireparq-owner"):
            files[key] = "<lock:%d>" % (1 if data else 0)
        else:
            files[key] = hashlib.sha256(data).hexdigest()
    return files


def duck(sql):
    out = subprocess.run(["duckdb", "-csv", "-noheader", "-c", sql], capture_output=True, text=True)
    if out.returncode != 0:
        return "ERR:" + out.stderr.strip().splitlines()[-1] if out.stderr.strip() else "ERR"
    return out.stdout.strip()


def rows_and_except(base_dir, cand_dir):
    result = {}
    for root in ("data", "out"):
        for table in TABLES:
            b = base_dir / root / table
            c = cand_dir / root / table
            if not b.exists() and not c.exists():
                continue
            key = f"{root}/{table}"
            if b.exists() != c.exists():
                result[key] = {"presence_mismatch": [b.exists(), c.exists()]}
                continue
            bg, cg = f"{b}/**/*.parquet", f"{c}/**/*.parquet"
            if not list(b.rglob("*.parquet")) and not list(c.rglob("*.parquet")):
                result[key] = {"rows": [0, 0], "except_b_minus_c": 0, "except_c_minus_b": 0}
                continue
            rb = duck(f"select count(*) from read_parquet('{bg}')")
            rc = duck(f"select count(*) from read_parquet('{cg}')")
            e1 = duck(f"select count(*) from (select * from read_parquet('{bg}') except all select * from read_parquet('{cg}'))")
            e2 = duck(f"select count(*) from (select * from read_parquet('{cg}') except all select * from read_parquet('{bg}'))")
            result[key] = {"rows": [rb, rc], "except_b_minus_c": e1, "except_c_minus_b": e2}
    return result


def run_scenario(name, steps, binary, label):
    scenario_dir = a.work / name / label
    if scenario_dir.exists():
        shutil.rmtree(scenario_dir)
    scenario_dir.mkdir(parents=True)
    shutil.copytree(a.pristine, scenario_dir / "data", symlinks=True)
    outputs = []
    for index, (argv, extra) in enumerate(steps):
        env = dict(ENV)
        env.update(extra)
        proc = subprocess.run([str(binary)] + argv, cwd=scenario_dir, env=env,
                              capture_output=True, text=True, timeout=600)
        log = normalize(proc.stdout, scenario_dir) + "\n--- stderr ---\n" + normalize(proc.stderr, scenario_dir)
        (scenario_dir / f"log-{index}.txt").write_text(log)
        outputs.append({"argv": argv, "env": extra, "code": proc.returncode, "log": log,
                        "snapshot": snapshot(scenario_dir)})
    return scenario_dir, outputs


report = {"baseline": str(a.baseline), "candidate": str(a.candidate), "scenarios": {}}
all_equal = True
for name, steps in SCENARIOS.items():
    bdir, base = run_scenario(name, steps, a.baseline, "baseline")
    cdir, cand = run_scenario(name, steps, a.candidate, "candidate")
    step_results = []
    for index, (b, c) in enumerate(zip(base, cand)):
        same_code = b["code"] == c["code"]
        same_log = b["log"] == c["log"]
        same_files = b["snapshot"] == c["snapshot"]
        diff = sorted(set(b["snapshot"].items()) ^ set(c["snapshot"].items()))[:10]
        step_results.append({
            "argv": b["argv"], "env": b["env"], "exit": [b["code"], c["code"]],
            "same_exit": same_code, "same_log": same_log, "same_files": same_files,
            "files": len(c["snapshot"]),
            "parquet_files": sum(1 for k in c["snapshot"] if k.endswith(".parquet")),
            "file_diff_sample": diff,
        })
        if not (same_code and same_log and same_files):
            all_equal = False
            if not same_log:
                (a.work / name / f"logdiff-{index}.txt").write_text(
                    "=== baseline ===\n" + b["log"] + "\n=== candidate ===\n" + c["log"])
    data = rows_and_except(bdir, cdir)
    for v in data.values():
        if "presence_mismatch" in v or v["rows"][0] != v["rows"][1] or str(v["except_b_minus_c"]) != "0" or str(v["except_c_minus_b"]) != "0":
            all_equal = False
    report["scenarios"][name] = {"steps": step_results, "tables": data}
    status = all(s["same_exit"] and s["same_log"] and s["same_files"] for s in step_results)
    print(f"{name:34s} {'EQUAL' if status else 'DIFF'} exits={[s['exit'] for s in step_results]} "
          f"files={[s['files'] for s in step_results]}")

report["all_equal"] = all_equal
a.report.write_text(json.dumps(report, indent=1, sort_keys=True))
print("ALL EQUAL" if all_equal else "DIFFERENCES FOUND")
sys.exit(0 if all_equal else 1)
