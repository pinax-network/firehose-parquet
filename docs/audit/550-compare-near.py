#!/usr/bin/env python3
"""Independent #550 NEAR comparison of replay outputs against original JSON.

Expected `receipt_status` values come from the original NearData block JSON
(`execution_outcome.outcome.status` of each receipt), keyed by receipt ID. The
replay's base58 case supplies the encoding-independent receipt_index -> ID map.

For every replay case it checks:
- `receipt_status` on every receipt_actions and execution_logs row equals the
  original receipt outcome variant (SuccessValue, SuccessReceiptId, Failure);
- the candidate schema is the baseline schema plus exactly the appended column;
- every legacy column value equals the baseline, row by row, in every table;
- failed receipts and their child rows are present with and without
  `--include-failed-transactions` (the flag gates transaction rows only).
"""
import argparse
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ADDED = {"receipt_actions": ["receipt_status"], "execution_logs": ["receipt_status"]}
TABLES = ["blocks", "chunks", "transactions", "receipts", "receipt_actions", "execution_logs", "state_changes"]


def read(case_dir, table):
    files = sorted((case_dir / table).glob("*.parquet"))
    return pa.concat_tables([pq.read_table(f) for f in files]) if files else None


def status_variant(status):
    (variant,) = status.keys()
    return variant


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--json", type=Path, required=True, help="original NearData block JSON")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    source = json.loads(args.json.read_text())
    expected_by_id = {}
    for shard in source["shards"]:
        for item in shard["receipt_execution_outcomes"]:
            expected_by_id[item["receipt"]["receipt_id"]] = status_variant(item["execution_outcome"]["outcome"]["status"])
    manifest = json.loads((args.after / "manifest.json").read_text())
    cases = sorted(manifest["cases"])
    id_by_index = {}
    for case in cases:
        if case.startswith("base58-"):
            receipts = read(args.after / case, "receipts").to_pylist()
            mapping = {r["receipt_index"]: r["receipt_id"] for r in receipts}
            assert not id_by_index or id_by_index == mapping
            id_by_index = mapping
    assert set(id_by_index.values()) == set(expected_by_id), "every source receipt is mapped"
    report = {"cases": {}, "source_statuses": {}, "totals": {"status_values": 0, "legacy_values": 0, "rows": 0}}
    for variant in expected_by_id.values():
        report["source_statuses"][variant] = report["source_statuses"].get(variant, 0) + 1
    for case in cases:
        counts = {"status_values": 0, "legacy_values": 0, "rows": 0, "failure_rows": 0}
        for table in TABLES:
            before, after = read(args.before / case, table), read(args.after / case, table)
            if after is None:
                assert before is None, f"{case}/{table}"
                continue
            added = ADDED.get(table, [])
            legacy = [n for n in after.schema.names if n not in added]
            if added:
                assert after.schema.names[-len(added):] == added, f"{case}/{table}: appended last"
            assert pa.schema([after.schema.field(n) for n in legacy]).equals(before.schema, check_metadata=False), f"{case}/{table}"
            assert after.select(legacy).to_pylist() == before.to_pylist(), f"{case}/{table}: legacy values"
            counts["legacy_values"] += after.num_rows * len(legacy)
            counts["rows"] += after.num_rows
            if added:
                for row in after.select(["receipt_index", "receipt_status"]).to_pylist():
                    expected = expected_by_id[id_by_index[row["receipt_index"]]]
                    assert row["receipt_status"] == expected, (case, table, row)
                    counts["status_values"] += 1
                    counts["failure_rows"] += expected == "Failure"
        assert counts["failure_rows"] > 0, f"{case}: failed receipt rows present regardless of the flag"
        report["cases"][case] = counts
        for key in report["totals"]:
            report["totals"][key] += counts[key]
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"cases": len(cases), **report["totals"], "source_statuses": report["source_statuses"]}, indent=2))


if __name__ == "__main__":
    main()
