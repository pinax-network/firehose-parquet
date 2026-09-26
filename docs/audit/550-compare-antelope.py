#!/usr/bin/env python3
"""Independent #550 Antelope comparison of replay outputs against raw blocks.

Expected selection and outcome labels come from the raw Firehose protobufs
(decoded here with the repository descriptor), not from the new mapper:
a trace succeeds when it has no trace-level exception and its receipt status is
EXECUTED (1), SOFTFAIL (2, the onerror handler) or DELAYED (4).

For every replay case it checks:
- transaction selection and order, `transaction_success` on transactions,
  `transaction_status` + `transaction_success` on every action and db_op row;
- the candidate schema is the baseline schema plus exactly the appended columns;
- every legacy column value: with failed transactions included, all rows equal
  the baseline; by default, the candidate rows of EXECUTED traces equal the
  baseline's default rows, and the rows of newly selected traces equal the
  baseline's rows for the same traces with failed transactions included.
"""
import argparse
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

STATUS = {0: "NONE", 1: "EXECUTED", 2: "SOFTFAIL", 3: "HARDFAIL", 4: "DELAYED", 5: "EXPIRED", 6: "UNKNOWN", 7: "CANCELED"}
ADDED = {
    "transactions": ["transaction_success"],
    "actions": ["transaction_status", "transaction_success"],
    "db_ops": ["transaction_status", "transaction_success"],
    "blocks": [],
}


def load_block_class(descriptor):
    pool = descriptor_pool.DescriptorPool()
    for file in descriptor_pb2.FileDescriptorSet.FromString(Path(descriptor).read_bytes()).file:
        pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("sf.antelope.type.v1.Block"))


def expected_traces(raw_dir, block_class):
    manifest = json.loads((raw_dir / "manifest.json").read_text())
    traces = []
    for entry in manifest:
        data = (raw_dir / f"{entry['block_num']}.pb").read_bytes()
        assert hashlib.sha256(data).hexdigest() == entry["sha256"]
        block = block_class.FromString(data)
        source = block.filtered_transaction_traces if block.filtering_applied else block.unfiltered_transaction_traces
        ids = [trace.id for trace in source]
        assert len(ids) == len(set(ids)), "trace ids must be unique within a block"
        for trace in source:
            status = trace.receipt.status if trace.HasField("receipt") else 0
            success = not trace.HasField("exception") and status in (1, 2, 4)
            traces.append({
                "block_num": entry["block_num"], "tx_hash": trace.id, "index": trace.index,
                "status": STATUS.get(status, "UNKNOWN"), "success": success,
                "actions": len(trace.action_traces), "db_ops": len(trace.db_ops),
                "onerror": trace.HasField("failed_dtrx_trace"),
            })
    return traces


def read(case_dir, table):
    files = sorted((case_dir / table).glob("*.parquet"))
    if not files:
        return None
    return pa.concat_tables([pq.read_table(f) for f in files])


def rows(table, columns=None):
    if table is None:
        return []
    table = table if columns is None else table.select(columns)
    return table.to_pylist()


def keyed(table_rows, trace_by_key):
    return [trace_by_key[(row["block_num"], row["tx_hash"])] for row in table_rows]


def compare_case(case, before_dir, after_dir, traces, include_failed, report):
    trace_by_key = {(t["block_num"], t["tx_hash"]): t for t in traces}
    selected = [t for t in traces if include_failed or t["success"]]
    counts = {"values": 0, "legacy_values": 0, "rows": 0}
    for table_name in ["blocks", "transactions", "actions", "db_ops"]:
        before = read(before_dir, table_name)
        after = read(after_dir, table_name)
        if after is None:
            assert before is None, f"{case}/{table_name}: missing candidate part"
            continue
        added = ADDED[table_name]
        legacy = [name for name in after.schema.names if name not in added]
        assert after.schema.names[-len(added):] == added if added else True, f"{case}/{table_name}: appended last"
        if before is not None:
            assert pa.schema([after.schema.field(n) for n in legacy]).equals(before.schema, check_metadata=False), f"{case}/{table_name}: legacy schema"
        after_rows = rows(after)
        counts["rows"] += len(after_rows)
        if table_name == "transactions":
            got = [(r["block_num"], r["tx_hash"], r["index"]) for r in after_rows]
            want = [(t["block_num"], t["tx_hash"], t["index"]) for t in selected]
            assert got == want, f"{case}: transaction selection"
        if table_name != "blocks":
            parents = keyed(after_rows, trace_by_key)
            for row, parent in zip(after_rows, parents):
                assert include_failed or parent["success"], f"{case}/{table_name}: failed row selected"
                assert row["transaction_success"] == parent["success"]
                counts["values"] += 1
                if "transaction_status" in row:
                    assert row["transaction_status"] == parent["status"], (case, table_name, row["tx_hash"])
                    counts["values"] += 1
                if table_name == "transactions":
                    assert row["status"] == parent["status"]
        # Legacy values.
        after_legacy = rows(after, legacy)
        if include_failed or table_name == "blocks":
            before_rows = rows(before)
            assert after_legacy == before_rows, f"{case}/{table_name}: legacy values"
        else:
            executed = [r for r in after_legacy if trace_by_key[(r["block_num"], r["tx_hash"])]["status"] == "EXECUTED"]
            assert executed == rows(before), f"{case}/{table_name}: legacy EXECUTED rows"
            newly = [r for r in after_legacy if trace_by_key[(r["block_num"], r["tx_hash"])]["status"] != "EXECUTED"]
            full = rows(read(before_dir.parent / case.replace("failedfalse", "failedtrue"), table_name))
            expected_new = [r for r in full if trace_by_key[(r["block_num"], r["tx_hash"])]["status"] != "EXECUTED"
                            and trace_by_key[(r["block_num"], r["tx_hash"])]["success"]]
            assert newly == expected_new, f"{case}/{table_name}: newly selected rows"
            report.setdefault("newly_selected", {}).setdefault(table_name, len(newly))
        counts["legacy_values"] += len(after_legacy) * len(legacy)
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    traces = expected_traces(args.raw, load_block_class(args.descriptor))
    after_manifest = json.loads((args.after / "manifest.json").read_text())
    report = {"blocks": after_manifest["blocks"], "cases": {}, "totals": {"values": 0, "legacy_values": 0, "rows": 0}}
    roles = {}
    for trace in traces:
        key = f"{trace['status']}|success={trace['success']}|onerror={trace['onerror']}"
        roles[key] = roles.get(key, 0) + 1
    report["trace_roles"] = roles
    for case, meta in sorted(after_manifest["cases"].items()):
        counts = compare_case(case, args.before / case, args.after / case, traces, meta["include_failed"], report)
        report["cases"][case] = counts
        for key, value in counts.items():
            report["totals"][key] += value
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"cases": len(report["cases"]), **report["totals"], "trace_roles": roles,
                      "newly_selected": report.get("newly_selected")}, indent=2))


if __name__ == "__main__":
    main()
