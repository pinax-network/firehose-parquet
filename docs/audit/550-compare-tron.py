#!/usr/bin/env python3
"""Offline independent #550 Tron comparison: native RPC -> every Parquet value.

Expected rows come from the full pinned upstream protocol messages through the
reviewed #509 generator (`509-compare-tron-rpc.py`), then apply the #550
contract independently of the Rust mapper:

- a transaction succeeds when its wrapper result is true, TransactionInfo
  `result` is SUCESS and its receipt result (if any) is DEFAULT or SUCCESS;
- failed transactions are excluded unless failed transactions are included;
- `transaction_success` is appended to transactions, logs,
  internal_transactions, contracts and internal_call_values;
- an empty TransactionInfo `contract_address` is NULL.

Legacy comparison against the same-input baseline: every legacy field and
value is unchanged, except that the empty contract address is now NULL, and by
default the failed transactions' rows are no longer selected.
"""
import argparse
import hashlib
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path

import pyarrow as pa
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

sys.dont_write_bytecode = True  # importing the #509 scripts must not leave caches
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("compare509", HERE / "509-compare-tron-rpc.py")
c509 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c509)

OUTCOME_TABLES = {"transactions", "logs", "internal_transactions", "contracts", "internal_call_values"}


def success(tx, info):
    receipt = info.receipt.result if info.HasField("receipt") else None
    return tx.result.result and info.result == 0 and receipt in (None, 0, 1)


def expected_rows(block, infos, cls, mode, fork, include_failed):
    rows = c509.expected_rows(block, infos, cls, mode, fork, True)
    outcome = {ti: success(tx, info) for ti, (tx, info) in enumerate(zip(block.transactions, infos.transactionInfo))}
    empty_address = {ti for ti, info in enumerate(infos.transactionInfo) if not info.contract_address}
    result = {}
    for table, table_rows in rows.items():
        kept = []
        for row in table_rows:
            if table == "blocks":
                kept.append(row)
                continue
            ti = row["transaction_index"]
            if not include_failed and not outcome[ti]:
                continue
            row = dict(row, transaction_success=outcome[ti])
            if table == "transactions" and ti in empty_address:
                row["contract_address"] = None
            kept.append(row)
        result[table] = kept
    return result, outcome


def legacy_equal(previous, current, table, outcome, include_failed, mode):
    """Baseline rows (legacy selection) against candidate legacy columns."""
    old_rows = c509.normalized_rows(previous) if previous is not None else []
    new_rows = c509.normalized_rows(current.select(previous.column_names)) if current is not None else []
    if not include_failed and table != "blocks":
        old_rows = [row for row in old_rows if outcome[row["transaction_index"]]]
    assert len(old_rows) == len(new_rows), (table, len(old_rows), len(new_rows))
    changed_addresses = 0
    for old, new in zip(old_rows, new_rows):
        if table == "transactions" and new["contract_address"] is None and old["contract_address"] == c509.encode(b"", mode):
            old = dict(old, contract_address=None)
            changed_addresses += 1
        assert old == new, (table, {k: (old[k], new[k]) for k in old if old[k] != new[k]})
    return len(new_rows) * len(previous.column_names) if previous is not None else 0, changed_addresses


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for arg in ["before", "after", "raw", "descriptor", "report"]:
        p.add_argument("--" + arg, type=Path, required=True)
    args = p.parse_args()
    pool = descriptor_pool.DescriptorPool()
    for file in descriptor_pb2.FileDescriptorSet.FromString(args.descriptor.read_bytes()).file:
        pool.Add(file)
    cls = lambda name: message_factory.GetMessageClass(pool.FindMessageTypeByName(name))
    capture = json.loads((args.raw / "capture.json").read_text())
    height = capture["selected_height"]
    raw_block = (args.raw / "block-extension.pb").read_bytes()
    raw_infos = (args.raw / "transaction-info-list.pb").read_bytes()
    block = cls("protocol.BlockExtention").FromString(raw_block)
    infos = cls("protocol.TransactionInfoList").FromString(raw_infos)
    assert block.block_header.raw_data.number == height
    assert len(block.transactions) == len(infos.transactionInfo)
    for tx, info in zip(block.transactions, infos.transactionInfo):
        assert tx.txid == info.id and info.blockNumber == height
    before = json.loads((args.before / "manifest.json").read_text())
    after = json.loads((args.after / "manifest.json").read_text())
    assert before["block_sha256"] == after["block_sha256"] == hashlib.sha256((args.raw / "block.pb").read_bytes()).hexdigest()
    assert before["cases"].keys() == after["cases"].keys() and len(after["cases"]) == 20
    cases, totals = {}, Counter()
    for case, old in before["cases"].items():
        new = after["cases"][case]
        expected, outcome = expected_rows(block, infos, cls, new["encoding"], new["fork"], new["include_failed"])
        tables = {}
        for table, count in new["rows"].items():
            assert count == len(expected[table]), (case, table, "raw row count", count, len(expected[table]))
            schema = new["schemas"][table]
            names = [f["name"] for f in schema["fields"]]
            if table in OUTCOME_TABLES:
                assert names[-1] == "transaction_success", (case, table)
                field = schema["fields"][-1]
                assert field["data_type"] == "Boolean" and field["nullable"] is False
            current = c509.read_table(args.after, case, table, count, schema)
            if count:
                actual = c509.normalized_rows(current)
                assert set(actual[0]) == set(expected[table][0]), (case, table, "column inventory")
                for i, (row, exp) in enumerate(zip(actual, expected[table])):
                    assert row == exp, (case, table, i, {k: (row.get(k), exp.get(k)) for k in row.keys() | exp.keys() if row.get(k) != exp.get(k)})
                totals["raw_values"] += count * len(actual[0])
                totals["outcome_values"] += count if table in OUTCOME_TABLES else 0
            old_schema = old["schemas"][table]
            projected = dict(schema, fields=[f for f in schema["fields"] if f["name"] != "transaction_success"])
            assert projected == old_schema, (case, table, "legacy schema changed")
            previous = c509.read_table(args.before, case, table, old["rows"][table], old_schema)
            if previous is not None or current is not None:
                values, addresses = legacy_equal(previous, current, table, outcome, new["include_failed"], new["encoding"])
                totals["legacy_values"] += values
                totals["null_contract_addresses"] += addresses
            totals["rows"] += count
            tables[table] = {"rows": count, "baseline_rows": old["rows"][table]}
        cases[case] = tables
    failed = [ti for ti, ok in outcome.items() if not ok]
    coverage = {
        "transactions": len(block.transactions),
        "failed_transactions": [
            {"transaction_index": ti, "txid": block.transactions[ti].txid.hex(),
             "wrapper": f"{block.transactions[ti].result.result}/{c509.enum_name(block.transactions[ti].result, 'code')}",
             "info_result": c509.enum_name(infos.transactionInfo[ti], "result"),
             "receipt_result": c509.enum_name(infos.transactionInfo[ti].receipt, "result"),
             "logs": len(infos.transactionInfo[ti].log),
             "internal_transactions": len(infos.transactionInfo[ti].internal_transactions),
             "fee": infos.transactionInfo[ti].fee}
            for ti in failed],
        "wrapper_result_and_code": dict(Counter(f"{tx.result.result}/{c509.enum_name(tx.result, 'code')}" for tx in block.transactions)),
        "transaction_info_result": dict(Counter(c509.enum_name(i, "result") for i in infos.transactionInfo)),
        "receipt_result": dict(Counter(c509.enum_name(i.receipt, "result") if i.HasField("receipt") else "ABSENT" for i in infos.transactionInfo)),
        "empty_contract_address": sum(not i.contract_address for i in infos.transactionInfo),
        "logs": sum(len(i.log) for i in infos.transactionInfo),
        "internal_transactions": sum(len(i.internal_transactions) for i in infos.transactionInfo),
        "call_values": sum(len(t.callValueInfo) for i in infos.transactionInfo for t in i.internal_transactions),
    }
    report = {"status": "passed", "height": height,
              "qualification": "native RPC backed; pinned #509 producer conversion; no Firehose transport or cursor proof",
              "raw_sha256": {"block-extension.pb": hashlib.sha256(raw_block).hexdigest(),
                             "transaction-info-list.pb": hashlib.sha256(raw_infos).hexdigest(),
                             "block.pb": after["block_sha256"]},
              "descriptor_sha256": hashlib.sha256(args.descriptor.read_bytes()).hexdigest(),
              "coverage": coverage, "cases": cases, **dict(totals)}
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "cases"}, indent=2))


if __name__ == "__main__":
    main()
