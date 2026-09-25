#!/usr/bin/env python3
"""Compare offline mapper replays and derive outcome context from raw protobufs.

Dependencies: pyarrow, protobuf. See 550-solana-execution-context.md.
No provider access; neither expected context nor legacy values are taken from
the new mapper's transaction table.
"""
import argparse
import copy
import hashlib
import json
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


CONTEXT_TABLES = {
    "messages", "instructions", "token_balances", "account_lookups", "rewards"
}


def read_table(root, case, table):
    paths = sorted((root / case / table).glob("*.parquet"))
    assert paths, (root, case, table)
    return pa.concat_tables([pq.ParquetFile(path).read() for path in paths])


def token_snapshot(balances):
    return sorted(
        (x.account_index, x.mint, x.owner, x.program_id,
         x.ui_token_amount.SerializeToString(deterministic=True))
        for x in balances
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--raw", type=Path, action="append", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    pool = descriptor_pool.DescriptorPool()
    descriptors = descriptor_pb2.FileDescriptorSet.FromString(args.descriptor.read_bytes())
    for file in descriptors.file:
        pool.Add(file)
    block_type = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("sf.solana.type.v1.Block")
    )
    source = {}
    hashes = {}
    raw_counts = Counter()
    for path in args.raw:
        raw = path.read_bytes()
        block = block_type.FromString(raw)
        assert str(block.slot) not in hashes, "duplicate raw slot"
        hashes[str(block.slot)] = hashlib.sha256(raw).hexdigest()
        for index, tx in enumerate(block.transactions):
            assert tx.HasField("meta"), "sample includes unknown outcome"
            success = not bool(tx.meta.err.err)
            source[block.slot, index] = (success, tx)
            raw_counts["transactions"] += 1
            if not success:
                raw_counts["failed_transactions"] += 1
                raw_counts["failed_with_inner_calls"] += bool(tx.meta.inner_instructions)
                assert token_snapshot(tx.meta.pre_token_balances) == token_snapshot(tx.meta.post_token_balances)
                assert len(tx.meta.pre_balances) == len(tx.meta.post_balances)
                changes = [(i, after - before) for i, (before, after) in enumerate(
                    zip(tx.meta.pre_balances, tx.meta.post_balances)) if before != after]
                assert changes == [(0, -tx.meta.fee)], (block.slot, index, changes)

    before = json.loads((args.before / "manifest.json").read_text())
    after = json.loads((args.after / "manifest.json").read_text())
    assert before["raw_sha256"] == after["raw_sha256"] == hashes
    assert before["cases"].keys() == after["cases"].keys()
    assert len(before["cases"]) == 40
    cases = {}
    for case, old_manifest in before["cases"].items():
        new_manifest = after["cases"][case]
        old_settings = {k: v for k, v in old_manifest.items() if k != "schemas"}
        new_settings = {k: v for k, v in new_manifest.items() if k != "schemas"}
        assert old_settings == new_settings, (case, "row selection/settings changed")
        assert old_manifest["schemas"].keys() == new_manifest["schemas"].keys()
        tables = {}
        for name, old_schema in old_manifest["schemas"].items():
            new_schema = copy.deepcopy(new_manifest["schemas"][name])
            if name in CONTEXT_TABLES:
                added = new_schema["fields"].pop()
                assert added["name"] == "transaction_success"
                assert added["data_type"] == "Boolean"
                assert added["nullable"] == (name == "rewards")
            assert old_schema == new_schema, (case, name, "legacy Arrow schema changed")
            old = read_table(args.before, case, name)
            new = read_table(args.after, case, name)
            assert old.equals(new.select(old.column_names), check_metadata=True), (
                case, name, "legacy values, row order, nulls or metadata changed"
            )
            contexts = Counter()
            if name in CONTEXT_TABLES:
                for row in new.to_pylist():
                    index = row["transaction_index"]
                    if index is None:
                        assert name == "rewards" and row["source"] == "block"
                        assert row["transaction_success"] is None
                        contexts["block_null"] += 1
                        continue
                    expected, raw_tx = source[row["slot"], index]
                    assert row["transaction_success"] is expected, (case, name, index)
                    assert expected or new_manifest["include_failed"]
                    contexts["success" if expected else "failed"] += 1
                    if name == "rewards":
                        assert row["source"] == "transaction"
                    if name == "token_balances":
                        balances = (raw_tx.meta.pre_token_balances if row["balance_type"] == "pre"
                                    else raw_tx.meta.post_token_balances)
                        assert row["balance_type"] in {"pre", "post"}
                        raw_balance = balances[row["balance_index"]]
                        assert row["account_index"] == raw_balance.account_index
                        assert row["amount"] == raw_balance.ui_token_amount.amount
                        assert (row["mint"], row["owner"], row["program_id"]) == (
                            raw_balance.mint, raw_balance.owner, raw_balance.program_id
                        )
            tables[name] = {
                "rows": new.num_rows, "legacy_columns": old.num_columns,
                "context_counts": dict(contexts),
            }
        cases[case] = tables
    report = {
        "raw_sha256": hashes,
        "raw_counts": dict(raw_counts),
        "baseline_input_api": "owned" if before.get("owned", False) else "borrowed",
        "candidate_input_api": "owned" if after.get("owned", False) else "borrowed",
        "cases": cases,
        "case_count": len(cases),
        "table_comparisons": sum(len(tables) for tables in cases.values()),
        "compared_rows": sum(table["rows"] for tables in cases.values() for table in tables.values()),
        "all_legacy_schemas_and_values_equal": True,
        "all_context_values_match_raw_metadata": True,
        "scope": "Offline replay of retained source blocks; no new transport or per-instruction result claim",
    }
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "cases"}, indent=2))


if __name__ == "__main__":
    main()
