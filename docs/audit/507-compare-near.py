#!/usr/bin/env python3
"""Independent #507 NEAR comparison against the original NearData JSON.

1. Producer-compatible replay (`--before` baseline, `--after` candidate) of the
   retained #506 block: every legacy column of every table is unchanged;
   `receipts.success_receipt_id` equals the id in each receipt's original
   `SuccessReceiptId` status (NULL otherwise); `state_changes` is empty with
   the #507 layout.
2. Populated replay (`--populated`) of the same block with the per-shard state
   changes projected by 507-near-state-changes.py: every `state_changes` value
   equals the original JSON, and every other table equals the candidate's
   producer-compatible replay.

Expected values are derived from the JSON only, with the byte encodings of the
reviewed #509 comparator; mapper output is never the oracle.
"""
import argparse
import base64
import importlib.util
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("compare509", HERE / "509-compare-tron-rpc.py")
c509 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c509)

TABLES = ["blocks", "chunks", "transactions", "receipts", "receipt_actions", "execution_logs", "state_changes"]
APPENDED = {"receipts": ["success_receipt_id"]}
STATE_CHANGE_COLUMNS = ["state_change_index", "type", "cause", "cause_tx_hash", "cause_receipt_hash", "account_id",
                        "data_key", "data_value", "amount", "locked", "storage_usage", "code_hash"]
TYPE = {"account_update": "AccountUpdate", "account_deletion": "AccountDeletion",
        "access_key_update": "AccessKeyUpdate", "access_key_deletion": "AccessKeyDeletion",
        "data_update": "DataUpdate", "data_deletion": "DataDeletion",
        "contract_code_update": "ContractCodeUpdate", "contract_code_deletion": "ContractCodeDeletion"}
CAUSE = {"not_writable_to_disk": "NotWritableToDisk", "initial_state": "InitialState",
         "transaction_processing": "TransactionProcessing",
         "action_receipt_processing_started": "ActionReceiptProcessingStarted",
         "action_receipt_gas_reward": "ActionReceiptGasReward", "receipt_processing": "ReceiptProcessing",
         "postponed_receipt": "PostponedReceipt", "updated_delayed_receipts": "UpdatedDelayedReceipts",
         "validator_accounts_update": "ValidatorAccountsUpdate", "migration": "Migration"}
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(text):
    num = 0
    for char in text:
        num = num * 58 + ALPHABET.index(char)
    raw = num.to_bytes((num.bit_length() + 7) // 8, "big") if num else b""
    return b"\0" * (len(text) - len(text.lstrip("1"))) + raw


def read(case_dir, table):
    files = sorted((case_dir / table).glob("*.parquet"))
    return pa.concat_tables([pq.read_table(f) for f in files]) if files else None


def expected_state_changes(document, mode):
    enc = lambda raw: None if raw is None else c509.encode(raw, mode)
    rows = []
    for index, change in enumerate(c for shard in document["shards"] for c in shard["state_changes"]):
        cause, value, kind = change["cause"], change["change"], change["type"]
        row = {"state_change_index": index, "type": TYPE[kind], "cause": CAUSE[cause["type"]],
               "cause_tx_hash": enc(b58decode(cause["tx_hash"])) if "tx_hash" in cause else None,
               "cause_receipt_hash": enc(b58decode(cause["receipt_hash"])) if "receipt_hash" in cause else None,
               "account_id": value["account_id"], "data_key": None, "data_value": None,
               "amount": None, "locked": None, "storage_usage": None, "code_hash": None}
        if kind in ("data_update", "data_deletion"):
            row["data_key"] = enc(base64.b64decode(value["key_base64"]))
        if kind == "data_update":
            row["data_value"] = enc(base64.b64decode(value["value_base64"]))
        if kind == "account_update":
            row.update(amount=value["amount"], locked=value["locked"], storage_usage=value["storage_usage"],
                       code_hash=enc(b58decode(value["code_hash"])))
        rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ["before", "after", "populated", "json", "report"]:
        parser.add_argument("--" + arg, type=Path, required=True)
    args = parser.parse_args()
    document = json.loads(args.json.read_text())
    success_target = {}
    for shard in document["shards"]:
        for item in shard["receipt_execution_outcomes"]:
            status = item["execution_outcome"]["outcome"]["status"]
            success_target[item["receipt"]["receipt_id"]] = status.get("SuccessReceiptId")
    manifest = json.loads((args.after / "manifest.json").read_text())
    cases = sorted(manifest["cases"])
    id_by_index = {r["receipt_index"]: r["receipt_id"] for r in read(args.after / "base58-failedfalse-forkfalse", "receipts").to_pylist()}
    report = {"cases": {}, "totals": {"legacy_values": 0, "success_receipt_id_values": 0,
                                      "state_change_values": 0, "populated_other_table_rows": 0}}
    source = [c for shard in document["shards"] for c in shard["state_changes"]]
    report["source_state_changes"] = {"total": len(source),
                                      "types": {}, "causes": {}}
    for change in source:
        report["source_state_changes"]["types"][change["type"]] = report["source_state_changes"]["types"].get(change["type"], 0) + 1
        report["source_state_changes"]["causes"][change["cause"]["type"]] = report["source_state_changes"]["causes"].get(change["cause"]["type"], 0) + 1
    report["success_receipt_id_receipts"] = sum(1 for v in success_target.values() if v)
    for case in cases:
        mode = manifest["cases"][case]["encoding"]
        counts = {"legacy_values": 0, "success_receipt_id_values": 0, "state_change_values": 0}
        for table in TABLES:
            before, after = read(args.before / case, table), read(args.after / case, table)
            if table == "state_changes":
                assert before is None and after is None, f"{case}: producer-compatible state changes are empty"
                schema = manifest["cases"][case]["schemas"]["state_changes"]
                names = [f["name"] for f in schema["fields"]]
                assert names[7:7 + len(STATE_CHANGE_COLUMNS)] == STATE_CHANGE_COLUMNS, names
                continue
            added = APPENDED.get(table, [])
            legacy = [n for n in after.schema.names if n not in added]
            if added:
                assert after.schema.names[-len(added):] == added
            assert pa.schema([after.schema.field(n) for n in legacy]).equals(before.schema, check_metadata=False), (case, table)
            assert after.select(legacy).to_pylist() == before.to_pylist(), (case, table, "legacy values")
            counts["legacy_values"] += after.num_rows * len(legacy)
            if table == "receipts":
                for row in after.select(["receipt_index", "success_receipt_id"]).to_pylist():
                    target = success_target[id_by_index[row["receipt_index"]]]
                    expected = None if target is None else c509.encode(b58decode(target), mode)
                    assert row["success_receipt_id"] == expected, (case, row)
                    counts["success_receipt_id_values"] += 1
        # Populated replay: state changes against JSON; other tables unchanged.
        populated = read(args.populated / case, "state_changes")
        actual = populated.select(STATE_CHANGE_COLUMNS).to_pylist()
        expected = expected_state_changes(document, mode)
        assert len(actual) == len(expected), (case, len(actual), len(expected))
        for got, want in zip(actual, expected):
            assert got == want, (case, {k: (got[k], want[k]) for k in want if got[k] != want[k]})
        counts["state_change_values"] += len(actual) * len(STATE_CHANGE_COLUMNS)
        for table in TABLES:
            if table == "state_changes":
                continue
            assert read(args.populated / case, table).to_pylist() == read(args.after / case, table).to_pylist(), (case, table)
            report["totals"]["populated_other_table_rows"] += read(args.after / case, table).num_rows
        report["cases"][case] = counts
        for key, value in counts.items():
            report["totals"][key] += value
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"cases": len(cases), **report["totals"], "source_state_changes": report["source_state_changes"],
                      "success_receipt_id_receipts": report["success_receipt_id_receipts"]}, indent=2))


if __name__ == "__main__":
    main()
