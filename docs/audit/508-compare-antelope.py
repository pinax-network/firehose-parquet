"""Read-only raw EOS / before-and-after Parquet qualification for issue #508.

Requires Python protobuf, a protoc --include_imports descriptor for antelope.proto,
and DuckDB. This script makes no network requests and never reads opaque cursors.
"""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--raw", type=Path, required=True)
p.add_argument("--descriptor", type=Path, required=True)
p.add_argument("--baseline", type=Path, required=True, help="Baseline eos dataset root")
p.add_argument("--dataset", type=Path, required=True, help="Updated eos dataset root")
p.add_argument("--cursor", type=Path, required=True)
p.add_argument("--duckdb", default="duckdb")
args = p.parse_args()
pool = descriptor_pool.DescriptorPool()
for file in descriptor_pb2.FileDescriptorSet.FromString(args.descriptor.read_bytes()).file:
    pool.Add(file)
Block = message_factory.GetMessageClass(pool.FindMessageTypeByName("sf.antelope.type.v1.Block"))
raw = (args.raw / "block.pb").read_bytes()
block = Block.FromString(raw)
meta = json.loads((args.raw / "metadata.json").read_text())
assert hashlib.sha256(raw).hexdigest() == meta["sha256"]
assert block.number == meta["block_num"] == 400000000
assert block.id == meta["block_id"] and block.header.previous == meta["parent_id"]
assert block.header.timestamp.seconds == meta["timestamp"]
assert block.header.timestamp.nanos == meta["timestamp_nanos"]
assert not block.filtering_applied


def sql(query):
    return json.loads(subprocess.check_output(
        [args.duckdb, "-json", "-c", "SET TimeZone='UTC'; " + query], text=True))


def quoted(path):
    return "'" + str(path).replace("'", "''") + "'"


def relation(root, table):
    return "read_parquet(" + quoted(root / table / "**/*.parquet") + ")"


tables = ["blocks", "transactions", "actions", "db_ops"]
current = {}
for table in tables:
    current[table] = sql("SELECT * FROM " + relation(args.dataset, table))
    baseline = sql("SELECT * FROM " + relation(args.baseline, table))
    prior = [{k: v for k, v in row.items()
              if table != "db_ops" or k not in {"tx_hash", "tx_index", "db_op_index"}}
             for row in current[table]]
    assert sorted(map(lambda r: json.dumps(r, sort_keys=True), baseline)) == sorted(
        map(lambda r: json.dumps(r, sort_keys=True), prior)), table
    for row in current[table]:
        assert int(row["block_num"]) == block.number
        assert row["block_id"] == block.id and row["parent_id"] == block.header.previous

traces = [t for t in block.unfiltered_transaction_traces if t.receipt.status == 1]
assert [len(current[t]) for t in tables] == [
    1, len(traces), sum(len(t.action_traces) for t in traces), sum(len(t.db_ops) for t in traces)]
ops = {(r["tx_hash"], int(r["tx_index"]), int(r["db_op_index"])): r for r in current["db_ops"]}
assert len(ops) == len(current["db_ops"])
for trace in traces:
    for index, op in enumerate(trace.db_ops):
        row = ops[(trace.id, trace.index, index)]
        assert int(row["action_index"]) == op.action_index
        expected_label = {0: "UNKNOWN", 1: "INSERT", 2: "UPDATE", 3: "REMOVE"}.get(op.operation, "UNKNOWN")
        assert row["operation"] == expected_label
        for field in ["code", "scope", "table_name", "primary_key", "old_payer", "new_payer"]:
            assert row[field] == getattr(op, field), field
        for field in ["old_data", "new_data"]:
            assert row[field] == (getattr(op, field).hex() or None), field
        for field in ["old_data_json", "new_data_json"]:
            assert row[field] == (getattr(op, field) or None), field

joined = sql("SELECT count(*) AS n FROM " + relation(args.dataset, "db_ops") + " d JOIN "
             + relation(args.dataset, "transactions")
             + ' t ON d.block_id=t.block_id AND d.tx_hash=t.tx_hash AND d.tx_index=t."index"')
assert int(joined[0]["n"]) == len(ops)
cursor = sql("SELECT last_block_num FROM read_parquet(" + quoted(args.cursor) + ")")
assert int(cursor[0]["last_block_num"]) == block.number
print(json.dumps({"block": block.number, "sha256": meta["sha256"],
                  "rows": {name: len(rows) for name, rows in current.items()},
                  "old_columns_equal": True, "raw_db_operations_equal": True,
                  "joined_db_rows": len(ops), "cursor_last_block": block.number}, indent=2))
