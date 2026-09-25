"""Read-only comparison of raw Solana blocks and old/new payload schemas.

Requires Python protobuf and DuckDB. No network access or dataset mutation.
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
p.add_argument("--baseline", type=Path, required=True)
p.add_argument("--dataset", type=Path, required=True)
p.add_argument("--cursor", type=Path, required=True)
p.add_argument("--duckdb", default="duckdb")
args = p.parse_args()
pool = descriptor_pool.DescriptorPool()
for file in descriptor_pb2.FileDescriptorSet.FromString(args.descriptor.read_bytes()).file:
    pool.Add(file)
Block = message_factory.GetMessageClass(pool.FindMessageTypeByName("sf.solana.type.v1.Block"))
alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def base58(value):
    n = 0
    for char in value:
        n = n * 58 + alphabet.index(char)
    return b"\0" * (len(value) - len(value.lstrip("1"))) + n.to_bytes((n.bit_length() + 7) // 8, "big")


def quoted(path):
    return "'" + str(path).replace("'", "''") + "'"


def sql(query):
    return json.loads(subprocess.check_output(
        [args.duckdb, "-json", "-c", "SET TimeZone='UTC'; " + query], text=True))


changed = {
    "transactions": {"err": "binary", "return_data": "binary"},
    "vote_transactions": {"err": "binary", "return_data": "binary"},
    "instructions": {"data": "binary", "accounts": "indices"},
    "account_lookups": {"writable_indexes": "indices", "readonly_indexes": "indices"},
}
tables = ["blocks", "transactions", "vote_transactions", "messages", "instructions",
          "rewards", "token_balances", "account_lookups"]
current = {}
for table in tables:
    fields = changed.get(table, {})
    selection = "*"
    if fields:
        selection += " EXCLUDE(" + ",".join(fields) + ")"
        for name, kind in fields.items():
            selection += f", {'hex' if kind == 'binary' else 'to_json'}({name}) AS {name}"
    now = sql("SELECT " + selection + " FROM read_parquet(" + quoted(args.dataset / table / "**/*.parquet") + ")")
    old = sql("SELECT * FROM read_parquet(" + quoted(args.baseline / table / "**/*.parquet") + ")")
    for row in old:
        for name, kind in fields.items():
            value = row[name]
            if value is not None:
                raw = base58(value)
                row[name] = raw.hex().upper() if kind == "binary" else list(raw)
    assert sorted(json.dumps(row, sort_keys=True) for row in now) == sorted(
        json.dumps(row, sort_keys=True) for row in old), table
    current[table] = now

raw_blocks = {}
hashes = {}
for path in sorted(args.raw.glob("*.pb")):
    data = path.read_bytes()
    block = Block.FromString(data)
    metadata = json.loads(path.with_suffix(".json").read_text())
    digest = hashlib.sha256(data).hexdigest()
    assert digest == metadata["sha256"] and block.slot == metadata["slot"]
    raw_blocks[block.slot] = block
    hashes[block.slot] = digest
assert set(raw_blocks) == {300000000, 300000001}


def source(row):
    return raw_blocks[int(row["slot"])].transactions[int(row["transaction_index"])]


errors = returned = 0
for table in ["transactions", "vote_transactions"]:
    for row in current[table]:
        confirmed = source(row)
        meta = confirmed.meta
        assert row["err"] == (meta.err.err.hex().upper() if meta.err.err else None)
        assert row["return_data"] == (meta.return_data.data.hex().upper() if meta.HasField("return_data") else None)
        assert base58(row["signature"]) == confirmed.transaction.signatures[0]
        errors += row["err"] is not None
        returned += row["return_data"] is not None
for row in current["instructions"]:
    confirmed = source(row)
    position = int(row["instruction_index"])
    ordered = list(confirmed.transaction.message.instructions)
    ordered.extend(inner for group in confirmed.meta.inner_instructions for inner in group.instructions)
    instruction = ordered[position]
    assert row["data"] == instruction.data.hex().upper()
    assert row["accounts"] == list(instruction.accounts)
for row in current["account_lookups"]:
    lookup = source(row).transaction.message.address_table_lookups[int(row["lookup_index"])]
    assert row["writable_indexes"] == list(lookup.writable_indexes)
    assert row["readonly_indexes"] == list(lookup.readonly_indexes)
    assert base58(row["account_key"]) == lookup.account_key
cursor = sql("SELECT last_block_num FROM read_parquet(" + quoted(args.cursor) + ")")
assert int(cursor[0]["last_block_num"]) == 300000001
print(json.dumps({"raw_sha256": hashes, "counts": {t: len(current[t]) for t in tables},
                  "all_previous_values_equal_after_payload_decoding": True,
                  "all_changed_values_equal_raw_protobuf": True,
                  "error_payloads": errors, "present_return_data": returned,
                  "cursor_last_block": 300000001}, indent=2))
