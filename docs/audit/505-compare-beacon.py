#!/usr/bin/env python3
"""Compare one bounded Deneb sample against raw source and pre-change Parquet.

Usage: 505-compare-beacon.py RAW_JSON OLD_CHAIN_ROOT NEW_CHAIN_ROOT
Requires DuckDB CLI; reads local files only. Never reads/displays cursor values.
"""
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def query(sql):
    return json.loads(subprocess.check_output(["duckdb", "-json", "-c", sql], text=True))


def rows(root, table, new):
    files = sorted((root / table).glob("*.parquet"))
    assert files, table
    select = "*"
    if new and table == "blob_sidecars":
        select = "* REPLACE ('0x' || lower(hex(blob)) AS blob)"
    return query(f"SELECT {select} FROM read_parquet([{','.join(map(sql_literal, files))}]);")


def main(raw_path, old_root, new_root):
    response = json.loads(raw_path.read_text())
    assert "cursor" not in response
    block = response["block"]
    assert block["spec"] == "DENEB"
    body = block["deneb"]
    # Proven producer representation, independently decoded in Python.
    raw_fee = base64.b64decode(body["executionPayload"]["baseFeePerGas"])
    expected_fee = str(int.from_bytes(raw_fee, "big"))
    tables = sorted(path.name for path in old_root.iterdir() if path.is_dir() and list(path.glob("*.parquet")))
    assert tables == sorted(path.name for path in new_root.iterdir() if path.is_dir() and list(path.glob("*.parquet")))
    counts = {}
    for table in tables:
        old = rows(old_root, table, False)
        new = rows(new_root, table, True)
        if table == "execution_payload":
            assert len(old) == len(new) == 1
            assert old[0]["base_fee_per_gas"] == "0x" + raw_fee.hex()
            assert new[0]["base_fee_per_gas"] == expected_fee
            old[0]["base_fee_per_gas"] = expected_fee
        assert new == old, (table, "changed row/column/value beyond declared fee/blob conversion")
        counts[table] = {"rows": len(new), "columns": len(new[0]), "values": len(new) * len(new[0])}
    blobs = rows(new_root, "blob_sidecars", True)
    assert len(blobs) == len(body["embeddedBlobs"])
    hashes = []
    for row, blob in zip(blobs, body["embeddedBlobs"]):
        value = base64.b64decode(blob["blob"])
        assert row["blob"] == "0x" + value.hex()
        assert int(row["blob_index"]) == int(blob.get("index", 0))
        assert len(value) == 131_072
        for column, field in [("kzg_commitment", "kzgCommitment"), ("kzg_proof", "kzgProof")]:
            assert row[column] == "0x" + base64.b64decode(blob[field]).hex()
        hashes.append(hashlib.sha256(value).hexdigest())
    blob_file = next((new_root / "blob_sidecars").glob("*.parquet"))
    blob_schema = query(f"DESCRIBE SELECT * FROM read_parquet({sql_literal(blob_file)});")
    types = {row["column_name"]: row["column_type"] for row in blob_schema}
    assert types["blob"] == "BLOB"
    assert types["kzg_commitment"] == types["kzg_proof"] == "VARCHAR"
    cursor = next(new_root.glob("cursor.parquet"))
    # Only the public checkpoint height, never the opaque provider token.
    saved = query(f"SELECT last_block_num FROM read_parquet({sql_literal(cursor)});")
    assert len(saved) == 1 and int(saved[0]["last_block_num"]) == int(block["slot"])
    print(json.dumps({"slot": int(block["slot"]), "execution_block": int(body["executionPayload"]["blockNumber"]), "base_fee_per_gas_wei": expected_fee, "blob_count": len(blobs), "blob_bytes_each": 131_072, "blob_sha256": hashes, "tables": counts, "all_other_values_unchanged": True, "checkpoint_height_matches": True}, indent=2))


if __name__ == "__main__":
    assert len(sys.argv) == 4, __doc__
    main(*map(Path, sys.argv[1:]))
