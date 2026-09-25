#!/usr/bin/env python3
"""Offline comparison for the two bounded samples in 504-beacon-qualification.md.

Inputs are cursor-stripped grpcurl JSON plus fireparq output directories. Requires
DuckDB CLI on PATH; performs no network calls and prints counts, never cursors.
"""
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys


def hex_bytes(value):
    return "0x" + base64.b64decode(value).hex()


def read_rows(root, slot, table):
    files = sorted((root / f"out-{slot}").glob(f"mainnet-cl/{table}/*.parquet"))
    if not files:
        return []
    paths = ",".join("'" + str(p).replace("'", "''") + "'" for p in files)
    result = subprocess.check_output(
        ["duckdb", "-json", "-c", f"SELECT * FROM read_parquet([{paths}]);"],
        text=True,
    )
    return json.loads(result)


def canonical(response):
    metadata, block = response["metadata"], response["block"]
    assert int(metadata["num"]) == int(block["slot"])
    assert metadata["id"] == hex_bytes(block["root"])
    assert metadata["parentId"] == hex_bytes(block["parentRoot"])
    assert int(metadata["parentNum"]) == int(block["parentSlot"])
    assert metadata["time"] == block["timestamp"]
    return {
        "block_num": int(metadata["num"]), "block_id": metadata["id"],
        "parent_num": int(metadata["parentNum"]), "parent_id": metadata["parentId"],
        "lib_num": int(metadata["libNum"]), "timestamp": metadata["time"],
        "date": metadata["time"][:10],
    }


def equal_rows(actual, expected, label):
    assert len(actual) == len(expected), (label, "row count", len(actual), len(expected))
    for row, want in zip(actual, expected):
        assert set(row) == set(want), (label, "column set")
        for key, value in want.items():
            got = row[key]
            if isinstance(value, int):
                got = int(got)
            elif isinstance(value, list):
                got = json.loads(got) if isinstance(got, str) else got
                got = [int(item) for item in got]
            elif key == "timestamp":
                got = datetime.fromisoformat(got).astimezone(timezone.utc)
                value = datetime.fromisoformat(value).astimezone(timezone.utc)
            assert got == value, (label, key, "value mismatch")
    print(f"{label}: {len(expected)} rows, all {len(expected[0])} columns match")


def main(root):
    for slot, fork, table in [
        (10597349, "deneb", "bls_to_execution_changes"),
        (15038051, "fusaka", "attester_slashings"),
    ]:
        response = json.loads((root / f"raw-{slot}.json").read_text())
        assert "cursor" not in response
        block = response["block"]
        body = block[fork]
        common = canonical(response)
        block_row = dict(common, slot=slot, parent_slot=int(block["parentSlot"]),
                         proposer_index=int(block["proposerIndex"]), spec=block["spec"])
        for col, key in [("root", "root"), ("parent_root", "parentRoot"),
                         ("state_root", "stateRoot"), ("body_root", "bodyRoot"),
                         ("signature", "signature")]:
            block_row[col] = hex_bytes(block[key])
        block_row["graffiti"] = hex_bytes(body["graffiti"])
        equal_rows(read_rows(root, slot, "blocks"), [block_row], f"{slot} blocks")
        expected = []
        if table == "bls_to_execution_changes":
            for index, change in enumerate(body["blsToExecutionChanges"]):
                message = change["message"]
                expected.append(dict(common, block_slot=slot, change_index=index,
                    validator_index=int(message["validatorIndex"]),
                    from_bls_pubkey=hex_bytes(message["fromBlsPubKey"]),
                    to_execution_address=hex_bytes(message["toExecutionAddress"]),
                    signature=hex_bytes(change["signature"])))
        else:
            for index, slashing in enumerate(body["attesterSlashings"]):
                row = dict(common, block_slot=slot, slashing_index=index)
                for number in [1, 2]:
                    attestation = slashing[f"attestation{number}"]
                    data = attestation["data"]
                    prefix = f"attestation_{number}_"
                    row[prefix + "attesting_indices"] = [int(x) for x in attestation["attestingIndices"]]
                    row[prefix + "slot"] = int(data["slot"])
                    row[prefix + "committee_index"] = int(data.get("committeeIndex", 0))
                    row[prefix + "beacon_block_root"] = hex_bytes(data["beaconBlockRoot"])
                    for checkpoint in ["source", "target"]:
                        row[prefix + checkpoint + "_epoch"] = int(data[checkpoint]["epoch"])
                        row[prefix + checkpoint + "_root"] = hex_bytes(data[checkpoint]["root"])
                expected.append(row)
            first, second = [expected[0][f"attestation_{n}_attesting_indices"] for n in [1, 2]]
            assert sorted(set(first) & set(second)) == [1731581]
            print(f"{slot} ordered attesting lists: {len(first)} and {len(second)} entries; intersection [1731581]")
        equal_rows(read_rows(root, slot, table), expected, f"{slot} {table}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
