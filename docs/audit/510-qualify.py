#!/usr/bin/env python3
"""Offline RPC fixture -> minimal Cosmos v2 replay; independent Parquet value checks.

prepare FIXTURE_DIR BLOCK.pb
check FIXTURE_DIR PARQUET_DIR --duckdb /path/to/duckdb
No network calls, SDK generated types, or mapper-derived expectations.
"""
import argparse
import base64
import datetime
import hashlib
import json
from pathlib import Path
import subprocess


def varint(value):
    value &= (1 << 64) - 1
    out = bytearray()
    while value > 127:
        out.append((value & 127) | 128)
        value >>= 7
    return bytes(out + bytes([value]))


def field(tag, value):
    if isinstance(value, int):
        return varint(tag << 3) + varint(value)
    if isinstance(value, str):
        value = value.encode()
    return varint((tag << 3) | 2) + varint(len(value)) + value


def wire(raw):
    """Small independent protobuf wire reader for the documented SDK field tags."""
    offset = 0

    def integer():
        nonlocal offset
        value = 0
        for shift in range(0, 70, 7):
            byte = raw[offset]
            offset += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ValueError("oversized varint")

    fields = {}
    while offset < len(raw):
        key = integer()
        tag, kind = key >> 3, key & 7
        assert tag
        if kind == 0:
            value = integer()
        else:
            length = integer() if kind == 2 else {1: 8, 5: 4}[kind]
            value = raw[offset:offset + length]
            assert len(value) == length
            offset += length
        fields.setdefault(tag, []).append(value)
    return fields


def one(fields, tag, default=b""):
    return fields.get(tag, [default])[-1]


def nested(fields, tag):
    # Selected live fixture contains at most one occurrence of each singular
    # embedded message. Refuse unsupported message-merge cases in this oracle.
    assert len(fields.get(tag, [])) <= 1
    return wire(one(fields, tag)) if tag in fields else None


def text(fields, tag):
    return one(fields, tag).decode()


def sdk_metadata(raw):
    tx = wire(raw)
    body = nested(tx, 1)
    auth = nested(tx, 2)
    fee = nested(auth, 2) if auth is not None else None
    signers = None
    if auth is not None:
        signers = []
        for encoded in auth.get(1, []):
            signer = wire(encoded)
            key = nested(signer, 1)
            signers.append({
                "public_key_type_url": text(key, 1) if key is not None else None,
                "public_key_value": one(key, 2).hex() if key is not None else None,
                "mode_info": b"".join(signer[2]).hex() if 2 in signer else None,
                "sequence": one(signer, 3, 0),
            })
    coins = None if fee is None else [
        {"denom": text(wire(c), 1), "amount": text(wire(c), 2)}
        for c in fee.get(1, [])
    ]
    metadata = {
        "raw_tx": raw.hex(), "decode_success": True,
        "memo": text(body, 2) if body is not None else None,
        "timeout_height": one(body, 3, 0) if body is not None else None,
        "fee_gas_limit": one(fee, 2, 0) if fee is not None else None,
        "fee_payer": text(fee, 3) if fee is not None else None,
        "fee_granter": text(fee, 4) if fee is not None else None,
        "fee_amount": coins, "signer_infos": signers,
        "signatures": [s.hex() for s in tx.get(3, [])],
    }
    messages = [] if body is None else [
        {"type_url": text(wire(m), 1), "value": one(wire(m), 2).hex()}
        for m in body.get(1, [])
    ]
    return metadata, messages


def load(directory):
    raw_block = (directory / "block.json").read_bytes()
    raw_results = (directory / "results.json").read_bytes()
    assert hashlib.sha256(raw_block).hexdigest() == "cf93c5a7eee0528ebcb2335c73ad4c13fb7f779ed16c345fde71a1f44cdf6b7c"
    assert hashlib.sha256(raw_results).hexdigest() == "d17c80bc3effa3227d8887b455aa8f456a7f206fa7f60e37f1dbe0707dc68a54"
    block = json.loads(raw_block)["result"]
    results = json.loads(raw_results)["result"]
    assert block["block"]["header"]["height"] == results["height"] == "33121486"
    assert len(block["block"]["data"]["txs"]) == len(results["txs_results"]) == 1
    return block, results


def event_proto(event):
    return field(1, event["type"]) + b"".join(
        field(2, field(1, a["key"]) + field(2, a["value"]))
        for a in event.get("attributes") or []
    )


def prepare(block, results, output):
    """Only fields consumed by the mapper; this is not a captured Firehose block."""
    header = block["block"]["header"]
    date, fraction = header["time"].removesuffix("Z").split(".")
    seconds = int(datetime.datetime.fromisoformat(date).replace(tzinfo=datetime.timezone.utc).timestamp())
    timestamp = field(1, seconds) + field(2, int(fraction.ljust(9, "0")))
    height = int(header["height"])
    h = field(2, header["chain_id"]) + field(3, height) + field(4, timestamp)
    h += field(5, field(1, bytes.fromhex(header["last_block_id"]["hash"])))
    for tag, name in [(8, "validators_hash"), (9, "next_validators_hash"), (14, "proposer_address")]:
        h += field(tag, bytes.fromhex(header[name]))
    out = field(1, bytes.fromhex(block["block_id"]["hash"])) + field(2, height)
    out += field(3, timestamp) + field(4, h)
    # The pinned converter's Injective-specific block_bloom reorder is absent
    # here: assert this rather than silently claiming producer equivalence.
    assert not any(e["type"] == "block_bloom" for e in results["finalize_block_events"])
    for event in results["finalize_block_events"]:
        out += field(7, event_proto(event))
    for tx in block["block"]["data"]["txs"]:
        out += field(8, base64.b64decode(tx, validate=True))
    for result in results["txs_results"]:
        tx_result = b"".join(field(tag, value) for tag, value in [
            (1, int(result["code"])), (2, base64.b64decode(result["data"] or "")),
            (3, result["log"]), (4, result["info"]),
            (5, int(result["gas_wanted"])), (6, int(result["gas_used"])),
            (8, result["codespace"]),
        ])
        tx_result += b"".join(field(7, event_proto(e)) for e in result["events"])
        out += field(9, tx_result)
    with output.open("xb") as file:
        file.write(out)
    print(json.dumps({"replay_bytes": len(out), "replay_sha256": hashlib.sha256(out).hexdigest()}))


def check(block, results, directory, duckdb):
    def query(table, columns, order=""):
        path = str((directory / f"{table}.parquet").resolve()).replace("'", "''")
        sql = f"SELECT {columns} FROM read_parquet('{path}') {order}"
        rows = json.loads(subprocess.check_output([duckdb, "-json", "-c", sql]))
        for row in rows:
            for name in ["index", "tx_index", "message_index", "event_index", "attribute_index", "block_num", "num_txs", "tx_decode_failures"]:
                if name in row and row[name] is not None:
                    row[name] = int(row[name])
        return rows

    expected_events = []

    def append_events(events, source, tx_index=None, tx_hash=None):
        for index, event in enumerate(events):
            attrs = event.get("attributes") or []
            for attr_index, attr in enumerate(attrs or [None]):
                expected_events.append({"source": source, "tx_index": tx_index,
                    "tx_hash": tx_hash, "event_index": index, "type": event["type"],
                    "attribute_index": attr_index if attr is not None else None,
                    "key": attr["key"] if attr is not None else None,
                    "value": attr["value"] if attr is not None else None})

    append_events(results["finalize_block_events"], "block")
    expected_txs, expected_messages = [], []
    for index, (encoded, result) in enumerate(zip(block["block"]["data"]["txs"], results["txs_results"])):
        raw = base64.b64decode(encoded, validate=True)
        tx_hash = hashlib.sha256(raw).hexdigest()
        metadata, messages = sdk_metadata(raw)
        expected_txs.append({"index": index, "tx_hash": tx_hash, **metadata,
            **{k: result[k] for k in ["code", "log", "info", "codespace"]},
            **{k: int(result[k]) for k in ["gas_wanted", "gas_used"]}})
        for msg_index, message in enumerate(messages):
            expected_messages.append({"tx_index": index, "tx_hash": tx_hash, "message_index": msg_index, **message})
        append_events(result["events"], "transaction", index, tx_hash)

    transactions = query("transactions", '''"index", lower(hex(tx_hash)) tx_hash,
        lower(hex(raw_tx)) raw_tx, decode_success, memo, timeout_height, fee_gas_limit,
        fee_payer, fee_granter, code, log, info, codespace, gas_wanted, gas_used,
        to_json(fee_amount) fee_amount,
        to_json(list_transform(signer_infos, s -> struct_pack(
            public_key_type_url := s.public_key_type_url,
            public_key_value := lower(hex(s.public_key_value)),
            mode_info := lower(hex(s.mode_info)), sequence := s.sequence))) signer_infos,
        to_json(list_transform(signatures, s -> lower(hex(s)))) signatures''')
    for tx in transactions:
        for name in ["fee_amount", "signer_infos", "signatures"]:
            if isinstance(tx[name], str):
                tx[name] = json.loads(tx[name])
        for name in ["index", "timeout_height", "fee_gas_limit", "code", "gas_wanted", "gas_used"]:
            if tx[name] is not None:
                tx[name] = int(tx[name])
    assert transactions == expected_txs, "transaction metadata/result mismatch"
    messages = query("messages", "tx_index, lower(hex(tx_hash)) tx_hash, message_index, type_url, lower(hex(value)) AS value")
    assert messages == expected_messages, "message payload/index mismatch"
    events = query("events", "source, tx_index, lower(hex(tx_hash)) tx_hash, event_index, type, attribute_index, key, value")
    assert events == expected_events, "event order/attributes/identity mismatch"
    height = int(results["height"])
    for table in ["blocks", "transactions", "events", "messages"]:
        identity = query(table, "DISTINCT block_num, lower(hex(block_id)) id, lower(hex(parent_id)) parent")
        expected = [{"block_num": height, "id": block["block_id"]["hash"].lower(),
            "parent": block["block"]["header"]["last_block_id"]["hash"].lower()}]
        for row in identity:
            row["block_num"] = int(row["block_num"])
        assert identity == expected, f"{table} canonical identity mismatch"
    blocks = query("blocks", "num_txs, tx_decode_failures")
    assert blocks == [{"num_txs": 1, "tx_decode_failures": 0}]
    print(json.dumps({"height": height, "transactions": len(transactions), "messages": len(messages),
        "source_events": len(results["finalize_block_events"]) + sum(len(t["events"]) for t in results["txs_results"]),
        "event_rows": len(events), "all_source_values_match": True}, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["prepare", "check"])
    parser.add_argument("fixture", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--duckdb", default="duckdb")
    args = parser.parse_args()
    block, results = load(args.fixture)
    if args.operation == "prepare":
        prepare(block, results, args.output)
    else:
        check(block, results, args.output, args.duckdb)
