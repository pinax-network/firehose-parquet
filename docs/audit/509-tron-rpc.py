#!/usr/bin/env python3
"""Bounded native Tron capture and pinned-producer conversion; see the audit plan.

Only `capture` opens a network channel. It makes at most two fixed unary reads,
with no credentials, retry, reflection, head lookup, or endpoint fallback.
"""
import argparse
import hashlib
import importlib.metadata
import json
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

HEIGHT = 80_000_000
ENDPOINT = "grpc.trongrid.io:50052"
METHODS = (
    "/protocol.WalletSolidity/GetBlockByNum2",
    "/protocol.WalletSolidity/GetTransactionInfoByBlockNum",
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def classes(path):
    pool = descriptor_pool.DescriptorPool()
    for file in descriptor_pb2.FileDescriptorSet.FromString(Path(path).read_bytes()).file:
        pool.Add(file)
    return lambda name: message_factory.GetMessageClass(pool.FindMessageTypeByName(name))


def varint(data, offset):
    value = 0
    for shift in range(0, 70, 7):
        require(offset < len(data), "truncated varint")
        byte = data[offset]
        offset += 1
        value |= (byte & 127) << shift
        if byte < 128:
            require(value < 1 << 64, "oversized varint")
            return value, offset
    raise ValueError("oversized varint")


def fields(data):
    """Retain exact nested wire bytes for hash verification; reject malformed wire."""
    offset = 0
    while offset < len(data):
        tag, offset = varint(data, offset)
        number, wire = tag >> 3, tag & 7
        require(0 < number < 1 << 29, "invalid field number")
        if wire == 0:
            value, offset = varint(data, offset)
        else:
            if wire == 2:
                size, offset = varint(data, offset)
            elif wire in (1, 5):
                size = 8 if wire == 1 else 4
            else:
                raise ValueError("unsupported wire group/type")
            require(size <= len(data) - offset, "truncated field")
            value = data[offset:offset + size]
            offset += size
        yield number, wire, value


def nested(data, number, required=True):
    found = [(wire, value) for tag, wire, value in fields(data) if tag == number]
    require(len(found) <= 1, f"ambiguous repeated singular field {number}")
    require(bool(found) or not required, f"missing field {number}")
    if not found:
        return None
    require(found[0][0] == 2, "wrong message/bytes wire type")
    return found[0][1]


def validate_block(raw, cls):
    block = cls("protocol.BlockExtention").FromString(raw)
    require(block.HasField("block_header") and block.block_header.HasField("raw_data"), "missing header")
    header = block.block_header.raw_data
    require(header.number == HEIGHT, "wrong source height")
    header_wire = nested(nested(raw, 2), 1)
    require(len(block.blockid) == 32, "invalid block ID length")
    require(block.blockid == HEIGHT.to_bytes(8, "big") + hashlib.sha256(header_wire).digest()[8:], "block ID mismatch")
    require(nested(raw, 3) == block.blockid, "ambiguous block ID")
    require(len(header.parentHash) == 32 and int.from_bytes(header.parentHash[:8], "big") == HEIGHT - 1, "parent ID mismatch")
    require(header.timestamp > 0, "invalid block timestamp")
    tx_wires = [value for tag, wire, value in fields(raw) if tag == 1 and wire == 2]
    require(len(tx_wires) == len(block.transactions), "transaction wire count mismatch")
    ids = set()
    for tx, wire in zip(block.transactions, tx_wires):
        require(tx.HasField("transaction") and tx.transaction.HasField("raw_data") and tx.HasField("result"), "missing transaction envelope")
        raw_data = nested(nested(wire, 1), 1)
        require(len(tx.txid) == 32 and hashlib.sha256(raw_data).digest() == tx.txid, "transaction ID mismatch")
        require(nested(wire, 2) == tx.txid, "ambiguous transaction ID")
        nested(wire, 4)
        require(tx.txid not in ids, "duplicate transaction ID")
        ids.add(tx.txid)
    return block


def convert(block_raw, infos_raw, cls):
    block = validate_block(block_raw, cls)
    infos = cls("protocol.TransactionInfoList").FromString(infos_raw)
    info_wires = [value for tag, wire, value in fields(infos_raw) if tag == 1 and wire == 2]
    require(len(block.transactions) == len(infos.transactionInfo) == len(info_wires), "receipt count mismatch")
    header = block.block_header.raw_data
    out = cls("sf.tron.type.v1.Block")()
    out.id = block.blockid
    out.header.number = header.number
    out.header.parent_number = header.number - 1
    out.header.tx_trie_root = header.txTrieRoot
    out.header.witness_address = header.witness_address
    out.header.parent_hash = header.parentHash
    out.header.version = header.version & 0xFFFFFFFF
    out.header.timestamp = header.timestamp
    out.header.witness_signature = block.block_header.witness_signature
    for source, info, info_wire in zip(block.transactions, infos.transactionInfo, info_wires):
        require(nested(info_wire, 1) == info.id == source.txid, "ordered receipt ID mismatch")
        nested(info_wire, 7, required=False)
        require(info.blockNumber == HEIGHT and info.blockTimeStamp == header.timestamp, "receipt block mismatch")
        tx = out.transactions.add()
        tx.txid = source.txid
        tx.signature.extend(source.transaction.signature)
        raw = source.transaction.raw_data
        for name in ("ref_block_bytes", "ref_block_hash", "expiration", "timestamp"):
            setattr(tx, name, getattr(raw, name))
        tx.contract_result.extend(source.constant_result)
        tx.result = source.result.result
        tx.code = source.result.code
        tx.message = source.result.message
        tx.energy_used = source.energy_used
        tx.energy_penalty = source.energy_penalty
        tx.info.CopyFrom(info)
        for contract in raw.contract:
            tx.contracts.add().CopyFrom(contract)
    identity = {
        "block_num": HEIGHT, "block_id": block.blockid.hex(),
        "parent_num": HEIGHT - 1, "parent_id": header.parentHash.hex(),
        "timestamp": header.timestamp // 1000,
        "timestamp_ns": header.timestamp % 1000 * 1_000_000,
        "lib_num": HEIGHT - 20,
        "lib_semantics": "pinned producer compatibility; not independent finality proof",
    }
    return out, identity


def write_conversion(root, cls):
    out, identity = convert((root / "block-extension.pb").read_bytes(), (root / "transaction-info-list.pb").read_bytes(), cls)
    (root / "block.pb").write_bytes(out.SerializeToString())
    (root / "identity.json").write_text(json.dumps(identity, indent=2) + "\n")
    return len(out.transactions)


def capture(args, cls):
    import grpc
    root = args.output
    root.mkdir(parents=True, exist_ok=False)
    now = lambda: datetime.now(timezone.utc).isoformat()
    report = {"endpoint": ENDPOINT, "height": HEIGHT, "transport": "public plaintext Solidity gRPC", "credentials": "none", "calls": [], "status": "started", "started_at": now(), "grpcio": grpc.__version__, "protobuf": importlib.metadata.version("protobuf"), "descriptor_sha256": hashlib.sha256(args.descriptor.read_bytes()).hexdigest(), "protocol_commit": "2a678934da3992b1a67f975769bbb2d31989451f", "producer_commit": "d4095accc0dcc8fb4da8c1c1b1e8f4be33921df2"}
    def save():
        (root / "capture.json").write_text(json.dumps(report, indent=2) + "\n")
    def deadline(_signum, _frame):
        raise TimeoutError("50-second process deadline")
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(50)
    save()
    channel = grpc.insecure_channel(ENDPOINT, options=[
        ("grpc.enable_retries", 0), ("grpc.service_config_disable_resolution", 1),
        ("grpc.enable_http_proxy", 0), ("grpc.max_receive_message_length", 32 * 1024 * 1024),
    ])
    try:
        request = cls("protocol.NumberMessage")(num=HEIGHT).SerializeToString()
        for index, (method, name) in enumerate(zip(METHODS, ("block-extension.pb", "transaction-info-list.pb"))):
            entry = {"method": method, "status": "started", "started_at": now()}
            report["calls"].append(entry)
            save()
            started = time.monotonic()
            response = channel.unary_unary(method)(request, timeout=20, wait_for_ready=False, metadata=())
            (root / name).write_bytes(response)
            entry.update(status="received", bytes=len(response), sha256=hashlib.sha256(response).hexdigest(), completed_at=now(), elapsed_seconds=time.monotonic()-started)
            save()
            if index == 0:
                validate_block(response, cls)
        report["transactions"] = write_conversion(root, cls)
        report["status"] = "converted"
        report["completed_at"] = now()
        save()
    except Exception as error:
        report["status"] = "failed"
        report["error_type"] = type(error).__name__
        report["error"] = str(error)[:1000]
        report["completed_at"] = now()
        if report["calls"] and report["calls"][-1]["status"] == "started":
            report["calls"][-1].update(status="failed", completed_at=now())
        save()
        raise
    finally:
        signal.alarm(0)
        channel.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("capture", "convert"))
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cls = classes(args.descriptor)
    if args.operation == "capture":
        capture(args, cls)
    else:
        print("converted transactions:", write_conversion(args.output, cls))


if __name__ == "__main__":
    main()
