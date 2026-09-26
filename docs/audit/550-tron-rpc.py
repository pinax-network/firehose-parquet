#!/usr/bin/env python3
"""Bounded native Tron capture of one block with a reverted receipt (#550).

Reuses the reviewed #509 transport and pinned-producer converter
(`509-tron-rpc.py`) unchanged, except that the height is chosen by a bounded
search instead of being fixed:

1. For each listed candidate height, in order, one
   `/protocol.WalletSolidity/GetTransactionInfoByBlockNum`. Stop at the first
   height whose receipts include `REVERT`. At most 10 candidates.
2. For that height only, one `/protocol.WalletSolidity/GetBlockByNum2`.
3. Offline conversion with the #509 converter at that height.

Same controls as #509: official public Solidity endpoint, one credential-free
plaintext channel, no retries/reflection/fallback, 20-second call deadlines,
32 MiB messages and a process deadline. Only `capture` opens a network channel.
"""
import argparse
import collections
import hashlib
import importlib.util
import json
import sys
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

sys.dont_write_bytecode = True  # importing the #509 scripts must not leave caches
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("tron509", HERE / "509-tron-rpc.py")
tron509 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tron509)

ENDPOINT = tron509.ENDPOINT
INFO_METHOD = "/protocol.WalletSolidity/GetTransactionInfoByBlockNum"
BLOCK_METHOD = "/protocol.WalletSolidity/GetBlockByNum2"
MAX_CANDIDATES = 10
REVERT = 2
DEFAULT, SUCCESS = 0, 1
FAILED = 1


def outcome_counts(infos):
    """Receipt/result histogram of a TransactionInfoList."""
    receipts = collections.Counter()
    results = collections.Counter()
    failed = 0
    for info in infos.transactionInfo:
        receipt = info.receipt.result if info.HasField("receipt") else None
        receipts[str(receipt)] += 1
        results[str(info.result)] += 1
        if info.result != 0 or receipt not in (None, DEFAULT, SUCCESS):
            failed += 1
    return {"transactions": len(infos.transactionInfo), "failed": failed,
            "receipt_results": dict(receipts), "info_results": dict(results)}


def has_revert(infos):
    return any(info.HasField("receipt") and info.receipt.result == REVERT for info in infos.transactionInfo)


def write_conversion(root, height, cls):
    tron509.HEIGHT = height
    return tron509.write_conversion(root, cls)


def capture(args, cls):
    import grpc
    candidates = args.heights
    tron509.require(1 <= len(candidates) <= MAX_CANDIDATES, "1..=10 candidate heights")
    tron509.require(len(set(candidates)) == len(candidates), "duplicate candidate")
    root = args.output
    root.mkdir(parents=True, exist_ok=False)
    now = lambda: datetime.now(timezone.utc).isoformat()
    report = {"endpoint": ENDPOINT, "candidates": candidates, "transport": "public plaintext Solidity gRPC",
              "credentials": "none", "calls": [], "status": "started", "started_at": now(),
              "grpcio": grpc.__version__, "descriptor_sha256": hashlib.sha256(args.descriptor.read_bytes()).hexdigest(),
              "converter": "509-tron-rpc.py", "converter_sha256": hashlib.sha256((HERE / "509-tron-rpc.py").read_bytes()).hexdigest()}

    def save():
        (root / "capture.json").write_text(json.dumps(report, indent=2) + "\n")

    def deadline(_signum, _frame):
        raise TimeoutError("process deadline")

    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(20 * (len(candidates) + 1) + 10)
    save()
    channel = grpc.insecure_channel(ENDPOINT, options=[
        ("grpc.enable_retries", 0), ("grpc.service_config_disable_resolution", 1),
        ("grpc.enable_http_proxy", 0), ("grpc.max_receive_message_length", 32 * 1024 * 1024),
    ])

    def call(method, height, name):
        entry = {"method": method, "height": height, "status": "started", "started_at": now()}
        report["calls"].append(entry)
        save()
        started = time.monotonic()
        request = cls("protocol.NumberMessage")(num=height).SerializeToString()
        response = channel.unary_unary(method)(request, timeout=20, wait_for_ready=False, metadata=())
        (root / name).write_bytes(response)
        entry.update(status="received", bytes=len(response), sha256=hashlib.sha256(response).hexdigest(),
                     completed_at=now(), elapsed_seconds=time.monotonic() - started)
        save()
        return response

    try:
        selected = None
        for height in candidates:
            raw = call(INFO_METHOD, height, f"transaction-info-list-{height}.pb")
            infos = cls("protocol.TransactionInfoList").FromString(raw)
            report["calls"][-1]["outcomes"] = outcome_counts(infos)
            save()
            if has_revert(infos):
                selected = height
                break
        tron509.require(selected is not None, "no candidate height has a REVERT receipt")
        report["selected_height"] = selected
        (root / "transaction-info-list.pb").write_bytes((root / f"transaction-info-list-{selected}.pb").read_bytes())
        raw = call(BLOCK_METHOD, selected, "block-extension.pb")
        tron509.HEIGHT = selected
        tron509.validate_block(raw, cls)
        report["transactions"] = write_conversion(root, selected, cls)
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


def fetch_block(args, cls):
    """One separately invoked GetBlockByNum2 for an already selected height, used
    when the capture's block call was rate limited. No retry within the call."""
    import grpc
    root = args.output
    report = json.loads((root / "capture.json").read_text())
    height = args.height
    tron509.require(report.get("status") == "failed" and report["calls"][-1]["method"] == BLOCK_METHOD,
                    "only resumes a capture whose block call failed")
    tron509.require(report["calls"][-1]["height"] == height, "height must be the selected one")
    tron509.require(not (root / "block-extension.pb").exists(), "block already captured")
    info_raw = (root / f"transaction-info-list-{height}.pb").read_bytes()
    tron509.require(has_revert(cls("protocol.TransactionInfoList").FromString(info_raw)), "selected height lacks REVERT")
    now = lambda: datetime.now(timezone.utc).isoformat()
    report["first_attempt"] = {k: report.pop(k) for k in ("status", "error_type", "error", "completed_at") if k in report}
    report["selected_height"] = height
    report["status"] = "resumed"
    entry = {"method": BLOCK_METHOD, "height": height, "status": "started", "started_at": now(),
             "invocation": "fetch-block (separate process after a failed block call)"}
    report["calls"].append(entry)
    (root / "capture.json").write_text(json.dumps(report, indent=2) + "\n")
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError("process deadline")))
    signal.alarm(30)
    channel = grpc.insecure_channel(ENDPOINT, options=[
        ("grpc.enable_retries", 0), ("grpc.service_config_disable_resolution", 1),
        ("grpc.enable_http_proxy", 0), ("grpc.max_receive_message_length", 32 * 1024 * 1024),
    ])
    try:
        request = cls("protocol.NumberMessage")(num=height).SerializeToString()
        started = time.monotonic()
        response = channel.unary_unary(BLOCK_METHOD)(request, timeout=20, wait_for_ready=False, metadata=())
        (root / "block-extension.pb").write_bytes(response)
        entry.update(status="received", bytes=len(response), sha256=hashlib.sha256(response).hexdigest(),
                     completed_at=now(), elapsed_seconds=time.monotonic() - started)
        (root / "transaction-info-list.pb").write_bytes(info_raw)
        tron509.HEIGHT = height
        tron509.validate_block(response, cls)
        report["transactions"] = write_conversion(root, height, cls)
        report["status"] = "converted"
        report["completed_at"] = now()
    except Exception as error:
        report["status"] = "failed"
        report["error_type"] = type(error).__name__
        report["error"] = str(error)[:1000]
        if entry["status"] == "started":
            entry.update(status="failed", completed_at=now())
        raise
    finally:
        signal.alarm(0)
        channel.close()
        (root / "capture.json").write_text(json.dumps(report, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("capture", "fetch-block", "convert"))
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--heights", type=int, nargs="*", default=[])
    parser.add_argument("--height", type=int, help="convert: the selected height")
    args = parser.parse_args()
    cls = tron509.classes(args.descriptor)
    if args.operation == "capture":
        capture(args, cls)
    elif args.operation == "fetch-block":
        fetch_block(args, cls)
    else:
        print("converted transactions:", write_conversion(args.output, args.height, cls))


if __name__ == "__main__":
    main()
