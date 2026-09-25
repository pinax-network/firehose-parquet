#!/usr/bin/env python3
"""Offline NearData -> pinned StreamingFast projection; bounded capture is explicit.

Source: near-firehose-indexer 144071c93685c057293459329e9ca35b07aba641,
codec/mod.rs; Firehose reader LIB enrichment 98d869ceab00a764a9addb18053aecf4c5ae1f33.
This is a qualification adapter, not a new ingestion transport. Unknown variants
fail. The producer drops state changes, enriched tx_hash, metadata details and
several newer view fields; original JSON is always retained independently.
"""
import argparse
import base64
import hashlib
import http.client
import json
import os
import re
import signal
import ssl
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

HEIGHT = 150_000_000
BLOCK_URL = f"https://mainnet.neardata.xyz/v0/block/{HEIGHT}"
ANCHOR_URL = "https://archival-rpc.mainnet.near.org"
WHOLE_SECONDS = 60
REQUEST_SECONDS = 20
BLOCK_CAP = 32 * 1024 * 1024
REDIRECT_CAP = 64 * 1024
ANCHOR_CAP = 2 * 1024 * 1024
SOURCE_PINS = {
    "near_firehose_indexer": "144071c93685c057293459329e9ca35b07aba641",
    "firehose_near": "98d869ceab00a764a9addb18053aecf4c5ae1f33",
    "nearcore": "74a6829e947512019773059d8b5905b4e6cb6236",
    "neardata_server": "7324d9131e7e8285306606ba05822b007e44c53a",
    "fastnear_libs": "8d9142c0bf86ea1c3f884bf362fd8bc127c69f92",
}
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def require(ok, message):
    if not ok:
        raise ValueError(message)


def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result
    def no_float(value):
        raise ValueError(f"non-integer JSON number: {value}")
    return json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                      parse_float=no_float, parse_constant=no_float)


def obj(value):
    require(type(value) is dict, "expected JSON object")
    return value


def seq(value):
    require(type(value) is list, "expected JSON list")
    return value


def string(value):
    require(type(value) is str, "expected string")
    return value


def uint(value, bits=64):
    require(type(value) is int or (type(value) is str and re.fullmatch(r"0|[1-9][0-9]*", value)),
            "expected unsigned integer or canonical decimal string")
    number = int(value)
    require(0 <= number < 1 << bits, f"integer outside u{bits}")
    return number


def b58(value, size):
    value = string(value)
    require(len(value) <= size * 2, "oversized base58")
    number = 0
    for char in value:
        require(char in ALPHABET, "invalid base58 character")
        number = number * 58 + ALPHABET.index(char)
    raw = b"\0" * (len(value) - len(value.lstrip("1")))
    raw += number.to_bytes((number.bit_length() + 7) // 8, "big")
    require(len(raw) == size, f"base58 decoded length must be {size}")
    return raw


def b64(raw):
    return base64.b64encode(raw).decode("ascii")


def decoded64(value):
    raw = base64.b64decode(string(value), validate=True)
    require(b64(raw) == value, "noncanonical base64")
    return raw


def hash_value(value):
    return {"bytes": b64(b58(value, 32))}


def bigint(value):
    return {"bytes": b64(uint(value, 128).to_bytes(16, "big"))}


def key(value, signature=False):
    parts = string(value).split(":")
    require(len(parts) == 2 and parts[0] in ("ed25519", "secp256k1"), "unsupported key/signature curve")
    curve = parts[0]
    size = (64 if curve == "ed25519" else 65) if signature else (32 if curve == "ed25519" else 64)
    return {"type": 0 if curve == "ed25519" else 1, "bytes": b64(b58(parts[1], size))}


def variant(value):
    if type(value) is str:
        return value, None
    value = obj(value)
    require(len(value) == 1, "expected exactly one enum variant")
    return next(iter(value.items()))


def snake(value):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def access_key(value):
    value = obj(value)
    tag, body = variant(value["permission"])
    if tag == "FullAccess":
        require(body is None, "FullAccess must be unit variant")
        permission = {"full_access": {}}
    elif tag == "FunctionCall":
        body = obj(body)
        fields = {"receiver_id": string(body["receiver_id"]),
                  "method_names": [string(x) for x in seq(body["method_names"])]}
        if body["allowance"] is not None:
            fields["allowance"] = bigint(body["allowance"])
        permission = {"function_call": fields}
    else:
        raise ValueError(f"unsupported access permission: {tag}")
    return {"nonce": uint(value["nonce"]), "permission": permission}


def action(value, allow_delegate=True):
    tag, body = variant(value)
    if tag == "CreateAccount":
        require(body is None, "CreateAccount must be unit variant")
        fields = {}
    else:
        body = obj(body)
        if tag == "DeployContract":
            fields = {"code": b64(decoded64(body["code"]))}
        elif tag == "FunctionCall":
            fields = {"method_name": string(body["method_name"]), "args": b64(decoded64(body["args"])),
                      "gas": uint(body["gas"]), "deposit": bigint(body["deposit"])}
        elif tag == "Transfer":
            fields = {"deposit": bigint(body["deposit"])}
        elif tag == "Stake":
            fields = {"stake": bigint(body["stake"]), "public_key": key(body["public_key"])}
        elif tag == "AddKey":
            fields = {"public_key": key(body["public_key"]), "access_key": access_key(body["access_key"])}
        elif tag == "DeleteKey":
            fields = {"public_key": key(body["public_key"])}
        elif tag == "DeleteAccount":
            fields = {"beneficiary_id": string(body["beneficiary_id"])}
        elif tag == "Delegate" and allow_delegate:
            delegated = obj(body["delegate_action"])
            fields = {"signature": key(body["signature"], True), "delegate_action": {
                "sender_id": string(delegated["sender_id"]), "receiver_id": string(delegated["receiver_id"]),
                "nonce": uint(delegated["nonce"]), "max_block_height": uint(delegated["max_block_height"]),
                "public_key": key(delegated["public_key"]),
                "actions": [action(x, False) for x in seq(delegated["actions"])]}}
        else:
            raise ValueError(f"unsupported action: {tag}")
    return {snake(tag): fields}


INVALID_TX = ["InvalidAccessKeyError", "InvalidSignerId", "SignerDoesNotExist", "InvalidNonce",
              "NonceTooLarge", "InvalidReceiverId", "InvalidSignature", "NotEnoughBalance",
              "LackBalanceForState", "CostOverflow", "InvalidChain", "Expired", "ActionsValidation",
              "TransactionSizeExceeded"]
FUNCTION_ERROR = ["CompilationError", "LinkError", "MethodResolveError", "WasmTrap", "WasmUnknownError",
                  "HostError", "_EVMError", "ExecutionError"]
RECEIPT_ERROR = ["InvalidPredecessorId", "InvalidReceiverId", "InvalidSignerId", "InvalidDataReceiverId",
                 "ReturnedValueLengthExceeded", "NumberInputDataDependenciesExceeded", "ActionsValidation"]
ACTION_FIELDS = {
    "AccountAlreadyExists": ("account_id",), "AccountDoesNotExist": ("account_id",),
    "CreateAccountOnlyByRegistrar": ("account_id", "registrar_account_id", "predecessor_id"),
    "CreateAccountNotAllowed": ("account_id", "predecessor_id"), "ActorNoPermission": ("account_id", "actor_id"),
    "DeleteKeyDoesNotExist": ("account_id", "public_key"), "AddKeyAlreadyExists": ("account_id", "public_key"),
    "LackBalanceForState": ("account_id", "amount"), "TriesToUnstake": ("account_id",),
    "TriesToStake": ("account_id", "stake", "locked", "balance"),
    "InsufficientStake": ("account_id", "stake", "minimum_stake"),
    "OnlyImplicitAccountCreationAllowed": ("account_id",), "DeleteAccountWithLargeState": ("account_id",),
    "DelegateActionSenderDoesNotMatchTxReceiver": ("sender_id", "receiver_id"),
    "DelegateActionInvalidNonce": ("delegate_nonce", "ak_nonce"),
    "DelegateActionNonceTooLarge": ("delegate_nonce", "upper_bound"),
}


def failure(value):
    tag, body = variant(value)
    if tag == "InvalidTxError":
        kind, _ = variant(body)
        require(kind in INVALID_TX, f"unsupported transaction error: {kind}")
        return {"invalid_tx_error": INVALID_TX.index(kind)}
    require(tag == "ActionError", f"unsupported failure: {tag}")
    body = obj(body)
    kind, payload = variant(body["kind"])
    field = snake(kind)
    if kind in ACTION_FIELDS:
        payload = obj(payload)
        fields = {}
        for name in ACTION_FIELDS[kind]:
            item = payload[name]
            if name == "public_key":
                item = key(item)
            elif name in ("amount", "stake", "locked", "balance", "minimum_stake"):
                item = bigint(item)
            elif name in ("delegate_nonce", "ak_nonce", "upper_bound"):
                item = uint(item)
            else:
                item = string(item)
            fields["balance" if name == "amount" else name] = item
        if kind == "AccountAlreadyExists":
            field = "account_already_exist"  # producer/protobuf spelling
    elif kind == "DeleteAccountStaking":
        fields = {"account_id": ""}  # producer intentionally discards payload
    elif kind in ("DelegateActionInvalidSignature", "DelegateActionExpired", "DelegateActionAccessKeyError"):
        fields = {}  # producer Default, including ignored access-key detail
    elif kind in ("FunctionCallError", "NewReceiptValidationError"):
        nested, _ = variant(payload)
        names = FUNCTION_ERROR if kind == "FunctionCallError" else RECEIPT_ERROR
        require(nested in names, f"unsupported nested error: {nested}")
        fields = {"error": names.index(nested)}
        field = "function_call" if kind == "FunctionCallError" else "new_receipt_validation"
    else:
        raise ValueError(f"unsupported action error: {kind}")
    return {"action_error": {"index": 0 if body["index"] is None else uint(body["index"]), field: fields}}


def status(value):
    tag, payload = variant(value)
    if tag == "Unknown":
        require(payload is None, "Unknown must be unit variant")
        return {"unknown": {}}
    if tag == "SuccessValue":
        return {"success_value": {"value": b64(decoded64(payload))}}
    if tag == "SuccessReceiptId":
        return {"success_receipt_id": {"id": hash_value(payload)}}
    if tag == "Failure":
        return {"failure": failure(payload)}
    raise ValueError(f"unsupported outcome status: {tag}")


def outcome(value):
    value = obj(value)
    raw = obj(value["outcome"])
    proof = []
    for item in seq(value["proof"]):
        require(item["direction"] in ("Left", "Right"), "invalid proof direction")
        proof.append({"hash": hash_value(item["hash"]), "direction": 0 if item["direction"] == "Left" else 1})
    return {"proof": {"path": proof}, "block_hash": hash_value(value["block_hash"]), "id": hash_value(value["id"]),
            "outcome": {"logs": [string(x) for x in seq(raw["logs"])],
                        "receipt_ids": [hash_value(x) for x in seq(raw["receipt_ids"])],
                        "gas_burnt": uint(raw["gas_burnt"]), "tokens_burnt": bigint(raw["tokens_burnt"]),
                        "executor_id": string(raw["executor_id"]), "metadata": 0, **status(raw["status"])}}


def receipt(value):
    value = obj(value)
    result = {"predecessor_id": string(value["predecessor_id"]), "receiver_id": string(value["receiver_id"]),
              "receipt_id": hash_value(value["receipt_id"])}
    tag, body = variant(value["receipt"])
    body = obj(body)
    if tag == "Action":
        result["action"] = {"signer_id": string(body["signer_id"]), "signer_public_key": key(body["signer_public_key"]),
                            "gas_price": bigint(body["gas_price"]),
                            "output_data_receivers": [{"data_id": hash_value(x["data_id"]),
                                                       "receiver_id": string(x["receiver_id"])} for x in seq(body["output_data_receivers"])],
                            "input_data_ids": [hash_value(x) for x in seq(body["input_data_ids"])],
                            "actions": [action(x) for x in seq(body["actions"])]}
    elif tag == "Data":
        # Rust producer unwrap_or(vec![]) collapses None and Some(empty).
        result["data"] = {"data_id": hash_value(body["data_id"]),
                          "data": "" if body["data"] is None else b64(decoded64(body["data"]))}
    else:
        raise ValueError(f"unsupported receipt: {tag}")
    return result


def transaction(value):
    value = obj(value)
    tx = obj(value["transaction"])
    execution = obj(value["outcome"])
    require(tx["hash"] == execution["execution_outcome"]["id"], "transaction/outcome ID mismatch")
    converted = {"transaction": {"signer_id": string(tx["signer_id"]), "public_key": key(tx["public_key"]),
        "nonce": uint(tx["nonce"]), "receiver_id": string(tx["receiver_id"]),
        "actions": [action(x) for x in seq(tx["actions"])], "signature": key(tx["signature"], True), "hash": hash_value(tx["hash"])},
        "outcome": {"execution_outcome": outcome(execution["execution_outcome"])}}
    if execution["receipt"] is not None:
        converted["outcome"]["receipt"] = receipt(execution["receipt"])
        require(execution["receipt"]["receipt_id"] in execution["execution_outcome"]["outcome"]["receipt_ids"],
                "optional transaction receipt not in produced receipt IDs")
    return converted


def validator(value):
    value = obj(value)
    require(value.get("validator_stake_struct_version") == "V1", "unsupported validator stake version")
    return {"account_id": string(value["account_id"]), "public_key": key(value["public_key"]), "stake": bigint(value["stake"])}


HEADER_HASHES = ("epoch_id", "next_epoch_id", "hash", "prev_hash", "prev_state_root", "chunk_receipts_root",
                 "chunk_headers_root", "chunk_tx_root", "outcome_root", "challenges_root", "random_value",
                 "last_final_block", "last_ds_final_block", "next_bp_hash", "block_merkle_root")
CHUNK_HASHES = ("chunk_hash", "prev_block_hash", "outcome_root", "prev_state_root", "encoded_merkle_root",
                "outgoing_receipts_root", "tx_root")
CHUNK_NUMBERS = ("encoded_length", "height_created", "height_included", "shard_id", "gas_used", "gas_limit")


def chunk_header(value):
    value = obj(value)
    return {**{x: b64(b58(value[x], 32)) for x in CHUNK_HASHES},
            **{x: uint(value[x]) for x in CHUNK_NUMBERS},
            "validator_reward": bigint(value["validator_reward"]), "balance_burnt": bigint(value["balance_burnt"]),
            "validator_proposals": [validator(x) for x in seq(value["validator_proposals"])],
            "signature": key(value["signature"], True)}


def project(document, expected_height=HEIGHT):
    """Return complete supported producer projection, before reader LIB enrichment."""
    document = obj(document)
    block = obj(document["block"])
    header = obj(block["header"])
    require(uint(header["height"]) == expected_height, "wrong selected block height")
    require(0 < uint(header["prev_height"]) < expected_height, "missing/invalid parent height; extra lookup forbidden")
    nanos = uint(header["timestamp_nanosec"])
    require(uint(header["timestamp"]) == nanos, "inconsistent nanosecond timestamp fields")
    mask = seq(header["chunk_mask"])
    require(all(type(x) is bool for x in mask), "invalid chunk mask")
    h = {**{x: hash_value(header[x]) for x in HEADER_HASHES},
         "height": expected_height, "prev_height": uint(header["prev_height"]), "timestamp": nanos,
         "timestamp_nanosec": nanos, "chunks_included": uint(header["chunks_included"]),
         "latest_protocol_version": uint(header["latest_protocol_version"], 32),
         "validator_proposals": [validator(x) for x in seq(header["validator_proposals"])], "chunk_mask": mask,
         "gas_price": bigint(header["gas_price"]), "total_supply": bigint(header["total_supply"]),
         "challenges_result": [], "approvals": [key(x, True) for x in seq(header["approvals"]) if x is not None],
         "signature": key(header["signature"], True)}
    for item in seq(header["challenges_result"]):
        require(type(item["is_double_sign"]) is bool, "invalid slashed-validator flag")
        h["challenges_result"].append({"account_id": string(item["account_id"]), "is_double_sign": item["is_double_sign"]})
    require(b58(header["last_final_block"], 32) != bytes(32), "missing LIB hash")
    headers = [chunk_header(x) for x in seq(block["chunks"])]
    headers_by_shard = {x["shard_id"]: x for x in headers}
    require(len(headers_by_shard) == len(headers), "duplicate chunk header shard")
    require(len(mask) == len(headers) and sum(mask) == h["chunks_included"], "inconsistent chunk inventory")
    require(all(flag == (ch["height_included"] == expected_height) for flag, ch in zip(mask, headers)),
            "chunk mask/inclusion-height mismatch")
    shards, shard_ids, tx_ids, outcome_ids = [], set(), set(), set()
    for source in seq(document["shards"]):
        sid = uint(source["shard_id"])
        require(sid not in shard_ids and sid in headers_by_shard, "duplicate/unlisted shard")
        shard_ids.add(sid)
        shard = {"shard_id": sid, "receipt_execution_outcomes": []}
        if source["chunk"] is not None:
            chunk = obj(source["chunk"])
            ch = chunk_header(chunk["header"])
            require(ch == headers_by_shard[sid] and ch["height_included"] == expected_height, "chunk/header mismatch")
            for tx in seq(chunk["transactions"]):
                tx_id = tx["transaction"]["hash"]
                require(tx_id not in tx_ids, "duplicate transaction ID")
                tx_ids.add(tx_id)
            shard["chunk"] = {"author": string(chunk["author"]), "header": ch,
                              "transactions": [transaction(x) for x in chunk["transactions"]],
                              "receipts": [receipt(x) for x in seq(chunk["receipts"])]}
        else:
            require(headers_by_shard[sid]["height_included"] < expected_height, "missing included chunk")
        for item in seq(source["receipt_execution_outcomes"]):
            eid = item["execution_outcome"]["id"]
            require(eid == item["receipt"]["receipt_id"], "receipt/outcome ID mismatch")
            require(eid not in outcome_ids, "duplicate receipt outcome ID")
            outcome_ids.add(eid)
            shard["receipt_execution_outcomes"].append({"execution_outcome": outcome(item["execution_outcome"]),
                                                      "receipt": receipt(item["receipt"])})
        if expected_height < 193_444_226:
            shard["receipt_execution_outcomes"].sort(key=lambda x: base64.b64decode(x["execution_outcome"]["id"]["bytes"]))
        shards.append(shard)
    require(shard_ids == set(headers_by_shard), "incomplete shard inventory")
    return {"author": string(block["author"]), "header": h, "chunk_headers": headers,
            "shards": shards, "state_changes": []}


def enrich(projected, document, anchor):
    obj(anchor)
    require(anchor.get("jsonrpc") == "2.0" and anchor.get("id") == "fireparq-506-lib" and "error" not in anchor,
            "invalid/error anchor RPC envelope")
    header = obj(obj(anchor["result"])["header"])
    expected = document["block"]["header"]["last_final_block"]
    require(header["hash"] == expected, "anchor hash mismatch")
    b58(header["hash"], 32)
    height = uint(header["height"])
    require(height < projected["header"]["height"], "anchor is not older than selected block")
    projected["header"]["last_final_block_height"] = height
    return projected


def protobuf(projected, descriptor):
    from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory
    pool = descriptor_pool.DescriptorPool()
    for file in descriptor_pb2.FileDescriptorSet.FromString(Path(descriptor).read_bytes()).file:
        pool.Add(file)
    block = message_factory.GetMessageClass(pool.FindMessageTypeByName("sf.near.type.v1.Block"))()
    json_format.ParseDict(projected, block)  # strict unknown fields, widths and oneof validation
    return block.SerializeToString(deterministic=True)


@contextmanager
def whole_deadline(seconds=WHOLE_SECONDS):
    """Main-process SIGALRM interrupts DNS, TLS, reads, parsing and conversion alike."""
    require(signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0), "existing process alarm")
    def expired(_signal, _frame):
        raise TimeoutError("whole capture deadline exceeded")
    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield time.monotonic() + seconds
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def redirect_url(location):
    url = urlsplit(location)
    require(url.scheme == "https" and url.username is None and url.password is None
            and url.port in (None, 443) and re.fullmatch(r"a[0-9]+\.mainnet\.neardata\.xyz", url.hostname or "")
            and url.path == f"/v0/block/{HEIGHT}" and not url.query and not url.fragment,
            "unapproved archive redirect")
    return location


def tls_context():
    # Avoid create_default_context's ambient SSLKEYLOGFILE handling.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_default_certs()
    return context


class Transport:
    """One fresh direct HTTPS connection per call; stdlib has no proxy/retry/cookie jar."""
    def __init__(self, deadline, connection=http.client.HTTPSConnection, checkpoint=lambda: None):
        self.deadline, self.connection, self.calls = deadline, connection, []
        self.checkpoint = checkpoint

    def request(self, url, method, cap, body=None, allow_redirect=False):
        require(len(self.calls) < 3, "request budget exhausted")
        parsed = urlsplit(url)
        require(parsed.scheme == "https" and parsed.port in (None, 443) and not parsed.username
                and not parsed.password and not parsed.query and not parsed.fragment, "unsafe URL")
        require(url in (BLOCK_URL, ANCHOR_URL) or redirect_url(url) == url, "unapproved URL")
        deadline = min(self.deadline, time.monotonic() + REQUEST_SECONDS)
        def remaining():
            value = deadline - time.monotonic()
            require(value > 0, "request deadline exceeded")
            return value
        # Direct HTTPSConnection does not consult HTTP(S)_PROXY, netrc, or cookies.
        connection = self.connection(parsed.hostname, port=443, timeout=remaining(), context=tls_context())
        record = {"url": url, "method": method, "stage": "requesting",
                  "started_at": datetime.now(timezone.utc).isoformat()}
        self.calls.append(record)
        self.checkpoint()  # durable attempted stage before any DNS/TLS/HTTP activity
        headers = {"Accept": "application/json", "Accept-Encoding": "identity", "Connection": "close"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        prior_alarm = signal.getitimer(signal.ITIMER_REAL)
        request_started = time.monotonic()
        signal.setitimer(signal.ITIMER_REAL, min(remaining(), prior_alarm[0]) if prior_alarm[0] else remaining())
        try:
            connection.request(method, parsed.path or "/", body=body, headers=headers)
            if connection.sock is not None:
                connection.sock.settimeout(remaining())
            response = connection.getresponse()
            record.update(status=response.status, stage="headers")
            self.checkpoint()
            fields = {}
            for name, value in response.getheaders():
                name = name.lower()
                require(name not in fields, f"duplicate HTTP header: {name}")
                fields[name] = value
            record["headers"] = {k: v for k, v in fields.items() if k in ("content-type", "content-length", "content-encoding")}
            self.checkpoint()
            require(fields.get("content-encoding", "identity").lower() == "identity", "encoded response rejected")
            require(fields.get("transfer-encoding", "chunked").lower() == "chunked", "unsupported transfer encoding")
            require(not ("transfer-encoding" in fields and "content-length" in fields), "ambiguous HTTP framing")
            require(response.status == 200 or (allow_redirect and response.status == 302), f"HTTP status {response.status}")
            limit = min(cap, REDIRECT_CAP) if response.status == 302 else cap
            if "content-length" in fields:
                require(re.fullmatch(r"[0-9]+", fields["content-length"]) and int(fields["content-length"]) <= limit,
                        "response Content-Length exceeds limit")
            content = bytearray()
            record["stage"] = "body"
            self.checkpoint()
            while True:
                remaining()
                if connection.sock is not None:
                    connection.sock.settimeout(remaining())
                # read1 bounds each physical read; content never exceeds cap+1.
                part = response.read1(min(64 * 1024, limit + 1 - len(content)))
                if not part:
                    break
                content.extend(part)
                require(len(content) <= limit, "streamed response exceeds limit")
            remaining()
            if "content-length" in fields:
                require(len(content) == int(fields["content-length"]), "truncated response")
            record.update(stage="complete", size=len(content), sha256=hashlib.sha256(content).hexdigest())
            return response.status, fields, bytes(content)
        except BaseException as error:
            record.update(stage="failed", error_type=type(error).__name__)
            raise
        finally:
            connection.close()
            record["finished_at"] = datetime.now(timezone.utc).isoformat()
            self.checkpoint()
            elapsed = time.monotonic() - request_started
            if prior_alarm[0]:
                signal.setitimer(signal.ITIMER_REAL, max(0.000001, prior_alarm[0] - elapsed), prior_alarm[1])
            else:
                signal.setitimer(signal.ITIMER_REAL, 0)


def capture(output, descriptor, connection=http.client.HTTPSConnection):
    """Exactly one selected height, optional single archive redirect, one derived LIB hash."""
    with whole_deadline(60) as deadline:
        output = Path(output)
        output.mkdir(exist_ok=False)
        provenance = {"source_pins": SOURCE_PINS, "height": HEIGHT, "stage": "starting",
                      "started_at": datetime.now(timezone.utc).isoformat(), "requests": []}
        def checkpoint():
            # Atomic replacement keeps the last complete record if the supervisor
            # kills the worker. No cookies, authorization or response body previews.
            temporary = output / ".provenance.json.tmp"
            temporary.write_text(json.dumps(provenance, indent=2) + "\n")
            os.replace(temporary, output / "provenance.json")
        transport = Transport(deadline, connection, checkpoint)
        provenance["requests"] = transport.calls
        checkpoint()
        def save(name, raw):
            (output / name).write_bytes(raw)
            return {"size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        try:
            provenance["stage"] = "document"
            status_code, headers, raw = transport.request(BLOCK_URL, "GET", BLOCK_CAP, allow_redirect=True)
            if status_code == 302:
                provenance["redirect"] = save("redirect-body.bin", raw)
                target = redirect_url(headers["location"])
                provenance["stage"] = "archive_document"
                status_code, headers, raw = transport.request(target, "GET", BLOCK_CAP)
            provenance["document"] = save("neardata.json", raw)
            provenance["stage"] = "conversion"
            checkpoint()
            document = strict_json(raw)
            projected = project(document)  # full conversion before the anchor request
            protobuf(projected, descriptor)  # strict descriptor check before another call
            request = {"jsonrpc": "2.0", "id": "fireparq-506-lib", "method": "block",
                       "params": {"block_id": document["block"]["header"]["last_final_block"]}}
            provenance["stage"] = "lib_anchor"
            _, _, raw = transport.request(ANCHOR_URL, "POST", ANCHOR_CAP,
                                          json.dumps(request, separators=(",", ":")).encode())
            provenance["anchor"] = save("lib-anchor.json", raw)
            provenance["stage"] = "anchor_conversion"
            checkpoint()
            projected = enrich(projected, document, strict_json(raw))
            provenance["protobuf"] = save("block.pb", protobuf(projected, descriptor))
            provenance["stage"] = "complete"
            return provenance
        except BaseException as error:
            provenance["failed_stage"] = provenance["stage"]
            provenance.update(stage="failed", error_type=type(error).__name__)
            raise
        finally:
            provenance["finished_at"] = datetime.now(timezone.utc).isoformat()
            checkpoint()


def supervise(command, seconds=WHOLE_SECONDS):
    """Hard worker-process limit also covers uninterruptible Python/C DNS calls.

    subprocess.run kills and reaps the sole worker on timeout. The worker creates
    no children and opens no subprocesses; no chain request is retried.
    """
    return subprocess.run(command, timeout=seconds, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    offline = sub.add_parser("convert")
    offline.add_argument("document")
    offline.add_argument("anchor")
    offline.add_argument("descriptor")
    offline.add_argument("output")
    online = sub.add_parser("capture")
    online.add_argument("descriptor")
    online.add_argument("output")
    worker = sub.add_parser("_capture-worker", help=argparse.SUPPRESS)
    worker.add_argument("descriptor")
    worker.add_argument("output")
    args = parser.parse_args()
    if args.command == "capture":
        supervise([sys.executable, str(Path(__file__).resolve()), "_capture-worker", args.descriptor, args.output], 60)
    elif args.command == "_capture-worker":
        print(json.dumps(capture(args.output, args.descriptor), indent=2))
    else:
        document = strict_json(Path(args.document).read_bytes())
        projected = enrich(project(document), document, strict_json(Path(args.anchor).read_bytes()))
        Path(args.output).write_bytes(protobuf(projected, args.descriptor))


if __name__ == "__main__":
    main()
