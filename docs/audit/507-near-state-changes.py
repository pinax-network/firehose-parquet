#!/usr/bin/env python3
"""Offline #507 NEAR state-change projection from retained NearData JSON.

Every known near-firehose-indexer version sets `Block.state_changes` to an empty
list, so producer-compatible blocks carry no state changes. To exercise the
mapper on real data, `populate` copies a producer-compatible `block.pb` and fills
`Block.state_changes` with the NearData document's per-shard state changes
(shards in order, each shard's list in order), converted to the repository's
protobuf. This is a hypothetical producer, labeled as such; it does not claim any
provider emits these rows.

Conversion follows nearcore's `StateChangeCauseView`/`StateChangeValueView`.
Receipt causes carry `receipt_hash`; the protobuf field for ActionReceiptGasReward,
ReceiptProcessing and PostponedReceipt is named `tx_hash` and receives it.
BigInt is 16-byte big-endian unsigned (the #506 producer projection). Unknown
variants fail.
"""
import argparse
import base64
import hashlib
import json
import sys
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(text):
    num = 0
    for char in text:
        num = num * 58 + ALPHABET.index(char)
    raw = num.to_bytes((num.bit_length() + 7) // 8, "big") if num else b""
    return b"\0" * (len(text) - len(text.lstrip("1"))) + raw


def hash32(text):
    raw = b58decode(text)
    if len(raw) != 32:
        raise ValueError(f"expected a 32-byte hash, got {len(raw)} bytes")
    return raw


def u128(text):
    value = int(text)
    if not 0 <= value < 1 << 128 or str(value) != text:
        raise ValueError(f"invalid u128 {text!r}")
    return value.to_bytes(16, "big")


def classes(descriptor):
    pool = descriptor_pool.DescriptorPool()
    for file in descriptor_pb2.FileDescriptorSet.FromString(Path(descriptor).read_bytes()).file:
        pool.Add(file)
    return lambda name: message_factory.GetMessageClass(pool.FindMessageTypeByName("sf.near.type.v1." + name))


RECEIPT_CAUSES = {
    "action_receipt_processing_started": ("action_receipt_processing_started", "receipt_hash"),
    "action_receipt_gas_reward": ("action_receipt_gas_reward", "tx_hash"),
    "receipt_processing": ("receipt_processing", "tx_hash"),
    "postponed_receipt": ("postponed_receipt", "tx_hash"),
}
EMPTY_CAUSES = {"not_writable_to_disk", "initial_state", "updated_delayed_receipts",
                "validator_accounts_update", "migration"}


def set_cause(message, cause):
    kind = cause["type"]
    if kind == "transaction_processing":
        assert set(cause) == {"type", "tx_hash"}
        message.transaction_processing.tx_hash.bytes = hash32(cause["tx_hash"])
    elif kind in RECEIPT_CAUSES:
        assert set(cause) == {"type", "receipt_hash"}
        field, hash_field = RECEIPT_CAUSES[kind]
        getattr(getattr(message, field), hash_field).bytes = hash32(cause["receipt_hash"])
    elif kind in EMPTY_CAUSES:
        assert set(cause) == {"type"}
        getattr(message, kind).SetInParent()
    else:
        raise ValueError(f"unsupported cause {kind}")


def set_value(value, kind, change):
    if kind == "account_update":
        assert set(change) == {"account_id", "amount", "locked", "code_hash", "storage_usage", "storage_paid_at"}
        update = value.account_update
        update.account_id = change["account_id"]
        update.account.amount.bytes = u128(change["amount"])
        update.account.locked.bytes = u128(change["locked"])
        update.account.code_hash.bytes = hash32(change["code_hash"])
        update.account.storage_usage = change["storage_usage"]
    elif kind == "account_deletion":
        value.account_deletion.account_id = change["account_id"]
    elif kind == "data_update":
        assert set(change) == {"account_id", "key_base64", "value_base64"}
        update = value.data_update
        update.account_id = change["account_id"]
        update.key = base64.b64decode(change["key_base64"], validate=True)
        update.value = base64.b64decode(change["value_base64"], validate=True)
    elif kind == "data_deletion":
        value.data_deletion.account_id = change["account_id"]
        value.data_deletion.key = base64.b64decode(change["key_base64"], validate=True)
    elif kind == "access_key_update":
        assert set(change) == {"account_id", "public_key", "access_key"}
        update = value.access_key_update
        update.account_id = change["account_id"]
        curve, key = change["public_key"].split(":", 1)
        update.public_key.type = {"ed25519": 0, "secp256k1": 1}[curve]
        update.public_key.bytes = b58decode(key)
        update.access_key.nonce = change["access_key"]["nonce"]
        permission = change["access_key"]["permission"]
        if permission == "FullAccess":
            update.access_key.permission.full_access.SetInParent()
        else:
            call = permission["FunctionCall"]
            target = update.access_key.permission.function_call
            if call.get("allowance") is not None:
                target.allowance.bytes = u128(call["allowance"])
            target.receiver_id = call["receiver_id"]
            target.method_names.extend(call["method_names"])
    elif kind == "access_key_deletion":
        value.access_key_deletion.account_id = change["account_id"]
    elif kind == "contract_code_update":
        value.contract_code_update.account_id = change["account_id"]
        value.contract_code_update.code = base64.b64decode(change["code_base64"], validate=True)
    elif kind == "contract_code_deletion":
        value.contract_deletion.account_id = change["account_id"]
    else:
        raise ValueError(f"unsupported state change {kind}")


def source_state_changes(document):
    return [change for shard in document["shards"] for change in shard["state_changes"]]


def populate(args):
    cls = classes(args.descriptor)
    document = json.loads(args.json.read_text())
    block = cls("Block").FromString(args.block.read_bytes())
    if block.state_changes:
        raise ValueError("expected a producer-compatible block without state changes")
    for change in source_state_changes(document):
        entry = block.state_changes.add()
        set_cause(entry.cause, change["cause"])
        set_value(entry.value, change["type"], change["change"])
    data = block.SerializeToString()
    args.output.write_bytes(data)
    print(json.dumps({"state_changes": len(block.state_changes), "bytes": len(data),
                      "sha256": hashlib.sha256(data).hexdigest()}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("populate",))
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--block", type=Path, required=True, help="producer-compatible block.pb")
    parser.add_argument("--json", type=Path, required=True, help="original NearData JSON")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        sys.exit("output exists")
    populate(args)


if __name__ == "__main__":
    main()
