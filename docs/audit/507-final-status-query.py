#!/usr/bin/env python3
"""Check the README NEAR final-outcome queries on a synthetic multi-block chain (#507).

Builds three synthetic NEAR blocks whose receipt chains cross block boundaries,
replays each through `replay_near`, extracts the two README queries that start
with `-- Final outcome of each NEAR transaction` and `-- Transactions with a
failed receipt anywhere in their tree`, points them at the replay output, and
asserts the expected results with the DuckDB CLI:

- T1 -> R1 (SuccessReceiptId R2, spawns R3) -> R2 SuccessValue; R3 fails on the
  side. Final: SuccessValue, with one failed receipt in the tree.
- T2 -> R4 (SuccessReceiptId R5) -> R5 Failure. Final: Failure.
- T3 fails inclusion. Final: Failure.
- T4 -> R7, which does not execute in the range. Final: Pending.
"""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

ROOT = Path(__file__).resolve().parents[2]


def classes(descriptor):
    pool = descriptor_pool.DescriptorPool()
    for file in descriptor_pb2.FileDescriptorSet.FromString(Path(descriptor).read_bytes()).file:
        pool.Add(file)
    return lambda name: message_factory.GetMessageClass(pool.FindMessageTypeByName("sf.near.type.v1." + name))


def h(byte):
    return bytes([byte]) * 32


def build_blocks(cls):
    def block(height):
        b = cls("Block")()
        b.author = "producer.near"
        header = b.header
        header.height, header.prev_height = height, height - 1
        header.hash.bytes, header.prev_hash.bytes = h(height % 256), h((height - 1) % 256)
        header.timestamp_nanosec = (1_700_000_000 + height) * 1_000_000_000
        header.last_final_block_height = height - 2
        return b

    def tx(shard, byte, status, receipt=None):
        entry = shard.chunk.transactions.add()
        entry.transaction.hash.bytes = h(byte)
        entry.transaction.signer_id = "alice.near"
        entry.transaction.receiver_id = "dex.near"
        outcome = entry.outcome.execution_outcome
        outcome.id.bytes = h(byte)
        outcome.outcome.executor_id = "alice.near"
        if status == "failure":
            outcome.outcome.failure.SetInParent()
        else:
            outcome.outcome.success_receipt_id.id.bytes = h(receipt)
            outcome.outcome.receipt_ids.add().bytes = h(receipt)

    def receipt(shard, byte, status, target=None, children=()):
        entry = shard.receipt_execution_outcomes.add()
        entry.receipt.receipt_id.bytes = h(byte)
        entry.receipt.predecessor_id = "alice.near"
        entry.receipt.receiver_id = "dex.near"
        entry.receipt.action.signer_id = "alice.near"
        outcome = entry.execution_outcome
        outcome.id.bytes = h(byte)
        outcome.outcome.executor_id = "dex.near"
        for child in children:
            outcome.outcome.receipt_ids.add().bytes = h(child)
        if status == "receipt":
            outcome.outcome.success_receipt_id.id.bytes = h(target)
        elif status == "value":
            outcome.outcome.success_value.value = b"1"
        else:
            outcome.outcome.failure.SetInParent()

    b100 = block(100)
    shard = b100.shards.add()
    shard.chunk.header.shard_id = 0
    tx(shard, 0xa1, "receipt", 0xb1)
    tx(shard, 0xa2, "receipt", 0xb4)
    tx(shard, 0xa3, "failure")
    tx(shard, 0xa4, "receipt", 0xb7)
    b101 = block(101)
    shard = b101.shards.add()
    receipt(shard, 0xb1, "receipt", 0xb2, (0xb2, 0xb3))
    receipt(shard, 0xb3, "failure")
    receipt(shard, 0xb4, "receipt", 0xb5, (0xb5,))
    b102 = block(102)
    shard = b102.shards.add()
    receipt(shard, 0xb2, "value")
    receipt(shard, 0xb5, "failure")
    return {100: b100, 101: b101, 102: b102}


def readme_query(marker, root):
    text = (ROOT / "README.md").read_text()
    for block in re.findall(r"```sql\n(.*?)```", text, re.S):
        if block.startswith(marker):
            return block.replace("output/near-mainnet", str(root) + "/*/base58-failedtrue-forkfalse")
    raise SystemExit(f"README query not found: {marker}")


def duck(sql):
    out = subprocess.run(["duckdb", "-json", "-c", sql], capture_output=True, text=True)
    if out.returncode:
        raise SystemExit(out.stderr)
    return json.loads(out.stdout or "[]")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--replay", type=Path, required=True, help="replay_near binary")
    parser.add_argument("--output", type=Path, required=True, help="fresh directory")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    cls = classes(args.descriptor)
    for height, block in build_blocks(cls).items():
        path = args.output / f"{height}.pb"
        path.write_bytes(block.SerializeToString())
        subprocess.run([str(args.replay), "--block", str(path), "--output", str(args.output / str(height))], check=True,
                       stdout=subprocess.DEVNULL)
    final = duck(readme_query("-- Final outcome of each NEAR transaction", args.output))
    failed = duck(readme_query("-- Transactions with a failed receipt anywhere in their tree", args.output))
    tx_order = [row["hash"] for row in duck(
        f"SELECT hash FROM read_parquet('{args.output}/100/base58-failedtrue-forkfalse/transactions/*.parquet') ORDER BY transaction_index")]
    final_by_tx = {row["hash"]: row["final_status"] for row in final}
    got = [final_by_tx[tx] for tx in tx_order]
    assert got == ["SuccessValue", "Failure", "Failure", "Pending"], got
    failed_by_tx = {row["tx_hash"]: row["failed_receipts"] for row in failed}
    assert [failed_by_tx.get(tx, 0) for tx in tx_order] == [1, 1, 0, 0], failed_by_tx
    print(json.dumps({"final_status": got, "failed_receipts": [failed_by_tx.get(tx, 0) for tx in tx_order]}))


if __name__ == "__main__":
    sys.dont_write_bytecode = True
    main()
