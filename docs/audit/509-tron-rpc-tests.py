#!/usr/bin/env python3
"""Offline wire fixtures and fake-channel bounds for 509-tron-rpc.py."""
import argparse
import hashlib
import importlib.util
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("tron_capture", Path(__file__).with_name("509-tron-rpc.py"))
rpc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rpc)
parser = argparse.ArgumentParser()
parser.add_argument("--descriptor", type=Path, required=True)
args = parser.parse_args()
cls = rpc.classes(args.descriptor)


def v(value):
    value &= (1 << 64) - 1
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def n(tag, value):
    return v(tag << 3) + v(value)


def b(tag, value):
    return v((tag << 3) | 2) + v(len(value)) + value


def fixture(empty_receipt=False):
    timestamp = 1_700_000_000_123
    owner, recipient = b"\x41" + b"a" * 20, b"\x41" + b"b" * 20
    transfer = b(1, owner) + b(2, recipient) + n(3, 42)
    parameter = b(1, b"type.googleapis.com/protocol.TransferContract") + b(2, transfer)
    contract = n(1, 1) + b(2, parameter) + n(5, 7) + b(99, b"future-contract")
    # A known absent parameter and an unsupported present-empty Any stay distinct.
    raw = b(1, b"ref") + b(4, b"refhash") + n(8, 500) + b(11, contract) + b(11, n(1, 1)) + b(11, n(1, 777) + b(2, b"")) + n(14, 400)
    raws = [raw, n(8, 501) + n(14, 401)]
    extensions, infos = [], []
    for i, raw in enumerate(raws):
        txid = hashlib.sha256(raw).digest()
        transaction = b(1, raw) + b(2, bytes([i + 1]) * 65)
        wrapper = n(1, 1) + n(2, 777 if i else 0) + b(3, b"wrapper-message")
        extensions.append(b(1, transaction) + b(2, txid) + b(3, b"wrapper-result") + b(4, wrapper) + n(5, 91) + n(8, 92))
        receipt = b(7, b"" if empty_receipt else n(1, 11) + n(7, 10)) if i == 0 else b""
        call_values = b(4, n(1, 17)) + b(4, n(1, -9) + b(2, b"TOKEN"))
        internal = b(1, b"h" * 32) + b(2, owner) + b(3, recipient) + call_values + b(5, b"call") + n(6, 1)
        infos.append(b(1, txid) + n(2, 123) + n(3, rpc.HEIGHT) + n(4, timestamp) + b(5, b"receipt-result") + receipt + n(9, 1) + b(10, b"receipt-message") + b(17, internal) + b(111, b"future-receipt"))
    parent = (rpc.HEIGHT - 1).to_bytes(8, "big") + b"p" * 24
    header = n(1, timestamp) + b(2, b"t" * 32) + b(3, parent) + n(7, rpc.HEIGHT) + b(9, owner) + n(10, 32)
    blockid = rpc.HEIGHT.to_bytes(8, "big") + hashlib.sha256(header).digest()[8:]
    block = b"".join(b(1, x) for x in extensions) + b(2, b(1, header) + b(2, b"witness")) + b(3, blockid)
    return block, b"".join(b(1, x) for x in infos)


class ConversionTests(unittest.TestCase):
    def test_exact_envelopes_presence_and_pinned_wrapper_semantics(self):
        raw, infos = fixture()
        source = cls("protocol.BlockExtention").FromString(raw)
        receipt_source = cls("protocol.TransactionInfoList").FromString(infos)
        out, identity = rpc.convert(raw, infos, cls)
        self.assertEqual(len(out.transactions), 2)
        self.assertEqual(out.header.number, rpc.HEIGHT)
        self.assertEqual(out.header.timestamp, 1_700_000_000_123)
        self.assertEqual(identity["timestamp_ns"], 123_000_000)
        self.assertEqual(identity["lib_num"], rpc.HEIGHT - 20)
        for i, tx in enumerate(out.transactions):
            self.assertEqual(tx.txid, source.transactions[i].txid)
            self.assertEqual(tx.signature, source.transactions[i].transaction.signature)
            self.assertTrue(tx.result)  # Independent receipt result is FAILED.
            self.assertEqual(tx.code, 777 if i else 0)
            self.assertEqual(tx.message, b"wrapper-message")
            self.assertEqual(tx.contract_result, [b"wrapper-result"])
            self.assertEqual((tx.energy_used, tx.energy_penalty), (91, 92))
            self.assertEqual(tx.info.SerializeToString(), receipt_source.transactionInfo[i].SerializeToString())
            self.assertEqual(tx.info.contractResult, [b"receipt-result"])
            self.assertEqual(tx.info.internal_transactions[0].callValueInfo[1].callValue, -9)
            self.assertEqual(tx.info.internal_transactions[0].callValueInfo[0].tokenId, "")
        self.assertTrue(out.transactions[0].info.HasField("receipt"))
        self.assertFalse(out.transactions[1].info.HasField("receipt"))
        a = out.transactions[0].contracts
        for left, right in zip(a, source.transactions[0].transaction.raw_data.contract):
            self.assertEqual(left.SerializeToString(), right.SerializeToString())
        self.assertFalse(a[1].HasField("parameter"))
        self.assertTrue(a[2].HasField("parameter"))
        self.assertEqual(a[2].parameter.value, b"")
        transfer = cls("protocol.TransferContract").FromString(a[0].parameter.value)
        self.assertEqual(transfer.amount, 42)
        self.assertEqual(transfer.owner_address, b"\x41" + b"a" * 20)
        empty, _ = rpc.convert(*fixture(empty_receipt=True), cls)
        self.assertTrue(empty.transactions[0].info.HasField("receipt"))
        self.assertEqual(empty.transactions[0].info.receipt.energy_usage, 0)

    def test_independent_comparison_covers_absent_and_internal_fixture_paths(self):
        spec = importlib.util.spec_from_file_location("tron_compare", Path(__file__).with_name("509-compare-tron-rpc.py"))
        compare = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(compare)
        raw, infos = fixture()
        block = cls("protocol.BlockExtention").FromString(raw)
        receipts = cls("protocol.TransactionInfoList").FromString(infos)
        for encoding in ("binary", "base58", "hex", "hex_no_prefix", "tron_base58"):
            rows = compare.expected_rows(block, receipts, cls, encoding, True, False)
            self.assertEqual([r["call_value"] for r in rows["internal_call_values"]], [17, -9, 17, -9])
            self.assertEqual([r["token_id"] for r in rows["internal_call_values"]], ["", "TOKEN", "", "TOKEN"])
            self.assertEqual([r["internal_index"] for r in rows["internal_call_values"]], [0, 0, 0, 0])
            self.assertEqual([r["call_value_index"] for r in rows["internal_call_values"]], [0, 1, 0, 1])
            self.assertIsNone(rows["transactions"][1]["receipt_result"])
            self.assertIsNone(rows["transactions"][1]["contract_type"])
            self.assertEqual([r["parameter"] for r in rows["contracts"]][1:], [None, b""])
            self.assertTrue(all(r["rejected"] for r in rows["internal_transactions"]))
        # The existing mapper filter follows the wrapper, even with failed receipts.
        block.transactions[0].result.result = False
        rows = compare.expected_rows(block, receipts, cls, "binary", False, False)
        self.assertEqual([r["transaction_index"] for r in rows["transactions"]], [1])
        self.assertEqual(len(rows["internal_call_values"]), 2)
        self.assertEqual(rows["contracts"], [])

    def test_actual_parquet_schema_check_rejects_type_nullability_and_metadata_drift(self):
        import copy
        import pyarrow as pa
        import pyarrow.parquet as pq
        spec = importlib.util.spec_from_file_location("tron_compare_schema", Path(__file__).with_name("509-compare-tron-rpc.py"))
        compare = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(compare)
        types = [("payload", "Binary", True), ("status", {"Dictionary": ["Int32", "Utf8"]}, True),
                 ("timestamp", {"Timestamp": ["Millisecond", "UTC"]}, False)]
        recorded = {"fields": [{"name": name, "data_type": dtype, "nullable": nullable,
                                  "dict_is_ordered": False, "metadata": {"field-key": "value"}}
                                 for name, dtype, nullable in types], "metadata": {"schema-key": "value"}}
        schema = pa.schema([pa.field(name, compare.arrow_type(dtype), nullable=nullable,
                                    metadata={b"field-key": b"value"}) for name, dtype, nullable in types],
                           metadata={b"schema-key": b"value"})
        arrays = [pa.array([b""], pa.binary()), pa.array(["SUCCESS"]).dictionary_encode(),
                  pa.array([1700000000123], pa.timestamp('ms', tz='UTC'))]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"schema.parquet"
            pq.write_table(pa.Table.from_arrays(arrays, schema=schema), path)
            restored = pq.ParquetFile(path).read().schema
            compare.check_schema(restored, recorded)
            for mutate in (
                lambda r: r['fields'][0].update(nullable=False),
                lambda r: r['fields'][0].update(data_type='Utf8'),
                lambda r: r['fields'][1].update(data_type={'Dictionary': ['Int64', 'Utf8']}),
                lambda r: r['fields'][2].update(data_type={'Timestamp': ['Millisecond', None]}),
                lambda r: r['fields'][0].update(metadata={'field-key': 'different'}),
                lambda r: r.update(metadata={'schema-key': 'different'}),
            ):
                modified = copy.deepcopy(recorded)
                mutate(modified)
                with self.assertRaises(AssertionError):
                    compare.check_schema(restored, modified)

    def test_bad_identity_missing_envelopes_and_wire_are_rejected(self):
        raw, infos = fixture()
        block_type = cls("protocol.BlockExtention")
        for mutate in (
            lambda x: setattr(x, "blockid", b"x" * 32),
            lambda x: setattr(x.block_header.raw_data, "number", 1),
            lambda x: setattr(x.transactions[0], "txid", b"x" * 32),
            lambda x: x.transactions[0].ClearField("result"),
        ):
            block = block_type.FromString(raw)
            mutate(block)
            with self.assertRaises(ValueError):
                rpc.convert(block.SerializeToString(), infos, cls)
        with self.assertRaises(ValueError):
            rpc.convert(raw + b(2, rpc.nested(raw, 2)), infos, cls)
        for bad in (b"\x80", b"\x0a\xff", b"\x00", b"\x0f"):
            with self.assertRaises(ValueError):
                list(rpc.fields(bad))

    def test_receipt_order_count_and_metadata_must_match(self):
        raw, original = fixture()
        typ = cls("protocol.TransactionInfoList")
        for mutate in (
            lambda x: x.transactionInfo.pop(),
            lambda x: x.transactionInfo.reverse(),
            lambda x: setattr(x.transactionInfo[0], "blockNumber", 1),
            lambda x: setattr(x.transactionInfo[0], "blockTimeStamp", 1),
            lambda x: setattr(x.transactionInfo[1], "id", x.transactionInfo[0].id),
        ):
            infos = typ.FromString(original)
            mutate(infos)
            with self.assertRaises(ValueError):
                rpc.convert(raw, infos.SerializeToString(), cls)

    def test_capture_has_two_fixed_calls_and_stops_on_first_failure(self):
        for fail_first in (False, True):
            calls, options = [], []
            block, infos = fixture()
            class Channel:
                def unary_unary(self, method):
                    def invoke(request, **kwargs):
                        calls.append((method, request, kwargs))
                        if fail_first:
                            raise RuntimeError("fixture denial")
                        return block if len(calls) == 1 else infos
                    return invoke
                def close(self):
                    pass
            def connect(endpoint, **kwargs):
                self.assertEqual(endpoint, rpc.ENDPOINT)
                options.extend(kwargs["options"])
                return Channel()
            fake = types.SimpleNamespace(insecure_channel=connect, __version__="offline-fixture")
            with tempfile.TemporaryDirectory() as tmp, patch.dict("sys.modules", grpc=fake):
                output = Path(tmp) / "capture"
                capture_args = types.SimpleNamespace(output=output, descriptor=args.descriptor)
                if fail_first:
                    with self.assertRaisesRegex(RuntimeError, "fixture denial"):
                        rpc.capture(capture_args, cls)
                else:
                    rpc.capture(capture_args, cls)
                self.assertEqual(len(calls), 1 if fail_first else 2)
                for i, (method, request, kwargs) in enumerate(calls):
                    self.assertEqual(method, rpc.METHODS[i])
                    self.assertEqual(cls("protocol.NumberMessage").FromString(request).num, rpc.HEIGHT)
                    self.assertEqual(kwargs, {"timeout": 20, "wait_for_ready": False, "metadata": ()})
                self.assertIn(("grpc.enable_retries", 0), options)
                self.assertIn(("grpc.enable_http_proxy", 0), options)
                self.assertIn(("grpc.service_config_disable_resolution", 1), options)
                report = json.loads((output / "capture.json").read_text())
                self.assertEqual(report["status"], "failed" if fail_first else "converted")


if __name__ == "__main__":
    unittest.main(argv=[__file__])
