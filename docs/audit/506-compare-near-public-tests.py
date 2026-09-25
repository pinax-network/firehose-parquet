#!/usr/bin/env python3
"""Offline independent row-oracle and recursive physical-schema regressions."""
import importlib.util
from pathlib import Path
import unittest

import pyarrow as pa


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(file))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


c = module('near_compare', '506-compare-near-public.py')
f = module('near_fixture', '506-near-public-tests.py')


class ComparisonTests(unittest.TestCase):
    def test_independent_full_row_oracle(self):
        raw = f.fixture()
        rows = c.expected_rows(raw, f.anchor(raw), 'binary', True, True)
        self.assertEqual({k: len(v) for k, v in rows.items()}, {
            'blocks': 1, 'chunks': 1, 'transactions': 1, 'receipts': 2,
            'receipt_actions': 9, 'execution_logs': 4, 'state_changes': 0})
        self.assertEqual(rows['blocks'][0]['timestamp'], 1725405232941)
        self.assertEqual(rows['blocks'][0]['lib_num'], 149999998)
        self.assertEqual(rows['transactions'][0]['hash'], bytes([50]) * 32)
        self.assertEqual(rows['receipts'][0]['receipt_id'], bytes([81]) * 32)
        self.assertEqual(rows['receipts'][1]['receipt_index'], 1)
        self.assertIsNone(rows['receipts'][1]['signer_id'])
        self.assertEqual(rows['receipt_actions'][2]['args'], b'\x00\xff')
        self.assertEqual(rows['receipt_actions'][3]['deposit'], str(2 ** 128 - 1))
        self.assertEqual(rows['execution_logs'][1]['log_index'], 1)
        self.assertIsNone(rows['receipts'][0]['tx_hash'])

    def test_filter_and_same_block_origins_are_separate(self):
        raw = f.fixture(); tx = raw['shards'][0]['chunk']['transactions'][0]
        tx['outcome']['execution_outcome']['outcome']['receipt_ids'] = [f.h(81)]
        rows = c.expected_rows(raw, f.anchor(raw), 'base58', False, False)
        self.assertEqual(rows['receipts'][0]['tx_hash'], f.h(50))
        self.assertEqual(rows['receipt_actions'][0]['tx_hash'], f.h(50))
        tx['outcome']['execution_outcome']['outcome']['status'] = {'Failure': {'InvalidTxError': 'Expired'}}
        filtered = c.expected_rows(raw, f.anchor(raw), 'base58', False, False)
        self.assertEqual(filtered['transactions'], [])
        # Origins use all source txs, and independently emitted receipts stay.
        self.assertEqual(filtered['receipts'], rows['receipts'])
        self.assertEqual(filtered['receipt_actions'], rows['receipt_actions'])
        self.assertEqual(filtered['execution_logs'], rows['execution_logs'])

    def test_encodings_preserve_raw_function_args(self):
        raw = f.fixture()
        for mode in ('binary', 'base58', 'hex', 'hex_no_prefix', 'tron_base58'):
            rows = c.expected_rows(raw, f.anchor(raw), mode, False, True)
            self.assertEqual(rows['receipt_actions'][2]['args'], b'\x00\xff')
            self.assertNotIn('fork_step', rows['blocks'][0])

    def test_physical_nested_type_nullability_metadata_and_order(self):
        child = {'name': 'item', 'data_type': 'Binary', 'nullable': True,
                 'dict_is_ordered': False, 'dict_id': 0, 'metadata': {}}
        field = dict(child, name='receipt_ids', data_type={'List': child}, nullable=False)
        recorded = {'fields': [field], 'metadata': {}}
        correct = pa.schema([pa.field('receipt_ids', pa.list_(pa.field('item', pa.binary(), nullable=True)), nullable=False)])
        c.check_schema(correct, recorded)
        for wrong in (
            pa.schema([pa.field('receipt_ids', pa.list_(pa.string()), nullable=False)]),
            pa.schema([pa.field('receipt_ids', pa.list_(pa.binary()), nullable=True)]),
            pa.schema([pa.field('receipt_ids', pa.list_(pa.field('item', pa.binary(), nullable=False)), nullable=False)]),
            correct.with_metadata({'unexpected': 'value'}),
        ):
            with self.subTest(wrong=wrong), self.assertRaises(AssertionError): c.check_schema(wrong, recorded)


if __name__ == '__main__': unittest.main()
