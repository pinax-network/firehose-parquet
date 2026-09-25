#!/usr/bin/env python3
"""Offline converter and fake HTTPS transport tests. No external request is made."""
import base64
import copy
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).with_name('506-near-public.py')
spec = importlib.util.spec_from_file_location('near_public', SCRIPT)
n = importlib.util.module_from_spec(spec)
spec.loader.exec_module(n)


def encoded(raw):
    number = int.from_bytes(raw, 'big')
    result = ''
    while number:
        number, digit = divmod(number, 58)
        result = n.ALPHABET[digit] + result
    return '1' * (len(raw) - len(raw.lstrip(b'\0'))) + result


def h(byte):
    return encoded(bytes([byte]) * 32)


PUB = 'ed25519:' + h(7)
SIG = 'ed25519:' + encoded(bytes([8]) * 64)


def all_actions():
    return ['CreateAccount', {'DeployContract': {'code': 'AGFzbQ=='}},
            {'FunctionCall': {'method_name': 'call', 'args': 'AP8=', 'gas': 9, 'deposit': '10'}},
            {'Transfer': {'deposit': str(2 ** 128 - 1)}}, {'Stake': {'stake': '99', 'public_key': PUB}},
            {'AddKey': {'public_key': PUB, 'access_key': {'nonce': 1, 'permission': 'FullAccess'}}},
            {'DeleteKey': {'public_key': PUB}}, {'DeleteAccount': {'beneficiary_id': 'beneficiary.near'}},
            {'Delegate': {'delegate_action': {'sender_id': 's.near', 'receiver_id': 'r.near',
                'actions': [{'Transfer': {'deposit': '11'}}], 'nonce': 4, 'max_block_height': n.HEIGHT + 1,
                'public_key': PUB}, 'signature': SIG}}]


def receipt(byte, data='action'):
    payload = {'Action': {'signer_id': 'signer.near', 'signer_public_key': PUB, 'gas_price': '123',
               'output_data_receivers': [{'data_id': h(70), 'receiver_id': 'data.near'}],
               'input_data_ids': [h(71)], 'actions': all_actions()}}
    if data != 'action':
        payload = {'Data': {'data_id': h(72), 'data': data}}
    return {'predecessor_id': 'before.near', 'receiver_id': 'receiver.near', 'receipt_id': h(byte), 'receipt': payload}


def outcome(byte, status=None):
    return {'proof': [{'hash': h(90), 'direction': 'Right'}], 'block_hash': h(2), 'id': h(byte),
            'outcome': {'logs': ['first', 'EVENT_JSON:{"event":"synthetic"}'], 'receipt_ids': [h(80)],
                        'gas_burnt': 456, 'tokens_burnt': str(2 ** 100), 'executor_id': 'receiver.near',
                        'status': {'SuccessValue': ''} if status is None else status,
                        'metadata': {'version': 3, 'gas_profile': []}}}


def fixture():
    header = {x: h(i + 1) for i, x in enumerate(n.HEADER_HASHES)}
    header.update(height=n.HEIGHT, prev_height=n.HEIGHT - 1, timestamp=1725405232941736000,
        timestamp_nanosec='1725405232941736000', chunks_included=1, latest_protocol_version=70,
        validator_proposals=[{'validator_stake_struct_version': 'V1', 'account_id': 'validator.near',
                             'public_key': PUB, 'stake': '12'}], chunk_mask=[True, False], gas_price='13',
        total_supply='14', challenges_result=[{'account_id': 'bad.near', 'is_double_sign': True}],
        approvals=[None, SIG], signature=SIG, block_ordinal=999, epoch_sync_data_hash=h(99))
    ch = {x: h(i + 20) for i, x in enumerate(n.CHUNK_HASHES)}
    ch.update(encoded_length=500, height_created=n.HEIGHT, height_included=n.HEIGHT, shard_id=4,
              gas_used=20, gas_limit=30, validator_reward='0', balance_burnt='22',
              validator_proposals=[], signature=SIG)
    absent = copy.deepcopy(ch)
    absent.update(shard_id=5, height_created=n.HEIGHT - 1, height_included=n.HEIGHT - 1)
    tx = {'transaction': {'hash': h(50), 'signer_id': 'signer.near', 'receiver_id': 'receiver.near',
          'public_key': PUB, 'nonce': 45, 'actions': all_actions(), 'signature': SIG},
          'outcome': {'execution_outcome': outcome(50, {'SuccessReceiptId': h(80)}), 'receipt': receipt(80)}}
    return {'block': {'author': 'author.near', 'header': header, 'chunks': [ch, absent]},
            'shards': [{'shard_id': 4, 'chunk': {'author': 'chunk.near', 'header': copy.deepcopy(ch),
                        'transactions': [tx], 'receipts': [receipt(90)], 'local_receipts': [receipt(91)],
                        'instant_receipts': [receipt(92)]},
                        'receipt_execution_outcomes': [
                            {'execution_outcome': outcome(82), 'receipt': receipt(82, None), 'tx_hash': h(51)},
                            {'execution_outcome': outcome(81), 'receipt': receipt(81), 'tx_hash': h(52)}],
                        'state_changes': [{'intentionally': 'omitted by producer'}]},
                       {'shard_id': 5, 'chunk': None, 'receipt_execution_outcomes': [], 'state_changes': []}]}


def anchor(document):
    return {'jsonrpc': '2.0', 'id': 'fireparq-506-lib', 'result': {'header': {
        'height': n.HEIGHT - 2, 'hash': document['block']['header']['last_final_block']}}}


class ConverterTests(unittest.TestCase):
    def test_exact_full_projection_and_intentional_losses(self):
        raw = fixture()
        result = n.project(raw)
        self.assertEqual(result['header']['timestamp_nanosec'], 1725405232941736000)
        self.assertEqual(len(result['header']['approvals']), 1)
        self.assertNotIn('block_ordinal', result['header'])
        self.assertNotIn('epoch_sync_data_hash', result['header'])
        self.assertEqual(result['state_changes'], [])
        self.assertEqual(len(result['shards']), 2)
        self.assertNotIn('chunk', result['shards'][1])
        chunk = result['shards'][0]['chunk']
        self.assertNotIn('local_receipts', chunk)
        self.assertEqual(len(chunk['receipts']), 1)
        self.assertEqual(chunk['transactions'][0]['outcome']['receipt']['receipt_id'], n.hash_value(h(80)))
        items = result['shards'][0]['receipt_execution_outcomes']
        self.assertEqual([x['execution_outcome']['id'] for x in items], [n.hash_value(h(81)), n.hash_value(h(82))])
        self.assertNotIn('tx_hash', items[0])
        self.assertEqual(items[0]['execution_outcome']['outcome']['metadata'], 0)
        self.assertEqual(items[0]['receipt']['action']['actions'][2]['function_call']['args'], 'AP8=')
        self.assertEqual(base64.b64decode(items[0]['receipt']['action']['actions'][3]['transfer']['deposit']['bytes']), b'\xff' * 16)
        self.assertEqual(items[1]['receipt']['data']['data'], '')
        # Same producer projection for None and Some(empty), while original JSON differs.
        self.assertEqual(n.receipt(receipt(82, None)), n.receipt(receipt(82, '')))
        self.assertNotEqual(receipt(82, None), receipt(82, ''))
        n.enrich(result, raw, anchor(raw))
        self.assertEqual(result['header']['last_final_block_height'], n.HEIGHT - 2)

    def test_protobuf_descriptor_validates_every_supported_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            desc = Path(tmp) / 'near.desc'
            root = SCRIPT.parents[2]
            subprocess.run(['protoc', '-I', str(root / 'proto'), '--descriptor_set_out=' + str(desc), 'near.proto'], check=True)
            raw = fixture()
            result = n.enrich(n.project(raw), raw, anchor(raw))
            self.assertGreater(len(n.protobuf(result, desc)), 1000)
            for tag in n.INVALID_TX:
                result['shards'][0]['receipt_execution_outcomes'][0]['execution_outcome']['outcome'].update(n.status({'Failure': {'InvalidTxError': tag}}))
                result['shards'][0]['receipt_execution_outcomes'][0]['execution_outcome']['outcome'].pop('success_value', None)
                n.protobuf(result, desc)

    def test_supported_failure_projection(self):
        def convert(kind, index=None):
            return n.failure({'ActionError': {'index': index, 'kind': kind}})['action_error']
        self.assertEqual(convert({'AccountAlreadyExists': {'account_id': 'a'}}), {'index': 0, 'account_already_exist': {'account_id': 'a'}})
        self.assertEqual(convert({'DeleteAccountStaking': {'account_id': 'omitted'}})['delete_account_staking'], {'account_id': ''})
        self.assertEqual(convert({'LackBalanceForState': {'account_id': 'a', 'amount': '258'}})['lack_balance_for_state']['balance'], n.bigint(258))
        for name in n.FUNCTION_ERROR:
            self.assertEqual(convert({'FunctionCallError': {name: 'detail'}})['function_call']['error'], n.FUNCTION_ERROR.index(name))
        for name in n.RECEIPT_ERROR:
            self.assertEqual(convert({'NewReceiptValidationError': {name: {}}})['new_receipt_validation']['error'], n.RECEIPT_ERROR.index(name))
        self.assertEqual(convert({'DelegateActionAccessKeyError': {'nested': 'omitted'}})['delegate_action_access_key_error'], {})

    def test_permissions_statuses_bytes_and_integer_bounds(self):
        permission = {'nonce': 3, 'permission': {'FunctionCall': {'allowance': None, 'receiver_id': 'r', 'method_names': ['', 'x']}}}
        self.assertNotIn('allowance', n.access_key(permission)['permission']['function_call'])
        permission['permission']['FunctionCall']['allowance'] = '0'
        self.assertEqual(n.access_key(permission)['permission']['function_call']['allowance'], n.bigint(0))
        self.assertEqual(n.status('Unknown'), {'unknown': {}})
        self.assertEqual(n.b58('1' * 32, 32), bytes(32))
        for value in (-1, 2 ** 128, True, 1.5, '+2', '01'):
            with self.subTest(value=value), self.assertRaises(ValueError): n.bigint(value)
        for value in ('AA=', 'AA===', 'AB==', '*'):
            with self.subTest(value=value), self.assertRaises(ValueError): n.decoded64(value)
        for value in ('0' * 32, '1' * 31, '1' * 33):
            with self.subTest(value=value), self.assertRaises(ValueError): n.b58(value, 32)

    def test_unknown_variants_and_linkage_fail_closed(self):
        for value in ({'DeployGlobalContract': {'code': ''}}, {'Future': {}}, {'Delegate': {'delegate_action': {'actions': ['Delegate']}}}):
            with self.subTest(value=value), self.assertRaises((ValueError, KeyError)): n.action(value)
        for mutate in (
            lambda d: d['block']['header'].update(height=n.HEIGHT + 1),
            lambda d: d['block']['header'].update(prev_height=0),
            lambda d: d['block']['header'].update(timestamp=1),
            lambda d: d['block']['header'].update(chunk_mask=[False, True]),
            lambda d: d['shards'].pop(),
            lambda d: d['shards'].append(copy.deepcopy(d['shards'][0])),
            lambda d: d['shards'][0]['chunk']['header'].update(gas_limit=0),
            lambda d: d['shards'][0]['chunk']['transactions'].append(copy.deepcopy(d['shards'][0]['chunk']['transactions'][0])),
            lambda d: d['shards'][0]['receipt_execution_outcomes'][0]['receipt'].update(receipt_id=h(30)),
            lambda d: d['shards'][0]['receipt_execution_outcomes'].append(copy.deepcopy(d['shards'][0]['receipt_execution_outcomes'][0])),
            lambda d: d['shards'][0]['chunk']['transactions'][0]['outcome']['receipt'].update(receipt_id=h(30)),
        ):
            raw = fixture(); mutate(raw)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError): n.project(raw)
        raw = fixture()
        for wrong in ({'jsonrpc': '2.0', 'id': 'fireparq-506-lib', 'error': {}},
                      {'jsonrpc': '2.0', 'id': 'fireparq-506-lib', 'result': {'header': {'height': n.HEIGHT, 'hash': raw['block']['header']['last_final_block']}}}):
            with self.assertRaises(ValueError): n.enrich(n.project(raw), raw, wrong)

    def test_duplicate_and_float_json_rejected(self):
        for data in (b'{"x":1,"x":2}', b'{"nested":{"x":1,"x":1}}', b'{"x":1.0}', b'{"x":NaN}', b'{"x":1e2}'):
            with self.subTest(data=data), self.assertRaises(ValueError): n.strict_json(data)


class FakeResponse:
    def __init__(self, status=200, raw=b'{}', headers=None):
        self.status, self.raw, self.offset, self.reads = status, raw, 0, 0
        self.headers = [('Content-Length', str(len(raw)))] if headers is None else headers
    def getheaders(self): return self.headers
    def read1(self, size):
        self.reads += 1
        part = self.raw[self.offset:self.offset + size]
        self.offset += len(part)
        return part


class FakeConnection:
    def __init__(self, response): self.response, self.sock, self.closed = response, None, False
    def request(self, method, path, body, headers): self.requested = (method, path, body, headers)
    def getresponse(self): return self.response
    def close(self): self.closed = True


class TransportTests(unittest.TestCase):
    def run_capture(self, responses):
        connections, hosts = [], []
        def factory(host, **kwargs):
            hosts.append((host, kwargs))
            conn = FakeConnection(responses[len(connections)])
            connections.append(conn)
            return conn
        with tempfile.TemporaryDirectory() as tmp, patch.object(n, 'protobuf', return_value=b'offline-test'):
            result = n.capture(Path(tmp) / 'capture', 'unused.desc', factory)
        return result, connections, hosts

    def test_exact_two_calls_and_no_ambient_credentials(self):
        raw = fixture()
        with patch.dict(os.environ, {'HTTPS_PROXY': 'http://user:password@invalid', 'ALL_PROXY': 'http://invalid', 'NETRC': '/invalid'}):
            result, connections, hosts = self.run_capture([FakeResponse(raw=json.dumps(raw).encode()), FakeResponse(raw=json.dumps(anchor(raw)).encode())])
        self.assertEqual(len(connections), 2)
        self.assertEqual([x[0] for x in hosts], ['mainnet.neardata.xyz', 'archival-rpc.mainnet.near.org'])
        self.assertEqual(connections[0].requested[0:2], ('GET', '/v0/block/150000000'))
        method, path, body, headers = connections[1].requested
        self.assertEqual((method, path), ('POST', '/'))
        self.assertEqual(json.loads(body)['params'], {'block_id': raw['block']['header']['last_final_block']})
        self.assertTrue(all(c.closed for c in connections))
        self.assertEqual(set(headers), {'Accept', 'Accept-Encoding', 'Connection', 'Content-Type'})
        self.assertEqual(result['requests'][0]['status'], 200)

    def test_exact_single_redirect_and_cookie_not_replayed(self):
        raw = fixture()
        result, connections, _ = self.run_capture([
            FakeResponse(302, b'', [('Location', 'https://a12.mainnet.neardata.xyz/v0/block/150000000'), ('Set-Cookie', 'session=discard')]),
            FakeResponse(raw=json.dumps(raw).encode()), FakeResponse(raw=json.dumps(anchor(raw)).encode())])
        self.assertEqual(len(result['requests']), 3)
        self.assertTrue(all('Cookie' not in c.requested[3] for c in connections))
        for target in ('http://a1.mainnet.neardata.xyz/v0/block/150000000', 'https://user@a1.mainnet.neardata.xyz/v0/block/150000000',
                       'https://a1.mainnet.neardata.xyz:444/v0/block/150000000', 'https://a1.mainnet.neardata.xyz.evil/v0/block/150000000',
                       'https://a1.mainnet.neardata.xyz/v0/block/150000001', 'https://a1.mainnet.neardata.xyz/v0/block/150000000?x',
                       '//a1.mainnet.neardata.xyz/v0/block/150000000'):
            with self.subTest(target=target), self.assertRaises(ValueError): n.redirect_url(target)

    def test_error_second_redirect_duplicate_json_stop_before_anchor(self):
        for responses in ([FakeResponse(429)], [FakeResponse(raw=b'{"x":1,"x":2}')],
                          [FakeResponse(302, b'', [('Location', 'https://a1.mainnet.neardata.xyz/v0/block/150000000')]), FakeResponse(302)]):
            connections = []
            def factory(*_args, **_kwargs):
                conn = FakeConnection(responses[len(connections)])
                connections.append(conn)
                return conn
            with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
                n.capture(Path(tmp) / 'capture', 'unused', factory)
            self.assertEqual(len(connections), len(responses))
            self.assertTrue(all(c.closed for c in connections))

    def test_stream_cap_encoding_and_duplicate_headers_rejected(self):
        for response in (FakeResponse(raw=b'x' * 12, headers=[]), FakeResponse(raw=b'x', headers=[('Content-Length', '100')]),
                         FakeResponse(headers=[('Content-Encoding', 'gzip')]),
                         FakeResponse(headers=[('Location', 'one'), ('location', 'two')])):
            connection = FakeConnection(response)
            with n.whole_deadline(60) as deadline:
                transport = n.Transport(deadline, lambda *_args, **_kwargs: connection)
                with self.assertRaises(ValueError): transport.request(n.BLOCK_URL, 'GET', 10)
            self.assertTrue(connection.closed)
            self.assertLessEqual(response.offset, 11)

    def test_request_alarm_bounds_header_wait_and_restores_outer_alarm(self):
        connection = FakeConnection(FakeResponse())
        connection.getresponse = lambda: time.sleep(10)
        started = time.monotonic()
        with n.whole_deadline(2) as deadline, patch.object(n, 'REQUEST_SECONDS', 0.05):
            with self.assertRaises(TimeoutError):
                n.Transport(deadline, lambda *_a, **_k: connection).request(n.BLOCK_URL, 'GET', 20)
            self.assertGreater(signal.getitimer(signal.ITIMER_REAL)[0], 1)
        self.assertTrue(connection.closed)
        self.assertLess(time.monotonic() - started, 1)

    def test_supervisor_kills_and_reaps_stalled_worker(self):
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            n.supervise([sys.executable, '-c', 'import time; time.sleep(10)'], 0.1)
        self.assertLess(time.monotonic() - started, 2)

    def test_budget_and_truncated_body(self):
        connection = FakeConnection(FakeResponse(raw=b'x', headers=[('Content-Length', '2')]))
        with n.whole_deadline(2) as deadline:
            transport = n.Transport(deadline, lambda *_a, **_k: connection)
            with self.assertRaisesRegex(ValueError, 'truncated'):
                transport.request(n.BLOCK_URL, 'GET', 10)
            transport.calls = [{}, {}, {}]
            with self.assertRaisesRegex(ValueError, 'budget'):
                transport.request(n.BLOCK_URL, 'GET', 10)
        self.assertTrue(connection.closed)

    def test_anchor_denial_retains_partial_provenance_and_document(self):
        raw = fixture()
        responses = [FakeResponse(raw=json.dumps(raw).encode()), FakeResponse(429)]
        connections = []
        def factory(*_a, **_k):
            connection = FakeConnection(responses[len(connections)])
            connections.append(connection)
            return connection
        with tempfile.TemporaryDirectory() as tmp, patch.object(n, 'protobuf', return_value=b'offline-test'):
            output = Path(tmp) / 'capture'
            with self.assertRaises(ValueError): n.capture(output, 'unused', factory)
            saved = json.loads((output / 'provenance.json').read_text())
            self.assertEqual(saved['failed_stage'], 'lib_anchor')
            self.assertEqual(saved['stage'], 'failed')
            self.assertEqual([r['status'] for r in saved['requests']], [200, 429])
            self.assertTrue(all('started_at' in r and 'finished_at' in r for r in saved['requests']))
            self.assertEqual(json.loads((output / 'neardata.json').read_text()), raw)
            self.assertFalse((output / 'block.pb').exists())

    def test_forced_worker_kill_keeps_attempted_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'capture'
            code = f'''import importlib.util,time
spec=importlib.util.spec_from_file_location("n", {str(SCRIPT)!r}); n=importlib.util.module_from_spec(spec); spec.loader.exec_module(n)
class Connection:
    sock=None
    def __init__(self,*a,**k): pass
    def request(self,*a,**k): pass
    def getresponse(self): time.sleep(10)
    def close(self): pass
n.capture({str(output)!r}, "unused", Connection)
'''
            with self.assertRaises(subprocess.TimeoutExpired):
                n.supervise([sys.executable, '-c', code], 0.5)
            saved = json.loads((output / 'provenance.json').read_text())
            self.assertEqual(saved['stage'], 'document')
            self.assertEqual(len(saved['requests']), 1)
            self.assertEqual(saved['requests'][0]['stage'], 'requesting')
            self.assertNotIn('finished_at', saved)

    def test_real_signal_deadline_interrupts_blocked_request_in_subprocess(self):
        # Real alarm, not a fake clock; shortened duration exercises identical primitive.
        code = f'''import importlib.util,time
spec=importlib.util.spec_from_file_location("n", {str(SCRIPT)!r}); n=importlib.util.module_from_spec(spec); spec.loader.exec_module(n)
with n.whole_deadline(0.1): time.sleep(10)
'''
        started = time.monotonic()
        result = subprocess.run([sys.executable, '-c', code], capture_output=True, timeout=3)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'whole capture deadline exceeded', result.stderr)
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual(n.WHOLE_SECONDS, 60)
        self.assertEqual(n.REQUEST_SECONDS, 20)


if __name__ == '__main__': unittest.main()
