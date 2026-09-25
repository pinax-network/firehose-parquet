#!/usr/bin/env python3
"""Independent original NearData JSON -> every NEAR Parquet value and legacy check.

No converter/converted protobuf is used to derive expected values. State-change
omission and old outcome ordering are explicit pinned producer rules, not claims
about the full native source. Requires pyarrow; entirely offline.
"""
import argparse
import base64
import copy
import hashlib
import json
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

TABLES = {'blocks', 'chunks', 'transactions', 'receipts', 'receipt_actions', 'execution_logs', 'state_changes'}
ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


def decode_hash(value):
    number = 0
    for c in value:
        number = number * 58 + ALPHABET.index(c)
    raw = b'\0' * (len(value) - len(value.lstrip('1')))
    raw += number.to_bytes((number.bit_length() + 7) // 8, 'big')
    assert len(raw) == 32
    return raw


def encode_hash(value, mode):
    raw = decode_hash(value)
    if mode == 'binary': return raw
    if mode == 'base58': return value
    if mode == 'hex': return '0x' + raw.hex()
    assert mode in ('hex_no_prefix', 'tron_base58')
    return raw.hex()  # 32 bytes are not Tron address length


def variant(value):
    if isinstance(value, str): return value, None
    assert len(value) == 1
    return next(iter(value.items()))


def expected_rows(document, anchor, mode, fork, include_failed):
    block = document['block']
    header = block['header']
    enc = lambda x: encode_hash(x, mode)
    millis = int(header['timestamp_nanosec']) // 1_000_000
    canonical = dict(block_num=header['height'], block_id=enc(header['hash']),
        parent_num=header['prev_height'], parent_id=enc(header['prev_hash']),
        lib_num=anchor['result']['header']['height'], timestamp=millis, date=millis // 86_400_000)
    if fork: canonical['fork_step'] = 'FINAL'
    rows = {name: [] for name in TABLES}
    rows['blocks'].append(dict(canonical, height=header['height'], hash=enc(header['hash']),
        prev_hash=enc(header['prev_hash']), prev_height=header['prev_height'], epoch_id=enc(header['epoch_id']),
        author=block['author'], gas_price=str(int(header['gas_price'])), total_supply=str(int(header['total_supply'])),
        chunks_included=header['chunks_included'], latest_protocol_version=header['latest_protocol_version']))
    origins = {}
    transactions = [tx for shard in document['shards'] if shard['chunk'] is not None for tx in shard['chunk']['transactions']]
    for tx in transactions:
        for rid in tx['outcome']['execution_outcome']['outcome']['receipt_ids']:
            assert rid not in origins, 'ambiguous source transaction origin'
            origins[rid] = tx['transaction']['hash']
    ti, ri = 0, 0
    for shard in document['shards']:
        sid = shard['shard_id']
        if shard['chunk'] is not None:
            chunk = shard['chunk']; ch = chunk['header']
            rows['chunks'].append(dict(canonical, shard_id=ch['shard_id'], chunk_hash=enc(ch['chunk_hash']),
                prev_state_root=enc(ch['prev_state_root']), gas_used=int(ch['gas_used']), gas_limit=int(ch['gas_limit']),
                height_created=ch['height_created'], height_included=ch['height_included'],
                encoded_length=ch['encoded_length'], author=chunk['author']))
            for item in chunk['transactions']:
                tx = item['transaction']; out = item['outcome']['execution_outcome']['outcome']
                status, _ = variant(out['status'])
                if include_failed or status != 'Failure':
                    rows['transactions'].append(dict(canonical, hash=enc(tx['hash']), transaction_index=ti,
                        signer_id=tx['signer_id'], receiver_id=tx['receiver_id'], shard_id=sid, nonce=tx['nonce'],
                        actions=','.join(variant(a)[0] for a in tx['actions']), status=status,
                        gas_burnt=int(out['gas_burnt']), tokens_burnt=str(int(out['tokens_burnt'])),
                        receipt_ids=[enc(r) for r in out['receipt_ids']],
                        converted_into_receipt_id=enc(out['receipt_ids'][0]) if out['receipt_ids'] else None))
                ti += 1
        outcomes = list(shard['receipt_execution_outcomes'])
        if header['height'] < 193_444_226:
            outcomes.sort(key=lambda item: decode_hash(item['execution_outcome']['id']))
        for item in outcomes:
            receipt = item['receipt']; out = item['execution_outcome']['outcome']
            rid = receipt['receipt_id']
            tag, body = variant(receipt['receipt'])
            origin = enc(origins[rid]) if rid in origins else None
            status, _ = variant(out['status'])
            context = dict(canonical, receipt_id=enc(rid), receipt_index=ri, tx_hash=origin,
                           shard_id=sid, predecessor_id=receipt['predecessor_id'])
            rows['receipts'].append(dict(context, receiver_id=receipt['receiver_id'],
                signer_id=body['signer_id'] if tag == 'Action' else None, status=status,
                gas_burnt=int(out['gas_burnt']), tokens_burnt=str(int(out['tokens_burnt'])),
                executor_id=out['executor_id'], receipt_ids=[enc(r) for r in out['receipt_ids']]))
            if tag == 'Action':
                for ai, act in enumerate(body['actions']):
                    kind, payload = variant(act)
                    rows['receipt_actions'].append(dict(context, action_index=ai,
                        receiver_id=receipt['receiver_id'], signer_id=body['signer_id'], action_kind=kind,
                        method_name=payload['method_name'] if kind == 'FunctionCall' else None,
                        args=base64.b64decode(payload['args'], validate=True) if kind == 'FunctionCall' else None,
                        gas=int(payload['gas']) if kind == 'FunctionCall' else None,
                        deposit=str(int(payload['deposit'])) if kind in ('FunctionCall', 'Transfer') else None))
            for li, line in enumerate(out['logs']):
                rows['execution_logs'].append(dict(context, log_index=li, executor_id=out['executor_id'], log=line))
            ri += 1
    return rows


def arrow_field(field):
    return pa.field(field['name'], arrow_type(field['data_type'], field['dict_is_ordered']), field['nullable'],
                    metadata={k.encode(): v.encode() for k, v in field['metadata'].items()})


def arrow_type(recorded, ordered=False):
    scalars = {'UInt64': pa.uint64(), 'UInt32': pa.uint32(), 'Int64': pa.int64(), 'Int32': pa.int32(),
               'Boolean': pa.bool_(), 'Binary': pa.binary(), 'Utf8': pa.string(), 'Date32': pa.date32()}
    if isinstance(recorded, str): return scalars[recorded]
    if set(recorded) == {'Dictionary'}:
        index, value = recorded['Dictionary']
        return pa.dictionary(arrow_type(index), arrow_type(value), ordered=ordered)
    if set(recorded) == {'List'}: return pa.list_(arrow_field(recorded['List']))
    assert set(recorded) == {'Timestamp'}, ('unsupported type', recorded)
    unit, timezone = recorded['Timestamp']; assert unit == 'Millisecond'
    return pa.timestamp('ms', tz=timezone)


def check_schema(actual, recorded):
    expected = pa.schema([arrow_field(f) for f in recorded['fields']],
                         metadata={k.encode(): v.encode() for k, v in recorded['metadata'].items()})
    assert actual.equals(expected, check_metadata=True), ('physical schema/type/nullability/metadata', actual, expected)


def read_table(root, case, name, count, schema):
    files = sorted((root / case / name).glob('*.parquet'))
    assert bool(files) == (count > 0), (case, name, 'file/row count contract')
    if not files: return None
    parts = []
    for file in files:
        part = pq.ParquetFile(file).read()
        check_schema(part.schema, schema)
        parts.append(part)
    result = pa.concat_tables(parts)
    assert result.num_rows == count
    return result


def normalized(table):
    for index, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type):
            table = table.set_column(index, field.name, table.column(index).cast(pa.int64()))
        elif pa.types.is_date32(field.type):
            table = table.set_column(index, field.name, table.column(index).cast(pa.int32()))
    return table.to_pylist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('before', 'after', 'raw', 'report'): parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    document = json.loads((args.raw / 'neardata.json').read_bytes())
    anchor = json.loads((args.raw / 'lib-anchor.json').read_bytes())
    assert anchor['result']['header']['hash'] == document['block']['header']['last_final_block']
    before = json.loads((args.before / 'manifest.json').read_text())
    after = json.loads((args.after / 'manifest.json').read_text())
    assert before['block_sha256'] == after['block_sha256'] == hashlib.sha256((args.raw / 'block.pb').read_bytes()).hexdigest()
    assert before['cases'].keys() == after['cases'].keys() and len(after['cases']) == 20
    raw_values, legacy_values, row_occurrences = 0, 0, 0
    cases = {}
    for case, old in before['cases'].items():
        new = after['cases'][case]
        assert all(new[k] == old[k] for k in ('encoding', 'fork', 'include_failed'))
        assert new['rows'].keys() == TABLES
        expected = expected_rows(document, anchor, new['encoding'], new['fork'], new['include_failed'])
        tables = {}
        for name, count in new['rows'].items():
            assert count == len(expected[name]), (case, name, 'row count', count, len(expected[name]))
            schema = new['schemas'][name]
            current = read_table(args.after, case, name, count, schema)
            if count:
                actual = normalized(current)
                for index, (row, exp) in enumerate(zip(actual, expected[name])):
                    assert row == exp, (case, name, index, {k: (row.get(k), exp.get(k)) for k in row.keys() | exp.keys() if row.get(k) != exp.get(k)})
                raw_values += count * len(actual[0])
            if name in old['rows']:
                assert count == old['rows'][name]
                old_schema = old['schemas'][name]
                fields = {f['name']: copy.deepcopy(f) for f in schema['fields']}
                assert dict(schema, fields=[fields[f['name']] for f in old_schema['fields']]) == old_schema, (case, name, 'legacy schema')
                previous = read_table(args.before, case, name, count, old_schema)
                if count:
                    for field in previous.column_names:
                        assert previous.column(field).equals(current.column(field)), (case, name, field, 'legacy values')
                    legacy_values += count * len(previous.column_names)
            row_occurrences += count
            tables[name] = {'rows': count, 'columns': len(schema['fields'])}
        cases[case] = tables
    txs = [t for s in document['shards'] if s['chunk'] is not None for t in s['chunk']['transactions']]
    receipts = [r for s in document['shards'] for r in s['receipt_execution_outcomes']]
    report = {'status': 'passed', 'qualification': 'NearData original indexer JSON with pinned producer conversion; no Firehose transport proof',
        'raw_sha256': {f: hashlib.sha256((args.raw / f).read_bytes()).hexdigest() for f in ('neardata.json', 'lib-anchor.json', 'block.pb')},
        'coverage': {'height': document['block']['header']['height'], 'transactions': len(txs), 'receipt_outcomes': len(receipts),
            'transaction_status': dict(Counter(variant(t['outcome']['execution_outcome']['outcome']['status'])[0] for t in txs)),
            'receipt_status': dict(Counter(variant(r['execution_outcome']['outcome']['status'])[0] for r in receipts)),
            'receipt_type': dict(Counter(variant(r['receipt']['receipt'])[0] for r in receipts)),
            'receipt_action_kinds': dict(Counter(variant(a)[0] for r in receipts if 'Action' in r['receipt']['receipt'] for a in r['receipt']['receipt']['Action']['actions'])),
            'logs': sum(len(r['execution_outcome']['outcome']['logs']) for r in receipts),
            'native_state_changes_omitted_by_producer': sum(len(s['state_changes']) for s in document['shards']),
            'enriched_receipt_tx_hashes_ignored': sum(r.get('tx_hash') is not None for r in receipts)},
        'actual_parquet_schema_checks': 'every nonempty baseline/candidate part: field order, recursive Arrow type, dictionary, UTC milliseconds, nullability and field/schema metadata; empty tables manifest only',
        'row_occurrences': row_occurrences, 'raw_value_comparisons': raw_values, 'legacy_value_comparisons': legacy_values, 'cases': cases}
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k != 'cases'}, indent=2))


if __name__ == '__main__': main()
