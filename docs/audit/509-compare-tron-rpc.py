#!/usr/bin/env python3
"""Offline independent native RPC -> every Parquet value and legacy comparison.

Expected values come from the full pinned upstream protocol messages, never from
converted sf.tron blocks or the Rust mapper/decoder. Requires protobuf and pyarrow.
"""
import argparse
import copy
import hashlib
import json
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

TABLES = {'blocks', 'transactions', 'logs', 'internal_transactions', 'contracts', 'internal_call_values'}
ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


def b58(data):
    num, digits = int.from_bytes(data, 'big'), ''
    while num:
        num, digit = divmod(num, 58)
        digits = ALPHABET[digit] + digits
    return '1' * (len(data) - len(data.lstrip(b'\0'))) + digits


def encode(data, mode, reserved=False):
    if mode == 'binary':
        return data
    if mode == 'hex':
        return '0x' + data.hex()
    if mode == 'hex_no_prefix' or (mode == 'tron_base58' and reserved):
        return data.hex()
    if mode == 'base58':
        return b58(data)
    assert mode == 'tron_base58'
    if len(data) == 20:
        data = b'\x41' + data
    if len(data) == 21:
        return b58(data + hashlib.sha256(hashlib.sha256(data).digest()).digest()[:4])
    return data.hex()


def enum_name(message, field):
    value = message.DESCRIPTOR.fields_by_name[field].enum_type.values_by_number.get(getattr(message, field))
    return value.name if value else 'UNKNOWN'


def expected_rows(block, infos, cls, mode, fork, include_failed):
    header = block.block_header.raw_data
    enc = lambda x: encode(x, mode)
    reserved = lambda x: encode(x, mode, True)
    canonical = dict(block_num=header.number, block_id=enc(block.blockid), parent_num=header.number-1,
                     parent_id=enc(header.parentHash), lib_num=header.number-20,
                     timestamp=header.timestamp, date=header.timestamp // 86_400_000)
    if fork:
        canonical['fork_step'] = 'FINAL'
    rows = {table: [] for table in TABLES}
    rows['blocks'].append(dict(canonical, number=header.number, hash=reserved(block.blockid),
                              parent_hash=reserved(header.parentHash), witness_address=enc(header.witness_address),
                              version=header.version & 0xFFFFFFFF, tx_trie_root=reserved(header.txTrieRoot),
                              parent_number=header.number-1, num_transactions=len(block.transactions)))
    block_log_index = 0
    for ti, (tx, info) in enumerate(zip(block.transactions, infos.transactionInfo)):
        assert tx.txid == info.id
        raw = tx.transaction.raw_data
        first_log_index = block_log_index
        block_log_index += len(info.log)
        if not include_failed and not tx.result.result:
            continue
        row = dict(canonical, block_number=header.number, txid=reserved(tx.txid), result=tx.result.result,
                   code=enum_name(tx.result, 'code'), energy_used=tx.energy_used, energy_penalty=tx.energy_penalty,
                   fee=info.fee, contract_type=enum_name(raw.contract[0], 'type') if raw.contract else None,
                   expiration_ms=raw.expiration, tx_timestamp_ms=raw.timestamp, transaction_index=ti,
                   contract_address=enc(info.contract_address), res_message=info.resMessage)
        for field in ('energy_usage', 'energy_fee', 'origin_energy_usage', 'energy_usage_total', 'net_usage', 'net_fee', 'result', 'energy_penalty_total'):
            row['receipt_' + field] = (enum_name(info.receipt, field) if field == 'result' else getattr(info.receipt, field)) if info.HasField('receipt') else None
        rows['transactions'].append(row)
        for ci, contract in enumerate(raw.contract):
            row = dict(canonical, transaction_index=ti, tx_hash=reserved(tx.txid), contract_index=ci,
                       contract_type=enum_name(contract, 'type'), contract_type_id=contract.type,
                       parameter_type_url=contract.parameter.type_url if contract.HasField('parameter') else None,
                       parameter=contract.parameter.value if contract.HasField('parameter') else None,
                       permission_id=contract.Permission_id)
            row.update(dict.fromkeys(('owner_address', 'to_address', 'amount', 'asset_name', 'contract_address', 'data', 'call_value', 'call_token_value', 'token_id')))
            names = {1: 'TransferContract', 2: 'TransferAssetContract', 31: 'TriggerSmartContract'}
            if contract.HasField('parameter') and contract.type in names:
                name = 'protocol.' + names[contract.type]
                assert contract.parameter.type_url.rsplit('/', 1)[-1] == name
                value = cls(name).FromString(contract.parameter.value)
                row['owner_address'] = enc(value.owner_address)
                if contract.type in (1, 2):
                    row.update(to_address=enc(value.to_address), amount=value.amount)
                    if contract.type == 2:
                        row['asset_name'] = value.asset_name
                else:
                    row.update(contract_address=enc(value.contract_address), data=value.data, call_value=value.call_value,
                               call_token_value=value.call_token_value, token_id=value.token_id)
            rows['contracts'].append(row)
        for li, log in enumerate(info.log):
            row = dict(canonical, block_number=header.number, tx_hash=reserved(tx.txid), log_index=li,
                       address=enc(log.address), data=enc(log.data), transaction_index=ti, block_log_index=first_log_index+li)
            row.update({f'topic{i}': reserved(log.topics[i]) if i < len(log.topics) else None for i in range(4)})
            rows['logs'].append(row)
        for ii, internal in enumerate(info.internal_transactions):
            rows['internal_transactions'].append(dict(canonical, block_number=header.number, tx_hash=reserved(tx.txid),
                internal_index=ii, hash=reserved(internal.hash), caller_address=enc(internal.caller_address),
                transfer_to_address=enc(internal.transferTo_address), note=internal.note.decode('utf-8', errors='replace'),
                rejected=internal.rejected, transaction_index=ti))
            for vi, value in enumerate(internal.callValueInfo):
                rows['internal_call_values'].append(dict(canonical, transaction_index=ti, tx_hash=reserved(tx.txid),
                    internal_index=ii, call_value_index=vi, call_value=value.callValue, token_id=value.tokenId))
    return rows


def arrow_type(recorded, ordered=False):
    scalars = {'UInt64': pa.uint64(), 'UInt32': pa.uint32(), 'Int64': pa.int64(),
               'Int32': pa.int32(), 'Boolean': pa.bool_(), 'Binary': pa.binary(),
               'Utf8': pa.string(), 'Date32': pa.date32()}
    if isinstance(recorded, str):
        return scalars[recorded]  # Unexpected types must fail qualification.
    if set(recorded) == {'Dictionary'}:
        index, value = recorded['Dictionary']
        return pa.dictionary(arrow_type(index), arrow_type(value), ordered=ordered)
    assert set(recorded) == {'Timestamp'}, ('unsupported Arrow type', recorded)
    unit, timezone = recorded['Timestamp']
    assert unit == 'Millisecond'
    return pa.timestamp('ms', tz=timezone)


def check_schema(actual, recorded):
    metadata = lambda values: {key.encode(): value.encode() for key, value in values.items()}
    assert (actual.metadata or {}) == metadata(recorded['metadata']), 'schema metadata changed'
    assert actual.names == [field['name'] for field in recorded['fields']], 'schema field order changed'
    for actual_field, expected in zip(actual, recorded['fields']):
        name = expected['name']
        assert actual_field.type == arrow_type(expected['data_type'], expected['dict_is_ordered']), (name, 'physical round-trip Arrow type changed', actual_field.type, expected['data_type'])
        assert actual_field.nullable == expected['nullable'], (name, 'field nullability changed')
        assert (actual_field.metadata or {}) == metadata(expected['metadata']), (name, 'field metadata changed')


def read_table(root, case, table, count, schema):
    files = sorted((root/case/table).glob('*.parquet'))
    assert bool(files) == (count > 0), (case, table, 'empty/nonempty file contract')
    if not files:
        return None
    parts = []
    for path in files:
        part = pq.ParquetFile(path).read()
        check_schema(part.schema, schema)
        parts.append(part)
    result = pa.concat_tables(parts)
    assert result.num_rows == count
    return result


def normalized_rows(table):
    result = table
    for index, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type):
            assert field.type.unit == 'ms' and field.type.tz == 'UTC'
            result = result.set_column(index, field.name, table.column(index).cast(pa.int64()))
        elif pa.types.is_date32(field.type):
            result = result.set_column(index, field.name, table.column(index).cast(pa.int32()))
    return result.to_pylist()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for arg in ['before', 'after', 'raw', 'descriptor', 'report']:
        p.add_argument('--'+arg, type=Path, required=True)
    args = p.parse_args()
    pool = descriptor_pool.DescriptorPool()
    for file in descriptor_pb2.FileDescriptorSet.FromString(args.descriptor.read_bytes()).file:
        pool.Add(file)
    cls = lambda name: message_factory.GetMessageClass(pool.FindMessageTypeByName(name))
    raw_block = (args.raw/'block-extension.pb').read_bytes()
    raw_infos = (args.raw/'transaction-info-list.pb').read_bytes()
    block = cls('protocol.BlockExtention').FromString(raw_block)
    infos = cls('protocol.TransactionInfoList').FromString(raw_infos)
    assert block.block_header.raw_data.number == 80_000_000
    assert len(block.transactions) == len(infos.transactionInfo)
    assert len({tx.txid for tx in block.transactions}) == len(block.transactions)
    for tx, info in zip(block.transactions, infos.transactionInfo):
        assert tx.txid == info.id and info.blockNumber == 80_000_000
        assert info.blockTimeStamp == block.block_header.raw_data.timestamp
    before = json.loads((args.before/'manifest.json').read_text())
    after = json.loads((args.after/'manifest.json').read_text())
    assert before['block_sha256'] == after['block_sha256'] == hashlib.sha256((args.raw/'block.pb').read_bytes()).hexdigest()
    assert before['cases'].keys() == after['cases'].keys() and len(after['cases']) == 20
    cases = {}
    legacy_values, raw_values, total_rows = 0, 0, 0
    for case, old in before['cases'].items():
        new = after['cases'][case]
        assert all(old[key] == new[key] for key in ('encoding', 'fork', 'include_failed'))
        assert new['rows'].keys() == TABLES
        expected = expected_rows(block, infos, cls, new['encoding'], new['fork'], new['include_failed'])
        tables = {}
        for table, count in new['rows'].items():
            assert count == len(expected[table]), (case, table, 'raw row count')
            schema = new['schemas'][table]
            current = read_table(args.after, case, table, count, schema)
            if count:
                actual = normalized_rows(current)
                assert set(actual[0]) == set(expected[table][0]), (case, table, 'complete expected column inventory')
                for i, (row, exp) in enumerate(zip(actual, expected[table])):
                    assert row == exp, (case, table, i, {key: (row.get(key), exp.get(key)) for key in row.keys() | exp.keys() if row.get(key) != exp.get(key)})
                raw_values += count*len(actual[0])
                assert current.column_names == [f['name'] for f in schema['fields']]
            if table in old['rows']:
                assert count == old['rows'][table]
                # Compare every legacy Arrow field including nullability/metadata,
                # with only the documented first-contract nullability migration.
                old_schema = old['schemas'][table]
                fields = {f['name']: copy.deepcopy(f) for f in schema['fields']}
                projected = dict(schema, fields=[fields[f['name']] for f in old_schema['fields']])
                if table == 'transactions':
                    field = next(f for f in projected['fields'] if f['name'] == 'contract_type')
                    assert field['nullable'] is True
                    field['nullable'] = False
                assert projected == old_schema, (case, table, 'legacy schema changed')
                previous = read_table(args.before, case, table, count, old_schema)
                if count:
                    for name in previous.column_names:
                        assert previous.column(name).equals(current.column(name)), (case, table, name, 'legacy values changed')
                    legacy_values += count*len(previous.column_names)
            tables[table] = {'rows': count, 'columns': len(schema['fields']), 'legacy_columns': len(old['schemas'][table]['fields']) if table in old['schemas'] else 0}
            total_rows += count
        cases[case] = tables
    coverage = {
        'transactions': len(block.transactions),
        'contract_types': dict(sorted(Counter(enum_name(c, 'type') for tx in block.transactions for c in tx.transaction.raw_data.contract).items())),
        'wrapper_result_and_code': dict(Counter(f'{tx.result.result}/{enum_name(tx.result, "code")}' for tx in block.transactions)),
        'transaction_info_result': dict(Counter(enum_name(i, 'result') for i in infos.transactionInfo)),
        'receipt_result': dict(Counter(enum_name(i.receipt, 'result') if i.HasField('receipt') else 'ABSENT' for i in infos.transactionInfo)),
        'logs': sum(len(i.log) for i in infos.transactionInfo),
        'internal_transactions': sum(len(i.internal_transactions) for i in infos.transactionInfo),
        'call_values': sum(len(t.callValueInfo) for i in infos.transactionInfo for t in i.internal_transactions),
        'multi_contract_transactions': sum(len(tx.transaction.raw_data.contract) > 1 for tx in block.transactions),
        'missing_parameter': sum(not c.HasField('parameter') for tx in block.transactions for c in tx.transaction.raw_data.contract),
        'empty_contract_transactions': sum(not tx.transaction.raw_data.contract for tx in block.transactions),
    }
    report = {'status': 'passed', 'qualification': 'native RPC backed; pinned producer conversion; no Firehose transport or cursor proof',
              'raw_sha256': {'block-extension.pb': hashlib.sha256(raw_block).hexdigest(), 'transaction-info-list.pb': hashlib.sha256(raw_infos).hexdigest(), 'block.pb': after['block_sha256']},
              'descriptor_sha256': hashlib.sha256(args.descriptor.read_bytes()).hexdigest(), 'coverage': coverage,
              'actual_parquet_schema_checks': 'every baseline/candidate nonempty part: field order, Arrow types including dictionaries and UTC timestamps, nullability, field/schema metadata; empty tables have manifest schemas only',
              'cases': cases, 'row_occurrences': total_rows, 'legacy_value_comparisons': legacy_values, 'raw_value_comparisons': raw_values}
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True)+'\n')
    print(json.dumps({key: value for key, value in report.items() if key != 'cases'}, indent=2))


if __name__ == '__main__':
    main()
