"""Offline qualification for slots 300000000/1; requires protobuf and DuckDB CLI.

This deliberately decodes only CompactUpdateVoteState, the only Vote instruction
variant observed in this bounded sample. A different candidate variant fails the
check instead of borrowing production classification. The mapper tests cover
all eight official variants and administrative/malformed boundaries separately.
"""
import argparse
import hashlib
import json
import pathlib
import subprocess
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--descriptor', type=pathlib.Path, required=True)
parser.add_argument('--raw-dir', type=pathlib.Path, required=True)
parser.add_argument('--with-votes', type=pathlib.Path, required=True)
parser.add_argument('--without-votes', type=pathlib.Path, required=True)
parser.add_argument('--baseline', type=pathlib.Path, required=True)
parser.add_argument('--summary', type=pathlib.Path, required=True)
parser.add_argument('--duckdb', default='/opt/homebrew/bin/duckdb')
args = parser.parse_args()
pool = descriptor_pool.DescriptorPool()
for descriptor in descriptor_pb2.FileDescriptorSet.FromString(args.descriptor.read_bytes()).file:
    pool.Add(descriptor)
Block = message_factory.GetMessageClass(pool.FindMessageTypeByName('sf.solana.type.v1.Block'))
VOTE = bytes.fromhex('0761481d357474bb7c4d7624ebd3bdb3d8355e73d11043fc0da3538000000000')


class Wire:
    def __init__(self, data):
        self.data, self.offset = data, 0

    def take(self, count):
        end = self.offset + count
        assert end <= len(self.data), 'truncated vote payload'
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def integer(self, count):
        return int.from_bytes(self.take(count), 'little')

    def varint(self, bits):
        value = shift = 0
        while True:
            byte = self.integer(1)
            value |= (byte & 127) << shift
            assert value < (1 << bits), 'varint overflow'
            if byte < 128:
                assert shift == 0 or byte != 0, 'noncanonical varint'
                return value
            shift += 7
            assert shift < bits, 'varint exceeds wire width'


def compact_vote(data):
    # Official compact layout: discriminant, root, short_vec of (LEB128
    # offset, u8 confirmations), 32-byte hash, Option<i64> timestamp.
    assert len(data) <= 1232
    wire = Wire(data)
    assert wire.integer(4) == 12, 'new sample variant needs independent decoder review'
    root = wire.integer(8)
    slot = 0 if root == (1 << 64) - 1 else root
    count = wire.varint(16)
    assert count > 0, 'empty vote is conservatively retained'
    for _ in range(count):
        slot += wire.varint(64)
        assert slot < (1 << 64), 'lockout offset overflow'
        wire.integer(1)
    wire.take(32)
    has_timestamp = wire.integer(1)
    assert has_timestamp in (0, 1)
    if has_timestamp:
        wire.take(8)
    assert wire.offset == len(data), 'trailing vote payload bytes'


def query(sql):
    result = subprocess.run([args.duckdb, '-json', '-c', sql], capture_output=True, text=True, check=True)
    return json.loads(result.stdout or '[]')


def source(root, table):
    escaped = str(root / table / '*.parquet').replace("'", "''")
    return f"read_parquet('{escaped}')"


def dataset(root):
    return next(path.parent for path in root.rglob('blocks') if path.is_dir())


expected = {'transactions': [], 'vote_transactions': []}
raw_summary, failed = [], 0
for slot in [300000000, 300000001]:
    data = (args.raw_dir / f'{slot}.pb').read_bytes()
    block = Block.FromString(data)
    assert block.slot == slot
    counts = {'transactions': 0, 'vote_transactions': 0, 'failed': 0}
    for index, confirmed in enumerate(block.transactions):
        assert confirmed.HasField('transaction') and confirmed.HasField('meta')
        transaction, meta = confirmed.transaction, confirmed.meta
        assert transaction.HasField('message')
        if meta.err.err:
            failed += 1
            counts['failed'] += 1
            continue
        message = transaction.message
        table = 'transactions'
        if VOTE in message.account_keys:
            # Every successful Vote mention in this sample is a legacy compact
            # vote. These assertions qualify the sample's boundary explicitly.
            assert not message.versioned and not message.address_table_lookups
            assert len(message.instructions) == 1 and message.HasField('header')
            assert len(transaction.signatures) in (1, 2)
            assert all(len(value) == 64 for value in transaction.signatures)
            header = message.header
            assert header.num_required_signatures == len(transaction.signatures)
            assert header.num_readonly_signed_accounts < header.num_required_signatures
            assert header.num_readonly_unsigned_accounts <= len(message.account_keys) - len(transaction.signatures)
            assert all(len(value) == 32 for value in message.account_keys)
            assert len(message.recent_blockhash) == 32
            instruction = message.instructions[0]
            assert 0 < instruction.program_id_index < len(message.account_keys)
            assert message.account_keys[instruction.program_id_index] == VOTE
            assert all(value < len(message.account_keys) for value in instruction.accounts)
            compact_vote(instruction.data)
            table = 'vote_transactions'
        expected[table].append((slot, index))
        counts[table] += 1
    raw_summary.append({'slot': slot, 'sha256': hashlib.sha256(data).hexdigest(), **counts})

with_votes, without_votes, baseline = map(dataset, [args.with_votes, args.without_votes, args.baseline])
for table, wanted in expected.items():
    rows = query(f'select slot, transaction_index from {source(with_votes, table)} order by slot, transaction_index')
    actual = [(int(row['slot']), int(row['transaction_index'])) for row in rows]
    assert actual == wanted, f'raw classification mismatch for {table}'
assert not (without_votes / 'vote_transactions').exists(), '--without-votes wrote vote data'
comparisons = []
for table in sorted(path.name for path in with_votes.iterdir() if path.is_dir()):
    current = source(with_votes, table)
    for label, other in [('previous', baseline), ('without_votes', without_votes)]:
        if label == 'without_votes' and table == 'vote_transactions':
            continue
        previous = source(other, table)
        # #502 adds instruction positions independently; compare every shared
        # prior column and its type, without attributing its schema change here.
        old_fields = query('describe select * from ' + previous)
        new_fields = query('describe select * from ' + current)
        columns = [field['column_name'] for field in old_fields]
        assert [field for field in new_fields if field['column_name'] in columns] == old_fields
        projection = ', '.join('"' + name + '"' for name in columns)
        result = query(f'select (select count(*) from {current}) as row_count, '
                       f'(select count(*) from (select {projection} from {current} except all select * from {previous})) new_only, '
                       f'(select count(*) from (select * from {previous} except all select {projection} from {current})) old_only')[0]
        assert result['new_only'] == result['old_only'] == 0, (table, label, result)
        comparisons.append({'table': table, 'comparison': label, **result})
summary = {'raw_blocks': raw_summary, 'recognized_variant': 'CompactUpdateVoteState (12)',
           'expected_votes': len(expected['vote_transactions']), 'expected_ordinary': len(expected['transactions']),
           'failed_transactions_excluded': failed, 'all_raw_classifications_match': True,
           'all_prior_columns_and_without_vote_details_match': True, 'comparisons': comparisons}
args.summary.write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
