#!/usr/bin/env python3
"""Execute the README's finalized-intersection SQL against bounded local fixtures.

Requires DuckDB CLI; makes no network calls. The input rows intentionally have
no order field, matching the public table's event-order limitation.
"""
import json
from pathlib import Path
import re
import subprocess
import tempfile


def query(sql):
    return json.loads(subprocess.check_output(['duckdb', '-json', '-c', sql], text=True))


readme = (Path(__file__).resolve().parents[2] / 'README.md').read_text()
section = readme.split('### Non-final streams and reorgs\n', 1)[1].split('\n### ', 1)[0]
sql = re.search(r'```sql\n(.*?)\n```', section, re.S).group(1)
with tempfile.TemporaryDirectory(prefix='fireparq-474-query-') as scratch:
    root = Path(scratch)
    reference = root / 'finalized.parquet'
    history = root / 'reversible.parquet'
    # A is re-added; B is replaced at the same height; C has repeated delivery;
    # D is an uncovered reversible tail; E has only an UNDO in this capture.
    setup = f"""
    COPY (SELECT * FROM (VALUES (100, 'A'), (101, 'C'), (103, 'E')) f(block_num,block_id)) TO '{reference}';
    COPY (SELECT * FROM (VALUES
      (100,'A','NEW'), (100,'A','UNDO'), (100,'B','NEW'), (100,'B','UNDO'),
      (100,'A','NEW'), (101,'C','NEW'), (101,'C','NEW'),
      (102,'D','NEW'), (103,'E','UNDO'), (104,'F','UNKNOWN_9')
    ) h(block_num,block_id,fork_step)) TO '{history}';
    """
    subprocess.run(['duckdb', '-c', setup], check=True, capture_output=True)
    sql = sql.replace('finalized/mainnet/blocks/**/*.parquet', str(reference)).replace('reversible/mainnet/blocks/**/*.parquet', str(history))
    expected = [{'block_num':100,'block_id':'A'}, {'block_num':101,'block_id':'C'}]
    assert query(sql) == expected
    # Permute physical input row order; a supported identity query is unchanged.
    subprocess.run(['duckdb', '-c', f"COPY (SELECT * FROM read_parquet('{history}') ORDER BY fork_step, block_id DESC) TO '{root / 'permuted.parquet'}';"], check=True, capture_output=True)
    assert query(sql.replace(str(history), str(root / 'permuted.parquet'))) == expected
    # Identical unordered events can encode opposite terminal states.
    first = ['NEW', 'UNDO', 'NEW']
    second = ['NEW', 'NEW', 'UNDO']
    assert sorted(first) == sorted(second)
    assert first[-1] != second[-1]
    print(json.dumps({'readme_query_passed':True, 'identities':expected, 'physical_reordering_invariant':True, 'unordered_state_ambiguity_proved':True}))
