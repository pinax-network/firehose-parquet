"""Reproduce #522 local whole-process RSS/CPU measurements with synthetic data.

Requires macOS /usr/bin/time and the DuckDB CLI. Never reads remote data or .env.
The before/after binaries must be preserved under the shared Cargo process lock.
"""
import argparse
import fcntl
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import tempfile
import time

p = argparse.ArgumentParser()
p.add_argument('--before', default='/tmp/fireparq-522-before')
p.add_argument('--after', default='/tmp/fireparq-522-final-after')
p.add_argument('--duckdb', default='/opt/homebrew/bin/duckdb')
p.add_argument('--root', default='/tmp/fireparq-522-benchmark')
p.add_argument('--prepare-only', action='store_true')
p.add_argument('--lock-file', default='/tmp/fireparq-cargo-session.lock', help='Shared build/benchmark lock held throughout preparation, timings, and verification')
p.add_argument('--samples', type=int, default=3)
p.add_argument('--flush-bytes', type=int, default=4194304)
p.add_argument('--files', type=int, nargs='+', default=[8, 32, 128])
p.add_argument('--versions', choices=['before', 'after'], nargs='+', default=['before', 'after'])
a = p.parse_args()
if a.samples < 1 or any(count < 1 for count in a.files) or a.flush_bytes < 0:
    p.error('samples/files must be positive and flush-bytes nonnegative')
# The process owns this descriptor until exit; concurrent builds and benchmarks
# use the same lock so their CPU/memory contention cannot contaminate timings.
run_lock = open(a.lock_file, 'a')
fcntl.flock(run_lock, fcntl.LOCK_EX)
started_at = datetime.now(timezone.utc).isoformat()
root = Path(a.root)
root.mkdir(exist_ok=True)

def quote(path):
    return "'" + str(path).replace("'", "''") + "'"

def sql(query):
    return subprocess.check_output([a.duckdb, ':memory:', '-csv', '-noheader', '-c', query], text=True).strip()

fixture = root / 'fixture.parquet'
if not fixture.exists():
    columns = ', '.join(f'hash(i, {n})::UBIGINT AS payload_{n}' for n in range(32))
    sql(f'COPY (SELECT i::UBIGINT AS block_num, {columns} FROM range(8192) t(i)) TO {quote(fixture)} (FORMAT PARQUET, COMPRESSION UNCOMPRESSED, ROW_GROUP_SIZE 2048)')
for count in a.files:
    destination = root / f'input-{count}' / 'blocks/year=2024/month=01/day=01/hour=00/minute=00'
    destination.mkdir(parents=True, exist_ok=True)
    for n in range(count):
        path = destination / f'part-{n:06}.parquet'
        if not path.exists():
            shutil.copyfile(fixture, path)
if a.prepare_only:
    print(f'Prepared synthetic local groups under {root}')
    raise SystemExit

env = {k: os.environ[k] for k in ('PATH', 'HOME', 'TMPDIR') if k in os.environ}
results = {
    'platform': platform.platform(), 'samples_per_version': a.samples,
    'serialization_lock': a.lock_file, 'started_at': started_at,
    'duckdb_version': subprocess.check_output([a.duckdb, '--version'], text=True).strip(),
    'input_fixture_sha256': hashlib.sha256(fixture.read_bytes()).hexdigest(),
    'configuration': {'flush_bytes': a.flush_bytes, 'compression': 'zstd', 'rows_per_input': 8192, 'columns': 33},
    'method': 'Whole CLI, cached local files, debug builds with debuginfo disabled; includes startup and two-pass decoding. Inputs are identical file copies. macOS time peak RSS is bytes. Output verification is outside the measured process.',
    'binaries': {label: {'path': getattr(a, label), 'sha256': hashlib.sha256(Path(getattr(a, label)).read_bytes()).hexdigest()} for label in ('before', 'after')},
    'datasets': [],
}
for count in a.files:
    source = root / f'input-{count}'
    samples = {label: [] for label in a.versions}
    for iteration in range(a.samples):
        for label in (a.versions if iteration % 2 == 0 else list(reversed(a.versions))):
            with tempfile.TemporaryDirectory(prefix='run-', dir=root) as directory:
                output = Path(directory) / 'output'
                (Path(directory) / '.env').write_text('')
                start = time.perf_counter_ns()
                result = subprocess.run(['/usr/bin/time', '-l', getattr(a, label), 'rollup', str(source), '--output', str(output), '--flush-bytes', str(a.flush_bytes), '--compression', 'zstd'], cwd=directory, env=env, capture_output=True, text=True, check=True)
                elapsed = (time.perf_counter_ns() - start) / 1e6
                timing = re.search(r'([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys', result.stderr)
                rss = re.search(r'(\d+)\s+maximum resident set size', result.stderr)
                assert timing and rss, result.stderr[-2000:]
                files = list(output.rglob('*.parquet'))
                actual = str(output / '**/*.parquet')
                expected = str(source / '**/*.parquet')
                # All 33 column values and their multiplicities must be identical.
                difference = sql(f'SELECT count(*) FROM ((SELECT * FROM read_parquet({quote(actual)}, hive_partitioning=false) EXCEPT ALL SELECT * FROM read_parquet({quote(expected)}, hive_partitioning=false)) UNION ALL (SELECT * FROM read_parquet({quote(expected)}, hive_partitioning=false) EXCEPT ALL SELECT * FROM read_parquet({quote(actual)}, hive_partitioning=false)))')
                assert difference == '0', difference
                expected_schema = sql(f'DESCRIBE SELECT * FROM read_parquet({quote(expected)}, hive_partitioning=false)')
                actual_schema = sql(f'DESCRIBE SELECT * FROM read_parquet({quote(actual)}, hive_partitioning=false)')
                assert actual_schema == expected_schema
                samples[label].append({'elapsed_ms': elapsed, 'user_seconds': float(timing[2]), 'system_seconds': float(timing[3]), 'peak_rss_bytes': int(rss[1]), 'output_files': len(files), 'output_bytes': sum(f.stat().st_size for f in files), 'difference_rows': int(difference)})
    results['datasets'].append({'input_files': count, 'input_bytes': fixture.stat().st_size * count, 'input_rows': count * 8192, 'samples': samples, 'medians': {label: {field: statistics.median(sample[field] for sample in values) for field in ('elapsed_ms', 'user_seconds', 'system_seconds', 'peak_rss_bytes')} for label, values in samples.items()}})
results['completed_at'] = datetime.now(timezone.utc).isoformat()
print(json.dumps(results, indent=2))
