"""Local-only #520 qualification: isolated client RSS and real bounded wire I/O.

The provider is a separate Python process (this parent), storing complete objects
on disk, not in the measured Rust process. No credentials or remote URLs needed.
"""
import argparse
import fcntl
import hashlib
import hmac
import http.server
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.parse

p = argparse.ArgumentParser()
p.add_argument('--binary', required=True, help='Preserved core test executable')
p.add_argument('--root', default='/tmp/fireparq-520-benchmark')
p.add_argument('--lock-file', default='/tmp/fireparq-cargo-session.lock')
p.add_argument('--sizes-mib', type=int, nargs='+', default=[16, 128, 512])
p.add_argument('--encode-sizes-mib', type=int, nargs='+', default=[16, 128])
p.add_argument('--slow-seconds', type=int, default=31)
a = p.parse_args()
assert all(0 < n <= 1024 for n in a.sizes_mib + a.encode_sizes_mib)
assert 0 <= a.slow_seconds <= 60
lock = open(a.lock_file, 'a')
fcntl.flock(lock, fcntl.LOCK_EX)
root = Path(a.root)
root.mkdir(parents=True, exist_ok=True)
objects = {}
object_lock = threading.Lock()
revision = 0
summary = {}
delay_seconds = 0
provider_root = tempfile.TemporaryDirectory(prefix='provider-', dir=root)


def signature_is_valid(handler, parts, query):
    """Independently verify real SDK SigV4 query signing using fixture-only key."""
    credential = query['X-Amz-Credential'][0].split('/')
    assert credential[0] == 'fixture-key' and credential[2:] == ['us-east-1', 's3', 'aws4_request']
    assert query['X-Amz-SignedHeaders'] == ['host']
    quote = lambda s: urllib.parse.quote(s, safe='-_.~')
    canonical_query = '&'.join(f'{quote(k)}={quote(v)}' for k, values in sorted(query.items()) if k != 'X-Amz-Signature' for v in sorted(values))
    canonical = f'PUT\n{parts.path}\n{canonical_query}\nhost:{handler.headers["Host"]}\n\nhost\nUNSIGNED-PAYLOAD'
    scope = '/'.join(credential[1:])
    string = f'AWS4-HMAC-SHA256\n{query["X-Amz-Date"][0]}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}'
    key = b'AWS4fixture-secret'
    for value in credential[1:]:
        key = hmac.new(key, value.encode(), hashlib.sha256).digest()
    return hmac.compare_digest(hmac.new(key, string.encode(), hashlib.sha256).hexdigest(), query['X-Amz-Signature'][0])


class Provider(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_):
        pass

    def reply(self, status, body=b'', obj=None):
        self.send_response(status)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.send_header('Last-Modified', 'Fri, 25 Sep 2026 00:00:00 GMT')
        if obj:
            self.send_header('ETag', obj['etag'])
            self.send_header('x-amz-version-id', obj['version'])
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)
        self.close_connection = True

    def do_PUT(self):
        global revision
        parts = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parts.query)
        key = parts.path
        data = key.endswith('.parquet')
        if data:
            assert signature_is_valid(self, parts, query)
            assert self.headers.get('If-None-Match') == '*'
            assert self.headers.get('Content-Type') == 'application/vnd.apache.parquet'
        length = int(self.headers['Content-Length'])
        temporary = tempfile.NamedTemporaryFile(dir=provider_root.name, delete=False)
        remaining = length
        while remaining:
            chunk = self.rfile.read(min(65536, remaining))
            if not chunk:
                temporary.close()
                os.unlink(temporary.name)
                return
            temporary.write(chunk)
            remaining -= len(chunk)
        temporary.close()
        with object_lock:
            current = objects.get(key)
            denied = (self.headers.get('If-None-Match') == '*' and current is not None) or (self.headers.get('If-Match') and (current is None or current['etag'] != self.headers['If-Match']))
            if denied:
                os.unlink(temporary.name)
                self.reply(412, b'<Error><Code>PreconditionFailed</Code></Error>')
                return
            revision += 1
            target = Path(provider_root.name) / hashlib.sha256(key.encode()).hexdigest()
            os.replace(temporary.name, target)
            obj = {'path': str(target), 'size': length, 'etag': f'"version-{revision}"', 'version': str(revision)}
            objects[key] = obj
            if data:
                summary['data_puts'] = summary.get('data_puts', 0) + 1
                summary['signature_verified'] = True
                summary['provider_disk_bytes'] = length
        if data and delay_seconds:
            time.sleep(delay_seconds)
        self.reply(200, obj=obj)

    def do_GET(self):
        parts = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parts.query)
        with object_lock:
            obj = objects.get(parts.path)
            if obj is None:
                self.reply(404, b'<Error><Code>NoSuchKey</Code></Error>')
                return
            if self.headers.get('If-Match') and self.headers['If-Match'] != obj['etag'] or query.get('versionId', [obj['version']])[0] != obj['version']:
                self.reply(412, b'<Error><Code>PreconditionFailed</Code></Error>')
                return
            obj = dict(obj)
            data = parts.path.endswith('.parquet')
            if data:
                summary['data_gets'] = summary.get('data_gets', 0) + 1
                summary['pinned_get'] = self.headers.get('If-Match') == obj['etag'] and query.get('versionId') == [obj['version']]
        if data and delay_seconds:
            time.sleep(delay_seconds)
        self.send_response(200)
        self.send_header('Content-Length', str(obj['size']))
        self.send_header('ETag', obj['etag'])
        self.send_header('x-amz-version-id', obj['version'])
        self.send_header('Last-Modified', 'Fri, 25 Sep 2026 00:00:00 GMT')
        self.send_header('Connection', 'close')
        self.end_headers()
        with open(obj['path'], 'rb') as source:
            shutil.copyfileobj(source, self.wfile, length=65536)
        self.close_connection = True

    def do_DELETE(self):
        key = urllib.parse.urlsplit(self.path).path
        with object_lock:
            old = objects.pop(key, None)
            if old:
                os.unlink(old['path'])
        self.reply(204)


server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Provider)
threading.Thread(target=server.serve_forever, daemon=True).start()
endpoint = f'http://127.0.0.1:{server.server_port}'
env = {k: os.environ[k] for k in ('PATH', 'HOME') if k in os.environ}
results = {'platform': platform.platform(), 'binary_sha256': hashlib.sha256(Path(a.binary).read_bytes()).hexdigest(), 'method': 'Each Rust child measured independently by macOS time; disk-backed loopback provider runs in separate parent. Serialized with build lock. Encoding RSS includes full mapper batch and its construction; transfer RSS excludes preparation and provider. No throughput claim.', 'cases': []}


def run(mode, mib, path):
    global summary
    summary = {}
    if mode == 'transfer':
        with object_lock:
            for obj in objects.values():
                os.unlink(obj['path'])
            objects.clear()
    scratch = tempfile.TemporaryDirectory(prefix='client-spool-', dir=root)
    child_env = dict(env, TMPDIR=scratch.name, FIREPARQ_520_MODE=mode, FIREPARQ_520_ROWS=str(mib * 1024), FIREPARQ_520_FILE=str(path), FIREPARQ_520_ENDPOINT=endpoint)
    started = time.monotonic()
    result = subprocess.run(['/usr/bin/time', '-l', a.binary, '--ignored', '--exact', 'writer::protected::tests::qualification::native_spool_process_qualification', '--nocapture'], env=child_env, cwd=root, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr[-6000:])
    output = re.search(r'QUALIFICATION (\{.*\})', result.stdout)
    rss = re.search(r'(\d+)\s+maximum resident set size', result.stderr)
    assert output and rss
    record = json.loads(output[1])
    record.update(peak_rss_bytes=int(rss[1]), elapsed_seconds=round(time.monotonic()-started, 3), payload_mib=mib)
    if mode == 'transfer':
        assert summary['data_puts'] == 1 and summary['data_gets'] == 1 and summary['signature_verified'] and summary['pinned_get']
        record.update(summary)
        record['injected_delay_seconds_per_put_and_get'] = delay_seconds
    assert not list(Path(scratch.name).iterdir()), 'private scratch must disappear after process exit'
    scratch.cleanup()
    print(json.dumps(record), flush=True)
    results['cases'].append(record)


try:
    for mib in a.sizes_mib:
        fixture = root / f'part-{mib}.parquet'
        run('prepare', mib, fixture)
        run('transfer', mib, fixture)
    for mib in a.encode_sizes_mib:
        for mode in ('encode-memory', 'encode-spool'):
            run(mode, mib, root / 'unused.parquet')
    if a.slow_seconds:
        delay_seconds = a.slow_seconds
        mib = a.sizes_mib[0]
        run('transfer', mib, root / f'part-{mib}.parquet')
finally:
    server.shutdown()
    provider_root.cleanup()
(root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
