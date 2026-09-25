import argparse, hashlib, json, pathlib, platform, statistics, subprocess, time
p=argparse.ArgumentParser()
p.add_argument('--before', default='/tmp/fireparq-524-before')
p.add_argument('--after', default='/tmp/fireparq-524-after')
p.add_argument('--prepare-only', action='store_true')
p.add_argument('--duckdb', default='duckdb')
p.add_argument('--root', default='/tmp/fireparq-524-data')
a=p.parse_args()
root=pathlib.Path(a.root); root.mkdir(exist_ok=True)
duck=a.duckdb
def sql(q):
    subprocess.run([duck, ':memory:', '-c', q],check=True,stdout=subprocess.DEVNULL)
wide=root/'wide.parquet'
if not wide.exists():
    extras=', '.join(f'hash(i, {j})::UBIGINT AS payload_{j}' for j in range(64))
    sql(f"COPY (SELECT i::UBIGINT AS block_num, 'id'||i AS block_id, 'id'||(i-1) AS parent_id, i::BIGINT AS timestamp, {extras} FROM range(1,250001) t(i)) TO '{wide}' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED, ROW_GROUP_SIZE 16384)")
for n in (25,100,400):
    d=root/f'partitions-{n}'
    if not d.exists():
        sql(f"COPY (SELECT i::UBIGINT AS block_num, 'id'||i AS block_id, 'id'||(i-1) AS parent_id, i::BIGINT AS timestamp, lpad(((i-1)//{200000//n})::VARCHAR, 4, '0') AS part FROM range(1,200001) t(i)) TO '{d}' (FORMAT PARQUET, PARTITION_BY(part), COMPRESSION UNCOMPRESSED)")
if a.prepare_only:
    print('Prepared local synthetic datasets:',root); raise SystemExit
results={'platform':platform.platform(),'samples_per_version':7,'warmups_per_version':1,'measure':'wall-clock whole CLI validate, cached local files; process startup and result formatting included','datasets':[]}
for path in (wide,*(root/f'partitions-{n}' for n in (25,100,400))):
    samples={'before':[],'after':[]}; outputs={}
    def run(label,measured):
        start=time.perf_counter_ns()
        r=subprocess.run([getattr(a,label),'validate',str(path),'--cross-partition'],cwd='/tmp',capture_output=True,check=True)
        duration=(time.perf_counter_ns()-start)/1e6
        if measured:samples[label].append(duration)
        output=r.stdout.decode()
        if label in outputs:assert outputs[label]==output
        outputs[label]=output
    for label in samples:run(label,False)
    for i in range(7):
        for label in (('before','after') if i%2==0 else ('after','before')):run(label,True)
    assert outputs['before']==outputs['after'], 'validation result changed'
    files=[path] if path.is_file() else list(path.rglob('*.parquet'))
    results['datasets'].append({'name':path.name,'files':len(files),'parquet_bytes':sum(p.stat().st_size for p in files),'before_ms':samples['before'],'after_ms':samples['after'],'before_median_ms':statistics.median(samples['before']),'after_median_ms':statistics.median(samples['after']),'output_sha256':hashlib.sha256(outputs['after'].encode()).hexdigest(),'output':outputs['after']})
print(json.dumps(results,indent=2))
