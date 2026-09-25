"""Compare preserved before/after example binaries using caller-supplied raw blocks.
No network access. Run without concurrent builds or CPU benchmarks. The optional
lock cooperates with a local build queue; it is not a repository requirement.
"""
import argparse, contextlib, fcntl, hashlib, json, pathlib, platform, statistics, subprocess

p = argparse.ArgumentParser()
p.add_argument('--before', required=True)
p.add_argument('--after', required=True)
p.add_argument('--evm', required=True)
p.add_argument('--solana', action='append', default=[])
p.add_argument('--beacon')
p.add_argument('--iterations', type=int, default=200)
p.add_argument('--samples', type=int, default=5)
p.add_argument('--output', required=True)
p.add_argument('--lock-file')
a = p.parse_args()
assert a.iterations > 0 and a.samples > 0
cases = [('evm', a.evm)] + [('solana', v) for v in a.solana]
if a.beacon:
    cases.append(('beacon', a.beacon))
modes = [('before', a.before, False), ('after_owned', a.after, False), ('after_borrowed', a.after, True)]
result = {'platform': platform.platform(), 'iterations': a.iterations, 'samples': a.samples,
          'binary_sha256': {name: hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest() for name, path in [('before', a.before), ('after', a.after)]},
          'cases': []}
with contextlib.ExitStack() as stack:
    if a.lock_file:
        lock = stack.enter_context(open(a.lock_file, 'a'))
        fcntl.flock(lock, fcntl.LOCK_EX)
    for chain, source in cases:
        records = {name: [] for name, _, _ in modes}
        reference = None
        for repetition in range(a.samples):
            # Rotate the first mode, avoiding a systematic ordering advantage.
            order = modes[repetition % 3:] + modes[:repetition % 3]
            for name, binary, borrowed in order:
                command = [binary, '--chain', chain, '--block', source, '--iterations', str(a.iterations)]
                if borrowed:
                    command.append('--borrowed')
                sample = json.loads(subprocess.check_output(command, text=True))
                signature = {k: sample[k] for k in ('input_bytes', 'input_sha256', 'output')}
                if reference is None:
                    reference = signature
                assert signature == reference, f'Logical output mismatch: {chain}/{name}'
                records[name].append({k: sample[k] for k in ('decode_seconds', 'map_flush_seconds')})
        result['cases'].append({'chain': chain, 'source_name': pathlib.Path(source).name,
                                **reference, 'samples': records,
                                'medians': {name: {metric: statistics.median(r[metric] for r in samples) for metric in ('decode_seconds', 'map_flush_seconds')} for name, samples in records.items()}})
        pathlib.Path(a.output).write_text(json.dumps(result, indent=2) + '\n')
        print(chain, pathlib.Path(source).name, result['cases'][-1]['medians'], flush=True)
