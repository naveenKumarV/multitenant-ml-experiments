"""Run fixed scenarios, preserve logs, fail on any unexpected result."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / 'src'))
import torch
from checkpoint_worker import newest, atomic_json


def same(a, b):
    if isinstance(a, torch.Tensor):
        return isinstance(b, torch.Tensor) and torch.equal(a, b)
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    return a == b


def run(root, phase, seed, interrupt='none', omit='none'):
    out = root / phase
    out.mkdir(parents=True, exist_ok=False)
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    cmd = [sys.executable, '-m', 'torch.distributed.run', '--master-addr=127.0.0.1',
           f'--master-port={port}', '--nnodes=1',
           '--nproc-per-node=2', str(BASE/'src/checkpoint_worker.py'), '--root', str(root),
           '--phase', phase, '--seed', str(seed), '--interrupt', interrupt, '--omit', omit]
    atomic_json(out/'command.json', cmd)
    with (out/'process.log').open('w') as log:
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                                env={**os.environ, 'OMP_NUM_THREADS': '1',
                                     'GLOO_SOCKET_IFNAME': 'lo0' if sys.platform == 'darwin' else 'lo'})
        try:
            if interrupt in ('sigterm', 'abrupt'):
                deadline = time.monotonic() + 90
                while len(list(out.glob('paused-*.json'))) != 2:
                    if proc.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError(f'Workers did not reach interruption: {out}')
                    time.sleep(0.05)
                sent = time.monotonic()
                for file in sorted(out.glob('pid-*.json')):
                    os.kill(json.loads(file.read_text())['pid'],
                            signal.SIGTERM if interrupt == 'sigterm' else signal.SIGKILL)
                atomic_json(out/'signal.json', {'signal': interrupt, 'at_step': 35,
                                               'unix_time': time.time()})
            rc = proc.wait(timeout=90)
            elapsed = time.monotonic() - sent if interrupt in ('sigterm','abrupt') else None
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
    expected_failure = interrupt in ('abrupt', 'missing_rank', 'coordinator_before_manifest',
                                    'coordinator_after_manifest')
    if (rc != 0) != expected_failure:
        raise RuntimeError(f'Unexpected process exit {rc}: {out}')
    atomic_json(out/'exit.json', {'returncode': rc, 'signal_to_launcher_exit_seconds': elapsed})
    return elapsed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output', required=True)
    ap.add_argument('--seeds', default='7,19,41')
    a = ap.parse_args()
    output = Path(a.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    rows = []
    scenarios = {'sigterm':35, 'abrupt':20, 'missing_rank':20, 'torn_write':20,
                 'coordinator_before_manifest':20, 'coordinator_after_manifest':35}
    for seed in map(int, a.seeds.split(',')):
        baseline = output / f'seed-{seed}-baseline'
        run(baseline, 'baseline', seed)
        for scenario, expected_step in scenarios.items():
            root = output / f'seed-{seed}-{scenario}'
            grace = run(root, 'interrupted', seed, scenario)
            step, _ = newest(root/'checkpoints', 2)
            pointer = json.loads((root/'checkpoints/latest.json').read_text())
            pointer_dir = root/'checkpoints'/pointer['directory']
            manifest = json.loads((pointer_dir/'manifest.json').read_text())
            pointer_valid = all(hashlib.sha256((pointer_dir/s['file']).read_bytes()).hexdigest()
                                == s['sha256'] for s in manifest['shards'])
            assert step == expected_step and pointer_valid, (scenario, step)
            run(root, 'resumed', seed)
            equality = {}
            data_equal = True
            for rank in range(2):
                original = torch.load(baseline/'baseline'/f'final-{rank}.pt', weights_only=False)
                resumed = torch.load(root/'resumed'/f'final-{rank}.pt', weights_only=False)
                for key in original:
                    equality[f'rank{rank}/{key}'] = same(original[key], resumed[key])
                bt = json.loads((baseline/'baseline'/f'trace-{rank}.json').read_text())
                rt = json.loads((root/'resumed'/f'trace-{rank}.json').read_text())
                data_equal &= rt == [x for x in bt if x['step'] > step]
            assert all(equality.values()) and data_equal, (scenario, equality)
            row = dict(seed=seed, scenario=scenario, resume_step=step, lost_steps=35-step,
                       pointer_valid=pointer_valid, final_state_equal=equality,
                       continuation_trace_equal=data_equal, signal_to_exit_seconds=grace)
            rows.append(row)
            atomic_json(output/'results.partial.json', rows)
            print(f'PASS seed={seed} {scenario}: resume={step}, exact state and sample/loss trace', flush=True)
        # Negative controls fork the successful checkpoint at 35, not final step 60.
        import shutil
        positive = output/f'seed-{seed}-sigterm'
        for omit in ('rng','data','optimizer'):
            root = output/f'seed-{seed}-omit-{omit}'
            (root/'checkpoints').mkdir(parents=True)
            source = positive/'checkpoints/interrupted-step-0035'
            shutil.copytree(source, root/'checkpoints'/source.name)
            run(root, 'resumed', seed, omit=omit)
            model_equal = all(same(torch.load(baseline/'baseline'/f'final-{r}.pt', weights_only=False)['model'],
                                   torch.load(root/'resumed'/f'final-{r}.pt', weights_only=False)['model'])
                              for r in range(2))
            assert not model_equal, f'Negative control did not discriminate: {omit}'
            rows.append(dict(seed=seed, scenario=f'omit_{omit}', expected_divergence=True,
                             model_equal=model_equal))
            print(f'PASS negative control seed={seed}: omitting {omit} changes model', flush=True)
    atomic_json(output/'results.json', dict(created_utc=datetime.now(timezone.utc).isoformat(),
                python=sys.version, torch=torch.__version__, platform=platform.platform(),
                world_size=2, backend='gloo', storage='shared local POSIX filesystem',
                steps=60, interrupt_step=35, periodic_interval_steps=20, cases=rows))


if __name__ == '__main__':
    main()
