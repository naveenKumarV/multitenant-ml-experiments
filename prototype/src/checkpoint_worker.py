"""Two actual CPU processes; synchronous gradient averaging with Gloo.
Storage contract: a shared POSIX filesystem, immutable attempt directories,
fsync + rename publication. This does not emulate an object-store API.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import time
from datetime import timedelta

import torch
import torch.distributed as dist


def atomic_json(path, value):
    path = Path(path)
    tmp = path.with_suffix('.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def newest(root, world):
    valid = []
    for p in root.glob('*/manifest.json'):
        try:
            m = json.loads(p.read_text())
            if m['world_size'] == world and len(m['shards']) == world and all(
                sha(p.parent / row['file']) == row['sha256'] for row in m['shards']
            ):
                valid.append((m['step'], p.parent))
        except (OSError, ValueError, KeyError):
            pass
    return max(valid, key=lambda x: x[0]) if valid else (0, None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', required=True)
    ap.add_argument('--phase', required=True)
    ap.add_argument('--seed', type=int, required=True)
    ap.add_argument('--steps', type=int, default=60)
    ap.add_argument('--interrupt', default='none')
    ap.add_argument('--omit', default='none')
    a = ap.parse_args()
    rank, world = int(os.environ['RANK']), int(os.environ['WORLD_SIZE'])
    root = Path(a.root)
    phase = root / a.phase
    phase.mkdir(parents=True, exist_ok=True)
    ckpts = root / 'checkpoints'
    ckpts.mkdir(exist_ok=True)
    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    dist.init_process_group('gloo', timeout=timedelta(seconds=15))
    torch.manual_seed(a.seed)
    model = torch.nn.Sequential(torch.nn.Linear(4, 8), torch.nn.Tanh(),
                                torch.nn.Dropout(0.25), torch.nn.Linear(8, 1)).double()
    opt = torch.optim.Adam(model.parameters(), lr=0.01)
    torch.manual_seed(a.seed + rank * 1000 + 100)
    random.seed(a.seed + rank)
    gen = torch.Generator().manual_seed(123)
    x = torch.randn(128, 4, generator=gen, dtype=torch.float64)
    y = (x[:, 0] * 0.7 - x[:, 1] * 0.2 + x[:, 2].sin()).unsqueeze(1)
    order = list(range(rank, 128, world))
    random.shuffle(order)
    cursor = 0
    start, directory = newest(ckpts, world)
    if directory:
        state = torch.load(directory / f'rank-{rank}.pt', weights_only=False)
        model.load_state_dict(state['model'])
        if a.omit != 'optimizer':
            opt.load_state_dict(state['optimizer'])
        if a.omit != 'rng':
            torch.set_rng_state(state['torch_rng'])
            random.setstate(state['python_rng'])
        if a.omit != 'data':
            order, cursor = state['order'], state['cursor']
    stop = False
    def term(_signum, _frame):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, term)
    atomic_json(phase / f'pid-{rank}.json', {'pid': os.getpid(), 'resume_step': start})
    trace = []

    def checkpoint(step, fault='none'):
        directory = ckpts / f'{a.phase}-step-{step:04d}'
        directory.mkdir(exist_ok=True)
        state = dict(step=step, model=model.state_dict(), optimizer=opt.state_dict(),
                     torch_rng=torch.get_rng_state(), python_rng=random.getstate(),
                     order=order, cursor=cursor)
        if fault == 'missing_rank' and rank == 1:
            # A real process loss before its shard is written.
            os.kill(os.getpid(), signal.SIGKILL)
        file = directory / f'rank-{rank}.pt'
        tmp = file.with_suffix('.tmp')
        with tmp.open('wb') as f:
            torch.save(state, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, file)
        digest = sha(file)
        if fault == 'torn_write' and rank == 1:
            with file.open('wb') as f:
                f.write(b'torn')
                f.flush()
                os.fsync(f.fileno())
        records = [None] * world
        dist.all_gather_object(records, {'file': file.name, 'sha256': digest})
        if fault == 'coordinator_before_manifest' and rank == 0:
            os._exit(17)
        committed = [False]
        if rank == 0:
            if all(sha(directory / rec['file']) == rec['sha256'] for rec in records):
                atomic_json(directory / 'manifest.json',
                            {'step': step, 'world_size': world, 'shards': records})
                committed[0] = True
                if fault == 'coordinator_after_manifest':
                    os._exit(18)
                atomic_json(ckpts / 'latest.json', {'directory': directory.name, 'step': step})
            atomic_json(phase / f'commit-{step}.json', {'committed': committed[0], 'fault': fault})
        dist.broadcast_object_list(committed, src=0)
        dist.barrier()
        return committed[0]

    for step in range(start + 1, a.steps + 1):
        if cursor + 4 > len(order):
            random.shuffle(order)
            cursor = 0
        ids = order[cursor:cursor + 4]
        cursor += 4
        opt.zero_grad()
        loss = torch.nn.functional.mse_loss(model(x[ids]), y[ids])
        loss.backward()
        for p in model.parameters():
            dist.all_reduce(p.grad)
            p.grad.div_(world)
        opt.step()
        trace.append({'step': step, 'sample_ids': ids, 'loss': float(loss.detach())})
        if step % 20 == 0:
            checkpoint(step)
        if a.interrupt != 'none' and step == 35:
            atomic_json(phase / f'paused-{rank}.json', {'step': step})
            if a.interrupt in ('sigterm', 'abrupt'):
                while not stop:
                    time.sleep(0.02)
                checkpoint(step)
            else:
                checkpoint(step, a.interrupt)
            atomic_json(phase / f'trace-{rank}.json', trace)
            dist.destroy_process_group()
            return
    final = dict(model=model.state_dict(), optimizer=opt.state_dict(),
                 torch_rng=torch.get_rng_state(), python_rng=random.getstate(),
                 order=order, cursor=cursor, step=a.steps)
    torch.save(final, phase / f'final-{rank}.pt')
    atomic_json(phase / f'trace-{rank}.json', trace)
    dist.destroy_process_group()


if __name__ == '__main__':
    main()
