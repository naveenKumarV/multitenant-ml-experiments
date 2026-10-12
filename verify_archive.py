"""Verify the preserved archive and its readable checkout with the stdlib."""
from pathlib import Path
import hashlib
import json
import zipfile

ROOT = Path(__file__).resolve().parent
expected, filename = (ROOT / 'evidence/SHA256SUMS').read_text().strip().split()
assert Path(filename).name == filename
archive = ROOT / 'evidence' / filename
assert hashlib.sha256(archive.read_bytes()).hexdigest() == expected, 'Archive SHA-256 mismatch'
with zipfile.ZipFile(archive) as z:
    assert z.testzip() is None, 'Archive CRC mismatch'
    names = z.namelist()
    assert len(names) == len(set(names)), 'Duplicate archive members'
    for name in names:
        path = Path(name)
        assert not path.is_absolute() and '..' not in path.parts
        assert path.parts[0] == 'prototype'
        assert not {'.runtime', '.tools', '.venv', 'kubeconfig'}.intersection(path.parts)
    manifest_data = z.read('prototype/SHA256SUMS.json')
    manifest = json.loads(manifest_data)
    assert len(manifest) == 1207
    assert set(names) == {'prototype/' + name for name in manifest} | {'prototype/SHA256SUMS.json'}
    assert (ROOT / 'prototype/SHA256SUMS.json').read_bytes() == manifest_data
    for name, digest in manifest.items():
        assert hashlib.sha256(z.read('prototype/' + name)).hexdigest() == digest, name
        local = ROOT / 'prototype' / name
        optional_binary = local.suffix == '.pt' or 'checkpoints' in Path(name).parts
        if local.exists():
            assert hashlib.sha256(local.read_bytes()).hexdigest() == digest, name
        else:
            assert optional_binary, 'Missing readable evidence file: ' + name
print(f'Verified original archive SHA-256, all {len(manifest)} file hashes, and readable checkout.')
