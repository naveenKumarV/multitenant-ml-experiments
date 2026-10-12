"""Build a local evidence archive without runtime credentials or tool installs."""
from pathlib import Path
import hashlib
import json
import zipfile

root = Path(__file__).resolve().parents[1]
allowed_dirs = ('src', 'scripts', 'config', 'vendor', 'results')
files = [p for name in allowed_dirs for p in (root / name).rglob('*')
         if p.is_file() and '__pycache__' not in p.parts]
files += [root / name for name in ('README.md', 'REPORT.md', 'TEST_PLAN.md',
                                  'requirements.txt', 'requirements.lock', '.gitignore')]
files = sorted(files)
assert not any(p.is_symlink() for p in files)
checksums = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
manifest = root / 'SHA256SUMS.json'
manifest.write_text(json.dumps(checksums, indent=2) + '\n')
archive = root.parent / 'prototype-evidence.zip'
with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as z:
    for p in files + [manifest]:
        z.write(p, str(Path('prototype') / p.relative_to(root)))
with zipfile.ZipFile(archive) as z:
    assert z.testzip() is None
    assert not any(part in ('.runtime', '.tools', '.venv', 'kubeconfig')
                   for name in z.namelist() for part in Path(name).parts)
    for name, digest in checksums.items():
        assert hashlib.sha256(z.read('prototype/' + name)).hexdigest() == digest
print(f'{archive}: {len(files)} evidence/source files; {archive.stat().st_size:,} bytes; all archive checksums verified')
