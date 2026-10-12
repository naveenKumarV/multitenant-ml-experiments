from pathlib import Path
import hashlib
import json
root=Path(__file__).resolve().parents[1]/'vendor'
for name, record in json.loads((root/'manifest.json').read_text()).items():
    assert hashlib.sha256((root/name).read_bytes()).hexdigest()==record['sha256'], name
print('Pinned upstream manifest checksums verified.')
