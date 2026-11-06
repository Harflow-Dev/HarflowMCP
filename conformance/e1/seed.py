"""The Python 0.0.11 file layout, used only by the Python fixture adapter in `port.py`.

Cases never import this module. It writes the documented private formats (store marker, task
metadata, normalized event lines, prompt sidecars) as literal bytes with the standard library and
imports nothing from the engine.
"""
import hashlib
import json
import os
from pathlib import Path

PRODUCT_VERSION = '0.0.11'

def _write(path, data, mode=0o600):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'xb') as stream:
        stream.write(data if isinstance(data, bytes) else data.encode('utf-8'))
    os.chmod(path, mode)


def marker(version=PRODUCT_VERSION):
    return {'format': 'harflow.store', 'layout_version': 1, 'product': 'harflow',
            'product_version': version, 'v': 1}


def init_root(root, version=PRODUCT_VERSION):
    """A compatible, empty root exactly as the documented marker and reserved directories describe."""
    root = Path(root)
    root.mkdir(mode=0o700)
    _write(root / '.harflow-store.json', json.dumps(marker(version), sort_keys=True))
    _write(root / '.harflow-store.lock', b'')
    for name in ('workflows', 'workflow-runs', 'workflow-leases', 'workflow-owners'):
        (root / name).mkdir(mode=0o700)
    (root / 'tasks').mkdir(mode=0o700)
    return root / 'tasks'


def task_document(task_id, **fields):
    document = {
        'task_id': task_id, 'backend': 'claude', 'session_id': 'session-' + task_id[:8],
        'repo_path': '/workspace/repo', 'started_at': '2026-01-10T10:00:00+00:00',
        'freedom': 'write_in_repo', 'markers': [], 'prompt': 'Summarize the repo.',
        'status': 'completed', 'exit_code': 0, 'finished_at': '2026-01-10T10:05:00+00:00',
        'output_drained': True,
        'product': 'harflow', 'format': 'harflow.task', 'v': 1, 'product_version': PRODUCT_VERSION,
    }
    document.update(fields)
    return document


def write_task(tasks, task_id, **fields):
    _write(tasks / f'{task_id}.meta.json', json.dumps(task_document(task_id, **fields), indent=2))


def event(seq, kind, task_id, stamp, **fields):
    line = {'product': 'harflow', 'format': 'harflow.event', 'v': 1, 'product_version': PRODUCT_VERSION,
            'seq': seq, 'observed_at': stamp, 'source_ts': None, 'raw_offset': None,
            'task_id': task_id, 'kind': kind}
    line.update(fields)
    return line


def write_events(tasks, task_id, lines):
    body = ''.join(json.dumps(line, ensure_ascii=False) + '\n' for line in lines)
    _write(tasks / f'{task_id}.events.jsonl', body)
