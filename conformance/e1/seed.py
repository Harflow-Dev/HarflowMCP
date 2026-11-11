"""The Python 0.0.13 file layout, used only by the Python fixture adapter in `port.py`.

Cases never import this module. It writes the documented private formats (store marker, task
metadata, normalized event lines, prompt sidecars) as literal bytes with the standard library and
imports nothing from the engine.
"""
import json
import os
from pathlib import Path

PRODUCT_VERSION = '0.0.13'

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


# --- chain-stop markers and launch receipts (Python 0.0.13) ------------------------------------------
# Mirrors `task_continuation.py:103` (the marker writer: `{'cancelled_task_id': ...}`) and
# `task_continuation_stop.py:20-22` (its name), and `task_supervision.py:118-165` (the receipt qualification
# the pending-launch reads apply).

LAUNCH_FORMAT = 'harflow.task-launch'
STOP_SUFFIX = '.continuation-stopped.json'
LAUNCH_SUFFIX = '.launch.json'
OVERSIZE_MARKER_BYTES = 5000   # above the 4096-byte attribution read limit (`ATTRIBUTION_BYTES`)


def stop_marker_path(tasks, chain_root):
    return Path(tasks) / f'{chain_root}{STOP_SUFFIX}'


def write_stop(tasks, chain_root, mode, attribution=None):
    """Create, damage or remove the chain-stop marker for `chain_root`."""
    path = stop_marker_path(tasks, chain_root)
    if mode == 'absent':
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            path.rmdir()
        return
    if os.path.lexists(path):
        raise FileExistsError(str(path))
    if mode == 'fault':
        # A directory is not a regular file: the observer reports `unknown` (task_continuation_stop.py:90-91).
        path.mkdir(mode=0o700)
        return
    kind = attribution['kind']
    if kind in ('valid', 'invalid'):
        _write(path, json.dumps({'cancelled_task_id': attribution['cancelled_task_id']}))
    elif kind == 'malformed':
        _write(path, 'not json {')
    elif kind == 'oversize':
        document = {'cancelled_task_id': attribution['cancelled_task_id'], 'padding': ''}
        padding = OVERSIZE_MARKER_BYTES - len(json.dumps(document))
        document['padding'] = 'x' * padding
        _write(path, json.dumps(document))
    else:
        raise ValueError('unknown attribution kind ' + repr(kind))


def _replace(path, data, mode=0o600):
    """Write `data` over `path` atomically so the directory's name set never changes."""
    path = Path(path)
    temporary = path.with_name(path.name + '.replacing')
    with open(temporary, 'wb') as stream:
        stream.write(data if isinstance(data, bytes) else data.encode('utf-8'))
    os.chmod(temporary, mode)
    os.replace(temporary, path)


REFUSAL_CHECKS = ('format', 'version', 'root_mismatch', 'task_id_mismatch', 'generation', 'oversize', 'field_shape',
                  'resource_names')


def launch_document(root, tasks, spec):
    """A qualified launch receipt (`task_supervision._read_launch`) built only from explicit spec values."""
    tasks = Path(tasks)
    info = tasks.stat()
    document = {
        'format': LAUNCH_FORMAT, 'product': 'harflow', 'v': 1, 'product_version': PRODUCT_VERSION,
        'root': str(root), 'task_id': spec['name'], 'generation': spec['generation'],
        'owner': None, 'request_sha256': 'e' * 64, 'backend': spec['backend'], 'state': spec['state'],
        'title': spec['title'], 'repo_path': spec['repo_path'], 'created_at': spec['created_at'],
        'not_started_reason': spec['not_started_reason'],
        'directory_identity': [info.st_dev, info.st_ino],
        'directory_binding': {'path': os.path.realpath(tasks), 'inode': info.st_ino},
    }
    lineage = spec['lineage'] or {}
    for key in ('spawned_by', 'root_task_id', 'parent_task_id', 'chain_root_task_id'):
        if lineage.get(key) is not None:
            document[key] = lineage[key]
    if spec['resources'] is not None:
        document['resources'] = spec['resources']
    if spec['worktree'] is not None:
        document['worktree'] = spec['worktree']
    refusal = spec.get('refusal')
    if refusal:
        check = refusal['check']
        if check == 'format':
            document['format'] = 'harflow.other'
        elif check == 'version':
            document['v'] = 2
        elif check == 'root_mismatch':
            document['root'] = str(root) + '-elsewhere'
        elif check == 'task_id_mismatch':
            document['task_id'] = 'f' * 8 + '-' + spec['name']
        elif check == 'generation':
            document['generation'] = 'not-a-generation'
        elif check == 'field_shape':
            document['title'] = 7
        elif check == 'resource_names':
            document['resources'] = {'names': ['Not A Valid Name']}
        elif check != 'oversize':
            raise ValueError('unknown refusal check ' + repr(check))
    return document


def write_launch(root, tasks, spec):
    """Write or replace `<name>.launch.json`; replacing keeps the directory's name set."""
    document = launch_document(root, tasks, spec)
    data = json.dumps(document)
    if (spec.get('refusal') or {}).get('check') == 'oversize':
        data = json.dumps({**document, 'padding': 'x' * 66000})
    path = Path(tasks) / (spec['name'] + LAUNCH_SUFFIX)
    if path.exists():
        _replace(path, data)
    else:
        _write(path, data)


def write_invalid_launch_name(tasks, name):
    """A `*.launch.json` file whose stem is not a task identity (counted, never trusted)."""
    _write(Path(tasks) / (name + LAUNCH_SUFFIX), '{}')
