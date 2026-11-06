"""Stdlib-only state-root seeding for the E1 read scenarios.

The seed writes the documented private formats (store marker, task metadata, normalized event
lines, prompt sidecars) as literal bytes. It imports nothing from the engine, so an engine
under test reads documents it did not write, and expected values in the cases are derived from
this module's constants, never from engine output.
"""
import hashlib
import json
import os
from pathlib import Path

PRODUCT_VERSION = '0.0.11'

A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'   # completed, final event log, continued by C
B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'   # failed, event log without task_finished
C = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'   # completed continuation of A, no event log
D = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd'   # completed, long prompt in a verified sidecar
E = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'   # completed, long prompt whose sidecar is missing
MISSING = '99999999-9999-4999-8999-999999999999'
BROKEN = 'ffffffff-ffff-4fff-8fff-ffffffffffff'  # metadata with a foreign format

LONG_PROMPT = ''.join(f'line {n:04d} é spaced   text\n' for n in range(120))  # > 2000 characters
PREVIEW_CHARS = 2000


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


def a_events():
    t = '2026-01-10T10:0%d:00+00:00'
    return [
        event(0, 'task_started', A, t % 0, backend='claude'),
        event(1, 'assistant_delta', A, t % 1, text='Sum'),
        event(2, 'assistant_delta', A, t % 1, text='marizing'),
        event(3, 'assistant_text', A, t % 2, text='Summarizing the repository.'),
        event(4, 'tool_call', A, t % 3, call_id='call-1', name='Read', input={'path': 'README.md'}),
        event(5, 'tool_result', A, t % 3, call_id='call-1', is_error=False, output='Readme body'),
        event(6, 'usage', A, t % 4, input_tokens=120, output_tokens=30),
        event(7, 'task_finished', A, t % 5, status='completed', exit_code=0),
    ]


def b_events():
    t = '2026-01-11T10:0%d:00+00:00'
    return [
        event(0, 'task_started', B, t % 0, backend='claude'),
        event(1, 'assistant_text', B, t % 1, text='Starting.'),
        event(2, 'notice', B, t % 2, text='stream ended'),
    ]


def seed_reads(root):
    """The shared fixture world for the read scenarios. Returns the tasks directory."""
    tasks = init_root(root)
    write_task(tasks, A, title='First task', group='g1', model='model-a')
    write_events(tasks, A, a_events())
    write_task(tasks, B, started_at='2026-01-11T10:00:00+00:00', finished_at='2026-01-11T10:03:00+00:00',
               status='failed', exit_code=1, prompt='Fix the bug.', title='Second task')
    write_events(tasks, B, b_events())
    write_task(tasks, C, started_at='2026-01-12T10:00:00+00:00', finished_at='2026-01-12T10:02:00+00:00',
               continuation_of_task_id=A, chain_root_task_id=A, prompt='Continue.', title='Third task')
    digest = hashlib.sha256(LONG_PROMPT.encode('utf-8')).hexdigest()
    sidecar = f'{D}.prompt.{digest}.txt'
    write_task(tasks, D, started_at='2026-01-13T10:00:00+00:00', finished_at='2026-01-13T10:02:00+00:00',
               prompt=LONG_PROMPT[:PREVIEW_CHARS], prompt_source=sidecar, prompt_truncated=True)
    _write(tasks / sidecar, LONG_PROMPT)
    missing_digest = hashlib.sha256(b'absent').hexdigest()
    write_task(tasks, E, started_at='2026-01-14T10:00:00+00:00', finished_at='2026-01-14T10:02:00+00:00',
               prompt=LONG_PROMPT[:PREVIEW_CHARS], prompt_source=f'{E}.prompt.{missing_digest}.txt',
               prompt_truncated=True)
    return tasks
