"""Layout-neutral description of the fixture world and the constants the cases expect.

Everything here is data. A fixture adapter (`port.py`) turns it into whatever store layout the
engine under test uses. Expected values in `cases.py` derive from these constants and the contract.
"""
A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'   # completed, final event log, continued by C
B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'   # failed, event log without task_finished
C = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'   # completed continuation of A, no event log
D = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd'   # completed, long prompt in a verified sidecar
E = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'   # completed, long prompt whose sidecar is missing
MISSING = '99999999-9999-4999-8999-999999999999'
BROKEN = 'ffffffff-ffff-4fff-8fff-ffffffffffff'  # metadata that is not a Harflow task
PRODUCT_VERSION = '0.0.11'

LONG_PROMPT = ''.join(f'line {n:04d} é spaced   text\n' for n in range(120))  # > 2000 characters
PREVIEW_CHARS = 2000


def event(seq, kind, observed_at, **fields):
    return {'seq': seq, 'kind': kind, 'observed_at': observed_at, 'fields': fields}


def _a_events():
    t = '2026-01-10T10:0%d:00+00:00'
    return [
        event(0, 'task_started', t % 0, backend='claude'),
        event(1, 'assistant_delta', t % 1, text='Sum'),
        event(2, 'assistant_delta', t % 1, text='marizing'),
        event(3, 'assistant_text', t % 2, text='Summarizing the repository.'),
        event(4, 'tool_call', t % 3, call_id='call-1', name='Read', input={'path': 'README.md'}),
        event(5, 'tool_result', t % 3, call_id='call-1', is_error=False, output='Readme body'),
        event(6, 'usage', t % 4, input_tokens=120, output_tokens=30),
        event(7, 'task_finished', t % 5, status='completed', exit_code=0),
    ]


def _b_events():
    t = '2026-01-11T10:0%d:00+00:00'
    return [
        event(0, 'task_started', t % 0, backend='claude'),
        event(1, 'assistant_text', t % 1, text='Starting.'),
        event(2, 'notice', t % 2, text='stream ended'),
    ]


BACKEND = 'claude'
REPO_PATH = '/workspace/repo'
FREEDOM = 'write_in_repo'


def session_id(task_id):
    return 'session-' + task_id[:8]


def artifact_path(repo_path, name):
    """Where an artifact target lives: the task's repository path, an `out` directory, then the name."""
    return f'{repo_path}/out/{name}'


def task(task_id, **fields):
    """A complete task spec. Every value a case asserts is an explicit field here, never an adapter default."""
    spec = {'task_id': task_id, 'backend': BACKEND, 'repo_path': REPO_PATH, 'freedom': FREEDOM,
            'session_id': session_id(task_id), 'status': 'completed', 'exit_code': 0,
            'started_at': '2026-01-10T10:00:00+00:00', 'finished_at': '2026-01-10T10:05:00+00:00',
            'prompt': 'Summarize the repo.', 'output_drained': True}
    spec.update(fields)
    return spec


def settled(task_id, started_at='2026-01-10T10:00:00+00:00', finished_at='2026-01-10T10:01:00+00:00', **fields):
    return task(task_id, started_at=started_at, finished_at=finished_at, **fields)


WORLD_READS = {
    'tasks': [
        task(A, title='First task', group='g1', model='model-a'),
        task(B, started_at='2026-01-11T10:00:00+00:00', finished_at='2026-01-11T10:03:00+00:00', status='failed',
             exit_code=1, prompt='Fix the bug.', title='Second task'),
        task(C, started_at='2026-01-12T10:00:00+00:00', finished_at='2026-01-12T10:02:00+00:00',
             continuation_of=A, prompt='Continue.', title='Third task'),
        task(D, started_at='2026-01-13T10:00:00+00:00', finished_at='2026-01-13T10:02:00+00:00',
             prompt=LONG_PROMPT, prompt_store='sidecar_verified'),
        task(E, started_at='2026-01-14T10:00:00+00:00', finished_at='2026-01-14T10:02:00+00:00',
             prompt=LONG_PROMPT, prompt_store='sidecar_missing'),
    ],
    'events': {A: _a_events(), B: _b_events()},
}
