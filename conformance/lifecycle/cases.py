"""The six S08 lifecycle cases written against a seam, with OS-level observation.

Every engine interaction is a public ctl command. Evidence about processes comes from the OS observer (via the
seam), never from engine state. A case that needs an agent on a seam without one raises `SeamRequired` and is
reported as blocked, not passed. Each requirement carries a tag: `[contract]` follows the published contract,
`[baseline]` pins Python 0.0.11 behaviour the contract does not spell out.
"""
import os
from pathlib import Path
import time

from ..oracles import ConformanceError
from .seams import SpawnEvidenceUnavailable, require

FINAL_BOUND_SECONDS = 20


class Lifecycle:
    def __init__(self, seam):
        self.seam = seam

    def status(self, task):
        return self.seam.call(['status', task['task_id']])['task']

    def poll(self, operation, predicate, timeout=12):
        deadline = time.monotonic() + timeout
        while True:
            value = operation()
            if predicate(value):
                return value
            if time.monotonic() >= deadline:
                raise ConformanceError('bounded public observation predicate not reached')
            time.sleep(.1)

    def terminal(self, task, expected='completed', exit_code=0):
        status = self.poll(lambda: self.status(task), lambda s: s.get('status') != 'running')
        require(status.get('status') == expected and status.get('exit_code') == exit_code,
                f'terminal status/exit contradict the scenario: {status.get("status")}/{status.get("exit_code")}', 'contract')
        require(status.get('output_drained') is True, 'terminal output not publicly drained', 'contract')
        require(self.seam.exited(task['fake'], 5), 'fake OS exit not observed', 'contract')
        # The log may be `settling` for a moment after the task finishes (contract); wait for `final` within a bound.
        page = self.poll(lambda: self.seam.call(['task-events', task['task_id'], '--from', 'oldest', '--all-kinds', '--limit', '200'])['result'],
                         lambda p: p['log'].get('state') == 'final', FINAL_BOUND_SECONDS)
        require(page['log'].get('output_drained') is True, 'public final event capture not drained', 'contract')
        require(any(e.get('kind') == 'task_finished' and e.get('status') == expected and e.get('exit_code') == exit_code
                    for e in page['events']), 'missing matching public finish event', 'contract')
        return status


def refusals(c):
    seam = c.seam
    verify = seam.watch_spawns()  # a seam without independent spawn evidence withholds the pass at the end
    attempts = [(['run', '--backend', 'nosuch', '--repo', str(seam.workspace), '--prompt', 'must not spawn'], 'unknown backend'),
                (['resume', '99999999-9999-4999-8999-999999999999', 'must not spawn'], 'resume of an unknown task'),
                (['cancel', '99999999-9999-4999-8999-999999999999'], 'cancel of an unknown task')]
    attempts += [(argv, 'seam-specific launch refusal') for argv in seam.extra_refusals()]
    for argv, label in attempts:
        doc = seam.call(argv, refused=True)
        require(isinstance(doc['error'].get('code'), str) and doc['error']['code'], 'refusal lacks a stable code: ' + label, 'contract')
    usage = seam.engine.command(['run', '--no-such-option', '--json']) if seam.engine else None
    if usage is not None:
        require(usage['exit_code'] == 2 and usage['document']['error']['code'] == 'usage', 'an unknown option is not a usage error', 'contract')
    require(seam.task_ids() == [], 'a refused request left a task record', 'contract')
    verified = {'refusals': len(attempts) + (1 if usage is not None else 0), 'codes_and_no_task_record': True}
    try:
        verify()
    except SpawnEvidenceUnavailable as exc:
        exc.partial = {**verified, 'spawn_check': seam.evidence.get('spawn_check'), 'no_spawn': 'unverified'}
        raise
    return {**verified, 'spawn_check': seam.evidence.get('spawn_check')}


def complete(c):
    task = c.seam.launch()
    c.terminal(task)
    return c.seam.raw_evidence(task, completed=True)


def nonzero(c):
    task = c.seam.launch('nonzero')
    c.terminal(task, 'failed', 7)
    return c.seam.raw_evidence(task, completed=True)


def detached_resume(c):
    seam = c.seam
    gate = seam.workspace / 'release'
    task = seam.launch('hold', gate=str(gate))
    require(c.status(task)['status'] == 'running', 'detached task prematurely terminal', 'contract')
    initial = next(op for op in seam.operations if op['argv'][0] == 'run')
    require(initial['process']['returncode'] == 0, 'initial CLI did not exit', 'baseline')
    require(seam.alive(task['owner']), 'detached owner did not survive the initial CLI', 'contract')
    count = seam.fakes_spawned()
    seam.call(['resume', task['task_id'], 'overlapping resume must refuse'], refused=True)
    require(seam.fakes_spawned() == count, 'busy resume spawned another fake', 'contract')
    gate.write_bytes(b'caller-selected release\n')
    c.terminal(task)
    return seam.raw_evidence(task, completed=True)


def cancel(c):
    seam = c.seam
    task = seam.launch('hold', gate=str(seam.workspace / 'never-release'))
    seam.call(['cancel', task['task_id']])
    status = c.poll(lambda: c.status(task), lambda s: s.get('status') != 'running')
    require(status.get('status') == 'cancelled' and status.get('output_drained') is True, 'cancel did not settle drained and cancelled', 'contract')
    require(type(status.get('exit_code')) is int and status['exit_code'] != 0, 'cancel exit unobserved or clean', 'baseline')
    c.terminal(task, 'cancelled', status['exit_code'])
    return seam.raw_evidence(task)


def owner_death(c):
    seam = c.seam
    task = seam.launch('hold', gate=str(seam.workspace / 'never-release'), survive_eof=True)
    require(seam.kill_owner(task), 'verified owner self-termination unavailable', 'baseline')
    require(seam.exited(task['owner'], 5), 'owner OS exit unobserved', 'baseline')
    for _ in range(3):
        status = c.status(task)
        require(status.get('status') == 'running' and status.get('exit_code') is None and status.get('output_drained') is False,
                'owner death fabricated terminal settlement', 'contract')
        require(status.get('supervision', {}).get('capture_state') == 'lost_with_owner', 'owner loss not publicly reported', 'contract')
        require(seam.alive(task['fake']), 'owner-death fixture no longer alive', 'baseline')
    require(seam.fakes_spawned() == 1, 'a read replayed a task', 'contract')
    return seam.raw_evidence(task)


def _root_entries(root):
    """Name, inode, mode and size of every entry directly under the root; a refused request must not change them."""
    return sorted((e.name, e.inode(), e.stat(follow_symlinks=False).st_mode, e.stat(follow_symlinks=False).st_size)
                  for e in os.scandir(root))


def pre_state_of(root):
    """`missing`, `empty`, `own-lock-only` or `stray`, read from the root the case was given (see `prestate.py`)."""
    if root is None or not os.path.lexists(root):
        return 'missing'
    names = {entry.name for entry in os.scandir(root)}
    if not names:
        return 'empty'
    return 'own-lock-only' if names <= {'.harflow-store.lock', 'store.lock', 'io.lock'} else 'stray'


def pre_state_run(c):
    """`run` on the root the case was given. A missing, empty or own-lock-only root is initialized and the task runs
    to completion; a root that holds anything else is refused as `state_incompatible` with reason `unmarked_nonempty`
    and is left exactly as found, with no agent started."""
    seam = c.seam
    root = Path(seam.engine.env['HARFLOW_STATE_ROOT']) if getattr(seam, 'engine', None) else None
    state = pre_state_of(root)
    if state != 'stray':
        task = seam.launch()
        c.terminal(task)
        return {'pre_state': state, **seam.raw_evidence(task, completed=True)}
    before = _root_entries(root)
    verify = seam.watch_spawns()
    doc = seam.call(['run', '--backend', 'claude', '--freedom', 'unrestricted', '--repo', str(seam.workspace),
                     '--prompt', 'must not spawn'], refused=True)
    error = doc['error']
    require(error.get('code') == 'state_incompatible', f'a run on a stray root gave {error.get("code")!r}, not state_incompatible', 'contract')
    require((error.get('data') or {}).get('reason') == 'unmarked_nonempty', 'a run on a stray root lacks reason unmarked_nonempty', 'contract')
    require(_root_entries(root) == before, 'a refused run changed the root it was given', 'contract')
    verified = {'pre_state': state, 'code': error['code'], 'reason': error['data']['reason'], 'root_unchanged': True}
    try:
        verify()
    except SpawnEvidenceUnavailable as exc:
        exc.partial = {**verified, 'spawn_check': seam.evidence.get('spawn_check'), 'no_spawn': 'unverified'}
        raise
    return {**verified, 'spawn_check': seam.evidence.get('spawn_check')}


CASES = {'refusals': refusals, 'complete': complete, 'nonzero': nonzero, 'detached-resume': detached_resume,
         'cancel': cancel, 'owner-death': owner_death, 'pre-state-run': pre_state_run}


def resume_probe(c):
    """Unscored: after a settled `complete` launch, one explicit resume starts a new agent in the same session.

    Not a member of `CASES` (the six names are the E2 exit's list); its result is reported beside, never inside, `complete`."""
    seam = c.seam
    first = seam.launch()
    c.terminal(first)
    before = seam.fakes_spawned()
    second = seam.resume(first)
    status = c.terminal(second)
    require(seam.fakes_spawned() == before + 1, 'resume did not start exactly one new agent', 'contract')
    require(second['fake']['token'] != first['fake']['token'] and second['session_id'] == first['session_id'], 'resume did not continue the same session', 'contract')
    return {'first': seam.raw_evidence(first, completed=True), 'resumed': seam.raw_evidence(second, completed=True),
            'same_session': True, 'resumed_status': {k: status.get(k) for k in ('status', 'exit_code', 'session_id')}}
