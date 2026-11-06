"""The six S08 lifecycle cases written against a seam, with OS-level observation.

Every engine interaction is a public ctl command. Evidence about processes comes from the OS observer (via the
seam), never from engine state. A case that needs an agent on a seam without one raises `SeamRequired` and is
reported as blocked, not passed.
"""
import time

from ..oracles import ConformanceError, validate_event_page
from .seams import SeamRequired, require


class Lifecycle:
    def __init__(self, seam):
        self.seam = seam

    def status(self, task):
        return self.seam.call(['status', task['task_id']])['task']

    def poll(self, operation, predicate, timeout=12):
        deadline = time.monotonic() + timeout
        for _ in range(60):
            value = operation()
            if predicate(value):
                return value
            if time.monotonic() >= deadline:
                break
            time.sleep(.1)
        raise ConformanceError('bounded public observation predicate not reached')

    def terminal(self, task, expected='completed', exit_code=0):
        status = self.poll(lambda: self.status(task), lambda s: s.get('status') != 'running')
        require(status.get('status') == expected and status.get('exit_code') == exit_code,
                f'terminal status/exit contradict the scenario: {status.get("status")}/{status.get("exit_code")}')
        require(status.get('output_drained') is True, 'terminal output not publicly drained')
        require(self.seam.exited(task['fake'], 5), 'fake OS exit not observed')
        page = self.seam.call(['task-events', task['task_id'], '--from', 'oldest', '--all-kinds', '--limit', '200'])['result']
        require(page['log'].get('state') == 'final' and page['log'].get('output_drained') is True, 'public final event capture not drained')
        require(any(e.get('kind') == 'task_finished' and e.get('status') == expected and e.get('exit_code') == exit_code
                    for e in page['events']), 'missing matching public finish event')
        return status


def refusals(c):
    seam = c.seam
    before = seam.fakes_spawned()
    attempts = [(['run', '--backend', 'nosuch', '--repo', str(seam.workspace), '--prompt', 'must not spawn'], 'unknown backend'),
                (['resume', '99999999-9999-4999-8999-999999999999', 'must not spawn'], 'resume of an unknown task'),
                (['cancel', '99999999-9999-4999-8999-999999999999'], 'cancel of an unknown task')]
    attempts += [(argv, 'seam-specific launch refusal') for argv in seam.extra_refusals()]
    for argv, label in attempts:
        doc = seam.call(argv, refused=True)
        require(isinstance(doc['error'].get('code'), str) and doc['error']['code'], 'refusal lacks a stable code: ' + label)
    usage = seam.engine.command(['run', '--no-such-option', '--json']) if seam.engine else None
    if usage is not None:
        require(usage['exit_code'] == 2 and usage['document']['error']['code'] == 'usage', 'an unknown option is not a usage error')
    require(seam.task_ids() == [], 'a refused request left a task record')
    require(seam.fakes_spawned() == before == 0, 'prelaunch refusal spawned a fake')
    require(seam.ledgers_written() == 0, 'prelaunch refusal wrote fake evidence')
    return {'refusals': len(attempts) + (1 if usage is not None else 0)}


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
    require(c.status(task)['status'] == 'running', 'detached task prematurely terminal')
    initial = next(op for op in seam.operations if op['argv'][0] == 'run')
    require(initial['process']['returncode'] == 0, 'initial CLI did not exit')
    require(seam.alive(task['owner']), 'detached owner did not survive the initial CLI')
    count = seam.fakes_spawned()
    seam.call(['resume', task['task_id'], 'overlapping resume must refuse'], refused=True)
    require(seam.fakes_spawned() == count, 'busy resume spawned another fake')
    gate.write_bytes(b'caller-selected release\n')
    c.terminal(task)
    return seam.raw_evidence(task, completed=True)


def cancel(c):
    seam = c.seam
    task = seam.launch('hold', gate=str(seam.workspace / 'never-release'))
    seam.call(['cancel', task['task_id']])
    status = c.poll(lambda: c.status(task), lambda s: s.get('status') != 'running')
    require(status.get('status') == 'cancelled' and status.get('output_drained') is True, 'cancel did not settle drained and cancelled')
    require(type(status.get('exit_code')) is int and status['exit_code'] != 0, 'cancel exit unobserved or clean')
    c.terminal(task, 'cancelled', status['exit_code'])
    return seam.raw_evidence(task)


def owner_death(c):
    seam = c.seam
    task = seam.launch('hold', gate=str(seam.workspace / 'never-release'), survive_eof=True)
    require(seam.kill_owner(task), 'verified owner self-termination unavailable')
    require(seam.exited(task['owner'], 5), 'owner OS exit unobserved')
    for _ in range(3):
        status = c.status(task)
        require(status.get('status') == 'running' and status.get('exit_code') is None and status.get('output_drained') is False,
                'owner death fabricated terminal settlement')
        require(status.get('supervision', {}).get('capture_state') == 'lost_with_owner', 'owner loss not publicly reported')
        require(seam.alive(task['fake']), 'owner-death fixture no longer alive')
    require(seam.fakes_spawned() == 1, 'a read replayed a task')
    return seam.raw_evidence(task)


CASES = {'refusals': refusals, 'complete': complete, 'nonzero': nonzero, 'detached-resume': detached_resume,
         'cancel': cancel, 'owner-death': owner_death}
