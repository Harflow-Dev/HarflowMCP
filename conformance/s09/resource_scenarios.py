"""Public resource/scratch observations; no engine persistence or outcome helpers.

These synthetic cases cover only known registered processes. Owner-loss evidence
retains uncertainty and never authorizes releasing a reservation or replaying work.
"""
import hashlib
from pathlib import Path

from conformance.oracles import read_regular, strict_json
from .oracles import (require, resource_pending, resource_inventory,
                      resource_task_order, resource_spawn_order)


def _fakes(c):
    return [row for row in c.p.observer.records if row['role'] == 'fake']


def _occupancy(document, name, holders, queue):
    rows = [row for row in resource_inventory(document) if row['name'] == name]
    return (len(rows) == 1 and
            [item['owner'].get('task_id') for item in rows[0]['holders']] == holders and
            [item['owner'].get('task_id') for item in rows[0]['queue']] == queue)


def _ordered(c, name, holders, queue):
    document = c.poll(lambda: c.call(['resources']),
                      lambda doc: _occupancy(doc, name, holders, queue))
    return resource_task_order(document, name, holders, queue)


def _pending(c, task):
    document = c.call(['status', task['task_id']], refused=True, code='task_pending_launch')
    return resource_pending(document, task['task_id'])


def _live(c, participant):
    rows = [row for row in c.p.observer.records if row['token'] == participant['token']]
    require(len(rows) == 1, 'registered participant identity missing')
    return not rows[0]['exited']


def resources_fifo(c):
    name = 's09.fifo'
    flags = ('--resource', name, '--resource-wait-seconds', '120')
    gate_a, gate_b = c.p.workspace / 'fifo-a', c.p.workspace / 'fifo-b'
    a = c.launch('hold', flags=flags, gate=str(gate_a), survive_eof=True)
    _ordered(c, name, [a['task_id']], [])
    b = c.accept('hold', flags=flags, gate=str(gate_b), survive_eof=True)
    first_queue = _ordered(c, name, [a['task_id']], [b['task_id']])
    pending_b = _pending(c, b)
    require(len(_fakes(c)) == 1, 'queued second task spawned a fake')
    third = c.accept(flags=flags)
    both_queued = _ordered(c, name, [a['task_id']], [b['task_id'], third['task_id']])
    pending_c = _pending(c, third)
    require(len(_fakes(c)) == 1, 'queued third task spawned a fake')
    gate_a.write_text('release A\n')
    c.terminal(a)
    raw_a = c.raw(a, completed=True)
    c.attach(b)
    second_holder = _ordered(c, name, [b['task_id']], [third['task_id']])
    _pending(c, third)
    require(len(_fakes(c)) == 2, 'third task spawned before second holder released')
    gate_b.write_text('release B\n')
    c.terminal(b)
    raw_b = c.raw(b, completed=True)
    c.attach(third)
    c.terminal(third)
    raw_c = c.raw(third, completed=True)
    final = c.poll(lambda: c.call(['resources']),
                   lambda doc: not any(row['name'] == name for row in resource_inventory(doc)))
    require(c.p.observer.wait_for(lambda rows: all(row['exited'] for row in rows), 5),
            'FIFO participant exits not all observed before ledger capture')
    data = read_regular(c.p.output / 'observer' / 'observer.jsonl')
    events = [strict_json(line) for line in data.splitlines()]
    pids = [task['fake']['pid'] for task in (a, b, third)]
    resource_spawn_order(events, pids)
    return {'task_ids': [a['task_id'], b['task_id'], third['task_id']],
            'pending': [pending_b, pending_c],
            'occupancy': [first_queue, both_queued, second_holder],
            'final_resources': final['resources'], 'fake_spawn_order': pids,
            'observer_sha256': hashlib.sha256(data).hexdigest(),
            'raw': [raw_a, raw_b, raw_c]}


def resource_owner_loss(c):
    name = 's09.owner-loss'
    flags = ('--resource', name, '--resource-wait-seconds', '120')
    gate = c.p.workspace / 'owner-loss-never-release'
    a = c.launch('hold', flags=flags, gate=str(gate), survive_eof=True)
    _ordered(c, name, [a['task_id']], [])
    b = c.accept(flags=flags)
    _ordered(c, name, [a['task_id']], [b['task_id']])
    _pending(c, b)
    require(len(_fakes(c)) == 1 and _live(c, a['fake']), 'owner-loss setup has unexpected fake state')
    require(c.p.observer.kill_participant(a['supervisor']['token']),
            'exact registered supervisor self-termination request failed')
    require(c.p.observer.wait_exited(a['supervisor']['token'], 5),
            'supervisor kernel exit not observed')
    observations = []
    for _ in range(3):
        status = c.status(a)
        require(status.get('status') == 'running' and status.get('exit_code') is None and status.get('output_drained') is False,
                'owner loss fabricated observed agent exit or drain')
        require(_live(c, a['fake']) and len(_fakes(c)) == 1,
                'owner loss freed capacity or lost surviving registered fake')
        resources = c.call(['resources'])
        row = resource_task_order(resources, name, [a['task_id']], [b['task_id']])
        pending = _pending(c, b)
        observations.append({'status': status, 'resource': row, 'pending': pending})
    raw = c.raw(a, completed=False)
    require(not gate.exists(), 'owner-loss gate unexpectedly released')
    return {'holder_task_id': a['task_id'], 'queued_task_id': b['task_id'],
            'supervisor_token': a['supervisor']['token'], 'fake_pid': a['fake']['pid'],
            'observations': observations, 'raw': raw,
            'reservation_released': False, 'replacement_started': False,
            'cleanup_scope': 'Known registered participants only; reservation uncertainty retained'}


def scratch_phase_refusals(c):
    gate = c.p.workspace / 'scratch-hold'
    sentinel = c.p.workspace / 'scratch-unrelated-sentinel'
    sentinel_bytes = 'Fixed unrelated workspace sentinel Ω\n'.encode()
    sentinel.write_bytes(sentinel_bytes)
    task = c.launch('hold', gate=str(gate), survive_eof=True)
    status = c.status(task)
    scratch = status.get('scratch', {})
    chain_id, chain_value = scratch.get('chain_id'), scratch.get('chain')
    require(isinstance(chain_id, str) and bool(chain_id) and isinstance(chain_value, str),
            'public scratch chain identity/path missing')
    chain = Path(chain_value)
    require(chain.is_absolute() and chain.resolve().is_relative_to(c.p.state_root.resolve())
            and chain.is_dir(), 'public scratch path not an existing owned-state directory')
    listing = c.call(['scratch', 'list'])
    rows = [row for row in listing.get('shared_scratch', [])
            if row.get('kind') == 'chain' and row.get('identity') == chain_id]
    require(len(rows) == 1 and rows[0].get('path') == str(chain)
            and type(rows[0].get('active_tasks')) is int and rows[0]['active_tasks'] >= 1
            and rows[0].get('blockers', {}).get('active', 0) >= 1,
            'active public scratch blocker missing')
    refusal = c.call(['scratch', 'clean', '--chain', chain_id],
                     refused=True, code='shared_scratch_in_use')
    require(chain.is_dir(), 'refused scratch cleanup removed active directory')
    require(c.call(['worktree-list']).get('worktrees') == [], 'fresh worktree inventory not empty')
    before = len(_fakes(c))
    worktree_refusal = c.call(['run', '--backend', 'conformance_fake', '--repo', str(c.p.workspace),
        '--freedom', 'read_only', '--prompt', 'S09 unsupported read-only worktree request', '--worktree'],
        refused=True)
    require(len(_fakes(c)) == before and c.call(['worktree-list']).get('worktrees') == [],
            'unsupported read-only worktree request spawned an agent or created inventory')
    phase_refusal = c.call(['slot', 'status'], refused=True, code='phase_slots_not_enabled')
    gate.write_text('release scratch holder\n')
    c.terminal(task)
    require(c.p.observer.wait_exited(task['supervisor']['token'], 5),
            'scratch holder supervisor exit not observed')
    raw = c.raw(task, completed=True)
    cleaned = c.call(['scratch', 'clean', '--chain', chain_id])['result']
    require(cleaned.get('deleted') is True and cleaned.get('kind') == 'chain'
            and cleaned.get('identity') == chain_id and cleaned.get('path') == str(chain)
            and not chain.exists(), 'explicit terminal scratch cleanup not independently observed')
    final = c.call(['scratch', 'list'])
    require(not any(row.get('kind') == 'chain' and row.get('identity') == chain_id
                    for row in final.get('shared_scratch', [])), 'cleaned chain remains publicly listed')
    require(read_regular(sentinel) == sentinel_bytes, 'scratch cleanup changed unrelated sentinel')
    return {'task_id': task['task_id'], 'chain_id': chain_id,
            'active_scratch': rows[0], 'active_refusal': refusal['error']['code'],
            'worktree_refusal': worktree_refusal['error']['code'],
            'phase_refusal': phase_refusal['error']['code'], 'cleaned': cleaned,
            'sentinel_sha256': hashlib.sha256(sentinel_bytes).hexdigest(), 'raw': raw}
