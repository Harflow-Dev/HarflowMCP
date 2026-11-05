"""Root-serialized steering compaction and durable retired-task ownership.

Ownership receipts survive inline history retirement. A missing or contradictory
receipt never makes an old managed provider task independently resumable.
"""
from __future__ import annotations

import copy
import json

from .state_access import storage_operation


def _tree(storage, root):
    if root.get('kind') == 'builder':
        return {root['workflow_run_id']: (root, [root['workflow_run_id']])}
    from .orchestrator_steering import records
    return records(storage, root)


def resource_task_pins(storage, snapshot, tree=None):
    """Resolve actual workflow_attempt owners through resource_attempt.id.

    Return uncertainty separately; an unresolvable reservation never frees old
    managed evidence merely because its owner omits task_id.
    """
    pins, unresolved = set(), False
    for resource in snapshot:
        for row in resource['holders'] + resource['queue']:
            owner = row['owner']
            if owner.get('task_id'):
                pins.add(owner['task_id'])
            if owner.get('kind') != 'workflow_attempt':
                continue
            try:
                rid, resource_id = owner['workflow_run_id'], owner['activation_id']
                if tree is not None and rid not in tree:
                    # Qualified exact owner scope is unrelated to this tree.
                    continue
                run = tree[rid][0] if tree is not None else storage.get_run(rid, refresh_atomic=True)
                found = False
                for activation in run['activations']:
                    attempts = [activation.get('resource_attempt')] + [task.get('resource_attempt') for task in activation['tasks']]
                    matched = [ra for ra in attempts if ra and ra['id'] == resource_id]
                    if not matched:
                        continue
                    found = True
                    pins.update(t['task_id'] for t in activation['tasks'])
                    pins.update(ra['task_id'] for ra in matched if ra.get('task_id'))
                    if tree is not None:
                        root = next(value for value, path in tree.values() if len(path) == 1)
                        pins.update(t['task_id'] for t in root['steering']['turns'] if
                                    t['target']['workflow_run_id'] == rid and t['target']['activation_id'] == activation['id']
                                    or t['turn_id'] == activation.get('steering_turn_id'))
                unresolved |= not found
            except (KeyError, TypeError, ValueError, OSError):
                unresolved = True
    return pins, unresolved


def _effect_proofs(storage, root, tree):
    from .workflow_steering import _digest
    result = set(root["steering"].get("compaction_pending", {}).get("effect_turn_ids", []))
    for turn in root['steering']['turns']:
        if turn['state'] != 'settled' or turn.get('outcome') != 'committed':
            continue
        tid = turn['turn_id']
        target = tree.get(turn['target']['workflow_run_id'], (None,))[0]
        if target is None:
            continue
        if turn['target']['role'] == 'worker':
            activation = next((a for a in target['activations'] if a['id'] == turn['target']['activation_id']), None)
            from .worker_steering import worker_effect_proven
            if activation and worker_effect_proven(target, activation, turn, require_delivery=True):
                result.add(tid)
        elif turn['target']['role'] == 'orchestrator':
            intent = turn.get('action_commit') or {}
            receipt = target.get('steering_action_receipts', {}).get(tid)
            if intent.get('state') != 'acknowledged' or receipt != {'root_workflow_run_id': root['workflow_run_id'], 'turn_id': tid, 'command_digest': _digest(intent['command']), 'state': 'applied'}:
                continue
            action = intent['command']['action']
            if action == 'inspect':
                proven = any(a['id'] == tid and a.get('decision_disposition') == 'inspect' for a in target['activations'])
            elif action == 'call_on_demand':
                owner = tree.get(turn['session_owner_run_id'], (None,))[0]
                proven = owner and any(c['id'] == intent['call_id'] and c['orchestration_activation_id'] == tid and c['status'] not in {'reserved', 'running', 'uncertain'} and c.get('result_delivered') for c in owner.get('on_demand_calls', []))
            else:
                proven = any(d.get('activation_id') == tid and d.get('decision_id') == intent['command'].get('decision_id') for d in target.get('decisions', []))
            if proven:
                result.add(tid)
    return result


def _write_binding(storage, root, turn, tree):
    from .bounded_io import read_receipt
    from .workflow_receipt_documents import encode
    from .workflows import _write
    task_id = turn['task_id']
    owner = None
    for run, _ in tree.values():
        for activation in run['activations']:
            if any(t['task_id'] == task_id for t in activation['tasks']):
                owner = (run, activation)
                break
        if owner:
            break
    path = storage.owners / (task_id + '.json')
    try:
        receipt = read_receipt(path)
    except FileNotFoundError:
        receipt = None
    if owner is None:
        if receipt and receipt.get('steering_binding'):
            binding = receipt['steering_binding']
            return (binding['root_workflow_run_id'] == root['workflow_run_id'] and binding['turn_id'] == turn['turn_id']
                    and binding['ledger_epoch'] == root['steering']['requests'][turn['request_id']]['ledger_epoch']
                    and binding['role'] == turn['target']['role'] and receipt['workflow_run_id'] in tree)
        # A proven never-started issuance has no provider task to detach.
        from . import store
        if store.read(storage.root / 'tasks', task_id) is not None:
            return False
        return turn.get('not_started') is True or turn.get('outcome') in {'steering_target_stale', 'request_expired', 'dismissed', 'workflow_terminal'}
    run, activation = owner
    if receipt is not None and (receipt.get('workflow_run_id') != run['workflow_run_id'] or receipt.get('workflow_execution_id') != activation['id']):
        from .workflow_steering import SteeringError
        raise SteeringError('steering_source_unavailable', 'Task ownership changed before steering retirement')
    receipt = receipt or {'workflow_run_id': run['workflow_run_id'], 'workflow_execution_id': activation['id'], 'workflow_node_id': activation['node_id'], 'workflow_role': activation['role'], 'root_workflow_run_id': root['workflow_run_id']}
    receipt['steering_binding'] = {'v': 1, 'root_workflow_run_id': root['workflow_run_id'], 'task_id': task_id,
        'turn_id': turn['turn_id'], 'ledger_epoch': root['steering']['requests'][turn['request_id']]['ledger_epoch'],
        'role': turn['target']['role']}
    _write(path, encode(receipt, task_id), layout=storage.state_layout)
    return True


def retired_association(storage, receipt, task_id):
    """Return a qualified compacted association, never standalone authority."""
    binding = receipt.get('steering_binding')
    if binding is None:
        return None
    from .workflows import WorkflowError
    from .workflow_receipt_documents import validate_steering_binding
    try:
        validate_steering_binding(binding, task_id)
        if receipt.get('root_workflow_run_id', binding['root_workflow_run_id']) != binding['root_workflow_run_id']:
            raise ValueError('Compacted ownership root contradicts receipt')
    except ValueError as exc:
        raise WorkflowError('Compacted steering ownership binding is malformed') from exc
    root = storage.get_run(binding['root_workflow_run_id'], refresh_atomic=True)
    ledger = root.get('steering') or {}
    if ledger.get('schema_version') != 2 or binding['ledger_epoch'] > ledger['closed_epoch']:
        raise WorkflowError('Compacted steering ownership is not covered by a closed namespace')
    if any(v.get('task_id') == task_id for owner, _ in _tree(storage, root).values() for v in owner.get('sessions', {}).values() if type(v) is dict):
        raise WorkflowError('Current managed session head cannot be retired')
    if any(t['task_id'] == task_id for t in ledger['turns']):
        raise WorkflowError('Retained steering task has lost its live activation association')
    return {'workflow_run_id': receipt['workflow_run_id'], 'workflow_node_id': receipt.get('workflow_node_id'),
        'workflow_role': receipt.get('workflow_role'), 'node_id': receipt.get('workflow_node_id'),
        'role': receipt.get('workflow_role'), 'activation_id': receipt.get('workflow_execution_id'),
        'status': root['status'], 'workflow_status': root['status'], 'workflow_settling': root.get('settling', False),
        'steering_history_expired': True, 'root_workflow_run_id': root['workflow_run_id']}


@storage_operation
def maintain(storage, run_id):
    """Close a namespace before reclaiming positively settled inline evidence."""
    from .steering_window import close_namespace, compact, retirement_bindings
    with storage.tree_mutation(run_id):
        root = storage.get_run(run_id, refresh_atomic=True)
        if root.get('parent_link') or root.get('steering', {}).get('schema_version') != 2:
            return
        tree = _tree(storage, root)
        effects = _effect_proofs(storage, root, tree)
        heads = {v['task_id'] for run, _ in tree.values() for v in run.get('sessions', {}).values() if isinstance(v, dict) and v.get('task_id')}
        # Any unresolved reservation remains authority even after output capture.
        # The ledger is read-only and its leaf lock is not retained across writes.
        from .resources import ResourceLedger
        resource_tasks, resource_unknown = resource_task_pins(storage, ResourceLedger(storage.root).snapshot(), tree)
        heads |= resource_tasks
        if resource_unknown:
            heads.update(t['task_id'] for t in root['steering']['turns'])
        candidates = retirement_bindings(root, settled_effect_turn_ids=effects)
        pending = root['steering'].get('compaction_pending')
        if pending is not None:
            # Resume the exact durable plan; NEVER close another namespace on recovery.
            removed = set(pending['turn_ids'])
            if any(t['turn_id'] in removed and t['task_id'] in heads for t in root['steering']['turns']):
                return
        else:
            trial = copy.deepcopy(root)
            close_namespace(trial)
            removed = compact(trial, managed_task_ids=set(candidates), settled_effect_turn_ids=effects, protected_task_ids=heads) or set()
            before = root['steering']
            after = trial['steering']
            changed = bool(removed or set(before['requests']) != set(after['requests']) or before['history'] != after['history'])
            if not changed:
                return
        # Bind before detaching. Crash at any later point is still managed/fail-closed.
        managed = set()
        for turn in root['steering']['turns']:
            if turn['turn_id'] in removed:
                if not _write_binding(storage, root, turn, tree):
                    from .workflow_steering import SteeringError
                    raise SteeringError('steering_source_unavailable', 'Managed task retirement lacks durable matching ownership proof')
                managed.add(turn['task_id'])
        if pending is None:
            def close(run):
                close_namespace(run)
                run['steering']['compaction_pending'] = {'turn_ids': sorted(removed), 'effect_turn_ids': sorted(removed & effects)}
            storage.update_run(run_id, close, 'steering_namespace_closed')
        # Child receipt retirement is an idempotent part of this durable intent.
        for child_id, (child, _) in tree.items():
            def detach(value):
                for key in ('steering_hold_receipts', 'steering_action_receipts'):
                    receipts = value.get(key, {})
                    for tid in removed:
                        receipt = receipts.get(tid)
                        if receipt and receipt.get('root_workflow_run_id') == run_id:
                            del receipts[tid]
                value['activations'][:] = [a for a in value['activations'] if a.get('steering_turn_id') not in removed]
                for activation in value['activations']:
                    for tid in removed:
                        activation.get('worker_steering_receipts', {}).pop(tid, None)
                    activation['tasks'][:] = [t for t in activation['tasks'] if t.get('steering_turn_id') not in removed or t['task_id'] not in managed]
            if child_id != run_id:
                storage.update_run(child_id, detach, 'steering_child_history_compacted')
        def reclaim(run):
            for key in ('steering_hold_receipts', 'steering_action_receipts'):
                for tid in removed:
                    receipt = run.get(key, {}).get(tid)
                    if receipt and receipt.get('root_workflow_run_id') == run_id:
                        del run[key][tid]
            for activation in run['activations']:
                for tid in removed:
                    activation.get('worker_steering_receipts', {}).pop(tid, None)
            compact(run, managed_task_ids=managed, settled_effect_turn_ids=effects, protected_task_ids=heads, retirement_turn_ids=removed)
            run['steering'].pop('compaction_pending', None)
        storage.update_run(run_id, reclaim, 'steering_history_compacted')
    current = storage.get_run(run_id, refresh_atomic=True)
    ledger = current.get('steering', {})
    if ledger.get('replans') or ledger.get('replan_compaction_pending') or any(a.get('withdrawing_replan_id') for a in current.get('activations', [])):
        from .replan_retention import maintain as maintain_replans
        maintain_replans(storage, run_id)


def worker_sources(storage, run):
    """Inspect pending worker dispositions without inventing a new execution grant."""
    root_id = (run.get('parent_link') or {}).get('root_workflow_run_id') or run['workflow_run_id']
    root = run if root_id == run['workflow_run_id'] else storage.get_run(root_id, refresh_atomic=True)
    if root.get('steering', {}).get('schema_version') != 2:
        return []
    from .workflow_replan import superseded_execution
    return [(value, a) for value, _ in _tree(storage, root).values() for a in value['activations'] if a.get('worker_result_required') and not superseded_execution(root, a, storage)]


def scheduling_blocked(storage, run, token):
    activation = next((a for a in run['activations'] if a['id'] == token.get('execution_activation_id')), None)
    if not activation or not activation.get('worker_result_required'):
        return False
    question_id = activation.get('worker_steering_question_id')
    question = next((q for q in activation.get('questions', []) if q.get('question_id') == question_id), None)
    # Only the ordinary orchestrator answer is runnable; no worker continuation.
    return not bool(question and question.get('status') == 'waiting')


def control_guard(storage, run):
    from .workflow_steering import SteeringError
    for owner, activation in worker_sources(storage, run):
        if not activation.get('worker_steering_question_id'):
            raise SteeringError('worker_result_required', 'Worker steering requires an explicit result response; ordinary recovery cannot replenish execution allowance')


def fence(storage, run, event, detail=None):
    from .workflow_steering import held
    from .workflows import DispatchNotStarted
    rid = (run.get('parent_link') or {}).get('root_workflow_run_id') or run['workflow_run_id']
    root = run if rid == run['workflow_run_id'] else storage.get_run(rid, refresh_atomic=True)
    worker_holds = [ep for ep in root.get('steering', {}).get('episodes', []) if ep['hold'] and ep['target']['role'] == 'worker']
    if worker_holds and event in {'decision_attempt_reserved', 'decision_checkpoint', 'decision_accepted', 'orchestrator_inspection', 'on_demand_reserved'}:
        raise DispatchNotStarted('Worker steering holds new orchestration until its exact response settles')
    if event in {'control:resume', 'control:recover', 'scheduler_recovery_requested'}:
        control_guard(storage, run)
