"""Closed restricted prestart replan protocol; no graph editing authority."""
from __future__ import annotations

import json
import math
import re
import uuid
from pathlib import Path

from .workflow_run_documents import identifier

PHASES = {'cancel_requested', 'settling', 'settlement_unknown', 'ready_to_replan', 'replan_committed'}
WITNESS_KEYS = {'activation_id', 'task_id', 'token_id', 'visit_id', 'assignment_digest', 'launch_generation', 'request_sha256', 'configuration_sha256', 'resolved_configuration_digest', 'queued_turn_id', 'queued_task_id'}


def witness(value):
    if type(value) is not dict or set(value) != WITNESS_KEYS:
        raise ValueError('Malformed exact replan withdrawal witness')
    for key in ('activation_id', 'task_id', 'token_id', 'visit_id', 'queued_turn_id', 'queued_task_id'):
        identifier(value[key])
    for key in ('assignment_digest', 'resolved_configuration_digest'):
        if type(value[key]) is not str or re.fullmatch('sha256:[a-f0-9]{64}', value[key]) is None:
            raise ValueError('Malformed replan assignment/configuration digest')
    for key in ('request_sha256', 'configuration_sha256'):
        if type(value[key]) is not str or re.fullmatch('[a-f0-9]{64}', value[key]) is None:
            raise ValueError('Malformed replan launch fingerprint')
    if type(value['launch_generation']) is not str or re.fullmatch('[a-f0-9]{32}', value['launch_generation']) is None:
        raise ValueError('Malformed replan launch generation')


def request(value):
    from .workflow_steering import SteeringError
    from .steering_output import strict_loads
    required = {'schema_version', 'action', 'ledger_epoch', 'request_id', 'episode_id', 'expected_steering_revision', 'expected_control_revision', 'expected_interaction_epoch', 'reason', 'target', 'withdrawal_witness'}
    try:
        if type(value) is not dict or set(value) != required or type(value['schema_version']) is not int or value['schema_version'] != 2 or value['action'] != 'replan':
            raise ValueError('Replan requires the closed schema2 action=replan object')
        raw = json.dumps(value, allow_nan=False, ensure_ascii=True)
        if len(raw.encode()) > 128 * 1024:
            raise ValueError('Replan request exceeds128KiB')
        result = strict_loads(raw)
        if str(uuid.UUID(result['request_id'])) != result['request_id']:
            raise ValueError('Replan request_id must be a canonical UUID')
        identifier(result['episode_id'])
        for key in ('ledger_epoch', 'expected_control_revision', 'expected_interaction_epoch', 'expected_steering_revision'):
            if type(result[key]) is not int or result[key] < (1 if key == 'expected_steering_revision' else 0):
                raise ValueError('Replan revisions must be nonnegative integers')
        if type(result['reason']) is not str or not result['reason'].strip() or len(result['reason'].encode()) > 32768:
            raise ValueError('Replan requires a bounded nonblank reason')
        witness(result['withdrawal_witness'])
        from .worker_steering import validate_target
        validate_target(result['target'], result['target']['root_workflow_run_id'])
        return result
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError) as exc:
        raise SteeringError('malformed_command', str(exc)) from None


def validate_quiescence(value, replan_id, activation_id, task_id, dispatcher_identity, job_id):
    from .workflow_run_documents import _identity
    keys = {'v','origin','replan_id','activation_id','task_id','dispatcher_identity','job_id'}
    if (type(value) is not dict or set(value) != keys or type(value['v']) is not int or value['v'] != 1 or value['origin'] not in {'outer_node_job_done','dispatcher_generation_dead'}
            or value['replan_id'] != replan_id or value['activation_id'] != activation_id or value['task_id'] != task_id or value['dispatcher_identity'] != dispatcher_identity or value['job_id'] != job_id):
        raise ValueError('Quiescence lacks exact outer-job execution/task/replan/generation binding')
    _identity(dispatcher_identity, 'quiescence.dispatcher')
    identifier(job_id)


def accepted_normal_result(run, activation_id):
    a = next((a for a in run['activations'] if a['id'] == activation_id), None)
    if not a or a.get('status') not in {'completed','failed'} or not a.get('node_result') or a['node_result'].get('status') == 'asking':
        return False
    token_id = (a.get('token') or {}).get('id')
    token = next((t for t in run['pending'] if t['id'] == token_id), None)
    if token and token.get('execution_activation_id') == activation_id and token.get('execution_complete') is True:
        return True
    return normal_progress_valid(run, a)


def normal_progress_valid(run, a):
    token_id = (a.get('token') or {}).get('id')
    receipt = a.get('normal_progress_receipt')
    if not receipt or not a.get('node_result'):
        return False
    from .workflow_steering import _digest
    if receipt['execution_id'] != a['id'] or receipt['origin_token_id'] != token_id or receipt['visit_id'] != (a.get('visit_id') or token_id) or receipt['result_digest'] != _digest(a['node_result']):
        return False
    decision = next((d for d in run.get('decisions', []) if d.get('decision_id') == receipt['decision_id'] and _digest(d) == receipt['decision_digest']), None)
    if decision is None:
        return False
    if receipt['kind'] == 'normal_transition':
        return decision.get('actor') == 'harflow' and decision.get('token_id') == receipt['routed_token_id'] and decision.get('transition_sequence') == receipt['transition_sequence'] and decision.get('action') == 'continue' and decision.get('node_id') == a['node_id']
    control = next((c for c in run['activations'] if c['id'] == receipt['decision_activation_id'] and c.get('role') == 'orchestrator'), None)
    issued = (control or {}).get('token') or {}
    exact_source = issued.get('execution_activation_id') == a['id'] and (issued.get('execution_complete') is True or issued.get('completion_source_node_id') == a['node_id'] and a['id'] in issued.get('input_result_refs', []))
    return bool(control and exact_source and issued.get('id') == receipt['routed_token_id'] and control.get('decision_disposition') == 'accepted' and not control.get('decision_ignored') and control.get('decision_id') == receipt['decision_id'] and decision.get('activation_id') == control['id'])


def record_normal_progress(run, token, decision):
    source = next((a for a in run['activations'] if a['id'] == token.get('execution_activation_id')), None)
    if not source or source.get('status') not in {'completed','failed'} or not source.get('node_result') or source['node_result'].get('status') == 'asking':
        return
    relevant = source.get('replan_id') or any(p['withdrawal_witness']['activation_id'] == source['id'] or p['planned_activation_id'] == source['id'] for p in run.get('steering', {}).get('replans', []))
    if not relevant:
        return
    if not token.get('execution_complete') and not (token.get('completion_source_node_id') == source['node_id'] and source['id'] in token.get('input_result_refs', [])):
        return
    from .workflow_steering import _digest
    origin_token = (source.get('token') or {}).get('id')
    if not origin_token:
        return
    kind = 'normal_transition' if decision.get('actor') == 'harflow' else 'normal_decision'
    source['normal_progress_receipt'] = {'v': 1, 'kind': kind, 'execution_id': source['id'], 'origin_token_id': origin_token, 'routed_token_id': token['id'], 'visit_id': source.get('visit_id') or origin_token,
        'result_digest': _digest(source['node_result']), 'decision_id': decision['decision_id'], 'decision_activation_id': decision.get('activation_id'), 'decision_digest': _digest(decision), 'transition_sequence': decision.get('transition_sequence')}


def token_obligation_discharged(run, record):
    replacement = record['planned_activation_id']
    if accepted_normal_result(run, replacement):
        return True
    reference = record.get('successor_certificate')
    if reference:
        old = next((a for a in run['activations'] if a['id'] == replacement), None)
        if old and reference['predecessor_execution_id'] == replacement and old.get('status') == 'not_started' and old.get('superseded_by_execution_id') == reference['replacement_execution_id'] and any(t['task_id'] == reference['predecessor_task_id'] and t.get('replan_withdrawn') is True and t['status'] == 'not_started' for t in old['tasks']):
            return True
    # One direct certified next edge discharges this edge forever; no recursive
    # traversal or arbitrary current-token pointer can provide that proof.
    for later in run.get('steering', {}).get('replans', []):
        if later['withdrawal_witness']['activation_id'] != replacement or later['phase'] != 'replan_committed' or not all(later['journal'].values()):
            continue
        old = next((a for a in run['activations'] if a['id'] == replacement), None)
        new = next((a for a in run['activations'] if a['id'] == later['planned_activation_id']), None)
        if old and new and old.get('status') == 'not_started' and old.get('worker_result_required') is True and any(t.get('replan_withdrawn') is True and t['status'] == 'not_started' and t['task_id'] == later['withdrawal_witness']['task_id'] for t in old['tasks']) and old.get('superseded_by_execution_id') == new['id'] and new.get('supersedes_execution_id') == old['id'] and new.get('replan_id') == later['replan_id'] and later.get('withdrawal_ack', {}).get('outcome') == 'withdrawn':
            return True
    return False


def records(value, run):
    required = {'replan_id', 'request_key', 'phase', 'target', 'withdrawal_witness', 'reason', 'created_at', 'candidate', 'planned_activation_id', 'planned_task_id', 'attempt_in_visit', 'journal'}
    if type(value) is not list:
        raise ValueError('Malformed replan records')
    seen = set()
    for record in value:
        if type(record) is not dict or not required <= set(record) or set(record) - required - {'withdrawal_ack', 'diagnostic', 'sequence', 'reason_ref', 'source_configuration', 'source_dispatcher_identity', 'source_job_id', 'quiescence', 'successor_certificate'}:
            raise ValueError('Malformed replan record fields')
        for key in ('replan_id', 'planned_activation_id', 'planned_task_id'):
            identifier(record[key])
        if record['replan_id'] in seen or record['phase'] not in PHASES:
            raise ValueError('Duplicate or malformed replan identity/state')
        seen.add(record['replan_id'])
        witness(record['withdrawal_witness'])
        if 'sequence' in record and (type(record['sequence']) is not int or record['sequence'] < 1):
            raise ValueError('Malformed absolute replan sequence')
        source_identity = record.get('source_dispatcher_identity')
        if source_identity is not None:
            from .workflow_run_documents import _identity
            _identity(source_identity, 'replan.source_dispatcher_identity')
            identifier(record.get('source_job_id'))
        if 'successor_certificate' in record:
            ref = record['successor_certificate']
            if type(ref) is not dict or set(ref) != {'replan_id','predecessor_execution_id','predecessor_task_id','replacement_execution_id','replacement_task_id','certificate_digest'}:
                raise ValueError('Malformed bounded direct successor certificate reference')
            for key in set(ref) - {'certificate_digest'}:
                identifier(ref[key])
            if re.fullmatch('sha256:[a-f0-9]{64}', str(ref['certificate_digest'])) is None or ref['predecessor_execution_id'] != record['planned_activation_id'] or ref['predecessor_task_id'] != record['planned_task_id']:
                raise ValueError('Direct successor proof contradicts its predecessor planned identity')
        if 'quiescence' in record:
            validate_quiescence(record['quiescence'], record['replan_id'], record['withdrawal_witness']['activation_id'], record['withdrawal_witness']['task_id'], source_identity, record.get('source_job_id'))
        from .worker_steering import validate_target
        validate_target(record['target'], run['workflow_run_id'])
        w = record['withdrawal_witness']
        if any(record['target'][key] != w[key] for key in ('activation_id','task_id','token_id','assignment_digest')):
            raise ValueError('Replan target and exact withdrawal witness contradict each other')
        queued = next((t for t in run['steering']['turns'] if t['turn_id'] == w['queued_turn_id']), None)
        if not queued or queued['task_id'] != w['queued_task_id'] or queued['target'] != record['target'] or queued['commit_target'] != record['target'] or queued['candidate'] != record['candidate'] or queued['boundary_task_id'] != w['task_id'] or queued['state'] != 'settled' or queued.get('not_started') is not True or queued.get('outcome') != 'request_expired':
            raise ValueError('Replan configuration lacks its exact immutable queued-response provenance')
        if 'source_configuration' in record:
            from .workflow_steering import _digest
            cfg = record['source_configuration']
            if type(cfg) is not dict or _digest(cfg) != w['resolved_configuration_digest'] or cfg.get('candidate') != {k: v for k, v in record['candidate'].items() if k != 'fallbacks'} or cfg.get('definition_digest') != record['target']['definition_digest'] or cfg.get('goal_revision') != record['target']['goal_revision'] or cfg.get('node_id') != record['target']['node_id']:
                raise ValueError('Replan immutable source configuration digest contradicts its candidate/target')
        if record['request_key'] not in run['steering']['requests'] or run['steering']['requests'][record['request_key']].get('replan_id') != record['replan_id']:
            raise ValueError('Replan lacks its exact shared request receipt')
        from .workflow_steering import _digest
        receipt = run['steering']['requests'][record['request_key']]
        original_payload = {'schema_version': 2, 'action': 'replan', 'ledger_epoch': receipt['ledger_epoch'], 'episode_id': queued['episode_id'], 'expected_steering_revision': receipt['steering_revision'] - 1, 'expected_control_revision': record['target']['control_revision'], 'expected_interaction_epoch': record['target']['interaction_epoch'], 'reason': record['reason'], 'target': record['target'], 'withdrawal_witness': record['withdrawal_witness']}
        if _digest(original_payload) != receipt['payload_digest']:
            raise ValueError('Replan immutable witness/configuration contradicts its accepted request payload')
        if type(record['attempt_in_visit']) is not int or record['attempt_in_visit'] < 1:
            raise ValueError('Replan attempt number must remain positive')
        if type(record['created_at']) not in (int, float) or not math.isfinite(record['created_at']) or record['created_at'] <= 0:
            raise ValueError('Malformed replan timestamp')
        if 'reason_ref' in record:
            if type(record['reason_ref']) is not str or re.fullmatch('history:[1-9][0-9]*', record['reason_ref']) is None:
                raise ValueError('Malformed bounded replan reason reference')
            entry = next((h for h in run['steering']['history'] if h['sequence'] == int(record['reason_ref'].split(':')[1])), None)
            if not entry or entry.get('replan_id') != record['replan_id'] or entry.get('reason') != record['reason']:
                raise ValueError('Replan reason reference lacks its exact verbatim history entry')
        if type(record['reason']) is not str or not record['reason'].strip() or len(record['reason'].encode()) > 32768:
            raise ValueError('Malformed replan reason')
        if type(record['candidate']) is not dict or type(record['candidate'].get('max_turns')) is not int or record['candidate']['max_turns'] < 1:
            raise ValueError('Replan candidate lacks finite positive cap')
        if type(record['journal']) is not dict or set(record['journal']) != {'original_withdrawn', 'response_revoked', 'resources_released', 'messages_released'} or any(type(v) is not bool for v in record['journal'].values()):
            raise ValueError('Malformed replan settlement journal')

        if record['phase'] in {'ready_to_replan', 'replan_committed'} and not all(record['journal'].values()):
            raise ValueError('Replan readiness lacks complete positive settlement journal')
        if 'withdrawal_ack' in record:
            ack = record['withdrawal_ack']
            from .wire_contracts import PRODUCT, PRODUCT_VERSION, compatible_line
            from .task_withdrawal import BINDING, PROOF_KEYS, ACK_FORMAT
            if type(ack) is not dict or set(ack) != BINDING | {'format', 'outcome', 'proof'} or ack['format'] != ACK_FORMAT or ack['outcome'] != 'withdrawn' or ack.get('product') != PRODUCT or not compatible_line(ack.get('product_version'), PRODUCT_VERSION):
                raise ValueError('Malformed persisted positive withdrawal acknowledgement')
            expected = {'task_id': record['withdrawal_witness']['task_id'], 'generation': record['withdrawal_witness']['launch_generation'], 'request_sha256': record['withdrawal_witness']['request_sha256'], 'configuration_sha256': record['withdrawal_witness']['configuration_sha256'], 'replan_id': record['replan_id'], 'root_workflow_run_id': run['workflow_run_id'], 'source_execution_id': record['withdrawal_witness']['activation_id']}
            if any(type(ack.get(k)) is not type(v) or ack[k] != v for k, v in expected.items()) or type(ack.get('v')) is not int or ack['v'] != 1:
                raise ValueError('Withdrawal acknowledgement binding contradicts replan intent')
            proof = ack['proof']
            if (type(proof) is not dict or set(proof) != PROOF_KEYS or type(proof.get('v')) is not int or proof['v'] != 1
                    or proof.get('origin') not in {'owner_prestart', 'dead_waiting_owner'} or proof.get('resources_released') is not True or proof.get('followups_released') is not True
                    or type(proof.get('intent_sha256')) is not str or re.fullmatch('[a-f0-9]{64}', proof['intent_sha256']) is None
                    or any(proof.get(k) != v for k, v in expected.items() if k != 'task_id')):
                raise ValueError('Withdrawal acknowledgement lacks exact positive settlement proof')
            directory = ack['directory_identity']
            binding = ack['directory_binding']
            if (type(ack['root']) is not str or not Path(ack['root']).is_absolute() or type(directory) is not list or len(directory) != 2 or any(type(n) is not int or n < 0 for n in directory)
                    or type(binding) is not dict or set(binding) != {'path', 'inode'} or type(binding['path']) is not str or not Path(binding['path']).is_absolute() or type(binding['inode']) is not int or binding['inode'] < 0):
                raise ValueError('Malformed private withdrawal directory binding')
        elif record['phase'] in {'ready_to_replan', 'replan_committed'}:
            raise ValueError('Replan readiness lacks the positive owner acknowledgement')
        source = next((a for a in run['activations'] if a['id'] == record['withdrawal_witness']['activation_id']), None)
        source_attempt = next((t for t in (source or {}).get('tasks', []) if t['task_id'] == record['withdrawal_witness']['task_id']), None)
        if not source_attempt or source_attempt.get('candidate') != record['candidate'] or source_attempt.get('harness_metadata', {}).get('requested') != record['candidate']:
            raise ValueError('Replan selected candidate contradicts original configured launch provenance')
        if record['journal']['original_withdrawn']:
            old = next((a for a in run['activations'] if a['id'] == record['withdrawal_witness']['activation_id']), None)
            attempt = next((t for t in (old or {}).get('tasks', []) if t['task_id'] == record['withdrawal_witness']['task_id']), None)
            if ('withdrawal_ack' not in record or not old or old.get('worker_result_required') is not True or old.get('status') != 'not_started'
                    or not attempt or attempt.get('status') != 'not_started' or attempt.get('replan_withdrawn') is not True):
                raise ValueError('Positive replan journal lacks certified original settlement and permanent worker blocker')
        elif record['phase'] in {'ready_to_replan', 'replan_committed'}:
            raise ValueError('Replan readiness cannot infer original settlement')
        if record['phase'] == 'replan_committed':
            old = next((a for a in run['activations'] if a['id'] == record['withdrawal_witness']['activation_id']), None)
            new = next((a for a in run['activations'] if a['id'] == record['planned_activation_id']), None)
            if not old or not new or old.get('superseded_by_execution_id') != new['id'] or new.get('supersedes_execution_id') != old['id'] or new.get('planned_task_id') != record['planned_task_id'] or new.get('replan_id') != record['replan_id'] or new.get('attempt_in_visit') != record['attempt_in_visit'] or new.get('execution_candidate') != record['candidate'] or new.get('role') != 'node' or old.get('role') != 'node' or new.get('node_id') != old.get('node_id') or old.get('node_id') != record['target']['node_id'] or new.get('assignment_prompt') != old.get('assignment_prompt') or new.get('assigned_task_ids', []) != old.get('assigned_task_ids', []):
                raise ValueError('Committed replan lacks exact atomic supersedes linkage')

            pending = next((t for t in run['pending'] if t['id'] == record['withdrawal_witness']['token_id']), None)
            if pending and pending.get('node_id') == new['node_id'] and not pending.get('execution_complete') and pending.get('execution_activation_id') != new['id'] and not token_obligation_discharged(run, record):
                raise ValueError('Committed replan contradicts the current uncompleted token linkage')
