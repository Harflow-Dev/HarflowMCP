"""Malformed envelopes and optional planning contracts remain explicit."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from polybridge import workflow_delegation as d
from polybridge import workflows as w


@pytest.mark.parametrize('wrapper', ['{}', 'Finished review.\n{}\nReview complete.', 'Here is the result:\n```json\n{}\n```\nDone.'])
def test_contract_accepts_unambiguous_surrounding_prose(wrapper):
    value = {'status': 'succeeded', 'result': {'verdict': 'changes_needed'}, 'evidence': []}
    assert d.parse_contract(wrapper.format(json.dumps(value)), guided=True) == value


@pytest.mark.parametrize('tail', ['\n{"status":"failed"}'])
def test_contract_rejects_broken_or_conflicting_trailing_json(tail):
    with pytest.raises(w.WorkflowError):
        d.parse_contract('{"status":"succeeded","result":{},"evidence":[]}' + tail, guided=True)


def test_protocol_failure_only_explicitly_retryable_after_settlement():
    activation = {'status': 'failed', 'tasks': [], 'node_result': {'status': 'failed', 'result': {'failure_kind': 'protocol'}}}
    assert d.retry_eligible(activation, guided=True)
    activation['tasks'] = [{'status': 'uncertain'}]
    assert not d.retry_eligible(activation, guided=True)
    activation['tasks'] = [{'status': 'completed', 'result': {'permission_denials': ['denied']}}]
    assert not d.retry_eligible(activation, guided=True)


def test_optional_plan_accepts_brief_and_preserves_existing_technical_plan():
    node = {'id': 'brief', 'role': 'planning', 'require_technical_plan': False}
    raw = {'status': 'succeeded', 'result': {'tasks': [{'id': 'review', 'title': 'Review pinned patch'}], 'brief': 'Review scope'}, 'evidence': []}
    value = d.normalize_result(node, {'summary': json.dumps(raw)}, [])
    run = {'activations': [{'id': 'execution', 'tasks': []}], 'pending': [{'id': 'token'}], 'technical_plan': 'Existing implementation plan', 'tasks': [], 'joins': {}}
    d.finish_result(run, node, 'token', 'execution', value, {'summary': json.dumps(raw)})
    assert run['technical_plan'] == 'Existing implementation plan'
    assert run['tasks'][0]['id'] == 'review'
    with pytest.raises(w.WorkflowError, match='technical_plan'):
        d.normalize_result({**node, 'require_technical_plan': True}, {'summary': json.dumps(raw)}, [])


def test_missing_retained_session_is_not_advertised(monkeypatch):
    node = {'id': 'worker', 'agent': {'backend': 'codex'}, 'freedom': 'read_only'}
    run = {'repo_path': '/repo', 'activations': [{'id': 'execution', 'role': 'node', 'node_id': 'worker', 'status': 'completed', 'tasks': [{'task_id': 'old', 'status': 'completed', 'result': {'session_id': 'session'}, 'repo_path': '/repo', 'freedom': 'read_only', 'network': None, 'candidate': {'backend': 'codex'}}]}]}
    from polybridge import store
    monkeypatch.setattr(store, 'read', lambda *args: None)
    assert d.available_sessions(run, node, root=Path('/tmp')) == []
    monkeypatch.setattr(store, 'read', lambda *args: SimpleNamespace(session_id='session', status='completed', repo_path='/repo', freedom='read_only', network=None, backend='codex'))
    assert d.available_sessions(run, node, root=Path('/tmp'))[0]['task_id'] == 'old'


def test_action_examples_do_not_leak_fields():
    prompt = d.decision_prompt({'decision_id': 'decision'})
    examples = json.loads(prompt.split('Examples:\n', 1)[1].split('\nContext:', 1)[0])
    assert set(examples['inspect']) == {'decision_id', 'reason', 'action', 'requests'}
    assert set(examples['structural_continue']['next'][0]) == {'continuation_id'}
    assert 'question' not in examples['agent_continue']


def test_fixed_resume_can_explicitly_bootstrap_after_session_retention(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, 'is_installed', lambda backend: True)
    definition = w.validate_definition({'name': 'resume', 'orchestrator': {'backend': 'codex'}, 'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'work', 'type': 'agent', 'agent': {'backend': 'codex'}, 'session_mode': 'resume'}, {'id': 'end', 'type': 'end'}], 'connections': [{'id': 'begin', 'source': 'start', 'target': 'work'}, {'id': 'finish', 'source': 'work', 'target': 'end'}]})
    run = w.WorkflowStore(tmp_path).create_run(definition, 'request', tmp_path)
    run['activations'] = [{'id': 'previous', 'node_id': 'work', 'role': 'node', 'status': 'completed', 'tasks': [], 'node_result': {'status': 'succeeded', 'result': {}, 'evidence': []}}]
    token = {'id': 'token', 'decision_id': 'decision', 'node_id': 'start'}
    decision = {'decision_id': 'decision', 'action': 'continue', 'reason': 'Previous session was retained away', 'next': [{'continuation_id': 'begin', 'prompt': 'Continue with a new session', 'session_mode': 'fresh'}]}
    _, assignments, _ = d.validate_decision(run, definition['nodes'][0], token, decision, False, root=tmp_path)
    assert assignments['begin']['execution_session_mode'] == 'fresh'


@pytest.mark.parametrize('tail', [',"evidence":[]}', '\n{"broken":', '\nExtra commentary.'])
def test_guided_result_keeps_first_complete_envelope_despite_surplus(tail):
    value = {'status': 'succeeded', 'result': {'verdict': 'changes_needed'}, 'evidence': []}
    text = json.dumps(value) + tail
    assert d.parse_contract(text, guided=True) == value
    with pytest.raises(ValueError):
        d.parse_contract(text)


def test_guided_decision_ignores_presentation_fields_but_never_permission_claims():
    run = {'runner_policy': 'guided'}
    decision = {'decision_id': 'decision', 'action': 'inspect', 'reason': 'Check evidence', 'question': '', 'requests': [{'execution_id': 'settled', 'view': 'result', 'display_label': 'Review'}], 'format': 'json'}
    cleaned, warnings = d.normalize_decision(run, {}, {}, decision)
    assert 'question' not in cleaned and 'format' not in cleaned
    assert 'display_label' not in cleaned['requests'][0]
    assert len(warnings) == 3
    for claim in ('access', 'permissions', 'freedom', 'network', 'backend'):
        with pytest.raises(w.WorkflowError, match='cannot override'):
            d.normalize_decision(run, {}, {}, {**decision, claim: 'unrestricted'})


def test_guided_exhaustion_preserves_correction_as_resumable_attention():
    run = {'runner_policy': 'guided', 'status': 'running'}
    checkpoint = {'decision_id': 'decision', 'decision_error': 'Unknown continuation supplied'}
    d.exhaust_decision(run, checkpoint)
    assert run['status'] == 'needs_attention'
    assert 'Unknown continuation supplied' in run['attention_reason']
    old = {'status': 'running'}
    d.exhaust_decision(old, checkpoint)
    assert old['status'] == 'failed'


@pytest.mark.parametrize("fenced", [True, False])
def test_guided_conflicting_fenced_envelope_after_broken_tail_is_rejected(fenced):
    success = {'status': 'succeeded', 'result': {'verdict': 'approved'}, 'evidence': []}
    failed = {'status': 'failed', 'result': {'reason': 'Unable to review'}, 'evidence': []}
    final = '```json\n' + json.dumps(failed) + '\n```' if fenced else json.dumps(failed)
    text = json.dumps(success) + '\n{unfinished\n' + final
    with pytest.raises(w.WorkflowError, match='conflicting'):
        d.parse_contract(text, guided=True)


def test_guided_malformed_first_object_is_not_salvaged_by_later_valid_fence():
    success = {'status': 'succeeded', 'result': {}, 'evidence': []}
    text = '{unfinished\n```json\n' + json.dumps(success) + '\n```'
    with pytest.raises(w.WorkflowError, match='Malformed'):
        d.parse_contract(text, guided=True)


def test_planning_brief_can_omit_checklist_without_resetting_prior_tasks():
    node = {'id': 'brief', 'role': 'planning', 'require_technical_plan': False, 'require_tasks': False}
    value = {'status': 'succeeded', 'result': {'brief': 'Pinned patch scope and review boundaries'}, 'evidence': []}
    assert d.normalize_result(node, {'summary': json.dumps(value)}, []) == value
    existing = [{'id': 'known', 'title': 'Existing task', 'status': 'completed'}]
    run = {'activations': [{'id': 'execution', 'tasks': []}], 'pending': [{'id': 'token'}], 'tasks': list(existing), 'technical_plan': 'Existing technical plan', 'joins': {}}
    d.finish_result(run, node, 'token', 'execution', value, {'summary': json.dumps(value)})
    assert run['tasks'] == existing
    assert run['technical_plan'] == 'Existing technical plan'
    with pytest.raises(w.WorkflowError, match='tasks array'):
        d.normalize_result({**node, 'require_tasks': True}, {'summary': json.dumps(value)}, [])
    with pytest.raises(w.WorkflowError, match='must be an array'):
        d.normalize_result(node, {'summary': json.dumps({**value, 'result': {'tasks': 'invalid'}})}, [])


@pytest.mark.parametrize('current_id', [None, 'retry'])
def test_reconciliation_cannot_overwrite_accepted_retry_with_superseded_failure(current_id):
    old_result = {'status': 'failed', 'result': {'reason': 'Old failed output'}, 'evidence': []}
    old = {'id': 'old', 'role': 'node', 'node_id': 'work', 'status': 'failed', 'tasks': [], 'node_result': old_result, 'token': {'id': 'token'}}
    token = {'id': 'token', 'node_id': 'work', 'retry_of_execution_id': 'old', 'assignment_prompt': 'Correct the output'}
    if current_id:
        token['execution_activation_id'] = current_id
    run = {'definition': {'nodes': [{'id': 'work', 'role': 'task'}]}, 'pending': [token], 'activations': [old], 'tasks': [], 'joins': {}}
    d.reconcile_delegation(run)
    assert token.get('execution_activation_id') == current_id
    assert not token.get('execution_complete')
    assert token['assignment_prompt'] == 'Correct the output'


def test_reconciliation_restores_settled_retry_success_instead_of_old_failure():
    node = {'id': 'work', 'role': 'task'}
    old = {'id': 'old', 'role': 'node', 'node_id': 'work', 'status': 'failed', 'tasks': [], 'node_result': {'status': 'failed', 'result': {}, 'evidence': []}, 'token': {'id': 'token'}}
    result = {'status': 'succeeded', 'result': {'summary': 'Corrected'}, 'evidence': []}
    retry = {'id': 'retry', 'role': 'node', 'node_id': 'work', 'status': 'completed', 'tasks': [], 'node_result': result, 'token': {'id': 'token'}}
    token = {'id': 'token', 'node_id': 'work', 'execution_activation_id': 'retry', 'retry_of_execution_id': 'old', 'failed_execution_refs': ['old']}
    run = {'definition': {'nodes': [node]}, 'pending': [token], 'activations': [old, retry], 'tasks': [], 'joins': {}}
    d.reconcile_delegation(run)
    assert token['result']['status'] == 'succeeded'
    assert old['resolved_by_execution_id'] == 'retry'
    assert token['execution_activation_id'] == 'retry'


@pytest.mark.parametrize('require_tasks', [True, False])
@pytest.mark.parametrize('require_technical_plan', [True, False])
def test_no_checklist_needed_preserves_state_and_independent_technical_plan_contract(require_tasks, require_technical_plan):
    node = {'id': 'plan', 'role': 'planning', 'require_tasks': require_tasks, 'require_technical_plan': require_technical_plan}
    result = {'no_checklist_needed': True, 'checklist_reason': 'Single bounded review needs only a reviewer brief'}
    if require_technical_plan:
        result['technical_plan'] = 'Review the pinned patch and validate findings.'
    value = {'status': 'succeeded', 'result': result, 'evidence': []}
    assert d.normalize_result(node, {'summary': json.dumps(value)}, []) == value
    if require_technical_plan:
        with pytest.raises(w.WorkflowError, match='technical_plan'):
            d.normalize_result(node, {'summary': json.dumps({**value, 'result': {k: v for k, v in result.items() if k != 'technical_plan'}})}, [])


@pytest.mark.parametrize('result', [
    {'no_checklist_needed': True},
    {'no_checklist_needed': True, 'checklist_reason': ''},
    {'no_checklist_needed': 'true', 'checklist_reason': 'Simple'},
    {'no_checklist_needed': True, 'checklist_reason': 'Simple', 'tasks': [{'id': 'task', 'title': 'Contradictory'}]},
])
def test_no_checklist_needed_rejects_missing_reason_nonboolean_and_contradictions(result):
    with pytest.raises(w.WorkflowError):
        d.normalize_result({'role': 'planning', 'require_technical_plan': False}, {'summary': json.dumps({'status': 'succeeded', 'result': result, 'evidence': []})}, [])


def test_no_checklist_result_remains_authoritative_and_preserves_existing_checklist():
    node = {'id': 'plan', 'role': 'planning', 'require_technical_plan': False}
    value = {'status': 'succeeded', 'result': {'no_checklist_needed': True, 'checklist_reason': 'Review only; no implementation tasks'}, 'evidence': []}
    existing = [{'id': 'earlier', 'title': 'Prior task', 'status': 'pending'}]
    run = {'definition': {'nodes': [node]}, 'activations': [{'id': 'execution', 'role': 'node', 'node_id': 'plan', 'tasks': []}], 'pending': [{'id': 'token'}], 'tasks': existing.copy(), 'joins': {}}
    d.finish_result(run, node, 'token', 'execution', value, {'summary': json.dumps(value)})
    assert run['tasks'] == existing
    assert d.result_inputs(run, ['execution'])[0]['node_result']['result'] == value['result']


@pytest.mark.parametrize('settled_retry', [False, True])
@pytest.mark.parametrize('old_status', ['completed', 'failed'])
def test_store_reconcile_preserves_retry_owner_before_delegation_pass(tmp_path, monkeypatch, settled_retry, old_status):
    monkeypatch.setattr(w.backends, 'is_installed', lambda backend: True)
    monkeypatch.setattr(w, 'optional_failure_join', lambda *args: 'end')
    definition = w.validate_definition({'name': 'ownership', 'orchestrator': {'backend': 'codex'}, 'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'work', 'type': 'agent', 'agent': {'backend': 'codex'}}, {'id': 'end', 'type': 'end'}], 'connections': [{'id': 'begin', 'source': 'start', 'target': 'work'}, {'id': 'finish', 'source': 'work', 'target': 'end'}]})
    definition['nodes'][1]['optional'] = True
    storage = w.WorkflowStore(tmp_path)
    run = storage.create_run(definition, 'request', tmp_path)
    old_result = {'status': 'failed', 'result': {'reason': 'superseded'}, 'evidence': []}
    old = {'id': 'old', 'role': 'node', 'node_id': 'work', 'status': old_status, 'tasks': [{'task_id': 'old-task', 'status': old_status, 'result': {'status': old_status, 'summary': 'old output'}}], 'node_result': old_result, 'token': {'id': 'token'}}
    token = {'id': 'token', 'node_id': 'work', 'retry_of_execution_id': 'old', 'assignment_prompt': 'Accepted retry', 'failed_execution_refs': ['old']}
    activations = [old]
    if settled_retry:
        value = {'status': 'succeeded', 'result': {'summary': 'New result'}, 'evidence': []}
        activations.append({'id': 'retry', 'role': 'node', 'node_id': 'work', 'status': 'completed', 'tasks': [{'task_id': 'retry-task', 'status': 'completed', 'result': {'status': 'completed', 'summary': json.dumps(value)}}], 'node_result': value, 'token': {'id': 'token'}})
        token['execution_activation_id'] = 'retry'
    storage.update_run(run['workflow_run_id'], lambda r: r.update(activations=activations, pending=[token]), 'fixture')
    recovered = storage.reconcile_run(run['workflow_run_id'])
    current = recovered['pending'][0]
    if settled_retry:
        assert current['execution_activation_id'] == 'retry'
        assert current['result']['status'] == 'succeeded'
        assert recovered['activations'][0]['resolved_by_execution_id'] == 'retry'
    else:
        assert 'execution_activation_id' not in current
        assert not current.get('execution_complete')
        assert not current.get('recovered_result')
        assert not current.get('recovered_failed_result')
    assert current['assignment_prompt'] == 'Accepted retry'


def test_no_checklist_replan_is_guided_and_bounded_by_existing_node_attempt_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, 'is_installed', lambda backend: True)
    definition = w.validate_definition({'name': 'bounded-plan', 'orchestrator': {'backend': 'codex'}, 'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'plan', 'type': 'agent', 'role': 'planning', 'agent': {'backend': 'codex'}, 'require_technical_plan': False, 'max_attempts': 1}, {'id': 'end', 'type': 'end'}], 'connections': [{'id': 'begin', 'source': 'start', 'target': 'plan'}, {'id': 'finish', 'source': 'plan', 'target': 'end'}]})
    run = w.WorkflowStore(tmp_path).create_run(definition, 'request', tmp_path)
    node = definition['nodes'][1]
    value = {'status': 'succeeded', 'result': {'no_checklist_needed': True, 'checklist_reason': 'Simple'}, 'evidence': []}
    activation = {'id': 'execution', 'role': 'node', 'node_id': 'plan', 'status': 'completed', 'tasks': [{'task_id': 'task', 'status': 'completed'}], 'node_result': value}
    run['activations'] = [activation]
    token = {'id': 'token', 'execution_complete': True, 'execution_activation_id': 'execution', 'result': value}
    assert d.retry_eligible(activation, guided=True, node=node)
    assert not d.retry_eligible(activation, node=node)
    assert not any(choice['kind'] == 'retry_execution' for choice in d.continuations(run, node, token, False))
    run['attempt_grants'] = {'plan': 1}
    assert next(choice for choice in d.continuations(run, node, token, False) if choice['kind'] == 'retry_execution')['attempts_remaining'] == 1
