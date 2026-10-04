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
