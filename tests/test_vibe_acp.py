"""Vibe ACP control protocol: verification, correlation and one-shot decisions."""
import json
from copy import deepcopy

import pytest

from harflow_mcp.backends import Accumulator
from harflow_mcp.backends import vibe_acp as acp
from harflow_mcp.backends.base import ApprovalRefused, Invocation


def context(**changes):
    return {"transport": acp.TRANSPORT, "version": acp.VERSION, "cwd": "/fixture/repo", "prompt": "exact prompt", "freedom": "read_only", "mode": "ask", "model": None,
            "session_id": None, "max_turns": 3, "write": False, "caller_approvals": True, "trust": "trust_cwd", **changes}


def response(state, result):
    key = next(iter(state.pending))
    return {"jsonrpc": "2.0", "id": acp.untyped(key), "result": result}


def config(mode="ask", model="local"):
    return {"configOptions": [{"id": "mode", "currentValue": mode}, {"id": "model", "currentValue": model}]}


def ready(**changes):
    acc = Accumulator()
    acc.stream_state['control_context'] = context(**changes)
    acp.initial_frames(acc)
    state = acp.state_for(acc)
    acp.step(state, response(state, {'protocolVersion': 1, 'agentInfo': {'name': '@mistralai/mistral-vibe', 'version': '3.0.3'}}))
    result = {'_meta': {'workspace_trust': {'status': 'untrusted', 'details': None}}, 'sessionId': 'session'}
    acp.step(state, response(state, result))
    while not state.prompt_started:
        acp.step(state, response(state, config(model=changes.get('model') or 'local')))
    return acc, state


def update(tool_id='tool', raw=None, tool='bash', status='pending', **extra):
    return {'jsonrpc': '2.0', 'method': 'session/update', 'params': {'sessionId': 'session', 'update': {
        'sessionUpdate': 'tool_call', 'toolCallId': tool_id, 'rawInput': raw if raw is not None else {'command': 'make test'},
        '_meta': {'tool_name': tool}, 'status': status, **extra}}}


def permission(tool_id='tool', request_id=1):
    return {'jsonrpc': '2.0', 'method': acp.REQUEST, 'id': request_id, 'params': {'sessionId': 'session', 'toolCall': {'toolCallId': tool_id}, 'options': [
        {'optionId': 'allow_once', 'kind': 'allow_once'}, {'optionId': 'reject_once', 'kind': 'reject_once'},
        {'optionId': 'allow_always', 'kind': 'allow_always', '_meta': {'required_permissions': [{'scope': 'command_pattern', 'invocation_pattern': 'make test', 'session_pattern': 'make *'}]}}
    ]}}


def test_full_lifecycle_verifies_config_and_only_then_prompts():
    acc, state = ready(model='selected')
    assert state.admissible
    frames = [json.loads(f) for f in state.outbox]
    assert [f['method'] for f in frames] == ['session/new', 'session/set_config_option', 'session/set_config_option', 'session/prompt']
    assert frames[-1]['params'] == {'sessionId': 'session', 'prompt': [{'type': 'text', 'text': 'exact prompt'}]}
    acp.step(state, response(state, {'stopReason': 'end_turn'}))
    assert acp.finished(acc) and state.terminal['stopReason'] == 'end_turn'


def test_runtime_version_mismatch_stops_before_session():
    state = acp.State(context())
    acc = Accumulator(); acc.stream_state['vibe_acp'] = state
    acp.initial_frames(acc)
    with pytest.raises(ApprovalRefused):
        acp.step(state, response(state, {'protocolVersion': 1, 'agentInfo': {'name': '@mistralai/mistral-vibe', 'version': '3.0.4'}}))
    assert not state.outbox and state.failed


def test_exact_scoped_trust_only_when_project_needs_it():
    state = acp.State(context())
    acc = Accumulator(); acc.stream_state['vibe_acp'] = state
    acp.initial_frames(acc)
    acp.step(state, response(state, {'protocolVersion': 1, 'agentInfo': {'name': '@mistralai/mistral-vibe', 'version': '3.0.3'}}))
    acp.step(state, response(state, {'sessionId': 'session', '_meta': {'workspace_trust': {'status': 'untrusted', 'details': {'cwd': '/fixture/repo'}}}}))
    trust = json.loads(state.outbox[-1])
    assert trust['method'] == '_trust/decision'
    assert trust['params'] == {'sessionId': 'session', 'cwd': '/fixture/repo', 'decision': 'trust_cwd'}
    acp.step(state, response(state, {'trust_status': 'trusted'}))
    assert not state.prompt_started


def test_approval_joins_raw_input_never_title_and_response_is_once():
    acc, state = ready()
    event = update(title='Pretend this is harmless')
    acp.step(state, event); acp.ingest(event, acc)
    decoded = acp.decode(permission(request_id='1'), {'state': state})
    assert decoded['action'] == {'tool_name': 'bash', 'input': {'command': 'make test'}}
    assert decoded['native_request_id'] == 's:1'
    for decision, option in [('approve', 'allow_once'), ('reject', 'reject_once')]:
        frame = json.loads(acp.encode(decoded, decision))
        assert frame['id'] == '1'
        assert frame['result']['outcome'] == {'outcome': 'selected', 'optionId': option}
    assert json.loads(acp.encode(decoded, 'cancel'))['result']['outcome'] == {'outcome': 'cancelled'}


@pytest.mark.parametrize('request_id', [None, True, 1.5, {}, []])
def test_malformed_request_ids_refused(request_id):
    _, state = ready()
    with pytest.raises(ApprovalRefused):
        acp.decode(permission(request_id=request_id), {'state': state})


def test_foreign_session_stale_and_missing_tool_fail_closed():
    _, state = ready()
    event = permission(); event['params']['sessionId'] = 'foreign'
    with pytest.raises(ApprovalRefused):
        acp.decode(event, {'state': state})
    assert acp.decode(permission(), {'state': state})['system_rejection'] == 'tool_call_not_started'


def test_changed_input_after_admission_stops_run():
    _, state = ready()
    acp.step(state, update())
    acp.decode(permission(), {'state': state})
    with pytest.raises(ApprovalRefused):
        acp.step(state, update(raw={'command': 'make publish'}))


def test_write_requires_opt_in_and_paths_use_full_input():
    _, state = ready()
    acp.step(state, update(tool='write_file', raw={'filePath': '/outside/a', 'content': 'exact'}))
    denied = acp.decode(permission(), {'state': state})
    assert denied['system_rejection'] == 'write_approval_disabled'
    assert acp.change_paths(denied['action']) == ['/outside/a']
    _, state = ready(write=True)
    acp.step(state, update(tool='write_file', raw={'filePath': '/outside/a', 'content': 'exact'}))
    assert 'system_rejection' not in acp.decode(permission(), {'state': state})


def test_history_before_prompt_and_foreign_updates_cannot_be_answers():
    acc = Accumulator(); acc.stream_state['control_context'] = context(session_id='session')
    state = acp.state_for(acc)
    text = {'jsonrpc': '2.0', 'method': 'session/update', 'params': {'sessionId': 'session', 'update': {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'history'}}}}
    acp.ingest(text, acc)
    assert not acc.summary
    acc, state = ready()
    text['params']['sessionId'] = 'foreign'
    with pytest.raises(ApprovalRefused):
        acp.ingest(text, acc)


def test_no_caller_request_rejected_without_waiting():
    acc, state = ready(caller_approvals=False)
    acp.ingest(update(), acc)
    acp.ingest(permission(), acc)
    assert json.loads(state.outbox[-1])['result']['outcome']['optionId'] == 'reject_once'


def test_notifications_observed_twice_do_not_duplicate_tools_or_text():
    acc, state = ready()
    event = update()
    acp.step(state, event); acp.ingest(event, acc)
    assert not state.ambiguous
    assert acp.normalize(event, acc)[0]['kind'] == 'tool_call'
    assert acp.normalize(event, acc) == []
    text = {'jsonrpc': '2.0', 'method': 'session/update', 'params': {'sessionId': 'session', 'update': {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'repeat'}}}}
    acp.step(state, text); acp.ingest(text, acc)
    acp.step(state, deepcopy(text)); acp.ingest(deepcopy(text), acc)
    assert acc.summary == 'repeatrepeat'


def test_consumed_grant_then_ingest_sends_only_one_response(monkeypatch):
    from harflow_mcp.backends import vibe_commands
    acc, state = ready(caller_approvals=False)
    state.ctx['vibe_commands'] = {'placeholder': True}
    monkeypatch.setattr(vibe_commands, 'automatic_decision', lambda *_: True)
    acp.step(state, update())
    state.outbox.clear()
    event = permission()
    assert acp.consume_granted(event, {'state': state})
    acp.ingest(event, acc)
    responses = [json.loads(frame) for frame in state.outbox]
    assert len(responses) == 1
    assert responses[0]['result']['outcome']['optionId'] == 'allow_once'
    assert not acc.denials


def test_duplicate_option_identity_with_other_kind_is_never_approved():
    _, state = ready()
    acp.step(state, update())
    event = permission()
    event['params']['options'].append({'optionId': 'allow_once', 'kind': 'allow_always'})
    with pytest.raises(ApprovalRefused):
        acp.decode(event, {'state': state})


@pytest.mark.parametrize('kind', ['tool_call', 'tool_call_update'])
def test_tool_metadata_identity_drift_after_admission_stops(kind):
    _, state = ready()
    acp.step(state, update(kind='execute'))
    acp.decode(permission(), {'state': state})
    changed = update(tool='write_file', kind='execute')
    changed['params']['update']['sessionUpdate'] = kind
    with pytest.raises(ApprovalRefused):
        acp.step(state, changed)


@pytest.mark.parametrize('metadata', [None, [], 'bad'])
def test_malformed_handshake_metadata_fails_closed(metadata):
    state = acp.State(context())
    acc = Accumulator(); acc.stream_state['vibe_acp'] = state
    acp.initial_frames(acc)
    acp.step(state, response(state, {'protocolVersion': 1, 'agentInfo': {'name': '@mistralai/mistral-vibe', 'version': '3.0.3'}}))
    with pytest.raises(ApprovalRefused):
        acp.step(state, response(state, {'sessionId': 'session', '_meta': metadata}))
    assert state.failed and not state.prompt_started


def test_assert_safe_rejects_unexpected_environment(monkeypatch):
    from dataclasses import replace
    identity = {'launcher': '/fixture/bin/vibe-acp', 'files': []}
    monkeypatch.setattr(acp, 'runtime_identity', lambda *_: identity)
    invocation = acp.prepare(Invocation(['vibe'], env={'VIBE_CLI': 'python'}), {'repo': '/fixture/repo', 'prompt': 'exact'}, 'read_only')
    assert acp.assert_safe(invocation, 'read_only') is None
    assert acp.assert_safe(replace(invocation, env={'VIBE_CLI': 'python', 'VIBE_HOME': '/other'}), 'read_only')


def test_ledger_rejection_classification_and_matching_effect_enrichment():
    acc, state = ready()
    acp.step(state, update())
    row = acp.decode(permission(), {'state': state})
    acp.note_answer(acc, {**row, 'decided_by': 'caller'}, 'reject', True)
    assert acp.classify_denials(acc.denials, acc)[0]['denial_category'] == 'caller_rejected'
    recovered = {'jsonrpc': '2.0', 'method': 'session/update', 'params': {'sessionId': 'session', 'update': {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'recovered'}}}}
    acp.ingest(recovered, acc)
    effect = update(status='failed', rawOutput={'error': {'code': 'tool_denied', 'message': 'Tool execution denied by approval callback'}})
    effect['params']['update']['sessionUpdate'] = 'tool_call_update'
    acp.ingest(effect, acc)
    assert len(acc.denials) == 1 and acc.summary == 'recovered' and not acc.notices


def test_structured_prompt_usage_only_nonnegative_integer_fields():
    acc, state = ready()
    terminal = response(state, {'stopReason': 'end_turn', 'usage': {'inputTokens': 10, 'outputTokens': 3, 'totalTokens': 13, 'cachedReadTokens': -1}})
    acp.ingest(terminal, acc)
    assert acc.usage == {'input_tokens': 10, 'output_tokens': 3, 'total_tokens': 13}
    assert acc.total_cost_usd is None


def test_config_response_cannot_silently_change_selected_mode():
    state = acp.State(context())
    acc = Accumulator(); acc.stream_state['vibe_acp'] = state
    acp.initial_frames(acc)
    acp.step(state, response(state, {'protocolVersion': 1, 'agentInfo': {'name': '@mistralai/mistral-vibe', 'version': '3.0.3'}}))
    acp.step(state, response(state, {'sessionId': 'session', '_meta': {'workspace_trust': {'status': 'trusted'}}}))
    with pytest.raises(ApprovalRefused):
        acp.step(state, response(state, config(mode='auto-approve')))
    assert not state.prompt_started


def test_command_grant_frozen_repository_bound_to_control_cwd(monkeypatch):
    from dataclasses import replace
    from harflow_mcp.backends import vibe_commands
    identity = {'launcher': '/fixture/bin/vibe-acp', 'files': []}
    monkeypatch.setattr(acp, 'runtime_identity', lambda *_: identity)
    invocation = vibe_commands.configure(Invocation(['vibe'], env={'VIBE_CLI': 'python'}), ('make test',), 'write_in_repo', '/fixture/repo', '3.0.3')
    invocation = acp.prepare(invocation, {'repo': '/fixture/repo', 'prompt': 'exact'}, 'write_in_repo')
    assert acp.assert_safe(invocation, 'write_in_repo') is None
    changed = deepcopy(invocation.control_context)
    changed['cwd'] = '/fixture/other'
    assert acp.assert_safe(replace(invocation, control_context=changed), 'write_in_repo')
    changed = deepcopy(invocation.control_context)
    changed['vibe_commands']['patterns'] = ['make publish']
    assert acp.assert_safe(replace(invocation, control_context=changed), 'write_in_repo')


def test_known_bound_write_policy_rejection_has_observed_failed_outcome():
    _, state = ready(write=False)
    acp.step(state, update(tool='write_file', raw={'filePath': '/outside/a', 'content': 'exact'}))
    denied = acp.decode(permission(), {'state': state})
    assert denied['system_rejection'] == 'write_approval_disabled'
    failed = update(tool='write_file', raw={'filePath': '/outside/a', 'content': 'exact'}, status='failed')
    failed['params']['update']['sessionUpdate'] = 'tool_call_update'
    acp.step(state, failed)
    assert acp.outcomes(failed, state) == [('tool', 'failed')]


def test_bootstrap_deadline_not_extended_by_wrong_id_and_not_active_prompt(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(acp.time, 'monotonic', lambda: clock[0])
    acc = Accumulator(); acc.stream_state['control_context'] = context()
    acp.initial_frames(acc)
    state = acp.state_for(acc)
    clock[0] = 129.0
    acp.step(state, {'jsonrpc': '2.0', 'id': '1', 'result': {}})
    assert state.bootstrap_deadline == 130.0
    clock[0] = 130.0
    with pytest.raises(ApprovalRefused):
        acp.turn_frames(acc)
    assert state.failed and not state.prompt_started
    active_acc, active_state = ready()
    clock[0] = 10000.0
    acp.turn_frames(active_acc)
    assert active_state.admissible


def test_provisional_untrusted_session_is_not_exposed_as_task_session():
    acc = Accumulator(); acc.stream_state['control_context'] = context()
    acp.initial_frames(acc)
    state = acp.state_for(acc)
    acp.ingest(response(state, {'protocolVersion': 1, 'agentInfo': {'name': '@mistralai/mistral-vibe', 'version': '3.0.3'}}), acc)
    acp.ingest(response(state, {'sessionId': 'provisional', '_meta': {'workspace_trust': {'status': 'untrusted', 'details': {'cwd': '/fixture/repo'}}}}), acc)
    assert acc.session_id is None
    acp.ingest(response(state, {'trust_status': 'trusted'}), acc)
    acp.ingest(response(state, {}), acc)  # close the provisional session
    acp.ingest(response(state, {'sessionId': 'final', '_meta': {'workspace_trust': {'status': 'trusted'}}}), acc)
    assert acc.session_id is None
    while not state.prompt_started:
        acp.ingest(response(state, config()), acc)
    assert acc.session_id == 'final'
    assert any(json.loads(f)['method'] == 'session/prompt' for f in state.outbox)


def test_native_persistent_model_setter_cannot_be_emitted():
    state = acp.State(context(model='selected'))
    with pytest.raises(ValueError):
        acp.request_frame(state, 'session/set_config_option', {'sessionId': 'session', 'configId': 'model', 'value': 'selected'})
    assert not state.pending


@pytest.mark.parametrize('reason', ['end_turn', 'cancelled'])
def test_queued_dispatch_grant_is_discarded_when_prompt_ends(monkeypatch, reason):
    from harflow_mcp.backends import vibe_commands
    acc, state = ready(caller_approvals=False)
    state.ctx['vibe_commands'] = {'placeholder': True}
    monkeypatch.setattr(vibe_commands, 'automatic_decision', lambda *_: True)
    acp.step(state, update())
    state.outbox.clear()
    assert acp.consume_granted(permission(), {'state': state})
    assert len(state.outbox) == 1
    acp.ingest(response(state, {'stopReason': reason}), acc)
    assert state.terminal and not state.outbox
    assert acp.turn_frames(acc) == []
    # Defensive suppression also covers frames appended after terminal handling.
    state.outbox.append(b'queued stale response\n')
    assert acp.turn_frames(acc) == [] and not state.outbox
